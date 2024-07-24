import os
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
import subprocess
from subprocess import run
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging
import boto3
import scrapy
from pydantic import BaseModel
from scrapy.http import FormRequest
import logging
import boto3
import os
from scrapy.utils.reactor import install_reactor

# Sample website to run: https://staging.d5dg81r3e79zs.amplifyapp.com/
app = FastAPI()

class ScrapyRequest(BaseModel):
    url:str
    depth_limit:int
    total_links:int
    is_login_page: bool
    username:str
    password:str
    allowed_domains:str

# Spider class that takes in a start URL and created pdf files
class mySpider(scrapy.Spider):
    name = 'mySpider'
    install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")
    
    #Initializes the spider
    def __init__(self, url, depth_limit, total_links, is_login_page, username, password, allowed_domains=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_urls=[url]
        self.total_links = total_links
        self.res = []
        self.username = username
        self.password=password
        self.is_login_page = is_login_page
        self.count=0
        self.tick=False
        self.linkmap = {}
        if allowed_domains:
            self.allowed_domains = allowed_domains.split(',')
        self.depth_limit=depth_limit

    # Parses the webpages and goes n depth
    def parse(self, response):
        self.logger.info("Entered parse function")
        currentdepth = response.meta.get('depth')
        self.res.append(response.url)
        links = response.css('a::attr(href)').getall()
        self.linkmap[response.url] = links
        if self.is_login_page=='True' and self.tick==False:
            self.logger.info("Entered login statement")
            yield scrapy.Request(response.url, callback=self.login)
            self.tick==True
        if self.is_login_page=='False' or self.tick==True:
            self.logger.info("Entered conversion process")
            if len(self.res)<=int(self.total_links):
                self.logger.info("Converting into pdf")
                self.convertURLtoPDF(response)
            if currentdepth<int(self.depth_limit):
                for href in response.css('a::attr(href)'):
                    yield response.follow(href, self.parse, meta = {'depth':currentdepth+1})

    # Logins into specified page
    def login(self, response):
        self.logger.info("Enetered login function")
        token = response.css("form input[name=csrf_token]::attr(value)").extract_first()
        return FormRequest.from_response(response,
                                         formdata={'csrf_token': token,
                                                   'password': self.password,
                                                   'username': self.username},
                                         callback=self.scr)


    def convertURLtoPDF(self, response):
        BUCKET_NAME = 'cortex-web-crawler-bucket'
        s3 = boto3.client('s3')
        s3_key = 'wkhtmltopdf.exe'  # Adjust the key based on your S3 location

        try:
            self.count += 1
            filename = f"file{self.count}.html"
            s3_key = f'{filename}'
            s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=response.body)
            self.logger.info("Upload success")

        except Exception as e:
            print(f'Error converting URL to PDF: {e}')
 


    # Scrapes all webpages from a specified login page
    def scr(self, response):
        self.logger.info("Entered login scr")
        currentdepth = response.meta.get('depth')
        self.res.append(response.url)
        links = response.css('a::attr(href)').getall()
        self.linkmap[response.url] = links
        self.logger.info("Entered conversion process")
        if len(self.res)<=int(self.total_links)+1:
            self.logger.info("Converting into pdf")
            self.convertURLtoPDF(response)
        if currentdepth<int(self.depth_limit)+1:
            for href in response.css('a::attr(href)'):
                yield response.follow(href, self.parse, meta = {'depth':currentdepth+1})


@app.post("/crawl", status_code=201)
async def crawl_website(request: ScrapyRequest):
    url = request.url
    depth_limit = request.depth_limit
    total_links = request.total_links
    allowed_domains = ",".join(request.allowed_domains)
    is_login_page = request.is_login_page
    username = request.username
    password = request.password
    base_path = os.path.dirname(os.path.realpath(__file__))  # Current script directory
    relative_path = os.path.join(base_path, "main.py")
    logging.info(relative_path)
    # Building the subprocess command
    command = [
        "scrapy", "runspider", relative_path,
        "-a", f"url={url}",
        "-a", f"depth_limit={depth_limit}",
        "-a", f"total_links={total_links}",
        "-a", f"is_login_page={is_login_page}",
        "-a", f"username={username}",
        "-a", f"password={password}",
        "-a", f"allowed_domains={allowed_domains}"

    ]

    try:
        subprocess.call(command)
        
        return {"message": "Successful response"}
    except Exception as e:
        return {"message": f"Error: {str(e)}"}


@app.get("/download_links")
def download_all_files():
    bucket_name = 'cortex-web-crawler-bucket'
# Create Boto3 S3 client
    s3_client = boto3.client('s3')
    res = []
    try:
        # List all objects in the bucket
        objects = s3_client.list_objects_v2(Bucket=bucket_name)

        # Iterate over objects and download each one
        for obj in objects['Contents']:
            # Generate presigned URL for the object
            presigned_url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': obj['Key']}
            )
            # For simplicity, printing the URL here
            print(f"Downloading: {presigned_url}")
            res.append("Link: "+ presigned_url)
        return {"Download Links": res}

    except Exception as e:
        return {"error": str(e)}
    

@app.delete("/delete_s3_bucket_contents")
async def delete_s3_bucket_contents():

# Create an S3 client
    bucket_name = 'cortex-web-crawler-bucket'
    s3_client = boto3.client('s3')


    try:
        objects = s3_client.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' in objects:
            for obj in objects['Contents']:
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        
        return {"message": f"Contents of S3 bucket '{bucket_name}' deleted successfully."}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete bucket contents: {str(e)}")
