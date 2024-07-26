import json
import time
import random
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import boto3
import os

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
LAMBDA_CONFIG_FILE = "lambda.json"


def start():
    lambda_data = json.loads(open(LAMBDA_CONFIG_FILE, encoding="utf-8").read())

    lambda_clients = []
    for lambda_item in lambda_data:
        lambda_clients.append({
            "client": boto3.client(
                "lambda",
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=lambda_item["region"]),
            "arn": lambda_item["arn"],
            "url": lambda_item["url"]       
        }
        )

    while True:
        with ThreadPoolExecutor(max_workers=100) as executor:
            for lambda_item in lambda_clients:
                executor.map(lambda_refresh_thread, [lambda_item])
        time.sleep(0.5)


def lambda_refresh_thread(lambda_item):
    timeout = random.randint(30, 40)
    try:
        lambda_item["client"].update_function_configuration(
            FunctionName=lambda_item["arn"],
            Timeout=timeout
        )
    except Exception as e:
        pass


start()
