import requests
from faker import Faker
import json
import random
import string
import time

fake = Faker()

def generate_random_log():
    log_data = {
        "level": random.choice(["info", "warning", "error"]),
        "message": fake.sentence(),
        "resourceId": "server-" + ''.join(random.choices(string.digits, k=4)),
        "timestamp": fake.iso8601(),
        "traceId": fake.uuid4(),
        "spanId": "span-" + ''.join(random.choices(string.digits + string.ascii_lowercase, k=3)),
        "commit": fake.sha1(),
        "metadata": {
            "parentResourceId": "server-" + ''.join(random.choices(string.digits, k=4))
        }
    }
    return log_data

def post_log(log_data, api_url):
    start_time = time.time()
    response = requests.post(api_url, json=log_data)
    end_time = time.time()
    
    if response.status_code == 200:
        print(f"Log posted successfully. Time taken: {end_time - start_time:.2f} seconds.")
    else:
        print(f"Failed to post log. Status code: {response.status_code}, Response: {response.text}")

if __name__ == "__main__":
    api_url = "http://127.0.0.1:3000/ingest"  # Replace with your FastAPI endpoint URL
    for i in range(10):
        log_data = generate_random_log()    
        post_log(log_data, api_url)