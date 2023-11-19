from fastapi import FastAPI,HTTPException,Query
from typing import Union
from pydantic import BaseModel

from elasticsearch import Elasticsearch
from fastapi.responses import HTMLResponse
import json
import re

query_form = """
<style>
    table {
        border-collapse: collapse;
        width: 100%;
    }
    th, td {
        border: 1px solid #dddddd;
        text-align: left;
        padding: 8px;
    }
    th {
        background-color: #f2f2f2;
    }

    form input {
        margin-top: 10px;
    }
    
</style>
<form action="/search" method="get">
    
    <label>Query:</label>
    <input type="text" name="q" value=""><br>

    
    <label>Level:</label>
    <input type="text" name="level" value=""><br>
    
    <label>Message:</label>
    <input type="text" name="message" value=""><br>
    
    <label>Resource ID:</label>
    <input type="text" name="resourceId" value=""><br>
    
    <label>Timestamp:</label>
    <input type="text" name="timestamp" value=""><br>

    <label>Start Timestamp:</label>
    <input type="text" name="start_timestamp" value=""><br>

    <label>End Timestamp:</label>
    <input type="text" name="end_timestamp" value=""><br>
    
    <label>Trace ID:</label>
    <input type="text" name="traceId" value=""><br>
    
    <label>Span ID:</label>
    <input type="text" name="spanId" value=""><br>
    
    <label>Commit:</label>
    <input type="text" name="commit" value=""><br>
    
    <label>Parent Resource ID:</label>
    <input type="text" name="parentResourceId" value=""><br>
    <label>Number Of Results(Default 100):</label>
    <input type="number" name="size" value="100"><br>
    
    <button type="submit">Search</button>
</form>
"""


#Creating a model to get data

class Log(BaseModel):
    level:str
    message:str
    resourceId: str
    timestamp:str
    traceId:str
    spanId:str
    commit:str
    metadata:dict

es = Elasticsearch(["http://localhost:9200"])

index_name = "logs"
mapping_body = {
    "mappings": {
        "properties": {
            "level": {
                "type": "keyword"
            },
            "resourceId": {
                "type": "keyword"
            },
            "traceId": {
                "type": "keyword"
            },
            "spanId": {
                "type": "keyword"
            },
            "commit": {
                "type": "keyword"
            },
            "message": {
                "type": "text"
            },
            "timestamp": {
                "type": "date"
            },
            "metadata": {
                "properties": {
                    "parentResourceId": {
                        "type": "keyword"
                    }
                }
            },
            "timestamp": {
                "type": "date",
                "format": "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
            },
        }
    }
}

if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, body=mapping_body)

app=FastAPI()

@app.get("/")
def home():
    return {"msg":"Home page Contains some instructions","routes":{"/ingest":"for injecting log data","/search":{"1":"for searcing data or querying log data",
    "2":"Query option ins search is like General search it will search include regxi search",
    "3":"if you want to apply specific filter then only type value on that feild only unlike  query feild specific feild does doesnt support regix search",
    "4":"you can combile all filters but dont combile Timestamp feild with start time stamp and end time stamp ",
    "5":"you combine more than one filter"}},
    "Technology or libraries used":["FastAPI","uvicorn sever","elastic search python library","Elastic search need to be installed on the your operating system"],
    "how to run this app":["First install all necessary software liek python libraries and software like elastic search and you need to run start elastic search server before staring fastapi","then start elastic search server using command  'sudo service elasticsearch start' ","to run fast api use this command 'uvicorn main:app  --reload --port 3000'"],
    "There is a dummy script along with program to generate dummp data ":"script name is gen3.py"
    }

@app.post("/ingest")
async def takinglogdata(log:Log):
    log_data=log.model_dump()
    try:
        # Index the log data into Elasticsearch
        index_result = es.index(index="logs", body=log_data)

        return {"message": "Log data indexed successfully", "elasticsearch_response": index_result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error indexing log data: {str(e)}")

@app.get("/search", response_class=HTMLResponse)
async def search_logs(
    index: str = "logs",
    q: str = Query(""),
    level: str = Query(""),
    message: str = Query(""),
    resourceId: str = Query(""),
    timestamp: str = Query(""),
    traceId: str = Query(""),
    spanId: str = Query(""),
    commit: str = Query(""),
    parentResourceId: str = Query(""),
    size: int = Query(""),
    start_timestamp: str=Query(""),
    end_timestamp: str=Query(""),
):
    # Check if no search parameters are provided
    if not any([q,level, message, resourceId, timestamp, traceId, spanId, commit, parentResourceId,start_timestamp,end_timestamp]):
        return HTMLResponse(content=query_form + "<p>No search parameters provided.</p>", status_code=200)

    elif all([timestamp,start_timestamp,end_timestamp]):
        return HTMLResponse(content=query_form + "<p>use timestamp or start timestamp and end timestamp</p>", status_code=200)

    try:
        # Build Elasticsearch query based on filters
        query_body = {
            "query": {
                "bool": {
                    "must": [],
                }
            }
        }

        if q:
            #query_body["query"]["bool"]["must"].append({"query_string": {"query": q}})
            query_body["query"]["bool"]["must"].append({"query_string": {"query": f"*{q}*",
                    "fields": ["message", "level", "resourceId", "traceId", "spanId", "commit", "parentResourceId"],
                }
            })

        if level:
            query_body["query"]["bool"]["must"].append({"match": {"level": level}})

        if message:
            query_body["query"]["bool"]["must"].append({"match": {"message": message}})

        if resourceId:
            query_body["query"]["bool"]["must"].append({"match": {"resourceId": resourceId}})


        if timestamp:
            query_body["query"]["bool"]["must"].append({"match": {"timestamp": timestamp}})

        if traceId:
            query_body["query"]["bool"]["must"].append({"match": {"traceId": traceId}})

        if spanId:
            query_body["query"]["bool"]["must"].append({"match": {"spanId": spanId}})

        if commit:
            query_body["query"]["bool"]["must"].append({"match": {"commit": commit}})


        if parentResourceId:
            query_body["query"]["bool"]["must"].append({"match": {"parentResourceId": parentResourceId}})
        
        if all([start_timestamp,end_timestamp]):
            timestamp_range = {}
            timestamp_range["gte"] = start_timestamp
            timestamp_range["lte"] = end_timestamp
            query_body["query"]["bool"]["must"].append({"range": {"timestamp": timestamp_range}})


        if size:
            query_body["size"] = size

        # Perform search
        result = es.search(index=index, body=query_body)

        # Extract hits
        hits = result.get("hits", {}).get("hits", [])

        # Return HTML response
        response_content = query_form + f"<p>Search Results: {len(hits)} hits</p>"

        if hits:
            response_content += "<table border='1'>"
            
            # Add table headers
            response_content += "<tr>"
            headers = hits[0].get("_source", {}).keys()
            for header in headers:
                response_content += f"<th>{header}</th>"
            response_content += "</tr>"
            
            # Add table rows
            for hit in hits:
                source = hit.get("_source", {})
                response_content += "<tr>"
                for value in source.values():
                    response_content += f"<td>{value}</td>"
                response_content += "</tr>"
            
            response_content += "</table>"
        else:
            response_content += "<p>No results found.</p>"
        return HTMLResponse(content=response_content, status_code=200)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching logs: {str(e)}")

"""if __name__=="__main__":
    import uvicorn
    uvicorn.run("main:app",host="127.0.0.1",port=3000,reload=True)"""