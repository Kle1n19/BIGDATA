from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from pydantic import BaseModel
from typing import List, Optional
from datetime import date, datetime
import json
import os

app = FastAPI()

cluster = Cluster(['cassandra'])
session = cluster.connect('wiki')

def load_json(filename: str):
    try:
        with open(filename, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"{filename} not found")
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail=f"{filename} contains invalid JSON")

@app.get("/domains", response_model=List[str])
def get_distinct_domains():
    rows = session.execute("SELECT domain FROM distinct_domains")
    return [row.domain for row in rows]

class Page(BaseModel):
    page_title: str
    page_id: int

@app.get("/users/{user_id}/pages", response_model=List[Page])
def get_pages_by_user(user_id: int):
    rows = session.execute("SELECT page_id, page_title FROM pages_by_user WHERE user_id = %s", [user_id])
    return [Page(**row._asdict()) for row in rows] if rows else []

@app.get("/domains/{domain}/count")
def get_domain_page_count(domain: str):
    rows = session.execute("SELECT page_id FROM domain_page_counts WHERE domain = %s", [domain])
    page_count = sum(1 for _ in rows)
    return {"domain": domain, "page_count": page_count}


class PageDetails(BaseModel):
    page_id: int
    page_title: str
    domain: str
    user_id: int
    username: str
    created_by_bot: bool
    time: datetime

@app.get("/pages/{page_id}", response_model=Optional[PageDetails])
def get_page_by_id(page_id: int):
    row = session.execute("SELECT * FROM page_details_by_page_id WHERE page_id = %s", [page_id]).one()
    return PageDetails(**row._asdict()) if row else None

class UserPageCount(BaseModel):
    user_id: int
    username: str
    page_count: int

@app.get("/page-creations", response_model=List[UserPageCount])
def get_user_page_counts(start_date: date, end_date: date):
    result = []
    current = start_date
    user_page_counts = {}

    while current <= end_date:
        rows = session.execute(
            "SELECT user_id, username FROM page_creations_by_time WHERE day = %s", [current]
        )
        for row in rows:
            user_id = row.user_id
            username = row.username
            if user_id in user_page_counts:
                user_page_counts[user_id]["page_count"] += 1
            else:
                user_page_counts[user_id] = {"username": username, "page_count": 1}
        current = date.fromordinal(current.toordinal() + 1)

    for user_id, data in user_page_counts.items():
        result.append(UserPageCount(user_id=user_id, username=data["username"], page_count=data["page_count"]))
    return result

@app.get("/bot_stat")
async def read_bot_stat():
    data = load_json("/output/bot_created_domain_stats.json")
    return JSONResponse(content=data)

@app.get("/domain_stat")
async def read_domain_stat():
    data = load_json("/output/hourly_domain_page_stats.json")
    return JSONResponse(content=data)

@app.get("/top_20")
async def read_top_users():
    data = load_json("/output/top_20_users.json")
    return JSONResponse(content=data)
