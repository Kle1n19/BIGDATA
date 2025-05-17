from fastapi import FastAPI
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from pydantic import BaseModel
from typing import List, Optional
from datetime import date, datetime

app = FastAPI()

# Connect to Cassandra
cluster = Cluster(['cassandra-node1'])  # Replace with your Cassandra host
session = cluster.connect('wiki')  # Replace with your keyspace

# ------------------------
# 1. Get all distinct domains
# ------------------------
@app.get("/domains", response_model=List[str])
def get_distinct_domains():
    rows = session.execute("SELECT domain FROM distinct_domains")
    return [row.domain for row in rows]

# ------------------------
# 2. Get all pages by user_id
# ------------------------
class Page(BaseModel):
    page_title: str
    page_id: int

@app.get("/users/{user_id}/pages", response_model=List[Page])
def get_pages_by_user(user_id: int):
    rows = session.execute("SELECT page_id, page_title FROM pages_by_user WHERE user_id = %s", [user_id])
    if not rows:
        return []
    return [Page(**row._asdict()) for row in rows]

# ------------------------
# 3. Get article count by domain
# ------------------------
@app.get("/domains/{domain}/count")
def get_domain_page_count(domain: str):
    rows = session.execute("SELECT page_id FROM domain_page_counts WHERE domain = %s", [domain])
    page_count = sum(1 for _ in rows)  # Count the number of rows for the domain
    return {"domain": domain, "page_count": page_count}

# ------------------------
# 4. Get page by page_id
# ------------------------
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
    if not row:
        return None
    return PageDetails(**row._asdict()) if row else None

# ------------------------
# 5. Get user page counts in a date range
# ------------------------
# ------------------------
# 5. Get user page counts in a date range
# ------------------------
class UserPageCount(BaseModel):
    user_id: int
    username: str
    page_count: int  # Add a field for the count of page creations

@app.get("/page-creations", response_model=List[UserPageCount])
def get_user_page_counts(start_date: date, end_date: date):
    result = []
    current = start_date

    # Dictionary to aggregate page counts by user_id
    user_page_counts = {}

    while current <= end_date:
        rows = session.execute(
            "SELECT user_id, username FROM page_creations_by_time WHERE day = %s", [current]
        )
        for row in rows:
            user_id = row.user_id
            username = row.username

            # Aggregate page counts for each user
            if user_id in user_page_counts:
                user_page_counts[user_id]["page_count"] += 1
            else:
                user_page_counts[user_id] = {"username": username, "page_count": 1}

        # Move to the next day
        current = date.fromordinal(current.toordinal() + 1)

    # Convert aggregated results to the response model
    for user_id, data in user_page_counts.items():
        result.append(UserPageCount(user_id=user_id, username=data["username"], page_count=data["page_count"]))

    return result
