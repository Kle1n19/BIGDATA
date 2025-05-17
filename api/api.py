from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import json
import os

app = FastAPI()

def load_json(filename: str):
    try:
        with open(filename, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"{filename} not found")
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail=f"{filename} contains invalid JSON")

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
