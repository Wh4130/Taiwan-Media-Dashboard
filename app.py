from pipelines.cna_etl import CNA_ETL
from pipelines.udn_etl import UDN_ETL
from utils.constants import SCRAPER_SETTINGS

import asyncio
import logging
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import uvicorn
from fastapi import FastAPI, BackgroundTasks, Query

app = FastAPI()

@app.get("/start_cna_etl")
async def start_cna_etl(background_tasks: BackgroundTasks,
                    k: str = Query('25', description = "factor k for clicking more button. should be a integer like string"),
                    t: str = Query('0.1', description = "factor t for waiting time. should be a float like string")
                    ):

    logging.info("start CNA ETL process")

    k = int(k)
    t = float(t)

    background_tasks.add_task(CNA_ETL, k, t)

    return {"message": "ETL process started in background"}

@app.get("/start_udn_etl")
async def start_udn_etl(background_tasks: BackgroundTasks,
                    k: str = Query('25', description = "factor k for clicking more button. should be a integer like string"),
                    t: str = Query('0.1', description = "factor t for waiting time. should be a float like string")
                    ):
    
    logging.info("start UDN ETL process")
    
    k = int(k)
    t = float(t)

    background_tasks.add_task(UDN_ETL, k, t)

    return {"message": "ETL process started in background"}





# async def main():

    # tasks = [
    #     asyncio.to_thread(UDN_ETL),
    #     asyncio.to_thread(CNA_ETL)
    # ]

    # await asyncio.gather(*tasks)


# app = FastAPI()

# @app.get("/cna_scrape")
# async def cna_scrape(background_tasks: BackgroundTasks):
#     background_tasks.add_task(CNA_ETL)

if __name__ == "__main__":
    # asyncio.run(main())
    
    uvicorn.run(app, host="0.0.0.0", port=8000)