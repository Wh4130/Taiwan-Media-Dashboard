from etl_tasks.tvbs_scraping import TVBS_scraper
from etl_tasks.mongodb import MongoDbManager
from utils.constants import SCRAPER_SETTINGS
from utils.email_sender import EmailSender
import logging
import datetime as dt

import asyncio
import os
from dotenv import load_dotenv

load_dotenv()


import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

def TVBS_ETL(p = SCRAPER_SETTINGS['tvbs']['p']):
    try:
        begin = dt.datetime.now()

        # --- Instantiate TVBS scraper class
        print("[tvbs] start scraping TVBS news...")
        tvbs = TVBS_scraper(SCRAPER_SETTINGS['tvbs']['base_url'])

        print("[tvbs] start driver...")
        tvbs.start_tvbs_driver()

        print("[tvbs] get news url list...")
        tvbs.get_news_url_ls()

        tvbs.quit()
        
        print("[tvbs] start scraping individual news...")
        tvbs.scrape_news_batch(p)

        print("[tvbs] Done scraping! Loading to MongoDB atlas...")

        mongo = MongoDbManager()
        count_before = mongo.COUNT_DOCUMENT("tvbs")
        mongo.LOAD_TO_MONGODB("tvbs", tvbs.scraped_results)
        
        print("[tvbs] Done loading! Removing duplicated data...")
        removed_count = mongo.REMOVE_DUPLICATE("tvbs")["removed_count"]
        count_after = mongo.COUNT_DOCUMENT("tvbs")

        end = dt.datetime.now()

        return {
            "source": "tvbs",
            "count_before": count_before,
            "count_after": count_after,
            "removed_count": removed_count,
            "duration": end - begin
        }              

    except Exception as e:

        EmailSender.send(os.getenv("RECIPIENT"), f"Error occurred:\n\n {e}")
    