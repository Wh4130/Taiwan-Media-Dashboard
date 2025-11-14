from python_scripts.cna_scraping import CNA_scraper
from python_scripts.mongodb import MongoDbManager
from utils.constants import SCRAPER_SETTINGS
import logging

import asyncio

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

def CNA_ETL(k = SCRAPER_SETTINGS['cna']['K'] , t = SCRAPER_SETTINGS['cna']['T']):
    # --- Instantiate CNA scraper class
    print("start scraping CNA news...")
    cna = CNA_scraper(SCRAPER_SETTINGS['cna']['base_url'])

    print("start driver...")
    cna.start_cna_driver()

    print("click more button...")
    cna.click_more_btn(k, t)

    print("get news url list...")
    cna.get_news_url_ls()

    cna.quit()
    
    print("start scraping individual news...")
    cna.scrape_news_batch()

    print("Done scraping! Loading to MongoDB atlas...")

    mongo = MongoDbManager()
    mongo.LOAD_TO_MONGODB("cna", cna.scraped_results)
    
    print("Done loading! Removing duplicated data...")
    mongo.REMOVE_DUPLICATE("cna")

    print("Done!")


    