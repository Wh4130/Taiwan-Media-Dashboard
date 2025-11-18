import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import requests
import logging 
import selenium
import datetime as dt
import re
from bs4 import BeautifulSoup
import time
import math
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
import time
from tqdm import tqdm
from multiprocessing import Pool
from utils.constants import *


class TVBS_scraper:
    def __init__(self, link) -> None:
        self.url     = link 
        
        self.options = webdriver.ChromeOptions()
        self.options.add_argument("--headless=new")           # 無頭模式（不開視窗）
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")  # 避免 shared memory 不夠用
        self.options.add_argument("--disable-gpu")            # 在某些 Linux 上避免錯誤

        self.driver  = webdriver.Chrome(self.options)

        self.news_url_ls = []
        self.scraped_results = []


        
    def start_tvbs_driver(self):
        self.driver.get(self.url)
        self.driver.maximize_window()

    def scroll_down(self, k, t):

        div = self.driver.find_element(By.TAG_NAME, "body")
        for i in range(k):
            try:
                # 滾動到底部
                print(f"scrolling down...{i}")
                div.send_keys(Keys.PAGE_DOWN)

                # 如果是最後一次點擊，可以不等待
                if i < k - 1:
                    time.sleep(t) # 如果仍要強制等待，請保留
                
            except Exception as e:
                print(f"Scrolling down failed or finished: {e}")
                break

    def get_news_url_ls(self):
        """取得所有新聞連結"""
        container = self.driver.find_element(By.XPATH, 
                                             "/html/body/div[1]/main/div/article/div[2]/div[3]")
        container = container.find_element(By.TAG_NAME, "ul")
        news = container.find_elements(by = By.TAG_NAME, value = "li")
        news_url_ls = [_.find_elements(By.TAG_NAME, "a")[0].get_attribute("href") 
                       if _.find_elements(By.TAG_NAME, "a") else None for _ in news]
        self.news_url_ls = news_url_ls
        return news_url_ls
    
    def quit(self):
        self.driver.quit()
        print("web driver quit successfully.")

    def scrape_news_batch(self, p):
        """用 for loop 逐個爬取輸入列表內的新聞連結

        - p: 僅爬取 p 比例的新聞（p 介於 0 1 之間）
        
        """
        

        batch_results = []

        for url in self.news_url_ls[:math.ceil(len(self.news_url_ls) * p )]:

            try:

                headers = get_random_headers()
                
                body = requests.get(url, headers = headers)
                soup = BeautifulSoup(body.text, 'html.parser')

                article = soup.find("div", class_ = "article_new")
                title_box  = article.find("div", class_ = "title_box")
                title = title_box.find("h1", class_ = "title").text.strip()
                cate  = title_box.find("div", class_ = "bread").find_all("a")[-1].text.strip()
                url    = url


                updated_time = (title_box
                        .find("div", class_ = "author_box")
                        .find("div", class_ = "author")
                        .text
                        )
                try:
                    updated_time = (re.search(r"發佈時間：(\d{4}/\d{2}/\d{2}\s\d{2}:\d{2})", 
                                            updated_time)
                                    .group(1)
                                    .strip())

                    updated_time = dt.datetime.strptime(updated_time, "%Y/%m/%d %H:%M")
                except:
                    updated_time = None
                

                body = article.find("div", class_ = "article_content")
                content = body.get_text(separator="\n").strip()
                keywords = [
                    kw.text for kw in article.find("div", class_ = "article_keyword").find_all("a")
                ]
                
                batch_results.append(
                                        {
                                            "title": title,
                                            "url": url,
                                            "type": cate,
                                            "updated_time": updated_time,
                                            "content": content,
                                            "keywords": keywords
                                        }
                                    ) 
                
            except Exception as e:

                batch_results.append(
                    {
                        "url": url,
                        "error": str(e)
                    }
                )

                continue
        
        self.scraped_results = batch_results
        return batch_results



if __name__ == "__main__":
    tvbs = TVBS_scraper("https://news.tvbs.com.tw/realtime")
    tvbs.start_TVBS_driver()
    tvbs.scroll_down(3, 0.1)
    tvbs.get_news_url_ls()
    tvbs.quit()
    print(tvbs.news_url_ls)





    