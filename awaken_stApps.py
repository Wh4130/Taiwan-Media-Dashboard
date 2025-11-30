import time
from selenium import webdriver
from selenium.webdriver.common.by import By

# * Cronjob that wakes up the streamlit apps
urls = {"bubble_tea": "https://dssq-bubble-tea-m9jzmospevfsw8uh2ncz4o.streamlit.app/",
        "easyessay_master": "https://easyessay-literature-review-toolkit-wally.streamlit.app/",
        "easyessay_guest": "https://easyessay-literature-review-toolkit-guest.streamlit.app/",
        "iii_Demand_Foresight_Tools (dev)": "https://demand-foresight-trend-report-generator-dev.streamlit.app/",
        "iii_Demand_Foresight_Tools (main)": "https://demand-foresight-trend-report-generator-main.streamlit.app/",
        "media_dashboard": "https://taiwan-media-dashboard-tool.streamlit.app/"}

def awaken_sleeping_apps():

    print("Logging streamlit apps...")

    driver = webdriver.Chrome()

    for title, url in urls.items():
        # Open a web page
        driver.get(url)
        time.sleep(5)

        try:
            driver.find_element(By.TAG_NAME, 'button').click()
            time.sleep(1)
            print(f"'{title}' is awaken!")
        except Exception as E:
            print(E)

        time.sleep(2)

    # Close the browser
    driver.quit()
