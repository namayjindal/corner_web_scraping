import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import json

def debug_scraper():
    # Initialize driver
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 15)
    
    try:
        # Load the page
        search_url = "https://www.google.com/maps/search/L'Industrie+Pizzeria/"
        driver.get(search_url)
        time.sleep(5)
        
        debug_info = {
            "page_source": driver.page_source,
            "visible_text": driver.find_element(By.TAG_NAME, "body").text,
            "elements": {}
        }
        
        # Try different selectors and log what we find
        selectors_to_check = {
            "body_medium": "div.fontBodyMedium",
            "reviews_tab": "button[role='tab']",
            "reviews_button": "[aria-label*='Reviews']",
            "all_buttons": "button",
            "all_divs": "div",
            "review_elements": ".wiI7pd"
        }
        
        for name, selector in selectors_to_check.items():
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            debug_info["elements"][name] = {
                "count": len(elements),
                "texts": [elem.text for elem in elements],
                "attributes": [{
                    "class": elem.get_attribute("class"),
                    "role": elem.get_attribute("role"),
                    "aria-label": elem.get_attribute("aria-label")
                } for elem in elements]
            }
        
        # Save all debug info
        with open('debug_output.json', 'w', encoding='utf-8') as f:
            json.dump(debug_info, f, indent=4, ensure_ascii=False)
        
        # Print summary
        print("\n=== Debug Summary ===")
        print("\nElements found:")
        for name, data in debug_info["elements"].items():
            print(f"\n{name}:")
            print(f"Count: {data['count']}")
            if data['count'] > 0:
                print("First element text:", data['texts'][0][:100] + "..." if data['texts'][0] else "No text")
                print("First element attributes:", data['attributes'][0])
        
    except Exception as e:
        print(f"Error during debugging: {str(e)}")
    finally:
        driver.quit()

if __name__ == "__main__":
    debug_scraper()