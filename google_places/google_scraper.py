import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import json
import logging
import os
from datetime import datetime
import re
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GooglePlacesScraper:
    def __init__(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, 15)

    def extract_place_details(self, original_name: str, google_id: str) -> dict:
        """Extract details using precise selectors we found working"""
        try:
            url = f"https://www.google.com/maps/place/?q=place_id:{google_id}"
            self.driver.get(url)
            time.sleep(5)  # Wait for initial load

            details = {
                'timestamp': datetime.now().isoformat(),
                'google_id': google_id
            }

            # Hours - This is working well from before
            try:
                hours_element = self.driver.find_element(By.CLASS_NAME, "t39EBf")
                hours_text = hours_element.get_attribute('aria-label')
                if hours_text:
                    hours_dict = {}
                    for day_hours in hours_text.split(';'):
                        if ',' in day_hours and 'Hide' not in day_hours:
                            day, hours = day_hours.split(',', 1)
                            hours_dict[day.strip()] = hours.strip()
                    details['hours'] = hours_dict
            except NoSuchElementException:
                details['hours'] = None

            # Price - New enhanced extraction
            try:
                # Strategy 1: Header area near name/category
                header_selectors = [
                    "span.ZDu9vd",
                    "div.LBgpqf",
                    "span.mgr77e",
                    "div.iTxXHe"
                ]
                
                for selector in header_selectors:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        text = element.text
                        if '$' in text:
                            price_match = re.search(r'(\$+(?!\d)|\$\d+(?:[-–]\$?\d+)?)', text)
                            if price_match:
                                details['price'] = price_match.group(0)
                                break
                    if details.get('price'):
                        break

                # Strategy 2: Attributes section
                if not details.get('price'):
                    attribute_selectors = [
                        "div[aria-label*='Price range']",
                        "div[aria-label*='Price: ']",
                        "button[aria-label*='Price']",
                        "span[aria-label*='Price']"
                    ]
                    
                    for selector in attribute_selectors:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        for element in elements:
                            aria_label = element.get_attribute('aria-label')
                            if aria_label and '$' in aria_label:
                                price_match = re.search(r'(\$+(?!\d)|\$\d+(?:[-–]\$?\d+)?)', aria_label)
                                if price_match:
                                    details['price'] = price_match.group(0)
                                    break
                        if details.get('price'):
                            break

                # Strategy 3: About section
                if not details.get('price'):
                    try:
                        about_button = self.driver.find_element(By.CSS_SELECTOR, "button[aria-label*='About']")
                        about_button.click()
                        time.sleep(1)
                        
                        about_content = self.driver.find_element(By.CSS_SELECTOR, "div.m6QErb")
                        about_text = about_content.text
                        
                        if '$' in about_text:
                            price_match = re.search(r'(\$+(?!\d)|\$\d+(?:[-–]\$?\d+)?)', about_text)
                            if price_match:
                                details['price'] = price_match.group(0)
                    except:
                        pass

            except Exception as e:
                logger.debug(f"Error extracting price: {str(e)}")
                details['price'] = None

            # Reviews - This is working well from before
            try:
                reviews_button = self.driver.find_element(By.CSS_SELECTOR, "[aria-label*='Reviews']")
                reviews_button.click()
                time.sleep(3)
                
                review_elements = self.driver.find_elements(By.CSS_SELECTOR, "span.wiI7pd")
                reviews = []
                for review in review_elements[:5]:
                    try:
                        try:
                            more_button = review.find_element(By.CSS_SELECTOR, "button.w8nwRe")
                            more_button.click()
                            time.sleep(0.5)
                        except:
                            pass
                        
                        review_text = review.text
                        if review_text:
                            reviews.append(review_text)
                    except:
                        continue
                details['reviews'] = reviews if reviews else None
                
                # Rating
                try:
                    rating_element = self.driver.find_element(By.CSS_SELECTOR, "div.F7nice span")
                    details['rating'] = float(rating_element.text)
                except:
                    details['rating'] = None
                    
            except Exception as e:
                details['reviews'] = None
                details['rating'] = None

            return details

        except Exception as e:
            logger.error(f"Error scraping place {original_name} ({google_id}): {str(e)}")
            return None

    def scrape_places_incrementally(self, input_csv: str, output_csv: str):
        """Scrape places and save incrementally"""
        df = pd.read_csv(input_csv)
        
        scraped_ids = set()
        if os.path.exists(output_csv):
            existing_df = pd.read_csv(output_csv)
            scraped_ids = set(existing_df['google_id'])
            logger.info(f"Found {len(scraped_ids)} previously scraped places")

        output_file_exists = os.path.exists(output_csv)
        
        for idx, row in df.iterrows():
            if row['google_id'] in scraped_ids:
                logger.info(f"Skipping {row['name']} - already scraped")
                continue
                
            logger.info(f"Scraping {idx + 1}/{len(df)}: {row['name']}")
            
            details = self.extract_place_details(row['name'], row['google_id'])
            if details:
                result = {
                    'corner_place_id': row['corner_place_id'],
                    'name': row['name'],
                    'neighborhood': row['neighborhood'],
                    'website': row['website'],
                    'instagram_handle': row['instagram_handle'],
                    **details
                }
                
                result_df = pd.DataFrame([result])
                result_df.to_csv(output_csv, 
                               mode='a', 
                               header=not output_file_exists,
                               index=False)
                
                output_file_exists = True
                scraped_ids.add(row['google_id'])
            
            if (idx + 1) % 10 == 0:
                time.sleep(random.uniform(15, 25))

        logger.info(f"Scraping completed. Results saved to {output_csv}")

    def close(self):
        self.driver.quit()

def main():
    scraper = GooglePlacesScraper()
    try:
        scraper.scrape_places_incrementally('places.csv', 'places_with_google_data.csv')
    finally:
        scraper.close()

if __name__ == "__main__":
    main()