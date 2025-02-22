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

            # Hours
            details['hours'] = self._extract_hours()

            # Category
            try:
                category_element = self.driver.find_element(By.CLASS_NAME, "DkEaL")
                details['category'] = category_element.text
            except NoSuchElementException:
                try:
                    # Backup method - look for category in header area
                    category_element = self.driver.find_element(By.CSS_SELECTOR, "button[jsaction*='pane.rating.category']")
                    details['category'] = category_element.text
                except NoSuchElementException:
                    details['category'] = None

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
        
    def _clean_hours_text(self, text: str) -> str:
        """Clean up unicode characters and format hours text"""
        # Remove unicode characters
        text = text.replace('\u202f', ' ')
        # Standardize various dash types to a simple hyphen
        text = text.replace('–', '-').replace('—', '-')
        # Remove extra spaces
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _extract_hours(self) -> dict:
        """Extract hours with multiple fallback methods and cleaner formatting"""
        try:
            hours_dict = {}
            
            # Method 1: Try t39EBf class with aria-label (current working method)
            try:
                hours_element = self.driver.find_element(By.CLASS_NAME, "t39EBf")
                hours_text = hours_element.get_attribute('aria-label')
                if hours_text and ',' in hours_text:
                    for day_hours in hours_text.split(';'):
                        if ',' in day_hours and 'Hide' not in day_hours:
                            day, hours = day_hours.split(',', 1)
                            hours_dict[day.strip()] = self._clean_hours_text(hours.strip())
                    if hours_dict:
                        return hours_dict
            except NoSuchElementException:
                pass

            # Method 2: Try finding the hours table directly
            try:
                table = self.driver.find_element(By.CLASS_NAME, "eK4R0e")
                rows = table.find_elements(By.CLASS_NAME, "y0skZc")
                for row in rows:
                    try:
                        day = row.find_element(By.CLASS_NAME, "ylH6lf").text
                        time = row.find_element(By.CLASS_NAME, "mxowUb").text
                        if day and time:
                            hours_dict[day.strip()] = self._clean_hours_text(time)
                    except:
                        continue
                if hours_dict:
                    return hours_dict
            except NoSuchElementException:
                pass

            # Method 3: Try finding hours in different format
            selectors = [
                "div[aria-label*='Hours'] span.ZDu9vd",  # Current status
                "div[aria-label*='Hours'] div.MkV9",     # Another common location
                "div.o0Svhf span.ZDu9vd",                # Alternate structure
                "div[data-item-id*='oh']"                # Hours container
            ]
            
            for selector in selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        text = element.get_attribute('aria-label') or element.text
                        if text:
                            # Try to parse different time formats
                            # Pattern: "Open today: 9 AM - 5 PM" or similar
                            matches = re.findall(r'(\w+):\s*((?:\d{1,2}(?::\d{2})?\s*(?:AM|PM|noon|midnight))\s*[-–]\s*(?:\d{1,2}(?::\d{2})?\s*(?:AM|PM|noon|midnight)))', text)
                            for day, time in matches:
                                hours_dict[day.strip()] = self._clean_hours_text(time)
                except:
                    continue

            # Method 4: Try clicking hours button and getting expanded info
            try:
                hours_button = self.driver.find_element(By.CSS_SELECTOR, "[aria-label*='Hours']")
                hours_button.click()
                time.sleep(1)
                
                expanded_hours = self.driver.find_element(By.CSS_SELECTOR, "div.t39EBf[role='dialog']")
                days = expanded_hours.find_elements(By.CSS_SELECTOR, "div.ylH6lf")
                times = expanded_hours.find_elements(By.CSS_SELECTOR, "div.mxowUb")
                
                for day, time in zip(days, times):
                    day_text = day.text
                    time_text = time.text
                    if day_text and time_text:
                        hours_dict[day_text.strip()] = self._clean_hours_text(time_text)
            except:
                pass

            # If we found any hours, return them
            if hours_dict:
                return hours_dict

            # Method 5: Last resort - try to get current status
            try:
                status_element = self.driver.find_element(By.CSS_SELECTOR, "div.MkV9 span.ZDu9vd")
                status_text = status_element.text
                if status_text:
                    return {"current_status": self._clean_hours_text(status_text)}
            except:
                pass

            return None

        except Exception as e:
            logger.debug(f"Error extracting hours: {str(e)}")
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