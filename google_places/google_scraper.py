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

            # Description - New addition
            try:
                description_selectors = [
                    "div.PYvSYb",  # Main description class
                    "div[jslog*='metadata'] div.fontBodyMedium",  # Alternative location
                    "div.WeS02d.fontBodyMedium",  # Another variation
                ]
                
                for selector in description_selectors:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        text = element.text
                        if text and not text.startswith('·') and len(text) > 10:  # Avoid selecting bullet points
                            details['description'] = text.strip()
                            break
                    if details.get('description'):
                        break
            except Exception as e:
                logger.debug(f"Error extracting description: {str(e)}")
                details['description'] = None

            # Hours - Enhanced with temporary closure handling
            hours_data = self._extract_hours()
            if hours_data:
                if isinstance(hours_data, dict) and hours_data.get('current_status') == 'Temporarily closed':
                    details['hours'] = 'Temporarily closed'
                else:
                    details['hours'] = hours_data
            else:
                details['hours'] = None

            # Category
            try:
                category_element = self.driver.find_element(By.CLASS_NAME, "DkEaL")
                details['category'] = category_element.text
            except NoSuchElementException:
                try:
                    category_element = self.driver.find_element(By.CSS_SELECTOR, "button[jsaction*='pane.rating.category']")
                    details['category'] = category_element.text
                except NoSuchElementException:
                    details['category'] = None

            # Price - Enhanced extraction
            details['price'] = self._extract_price()

            # Reviews - Using existing robust implementation
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

    def _extract_price(self) -> str:
        """Enhanced price extraction with better handling of missing prices"""
        try:
            # Strategy 1: Header area near name/category
            header_selectors = [
                "span.ZDu9vd",
                "div.LBgpqf",
                "span.mgr77e",
                "div.iTxXHe",
                "div[aria-label*='Price range']",
                "span[aria-label*='Price']"
            ]
            
            for selector in header_selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text or element.get_attribute('aria-label') or ''
                    if '$' in text:
                        price_match = re.search(r'(\$+(?!\d)|\$\d+(?:[-–]\$?\d+)?)', text)
                        if price_match:
                            return price_match.group(0)

            # Strategy 2: About section
            try:
                about_button = self.driver.find_element(By.CSS_SELECTOR, "button[aria-label*='About']")
                about_button.click()
                time.sleep(1)
                
                about_content = self.driver.find_element(By.CSS_SELECTOR, "div.m6QErb")
                about_text = about_content.text
                
                if '$' in about_text:
                    price_match = re.search(r'(\$+(?!\d)|\$\d+(?:[-–]\$?\d+)?)', about_text)
                    if price_match:
                        return price_match.group(0)
            except:
                pass

            # If we couldn't find a price but the place exists, return empty string
            # This distinguishes between "no price available" and "failed to extract"
            if self.driver.find_elements(By.CSS_SELECTOR, "div.DkEaL"):
                return ""
            
            return None

        except Exception as e:
            logger.debug(f"Error extracting price: {str(e)}")
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

    def _parse_hours_from_text(self, text: str) -> dict:
        """Parse hours from various text formats"""
        hours_dict = {}
        
        # Check for temporary closure
        if 'temporarily closed' in text.lower():
            return {'current_status': 'Temporarily closed'}
            
        # Split by semicolon and process each day's hours
        for day_hours in text.split(';'):
            if ',' in day_hours and 'Hide' not in day_hours:
                try:
                    # Split into day and hours, handling potential extra commas
                    parts = day_hours.split(',', 1)
                    day = parts[0].strip()
                    hours = parts[1].strip()
                    
                    # Clean the hours text
                    hours = self._clean_hours_text(hours)
                    
                    # Handle special cases like "Closed"
                    if 'Closed' in hours:
                        hours = 'Closed'
                        
                    hours_dict[day] = hours
                except:
                    continue
        
        return hours_dict if hours_dict else None

    def _extract_hours(self) -> dict:
        """Extract hours with improved handling of temporary closures and current status"""
        try:
            # Check for temporary closure first
            closure_selectors = [
                "div[aria-label*='Temporarily closed']",
                "div.o0Svhf",
                "span.ZDu9vd"
            ]
            
            for selector in closure_selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text or element.get_attribute('aria-label') or ''
                    if 'temporarily closed' in text.lower():
                        return {'current_status': 'Temporarily closed'}

            # Try regular hours extraction methods
            hours_selectors = [
                "div.t39EBf",
                "div[aria-label*='Hours']",
                "div[jsaction*='openhours']",
                "div[data-hide-tooltip-on-mouse-move='true']"
            ]
            
            for selector in hours_selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    aria_text = element.get_attribute('aria-label')
                    if aria_text and ('AM' in aria_text or 'PM' in aria_text or 'Closed' in aria_text):
                        hours_dict = self._parse_hours_from_text(aria_text)
                        if hours_dict:
                            return hours_dict
                            
                    element_text = element.text
                    if element_text and ('AM' in element_text or 'PM' in element_text or 'Closed' in element_text):
                        hours_dict = self._parse_hours_from_text(element_text)
                        if hours_dict:
                            return hours_dict

            # Try getting current status as fallback
            try:
                status = self.driver.find_element(By.CSS_SELECTOR, "span.ZDu9vd").text
                if status:
                    return {"current_status": self._clean_hours_text(status)}
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
            
            # Random delay between requests to avoid rate limiting
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