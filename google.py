import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import json
from datetime import datetime
import re
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GooglePlacesScraper:
    def __init__(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')  # Set a proper window size
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, 15)

    def wait_and_find_element(self, by, value, timeout=15):
        """Wait for and find a single element"""
        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return element
        except TimeoutException:
            logger.warning(f"Timeout waiting for element: {value}")
            return None

    def wait_and_find_elements(self, by, value, timeout=15):
        """Wait for and find multiple elements"""
        try:
            elements = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_all_elements_located((by, value))
            )
            return elements
        except TimeoutException:
            logger.warning(f"Timeout waiting for elements: {value}")
            return []

    def extract_rating(self, text):
        """Extract rating from text"""
        match = re.search(r'(\d+\.\d+)', text)
        return float(match.group(1)) if match else None

    def extract_reviews_count(self, text):
        """Extract review count from text"""
        match = re.search(r'\((\d+[,\d]*)\)', text)
        if match:
            return int(match.group(1).replace(',', ''))
        return None

    def extract_price_level(self, text):
        """Extract price level from text"""
        match = re.search(r'(\$+)', text)
        return match.group(1) if match else None

    def scrape_place(self, place_name, google_id):
        try:
            logger.info(f"Starting to scrape {place_name}")
            search_url = f"https://www.google.com/maps/search/{place_name.replace(' ', '+')}/"
            self.driver.get(search_url)
            
            # Wait longer for initial load and let JavaScript execute
            time.sleep(3)
            
            # Initialize data dictionary
            data = {
                'name': place_name,
                'google_id': google_id,
                'timestamp': datetime.now().isoformat(),
                'url': search_url
            }
            
            # Wait for any content to load (more reliable than waiting for specific elements)
            try:
                WebDriverWait(self.driver, 10).until(
                    lambda driver: driver.execute_script("return document.readyState") == "complete"
                )
            except TimeoutException:
                logger.warning("Page load timeout")
            
            # Get all text content first to verify we're on the right page
            page_text = self.driver.page_source
            if place_name.lower() not in page_text.lower():
                logger.error(f"Business name '{place_name}' not found in page content")
                return None
            
            # Try multiple selectors for rating
            rating_selectors = [
                '[aria-label*="stars"]',
                '[aria-label*="rating"]',
                'span[aria-hidden="true"]'
            ]
            
            for selector in rating_selectors:
                rating_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in rating_elements:
                    text = element.get_attribute('aria-label') or element.text
                    if text and ('stars' in text.lower() or 'rating' in text.lower()):
                        data['rating'] = self.extract_rating(text)
                        data['reviews_count'] = self.extract_reviews_count(text)
                        break
                if 'rating' in data:
                    break
            
            # Try to get price level - look for dollar signs
            price_selectors = [
                'span:not([class])',  # Unclassed spans often contain price info
                '[aria-label*="Price"]',
                'button[aria-label*="Price"]'
            ]
            
            for selector in price_selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text or element.get_attribute('aria-label')
                    if text and '$' in text:
                        data['price_level'] = self.extract_price_level(text)
                        break
                if 'price_level' in data:
                    break
            
            # Try to get category - look for common category indicators
            category_selectors = [
                'button[aria-label*="Category"]',
                'span[aria-label*="Category"]',
                'a[href*="search"]'  # Categories are often in search links
            ]
            
            for selector in category_selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text or element.get_attribute('aria-label')
                    if text and any(keyword in text.lower() for keyword in ['restaurant', 'pizza', 'food']):
                        data['category'] = text.strip()
                        break
                if 'category' in data:
                    break
            
            # Try to get hours - look for time-related text
            hours_selectors = [
                '[aria-label*="Hours"]',
                'button[aria-label*="Opens"]',
                'button[aria-label*="Closed"]'
            ]
            
            for selector in hours_selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text or element.get_attribute('aria-label')
                    if text and any(keyword in text for keyword in ['Opens', 'Closed', 'hours']):
                        data['hours'] = text.strip()
                        break
                if 'hours' in data:
                    break
            
            # Try to get reviews
            try:
                # Look for reviews section using multiple selectors
                review_button_selectors = [
                    'button[aria-label*="Reviews"]',
                    'button[data-tab-index*="reviews"]',
                    'a[href*="reviews"]'
                ]
                
                reviews_button = None
                for selector in review_button_selectors:
                    buttons = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for button in buttons:
                        if 'review' in (button.text or '').lower():
                            reviews_button = button
                            break
                    if reviews_button:
                        break
                
                if reviews_button:
                    reviews_button.click()
                    time.sleep(2)
                    
                    # Try multiple selectors for review content
                    review_selectors = [
                        '[data-review-id]',
                        '[class*="review"]',
                        'div[jsan*="review"]'
                    ]
                    
                    reviews = []
                    for selector in review_selectors:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        for element in elements[:5]:
                            review_text = element.text
                            if review_text and len(review_text) > 10:  # Ensure it's actual review content
                                reviews.append(review_text)
                        if reviews:
                            break
                    
                    data['reviews'] = reviews[:5]  # Limit to 5 reviews
            except Exception as e:
                logger.error(f"Error getting reviews: {str(e)}")
                data['reviews'] = []
            
            # Log what we found
            found_fields = [k for k, v in data.items() if v is not None and k not in ['name', 'google_id', 'timestamp', 'url']]
            logger.info(f"Successfully scraped fields: {', '.join(found_fields)}")
            
            return data
            
        except Exception as e:
            logger.error(f"Error scraping {place_name}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def close(self):
        self.driver.quit()

def test_scraper():
    try:
        df = pd.read_csv('places.csv')
        if df.empty:
            logger.error("places.csv is empty")
            return
            
        first_place = df.iloc[0]
        scraper = GooglePlacesScraper()
        
        try:
            result = scraper.scrape_place(
                place_name=first_place['name'],
                google_id=first_place['google_id']
            )
            
            if result:
                with open('test_scrape_result.json', 'w') as f:
                    json.dump(result, f, indent=4)
                logger.info("Scraping completed successfully!")
                logger.info("Results saved to test_scrape_result.json")
            else:
                logger.error("Scraping failed - no data returned")
                
        finally:
            scraper.close()
    except FileNotFoundError:
        logger.error("places.csv not found")
    except Exception as e:
        logger.error(f"Error in test_scraper: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    test_scraper()