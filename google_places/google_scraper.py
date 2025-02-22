import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import random
import json
from typing import Dict, Optional
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GooglePlacesScraper:
    def __init__(self):
        """Initialize the scraper with Selenium options optimized for Google Places"""
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        # Add randomized user agent
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        ]
        options.add_argument(f'user-agent={random.choice(user_agents)}')
        
        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, 10)

    def _random_delay(self):
        """Add random delay between requests to avoid detection"""
        time.sleep(random.uniform(2, 5))

    def extract_place_details(self, google_id: str) -> Optional[Dict]:
        """
        Extract details for a single place using its Google Place ID
        """
        try:
            url = f"https://www.google.com/maps/place/?q=place_id:{google_id}"
            self.driver.get(url)
            self._random_delay()

            # Wait for the main content to load
            self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "fontHeadlineSmall")))

            details = {}

            # Extract name
            try:
                details['name'] = self.driver.find_element(By.CLASS_NAME, "fontHeadlineSmall").text
            except NoSuchElementException:
                details['name'] = None

            # Extract category
            try:
                details['category'] = self.driver.find_element(By.CLASS_NAME, "DkEaL").text
            except NoSuchElementException:
                details['category'] = None

            # Extract price level
            try:
                price_element = self.driver.find_element(By.CSS_SELECTOR, "[aria-label*='Price:']")
                details['price'] = price_element.get_attribute('aria-label').count('$')
            except NoSuchElementException:
                details['price'] = None

            # Extract hours
            try:
                hours_button = self.driver.find_element(By.CSS_SELECTOR, "[aria-label*='Hours']")
                hours_button.click()
                self._random_delay()
                
                hours_elements = self.driver.find_elements(By.CSS_SELECTOR, ".lo7U087hsMA__row-hours")
                hours = {}
                for element in hours_elements:
                    day_time = element.text.split('\n')
                    if len(day_time) == 2:
                        hours[day_time[0]] = day_time[1]
                details['hours'] = hours
            except NoSuchElementException:
                details['hours'] = None

            # Extract reviews
            try:
                reviews_button = self.driver.find_element(By.CSS_SELECTOR, "[aria-label*='Reviews']")
                reviews_button.click()
                self._random_delay()
                
                review_elements = self.driver.find_elements(By.CSS_SELECTOR, ".wiI7pd")
                details['reviews'] = [review.text for review in review_elements[:5]]  # Get first 5 reviews
            except NoSuchElementException:
                details['reviews'] = None

            return details

        except Exception as e:
            logger.error(f"Error scraping place {google_id}: {str(e)}")
            return None

    def scrape_places_incrementally(self, input_csv: str, output_csv: str):
        """
        Scrape details for all places in the input CSV and save incrementally to output CSV
        """
        # Read input CSV
        df = pd.read_csv(input_csv)
        
        # Check if output file exists and load previously scraped data
        scraped_ids = set()
        if os.path.exists(output_csv):
            existing_df = pd.read_csv(output_csv)
            scraped_ids = set(existing_df['google_id'])
            logger.info(f"Found {len(scraped_ids)} previously scraped places")

        # Create/open output file in append mode
        output_file_exists = os.path.exists(output_csv)
        
        for idx, row in df.iterrows():
            # Skip if already scraped
            if row['google_id'] in scraped_ids:
                logger.info(f"Skipping {row['name']} - already scraped")
                continue
                
            logger.info(f"Scraping {idx + 1}/{len(df)}: {row['name']}")
            
            details = self.extract_place_details(row['google_id'])
            if details:
                # Combine original data with scraped details
                result = {
                    'corner_place_id': row['corner_place_id'],
                    'google_id': row['google_id'],
                    'original_name': row['name'],
                    'neighborhood': row['neighborhood'],
                    'website': row['website'],
                    'instagram_handle': row['instagram_handle'],
                    **details
                }
                
                # Convert to DataFrame and save single row
                result_df = pd.DataFrame([result])
                result_df.to_csv(output_csv, 
                               mode='a', 
                               header=not output_file_exists,
                               index=False)
                
                # Set flag so we don't write headers again
                output_file_exists = True
                scraped_ids.add(row['google_id'])
            
            # Add longer delay every 10 requests
            if (idx + 1) % 10 == 0:
                time.sleep(random.uniform(20, 30))

        logger.info(f"Scraping completed. Results saved to {output_csv}")

    def close(self):
        """Close the Selenium driver"""
        self.driver.quit()

def main():
    scraper = GooglePlacesScraper()
    try:
        scraper.scrape_places_incrementally('places.csv', 'places_with_google_data.csv')
    finally:
        scraper.close()

if __name__ == "__main__":
    main()