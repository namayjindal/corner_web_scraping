import undetected_chromedriver as uc
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import csv
import logging
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EnhancedTripAdvisorScraper:
    def __init__(self):
        """Initialize the scraper with undetected-chromedriver"""
        options = uc.ChromeOptions()
        options.add_argument("--headless=new")  # Updated for stability
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        
        self.driver = uc.Chrome(
            options=options,
            use_subprocess=True
        )
        self.wait = WebDriverWait(self.driver, 20)  # Increased wait time

    def _save_debug_info(self, page_source, name):
        """Save page source for debugging"""
        with open(f"debug_{name.replace(' ', '_')}.html", 'w', encoding='utf-8') as f:
            f.write(page_source)

    def scrape_place(self, name: str, neighborhood: str) -> dict:
        """Scrape details for a single place"""
        try:
            # Construct TripAdvisor search URL directly
            search_url = f"https://www.tripadvisor.com/Search?q={name}+{neighborhood}+New+York"
            logger.info(f"Searching on TripAdvisor: {search_url}")
            self.driver.get(search_url)
            time.sleep(random.uniform(3, 5))

            # Click the first result
            try:
                first_result = self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.result-title a"))
                )
                href = first_result.get_attribute('href')
                logger.info(f"Found TripAdvisor listing: {href}")
                self.driver.get(href)
            except TimeoutException:
                logger.warning(f"No search results found for {name}")
                return None
            
            # Wait for page to load
            time.sleep(random.uniform(3, 5))
            
            # Save page source for debugging
            self._save_debug_info(self.driver.page_source, name)
            
            # Extract information
            details = {}

            # Name
            try:
                name_element = self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "h1[data-test-target='top-info-header']"))
                )
                details['name'] = name_element.text.strip()
            except TimeoutException:
                logger.warning(f"Could not find name element for {name}")
                details['name'] = None

            # Rating
            try:
                rating_element = self.driver.find_element(By.CSS_SELECTOR, "span[data-test-target='review-rating']")
                details['rating'] = rating_element.text.strip()
            except NoSuchElementException:
                details['rating'] = None

            # Price Level
            try:
                price_element = self.driver.find_element(By.CSS_SELECTOR, "span[class*='PriceRange']")
                details['price_level'] = price_element.text.strip()
            except NoSuchElementException:
                details['price_level'] = None

            # Address
            try:
                address_element = self.driver.find_element(By.CSS_SELECTOR, "span[data-test-target='address']")
                details['address'] = address_element.text.strip()
            except NoSuchElementException:
                details['address'] = None

            # Reviews
            reviews = []
            try:
                review_elements = self.driver.find_elements(By.CSS_SELECTOR, "q[class*='review-text']")
                for review in review_elements[:5]:  # Get first 5 reviews
                    reviews.append(review.text.strip())
                details['reviews'] = reviews
            except NoSuchElementException:
                details['reviews'] = []

            return details

        except Exception as e:
            logger.error(f"Error extracting details: {str(e)}")
            self._save_debug_info(self.driver.page_source, f"{name}_error")
            return None

    def scrape_places_to_csv(self, input_csv: str, output_csv: str):
        """Scrape details for all places and save incrementally"""
        # Read input CSV
        df = pd.read_csv(input_csv)

        # Create/open output CSV with headers
        with open(output_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            headers = ['corner_place_id', 'name', 'neighborhood', 'tripadvisor_name',
                      'rating', 'price_level', 'address', 'reviews']
            writer.writerow(headers)

        # Process each place
        for idx, row in df.iterrows():
            logger.info(f"Processing {idx + 1}/{len(df)}: {row['name']}")

            try:
                details = self.scrape_place(row['name'], row['neighborhood'])

                if details:
                    # Write to CSV
                    with open(output_csv, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow([
                            row['corner_place_id'],
                            row['name'],
                            row['neighborhood'],
                            details.get('name', ''),
                            details.get('rating', ''),
                            details.get('price_level', ''),
                            details.get('address', ''),
                            '|'.join(details.get('reviews', []))
                        ])

                    logger.info(f"Successfully saved data for {row['name']}")

                # Add random delay between requests
                time.sleep(random.uniform(5, 10))

            except Exception as e:
                logger.error(f"Error processing {row['name']}: {str(e)}")
                continue

    def close(self):
        """Close the browser"""
        self.driver.quit()

def main():
    try:
        scraper = EnhancedTripAdvisorScraper()
        scraper.scrape_places_to_csv('places.csv', 'places_with_tripadvisor_data.csv')
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
    finally:
        if 'scraper' in locals():
            scraper.close()

if __name__ == "__main__":
    main()
