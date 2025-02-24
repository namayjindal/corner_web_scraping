import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
import logging
import time
import random
from datetime import datetime
import re
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ResyScraper:
    def __init__(self):
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        self.driver = webdriver.Chrome(options=chrome_options)
        self.successful_matches = 0
        self.total_processed = 0
        self.output_json = 'resy_data.json'
        self.scraped_data = []

    def _random_delay(self):
        """Add substantial random delay between requests"""
        delay = random.uniform(3, 5)
        print(f"\nWaiting {delay:.1f} seconds before next request...")
        
        # Break the delay into chunks to show progress
        chunk_size = delay / 5
        for i in range(5):
            time.sleep(chunk_size)
            if i < 4:
                print(".", end="", flush=True)
        print()

    def _format_venue_name(self, name: str) -> str:
        """Format restaurant name for Resy URL"""
        formatted = name.lower()
        formatted = re.sub(r'[\'"""&,]', '', formatted)
        formatted = re.sub(r'[^a-z0-9\s-]', '', formatted)
        formatted = re.sub(r'\s+', '-', formatted)
        formatted = re.sub(r'-+', '-', formatted)
        return formatted.strip('-')

    def _extract_venue_data(self, driver, timeout=7) -> dict:
        """Extract venue data from Resy page"""
        data = {'found': False}
        
        try:
            wait = WebDriverWait(driver, timeout)
            print("Waiting for page to load...")
            
            # Wait for specific content sections
            try:
                # Wait for why we like it section
                why_we_like_it = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "VenuePage__why-we-like-it__body"))
                )
                data['why_we_like_it'] = why_we_like_it.text.strip()
                print("Found 'Why We Like It' section")
            except:
                print("No 'Why We Like It' section found")
                data['why_we_like_it'] = None

            try:
                # Wait for need to know section
                need_to_know = driver.find_element(By.ID, "clamped-content-need-to-know")
                data['need_to_know'] = need_to_know.text.strip()
                print("Found 'Need to Know' section")
            except:
                print("No 'Need to Know' section found")
                data['need_to_know'] = None

            try:
                # Wait for about section
                about = driver.find_element(By.ID, "clamped-content-about-venue") 
                data['about'] = about.text.strip()
                print("Found 'About' section")
            except:
                print("No 'About' section found")
                data['about'] = None

            # If we found any content, mark as found
            if any([data['why_we_like_it'], data['need_to_know'], data['about']]):
                data['found'] = True
                print("Successfully found venue data!")

        except Exception as e:
            logger.error(f"Error extracting data: {e}")
            
        return data

    def _save_incremental_result(self, result: dict):
        """Save a single result to JSON incrementally"""
        self.scraped_data.append(result)
        
        # Write entire list to JSON file
        with open(self.output_json, 'w', encoding='utf-8') as f:
            json.dump(self.scraped_data, f, indent=2, ensure_ascii=False)

    def scrape_venue(self, name: str, venue_id: str) -> dict:
        """Attempt to scrape venue data from Resy"""
        url = f"https://resy.com/cities/new-york-ny/venues/{self._format_venue_name(name)}?date=2025-02-23&seats=2"
        print(f"\nAttempting to scrape: {url}")
        
        try:
            print("Loading page...")
            self.driver.get(url)
            data = self._extract_venue_data(self.driver)
            
            if data['found']:
                data['url'] = url
                data['corner_place_id'] = venue_id
                data['name'] = name
                
                # Save result immediately after finding data
                self._save_incremental_result(data)
                self.successful_matches += 1
            
            self._random_delay()
            return data
                
        except Exception as e:
            logger.error(f"Error with URL {url}: {e}")
            error_result = {
                'found': False, 
                'name': name, 
                'corner_place_id': venue_id,
                'error': str(e)
            }
            self._save_incremental_result(error_result)
            return error_result

    def process_csv(self, input_path: str):
        """Process all venues from CSV with incremental saving"""
        df = pd.read_csv(input_path)
        start_time = datetime.now()
        
        print("\nStarting Resy data collection...")
        print(f"Total venues to process: {len(df)}")
        print("-" * 50)
        
        try:
            for idx, row in df.iterrows():
                self.total_processed += 1
                venue_name = row['name']
                venue_id = str(row['corner_place_id'])
                
                print(f"\nProcessing ({idx + 1}/{len(df)}): {venue_name}")
                result = self.scrape_venue(venue_name, venue_id)
                
                # Update and print success rate
                success_rate = (self.successful_matches / self.total_processed) * 100
                print(f"Current success rate: {success_rate:.1f}%")
                
                # Print elapsed time and estimate remaining time
                elapsed_time = datetime.now() - start_time
                avg_time_per_venue = elapsed_time.total_seconds() / self.total_processed
                remaining_venues = len(df) - (idx + 1)
                estimated_remaining_time = remaining_venues * avg_time_per_venue
                
                print(f"\nElapsed time: {elapsed_time}")
                print(f"Estimated time remaining: {estimated_remaining_time:.0f} seconds")
                
        finally:
            # Make sure to close the browser
            self.driver.quit()

if __name__ == '__main__':
    scraper = ResyScraper()
    # Process the CSV and save results incrementally to JSON
    scraper.process_csv('places.csv')
    logger.info(f'Data saved to {scraper.output_json}')
    print(f"Total venues processed: {scraper.total_processed}")
    print(f"Successful matches: {scraper.successful_matches}")