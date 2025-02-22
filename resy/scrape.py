import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import json
import logging
import time
import random
from datetime import datetime
from pathlib import Path
import re

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
        self.data_dir = Path('resy_data')
        self.data_dir.mkdir(exist_ok=True)

    def _random_delay(self):
        """Add substantial random delay between requests"""
        delay = random.uniform(15, 30)
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

    def _extract_venue_data(self, driver, timeout=45) -> dict:
        """Extract venue data from Resy page"""
        data = {'found': False}
        
        try:
            print("Waiting for page to load...")
            wait = WebDriverWait(driver, timeout)
            
            # Wait for content to load
            time.sleep(10)  # Initial wait for JavaScript
            print("Checking for venue content...")
            
            # Check if we're on a valid venue page
            if "The page you're looking for cannot be found" in driver.page_source:
                print("Venue page not found...")
                return data
            
            # Try to find venue content
            try:
                venue_content = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'VenuePage__content'))
                )
                print("Found venue content!")
            except:
                print("No venue content found...")
                return data
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Extract basic information
            name_element = soup.find('h1', {'class': 'VenuePage__title'})
            description_element = soup.find('div', {'class': 'VenuePage__description'})
            
            data.update({
                'found': True,
                'name': name_element.text.strip() if name_element else '',
                'description': description_element.text.strip() if description_element else '',
                'details': {}
            })
            
            # Extract additional details
            time.sleep(5)  # Wait for details to load
            details_section = soup.find('div', {'class': 'VenueDetails'})
            if details_section:
                detail_items = details_section.find_all('div', {'class': 'DetailItem'})
                for item in detail_items:
                    label = item.find('div', {'class': 'DetailItem__label'})
                    value = item.find('div', {'class': 'DetailItem__value'})
                    if label and value:
                        data['details'][label.text.strip()] = value.text.strip()
            
            # Get location info
            location_element = soup.find('address', {'class': 'VenuePage__address'})
            if location_element:
                data['location'] = location_element.text.strip()
            
            print("Venue data extraction complete!")
            
        except Exception as e:
            logger.error(f"Error extracting data: {e}")
            
        return data

    def _save_venue_data(self, venue_data: dict, venue_id: str):
        """Save venue data to JSON file"""
        if venue_data['found']:
            file_path = self.data_dir / f"{venue_id}.json"
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(venue_data, f, indent=2, ensure_ascii=False)

    def scrape_venue(self, name: str, venue_id: str) -> dict:
        """Attempt to scrape venue data from Resy"""
        url = f"https://resy.com/cities/new-york-ny/venues/{self._format_venue_name(name)}?date=2025-02-22&seats=2"
        print(f"\nAttempting to scrape: {url}")
        
        try:
            print("Loading page...")
            self.driver.get(url)
            data = self._extract_venue_data(self.driver)
            
            if data['found']:
                data['url'] = url
                self._save_venue_data(data, venue_id)
            
            self._random_delay()
            return data
                
        except Exception as e:
            logger.error(f"Error with URL {url}: {e}")
            return {'found': False, 'name': name}

    def process_csv(self, input_path: str, output_path: str):
        """Process all venues from CSV with detailed progress tracking"""
        df = pd.read_csv(input_path)
        results = []
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
                
                if result['found']:
                    self.successful_matches += 1
                    print(f"✓ Found on Resy!")
                    print(f"  Details: {len(result.get('details', {}))} attributes found")
                    print(f"  Description length: {len(result.get('description', ''))}")
                else:
                    print("✗ Not found on Resy")
                
                # Update success rate
                success_rate = (self.successful_matches / self.total_processed) * 100
                print(f"Current success rate: {success_rate:.1f}%")
                
                result.update({
                    'corner_place_id': venue_id,
                    'google_id': row['google_id'],
                    'original_name': venue_name,
                    'neighborhood': row['neighborhood']
                })
                results.append(result)
                
                # Save progress every 10 venues
                if (idx + 1) % 10 == 0:
                    self._save_progress(results, output_path, start_time)
                
        finally:
            # Make sure to close the browser
            self.driver.quit()
            
        # Final save
        self._save_progress(results, output_path, start_time, final=True)

    def _save_progress(self, results, output_path, start_time, final=False):
        """Save progress and print statistics"""
        pd.DataFrame(results).to_csv(output_path, index=False)
        
        elapsed_time = datetime.now() - start_time
        avg_time_per_venue = elapsed_time.total_seconds() / self.total_processed
        
        print("\n" + "=" * 50)
        print("Progress Update:")
        print(f"Venues processed: {self.total_processed}")
        print(f"Successful matches: {self.successful_matches}")
        print(f"Success rate: {(self.successful_matches / self.total_processed) * 100:.1f}%")
        print(f"Average time per venue: {avg_time_per_venue:.1f} seconds")
        print(f"Elapsed time: {elapsed_time}")
        if not final:
            remaining = len(results) - self.total_processed
            estimated_remaining_time = remaining * avg_time_per_venue
            print(f"Estimated time remaining: {estimated_remaining_time:.0f} seconds")
        print("=" * 50)

if __name__ == "__main__":
    scraper = ResyScraper()
    scraper.process_csv('places.csv', 'resy_results.csv')