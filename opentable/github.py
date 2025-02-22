import pandas as pd
from requests_html import HTMLSession
from bs4 import BeautifulSoup
import json
import logging
import time
import random
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OpenTableScraper:
    def __init__(self):
        self.session = HTMLSession()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })
        self.successful_matches = 0
        self.total_processed = 0
        
    def _random_delay(self):
        time.sleep(random.uniform(1, 3))

    def _format_url_name(self, name: str) -> str:
        """Format restaurant name for URL"""
        return name.lower().replace('\'', '').replace(' & ', '-').replace(' ', '-')

    def _get_url_variations(self, name: str) -> list:
        """Generate possible URL variations"""
        formatted_name = self._format_url_name(name)
        # dummy_params = "?avt=eyJ2IjoyLCJtIjoxLCJwIjowLCJzIjowLCJuIjowfQ"
        dummy_params = ''
        
        return [
            f"https://www.opentable.com/{formatted_name}{dummy_params}",
            f"https://www.opentable.com/r/{formatted_name}-new-york{dummy_params}"
        ]

    def _extract_restaurant_data(self, soup: BeautifulSoup) -> dict:
        """Extract restaurant data from page"""
        data = {'found': False}
        
        try:
            for script in soup.find_all('script', {'type': 'application/ld+json'}):
                if script.string and '"@type":"Restaurant"' in script.string:
                    json_data = json.loads(script.string)
                    data.update({
                        'found': True,
                        'name': json_data.get('name'),
                        'description': json_data.get('description'),
                        'price_range': json_data.get('priceRange'),
                        'cuisine': json_data.get('servesCuisine'),
                        'address': json_data.get('address', {}).get('streetAddress'),
                        'reviews': [
                            review.get('reviewBody')
                            for review in json_data.get('review', [])[:10]
                            if review.get('reviewBody')
                        ]
                    })
                    break
        except Exception as e:
            logger.error(f"Error extracting data: {e}")
            
        return data

    def scrape_restaurant(self, name: str) -> dict:
        """Try to scrape restaurant data using multiple URL patterns"""
        urls = self._get_url_variations(name)
        
        for url in urls:
            try:
                response = self.session.get(url)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    data = self._extract_restaurant_data(soup)
                    
                    if data['found']:
                        data['url'] = url
                        return data
                        
                self._random_delay()
                
            except Exception as e:
                logger.error(f"Error with URL {url}: {e}")
                continue
                
        return {'found': False, 'name': name}

    def process_csv(self, input_path: str, output_path: str):
        """Process all restaurants from CSV with detailed progress tracking"""
        df = pd.read_csv(input_path)
        results = []
        start_time = datetime.now()
        
        print("\nStarting OpenTable data collection...")
        print(f"Total restaurants to process: {len(df)}")
        print("-" * 50)
        
        for idx, row in df.iterrows():
            self.total_processed += 1
            restaurant_name = row['name']
            
            print(f"\nProcessing ({idx + 1}/{len(df)}): {restaurant_name}")
            result = self.scrape_restaurant(restaurant_name)
            
            if result['found']:
                self.successful_matches += 1
                print(f"✓ Found on OpenTable!")
                print(f"  Price: {result.get('price_range', 'N/A')}")
                print(f"  Cuisine: {result.get('cuisine', 'N/A')}")
            else:
                print("✗ Not found on OpenTable")
            
            # Update success rate
            success_rate = (self.successful_matches / self.total_processed) * 100
            print(f"Current success rate: {success_rate:.1f}%")
            
            result.update({
                'corner_place_id': row['corner_place_id'],
                'google_id': row['google_id'],
                'original_name': restaurant_name,
                'neighborhood': row['neighborhood']
            })
            results.append(result)
            
            # Save progress every 10 restaurants
            if (idx + 1) % 10 == 0:
                self._save_progress(results, output_path, start_time)
            
        # Final save
        self._save_progress(results, output_path, start_time, final=True)

    def _save_progress(self, results, output_path, start_time, final=False):
        """Save progress and print statistics"""
        pd.DataFrame(results).to_csv(output_path, index=False)
        
        elapsed_time = datetime.now() - start_time
        avg_time_per_restaurant = elapsed_time.total_seconds() / self.total_processed
        
        print("\n" + "=" * 50)
        print("Progress Update:")
        print(f"Restaurants processed: {self.total_processed}")
        print(f"Successful matches: {self.successful_matches}")
        print(f"Success rate: {(self.successful_matches / self.total_processed) * 100:.1f}%")
        print(f"Average time per restaurant: {avg_time_per_restaurant:.1f} seconds")
        print(f"Elapsed time: {elapsed_time}")
        if not final:
            remaining = len(results) - self.total_processed
            estimated_remaining_time = remaining * avg_time_per_restaurant
            print(f"Estimated time remaining: {estimated_remaining_time:.0f} seconds")
        print("=" * 50)

if __name__ == "__main__":
    scraper = OpenTableScraper()
    scraper.process_csv('places.csv', 'opentable_results.csv')