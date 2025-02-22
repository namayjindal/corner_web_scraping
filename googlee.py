import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
import json
from datetime import datetime
import re

class GooglePlacesScraper:
    def __init__(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, 15)

    def scrape_place(self, place_name, google_id):
        try:
            search_url = f"https://www.google.com/maps/search/{place_name.replace(' ', '+')}/"
            self.driver.get(search_url)
            time.sleep(5)
            
            # Get the main text content first
            main_content = self.driver.find_element(By.CSS_SELECTOR, "div.fontBodyMedium").text
            
            # Parse all the information
            data = {
                'name': place_name,
                'google_id': google_id,
                'timestamp': datetime.now().isoformat(),
                'hours': self._parse_hours(main_content),
                'price_level': self._parse_price(main_content),
                'rating': self._parse_rating(main_content),
                'category': self._parse_category(main_content),
                'reviews_count': self._parse_reviews_count(main_content),
                'description': self._parse_description(main_content)
            }
            
            return data
            
        except Exception as e:
            print(f"Error scraping {place_name}: {str(e)}")
            return None

    def _parse_hours(self, content):
        try:
            # Look for patterns like "Closed • Opens 12PM" or similar
            hours_match = re.search(r'(Closed|Open)[\s•]+([^•\n]+)', content)
            if hours_match:
                return f"{hours_match.group(1)} • {hours_match.group(2)}"
            return None
        except:
            return None

    def _parse_price(self, content):
        try:
            # Look for price pattern like "$10-20"
            price_match = re.search(r'\$\d+[-–]\d+', content)
            if price_match:
                return price_match.group(0)
            return None
        except:
            return None

    def _parse_rating(self, content):
        try:
            # Look for rating pattern like "4.7"
            rating_match = re.search(r'(\d+\.\d+)\s*\(', content)
            if rating_match:
                return float(rating_match.group(1))
            return None
        except:
            return None

    def _parse_reviews_count(self, content):
        try:
            # Look for reviews count pattern like "(3,971)"
            reviews_match = re.search(r'\((\d+,?\d*)\)', content)
            if reviews_match:
                return reviews_match.group(1).replace(',', '')
            return None
        except:
            return None

    def _parse_category(self, content):
        try:
            # Look for the category text before the bullet point
            category_match = re.search(r'\n([^•\n]+)(?:\s*•|$)', content)
            if category_match:
                return category_match.group(1).strip()
            return None
        except:
            return None

    def _parse_description(self, content):
        try:
            # Look for descriptive text after the address
            lines = content.split('\n')
            for i, line in enumerate(lines):
                if 'St' in line or 'Ave' in line:  # After address
                    if i + 1 < len(lines):
                        return lines[i + 1].strip()
            return None
        except:
            return None

    def close(self):
        self.driver.quit()

def test_scraper():
    df = pd.read_csv('places.csv')
    first_place = df.iloc[0]
    scraper = GooglePlacesScraper()
    
    try:
        result = scraper.scrape_place(
            place_name=first_place['name'],
            google_id=first_place['google_id']
        )
        
        with open('test_scrape_result.json', 'w') as f:
            json.dump(result, f, indent=4)
            
        print("Scraping completed successfully!")
        print("Results saved to test_scrape_result.json")
        
    finally:
        scraper.close()

if __name__ == "__main__":
    test_scraper()