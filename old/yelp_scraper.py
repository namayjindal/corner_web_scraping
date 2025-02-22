import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
import time
import random
import json
import logging
from typing import Optional, Dict
import os
from dataclasses import dataclass, asdict
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

@dataclass
class YelpPlace:
    name: str
    category: Optional[str] = None
    price_range: Optional[str] = None
    hours: Optional[Dict] = None
    reviews_text: Optional[list] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    url: str = ""

class YelpScraper:
    def __init__(self, places_csv: str, output_csv: str):
        self.places_df = pd.read_csv(places_csv)
        self.output_csv = output_csv
        self.completed_places = set()
        
        # Load already completed places
        if os.path.exists(output_csv):
            completed_df = pd.read_csv(output_csv)
            self.completed_places = set(completed_df['name'].values)
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize driver
        self.driver = self._setup_driver()
        self.wait = WebDriverWait(self.driver, 15)

    def _setup_driver(self) -> uc.Chrome:
        """Setup undetectable-chromedriver with enhanced anti-detection"""
        options = uc.ChromeOptions()
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-first-run')
        options.add_argument('--no-service-autorun')
        options.add_argument('--password-store=basic')
        options.add_argument('--no-sandbox')
        
        # Use random window size to appear more human-like
        window_sizes = [(1920, 1080), (1366, 768), (1440, 900), (1536, 864)]
        window_size = random.choice(window_sizes)
        options.add_argument(f'--window-size={window_size[0]},{window_size[1]}')
        
        driver = uc.Chrome(options=options)
        
        # Add additional JavaScript evasions
        driver.execute_script("""
            // Overwrite the 'webdriver' property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            
            // Add random fingerprint data
            Object.defineProperty(navigator, 'hardwareConcurrency', {
                get: () => Math.floor(Math.random() * (16 - 2) + 2)
            });
            
            // Add random device memory
            Object.defineProperty(navigator, 'deviceMemory', {
                get: () => Math.floor(Math.random() * (8 - 2) + 2)
            });
        """)
        
        return driver

    def _human_like_scroll(self):
        """Scroll the page in a human-like manner"""
        total_height = self.driver.execute_script("return document.body.scrollHeight")
        viewport_height = self.driver.execute_script("return window.innerHeight")
        
        current_position = 0
        while current_position < total_height:
            # Random scroll amount
            scroll_amount = random.randint(100, 400)
            current_position += scroll_amount
            
            # Scroll with random speed
            self.driver.execute_script(f"window.scrollTo({{top: {current_position}, behavior: 'smooth'}})")
            
            # Random pause
            time.sleep(random.uniform(0.5, 1.5))
            
            # Sometimes move mouse randomly
            if random.random() < 0.3:
                ActionChains(self.driver).move_by_offset(
                    random.randint(-100, 100),
                    random.randint(-100, 100)
                ).perform()

    def _human_like_type(self, element, text: str):
        """Type text in a human-like manner"""
        for char in text:
            element.send_keys(char)
            time.sleep(random.uniform(0.1, 0.3))

    def _add_random_behavior(self):
        """Add random human-like behavior"""
        # Random mouse movements
        action = ActionChains(self.driver)
        for _ in range(random.randint(1, 3)):
            action.move_by_offset(
                random.randint(-100, 100),
                random.randint(-100, 100)
            ).perform()
            time.sleep(random.uniform(0.1, 0.3))
        
        # Sometimes highlight text
        if random.random() < 0.2:
            action.key_down(Keys.CONTROL).send_keys('a').key_up(Keys.CONTROL).perform()
            time.sleep(random.uniform(0.1, 0.3))
            action.send_keys(Keys.NULL).perform()

    def search_business(self, name: str, neighborhood: str) -> Optional[str]:
        """Search for a business on Yelp with human-like behavior"""
        try:
            # First visit Yelp homepage
            self.driver.get("https://www.yelp.com")
            time.sleep(random.uniform(2, 4))
            
            # Find and interact with search inputs
            search_business = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='find_desc']"))
            )
            search_location = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='find_loc']"))
            )
            
            # Clear existing text with human-like behavior
            search_business.click()
            time.sleep(random.uniform(0.3, 0.7))
            search_business.clear()
            time.sleep(random.uniform(0.3, 0.7))
            
            # Type search terms
            search_query = f"{name} {neighborhood}"
            self._human_like_type(search_business, search_query)
            
            # Sometimes modify location
            if random.random() < 0.3:
                search_location.click()
                time.sleep(random.uniform(0.3, 0.7))
                search_location.clear()
                time.sleep(random.uniform(0.3, 0.7))
                self._human_like_type(search_location, "New York, NY")
            
            # Add some random behavior
            self._add_random_behavior()
            
            # Submit search
            search_business.send_keys(Keys.RETURN)
            time.sleep(random.uniform(3, 5))
            
            # Wait for and find results
            results = self.wait.until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a[href^='/biz/']"))
            )
            
            # Scroll through results
            self._human_like_scroll()
            
            if results:
                # Get the first result URL
                business_url = results[0].get_attribute('href')
                found_name = results[0].text.strip()
                
                if self._similar_names(name, found_name):
                    return business_url
                    
            self.logger.warning(f"No matching business found for {name}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error searching for {name}: {str(e)}")
            return None

    def _similar_names(self, name1: str, name2: str) -> bool:
        """Enhanced name similarity checking"""
        name1 = name1.lower().replace('&', 'and')
        name2 = name2.lower().replace('&', 'and')
        
        # Remove common business terms and punctuation
        common_terms = ['restaurant', 'cafe', 'bar', 'grill', 'the', 'kitchen', 'eatery']
        for term in common_terms:
            name1 = name1.replace(term, '')
            name2 = name2.replace(term, '')
            
        name1 = ''.join(c for c in name1 if c.isalnum() or c.isspace()).strip()
        name2 = ''.join(c for c in name2 if c.isalnum() or c.isspace()).strip()
        
        name1_words = set(name1.split())
        name2_words = set(name2.split())
        
        # Check for word overlap
        common_words = name1_words & name2_words
        total_words = name1_words | name2_words
        
        if len(common_words) / len(total_words) > 0.5:
            return True
            
        # Check for substring match
        return (name1 in name2 or name2 in name1 or
                name1[:10] == name2[:10])

    def scrape_business_page(self, url: str) -> Optional[YelpPlace]:
        """Scrape business details with human-like behavior"""
        try:
            self.driver.get(url)
            time.sleep(random.uniform(2, 4))
            
            # Initial scroll and random behavior
            self._human_like_scroll()
            self._add_random_behavior()
            
            # Get business details with enhanced waiting
            name = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h1"))
            ).text
            
            # Get other details with appropriate waits and scrolls
            details = {}
            
            try:
                details['category'] = self.driver.find_element(
                    By.CSS_SELECTOR, "[class*='category-str-list']"
                ).text
            except NoSuchElementException:
                details['category'] = None
                
            try:
                details['price_range'] = self.driver.find_element(
                    By.CSS_SELECTOR, "span.price-range"
                ).text
            except NoSuchElementException:
                details['price_range'] = None
            
            # Scroll to hours section
            hours = {}
            try:
                hours_section = self.driver.find_element(
                    By.CSS_SELECTOR, "table.hours-table"
                )
                self.driver.execute_script(
                    "arguments[0].scrollIntoView(true);", hours_section
                )
                time.sleep(random.uniform(1, 2))
                
                for row in hours_section.find_elements(By.TAG_NAME, "tr"):
                    day = row.find_element(By.TAG_NAME, "th").text
                    time_text = row.find_element(By.TAG_NAME, "td").text
                    hours[day] = time_text
            except NoSuchElementException:
                pass
            
            # Scroll to reviews
            reviews_text = []
            try:
                reviews_section = self.driver.find_element(
                    By.CSS_SELECTOR, "[class*='review-list']"
                )
                self.driver.execute_script(
                    "arguments[0].scrollIntoView(true);", reviews_section
                )
                time.sleep(random.uniform(1, 2))
                
                reviews = self.driver.find_elements(
                    By.CSS_SELECTOR, "p.comment"
                )[:5]
                reviews_text = [review.text for review in reviews]
            except NoSuchElementException:
                pass
            
            # Rating and review count
            try:
                rating_elem = self.driver.find_element(
                    By.CSS_SELECTOR, "[aria-label*='star rating']"
                )
                rating = float(rating_elem.get_attribute("aria-label").split()[0])
            except NoSuchElementException:
                rating = None
                
            try:
                review_count_elem = self.driver.find_element(
                    By.CSS_SELECTOR, "span.review-count"
                )
                review_count = int(''.join(
                    filter(str.isdigit, review_count_elem.text)
                ))
            except NoSuchElementException:
                review_count = None
            
            return YelpPlace(
                name=name,
                category=details['category'],
                price_range=details['price_range'],
                hours=hours,
                reviews_text=reviews_text,
                rating=rating,
                review_count=review_count,
                url=url
            )
            
        except Exception as e:
            self.logger.error(f"Error scraping {url}: {str(e)}")
            return None

    def save_place(self, place: YelpPlace):
        """Save a single place to the CSV file"""
        place_dict = asdict(place)
        
        # Convert dictionary values to JSON strings
        place_dict['hours'] = json.dumps(place_dict['hours'])
        place_dict['reviews_text'] = json.dumps(place_dict['reviews_text'])
        
        # Create DataFrame with single row
        df = pd.DataFrame([place_dict])
        
        # Append to CSV file
        if os.path.exists(self.output_csv):
            df.to_csv(self.output_csv, mode='a', header=False, index=False)
        else:
            df.to_csv(self.output_csv, index=False)
            
        self.completed_places.add(place.name)
        self.logger.info(f"Saved data for: {place.name}")

    def scrape(self):
        """Main scraping function with enhanced error handling"""
        try:
            for _, row in self.places_df.iterrows():
                name = row['name']
                neighborhood = row['neighborhood']
                
                # Skip if already scraped
                if name in self.completed_places:
                    self.logger.info(f"Skipping {name} - already scraped")
                    continue
                
                try:
                    # Search for business
                    business_url = self.search_business(name, neighborhood)
                    if not business_url:
                        continue
                    
                    # Random delay between search and scraping
                    time.sleep(random.uniform(3, 5))
                    
                    # Scrape business details
                    place = self.scrape_business_page(business_url)
                    if place:
                        self.save_place(place)
                    
                    # Longer delay between businesses
                    time.sleep(random.uniform(5, 8))
                    
                except Exception as e:
                    self.logger.error(f"Error processing {name}: {str(e)}")
                    time.sleep(random.uniform(10, 15))  # Longer delay after error
                    continue
                
        except Exception as e:
            self.logger.error(f"Fatal error during scraping: {str(e)}")
        finally:
            self.driver.quit()

def main():
    scraper = YelpScraper('places.csv', 'yelp_data.csv')
    scraper.scrape()

if __name__ == "__main__":
    main()