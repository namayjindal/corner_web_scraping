from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import time

def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def get_restaurant_description(url):
    driver = setup_driver()
    try:
        print("Accessing URL...")
        driver.get(url)
        time.sleep(5)  # Give the page time to load completely
        
        # Looking at the screenshot, we can see the description is in a span with these classes
        description_selector = "span[data-test='wrapper-span']"
        
        print("Waiting for description element...")
        description_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, description_selector))
        )
        
        description_text = description_element.text
        print("\nDescription found!")
        return description_text
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return None
    finally:
        driver.quit()

if __name__ == "__main__":
    test_url = "https://www.opentable.com/the-smith-east-village?originId=eb97098a-30c5-4fd6-906a-20861c33113e&corrid=eb97098a-30c5-4fd6-906a-20861c33113e&avt=eyJ2IjoyLCJtIjoxLCJwIjowLCJzIjowLCJuIjowfQ"
    
    description = get_restaurant_description(test_url)
    if description:
        print("\nRestaurant Description:")
        print(description)
    else:
        print("\nFailed to get description")