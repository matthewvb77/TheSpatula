from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

from tqdm import tqdm

import DatabaseHandler

# GLOBAL VARIABLES
driver = None


def get_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    return webdriver.Chrome(user.path, options=options)


def get_entries():
    # Use Headless browser

    driver.get("https://ca.investing.com/crypto/currencies")

    # Wait for all rows to load
    num_rows = driver.find_element_by_xpath(
        "//div[@class='info_line']/span[1]/a[@href='/crypto/currencies']/../../span[2]").text
    num_rows = int(num_rows.replace(',', ''))

    wait.until(EC.visibility_of_element_located((By.XPATH, "//table/tbody/tr[" + str(num_rows - 1) + "]")))

    return driver.find_elements_by_xpath("//html/body/div[5]/section/div[10]/table/tbody/tr")


def add_symbols(entries):
    cnx = DatabaseHandler.connect_to_db(user, "TheSpatula")
    mycursor = cnx.cursor()
    assert mycursor
    assert DatabaseHandler.table_exists(mycursor, "crypto_symbols")

    for i in tqdm(range(len(entries)), desc='[2/2] Scraping Rows'):
        symbol = entries[i].find_element_by_xpath(".//td[4]").text
        mycursor.execute(f"""INSERT IGNORE INTO crypto_symbols (symbol) VALUES("{symbol}");""")
        if i % 100 == 0:
            cnx.commit()
    cnx.commit()


if __name__ == "__main__":
    # Setup
    user = DatabaseHandler.User("my_win")
    driver = get_driver()
    wait = WebDriverWait(driver, 20)

    # Action
    rows = get_entries()
    add_symbols(rows)

    driver.close()
    print("DONE!!")
