from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

import threading
import constant

from Scripts import DatabaseHandler


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


# Parses list of row entries for symbols
def add_symbols(entries):
    """ NOTE: MySQL connector is NOT thread-safe. Every thread needs it's own instance."""
    cnx = DatabaseHandler.connect_to_db(user, "TheSpatula")
    mycursor = cnx.cursor()
    assert mycursor
    assert DatabaseHandler.table_exists(mycursor, "crypto_symbols")

    for i in range(len(entries)):
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
    num_threads = 8
    threads = []

    # Action
    rows = get_entries()

    start = 0
    end = 0
    for block_index in range(num_threads - 1):
        block_length = len(rows) // num_threads
        start = block_length * block_index
        end = block_length * (block_index + 1)  # end index (non-inclusive)
        threads.append(threading.Thread(target=add_symbols, args=[rows[start: end]]))
    threads.append(threading.Thread(target=add_symbols, args=[rows[end:]]))

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=constant.SECONDS_IN_DAY)

    driver.close()
