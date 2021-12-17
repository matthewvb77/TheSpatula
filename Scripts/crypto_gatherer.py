from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

from tqdm import tqdm

import DatabaseHandler


def get_cryptos():
    print("[1/2] Opening Headless Browser...")

    # Use Headless browser
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    driver = webdriver.Chrome(user.path, options=options)
    wait = WebDriverWait(driver, 20)

    driver.get("https://ca.investing.com/crypto/currencies")

    # Wait for all rows to load
    num_rows = driver.find_element_by_xpath(
        "//div[@class='info_line']/span[1]/a[@href='/crypto/currencies']/../../span[2]").text
    num_rows = int(num_rows.replace(',', ''))

    wait.until(EC.visibility_of_element_located((By.XPATH, "//table/tbody/tr[" + str(num_rows - 1) + "]")))
    rows = driver.find_elements_by_xpath("//html/body/div[5]/section/div[10]/table/tbody/tr")

    cnx = DatabaseHandler.connect_to_db(user, "TheSpatula")
    mycursor = cnx.cursor()
    assert mycursor
    assert DatabaseHandler.table_exists(mycursor, "crypto_symbols")

    for i in tqdm(range(num_rows), desc='[2/2] Scraping Rows'):
        symbol = rows[i].find_element_by_xpath(".//td[4]").text
        mycursor.execute(f"""INSERT IGNORE INTO crypto_symbols (symbol) VALUES("{symbol}");""")
        if i % 100 == 0:
            cnx.commit()
    cnx.commit()

    driver.close()


if __name__ == "__main__":
    user = DatabaseHandler.User("my_win")
    get_cryptos()

    print("DONE!!")
