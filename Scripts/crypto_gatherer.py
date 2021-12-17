# Creates .csv list of crypto tickers
import codecs
import traceback

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

from tqdm import tqdm

import DatabaseHandler


def get_cryptos():
    print("[1/3] Opening Headless Browser...")

    # Use Headless browser
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    driver = webdriver.Chrome(user.path, options=options)
    wait = WebDriverWait(driver, 20)

    driver.get("https://ca.investing.com/crypto/currencies")

    print("[2/3] Waiting for Popup...")
    # close popup if it appears
    try:
        popup_close = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//div/div/i[contains(@class, 'popupCloseIcon')]")))
        popup_close.click()
    except:
        print("popup not found")
        pass

    # Wait for all rows to load
    num_rows = driver.find_element_by_xpath(
        "//div[@class='info_line']/span[1]/a[@href='/crypto/currencies']/../../span[2]").text
    num_rows = int(num_rows.replace(',', ''))

    wait.until(EC.visibility_of_element_located((By.XPATH, "//table/tbody/tr[" + str(num_rows - 1) + "]")))
    rows = driver.find_elements_by_xpath("//html/body/div[5]/section/div[10]/table/tbody/tr")

    with codecs.open("./../Tickers/tickers_crypto.csv", "w", encoding='utf-8') as f:
        for i in tqdm(range(num_rows), desc='[3/3] Scraping Rows'):
            try:
                symbol = str(rows[i].find_element_by_xpath(".//td[4]").text)
                f.write(f"{symbol}\n")
            except:
                print(f"Had trouble with symbol: {symbol}")
                traceback.print_exc()
                exit(1)

    driver.close()


if __name__ == "__main__":
    user = DatabaseHandler.User("my_win")

    get_cryptos()

    print("DONE!!")
