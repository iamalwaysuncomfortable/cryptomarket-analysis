from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from bs4 import BeautifulSoup
from time import sleep
def get_number_of_erc20_token_contracts():
    browser_agent = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
    url = 'https://etherscan.io/tokens'
    dcap = dict(DesiredCapabilities.CHROME)
    dcap["chrome.page.settings.userAgent"] = browser_agent
    mydriver = webdriver.Chrome(desired_capabilities=dcap)
    mydriver.implicitly_wait(30)
    mydriver.set_window_size(1366,768)

    mydriver.get(url)
    title = mydriver.title
    page = mydriver.page_source
    sleep(15)
    page = mydriver.page_source
    soup = BeautifulSoup(page, 'html.parser')