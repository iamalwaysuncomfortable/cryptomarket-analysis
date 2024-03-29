import pandas as pd
import requests
import io
import log_service.logger_factory as lf
from unicodedata import normalize
from random import randint
from time import sleep
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from requests.packages.urllib3.util.retry import Retry

logging = lf.get_loggly_logger(__name__)

###Utility Methods
def read_csv(url, ):
    with requests.Session() as s:
        s = requests.get(url).content
        cr = pd.read_csv(io.StringIO(s.decode('utf-8')), index_col="symbol")
        return cr

def post_http(uri, data="", headers = None, max_retries=5, auth=None):
    retries = Retry(total=max_retries, backoff_factor=1)
    with requests.Session() as s:
        a = HTTPAdapter(max_retries=retries)
        s.mount('https://', a)
        response = s.post(uri, data, headers=headers, auth=auth)
    return response

def get_oauth_token(url, app_id, app_secret, post_data, header = None):
    logging.debug("get_oauth_token launched")
    client_auth = HTTPBasicAuth(app_id, app_secret)
    response = post_http(url, auth=client_auth, data=post_data, headers=header)
    logging.debug("client_auth is %s, response is %s", client_auth, response)
    return response.json()

def get_http(uri, parameters = None, max_retries=5, headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}):
    """ Get data from API or download HTML, try each URI 5 times """
    with requests.Session() as s:
        a = requests.adapters.HTTPAdapter(max_retries)
        s.mount('https://', a)
        response = s.get(uri, params = parameters, headers = headers)
    return response

def get_tr_text(data, give_tuple=False):
    """ Create a list or tuple from text in HTML table rows """
    if give_tuple:
        cleaned_data = tuple(line.lstrip(' ') for line in data.split('\n') if line.lstrip() != '')
    else:
        cleaned_data = [line.lstrip(' ') for line in data.split('\n') if line.lstrip() != '']
    return cleaned_data

def get_coinlist(uri, parameters = None):
    response = requests.get(uri, parameters)
    json_data = response.json()
    data = pd.DataFrame(json_data)
    return data

###Get main ranking data
def get_techinfo(uri):
    response = get_http(uri)
    json_data = response.json()
    algo_dict = {}
    for coin, metadata in json_data["Data"].items():
        coin = str(coin)
        algo_dict[coin] = {"algorithm": str(metadata["Algorithm"]), "prooftype": str(metadata["ProofType"]), "fullypremined": int(metadata["FullyPremined"])}
    data = pd.DataFrame(algo_dict.values(), index=algo_dict.keys())
    return data

def get_derivative_coin_data():
    page = get_http('https://coinmarketcap.com/tokens/views/all/')
    if page.status_code == 200:
        contents = page.content
        soup = BeautifulSoup(contents, 'html.parser')
        coin_list = soup.tbody.find_all("tr")
        coin_dict = {}
        for coin in coin_list:
            coin_symbol = str(coin.find("a").contents[0])
            platform_symbol = str(coin["data-platformsymbol"])
            coin_dict[coin_symbol] = platform_symbol
        return coin_dict
    else:
        return "could not collect data due to site error"

def get_social_and_dev_data():
    result = "could not collect data due to site error"
    data = {}
    for i in xrange(1, 5):
        sleep(randint(1, 10))
        if i == 1:
            page = get_http("https://www.coingecko.com/en")
        else:
            page = get_http("https://www.coingecko.com/en?page=" + str(i))
        if page.status_code is not 200:
            logging.warn("failure http status of %s and result: %s", page.status_code, page.content)
            page.raise_for_status()
        soup = BeautifulSoup(page.content, 'lxml')
        coin_table = soup.find_all("tr")
        for j in xrange(2, 62):
            entries = coin_table[j].find_all("td")
            symbol = coin_table[j].find("span",class_="coin-crypto-symbol text-muted").get_text(strip=True)
            liquidity_btc = entries[4].get_text(strip=True)
            liquidity_btc = normalize('NFKD', liquidity_btc).replace(u"\u0e3f", "")
            try:
                subs, av_users, hourly_posts, hourly_comments = get_tr_text(entries[9].get_text(), give_tuple=True)
            except:
                subs, av_users, hourly_posts, hourly_comments = (0,0,0,0)
            try:
                facebook_likes, twitter_followers = get_tr_text(entries[10].get_text(), give_tuple=True)
            except:
                facebook_likes, twitter_followers = (0,0)
            try:
                bing_results, alexa_ranking = get_tr_text(entries[12].get_text(), give_tuple=True)
            except:
                bing_results, alexa_ranking = (0,0)
            data[symbol] = {"liquidity_btc":liquidity_btc, "reddit_subscibers":subs, "av_users_online":av_users, "av_posts_per_hour":hourly_posts,"av_hourly_comments_on_hot_posts":hourly_comments, "facebook_likes":facebook_likes, "twitter_followers":twitter_followers, "bing_results":bing_results, "alexa_ranking":alexa_ranking}
    result = pd.DataFrame(data.values(), data.keys())
    return result

def get_ico_data():
    page = get_http('https://www.coingecko.com/ico?locale=en')
    ico_dict = {}
    print(page.status_code)
    if page.status_code == 200:
        contents = page.content
        soup = BeautifulSoup(contents, 'html.parser')
        for coin in soup.find_all("tr"):
            if coin.find("div", attrs={'class': 'text-muted'}) is not None:
                data = coin.find_all("td")
                symbol = data[0].get_text(strip="True")
                name = data[1].get_text(strip="True")
                description = data[2].get_text(strip="True")
                date_info = data[3].find_all("div")
                launch_date = ""
                if len(date_info) == 3 and date_info[2].small is not None:
                    launch_date = date_info[2].small.get_text(strip=True)
                ico_dict[symbol] = {"name":name, "main_claim":description, "launch_date":launch_date}
    return ico_dict

def get_static_data():
    categorical_data = read_csv("https://docs.google.com/spreadsheet/ccc?key=1ZkwYGJ1m-X2xJ4vCdaeq-viGecvRnWniLQ5Jyt1D0YM&output=csv")
    replacement_data = read_csv("https://docs.google.com/spreadsheet/ccc?key=10YZu84w7FnjT7-rQEQ57KT7IK2RB9s3SYnSEvaQkW7Y&output=csv")
    return categorical_data, replacement_data




