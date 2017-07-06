import pandas as pd
import requests
import io
from unicodedata import normalize
from random import randint
from time import sleep
from bs4 import BeautifulSoup

###Utility Methods
def read_csv(url):
    with requests.Session() as s:
        s = requests.get(url).content
        cr = pd.read_csv(io.StringIO(s.decode('utf-8')), index_col="symbol")
        return cr


def fetch_data(uri, parameters = None):
    """ Get data from API or download HTML, try each URI 5 times """
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    with requests.Session() as s:
        a = requests.adapters.HTTPAdapter(max_retries=5)
        s.mount('https://', a)
        response = s.get(uri, params = parameters, headers = headers)
    return response

def clean_list(data, give_tuple=False):
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
    response = fetch_data(uri)
    json_data = response.json()
    algo_dict = {}
    for coin, metadata in json_data["Data"].items():
        coin = str(coin)
        algo_dict[coin] = {"algorithm": str(metadata["Algorithm"]), "prooftype": str(metadata["ProofType"]), "fullypremined": int(metadata["FullyPremined"])}
    data = pd.DataFrame(algo_dict.values(), index=algo_dict.keys())
    return data

def get_derivative_coin_data():
    page = fetch_data('https://coinmarketcap.com/assets/views/all/')
    if page.status_code == 200:
        contents = page.content
        soup = BeautifulSoup(contents, 'html.parser')
        coin_list = soup.tbody.find_all("tr")
        coin_dict = {}
        for coin in coin_list:
            coin_name = str(coin.find("a").contents[0])
            platform_name = str(coin["data-platformsymbol"])
            coin_dict[coin_name] = platform_name
        return coin_dict
    else:
        return "could not collect data due to site error"

def get_social_and_dev_data():
    result = "could not collect data due to site error"
    data = {}
    for i in xrange(1, 5):
        sleep(randint(1, 4))
        page = fetch_data("https://www.coingecko.com/en?page=" + str(i))
        if page.status_code is not 200:
            if bool(data):
                result = pd.DataFrame(data.values(), data.keys())
            return result
        soup = BeautifulSoup(page.content, 'lxml')
        coin_table = soup.find_all("tr")
        for i in xrange(2, 62):
            entries = coin_table[i].find_all("td")
            symbol = coin_table[i].find("div",class_="text-muted text-center").get_text(strip=True)
            liquidity_btc = entries[4].get_text(strip=True)
            liquidity_btc = normalize('NFKD', liquidity_btc).replace(u"\u0e3f", "")
            dev_data = clean_list(entries[6].get_text() + entries[7].get_text())
            reddit_data = clean_list(entries[9].get_text())
            facebook_likes, twitter_followers = clean_list(entries[10].get_text(), give_tuple=True)
            bing_results, alexa_ranking = clean_list(entries[12].get_text(), give_tuple=True)
            data[symbol] = {"liquidity_btc":liquidity_btc, "stars":dev_data[0], "forks":dev_data[1], "watchers":dev_data[2], "issues":dev_data[3],"closed_issues":dev_data[4],"merged_pull_requests":dev_data[5], "contributors":dev_data[6],"commits_last_4_weeks":dev_data[7], "reddit_subscibers":reddit_data[0], "av_users_online":reddit_data[1], "av_posts_per_hour":reddit_data[2],"av_hourly_comments_on_hot_posts":reddit_data[3], "facebook_likes":facebook_likes, "twitter_followers":twitter_followers, "bing_results":bing_results, "alexa_ranking":alexa_ranking}
    result = pd.DataFrame(data.values(), data.keys())
    return result


result = "could not collect data due to site error"
data = {}
for i in xrange(1, 5):
    sleep(randint(1, 4))
    page = fetch_data("https://www.coingecko.com/en?page=" + str(i))
    if page.status_code is not 200:
        if bool(data):
            result = pd.DataFrame(data.values(), data.keys())

    soup = BeautifulSoup(page.content, 'lxml')
    coin_table = soup.find_all("tr")
    for i in xrange(2, 62):
        entries = coin_table[i].find_all("td")
        symbol = coin_table[i].find("div",class_="text-muted text-center").get_text(strip=True)
        liquidity_btc = entries[4].get_text(strip=True)
        liquidity_btc = normalize('NFKD', liquidity_btc).replace(u"\u0e3f", "")
        dev_data = clean_list(entries[6].get_text() + entries[7].get_text())
        reddit_data = clean_list(entries[9].get_text())
        facebook_likes, twitter_followers = clean_list(entries[10].get_text(), give_tuple=True)
        bing_results, alexa_ranking = clean_list(entries[12].get_text(), give_tuple=True)
        data[symbol] = {"liquidity_btc":liquidity_btc, "stars":dev_data[0], "forks":dev_data[1], "watchers":dev_data[2], "issues":dev_data[3],"closed_issues":dev_data[4],"merged_pull_requests":dev_data[5], "contributors":dev_data[6],"commits_last_4_weeks":dev_data[7], "reddit_subscibers":reddit_data[0], "av_users_online":reddit_data[1], "av_posts_per_hour":reddit_data[2],"av_hourly_comments_on_hot_posts":reddit_data[3], "facebook_likes":facebook_likes, "twitter_followers":twitter_followers, "bing_results":bing_results, "alexa_ranking":alexa_ranking}
result = pd.DataFrame(data.values(), data.keys())

def get_ico_data():
    page = fetch_data('https://www.coingecko.com/ico?locale=en')
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





