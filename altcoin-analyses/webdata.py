import pandas as pd
import requests
from bs4 import BeautifulSoup

##Get main ranking data
def get_coin_data(uri, parameters = None, give_json = False):
    if parameters is None:
        response = requests.get(uri)
    else:
        response = requests.get(uri, parameters)
    print(response.status_code)
    print(response.content)
    json_data = response.json()
    if give_json is False:
        data = pd.DataFrame(json_data)
        return data
    else:
        return json_data

def get_crypto_compare_data(uri):
    data = get_coin_data(uri, give_json=True)
    algo_dict = {}
    for coin, metadata in data["Data"].items():
        coin = str(coin)
        algo_dict[coin] = {"algorithm": str(metadata["Algorithm"]), "prooftype": str(metadata["ProofType"]), "fullypremined": int(metadata["FullyPremined"])}
    data = pd.DataFrame(algo_dict.values(), index=algo_dict.keys())
    return data

def get_derivative_coin_data():
    page = requests.get('https://coinmarketcap.com/assets/views/all/')
    contents = page.content
    soup = BeautifulSoup(contents, 'html.parser')
    coin_list = soup.tbody.find_all("tr")
    coin_dict = {}
    for coin in coin_list:
        coin_name = str(coin.find("a").contents[0])
        platform_name = str(coin["data-platformsymbol"])
        coin_dict[coin_name] = platform_name
    return coin_dict

