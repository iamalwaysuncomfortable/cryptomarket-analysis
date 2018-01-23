import data_scraping.HTTP_helpers as HTTPh
import coinmarketcap as cmc
import log_service.logger_factory as lf
from bs4 import BeautifulSoup
logging = lf.get_loggly_logger(__name__)

name_lookup_table = cmc.get_name_symbol_lookup_table(lookup_by_name=True)
parent_blockchain_stats = []
parent_blockchain_url_list = []

def get_parent_blockchain_links(min_derivatives=2):
    call = "https://tokenmarket.net/blockchain/"
    page = HTTPh.general_api_call(call)
    if page.status_code == 200:
        page = page.content
        soup = BeautifulSoup(page, 'html.parser')
        parent_blockchain_html_table = soup.tbody.find_all("tr")
        coin_dict = {}
        for entry in parent_blockchain_html_table:
            entry_contents = entry.find_all("td")
            num_derivative = int(entry_contents[1].contents[0].string.strip(' \t\n\r'))
            parent_blockchain_name = entry_contents[0].contents[1].string.strip(' \t\n\r')
            if num_derivative < 2:
                break
            if parent_blockchain_name != "HyperLedger" and parent_blockchain_name not in name_lookup_table:
                continue

            if parent_blockchain_name in name_lookup_table:
                parent_blockchain_symbol = name_lookup_table[parent_blockchain_name]
                coin_dict[parent_blockchain_symbol] = {"symbol":parent_blockchain_symbol, "name":parent_blockchain_name,
                                                       "asset_list_url": entry_contents[2].contents[1]['href']}
            elif parent_blockchain_name == "HyperLedger":
                coin_dict["HyperLedger"] = {"symbol": "HyperLedger", "token_count":num_derivative, "name": parent_blockchain_name,
                                                       "asset_list_url": entry_contents[2].contents[1]['href']}

        return coin_dict
    else:
        msg = "Status code of %s returned, data not collected" %str(page.status_code)
        logging.warn(msg)
        HTTPh.throw_HTTP_error(msg)

def get_derivative_token_list(parent_blockchain_list=None):
    platform_dict = {}
    if parent_blockchain_list == None:
        parent_blockchains = get_parent_blockchain_links()
    else:
        parent_blockchains = parent_blockchain_list
    derivative_asset_dict = {}
    for parent_blockchain in parent_blockchains.values():
        derivative_coins = {}
        page = HTTPh.general_api_call(parent_blockchain["asset_list_url"])
        page = page.content
        soup = BeautifulSoup(page, 'html.parser')
        asset_table = soup.tbody.find_all("tr")
        for entry in asset_table:
            entry_contents = entry.find_all("td")
            token_name = entry_contents[1].contents[1].string.strip(' \t\n\r')
            token_symbol = entry_contents[2].contents[0].string.strip(' \t\n\r')
            token_description = entry_contents[3].contents[0].string.strip(' \t\n\r')
            derivative_coins[token_symbol] = {'symbol': token_symbol, 'name': token_name, 'description':token_description,
                                      'parent_platform_symbol': parent_blockchain['symbol'],
                                      'parent_platform_name': parent_blockchain['name']}
            if parent_blockchain['symbol'] not in platform_dict:
                platform_dict[parent_blockchain['symbol']] = [derivative_coins[token_symbol]]
            else:
                platform_dict[parent_blockchain['symbol']].append(derivative_coins[token_symbol])
        derivative_asset_dict.update(derivative_coins)
    return derivative_asset_dict, parent_blockchains

