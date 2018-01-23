
##Crypto Compare

def result_returner(options):
    if len(options) > 1:
        return tuple(options)
    if len(options) == 1:
        return options[0]

def price_history_one_token(coin_symbol, all_fields=False, price_pairs = "price_USD"):
    if all_fields == True:
        query = "SELECT * from pricedata.token_prices WHERE coin_symbol = '" + coin_symbol + "'"
    else:
        query = "SELECT uts, "+price_pairs+" from pricedata.token_prices WHERE coin_symbol = '" +coin_symbol +"'"
    return query

def crypto_price_data_statements(read=True, write=True):
    results = []
    writes = {"price":"INSERT INTO pricedata.token_prices (coin_symbol, coin_name, price_BTC, price_USD, price_EUR, "
              "price_CNY, utcdate, uts, source) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"}
    reads = {"get_all":"SELECT * FROM pricedata.token_prices",
             "token_symbols":"SELECT coin_symbol FROM pricedata.token_prices"}
    if read==True:
        results.append(reads)
    if write==True:
        results.append(writes)
    return result_returner(results)

def crypto_categorical_data_statements(read=True,write=True):
    results = []
    if read == True:
        reads = {"token_symbols": "SELECT coin_symbol FROM pricedata.static_token_data"}
        results.append(reads)
    if write == True:
        writes = {"write_cc_record":"INSERT INTO pricedata.static_token_data (coin_symbol, coin_name, "
        "start_date, start_date_utc, algorithm, total_supply, amount_premined, fully_premined, coinmarketcap_id, "
         "proof_type, description, features, technology, website, twitter, derivative_token, parent_blockchain) "
        "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"}
        results.append(writes)
    return result_returner(results)