
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

def pair_history_one_pair(from_symbol, to_symbol):
    return "SELECT * from pricedata.pair_data WHERE from_symbol = '"+from_symbol+"' AND to_symbol = '"+to_symbol+"'"

def pair_histories_start_date_truth(to_symbol="USD", start_date_truth='t'):
    return "SELECT from_symbol, uts, first_date, to_symbol FROM pricedata.pair_data WHERE to_symbol =" \
           " '"+to_symbol+"' and first_date = '"+start_date_truth+"'"



def crypto_price_data_statements(read=False, write=False):
    results = []
    writes = {"price_history":"INSERT INTO pricedata.token_prices (coin_symbol, coin_name, price_BTC, price_USD, price_EUR, "
              "price_CNY, utcdate, uts, source) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
              "pair_history":"INSERT INTO pricedata.pair_data (uts, utc_time, from_symbol, to_symbol, "
              "from_name, to_name, low, high, _open, _close, first_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
              "ON CONFLICT (uts, from_symbol, to_symbol) DO UPDATE SET (first_date) = (EXCLUDED.first_date)"}
    reads = {"price_history_get_all":"SELECT * FROM pricedata.token_prices",
             "price_history_token_symbols":"SELECT coin_symbol FROM pricedata.token_prices",
             "pair_history_get_all": "SELECT * FROM pricedata.pair_data",
             "pair_history_token_symbols": "SELECT from_symbol, to_symbol FROM pricedata.pair_data",
             "min_pair_dates":"SELECT from_symbol, MIN(uts), first_date, to_symbol FROM pricedata.pair_data GROUP BY from_symbol, first_date, to_symbol",
             "maxn_pair_dates": "SELECT from_symbol, MAX(uts), first_date, to_symbol FROM pricedata.pair_data GROUP BY from_symbol, first_date, to_symbol",
             "min_price_dates": "SELECT t2.coin_symbol, t2.uts, t2.price_usd FROM (SELECT coin_symbol, min(uts) as uts "
                                "FROM pricedata.token_prices GROUP BY coin_symbol) t INNER JOIN pricedata.token_prices "
                                " t2 on t.uts=t2.uts and t.coin_symbol = t2.coin_symbol ORDER BY t2.uts;",
             "start_dates":"select coin_symbol, start_date_utc from pricedata.static_token_data",
             "price_columns":"SELECT attname AS col FROM   pg_attribute "
                           "WHERE  attrelid = 'pricedata.token_prices'::regclass "
                           "AND    attnum > 0 AND    NOT attisdropped ORDER  BY attnum"}
    if read==True:
        results.append(reads)
    if write==True:
        results.append(writes)
    return result_returner(results)

def crypto_categorical_data_statements(read=False,write=False):
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