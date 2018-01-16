
##Crypto Compare
def crypto_compare_statements():
    writes = {"price":"INSERT INTO pricedata.token_prices (coin_symbol, coin_name, price, update, uts, source)"
                                "VALUES(%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"}
    reads = {"get_all":"SELECT * FROM pricedata.token_prices"}
    return writes, reads
