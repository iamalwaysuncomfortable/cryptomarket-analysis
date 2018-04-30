import custom_utils.datautils as du
import data_scraping.aggregation_service.aggregation_helpers as ah
import db_services.readwrite_utils as rw
import log_service.logger_factory as lf

logging = lf.get_loggly_logger(__name__)
lf.launch_logging_service()

def export_table_query(price_point, timestamp_type="epoch", export_statement=False):
    if timestamp_type == "epoch":
        ts = "uts"
    else:
        ts = "utc_time"
    i = 0
    ordered_symbol_query = "SELECT DISTINCT s.from_symbol, t.marketcap FROM pricedata.pair_data s LEFT JOIN pricedata.coin_ranks t ON s.from_symbol = t.symbol WHERE t.marketcap > 0 ORDER BY t.marketcap DESC"
    symbols = [x[0] for x in ah.make_db_call(7200, ordered_symbol_query)]
    if export_statement == True:
        text = "COPY (SELECT * FROM "
    else:
        text = "SELECT * FROM "

    table_text = "(SELECT s."+ts+", s.from_symbol, s."+price_point+" FROM pricedata.pair_data s LEFT JOIN pricedata.coin_ranks t ON " \
                 "s.from_symbol = t.symbol WHERE t.marketcap > 0 ORDER BY t.marketcap DESC, s."+ts+") mcp "

    text = text + "crosstab($$select mcp."+ts+", mcp.from_symbol, mcp."+price_point+" from "+table_text+" ORDER BY 1$$ , "

    text = text + "$$VALUES "
    s_count = []
    def value_order(text, symbol):
        s_count.append(text + "('"+ symbol + "'), ")
        return text + "('"+ symbol + "'), "

    for symbol in symbols:
        if i < 1599:
            if symbol[0] == '*':
                symbol = "s" + symbol[1:len(symbol)]
                text = value_order(text, symbol)
                i += 1
            elif symbol[-1] == "*":
                symbol = "s" + symbol[0:-1]
                text = value_order(text, symbol)
                i += 1
            elif symbol[0] in '1234567890' or symbol == 'AND' or symbol == "IN" or symbol == "OR":
                symbol = "s" + symbol
                text = value_order(text, symbol)
                i += 1
            else:
                text = value_order(text, symbol)
                i += 1
        else:
            break
    text = text[0:len(text) - 2] + "$$) "
    print(i)
    print(len(s_count))
    i = 0

##
 #   text = text + "FROM crosstab($$select uts, from_symbol, high from pricedata.pair_data ORDER BY 1,2$$, " \
 #                 "$$select DISTINCT from_symbol from pricedata.pair_data ORDER BY 1$$)"
    if timestamp_type == "epoch":
        text = text + "AS final(UTS numeric, "
    else:
        text = text + "AS final(UTC_TIME text, "



    def update_text(text, symbol):
        return text + symbol + " NUMERIC, "

    for symbol in symbols:
        if i < 1599:
            if symbol[0] == '*':
                symbol = "s" + symbol[1:len(symbol)]
                text = update_text(text, symbol)
                i+=1
                continue
            elif symbol[-1] == "*":
                symbol = "s" + symbol[0:-1]
                text = update_text(text, symbol)
                i += 1
                continue
            elif symbol[0] in '1234567890' or symbol == 'AND' or symbol == "IN" or symbol == "OR":
                symbol = "s" + symbol
                text = update_text(text, symbol)
                i += 1
                continue
            else:
                text = update_text(text, symbol)
                i += 1
        else:
            break
    print(i)

    text = text[0:len(text) - 2]
    text = text + ") "
    time_stamp = du.get_time(now=True,utc_string=True, custom_format="%Y_%m_%dT%H_%M_%SZ")
    if export_statement == True:
        return text + ") TO '/Users/CommanderTurner/Google_Drive_C/Price_pair_data_dumps/"+price_point+".csv' DELIMITER ',' CSV HEADER"
    else:
        return text

def execute_table_query(query):
    rw.get_data_from_one_table(query)

def get_one_sheet(sheet, timestamp_type="utc"):
    query = export_table_query(sheet,timestamp_type=timestamp_type,export_statement=True)
    execute_table_query(query)

def main():
    for item in ["high", "low", "_open", "_close"]:
        query = export_table_query(item, "utc", True)
        execute_table_query(query)