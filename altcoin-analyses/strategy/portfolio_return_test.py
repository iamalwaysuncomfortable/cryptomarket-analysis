import custom_utils.datautils as du
import db_services.readwrite_utils as rw
import res.db_data.sql_statement_library as psqll
import data_scraping.coin_infosite_service.cryptocompare as cc
import custom_utils.type_validation as tv
import pandas as pd
import numpy as np


def execute_running_strategy():
    pass

def store_strategy_results():
    pass

_YEAR_ = 31536000.0
_WEEK_ = 604800.0
_DAY_ = 86400.0
_HOUR_ = 3600.0
_MINUTE_ = 60.0

class StrategySimulator(object):
    def __init__(self, portfolio_df=None):
        self.portfolio_df = portfolio_df
        self.returns_df = None
        self.position = 0
        self.hold_period = None
        self._types_ = (list, tuple, np.ndarray)
        self.returns = 0
    def set_mock_holdings(self, symbols=None,purchase_dates=None, purchase_prices=None, quantity=None, hold_period=None):
        if isinstance(symbols, self._types_) and isinstance(purchase_dates, self._types_):
            self.portfolio_df = pd.DataFrame(data={"coin_symbol":symbols, "buy_date":purchase_dates})
            print(self._types_)
            print type(self._types_)
            if isinstance(purchase_prices, self._types_):
                self.portfolio_df["buy_price"] = purchase_prices
            else:
                self.get_prices_at_dates(date_column="buy_date")
            if isinstance(quantity, self._types_):
                self.portfolio_df["quantity"] = quantity
            self.returns = -sum(ss.portfolio_df["buy_price"]*ss.portfolio_df["quantity"])
            self.hold_period=hold_period
    def get_prices_at_dates(self, date_column="uts", symbol_column="coin_symbol", get_from_online=False):
        if not isinstance(self.portfolio_df, pd.DataFrame):
            raise ValueError("Pandas Dataframe with a column containing symbols and a date ")
        date_symbol_pair = [[], tuple(self.portfolio_df[symbol_column])]
        if get_from_online == True:
            if tv.validate_list_element_types(tuple(self.portfolio_df[date_column]), (str, unicode)):
                date_symbol_pair[0] = [du.convert_epoch(date, str2e=True) for date in tuple(self.portfolio_df[date_column])]
            elif tv.validate_list_element_types(tuple(self.portfolio_df[date_column]), (int, float)):
                date_symbol_pair[0] = tuple(self.portfolio_df[date_column])
            else:
                raise ValueError("All dates in date column must be either str or unicode in %Y-%m-%d format or unix epoch in int or float format")
        else:
            if tv.validate_list_element_types(list(self.portfolio_df[date_column]), (int, float)):
                date_symbol_pair[0] = (du.convert_epoch(date) for date in tuple(self.portfolio_df[date_column]))
            else:
                date_symbol_pair[0] = tuple(self.portfolio_df[date_column])
        prices = []
        print(date_symbol_pair[0])
        for i in range(0, len(date_symbol_pair[0])):
            date = date_symbol_pair[0][i]
            symbol = date_symbol_pair[1][i]
            if get_from_online == True:
                prices.append(cc.get_price_data(date, symbol, "USD")[date_symbol_pair[0][i]]["USD"])
            else:
                price = rw.get_data_from_one_table(psqll.price_at_date(date, symbol))[0][0]
                prices.append(price)
        print(prices)
        self.portfolio_df["buy_price"] = pd.Series(data=prices)
    def calculate_historical_returns_on_portfolio(self):
        if not isinstance(self.portfolio_df, pd.DataFrame):
            raise AttributeError("Manager doesn't have portfolio information set, please set and continue")
        symbols = list(set(self.portfolio_df["coin_symbol"]))
        if isinstance(self.hold_period, (list, tuple)) and len(self.hold_period) == 2 and \
            (tv.validate_list_element_types(self.hold_period, (str, unicode)) or tv.validate_list_element_types(self.hold_period, (int, float))):
            prices = get_prices_in_date_range(symbols, self.hold_period[0], self.hold_period[1])
        else:
            prices = get_prices_in_date_range(symbols, self.portfolio_df["buy_date"].min(), self.portfolio_df["buy_date"].max())
        dates = [record[6] for record in prices if record[0] == symbols[0]]
        uts = np.array([int(record[7]) for record in prices if record[0] == symbols[0]], dtype=np.int32)
        df = pd.DataFrame(data={"date":dates,"uts":uts})
        for symbol in symbols:
            df[symbol + "_price_usd"] = np.array([record[3] for record in prices if record[0] == symbol], dtype=np.float16)
            df[symbol + "_price_btc"] = np.array([record[2] for record in prices if record[0] == symbol], dtype=np.float16)
            for index, row in self.portfolio_df[self.portfolio_df['coin_symbol'] == symbol].iterrows():
                buy_date_uts = du.convert_epoch(row["buy_date"], str2e=True)
                df.loc[df.uts > buy_date_uts, symbol +"_"+ str(row["buy_date"]) + "_pct_increase"] = \
                df.loc[df.uts > buy_date_uts][symbol + "_price_usd"] / row["buy_price"] - 1
                df.loc[df.uts > buy_date_uts, symbol +"_"+ str(row["buy_date"]) + "_returns"] = \
                df.loc[df.uts > buy_date_uts][symbol + "_price_usd"] - row["buy_price"]
        df.sort_values(["uts"], inplace=True)
        self.returns_df = df
        return df
    def test_strategy(self,simple_multiple=None, cumulative=False, data=None, hold_period=None, period=None):
        if isinstance(data, dict) and all(True for key in ("buy_date","quantity","coin_symbol") if key in data.keys()):
            self.hold_period = hold_period
            if "buy_price" in data.keys():
                self.set_mock_holdings(symbols=data["coin_symbol"],purchase_dates=data["buy_date"],
                                       quantity=data["quantity"],purchase_prices=data["buy_price"])
            else:
                self.set_mock_holdings(symbols=data["coin_symbol"], purchase_dates=data["buy_date"],
                                       quantity=data["quantity"])
            self.calculate_historical_returns_on_portfolio()
        returns = self.returns
        if isinstance(simple_multiple, (int, float)):
            sold_holdings = []
            for index, row in self.portfolio_df.iterrows():
                if period is not None:
                    if isinstance(period, (list, tuple)) and len(period) == 2 and (tv.validate_list_element_types(period, (str, unicode)) or tv.validate_list_element_types(period,(int, float))):
                        start, end = du.convert_epoch(period[0], str2e=True), du.convert_epoch(period[1], str2e=True)
                        returns_df = self.returns_df[(self.returns_df[row["coin_symbol"] + "_" + str(
                            row["buy_date"]) + "_pct_increase"] > simple_multiple) & ((start <= self.returns_df['uts']) | (self.returns_df['uts']) <= end)]
                    else:
                        raise ValueError("Period must be specified as list or tuple with dates in either unix epoch or %Y-%m-%d format")
                else:
                    returns_df = self.returns_df[self.returns_df[row["coin_symbol"] + "_" + str(row["buy_date"]) + "_pct_increase"] > simple_multiple]
                if len(returns_df) > 0:
                   sell_price = returns_df[row["coin_symbol"] + "_price_usd"].iloc[0]
                   sold_holdings.append((row["coin_symbol"], row["buy_date"]))
                   if cumulative == True:
                       self.returns += row["quantity"]*sell_price
                   else:
                       returns += row["quantity"]*sell_price
            if cumulative == True:
                for holding in sold_holdings:
                    self.portfolio_df.loc[(self.portfolio_df["coin_symbol"] ==
                                  sold_holdings[0]) & (self.portfolio_df["buy_date"] == sold_holdings[1]), "quantity"] = 0
                print("Return in period is %s" % self.returns)
                return self.returns
            else:
                print("Return in period is %s" % returns)
                print("Sold Holdings Were %s" % sold_holdings)
                return returns
    def monte_carlo_simple_multiples(self, initialization_data=None, test_multiples=None, periods_to_cycle=None, period_start = None, period_end=None):
        report_container = []
        periods_to_cycle = np.array(periods_to_cycle)
        if period_start == None:
            start = self.returns_df['uts'].iloc[0]
        else:
            start = du.convert_epoch(period_start, str2e=True)
        if period_end == None:
            end = self.returns_df['uts'].iloc[-1]
        else:
            end = du.convert_epoch(period_end, str2e=True)
        total_days_in_period = (end - start)/_DAY_
        if start < self.returns_df['uts'].iloc[0]:
            raise ValueError("Start date specified is before start date of %s in stored records" % du.convert_epoch(self.returns_df['date'].iloc[0]))
        periods_to_cycle = periods_to_cycle[periods_to_cycle <= (end - start)/_DAY_]
        periods_to_test = []
        for period in periods_to_cycle:
            for i in range(0, int(total_days_in_period/period)):
                periods_to_test.append((start + _DAY_*period*i, start + _DAY_*period*(i+1)))
        for multiple in test_multiples:
            for period in periods_to_test:
                gain = self.test_strategy(simple_multiple=multiple,period=period)
                report_container.append((gain, multiple, (period[1]-period[0])/_DAY_,
                                         du.convert_epoch(float(start),e2str=True), du.convert_epoch(float(end),e2str=True)))
        return report_container

ss = StrategySimulator()
ss.set_mock_holdings(["ETH","ETC","NEO","BTC"],["2017-01-04","2017-04-20","2017-08-08","2017-04-26"],quantity=[100,8,40,1.55], hold_period=("2017-01-04","2018-08-21"))
ss.calculate_historical_returns_on_portfolio()
#ss.test_strategy(simple_multiple=2)
ss.test_strategy(simple_multiple=2, period=["2017-12-01", "2017-12-31"])
monte_data = ss.monte_carlo_simple_multiples(test_multiples=(0.1,0.25,0.5,1,2,3,5,7.5,10,15,25,30,40,50,60,70,80,100),periods_to_cycle=[14,30,45,60,90,120,180,270,360])

z = [d[0] for d in monte_data]
x = [d[1] for d in monte_data]
y = [d[2] for d in monte_data]

def get_prices_in_date_range(symbols, period_start,period_end):
    query = psqll.price_history_in_custom_date_range(symbols=symbols, custom_range=(period_start,period_end))
    return rw.get_data_from_one_table(query)

