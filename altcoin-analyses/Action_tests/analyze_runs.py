import analysis.pre_processing.price_pre_processing as pcp
import custom_utils.datautils as du
import custom_utils.type_validation as tv
import pandas as pd
import analysis.visualization.dash_reports as dr
import numpy as np
import matplotlib.pyplot as plt
import analysis.visualization.mpl_methods as sp
from res.env_config import set_master_config
set_master_config()
from log_service.logger_factory import get_loggly_logger, launch_logging_service
logging = get_loggly_logger(__name__, level="INFO")
launch_logging_service()

_MINUTE_ = 60
_HOUR_ = 24
_DAY_ = 86400
_WEEK_ = 604800
_YEAR_ = 31536000

increase_percentages = [0.1,0.2,0.3,0.5,0.75,1,2,3,5,10,25,50]
_periods=[30, 90, 180]
measures = ("7d_med_btc", "7d_med_usd", "7d_med_eur", "7d_med_cny",
                    "ewma", "ewma_std")

df, df_ix, intervals = pcp.create_measures(days_from_present=31
                                           , custom_measures=measures
                                           , custom_intervals=(30,)
                                           , pct_increases=(-.2, -.1)
                                           , pct_study_periods=(30,)
                                           , measure_percent_increases=True
                                           , add_coin_attributes=True, alpha=0.8)


df["derivative_token"] = df["derivative_token"].astype("float64")
df["fully_premined"] = df["fully_premined"].astype("float64")
#increase_periods = ('90_open_300_pct_increase', '30_open_1000_pct_increase')
target_attributes = ("total_supply", "start_date_utc","uts","24h_volume_usd","derivative_token", "market_cap_usd")
log_scale_columns = ("total_supply")

pct_increases, time_windows = (-.1, -.25, -.5,-.75, -.9, -.95, 10.0, 20.0, 50.0, 75.0, 100, 200, 300,500,1000, 2500, 5000), (30,90, 180)
pct_increases, time_windows = (-10.0, -20.0), (30,)

increase_periods = pcp.create_periods(pct_increases,time_windows)
pct_increases, time_windows = (0.1, 20.0, 50.0, 75.0, 100, 200, 300,500,1000, 2500, 5000), (30,90, 180)

stats_df = pcp.create_stats_df(df, target_attributes, pct_increases, time_windows)
summary_df = pcp.create_average_stats(stats_df)


dr.generate_coin_price_exploratory_report(df,run_server=True,)


period = '90_open_1000_pct_increase'
min_period = 15
sampling_df = df[df[period] > min_period].loc[:,("coin_symbol","24h_volume_usd")]

def monte_carlo_simple_multiples(self, initialization_data=None, test_multiples=None, periods_to_cycle=None,
                                 period_start=None, period_end=None):
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
    total_days_in_period = (end - start) / _DAY_
    if start < self.returns_df['uts'].iloc[0]:
        raise ValueError("Start date specified is before start date of %s in stored records" % du.convert_epoch(
            self.returns_df['date'].iloc[0]))
    periods_to_cycle = periods_to_cycle[periods_to_cycle <= (end - start) / _DAY_]
    periods_to_test = []
    for period in periods_to_cycle:
        for i in range(0, int(total_days_in_period / period)):
            periods_to_test.append((start + _DAY_ * period * i, start + _DAY_ * period * (i + 1)))
    for multiple in test_multiples:
        for period in periods_to_test:
            gain = self.test_strategy(simple_multiple=multiple, period=period)
            report_container.append((gain, multiple, (period[1] - period[0]) / _DAY_,
                                     du.convert_epoch(float(start), e2str=True),
                                     du.convert_epoch(float(end), e2str=True)))
    return report_container

