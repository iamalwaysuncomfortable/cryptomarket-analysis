import analysis.pre_processing.price_pre_processing as pcp
import pandas as pd
import redis

custom_measures = ("7d_med_btc", "7d_med_usd", "7d_med_eur", "7d_med_cny",
                    "ewma", "ewma_std")

r = redis.Redis(
    host='127.0.0.1',
    port=6379)

df, df_ix, intervals = pcp.create_measures(days_from_present=31
                                           , custom_measures=custom_measures
                                           , custom_intervals=(30,)
                                           , pct_increases=(-.2, -.1)
                                           , pct_study_periods=(30,)
                                           , measure_percent_increases=True
                                           , add_coin_attributes=True, alpha=0.8)

r.set("df",df.to_msgpack(compress='zlib'))

measures = ('derivative_token', 'market_cap_usd'
                      , '24h_volume_usd', 'start_date_utc',
                      'total_supply')

freq_df, stats_df, summary_df = pcp.get_coin_price_data(check_cache_first=True, pct_increases=(-.2, -.1),time_windows=(30,),pct_study_periods=(30,), measures=measures, min_days=10)




import pandas as pd
import redis

r = redis.Redis(
    host='127.0.0.1',
    port=6379)

df = pd.read_msgpack(r.get("df"))

import analysis.visualization.mpl_methods as mpl

pct_increases, time_windows, pct_study_periods, min_days = (10.0, 20.0, 50.0, 75.0, 100, 200, 300, 500, 1000, 2500,
                                                            5000), (30, 60, 90, 180), (30, 60, 90, 180), 10
increase_periods = pcp.create_periods(pct_increases, pct_study_periods)
mpl.plot_price_increase_histograms(df,increase_periods,measures,log_scale_columns=["toal_supply"],min_period=10 )