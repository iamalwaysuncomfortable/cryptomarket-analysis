external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
import dash
import dash_core_components as dcc
import dash_html_components as html
import analysis.pre_processing.price_pre_processing as pcp
import analysis.visualization.dash_utils as dau
import log_service.logger_factory as lf
import pandas as pd
import redis
logging = lf.get_loggly_logger(__name__)
lf.launch_logging_service()

r = redis.Redis(
    host='127.0.0.1',
    port=6379)

df = pd.read_msgpack(r.get("df"))
print(df)
min_period = 10
pct_threshold = 0.9
measures = ('24h_volume_usd',
            'start_date_utc', 'total_supply')
pct_increases = (-0.9, -.75, -.5, -.25, -.1, 0.1, 0.25, 0.5, 1, 2, 3, 5, 10, 25)
time_windows, pct_study_periods, min_days = (30, 60, 90, 180), (30, 60, 90, 180), 10
performant_pcts = [pct for pct in pct_increases if pct > pct_threshold]
pct_window_pairs = []
for window in pct_study_periods:
    for pct in performant_pcts:
        pct_window_pairs.append((pct, window))
freq_df, stats_df, summary_df = pcp.get_coin_price_data(check_cache_first=True)


histogram_rows = [column for column in freq_df.columns if (('increase' in column) or ('decrease' in column))]
n_rows = len(histogram_rows)

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}

app.layout = html.Div(children=[
    html.H1(children='Hello Dash', style={
        'textAlign': 'center',
        'color': colors['text']
    }),

    html.Div(children='''
        Dash: A web application framework for Python.
    ''', style={
        'textAlign': 'center',
        'color': colors['text']
    }),

    html.H4(
        children='''Characteristics of Coins in Range'''),
    dcc.Graph(
        figure=dau.create_multi_plot_historgram_from_df(freq_df,histogram_rows,
                                                 measures,filter="df[df[label] > " + str(min_period) + "]",log_scale_columns=["total_supply"]),
        id='histos',style={"height": str(28*n_rows) +"vh"}),
    html.H4(children='Averaged Stats for Increase'),
    dau.generate_table_from_df(stats_df),
    html.H4(children='correlations'),
    dcc.Graph(
        figure=dau.create_stats_df_scatterplot(stats_df,('total_supply', '24h_volume_usd'),('75th_percentile','mean','median'),log_features=("total_supply",)),
        id = 'corr', style={"height": str(28*3) +"vh"}),
    html.H4(children='Leading Coin Stats')
    ] + [dau.single_out_performing_coins(df,min_days=min_days,increase=pair[0]
                                         , window=pair[1]
                                         , desired_stats=('total_supply', '24h_volume_usd', 'price_usd','start_date')) for pair in pct_window_pairs])

app.scripts.config.serve_locally=True

if __name__ == "__main__":
    app.run_server(debug=True)