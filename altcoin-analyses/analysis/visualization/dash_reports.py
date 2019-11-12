external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
import dash
import dash_core_components as dcc
import dash_html_components as html
import Learning.dash_utils as dau
import pandas as pd
import analysis.pre_processing.price_pre_processing as pcp

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}

def set_colors(new_colors, update=False):
    if not isinstance(new_colors, dict):
        raise ValueError("Colors must be specified as a dict")
    global colors
    if update == True:
        colors.update(new_colors)
    else:
        colors = new_colors

def start_server():
    app.run_server(debug=True)

def generate_coin_price_exploratory_report(run_server=True,  check_cache=True, df=None, **kwargs):
    if all([True if item in kwargs else False for item in ("stats_df","freq_df","summary_df")]):
        freq_df = kwargs['freq_df']
        stats_df = kwargs['stats_df']
        summary_df = kwargs['stats_df']
    else:
        freq_df, stats_df, summary_df = pcp.get_coin_price_data(df=df)

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
        dcc.Graph(
            id='example-graph',
            figure={
                'data': [
                    {'x': [1, 2, 3], 'y': [4, 1, 2], 'type': 'bar', 'name': 'SF'},
                    {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': u'Montreal'},
                ],
                'layout': {
                    'title': 'Dash Data Visualization'
                }
            }
        )
        , html.H4(children='US Agriculture Exports (2011)'), dau.generate_table_from_df()])
    if run_server == True:
        start_server()

