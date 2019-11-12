external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
import dash_html_components as html
import custom_utils.type_validation as tv
from plotly import tools
import pandas as pd
import numpy as np
import plotly.graph_objs as go
import analysis.pre_processing.price_pre_processing as pcp

def generate_table_from_df(dataframe, max_rows=10, title=None):
    if isinstance(title, (unicode, str)):
        table = [html.Tr([title])] + [html.Tr([html.Th(col) for col in dataframe.columns])] + [html.Tr([
        html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
    ]) for i in range(min(len(dataframe), max_rows))]
    else:
        table = [html.Tr([html.Th(col) for col in dataframe.columns])] + [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]

    return html.Table(table)

def create_multi_plot_historgram_from_df(df, label_columns,data_columns, filter=None, log_scale_columns=None, **kwargs):
    cols, rows = len(data_columns) + 1, len(label_columns)
    fig = tools.make_subplots(cols=cols, rows=rows)
    row = 1
    for label in label_columns:
        seriescount = [row, []]
        sampling_df_columns = tuple(data_columns) + (label,)
        if isinstance(filter, str):
            sampling_df = eval(filter)
            sampling_df = sampling_df.loc[:,sampling_df_columns]
        else:
            sampling_df = df.loc[:, sampling_df_columns]
        if len(sampling_df) == 0:
            continue
        col = 1
        for c in sampling_df_columns:
            series = sampling_df[c][~pd.isnull(sampling_df[c])]
            if isinstance(log_scale_columns, (list, tuple)) and c in log_scale_columns:
                series = np.log10(series.replace(0,1))
                c = c + " (Log10)"
            if len(series) > 0:
                step = (max(series) - min(series)) / 20
                start = max((min(series) - step, 0))
                end = max(series) + step
                fig.append_trace(go.Histogram(x=series, name=c, xbins={'start':start, 'end':end, 'size':step}), row, col)
            else:
                fig.append_trace(go.Histogram(x=series, name=c), row,col)
            seriescount[1].append((c, len(series)))
            if col == 1 and row != rows:
                fig['layout']['yaxis' + str((row-1)*cols + 1)].update(title=str(label[:-6].replace("_"," ")))
            if col == cols:
                fig["layout"]['xaxis' + str(cols*(row - 1) + col)].update(title="run length")
            else:
                fig["layout"]['xaxis' + str(cols*(row - 1) + col)].update(title=str(c))
            col += 1
        row += 1
    return fig

def create_stats_df_scatterplot(stats_df, features,stats, log_features = None,**kwargs):
    if not tv.validate_list_element_types(log_features,(str, unicode)) and log_features != None:
        raise ValueError("If log_stats is specified, it must be a tuple or list specifying the desired statistics to apply a base10 log to")
    cols, rows = len(stats), len(features)
    fig = tools.make_subplots(cols=cols, rows=rows)
    row = 1
    for feature in features:
        col = 1
        for stat in stats:
            for period in stats_df['period'].unique():
                sampling_df = stats_df[(stats_df['feature'] == feature) & (stats_df['period'] == period) & (~np.isnan(stats_df[stat]))]
                if feature in log_features:
                    x = np.log10(sampling_df[stat])
                else:
                    x = sampling_df[stat]
                y = sampling_df['increase']
                fig.append_trace(go.Scatter(x=x, y=y, name=str(feature) + "_" + str(stat) + "_" +str(period)+"d",mode="markers"), row, col)
            fig['layout']['yaxis' + str((row) * cols)].update(title="Increase")
            fig["layout"]['xaxis' + str(cols * (row - 1) + col)].update(title=stat.replace("_"," ") + "(" + feature +  ")")
            col +=1
        row += 1
    return fig

def single_out_performing_coins(df, min_days, increase, window, desired_stats):
    if increase > 0:
        period = "open_"+ str(window) +"_"+ str(int(100*increase))+"_pct_increase"
    else:
        period = "open_" + str(window) + "_" + str(int(abs(100 * increase))) + "_pct_decrease"
    sampling_df = df[df[period] > min_days].loc[:,("coin_name",) + tuple(desired_stats)]
    return generate_table_from_df(sampling_df, title=str(100*increase) + " percent increase - " + str(window) + " day period")