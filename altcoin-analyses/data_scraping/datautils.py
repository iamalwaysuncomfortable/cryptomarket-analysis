import pandas as pd
import csv
import json
import pytz
import datetime
from dateutil.relativedelta import relativedelta as rd

##Data Cleaning Utility Methods
def pre_process_types(df, num_columns = None, txt_columns = None):
    processed_data = df
    if num_columns is not None:
        for column in num_columns:
            processed_data[column] = pd.to_numeric(df[column])
    if txt_columns is not None:
        for column in txt_columns:
            processed_data[column] = df[column].astype(str)
    return processed_data

def clean_outliers(df, max_price, max_supply):
    cleaned_data = df[(df.price_usd < max_price) & (df.total_supply < max_supply)]
    return cleaned_data

def filter_df(df, m_filters = {}, d_filters = {}, d_exclude=False):
    """ filters pandas dataframe for specific dimensions entries, and measure ranges  """
    data = df
    if bool(d_filters):
        for key, values in d_filters.iteritems():
            if key in df.columns:
                if d_exclude == False:
                    data = data[data[key].isin(values)]
                else:
                    data = data[-data[key].isin(values)]
    if bool(m_filters):
        for key, value in m_filters.iteritems():
            if key in df.columns:
                data = data[(data[key] >= value[0]) & (data[key] <= value[1])]
    return data

##Data Export Methods
def dict_tocsv(csv_file,csv_columns,dict_data):
    try:
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for key, value in dict_data.iteritems():
                writer.writerow({key:value})
    except IOError as (errno, strerror):
            print("I/O error({0}): {1}".format(errno, strerror))
    return

def write_json(filename, data):
    with open(filename + '.json', 'w') as outfile:
        json.dump(data, outfile)
    return

def write_log(filename, data):
    with open(filename + '.log', 'w') as outfile:
        for item in data:
            outfile.write("%s\n" % item)
    return

def get_time(now=False, months=0, weeks=0, days=0, minutes=0 , seconds=0, utc_string=False, give_unicode=False):
    nowtime = datetime.datetime.now(pytz.utc)
    if now == False:
        nowtime = nowtime - rd(months=months) - rd(weeks=weeks) - rd(days=days) - rd(minutes=minutes) - rd(seconds=seconds)
    if utc_string and give_unicode:
        return unicode(nowtime.strftime("%Y-%m-%dT%H:%M:%SZ"))
    elif utc_string and not give_unicode:
        return nowtime.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        return nowtime
