import pandas as pd
import csv
import json
import pytz
import datetime
import time
import logging
from dateutil.relativedelta import relativedelta as rd
from type_validation import validate_type as vt
from type_validation import validate_list_element_types as vlet

BOLD = "\033[1m"
END = "'\033[0m"

##Printing methods
def boldprint(text):
    print(BOLD + text + END)

def select_elements_by_boolean(input_table, boolean_table):
    if vt(input_table, (tuple, list)) and vt(boolean_table, (tuple, list)) \
        and len(boolean_table) == len(input_table) and vlet(boolean_table, bool):
        return [i for (i, v) in zip(input_table, boolean_table) if v]


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

##Data Import/Export Methods
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

def many_dicts_tocsv(csv_file, keys, dict_data):
    with open(csv_file, 'wb') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(dict_data)

def dict_from_csv(csv_file, keys):
    with open('coors.csv', mode='r') as infile:
        reader = csv.reader(csv_file)
        with open('coors_new.csv', mode='w') as outfile:
            writer = csv.writer(outfile)
            mydict = {rows[0]: rows[1] for rows in reader}

def write_json(filename, data, remove_bad_chars=True, ascii=False):
    if remove_bad_chars == True:
        for char in (" ", ".", "/", ":"):
            filename = filename.replace(char, "-")
    with open(filename + '.json', 'w') as outfile:
        if ascii:
            json.dump(data, outfile, ensure_ascii=True)
        else:
            json.dump(data, outfile)
    return

def write_log(filename, data, file_extension=None):
    ext = ".log"
    if isinstance(file_extension, (str, unicode)): ext = file_extension
    with open(filename + ext, 'w') as outfile:
        if isinstance(data, dict):
            outfile.write(str(data))
        else:
            for item in data:
                outfile.write("%s\n" % item)
    return



#Time methods
def make_utc_aware(time):
    if not isinstance(time, datetime.date):
        raise TypeError("Incorrect input format, input must be in datetime.time format")
    if not time.tzname():
        aware_time = pytz.utc.localize(time)
    elif time.tzname() is not 'UTC':
        raise ValueError("Datetime must be UTC! Non UTC timezones are not allowed")
    else:
        aware_time = time
    return aware_time

def get_time(now=False, months=0, weeks=0, days=0, minutes=0 , seconds=0, utc_string=False,
             give_unicode=False, custom_format=None, input_time=None, add=False, tz_aware=True):
    if input_time:
        try:
            nowtime = convert_time_format(input_time, str2dt=True, custom_format=custom_format)
        except TypeError:
            raise TypeError("Incorrect time, input must be in str, unicode, or datetime.time format")
    else:
        if tz_aware == True:
            nowtime = datetime.datetime.now(pytz.utc)
        else:
            nowtime = datetime.datetime.now()
    if now == False:
        if add == True:
            nowtime = nowtime + rd(months=months) + rd(weeks=weeks) + rd(days=days) + rd(minutes=minutes) + rd(
                seconds=seconds)
        else:
            nowtime = nowtime - rd(months=months) - rd(weeks=weeks) - rd(days=days) - rd(minutes=minutes) - rd(
                seconds=seconds)
    if utc_string and give_unicode:
        if custom_format:
            return unicode(nowtime.strftime(custom_format))
        else:
            return unicode(nowtime.strftime("%Y-%m-%dT%H:%M:%SZ"))

    elif utc_string and not give_unicode:
        if custom_format:
            return nowtime.strftime(custom_format)
        else:
            return nowtime.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        return nowtime

def convert_time_format(time, str2dt=False, dt2str=False, custom_format=None):
    if (str2dt and dt2str) or (not str2dt and not dt2str):
        raise(RuntimeError("Only one option (string-->datetime or datetime-->string) allowed"))
    if not (isinstance(time, datetime.date) or isinstance(time, str) or isinstance(time, unicode)):
        raise(TypeError("Incorrect time, input must be in str, unicode, or datetime.time format"))
    if str2dt:
        if isinstance(time, datetime.date):
            return time
        if custom_format:
            return pytz.utc.localize(datetime.datetime.strptime(time, custom_format))
        else:
            return pytz.utc.localize(datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%SZ"))
    if dt2str:
        if isinstance(time, str) or isinstance(time, unicode):
            return time
        if custom_format:
            return time.strftime(custom_format)
        else:
            return time.strftime("%Y-%m-%dT%H:%M:%SZ")

def convert_epoch(time, e2dt=False, e2str=False, str2e=False, dt2e=False, custom_format=None):
    if sum((e2dt, e2str, str2e, dt2e)) != 1:
        logging.error("Exactly one conversion option must be selected, no more, no less")
        raise (RuntimeError("Exactly one conversion option must be selected"))
    if not isinstance(time, (datetime.date, str, unicode, int, float)):
        logging.error("Time must be in epoch, datetime, or string/unicode format")
        raise (RuntimeError)
    if e2dt and isinstance(time, (int, float)):
        return pytz.utc.localize(datetime.datetime.utcfromtimestamp(time))
    elif dt2e and isinstance(time, datetime.date):
        return (time - get_time(input_time='1970-01-01T00:00:00Z')).total_seconds()
    elif str2e and isinstance(time, (str, unicode)):
        dt = convert_time_format(time, str2dt=True, custom_format=custom_format)
        return (dt - get_time(input_time='1970-01-01T00:00:00Z')).total_seconds()
    elif e2str and isinstance(time, (int, float)):
        dt = pytz.utc.localize(datetime.datetime.utcfromtimestamp(time))
        return convert_time_format(dt, dt2str=True, custom_format=custom_format)

def dt_tostring(dt, give_unicode=False):
    if give_unicode:
        return unicode(dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
    else:
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def time_is_after(time_to_check, base_time=None):
    _time_to_check = convert_time_format(time_to_check, str2dt=True)
    if base_time == None:
        _base_time = get_time(now=True)
    else:
        _base_time = convert_time_format(base_time, str2dt=True)
    if _time_to_check > _base_time:
        return True
    else:
        return False

###Epoch Methods
def get_epoch(epoch=None, current=False, cast_int=False, cast_str=False, seconds=0, minutes=0,
              hours = 0, days=0, years=0, add=False):
    _epoch = epoch
    if isinstance(_epoch, (int, float)) and current==True:
        logging.warn("If current param is set to true, epoch input will be erased in favor of current epoch")
    elif not isinstance(_epoch, (int, float)) and current==False:
        error_msg = "An epoch must be supplied or current param must be set to True"
        logging.error(error_msg, exc_info=True)
        raise ValueError(error_msg)

    if isinstance(_epoch, (int, float)):
        extra_seconds = seconds + minutes * 60 + hours * 3600 + days * 86400 + years * 86400 * 365
        if add==True:
            _epoch += extra_seconds
        if add==False:
            _epoch -= extra_seconds
        if _epoch < 0:
            _epoch = 0

    if current==True:
        _epoch = time.time()

    if cast_int is False and cast_str is False:
        return _epoch
    if cast_int is True and cast_str is False:
        return int(_epoch)
    if cast_int is False and cast_str is True:
        return str(_epoch)
    if cast_int is True and cast_str is True:
        return str(int(_epoch))



def validate_epoch(date, default_date=None):
    def raise_epoch_error():
        error_msg = "Start date must be a numerical types or a string castable to float," \
                    " date was %s, default_date was %s" %(str(date), str(default_date))
        logging.error(error_msg, exc_info=True)
        raise ValueError(error_msg)
    if isinstance(date, (int, float, str)):
        try:
            epoch = float(date)
            date = get_epoch(epoch, cast_str=False, cast_int=False)
            return date
        except:
            raise_epoch_error()
    else:
        if isinstance(default_date, (int, float)):
            return get_epoch(default_date, cast_str=False, cast_int=True)
        else:
            raise_epoch_error()

#Misc Methods
def validate_number(number, default_number):
    if isinstance(number, (int, float)):
        return number
    elif isinstance(default_number, (int, float)):
        return default_number
    else:
        raise ValueError("No numbers input for number or default number")

def any_num(*args):
    if not all(isinstance(arg, (int, float)) or arg==None for arg in args):
        error_msg = "At least one passed is not a number or None type, args were %s" %(str(args))
        logging.error(error_msg, exc_info=True)
        raise ValueError(error_msg)
    elif any(isinstance(arg,(int, float)) for arg in args):
        return True
    else:
        return False


def import_json_file(filename, encoding="utf-8"):
    with open(filename) as file:
        data = json.load(file, encoding=encoding)
    return data

def striplines(file):
    'strip lines'
    f = open(file, 'r')
    lines = f.readlines()
    result = ' '.join([line.strip() for line in lines])
    return result
