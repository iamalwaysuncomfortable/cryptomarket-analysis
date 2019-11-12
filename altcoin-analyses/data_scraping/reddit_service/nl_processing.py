 import pandas as pd
import nltk
import log_service.logger_factory as lf
import string
import numpy as np
import data_scraping.scrapingutils as su
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import SnowballStemmer

snow = SnowballStemmer("english")
stop_words = set(stopwords.words("english"))
logging = lf.get_loggly_logger(__name__)


def get_canonical_coinlist(start = 0, result_limit=0):
    api_call = "https://api.coinmarketcap.com/v1/ticker/?start=" + str(start) + "&limit=" + str(result_limit)
    data = su.get_coinlist(api_call)
    data['normalized_title'] = data.name.map(lambda x: x if type(x) != unicode else (x.lower()).replace(' ', ''))
    return data['normalized_title'].tolist()

coin_list = get_canonical_coinlist()
title_list = ["r/" + item for item in coin_list]

def generate_training_features(dataframe, feature_limit=None, description_col="sr_desc", title_col="sr_name",
                               coinname_col = "normalized_title", summary_col="sr_summary",
                               class_column = "relevant"):
    df = dataframe
    worderator = __tokenize_batch_subreddit_results__(df, description_col, summary_col, title_col, coinname_col,
                                                      class_column=class_column)
    wf_dist = nltk.FreqDist([])
    for document in worderator:
        wf_dist += nltk.FreqDist(document)
    if isinstance(feature_limit, int):
         features = [result[0] for result in wf_dist.most_common(feature_limit)]
    else:
         features = [result[0] for result in wf_dist.most_common()]
    return features

def prepare_batch_data_for_nltk_NB_model(dataframe, word_features, description_col="sr_desc",title_col="sr_name",
                                         coinname_col = "normalized_title",summary_col="sr_summary",
                                         class_column = "relevant",includer_classifier=False, place_results_in_df=False,
                                         results_column="NB_Format"):
    df = dataframe
    if place_results_in_df == True:
        df[results_column] = None
    NB_Inputs = []
    worderator = __tokenize_batch_subreddit_results__(df, description_col, summary_col, title_col, coinname_col,
                                                      include_classifier=includer_classifier, class_column=class_column)

    if isinstance(word_features, (list, set, tuple)):
        index = 0
        for doc in worderator:
            if includer_classifier == True:
                doc_nb = find_features_in_doc(doc[0], word_features, doc[1])
            else:
                doc_nb = find_features_in_doc(doc, word_features)
            if place_results_in_df == True:
                df.at[index, results_column] = doc_nb
            NB_Inputs.append(doc_nb)
    return NB_Inputs

def __tokenize_batch_subreddit_results__(dataframe, description_col="sr_desc", summary_col="sr_summary",
                                         title_col = "sr_name", coinname_col="normalized_title",
                                         class_column="relevant", include_classifier=False, include_coin_name=False):

    df = dataframe
    df = cast_columns_to_type(df, unicode, description_col, coinname_col, summary_col)

    for index, row in df.iterrows():
        words = tokenize_one_subreddit_result(row[title_col], (row[summary_col], "_sum"), (row[description_col],))
        if row[coinname_col] in row[title_col]:
            words.append("coinname_title")
        elif row[title_col] in title_list:
            words.append("foreigntoken_title")

        result = [words]
        if include_classifier == True:
            result.append(row[class_column])
        if include_coin_name == True:
            result.append(row[title_col])
        if len(result) == 1:
            result = words

        yield result

def tokenize_one_subreddit_result(coin_title, *args):
    """
    Args:
        coin_title: Normalized title (lowercase + no newspace) ex: "litecoin", "bitcoin", "thirdparty", etc.
        args: Tuple with text to tokenize + any part label ex ("I can doge?", "_title") OR ("I can doge?",)
        if the text should include no part label, only include the text
    
    Returns:
        Python list of parsed words
    """
    words = []
    for arg in args:
        if len(arg) == 1:
            words += prep_words(arg[0], coin_title)
        if len(arg) == 2:
            words += prep_words(arg[0],coin_title, part_tag=arg[1])
    return words

def find_features_in_doc(document, features, category=None):
    doc_words = set(document)
    feature_table = {}
    for f in features:
        feature_table[f] = (f in doc_words)
    if category is None:
        return feature_table
    else:
        return (feature_table, category)

def normalize_coin_title(coin_name, word_list,coin_name_normalized="coinname",part_tag=None):
    part_suffix = part_tag
    if part_suffix == None: part_suffix = ""
    return [word if word != coin_name else coin_name_normalized+part_suffix for word in word_list]

def normalize_foreign_coin_titles(coin_name, word_list, coin_list, part_tag=None):
    part_suffix = part_tag
    if part_suffix == None: part_suffix = ""
    words = [word if (not word in coin_list or word == coin_name) else "foreigntoken" for word in word_list]
    return words

def prep_words(text, coin_title, part_tag = None):
    t = text
    t = remove_punctuation(t)
    words = word_tokenize(t)
    words = normalize_foreign_coin_titles(coin_title, words, coin_list, part_tag)
    words = normalize_coin_title(coin_title, words, part_tag)
    words = [word for word in words if (not word in stop_words and word != 'nan')]
    words = [snow.stem(word) for word in words]
    if part_tag:
        words = [word + part_tag for word in words]
    return words

def cast_columns_to_type(data, datatype, *args):
    df = data
    for arg in args:
        if isinstance(arg, (str, unicode)):
            data[arg] = data[arg].values.astype(datatype)
        else:
            raise TypeError("column names passed must be in str or unicode format")
    return df

def remove_punctuation(text, punctuation=string.punctuation):
    return "".join([char for char in text if char not in punctuation])

def generate_csv_style(coin_list):
    style_template = []
    df = coin_list[coin_list['sr_data'].str.len() > 0]
    for index, row in df.iterrows():
        for item in row['sr_data']:
            style_template.append({"normalized_title":row["normalized_title"], "name":row["name"], "rank":row["rank"],
                           "symbol":row["symbol"], "sr_name":item[0], "sr_summary":item[1], "sr_desc":item[2]})
    new_df = pd.DataFrame(style_template)
    return new_df

def train_test_split(dataframe, train=.6, test=.2, validate=.2):
    df = dataframe
    return np.split(df.sample(frac=1), [int(train * len(df)), int((train + test) * len(df))])

###Data generation functions
def generate_test_data(coin_list, first_results = 240, sample_size=120):
    coin_list['rank'] = coin_list.index + 1
    df = coin_list[:first_results]
    df = df[coin_list.sr_data.notnull() & coin_list['sr_data'].str.len() > 0]
    df = df.sample(sample_size)
    df['normalized_title'] = df.name.map(lambda x: x if type(x) != unicode else (x.lower()).replace(' ', ''))
    training_data = generate_csv_style(df)
    training_data.to_csv("reddit_trainining_data", encoding='utf-8')
