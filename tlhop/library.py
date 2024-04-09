#!/usr/bin/env python3
# -*- coding: utf-8 -*-
                                                                            
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import re
from functools import reduce
import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import numpy as np
import os
import textdistance
import nltk
import ulid
#nltk.download('stopwords')
import plotly.io as pio
import plotly.graph_objects as go
import plotly.express as px
from bs4 import BeautifulSoup


import pyspark.sql.functions as F
from pyspark.sql.types import StringType, ArrayType, StructType,IntegerType,BooleanType

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

from tlhop.schemas import Schemas


_library_path = os.path.dirname(os.path.abspath(__file__))



@F.pandas_udf(returnType=StringType())
def normalize_string(s: pd.Series) -> pd.Series:
    """
    Function to string normalization (remove accents)
    """
    return s.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
# normalize_string = pandas_udf(normalize_string, StringType())


UTF8_MAPPING_FILE = _library_path +"/tlhop_internal_datasets/utf-codes/mapping-utf8-characters.csv"
utf8_mappings = pd.read_csv(UTF8_MAPPING_FILE, sep=";", quotechar='"')\
    .set_index('Actual')\
    .T\
    .to_dict('records')[0]

HTTP_STATUS_FILE = _library_path +"/tlhop_internal_datasets/http-codes/http-response-status.csv"
http_status = pd.read_csv(HTTP_STATUS_FILE, sep=";", dtype="str")\
        .set_index("code")["name"]\
        .to_dict()

ACCENTED_WORDS_PT_BR_FILE = _library_path + "/tlhop_internal_datasets/brazil-dictionaries/accented_words_ptbr.json"
accented_dict = pd.read_json(ACCENTED_WORDS_PT_BR_FILE, lines=True, orient="records").set_index("length")


@F.udf(returnType=StringType())
def cleaning_text_udf(s, cleaning_org=False):
    """
    Method for standardizing texts in general (orgs names, html titles, etc.)
    
        - Replace html/unicode/hexa codes with their Latin equivalents;
        - Convert all letters to lowercase;
        - Replace letters with accents and the 'ç' with their ASCII equivalents;
        - Remove all symbols;
        - Remove business terms like ('s/a', 's.a', 'sa', 'ltda', 'ltd', 'inc', 'me', etc);
        
    :param str s: Text to be edited
    :param boolean cleaning_org: True to remove business terms (default, False)

    :returns: Text standardized

    :rtype: str
    """
    
    if s:
        repl = str.maketrans(
            "áéúíóçãõôêâªº",
            "aeuiocaooeaao"
        )
        exclude = [" s a ", " sa "," ltda", " ltd ", " lt ", " inc ", 
                   " me ", " eireli ", " eirelli ", " eirel ", " epp ", 
                   " llc "]

        s = check_unicode_symbols(s.lower())
        s = s.translate(repl) + " "
        s = re.sub('[^A-Za-z0-9]+', ' ', s)
        if cleaning_org:
            s = reduce(lambda a, key: a.replace(key, " "), exclude, s)
        s = re.sub(' +', ' ', s).strip()

    return s

##############

def check_unicode_symbols(text):
    """
    This code search for all unicode symbols in text and replace it, adjusting
    the text that may be corrupted.

    :param str text: Text to be edited

    :returns: Text without unicode symbols

    :rtype: str
    """
    if "ï¿½" in text:
        text = text.replace("ï¿½", "�")

    if "�" in text:
        words = re.findall(r"\w*�+[�\w]*", text)
        for word in words:
            word_ajusted = word.replace("�", "?")
            len_word = len(word_ajusted)
            if len_word in accented_dict.index:
                for candidate in accented_dict.loc[len_word]['words']:
                    found = True
                    for p1, p2 in zip(word_ajusted, candidate):
                        if p1 != p2 and p1 != "?":
                            found = False
                            break
                    if found:
                        text = text.replace(word, candidate)
                        break
    
    for pattern in utf8_mappings:
            text = re.sub(pattern, utf8_mappings.get(pattern), text)
    
    return text

##############


@F.udf(returnType=StringType())
def get_keys_udf(field):
    """
    Method to read a column that contains a struct/json
    and return a list of its field names.
    
    For example, the column `vulns` has a 
    json structure, where each key is the name of a CVE.

    :param field: A column field that represents a struct/json/dict

    :returns: List of Keys contained in the struct/json/dict

    :rtype: list
    """
    if field:
        field = json.loads(field)
        return list(field.keys())
    else:
        return None

##############

def get_fields_udf(col_name, fields_list):
    """
    An UDF method to read a column that contains a struct/dict/json
    and return a list with the values of the selected fields.

    :param 1° - column: A column name that represents a struct/json/dict
    :param 2° - fields: A list of subcolumns to be extracted
    :returns: List with values of selected keys

    :rtype: list
    """
    @F.udf(returnType=ArrayType(StringType()))
    def _get_fields(column, fields): 
        json_content = json.loads(column)
        filtered_fields = [json_content.get(f, None) for f in fields]
        result = [r if not isinstance(r, dict) 
                    else json.dumps(r) 
                  for r in filtered_fields]
        return result
    return _get_fields(col_name, F.array([F.lit(f) for f in fields_list]))

##############

def get_frequency_orgs(list_orgs, dataset):
    """Calculates the representativeness of a set of organizations.
    Representativeness = (number of banners from organizations)/(total number of banners of the sample)
    
    :parameter 1° - List: Organization name list
    :parameter 2° - Dataset: Dataset with organizations
    
    :returns: Organizations representativeness
    :rtype: float
    """
    n_records_filtered = dataset.withColumn("org_clean", cleaning_org_udf(F.col('org'))).filter(F.col("org_clean").isin(list_orgs)).count()
    n_records_total = dataset.count()
    representatividade = (n_records_equals/n_records_total) * 100
    return representatividade
##############

def clean_pandas_temporal_plot(df, x, y, pivot):
    """
        Method to clean and transform the dataset appling pivot method in it.
        Futhermore, construct a matrix to represent a column value where X
        represents the rows and Y represents the columns, and the cell values
        (x,y) are the pivot values. Pivot tables rearrange data and perform statistics
        on them which can help us find meaningful insights.

        :param 1° - DataFrame: DataFrame
        :param 2° - x: x axis
        :param 3° - y: y axis
        :param 4° - pivot: reshape value

        :returns: cleaned DataFrame
        :rtype: DataFrame
    """
    df_clean = df.copy()
    df_clean.columns = [col.replace(" ", "_") for col in df_clean.columns]
    
    df_clean = df_clean.pivot(index=x, columns=y, values=pivot)
    df_clean.columns = ["_".join(pair) for pair in df_clean.columns]
    
    df_clean = df_clean.reset_index(level=0)
    max_x = np.max(df_clean[x])
    tmp = pd.DataFrame(range(1, max_x + 1), columns=[x])
    df_clean = df_clean.merge(tmp, on=[x], how='right')
    return df_clean

##############

def gen_grafico_temporal(df, x, y_cols, std_cols=None, labels=None,
                        logY=True, xlabel="", ylabel="", useLegend=True):
    """
    Method for creating a time series chart for a given
    given set of columns.

    :param 1°- df: dataframe pandas;
    :param 2° - x: column name for the x-axis;
    :param 3° - y_cols: list of columns for the y-axis;
    :param 4°- std_cols: list with the columns for the standard deviation values.
        standard deviation. It is necessary to inform the same order as in
        y_cols;
    :param 5°- labels: list with the labels. If None then it will not display
        legend;
    :param 6° - logY: True (default) for the Y axis to be in log scale;
    :param 7° - xlabel: Text for the X axis;
    :param 8° - ylabel: Text for the Y axis;

    :returns: plot
    :rtype: graph
    """
    fig, ax = plt.subplots()
    fig.set_size_inches(18.5, 10.5)

    if not labels:
        labels = y_cols
         

    for i, col in enumerate(y_cols):
        
        first_id = (df[col].values > 0).argmax()
        if std_cols:
            err_col = std_cols[i]
            tmp = df[[x, col, err_col]][first_id:]
            ax.plot(tmp[x], tmp[col], '--o', label=labels[i])
            ax.fill_between(tmp[x], tmp[col]-tmp[err_col], tmp[col]+tmp[err_col], alpha=0.2)
        else:
            tmp = df[[x, col]][first_id:]
            ax.plot(tmp[x], tmp[col], '--o', label=labels[i])
    
    if logY:
        ax.set_yscale('log')
    else:
        plt.ylim(bottom=0)
        
    weeks = df[x].to_numpy()
    ax.set_xticks(weeks)
    months = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
    labels = [months[i//4] if i % 4 == 0 else '' for i, l in enumerate(weeks)]
    ax.set_xticklabels(labels)

    if labels:
        ax.legend(fontsize="xx-large")

    ax.set_xlabel(xlabel, fontsize=18)
    ax.set_ylabel(ylabel, fontsize=18)
    return ax




def gen_single_temporal_plot(df, x, y, xlabel, ylabel, legend=False):
    fig, ax = plt.subplots(figsize=(12,6))
    ax.plot(df[x], df[y])
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)

    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%Y'))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=14))
    plt.gcf().autofmt_xdate()
    
    if legend:
        ax.legend(fontsize="large")
        ax.legend(y,loc='upper left')
    #plt.tight_layout()
    return ax

##############

def check_nulls(df, nan_as_null=False):
    """
    Method to count how many NULL elements are present in each column of an DataFrame. 

    :param 1° - df: Spark's DataFrame
    :param 2° - nan_as_null: Flag to consider NaN as NULL values (default, False)

    :returns: DataFrame that shows how many NULL and NAN values exists in each column
    :rtype: DataFrame
    """
    
    def count_not_null(column, nan_as_null=False):
        pred = F.col(column).isNotNull() & (~F.isnan(column) if nan_as_null else F.lit(True))
        return F.sum(pred.cast("integer")).alias(column)

    return df.agg(*[count_not_null(c, nan_as_null) for c in df.columns])


##############

def gen_cdf_pdf(pd_df, col, ascending=False):
    """
    Method for generating PDF and CDF from a sample data in Pandas.

    :param 1° - pd_df: Pandas DataFrame
    :param 2° - col: column
    :param 3° - ascending: Flag to decide if asceding or descending sort default(False)

    :returns: Pandas DataFrame with column cdf and pdf
    :rtype: DataFrame
    """
    pd_df = pd_df.sort_values(col, ascending=ascending)
    pd_df['pdf'] = pd_df[col] / pd_df[col].sum()
    pd_df['cdf'] = pd_df['pdf'].cumsum()
    pd_df = pd_df.reset_index(drop=True)
    return pd_df

##############


def gantt_chart_ips(pd_df, min_var, max_var):

    """
    Method that plot a gantt chart(commonly used in project management,
    is one of the most popular and useful ways of showing activities (tasks or events)
    displayed against time.) for ips given a DataFrame.

    :param 1° - pd_df: Pandas DataFrame
    :param 2° - min_var: Beggining of counting
    :param 3° - max_var: End of couting to create range

    :returns: None
    :rtype: None
    """

    proj_start = pd_df[min_var].min()
    # number of days from project start to task start
    pd_df['start_num'] = (pd_df[min_var]-proj_start)#.dt.days
    # number of days from project start to end of tasks
    pd_df['end_num'] = (pd_df[max_var]-proj_start)#.dt.days
    # days between start and end of each task
    pd_df['days_start_to_end'] = pd_df.end_num - pd_df.start_num

    fig, ax = plt.subplots(1, figsize=(10, 20))

    ax.barh(pd_df.ip_str, pd_df.days_start_to_end, left=pd_df.start_num)
    ##### TICKS #####
    xticks = np.arange(0, pd_df.end_num.max()+1, 10)
    xticks_labels = pd.date_range(proj_start, end=pd_df[max_var].max()).strftime("%d/%m")
    ax.set_xticks(xticks)
    ax.set_xticklabels(xticks_labels[::10])
    plt.show()

##############   
    
def print_full(pandas_df):
    """
    Method to print a Pandas DataFrame with custom 
    setting in order to display in a larger area.

    :param 1° - pandas_df : A Pandas DataFrame
    :returns: None
    :rtype: None
    """
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 2000)
    pd.set_option('display.float_format', '{:20,.2f}'.format)
    pd.set_option('display.max_colwidth', None)
    display(pandas_df)
    pd.reset_option('display.max_rows')
    pd.reset_option('display.max_columns')
    pd.reset_option('display.width')
    pd.reset_option('display.float_format')
    pd.reset_option('display.max_colwidth')
    

iso_639_choices = [('ab', 'Abkhaz'), ('aa', 'Afar'), ('af', 'Afrikaans'), ('ak', 'Akan'), ('sq', 'Albanian'), 
                   ('am', 'Amharic'), ('ar', 'Arabic'), ('an', 'Aragonese'), ('hy', 'Armenian'), 
                   ('as', 'Assamese'), ('av', 'Avaric'), ('ae', 'Avestan'), ('ay', 'Aymara'), 
                   ('az', 'Azerbaijani'), ('bm', 'Bambara'), ('ba', 'Bashkir'), ('eu', 'Basque'), 
                   ('be', 'Belarusian'), ('bn', 'Bengali'), ('bh', 'Bihari'), ('bi', 'Bislama'), 
                   ('bs', 'Bosnian'), ('br', 'Breton'), ('bg', 'Bulgarian'), ('my', 'Burmese'), 
                   ('ca', 'Catalan; Valencian'), ('ch', 'Chamorro'), ('ce', 'Chechen'), 
                   ('ny', 'Chichewa; Chewa; Nyanja'), ('zh', 'Chinese'), ('cv', 'Chuvash'), 
                   ('kw', 'Cornish'), ('co', 'Corsican'), ('cr', 'Cree'), ('hr', 'Croatian'), 
                   ('cs', 'Czech'), ('da', 'Danish'), ('dv', 'Divehi; Maldivian;'), ('nl', 'Dutch'), 
                   ('dz', 'Dzongkha'), ('en', 'English'), ('eo', 'Esperanto'), ('et', 'Estonian'), 
                   ('ee', 'Ewe'), ('fo', 'Faroese'), ('fj', 'Fijian'), ('fi', 'Finnish'), ('fr', 'French'), 
                   ('ff', 'Fula'), ('gl', 'Galician'), ('ka', 'Georgian'), ('de', 'German'), 
                   ('el', 'Greek, Modern'), ('gn', 'Guaraní'), ('gu', 'Gujarati'), ('ht', 'Haitian'), 
                   ('ha', 'Hausa'), ('he', 'Hebrew (modern)'), ('hz', 'Herero'), ('hi', 'Hindi'), 
                   ('ho', 'Hiri Motu'), ('hu', 'Hungarian'), ('ia', 'Interlingua'), ('id', 'Indonesian'), 
                   ('ie', 'Interlingue'), ('ga', 'Irish'), ('ig', 'Igbo'), ('ik', 'Inupiaq'), ('io', 'Ido'),
                   ('is', 'Icelandic'), ('it', 'Italian'), ('iu', 'Inuktitut'), ('ja', 'Japanese'), 
                   ('jv', 'Javanese'), ('kl', 'Kalaallisut'), ('kn', 'Kannada'), ('kr', 'Kanuri'), 
                   ('ks', 'Kashmiri'), ('kk', 'Kazakh'), ('km', 'Khmer'), ('ki', 'Kikuyu, Gikuyu'), 
                   ('rw', 'Kinyarwanda'), ('ky', 'Kirghiz, Kyrgyz'), ('kv', 'Komi'), ('kg', 'Kongo'), 
                   ('ko', 'Korean'), ('ku', 'Kurdish'), ('kj', 'Kwanyama, Kuanyama'), ('la', 'Latin'), 
                   ('lb', 'Luxembourgish'), ('lg', 'Luganda'), ('li', 'Limburgish'), ('ln', 'Lingala'), 
                   ('lo', 'Lao'), ('lt', 'Lithuanian'), ('lu', 'Luba-Katanga'), ('lv', 'Latvian'), 
                   ('gv', 'Manx'), ('mk', 'Macedonian'), ('mg', 'Malagasy'), ('ms', 'Malay'), 
                   ('ml', 'Malayalam'), ('mt', 'Maltese'), ('mi', 'Māori'), ('mr', 'Marathi (Marāṭhī)'), 
                   ('mh', 'Marshallese'), ('mn', 'Mongolian'), ('na', 'Nauru'), ('nv', 'Navajo, Navaho'), 
                   ('nb', 'Norwegian Bokmål'), ('nd', 'North Ndebele'), ('ne', 'Nepali'), ('ng', 'Ndonga'), 
                   ('nn', 'Norwegian Nynorsk'), ('no', 'Norwegian'), ('ii', 'Nuosu'), ('nr', 'South Ndebele'), 
                   ('oc', 'Occitan'), ('oj', 'Ojibwe, Ojibwa'), ('cu', 'Old Church Slavonic'), ('om', 'Oromo'), 
                   ('or', 'Oriya'), ('os', 'Ossetian, Ossetic'), ('pa', 'Panjabi, Punjabi'), ('pi', 'Pāli'), 
                   ('fa', 'Persian'), ('pl', 'Polish'), ('ps', 'Pashto, Pushto'), ('pt', 'Portuguese'), 
                   ('qu', 'Quechua'), ('rm', 'Romansh'), ('rn', 'Kirundi'), ('ro', 'Romanian, Moldavan'), 
                   ('ru', 'Russian'), ('sa', 'Sanskrit (Saṁskṛta)'), ('sc', 'Sardinian'), ('sd', 'Sindhi'), 
                   ('se', 'Northern Sami'), ('sm', 'Samoan'), ('sg', 'Sango'), ('sr', 'Serbian'), 
                   ('gd', 'Scottish Gaelic'), ('sn', 'Shona'), ('si', 'Sinhala, Sinhalese'), ('sk', 'Slovak'), 
                   ('sl', 'Slovene'), ('so', 'Somali'), ('st', 'Southern Sotho'), ('es', 'Spanish; Castilian'), 
                   ('su', 'Sundanese'), ('sw', 'Swahili'), ('ss', 'Swati'), ('sv', 'Swedish'), 
                   ('ta', 'Tamil'), ('te', 'Telugu'), ('tg', 'Tajik'), ('th', 'Thai'), ('ti', 'Tigrinya'),
                   ('bo', 'Tibetan'), ('tk', 'Turkmen'), ('tl', 'Tagalog'), ('tn', 'Tswana'), ('to', 'Tonga'), 
                   ('tr', 'Turkish'), ('ts', 'Tsonga'), ('tt', 'Tatar'), ('tw', 'Twi'), ('ty', 'Tahitian'), 
                   ('ug', 'Uighur, Uyghur'), ('uk', 'Ukrainian'), ('ur', 'Urdu'), ('uz', 'Uzbek'), 
                   ('ve', 'Venda'), ('vi', 'Vietnamese'), ('vo', 'Volapük'), ('wa', 'Walloon'), ('cy', 'Welsh'), 
                   ('wo', 'Wolof'), ('fy', 'Western Frisian'), ('xh', 'Xhosa'), ('yi', 'Yiddish'), 
                   ('yo', 'Yoruba'), ('za', 'Zhuang, Chuang'), ('zu', 'Zulu')]

iso_639_choices = {v[0]:v[1] for v in iso_639_choices}


@F.udf(returnType=StringType())
def process_lang(s):
    """
        Method that process the text replacing special characters and
        "lang=" expression.

        :param 1° - s: Text that will be processed

        :returns: None if text is NULL or if its length is less than 2 or begin with "-"
        :rtype: String
    """
    if not s:
        return None

    s = s.lower()\
        .replace("lang=", "")\
        .replace("\"", "")\
        .replace("'", "")\
        .replace("\_", "-")

    s = re.sub("\s+", "", s).strip()

    if (len(s) < 2) or (s[0] == "-"):
        s = None
    else:
        s = s.split("-")[0]
        s = iso_639_choices.get(s, None)
    return s

########################

schema_html_code = StructType()\
    .add("preview-body", StringType(), True, None)\
    .add("code-length", IntegerType(), True, None)\
    .add("body-length", IntegerType(), True, None)\
    .add("keywords", StringType(), True, None)\
    .add("description", StringType(), True, None)

@F.udf(returnType=schema_html_code)
def parser_html_code_udf(raw):
    """
        Method that performs the processing of html information, looking for
        tags, meta, name, content, etc.

        :param 1° - raw: not processed HTML text

        :returns: array with parsed html
        :rtype: ArrayType(StringType)
    """
    if raw:
        try:
            page = BeautifulSoup(raw, 'html.parser')
        except:
            return None

        body = page.find_all("body")
        if len(body) > 0:
            body = body[0].get_text(" ", strip=True)
        else:
            body = ""

        code_size = re.sub("(\s+|\n+)", " ", raw)
        info = [body, len(code_size), len(body), "", ""]

        for tag in page.find_all("meta"):
            tag_key = tag.get("name", "")
            if "description" in tag_key:
                info[3] += " " + tag.get("content", "")
            elif "keywords" in tag_key:
                info[4] += " " + tag.get("content", "")

        if info[3] == "":
            info[3] = None
        else:
            info[3] = info[3].strip()

        if info[4] == "":
            info[4] = None
        else:
            info[4] = info[4].strip()

        return info

#########################

@F.udf(returnType=ArrayType(StringType()))
def get_unique_tokens(row):
    """
    Get unique tokens splitted by blanked spaces with length greater than 2.

    :param 1° - row: DataFrame row with tokens

    :returns: Empty List or List with set of tokens
    :rtype: ArrayType(StringType)
    """
    if row:
        return list(set([r for r in row.split(" ") if len(r) > 2]))
    else:
        return []
    
@F.udf(returnType=ArrayType(StringType()))
def get_sorted_tokens(row, min_size):
    """
        Get all tokens greater than min_size and spllited by blanked spaces.

        :param 1° - row: DataFrame row with tokens
        :param 2° - min_size: Min size for word length

        :returns: Empty List or List with tokens
        :rtype:ArrayType(StringType)
    """
    if row:
        return [r for r in row.split(" ") if len(r) >= min_size]
    else:
        return []
    
##########################

#stopwords_pt = nltk.corpus.stopwords.words('portuguese')
#stopwords_en = nltk.corpus.stopwords.words('english')

#stopwords = stopwords_pt + stopwords_en

##########################


def plot_bubble_map(cities_df, lat_col, lon_col, color_col=None, size=None, hover_name=None, 
                    hover_data=None, opacity=0.9, kwargs={}):
    
    '''
        Method that plots a bubble map(Bubble Maps are used to describe qualities associated with
        a specific item, person, idea or event.).

        :param cities_df: DataFrame with cities
        :param lat_col: column with cities latitude
        :param lon_col: column with cities longitude
        :param color_col: define column color, default(None)
        :param size: define the plot size, default(None), if keeps none than size = 3
        :param hover_name: name to display when hover, default(None)
        :param hover_data: data to display when hover, default(None)
        :param opacity: define graph opacity, default(0.9)
        :param kwargs: key args, default({})

        :returns: map_box object
        :rtype: scatter_mapbox
    '''
    cities_df[lat_col] = pd.to_numeric(cities_df[lat_col])
    cities_df[lon_col] = pd.to_numeric(cities_df[lon_col])
    
    if not size:
        size = 3.0  
    elif isinstance(size, int):
        size = float(size)
    
    if isinstance(size, str):
        cities_df["size_tmp_plot"] = (cities_df[size] - cities_df[size].min()) / (cities_df[size].max() - cities_df[size].min())   
        cities_df["size_tmp_plot"] = pd.cut(cities_df["size_tmp_plot"], 
                                            bins=[-np.inf, 0, .2, .33, 0.5,  0.67, 0.84, 1], 
                                            labels=False) * 5
        kwargs["size"] = cities_df["size_tmp_plot"]
    else:
        
        kwargs["size"] = [size for _ in range(len(cities_df))]
    
    if color_col:
        kwargs["color"] = color_col
    
    # bubble
    fig = px.scatter_mapbox(cities_df,
        lon = lon_col,
        lat = lat_col, zoom=3, 
        hover_name=hover_name, hover_data=hover_data, opacity=opacity, **kwargs)

    fig.update_layout(mapbox_style="open-street-map", mapbox_center_lon=-55, mapbox_center_lat=-14)
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

    fig.update_layout(
            showlegend = True,
            geo = dict(
                scope = 'south america',
                landcolor = 'rgb(217, 217, 217)'
            )
        )

    return fig



##########


def plot_heatmap_map(cities_df, lat_col, lon_col, z_col=None, hover_name=None, hover_data=None, 
                     radius=8, opacity=0.9, kwargs={}):
    """
        Method that plots a heatmap(By definition, Heat Maps are graphical representations of data that
        utilize color-coded systems.The primary purpose of Heat Maps is to better visualize the volume of
        locations/events within a dataset and assist in directing viewers towards areas on data visualizations
        that matter most.).

        :param 1° - cities_df: DataFrame with cities
        :param 2° - lat_col: column with cities latitude
        :param 3° - lon_col: column with cities longitude
        :param 4° - z_col:
        :param 5° - hover_name: name to display when hover, default(None)
        :param 6° - hover_data: data to display when hover, default(None)
        :param 7° - radius: the radius searches a set distance, rather than for a certain number of nearby points,
        default(8)
        :param 8° - opacity: define graph opacity, default(0.9)
        :param 9° - kwargs: key args, default({})


        :returns: map_box object
        :rtype: density_mapbox
    """
    cities_df[lat_col] = pd.to_numeric(cities_df[lat_col])
    cities_df[lon_col] = pd.to_numeric(cities_df[lon_col])
    
    if z_col:
        cities_df["size_tmp_plot"] = (
            (cities_df[z_col] - cities_df[z_col].min()) / 
            (cities_df[z_col].max() - cities_df[z_col].min())
        )
        cities_df["size_tmp_plot"] = pd.cut(cities_df["size_tmp_plot"], 
                                            bins=[-np.inf, 0, .2, .33, 0.5,  0.67, 0.84, 1], 
                                            labels=False) * 5
        kwargs["z"] = cities_df["size_tmp_plot"]
    
    #mapbox
    fig = px.density_mapbox(cities_df, lat=lat_col, lon=lon_col, zoom=3, 
                            hover_name=hover_name, hover_data=hover_data, opacity=opacity, radius=radius, **kwargs)
    
    fig.update_layout(mapbox_style="open-street-map", mapbox_center_lon=-55, mapbox_center_lat=-14)
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

    fig.update_layout(
            showlegend = False,
            geo = dict(
                scope = 'south america',
                landcolor = 'rgb(217, 217, 217)'
            )
        )
    return fig


################

uf_codes = {
    'AC': 'Acre', 'AL': 'Alagoas', 'AM': 'Amazonas', 'AP': 'Amapá', 
    'BA': 'Bahia', 'CE': 'Ceará', 'DF': 'Distrito Federal', 
    'ES': 'Espírito Santo', 'GO': 'Goiás', 'MA': 'Maranhão', 
    'MT': 'Mato Grosso', 'MS': 'Mato Grosso do Sul', 
    'MG': 'Minas Gerais', 'PA': 'Pará', 'PB': 'Paraíba', 
    'PR': 'Paraná', 'PE': 'Pernambuco', 'PI': 'Piauí', 
    'RJ': 'Rio de Janeiro', 'RN': 'Rio Grande do Norte', 
    'RS': 'Rio Grande do Sul', 'RO': 'Rondônia', 
    'RR': 'Roraima', 'SC': 'Santa Catarina', 'SP': 'São Paulo', 
    'SE': 'Sergipe', 'TO': 'Tocantins'
}

@F.udf(returnType=BooleanType())
def is_valid_uf_code(code):
    '''
        Method that receive a code and verify if it is in the federation units array.
        :param 1° - code: federation units code

        :returns: boolean
        :rtype: boolean
    '''
    if code:
        return code in uf_codes
    else:
         return False
        
        
        
################

cpe_schema = Schemas().get_external_schema_by_column("cpe", 'any')


@F.udf(returnType=cpe_schema)
def parser_cpe_udf(text):
    '''
        Method that receive a cpe object and parses it.
        :param 1° - text: cpe data

        :returns: cpe_info
        :rtype: Dictionary Array or Empty Array
    '''
    results = {}
    if text:
        if isinstance(text, str):
            text = text.split(",")

        for cpe in text:
            part = None
            
            if "cpe:2.3" in cpe:
                _, _, part, vendor, *cpe = cpe.split(":")
                if len(cpe) == 1:
                    product = cpe[0]
                    version = None
                    update = ""
                elif len(cpe) > 1:
                    product = cpe[0]
                    version = cpe[1]
                    update = cpe[2]
                else:
                    product = None
                    version = None
                    update = ""

            elif "cpe:/" in cpe:
                cpe = cpe.replace("cpe:/", "").split(":")
                part, vendor, product = cpe[0], cpe[1], cpe[2]
                if len(cpe) > 3:
                    version = cpe[3]
                    update = ""
                else:
                    version = None
                    update = ""
            
            if part:
                if "a" == part:
                    part = "Application"
                elif "h" == part:
                    part = "Hardware"
                elif "o" == part:
                    part = "Operating System"
                    
                key = str({"cpe_type": part, "cpe_vendor": vendor, "cpe_product": product})
                
                if key not in results:
                    results[key] = {"cpe_type": part, "cpe_vendor": vendor, "cpe_product": product, "cpe_versions": set()}

                if version:    
                    # if version == "-":
                    #     version = "*"
                    # if update == "-":
                    #     update = "*"
                    r = ":".join([version, update])
                    results[key]["cpe_versions"].add(r)
                    
        for e in results:
            results[e]["cpe_versions"] = sorted(list(results[e]["cpe_versions"]))
        results = [v for v in results.values()]

    return results



@F.udf
def gen_ulid(timestamp):
    return str(ulid.from_timestamp(timestamp))