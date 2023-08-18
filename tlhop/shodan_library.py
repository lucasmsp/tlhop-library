#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import builtins
import pandas as pd

from IPython.display import HTML

from pyspark.sql.functions import *                                                                              
from pyspark.sql.types import *
from pyspark.sql.window import Window

from tlhop.library import *

from tlhop.schemas import Schemas

_library_path = os.path.dirname(os.path.abspath(__file__))

# This file maps all operators avaible in Shodan Abstraction. 
# Although these definitions could be done in the `shodan_abstraction.py`
# file, it was separated so that Sphinx could generate the HTML 
# documentation automatically.

#
# HTTP operators
#
def get_http_status(self, outname="http-code"):
    """
    Process `data` column to retrieve the HTTP response numeric code and convert it to message code pattern.

    :param outname: Output column name (default, 'http-code')
    :return: Spark DataFrame
    """

    result = (
        self.withColumn(outname, substring(regexp_extract(col("data"), r'HTTP.* \d{3} \w', 0),10,3))\
            .replace(http_status, subset=[outname])
            .withColumn(outname, when(col(outname) == lit(""), "EMPTY_RESPONSE_CODE").otherwise(col(outname)))
    )
    return result

def parser_http_col(self, outname="http", cols=None):
    """
    Parser `http` column (an original string field) into a struct field with the selected sub-field, 
    passed as parameters.

    :param outname: Output column name. The default, 'http', will replace the original column
    :param cols: Columns to include during transformation. By default (None), it will include: "host", 
     "html", "html_hash", 'title'
    :return: Spark DataFrame 
    """
    if not cols:
        cols = ["host", "html", "html_hash", 'title']
    schema = {"fields": [], "type": "struct"}
    for k in cols:
        schema["fields"].append({"metadata": {}, "name": k, "nullable": True, "type": "string"})
    struct_http = StructType.fromJson(schema)

    return self.withColumn(outname, from_json(col("http"), struct_http))


def parser_html_code(self, input_col="html", output_col="html_parsed"):
    """
    Parser the `html` code. The resultant column will be a struct containing 
    the following fields:
    - `preview-body`: All text present in the <body> part;
    - `code-length`: Size of the complete html code;
    - `body-length`: Text size of `preview-body`;
    - `keywords`: Text present in keyword metadata;
    - `description`: Text present in description metadata;

    :param input_col: Input column containing the `html` code
    :param output_col: Output column name (default, `html_parsed`)
    :return: Spark DataFrame 
    """

    return self.withColumn(output_col, parser_html_code_udf(col(input_col)))


def webpage_is_available(self, html_col="html", title_col="title_clean"):
    """
    Create a new column `webpage_stats` containing information if: both code 
    and html title are filled ('CODE_AND_TITLE'); only code ('ONLY_CODE');
    only html title ('ONLY_TITLE'); or none ('BOTH_EMPTY').

    :param html_col: Column with the html code (default, `html`)
    :param title_col: Column with the html title (default, `title_clean`)
    :return: Spark DataFrame 
    """

    result = self.withColumn("webpage_stats", 
                    when( (length("html") > 0) & (length("title_clean") > 0), lit("CODE_AND_TITLE"))\
                    .when(length("html") > 0, lit("ONLY_CODE"))\
                    .when(length("title_clean") > 0, lit("ONLY_TITLE"))\
                    .otherwise(lit("BOTH_EMPTY")))
    return result

def filter_valid_html_page(self, hash_col="html_hash", status_col="http-code"):
    """
    
    :param hash_col:
    :param status_col:
    :return: Spark DataFrame 
    """

    result = (self.filter((col(hash_col) != "0") & 
                          (col(hash_col).isNotNull()) &
                          (col(status_col) == "200_HTTP_OK")))
    return result


def get_html_lang(self, input_col="html", output_col="html_lang"):
    """
    Detect the site official language (infered by lang metadada)

    :param input_col: Input column name containing the html code (default, `html`)
    :param output_col: Output column name (default, `html_lang`)
    :return: Spark DataFrame 
    """

    return self.withColumn(output_col, 
                            process_lang(
                                regexp_extract(col(input_col), r' lang=["\']*[a-zA-Z\-\_]+', 0)))

#
#  Cleaning text operators
#
def cleaning_org(self, input_col="org", output_col=None):
    """
    Method to apply all cleaning and standardization operations to an organization name.

    :param input_col: Original column name. By default, is `org` but can be also used with `isp`
    :param output_col: Output column name. The default configuration will add a suffix `_clean` 
     on the input name
    :return: Spark DataFrame 
    """

    if not output_col:
        output_col = input_col + "_clean"

    return self.withColumn(output_col, cleaning_text_udf(col(input_col), lit(True)))


def cleaning_text(self, input_col, output_col=None):
    """
    Method to apply all cleaning operations to a text.

    :param input_col: Original column name
    :param output_col: Output column name. The default configuration will add a suffix `_clean` 
    :return: Spark DataFrame 
    """

    if not output_col:
        output_col = input_col + "_clean"

    return self.withColumn(output_col, cleaning_text_udf(col(input_col), lit(False)))


#
# ScreenShot operators
#
def filter_banners_with_screenshot(self):
    """
    Filtering Shodan records that contain screenshot in it data (within `opts` column).

    :return: Spark DataFrame 
    """

    return self.filter(col("opts").contains("screenshot"))


def extract_screenshot(self):
    """
    Convert the original screenshot content (inside the `opts` field) to two new columns:
    `screenshot_labels` (containing the label created by Shodan) and `screenshot_img` (a base64 
    representation of the image)

    :return: Spark DataFrame 
    """

    col_schema = Schemas().get_shodan_schema_by_column("screenshot", force_original=False)
    
    result = self.withColumn("screenshot_tmp", get_fields_udf("opts", ["screenshot"]).getItem(0))\
        .withColumn('screenshot_tmp', from_json(col('screenshot_tmp'), col_schema))\
        .withColumn("screenshot_labels", col("screenshot_tmp.labels"))\
        .withColumn("screenshot_img", concat_ws("",lit('<img src="data:'), 
                                                col('screenshot_tmp.mime'),
                                                lit(';base64,'),
                                                translate("screenshot_tmp.data", "\n", ""),
                                                lit('" >'),
                                                ))\
        .drop("screenshot_tmp")

    return result


def print_screenshot(self, size):
    """
    Print `size` records as a Pandas DataFrame containing images.

    :param size: Value that limits how many records will be 
     converted into Pandas (default, 1000)
    :return: A HMTL to be displayed
    """

    if size > 1000:
        size = 1000

    return HTML(self.limit(size).toPandas().to_html(escape=False))  


#
#  CVE and CPE operators
#
def contains_vulnerability(self, cves, mode="any", vulns_col="vulns_cve"):
    """
    Selects records with the vulnerabilities listed in `cve` parameters. 

    :param cves: A string (containing only one CVE code) or a list with multiple CVEs
    :param mode: 'any' (default) to filter records with at least one CVE or 'all' containing all CVEs
    :param vulns_col: Input column containing the list of vulnerabilities of a record (default, `vulns_cve`)

    :return: Spark DataFrame 
    """

    if isinstance(cves, str):
        cves = [cves]
    
    if dict(self.dtypes)[vulns_col] == "string":
        return self.filter(col(vulns_col).isin(cves))
    else:
        if mode == "all":
            return self.filter(size(arrays_intersect(col(vulns_col), array(*[lit(i) for i in cves]))) == len(cves))
        elif mode == "any":
            return self.filter(arrays_overlap(col(vulns_col), array(*[lit(i) for i in cves])))
        else:
            Exception("Only 'any' and 'all' modes are supported.")


# https://en.wikipedia.org/wiki/Common_Platform_Enumeration
def parser_cpe(self, input_col="cpe", output_col="cpe_parsed"):
    """
    Parser CPE column to extract "cpe_type", "cpe_vendor", "cpe_product" and "cpe_versions" informations.

    :param input_col: Input CPE field name (default, `cpe`)
    :param output_col: Output column name (default, `cpe_parsed`)
    :return: Spark DataFrame 
    """
    return self.withColumn(output_col, parser_cpe_udf(col(input_col)))


#
# Plot operators
#
def gen_cdf_pdf(self, input_col, to_pandas=True):
    """
    Generate a CDF/PDF that represents a frequency of a elements in a column.
    :param input_col: Target column to generate its frequency;
    :param to_pandas: True to return result as a Pandas DataFrame (default), 
     otherwise it keeps as Spark DataFrame;
    :return: Spark DataFrame 
    """
    
    cdf_col = input_col+"_cdf"
    pdf_col = input_col+"_pdf"

    # Pandas approach
    stats_df = self.filter(col(input_col).isNotNull())\
                  .groupby(input_col).count()\
                  .orderBy(desc("count"))\
                  .toPandas()
    
    stats_df.columns = [input_col, "frequency"]
    # PDF
    stats_df[pdf_col] = stats_df['frequency'] / builtins.sum(stats_df['frequency'])

    # CDF
    stats_df[cdf_col] = stats_df[pdf_col].cumsum()
    result = stats_df.reset_index()

    if not to_pandas:
        result = self.sparkSession.createDataFrame(result)
    
    # Spark approach: We do not use this approach, to avoid Warnings (although is safe)
    #w0 = Window.partitionBy()
    #w = Window.orderBy(desc("count")).rowsBetween(Window.unboundedPreceding, 0)
    # result = (self.filter(col(input_col).isNotNull())
    #               .groupby(input_col).count()
    #               .orderBy(desc("count"))
    #               .withColumn(pdf_col, col("count") / sum("count").over(w0))
    #               .withColumn(cdf_col, sum(pdf_col).over(w))
    #         )

    return result

def plot_bubble(self, lat_col, lon_col, color_col=None, size=3.0, hover_name=None, 
                    hover_data=None, opacity=0.9, max_rows=10000):
    """
    
    """
    if self.count() > max_rows:
        cities_df = self.limit(max_rows).toPandas()
    else:
        cities_df = self.toPandas()
    
    fig = plot_bubble_map(cities_df, lat_col, lon_col, color_col, 
                            size, hover_name, hover_data, opacity)
    return fig


def plot_heatmap(self, lat_col, lon_col, z_col=None, hover_name=None, hover_data=None, radius=8, opacity=0.9, max_rows=10000):
    """
    
    """
    if self.count() > max_rows:
        cities_df = self.limit(max_rows).toPandas()
    else:
        cities_df = self.toPandas()
    fig = plot_heatmap_map(cities_df, lat_col, lon_col, z_col, hover_name, hover_data, radius, opacity)
    return fig


#
# Miscellaneous Operators
#
def get_ip_mask(self, input_col="ip_str", output_col=None, level=3):
    """
    Convert a string IP into a subnet mask format.

    :param input_col: Input column contaning the string ip representation (defaul, `ip_str`)
    :param output_col: Output column name. By default it will in `mask_<level>` layout, where <level> is the
     value selected in `level` param;
    :param level: The number of octets to be extracted
    :return: Spark DataFrame 
    """

    if not output_col:
        output_col = "mask_{}".format(level)

    result = self.withColumn("octet_parts", split(input_col, "\."))\
            .withColumn(output_col, concat_ws(".", *[col("octet_parts").getItem(i) for i in range(level)]))\
            .drop("octet_parts")
    return result

def parser_complex_column(self, input_col, output_col=None):
    """
    Because of their information complexity, some columns saved in string type are actually a JSON information. 
    Saving in a format already structured in Spark (Struct) would make the final dataset schema very large,
    and would have a significantly higher storage cost. As not all columns are used in a single query, we 
    persist such columns in string and create an API containing the schema for easy structuring, when necessary.
    This method can be used to parser these type of columns during execution.
    
    :param input_col: Column name to be parsed;
    :param output_col: Output column name. By default, it will add a suffix "_parsed" in the original name;
    :return: Spark DataFrame 
    """
    if not output_col:
        output_col = f"{input_col}_parsed"
        
    col_schema = Schemas().get_shodan_schema_by_column(input_col, force_original=False)
    result = self.withColumn(output_col, from_json(input_col, col_schema))
    return result



def get_events(self, new_rules=None):
    """
    Method to analize all records and columns to map some specificic events of a record in a new `meta_events` column.
    The mapped events can be like: it has screenshots; it has some compromised database or has other type of vulnerability;
    and others.

    :params new_rules: Another patterns can be informed as string (using Spark SQL syntax);
    :return: Spark DataFrame 
    """

    events_dict = {
        'vulns-heartbleed': (col("opts").contains("heartbleed") & col("opts").contains("VULNERABLE")),
        'password-leak-lantronix': col("lantronix").contains("password"),
        'unauthenticated-admin-lantronix': col("data").contains("Press Enter for Setup Mode"), # Lantronix ethernet adapter’s admin interface without password
        'unauthenticated-terminal': (
                (col("shodan_module").contains("telnet") | col("shodan_module").contains("ssh")) & 
                (col("data").contains("@") & col("data").contains("\r\n") & (col("data").contains("]$") | col("data").contains("]#") | col("data").contains("/ #")))
        ),
        'android_debug_bridge': (col("android_debug_bridge").isNotNull()),
        'has_screenshot': col("opts").contains("screenshot"),
        
        # databases
        'shodan-tag-database': array_contains(col("tags"), "database"),
        'unauthenticated-ftp': col("data").contains("230 Login successful"), # FTP Servers with Anonymous Login
        'unauthenticated-samba': col("data").contains("SMB Status:\n  Authentication: disabled"),
        'unauthenticated-mongodb': col("mongodb").contains('"authentication":"false"'),
        'unauthenticated-elastic': col("elastic").contains('"indices":{"'),
        'unauthenticated-kibana':  (col("data").contains("kibana") & col("data").contains("content-length: 217")),
        'unauthenticated-redis': (col("redis").isNotNull() & col("redis").contains('"keys":{')),
        'unauthenticated-couchdb': (col("couchdb").isNotNull() & col("couchdb").contains('"dbs":["')),
        
        'compromised-mongodb': (col("mongodb").contains("meow") | lower("mongodb").contains("read_me_to_recovert_your_data")),
        'compromised-elastic': (col("elastic").contains("-meow")),

        # https://cloud7.news/security/39000-exposed-unauthenticated-redis-servers-are-under-attack/
        'compromised-redis': (col("redis").isNotNull() &  col('redis').contains('"backup1"') &  col('redis').contains('"backup4"')), 

        'vulns-couchdb-CVE-2017-12635': (
            col("couchdb").isNotNull() & col("couchdb").contains('"dbs":["') &
            (substring(regexp_extract("couchdb", 'version\"\:\"[0-2].\d+\.\d+', 0), 11, 5) < '2.1.1') 
        ),
        
        'compromised-website': lower("http").rlike("hacked by \w"),
        'compromised-routers': lower("data").contains("hacked-router-help-sos"),
        'shodan-tag-malware': array_contains(col("tags"), "malware"),
        'shodan-tag-compromised': array_contains(col("tags"), "compromised"),
        'shodan-tag-ics': array_contains(col("tags"), "ics"),
        'shodan-tag-medical': array_contains(col("tags"), "medical"),
        'unauthenticated-vnc': (col("data").contains("Authentication Disabled") & col("data").contains("RFB 003.008")),
        'vulns-hp-ilo4-CVE-2017-12542' : (
            col("data").contains("iLO 4") & col("data").contains("iLO Firmware:") & 
            ((substring(regexp_extract("data", 'iLO Firmware: \d\.\d+', 0),15, 5) < '2.53'))
        ),
        'has_vulns': col("vulns_cve").isNotNull(),
        'has_verified_vulns': col("vulns_verified").isNotNull(),
    }
    
    new_cols = []

    for new_col, cond in events_dict.items():
        tmp_col = "tmp_get_events_" + new_col
        new_cols.append(tmp_col)
        self = self.withColumn(tmp_col, when(cond, lit(new_col)))
        
    result = self.withColumn("meta_events", array_sort(array_distinct(array(*new_cols))))\
        .withColumn("meta_events", expr('filter(meta_events, x -> x is not null)'))\
        .drop(*new_cols)
    
    return result


def get_softwares_in_data():
    return None


def efficient_join(self, df2):
    """
    Method to efficient join using partitioning features of Delta Lake. It assumes that 
    both DataFrames contains `year`, `date`, `meta_module` and `shodan_id` columns (those 
    columns will be used as keys). The  main idea is that the df2 must has equal or less 
    records than df1.

    :param df2: Other DataFrame to join with. 
    :return: Spark DataFrame 
    """

    pushes = df2.agg(collect_set("year").alias("year"), 
                     collect_set("date").alias("date"), 
                     collect_set("meta_module").alias("meta_module")).collect()[0]
    pushes_year = pushes.year
    pushes_date = pushes.date
    pushes_meta_module = pushes.meta_module

    result = self.filter(col("year").isin(pushes_year) & col("date").isin(pushes_date) & col("meta_module").isin(pushes_meta_module))\
        .join(df2, ["year", "date", "meta_module", 'shodan_id'])

    return result
