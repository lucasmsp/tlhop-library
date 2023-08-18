#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from functools import wraps

import os
import sys
import tlhop.shodan_library as shodan_lib

from pyspark.sql.dataframe import DataFrame   


# Trick to extending the PySpark DataFrame. The traditional way 
# (by inherit DataFrame Class) will not work as espected, because 
# as soon as you call Parent DataFrame method that returns a 
# DataFrame object you will lose the functionality of 
# extend DataFrame methods. More details can be found at: 
# https://medium.com/@rdgovindarajan/extending-pyspark-dataframe-feasible-8a5981ed50a4


def shodan_extension(self):
    
    # Create a decorator to add a function to a python object
    def add_attr(cls):
        def decorator(func):
            @wraps(func)
            def _wrapper(*args, **kwargs):
                f = func(*args, **kwargs)
                return f

            setattr(cls, func.__name__, _wrapper)
            return func

        return decorator

    @add_attr(shodan_extension)
    def efficient_join(df2):
        return shodan_lib.efficient_join(self, df2)
    
    @add_attr(shodan_extension)
    def get_http_status(outname="http-code"):
        return shodan_lib.get_http_status(self, outname=outname)
    
    @add_attr(shodan_extension)
    def get_events(new_rules):
        return shodan_lib.get_events(self, new_rules)
        
    @add_attr(shodan_extension)
    def parser_http_col(outname=None, cols=None):
        return shodan_lib.parser_http_col(self, outname=outname, cols=cols)
    
    @add_attr(shodan_extension)
    def cleaning_org(input_col="org", output_col=None):
        return shodan_lib.cleaning_org(self, input_col=input_col, output_col=output_col)
    
    @add_attr(shodan_extension)
    def cleaning_text(input_col, output_col=None):
        return shodan_lib.cleaning_text(self, input_col, output_col=output_col)

    @add_attr(shodan_extension)
    def get_html_lang(input_col="html", output_col="html_lang"):
        return shodan_lib.get_html_lang(self, input_col=input_col, output_col=output_col)
    
    @add_attr(shodan_extension)
    def describe_html(html_col="html", title_col="title_clean"):
        return shodan_lib.shodan_lib.describe_html(self, html_col=html_col, title_col=title_col)
    
    @add_attr(shodan_extension)
    def parser_html_code(input_col="html", output_col="html_parsed"):
        return shodan_lib.parser_html_code(self, input_col=input_col, output_col=output_col)
    
    @add_attr(shodan_extension)
    def filter_modules_with_column(input_col):
        return shodan_lib.filter_modules_with_column(self, input_col)
    
    @add_attr(shodan_extension)
    def filter_banners_with_screenshot():
        return shodan_lib.filter_banners_with_screenshot(self)
    
    @add_attr(shodan_extension)
    def extract_screenshot():
        return shodan_lib.extract_screenshot(self)

    @add_attr(shodan_extension)
    def print_screenshot(size):
        return shodan_lib.print_screenshot(self, size)
    
    @add_attr(shodan_extension)
    def get_softwares_in_data():
        return shodan_lib.get_softwares_in_data()
    
    @add_attr(shodan_extension)
    def filter_valid_html_page(hash_col="http.html_hash", status_col="http-code"):
        return shodan_lib.filter_valid_html_page(self, hash_col=hash_col, status_col=status_col)
    
    @add_attr(shodan_extension)
    def gen_cdf_pdf(input_col, to_pandas=True):
        return shodan_lib.gen_cdf_pdf(self, input_col, to_pandas)

    @add_attr(shodan_extension)
    def contains_vulnerability(cves, mode="any", vulns_col="vulns_cve"):
        return shodan_lib.contains_vulnerability(self, cves, mode=mode, vulns_col=vulns_col)

    @add_attr(shodan_extension)
    def parser_cpe(input_col="cpe", output_col="cpe_parsed"):
        return shodan_lib.parser_cpe(self, input_col=input_col, output_col=output_col)
    
    @add_attr(shodan_extension)
    def parser_complex_column(input_col, output_col=None):
        return shodan_lib.parser_complex_column(self, input_col=input_col, output_col=output_col)
     
    @add_attr(shodan_extension) 
    def plot_bubble(lat_col, lon_col, color_col=None, size=3.0, hover_name=None, 
                hover_data=None, opacity=0.9, max_rows=10000):
        return shodan_lib.plot_bubble(self, lat_col, lon_col, color_col=color_col, size=size, 
                           hover_name=hover_name, hover_data=hover_data, opacity=opacity, 
                           max_rows=max_rows)
    
    @add_attr(shodan_extension)
    def plot_heatmap(lat_col, lon_col, z_col=None, hover_name=None, hover_data=None, 
                     radius=8, opacity=0.9, max_rows=10000):
        return shodan_lib.plot_heatmap(self, lat_col, lon_col, z_col=z_col, hover_name=hover_name, 
                            hover_data=hover_data, radius=radius, opacity=opacity, max_rows=max_rows)
    
    @add_attr(shodan_extension)
    def get_ip_mask(input_col="ip_str", output_col=None, level=3):
        return shodan_lib.get_ip_mask(self, input_col=input_col, output_col=output_col, level=level)

    return shodan_extension

DataFrame.shodan_extension = property(shodan_extension)