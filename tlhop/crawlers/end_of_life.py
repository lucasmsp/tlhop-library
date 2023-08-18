#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import os
import glob
from datetime import datetime
import urllib.request
import re
import pandas as pd
import json

class EndOfLife(object):

    """
    EndOfLife Dataset

    Keep track of various End of Life dates and support lifecycles for various products.
    Reference: https://endoflife.date/
    """

    _ERROR_MESSAGE_000 = "[ERROR] This crawler requires an environment variable 'TLHOP_DATASETS_PATH' containing a folder path to be used as storage to all TLHOP datasets."
    _ERROR_MESSAGE_001 = ("[ERROR] Destination output already exists ('{}') but RELEASE file is " +
        "missing. Desconsidering previous files, if they exists.")
    _ERROR_MESSAGE_002 = "[ERROR] Mentioned file '{}' in RELEASE was not found."
    
    _INFO_MESSAGE_001 = "Last crawling timestamp: {}"
    _INFO_MESSAGE_002 = "Crawling for new records."
    _INFO_MESSAGE_003 = "New dataset version is download with success!"
    _INFO_MESSAGE_004 ="{} products found to be crawled."

    
    def __init__(self):
        """
        During the initialization process, it will check if a TLHOP directory folder is set and valid.
        It also will check on the Internet if an new dataset version is available to download in case 
        of a previous download was found during folder lookup.
        """

        self.download_url = "https://endoflife.date/docs/api/"
        self.path = "endoflife/"
        self.expected_schema = {"outname": "endoflife.json"}
        self.last_file = {}
    
        root_path = os.environ.get("TLHOP_DATASETS_PATH", None)
        if not root_path:
            raise Exception(self._ERROR_MESSAGE_000)
        self.root_path = (root_path+"/").replace("//", "/")
        self.basepath = self.root_path+self.path
                
        self._check_status()

    def _check_status(self):
        if not os.path.exists(self.basepath):
            os.mkdir(self.basepath)

        elif os.path.exists(self.basepath+"RELEASE"):
            with open(self.basepath+"RELEASE", "r") as f:
                timestamp, filename = f.read().split("\n")[0].split("|")
                if not os.path.exists(self.basepath+filename):
                    raise Exception(self._ERROR_MESSAGE_001.format(filename))
                else:
                    self.last_file = {"timestamp": timestamp, "outname": filename}
                    print(self._INFO_MESSAGE_001.format(timestamp))
        else:
            print(self._ERROR_MESSAGE_001.format(self.basepath))

    def describe(self):
        """
        Function to describe some informations about the dataset: 
        description, reference and fields.
        """
        print(f"""
        # EndOfLife Dataset
        
        - Description: Keep track of various End of Life dates and support lifecycles for various products.
        - References: {self.download_url} and https://github.com/endoflife-date/release-data
        """)

    def download(self):
        """
        Downloads a new dataset version available in its source link. 
        """
        print(self._INFO_MESSAGE_002)
        outfile = self.basepath + self.expected_schema["outname"]
        headers = {
            'Accept': 'application/json',
        }

        response = requests.get('https://endoflife.date/api/all.json', headers=headers)
        products = response.text[1:-2].replace('"', '').split(",")
        print(self._INFO_MESSAGE_004.format(len(products)))
        table = [] 
        for product in products:
            response = requests.get(f'https://endoflife.date/api/{product}.json', headers=headers)
            tmp = json.loads(response.text)
            for t in tmp:
                t["product"] = product
            table += tmp

        pd.DataFrame.from_dict(table)\
            .to_json(outfile, orient='records', lines=True)
                
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        msg = "{}|{}".format(now, self.expected_schema["outname"])
        f = open(self.basepath+"RELEASE", "w")
        f.write(msg)
        f.close()
        print(self._INFO_MESSAGE_003)
        return True
        
        
