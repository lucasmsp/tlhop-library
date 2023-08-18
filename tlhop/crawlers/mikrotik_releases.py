#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import shutil
from bs4 import BeautifulSoup
import os
import glob
from datetime import datetime
import urllib.request
import re
import pandas as pd

# TODO: Check updates using field <pubDate> in https://mikrotik.com/download.rss
# TODO: add time elapsed 

class MikrotikReleases(object):

    """
    Mikrotik Releases Dataset

    A dataset about Mikrotik's releases information crawled from official changelog.
    
    Reference: https://mikrotik.com/download/changelogs/
    """

    _ERROR_MESSAGE_000 = "[ERROR] This crawler requires an environment variable 'TLHOP_DATASETS_PATH' containing a folder path to be used as storage to all TLHOP datasets."
    _ERROR_MESSAGE_001 = ("[ERROR] Destination output already exists ('{}') but RELEASE file is " +
        "missing. Desconsidering previous files, if they exists.")
    _ERROR_MESSAGE_002 = "[ERROR] Mentioned file '{}' in RELEASE was not found."
    
    _INFO_MESSAGE_001 = "Last crawling timestamp: {}"
    _INFO_MESSAGE_002 = "Crawling for new records."
    _INFO_MESSAGE_003 = "New dataset version is download with success!"

    
    def __init__(self):
        """
        During the initialization process, it will check if a TLHOP directory folder is set and valid.
        It also will check on the Internet if an new dataset version is available to download in case 
        of a previous download was found during folder lookup.
        """

        self.download_url = "https://mikrotik.com/download/changelogs/"
        self.path = "mikrotik-releases/"
        self.expected_schema = {"outname": "mikrotik_releases.csv"}
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
        print("""
        # Mikrotik Releases Dataset
        
        - Description: A dataset about Mikrotik's releases information crawled from official changelog.
        - Reference: {}
        - Fields: environment deployment, release/version, date
        """.format(self.download_url))

    def download(self):
        """
        Downloads a new dataset version available in its source link. 
        """
        print(self._INFO_MESSAGE_002)
        outfile = self.basepath + self.expected_schema["outname"]
        tmp = []
        releases = self._get_releases()
        for i, (deployment, release, date) in enumerate(releases):
            content = self._get_info(release) 
            msg = "What's new in "
            if msg in content:
                for rr in content.split("\n"):
                    if msg in rr:
                        rr = rr[14:].lstrip()
                        s = rr.find(" ")
                        version = rr[0:s]
                        date = None
                        p1 = re.search(r'\(\d+-\w+-\d+', rr)
                        if p1:
                            date = datetime.strptime(p1.group(0)[1:], '%Y-%b-%d').date()
                        else:
                            p2 = re.search(r'\(\w+.\d+.\d+', rr)
                            if p2:
                                date = datetime.strptime(p2.group(0)[1:], '%b.%d.%y').date()
                        tmp.append([deployment, version, date])
            else:
                tmp.append([deployment, release, date])

        pd.DataFrame(tmp, columns=["deployment", 'release', "date"])\
            .sort_values(by=['date'], ascending=0)\
            .to_csv(outfile, sep=";", index=False)
                
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        msg = "{}|{}".format(now, self.expected_schema["outname"])
        f = open(self.basepath+"RELEASE", "w")
        f.write(msg)
        f.close()
        print(self._INFO_MESSAGE_003)
        return True
        
        
    def _get_releases(self):

        response = requests.get(self.download_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        deployments = [d.text for d in soup.findAll("h4")]
        releases = []
        for i, d in enumerate(deployments):
            d = d[:d.find(" release")]
            elements = soup.findAll("ul", {"class":"accordion"})[i]
            for e1, e2 in zip(elements.findAll("b"), elements.findAll("span")):
                release = e1.getText()
                if "Release" in release:
                    date = datetime.strptime(e2.getText(), '%Y-%m-%d').date()
                    r = [d, release.replace("Release ", ""), date]
                    releases.append(r)
        return releases

    def _get_info(self, release):
        url = '{}?ax=loadLog&val={}'.format(self.download_url, release)  
        headers = {'Referer': self.download_url}
        r = requests.get(url, headers=headers)
        return r.text
