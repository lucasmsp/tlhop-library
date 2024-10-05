#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import shutil
import os
import glob
from datetime import datetime
import pandas as pd
import json
import time
from graphqlclient import GraphQLClient

class ASRank(object):

    """
    AS Rank Dataset

    A ranking of Autonomous Systems (AS). ASes are ranked by their 
    customer cone size, which is the number of their direct and indirect customers. 
    
    Reference: https://asrank.caida.org/
    """

    _ERROR_MESSAGE_000 = "[ERROR] This crawler requires an environment variable 'TLHOP_DATASETS_PATH' containing a folder path to be used as storage to all TLHOP datasets."
    _ERROR_MESSAGE_001 = (
                "[ERROR] Destination output already exists ('{}') but RELEASE file is "
                +"missing. Desconsidering previous files, if they exists.")
    _ERROR_MESSAGE_002 = "[ERROR] Mentioned file '{}' in RELEASE was not found."
    _ERROR_MESSAGE_003 = "[ERROR] Failed to parse: {}"

    _INFO_MESSAGE_001 = "[INFO] Last crawling timestamp: {}"
    _INFO_MESSAGE_002 = "[INFO] Running - retrivied {} of {} records in {:.2f} seconds."
    _INFO_MESSAGE_003 = "[INFO] New dataset version is download with success!"
    _INFO_MESSAGE_004 = "A most recent version of the current dataset was found."
    _INFO_MESSAGE_005 = "The current dataset version is the most recent."
    
    def __init__(self):
        """
        During the initialization process, it will check if a TLHOP directory folder is set and valid.
        It also will check on the Internet if an new dataset version is available to download in case 
        of a previous download was found during folder lookup.
        """
        self.download_url = "https://api.asrank.caida.org/v2/graphql"
        self.path = "caida-rank/"
        self.expected_schema = {"outname": "as_rank.csv"}
        self.last_file = {}
    
        root_path = os.environ.get("TLHOP_DATASETS_PATH", None)
        if not root_path:
            raise Exception(self._ERROR_MESSAGE_000)
        self.root_path = (root_path+"/").replace("//", "/")
        self.basepath = self.root_path+self.path
        
        self._check_status()
        self._check_for_new_files()

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

    def _check_for_new_files(self):

        download_url = 'https://api.asrank.caida.org/v2/restful/datasets'
        response = requests.get(download_url)
        now = '2024-09-30 00:00:00'
        if response.ok:
            response_text = response.text
            now = response.json()['data'][0]['modified_at'][:-7]
            
        self.now = datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
        latest = datetime.strptime(self.last_file['timestamp'], "%Y%m%d_%H%M%S")

        if abs(self.now - latest).days > 1:
            print(self._INFO_MESSAGE_004)
            self.new_version = True
        else:
            print(self._INFO_MESSAGE_005)
            self.new_version = False

    def describe(self):
        """
        Function to describe some informations about the dataset: 
        description, reference and fields.
        """
        print("""
        # AS Rank Dataset
        
        - Description:  A ranking of Autonomous Systems (AS). ASes are ranked by their 
          customer cone size, which is the number of their direct and indirect customers. 
        - Reference: https://asrank.caida.org/
        - Download link: {}
        - Fields: 
            * asn - code
            * asnName - name of the ASN;
            * cliqueMember - is true if the ASN is inferred to be a member of the clique of ASN at the top of the ASN hierarchy;
            * longitude - longitude of the ASN;
            * latitude - latitude of the ASN;
            * rank - is ASN's rank, which is based on it's customer cone size, which in turn;
            * seen - is true when ASN is seen in BGP;
            * announcing_numberAddresses - number of addresses announced by the ASN;
            * announcing_numberPrefixes - set of prefixes announced by the ASN;
            * asnDegree_customer - The number of ASNs that are customers of the selected ASN.;
            * asnDegree_sibling -  The number of ASNs that are providers of the selected ASN;
            * asnDegree_peer -  The number of ASNs that are peers of the selected ASN;
            * asnDegree_total -  The number of ASNs that were observed as neighbors of the selected ASN in a path;
            * asnDegree_transit -  The number of ASNs that where observed as neighbors of the selected ASN in a path, 
              where the selected ASN was between, i.e. providing transit, for two other ASNs;
            * organization_orgId - Organization's id of responsible to the ASN;
            * organization_orgName - Organization's name of responsible to the ASN;
            * country_iso - Country ISO code which the ASN is in;
            * country_name - Country name code which the ASN is in;
        """.format(self.download_url))

    def download(self):
        """
        Downloads a new dataset version available in the source link. After download, it process the
        original data format to enrich its data and to convert to Parquet format.
        """
        if self.new_version:
            page_size = 10000
            decoder = json.JSONDecoder()
            encoder = json.JSONEncoder()
            
            hasNextPage = True
            first = page_size
            offset = 0
            local_filename = self.basepath + "asns.jsonl"
            client = GraphQLClient(self.download_url)
            
            start = time.time()
            with open(local_filename, "w") as f:
                while hasNextPage:
                    query = self._asn_query(first, offset)
                    data = decoder.decode(client.execute(query))
                    
                    if not ("data" in data and "asns" in data["data"]):
                        raise Exception(self._ERROR_MESSAGE_003.format(data))
                        
                    data = data["data"]["asns"]
                    for node in data["edges"]:
                        f.write(encoder.encode(node["node"])+"\n")
    
                    hasNextPage = data["pageInfo"]["hasNextPage"]
                    offset += data["pageInfo"]["first"]
                    elapsed = time.time() - start
                    print(self._INFO_MESSAGE_002.format(offset, data["totalCount"], elapsed))
                    start = time.time()
            
            outfile = self.basepath + self.expected_schema["outname"]
            self._process(local_filename, outfile)
            os.remove(local_filename)
            
            now = self.now.strftime("%Y%m%d_%H%M%S")
            msg = "{}|{}".format(now, self.expected_schema["outname"])
            f = open(self.basepath+"RELEASE", "w")
            f.write(msg)
            f.close()
            print(self._INFO_MESSAGE_003)
        return True
    
    def _process(self, inputfile, outfile):
        df = pd.read_json(inputfile, lines=True)
        
        asnDegree = pd.json_normalize(df["asnDegree"])
        asnDegree.columns = ["degree_provider","degree_peer","degree_customer", "degree_total"]
        
        announcing  = pd.json_normalize(df["announcing"])
        announcing.columns = ["announcing_prefixes", "announcing_addresses"]

        df["organization"] = df["organization"].apply(lambda x: {} if pd.isna(x) else x)
        organization = pd.json_normalize(df["organization"])
        organization.columns = ["org_id", "org_name", "org_contry_iso", "org_country_name"]
        
        country = pd.json_normalize(df["country"])
        country.columns = ["as_country_iso", "as_country_name"]
        
        
        df = df.drop(columns=['country', "organization", "announcing", "asnDegree"], axis=1)
        df = pd.concat([df, asnDegree, organization, country, announcing], axis=1)

        df = df[["asn", "asnName", "as_country_iso", "as_country_name", "longitude", "latitude",
                 "rank", "cliqueMember", "seen", 
                 "degree_provider","degree_peer","degree_customer", "degree_total", 
                 "org_id", "org_name", "org_contry_iso", "org_country_name",
                 "announcing_prefixes", "announcing_addresses"]]
        
        df.columns = [["asn", "as_name", "as_country_iso", "as_country_name", "longitude", "latitude",
                       "rank", "clique", "seen",
                       "degree_provider","degree_peer","degree_customer", "degree_total", 
                       "org_id", "org_name", "org_contry_iso", "org_country_name",
                       "announcing_prefixes", "announcing_addresses"]]
        
        df.to_csv(outfile, index=False, header=True, sep=";")
        
    def _asn_query(self, first, offset):
        return """
    {
        asns(first:%s, offset:%s) {
            totalCount
            pageInfo {
                first
                hasNextPage
            }
            edges {
                node {
                    asn
                    asnName
                    rank
                    organization {
                        orgId
                        orgName
                        country {
                            iso
                            name
                        }
                    }
                    cliqueMember
                    seen
                    longitude
                    latitude
                    country {
                        iso
                        name
                    }
                    asnDegree {
                        provider
                        peer
                        customer
                        total
                    }
                    announcing {
                        numberPrefixes
                        numberAddresses
                    }
                }
            }
        }
    }""" % (first, offset)
