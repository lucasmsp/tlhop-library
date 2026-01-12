#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession                                                
from pyspark.sql.types import *
import pyspark.sql.functions as F

import os
import re
import time
import json
import glob
import shutil
import pickle
import urllib3
import ipaddress
import textdistance
import requests as req
from datetime import datetime
from intervaltree import Interval, IntervalTree

from tlhop.library import cleaning_text_udf

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
class RDAP(object):
    
    """
    RDAP Dataset
        
    Description: RDAP is a protocol that delivers registration data like WHOIS, this dataset is 
                 generated from a list of <IPs, orgs>. Each IP will be requested in an ordered RDAP url list.
    Download url: https://rdap.registro.br/ip/<ip>, https://rdap-bootstrap.arin.net/bootstrap/ip/<ip>,
                  https://rdap.org/ip/<ip>, https://rdap.db.ripe.net/ip/<ip>
    """

    _ERROR_MESSAGE_000 = "[ERROR] This crawler requires an environment variable 'TLHOP_DATASETS_PATH' containing a folder path to be used as storage to all TLHOP datasets."
    _ERROR_MESSAGE_001 = "None active Spark session was found. Please start a new Spark session before use DataSets API."
    
    _WARN_MESSAGE_000 = ("[WARN] Destination output already exists ('{}') but RELEASE file is " +
        "missing. Desconsidering previous files, if they exists.")
    _WARN_MESSAGE_001 = "[WARN] Mentioned file '{}' in RELEASE was not found. Maybe the last execution was interrupted."
    
    _INFO_MESSAGE_001 = "[INFO] Last crawling timestamp: {}"
    _INFO_MESSAGE_002 = "[INFO] Generating new consolided file."
    
    def __init__(self):
        """
        
        """
        self.path = "rdap/"
        self.expected_schema = {
          "json": "rdap_data.json",
          "log": "rdap_log.log",
          "tree": "interval_tree.pickle", 
          "parquet": "rdap.parquet"
        }

        root_path = os.environ.get("TLHOP_DATASETS_PATH", None)
        if not root_path:
            raise Exception(self._ERROR_MESSAGE_000)
        self.root_path = (root_path+"/").replace("//", "/")
        self.basepath = self.root_path + self.path
        self.now = datetime.now()
        self.last_folder = None
        
        self.spark_session = SparkSession.getActiveSession()
        if not self.spark_session:
            raise Exception(self._ERROR_MESSAGE_001)
        
        self._check_status()
    
    def _check_status(self):
        if not os.path.exists(self.basepath):
            os.mkdir(self.basepath)
            
        elif os.path.exists(self.basepath+"RELEASE"):
            with open(self.basepath+"RELEASE", "r") as f:
                filename_parquet, timestamp = f.readlines()[0].split("|")
                timestamp = timestamp.replace("\n", "")
                self.last_folder = self.basepath + "scan_" + timestamp + "/"
                if not os.path.exists(self.last_folder + filename_parquet):
                    raise Exception(self._WARN_MESSAGE_001.format(timestamp + "/" + filename_parquet))
                        
                print(self._INFO_MESSAGE_001.format(timestamp))
        else:
            print(self._WARN_MESSAGE_000.format(self.basepath))
    
    def describe(self):
        """
        Function to describe some informations about the dataset: 
        description, reference and fields.
        """
        print("""
        # RDAP Dataset
        
        - Description: RDAP is a protocol that delivers registration data like WHOIS, this dataset is 
          generated from a list of <IPs, orgs>. Each IP will be requested in an ordered RDAP url list.
        - Download url: https://rdap.registro.br/ip/<ip>, https://rdap-bootstrap.arin.net/bootstrap/ip/<ip>,
                        https://rdap.org/ip/<ip>, https://rdap.db.ripe.net/ip/<ip>
        - Schema:
             |-- block_id: long
             |-- country: string
             |-- whois_shodan_original: string
             |-- reference: string
             |-- status: array
             |    |-- element: string
             |-- type: string
             |-- AS: string
             |-- whois_legal_representative: string
             |-- whois_identifier: string
             |-- whois_identifier_type: string
             |-- registrant_name: string
             |-- registrant_email: string
             |-- registrant_address: string
             |-- registrant_registration: date
             |-- registrant_last_changed: date
             |-- administrative_name: string
             |-- administrative_email: string
             |-- administrative_address: string
             |-- administrative_registration: date
             |-- administrative_last_changed: date
             |-- technical_name: string
             |-- technical_email: string
             |-- technical_address: string
             |-- technical_registration: date
             |-- technical_last_changed: date
             |-- abuse_name: string
             |-- abuse_email: string
             |-- abuse_address: string
             |-- abuse_registration: date
             |-- abuse_last_changed: date
             |-- start_ip_str: string
             |-- end_ip_str: string
             |-- ip_start: long
             |-- ip_end: long
             |-- whois_org: string
             |-- whois_address: string
             |-- registration: date
             |-- last_changed: date
             |-- reverseDelegations: string
        """)
        
    def download(self, target_ip_list, append=True, range_ips=None, resume=False, timeout=9, use_selenium=False, historical_data=True):
        """
        Generates the dataset. 
        
        :params target_ip_list: A list of ip (integer format) and its organization to be crawled. The organization name is only used to 
         avoid multiple requests over a subnet. As the IP network to be searched is already known, we skip its request unless the organization
         passed by parameter is different from the one previously identified;
        :params append: True to increment an previous execution with new rdap requests, otherwise it will remove previous dataset (default, True);
        :params range_ips: The path of a file containing the range of IPs (int format) to be considered. For instance, if we have a list of the
         brazilian IPs, we can force the application only to request IPs in this range.
         Each line contains a interval in format <IP begin>-<IP end>, separated by line. 
         Any IP not in this range will not be crawled;
        :params resume: True to resume the crawler, starting from the last probed IP (default, False);
        :params timeout: A timeout in seconds between each RDAP request to avoid IP blocking (default is 9 seconds);
        :params historical_data: If we are using historical data (old relations between IP and org) the changes over time are not captured 
         (we can only crawler the current state of an IP registration), because of that, is possible that responsible organizations may be changed. 
         Due our implementation, we crawler RDAP to each organization change, creating multiple collisions (same IP interval but different organizations).
         If historical_data is True (default), we will only keep the most recent RDAP record. 
        """
        
        mask3 = ""   
        start_line = 0
        olders_scan_folder = sorted(glob.glob(self.basepath + "scan_*"), reverse=True)
        
        if resume and (len(olders_scan_folder) > 0):
            self.folder = olders_scan_folder[0]
        else:
            print("[INFO] Starting a new execution.")
            self.folder = self.basepath + "scan_" + self.now.strftime("%Y%m%d_%H%M%S") + "/"
            os.mkdir(self.folder)

        current_logfile_name = self.folder + self.expected_schema['log']
        output_json_data_name = self.folder + self.expected_schema['json']
        output_parquet_data = self.folder + self.expected_schema["parquet"]
        
        if resume and os.path.exists(current_logfile_name):
            with open(current_logfile_name) as f:
                for line in f:
                    pass
                last_line = line
            start_line = int(last_line.split(".")[0][16:])
            print(f"[INFO] Resuming previous execution from line {start_line}.")
        
        tree = IntervalTree()
        last_block_id = 0
        
        olders_tree_files = sorted(glob.glob(self.basepath + "scan_*/interval_tree.pickle", recursive=True), reverse=True)
        if append and (len(olders_tree_files) != 0):
            older_tree_path = olders_tree_files[0]
            tree = self._load_tree(older_tree_path)
            last_block_id = self._get_last_blockID(tree)                
            
        tree_path = self.folder + self.expected_schema["tree"]
        output = open(output_json_data_name, "w", buffering=1)
        logfile = open(current_logfile_name, "w", buffering=1)

        if range_ips:
            range_ips = pd.read_csv(range_ips, sep="-")
            range_ips.columns = ["start_ip_int", "end_ip_int"]

        if use_selenium:
            from selenium import webdriver
            from selenium.webdriver.firefox.service import Service as FirefoxService
            from webdriver_manager.firefox import GeckoDriverManager
            from selenium.webdriver.firefox.options import Options

            options = Options()
            options.add_argument("--headless")
            
            browser = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()), options=options)
        else:
            browser = None


        with open(target_ip_list) as f:
            for i, line in enumerate(f):

                if (i < start_line) or (len(line) < 10):
                    continue

                if i % 1000 == 0:
                    print("[INFO] Position {} - Current block_id {} ".format(i, last_block_id))

                ip_int, org_clean = line.replace("\n", "").split(",")
                ip_int = int(ip_int)

                if self._is_IP_in_range(ip_int, range_ips):
                    elem = tree[ip_int]
                    found = False
                    n_elems = len(elem)

                    # checking if already exists a subnet to this IP
                    if n_elems > 0:
                        if n_elems == 0:
                            x = list(elem)
                        else:
                            x = elem
                        min_range = sorted([f.data['n_ips'] for f in x])[0]
                        for e in x:
                            if e.data['n_ips'] == min_range: 
                                org = e.data['org']
                                if org == org_clean:
                                    logfile.write(f"[INFO] Position {i}. Found previous element to {ip_int}/{org_clean} (nº ips: {min_range}) ({n_elems} overlap)\n")
                                    found = True

                    if not found: 
                        ip_str = str(ipaddress.ip_address(ip_int))
                        current_mask3 = re.sub(r"\.\d+$", "", ip_str)

                        if mask3 != current_mask3:
                            data = self._request_rdap(ip_str, browser, timeout=timeout, use_selenium=use_selenium)
                            mask3 = current_mask3
                            if data:
                                current_datetime = datetime.now()
                                last_block_id += 1
                                if "cidr0_cidrs" in data:
                                    for e in data["cidr0_cidrs"]:
                                        startAddress = e["v4prefix"]
                                        mask = e["length"]
                                        endAddress = [str(ip) for ip in ipaddress.IPv4Network('{}/{}'.format(startAddress, mask))][-1]
                                        last_block_1 = int(ipaddress.IPv4Address(startAddress))
                                        last_block_2 = int(ipaddress.IPv4Address(endAddress))
                                        n_ips = last_block_2 - last_block_1 +1
                                        tree[last_block_1:last_block_2+1] = {"block_id": last_block_id, "org": org_clean, "n_ips": n_ips, "timestamp": current_datetime}
                                else:
                                    last_block_1 = int(ipaddress.IPv4Address(data["startAddress"]))
                                    last_block_2 = int(ipaddress.IPv4Address(data["endAddress"]))
                                    n_ips = last_block_2 - last_block_1 +1
                                    tree[last_block_1:last_block_2+1] = {"block_id": last_block_id, "org": org_clean, "n_ips": n_ips, "timestamp": current_datetime}

                                data['block_id'] = last_block_id
                                data['org_clean'] = org_clean
                                data['crawler_timestamp'] = current_datetime.strftime("%Y%m%d_%H%M%S")
                                output.write(json.dumps(data) + "\n")
                                logfile.write(f"[INFO] Position {i}. {ip_int}/{org_clean} crawled with success (new block_id {last_block_id})\n")
                        else:
                            logfile.write(f"[INFO] Position {i}. Avoiding crawler the ip {ip_int} because its prefix was already searched without success\n")
                else:
                    logfile.write(f"[INFO] Position {i}. IP {ip_int} is not in delimited range.\n")
                last_org = org_clean

        output.close()
        logfile.close()
        if use_selenium:
            browser.close()
        
        self._save_tree(tree_path, tree)

        if historical_data:
            tree = self._clean_tree(tree)
            os.rename(tree_path, tree_path + ".bkp")
            self._save_tree(tree_path, tree)

        self._process(output_json_data_name, output_parquet_data, append)

        # registering the current execution
        with open(self.basepath+"RELEASE", "w") as f:
            msg = "{}|{}".format(self.expected_schema["parquet"], 
                     self.folder.split("/")[-2].replace("scan_", ""))
            f.write(msg)
        return True
        
    def _load_tree(self, tree_input):
        with open(tree_input, 'rb') as handle:
            tree = pickle.load(handle)
        return tree

    def _get_last_blockID(self, tree):
        max_block_id = -1
        for e in tree:
            block_id = e.data["block_id"]
            if block_id > max_block_id:
                max_block_id = block_id
        print(f"[INFO] Starting incremental running. Last block_id {max_block_id}")
        return max_block_id
    
    def _is_IP_in_range(self, ip_int, range_ips):
        if range_ips:
            return len(range_ips[(range_ips["start_ip_int"] <=  ip_int) &  (ip_int <= range_ips["end_ip_int"])]) > 0
        return True
    
    
    def _save_tree(self, tree_output, tree):
        with open(tree_output, 'wb') as handle:
            pickle.dump(tree, handle, protocol=pickle.HIGHEST_PROTOCOL)

    def _process(self, output_json_data_name, output_data, append=False):
        """
        Convert JSON file into a Parquet format.
        """
        print(self._INFO_MESSAGE_002)
        
        rdap_df = self.spark_session.read.json(output_json_data_name).drop("reference")
        rdap_columns = rdap_df.columns
        
        if rdap_df.count() == 0:
            print("This execution had none requisition. Operation Completed.")
            
        else:
            print("The original RDAP dataset contains the following columns:", rdap_columns)

            # normalizing columns
            for col in ['nicbr_reverseDelegations', 'lacnic_reverseDelegations']:
                if col in rdap_columns:
                    rdap_df = rdap_df.withColumn(col, F.to_json(col))

            mapping = {
                "nicbr_autnum": StringType(), 
                "lacnic_originAutnum": ArrayType(StringType()),
                "arin_originas0_originautnums": ArrayType(StringType()),

                "nicbr_reverseDelegations": StringType(), 
                "lacnic_reverseDelegations": StringType(),
                'whois_legal_representative': StringType(),
                "lacnic_legalRepresentative": StringType(),

                "registrant_name": StringType(),
                "technical_name": StringType(),
                "abuse_name": StringType(),

                "administrative_registration": StringType(),
                "registrant_registration": StringType(),
                "abuse_registration": StringType(),
                "technical_registration": StringType(),

                "remarks": StringType(),

                "registrant_address": StringType(),
                "technical_address": StringType(),
                "abuse_address": StringType(),

                "registrant_last_changed": TimestampType(),
                "abuse_last_changed": TimestampType(),
                "technical_last_changed": TimestampType(),
                "administrative_last_changed": TimestampType()
            }
            for col, dtype in mapping.items():
                if col not in rdap_columns:
                    rdap_df = rdap_df.withColumn(col, F.lit(None).cast(dtype))


            rdap_df\
                .withColumn("AS", F.when(F.col("nicbr_autnum").isNotNull(), F.col("nicbr_autnum"))\
                                   .when(F.col("lacnic_originAutnum").isNotNull(), F.col("lacnic_originAutnum").getItem(0))\
                                   .otherwise(F.col("arin_originas0_originautnums").getItem(0)))\
                .withColumnRenamed("port43", "reference")\
                .withColumnRenamed("org_clean", "whois_shodan_original")\
                .withColumn("entities", _process_entities_field(F.to_json("entities")))\
                .select("*", "entities.*")

            if "cidr0_cidrs" in rdap_df.columns:
                rdap_df = rdap_df.withColumn("cidr0_cidrs", F.explode(_get_cid(F.to_json("cidr0_cidrs"), F.col("startAddress"), F.col("endAddress"))))
            else:
                rdap_df = rdap_df.withColumn("cidr0_cidrs", F.explode(_get_cid(F.lit(None), F.col("startAddress"), F.col("endAddress"))))

            rdap_df = rdap_df.select("*", "cidr0_cidrs.*")\
                .withColumn("ip_start", _convert_to_int(F.col("start_ip_str")))\
                .withColumn("ip_end", _convert_to_int(F.col("end_ip_str")))\
                .withColumn("whois_org", F.when(F.col("registrant_name").isNotNull(), F.col("registrant_name"))\
                                          .when(F.col("technical_name").isNotNull(), F.col("technical_name"))\
                                          .otherwise(F.col("abuse_name")))\
                .withColumn("remarks", _get_remarks(F.col("remarks")))\
                .withColumn("whois_org", F.when(F.lower("whois_org").contains("abuse") & F.col("remarks").isNotNull(), F.col("remarks"))\
                                          .otherwise(F.col("whois_org")))\
                .withColumn("whois_address", F.when(F.col("registrant_name").isNotNull(), F.col("registrant_address"))\
                                              .when(F.col("technical_name").isNotNull(), F.col("technical_address"))\
                                              .otherwise(F.col("abuse_address")))\
                .withColumn("registration", F.greatest("registrant_registration", "abuse_registration", "technical_registration", "administrative_registration"))\
                .withColumn("last_changed", F.greatest("registrant_last_changed", "abuse_last_changed", "technical_last_changed", "administrative_last_changed"))\
                .withColumn("registration", F.when(F.col("registration").isNull(), _get_events_udf(F.col("events")).getField("registration")).otherwise(F.col("registration")))\
                .withColumn("last_changed", F.when(F.col("last_changed").isNull(), _get_events_udf(F.col("events")).getField("last_changed")).otherwise(F.col("last_changed")))\
                .withColumn("reverseDelegations", F.when(F.col("nicbr_reverseDelegations").isNotNull(), F.col("nicbr_reverseDelegations"))\
                                                   .otherwise(F.col("lacnic_reverseDelegations")))\
                .withColumn("whois_legal_representative", F.when(F.col("whois_legal_representative").isNotNull(), F.col("whois_legal_representative"))\
                                                           .otherwise(F.col("lacnic_legalRepresentative")))\
                .withColumn("type", F.upper("type"))\
                .drop("entities", "cidr0_cidrs", "startAddress", "endAddress", "handle", "lacnic_originAutnum", "nicbr_autnum", 
                      "arin_originas0_originautnums", "rdapConformance", "notices", "links", "objectClassName", "ipVersion", 
                      "lang", "events", "parentHandle", "lacnic_reverseDelegations", "nicbr_reverseDelegations", 
                      "lacnic_legalRepresentative", "name", "remarks")\
                .withColumn("whois_org", cleaning_text_udf(F.col("whois_org"), F.lit(True)))\
                .orderBy(F.desc("ip_start"))

            if append and os.path.exists(output_data):
                old_rdap = self.spark_session.read.parquet(output_data)
                rdap_df = rdap_df.unionByName(old_rdap)

            rdap_df.coalesce(1)\
                .write.parquet(output_data + "_folder", mode="overwrite", compression="gzip")

            sparkfile = glob.glob(output_data + "_folder/part-*.parquet")[0]
            os.rename(sparkfile, output_data)
            shutil.rmtree(output_data + "_folder")
            print("Operation Completed")
    
        
    def _request_rdap(self, ip, browser, retry=True, timeout=9, use_selenium=True):
        """
        We use Selenium to retrieve RDAP informations. We can 
        use 4 RDAP servers to minimize failures. There is a timeout 
        between each attempt to avoid IP blocking.
        """

        urls = [
                f"https://rdap.registro.br/ip/{ip}",
                f"https://rdap-bootstrap.arin.net/bootstrap/ip/{ip}",
                f"https://rdap.org/ip/{ip}",
                f"https://rdap.db.ripe.net/ip/{ip}"
        ]

        if retry == False:
            urls = [urls[0]]

        found = False
        for url in urls:
            time.sleep(timeout)
            if use_selenium:
                try:
                    browser.get(url)
                    raw = browser.find_element_by_tag_name("body").text
                    reference = browser.current_url.replace("https://", "").split("/")[0]
                    if "ERROR" not in raw:
                        found = True
                        break
                except:
                    raw = "ERROR - selenium"
            else:
                try:
                    resp = req.get(url, timeout=timeout, verify=False)
                    raw = resp.text
                    reference = resp.history[0].url if resp.history else resp.url
                    if resp.status_code == 200:
                        found = True
                        break
                except:
                    raw = "ERROR - requests"

        if found:
            result = json.loads(raw)
            result["reference"] = reference
            if "startAddress" in result:
                return result 

        print("[ERROR] IP {} could not be crawled: {}".format(ip, raw))    


    def _clean_tree(self, tree):
        """
        Our Interval tree depends on each IP subnet and its organization
        (that was passed as parameters, i.e., identified by Shodan or other system). 
        Because of that, the final tree can have interval collisions. A collision will 
        be two or more equal intervals but with different organizations. 
        However, it doesn't mean an error in our crawler. For instance, it could be 
        due to a misidentification of the organization by Shodan or, if we are 
        processing old data, it could be due to changes in records.

        """

        print(f"[INFO] Cleaning Tree - Initial size: {len(tree)}. Analysis possible collisions...")

        # checking which ranges have collisions 
        # (i.e. same range but different org names)
        colisions = {}
        for t1 in tree.items():
            b1 = t1.begin
            e1 = t1.end
            for t2 in tree.items():
                b2 = t2.begin
                e2 = t2.end
                if (b1 == b2) and (e1 == e2) and (t1.data['org'] != t2.data['org']):
                    key = "{}_{}".format(b1, e1)
                    if key not in colisions:
                        colisions[key] = []
                    if t1.data not in colisions[key]:
                        colisions[key].append(t1.data)
                    if t2.data not in colisions[key]:    
                        colisions[key].append(t2.data)

        print(f"[INFO] Cleaning Tree - {len(colisions)} intervals with colisions.")
        to_remove = []
        for t1 in colisions:
            for b in colisions[t1][1:]:
                to_remove.append(b['block_id'])

        for elem in list(tree.items()):
            if elem.data['block_id'] in to_remove:
                tree.discard(elem)

        print("[INFO] Cleaning Tree - Final size: {} .".format(len(tree)))
        return  tree


# @udf
# def get_reverseDelegations(col1, col2):
#     """
#     Usage example: 
    
#     .withColumn(OUTPUT, get_reverseDelegations(
#                             to_json(col("nicbr_reverseDelegations")), 
#                             to_json(col("lacnic_reverseDelegations"))
#                )    
#     """
    
#     if col1:
#         col1 = json.loads(col1)
#         return json.dumps(col1)
#     elif col2:
#         col2 = json.loads(col2)
#         return json.dumps(col2)
#     else:
#         return None


    
@F.udf
def _get_remarks(remarks):
    """
    Usage example:
    
    .withColumn("remarks", get_remarks(col("remarks")))
    
    """
    tmp = ""
    blocklist = ["O objeto não possui todas as informações por políticas no servidor. Pode ser devido ao número total de consultas.", 
                 "ADDRESSES WITHIN THIS BLOCK ARE NON-PORTABLE", 
                 "BEGIN CERTIFICATE", "===REMARK===", 
                 "Reassignment information for this block is",
                 "-----------------------------", 
                 "To report network abuse",
                 "For troubleshooting,", "Geofeed", 
                 "The custodianship of this IP prefix is presently", "available at"]
    if remarks:
        for r in remarks:
            for f in r["description"]:
                notFound = True
                for b in blocklist:
                    if b in f:
                        notFound = False
                if notFound:
                    tmp += " - " + f
        if len(tmp) > 0:
            tmp = tmp[3:].replace("Owner: ", "").replace("Abuse POC: ", "")
            return tmp


@F.udf(returnType=LongType())
def _convert_to_int(ip):
    """
    Convert IP from string (octets) to integer
    """
    return int(ipaddress.IPv4Address(ip))


def _get_events(events):
    """
    Returns creation and last change timestamp as new columns.
    """
    
    reg = None
    last = None
    
    if events:
        for e in events:
            d = e["eventDate"].split("T")[0]
            if e["eventAction"] == "registration":
                reg = datetime.strptime(d, "%Y-%m-%d").date()
            elif e["eventAction"] == "last changed":
                tmp = datetime.strptime(d, "%Y-%m-%d").date()
                if last:
                    if last < tmp:
                        last = tmp
                else:
                    last = tmp

    return reg, last

schema1 = StructType()\
    .add(StructField("registration",DateType(),True))\
    .add(StructField("last_changed",DateType(),True))

@F.udf(returnType=schema1)
def _get_events_udf(events):
    return _get_events(events)

schema3 = ArrayType(StructType()\
    .add(StructField("start_ip_str",StringType(),True))\
    .add(StructField("end_ip_str",StringType(),True)),True)

@F.udf(returnType=schema3)
def _get_cid(cids, start_ip, end_ip):

    def get_endAddress(ip, subnet):
        ip = ipaddress.ip_network(ip + "/" + str(subnet))[-1]
        return str(ip)

    ips = []
    if cids:
        cids = json.loads(cids)
        for cid in cids:
            length = cid["length"]
            startAddress = cid["v4prefix"]
            if startAddress:
                endAddress = get_endAddress(startAddress, length)
                ips.append([startAddress, endAddress])

    if len(ips) == 0:
        ips.append([start_ip, end_ip])
    return ips  

    
schema2 = StructType()\
    .add(StructField("whois_legal_representative", StringType(),True))\
    .add(StructField("whois_identifier", StringType(),True))\
    .add(StructField("whois_identifier_type", StringType(),True))\
    .add(StructField("registrant_name", StringType(),True))\
    .add(StructField("registrant_email", StringType(),True))\
    .add(StructField("registrant_address", StringType(),True))\
    .add(StructField("registrant_registration", DateType(),True))\
    .add(StructField("registrant_last_changed", DateType(),True))\
    .add(StructField("administrative_name", StringType(),True))\
    .add(StructField("administrative_email", StringType(),True))\
    .add(StructField("administrative_address", StringType(),True))\
    .add(StructField("administrative_registration", DateType(),True))\
    .add(StructField("administrative_last_changed", DateType(),True))\
    .add(StructField("technical_name", StringType(),True))\
    .add(StructField("technical_email", StringType(),True))\
    .add(StructField("technical_address", StringType(),True))\
    .add(StructField("technical_registration", DateType(),True))\
    .add(StructField("technical_last_changed", DateType(),True))\
    .add(StructField("abuse_name", StringType(),True))\
    .add(StructField("abuse_email", StringType(),True))\
    .add(StructField("abuse_address", StringType(),True))\
    .add(StructField("abuse_registration", DateType(),True))\
    .add(StructField("abuse_last_changed", DateType(),True))

@F.udf(returnType=schema2)
def _process_entities_field(entities):
    """
    
    """  
    
    if entities:
        entities = json.loads(entities)       
        result = {}
        
        for entitie in entities:
            for role in entitie["roles"]:
                if role in ["registrant", "administrative", "technical", "abuse"]:
                    
                    if "legalRepresentative" in entitie:
                        if "whois_legal_representative" not in result:
                            result["whois_legal_representative"] = entitie["legalRepresentative"]
                        else:
                            raise Exception("More than one legal_representative")
                        
                        
                    name, email, adr = _collect_vcard2(entitie)
                    result[role+"_name"]= name
                    result[role+"_email"]= email
                    result[role+"_address"]= adr
                    
                    if "publicIds" in entitie:
                        for r in entitie["publicIds"]:
                            t = r['type']
                            if t in ["cnpj", "cpf", "eid"]:
                                if "whois_identifier" not in result:
                                    result["whois_identifier"] = r['identifier'].replace("/","").replace(".", "").replace("-", "")
                                    result["whois_identifier_type"] = t
                                else:
                                    raise Exception("More than one identifier")
                            else:
                                raise Exception("publicIds unknown: {}".format(r["type"]))
        
                    if "events" in entitie:
                        reg, last = _get_events(entitie["events"])
                        result[role+"_registration"] = reg
                        result[role+"_last_changed"] = last
                        
                    if "entities" in entitie:
                        for entitie_level2 in entitie["entities"]:
                            role2 = entitie_level2["roles"]
                            if role2 in ["registrant", "administrative", "technical", "abuse"]:
                                
                                name, email, adr = _collect_vcard2(entitie_level2)
                                result[role2+"_name"] = name
                                result[role2+"_email"] = email
                                result[role2+"_address"] = adr
            
                                if "events" in entitie_level2:
                                    reg, last = _get_events(entitie_level2["events"])
                                    result[role2+"_registration"] = reg
                                    result[role2+"_last_changed"] = last

            
        return result
    

def _collect_vcard2(entitie):
    name  = None
    email = None
    adr   = None
    
    if "vcardArray" in entitie:
        vcard = json.loads(entitie["vcardArray"][1])
        for content in vcard:
            if "fn" in content and name is None:
                name = content[-1]
            elif "email" in content:
                email = content[-1]
            elif "adr" in content:
                if "label" in content[1]:
                    adr = content[1]["label"].replace("\n", " ")
                else:
                    adr = " ".join(content[-1]).replace("\n", " ").strip()
    
    return name, email, adr  
