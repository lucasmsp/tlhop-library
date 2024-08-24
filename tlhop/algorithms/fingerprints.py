#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
import pyspark.sql.functions as F
from pyspark.sql.functions import * # TODO: when using `events`

import xml.etree.ElementTree as ET

import hyperscan
import json
import glob
import collections
import builtins
import time
import os
import re
import re2
from functools import reduce
import cydifflib

class Fingerprints(object):

    _ERROR_MESSAGE_001 = "[ERROR] None active Spark session was found. Please start a new Spark session before use DataSets API."
    _ERROR_MESSAGE_002 = "[ERROR] Currently we only support `fingerprint_type` of type `raw` or `events`."
    
    _WARN_MESSAGE_001 = "[WARN] None fingerprints were found in `{}`."

    _INFO_MESSAGE_001 = "[INFO] {} patterns was loaded."


    _FINGERPRINTS_TYPE_RAW = "raw"
    _FINGERPRINTS_TYPE_EVENTS = "events"

    def __init__(self, fingerprints_path, fingerprints_type="raw"):

        self.spark_session = SparkSession.getActiveSession()
        if not self.spark_session:
            raise Exception(self._ERROR_MESSAGE_001)

        self.fingerprints_raw = []
        self.fingerprints_events = {'patterns': [], 'fingerprints': [], 'fields': set()}
        self.fingerprints_type = fingerprints_type
        self.fingerprints_path = os.path.expanduser(fingerprints_path)
        
        if self.fingerprints_type == self._FINGERPRINTS_TYPE_RAW:
        
            xml_files = glob.glob(self.fingerprints_path+"/*.xml")
            block_list = read_blocklist(self.fingerprints_path+"/block_list.txt")
            
            self.fingerprints_raw, self.tag_lookup, self.skipped_patterns = self._populate_fingerprint_raw(xml_files, block_list)

            if len(self.fingerprints_raw) == 0:
                print(self._WARN_MESSAGE_001.format(self.fingerprints_path))
            else:
                print(self._INFO_MESSAGE_001.format(len(self.fingerprints_raw)))
                
        elif self.fingerprints_type == self._FINGERPRINTS_TYPE_EVENTS:
            
            for input_file in glob.glob(self.fingerprints_path+"/*.xml"):
                self._populate_fingerprint_events(input_file)

            if len(self.fingerprints_events['patterns']) == 0:
                print(self._WARN_MESSAGE_001.format(self.fingerprints_path))
            else:
                print(self._INFO_MESSAGE_001.format(len(self.fingerprints_events['patterns'])))
        else:
            raise Exception(self._ERROR_MESSAGE_002)
         

    def run(self, input_df, keep_all_columns=False, keep_all_rows=False):
        """
        Method to analize all records and columns to map some specificic events of a record in a new `meta_events` column.
        The mapped events can be like: it has screenshots; it has some compromised database or has other type of vulnerability;
        and others.
    
        :return: Spark DataFrame 
        """
        
        if self.fingerprints_type == self._FINGERPRINTS_TYPE_EVENTS:
            
            self.output_col = "meta_events"

            fields = ",".join(['{"metadata":{},"name":"'+c+'","nullable":true,"type":"string"}' 
                               for c in self.fingerprints_events['fields']
                              ])
            json_schema =  '{"fields":[' + fields + '],"type":"struct"}'
            schema = StructType.fromJson(json.loads(json_schema))
            
            conditions_list = [F.when(pattern, F.from_json(F.lit(fingerprint), schema)) 
                               for pattern, fingerprint in zip(self.fingerprints_events['patterns'], self.fingerprints_events['fingerprints']) ]
            
            self.output = input_df.withColumn(self.output_col, F.array(*conditions_list))\
                .withColumn(self.output_col, F.expr("filter("+self.output_col+", x -> x is not null)"))\
                .withColumn(self.output_col, F.when(F.size(self.output_col) == 0, F.lit(None)).otherwise(F.col(self.output_col)))

            if not keep_all_rows:
                self.output = self.output.filter(F.size(self.output_col) > 0)

            if not keep_all_columns:
                self.output = self.output.select("shodan_id", "year", "date", "meta_module", 'timestamp', self.output_col)
                

        elif self.fingerprints_type == self._FINGERPRINTS_TYPE_RAW:

            self.output_col = "infer"
            tmp_df = input_df.filter(F.length("data") > 3)

            if not keep_all_columns:
                tmp_df = tmp_df.select("shodan_id", "year", "date", "meta_module", 'timestamp', "data")

            schema = tmp_df.schema
            to_prepend = [StructField(self.output_col, ArrayType(StringType()), True)]
            schema = StructType(schema.fields + to_prepend)

            patterns = self.fingerprints_raw
            tag_lookup = self.tag_lookup
            infer_function = lambda row: hyperscan_matchs(row, patterns, tag_lookup, keep_all_rows)
    
            self.output = tmp_df.rdd.mapPartitions(infer_function)\
                .toDF(schema)
        
        return self.output


    def _populate_fingerprint_events(self, input_file):
        """

        """
        for item in ET.parse(input_file).getroot().findall("fingerprint"):
            pattern = eval(item.get("pattern"))
            record = {}
            for elements in item.findall("param"):
                field = elements.get("name")
                value = elements.get("value")
                self.fingerprints_events['fields'].add(field)
                record[field] = value 
            self.fingerprints_events['patterns'].append(pattern)
            self.fingerprints_events['fingerprints'].append(json.dumps(record))


    def _populate_fingerprint_raw(self, files, blocklist):
        PATTERNS = []
        TAG_LOOKUP = []
        REMOVED_PATTERNS = []
    
        for file in files:
            tree =  ET.parse(file)
            fingerprints = []
            source_xml = os.path.basename(file)
            for item in tree.getroot().findall("fingerprint"):
                pattern = item.get("pattern")
                if (pattern not in blocklist) and (len(pattern.replace("^", "").replace("$", "")) > 2):
                    pattern = converting_pattern_to_data_compatibility(pattern)
                    description = item.find("description").text
                    example = item.find("example").text if item.find("example") is not None else None
                    pattern_clean = gen_temporary_pattern_clean(pattern)
                    fingerprint = {
                         #"description": description,
                         #"example": example, 
                         'source_xml': source_xml, 
                         'pattern': pattern,
                         'pattern_clean': pattern_clean,
                         'weight_pattern': len(pattern_clean)
                    }
                    
                    for elements in item.findall("param"):
                        fields = elements.get("name")
    
                        if "apache.variant" == fields:
                            fields = "apache.variant.name"
                        elif "version.version.version.version" in fields:
                            fields = fields.replace("version.version.version.version", "version_version_version_version")
                        elif "version.version.version" in fields:
                            fields = fields.replace("version.version.version", "version_version_version")
                        elif "version.version" in fields:
                            fields = fields.replace("version.version", "version_version")
                        
                        value = elements.get("value")
                        record = reduce(lambda res, cur: {cur: res}, reversed(fields.split(".")), value)
                        fingerprint = merge_nested_dicts(fingerprint, record)
                        
                    PATTERNS.append(pattern)
                    TAG_LOOKUP.append(fingerprint)
                else:
                    REMOVED_PATTERNS.append(pattern)
    
        return PATTERNS, TAG_LOOKUP, REMOVED_PATTERNS




def remove_symbol(symbol, pattern):
    return pattern.replace("\\"+symbol, "{TMP}")\
        .replace(symbol, "")\
        .replace("{TMP}", symbol)

    
def gen_temporary_pattern_clean(pattern):

    pattern = pattern.replace("\/","/")\
        .replace("\w", "")\
        .replace("\W", "")\
        .replace("\d", "")\
        .replace("\S", "")\
        .replace("\s", "")\
        .replace(".?", "")\
        .replace(")?", "")\
        .replace("(^|\s)", "")\
        .replace("(?:", "")

    pattern = re.sub("\[[^\]]+\]", "", pattern) # [...]
    pattern = re.sub("\{[\d,]+\}", "", pattern) # { ...}
    
    pattern = remove_symbol("^", pattern)
    pattern = remove_symbol("|", pattern)
    pattern = remove_symbol("$", pattern)
    pattern = remove_symbol(".", pattern)
    pattern = remove_symbol("*", pattern)
    pattern = remove_symbol("(", pattern)
    pattern = remove_symbol(")", pattern)
    pattern = remove_symbol("+", pattern)
    pattern = remove_symbol("-", pattern)
    pattern = remove_symbol("?", pattern)

    return pattern


def converting_pattern_to_data_compatibility(pattern):

    pattern = re.sub("\{0,\d+\}", "*", pattern) # \d*
    pattern = re.sub("\{\d+,\d+\}", "+", pattern) # +

    pattern = pattern.replace('(?i)', "")\
        .replace('(?m)', "")

    if pattern[0] == "^":
        pattern = pattern[1:] 
        
    if pattern[-1] == "$":
        pattern = pattern[:-1]

    # o ideal seria manter a abordagem abaixo, o
    # o problema que algumas regex do recog estão 
    # "erradas" e precisariamos ter um esforço de corrigir, por exemplo:
    # ^OpenSSH_for_Windows_([\d.]+)$  sendo que o esperado é SSH-2.0-OpenSSH_for_Windows_7.9
    # Sendo assim, vamos deixar as regex mais flexiveis e arrumar isso nos "pesos"
    # if pattern[0] == "^":
    #     pattern = "\s" + pattern[1:] 
        
    # if pattern[-1] == "$":
    #     pattern = pattern[:-1] + "\s"

    return pattern

    
def compute_confidence(pattern_clean, match):
    seq = cydifflib.SequenceMatcher(None, pattern_clean, match)
    size = builtins.sum(m.size for m in seq.get_matching_blocks())
    return size


def read_blocklist(filename):
    block_list = [ line.replace('"\n', "")[1:] for line in open("./source/block_list.txt")]
    block_list[-1] = block_list[-1][:-1] # removing last '"'
    return block_list

def merge_nested_dicts(a: dict, b: dict, path=[]):
    for key in b:
        if key in a:
            if not a[key]:
                a[key] = {}
                
            if not b[key]:
                b[key] = {}
                
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge_nested_dicts(a[key], b[key], path + [str(key)])
                
            elif a[key] != b[key]:
                print(a)
                print(b)
                raise Exception('Conflict at ' + '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a

def hyperscan_matchs(lines, patterns_list, tag_lookup, keep_all_rows=False):
    db = hyperscan.Database(mode=hyperscan.HS_MODE_BLOCK)
    patterns = [
        (pattern.encode("utf-8"), id, hyperscan.HS_FLAG_CASELESS | hyperscan.HS_FLAG_SINGLEMATCH) for id, pattern in enumerate(patterns_list)
    ]
    expressions, ids, flags = zip(*patterns)
    db.compile(expressions=expressions, ids=ids, elements=len(patterns), flags=flags)

    new_rows = []
    result = []
    
    def on_match(id, start, end, flags, context):
        result.append([id, end])
    
    for row in lines:
        result = []
        db.scan(row.data.encode("utf-8"), match_event_handler=on_match)
        if len(result) > 0:
            temp = row.asDict()
            temp['infer'] =  []
            
            for i in range(len(result)):
                id, offset  = result[i]
                sample = row.data[:offset+1]
                pattern_info = tag_lookup[id]
                matched = re2.search(pattern_info['pattern'].encode('ascii'), 
                                    sample.encode('utf-8'),
                                    re2.IGNORECASE).group(0).decode('utf-8')
                
                if len(matched) > 2:
                    pattern_info['match'] = matched
                    pattern_info['weight'] = compute_confidence(pattern_info['pattern_clean'], matched)                     
                    temp['infer'].append(pattern_info)
                    
            if len(temp['infer']) == 0:
                temp['infer'] = None
            else:
                temp['infer'] = [json.dumps(d) for d in sorted(temp['infer'], key=lambda d: d['weight'], reverse=True)]
            
            new_rows.append(Row(**temp))
            
        elif keep_all_rows:
            temp = row.asDict()
            temp['infer'] = None
            new_rows.append(Row(**temp))

    return new_rows



        