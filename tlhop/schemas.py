#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import pickle
import os
import yaml

from pyspark.sql.types import *

class Schemas(object):
    
    def __init__(self):
        
        self.original_schema = {}
        root_path = os.environ.get("TLHOP_DATASETS_PATH", None)
        if not root_path:
            raise Exception("TLHOP auxiliar folder not found, you must set the TLHOP_DATASETS_PATH.")
        
        path =  root_path + "/auxiliar/original_schema.pickle"
        with open(path, 'rb') as f:
            self.original_schema = pickle.load(f)
        
        self.shodan_mapping = [k for k, v in self.original_schema.items() if ('type": "struct"' in v) and (k[0] != "_")]
                                   
        self.external_schemas = {}
        
        custom_schemas_path = os.environ.get("TLHOP_CUSTOM_SCHEMAS", None) 
        if custom_schemas_path:
            with open(custom_schemas_path, 'r') as file:
                self.custom_schemas = yaml.safe_load(file)

            for k in self.custom_schemas:   
                dataset = self.custom_schemas[k]["dataset"]
                if dataset == "shodan":
                    self.shodan_mapping.append(k)
                else:
                    entry = f"{dataset}:{k}"
                    self.external_schemas[entry] = self.custom_schemas[k]["schema"]                   
        
        self.shodan_mapping = sorted(list(set(self.shodan_mapping)))
        

    def list_shodan_schemes(self):
        """
        List all Shodan's complexes columns that have a schema for easy retrieving.
        
        returns: a list with all complexes columns
        """
        return self.shodan_mapping
    
    def list_external_schemes(self):
        """
        List all other complexes columns present in external DataSets that have a schema.
        
        returns: a list with all complexes columns that a schema is available
        """
        
        return sorted(self.external_schemas.keys())
    
    def get_shodan_schema_by_column(self, column, force_original=False):
        """
        Retrieve the Spark's StructType for parser an column.
        
        :params column: Column name.
        :params force_original: True to only use the original Shodan schema (default, False)
        :returns: Spark's StructType
        """
        if (column in self.custom_schemas) and not force_original:
            schema_tmp = self.custom_schemas[column]['schema']
            struct_tmp = StructType.fromJson(json.loads(schema_tmp))
            return struct_tmp
        elif column in self.original_schema:
            schema_tmp = json.loads(self.original_schema[column])
            schema_tmp['fields'] = schema_tmp["type"]["fields"]
            schema_tmp["type"] = "struct"
            schema_tmp.pop('name', None)
            schema_tmp.pop('nullable', None)
            schema_tmp.pop('metadata', None)
            struct_tmp = StructType.fromJson(schema_tmp)
            return struct_tmp
        else:
            raise Exception("Schema not defined.")
            
    def get_external_schema_by_column(self, column, dataset_code="any"):
        """
        Retrieve the Spark's StructType for parser an column.
        
        :params column: Column name.
        :params dataset_code: Dataset code.
        :returns: Spark's StructType
        """
        entry = f"{dataset_code}:{column}"
        if (entry in self.external_schemas):
            schema_tmp = json.loads(self.external_schemas[entry])
            if schema_tmp["type"] == "struct":
                struct_tmp = StructType.fromJson(schema_tmp)
            else:
                struct_tmp = ArrayType.fromJson(schema_tmp)
            return struct_tmp
        else:
            raise Exception("Schema not defined.")
