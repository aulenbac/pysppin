import datetime
from collections import Counter
import random
import os
import json
from genson import SchemaBuilder
from jsonschema import validate
import pkg_resources
import sciencebasepy
import re
from ftfy import fix_text
import pandas as pd
import sqlite3
from sqlite_utils import Database
from pandas.io.json import json_normalize


class Sciencebase:
    def __init__(self):
        self.sbpy = sciencebasepy.SbSession()

    def collection_items(self, collection_id, fields="id"):
        '''
        Loops through specified ScienceBase collection to return all items in a list with a set of fields. This
        function handles the issue of looping through the ScienceBase pagination when you need to get more items than
        the maximum that can be returned. Note a max of 100,000 records can be returned using this method.
        :param collectionid: str, ScienceBase parent item ID
        :param fields: str, comma delimited string of ScienceBase Item fields to return
        :return: List of the ScienceBase child items under parent item
        '''
        filter = f"parentId={collection_id}"
        params = {"max": 100, "filter": filter, "fields": fields}

        item_list = list()

        items = self.sbpy.find_items(params)

        while items and 'items' in items:
            item_list.extend(items['items'])
            items = self.sbpy.next(items)

        return item_list


class Utils:
    def __init__(self):
        self.data = {}

    def processing_metadata(self, default_status="error"):
        packaged_stub = {
            "processing_metadata": {
                "status": default_status,
                "date_processed": datetime.datetime.utcnow().isoformat()
            }
        }
        return packaged_stub

    def get_cache(self, cache_name, cache_location):
        file_location = f"{cache_location}/{cache_name}"

        if not os.path.exists(file_location):
            raise ValueError(f'The cache file does not exist in the specified location: {file_location}')

        return pd.read_pickle(file_location)

    def cache_df(self, df, file_name, cache_location, file_type="pickle", ):
        cache_location = f"{cache_location}{cache_path}"

        if not os.path.exists(cache_location):
            raise ValueError(f'The cache location does not exist: {cache_location}')

        file_location = f"{cache_location}/{file_name}"

        if file_type == "pickle":
            df.to_pickle(file_location)
        elif file_type == "feather":
            df.to_feather(file_location)

        return file_location

    def key_in_cache(self, cache_name, cache_location, search_key, return_record=False):
        cache_data = self.get_cache(cache_name, cache_location)

        existing_record = cache_data[cache_data["processing_metadata.search_key"] == search_key]

        if existing_record.empty:
            return False

        if return_record:
            return True, existing_record.to_dict("records")

        return True

    def append_to_cache(self, cache_name, cache_location, new_record, return_cache=False):
        current_cache = self.get_cache(cache_name, cache_location)
        df_new_record = json_normalize(new_record)
        new_cache = pd.concat([current_cache, df_new_record], ignore_index=True, sort=False)

        self.cache_df(new_cache, cache_name, cache_location)

        if return_cache:
            return new_cache
        else:
            return True

    def filter_mq_list(self, mq_list, cache_name, operation="processable", cache_threshold=30):
        df_cache = self.get_cache(cache_name)

        search_key_list = [i["search_key"] for i in mq_list]
        start_date = datetime.datetime.now() + datetime.timedelta(-cache_threshold)

        if operation == "processable":
            not_processable = df_cache[
                (df_cache["processing_metadata.search_key"].isin(search_key_list))
                &
                (pd.to_datetime(df_cache["processing_metadata.date_processed"]) > pd.to_datetime(start_date))
                ]["processing_metadata.search_key"].tolist()

            new_list = [i for i in mq_list if i["search_key"] not in not_processable]

            return new_list

        elif operation == "flagged":
            in_cache_list = df_cache[df_cache["processing_metadata.search_key"].isin(search_key_list)][
                "processing_metadata.search_key"].tolist()
            flagged_list = [dict(item, **{'in_cache': False}) for item in mq_list]
            flagged_list = [dict(item, **{'in_cache': True}) for item in flagged_list if
                           item["search_key"] in in_cache_list]

            return flagged_list

    def doc_cache(self, cache_path, cache_data=None, return_sample=True):
        '''
        Caches a list of dictionaries as a JSON document array to a specified relative path and returns a sample.

        :param cache_path: relative file path to write to; will overwrite if it exists
        :param cache_data: list of dictionaries to cache as JSON document array
        :param return_sample: return a random sample for verification
        :return:
        '''
        if cache_data is not None:
            if not isinstance(cache_data, list):
                return "Error: cache_data needs to be a list of dictionaries"

            if len(cache_data) == 0:
                return "Error: cache_data needs to be a list with at least one dictionary"

            if not isinstance(cache_data[0], dict):
                return "Error: cache_data needs to be a list of dictionaries"

            try:
                with open(cache_path, "w") as f:
                    f.write(json.dumps(cache_data))
            except Exception as e:
                return f"Error: {e}"

        if not return_sample:
            return "Success"
        else:
            if not os.path.exists(cache_path):
                return "Error: file does not exist"

            try:
                with open(cache_path, "r") as f:
                    the_cache = json.loads(f.read())
            except Exception as e:
                return f"Error: {e}"

            if not isinstance(the_cache, list):
                return "Error: file does not contain an array"

            if not isinstance(the_cache[0], dict):
                return "Error: file does not contain an array of JSON objects (documents)"

            doc_number = random.randint(0, len(the_cache) - 1)
            return {
                "Doc Cache File": cache_path,
                "Number of Documents in Cache": len(the_cache),
                f"Document Number {doc_number}": the_cache[doc_number]
            }

    def generate_json_schema(self, data):
        '''
        Uses the genson package to introspect json type data and generate the skeleton of a JSON Schema document
        (Draft 6) for further documentation.

        :param data: must be one of the following - python dictionary object, python list of dictionaries, json string
        that can be loaded to a dictionary or list of dictionaries
        :return: json string containing the generated json schema skeleton
        '''
        if isinstance(data, str):
            data = json.loads(data)

        if isinstance(data, dict):
            data = [data]

        if len(data) == 0:
            return "Error: your list of objects (dictionaries) must contain at least one object to process"

        if not isinstance(data[0], dict):
            return "Error: your list must contain a dictionary type object"

        try:
            builder = SchemaBuilder()
            builder.add_schema({"type": "object", "properties": {}})
            for r in data:
                for k, v in r.items():
                    builder.add_object({k: v})
        except Exception as e:
            return f"Error: {e}"

        return builder.to_json()

    def validate_data(self, dataset, schema):
        if isinstance(dataset, str):
            dataset = json.loads(dataset)

        if isinstance(dataset, dict):
            dataset = [dataset]

        if len(dataset) == 0:
            return "Error: your list of objects (dictionaries) must contain at least one object to process"

        if not isinstance(dataset[0], dict):
            return "Error: your list must contain a dictionary type object"

        record_report = list()
        for record in dataset:
            try:
                validate(record, schema)
                record_report.append({
                    "record": record,
                    "valid": True
                })
            except Exception as e:
                record_report.append({
                    "record": record,
                    "valid": False,
                    "validator": e.validator,
                    "validator_message": e.message
                })

        return record_report

    def alter_keys(self, item, mappings, layer=None, key=None):
        if layer is None:
            layer = item
        if isinstance(item, dict):
            for k, v in item.items():
                self.alter_keys(v, mappings, item, k)
        if isinstance(key, str):
            for orig, new in mappings.items():
                if orig in layer.keys():
                    layer[new] = layer.pop(orig)

        return layer

    def integrate_recordset(self, recordset, target_properties=None):
        '''
        This function is a rudimentary attempt at providing a simplistic integration routine for simply mapping
        field names from specified source datasets to a set of preferred property names from the "common_properties"
        JSON Schema set of definitions. I included an aliases list there as an extra parameter on properties to house
        known aliases from known datasets. Future work needs to also include at least a schema compliance check at this
        point.

        :param recordset:
        :param target_properties:
        :return: recordset with applicable property names registered as aliases mapped to target/preferred names
        '''

        path = 'resources/common_properties.json'
        filepath = pkg_resources.resource_filename(__name__, path)
        with open(filepath, 'r') as f:
            common_properties = json.loads(f.read())
            f.close()

        # This is completely stupid, but I can't get my head around a combination of list and dict
        # comprehension to do this more elegantly right now
        mappings = dict()
        for k, v in common_properties["definitions"].items():
            if target_properties is None:
                if "aliases" in common_properties["definitions"][k]:
                    for alias in common_properties["definitions"][k]["aliases"]:
                        mappings[alias] = k
            else:
                if "aliases" in common_properties["definitions"][k] and k in target_properties:
                    for alias in common_properties["definitions"][k]["aliases"]:
                        mappings[alias] = k

        if isinstance(recordset, dict):
            recordset = [recordset]

        new_recordset = list()
        for record in recordset:
            new_recordset.append(self.alter_keys(record, mappings))

        return recordset

    def clean_scientific_name(self, scientificname):
        if isinstance(scientificname, float):
            return None

        nameString = str(scientificname)

        # Fix encoding translation issues
        nameString = fix_text(nameString)

        # Remove digits, we can't work with these right now
        nameString = re.sub(r'\d+', '', nameString)

        # Get rid of strings in parentheses and brackets (these might need to be revisited eventually, but we can
        # often find a match without this information)
        nameString = re.sub('[\(\[\"].*?[\)\]\"]', "", nameString)
        nameString = ' '.join(nameString.split())

        # Remove some specific substrings
        removeList = ["?", "Family "]
        nameString = re.sub(r'|'.join(map(re.escape, removeList)), '', nameString)

        # Change uses of "subsp." to "ssp." for ITIS
        nameString = nameString.replace("subsp.", "ssp.")

        # Particular words are used to describe variations or nuances in taxonomy but are not able to be used in
        # matching names at this time
        afterChars = ["(", " AND ", "/", " & ", " vs ", " undescribed ", ",", " formerly ", " near ", "Columbia Basin",
                      "Puget Trough", " n.sp. ", " n. ", " sp. ", " sp ", " pop. ", " spp. ", " cf. ", " ] "]
        nameString = nameString + " "
        while any(substring in nameString for substring in afterChars):
            for substring in afterChars:
                nameString = nameString.split(substring, 1)[0]
                nameString = nameString + " "

        nameString = nameString.strip()

        # Deal with cases where an "_" was used
        if nameString.find("_") != -1:
            nameString = ' '.join(nameString.split("_"))

        # Check to make sure there is actually a subspecies or variety name supplied
        if len(nameString) > 0:
            namesList = nameString.split(" ")
            if namesList[-1] in ["ssp.", "var."]:
                nameString = ' '.join(namesList[:-1])

        # Take care of capitalizing final cross indicator
        nameString = nameString.replace(" x ", " X ")

        return nameString.capitalize()

    def denormalize_dict(self, d):
        new_dict = dict()
        for key in d:
            if "." in key:
                if key.split(".")[0] not in new_dict:
                    new_dict[key.split(".")[0]] = dict()
                new_dict[key.split(".")[0]][key.split(".")[1]] = d[key]
            else:
                new_dict[key] = d[key]

        return new_dict

    def spp_queue_assembler(self, name_list, source):
        q_list = [
            {
                "source": source,
                "search_key": f"Scientific Name:{i}",
                "search_term": i
            } for i in name_list
        ]

        return q_list

    def tsn_queue_assembler(self, tsn_list, source):
        q_list = [
            {
                "source": source,
                "search_key": f"TSN:{i}",
                "search_term": i
            } for i in tsn_list
        ]

        return q_list


class AttributeValueCount:
    def __init__(self, iterable, *, missing=None):
        self._missing = missing
        self.length = 0
        self._counts = {}
        self.update(iterable)

    def update(self, iterable):
        categories = set(self._counts)
        for length, element in enumerate(iterable, self.length):
            categories.update(element)
            for category in categories:
                try:
                    counter = self._counts[category]
                except KeyError:
                    self._counts[category] = counter = Counter({self._missing: length})
                counter[element.get(category, self._missing)] += 1
        self.length = length + 1

    def add(self, element):
        self.update([element])

    def __getitem__(self, key):
        return self._counts[key]

    def summary(self, key=None):
        if key is None:
            return '\n'.join(self.summary(key) for key in self._counts)

        return '-- {} --\n{}'.format(key, '\n'.join(
            '\t {}: {}'.format(value, count)
            for value, count in self._counts[key].items()
        ))


class Sql:
    def __init__(self, cache_location=None):
        self.description = "Temporary way to externalize messages from processing"
        self.cache_location = cache_location

    def get_db(self, db_name):
        return Database(f"{self.cache_location}/{db_name}.db")

    def insert_record(self, db_name, table_name, record, mq=False):
        db = Database(sqlite3.connect(f"{self.cache_location}/{db_name}.db", check_same_thread=False))

        if not isinstance(record, dict):
            raise ValueError("Record must be a dictionary")

        if mq:
            record = {
                "date_inserted": datetime.datetime.utcnow().isoformat(),
                "body": record
            }

        return db[table_name].insert(record, hash_id="id").last_pk

    def bulk_insert(self, db_name, table_name, bulk_data):
        db = Database(f"{self.cache_location}/{db_name}.db")

        if not isinstance(bulk_data, list):
            raise ValueError("Bulk data must be a list")

        if not isinstance(bulk_data[0], dict):
            raise ValueError("Bulk data must contain a list of dictionary objects")

        db[table_name].insert_all(bulk_data, hash_id="id")

        return len(bulk_data)

    def get_single_record(self, db_name, table_name, json_to_dict=True):
        db = Database(f"{self.cache_location}/{db_name}.db")

        for row in db[table_name].rows_where("0 = 0"):
            if json_to_dict:
                record = dict()
                for k, v in row.items():
                    try:
                        record[k] = json.loads(v)
                    except:
                        record[k] = v
            else:
                record = row

            return record

    def get_all_records(self, db_name, table_name, json_to_dict=True):
        db = Database(f"{self.cache_location}/{db_name}.db")

        result_list = list()
        for row in db[table_name].rows:
            if json_to_dict:
                record = dict()
                for k, v in row.items():
                    try:
                        record[k] = json.loads(v)
                    except:
                        record[k] = v
            else:
                record = row
            result_list.append(record)

        if len(result_list) == 0:
            return None

        return result_list

    def get_select_records(self, db_name, table_name, where, value, json_to_dict=True):
        db = Database(f"{self.cache_location}/{db_name}.db")

        result_list = list()
        for row in db[table_name].rows_where(where, [value]):
            if json_to_dict:
                record = dict()
                for k, v in row.items():
                    try:
                        record[k] = json.loads(v)
                    except:
                        record[k] = v
            else:
                record = row
            result_list.append(record)

        if len(result_list) == 0:
            return None

        return result_list

    def delete_record(self, db_name, table_name, identifier):
        db = Database(f"{self.cache_location}/{db_name}.db")

        db[table_name].delete(identifier)

        return identifier

    def insert_sppin_props(self, db_name, table_name, props, identifiers):
        db = Database(f"{self.cache_location}/{db_name}.db")

        returns = list()
        for identifier in identifiers:
            returns.append(
                db[table_name].update(
                    identifier,
                    props,
                    alter=True
                )
            )

        return returns

    def sppin_key_current_record(self, table_name, sppin_key, currency_threshold=-30, db_name="sppin"):
        db = Database(sqlite3.connect(f"{self.cache_location}/{db_name}.db"))

        currency_date = (datetime.datetime.now() + datetime.timedelta(currency_threshold)).isoformat()

        where = "sppin_key = ? and date_processed > ?"
        values = [sppin_key, currency_date]

        result_list = list()
        for row in db[table_name].rows_where(where, values):
            record = dict()
            for k, v in row.items():
                try:
                    record[k] = json.loads(v)
                except:
                    record[k] = v
            result_list.append(record)

        if len(result_list) == 0:
            return None

        # This takes care of an issue where multiple records were being inserted for a given sppin_key value in
        # parallel processing
        if len(result_list) > 1:
            for result in result_list[1:]:
                self.delete_record(
                    db_name,
                    table_name,
                    result["id"]
                )

        return result_list[0]



