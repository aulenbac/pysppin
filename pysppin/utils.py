from datetime import datetime
from collections import Counter
import random
import os
import json
from genson import SchemaBuilder
import pkg_resources
import sciencebasepy
import re
from ftfy import fix_text


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
                "date_processed": datetime.utcnow().isoformat()
            }
        }
        return packaged_stub

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
