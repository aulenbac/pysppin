import requests
import xmltodict
from . import utils

common_utils = utils.Utils()


class Natureserve:
    def __init__(self):
        self.description = "Set of functions for working with the NatureServe APIs"
        self.ns_api_base = "https://services.natureserve.org/idd/rest/v1"
        self.us_name_search_api = "nationalSpecies/summary/nameSearch?nationCode=US"

    def search(self, sppin_key, name_source=None):
        '''
        This function searches the open public API for the NatureServe Explorer system of species information and
        assembles a basic summary of available information.

        :param sppin_key: Search term in the form "Scientific Name:<species scientific name>"
        :param name_source: String indicating where the scientific name was sourced for tracking purposes
        :return: Dictionary structure containing the results of the name search and the information from the API
        transformed to a dictionary from XML
        '''
        sppin_key_parts = sppin_key.split(":")
        scientificname = sppin_key_parts[1]

        result = common_utils.processing_metadata()
        result["sppin_key"] = sppin_key
        result["date_processed"] = result["processing_metadata"]["date_processed"]
        result["processing_metadata"]["status"] = "failure"
        result["processing_metadata"]["status_message"] = "Not Matched"
        result["processing_metadata"]["api"] = \
            f"{self.ns_api_base}/{self.us_name_search_api}&name={scientificname}"

        result["parameters"] = {
            "Scientific Name": scientificname,
            "Name Source": name_source
        }

        ns_api_result = requests.get(result["processing_metadata"]["api"])

        if ns_api_result.status_code != 200:
            return None
        else:
            ns_dict = xmltodict.parse(ns_api_result.text, dict_constructor=dict)

            if "species" not in ns_dict["speciesList"].keys():
                return result
            else:
                if isinstance(ns_dict["speciesList"]["species"], list):
                    ns_species = next(
                        (
                            r for r in ns_dict["speciesList"]["species"]
                            if r["nationalScientificName"] == scientificname
                         ),
                        None
                    )
                    if ns_species is not None:
                        result["data"] = ns_species
                        result["processing_metadata"]["status"] = "success"
                        result["processing_metadata"]["status_message"] = "Multiple Match"
                else:
                    result["data"] = ns_dict["speciesList"]["species"]
                    result["processing_metadata"]["status"] = "success"
                    result["processing_metadata"]["status_message"] = "Single Match"

        return result


