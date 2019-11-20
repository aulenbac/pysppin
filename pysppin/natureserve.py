import requests
import xmltodict
from . import utils

common_utils = utils.Utils()


class Natureserve:
    def __init__(self):
        self.description = "Set of functions for working with the NatureServe APIs"
        self.ns_api_base = "https://services.natureserve.org/idd/rest/v1"
        self.us_name_search_api = "nationalSpecies/summary/nameSearch?nationCode=US"

    def search(self, scientificname, name_source=None):

        result = common_utils.processing_metadata()
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


