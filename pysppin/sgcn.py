import requests
from . import utils

common_utils = utils.Utils()


class Search:
    def __init__(self):
        self.description = "Set of functions for searching the Species of Greatest Conservation Need API"
        self.sgcn_spp_search_api = "https://api.sciencebase.gov/bis-api/api/v1/swap/nationallist"

    def search(self, scientificname, name_source=None):
        result = common_utils.processing_metadata()
        result["processing_metadata"]["status_message"] = "Not Matched"
        result["processing_metadata"]["status"] = "failure"
        result["processing_metadata"]["api"] = f"{self.sgcn_spp_search_api}?scientificname={scientificname}"

        result["parameters"] = {
            "Scientific Name": scientificname,
            "Name Source": name_source
        }

        r_search = requests.get(result["processing_metadata"]["api"]).json()
        sgcn_species = next((i["_source"]["properties"] for i in r_search["hits"]["hits"]
                                       if i["_source"]["properties"]["scientificname"] == scientificname), None)

        if sgcn_species is not None:
            result["processing_metadata"]["status_message"] = "Name Match"
            result["processing_metadata"]["status"] = "success"

            primaryKeysToPop = ["gid", "sgcn2005", "sgcn2015"]

            for key in primaryKeysToPop:
                sgcn_species.pop(key, None)

            result["data"] = sgcn_species

        return result
