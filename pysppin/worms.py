import requests
from . import utils

common_utils = utils.Utils()

class Worms:
    def __init__(self):
        self.description = 'Set of functions for working with the World Register of Marine Species'
        self.filter_ranks = ["kingdom", "phylum", "class", "order", "family", "genus"]
        self.worms_url_base = "http://www.marinespecies.org/aphia.php?p=taxdetails&id="

    def get_worms_search_url(self, searchType,target):
        if searchType == "ExactName":
            return f"http://www.marinespecies.org/rest/AphiaRecordsByName/{target}?like=false&marine_only=false&offset=1"
        elif searchType == "FuzzyName":
            return f"http://www.marinespecies.org/rest/AphiaRecordsByName/{target}?like=true&marine_only=false&offset=1"
        elif searchType == "AphiaID":
            return f"http://www.marinespecies.org/rest/AphiaRecordByAphiaID/{str(target)}"
        elif searchType == "searchAphiaID":
            return f"http://www.marinespecies.org/rest/AphiaIDByName/{str(target)}?marine_only=false"

    def build_worms_taxonomy(self, wormsData):
        taxonomy = []
        for taxRank in self.filter_ranks:
            taxonomy.append({
                "rank": taxRank,
                "name": wormsData[taxRank]
            })
        taxonomy.append({
            "rank": "Species",
            "name": wormsData["valid_name"]
        })
        return taxonomy

    def search(self, sppin_key, name_source=None, source_date=None):

        sppin_key_parts = sppin_key.split(":")

        headers = {'content-type': 'application/json'}

        worms_result = common_utils.processing_metadata()
        worms_result["sppin_key"] = sppin_key
        worms_result["date_processed"] = worms_result["processing_metadata"]["date_processed"]
        worms_result["processing_metadata"]["status"] = None
        worms_result["processing_metadata"]["status_message"] = "Not Matched"

        if name_source is not None:
            worms_result["processing_metadata"]["name_source"] = name_source

        if source_date is not None:
            worms_result["processing_metadata"]["source_date"] = source_date

        worms_data = list()
        aphia_ids = list()

        url_exact_match = self.get_worms_search_url("ExactName", sppin_key_parts[1])
        name_results_exact = requests.get(url_exact_match, headers=headers)

        if name_results_exact.status_code == 200:
            worms_doc = name_results_exact.json()[0]
            worms_doc["biological_taxonomy"] = self.build_worms_taxonomy(worms_doc)
            worms_result["processing_metadata"]["api"] = url_exact_match
            worms_result["processing_metadata"]["status"] = "success"
            worms_result["processing_metadata"]["status_message"] = "Exact Match"
            worms_data.append(worms_doc)
            if worms_doc["AphiaID"] not in aphia_ids:
                aphia_ids.append(worms_doc["AphiaID"])
        else:
            url_fuzzy_match = self.get_worms_search_url("FuzzyName", sppin_key_parts[1])
            worms_result["processing_metadata"]["api"] = url_fuzzy_match
            name_results_fuzzy = requests.get(url_fuzzy_match, headers=headers)
            if name_results_fuzzy.status_code == 200:
                worms_doc = name_results_fuzzy.json()[0]
                worms_doc["biological_taxonomy"] = self.build_worms_taxonomy(worms_doc)
                worms_result["processing_metadata"]["status"] = "success"
                worms_result["processing_metadata"]["status_message"] = "Fuzzy Match"
                worms_data.append(worms_doc)
                if worms_doc["AphiaID"] not in aphia_ids:
                    aphia_ids.append(worms_doc["AphiaID"])

        if len(worms_data) > 0 and "valid_AphiaID" in worms_data[0].keys():
            valid_aphiaid = worms_data[0]["valid_AphiaID"]
            while valid_aphiaid is not None:
                if valid_aphiaid not in aphia_ids:
                    url_aphiaid = self.get_worms_search_url("AphiaID", valid_aphiaid)
                    aphiaid_results = requests.get(url_aphiaid, headers=headers)
                    if aphiaid_results.status_code == 200:
                        worms_doc = aphiaid_results.json()
                        # Build common biological_taxonomy structure
                        worms_doc["biological_taxonomy"] = self.build_worms_taxonomy(worms_doc)
                        worms_result["processing_metadata"]["api"] = url_aphiaid
                        worms_result["processing_metadata"]["status"] = "success"
                        worms_result["processing_metadata"]["status_message"] = "Followed Valid AphiaID"
                        worms_data.append(worms_doc)
                        if worms_doc["AphiaID"] not in aphia_ids:
                            aphia_ids.append(worms_doc["AphiaID"])
                        if "valid_AphiaID" in worms_doc.keys():
                            valid_aphiaid = worms_doc["valid_AphiaID"]
                        else:
                            valid_aphiaid = None
                    else:
                        valid_aphiaid = None
                else:
                    valid_aphiaid = None

        if len(worms_data) > 0:
            # Convert to common property names for resolvable_identifier, citation_string, and date_modified
            # from source properties
            new_worms_data = list()
            for record in worms_data:
                record["resolvable_identifier"] = record.pop("url")
                record["citation_string"] = record.pop("citation")
                record["date_modified"] = record.pop("modified")
                new_worms_data.append(record)

            worms_result["data"] = new_worms_data

        valid_worms_doc = next((d for d in worms_data if d["status"] == "accepted"), None)
        if valid_worms_doc is not None:
            worms_result["summary"] = {
                "scientificname": valid_worms_doc["scientificname"],
                "taxonomicrank": valid_worms_doc["rank"],
                "taxonomic_authority_url": f"{self.worms_url_base}{valid_worms_doc['AphiaID']}",
                "match_method": worms_result["processing_metadata"]["status_message"]
            }

        return worms_result

