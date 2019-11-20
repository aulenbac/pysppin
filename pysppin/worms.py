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

    def search(self, scientificname, name_source=None):

        headers = {'content-type': 'application/json'}

        wormsResult = common_utils.processing_metadata()
        wormsResult["processing_metadata"]["status_message"] = "Not Matched"
        wormsResult["processing_metadata"]["search_key"] = f"Scientific Name:{scientificname}"

        wormsResult["parameters"] = {
            "Scientific Name": scientificname,
            "Name Source": name_source
        }

        wormsData = list()
        aphiaIDs = list()

        url_ExactMatch = self.get_worms_search_url("ExactName", scientificname)
        nameResults_exact = requests.get(url_ExactMatch, headers=headers)

        if nameResults_exact.status_code == 200:
            wormsDoc = nameResults_exact.json()[0]
            wormsDoc["biological_taxonomy"] = self.build_worms_taxonomy(wormsDoc)
            wormsResult["processing_metadata"]["api"] = url_ExactMatch
            wormsResult["processing_metadata"]["status"] = "success"
            wormsResult["processing_metadata"]["status_message"] = "Exact Match"
            wormsData.append(wormsDoc)
            if wormsDoc["AphiaID"] not in aphiaIDs:
                aphiaIDs.append(wormsDoc["AphiaID"])
        else:
            url_FuzzyMatch = self.get_worms_search_url("FuzzyName", scientificname)
            wormsResult["processing_metadata"]["api"] = url_FuzzyMatch
            nameResults_fuzzy = requests.get(url_FuzzyMatch, headers=headers)
            if nameResults_fuzzy.status_code == 200:
                wormsDoc = nameResults_fuzzy.json()[0]
                wormsDoc["biological_taxonomy"] = self.build_worms_taxonomy(wormsDoc)
                wormsResult["processing_metadata"]["status"] = "success"
                wormsResult["processing_metadata"]["status_message"] = "Fuzzy Match"
                wormsData.append(wormsDoc)
                if wormsDoc["AphiaID"] not in aphiaIDs:
                    aphiaIDs.append(wormsDoc["AphiaID"])

        if len(wormsData) > 0 and "valid_AphiaID" in wormsData[0].keys():
            valid_AphiaID = wormsData[0]["valid_AphiaID"]
            while valid_AphiaID is not None:
                if valid_AphiaID not in aphiaIDs:
                    url_AphiaID = self.get_worms_search_url("AphiaID", valid_AphiaID)
                    aphiaIDResults = requests.get(url_AphiaID, headers=headers)
                    if aphiaIDResults.status_code == 200:
                        wormsDoc = aphiaIDResults.json()
                        # Build common biological_taxonomy structure
                        wormsDoc["biological_taxonomy"] = self.build_worms_taxonomy(wormsDoc)
                        wormsResult["processing_metadata"]["api"] = url_AphiaID
                        wormsResult["processing_metadata"]["status"] = "success"
                        wormsResult["processing_metadata"]["status_message"] = "Followed Valid AphiaID"
                        wormsData.append(wormsDoc)
                        if wormsDoc["AphiaID"] not in aphiaIDs:
                            aphiaIDs.append(wormsDoc["AphiaID"])
                        if "valid_AphiaID" in wormsDoc.keys():
                            valid_AphiaID = wormsDoc["valid_AphiaID"]
                        else:
                            valid_AphiaID = None
                    else:
                        valid_AphiaID = None
                else:
                    valid_AphiaID = None

        if len(wormsData) > 0:
            # Convert to common property names for resolvable_identifier, citation_string, and date_modified
            # from source properties
            worms_data = list()
            for record in wormsData:
                record["resolvable_identifier"] = record.pop("url")
                record["citation_string"] = record.pop("citation")
                record["date_modified"] = record.pop("modified")
                worms_data.append(record)

            wormsResult["data"] = worms_data

        valid_worms_doc = next((d for d in wormsData if d["status"] == "accepted"), None)
        if valid_worms_doc is not None:
            wormsResult["summary"] = {
                "scientificname": valid_worms_doc["scientificname"],
                "taxonomicrank": valid_worms_doc["rank"],
                "taxonomic_authority_url": f"{self.worms_url_base}{valid_worms_doc['AphiaID']}"
            }

        return wormsResult

