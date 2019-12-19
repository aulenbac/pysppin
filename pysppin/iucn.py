import requests
import os
import re
from . import utils

common_utils = utils.Utils()

class Iucn:
    def __init__(self):
        self.iucn_api_base = "http://apiv3.iucnredlist.org/api/v3"
        self.iucn_species_api = f"{self.iucn_api_base}/species"
        self.iucn_threats_api = f"{self.iucn_api_base}/threats/species/id"
        self.iucn_habitats_api = f"{self.iucn_api_base}/habitats/species/id"
        self.iucn_measures_api = f"{self.iucn_api_base}/measures/species/id"
        self.iucn_citation_api = f"{self.iucn_api_base}/species/citation/id"
        self.iucn_resolvable_id_base = "https://www.iucnredlist.org/species/"
        self.doi_pattern_start = "http://dx.doi.org"
        self.doi_pattern_end = ".en"

        self.iucn_categories = {
            "NE": "Not Evaluated",
            "DD": "Data Deficient",
            "LC": "Least Concern",
            "NT": "Near Threatened",
            "VU": "Vulnerable",
            "EN": "Endangered",
            "CR": "Critically Endangered",
            "EW": "Extinct in the Wild",
            "EX": "Extinct",
            "LR/lc": "Least Concern (in review)",
            "LR/nt": "Near Threatened (in review)",
            "LR/cd": "Not Categorized (in review)"
        }

    def search_species(self, sppin_key, name_source=None):
        sppin_key_parts = sppin_key.split(":")
        scientificname = sppin_key_parts[1]

        result = common_utils.processing_metadata()
        result["sppin_key"] = sppin_key
        result["date_processed"] = result["processing_metadata"]["date_processed"]
        result["processing_metadata"]["api"] = f"{self.iucn_species_api}/{scientificname}"
        result["parameters"] = {
            "Scientific Name": scientificname,
            "Name Source": name_source
        }

        if "token_iucn" not in os.environ:
            result["processing_metadata"]["status"] = "error"
            result["processing_metadata"]["status_message"] = "API token not present to run IUCN Red List query"
            return result

        iucn_response = requests.get(
            f'{result["processing_metadata"]["api"]}?token={os.environ["token_iucn"]}'
        )

        if iucn_response.status_code != 200:
            result["processing_metadata"]["status"] = "error"
            result["processing_metadata"]["status_message"] = "IUCN API returned an unprocessable result"
            return result

        iucn_species_data = iucn_response.json()

        #if a token is passed but it is not valid status code == 200 but you get a message returned "Token not valid!"
        if "message" in iucn_species_data.keys() and iucn_species_data["message"]=="Token not valid!":
            result["processing_metadata"]["status"] = "failure"
            result["processing_metadata"]["status_message"] = iucn_species_data["message"]
            return result


        if "result" not in iucn_species_data.keys() or len(iucn_species_data["result"]) == 0:
            result["processing_metadata"]["status"] = "failure"
            result["processing_metadata"]["status_message"] = "Species Name Not Found"
            return result

        result["processing_metadata"]["status"] = "success"
        result["processing_metadata"]["status_message"] = "Species Name Matched"

        result["data"] = {
            "iucn_taxonid": iucn_species_data['result'][0]['taxonid'],
            "iucn_status_code": iucn_species_data['result'][0]['category'],
            "iucn_status_name": self.iucn_categories[iucn_species_data['result'][0]['category']],
            "record_date": iucn_species_data['result'][0]['assessment_date'],
            "iucn_population_trend": iucn_species_data['result'][0]['population_trend'],
        }

        iucn_citation_response = requests.get(
            f"{self.iucn_citation_api}/{result['data']['iucn_taxonid']}?token={os.environ['token_iucn']}"
        ).json()

        result["data"]["citation_string"] = iucn_citation_response["result"][0]["citation"]

        regex_string_secondary_id = f"e\.T{result['data']['iucn_taxonid']}A(.*?)\."
        match_secondary_id = re.search(regex_string_secondary_id,
                                              result["data"]["citation_string"])
        if match_secondary_id is not None:
            result["data"]["iucn_secondary_identifier"] = match_secondary_id.group(1)
            result["data"]["resolvable_identifier"] = \
                f"{self.iucn_resolvable_id_base}" \
                f"{result['data']['iucn_taxonid']}/" \
                f"{match_secondary_id.group(1)}"
        else:
            result["data"]["iucn_secondary_identifier"] = None
            result["data"]["resolvable_identifier"] = None

        regex_string_doi = f"{self.doi_pattern_start}(.*?){self.doi_pattern_end}"
        match_iucn_doi = re.search(regex_string_doi, result["data"]["citation_string"])

        if match_iucn_doi is not None:
            result["data"]["doi"] = f"{self.doi_pattern_start}{match_iucn_doi.group(1)}{self.doi_pattern_end}"
        else:
            result["data"]["doi"] = None

        return result

