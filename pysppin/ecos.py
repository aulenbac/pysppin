import requests
import xmltodict
from bs4 import BeautifulSoup
from . import utils
from urllib.parse import urlparse

common_utils = utils.Utils()


class Tess:
    def __init__(self):
        self.description = 'Set of functions for working with the USFWS Threatened and Endangered Species System'
        self.tess_api_base = "https://ecos.fws.gov/ecp0/TessQuery?request=query&xquery=/SPECIES_DETAIL"

    def search(self, criteria):

        tess_result = common_utils.processing_metadata()
        tess_result["processing_metadata"]["status"] = "failure"
        tess_result["processing_metadata"]["status_message"] = "Search failed"
        if criteria.isdigit():
            tess_result["processing_metadata"]["api"] = f'{self.tess_api_base}[TSN={criteria}]'
            tess_result["parameters"]= {'tsn': criteria}
        else:
            tess_result["processing_metadata"]["api"] = f'{self.tess_api_base}[SCINAME="{criteria}"]'
            tess_result["parameters"]= {'Scientific Name': criteria}

        # Query the TESS XQuery service
        tess_response = requests.get(tess_result["processing_metadata"]["api"])

        if tess_response.status_code != 200:
            tess_result["processing_metadata"]["status"] = "error"
            tess_result["processing_metadata"]["status_message"] = f"HTTP Status Code: {tess_response.status_code}"
            return tess_result

        # Build an unordered dict from the TESS XML response (we don't care about ordering for our purposes here)
        tessDict = xmltodict.parse(tess_response.text, dict_constructor=dict)

        if "results" not in tessDict.keys() or tessDict["results"] is None:
            tess_result["processing_metadata"]["status"] = "failure"
            return tess_result

        tess_result["processing_metadata"]["status"] = "success"
        tess_result["data"] = tessDict["results"]

        return tess_result


class Ecos:
    def __init__(self):
        self.property_registry = [
            {
                'Properties': ['Status', 'Date Listed', 'Lead Region', 'Where Listed'],
                'Table Name': 'Current Listing Status Summary'},
            {
                'Properties': ['Date', 'Citation Page', 'Title'],
                'Table Name': 'Federal Register Documents'},
            {
                'Properties': ['Date', 'Citation Page', 'Title'],
                'Table Name': 'Special Rule Publications'},
            {
                'Properties': ['Date', 'Title', 'Plan Action Status', 'Plan Status'],
                'Table Name': 'Current Recovery Plan(s)'},
            {
                'Properties': ['Date', 'Citation Page', 'Title', 'Document Type'],
                'Table Name': 'Other Recovery Documents'},
            {
                'Properties': ['Date', 'Title'],
                'Table Name': 'Five Year Review'},
            {
                'Properties': ['HCP Plan Summaries'],
                'Table Name': 'Habitat Conservation Plans (HCP)'
            }
        ]
        self.property_mapping = {
            "title": "document_title",
            "link": "document_link",
            "date": "publication_date"
        }
        self.description = 'Set of functions for working with other parts of ECOS'

    def extract_js_function_value(self, string):
        return string[string.find('"') + len('"'):string.rfind('"')]

    def itis_tsn(self, ecos_soup):
        try:
            tsn = ecos_soup.find('div', {'class': 'taxonomy new-row'}).findNext('div').findNext('a')['href'].split('=')[-1]
            if tsn:
                return tsn
            else:
                return None
        except:
            return None

    def scrape_ecos(self, ecos_url):
        extracted_data = common_utils.processing_metadata()
        extracted_data["processing_metadata"]["api"] = ecos_url

        page = requests.get(ecos_url)
        soup = BeautifulSoup(page.content, "html.parser")

        if not soup:
            return extracted_data

        extracted_data["processing_metadata"]["status"] = "success"
        extracted_data["data"] = dict()
        extracted_data["data"]["ITIS TSN"] = self.itis_tsn(soup)

        html_title = soup.find('title')

        if html_title.text.find('(') > 0:
            extracted_data["data"]["Scientific Name"] = html_title.text.split('(')[1].split(')')[0].strip()
            extracted_data["data"]["Common Name"] = html_title.text.split('(')[0].replace('Species Profile for', '').strip()
        else:
            extracted_data["data"]["Scientific Name"] = html_title.text.replace('Species Profile for', '').strip()
            extracted_data["data"]["Common Name"] = None

        for section in soup.findAll('div', {'class': 'table-caption'}):
            table_title = section.text.replace("(learn more)", "").strip()
            next_table = section.findNext('table')
            table_header = next_table.find('thead')
            if table_header is not None:
                this_table = {
                    "Table Name": table_title
                }
                table_props = list()
                for index, prop in enumerate(table_header.select('tr')[0].select('th')):
                    table_props.append(prop.text.strip())
                this_table["Properties"] = table_props

                if next((t for t in self.property_registry if t == this_table), None) is not None:
                    tbody = next_table.find('tbody')
                    if tbody is not None:
                        extracted_data["data"][this_table["Table Name"]] = list()
                        for row in tbody.find_all('tr'):
                            this_record = dict()
                            for i, column in enumerate(row.select('td')):
                                if this_table["Table Name"] == "Current Listing Status Summary" and \
                                        this_table["Properties"][i] == "Status":
                                    value = self.extract_js_function_value(column.text.strip())
                                else:
                                    value = column.text.strip()

                                this_record[this_table["Properties"][i]] = value

                                if column.find('a'):
                                    link_href = column.find('a')["href"]
                                    parsed_link = urlparse(link_href)
                                    if len(parsed_link.scheme) == 0:
                                        parsed_parent_url = urlparse(ecos_url)
                                        link_href = f"{parsed_parent_url.scheme}://{parsed_parent_url.netloc}{link_href}"
                                    this_record["document_link"] = link_href

                            extracted_data["data"][this_table["Table Name"]].append(this_record)

        if "data" in extracted_data.keys():
            extracted_data["data"] = common_utils.integrate_recordset(
                extracted_data["data"],
                target_properties=["itis_tsn"]
            )

        return extracted_data
