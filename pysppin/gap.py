import requests
import json
import geopandas as gpd
from shapely.geometry import box
from . import utils

common_utils = utils.Utils()

class Gap:
    def __init__(self):
        self.gap_species_collection = "527d0a83e4b0850ea0518326"
        self.sb_api_root = "https://www.sciencebase.gov/catalog/items"
        self.sb_geoserver = "https://www.sciencebase.gov/geoserver/CONUS_Range_2001v1/ows"
        self.bis_api_gap_state_metrics = "https://api.sciencebase.gov/bis-api/api/v1/gapmetrics/species/protection?feature_id=US_States_and_Territories%3Astate_fipscode%3A"

    def gap_species_search(self, scientificname, name_source=None, *args):
        '''
        This function looks for a GAP species in the core habitat maps collection in ScienceBase. If it finds a match,
        it assembles a combined GAP species document from available information in ScienceBase. This includes the basic
        metadata in the item (identifiers, names, etc.), the database parameters file, a cached set of ITIS
        information, and links to habitat map and range map items and services. In addition, the total bounding box
        for all seasons is calculated by retrieving the WFS feature for the range map. This provides a basic idea of
        the geospatial coverage to be expected for a species.

        :param scientificname: scientific name to search
        :return: Dictionary containing at least the processing metadata (date/time and URL used) and will contain a GAP
        Species document with all the information assembled for the given species.
        '''

        gap_result =  common_utils.processing_metadata()
        gap_result["processing_metadata"]["status"] = "failure"
        gap_result["processing_metadata"]["status_message"] = "Not Matched"

        gap_result["parameters"] = {
            "Scientific Name": scientificname,
            "Name Source": name_source
        }

        identifier_param = {
            "key": scientificname
        }
        gap_result["processing_metadata"]["api"] = \
            f"{self.sb_api_root}?parentId={self.gap_species_collection}" \
            f"&format=json&fields=identifiers,files,webLinks,distributionLinks,dates" \
            f"&filter=itemIdentifier%3D{identifier_param}"

        sb_result = requests.get(gap_result["processing_metadata"]["api"]).json()

        if sb_result["total"] == 1:
            gap_result["data"] = self.package_gap_species(self.package_habmap_item(sb_result["items"][0]))
            gap_result["processing_metadata"]["status"] = "success"
            gap_result["processing_metadata"]["status_message"] = "Exact Match"

        return gap_result

    def package_habmap_item(self, habmap_item):
        item = {
            "GAP Habitat Map Item": habmap_item["link"]["url"],
            "GAP Range Map Item": next((
                l["uri"] for l in habmap_item["webLinks"] if l["title"].find("Range Map") != -1
            ), None),
            "GAP Habitat Map WMS": next((
                l["uri"] for l in habmap_item["distributionLinks"] if "title" in l.keys() and
                                                                      l["title"] == "External WMS Service"
            ), None),
            "GAP Modeling Database Parameters URL": next((
                f["url"] for f in habmap_item["files"] if "title" in f.keys() and
                                                          f["title"] == "Machine Readable Habitat Database Parameters"
            ), None),
            "GAP ITIS Information URL": next((
                f["url"] for f in habmap_item["files"] if "title" in f.keys() and f["title"] == "ITIS Information"
            ), None),
            "GAP Habitat Map File Size": next((
                f["size"] for f in habmap_item["files"] if "title" in f.keys() and
                                                           f["title"] == "Habitat Map Raster Data"
            ), None),
            "GAP Habitat Map Last Updated": next((
                d["dateString"] for d in habmap_item["dates"] if d["type"] == "lastUpdated"
            ), None)
        }

        for i in habmap_item["identifiers"]:
            item[i["type"]] = i["key"]

        return item

    def package_rangemap_item(self, sppcode, rangemap_url):
        sb_range_map_item = requests.get(
            f"{rangemap_url}?format=json&fields=distributionLinks"
        ).json()

        rangemap_package = dict()
        rangemap_package["GAP Range Map WMS"] = next((
            l["uri"] for l in sb_range_map_item["distributionLinks"] if l["title"] == "External WMS Service"
        ), None)

        rangemap_package["Range Bounding Box"] = self.gap_spp_range_bbox(sppcode)

        return rangemap_package

    def package_gap_species(self, hab_map_package):
        hab_map_package.update(
            self.package_rangemap_item(
                sppcode=hab_map_package["GAP_SpeciesCode"],
                rangemap_url=hab_map_package["GAP Range Map Item"]
            )
        )

        if hab_map_package["GAP Modeling Database Parameters URL"] is not None:
            hab_map_package["GAP Modeling Database Parameters"] = json.loads(
                requests.get(
                    hab_map_package["GAP Modeling Database Parameters URL"]
                ).text
            )

        if hab_map_package["GAP ITIS Information URL"] is not None:
            hab_map_package["GAP ITIS Information"] = json.loads(
                requests.get(
                    hab_map_package["GAP ITIS Information URL"]
                ).text
            )

        return hab_map_package

    def gap_spp_range_bbox(self, sppcode):
        '''
        Queries the WFS for a given GAP species range and returns the total bounding box (all seasons) for the species.

        :param sppcode: GAP Species Code
        :return: Simple bounding box in a list in EPSG:4326
        '''
        params = dict(
            service="WFS",
            version="1.0.0",
            request="GetFeature",
            typeName="CONUS_Range_2001v1:Species_CONUS_Range_2001v1",
            outputFormat="json",
            CQL_FILTER=f"SppCode='{sppcode}'"
        )

        q = requests.Request("GET", self.sb_geoserver, params=params).prepare().url

        spp_range = gpd.read_file(q)
        spp_range = spp_range.to_crs({"init": "epsg:4326"})

        return spp_range.total_bounds.tolist()

    def gap_metrics_species(self, us_states, GAP_SpeciesCode, range_bbox):
        species_metrics_report = {
            "GAP_SpeciesCode": GAP_SpeciesCode,
            "State Metrics": list()
        }

        b = box(range_bbox[0], range_bbox[1], range_bbox[2], range_bbox[3])
        spp_bbox = gpd.GeoDataFrame(gpd.GeoSeries(b), columns=['geometry'])
        spp_bbox.crs = {'init': 'epsg:4326'}
        spp_bbox = spp_bbox.to_crs(us_states.crs)
        intersections = gpd.overlay(spp_bbox, us_states, how='intersection')
        for fips_code in intersections["STATEFP"]:
            state_gap_metrics = requests.get(f"{self.bis_api_gap_state_metrics}{fips_code}").json()
            species_state_metrics = [i for i in state_gap_metrics["result"] if
                                     i["sppcode"] == GAP_SpeciesCode]
            if len(species_state_metrics) > 0:
                species_metrics_report["State Metrics"].extend(species_state_metrics)

        return species_metrics_report
