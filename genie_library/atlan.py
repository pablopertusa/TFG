from pyatlan.client.atlan import AtlanClient
from pyatlan.model.fluent_search import CompoundQuery, FluentSearch
from pyatlan.model.assets import Table, Column, Readme, AtlasGlossaryTerm, DatabricksMetricView, Asset
from genie_library.base_classes import LoggingClass
from genie_library.enums import Schema
from bs4 import BeautifulSoup

class AtlanController(LoggingClass):
    """
    Class used to perform actions in Atlan
    """
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
        self.atlan_client = AtlanClient(base_url=base_url, api_key=api_key)
        self.logger = super().init_logger()

    def search_table(self, name: str) -> list[Asset]:
        qn = Schema.gold.value + name
        request = (
            FluentSearch.select()
            .where(Table.QUALIFIED_NAME.eq(qn))
            .where(CompoundQuery.asset_type(Table))
            .where(CompoundQuery.active_assets())
            .include_relationship_attributes(True)
            .include_on_results(Table.ASSIGNED_TERMS) 
            .include_on_relations(AtlasGlossaryTerm.NAME) 
            .include_on_relations(AtlasGlossaryTerm.DESCRIPTION)
            .include_on_relations(AtlasGlossaryTerm.README)
        ).to_request()

        results = self.atlan_client.asset.search(criteria=request)
        results = [t for t in results]
        return results

    def search_metric_view(self, name: str) -> list[Asset]:
        qn = Schema.metric_views.value + name
        request = (
            FluentSearch.select()
            .where(Table.QUALIFIED_NAME.eq(qn))
            .where(CompoundQuery.asset_type(DatabricksMetricView))
            .where(CompoundQuery.active_assets())
            .include_relationship_attributes(True)
            .include_on_results(DatabricksMetricView.ASSIGNED_TERMS) 
            .include_on_relations(AtlasGlossaryTerm.NAME) 
            .include_on_relations(AtlasGlossaryTerm.DESCRIPTION)
            .include_on_relations(AtlasGlossaryTerm.README)
        ).to_request()

        results = self.atlan_client.asset.search(criteria=request)
        results = [t for t in results]
        return results
    
    def search_term_by_guid(self, guid: str) -> AtlasGlossaryTerm:
        return self.atlan_client.asset.get_by_guid(guid=guid, asset_type=AtlasGlossaryTerm, ignore_relationships=False)

    def search_readme_by_guid(self, guid: str) -> Readme:
        return self.atlan_client.asset.get_by_guid(guid=guid, asset_type=Readme, ignore_relationships=False)
    
    def __remove_html_tags(self, string: str) -> str:
        soup = BeautifulSoup(string, "html.parser")
        return soup.get_text()
    
    def _get_description_from_term(self, term: AtlasGlossaryTerm) -> str | None:
        if term.user_description:
            return term.user_description
        elif term.description:
            return term.description
        return None

    def _get_readme_from_term(self, term: AtlasGlossaryTerm) -> str | None:
        if term.readme:
            readme = self.search_readme_by_guid(term.readme.guid)
            if readme.description:
                return self.__remove_html_tags(readme.description)
        return None
    
    def get_terms(self, table_name: str) -> dict:
        response = {
            "error": 0,
            "reason": None,
            "terms": []
        }
        try:
            if table_name.split(".")[1] == "gold":
                name = table_name.split(".")[-1]
                results = self.search_table(name=name)
            else:
                name = table_name.split(".")[-1]
                results = self.search_metric_view(name=name)
            if len(results) > 1:
                response["error"] = 1
                response["reason"] = "There is not a unique table with that name"
                return response
            elif len(results) == 0:
                response["error"] = 1
                response["reason"] = "There is no table with that name"
                return response

            table = results[0]
            if table.assigned_terms:
                for term in table.assigned_terms:
                    term = self.search_term_by_guid(term.guid)
                    desc = self._get_description_from_term(term)
                    readme = self._get_readme_from_term(term)
                    new = {
                        "description": desc,
                        "readme": readme
                    }
                    response["terms"].append(new)
            else:
                response["error"] = 1
                response["reason"] = "There are no assigned terms"

        except Exception as e:
            response["error"] = 1
            response["reason"] = str(e)
        
        return response
