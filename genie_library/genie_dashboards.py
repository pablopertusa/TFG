import json
import os
import uuid
from dotenv import load_dotenv
import re
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard
from genie_library.atlan import AtlanController
from genie_library.enums import Environment
from genie_library.genie_library import GenieSpaceController


ATLAN_BASE_URL_ENV = "ATLAN_BASE_URL"
ATLAN_SECRET_SCOPE_ENV = "ATLAN_SECRET_SCOPE"
ATLAN_API_KEY_SECRET_KEY_ENV = "ATLAN_API_KEY_SECRET_KEY"
DEFAULT_ATLAN_SECRET_SCOPE = "atlan"
DEFAULT_ATLAN_API_KEY_SECRET_KEY = "api-key"

class GenieDashboards(GenieSpaceController):
    """
    Class used to bootstrap Genie Spaces from Dashboards
    """
    def __init__(self, environment: Environment = Environment.databricks_runtime):
        self.logger = super().init_logger()
        self.w = super().get_workspace_client(environment=environment)
    
    def list_dashboards(self) -> list[Dashboard]:
        return [d for d in self.w.lakeview.list()] 
    
    def get_dashboard_by_id(self, dashboard_id: str) -> Dashboard:
        return self.w.lakeview.get(dashboard_id=dashboard_id)

    def get_dashboard_by_name(self, name: str) -> list[Dashboard]:
        result = []
        for dashboard in self.list_dashboards():
            if dashboard.display_name == name:
                result.append(dashboard)
        return result
    
    def _create_context_from_atlan(self, table_name: str) -> str | None:
        load_dotenv(dotenv_path="./.env")
        base_url = os.getenv(ATLAN_BASE_URL_ENV)
        secret_scope = os.getenv(ATLAN_SECRET_SCOPE_ENV, DEFAULT_ATLAN_SECRET_SCOPE)
        secret_key = os.getenv(ATLAN_API_KEY_SECRET_KEY_ENV, DEFAULT_ATLAN_API_KEY_SECRET_KEY)
        if not base_url:
            raise ValueError(f"{ATLAN_BASE_URL_ENV} is required to read Atlan context")
        if not secret_scope or not secret_key:
            raise ValueError(
                f"{ATLAN_SECRET_SCOPE_ENV} and {ATLAN_API_KEY_SECRET_KEY_ENV} are required to read Atlan context"
            )

        secret = WorkspaceClient().secrets.get_secret(scope=secret_scope, key=secret_key)
        api_key = secret.value
        if not api_key:
            raise ValueError(f"Atlan API key secret {secret_scope}/{secret_key} is empty or unavailable")

        controller = AtlanController(api_key=api_key, base_url=base_url)
        self.logger.debug(f"Gathering context from {table_name}")
        context = controller.get_terms(table_name=table_name)
        text = ""

        if context["error"] != 1:
            for term in context["terms"]:
                if term["description"] != None and term["description"] != "":
                    text += f"- Information from {table_name}\n{term['description']}"
                    if term["readme"] != None and term["readme"] != "":
                        text += f"\n{term['readme']}\n"
                    text += "\n\n"
                elif term["readme"] != None and term["readme"] != "":
                        text = f"Information from {table_name}\n{term['readme']}\n\n"
            text = "TERMS CONTEXT\n\n" + text
            return text
        else:
            self.logger.warning(f"No context from Atlan for {table_name}: {context['reason']}")
            return None
    
    def _clean_complex_params_from_query(self, query: list[str]) -> list[str]:
        expr = r"""array_contains\(\s*(:[`\w]+)\s*,\s*([`\w.'"]+(?: [`\w.'"]+)*)\s*\)"""
        result = []
        for line in query:
            clean = re.sub(expr, r"\2 = \1", line)
            result.append(clean)
        return result
    
    def _parse_dashboard_by_id(self, dashboard_id: str, write: bool = True) -> dict:
        dashboard = self.get_dashboard_by_id(dashboard_id=dashboard_id)
        serialized_dashboard = dashboard.serialized_dashboard
        if not serialized_dashboard:
            self.logger.error("Selected dashboard has no serialized_dashboard attribute, cannot proceed")
            raise ValueError("Invalid dashboard")

        serialized_dict = json.loads(serialized_dashboard)
        result = {}
        result["data"] = []
        result["joins"] = []
        result["queries"] = []
        for source in serialized_dict["datasets"]:
            new = {}
            # it is a SQL query
            if "queryLines" in source:
                new["display_name"] = source["displayName"]
                new["query_lines"] = self._clean_complex_params_from_query(source["queryLines"])
                if "parameters" in source:
                    new["parameters"] = []
                    for p in source["parameters"]:
                        new["parameters"].append(p)
                result["queries"].append(new)
            # it is a metric view
            elif "asset_name" in source:
                new["type"] = "metric_view"
                new["display_name"] = source["displayName"]
                new["source"] = source["asset_name"]
                result["data"].append(new)
            # it is a table
            elif "config" in source:
                new["type"] = "table"
                new["display_name"] = source["displayName"]
                new["source"] = source["config"]["source"]
                new["dimensions"] = source["config"]["dimensions"]
                result["data"].append(new)
                if "joins" in source["config"]:
                    for join in source["config"]["joins"]:
                        new_join = {}
                        new_join["name"] = join["name"]
                        new_join["left"] = source["config"]["source"]
                        new_join["right"] = join["source"]
                        normalized_on = join["on"].replace("source.", new["source"].split(".")[2] + ".").replace(new_join["name"], join["source"].split(".")[2])
                        new_join["on"] = normalized_on
                        result["joins"].append(new_join)
        
        if write:
            os.makedirs("./_d", exist_ok=True)
            with open("./_d/parsed_dashboard.json", "w") as file:
                json.dump(result, file, indent=4)
        
        return result
    
    def _create_serialized_space_from_parsed_dashboard(
        self,
        parsed_dashboard: dict,
        write: bool = True,
        include_atlan_context: bool = True,
    ) -> str:
        space = {
            "version": 2,
            "data_sources": {},
            "instructions": {
                "example_question_sqls": [],
                "join_specs": [],
                "text_instructions": []
            },
        }

        content_context = []
        for data in parsed_dashboard["data"]:
            if "tables" not in space["data_sources"]:
                space["data_sources"]["tables"] = []
            new = {
                "identifier": data["source"],
            }
            if include_atlan_context:
                context = self._create_context_from_atlan(table_name=data["source"])
                if context and len(context) > 0:
                    content_context.append(context)

            if data["type"] == "table":
                column_configs = []
                for dim in data["dimensions"]:
                    c = {
                        "column_name": dim["name"],
                        "enable_format_assistance": True
                    }
                    column_configs.append(c)
                # Why sorted? Ask databricks
                new["column_configs"] = sorted(column_configs, key=lambda x: x["column_name"])
            space["data_sources"]["tables"].append(new)

        space["instructions"]["text_instructions"] = [
            {
                "id": uuid.uuid4().hex,
                "content": content_context if len(content_context) > 0 else [""]
            }
        ]
        # Why sorted? Ask databricks
        if "tables" in space["data_sources"]:
            space["data_sources"]["tables"] = sorted(space["data_sources"]["tables"], key=lambda x: x["identifier"])
        
        for query in parsed_dashboard["queries"]:
            new_query = {
                "id": uuid.uuid4().hex,
                "question": [query["display_name"]],
                "sql": query["query_lines"]
            }
            if "parameters" in query:
                new_query["parameters"] = []
                for p in query["parameters"]:
                    new_query["parameters"].append({
                        "name": p["displayName"],
                        "type_hint": p["dataType"]
                    })
            space["instructions"]["example_question_sqls"].append(new_query)
        space["instructions"]["example_question_sqls"] = sorted(space["instructions"]["example_question_sqls"], key=lambda x: x["id"])

        for join in parsed_dashboard["joins"]:
            new_join = {
                "id": uuid.uuid4().hex,
                "left": {
                    "identifier": join["left"],
                    "alias": join["left"].split(".")[-1]
                },
                "right": {
                    "identifier": join["right"],
                    "alias": join["right"].split(".")[-1]
                },
                "sql": [
                    join["on"],
                    # TODO
                    # this relationship is a placeholder, should have a way to check the type
                    "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"
                ]
            }
            space["instructions"]["join_specs"].append(new_join)
        space["instructions"]["join_specs"] = sorted(space["instructions"]["join_specs"], key=lambda x: x["id"])

        if write:
            os.makedirs("./_d", exist_ok=True)
            with open("./_d/serialized_space.json", "w") as file:
                json.dump(space, file, indent=4)

        serialized_space = json.dumps(space)

        return serialized_space
    
    
    def create_genie_space_from_dashboard(
        self,
        dashboard_id: str,
        genie_space_title: str | None = None,
        genie_space_description: str | None = None,
        user_list: list[str] | None = None,
        warehouse_id: str = "38cb31e24512fd55",
        write_debug_files: bool = False,
        include_atlan_context: bool = False,
    ):
        parsed_dashboard = self._parse_dashboard_by_id(dashboard_id=dashboard_id, write=write_debug_files)
        serialized_space = self._create_serialized_space_from_parsed_dashboard(
            parsed_dashboard=parsed_dashboard,
            write=write_debug_files,
            include_atlan_context=include_atlan_context,
        )
        space_args = {
            "title": genie_space_title,
            "warehouse_id": warehouse_id,
            "description": genie_space_description,
            "serialized_space": serialized_space,
        }
        space = self.w.genie.create_space(**space_args)
        if user_list:
            self.assign_users_to_space(space.space_id, user_list=user_list)
        return space
    
    def get_official_dashboards(self) -> list[Dashboard]:
        dashboards = self.list_dashboards()
        result: list[Dashboard] = []
        for d in dashboards:
            if d.dashboard_id:
                d = self.get_dashboard_by_id(d.dashboard_id)
                if d.display_name and (d.display_name.startswith("LF ")) and \
                d.path and d.path.split("/")[2] == "dataos-prod-team-lf-bigdata-deployment-user@external.groups.hp.com":
                    result.append(d)
        return result
    
    def create_genie_spaces_from_official_dashboards(self, user_list: list[str] | None = None):
        dashboards = self.get_official_dashboards()
        for d in dashboards:
            if d.dashboard_id and d.display_name:
                genie_title = "PoC Pablo " + d.display_name
                self.create_genie_space_from_dashboard(dashboard_id=d.dashboard_id, genie_space_title=genie_title,
                                                       user_list=user_list)
