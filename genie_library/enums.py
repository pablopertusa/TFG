from enum import Enum

class Mode(Enum):
    dry = 0
    live = 1
    once = 2

class Schema(Enum):
    gold = "default/databricks/1732657096/lf_udm_prod/gold/"
    metric_views = "default/databricks/1732657096/lf_udm_prod/gold_metric_views/"
    telemetry_rw = "default/redshift/1717508090/gbd_lf_data_lake_prod/telemetry_rw/"

class Environment(Enum):
    databricks_runtime = 0
    other = 1