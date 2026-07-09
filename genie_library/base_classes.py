import logging
from logging import Logger
import sys
from databricks.sdk import WorkspaceClient
import os
from genie_library.enums import Environment

class LoggingClass:
    """
    Base class for logging
    """
    @staticmethod
    def init_logger() -> Logger:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        if not logger.handlers:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.DEBUG)
            console_format = logging.Formatter('%(asctime)s - [%(levelname)s] - %(filename)s:%(lineno)d - %(message)s')
            console_handler.setFormatter(console_format)
            logger.addHandler(console_handler)
        return logger

class DatabricksController:
    """
    Base class for databricks auth
    """
    @staticmethod
    def get_workspace_client(environment: Environment = Environment.databricks_runtime):
        if environment != Environment.databricks_runtime:
            host = os.getenv("DATABRICKS_HOST")
            token = os.getenv("DATABRICKS_TOKEN")
            if not all([host, token]):
                raise ValueError("Missing env variables. Make sure you have defined DATABRICKS_HOST and DATABRICKS_TOKEN.")
            w = WorkspaceClient(host=host, token=token)
        else:
            w = WorkspaceClient()
        return w
