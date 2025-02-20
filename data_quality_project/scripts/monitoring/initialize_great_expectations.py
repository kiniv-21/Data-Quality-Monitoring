# scripts/initialize_great_expectations.py
import great_expectations as ge
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context import BaseDataContext

def initialize_great_expectations():
    # Create a basic configuration
    data_context_config = DataContextConfig(
        config_version=2,
        plugins_directory=None,
        config_variables_file_path="config/variables.yml",
        datasources={
            "source_postgres": {
                "execution_engine": {
                    "class_name": "SqlAlchemyExecutionEngine",
                    "credentials": {
                        "url": "${SOURCE_DB_CONNECTION_STRING}"
                    }
                },
                "data_connectors": {
                    "default_inferred_data_connector": {
                        "class_name": "InferredAssetSqlDataConnector",
                        "include_tables": True
                    }
                }
            }
        }
    )
    
    # Initialize context
    context = BaseDataContext(project_config=data_context_config)
    context.build_data_docs()
    
    print("Great Expectations initialized successfully!")

if __name__ == "__main__":
    initialize_great_expectations()