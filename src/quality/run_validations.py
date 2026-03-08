import os
import great_expectations as gx
from great_expectations.core.batch import BatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration

# ==========================================
# Configuration
# ==========================================
# Authenticate to GCP using your downloaded key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../../credentials/gcp-key.json"

# Replace this with your exact GCP Project ID!
GCP_PROJECT_ID = "olist-data-pipeline-489511"  
DATASET_ID = "olist_analytics"

def main():
    # 1. Initialize Data Context
    # This connects to the great_expectations folder you just created
    context = gx.get_context()

    # 2. Configure BigQuery Datasource
    datasource_name = "my_bigquery_datasource"
    connection_string = f"bigquery://{GCP_PROJECT_ID}/{DATASET_ID}"
    
    # Add datasource if it doesn't exist
    if datasource_name not in context.datasources:
        context.add_datasource(
            name=datasource_name,
            class_name="Datasource",
            execution_engine={
                "class_name": "SqlAlchemyExecutionEngine",
                "connection_string": connection_string,
            },
            data_connectors={
                "default_inferred_data_connector_name": {
                    "class_name": "InferredAssetSqlDataConnector",
                    "name": "whole_table",
                },
            },
        )
        print(f"✅ Connected to BigQuery: {DATASET_ID}")

    # 3. Create an Expectation Suite
    suite_name = "olist_star_schema_suite"
    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    suite = context.get_expectation_suite(expectation_suite_name=suite_name)

    # ==========================================
    # 4. Define Data Quality Rules (Expectations)
    # ==========================================
    expectations = [
        # Rule 1: Null Values - Order IDs in fact_sales should never be null
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "order_id"}
        ),
        # Rule 2: Business Logic - Total sale amount should always be strictly positive
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "total_sale_amount", "min_value": 0.01}
        ),
        # Rule 3: Referential Integrity / Logic - Freight value cannot be negative
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "freight_value", "min_value": 0.00}
        )
    ]

    # Add rules to the suite
    for expectation in expectations:
        suite.add_expectation(expectation_configuration=expectation)
    context.save_expectation_suite(expectation_suite=suite, expectation_suite_name=suite_name)
    print(f"✅ Expectation Suite '{suite_name}' saved.")

    # 5. Run Validation on the fact_sales table
    batch_request = BatchRequest(
        datasource_name=datasource_name,
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="fact_sales",  # The dbt table we are testing
    )

    checkpoint_name = "fact_sales_checkpoint"
    context.add_or_update_checkpoint(
        name=checkpoint_name,
        config_version=1,
        class_name="SimpleCheckpoint",
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": suite_name,
            }
        ],
    )

    print("🚀 Running data quality validations on 'fact_sales'...")
    checkpoint_result = context.run_checkpoint(checkpoint_name=checkpoint_name)

    # 6. Output Results
    if checkpoint_result.success:
        print("🎉 All Data Quality Tests PASSED!")
    else:
        print("❌ Data Quality Tests FAILED. Check Data Docs for details.")

    # Build and open the visual HTML documentation
    context.build_data_docs()
    context.open_data_docs()

if __name__ == "__main__":
    main()