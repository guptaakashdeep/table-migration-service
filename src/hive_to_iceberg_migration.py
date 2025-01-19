"""Using Migrator Service to migrate from Hive to Iceberg"""

from core.spark_session_factory import IcebergSparkSession
# from core.table_migrator import IcebergMigrator

## Code to migrate tables
if __name__ == "__main__":
    APP_NAME = "HiveToIcebergMigration"
    WAREHOUSE = "s3://bucket/warehouse"

    spark = IcebergSparkSession(
        APP_NAME, WAREHOUSE, catalog_type="glue"
    ).create_spark_session()
