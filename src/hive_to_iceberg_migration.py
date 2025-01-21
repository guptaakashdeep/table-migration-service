"""Using Migrator Service to migrate from Hive to Iceberg"""

from core.spark_session_factory import IcebergSparkSession
from core.table_migrator import IcebergMigrator

## Code to migrate tables
if __name__ == "__main__":
    APP_NAME = "HiveToIcebergMigration"
    WAREHOUSE = "s3://bucket/warehouse"
    CATALOG = "spark_catalog"

    # Default catalog: 'spark_catalog'
    spark = IcebergSparkSession(
        APP_NAME, WAREHOUSE, catalog_type="glue"
    ).create_spark_session()

    # Name of HIVE Format table that needs to be migrated to Iceberg.
    TABLE_NAME = "test_db.test_table"
    migrator = IcebergMigrator(spark, TABLE_NAME, CATALOG)
    response = migrator.migrate_table()
    print(response)
