"""Base Class for Migration Class implementations"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
from pyspark.sql.catalog import Column


class TableMigrator(ABC):
    """Abstract base class for table migration implemenations."""

    @abstractmethod
    def validate_source_table(self):
        """Validate if source table exists and is of expected type."""
        pass

    @abstractmethod
    def validate_migration(self):
        """Validate if the table migration is successful"""
        pass

    @abstractmethod
    def migrate_table(self):
        """Migrate table to target"""
        pass


class IcebergMigrator(TableMigrator):
    """Class for migrating Hive Table Format to Iceberg Table Format."""

    def __init__(
        self,
        spark: SparkSession,
        table: str,
        catalog: str = "spark_catalog",
        migration_type: str = "inplace",
        table_suffix: str = "_iceberg",
    ):
        self.spark = spark
        self.table = table
        self.catalog = catalog
        self.full_table_name = f"{self.catalog}.{self.table}"
        self.migration_type = migration_type
        self.table_suffix = table_suffix

    def validate_source_table(self) -> None:
        try:
            desc_df = self.spark.sql(f"DESC FORMATTED {self.table}")
            # check if table is not already iceberg
            provider = desc_df.filter(col("col_name") == "Provider").collect()
            if provider:
                provider_name = provider[0]["data_type"].lower()
                if provider_name == "iceberg":
                    raise ValueError(
                        f"Table {self.full_table_name} is already an iceberg table."
                    )
                if provider_name != "hive":
                    raise ValueError(
                        f"Table {self.table} is not a Hive Format table."
                    )
        except AnalysisException as e:
            if "Table or view not found" in str(e):
                raise ValueError(f"Table {self.full_table_name} does not exist.") from e
            raise e

    def _get_timestamp_cols(self, col_details: List[Column]) -> List[str]:
        timestamp_cols = list(
            filter(lambda column: column.dataType == "timestamp", col_details)
        )
        if timestamp_cols:
            return [field.name for field in timestamp_cols]
        return []

    def _get_partition_cols(self, col_details: List[Column]) -> List[str]:
        partition_cols = list(filter(lambda column: column.isPartition, col_details))
        if partition_cols:
            return [field.name for field in partition_cols]
        return []

    def _get_iceberg_table_location(self) -> str:
        desc_df = self.spark.sql(f"DESC FORMATTED {self.full_table_name}")
        src_location = desc_df.filter(col("col_name") == "Location").collect()[0][
            "data_type"
        ]
        location, table = src_location.rsplit("/", 1)
        return f"{location}/iceberg/{table}"

    def validate_migration(self) -> bool:
        try:
            self.spark.table(f"{self.full_table_name}.history")
            return True
        except Exception:
            return False

    def migrate_table(self) -> Dict[str, Any]:
        """Main method for table migration to Iceberg."""
        # Check if the table is existing and not an Iceberg Table
        try:
            self.validate_source_table()
        except ValueError as e:
            print("Table is already an iceberg table.")
            return {"table": self.table, "migration_status": "Failed", "error": str(e)}

        try:
            table_cols_detail = self.spark.catalog.listColumns(self.table)
            timestamp_cols = self._get_timestamp_cols(table_cols_detail)
            partition_cols = self._get_partition_cols(table_cols_detail)
            src_df = self.spark.read.table(self.table)

            # casting timestamp to timestamp_ntz
            # because Spark timestamp is with timezone specific,
            # and Iceberg timestamp is without timezone
            if timestamp_cols:
                cast_dict = {
                    cname: col(cname).cast("timestamp_ntz") for cname in timestamp_cols
                }
                src_df = src_df.withColumns(cast_dict)

            if partition_cols:
                partition_col_str = ", ".join(partition_cols)
            else:
                partition_col_str = ""
            mem_table = f"{self.table.split('.')[-1]}_mem"
            src_df.createOrReplaceTempView(mem_table)

            iceberg_table_location = self._get_iceberg_table_location()
            iceberg_table_name = f"{self.table}{self.table_suffix}"
            # CREATE new ICEBERG table without data
            # with same schema as source table with partitioning
            if partition_col_str:
                iceberg_ctas = f"""
                CREATE TABLE {self.catalog}.{iceberg_table_name} USING iceberg
                PARTITIONED BY ({partition_col_str})
                LOCATION '{iceberg_table_location}'
                AS SELECT * FROM {mem_table} LIMIT 0
                """
            else:
                iceberg_ctas = f"""
                CREATE TABLE {self.catalog}.{iceberg_table_name} USING iceberg
                LOCATION '{iceberg_table_location}'
                AS SELECT * FROM {mem_table} LIMIT 0
                """

            print("Executing CTAS: \n", iceberg_ctas)
            # CREATE Iceberg table with suffix
            self.spark.sql(iceberg_ctas)

            # check if table creation is completed
            if self.spark.catalog.tableExists(f"{self.catalog}.{iceberg_table_name}"):
                # add_files into iceberg table
                call_procedure = f"CALL {self.catalog}.system.add_files(table => '{iceberg_table_name}', source_table => '{self.table}')"
                print("Executing Migration Procedure: ", call_procedure)
                # TODO: get the output from here into a response object
                migration_df = self.spark.sql(call_procedure)

                # drop the actual table
                self.spark.sql(f"DROP TABLE {self.table}")

                # rename the Iceberg table to actual table name
                rename_table_stmt = f"ALTER TABLE {iceberg_table_name} RENAME TO {self.table}"
                print("Renaming Table: ", rename_table_stmt)
                self.spark.sql(rename_table_stmt)

                # validate migration
                if self.validate_migration():
                    return {
                        "table": self.full_table_name,
                        "migration_status": "Successful",
                        "migration_response": migration_df.select("added_files_count")
                        .collect()[0]
                        .asDict(),
                    }
                else:
                    return {
                        "table": self.full_table_name,
                        "migration_status": "Failed",
                        "error": "Migration Validation Failed.",
                    }
            else:
                raise Exception(f"{iceberg_table_name} Table creation failed.")

        except Exception as e:
            raise e
