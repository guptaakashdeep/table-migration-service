from abc import ABC, abstractmethod
from typing import Dict
from pyspark.sql import SparkSession


class SparkSessionFactory(ABC):
    """Abstract base class for creating SparkSession instances."""

    @abstractmethod
    def create_spark_session(self) -> SparkSession:
        """Create a SparkSession instance."""
        pass

    @abstractmethod
    def set_table_spec_config(self, builder: SparkSession.builder) -> SparkSession.builder:
        """Set table configuration for the SparkSession."""
        pass

    @abstractmethod
    def set_additional_config(self, spark: SparkSession) -> SparkSession:
        """Set additional configuration for the SparkSession."""
        pass


class IcebergSparkSession(SparkSessionFactory):
    """Concrete implementation for creating SparkSession instances with Iceberg support."""

    def __init__(
        self,
        app_name: str,
        warehouse: str,
        catalog_name: str = "spark_catalog",
        catalog_type: str = "hadoop",
        master: str = "yarn",
        additional_configs: Dict[str, str] = None,
        spj_enabled: bool = True
    ):
        self.app_name = app_name
        self.warehouse = warehouse
        self.catalog_name = catalog_name
        self.catalog_type = catalog_type
        self.master = master
        self.additional_configs = additional_configs if additional_configs else {}
        self.spj_enabled = spj_enabled

    def _create_spark_builder(self) -> SparkSession.builder:
        """Create a SparkSession builder instance."""
        builder = SparkSession.builder.appName(self.app_name).master(self.master)
        return builder

    def _set_glue_config(self, builder: SparkSession.builder) -> SparkSession.builder:
        """Set AWS Glue Configuration for using Iceberg."""
        return (
                builder.config(
                    f"spark.sql.catalog.{self.catalog_name}.catalog-impl",
                    "org.apache.iceberg.aws.glue.GlueCatalog"
                )
                .config(
                    f"spark.sql.catalog.{self.catalog_name}.io-impl",
                    "org.apache.iceberg.aws.s3.S3FileIO"
                )
            )

    def _set_common_configs(self, builder: SparkSession.builder) -> SparkSession.builder:
        """Set common Iceberg Session Config"""
        if self.catalog_name == "spark_catalog":
            builder = builder.config(
                    f"spark.sql.catalog.{self.catalog_name}",
                    "org.apache.iceberg.spark.SparkSessionCatalog"
                )
        else:
            builder = builder.config(
                    f"spark.sql.catalog.{self.catalog_name}",
                    "org.apache.iceberg.spark.SparkCatalog"
                )
        # Additional common configs
        builder = builder.config(
            f"spark.sql.catalog.{self.catalog_name}.warehouse", 
            self.warehouse
            ).config(
                "spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            )
        return builder

    def set_table_spec_config(self, builder: SparkSession.builder):
        builder = self._set_common_configs(builder)
        if self.catalog_type == "glue":
            builder = self._set_glue_config(builder)
        elif self.catalog_type == "hadoop":
            builder = builder.config(
                f"spark.sql.catalog.{self.catalog_name}.type",
                "hadoop"
            )
        else:
            raise ValueError(f"Catalog type {self.catalog_type} is not supported.")
        return builder

    def set_additional_config(self, spark):
        # Can be used to define multiple catalogs.
        if self.additional_configs:
            for key, value in self.additional_configs.items():
                spark.conf.set(key, value)
        if self.spj_enabled:
            spark.conf.set("spark.sql.sources.v2.bucketing.enabled","true")
            spark.conf.set("spark.sql.sources.v2.bucketing.pushPartValues.enabled","true")
            spark.conf.set("spark.sql.iceberg.planning.preserve-data-grouping","true")
            spark.conf.set("spark.sql.requireAllClusterKeysForCoPartition","false")
            spark.conf.set("spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled","true")
        return spark

    def create_spark_session(self) -> SparkSession:
        """Create a SparkSession instance with Iceberg support."""
        spark = self.set_table_spec_config(self._create_spark_builder()).getOrCreate()
        return self.set_additional_config(spark)


def get_spark_session(table_format="iceberg",**kwargs):
    """Get a SparkSession instance with Iceberg support."""
    if table_format == "iceberg":
        app_name = kwargs.get("app_name")
        warehouse = kwargs.get("warehouse_path")
        spark_session_factory = IcebergSparkSession(app_name, warehouse, **kwargs)
    else:
        raise ValueError(f"Table format {table_format} is not supported.")
    return spark_session_factory.create_spark_session()
