"""Base Class for Migration Class implementations"""
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession


class TableMigrator(ABC):
    """Abstract base class for table migration implemenations."""

    def __init__(self, spark: SparkSession, table: str):
        self.spark = spark
        self.table = table

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


