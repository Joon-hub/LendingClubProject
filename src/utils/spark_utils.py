"""
Spark utilities module.
This module contains functions for managing Spark sessions.
"""

import os
from pyspark.sql import SparkSession
from typing import Dict, Any

def create_spark_session(app_name: str, config: Dict[str, Any]) -> SparkSession:
    """
    Create a Spark session with the given configuration.
    
    Args:
        app_name (str): Name of the Spark application
        config (Dict[str, Any]): Spark configuration parameters
        
    Returns:
        SparkSession: Configured Spark session
    """
    # Set Python environment variables
    os.environ['PYSPARK_PYTHON'] = '/opt/anaconda3/bin/python'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/opt/anaconda3/bin/python'
    
    # Create Spark session builder
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4")
    
    # Add additional configurations
    for key, value in config.items():
        builder = builder.config(key, value)
    
    return builder.getOrCreate()

def get_spark_config() -> Dict[str, Any]:
    """
    Get default Spark configuration.
    
    Returns:
        Dict[str, Any]: Default Spark configuration
    """
    return {
        "spark.sql.legacy.timeParserPolicy": "LEGACY",
        "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "LEGACY",
        "spark.sql.legacy.parquet.datetimeRebaseModeInRead": "LEGACY"
    }

def stop_spark_session(spark: SparkSession) -> None:
    """
    Stop the Spark session.
    
    Args:
        spark (SparkSession): Spark session to stop
    """
    spark.stop() 