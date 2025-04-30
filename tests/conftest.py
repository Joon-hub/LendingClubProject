"""
Test configuration file.
This file contains common fixtures and test data for the loan scoring system tests.
"""

import pytest
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from src.loan_scorer import LoanScorer
from src.config.scoring_config import (
    PAYMENT_HISTORY_POINTS,
    DEFAULT_HISTORY_POINTS,
    FINANCIAL_HEALTH_POINTS,
    GRADE_THRESHOLDS,
    COMPONENT_WEIGHTS,
    PAYMENT_THRESHOLDS
)

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    # Set Python environment variables
    os.environ['PYSPARK_PYTHON'] = '/opt/anaconda3/bin/python'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/opt/anaconda3/bin/python'
    
    spark = SparkSession.builder \
        .appName("LoanScorerTest") \
        .master("local[2]") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def loan_scorer(spark):
    """Create a LoanScorer instance for testing."""
    return LoanScorer(spark)

@pytest.fixture
def sample_loans_data(spark):
    """Create sample loans data for testing."""
    data = [
        ("L1", "M1", "Fully Paid", "A", 10000.0, 1000.0),
        ("L2", "M2", "Current", "B", 20000.0, 2000.0),
        ("L3", "M3", "Late", "C", 30000.0, 3000.0)
    ]
    schema = StructType([
        StructField("loan_id", StringType(), True),
        StructField("member_id", StringType(), True),
        StructField("loan_status", StringType(), True),
        StructField("grade", StringType(), True),
        StructField("funded_amount", DoubleType(), True),
        StructField("monthly_installment", DoubleType(), True)
    ])
    return spark.createDataFrame(data, schema)

@pytest.fixture
def sample_repayments_data(spark):
    """Create sample repayments data for testing."""
    data = [
        ("L1", 1000.0, 10000.0),
        ("L2", 2000.0, 15000.0),
        ("L3", 500.0, 5000.0)
    ]
    schema = StructType([
        StructField("loan_id", StringType(), True),
        StructField("last_payment_amount", DoubleType(), True),
        StructField("total_payment_received", DoubleType(), True)
    ])
    return spark.createDataFrame(data, schema)

@pytest.fixture
def sample_customers_data(spark):
    """Create sample customers data for testing."""
    data = [
        ("M1", "OWN", 50000.0),
        ("M2", "MORTGAGE", 75000.0),
        ("M3", "RENT", 25000.0)
    ]
    schema = StructType([
        StructField("member_id", StringType(), True),
        StructField("home_ownership", StringType(), True),
        StructField("total_high_credit_limit", DoubleType(), True)
    ])
    return spark.createDataFrame(data, schema)

@pytest.fixture
def sample_defaulters_data(spark):
    """Create sample defaulters data for testing."""
    data = [
        ("M1", 0, 0, 0, 0),  # Excellent credit
        ("M2", 1, 1, 0, 1),  # Good credit
        ("M3", 2, 2, 1, 2)   # Bad credit
    ]
    schema = StructType([
        StructField("member_id", StringType(), True),
        StructField("delinq_2yrs", IntegerType(), True),
        StructField("pub_rec", IntegerType(), True),
        StructField("pub_rec_bankruptcies", IntegerType(), True),
        StructField("inq_last_6mths", IntegerType(), True)
    ])
    return spark.createDataFrame(data, schema)

@pytest.fixture(scope="session")
def sample_bad_customers_data(spark):
    """Create sample bad customers data for testing."""
    data = [("M4",)]
    schema = ["member_id"]
    return spark.createDataFrame(data, schema) 