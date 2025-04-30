"""
Test file for utility functions.
This file contains tests for all utility functions.
"""

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.utils.data_utils import (
    calculate_percentage,
    assign_score_based_on_threshold,
    clean_string_column,
    validate_dataframe
)
from src.utils.validation_utils import (
    check_data_quality,
    validate_loan_data,
    validate_customer_data,
    validate_repayment_data
)

def test_calculate_percentage(spark):
    """Test percentage calculation function."""
    # Create test data
    data = [
        (100.0, 200.0),  # 50%
        (75.0, 100.0),   # 75%
        (50.0, 200.0)    # 25%
    ]
    df = spark.createDataFrame(data, ["numerator", "denominator"])
    
    # Calculate percentages
    result_df = calculate_percentage(df, "numerator", "denominator", "percentage")
    
    # Verify results
    results = result_df.collect()
    assert results[0].percentage == 50.0
    assert results[1].percentage == 75.0
    assert results[2].percentage == 25.0

def test_assign_score_based_on_threshold(spark):
    """Test score assignment based on thresholds."""
    # Create test data
    data = [
        (0.9,),  # Excellent
        (0.6,),  # Good
        (0.3,),  # Fair
        (0.1,)   # Poor
    ]
    df = spark.createDataFrame(data, ["value"])
    
    # Define thresholds
    thresholds = {
        0.8: 800,  # Excellent
        0.6: 600,  # Good
        0.4: 400,  # Fair
        0.2: 200   # Poor
    }
    
    # Assign scores
    result_df = assign_score_based_on_threshold(df, "value", thresholds, "score")
    
    # Verify results
    results = result_df.collect()
    assert results[0].score == 800  # 0.9 >= 0.8
    assert results[1].score == 600  # 0.6 >= 0.6
    assert results[2].score == 200  # 0.3 < 0.4 but >= 0.2
    assert results[3].score == 0    # 0.1 < 0.2

def test_clean_string_column(spark):
    """Test string column cleaning function."""
    # Create test data
    data = [
        ("RENT",),
        ("MORTGAGE",),
        ("OWN",),
        ("OTHER",)
    ]
    df = spark.createDataFrame(data, ["home_ownership"])
    
    # Define replacements
    replacements = {
        "RENT": "RENT",
        "MORTGAGE": "MORTGAGE",
        "OWN": "OWN",
        "OTHER": "OTHER"
    }
    
    # Clean column
    result_df = clean_string_column(df, "home_ownership", replacements)
    
    # Verify results
    results = result_df.collect()
    assert results[0].home_ownership == "RENT"
    assert results[1].home_ownership == "MORTGAGE"
    assert results[2].home_ownership == "OWN"
    assert results[3].home_ownership == "OTHER"

def test_validate_dataframe(spark):
    """Test DataFrame validation function."""
    # Create valid DataFrame
    valid_data = [
        ("M1", "L1"),
        ("M2", "L2")
    ]
    valid_df = spark.createDataFrame(valid_data, ["member_id", "loan_id"])
    
    # Create invalid DataFrame (missing required columns)
    invalid_data = [
        (1, "L1"),
        (2, "L2")
    ]
    invalid_df = spark.createDataFrame(invalid_data, ["id", "loan_id"])
    
    # Verify validation
    assert validate_dataframe(valid_df) is True
    assert validate_dataframe(invalid_df) is False

def test_check_data_quality(spark):
    """Test data quality check function."""
    # Create test data
    data = [
        ("M1", 1000, 0),
        ("M2", 2000, 1),
        ("M3", 3000, 2),
        ("M4", None, 3)
    ]
    df = spark.createDataFrame(data, ["member_id", "amount", "count"])
    
    # Define checks
    checks = {
        "null_checks": ["amount"],
        "unique_checks": ["member_id"],
        "range_checks": {"amount": (0, 5000)}
    }
    
    # Check data quality
    results = check_data_quality(df, checks)
    
    # Verify results
    assert "null_checks" in results
    assert "unique_checks" in results
    assert "range_checks" in results
    assert results["null_checks"]["amount"]["null_count"] == 1

def test_validate_loan_data(spark):
    """Test loan data validation function."""
    # Create test data
    data = [
        ("L1", "M1", 10000.0, 1000.0),
        ("L2", "M2", 20000.0, 2000.0),
        ("L3", "M3", None, None)
    ]
    df = spark.createDataFrame(data, ["loan_id", "member_id", "funded_amount", "monthly_installment"])
    
    # Validate loan data
    results = validate_loan_data(df)
    
    # Verify results
    assert "null_checks" in results
    assert "unique_checks" in results
    assert "range_checks" in results

def test_validate_customer_data(spark):
    """Test customer data validation function."""
    # Create test data
    data = [
        ("M1", 50000.0, "OWN"),
        ("M2", 75000.0, "MORTGAGE"),
        ("M3", None, None)
    ]
    df = spark.createDataFrame(data, ["member_id", "annual_income", "home_ownership"])
    
    # Validate customer data
    results = validate_customer_data(df)
    
    # Verify results
    assert "null_checks" in results
    assert "unique_checks" in results
    assert "range_checks" in results

def test_validate_repayment_data(spark):
    """Test repayment data validation function."""
    # Create test data
    data = [
        ("L1", 1000.0, 5000.0),
        ("L2", 2000.0, 10000.0),
        ("L3", None, None)
    ]
    df = spark.createDataFrame(data, ["loan_id", "last_payment_amount", "total_payment_received"])
    
    # Validate repayment data
    results = validate_repayment_data(df)
    
    # Verify results
    assert "null_checks" in results
    assert "unique_checks" in results
    assert "range_checks" in results 