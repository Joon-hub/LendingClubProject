"""
Utility functions for data validation.
This module provides functions to validate data quality and integrity.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnull
from typing import List, Dict, Any

def check_data_quality(df: DataFrame, 
                      checks: Dict[str, List[str]]) -> Dict[str, Any]:
    """
    Perform data quality checks on a DataFrame.
    
    Args:
        df (DataFrame): DataFrame to check
        checks (Dict[str, List[str]]): Dictionary of checks to perform
        
    Returns:
        Dict[str, Any]: Results of the checks
    """
    results = {}
    
    # Check for null values
    if 'null_checks' in checks:
        null_results = {}
        for column in checks['null_checks']:
            null_count = df.filter(col(column).isNull()).count()
            null_results[column] = {
                'null_count': null_count,
                'total_count': df.count(),
                'null_percentage': (null_count / df.count()) * 100 if df.count() > 0 else 0
            }
        results['null_checks'] = null_results
    
    # Check for unique values
    if 'unique_checks' in checks:
        unique_results = {}
        for column in checks['unique_checks']:
            unique_count = df.select(column).distinct().count()
            unique_results[column] = {
                'unique_count': unique_count,
                'total_count': df.count(),
                'uniqueness_percentage': (unique_count / df.count()) * 100 if df.count() > 0 else 0
            }
        results['unique_checks'] = unique_results
    
    # Check for value ranges
    if 'range_checks' in checks:
        range_results = {}
        for column, (min_val, max_val) in checks['range_checks'].items():
            out_of_range = df.filter(
                (col(column) < min_val) | (col(column) > max_val)
            ).count()
            range_results[column] = {
                'out_of_range_count': out_of_range,
                'total_count': df.count(),
                'out_of_range_percentage': (out_of_range / df.count()) * 100 if df.count() > 0 else 0
            }
        results['range_checks'] = range_results
    
    return results

def validate_loan_data(df: DataFrame) -> Dict[str, Any]:
    """
    Perform specific validation checks for loan data.
    
    Args:
        df (DataFrame): Loan data DataFrame
        
    Returns:
        Dict[str, Any]: Validation results
    """
    checks = {
        'null_checks': [
            'loan_id',
            'member_id',
            'funded_amount',
            'monthly_installment'
        ],
        'unique_checks': [
            'loan_id',
            'member_id'
        ],
        'range_checks': {
            'funded_amount': (0, 1000000),
            'monthly_installment': (0, 10000)
        }
    }
    
    return check_data_quality(df, checks)

def validate_customer_data(df: DataFrame) -> Dict[str, Any]:
    """
    Perform specific validation checks for customer data.
    
    Args:
        df (DataFrame): Customer data DataFrame
        
    Returns:
        Dict[str, Any]: Validation results
    """
    checks = {
        'null_checks': [
            'member_id',
            'annual_income',
            'home_ownership'
        ],
        'unique_checks': [
            'member_id'
        ],
        'range_checks': {
            'annual_income': (0, 10000000)
        }
    }
    
    return check_data_quality(df, checks)

def validate_repayment_data(df: DataFrame) -> Dict[str, Any]:
    """
    Perform specific validation checks for repayment data.
    
    Args:
        df (DataFrame): Repayment data DataFrame
        
    Returns:
        Dict[str, Any]: Validation results
    """
    checks = {
        'null_checks': [
            'loan_id',
            'last_payment_amount',
            'total_payment_received'
        ],
        'unique_checks': [
            'loan_id'
        ],
        'range_checks': {
            'last_payment_amount': (0, 10000),
            'total_payment_received': (0, 1000000)
        }
    }
    
    return check_data_quality(df, checks) 