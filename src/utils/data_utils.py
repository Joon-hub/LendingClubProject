"""
Utility functions for data manipulation.
This module provides helper functions for data processing and transformation.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, round
from typing import Dict, Any

def calculate_percentage(df: DataFrame, 
                       numerator_col: str, 
                       denominator_col: str, 
                       output_col: str) -> DataFrame:
    """
    Calculate percentage of one column relative to another.
    
    Args:
        df (DataFrame): Input DataFrame
        numerator_col (str): Column name for numerator
        denominator_col (str): Column name for denominator
        output_col (str): Column name for output percentage
        
    Returns:
        DataFrame: DataFrame with percentage column
    """
    return df.withColumn(
        output_col,
        (col(numerator_col) / col(denominator_col)) * 100
    )

def assign_score_based_on_threshold(df: DataFrame,
                                  input_col: str,
                                  thresholds: Dict[float, float],
                                  output_col: str) -> DataFrame:
    """
    Assign scores based on threshold values.
    
    Args:
        df (DataFrame): Input DataFrame
        input_col (str): Column name to check against thresholds
        thresholds (Dict[float, float]): Dictionary of threshold values and corresponding scores
        output_col (str): Column name for output score
        
    Returns:
        DataFrame: DataFrame with score column
    """
    # Sort thresholds in descending order
    sorted_thresholds = sorted(thresholds.items(), key=lambda x: x[0], reverse=True)
    
    # Build case expression
    case_expr = None
    for threshold, score in sorted_thresholds:
        if case_expr is None:
            case_expr = when(col(input_col) >= threshold, lit(score))
        else:
            case_expr = case_expr.when(col(input_col) >= threshold, lit(score))
    
    # Add default case for values below all thresholds
    case_expr = case_expr.otherwise(lit(0))
    
    return df.withColumn(output_col, case_expr)

def clean_string_column(df: DataFrame,
                       column_name: str,
                       replacements: Dict[str, str]) -> DataFrame:
    """
    Clean string column by replacing values according to mapping.
    
    Args:
        df (DataFrame): Input DataFrame
        column_name (str): Column name to clean
        replacements (Dict[str, str]): Dictionary of value replacements
        
    Returns:
        DataFrame: DataFrame with cleaned column
    """
    case_expr = None
    for old_value, new_value in replacements.items():
        if case_expr is None:
            case_expr = when(col(column_name) == old_value, lit(new_value))
        else:
            case_expr = case_expr.when(col(column_name) == old_value, lit(new_value))
    
    return df.withColumn(column_name, case_expr.otherwise(col(column_name)))

def validate_dataframe(df: DataFrame) -> bool:
    """
    Validate DataFrame for required columns and data types.
    
    Args:
        df (DataFrame): DataFrame to validate
        
    Returns:
        bool: True if DataFrame is valid, False otherwise
    """
    required_columns = {
        "member_id": "string",
        "loan_id": "string"
    }
    
    for column, dtype in required_columns.items():
        if column not in df.columns:
            return False
        if df.schema[column].dataType.typeName() != dtype:
            return False
    
    return True 