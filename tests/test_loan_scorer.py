#!/usr/bin/env python
# coding: utf-8

import pytest
from pyspark.sql import SparkSession, DataFrame
from src.loan_scorer import LoanScorer
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from src.config.scoring_config import (
    PAYMENT_HISTORY_POINTS,
    DEFAULT_HISTORY_POINTS,
    FINANCIAL_HEALTH_POINTS,
    GRADE_THRESHOLDS,
    COMPONENT_WEIGHTS,
    PAYMENT_THRESHOLDS
)

@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing."""
    spark = SparkSession.builder \
        .appName("LoanScorerTest") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(scope="module")
def loan_scorer(spark):
    """Create a LoanScorer instance for testing."""
    return LoanScorer(spark)

def test_scoring_parameters(loan_scorer):
    """Test if scoring parameters are set correctly."""
    # Check payment history points
    for key, value in PAYMENT_HISTORY_POINTS.items():
        assert loan_scorer.spark.conf.get(f"scoring.payment_history.{key}") == str(value)
    
    # Check default history points
    for key, value in DEFAULT_HISTORY_POINTS.items():
        assert loan_scorer.spark.conf.get(f"scoring.default_history.{key}") == str(value)
    
    # Check financial health points
    for key, value in FINANCIAL_HEALTH_POINTS.items():
        assert loan_scorer.spark.conf.get(f"scoring.financial_health.{key}") == str(value)

def test_payment_history_score(loan_scorer, sample_loans_data, sample_repayments_data):
    """Test payment history score calculation."""
    # Calculate scores
    scores_df = loan_scorer.calculate_payment_history_score(
        sample_repayments_data,
        sample_loans_data
    )
    
    # Check DataFrame structure
    assert isinstance(scores_df, DataFrame)
    assert "member_id" in scores_df.columns
    assert "last_payment_pts" in scores_df.columns
    assert "total_payment_pts" in scores_df.columns
    
    # Check score calculations
    scores = scores_df.collect()
    for row in scores:
        # Check if scores are within expected range
        assert 0 <= row.last_payment_pts <= PAYMENT_HISTORY_POINTS['excellent']
        assert 0 <= row.total_payment_pts <= PAYMENT_HISTORY_POINTS['excellent']

def test_defaulters_history_score(loan_scorer, sample_defaulters_data):
    """Test defaulters history score calculation."""
    # Calculate scores
    scores_df = loan_scorer.calculate_defaulters_history_score(sample_defaulters_data)
    
    # Check DataFrame structure
    assert isinstance(scores_df, DataFrame)
    assert "member_id" in scores_df.columns
    assert "delinq_pts" in scores_df.columns
    assert "public_records_pts" in scores_df.columns
    assert "public_bankruptcies_pts" in scores_df.columns
    assert "enq_pts" in scores_df.columns
    
    # Check score calculations
    scores = scores_df.collect()
    for row in scores:
        # Check if scores are within expected range
        assert 0 <= row.delinq_pts <= DEFAULT_HISTORY_POINTS['excellent']
        assert 0 <= row.public_records_pts <= DEFAULT_HISTORY_POINTS['excellent']
        assert 0 <= row.public_bankruptcies_pts <= DEFAULT_HISTORY_POINTS['excellent']
        assert 0 <= row.enq_pts <= DEFAULT_HISTORY_POINTS['excellent']
        
        # Check specific scores based on test data
        if row.member_id == "M1":
            assert row.delinq_pts == DEFAULT_HISTORY_POINTS['excellent']  # No delinquencies
            assert row.public_records_pts == DEFAULT_HISTORY_POINTS['excellent']  # No public records
            assert row.public_bankruptcies_pts == DEFAULT_HISTORY_POINTS['excellent']  # No bankruptcies
            assert row.enq_pts == DEFAULT_HISTORY_POINTS['excellent']  # No inquiries
        elif row.member_id == "M2":
            assert row.delinq_pts == DEFAULT_HISTORY_POINTS['good']  # 1 delinquency
            assert row.public_records_pts == DEFAULT_HISTORY_POINTS['good']  # 1 public record
            assert row.public_bankruptcies_pts == DEFAULT_HISTORY_POINTS['excellent']  # No bankruptcies
            assert row.enq_pts == DEFAULT_HISTORY_POINTS['good']  # 1 inquiry
        elif row.member_id == "M3":
            assert row.delinq_pts == DEFAULT_HISTORY_POINTS['bad']  # 2 delinquencies
            assert row.public_records_pts == DEFAULT_HISTORY_POINTS['bad']  # 2 public records
            assert row.public_bankruptcies_pts == DEFAULT_HISTORY_POINTS['good']  # 1 bankruptcy
            assert row.enq_pts == DEFAULT_HISTORY_POINTS['bad']  # 2 inquiries

def test_financial_health_score(loan_scorer, sample_loans_data, sample_customers_data):
    """Test financial health score calculation."""
    # Calculate scores
    scores_df = loan_scorer.calculate_financial_health_score(
        sample_loans_data,
        sample_customers_data
    )
    
    # Check DataFrame structure
    assert isinstance(scores_df, DataFrame)
    assert "member_id" in scores_df.columns
    assert "loan_status_pts" in scores_df.columns
    assert "home_pts" in scores_df.columns
    assert "credit_limit_pts" in scores_df.columns
    assert "grade_pts" in scores_df.columns
    
    # Check score calculations
    scores = scores_df.collect()
    for row in scores:
        # Check if scores are within expected range
        assert 0 <= row.loan_status_pts <= FINANCIAL_HEALTH_POINTS['excellent']
        assert 0 <= row.home_pts <= FINANCIAL_HEALTH_POINTS['excellent']
        assert 0 <= row.credit_limit_pts <= FINANCIAL_HEALTH_POINTS['excellent']
        assert 0 <= row.grade_pts <= FINANCIAL_HEALTH_POINTS['excellent']
        
        # Check specific scores based on test data
        if row.member_id == "M1":
            assert row.loan_status_pts == FINANCIAL_HEALTH_POINTS['excellent']  # Fully Paid
            assert row.home_pts == FINANCIAL_HEALTH_POINTS['excellent']  # OWN
            assert row.grade_pts == FINANCIAL_HEALTH_POINTS['excellent']  # Grade A
        elif row.member_id == "M2":
            assert row.loan_status_pts == FINANCIAL_HEALTH_POINTS['very_good']  # Current
            assert row.home_pts == FINANCIAL_HEALTH_POINTS['good']  # MORTGAGE
            assert row.grade_pts == FINANCIAL_HEALTH_POINTS['very_good']  # Grade B
        elif row.member_id == "M3":
            assert row.loan_status_pts == FINANCIAL_HEALTH_POINTS['bad']  # Late
            assert row.home_pts == FINANCIAL_HEALTH_POINTS['bad']  # RENT
            assert row.grade_pts == FINANCIAL_HEALTH_POINTS['good']  # Grade C

def test_final_loan_score(loan_scorer, 
                         sample_loans_data, 
                         sample_repayments_data,
                         sample_customers_data,
                         sample_defaulters_data):
    """Test final loan score calculation."""
    # Calculate component scores
    payment_scores = loan_scorer.calculate_payment_history_score(
        sample_repayments_data,
        sample_loans_data
    )
    
    defaulters_scores = loan_scorer.calculate_defaulters_history_score(
        sample_defaulters_data
    )
    
    financial_scores = loan_scorer.calculate_financial_health_score(
        sample_loans_data,
        sample_customers_data
    )
    
    # Calculate final scores
    final_scores = loan_scorer.calculate_final_loan_score(
        payment_scores,
        defaulters_scores,
        financial_scores
    )
    
    # Check DataFrame structure
    assert isinstance(final_scores, DataFrame)
    assert "member_id" in final_scores.columns
    assert "payment_history_score" in final_scores.columns
    assert "defaulters_history_score" in final_scores.columns
    assert "financial_health_score" in final_scores.columns
    assert "total_score" in final_scores.columns
    assert "grade" in final_scores.columns
    
    # Check score calculations
    scores = final_scores.collect()
    for row in scores:
        # Check if component scores are weighted correctly
        assert row.payment_history_score == (
            (row.last_payment_pts + row.total_payment_pts) * COMPONENT_WEIGHTS['payment_history']
        )
        assert row.defaulters_history_score == (
            (row.delinq_pts + row.public_records_pts + 
             row.public_bankruptcies_pts + row.enq_pts) * COMPONENT_WEIGHTS['defaulters_history']
        )
        assert row.financial_health_score == (
            (row.loan_status_pts + row.home_pts + 
             row.credit_limit_pts + row.grade_pts) * COMPONENT_WEIGHTS['financial_health']
        )
        
        # Check if total score is sum of component scores
        assert row.total_score == (
            row.payment_history_score + 
            row.defaulters_history_score + 
            row.financial_health_score
        )
        
        # Check if grade is assigned correctly
        if row.total_score > GRADE_THRESHOLDS['A']:
            assert row.grade == "A"
        elif row.total_score > GRADE_THRESHOLDS['B']:
            assert row.grade == "B"
        elif row.total_score > GRADE_THRESHOLDS['C']:
            assert row.grade == "C"
        elif row.total_score > GRADE_THRESHOLDS['D']:
            assert row.grade == "D"
        elif row.total_score > GRADE_THRESHOLDS['E']:
            assert row.grade == "E"
        else:
            assert row.grade == "F"

def test_save_loan_scores(loan_scorer, 
                         sample_loans_data, 
                         sample_repayments_data,
                         sample_customers_data,
                         sample_defaulters_data,
                         tmp_path):
    """Test saving loan scores to a file."""
    # Calculate final scores
    payment_scores = loan_scorer.calculate_payment_history_score(
        sample_repayments_data,
        sample_loans_data
    )
    
    defaulters_scores = loan_scorer.calculate_defaulters_history_score(
        sample_defaulters_data
    )
    
    financial_scores = loan_scorer.calculate_financial_health_score(
        sample_loans_data,
        sample_customers_data
    )
    
    final_scores = loan_scorer.calculate_final_loan_score(
        payment_scores,
        defaulters_scores,
        financial_scores
    )
    
    # Save scores
    output_path = str(tmp_path / "loan_scores")
    loan_scorer.save_loan_scores(final_scores, output_path)
    
    # Verify saved data
    saved_df = loan_scorer.spark.read.parquet(output_path)
    assert saved_df.count() == final_scores.count()
    assert set(saved_df.columns) == set(final_scores.columns) 