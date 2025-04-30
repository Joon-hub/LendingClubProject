"""
Loan Scorer Module.
This module contains the LoanScorer class for calculating loan scores.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, sum as spark_sum, count as spark_count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from typing import Dict, Any

from src.config.scoring_config import (
    PAYMENT_HISTORY_POINTS,
    DEFAULT_HISTORY_POINTS,
    FINANCIAL_HEALTH_POINTS,
    GRADE_THRESHOLDS,
    COMPONENT_WEIGHTS,
    PAYMENT_THRESHOLDS
)
from src.config.paths_config import DATA_PATHS, OUTPUT_PATHS
from src.utils.spark_utils import create_spark_session, get_spark_config
from src.utils.data_utils import (
    calculate_percentage,
    assign_score_based_on_threshold,
    clean_string_column,
    validate_dataframe
)
from src.utils.validation_utils import (
    validate_loan_data,
    validate_customer_data,
    validate_repayment_data
)

class LoanScorer:
    """
    A class for calculating loan scores based on various criteria.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize the LoanScorer.
        
        Args:
            spark (SparkSession): Spark session
        """
        self.spark = spark
        
        # Set scoring parameters as Spark config
        for key, value in PAYMENT_HISTORY_POINTS.items():
            self.spark.conf.set(f"scoring.payment_history.{key}", str(value))
        for key, value in DEFAULT_HISTORY_POINTS.items():
            self.spark.conf.set(f"scoring.default_history.{key}", str(value))
        for key, value in FINANCIAL_HEALTH_POINTS.items():
            self.spark.conf.set(f"scoring.financial_health.{key}", str(value))
            
    def calculate_payment_history_score(self, 
                                     loans_repayments: DataFrame,
                                     loans: DataFrame) -> DataFrame:
        """
        Calculate payment history score based on last payment amount and total payments.
        
        Args:
            loans_repayments (DataFrame): DataFrame containing loan repayment data
            loans (DataFrame): DataFrame containing loan data
            
        Returns:
            DataFrame: DataFrame with payment history scores
        """
        try:
            # Join loans and repayments data
            payment_df = loans_repayments.join(
                loans,
                "loan_id",
                "inner"
            )
            
            # Calculate payment percentages
            payment_df = calculate_percentage(
                payment_df,
                "last_payment_amount",
                "monthly_installment",
                "last_payment_percentage"
            )
            
            payment_df = calculate_percentage(
                payment_df,
                "total_payment_received",
                "funded_amount",
                "total_payment_percentage"
            )
            
            # Assign scores based on thresholds
            payment_df = assign_score_based_on_threshold(
                payment_df,
                "last_payment_percentage",
                PAYMENT_THRESHOLDS,
                "last_payment_pts"
            )
            
            payment_df = assign_score_based_on_threshold(
                payment_df,
                "total_payment_percentage",
                PAYMENT_THRESHOLDS,
                "total_payment_pts"
            )
            
            return payment_df
            
        except Exception as e:
            raise
            
    def calculate_defaulters_history_score(self, defaulters_df: DataFrame) -> DataFrame:
        """Calculate defaulters history score based on delinquency, public records, and inquiries.
        
        Args:
            defaulters_df: DataFrame containing defaulters information
            
        Returns:
            DataFrame with defaulters history scores
        """
        # Create aliases for the DataFrame to avoid ambiguous references
        df = defaulters_df.alias("df")
        
        # Define thresholds for each metric
        delinq_thresholds = {
            0: DEFAULT_HISTORY_POINTS['excellent'],
            1: DEFAULT_HISTORY_POINTS['good'],
            2: DEFAULT_HISTORY_POINTS['bad'],
            3: DEFAULT_HISTORY_POINTS['very_bad']
        }
        
        pub_rec_thresholds = {
            0: DEFAULT_HISTORY_POINTS['excellent'],
            1: DEFAULT_HISTORY_POINTS['good'],
            2: DEFAULT_HISTORY_POINTS['bad'],
            3: DEFAULT_HISTORY_POINTS['very_bad']
        }
        
        pub_bankrupt_thresholds = {
            0: DEFAULT_HISTORY_POINTS['excellent'],
            1: DEFAULT_HISTORY_POINTS['good'],
            2: DEFAULT_HISTORY_POINTS['bad'],
            3: DEFAULT_HISTORY_POINTS['very_bad']
        }
        
        enq_thresholds = {
            0: DEFAULT_HISTORY_POINTS['excellent'],
            1: DEFAULT_HISTORY_POINTS['good'],
            2: DEFAULT_HISTORY_POINTS['bad'],
            3: DEFAULT_HISTORY_POINTS['very_bad']
        }
        
        # Calculate delinquency score
        delinq_df = assign_score_based_on_threshold(
            df,
            "delinq_2yrs",
            delinq_thresholds,
            "delinq_pts"
        )
        
        # Calculate public records score
        pub_rec_df = assign_score_based_on_threshold(
            delinq_df,
            "pub_rec",
            pub_rec_thresholds,
            "public_records_pts"
        )
        
        # Calculate public bankruptcies score
        pub_bankrupt_df = assign_score_based_on_threshold(
            pub_rec_df,
            "pub_rec_bankruptcies",
            pub_bankrupt_thresholds,
            "public_bankruptcies_pts"
        )
        
        # Calculate inquiries score
        enq_df = assign_score_based_on_threshold(
            pub_bankrupt_df,
            "inq_last_6mths",
            enq_thresholds,
            "enq_pts"
        )
        
        # Select and return final columns
        return enq_df.select(
            "member_id",
            "delinq_pts",
            "public_records_pts",
            "public_bankruptcies_pts",
            "enq_pts"
        )
            
    def calculate_financial_health_score(self,
                                       loans: DataFrame,
                                       customers: DataFrame) -> DataFrame:
        """
        Calculate financial health score based on loan status, home ownership, credit limit, and grade.
        
        Args:
            loans (DataFrame): DataFrame containing loan data
            customers (DataFrame): DataFrame containing customer data
            
        Returns:
            DataFrame: DataFrame with financial health scores
        """
        try:
            # Join loans and customers data
            financial_df = loans.join(
                customers,
                "member_id",
                "inner"
            )
            
            # Clean home ownership values
            home_ownership_replacements = {
                "RENT": "RENT",
                "MORTGAGE": "MORTGAGE",
                "OWN": "OWN",
                "OTHER": "OTHER"
            }
            
            financial_df = clean_string_column(
                financial_df,
                "home_ownership",
                home_ownership_replacements
            )
            
            # Calculate credit limit utilization
            financial_df = calculate_percentage(
                financial_df,
                "funded_amount",
                "total_high_credit_limit",
                "credit_limit_utilization"
            )
            
            # Assign scores for different aspects
            financial_df = financial_df.withColumn(
                "loan_status_pts",
                when(col("loan_status") == "Fully Paid", lit(FINANCIAL_HEALTH_POINTS['excellent']))
                .when(col("loan_status") == "Current", lit(FINANCIAL_HEALTH_POINTS['very_good']))
                .when(col("loan_status") == "Late", lit(FINANCIAL_HEALTH_POINTS['bad']))
                .otherwise(lit(FINANCIAL_HEALTH_POINTS['very_bad']))
            )
            
            financial_df = financial_df.withColumn(
                "home_pts",
                when(col("home_ownership") == "OWN", lit(FINANCIAL_HEALTH_POINTS['excellent']))
                .when(col("home_ownership") == "MORTGAGE", lit(FINANCIAL_HEALTH_POINTS['good']))
                .when(col("home_ownership") == "RENT", lit(FINANCIAL_HEALTH_POINTS['bad']))
                .otherwise(lit(FINANCIAL_HEALTH_POINTS['very_bad']))
            )
            
            financial_df = assign_score_based_on_threshold(
                financial_df,
                "credit_limit_utilization",
                FINANCIAL_HEALTH_POINTS,
                "credit_limit_pts"
            )
            
            financial_df = financial_df.withColumn(
                "grade_pts",
                when(col("grade") == "A", lit(FINANCIAL_HEALTH_POINTS['excellent']))
                .when(col("grade") == "B", lit(FINANCIAL_HEALTH_POINTS['very_good']))
                .when(col("grade") == "C", lit(FINANCIAL_HEALTH_POINTS['good']))
                .when(col("grade") == "D", lit(FINANCIAL_HEALTH_POINTS['bad']))
                .otherwise(lit(FINANCIAL_HEALTH_POINTS['very_bad']))
            )
            
            return financial_df
            
        except Exception as e:
            raise
            
    def calculate_final_loan_score(self,
                                 payment_scores: DataFrame,
                                 defaulters_scores: DataFrame,
                                 financial_scores: DataFrame) -> DataFrame:
        """Calculate final loan score by combining component scores.
        
        Args:
            payment_scores: DataFrame with payment history scores
            defaulters_scores: DataFrame with defaulters history scores
            financial_scores: DataFrame with financial health scores
            
        Returns:
            DataFrame with final loan scores and grades
        """
        # Calculate weighted component scores
        payment_history_score = (
            payment_scores.withColumn(
                "payment_history_score",
                (col("last_payment_pts") + col("total_payment_pts")) * lit(COMPONENT_WEIGHTS['payment_history'])
            )
            .select("member_id", "payment_history_score")
        )
        
        defaulters_history_score = (
            defaulters_scores.withColumn(
                "defaulters_history_score",
                (col("delinq_pts") + col("public_records_pts") +
                 col("public_bankruptcies_pts") + col("enq_pts")) * lit(COMPONENT_WEIGHTS['defaulters_history'])
            )
            .select("member_id", "defaulters_history_score")
        )
        
        financial_health_score = (
            financial_scores.withColumn(
                "financial_health_score",
                (col("loan_status_pts") + col("home_pts") +
                 col("credit_limit_pts") + col("grade_pts")) * lit(COMPONENT_WEIGHTS['financial_health'])
            )
            .select("member_id", "financial_health_score")
        )
        
        # Join scores
        final_scores = (
            payment_history_score
            .join(
                defaulters_history_score,
                "member_id",
                "inner"
            )
            .join(
                financial_health_score,
                "member_id",
                "inner"
            )
        )
        
        # Calculate total score
        final_scores = final_scores.withColumn(
            "total_score",
            col("payment_history_score") +
            col("defaulters_history_score") +
            col("financial_health_score")
        )
        
        # Assign grade based on total score
        final_scores = final_scores.withColumn(
            "grade",
            when(col("total_score") > GRADE_THRESHOLDS['A'], "A")
            .when(col("total_score") > GRADE_THRESHOLDS['B'], "B")
            .when(col("total_score") > GRADE_THRESHOLDS['C'], "C")
            .when(col("total_score") > GRADE_THRESHOLDS['D'], "D")
            .when(col("total_score") > GRADE_THRESHOLDS['E'], "E")
            .otherwise("F")
        )
        
        return final_scores
            
    def save_loan_scores(self, loan_scores: DataFrame, output_path: str) -> None:
        """Save loan scores to a parquet file.
        
        Args:
            loan_scores: DataFrame with loan scores
            output_path: Path to save the parquet file
        """
        # Select only the necessary columns to avoid duplicates
        final_scores = loan_scores.select(
            "member_id",
            "payment_history_score",
            "defaulters_history_score",
            "financial_health_score",
            "total_score",
            "grade"
        )
        
        # Save to parquet
        final_scores.write.parquet(output_path, mode="overwrite")

def main():
    """Main function to run the loan scoring process."""
    # Create Spark session
    spark = create_spark_session(
        app_name="LoanScorer",
        config=get_spark_config()
    )
    
    try:
        # Initialize LoanScorer
        scorer = LoanScorer(spark)
        
        # Load required data
        loans_df = spark.read.parquet(DATA_PATHS['loans'])
        repayments_df = spark.read.parquet(DATA_PATHS['repayments'])
        customers_df = spark.read.parquet(DATA_PATHS['customers'])
        detail_df = spark.read.parquet(DATA_PATHS['defaulters_detail'])
        delinq_df = spark.read.parquet(DATA_PATHS['defaulters_delinq'])
        
        # Validate data
        validate_loan_data(loans_df)
        validate_customer_data(customers_df)
        validate_repayment_data(repayments_df)
        
        # Calculate scores
        payment_history_scores = scorer.calculate_payment_history_score(
            repayments_df, loans_df
        )
        
        defaulters_history_scores = scorer.calculate_defaulters_history_score(
            detail_df, delinq_df
        )
        
        financial_health_scores = scorer.calculate_financial_health_score(
            loans_df, customers_df
        )
        
        # Calculate and save final scores
        final_scores = scorer.calculate_final_loan_score(
            payment_history_scores,
            defaulters_history_scores,
            financial_health_scores
        )
        
        scorer.save_loan_scores(final_scores, OUTPUT_PATHS['loan_scores'])
        
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main() 