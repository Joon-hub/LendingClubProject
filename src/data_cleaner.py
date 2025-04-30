#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Data Cleaning Module
This module handles the cleaning and transformation of the Lending Club datasets.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, regexp_replace, col, when

def create_spark_session():
    """Create and return a Spark session configured for local processing."""
    return SparkSession.builder \
        .appName('lending_club_data_cleaner') \
        .master('local[*]') \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def clean_customers_data(spark, input_path, output_path):
    """Clean and transform customer data."""
    # Define schema
    customer_schema = """
        member_id string,
        emp_title string,
        emp_length string,
        home_ownership string,
        annual_inc float,
        addr_state string,
        zip_code string,
        country string,
        grade string,
        sub_grade string,
        verification_status string,
        tot_hi_cred_lim float,
        application_type string,
        annual_inc_joint float,
        verification_status_joint string
    """
    
    # Load data
    customers_df = spark.read \
        .format("csv") \
        .option("header", True) \
        .schema(customer_schema) \
        .load(input_path)
    
    # Rename columns
    customers_df = customers_df \
        .withColumnRenamed("annual_inc", "annual_income") \
        .withColumnRenamed("addr_state", "address_state") \
        .withColumnRenamed("zip_code", "address_zipcode") \
        .withColumnRenamed("country", "address_country") \
        .withColumnRenamed("tot_hi_cred_lim", "total_high_credit_limit") \
        .withColumnRenamed("annual_inc_joint", "join_annual_income")
    
    # Add ingestion timestamp
    customers_df = customers_df.withColumn("ingest_date", current_timestamp())
    
    # Remove duplicates
    customers_df = customers_df.distinct()
    
    # Filter out null annual income
    customers_df = customers_df.filter("annual_income is not null")
    
    # Clean employment length
    customers_df = customers_df \
        .withColumn("emp_length", regexp_replace(col("emp_length"), "\D", "")) \
        .withColumn("emp_length", col("emp_length").cast("int"))
    
    # Fill null employment length with average
    avg_emp_length = customers_df.selectExpr("floor(avg(emp_length))").collect()[0][0]
    customers_df = customers_df.na.fill(avg_emp_length, subset=["emp_length"])
    
    # Clean address state
    customers_df = customers_df \
        .withColumn("address_state", 
                   when(col("address_state").rlike("^[A-Z]{2}$"), col("address_state"))
                   .otherwise("NA"))
    
    # Save cleaned data
    customers_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(os.path.join(output_path, "customers_parquet"))
    
    customers_df.write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(os.path.join(output_path, "customers_csv"))
    
    return customers_df

def clean_loans_data(spark, input_path, output_path):
    """Clean and transform loan data."""
    # Define schema
    loans_schema = """
        loan_id string,
        member_id string,
        loan_amount float,
        funded_amount float,
        loan_term_months string,
        interest_rate string,
        monthly_installment float,
        issue_date string,
        loan_status string,
        loan_purpose string,
        loan_title string
    """
    
    # Load data
    loans_df = spark.read \
        .format("csv") \
        .option("header", True) \
        .schema(loans_schema) \
        .load(input_path)
    
    # Add ingestion timestamp
    loans_df = loans_df.withColumn("ingest_date", current_timestamp())
    
    # Drop rows with nulls in critical columns
    critical_columns = [
        "loan_amount", "funded_amount", "loan_term_months",
        "monthly_installment", "issue_date", "loan_status", "loan_purpose"
    ]
    loans_df = loans_df.na.drop(subset=critical_columns)
    
    # Clean loan term
    loans_df = loans_df \
        .withColumn("loan_term_months", regexp_replace(col("loan_term_months"), "\D", "")) \
        .withColumn("loan_term_years", col("loan_term_months").cast("int")/12) \
        .drop("loan_term_months")
    
    # Clean loan purpose
    main_purposes = [
        "debt_consolidation", "credit_card", "home_improvement", 
        "other", "major_purchase", "medical", "small_business", 
        "car", "vacation", "moving", "house", "wedding", "renewable_energy", 
        "educational"
    ]
    loans_df = loans_df \
        .withColumn("loan_purpose", 
                   when(col("loan_purpose").isin(main_purposes), col("loan_purpose"))
                   .otherwise("other"))
    
    # Save cleaned data
    loans_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(os.path.join(output_path, "loans_parquet"))
    
    loans_df.write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(os.path.join(output_path, "loans_csv"))
    
    return loans_df

def clean_repayments_data(spark, input_path, output_path):
    """Clean and transform repayment data."""
    # Define schema
    repayments_schema = """
        loan_id string,
        total_principal_received float,
        total_interest_received float,
        total_late_fee_received float,
        total_payment_received float,
        last_payment_amount float,
        last_payment_date string,
        next_payment_date string
    """
    
    # Load data
    repayments_df = spark.read \
        .format("csv") \
        .option("header", True) \
        .schema(repayments_schema) \
        .load(input_path)
    
    # Add ingestion timestamp
    repayments_df = repayments_df.withColumn("ingest_date", current_timestamp())
    
    # Drop rows with nulls in critical columns
    critical_columns = [
        "total_principal_received",
        "total_interest_received",
        "total_late_fee_received",
        "total_payment_received",
        "last_payment_amount"
    ]
    repayments_df = repayments_df.na.drop(subset=critical_columns)
    
    # Fix payment anomalies
    repayments_df = repayments_df.withColumn(
        "total_payment_received",
        when(
            (col("total_principal_received") != 0.0) &
            (col("total_payment_received") == 0.0),
            col("total_principal_received") + 
            col("total_interest_received") + 
            col("total_late_fee_received")
        ).otherwise(col("total_payment_received"))
    )
    
    # Filter out zero payments
    repayments_df = repayments_df.filter("total_payment_received != 0.0")
    
    # Clean date fields
    repayments_df = repayments_df \
        .withColumn("last_payment_date", 
                   when(col("last_payment_date") == "0.0", None)
                   .otherwise(col("last_payment_date"))) \
        .withColumn("next_payment_date", 
                   when(col("next_payment_date") == "0.0", None)
                   .otherwise(col("next_payment_date")))
    
    # Save cleaned data
    repayments_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(os.path.join(output_path, "repayments_parquet"))
    
    repayments_df.write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(os.path.join(output_path, "repayments_csv"))
    
    return repayments_df

def clean_defaulters_data(spark, input_path, output_path):
    """Clean and transform defaulters data."""
    # Define schema
    defaulters_schema = """
        member_id string,
        delinq_2yrs float,
        delinq_amnt float,
        pub_rec float,
        pub_rec_bankruptcies float,
        inq_last_6mths float,
        total_rec_late_fee float,
        mths_since_last_delinq float,
        mths_since_last_record float
    """
    
    # Load data
    defaulters_df = spark.read \
        .format("csv") \
        .option("header", True) \
        .schema(defaulters_schema) \
        .load(input_path)
    
    # Clean delinquency data
    defaulters_df = defaulters_df \
        .withColumn("delinq_2yrs", col("delinq_2yrs").cast("integer")) \
        .fillna(0, subset=["delinq_2yrs"])
    
    # Create primary defaulters view
    primary_defaulters_df = defaulters_df.filter(
        (col("delinq_2yrs") > 0) | 
        (col("mths_since_last_delinq") > 0)
    )
    
    # Create credit issues view
    credit_issues_df = defaulters_df.filter(
        (col("pub_rec") > 0) |
        (col("pub_rec_bankruptcies") > 0) |
        (col("inq_last_6mths") > 0)
    )
    
    # Save cleaned data
    primary_defaulters_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(os.path.join(output_path, "defaulters_parquet"))
    
    credit_issues_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(os.path.join(output_path, "credit_issues_parquet"))
    
    return primary_defaulters_df, credit_issues_df

def clean_all_data(input_base_path, output_base_path):
    """Clean all datasets."""
    spark = create_spark_session()
    
    try:
        # Clean customers data
        customers_df = clean_customers_data(
            spark,
            os.path.join(input_base_path, "customers_data"),
            os.path.join(output_base_path, "cleaned")
        )
        
        # Clean loans data
        loans_df = clean_loans_data(
            spark,
            os.path.join(input_base_path, "loans_data"),
            os.path.join(output_base_path, "cleaned")
        )
        
        # Clean repayments data
        repayments_df = clean_repayments_data(
            spark,
            os.path.join(input_base_path, "repayments_data"),
            os.path.join(output_base_path, "cleaned")
        )
        
        # Clean defaulters data
        defaulters_df, credit_issues_df = clean_defaulters_data(
            spark,
            os.path.join(input_base_path, "defaulters_data"),
            os.path.join(output_base_path, "cleaned")
        )
        
        return customers_df, loans_df, repayments_df, defaulters_df, credit_issues_df
        
    finally:
        spark.stop()

if __name__ == "__main__":
    # Example usage
    input_base_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Data', 'processed')
    output_base_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Data', 'cleaned')
    clean_all_data(input_base_path, output_base_path) 