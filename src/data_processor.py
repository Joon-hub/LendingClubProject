#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Data Processing Module
This module handles the processing of raw Lending Club data into structured datasets.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws

def create_spark_session():
    """Create and return a Spark session configured for local processing."""
    return SparkSession.builder \
        .appName('lending_club_data_processor') \
        .master('local[*]') \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def load_raw_data(spark, data_path):
    """Load the raw CSV data into a Spark DataFrame."""
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data file not found at: {data_path}")
    
    return spark.read \
        .format('csv') \
        .option("InferSchema", "True") \
        .option("header", "True") \
        .load(data_path)

def create_member_id(df):
    """Create a unique member ID using SHA-256 hash of selected features."""
    features = [
        "emp_title", "emp_length", "home_ownership", "annual_inc",
        "zip_code", "addr_state", "grade", "sub_grade", "verification_status"
    ]
    
    return df.withColumn(
        "name_sha2",
        sha2(concat_ws("||", *features), 256)
    )

def validate_data(spark, raw_df, processed_df):
    """Perform validation checks on the data."""
    # Register DataFrames as temporary views
    raw_df.createOrReplaceTempView("lending_club_data")
    processed_df.createOrReplaceTempView("processed_data")
    
    # 1. Total number of records
    row_count = spark.sql("select count(*) as total from lending_club_data").collect()[0]['total']
    print(f'Number of rows in dataframe: {row_count}')
    
    # 2. Number of columns
    print(f'Number of columns in dataframe: {len(raw_df.columns)}')
    
    # 3. Number of unique member IDs
    print("Number of unique member IDs:")
    spark.sql("select count(distinct(name_sha2)) from processed_data").show()
    
    # 4. Show member IDs with multiple records
    print("Member IDs with multiple records:")
    spark.sql("""
        select name_sha2, count(*) as total_cnt 
        from processed_data 
        group by name_sha2 
        having count(*) > 1 
        order by total_cnt desc 
        limit 2
    """).show()

def export_customers_data(spark, processed_df, output_path):
    """Export customer-specific attributes to CSV."""
    customers_df = spark.sql("""
        SELECT 
            name_sha2 as member_id,
            emp_title,
            emp_length,
            home_ownership,
            annual_inc,
            addr_state,
            zip_code,
            'USA' as country,
            grade,
            sub_grade,
            verification_status,
            tot_hi_cred_lim,
            application_type,
            annual_inc_joint,
            verification_status_joint 
        FROM processed_data
    """)
    
    customers_df.repartition(1).write \
        .option("header", "true") \
        .format("csv") \
        .mode("overwrite") \
        .save(os.path.join(output_path, "customers_data"))
    
    return customers_df

def export_loans_data(spark, processed_df, output_path):
    """Export loan-related data to CSV."""
    loans_df = spark.sql("""
        SELECT 
            id as loan_id,
            name_sha2 as member_id,
            loan_amnt,
            funded_amnt,
            term,
            int_rate,
            installment,
            issue_d,
            loan_status,
            purpose,
            title 
        FROM processed_data
    """)
    
    loans_df.repartition(1).write \
        .option("header", "true") \
        .format("csv") \
        .mode("overwrite") \
        .save(os.path.join(output_path, "loans_data"))
    
    return loans_df

def export_repayments_data(spark, processed_df, output_path):
    """Export loan repayment details to CSV."""
    repayments_df = spark.sql("""
        SELECT 
            id as loan_id,
            total_rec_prncp,
            total_rec_int,
            total_rec_late_fee,
            total_pymnt,
            last_pymnt_amnt,
            last_pymnt_d,
            next_pymnt_d 
        FROM processed_data
    """)
    
    repayments_df.repartition(1).write \
        .option("header", "true") \
        .format("csv") \
        .mode("overwrite") \
        .save(os.path.join(output_path, "repayments_data"))
    
    return repayments_df

def export_defaulters_data(spark, processed_df, output_path):
    """Export defaulter information to CSV."""
    defaulters_df = spark.sql("""
        SELECT 
            name_sha2 as member_id,
            delinq_2yrs,
            delinq_amnt,
            pub_rec,
            pub_rec_bankruptcies,
            inq_last_6mths,
            total_rec_late_fee,
            mths_since_last_delinq,
            mths_since_last_record 
        FROM processed_data
    """)
    
    defaulters_df.repartition(1).write \
        .option("header", "true") \
        .format("csv") \
        .mode("overwrite") \
        .save(os.path.join(output_path, "defaulters_data"))
    
    return defaulters_df

def process_data(input_path, output_path):
    """Main function to process the raw data and export structured datasets."""
    spark = create_spark_session()
    
    try:
        # Load and process raw data
        raw_df = load_raw_data(spark, input_path)
        processed_df = create_member_id(raw_df)
        processed_df.createOrReplaceTempView("processed_data")
        
        # Validate data
        validate_data(spark, raw_df, processed_df)
        
        # Export structured datasets
        customers_df = export_customers_data(spark, processed_df, output_path)
        loans_df = export_loans_data(spark, processed_df, output_path)
        repayments_df = export_repayments_data(spark, processed_df, output_path)
        defaulters_df = export_defaulters_data(spark, processed_df, output_path)
        
        # Show sample data
        print("\nSample of customers data:")
        customers_df.show(5)
        print("\nSample of loans data:")
        loans_df.show(5)
        print("\nSample of repayments data:")
        repayments_df.show(5)
        print("\nSample of defaulters data:")
        defaulters_df.show(5)
        
    finally:
        spark.stop()

if __name__ == "__main__":
    # Example usage
    input_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Data', 'raw_data_10k.csv')
    output_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Data', 'processed')
    process_data(input_path, output_path) 