"""
Configuration file for data paths.
This file contains all the paths used in the project for data storage and retrieval.
"""

# Base paths
BASE_PATH = "/public/trendytech/lendingclubproject"
OUTPUT_BASE_PATH = "/user/itv006277/lendingclubproject"

# Input data paths
DATA_PATHS = {
    'loans': f"{BASE_PATH}/loans",
    'repayments': f"{BASE_PATH}/loans_repayments",
    'customers': f"{BASE_PATH}/customers_new",
    'defaulters_detail': f"{BASE_PATH}/loans_defaulters_detail_rec_enq_new",
    'defaulters_delinq': f"{BASE_PATH}/loans_defaulters_delinq_new",
    'bad_customers': f"{BASE_PATH}/bad/bad_customer_data_final"
}

# Output data paths
OUTPUT_PATHS = {
    'processed': f"{OUTPUT_BASE_PATH}/processed",
    'loan_scores': f"{OUTPUT_BASE_PATH}/processed/loan_score",
    'reports': f"{OUTPUT_BASE_PATH}/reports"
}

# Local development paths (for testing)
LOCAL_PATHS = {
    'raw': "data/raw",
    'processed': "data/processed",
    'output': "data/output"
} 