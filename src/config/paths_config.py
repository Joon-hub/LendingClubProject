"""
Configuration file for data paths.
This file contains all the paths used in the project for data storage and retrieval.
"""

# Base paths
BASE_PATH = "LendingClubProject/data"
OUTPUT_BASE_PATH = "LendingClubProject/loan_score"

# Input data paths
DATA_PATHS = {
    'loans': f"{BASE_PATH}/raw/loans",
    'repayments': f"{BASE_PATH}/raw/loans_repayments",
    'customers': f"{BASE_PATH}/raw/customers_new",
    'defaulters_detail': f"{BASE_PATH}/raw/loans_defaulters_detail_rec_enq_new",
    'defaulters_delinq': f"{BASE_PATH}/raw/loans_defaulters_delinq_new",
    'bad_customers': f"{BASE_PATH}/raw/bad_customer_data_final"
}

# Output data paths
OUTPUT_PATHS = {
    'processed': f"{BASE_PATH}/processed",
    'loan_scores': f"{OUTPUT_BASE_PATH}/daily",
    'reports': f"{OUTPUT_BASE_PATH}/reports"
}

# Local development paths (for testing)
LOCAL_PATHS = {
    'raw': f"{BASE_PATH}/raw",
    'processed': f"{BASE_PATH}/processed",
    'output': f"{OUTPUT_BASE_PATH}"
}