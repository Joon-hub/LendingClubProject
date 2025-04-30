from pyspark.sql import SparkSession
import getpass

from src.raw_data_processing import process_raw_data
from src.clean_customers import clean_customers
from src.clean_loans import clean_loans
from src.clean_repayments import clean_repayments
from src.clean_defaulters import clean_defaulters

def main():
    # Initialize Spark session for YARN cluster
    username = getpass.getuser()
    spark = SparkSession.builder \
        .appName("LendingClubProcessing") \
        .config('spark.ui.port', '0') \
        .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
        .config('spark.shuffle.useOldFetchProtocol', 'true') \
        .enableHiveSupport() \
        .master('yarn') \
        .getOrCreate()

    # Define HDFS paths
    RAW_DATA_PATH = "/public/trendytech/lendingclubproject/raw/accepted_2007_to_2018Q4.csv"
    RAW_CATEGORIES_DIR = f"/user/{username}/lendingclubproject/raw/"
    CLEANED_DIR = f"/user/{username}/lendingclubproject/cleaned/"

    # Execute pipeline steps
    process_raw_data(spark, RAW_DATA_PATH, RAW_CATEGORIES_DIR)
    clean_customers(spark, RAW_CATEGORIES_DIR + "customers_data_csv", CLEANED_DIR + "customers_parquet")
    clean_loans(spark, RAW_CATEGORIES_DIR + "loans_data_csv", CLEANED_DIR + "loans_parquet")
    clean_repayments(spark, RAW_CATEGORIES_DIR + "loans_repayments_csv", CLEANED_DIR + "loans_repayments_parquet")
    clean_defaulters(spark, RAW_CATEGORIES_DIR + "loans_defaulters_csv",
                     CLEANED_DIR + "loans_defaulters_parquet",
                     CLEANED_DIR + "loans_defaulters_records_parquet")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()