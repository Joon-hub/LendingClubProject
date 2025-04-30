#!/usr/bin/env python
# coding: utf-8

# #### Data cleaning plan for loans-repayments dataset
# 
# 1. **Initialize SparkSession**
# 
# 2. **Define loans-repayments schema**
# 
# 3. **Load raw repayments CSV**
#    - Set `header=True`
#    - Apply predefined schema
# 
# 4. **Inspect raw DataFrame**
#    - Show sample rows
#    - Print schema
# 
# 5. **Add ingestion timestamp** (`ingest_date`)
# 
# 6. **Create temporary view** `"loan_repayments"`
# 
# 7. **Data quality checks**
#    - Total record count
#    - Count of null `total_principal_received`
# 
# 8. **Drop rows with nulls in critical repayment fields**
# 
# 9. **Recreate temp view**
# 
# 10. **Identify zero-payment anomalies**
#     - Count `total_payment_received == 0.0`
#     - Count rows where `total_payment_received == 0.0` AND `total_principal_received != 0.0`
#     - Show those anomalous rows
# 
# 11. **Fix `total_payment_received` where anomalies exist**
# 
# 12. **Filter out any remaining zero-payment rows**
# 
# 13. **Clean up date fields**
#     - Replace `0.0` in `last_payment_date` with `null`
#     - Replace `0.0` in `next_payment_date` with `null`
# 
# 14. **Save cleaned DataFrame**
#     - Parquet → `/user/itv006277/lendingclubproject/raw/cleaned/loans_repayments_parquet`
#     - CSV → `/user/itv006277/lendingclubproject/raw/cleaned/loans_repayments_csv`
# 

# ### 1. Initialize SparkSession

# In[1]:


from pyspark.sql import SparkSession
import getpass 
username=getpass.getuser()
spark=SparkSession.     builder.     config('spark.ui.port','0').     config("spark.sql.warehouse.dir", f"/user/{username}/warehouse").     config('spark.shuffle.useOldFetchProtocol', 'true').     enableHiveSupport().     master('yarn').     getOrCreate()


# ### 2. Define loans‑repayments schema

# In[2]:


# Define exact column names and types for repayments data
loans_repay_schema = """
    loan_id string,
    total_principal_received float,
    total_interest_received float,
    total_late_fee_received float,
    total_payment_received float,
    last_payment_amount float,
    last_payment_date string,
    next_payment_date string
"""


# ### 3. Load raw repayments CSV

# In[3]:


# Read the raw CSV with header and enforced schema
loans_repay_raw_df = (
    spark.read
      .format("csv")
      .option("header", True)
      .schema(loans_repay_schema)
      .load("/public/trendytech/lendingclubproject/raw/loans_repayments_csv")
)


# In[4]:


# Peek at first few rows
loans_repay_raw_df.show(5)


# ### 4. Inspect raw DataFrame

# In[5]:


# Print schema to verify types
loans_repay_raw_df.printSchema()


# ### 5. Add ingestion timestamp (`ingest_date`)

# In[6]:


from pyspark.sql.functions import current_timestamp

# Tag each row with the ingestion time
loans_repay_df_ingestd = loans_repay_raw_df.withColumn("ingest_date", current_timestamp())
loans_repay_df_ingestd.show(5)


# ### 6. Create temporary view "loan_repayments"

# In[7]:


loans_repay_df_ingestd.createOrReplaceTempView("loan_repayments")


# ### 7. Data quality checks

# In[8]:


# Total records
spark.sql("SELECT COUNT(*) FROM loan_repayments").show()

# How many missing principal?
spark.sql("""
    SELECT COUNT(*) 
      FROM loan_repayments 
     WHERE total_principal_received IS NULL
""").show()


# ### 8. Drop rows with nulls in critical fields

# In[9]:


# Define fields that must not be null
columns_to_check = [
    "total_principal_received",
    "total_interest_received",
    "total_late_fee_received",
    "total_payment_received",
    "last_payment_amount"
]

# Drop any rows missing one of these
loans_repay_filtered_df = loans_repay_df_ingestd.na.drop(subset=columns_to_check)
print("After drop:", loans_repay_filtered_df.count())

# Refresh SQL view
loans_repay_filtered_df.createOrReplaceTempView("loan_repayments")


# ### 9. Identify zero‑payment anomalies

# In[10]:


# Count zero-payment rows
spark.sql("""
    SELECT COUNT(*) 
      FROM loan_repayments 
     WHERE total_payment_received = 0.0
""").show()


# In[11]:


# Count those where principal != 0 but payment = 0
spark.sql("""
    SELECT COUNT(*) 
      FROM loan_repayments 
     WHERE total_payment_received = 0.0 
       AND total_principal_received != 0.0
""").show()


# In[12]:


# Show some examples of these anomalies
spark.sql("""
    SELECT * 
      FROM loan_repayments 
     WHERE total_payment_received = 0.0 
       AND total_principal_received != 0.0
     LIMIT 5
""").show()


# ### 10. Fix total_payment_received for anomalies

# In[13]:


from pyspark.sql.functions import when, col

# Where principal ≠ 0 & payment = 0, recompute payment as principal + interest + late fee
loans_payments_fixed_df = loans_repay_filtered_df.withColumn(
    "total_payment_received",
    when(
        (col("total_principal_received") != 0.0) &
        (col("total_payment_received") == 0.0),
        col("total_principal_received") 
          + col("total_interest_received") 
          + col("total_late_fee_received")
    ).otherwise(col("total_payment_received"))
)

loans_payments_fixed_df.show(5)


# In[14]:


# even after fixing loan_payments there are values 0.0, then we drop those rows
loans_payments_fixed2_df = loans_payments_fixed_df.filter("total_payment_received != 0.0")


# ### 11. Filter out any remaining zero‑payment rows

# In[15]:


loans_payments_fixed2_df.filter("last_payment_date = 0.0").count()


# In[16]:


loans_payments_fixed2_df.filter("next_payment_date =0.0").count()


# In[17]:


loans_payments_fixed2_df.filter("last_payment_date is null").count()


# In[18]:


loans_payments_fixed2_df.filter("next_payment_date is null").count()


# In[19]:


from pyspark.sql.functions import when

# For last_payment_date
loans_payments_ldate_fixed_df = loans_payments_fixed2_df.withColumn(
    "last_payment_date",
    when(col("last_payment_date") == "0.0", None)
     .otherwise(col("last_payment_date"))
)

# For next_payment_date
loans_payments_ndate_fixed_df = loans_payments_ldate_fixed_df.withColumn(
    "next_payment_date",
    when(col("next_payment_date") == "0.0", None)
     .otherwise(col("next_payment_date"))
)


# ### 12. Clean date fields: replace `0.0` with null

# In[20]:


from pyspark.sql.functions import when

# For last_payment_date
loans_payments_ldate_fixed_df = loans_payments_fixed2_df.withColumn(
    "last_payment_date",
    when(col("last_payment_date") == "0.0", None)
     .otherwise(col("last_payment_date"))
)

# For next_payment_date
loans_payments_ndate_fixed_df = loans_payments_ldate_fixed_df.withColumn(
    "next_payment_date",
    when(col("next_payment_date") == "0.0", None)
     .otherwise(col("next_payment_date"))
)

loans_payments_ndate_fixed_df.show(5)


# ### 13. Save cleaned DataFrame

# In[21]:


# Parquet (optimized format)
loans_payments_ndate_fixed_df.write     .format("parquet")     .mode("overwrite")     .option("path", "/user/itv017499/lendingclubproject/cleaned/loans_repayments_parquet")     .save()

