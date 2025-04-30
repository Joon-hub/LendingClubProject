#!/usr/bin/env python
# coding: utf-8

# #### Data cleaning plan for loan-defaulters dataset
# 
# 1. **Initialize SparkSession**
# 
# 2. **Define defaulters schema**
# 
# 3. **Load raw loan-defaulters CSV**
#    - Set `header=True`
#    - Apply predefined schema
# 
# 4. **Inspect raw DataFrame**
#    - Show sample rows
#    - Print schema
# 
# 5. **Create temporary view** `"loan_defaulters"`
# 
# 6. **Explore `delinq_2yrs`**
#    - List distinct values
#    - Count by value
# 
# 7. **Clean `delinq_2yrs`**
#    - Cast to `Integer`
#    - Fill nulls with `0`
# 
# 8. **Re-register temp view**
# 
# 9. **Validate no nulls remain in `delinq_2yrs`**
# 
# 10. **Filter primary defaulters**
#     - Keep rows where `delinq_2yrs > 0` OR `mths_since_last_delinq > 0`
# 
# 11. **Count defaulter records**
# 
# 12. **Filter all records with credit issues**
#     - Condition:
#       ```
#       delinq_2yrs > 0  
#       OR pub_rec_bankruptcies > 0  
#       OR inq_last_6mths > 0
#       ```
# 
# 13. **Count those “enforcement” records**
# 
# 14. **Save both DataFrames**
#     - Parquet → `/user/itv006277/lendingclubproject/raw/cleaned/loans_defaulters_parquet`
#     - CSV → `/user/itv006277/lendingclubproject/raw/cleaned/loans_defaulters_csv`
# 

# ### 1. Initialize SparkSession

# In[1]:


from pyspark.sql import SparkSession
import getpass

# Use OS username for a user-specific warehouse directory
username = getpass.getuser()

spark = (
    SparkSession.builder
      .config('spark.ui.port', '0')
      .config('spark.sql.warehouse.dir', f"/user/{username}/warehouse")
      .config('spark.shuffle.useOldFetchProtocol', 'true')
      .enableHiveSupport()
      .master('yarn')
      .getOrCreate()
)


# ### 2. Define defaulters schema

# In[2]:


# Exact column names & types for defaulters dataset
loan_defaulters_schema = """
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


# ### 3. Load raw loan‑defaulters CSV

# In[3]:


loans_def_raw_df = (
    spark.read
      .format("csv")
      .option("header", True)                    
      .schema(loan_defaulters_schema)           
      .load("/public/trendytech/lendingclubproject/raw/loans_defaulters_csv")
)

# Quick look at data
loans_def_raw_df.show(5)


# ### 4. Inspect raw DataFrame
# 

# In[4]:


# Verify the column types and nullability
loans_def_raw_df.printSchema()


# ### 5. Create temporary view "loan_defaulters"
# 

# In[5]:


loans_def_raw_df.createOrReplaceTempView("loan_defaulters")


# ### 6. Explore `delinq_2yrs`

# In[6]:


# Distinct values
spark.sql("SELECT DISTINCT(delinq_2yrs) FROM loan_defaulters").show()

# Count by each distinct value
spark.sql("""
    SELECT delinq_2yrs, COUNT(*) AS total
      FROM loan_defaulters
  GROUP BY delinq_2yrs
  ORDER BY total DESC
""").show(40)


# ### 7. Clean `delinq_2yrs`
# 

# In[7]:


from pyspark.sql.functions import col

# Cast to integer and fill any nulls with 0
loans_def_processed_df = (
    loans_def_raw_df
      .withColumn("delinq_2yrs", col("delinq_2yrs").cast("integer"))
      .fillna(0, subset=["delinq_2yrs"])
)

# Update the view
loans_def_processed_df.createOrReplaceTempView("loan_defaulters")


# ### 8. Re‑validate `delinq_2yrs` nulls

# In[8]:


# Should be zero now
spark.sql("SELECT COUNT(*) FROM loan_defaulters WHERE delinq_2yrs IS NULL").show()

# Re‑check distribution
spark.sql("""
    SELECT delinq_2yrs, COUNT(*) AS total
      FROM loan_defaulters
  GROUP BY delinq_2yrs
  ORDER BY total DESC
""").show(40)


# ### 9. Filter primary defaulters

# In[9]:


# Keep those with any recent delinquencies
loans_def_delinq_df = spark.sql("""
    SELECT member_id, delinq_2yrs, delinq_amnt, int(mths_since_last_delinq)
      FROM loan_defaulters
     WHERE delinq_2yrs > 0
        OR mths_since_last_delinq > 0
""")
loans_def_delinq_df


# In[10]:


print("Primary defaulters count:", loans_def_delinq_df.count())


# ### 10. Filter all records with credit issues

# In[11]:


# Broader “enforcement”: bankruptcies, inquiries, or delinquencies
loans_def_records_enq_df = spark.sql("""
    SELECT *
      FROM loan_defaulters
     WHERE pub_rec                   > 0
        OR pub_rec_bankruptcies      > 0
        OR inq_last_6mths            > 0
""")
loans_def_records_enq_df.show(5)


# In[12]:


print("Records with any credit issues:", loans_def_records_enq_df.count())


# ### 11. Save cleaned DataFrames

# In[13]:


# 11a. Save primary defaulters
loans_def_delinq_df.write     .format("parquet")     .mode("overwrite")     .option("path", "/user/itv017499/lendingclubproject/cleaned/loans_defaulters_parquet")     .save()
# 11b. Save broader credit‐issue records
loans_def_records_enq_df.write     .format("parquet")     .mode("overwrite")     .option("path", "/user/itv017499/lendingclubproject/cleaned/loans_defaulters_records_parquet")     .save()

