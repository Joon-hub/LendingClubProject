#!/usr/bin/env python
# coding: utf-8

# #### Data cleaning plan for loans dataset
# 
# 1. **Initialize SparkSession**
# 
# 2. **Define loans schema**
# 
# 3. **Load raw loans CSV**
#    - Set `header=True`
#    - Apply predefined schema
# 
# 4. **Inspect raw DataFrame**
#    - Display first few rows
#    - Print schema
# 
# 5. **Add ingestion timestamp** (`ingest_date`)
# 
# 6. **Create temporary view**
# 
# 7. **Quick data quality checks**
#    - Total record count
#    - Count of `loan_amount` nulls
# 
# 8. **Drop rows with nulls in critical columns**
# 
# 9. **Re‑inspect record count**
# 
# 10. **Clean `loan_term_months`**
#     - Remove the `" months"` suffix via `regexp_replace`
#     - Cast to `Integer`
# 
# 11. **Inspect and clean `loan_purpose`**
#     - Show distinct values & counts
#     - Define a lookup list of main purposes
#     - Map all others to `"other"`
# 
# 12. **Save cleaned loans data**
#     - Parquet to `/cleaned/loans_parquet`
#     - CSV to `/cleaned/loans_csv`
# 

# ### 1. Initialize SparkSession

# In[1]:


from pyspark.sql import SparkSession
import getpass 
username=getpass.getuser()
spark=SparkSession.     builder.     config('spark.ui.port','0').     config("spark.sql.warehouse.dir", f"/user/{username}/warehouse").     config('spark.shuffle.useOldFetchProtocol', 'true').     enableHiveSupport().     master('yarn').     getOrCreate()


# ### 2. Define loans schema

# In[2]:


# Defining the exact types and names of each loan column
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


# ### 3. Load raw loans CSV

# In[3]:


loans_raw_df = (
    spark.read
      .format("csv")
      .option("header", True)         
      .schema(loans_schema) 
      .load("/public/trendytech/lendingclubproject/raw/loans_data_csv")
)


# In[4]:


# create a temp view for sql queries
loans_raw_df.createOrReplaceTempView('loans')


# In[5]:


# Quick peek
spark.sql("select*from loans limit 5")


# ### 4. Inspect raw DataFrmae

# In[6]:


# Verify column types and nullability
loans_raw_df.printSchema()


# ### 5. Add ingestion timestamp (`ingest_date`)

# In[7]:


from pyspark.sql.functions import current_timestamp
loans_df_ingested = loans_raw_df.withColumn("ingest_date",current_timestamp())


# In[8]:


# create a temp view for sql queries
loans_df_ingested.createOrReplaceTempView('loans')


# ### 6. Create temporary view

# In[9]:


# Quick peek
spark.sql("select*from loans limit 5")


# ### 7. Quick data quality checks

# In[10]:


# Total number of records
spark.sql("SELECT COUNT(*) FROM loans").show()


# In[11]:



# How many rows are missing loan_amount?
spark.sql("SELECT COUNT(*) FROM loans WHERE loan_amount IS NULL").show()


# In[12]:


# checking where loan_amount is NULL
spark.sql("SELECT * FROM loans WHERE loan_amount IS NULL limit 5")


# ### 8. Drop rows with nulls in critical columns

# In[13]:


# Define which columns must not be null
columns_to_check = [
    "loan_amount", "funded_amount", "loan_term_months",
    "monthly_installment", "issue_date", "loan_status", "loan_purpose"
]


# In[14]:


# Remove any rows missing those
loans_filtered_df = loans_df_ingested.na.drop(subset=columns_to_check)


# In[15]:


print("After drop:", loans_filtered_df.count())


# In[16]:


loans_filtered_df.createOrReplaceTempView("loans")


# ### 9. Clean `loan_term_months`

# In[17]:


from pyspark.sql.functions import regexp_replace, col

# Strip out non‑digits (e.g. "36 months" → "36") and cast to int and convert it into year
loans_term_modified_df = (
    loans_filtered_df
      .withColumn(
         "loan_term_months",
         regexp_replace(col("loan_term_months"), "\D", "")
      )
      .withColumn("loan_term_years", col("loan_term_months").cast("int")/12)
      .drop("loan_term_months")
)


# In[18]:


loans_term_modified_df.printSchema()


# In[19]:


loans_term_modified_df.createOrReplaceTempView("loans")


# In[20]:


spark.sql("SELECT * FROM loans limit 10")


# ### 10. Inspect and clean `loan_purpose`

# In[21]:


# Check what purposes we have
spark.sql("SELECT loan_purpose, COUNT(*) AS cnt FROM loans GROUP BY loan_purpose ORDER BY cnt DESC").show(20)


# In[22]:


# Define the main-purpose list; everything else → "other"
main_purposes = [
"debt_consolidation", "credit_card", "home_improvement", 
    "other", "major_purchase", "medical", "small_business", 
    "car", "vacation", "moving", "house", "wedding", "renewable_energy", 
    "educational"
]


# In[23]:


from pyspark.sql.functions import when

# Map any non‑standard purpose to "other"
loans_purpose_modified = (
    loans_term_modified_df.withColumn(
      "loan_purpose",
      when(col("loan_purpose").isin(main_purposes), col("loan_purpose"))
        .otherwise("other")
    )
)


# In[24]:


loans_purpose_modified.createOrReplaceTempView("loans")


# In[25]:


# verify
spark.sql("""
    SELECT loan_purpose, COUNT(*) AS cnt 
    FROM loans 
    GROUP BY loan_purpose 
    ORDER BY cnt DESC
""")


# ### 11. Save cleaned loans data

# In[26]:


# Parquet for efficient downstream queries
loans_purpose_modified.write     .format("parquet")     .mode("overwrite")     .option("path", "/user/itv017499/lendingclubproject/cleaned/loans_parquet")     .save()


# In[ ]:




