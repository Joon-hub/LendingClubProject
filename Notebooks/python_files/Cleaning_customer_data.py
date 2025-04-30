#!/usr/bin/env python
# coding: utf-8

# #### Cleaning Customer Dataset - Plan of Action
# 
# 1. **Initialize SparkSession**
# 
# 2. **Define customer schema**
# 
# 3. **Load raw customers CSV**
#    - Set `header=True`
#    - Apply predefined schema
# 
# 4. **Inspect raw DataFrame**
#    - Show sample rows
#    - Print schema
# 
# 5. **Rename columns for clarity**
# 
# 6. **Add ingestion timestamp** (`ingest_date`)
# 
# 7. **Deduplicate rows**
# 
# 8. **Create temp view** `"customers"`
# 
# 9. **Filter out rows with NULL** `annual_income`
# 
# 10. **Clean `emp_length`**
#     - Show distinct raw values
#     - Remove non-digits via `regexp_replace`
#     - Cast to `Integer`
#     - Count resulting NULLs
#     - Compute `floor(avg(emp_length))`
#     - Fill NULL `emp_length` with average
# 
# 11. **Clean `address_state`**
#     - Inspect distinct state codes
#     - Count codes where length > 2
#     - Replace invalid codes (>2 chars) with `"NA"`
# 
# 12. **Save final DataFrame**
#     - Parquet format to `/cleaned/customers_parquet`
#     - CSV format to `/cleaned/customers_csv`
# 

# ####  1. SET UP SPARK SESSION 

# In[1]:


# Build a SparkSession with Hive support on YARN

from pyspark.sql import SparkSession
import getpass 
username=getpass.getuser()
spark=SparkSession.     builder.     config('spark.ui.port','0').     config("spark.sql.warehouse.dir", f"/user/{username}/warehouse").     config('spark.shuffle.useOldFetchProtocol', 'true').     enableHiveSupport().     master('yarn').     getOrCreate()


# #### 2. DEFINE AND LOAD CUSTOMERS DATA 

# In[2]:


# Define schema for customers CSV 
customer_schema = 'member_id string, emp_title string, emp_length string, home_ownership string, annual_inc float, addr_state string, zip_code string, country string, grade string, sub_grade string, verification_status string, tot_hi_cred_lim float, application_type string, annual_inc_joint float, verification_status_joint string'


# In[3]:


# Read the raw customers CSV into a DataFrame
customers_raw_df = spark.read .format("csv") .option("header",True) .schema(customer_schema) .load("/public/trendytech/lendingclubproject/raw/customers_data_csv")


# In[4]:


# Peek at the loaded DataFrame
customers_raw_df.show(5)


# In[5]:


# verify the schema
customers_raw_df.printSchema()


# #### 3. RENAME COLUMNS FOR CLARITY

# In[6]:


customer_df_renamed = customers_raw_df.withColumnRenamed("annual_inc", "annual_income") .withColumnRenamed("addr_state", "address_state") .withColumnRenamed("zip_code", "address_zipcode") .withColumnRenamed("country", "address_country") .withColumnRenamed("tot_hi_credit_lim", "total_high_credit_limit") .withColumnRenamed("annual_inc_joint", "join_annual_income")


# In[7]:


customer_df_renamed.show(2)


# #### 4. ADD INGESTION TIMESTAMP

# In[8]:


from pyspark.sql.functions import current_timestamp

# Tag each row with the timestamp when we ingested it
customers_df_ingestd = customer_df_renamed.withColumn("ingest_date", current_timestamp())
customers_df_ingestd.show(5)


# In[9]:


# Quick counts before deduplication
print("Total rows before dedup:", customers_df_ingestd.count())


# #### 5. DEDUPLICATE

# In[10]:


customers_distinct = customers_df_ingestd.distinct()
print("Total rows after dedup:", customers_distinct.count())


# In[11]:


# Make a SQL view for the next steps
customers_distinct.createOrReplaceTempView("customers")


# #### 6. FILTER OUT MISSING ANNUAL INCOME

# In[12]:


# How many have null annual_income?
spark.sql("select count(*) from customers where annual_income is null").show()


# In[13]:


# Keep only those with a valid annual_income
customers_income_filtered = spark.sql("""
    select * from customers
    where annual_income is not null
""")


# In[14]:


customers_income_filtered.createOrReplaceTempView("customers")


# In[15]:


spark.sql('select * from customers limit 5')


# #### 7. CLEAN EMPLOYMENT LENGTH

# In[16]:


# See what values exist
spark.sql("select distinct(emp_length) from customers").show()


# In[17]:


from pyspark.sql.functions import regexp_replace, col

# Strip out any non-digits (e.g. '10+ years' → '10')
customers_emplength_cleaned = customers_income_filtered.withColumn(
    "emp_length",
    regexp_replace(col("emp_length"), "\D", ""))


# In[18]:


# Cast the cleaned string to integer
customers_emplength_casted = customers_emplength_cleaned.withColumn(
    "emp_length",
    customers_emplength_cleaned.emp_length.cast("int")
)


# In[19]:


# Check how many ended up null after cast
print("Null emp_length after cast:", 
      customers_emplength_casted.filter("emp_length is null").count())


# In[20]:


# Make a SQL table for the next steps
customers_emplength_casted.createOrReplaceTempView("customers")


# In[21]:


# Compute the average (floored) and fill nulls
avg_emp_length = spark.sql("select floor(avg(emp_length)) as avg_emp_length from customers")                      .collect()[0][0]


# In[22]:


customers_emplength_replaced = customers_emplength_casted.na.fill(
    avg_emp_length, subset=["emp_length"]
)


# In[23]:


customers_emplength_replaced.createOrReplaceTempView("customers")
spark.sql('select * from customers limit 5')


# In[24]:


# checking any null values in emp_length column
customers_emplength_replaced.filter('emp_length is null').count()


# #### 8. CLEAN ADDRESS STATE CODES

# In[25]:


# Inspect distinct state codes and length issues
spark.sql("select distinct(address_state) from customers").show()


# In[26]:


spark.sql("select count(*) from customers where length(address_state) > 2").show()


# In[27]:


from pyspark.sql.functions import when, length

# Replace any code longer than 2 chars with 'NA'
customers_state_cleaned = customers_emplength_replaced.withColumn(
    "address_state",
    when(length(col("address_state")) > 2, "NA")
     .otherwise(col("address_state"))
)


# In[28]:


customers_state_cleaned.select("address_state").distinct()


# #### 9. SAVE CLEANED DATA

# In[29]:


# Write out parquet (efficient for downstream processing)
customers_state_cleaned.write     .format("parquet")     .mode("overwrite")     .option("path", "/user/itv017499/lendingclubproject/cleaned/customers_parquet")     .save()


# In[ ]:




