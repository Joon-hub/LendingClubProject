#!/usr/bin/env python
# coding: utf-8

# #### --- RAW DATA ANALYSIS ---

# In[1]:


#import libraries
from pyspark.sql import SparkSession

spark = SparkSession.    builder.    appName('raw_data_cap_project').    master('yarn').    getOrCreate()


# In[2]:


raw_df = spark.read.format('csv').option("InferSchema","True").option("header","True").load('/public/trendytech/datasets/accepted_2007_to_2018Q4.csv')


# In[3]:


# convert df to table for SQL queries
raw_df.createOrReplaceTempView("lending_club_data")


# In[5]:


# viewing the top5 rows of dataset
spark.sql("select * from lending_club_data LIMIT 5")


# - The id column in the dataset represents the unique identifier for each loan (loan_id).
# 
# - The member_id column contains only null values, so it cannot be used to identify borrowers.
# 
# - Since a single member can take multiple loans, the same member might appear more than once in the dataset.
# 
# - To uniquely identify each member, we use a combination of features that are likely to remain consistent across multiple loans taken by the same person.
# 
# - We selected the following nine features to construct a unique member ID: ["emp_title", "emp_length", "home_ownership", "annual_inc", "zip_code", "addr_state", "grade", "sub_grade", "verification_status"].

# In[4]:


# Import necessary PySpark functions
from pyspark.sql.functions import sha2,concat_ws

# Create a new column 'name_sha2' by combining selected features and applying SHA-256 hash.
# This hashed value serves as a unique member ID.
new_df = raw_df.withColumn("name_sha2", sha2(concat_ws("||", *["emp_title", "emp_length", "home_ownership", "annual_inc", "zip_code", "addr_state", "grade", "sub_grade","verification_status"]), 256))


# In[5]:


# Register the DataFrame as a temporary SQL view for querying
new_df.createOrReplaceTempView("newtable")


# #### --- VALIDATION STEPS ---

# In[11]:


# checking total no of records

row_count = spark.sql("select count(*) as total from lending_club_data").collect()[0]['total']
print(f'No of rows in dataframe: {row_count}')


# In[9]:


# Number of columns in the dataframe
len(raw_df.columns)


# In[12]:


# checking number of unique member IDs (name_sha2 values)
spark.sql("select count(distinct(name_sha2)) from newtable")


# In[13]:


# Find how many records share the same hashed ID (i.e., potential duplicates)
spark.sql("""
    SELECT name_sha2, COUNT(*) AS total_cnt 
    FROM newtable 
    GROUP BY name_sha2 
    HAVING total_cnt > 1 
    ORDER BY total_cnt DESC
""")


# record1 has 33 common hash id which is exception/ outlier 

# In[14]:


# Investigate empty or default hash values (often generated from null or empty feature combinations)
spark.sql("select * from newtable where name_sha2 like 'e3b0c44298fc1c149%'")


# #### --- EXPORT CUSTOMERS DATA ---

# In[38]:


# Export customer-specific attributes with the hashed member_id and additional country field as customers_data in a csv file
spark.sql("""select name_sha2 as member_id,emp_title,emp_length,home_ownership,annual_inc,addr_state,zip_code,'USA' as country,grade,sub_grade,
verification_status,tot_hi_cred_lim,application_type,annual_inc_joint,verification_status_joint from newtable
""").repartition(1).write \
.option("header","true")\
.format("csv") \
.mode("overwrite") \
.option("path", "/user/itv017499/lendingclubproject/raw/customers_data_csv") \
.save()


# In[41]:


# Load the saved customers data for inspection
customers_df = spark.read .format("csv") .option("InferSchema","true") .option("header","true") .load("/user/itv017499/lendingclubproject/raw/customers_data_csv")


# In[42]:


# View the loaded DataFrame
customers_df.show(5)


# #### --- EXPORT LOANS DATA ---

# In[43]:


# Extract and save loan-related data with the member_id and loan_id
spark.sql("""select id as loan_id, name_sha2 as member_id,loan_amnt,funded_amnt,term,int_rate,installment,issue_d,loan_status,purpose,
title from newtable""").repartition(1).write \
.option("header",True)\
.format("csv") \
.mode("overwrite") \
.option("path", "/user/itv017499/lendingclubproject/raw/loans_data_csv") \
.save()


# In[44]:


# Load the loan data
loans_df = spark.read .format("csv") .option("InferSchema","true") .option("header","true") .load("/user/itv017499/lendingclubproject/raw/loans_data_csv")


# In[45]:


# Load the loan data
loans_df.show(5)


# #### --- EXPORT LOAN repayments DATA ---

# In[46]:


# Extract and export repayment details related to each loan

spark.sql("""select id as loan_id,total_rec_prncp,total_rec_int,total_rec_late_fee,total_pymnt,last_pymnt_amnt,last_pymnt_d,next_pymnt_d from newtable""").repartition(1).write .option("header",True).format("csv") .mode("overwrite") .option("path", "/user/itv017499/lendingclubproject/raw/loans_repayments_csv") .save()


# In[47]:


# Load the repayment data
loans_repayments_df = spark.read .format("csv") .option("InferSchema","true") .option("header","true") .load("/user/itv017499/lendingclubproject/raw/loans_repayments_csv")


# In[48]:


# View the repayments DataFrame
loans_repayments_df.show(5)


# #### --- EXPORT LOAN DEFAULTERS DATA ---

# In[6]:


# Export credit behavior and delinquency information for identifying defaulters
spark.sql("""select name_sha2 as member_id,delinq_2yrs,delinq_amnt,pub_rec,pub_rec_bankruptcies,inq_last_6mths,total_rec_late_fee,mths_since_last_delinq,mths_since_last_record from newtable""").repartition(1).write .option("header",True).format("csv") .mode("overwrite") .option("path", "/user/itv017499/lendingclubproject/raw/loans_defaulters_csv") .save()


# In[7]:


# Load the defaulters data

loans_defaulters_df = spark.read .format("csv") .option("InferSchema","true") .option("header","true") .load("/user/itv017499/lendingclubproject/raw/loans_defaulters_csv")


# In[8]:


# View the defaulters DataFrame
loans_defaulters_df.show(5)


# In[10]:


spark.stop()


# In[ ]:




