#!/usr/bin/env python
# coding: utf-8

# ## Creating External tables

# In[1]:


from pyspark.sql import SparkSession
import getpass 
username=getpass.getuser()
spark=SparkSession.     builder.     config('spark.ui.port','0').     config("spark.sql.warehouse.dir", f"/user/{username}/warehouse").     config('spark.shuffle.useOldFetchProtocol', 'true').     enableHiveSupport().     master('yarn').     getOrCreate()


# In[2]:


customers_df = spark.read .format("parquet") .load("/public/trendytech/lendingclubproject/cleaned/customers_parquet")


# In[4]:


# creating new database 
spark.sql("create database itv017499_lending_club")


# ### cusotmers table

# In[6]:


# create cusotmers table
spark.sql("""
    CREATE EXTERNAL TABLE itv017499_lending_club.customers (
        member_id STRING, 
        emp_title STRING, 
        emp_length INT, 
        home_ownership STRING, 
        annual_income FLOAT, 
        address_state STRING, 
        address_zipcode STRING, 
        address_country STRING, 
        grade STRING, 
        sub_grade STRING, 
        verification_status STRING, 
        total_high_credit_limit FLOAT, 
        application_type STRING, 
        join_annual_income FLOAT, 
        verification_status_joint STRING, 
        ingest_date TIMESTAMP
    )
    STORED AS PARQUET 
    LOCATION '/public/trendytech/lendingclubproject/cleaned/customers_parquet'
""")


# In[17]:


# top 5 rows
spark.sql("select * from itv017499_lending_club.customers limit 5")


# ### loans table

# In[8]:


# creating loans table
spark.sql("""
    CREATE EXTERNAL TABLE itv017499_lending_club.loans (
        loan_id STRING, 
        member_id STRING, 
        loan_amount FLOAT, 
        funded_amount FLOAT, 
        loan_term_years INTEGER, 
        interest_rate FLOAT, 
        monthly_installment FLOAT, 
        issue_date STRING, 
        loan_status STRING, 
        loan_purpose STRING, 
        loan_title STRING, 
        ingest_date TIMESTAMP
    )
    STORED AS PARQUET 
    LOCATION '/public/trendytech/lendingclubproject/cleaned/loans_parquet'
""")


# In[18]:


# top 5 rows
spark.sql("select * from itv017499_lending_club.loans limit 5")


# ### loans repayments table

# In[10]:


# creating loans_repayments table
spark.sql("""
    CREATE EXTERNAL TABLE itv017499_lending_club.loans_repayments (
        loan_id STRING, 
        total_principal_received FLOAT, 
        total_interest_received FLOAT, 
        total_late_fee_received FLOAT, 
        total_payment_received FLOAT, 
        last_payment_amount FLOAT, 
        last_payment_date STRING, 
        next_payment_date STRING, 
        ingest_date TIMESTAMP
    )
    STORED AS PARQUET 
    LOCATION '/public/trendytech/lendingclubproject/cleaned/loans_repayments_parquet'
""")


# In[19]:


# top 5 rows
spark.sql("select * from itv017499_lending_club.loans_repayments limit 5")


# ### loans_defaulters_delinq table

# In[12]:


#creating loans_defaulters_delinq table
spark.sql("""
    CREATE EXTERNAL TABLE itv017499_lending_club.loans_defaulters_delinq (
        member_id STRING, 
        delinq_2yrs INTEGER, 
        delinq_amnt FLOAT, 
        mths_since_last_delinq INTEGER
    )
    STORED AS PARQUET 
    LOCATION '/public/trendytech/lendingclubproject/cleaned/loans_defaulters_delinq_parquet'
""")


# In[21]:


# top 5 rows
spark.sql("select * from itv017499_lending_club.loans_defaulters_delinq limit 5")


# ### loans_defaulters_detail_rec_enq table

# In[14]:


# create loans_defaulters_detail_rec_enq table
spark.sql("""
    CREATE EXTERNAL TABLE itv017499_lending_club.loans_defaulters_detail_rec_enq (
        member_id STRING, 
        pub_rec INTEGER, 
        pub_rec_bankruptcies INTEGER, 
        inq_last_6mths INTEGER
    )
    STORED AS PARQUET 
    LOCATION '/public/trendytech/lendingclubproject/cleaned/loans_defaulters_detail_records_enq_parquet'
""")


# In[22]:


# top 5 rows 
spark.sql("select * from itv017499_lending_club.loans_defaulters_detail_rec_enq limit 5")

