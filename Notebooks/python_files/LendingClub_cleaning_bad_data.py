#!/usr/bin/env python
# coding: utf-8

# ## Identifying bad data

# In[1]:


# import sparksession
from pyspark.sql import SparkSession
import getpass 
username=getpass.getuser()
spark=SparkSession.     builder.     config('spark.ui.port','0').     config("spark.sql.warehouse.dir", f"/user/{username}/warehouse").     config('spark.shuffle.useOldFetchProtocol', 'true').     enableHiveSupport().     master('yarn').     getOrCreate()


# ### Reviewing customer table for bad data

# In[2]:


# checking repaeating member_id in customer table
spark.sql("""
    select member_id, 
           count(*) as total
    from itv017499_lending_club.customers
    group by member_id 
    order by total desc
""")


# In[3]:


# viewing of a repeating member_id
spark.sql("""
    select *
    from itv017499_lending_club.customers
    where member_id like 'e4c167053d5418230%'
""")


# **Note:** The `member_id` `e4c167053d5418230` is associated with multiple values for `total_high_credit_limit`, which is inconsistent with the expected data structure. Ideally, each member should have only one `total_high_credit_limit`. This discrepancy indicates that the data is flawed or incorrect.

# ### Reviewing Loans defaulter delinquent table for bad data

# In[4]:


# checking repaeating member_id in loans_defaulters_delinq table
spark.sql("""
    select member_id, 
           count(*) as total
    from itv017499_lending_club.loans_defaulters_delinq
    group by member_id 
    order by total desc
""")


# In[5]:


# viewing indvidual case
spark.sql("""
    select *
    from itv017499_lending_club.loans_defaulters_delinq
    where member_id like 'e4c167053d5418230%'
""")


# **Note:** There is ambiguity in the data, as the same member_id is associated with different delinquency values. This inconsistency is observed in the delinq_2yrs column and indicates a flaw in the data. Such discrepancies need to be addressed to ensure data accuracy and reliability.

# ### Reviewing loans defaulters detail records enquiry table for bad data

# In[6]:


# checking repaeating member_id in loans_defaulters_detail_rec_enq table

spark.sql("""
    select member_id, 
           count(*) as total
    from itv017499_lending_club.loans_defaulters_detail_rec_enq
    group by member_id 
    order by total desc
""")


# In[7]:


# viewing indvidual case
spark.sql("""
    select *
    from itv017499_lending_club.loans_defaulters_detail_rec_enq
    where member_id like 'e4c167053d5418230%'
""")


# **Note:** The data shows inconsistencies in the `inq_last_6mths` column for the same `member_id` (`e4c167053d5418230`). Different records indicate varying values. These discrepancies suggest flaws in the data, as the inquiry number in the last six months should ideally be consistent for a single customer or member.  

# ### No of repeating member_ids in 3 dataset
# - customer dataset
# - loans_defaulters_delinq dataset
# - loans_defaulters_detail_rec_enq

# #### No of repeating member_ids in customer dataset

# In[8]:


# finding member_id repeating more than one time in customer table
bad_data_customer_df = spark.sql("""
    select member_id
    from (
        select member_id, 
               count(*) as total
        from itv017499_lending_club.customers
        group by member_id
        having total > 1
    )
""")


# In[9]:


# total distinct no of member id repeating for more than one time
bad_data_customer_df.count()


# #### No of repeating member_ids in 3 loans_defaulters_delinq dataset

# In[10]:


# finding member_id repeating more than one time in loans_defaulters_delinq table
bad_data_loans_defaulters_delinq_df = spark.sql("""
    select member_id
    from (
        select member_id, 
               count(*) as total
        from itv017499_lending_club.loans_defaulters_delinq
        group by member_id
        having total > 1
    )
""")


# In[11]:


# total distinct no of member id repeating for more than one time
bad_data_loans_defaulters_delinq_df.count()


# #### No of repeating member_ids in 3 loans_defaulters_detail_rec_enq dataset

# In[12]:


# finding member_id repeating more than one time in loans_defaulters_detail_rec_enq table

bad_data_loans_defaulters_detail_rec_enq_df = spark.sql("""
    select member_id
    from (
        select member_id, 
               count(*) as total
        from itv017499_lending_club.loans_defaulters_detail_rec_enq
        group by member_id
        having total > 1
    )
""")


# In[13]:


# total distinct no of member id repeating for more than one time
bad_data_loans_defaulters_detail_rec_enq_df.count()


# ### Saving the bad data in the database

# In[14]:


# Writing the bad_data_customer_df DataFrame to a single CSV file.
bad_data_customer_df.repartition(1).write     .format("csv")     .option("header", True)     .mode("overwrite")     .option("path", "/user/itv017499/lendingclubproject/bad/bad_data_customers")     .save()

# Writing the bad_data_loans_defaulters_delinq_df DataFrame to a single CSV file.
bad_data_loans_defaulters_delinq_df.repartition(1).write     .format("csv")     .option("header", True)     .mode("overwrite")     .option("path", "/user/itv017499/lendingclubproject/bad/bad_data_loans_defaulters_delinq")     .save()

# Writing the bad_data_loans_defaulters_detail_rec_enq_df DataFrame to a single CSV file.
bad_data_loans_defaulters_detail_rec_enq_df.repartition(1).write     .format("csv")     .option("header", True)     .mode("overwrite")     .option("path", "/user/itv017499/lendingclubproject/bad/bad_data_loans_defaulters_detail_rec_enq")     .save()


# In[15]:


# Combining all bad customer data into a single DataFrame.
# Selects the 'member_id' column from each DataFrame containing flawed data: 
# Using the union operation to merge the data into bad_customer_data_df.

bad_customer_data_df = bad_data_customer_df.select("member_id")     .union(bad_data_loans_defaulters_delinq_df.select("member_id"))     .union(bad_data_loans_defaulters_detail_rec_enq_df.select("member_id"))


# In[16]:


# Removing duplicate member_id entries from the combined bad customer data.
bad_customer_data_final_df = bad_customer_data_df.distinct()

# Counting the number of unique member_id entries in the final DataFrame.
bad_customer_data_final_df.count()


# In[17]:


# Writing the bad_customer_data_final_df DataFrame to a single CSV file.
# The data is repartitioned to ensure only one CSV file is created.

bad_customer_data_final_df.repartition(1).write     .format("csv")     .option("header", True)     .mode("overwrite")     .option("path", "/user/itv017499/lendingclubproject/bad/bad_customer_data_final")     .save()


# In[18]:


# creating a temp view of bad_customer df
bad_customer_data_final_df.createOrReplaceTempView("bad_data_customer")


# #### Saving customers_df into new cleaned_new directory 

# In[19]:


# Creating a DataFrame to filter out bad customer data.
customers_df = spark.sql("""
    select *
    from itv017499_lending_club.customers
    where member_id NOT IN (
        select member_id
        from bad_data_customer
    )
""")

# writing clean customers_df into database
# The data will be saved in the specified path in the "cleaned_new" directory.
customers_df.write .format("parquet") .mode("overwrite") .option("path", "/user/itv017499/lendingclubproject/raw/cleaned_new/customers_parquet") .save()


# #### Saving loans_defaulters_delinq_parquet into new cleaned_new directory 

# In[20]:


# Filtering the loans_defaulters_delinq table to exclude bad customer data.
# where the member_id is NOT present in the bad_data_customer table.
loans_defaulters_delinq_df = spark.sql("""
    select *
    from itv017499_lending_club.loans_defaulters_delinq
    where member_id NOT IN (
        select member_id
        from bad_data_customer
    )
""")

# Writing the filtered loans_defaulters_delinq DataFrame to a Parquet file.
# The data will be saved in the specified path in the "cleaned_new" directory.
loans_defaulters_delinq_df.write     .format("parquet")     .mode("overwrite")     .option("path", "/user/itv017499/lendingclubproject/raw/cleaned_new/loans_defaulters_delinq_parquet")     .save()


# #### Saving loans_defaulters_detail_rec_enq_parquet into new cleaned_new directory 

# In[21]:


# Filtering the loans_defaulters_detail_rec_enq table to exclude bad customer data.
# The query selects all rows from the loans_defaulters_detail_rec_enq table 
# where the member_id is NOT present in the bad_data_customer table.
loans_defaulters_detail_rec_enq_df = spark.sql("""
    select *
    from itv017499_lending_club.loans_defaulters_detail_rec_enq
    where member_id NOT IN (
        select member_id
        from bad_data_customer
    )
""")

# Writing the filtered loans_defaulters_detail_rec_enq DataFrame to a Parquet file.
# The data will be saved in the specified path in the "cleaned_new" directory.
# If a file already exists at the location, it will be overwritten.
loans_defaulters_detail_rec_enq_df.write     .format("parquet")     .mode("overwrite")     .option("path", "/user/itv017499/lendingclubproject/raw/cleaned_new/loans_defaulters_detail_rec_enq_parquet")     .save()


# ### Creating External tables in hive Database

# In[22]:


# Creating an external table named 'customers_new' in the Hive database.
# The table includes various fields like member_id, emp_title, emp_length, etc., 
# to store customer-related information.
# Data will be stored as Parquet format and will reside at the specified location.

spark.sql("""
    create EXTERNAL TABLE itv017499_lending_club.customers_new (
        member_id string, 
        emp_title string, 
        emp_length int, 
        home_ownership string, 
        annual_income float, 
        address_state string, 
        address_zipcode string, 
        address_country string, 
        grade string, 
        sub_grade string, 
        verification_status string, 
        total_high_credit_limit float, 
        application_type string, 
        join_annual_income float, 
        verification_status_joint string, 
        ingest_date timestamp
    )
    stored as parquet
    LOCATION '/public/trendytech/lendingclubproject/cleaned_new/customer_parquet'
""")


# In[23]:


# Creating an external table named 'loans_defaulters_delinq_new' in the Hive database.
# The table includes fields such as member_id, delinq_2yrs, delinq_amnt, and mths_since_last_delinq 
# to store information about loan defaulters and their delinquencies.
# Data will be stored as Parquet format and will reside at the specified location.

spark.sql("""
    create EXTERNAL TABLE itv017499_lending_club.loans_defaulters_delinq_new (
        member_id string,
        delinq_2yrs integer,
        delinq_amnt float,
        mths_since_last_delinq integer
    )
    stored as parquet
    LOCATION '/public/trendytech/lendingclubproject/cleaned_new/loan_defaulters_delinq_parquet'
""")


# In[24]:


# Creating an external table named 'loans_defaulters_detail_rec_enq_new' in the Hive database.
# The table includes fields such as member_id, pub_rec, pub_rec_bankruptcies, and inq_last_6mths 
# to store detailed information about loan defaulters and their recent inquiries or public records.
# Data will be stored in Parquet format and will reside at the specified location.

spark.sql("""
    create EXTERNAL TABLE itv017499_lending_club.loans_defaulters_detail_rec_enq_new (
        member_id string,
        pub_rec integer,
        pub_rec_bankruptcies integer,
        inq_last_6mths integer
    )
    stored as parquet
    LOCATION '/public/trendytech/lendingclubproject/cleaned_new/loan_defaulters_detail_rec_enq_parquet'
""")


# In[25]:


# Query to count the occurrences of each member_id in the customers_new table.
# Groups the data by member_id and calculates the total count for each.
# The results are ordered in descending order of the total count.

spark.sql("""
    select member_id, 
           count(*) as total 
    from itv017499_lending_club.customers_new
    group by member_id
    order by total desc
""")

