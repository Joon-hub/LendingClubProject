#!/usr/bin/env python
# coding: utf-8

# ### Access Patterns (Old data) and Slow Access(New Data)

# In[1]:


from pyspark.sql import SparkSession
import getpass 
username=getpass.getuser()
spark=SparkSession.     builder.     config('spark.ui.port','0').     config("spark.sql.warehouse.dir", f"/user/{username}/warehouse").     config('spark.shuffle.useOldFetchProtocol', 'true').     enableHiveSupport().     master('yarn').     getOrCreate()


# ### Single view of 5 tables
# - most recent data (it will be slow for query)
# - view updated every 24 hours 
# - source of data is the cleaned data 
# - the data will be stored on hive 

# In[10]:


# recent data 

spark.sql("""
create or replace view itv017499_lending_club.customers_loan_view as select
l.loan_id,
c.member_id,
c.emp_title,
c.emp_length,
c.home_ownership,
c.annual_income,
c.address_state,
c.address_zipcode,
c.address_country,
c.grade,
c.sub_grade,
c.verification_status,
c.total_high_credit_limit,
c.application_type,
c.join_annual_income,
c.verification_status_joint,
l.loan_amount,
l.funded_amount,
l.loan_term_years,
l.interest_rate,
l.monthly_installment,
l.issue_date,
l.loan_status,
l.loan_purpose,
r.total_principal_received,
r.total_interest_received,
r.total_late_fee_received,
r.last_payment_date,
r.next_payment_date,
d.delinq_2yrs,
d.delinq_amnt,
d.mths_since_last_delinq,
e.pub_rec,
e.pub_rec_bankruptcies,
e.inq_last_6mths

FROM itv017499_lending_club.customers c
LEFT JOIN itv017499_lending_club.loans l on c.member_id = l.member_id
LEFT JOIN itv017499_lending_club.loans_repayments r ON l.loan_id = r.loan_id
LEFT JOIN itv017499_lending_club.loans_defaulters_delinq d ON c.member_id = d.member_id
LEFT JOIN itv017499_lending_club.loans_defaulters_detail_rec_enq e ON c.member_id = e.member_id
""")


# **Note:** Creating a view would be much faster as there is no actual data processing taking place. However, a query to view the data, like the following 
# 
# <span style="color:red;">spark.sql("select * from itv017499_lending_club.customers_loan_view limit 5")</span>
# 
# 
# This query will take time to execute as it involves joining multiple tables to generate a view with the desired data.

# In[11]:


# top 5 rows
spark.sql("select * from itv017499_lending_club.customers_loan_view limit 5")


# ### Creating permanent table for weekly job 
# - We have a weekly job
# - The joins of 5 tables is precalculated and stored in a table in the DB 
# - query will be faster 
# - even though the results are faster in this case but the data will be week old
# - in this case it will be a **managed table**

# In[6]:


# weekly job

spark.sql("""
create table itv017499_lending_club.customers_loan_table as select
l.loan_id,
c.member_id,
c.emp_title,
c.emp_length,
c.home_ownership,
c.annual_income,
c.address_state,
c.address_zipcode,
c.address_country,
c.grade,
c.sub_grade,
c.verification_status,
c.total_high_credit_limit,
c.application_type,
c.join_annual_income,
c.verification_status_joint,
l.loan_amount,
l.funded_amount,
l.loan_term_years,
l.interest_rate,
l.monthly_installment,
l.issue_date,
l.loan_status,
l.loan_purpose,
r.total_principal_received,
r.total_interest_received,
r.total_late_fee_received,
r.last_payment_date,
r.next_payment_date,
d.delinq_2yrs,
d.delinq_amnt,
d.mths_since_last_delinq,
e.pub_rec,
e.pub_rec_bankruptcies, 
e.inq_last_6mths

FROM itv006277_lending_club.customers c
LEFT JOIN itv017499_lending_club.loans l on c.member_id = l.member_id
LEFT JOIN itv017499_lending_club.loans_repayments r ON l.loan_id = r.loan_id
LEFT JOIN itv017499_lending_club.loans_defaulters_delinq d ON c.member_id = d.member_id
LEFT JOIN itv017499_lending_club.loans_defaulters_detail_rec_enq e ON c.member_id = e.member_id
""")


# **Note:** In this case, a Managed Table is created. The actual data for this table will be stored in the warehouse directory and the metadata is present in the Hive metastore.

# In[7]:


# top 5 rows
spark.sql("select * from itv017499_lending_club.customers_loan_table limit 5")

