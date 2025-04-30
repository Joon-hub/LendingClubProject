#!/usr/bin/env python
# coding: utf-8

# # Calculating Loan Score and grades

# ### Intialize sparksession

# In[1]:


from pyspark.sql import SparkSession
import getpass 
username=getpass.getuser()
spark=SparkSession.     builder.     config('spark.ui.port','0').     config("spark.sql.warehouse.dir", f"/user/{username}/warehouse").     config('spark.shuffle.useOldFetchProtocol', 'true').     enableHiveSupport().     master('yarn').     getOrCreate()


# ### Associating points to the grades in order to calculate the Loan Score

# In[2]:


spark.conf.set("spark.sql.unacceptable_rated_pts", 0)
spark.conf.set("spark.sql.very_bad_rated_pts", 100)
spark.conf.set("spark.sql.bad_rated_pts", 250)
spark.conf.set("spark.sql.good_rated_pts", 500)
spark.conf.set("spark.sql.very_good_rated_pts", 650)
spark.conf.set("spark.sql.excellent_rated_pts", 800)


# In[3]:


spark.conf.set("spark.sql.unacceptable_grade_pts", 750)
spark.conf.set("spark.sql.very_bad_grade_pts", 1000)
spark.conf.set("spark.sql.bad_grade_pts", 1500)
spark.conf.set("spark.sql.good_grade_pts", 2000)
spark.conf.set("spark.sql.very_good_grade_pts", 2500)


# ### The tables required to calculate the Loan Score

# customers_new 
# 
# loans
# 
# loans_repayments
# 
# loans_defaulters_delinq_new
# 
# loans_defaulters_detail_red_enq_new

# ### Loan Score Calculation Criteria 1: Payment History(ph)

# In[4]:


bad_customer_data_final_df = spark.read .format("csv") .option("header", True) .option("inferSchema", True) .load("/public/trendytech/lendingclubproject/bad/bad_customer_data_final")


# In[6]:


# createing a temp view
bad_customer_data_final_df.createOrReplaceTempView("bad_data_customer")

# applying loan score condition on loans_repayments and loans table (Not processing members who have taken multiple loans)
ph_df = spark.sql("select c.member_id,    case    when p.last_payment_amount < (c.monthly_installment * 0.5) then ${spark.sql.very_bad_rated_pts}    when p.last_payment_amount >= (c.monthly_installment * 0.5) and p.last_payment_amount < c.monthly_installment then ${spark.sql.very_bad_rated_pts}    when (p.last_payment_amount = (c.monthly_installment)) then ${spark.sql.good_rated_pts}    when p.last_payment_amount > (c.monthly_installment) and p.last_payment_amount <= (c.monthly_installment * 1.50) then ${spark.sql.very_good_rated_pts}    when p.last_payment_amount > (c.monthly_installment * 1.50) then ${spark.sql.excellent_rated_pts}    else ${spark.sql.unacceptable_rated_pts}    end as last_payment_pts,    case    when p.total_payment_received >= (c.funded_amount * 0.50) then ${spark.sql.very_good_rated_pts}    when p.total_payment_received < (c.funded_amount * 0.50) and p.total_payment_received > 0 then ${spark.sql.good_rated_pts}    when p.total_payment_received = 0 or (p.total_payment_received) is null then ${spark.sql.unacceptable_rated_pts}    end as total_payment_pts from itv005857_lending_club.loans_repayments p inner join itv005857_lending_club.loans c on c.loan_id = p.loan_id where member_id NOT IN (select member_id from bad_data_customer)")


# In[7]:


# creating temp view 
ph_df.createOrReplaceTempView("ph_pts")

# top 10 rows 
spark.sql("select * from ph_pts")


# ### Loan Score Calculation Criteria 2: Loan Defaulters History(ldh)

# In[9]:


# applying loan score condition on 
# loans_defaulters_detail_rec_enq_new
# loans_defaulters_delinq_new and 
# ph_data table (Not processing members who have taken multiple loans)

ldh_ph_df = spark.sql(
    "select p.*, \
    CASE \
    WHEN d.delinq_2yrs = 0 THEN ${spark.sql.excellent_rated_pts} \
    WHEN d.delinq_2yrs BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} \
    WHEN d.delinq_2yrs BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} \
    WHEN d.delinq_2yrs > 5 OR d.delinq_2yrs IS NULL THEN ${spark.sql.unacceptable_grade_pts} \
    END AS delinq_pts, \
    CASE \
    WHEN l.pub_rec = 0 THEN ${spark.sql.excellent_rated_pts} \
    WHEN l.pub_rec BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} \
    WHEN l.pub_rec BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} \
    WHEN l.pub_rec > 5 OR l.pub_rec IS NULL THEN ${spark.sql.very_bad_rated_pts} \
    END AS public_records_pts, \
    CASE \
    WHEN l.pub_rec_bankruptcies = 0 THEN ${spark.sql.excellent_rated_pts} \
    WHEN l.pub_rec_bankruptcies BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} \
    WHEN l.pub_rec_bankruptcies BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} \
    WHEN l.pub_rec_bankruptcies > 5 OR l.pub_rec_bankruptcies IS NULL THEN ${spark.sql.very_bad_rated_pts} \
    END as public_bankruptcies_pts, \
    CASE \
    WHEN l.inq_last_6mths = 0 THEN ${spark.sql.excellent_rated_pts} \
    WHEN l.inq_last_6mths BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} \
    WHEN l.inq_last_6mths BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} \
    WHEN l.inq_last_6mths > 5 OR l.inq_last_6mths IS NULL THEN ${spark.sql.unacceptable_rated_pts} \
    END AS enq_pts \
    FROM itv005857_lending_club.loans_defaulters_detail_rec_enq_new l \
    INNER JOIN itv005857_lending_club.loans_defaulters_delinq_new d ON d.member_id = l.member_id  \
    INNER JOIN ph_pts p ON p.member_id = l.member_id where l.member_id NOT IN (select member_id from bad_data_customer)")


# In[10]:


ldh_ph_df.createOrReplaceTempView("ldh_ph_pts")
spark.sql("select * from ldh_ph_pts limit 10")


# ### Criteria 3: Financial health (fh)

# In[12]:


fh_ldh_ph_df = spark.sql("select ldef.*,    CASE    WHEN LOWER(l.loan_status) LIKE '%fully paid%' THEN ${spark.sql.excellent_rated_pts}    WHEN LOWER(l.loan_status) LIKE '%current%' THEN ${spark.sql.good_rated_pts}    WHEN LOWER(l.loan_status) LIKE '%in grace period%' THEN ${spark.sql.bad_rated_pts}    WHEN LOWER(l.loan_status) LIKE '%late (16-30 days)%' OR LOWER(l.loan_status) LIKE '%late (31-120 days)%' THEN ${spark.sql.very_bad_rated_pts}    WHEN LOWER(l.loan_status) LIKE '%charged off%' THEN ${spark.sql.unacceptable_rated_pts}    else ${spark.sql.unacceptable_rated_pts}    END AS loan_status_pts,    CASE    WHEN LOWER(a.home_ownership) LIKE '%own' THEN ${spark.sql.excellent_rated_pts}    WHEN LOWER(a.home_ownership) LIKE '%rent' THEN ${spark.sql.good_rated_pts}    WHEN LOWER(a.home_ownership) LIKE '%mortgage' THEN ${spark.sql.bad_rated_pts}    WHEN LOWER(a.home_ownership) LIKE '%any' OR LOWER(a.home_ownership) IS NULL THEN ${spark.sql.very_bad_rated_pts}    END AS home_pts,    CASE    WHEN l.funded_amount <= (a.total_high_credit_limit * 0.10) THEN ${spark.sql.excellent_rated_pts}    WHEN l.funded_amount > (a.total_high_credit_limit * 0.10) AND l.funded_amount <= (a.total_high_credit_limit * 0.20) THEN ${spark.sql.very_good_rated_pts}    WHEN l.funded_amount > (a.total_high_credit_limit * 0.20) AND l.funded_amount <= (a.total_high_credit_limit * 0.30) THEN ${spark.sql.good_rated_pts}    WHEN l.funded_amount > (a.total_high_credit_limit * 0.30) AND l.funded_amount <= (a.total_high_credit_limit * 0.50) THEN ${spark.sql.bad_rated_pts}    WHEN l.funded_amount > (a.total_high_credit_limit * 0.50) AND l.funded_amount <= (a.total_high_credit_limit * 0.70) THEN ${spark.sql.very_bad_rated_pts}    WHEN l.funded_amount > (a.total_high_credit_limit * 0.70) THEN ${spark.sql.unacceptable_rated_pts}    else ${spark.sql.unacceptable_rated_pts}    END AS credit_limit_pts,    CASE    WHEN (a.grade) = 'A' and (a.sub_grade)='A1' THEN ${spark.sql.excellent_rated_pts}    WHEN (a.grade) = 'A' and (a.sub_grade)='A2' THEN (${spark.sql.excellent_rated_pts} * 0.95)    WHEN (a.grade) = 'A' and (a.sub_grade)='A3' THEN (${spark.sql.excellent_rated_pts} * 0.90)    WHEN (a.grade) = 'A' and (a.sub_grade)='A4' THEN (${spark.sql.excellent_rated_pts} * 0.85)    WHEN (a.grade) = 'A' and (a.sub_grade)='A5' THEN (${spark.sql.excellent_rated_pts} * 0.80)    WHEN (a.grade) = 'B' and (a.sub_grade)='B1' THEN (${spark.sql.very_good_rated_pts})    WHEN (a.grade) = 'B' and (a.sub_grade)='B2' THEN (${spark.sql.very_good_rated_pts} * 0.95)    WHEN (a.grade) = 'B' and (a.sub_grade)='B3' THEN (${spark.sql.very_good_rated_pts} * 0.90)    WHEN (a.grade) = 'B' and (a.sub_grade)='B4' THEN (${spark.sql.very_good_rated_pts} * 0.85)    WHEN (a.grade) = 'B' and (a.sub_grade)='B5' THEN (${spark.sql.very_good_rated_pts} * 0.80)    WHEN (a.grade) = 'C' and (a.sub_grade)='C1' THEN (${spark.sql.good_rated_pts})    WHEN (a.grade) = 'C' and (a.sub_grade)='C2' THEN (${spark.sql.good_rated_pts} * 0.95)    WHEN (a.grade) = 'C' and (a.sub_grade)='C3' THEN (${spark.sql.good_rated_pts} * 0.90)    WHEN (a.grade) = 'C' and (a.sub_grade)='C4' THEN (${spark.sql.good_rated_pts} * 0.85)    WHEN (a.grade) = 'C' and (a.sub_grade)='C5' THEN (${spark.sql.good_rated_pts} * 0.80)    WHEN (a.grade) = 'D' and (a.sub_grade)='D1' THEN (${spark.sql.bad_rated_pts})    WHEN (a.grade) = 'D' and (a.sub_grade)='D2' THEN (${spark.sql.bad_rated_pts} * 0.95)    WHEN (a.grade) = 'D' and (a.sub_grade)='D3' THEN (${spark.sql.bad_rated_pts} * 0.90)    WHEN (a.grade) = 'D' and (a.sub_grade)='D4' THEN (${spark.sql.bad_rated_pts} * 0.85)    WHEN (a.grade) = 'D' and (a.sub_grade)='D5' THEN (${spark.sql.bad_rated_pts} * 0.80)    WHEN (a.grade) = 'E' and (a.sub_grade)='E1' THEN (${spark.sql.very_bad_rated_pts})    WHEN (a.grade) = 'E' and (a.sub_grade)='E2' THEN (${spark.sql.very_bad_rated_pts} * 0.95)    WHEN (a.grade) = 'E' and (a.sub_grade)='E3' THEN (${spark.sql.very_bad_rated_pts} * 0.90)    WHEN (a.grade) = 'E' and (a.sub_grade)='E4' THEN (${spark.sql.very_bad_rated_pts} * 0.85)    WHEN (a.grade) = 'E' and (a.sub_grade)='E5' THEN (${spark.sql.very_bad_rated_pts} * 0.80)    WHEN (a.grade) in ('F', 'G') THEN (${spark.sql.unacceptable_rated_pts})    END AS grade_pts    FROM ldh_ph_pts ldef    INNER JOIN itv005857_lending_club.loans l ON ldef.member_id = l.member_id    INNER JOIN itv005857_lending_club.customers_new a ON a.member_id = ldef.member_id where ldef.member_id NOT IN (select member_id from bad_data_customer)") 


# In[13]:


fh_ldh_ph_df.createOrReplaceTempView("fh_ldh_ph_pts")
spark.sql("select * from fh_ldh_ph_pts limit 10")


# ### Final loan score calculation by considering all the 3 criterias with the following %**

# #### 1. Payment History = 20%
# #### 2. Loan Defaults = 45%
# #### 3. Financial Health = 35%

# In[14]:


loan_score = spark.sql("SELECT member_id, ((last_payment_pts+total_payment_pts)*0.20) as payment_history_pts, ((delinq_pts + public_records_pts + public_bankruptcies_pts + enq_pts) * 0.45) as defaulters_history_pts, ((loan_status_pts + home_pts + credit_limit_pts + grade_pts)*0.35) as financial_health_pts FROM fh_ldh_ph_pts")


# In[15]:


loan_score


# ### Calculating final loan grade 

# In[16]:


final_loan_score = loan_score.withColumn('loan_score', loan_score.payment_history_pts + loan_score.defaulters_history_pts + loan_score.financial_health_pts)


# In[17]:


final_loan_score.createOrReplaceTempView("loan_score_eval")


# In[18]:


loan_score_final = spark.sql("select ls.*, case WHEN loan_score > ${spark.sql.very_good_grade_pts} THEN 'A' WHEN loan_score <= ${spark.sql.very_good_grade_pts} AND loan_score > ${spark.sql.good_grade_pts} THEN 'B' WHEN loan_score <= ${spark.sql.good_grade_pts} AND loan_score > ${spark.sql.bad_grade_pts} THEN 'C' WHEN loan_score <= ${spark.sql.bad_grade_pts} AND loan_score  > ${spark.sql.very_bad_grade_pts} THEN 'D' WHEN loan_score <= ${spark.sql.very_bad_grade_pts} AND loan_score > ${spark.sql.unacceptable_grade_pts} THEN 'E'  WHEN loan_score <= ${spark.sql.unacceptable_grade_pts} THEN 'F' end as loan_final_grade from loan_score_eval ls")


# In[19]:


loan_score_final.createOrReplaceTempView("loan_final_table")


# In[20]:


spark.sql("select * from loan_final_table where loan_final_grade in ('C')")


# In[21]:


spark.sql("select count(*) from loan_final_table")


# ### Saving final dataframe with loan_scores and loan_grades

# In[ ]:


loan_score_final.write .format("parquet") .mode("overwrite") .option("path", "/user/itv006277/lendingclubproject/processed/loan_score") .save()


# In[ ]:


spark.stop()

