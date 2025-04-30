#!/usr/bin/env python
# coding: utf-8

# # Data Cleaning and Creating detailed defalult user Tables 

# ### Loans_defaulters data

# In[1]:


# Import required modules and initialize SparkSession for PySpark operations
from pyspark.sql import SparkSession
import getpass 
username=getpass.getuser()
spark=SparkSession.     builder.     config('spark.ui.port','0').     config("spark.sql.warehouse.dir", f"/user/{username}/warehouse").     config('spark.shuffle.useOldFetchProtocol', 'true').     enableHiveSupport().     master('yarn').     getOrCreate()


# ### Load the raw data

# In[2]:


# Read raw loan defaulters CSV data with inferred schema
loans_def_raw_df = spark.read .format("csv") .option("header",True) .option("inferSchema", True) .load("/public/trendytech/lendingclubproject/raw/loans_defaulters_csv")


# In[3]:


#peek into the data
loans_def_raw_df


# In[4]:


# checking schema
loans_def_raw_df.printSchema()


# ### Initial Exploration

# In[5]:


# Create temporary view (loan_defaulters)
loans_def_raw_df.createOrReplaceTempView("loan_defaulters")

# Check distinct delinq_2yrs values
spark.sql("select distinct(delinq_2yrs) from loan_defaulters")


# In[6]:


# Count occurrences of delinq_2yrs (reveals data issues)
spark.sql("select delinq_2yrs, count(*) as total from loan_defaulters group by delinq_2yrs order by total desc").show(40)


# ### Handle Data Quality Issues

# In[7]:


# Define new schema (float types)
loan_defaulters_schema = "member_id string, delinq_2yrs float, delinq_amnt float, pub_rec float, pub_rec_bankruptcies float,inq_last_6mths float, total_rec_late_fee float, mths_since_last_delinq float, mths_since_last_record float"

# Re-read CSV with new schema
loans_def_raw_df = spark.read .format("csv") .option("header",True) .schema(loan_defaulters_schema) .load("/public/trendytech/lendingclubproject/raw/loans_defaulters_csv")

# creating temp view
loans_def_raw_df.createOrReplaceTempView("loan_defaulters")

# Non Int values appeared as NULL
spark.sql("select delinq_2yrs, count(*) as total from loan_defaulters group by delinq_2yrs order by total desc").show(40)


# - Now we have 261 nulls after changing datatype of deling_2yrs columns from string to float 

# In[8]:


# Cast delinq_2yrs to integer and Fill nulls with 0
from pyspark.sql.functions import col

loans_def_processed_df = (
    loans_def_raw_df
    .withColumn("delinq_2yrs", col("delinq_2yrs").cast("integer"))
    .fillna(0, subset=["delinq_2yrs"])
)

# creating temp view
loans_def_processed_df.createOrReplaceTempView("loan_defaulters")

# checking for null values
spark.sql("select count(*) from loan_defaulters where delinq_2yrs is null")


# ### Filter members with delinquencies in the past 2 years or a delinquency record

# In[9]:


# checking for active deliquent defaulters
loans_def_delinq_df = spark.sql("""
                                select member_id,delinq_2yrs, delinq_amnt, int(mths_since_last_delinq) 
                                from loan_defaulters 
                                where delinq_2yrs > 0 
                                or mths_since_last_delinq > 0""")

# counts
loans_def_delinq_df.count()


# ### Filter members with public records, bankruptcies, or inquiries in the last 6 months

# In[10]:


# Filter members with public records, bankruptcies, or inquiries in the last 6 months
loans_def_records_enq_df = spark.sql("""select member_id 
                                     from loan_defaulters 
                                     where pub_rec > 0.0 
                                     or pub_rec_bankruptcies > 0.0 
                                     or inq_last_6mths > 0.0
                                     """)

# count
loans_def_records_enq_df.count()


# ### Saving loans_def_delinq_df

# In[11]:


#parquet
loans_def_delinq_df.write .format("parquet") .mode("overwrite") .option("path", "/user/itv017499/lendingclubproject/cleaned/loans_defaulters_deling_parquet") .save()


# ### Saving loans_def_records_enq_df

# In[12]:


#parquet
loans_def_records_enq_df.write .format("parquet") .mode("overwrite") .option("path", "/user/itv017499/lendingclubproject/cleaned/loans_defaulters_records_enq_parquet") .save()


# ### Casting loans_def_processed_df columns into int and fillna with 0
# - pub_rec
# - pub_rec_bankruptcies
# - inq_last_6mths

# In[13]:


loans_def_p_pub_rec_df = loans_def_processed_df.withColumn("pub_rec", col("pub_rec").cast("integer")).fillna(0, subset = ["pub_rec"])
loans_def_p_pub_rec_bankruptcies_df = loans_def_p_pub_rec_df.withColumn("pub_rec_bankruptcies", col("pub_rec_bankruptcies").cast("integer")).fillna(0, subset = ["pub_rec_bankruptcies"])
loans_def_p_inq_last_6mths_df = loans_def_p_pub_rec_bankruptcies_df.withColumn("inq_last_6mths", col("inq_last_6mths").cast("integer")).fillna(0, subset = ["inq_last_6mths"])


# ### Select detailed records for public records and inquiries

# In[14]:


loans_def_p_inq_last_6mths_df.createOrReplaceTempView("loan_defaulters")
loans_def_detail_records_enq_df = spark.sql("select member_id, pub_rec, pub_rec_bankruptcies, inq_last_6mths from loan_defaulters")


# In[15]:


loans_def_detail_records_enq_df


# ### Saving loans_def_detail_records_enq_df

# In[16]:


#parquet
loans_def_detail_records_enq_df.write .format("parquet") .mode("overwrite") .option("path", "/user/itv017499/lendingclubproject/cleaned/loans_def_detail_records_enq_df_parquet") .save()


# In[ ]:




