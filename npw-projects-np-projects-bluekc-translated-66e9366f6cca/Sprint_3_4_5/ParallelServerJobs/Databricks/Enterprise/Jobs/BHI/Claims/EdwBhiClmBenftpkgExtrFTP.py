# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  This Job FTP's benefit_package_ref file
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC HASH FILES:  
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                    Project/Ticket #\(9)           Change Description\(9)\(9)\(9)              Development Project\(9)      Code Reviewer\(9)        Date Reviewed       
# MAGIC ------------------                    -------------------            ---------------------------\(9)           -----------------------------------------------------------------------            --------------------------------\(9)      -------------------------         -------------------------    
# MAGIC Aishwarya                     2016-07-19                5604                                       Original programming                                                         EnterpriseDevl                Kalyan Neelam           2016-07-19

# MAGIC Job Name: benefit_package_ref File FTP
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


ProdIn = get_widget_value('ProdIn','')

schema_benefit_package_ref = StructType([
    StructField("BHI_HOME_PLN_ID", StringType(), False),
    StructField("BNF_PCKG_ID", StringType(), False),
    StructField("MED_BNF_RMBRMT", StringType(), False),
    StructField("INDV_DEDCT_AMT", StringType(), False),
    StructField("INDV_PLUS_1_DEDCT_AMT", StringType(), False),
    StructField("INDV_PLUS_2_DEDCT_AMT", StringType(), False),
    StructField("FMLY_DEDCT_AMT", StringType(), False),
    StructField("EMBD_DEDCT_IN", StringType(), False),
    StructField("INDV_OOP_MAX_AMT", StringType(), False),
    StructField("INDV_PLUS_1_OOP_MAX_AMT", StringType(), False),
    StructField("INDV_PLUS_2_OOP_MAX_AMT", StringType(), False),
    StructField("FMLY_OOP_MAX_AMT", StringType(), False),
    StructField("EMBD_OOP_MAX_AMT_IN", StringType(), False),
    StructField("PCP_COPAY_AMT", StringType(), False),
    StructField("SPLST_PHYS_COPAY_AMT", StringType(), False),
    StructField("SPLST_PHYS_COPAY_IN", StringType(), False),
    StructField("SPLST_PHYS_AVG_COPAY_AMT", StringType(), False),
    StructField("IP_FCLTY_COPAY_AMT", StringType(), False),
    StructField("OP_FCLTY_COPAY_AMT", StringType(), False),
    StructField("ER_COPAY_AMT", StringType(), False),
    StructField("ADVD_IMG_COPAY_AMT", StringType(), False),
    StructField("ADVD_IMG_COPAY_IN", StringType(), False),
    StructField("ADVD_IMG_AVG_COPAY_AMT", StringType(), False),
    StructField("SPLST_PHYS_COINS_PCT", StringType(), False),
    StructField("SPLST_PHYS_COINS_IN", StringType(), False),
    StructField("SPLST_PHYS_AVG_COINS_PCT", StringType(), False),
    StructField("IP_FCLTY_COINS_PCT", StringType(), False),
    StructField("OP_FCLTY_COINS_PCT", StringType(), False),
    StructField("ER_COINS_PCT", StringType(), False),
    StructField("ADVD_IMG_COINS_PCT", StringType(), False),
    StructField("ADVD_IMG_COINS_IN", StringType(), False),
    StructField("ADVD_IMG_AVG_COINS_PCT", StringType(), False),
    StructField("COINS_MAX_AMT", StringType(), False),
    StructField("EFF_DT", StringType(), False),
    StructField("EXPRTN_DT", StringType(), False)
])

df_benefit_package_ref = (
    spark.read
    .option("header", False)
    .option("quote", None)
    .option("sep", ",")
    .schema(schema_benefit_package_ref)
    .csv(f"{adls_path_publish}/external/benefit_package_ref")
)

df_ftp_benefit_package_ref = df_benefit_package_ref.select(
    rpad("BHI_HOME_PLN_ID", 3, " ").alias("BHI_HOME_PLN_ID"),
    rpad("BNF_PCKG_ID", 15, " ").alias("BNF_PCKG_ID"),
    rpad("MED_BNF_RMBRMT", 4, " ").alias("MED_BNF_RMBRMT"),
    rpad("INDV_DEDCT_AMT", 11, " ").alias("INDV_DEDCT_AMT"),
    rpad("INDV_PLUS_1_DEDCT_AMT", 11, " ").alias("INDV_PLUS_1_DEDCT_AMT"),
    rpad("INDV_PLUS_2_DEDCT_AMT", 11, " ").alias("INDV_PLUS_2_DEDCT_AMT"),
    rpad("FMLY_DEDCT_AMT", 11, " ").alias("FMLY_DEDCT_AMT"),
    rpad("EMBD_DEDCT_IN", 1, " ").alias("EMBD_DEDCT_IN"),
    rpad("INDV_OOP_MAX_AMT", 11, " ").alias("INDV_OOP_MAX_AMT"),
    rpad("INDV_PLUS_1_OOP_MAX_AMT", 11, " ").alias("INDV_PLUS_1_OOP_MAX_AMT"),
    rpad("INDV_PLUS_2_OOP_MAX_AMT", 11, " ").alias("INDV_PLUS_2_OOP_MAX_AMT"),
    rpad("FMLY_OOP_MAX_AMT", 11, " ").alias("FMLY_OOP_MAX_AMT"),
    rpad("EMBD_OOP_MAX_AMT_IN", 1, " ").alias("EMBD_OOP_MAX_AMT_IN"),
    rpad("PCP_COPAY_AMT", 11, " ").alias("PCP_COPAY_AMT"),
    rpad("SPLST_PHYS_COPAY_AMT", 11, " ").alias("SPLST_PHYS_COPAY_AMT"),
    rpad("SPLST_PHYS_COPAY_IN", 1, " ").alias("SPLST_PHYS_COPAY_IN"),
    rpad("SPLST_PHYS_AVG_COPAY_AMT", 11, " ").alias("SPLST_PHYS_AVG_COPAY_AMT"),
    rpad("IP_FCLTY_COPAY_AMT", 11, " ").alias("IP_FCLTY_COPAY_AMT"),
    rpad("OP_FCLTY_COPAY_AMT", 11, " ").alias("OP_FCLTY_COPAY_AMT"),
    rpad("ER_COPAY_AMT", 11, " ").alias("ER_COPAY_AMT"),
    rpad("ADVD_IMG_COPAY_AMT", 11, " ").alias("ADVD_IMG_COPAY_AMT"),
    rpad("ADVD_IMG_COPAY_IN", 1, " ").alias("ADVD_IMG_COPAY_IN"),
    rpad("ADVD_IMG_AVG_COPAY_AMT", 11, " ").alias("ADVD_IMG_AVG_COPAY_AMT"),
    rpad("SPLST_PHYS_COINS_PCT", 6, " ").alias("SPLST_PHYS_COINS_PCT"),
    rpad("SPLST_PHYS_COINS_IN", 1, " ").alias("SPLST_PHYS_COINS_IN"),
    rpad("SPLST_PHYS_AVG_COINS_PCT", 6, " ").alias("SPLST_PHYS_AVG_COINS_PCT"),
    rpad("IP_FCLTY_COINS_PCT", 6, " ").alias("IP_FCLTY_COINS_PCT"),
    rpad("OP_FCLTY_COINS_PCT", 6, " ").alias("OP_FCLTY_COINS_PCT"),
    rpad("ER_COINS_PCT", 6, " ").alias("ER_COINS_PCT"),
    rpad("ADVD_IMG_COINS_PCT", 6, " ").alias("ADVD_IMG_COINS_PCT"),
    rpad("ADVD_IMG_COINS_IN", 1, " ").alias("ADVD_IMG_COINS_IN"),
    rpad("ADVD_IMG_AVG_COINS_PCT", 6, " ").alias("ADVD_IMG_AVG_COINS_PCT"),
    rpad("COINS_MAX_AMT", 11, " ").alias("COINS_MAX_AMT"),
    rpad("EFF_DT", 8, " ").alias("EFF_DT"),
    rpad("EXPRTN_DT", 8, " ").alias("EXPRTN_DT")
)

write_files(
    df_ftp_benefit_package_ref,
    f"{adls_path_publish}/external/benefit_package_ref",
    ",",
    "overwrite",
    False,
    False,
    None,
    None
)