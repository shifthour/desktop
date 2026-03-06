# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdwBhiProdClntCntrExtrFTP
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  This Job FTP's std_product_client_contract file
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
# MAGIC Praveen  Annam             2014-07-14              5115 BHI                                       Original programming                                                  EnterpriseNewDevl        Bhoomi Dasari           7/15/2014  
# MAGIC Bhoomi Dasari                10/7/2014               5115-BHI                                   Changes to FTP stage to remove extra bytes                 EnterpriseNewDevl         Kalyan Neelam          2014-10-08
# MAGIC Aishwarya                        2016-08-11              5604                                           Added  MKTPLC_TYP_CD,ACA_METAL_LVL_IN,      EnterpriseDev1              Jag Yelavarthi            2016-08-16     
# MAGIC                                                                                                                           TANSITIONAL_HLTH_PLN_IN,CNTR_GRP_SIZE_CD

# MAGIC Job Name: std_product_client_contract File FTP
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


ProdIn = get_widget_value('ProdIn','')

schema_std_product_client_contract = StructType([
    StructField("BHI_HOME_PLN_ID", StringType(), True),
    StructField("HOME_PLN_PROD_ID", StringType(), True),
    StructField("ACCT", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("INDST_GRPNG_SIC_CD", StringType(), True),
    StructField("INDST_GRPNG_NAICS_CD", StringType(), True),
    StructField("NTNL_ACCT_IN", StringType(), True),
    StructField("ASO_CD", StringType(), True),
    StructField("GRP_INDV_CD", StringType(), True),
    StructField("ALPHA_PFX", StringType(), True),
    StructField("CSTM_NTWK_IN", StringType(), True),
    StructField("CDPH_OFFRD", StringType(), True),
    StructField("TRACEABILITY_FLD", StringType(), True),
    StructField("MBR_ENR_IN", StringType(), True),
    StructField("BCBSA_PROD_ID", StringType(), False),
    StructField("MKTPLC_TYP_CD", StringType(), False),
    StructField("ACA_METAL_LVL_IN", StringType(), False),
    StructField("TANSITIONAL_HLTH_PLN_IN", StringType(), False),
    StructField("CNTR_GRP_SIZE_CD", StringType(), False)
])

df_std_product_client_contract = (
    spark.read.format("csv")
    .option("inferschema", False)
    .option("header", False)
    .option("delimiter", ",")
    .schema(schema_std_product_client_contract)
    .load(f"{adls_path_publish}/external/std_product_client_contract.dat")
)

df_FTP_std_product_client_contract = (
    df_std_product_client_contract
    .withColumn("BHI_HOME_PLN_ID", rpad(col("BHI_HOME_PLN_ID"), 3, " "))
    .withColumn("HOME_PLN_PROD_ID", rpad(col("HOME_PLN_PROD_ID"), 15, " "))
    .withColumn("ACCT", rpad(col("ACCT"), 14, " "))
    .withColumn("GRP_ID", rpad(col("GRP_ID"), 14, " "))
    .withColumn("SUBGRP_ID", rpad(col("SUBGRP_ID"), 10, " "))
    .withColumn("INDST_GRPNG_SIC_CD", rpad(col("INDST_GRPNG_SIC_CD"), 4, " "))
    .withColumn("INDST_GRPNG_NAICS_CD", rpad(col("INDST_GRPNG_NAICS_CD"), 6, " "))
    .withColumn("NTNL_ACCT_IN", rpad(col("NTNL_ACCT_IN"), 1, " "))
    .withColumn("ASO_CD", rpad(col("ASO_CD"), 1, " "))
    .withColumn("GRP_INDV_CD", rpad(col("GRP_INDV_CD"), 10, " "))
    .withColumn("ALPHA_PFX", rpad(col("ALPHA_PFX"), 3, " "))
    .withColumn("CSTM_NTWK_IN", rpad(col("CSTM_NTWK_IN"), 1, " "))
    .withColumn("CDPH_OFFRD", rpad(col("CDPH_OFFRD"), 5, " "))
    .withColumn("TRACEABILITY_FLD", rpad(col("TRACEABILITY_FLD"), 5, " "))
    .withColumn("MBR_ENR_IN", rpad(col("MBR_ENR_IN"), 1, " "))
    .withColumn("BCBSA_PROD_ID", rpad(col("BCBSA_PROD_ID"), 4, " "))
    .withColumn("MKTPLC_TYP_CD", rpad(col("MKTPLC_TYP_CD"), 3, " "))
    .withColumn("ACA_METAL_LVL_IN", rpad(col("ACA_METAL_LVL_IN"), 2, " "))
    .withColumn("TANSITIONAL_HLTH_PLN_IN", rpad(col("TANSITIONAL_HLTH_PLN_IN"), 1, " "))
    .withColumn("CNTR_GRP_SIZE_CD", rpad(col("CNTR_GRP_SIZE_CD"), 2, " "))
)

write_files(
    df_FTP_std_product_client_contract.select(
        "BHI_HOME_PLN_ID",
        "HOME_PLN_PROD_ID",
        "ACCT",
        "GRP_ID",
        "SUBGRP_ID",
        "INDST_GRPNG_SIC_CD",
        "INDST_GRPNG_NAICS_CD",
        "NTNL_ACCT_IN",
        "ASO_CD",
        "GRP_INDV_CD",
        "ALPHA_PFX",
        "CSTM_NTWK_IN",
        "CDPH_OFFRD",
        "TRACEABILITY_FLD",
        "MBR_ENR_IN",
        "BCBSA_PROD_ID",
        "MKTPLC_TYP_CD",
        "ACA_METAL_LVL_IN",
        "TANSITIONAL_HLTH_PLN_IN",
        "CNTR_GRP_SIZE_CD"
    ),
    f"{adls_path_publish}/external/std_product_client_contract_FTP.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)