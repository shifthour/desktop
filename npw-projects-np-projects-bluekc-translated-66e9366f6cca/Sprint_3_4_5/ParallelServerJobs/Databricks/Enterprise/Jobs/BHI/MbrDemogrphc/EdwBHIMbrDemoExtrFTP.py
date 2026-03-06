# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdwBHIMbrDemoExtrFTP
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  This Job FTP's std_member_demographic file
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
# MAGIC ODIFICATIONS:
# MAGIC =====================================================================================================================================
# MAGIC Developer\(9)Date\(9)\(9)Project/Ticket #\(9)Change Description\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC =====================================================================================================================================
# MAGIC Abhiram Dasarathy\(9)2015-01-20\(9)5212 Paymnent\(9)Original programming\(9)\(9)EnterpriseNewDevl                   Kalyan Neelam        2015-01-27
# MAGIC \(9)\(9)\(9)\(9)Innovations
# MAGIC Aishwarya                2016-08-09              5406 BHI                  Added 3 fields EFF_DT,EXPRTN_DT,   EnterpriseDevl                          Jag Yelavarthi         2016-08-11
# MAGIC                                                                                                  OOA_MBR_CD
# MAGIC 
# MAGIC Madhavan B            2018-04-16              5726                       Added 5 new fields (PROV_PLN_CD,        EnterpriseDev2                      Jaideep Mankala     04/18/2018
# MAGIC                                                                                                PROD_ID, PROV_NO, PROV_NO_SFX
# MAGIC                                                                                                and SEQ_NO) at the end of the extract file
# MAGIC 
# MAGIC Ravi Ranjan             2021-05-24         US 381576                Added new coulmns MCARE_BNFCRY_ID EnterpriseSITF                     Jeyaprasanna          2021-10-12
# MAGIC                                                                                               and MCARE_CNTR_ID
# MAGIC 
# MAGIC HariKrishnaRao Yadav 2023-04-19    US 581571                Added new coulmns PROV_MSTR_ID        EnterpriseDevB                    Jeyaprasanna         2023-04-20
# MAGIC                                                                                              and PROV_LOC

# MAGIC Job Name: std_member_demographic File FTP
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


ProdIn = get_widget_value("ProdIn","")

schema_std_member_demographic = StructType([
    StructField("BHI_HOME_PLN_ID", StringType(), False),
    StructField("CONSIS_MBR_ID", StringType(), False),
    StructField("MBR_PFX", StringType(), False),
    StructField("MBR_LAST_NM", StringType(), False),
    StructField("MBR_FIRST_NM", StringType(), False),
    StructField("MBR_MIDINT", StringType(), False),
    StructField("MBR_SFX", StringType(), False),
    StructField("MBR_PRI_ST_ADDR_1", StringType(), False),
    StructField("MBR_PRI_ST_ADDR_2", StringType(), False),
    StructField("MBR_PRI_CITY", StringType(), False),
    StructField("MBR_PRI_ST", StringType(), False),
    StructField("MBR_PRI_ZIP_CD", StringType(), False),
    StructField("MBR_PRI_ZIP_CD_PLUS_4", StringType(), False),
    StructField("MBR_PRI_PHN_NO", StringType(), False),
    StructField("MBR_PRI_EMAIL", StringType(), False),
    StructField("MBR_SEC_ST_ADDR_1", StringType(), False),
    StructField("MBR_SEC_ST_ADDR_2", StringType(), False),
    StructField("MBR_SEC_CITY", StringType(), False),
    StructField("MBR_SEC_ST", StringType(), False),
    StructField("MBR_SEC_ZIP_CD", StringType(), False),
    StructField("MBR_SEC_ZIP_CD_PLUS_4", StringType(), False),
    StructField("HOST_PLN_OVRD", StringType(), False),
    StructField("MBR_PARTCPN_CD", StringType(), False),
    StructField("ITS_SUB_ID", StringType(), False),
    StructField("MMI_ID", StringType(), False),
    StructField("HOST_PLN_CD", StringType(), False),
    StructField("PI_VOID_IN", StringType(), False),
    StructField("PI_MBR_CONFITY_CD", StringType(), False),
    StructField("EFF_DT", StringType(), False),
    StructField("EXPRTN_DT", StringType(), False),
    StructField("OOA_MBR_CD", StringType(), False),
    StructField("PROV_PLN_CD", StringType(), False),
    StructField("PROD_ID", StringType(), False),
    StructField("PROV_NO", StringType(), False),
    StructField("PROV_NO_SFX", StringType(), False),
    StructField("SEQ_NO", StringType(), False),
    StructField("MCARE_BNFCRY_ID", StringType(), False),
    StructField("MCARE_CNTR_ID", StringType(), False),
    StructField("PROV_MSTR_ID", StringType(), False),
    StructField("PROV_LOC", StringType(), False)
])

df_std_member_demographic = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .schema(schema_std_member_demographic)
    .csv(f"{adls_path_publish}/external/std_member_demographic.dat")
)

df_ftp = df_std_member_demographic.select(
    "BHI_HOME_PLN_ID",
    "CONSIS_MBR_ID",
    "MBR_PFX",
    "MBR_LAST_NM",
    "MBR_FIRST_NM",
    "MBR_MIDINT",
    "MBR_SFX",
    "MBR_PRI_ST_ADDR_1",
    "MBR_PRI_ST_ADDR_2",
    "MBR_PRI_CITY",
    "MBR_PRI_ST",
    "MBR_PRI_ZIP_CD",
    "MBR_PRI_ZIP_CD_PLUS_4",
    "MBR_PRI_PHN_NO",
    "MBR_PRI_EMAIL",
    "MBR_SEC_ST_ADDR_1",
    "MBR_SEC_ST_ADDR_2",
    "MBR_SEC_CITY",
    "MBR_SEC_ST",
    "MBR_SEC_ZIP_CD",
    "MBR_SEC_ZIP_CD_PLUS_4",
    "HOST_PLN_OVRD",
    "MBR_PARTCPN_CD",
    "ITS_SUB_ID",
    "MMI_ID",
    "HOST_PLN_CD",
    "PI_VOID_IN",
    "PI_MBR_CONFITY_CD",
    "EFF_DT",
    "EXPRTN_DT",
    "OOA_MBR_CD",
    "PROV_PLN_CD",
    "PROD_ID",
    "PROV_NO",
    "PROV_NO_SFX",
    "SEQ_NO",
    "MCARE_BNFCRY_ID",
    "MCARE_CNTR_ID",
    "PROV_MSTR_ID",
    "PROV_LOC"
)

df_ftp = df_ftp \
    .withColumn("BHI_HOME_PLN_ID", rpad(col("BHI_HOME_PLN_ID"), 3, " ")) \
    .withColumn("CONSIS_MBR_ID", rpad(col("CONSIS_MBR_ID"), 22, " ")) \
    .withColumn("MBR_PFX", rpad(col("MBR_PFX"), 20, " ")) \
    .withColumn("MBR_LAST_NM", rpad(col("MBR_LAST_NM"), 150, " ")) \
    .withColumn("MBR_FIRST_NM", rpad(col("MBR_FIRST_NM"), 70, " ")) \
    .withColumn("MBR_MIDINT", rpad(col("MBR_MIDINT"), 2, " ")) \
    .withColumn("MBR_SFX", rpad(col("MBR_SFX"), 20, " ")) \
    .withColumn("MBR_PRI_ST_ADDR_1", rpad(col("MBR_PRI_ST_ADDR_1"), 70, " ")) \
    .withColumn("MBR_PRI_ST_ADDR_2", rpad(col("MBR_PRI_ST_ADDR_2"), 70, " ")) \
    .withColumn("MBR_PRI_CITY", rpad(col("MBR_PRI_CITY"), 35, " ")) \
    .withColumn("MBR_PRI_ST", rpad(col("MBR_PRI_ST"), 2, " ")) \
    .withColumn("MBR_PRI_ZIP_CD", rpad(col("MBR_PRI_ZIP_CD"), 5, " ")) \
    .withColumn("MBR_PRI_ZIP_CD_PLUS_4", rpad(col("MBR_PRI_ZIP_CD_PLUS_4"), 4, " ")) \
    .withColumn("MBR_PRI_PHN_NO", rpad(col("MBR_PRI_PHN_NO"), 10, " ")) \
    .withColumn("MBR_PRI_EMAIL", rpad(col("MBR_PRI_EMAIL"), 70, " ")) \
    .withColumn("MBR_SEC_ST_ADDR_1", rpad(col("MBR_SEC_ST_ADDR_1"), 70, " ")) \
    .withColumn("MBR_SEC_ST_ADDR_2", rpad(col("MBR_SEC_ST_ADDR_2"), 70, " ")) \
    .withColumn("MBR_SEC_CITY", rpad(col("MBR_SEC_CITY"), 35, " ")) \
    .withColumn("MBR_SEC_ST", rpad(col("MBR_SEC_ST"), 2, " ")) \
    .withColumn("MBR_SEC_ZIP_CD", rpad(col("MBR_SEC_ZIP_CD"), 5, " ")) \
    .withColumn("MBR_SEC_ZIP_CD_PLUS_4", rpad(col("MBR_SEC_ZIP_CD_PLUS_4"), 4, " ")) \
    .withColumn("HOST_PLN_OVRD", rpad(col("HOST_PLN_OVRD"), 3, " ")) \
    .withColumn("MBR_PARTCPN_CD", rpad(col("MBR_PARTCPN_CD"), 1, " ")) \
    .withColumn("ITS_SUB_ID", rpad(col("ITS_SUB_ID"), 17, " ")) \
    .withColumn("MMI_ID", rpad(col("MMI_ID"), 22, " ")) \
    .withColumn("HOST_PLN_CD", rpad(col("HOST_PLN_CD"), 3, " ")) \
    .withColumn("PI_VOID_IN", rpad(col("PI_VOID_IN"), 1, " ")) \
    .withColumn("PI_MBR_CONFITY_CD", rpad(col("PI_MBR_CONFITY_CD"), 3, " ")) \
    .withColumn("EFF_DT", rpad(col("EFF_DT"), 8, " ")) \
    .withColumn("EXPRTN_DT", rpad(col("EXPRTN_DT"), 8, " ")) \
    .withColumn("OOA_MBR_CD", rpad(col("OOA_MBR_CD"), 2, " ")) \
    .withColumn("PROV_PLN_CD", rpad(col("PROV_PLN_CD"), 3, " ")) \
    .withColumn("PROD_ID", rpad(col("PROD_ID"), 4, " ")) \
    .withColumn("PROV_NO", rpad(col("PROV_NO"), 13, " ")) \
    .withColumn("PROV_NO_SFX", rpad(col("PROV_NO_SFX"), 2, " ")) \
    .withColumn("SEQ_NO", rpad(col("SEQ_NO"), 2, " ")) \
    .withColumn("MCARE_BNFCRY_ID", rpad(col("MCARE_BNFCRY_ID"), 11, " ")) \
    .withColumn("MCARE_CNTR_ID", rpad(col("MCARE_CNTR_ID"), 8, " ")) \
    .withColumn("PROV_MSTR_ID", rpad(col("PROV_MSTR_ID"), 36, " ")) \
    .withColumn("PROV_LOC", rpad(col("PROV_LOC"), 36, " "))

ftp_upload(df_ftp, <host_name>, <port>, <username>, <password>)  # Assume this user-defined function exists.