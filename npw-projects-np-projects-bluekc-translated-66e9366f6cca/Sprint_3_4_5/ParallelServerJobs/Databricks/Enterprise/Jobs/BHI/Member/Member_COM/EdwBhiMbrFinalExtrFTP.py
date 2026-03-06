# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  This Job FTP's std_member file
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
# MAGIC Bhoomi Dasari               10/7/2014                5115-BHI                                       Changes to FTP stage to remove extra bytes             EnterpriseNewDevl         Kalyan Neelam           2014-10-08
# MAGIC Aishwarya                      2016-07-25               5604                                              Added fields - BNF_PCKG_ID,                                    EnterpriseDevl               Jag Yelavarthi             2016-07-25
# MAGIC                                                                                                                              GVRNMT_SUBSIDIES_IN, CSRS_TYP,QHP_ID, 
# MAGIC                                                                                                                              NTNL_LABOR_OFC_ACCT_TYP,
# MAGIC                                                                                                                              NTNL_LABOR_OFC_PLN_TYP

# MAGIC Job Name: std_member File FTP
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


ProdIn = get_widget_value("ProdIn","")

schema_std_member = StructType([
    StructField("BHI_HOME_PLN_ID", StringType(), False),
    StructField("HOME_PLN_PROD_ID", StringType(), False),
    StructField("MBR_ID", StringType(), False),
    StructField("CONSIS_MBR_ID", StringType(), False),
    StructField("TRACEABILITY_FLD", StringType(), False),
    StructField("MBR_DOB", StringType(), False),
    StructField("MBR_CUR_PRI_ZIP_CD", StringType(), False),
    StructField("MBR_S_CUR_CTRY", StringType(), False),
    StructField("MBR_S_CUR_CNTY", StringType(), False),
    StructField("MBR_GNDR", StringType(), False),
    StructField("MBR_CONF_CD", StringType(), False),
    StructField("ACCT", StringType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("COV_BEG_DT", StringType(), False),
    StructField("COV_END_DT", StringType(), False),
    StructField("MBR_RELSHP", StringType(), False),
    StructField("HOME_PLN_PROD_ID_SUB", StringType(), False),
    StructField("SUB_ID", StringType(), False),
    StructField("ENR_ELIG_STTUS", StringType(), False),
    StructField("MBR_MED_COB_CD", StringType(), False),
    StructField("MBR_PDX_COB_CD", StringType(), False),
    StructField("DEDCT_CAT", StringType(), False),
    StructField("MHCD_ENR_BNF", StringType(), False),
    StructField("PDX_BNF_IN", StringType(), False),
    StructField("MHCD_BNF_IN", StringType(), False),
    StructField("MED_BNF_IN", StringType(), False),
    StructField("HOSP_BNF_IN", StringType(), False),
    StructField("BHI_NTWK_CAT_FCLTY", StringType(), False),
    StructField("BHI_NTWK_CAT_PROF", StringType(), False),
    StructField("PLN_NTWK_CAT_FCLTY", StringType(), False),
    StructField("PLN_NTWK_CAT_PROF", StringType(), False),
    StructField("PDX_CARVE_OUT_SUBMSN_IN", StringType(), False),
    StructField("PDX_BNF_TIERS", StringType(), False),
    StructField("BNF_PCKG_ID", StringType(), False),
    StructField("GVRNMT_SUBSIDIES_IN", StringType(), False),
    StructField("CSRS_TYP", StringType(), False),
    StructField("QHP_ID", StringType(), False),
    StructField("NTNL_LABOR_OFC_ACCT_TYP", StringType(), False),
    StructField("NTNL_LABOR_OFC_PLN_TYP", StringType(), False)
])

df_std_member = (
    spark.read.format("csv")
    .option("header","false")
    .option("delimiter",",")
    .option("quote","\u0000")
    .schema(schema_std_member)
    .load(f"{adls_path_publish}/external/std_member")
)

df_ftp_std_member = df_std_member.select(
    F.rpad(F.col("BHI_HOME_PLN_ID"), 3, " ").alias("BHI_HOME_PLN_ID"),
    F.rpad(F.col("HOME_PLN_PROD_ID"), 15, " ").alias("HOME_PLN_PROD_ID"),
    F.rpad(F.col("MBR_ID"), 22, " ").alias("MBR_ID"),
    F.rpad(F.col("CONSIS_MBR_ID"), 22, " ").alias("CONSIS_MBR_ID"),
    F.rpad(F.col("TRACEABILITY_FLD"), 5, " ").alias("TRACEABILITY_FLD"),
    F.rpad(F.col("MBR_DOB"), 8, " ").alias("MBR_DOB"),
    F.rpad(F.col("MBR_CUR_PRI_ZIP_CD"), 5, " ").alias("MBR_CUR_PRI_ZIP_CD"),
    F.rpad(F.col("MBR_S_CUR_CTRY"), 2, " ").alias("MBR_S_CUR_CTRY"),
    F.rpad(F.col("MBR_S_CUR_CNTY"), 5, " ").alias("MBR_S_CUR_CNTY"),
    F.rpad(F.col("MBR_GNDR"), 1, " ").alias("MBR_GNDR"),
    F.rpad(F.col("MBR_CONF_CD"), 3, " ").alias("MBR_CONF_CD"),
    F.rpad(F.col("ACCT"), 14, " ").alias("ACCT"),
    F.rpad(F.col("GRP_ID"), 14, " ").alias("GRP_ID"),
    F.rpad(F.col("SUBGRP_ID"), 10, " ").alias("SUBGRP_ID"),
    F.rpad(F.col("COV_BEG_DT"), 8, " ").alias("COV_BEG_DT"),
    F.rpad(F.col("COV_END_DT"), 8, " ").alias("COV_END_DT"),
    F.rpad(F.col("MBR_RELSHP"), 2, " ").alias("MBR_RELSHP"),
    F.rpad(F.col("HOME_PLN_PROD_ID_SUB"), 15, " ").alias("HOME_PLN_PROD_ID_SUB"),
    F.rpad(F.col("SUB_ID"), 22, " ").alias("SUB_ID"),
    F.rpad(F.col("ENR_ELIG_STTUS"), 3, " ").alias("ENR_ELIG_STTUS"),
    F.rpad(F.col("MBR_MED_COB_CD"), 1, " ").alias("MBR_MED_COB_CD"),
    F.rpad(F.col("MBR_PDX_COB_CD"), 1, " ").alias("MBR_PDX_COB_CD"),
    F.rpad(F.col("DEDCT_CAT"), 1, " ").alias("DEDCT_CAT"),
    F.rpad(F.col("MHCD_ENR_BNF"), 2, " ").alias("MHCD_ENR_BNF"),
    F.rpad(F.col("PDX_BNF_IN"), 1, " ").alias("PDX_BNF_IN"),
    F.rpad(F.col("MHCD_BNF_IN"), 1, " ").alias("MHCD_BNF_IN"),
    F.rpad(F.col("MED_BNF_IN"), 1, " ").alias("MED_BNF_IN"),
    F.rpad(F.col("HOSP_BNF_IN"), 1, " ").alias("HOSP_BNF_IN"),
    F.rpad(F.col("BHI_NTWK_CAT_FCLTY"), 3, " ").alias("BHI_NTWK_CAT_FCLTY"),
    F.rpad(F.col("BHI_NTWK_CAT_PROF"), 3, " ").alias("BHI_NTWK_CAT_PROF"),
    F.rpad(F.col("PLN_NTWK_CAT_FCLTY"), 2, " ").alias("PLN_NTWK_CAT_FCLTY"),
    F.rpad(F.col("PLN_NTWK_CAT_PROF"), 2, " ").alias("PLN_NTWK_CAT_PROF"),
    F.rpad(F.col("PDX_CARVE_OUT_SUBMSN_IN"), 1, " ").alias("PDX_CARVE_OUT_SUBMSN_IN"),
    F.rpad(F.col("PDX_BNF_TIERS"), 2, " ").alias("PDX_BNF_TIERS"),
    F.rpad(F.col("BNF_PCKG_ID"), 15, " ").alias("BNF_PCKG_ID"),
    F.rpad(F.col("GVRNMT_SUBSIDIES_IN"), 1, " ").alias("GVRNMT_SUBSIDIES_IN"),
    F.rpad(F.col("CSRS_TYP"), 2, " ").alias("CSRS_TYP"),
    F.rpad(F.col("QHP_ID"), 16, " ").alias("QHP_ID"),
    F.rpad(F.col("NTNL_LABOR_OFC_ACCT_TYP"), 2, " ").alias("NTNL_LABOR_OFC_ACCT_TYP"),
    F.rpad(F.col("NTNL_LABOR_OFC_PLN_TYP"), 2, " ").alias("NTNL_LABOR_OFC_PLN_TYP")
)

write_files(
    df_ftp_std_member,
    f"{adls_path_publish}/external/std_member_ftp",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)