# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  This Job creates the file to be loaded into the P_BCBSA_MBR_DTL table.
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
# MAGIC Abhiram Dasarathy\(9)2014-12-20\(9)5212 Paymnent\(9)Original programming\(9)\(9)EnterpriseNewDevl                  Kalyan Neelam         2015-01-26
# MAGIC \(9)\(9)\(9)\(9)Innovations
# MAGIC Jaideep Mankala\(9)2015-03-02\(9)5212 Paymnent\(9)Added MODIFY stage to handle nulls      EnterpriseNewDevl                  Kalyan Neelam        2015-03-06
# MAGIC \(9)\(9)\(9)\(9)Innovations          
# MAGIC 
# MAGIC Dan Long               2016-02-02               5212 Payment         Added P_GRP_MVP_OPT_OUT,            EnterpriseDev1                        Jag Yelavarthi         2016-02-02         
# MAGIC                                                                 Innovation              OptOutLookup and Transform_1
# MAGIC                                                                                                to populate the MBR_PARTCPN_CD
# MAGIC                                                                                                column.
# MAGIC Karthik Chintalapani  2016-04-01         5212 PI          Added the logic to populate the                                 EnterpriseDev1                  Kalyan Neelam         2016-04-19
# MAGIC                                                                                  HOST_PLN_OVRD  and HOST_PLN_CD columns.  
# MAGIC  
# MAGIC Aishwarya                 2016-11-02           5604           Added EFF_DT, EXPRTN_DT, OOA_MBR_CD in the input file  EnterpriseDev1    Jag Yelavarthi        2016-11-03                                                                                     
# MAGIC                                                                                                                     
# MAGIC  
# MAGIC Akhila M               3/17/2017                     5604           Join based on UNIQUE KEY along with the        EnterpriseDev2                  Jag Yelavarthi          2017-03-21                                                                                
# MAGIC                                                                                         existing key CONSIS_MBR_ID    
# MAGIC  
# MAGIC Ravi Ranjan           2021-05-24              US-381576           Added new column CMS_CNTR_ID        EnterpriseSITF                         Jeyaprasanna          2021-10-12       
# MAGIC 
# MAGIC 
# MAGIC Sudhan B              2025-05-14              US649995           Added new parameter "DemoMbrFile"         EnterpriseDev2  \(9)\(9)Harsha Ravuri\(9)2025-05-18                
# MAGIC                                                                                            and Mapped to  Final_BHI File stg

# MAGIC Member dataset generated in EdwBhiMbrEnrDsLd job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


CurrDate = get_widget_value('CurrDate','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
DemoMbrFile = get_widget_value('DemoMbrFile','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query = (
    f"SELECT GRP_ID, GRP_MVP_OPT_OUT_EFF_DT_SK, GRP_MVP_OPT_OUT_TERM_DT_SK, USER_ID, LAST_UPDT_DTM "
    f"FROM {EDWOwner}.P_GRP_MVP_OPT_OUT AS P_GRP_MVP_OPT_OUT "
    f"WHERE GRP_MVP_OPT_OUT_EFF_DT_SK <= '{CurrDate}' "
    f"AND GRP_MVP_OPT_OUT_TERM_DT_SK >= '{CurrDate}'"
)
df_P_GRP_MVP_OPT_OUT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

schema_Final_BHI = StructType([
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
    StructField("MCARE_BNFCRY_ID", StringType(), False),
    StructField("MCARE_CNTR_ID", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False)
])
df_Final_BHI = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("nullValue", None)
    .schema(schema_Final_BHI)
    .load(f"{adls_path_publish}/external/{DemoMbrFile}")
)

schema_Member_Hist_Enr = StructType([
    StructField("BHI_HOME_PLN_ID", StringType(), True),
    StructField("HOME_PLN_PROD_ID", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("CONSIS_MBR_ID", StringType(), True),
    StructField("TRACEABILITY_FLD", StringType(), True),
    StructField("MBR_DOB", StringType(), True),
    StructField("MBR_CUR_PRI_ZIP_CD", StringType(), True),
    StructField("MBR_S_CUR_CTRY", StringType(), True),
    StructField("MBR_S_CUR_CNTY", StringType(), True),
    StructField("MBR_GNDR", StringType(), True),
    StructField("MBR_CONF_CD", StringType(), True),
    StructField("ACCT", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("COV_BEG_DT", StringType(), True),
    StructField("COV_END_DT", StringType(), True),
    StructField("MBR_RELSHP", StringType(), True),
    StructField("HOME_PLN_PROD_ID_SUB", StringType(), True),
    StructField("SUB_ID", StringType(), True),
    StructField("ENR_ELIG_STTUS", StringType(), True),
    StructField("MBR_MED_COB_CD", StringType(), True),
    StructField("MBR_PDX_COB_CD", StringType(), True),
    StructField("DEDCT_CAT", StringType(), True),
    StructField("MHCD_ENR_BNF", StringType(), True),
    StructField("PDX_BNF_IN", StringType(), True),
    StructField("MHCD_BNF_IN", StringType(), True),
    StructField("MED_BNF_IN", StringType(), True),
    StructField("HOSP_BNF_IN", StringType(), True),
    StructField("BHI_NTWK_CAT_FCLTY", StringType(), True),
    StructField("BHI_NTWK_CAT_PROF", StringType(), True),
    StructField("PLN_NTWK_CAT_FCLTY", StringType(), True),
    StructField("PLN_NTWK_CAT_PROF", StringType(), True),
    StructField("PDX_CARVE_OUT_SUBMSN_IN", StringType(), True),
    StructField("PDX_BNF_TIERS", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("MBR_EFF_DT_SK", StringType(), True),
    StructField("MBR_TERM_DT_SK", StringType(), True),
    StructField("PROD_SK", IntegerType(), True),
    StructField("PROD_SH_NM", StringType(), True)
])
df_Member_Hist_Enr = spark.read.schema(schema_Member_Hist_Enr).parquet(f"{adls_path}/ds/W_BHI_MBR.parquet")

df_xfm_tohist = df_Member_Hist_Enr.alias("Hist_mem").select(
    F.col("Hist_mem.BHI_HOME_PLN_ID").alias("BHI_HOME_PLN_ID_DPE"),
    F.col("Hist_mem.HOME_PLN_PROD_ID").alias("HOME_PLN_PROD_ID"),
    F.col("Hist_mem.MBR_ID").alias("MBR_ID"),
    F.when(F.col("Hist_mem.CONSIS_MBR_ID").isNull(), F.lit("0")).otherwise(F.col("Hist_mem.CONSIS_MBR_ID")).alias("CONSIS_MBR_ID"),
    F.col("Hist_mem.TRACEABILITY_FLD").alias("TRACEABILITY_FLD"),
    F.col("Hist_mem.MBR_DOB").alias("MBR_DOB"),
    F.col("Hist_mem.MBR_CUR_PRI_ZIP_CD").alias("MBR_CUR_PRI_ZIP_CD"),
    F.col("Hist_mem.MBR_S_CUR_CTRY").alias("MBR_S_CUR_CTRY"),
    F.col("Hist_mem.MBR_S_CUR_CNTY").alias("MBR_S_CUR_CNTY"),
    F.col("Hist_mem.MBR_GNDR").alias("MBR_GNDR"),
    F.col("Hist_mem.MBR_CONF_CD").alias("MBR_CONF_CD"),
    F.col("Hist_mem.ACCT").alias("ACCT"),
    F.col("Hist_mem.GRP_ID").alias("GRP_ID"),
    F.col("Hist_mem.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("Hist_mem.COV_BEG_DT").alias("COV_BEG_DT"),
    F.col("Hist_mem.COV_END_DT").alias("COV_END_DT"),
    F.col("Hist_mem.MBR_RELSHP").alias("MBR_RELSHP"),
    F.col("Hist_mem.HOME_PLN_PROD_ID_SUB").alias("HOME_PLN_PROD_ID_SUB"),
    F.col("Hist_mem.SUB_ID").alias("SUB_ID"),
    F.col("Hist_mem.ENR_ELIG_STTUS").alias("ENR_ELIG_STTUS"),
    F.col("Hist_mem.MBR_MED_COB_CD").alias("MBR_MED_COB_CD"),
    F.col("Hist_mem.MBR_PDX_COB_CD").alias("MBR_PDX_COB_CD"),
    F.col("Hist_mem.DEDCT_CAT").alias("DEDCT_CAT"),
    F.col("Hist_mem.MHCD_ENR_BNF").alias("MHCD_ENR_BNF"),
    F.col("Hist_mem.PDX_BNF_IN").alias("PDX_BNF_IN"),
    F.col("Hist_mem.MHCD_BNF_IN").alias("MHCD_BNF_IN"),
    F.col("Hist_mem.MED_BNF_IN").alias("MED_BNF_IN"),
    F.col("Hist_mem.HOSP_BNF_IN").alias("HOSP_BNF_IN"),
    F.col("Hist_mem.BHI_NTWK_CAT_FCLTY").alias("BHI_NTWK_CAT_FCLTY"),
    F.col("Hist_mem.BHI_NTWK_CAT_PROF").alias("BHI_NTWK_CAT_PROF"),
    F.col("Hist_mem.PLN_NTWK_CAT_FCLTY").alias("PLN_NTWK_CAT_FCLTY"),
    F.col("Hist_mem.PLN_NTWK_CAT_PROF").alias("PLN_NTWK_CAT_PROF"),
    F.col("Hist_mem.PDX_CARVE_OUT_SUBMSN_IN").alias("PDX_CARVE_OUT_SUBMSN_IN"),
    F.col("Hist_mem.PDX_BNF_TIERS").alias("PDX_BNF_TIERS"),
    F.when(F.col("Hist_mem.MBR_UNIQ_KEY").isNull(), F.lit(0)).otherwise(F.col("Hist_mem.MBR_UNIQ_KEY")).alias("MBR_UNIQ_KEY"),
    F.col("Hist_mem.MBR_EFF_DT_SK").alias("MBR_EFF_DT_SK"),
    F.col("Hist_mem.MBR_TERM_DT_SK").alias("MBR_TERM_DT_SK"),
    F.col("Hist_mem.PROD_SK").alias("PROD_SK")
)

df_Hist_demo_file = df_Final_BHI.alias("Hist_demo_file")
df_Hist_mem_ds = df_xfm_tohist.alias("Hist_mem_ds")
df_Jn_Hist_Enr = df_Hist_demo_file.join(
    df_Hist_mem_ds,
    (
        (F.col("Hist_demo_file.MBR_UNIQ_KEY") == F.col("Hist_mem_ds.MBR_UNIQ_KEY"))
        & (F.col("Hist_demo_file.CONSIS_MBR_ID") == F.col("Hist_mem_ds.CONSIS_MBR_ID"))
    ),
    "inner"
).select(
    F.col("Hist_demo_file.BHI_HOME_PLN_ID").alias("BHI_HOME_PLN_ID"),
    F.col("Hist_demo_file.CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    F.col("Hist_demo_file.MBR_PFX").alias("MBR_PFX"),
    F.col("Hist_demo_file.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("Hist_demo_file.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("Hist_demo_file.MBR_MIDINT").alias("MBR_MIDINT"),
    F.col("Hist_demo_file.MBR_SFX").alias("MBR_SFX"),
    F.col("Hist_demo_file.MBR_PRI_ST_ADDR_1").alias("MBR_PRI_ST_ADDR_1"),
    F.col("Hist_demo_file.MBR_PRI_ST_ADDR_2").alias("MBR_PRI_ST_ADDR_2"),
    F.col("Hist_demo_file.MBR_PRI_CITY").alias("MBR_PRI_CITY"),
    F.col("Hist_demo_file.MBR_PRI_ST").alias("MBR_PRI_ST"),
    F.col("Hist_demo_file.MBR_PRI_ZIP_CD").alias("MBR_PRI_ZIP_CD"),
    F.col("Hist_demo_file.MBR_PRI_ZIP_CD_PLUS_4").alias("MBR_PRI_ZIP_CD_PLUS_4"),
    F.col("Hist_demo_file.MBR_PRI_PHN_NO").alias("MBR_PRI_PHN_NO"),
    F.col("Hist_demo_file.MBR_PRI_EMAIL").alias("MBR_PRI_EMAIL"),
    F.col("Hist_demo_file.MBR_SEC_ST_ADDR_1").alias("MBR_SEC_ST_ADDR_1"),
    F.col("Hist_demo_file.MBR_SEC_ST_ADDR_2").alias("MBR_SEC_ST_ADDR_2"),
    F.col("Hist_demo_file.MBR_SEC_CITY").alias("MBR_SEC_CITY"),
    F.col("Hist_demo_file.MBR_SEC_ST").alias("MBR_SEC_ST"),
    F.col("Hist_demo_file.MBR_SEC_ZIP_CD").alias("MBR_SEC_ZIP_CD"),
    F.col("Hist_demo_file.MBR_SEC_ZIP_CD_PLUS_4").alias("MBR_SEC_ZIP_CD_PLUS_4"),
    F.col("Hist_demo_file.HOST_PLN_OVRD").alias("HOST_PLN_OVRD"),
    F.col("Hist_demo_file.MBR_PARTCPN_CD").alias("MBR_PARTCPN_CD"),
    F.col("Hist_demo_file.ITS_SUB_ID").alias("ITS_SUB_ID"),
    F.col("Hist_demo_file.MMI_ID").alias("MMI_ID"),
    F.col("Hist_demo_file.HOST_PLN_CD").alias("HOST_PLN_CD"),
    F.col("Hist_demo_file.PI_VOID_IN").alias("PI_VOID_IN"),
    F.col("Hist_demo_file.PI_MBR_CONFITY_CD").alias("PI_MBR_CONFITY_CD"),
    F.col("Hist_mem_ds.HOME_PLN_PROD_ID").alias("HOME_PLN_PROD_ID"),
    F.col("Hist_mem_ds.MBR_ID").alias("MBR_ID"),
    F.col("Hist_mem_ds.TRACEABILITY_FLD").alias("TRACEABILITY_FLD"),
    F.col("Hist_mem_ds.MBR_DOB").alias("MBR_DOB"),
    F.col("Hist_mem_ds.MBR_CUR_PRI_ZIP_CD").alias("MBR_CUR_PRI_ZIP_CD"),
    F.col("Hist_mem_ds.MBR_S_CUR_CTRY").alias("MBR_S_CUR_CTRY"),
    F.col("Hist_mem_ds.MBR_S_CUR_CNTY").alias("MBR_S_CUR_CNTY"),
    F.col("Hist_mem_ds.MBR_GNDR").alias("MBR_GNDR"),
    F.col("Hist_mem_ds.MBR_CONF_CD").alias("MBR_CONFITY_CD"),
    F.col("Hist_mem_ds.ACCT").alias("ACCT"),
    F.col("Hist_mem_ds.GRP_ID").alias("GRP_ID"),
    F.col("Hist_mem_ds.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("Hist_mem_ds.COV_BEG_DT").alias("COV_BEG_DT"),
    F.col("Hist_mem_ds.COV_END_DT").alias("COV_END_DT"),
    F.col("Hist_mem_ds.MBR_RELSHP").alias("MBR_RELSHP"),
    F.col("Hist_mem_ds.HOME_PLN_PROD_ID_SUB").alias("HOME_PLN_PROD_ID_SUB"),
    F.col("Hist_mem_ds.SUB_ID").alias("SUB_ID"),
    F.col("Hist_mem_ds.ENR_ELIG_STTUS").alias("ENR_ELIG_STTUS"),
    F.col("Hist_mem_ds.MBR_MED_COB_CD").alias("MBR_MED_COB_CD"),
    F.col("Hist_mem_ds.MBR_PDX_COB_CD").alias("MBR_PDX_COB_CD"),
    F.col("Hist_mem_ds.DEDCT_CAT").alias("DEDCT_CAT"),
    F.col("Hist_mem_ds.MHCD_ENR_BNF").alias("MHCD_ENR_BNF"),
    F.col("Hist_mem_ds.PDX_BNF_IN").alias("PDX_BNF_IN"),
    F.col("Hist_mem_ds.MHCD_BNF_IN").alias("MHCD_BNF_IN"),
    F.col("Hist_mem_ds.MED_BNF_IN").alias("MED_BNF_IN"),
    F.col("Hist_mem_ds.HOSP_BNF_IN").alias("HOSP_BNF_IN"),
    F.col("Hist_mem_ds.BHI_NTWK_CAT_FCLTY").alias("BHI_NTWK_CAT_FCLTY"),
    F.col("Hist_mem_ds.BHI_NTWK_CAT_PROF").alias("BHI_NTWK_CAT_PROF"),
    F.col("Hist_mem_ds.PLN_NTWK_CAT_FCLTY").alias("PLN_NTWK_CAT_FCLTY"),
    F.col("Hist_mem_ds.PLN_NTWK_CAT_PROF").alias("PLN_NTWK_CAT_PROF"),
    F.col("Hist_mem_ds.PDX_CARVE_OUT_SUBMSN_IN").alias("PDX_CARVE_OUT_SUBMSN_IN"),
    F.col("Hist_mem_ds.PDX_BNF_TIERS").alias("PDX_BNF_TIERS"),
    F.col("Hist_demo_file.EFF_DT").alias("EFF_DT"),
    F.col("Hist_demo_file.EXPRTN_DT").alias("EXPRTN_DT"),
    F.col("Hist_demo_file.MCARE_CNTR_ID").alias("MCARE_CNTR_ID"),
    F.col("Hist_demo_file.OOA_MBR_CD").alias("OOA_MBR_CD"),
    F.col("Hist_demo_file.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

df_Transform = df_Jn_Hist_Enr.filter(F.col("MBR_MED_COB_CD") != F.lit("X")).select(
    F.col("CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    F.col("MBR_CONFITY_CD").alias("MBR_CONF_CD"),
    F.col("MBR_PARTCPN_CD").alias("MBR_PARTCPN_CD"),
    F.col("MBR_MED_COB_CD").alias("MBR_MED_COB_CD"),
    F.col("COV_BEG_DT").alias("COV_BEG_DT"),
    F.col("COV_END_DT").alias("COV_END_DT"),
    F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("MBR_DOB").alias("MBR_BRTH_DT"),
    F.col("MBR_GNDR").alias("MBR_GNDR_CD"),
    F.col("MBR_RELSHP").alias("MBR_RELSHP_CD"),
    F.col("MBR_PRI_ST_ADDR_1").alias("MBR_PRI_ADDR_1"),
    F.col("MBR_PRI_ST_ADDR_2").alias("MBR_PRI_ADDR_2"),
    F.col("MBR_PRI_CITY").alias("MBR_PRI_CITY_NM"),
    F.col("MBR_PRI_ST").alias("MBR_PRI_ST_CD"),
    F.col("MBR_PRI_ZIP_CD").alias("MBR_PRI_ZIP_CD"),
    F.col("MBR_PRI_ZIP_CD_PLUS_4").alias("MBR_PRI_ZIP_CD_4"),
    F.col("MBR_SEC_ST_ADDR_1").alias("MBR_SEC_ADDR_1"),
    F.col("MBR_SEC_ST_ADDR_2").alias("MBR_SEC_ADDR_2"),
    F.col("MBR_SEC_CITY").alias("MBR_SEC_CITY_NM"),
    F.col("MBR_SEC_ST").alias("MBR_SEC_ST_CD"),
    F.col("MBR_SEC_ZIP_CD").alias("MBR_SEC_ZIP_CD"),
    F.col("MBR_SEC_ZIP_CD_PLUS_4").alias("MBR_SEC_ZIP_CD_4"),
    F.col("ITS_SUB_ID").alias("ITS_SUB_ID"),
    F.col("MBR_ID").alias("MBR_ID"),
    trim(F.col("GRP_ID")).alias("GRP_ID"),
    F.col("HOST_PLN_OVRD").alias("HOST_PLN_OVRD"),
    F.col("HOST_PLN_CD").alias("HOST_PLN_CD"),
    F.col("EFF_DT").alias("EFF_DT"),
    F.col("EXPRTN_DT").alias("EXPRTN_DT"),
    F.col("MCARE_CNTR_ID").alias("MCARE_CNTR_ID"),
    F.col("OOA_MBR_CD").alias("OOA_MBR_CD")
)

df_OptOutLookup = df_Transform.alias("Load").join(
    df_P_GRP_MVP_OPT_OUT.alias("OptOutLink"),
    F.col("Load.GRP_ID") == F.col("OptOutLink.GRP_ID"),
    "left"
).select(
    F.col("Load.CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    F.col("Load.MBR_CONF_CD").alias("MBR_CONF_CD"),
    F.col("Load.MBR_PARTCPN_CD").alias("MBR_PARTCPN_CD"),
    F.col("Load.MBR_MED_COB_CD").alias("MBR_MED_COB_CD"),
    F.col("Load.COV_BEG_DT").alias("COV_BEG_DT"),
    F.col("Load.COV_END_DT").alias("COV_END_DT"),
    F.col("Load.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("Load.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("Load.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("Load.MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    F.col("Load.MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
    F.col("Load.MBR_PRI_ADDR_1").alias("MBR_PRI_ADDR_1"),
    F.col("Load.MBR_PRI_ADDR_2").alias("MBR_PRI_ADDR_2"),
    F.col("Load.MBR_PRI_CITY_NM").alias("MBR_PRI_CITY_NM"),
    F.col("Load.MBR_PRI_ST_CD").alias("MBR_PRI_ST_CD"),
    F.col("Load.MBR_PRI_ZIP_CD").alias("MBR_PRI_ZIP_CD"),
    F.col("Load.MBR_PRI_ZIP_CD_4").alias("MBR_PRI_ZIP_CD_4"),
    F.col("Load.MBR_SEC_ADDR_1").alias("MBR_SEC_ADDR_1"),
    F.col("Load.MBR_SEC_ADDR_2").alias("MBR_SEC_ADDR_2"),
    F.col("Load.MBR_SEC_CITY_NM").alias("MBR_SEC_CITY_NM"),
    F.col("Load.MBR_SEC_ST_CD").alias("MBR_SEC_ST_CD"),
    F.col("Load.MBR_SEC_ZIP_CD").alias("MBR_SEC_ZIP_CD"),
    F.col("Load.MBR_SEC_ZIP_CD_4").alias("MBR_SEC_ZIP_CD_4"),
    F.col("Load.ITS_SUB_ID").alias("ITS_SUB_ID"),
    F.col("Load.MBR_ID").alias("MBR_ID"),
    F.col("Load.GRP_ID").alias("GRP_ID"),
    F.col("OptOutLink.GRP_ID").alias("OPT_OUT_GRP_ID"),
    F.col("Load.HOST_PLN_OVRD").alias("HOST_PLN_OVRD"),
    F.col("Load.HOST_PLN_CD").alias("HOST_PLN_CD"),
    F.col("Load.EFF_DT").alias("EFF_DT"),
    F.col("Load.EXPRTN_DT").alias("EXPRTN_DT"),
    F.col("Load.MCARE_CNTR_ID").alias("MCARE_CNTR_ID"),
    F.col("Load.OOA_MBR_CD").alias("OOA_MBR_CD")
)

df_Xfm_Tbl_Load = df_OptOutLookup.select(
    F.col("CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    F.col("MBR_CONF_CD").alias("MBR_CONF_CD"),
    F.expr("CASE WHEN trim(GRP_ID) = trim(OPT_OUT_GRP_ID) THEN 'N' ELSE 'Y' END").alias("MBR_PARTCPN_CD"),
    F.col("MBR_MED_COB_CD").alias("MBR_MED_COB_CD"),
    F.col("COV_BEG_DT").alias("COV_BEG_DT"),
    F.col("COV_END_DT").alias("COV_END_DT"),
    F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    F.col("MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
    F.col("MBR_PRI_ADDR_1").alias("MBR_PRI_ADDR_1"),
    F.col("MBR_PRI_ADDR_2").alias("MBR_PRI_ADDR_2"),
    F.col("MBR_PRI_CITY_NM").alias("MBR_PRI_CITY_NM"),
    F.col("MBR_PRI_ST_CD").alias("MBR_PRI_ST_CD"),
    F.col("MBR_PRI_ZIP_CD").alias("MBR_PRI_ZIP_CD"),
    F.col("MBR_PRI_ZIP_CD_4").alias("MBR_PRI_ZIP_CD_4"),
    F.col("MBR_SEC_ADDR_1").alias("MBR_SEC_ADDR_1"),
    F.col("MBR_SEC_ADDR_2").alias("MBR_SEC_ADDR_2"),
    F.col("MBR_SEC_CITY_NM").alias("MBR_SEC_CITY_NM"),
    F.col("MBR_SEC_ST_CD").alias("MBR_SEC_ST_CD"),
    F.col("MBR_SEC_ZIP_CD").alias("MBR_SEC_ZIP_CD"),
    F.col("MBR_SEC_ZIP_CD_4").alias("MBR_SEC_ZIP_CD_4"),
    F.col("ITS_SUB_ID").alias("ITS_SUB_ID"),
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("HOST_PLN_OVRD").alias("BCBSA_HOST_PLN_OVRD_CD"),
    F.col("HOST_PLN_CD").alias("BCBSA_HOST_PLN_ID"),
    F.col("EFF_DT").alias("DMGRPHC_BEG_DT"),
    F.col("EXPRTN_DT").alias("DMGRPHC_END_DT"),
    F.col("OOA_MBR_CD").alias("OOA_MBR_CD"),
    F.col("MCARE_CNTR_ID").alias("CMS_CNTR_ID")
)

df_trgt_seq_load_file = df_Xfm_Tbl_Load.select(
    F.rpad(F.col("CONSIS_MBR_ID"), 22, " ").alias("CONSIS_MBR_ID"),
    F.rpad(F.col("MBR_CONF_CD"), 3, " ").alias("MBR_CONF_CD"),
    F.rpad(F.col("MBR_PARTCPN_CD"), 1, " ").alias("MBR_PARTCPN_CD"),
    F.rpad(F.col("MBR_MED_COB_CD"), 1, " ").alias("MBR_MED_COB_CD"),
    F.rpad(F.col("COV_BEG_DT"), 10, " ").alias("COV_BEG_DT"),
    F.rpad(F.col("COV_END_DT"), 10, " ").alias("COV_END_DT"),
    F.rpad(F.col("MBR_LAST_NM"), 150, " ").alias("MBR_LAST_NM"),
    F.rpad(F.col("MBR_FIRST_NM"), 70, " ").alias("MBR_FIRST_NM"),
    F.rpad(F.col("MBR_BRTH_DT"), 10, " ").alias("MBR_BRTH_DT"),
    F.rpad(F.col("MBR_GNDR_CD"), 1, " ").alias("MBR_GNDR_CD"),
    F.rpad(F.col("MBR_RELSHP_CD"), 2, " ").alias("MBR_RELSHP_CD"),
    F.rpad(F.col("MBR_PRI_ADDR_1"), 70, " ").alias("MBR_PRI_ADDR_1"),
    F.rpad(F.col("MBR_PRI_ADDR_2"), 70, " ").alias("MBR_PRI_ADDR_2"),
    F.rpad(F.col("MBR_PRI_CITY_NM"), 35, " ").alias("MBR_PRI_CITY_NM"),
    F.rpad(F.col("MBR_PRI_ST_CD"), 2, " ").alias("MBR_PRI_ST_CD"),
    F.rpad(F.col("MBR_PRI_ZIP_CD"), 5, " ").alias("MBR_PRI_ZIP_CD"),
    F.rpad(F.col("MBR_PRI_ZIP_CD_4"), 4, " ").alias("MBR_PRI_ZIP_CD_4"),
    F.rpad(F.col("MBR_SEC_ADDR_1"), 70, " ").alias("MBR_SEC_ADDR_1"),
    F.rpad(F.col("MBR_SEC_ADDR_2"), 70, " ").alias("MBR_SEC_ADDR_2"),
    F.rpad(F.col("MBR_SEC_CITY_NM"), 35, " ").alias("MBR_SEC_CITY_NM"),
    F.rpad(F.col("MBR_SEC_ST_CD"), 2, " ").alias("MBR_SEC_ST_CD"),
    F.rpad(F.col("MBR_SEC_ZIP_CD"), 5, " ").alias("MBR_SEC_ZIP_CD"),
    F.rpad(F.col("MBR_SEC_ZIP_CD_4"), 4, " ").alias("MBR_SEC_ZIP_CD_4"),
    F.rpad(F.col("ITS_SUB_ID"), 17, " ").alias("ITS_SUB_ID"),
    F.rpad(F.col("MBR_ID"), 22, " ").alias("MBR_ID"),
    F.rpad(F.col("BCBSA_HOST_PLN_OVRD_CD"), 3, " ").alias("BCBSA_HOST_PLN_OVRD_CD"),
    F.rpad(F.col("BCBSA_HOST_PLN_ID"), 3, " ").alias("BCBSA_HOST_PLN_ID"),
    F.rpad(F.col("DMGRPHC_BEG_DT"), 8, " ").alias("DMGRPHC_BEG_DT"),
    F.rpad(F.col("DMGRPHC_END_DT"), 8, " ").alias("DMGRPHC_END_DT"),
    F.rpad(F.col("OOA_MBR_CD"), 2, " ").alias("OOA_MBR_CD"),
    F.rpad(F.col("CMS_CNTR_ID"), 8, " ").alias("CMS_CNTR_ID")
)

write_files(
    df_trgt_seq_load_file,
    f"{adls_path}/load/P_BCBSA_MBR_DTL.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)