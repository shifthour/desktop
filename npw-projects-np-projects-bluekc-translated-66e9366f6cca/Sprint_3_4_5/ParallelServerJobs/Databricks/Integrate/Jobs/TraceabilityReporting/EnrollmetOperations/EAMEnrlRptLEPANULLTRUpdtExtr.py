# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Calling Job: EAMEnrlRptLEPLTRSeq
# MAGIC 
# MAGIC Description: This job fetches the data from tables to update the StopClock in EAM RPT table LEP_ANUL_LTR
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ================================================================================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                                                      Development Project                         Reviewer                     Review Date
# MAGIC ================================================================================================================================================================================================
# MAGIC John Abraham               2021-10-11                      US428922                                    Original Programming                                                                                       IntegrateDev2                                Reddy Sanam              10/12/2021

# MAGIC This job fetches the data from tables to update the StopClock in EAM RPT table LEP_ANUL_LTR
# MAGIC 
# MAGIC ---ER075 UPDATE Query
# MAGIC UPDATE LEP_ANNUAL_LTR SET StopClock = (SELECT MIN(ECIT_CREATE_DT) as ECIT_CREATE_DT  FROM dbo.TCS_MANIFEST_LOG b JOIN dbo.tbEENRLMembers c  ON c.MemberID = b.MEMBER_ID WHERE a.MBI = c.HIC AND b.PLACE_ID = '999OnDemand'  AND b.TEMPLATE_ID = '24090X_Lep_YearlyChange_999' AND a.StartClock < b.ECIT_CREATE_DT)   FROM LEP_ANNUAL_LTR a  WHERE a.RecordType = 'PD'    AND a.LEPAmount > 0;
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, min, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


EAMRPTOwner = get_widget_value("EAMRPTOwner","")
eamrpt_secret_name = get_widget_value("eamrpt_secret_name","")
EAMOwner = get_widget_value("EAMOwner","")
eam_secret_name = get_widget_value("eam_secret_name","")
RUNID = get_widget_value("RUNID","")

jdbc_url_eamrpt, jdbc_props_eamrpt = get_db_config(eamrpt_secret_name)
df_EAM_RPT_LEP_ANUL_LTR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eamrpt)
    .options(**jdbc_props_eamrpt)
    .option("query", f"Select MBI, START_CLCK_DTM From {EAMRPTOwner}.LEP_ANUL_LTR")
    .load()
)

jdbc_url_eamrpt_2, jdbc_props_eamrpt_2 = get_db_config(eamrpt_secret_name)
df_EAM_TCS_MANIFEST_LOG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eamrpt_2)
    .options(**jdbc_props_eamrpt_2)
    .option("query", f"Select MEMBER_ID, ECIT_CREATE_DT From {EAMRPTOwner}.TCS_MANIFEST_LOG Where PLACE_ID = '999OnDemand' And TEMPLATE_ID = '24090X_Lep_YearlyChange_999'")
    .load()
)

jdbc_url_eam, jdbc_props_eam = get_db_config(eam_secret_name)
df_EAM_tbEENRLMembers = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eam)
    .options(**jdbc_props_eam)
    .option("query", f"Select MemberID, HIC From {EAMOwner}.tbEENRLMembers")
    .load()
)

df_Lkp_MBRID = (
    df_EAM_tbEENRLMembers.alias("tbEENRLMembers")
    .crossJoin(df_EAM_TCS_MANIFEST_LOG.alias("TCS_MANIFEST_LOG"))
    .select(
        col("tbEENRLMembers.HIC").alias("HIC"),
        col("TCS_MANIFEST_LOG.ECIT_CREATE_DT").alias("ECIT_CREATE_DT")
    )
)

df_LKP_MBI = (
    df_EAM_RPT_LEP_ANUL_LTR.alias("LEP_ANUL_LTR")
    .join(
        df_Lkp_MBRID.alias("MBI"),
        col("LEP_ANUL_LTR.MBI") == col("MBI.HIC"),
        how="inner"
    )
    .select(
        col("LEP_ANUL_LTR.MBI").alias("MBI"),
        col("LEP_ANUL_LTR.START_CLCK_DTM").alias("START_CLCK_DTM"),
        col("MBI.ECIT_CREATE_DT").alias("ECIT_CREATE_DT")
    )
)

df_Trx_MBI = (
    df_LKP_MBI
    .filter(col("ECIT_CREATE_DT") > col("START_CLCK_DTM"))
    .select(
        col("MBI").alias("MBI"),
        col("ECIT_CREATE_DT").alias("ECIT_CREATE_DT")
    )
)

df_Aggr_ECITCRTDT = df_Trx_MBI.groupBy("MBI").agg(min("ECIT_CREATE_DT").alias("ECIT_CREATE_DT"))

df_final = df_Aggr_ECITCRTDT.select(
    rpad(col("MBI"), <...>, " ").alias("MBI"),
    rpad(col("ECIT_CREATE_DT"), <...>, " ").alias("ECIT_CREATE_DT")
)

write_files(
    df_final,
    f"{adls_path}/load/EAM_LEP_LTR_UPDATE_EXTR.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)