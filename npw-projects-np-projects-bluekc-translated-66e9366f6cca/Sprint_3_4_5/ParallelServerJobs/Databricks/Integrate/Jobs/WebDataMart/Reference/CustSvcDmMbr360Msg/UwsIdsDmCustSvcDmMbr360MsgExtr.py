# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC =============================================================================================
# MAGIC 
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC Documentation: Pulls data from UWS(MBR_360_MSG) table and loads in to a Datamart (CUST_SVC_DM_MBR_360_MSG) table.
# MAGIC 
# MAGIC 
# MAGIC Modification Log:
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project          Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------         -------------------------------       -------------------------
# MAGIC Terri O'Bryan                    09/23/2009           4113                                    Original Programming.                                                        devlIDSnew                     Steph Goddard            09/25/2009
# MAGIC 
# MAGIC Archana Palivela            03/26/2014            5114                                     Original Programming (server to Parallel Conv)                 IntegrateWrhsDevl           Bhoomi Dasari              4/6/2014

# MAGIC Write CUST_SVC_DM_MBR_360_MSG Data into a Sequential file for Load Job UwsIdsDmCustSvcDmHiVoltCntctLoad.
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: UwsIdsDmCustSvcDmHiVoltCntctExtr
# MAGIC Read from source table MBR_360_MSG from UWS.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url, jdbc_props = get_db_config(uws_secret_name)
extract_query = f"SELECT MBR_360_MSG_NO, EFF_DT_SK, TERM_DT_SK, MSG_TX, USER_ID, LAST_UPDT_DT_SK FROM {UWSOwner}.MBR_360_MSG"
df_odbc_MBR_360_MSG_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfm_BusinessLogic = (
    df_odbc_MBR_360_MSG_In
    .withColumn("MBR_360_MSG_NO", F.col("MBR_360_MSG_NO"))
    .withColumn("SRC_SYS_CD", F.lit("UWS"))
    .withColumn(
        "EFF_DT",
        F.when(
            (trim(F.col("EFF_DT_SK")) == "UNK") | (trim(F.col("EFF_DT_SK")) == "NA"),
            F.lit("1753-01-01 00:00:00")
        ).otherwise(
            FORMAT_DATE_EE(F.col("EFF_DT_SK"), F.lit("DATE"), F.lit("DATE"), F.lit("SYBTIMESTAMP"))
        )
    )
    .withColumn(
        "TERM_DT",
        F.when(
            (trim(F.col("TERM_DT_SK")) == "UNK") | (trim(F.col("TERM_DT_SK")) == "NA"),
            F.lit("1753-01-01 00:00:00")
        ).otherwise(
            FORMAT_DATE_EE(F.col("TERM_DT_SK"), F.lit("DATE"), F.lit("DATE"), F.lit("SYBTIMESTAMP"))
        )
    )
    .withColumn("MSG_TX", trim(F.col("MSG_TX")))
    .withColumn("USER_ID", F.col("USER_ID"))
    .withColumn("LAST_UPDT_DT", F.col("LAST_UPDT_DT_SK"))
)

df_final = (
    df_xfm_BusinessLogic
    .withColumn("LAST_UPDT_DT", F.rpad(F.col("LAST_UPDT_DT"), 10, " "))
    .select(
        "MBR_360_MSG_NO",
        "SRC_SYS_CD",
        "EFF_DT",
        "TERM_DT",
        "MSG_TX",
        "USER_ID",
        "LAST_UPDT_DT"
    )
)

write_files(
    df_final,
    f"{adls_path}/load/CUST_SVC_DM_MBR_360_MSG.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)