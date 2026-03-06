# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2015 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     OpsDashboardClmInvtryCntl
# MAGIC 
# MAGIC Processing:
# MAGIC                     *
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC              Previous Run Successful:    Restart, no other steps necessary
# MAGIC              Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                               Project/                                                                                                 Code                  Date
# MAGIC Developer        Date              Altiris #           Change Description                                                        Reviewer            Reviewed
# MAGIC ----------------------  -------------------   -------------------   ------------------------------------------------------------------------------------   ----------------------     -------------------   
# MAGIC ADasarathy      07/20/2015     5407             Original program                                                             Kalyan Neelam    2015-07-20

# MAGIC Extract UWS Operations Dashboard Work Unit Xref Data
# MAGIC SK Lookup for OPS_WORK_UNIT_ID
# MAGIC Writing Sequential File to ../load
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')

jdbc_url_lnk_OPS_WORK_UNIT_XREF, jdbc_props_lnk_OPS_WORK_UNIT_XREF = get_db_config(uws_secret_name)
df_lnk_OPS_WORK_UNIT_XREF = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lnk_OPS_WORK_UNIT_XREF)
    .options(**jdbc_props_lnk_OPS_WORK_UNIT_XREF)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, SRC_VAL_TX, OPS_WORK_UNIT_ID, USER_ID, LAST_UPDT_DT_SK FROM {UWSOwner}.OPS_WORK_UNIT_XREF"
    )
    .load()
)

jdbc_url_db2_OPS_WORK_UNIT_extr, jdbc_props_db2_OPS_WORK_UNIT_extr = get_db_config(ids_secret_name)
df_db2_OPS_WORK_UNIT_extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_OPS_WORK_UNIT_extr)
    .options(**jdbc_props_db2_OPS_WORK_UNIT_extr)
    .option(
        "query",
        f"SELECT OPS_WORK_UNIT_SK, OPS_WORK_UNIT_ID FROM {IDSOwner}.OPS_WORK_UNIT"
    )
    .load()
)

df_lookup_9 = (
    df_lnk_OPS_WORK_UNIT_XREF.alias("Extract")
    .join(
        df_db2_OPS_WORK_UNIT_extr.alias("cdMppngLkup"),
        F.col("Extract.OPS_WORK_UNIT_ID") == F.col("cdMppngLkup.OPS_WORK_UNIT_ID"),
        "left"
    )
    .select(
        F.col("Extract.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Extract.SRC_VAL_TX").alias("SRC_VAL_TX"),
        F.col("cdMppngLkup.OPS_WORK_UNIT_SK").alias("OPS_WORK_UNIT_SK"),
        F.col("Extract.USER_ID").alias("USER_ID"),
        F.col("Extract.LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK")
    )
)

df_xfrm_BusinessRules = df_lookup_9.select(
    F.col("SRC_SYS_CD"),
    F.col("SRC_VAL_TX"),
    F.when(F.col("OPS_WORK_UNIT_SK").isNull(), F.lit(0)).otherwise(F.col("OPS_WORK_UNIT_SK")).alias("OPS_WORK_UNIT_SK"),
    F.col("USER_ID"),
    F.col("LAST_UPDT_DT_SK")
)

df_final = df_xfrm_BusinessRules.withColumn(
    "LAST_UPDT_DT_SK",
    F.rpad("LAST_UPDT_DT_SK", 10, " ")
).select(
    "SRC_SYS_CD",
    "SRC_VAL_TX",
    "OPS_WORK_UNIT_SK",
    "USER_ID",
    "LAST_UPDT_DT_SK"
)

write_files(
    df_final,
    f"{adls_path}/load/P_OPS_WORK_UNIT_XREF.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)