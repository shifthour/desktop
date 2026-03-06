# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 08:28:47 Batch  14638_30532 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 11:16:35 Batch  14605_40601 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/21/07 14:41:36 Batch  14570_52900 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/10/07 13:50:33 Batch  14528_49851 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 10/10/07 13:38:24 Batch  14528_49120 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_1 10/08/07 17:37:28 Batch  14526_63461 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_1 10/08/07 17:30:51 Batch  14526_63055 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   FdbIdsDoseFormBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                     ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              04/16/2007         3264                              Originally Programmed                                      devlIDS30    
# MAGIC 
# MAGIC Parikshith Chada               8/28/2007         3264                              Modified the balancing process,                       devlIDS30                     Steph Goddard            09/27/2007
# MAGIC                                                                                                           by changing snapshot file to snapshot table

# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


runid = get_widget_value("RunID", "")
ToleranceCd = get_widget_value("ToleranceCd", "")
IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

extract_query = f"SELECT DOSE_FORM.DOSE_FORM_CD AS SRC_DOSE_FORM_CD, B_DOSE_FORM.DOSE_FORM_CD AS TRGT_DOSE_FORM_CD FROM {IDSOwner}.DOSE_FORM DOSE_FORM FULL OUTER JOIN {IDSOwner}.B_DOSE_FORM B_DOSE_FORM ON DOSE_FORM.DOSE_FORM_CD = B_DOSE_FORM.DOSE_FORM_CD"
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

w = Window.orderBy(F.lit(1))
df_SrcTrgtComp_rn = df_SrcTrgtComp.withColumn("_rownum", F.row_number().over(w))

df_Research = df_SrcTrgtComp.filter(
    (F.col("SRC_DOSE_FORM_CD").isNull()) | (F.col("TRGT_DOSE_FORM_CD").isNull())
).select("TRGT_DOSE_FORM_CD", "SRC_DOSE_FORM_CD")
df_Research = df_Research.withColumn("TRGT_DOSE_FORM_CD", rpad("TRGT_DOSE_FORM_CD", <...>, " "))
df_Research = df_Research.withColumn("SRC_DOSE_FORM_CD", rpad("SRC_DOSE_FORM_CD", <...>, " "))

df_Notify = df_SrcTrgtComp_rn.filter(
    (F.col("_rownum") == 1) & (F.lit(ToleranceCd) == F.lit("OUT"))
).select(
    F.lit("ROW COUNT BALANCING FIRST DATA BANK - IDS DOSE FORM OUT OF TOLERANCE").alias("NOTIFICATION")
)
df_Notify = df_Notify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))

write_files(
    df_Research,
    f"{adls_path}/balancing/research/FdbIdsDoseFormResearch.dat.{runid}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/NdcBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)