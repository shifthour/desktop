# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  ScorecardEmplProdItemDailyCntl
# MAGIC 
# MAGIC PROCESSING:     Load SoreCard EmployeeProd Item information file to SCORE_CARD table SCRCRD_EMPL_PROD_ITEM
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                 Change Description                                                          Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                --------------------     \(9)------------------------                                    -----------------------------------------------------------------------                  ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Sruthi M.                    2018-06-28         5236-Indigo Replacement\(9)             Original Programming\(9)\(9)\(9)                      EnterpriseDev1                              Kalyan Neelam            2018-07-16

# MAGIC Read the load ready file created in the ScrcrdEmpProdItemGroupServicesExtr job
# MAGIC Load the file created in ScrcrdEmpProdItemGroupServicesExtr job into the table SCRCRD_EMPL_PROD_ITEM in Score_Card Database
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


ScoreCardOwner = get_widget_value('ScoreCardOwner','')
scorecard_secret_name = get_widget_value('scorecard_secret_name','')
Source = get_widget_value('Source','')

schema_Scrcrd_Empl_Prod_Item_Load_Ready_File = StructType([
    StructField("EMPL_ID", StringType(), False),
    StructField("SCRCRD_ITEM_ID", StringType(), False),
    StructField("RCRD_EXTR_DTM", TimestampType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("SCRCRD_ITEM_CAT_ID", StringType(), False),
    StructField("SCRCRD_ITEM_TYP_ID", StringType(), False),
    StructField("SCRCRD_WORK_TYP_ID", StringType(), False),
    StructField("SRC_SYS_INPT_METH_CD", StringType(), True),
    StructField("SRC_SYS_STATUS_CD", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("PROD_ABBR", StringType(), True),
    StructField("PROD_LOB_NO", StringType(), True),
    StructField("SCCF_NO", StringType(), True),
    StructField("SRC_SYS_RCVD_DTM", TimestampType(), False),
    StructField("SRC_SYS_CMPLD_DTM", TimestampType(), True),
    StructField("SRC_SYS_TRANS_DTM", TimestampType(), False),
    StructField("ACTVTY_CT", IntegerType(), True),
    StructField("CRT_DTM", TimestampType(), False),
    StructField("CRT_USER_ID", StringType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("LAST_UPDT_USER_ID", StringType(), False)
])

file_path = f"{adls_path}/load/Scrcrd_Prod_Item_{Source}.dat"

df_Scrcrd_Empl_Prod_Item_Load_Ready_File = (
    spark.read.format("csv")
    .option("sep", "#$")
    .option("header", "false")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("nullValue", None)
    .schema(schema_Scrcrd_Empl_Prod_Item_Load_Ready_File)
    .load(file_path)
)

df_Scrcrd_Empl_Prod_Item_Load_Ready_File = df_Scrcrd_Empl_Prod_Item_Load_Ready_File.select(
    "EMPL_ID",
    "SCRCRD_ITEM_ID",
    "RCRD_EXTR_DTM",
    "SRC_SYS_CD",
    "SCRCRD_ITEM_CAT_ID",
    "SCRCRD_ITEM_TYP_ID",
    "SCRCRD_WORK_TYP_ID",
    "SRC_SYS_INPT_METH_CD",
    "SRC_SYS_STATUS_CD",
    "GRP_ID",
    "MBR_ID",
    "PROD_ABBR",
    "PROD_LOB_NO",
    "SCCF_NO",
    "SRC_SYS_RCVD_DTM",
    "SRC_SYS_CMPLD_DTM",
    "SRC_SYS_TRANS_DTM",
    "ACTVTY_CT",
    "CRT_DTM",
    "CRT_USER_ID",
    "LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID"
)

df_lnk_cpy_scrcrd_prod_item = df_Scrcrd_Empl_Prod_Item_Load_Ready_File.select(
    F.col("EMPL_ID").alias("EMPL_ID"),
    F.col("SCRCRD_ITEM_ID").alias("SCRCRD_ITEM_ID"),
    F.col("RCRD_EXTR_DTM").alias("RCRD_EXTR_DTM"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SCRCRD_ITEM_CAT_ID").alias("SCRCRD_ITEM_CAT_ID"),
    F.col("SCRCRD_ITEM_TYP_ID").alias("SCRCRD_ITEM_TYP_ID"),
    F.col("SCRCRD_WORK_TYP_ID").alias("SCRCRD_WORK_TYP_ID"),
    F.col("SRC_SYS_INPT_METH_CD").alias("SRC_SYS_INPT_METH_CD"),
    F.col("SRC_SYS_STATUS_CD").alias("SRC_SYS_STTUS_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("PROD_ABBR").alias("PROD_ABBR"),
    F.col("PROD_LOB_NO").alias("PROD_LOB_NO"),
    F.col("SCCF_NO").alias("SCCF_NO"),
    F.col("SRC_SYS_RCVD_DTM").alias("SRC_SYS_RCVD_DTM"),
    F.col("SRC_SYS_CMPLD_DTM").alias("SRC_SYS_CMPLD_DTM"),
    F.col("SRC_SYS_TRANS_DTM").alias("SRC_SYS_TRANS_DTM"),
    F.col("ACTVTY_CT").alias("ACTVTY_CT"),
    F.col("CRT_DTM").alias("CRT_DTM"),
    F.col("CRT_USER_ID").alias("CRT_USER_ID"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
)

df_lnk_cpy_scrcrd_prod_item = df_lnk_cpy_scrcrd_prod_item.withColumn(
    "EMPL_ID", F.rpad(F.col("EMPL_ID"), <...>, " ")
).withColumn(
    "SCRCRD_ITEM_ID", F.rpad(F.col("SCRCRD_ITEM_ID"), <...>, " ")
).withColumn(
    "SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " ")
).withColumn(
    "SCRCRD_ITEM_CAT_ID", F.rpad(F.col("SCRCRD_ITEM_CAT_ID"), <...>, " ")
).withColumn(
    "SCRCRD_ITEM_TYP_ID", F.rpad(F.col("SCRCRD_ITEM_TYP_ID"), <...>, " ")
).withColumn(
    "SCRCRD_WORK_TYP_ID", F.rpad(F.col("SCRCRD_WORK_TYP_ID"), <...>, " ")
).withColumn(
    "SRC_SYS_INPT_METH_CD", F.rpad(F.col("SRC_SYS_INPT_METH_CD"), <...>, " ")
).withColumn(
    "SRC_SYS_STTUS_CD", F.rpad(F.col("SRC_SYS_STTUS_CD"), <...>, " ")
).withColumn(
    "GRP_ID", F.rpad(F.col("GRP_ID"), <...>, " ")
).withColumn(
    "MBR_ID", F.rpad(F.col("MBR_ID"), <...>, " ")
).withColumn(
    "PROD_ABBR", F.rpad(F.col("PROD_ABBR"), <...>, " ")
).withColumn(
    "PROD_LOB_NO", F.rpad(F.col("PROD_LOB_NO"), <...>, " ")
).withColumn(
    "SCCF_NO", F.rpad(F.col("SCCF_NO"), <...>, " ")
).withColumn(
    "CRT_USER_ID", F.rpad(F.col("CRT_USER_ID"), <...>, " ")
).withColumn(
    "LAST_UPDT_USER_ID", F.rpad(F.col("LAST_UPDT_USER_ID"), <...>, " ")
)

df_lnk_cpy_scrcrd_prod_item = df_lnk_cpy_scrcrd_prod_item.select(
    "EMPL_ID",
    "SCRCRD_ITEM_ID",
    "RCRD_EXTR_DTM",
    "SRC_SYS_CD",
    "SCRCRD_ITEM_CAT_ID",
    "SCRCRD_ITEM_TYP_ID",
    "SCRCRD_WORK_TYP_ID",
    "SRC_SYS_INPT_METH_CD",
    "SRC_SYS_STTUS_CD",
    "GRP_ID",
    "MBR_ID",
    "PROD_ABBR",
    "PROD_LOB_NO",
    "SCCF_NO",
    "SRC_SYS_RCVD_DTM",
    "SRC_SYS_CMPLD_DTM",
    "SRC_SYS_TRANS_DTM",
    "ACTVTY_CT",
    "CRT_DTM",
    "CRT_USER_ID",
    "LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID"
)

jdbc_url, jdbc_props = get_db_config(scorecard_secret_name)

temp_table_name = "STAGING.ScrcrdEmpProdItemTableLoad_SCRCRD_EMP_PROD_ITEM_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)

df_lnk_cpy_scrcrd_prod_item.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table_name) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {ScoreCardOwner}.SCRCRD_EMPL_PROD_ITEM AS T
USING {temp_table_name} AS S
ON
    T.EMPL_ID = S.EMPL_ID
    AND T.SCRCRD_ITEM_ID = S.SCRCRD_ITEM_ID
    AND T.RCRD_EXTR_DTM = S.RCRD_EXTR_DTM
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.SCRCRD_WORK_TYP_ID = S.SCRCRD_WORK_TYP_ID
WHEN MATCHED THEN UPDATE SET
    T.EMPL_ID = S.EMPL_ID,
    T.SCRCRD_ITEM_ID = S.SCRCRD_ITEM_ID,
    T.RCRD_EXTR_DTM = S.RCRD_EXTR_DTM,
    T.SRC_SYS_CD = S.SRC_SYS_CD,
    T.SCRCRD_ITEM_CAT_ID = S.SCRCRD_ITEM_CAT_ID,
    T.SCRCRD_ITEM_TYP_ID = S.SCRCRD_ITEM_TYP_ID,
    T.SCRCRD_WORK_TYP_ID = S.SCRCRD_WORK_TYP_ID,
    T.SRC_SYS_INPT_METH_CD = S.SRC_SYS_INPT_METH_CD,
    T.SRC_SYS_STTUS_CD = S.SRC_SYS_STTUS_CD,
    T.GRP_ID = S.GRP_ID,
    T.MBR_ID = S.MBR_ID,
    T.PROD_ABBR = S.PROD_ABBR,
    T.PROD_LOB_NO = S.PROD_LOB_NO,
    T.SCCF_NO = S.SCCF_NO,
    T.SRC_SYS_RCVD_DTM = S.SRC_SYS_RCVD_DTM,
    T.SRC_SYS_CMPLD_DTM = S.SRC_SYS_CMPLD_DTM,
    T.SRC_SYS_TRANS_DTM = S.SRC_SYS_TRANS_DTM,
    T.ACTVTY_CT = S.ACTVTY_CT,
    T.CRT_DTM = S.CRT_DTM,
    T.CRT_USER_ID = S.CRT_USER_ID,
    T.LAST_UPDT_DTM = S.LAST_UPDT_DTM,
    T.LAST_UPDT_USER_ID = S.LAST_UPDT_USER_ID
WHEN NOT MATCHED THEN
    INSERT (
      EMPL_ID,
      SCRCRD_ITEM_ID,
      RCRD_EXTR_DTM,
      SRC_SYS_CD,
      SCRCRD_ITEM_CAT_ID,
      SCRCRD_ITEM_TYP_ID,
      SCRCRD_WORK_TYP_ID,
      SRC_SYS_INPT_METH_CD,
      SRC_SYS_STTUS_CD,
      GRP_ID,
      MBR_ID,
      PROD_ABBR,
      PROD_LOB_NO,
      SCCF_NO,
      SRC_SYS_RCVD_DTM,
      SRC_SYS_CMPLD_DTM,
      SRC_SYS_TRANS_DTM,
      ACTVTY_CT,
      CRT_DTM,
      CRT_USER_ID,
      LAST_UPDT_DTM,
      LAST_UPDT_USER_ID
    )
    VALUES (
      S.EMPL_ID,
      S.SCRCRD_ITEM_ID,
      S.RCRD_EXTR_DTM,
      S.SRC_SYS_CD,
      S.SCRCRD_ITEM_CAT_ID,
      S.SCRCRD_ITEM_TYP_ID,
      S.SCRCRD_WORK_TYP_ID,
      S.SRC_SYS_INPT_METH_CD,
      S.SRC_SYS_STTUS_CD,
      S.GRP_ID,
      S.MBR_ID,
      S.PROD_ABBR,
      S.PROD_LOB_NO,
      S.SCCF_NO,
      S.SRC_SYS_RCVD_DTM,
      S.SRC_SYS_CMPLD_DTM,
      S.SRC_SYS_TRANS_DTM,
      S.ACTVTY_CT,
      S.CRT_DTM,
      S.CRT_USER_ID,
      S.LAST_UPDT_DTM,
      S.LAST_UPDT_USER_ID
    );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

update_sql = f"UPDATE {ScoreCardOwner}.SCRCRD_EMPL_PROD_ITEM SET SCRCRD_ITEM_ID=Right('0000000000'+ cast(SCRCRD_EMPL_PROD_ITEM_SK as varchar(10)),10) WHERE CRT_USER_ID='PHONEEXTRACT'"
execute_dml(update_sql, jdbc_url, jdbc_props)