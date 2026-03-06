# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 11/12/07 10:00:11 Batch  14561_36015 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 11/02/07 13:07:35 Batch  14551_47275 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 11/02/07 12:52:45 Batch  14551_46368 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/31/07 13:17:35 Batch  14549_47857 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/31/07 13:09:37 Batch  14549_47387 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    FctsIncomeBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job loads the SUBJECT_AREA_BAL_SUM table after the balancing control job is done for a given Subject Area              
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               6/18/2007          3264                              Originally Programmed                           devlIDS30                    Steph Goddard            09/15/2007


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
Target = get_widget_value('Target','')
Subject = get_widget_value('Subject','')
CurrDate = get_widget_value('CurrDate','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(uws_secret_name)

extract_query_1 = f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, TBL_BAL_SUM.SUBJ_AREA_NM, TBL_BAL_SUM.BAL_DT, TBL_BAL_SUM.ROW_CT_TLRNC_CD FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' ORDER BY TBL_BAL_SUM.BAL_DT ASC"
df_TBL_BAL_SUM_TblBalExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_1)
    .load()
)

extract_query_2 = f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, TBL_BAL_SUM.SUBJ_AREA_NM, COUNT(*) AS ROW_COUNT FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' GROUP BY TBL_BAL_SUM.TRGT_SYS_CD, TBL_BAL_SUM.SUBJ_AREA_NM"
df_TBL_BAL_SUM_IncmCt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

df_hf_income_tbl_bal_lkup = dedup_sort(
    df_TBL_BAL_SUM_IncmCt,
    ["TRGT_SYS_CD", "SUBJ_AREA_NM"],
    [("TRGT_SYS_CD", "A"), ("SUBJ_AREA_NM", "A")]
)

df_Transform_join = df_TBL_BAL_SUM_TblBalExtr.alias("TblBalExtr").join(
    df_hf_income_tbl_bal_lkup.alias("IncmCtLoad"),
    (F.col("TblBalExtr.TRGT_SYS_CD") == F.col("IncmCtLoad.TRGT_SYS_CD")) &
    (F.col("TblBalExtr.SUBJ_AREA_NM") == F.col("IncmCtLoad.SUBJ_AREA_NM")),
    "left"
).select(
    F.col("TblBalExtr.TRGT_SYS_CD").alias("tbl_trgt_sys_cd"),
    F.col("TblBalExtr.SUBJ_AREA_NM").alias("tbl_subj_area_nm"),
    F.col("TblBalExtr.BAL_DT").alias("tbl_bal_dt"),
    F.col("TblBalExtr.ROW_CT_TLRNC_CD").alias("tbl_row_ct_tlrnc_cd"),
    F.col("IncmCtLoad.ROW_COUNT").alias("lkp_row_count")
).sort("tbl_bal_dt")

transform_rows = df_Transform_join.collect()

svPrevCount = 0
svTempTlrncCd = ""
svTempRslvIn = "Y"
svTempRslvIncd = ""
output_rows = []

for r in transform_rows:
    svCurrTlrncCd = r["tbl_row_ct_tlrnc_cd"]
    svCurrCount = svPrevCount + 1
    bal_dt_str = str(r["tbl_bal_dt"])[0:10]
    if bal_dt_str != CurrDate:
        new_svTempTlrncCd = "NEXT ROW"
    else:
        if svCurrTlrncCd == "BAL" and svTempTlrncCd not in ["OUT", "IN"]:
            new_svTempTlrncCd = "BAL"
        elif svCurrTlrncCd == "BAL" and svTempTlrncCd == "IN":
            new_svTempTlrncCd = "IN"
        elif svCurrTlrncCd == "IN" and svTempTlrncCd != "OUT":
            new_svTempTlrncCd = "IN"
        else:
            new_svTempTlrncCd = "OUT"
    if bal_dt_str == CurrDate:
        new_svTempRslvIn = svTempRslvIn
    else:
        if svCurrTlrncCd == "BAL" and svTempRslvIncd != "OUT":
            new_svTempRslvIn = "Y"
        elif svCurrTlrncCd == "IN" and svTempRslvIncd != "OUT":
            new_svTempRslvIn = "Y"
        else:
            new_svTempRslvIn = "N"
    if bal_dt_str == CurrDate:
        new_svTempRslvIncd = "NA"
    else:
        if svCurrTlrncCd == "BAL" and svTempRslvIncd not in ["OUT", "IN"]:
            new_svTempRslvIncd = "BAL"
        elif svCurrTlrncCd == "BAL" and svTempRslvIncd == "IN":
            new_svTempRslvIncd = "IN"
        elif svCurrTlrncCd == "IN" and svTempRslvIncd != "OUT":
            new_svTempRslvIncd = "IN"
        else:
            new_svTempRslvIncd = "OUT"
    new_svPrevCount = svCurrCount
    if new_svPrevCount == r["lkp_row_count"]:
        output_rows.append((
            Target,
            Subject,
            CurrDate,
            new_svTempTlrncCd,
            new_svTempRslvIn.ljust(1, " "),
            ExtrRunCycle,
            CurrDate,
            CurrDate,
            "$UWSAcct"
        ))
    svPrevCount = new_svPrevCount
    svTempTlrncCd = new_svTempTlrncCd
    svTempRslvIn = new_svTempRslvIn
    svTempRslvIncd = new_svTempRslvIncd

schema_SubjAreaBalLoad = StructType([
    StructField("TRGT_SYS_CD", StringType(), True),
    StructField("SUBJ_AREA_NM", StringType(), True),
    StructField("LAST_BAL_DT", StringType(), True),
    StructField("TLRNC_CD", StringType(), True),
    StructField("PREV_ISSUE_CRCTD_IN", StringType(), True),
    StructField("TRGT_RUN_CYC_NO", StringType(), True),
    StructField("CRT_DTM", StringType(), True),
    StructField("LAST_UPDT_DTM", StringType(), True),
    StructField("USER_ID", StringType(), True)
])

df_SubjAreaBalLoad = spark.createDataFrame(output_rows, schema_SubjAreaBalLoad)

temp_table_name = "STAGING.UwsIncomeSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp"
drop_temp_table_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
execute_dml(drop_temp_table_sql, jdbc_url, jdbc_props)

df_SubjAreaBalLoad.write\
    .format("jdbc")\
    .option("url", jdbc_url)\
    .options(**jdbc_props)\
    .option("dbtable", temp_table_name)\
    .mode("overwrite")\
    .save()

merge_sql = f"""
MERGE INTO {UWSOwner}.SUBJ_AREA_BAL_SUM AS T
USING {temp_table_name} AS S
ON T.TRGT_SYS_CD = S.TRGT_SYS_CD
AND T.SUBJ_AREA_NM = S.SUBJ_AREA_NM
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_BAL_DT = S.LAST_BAL_DT,
    T.TLRNC_CD = S.TLRNC_CD,
    T.PREV_ISSUE_CRCTD_IN = S.PREV_ISSUE_CRCTD_IN,
    T.TRGT_RUN_CYC_NO = S.TRGT_RUN_CYC_NO,
    T.CRT_DTM = S.CRT_DTM,
    T.LAST_UPDT_DTM = S.LAST_UPDT_DTM,
    T.USER_ID = S.USER_ID
WHEN NOT MATCHED THEN
  INSERT (
    TRGT_SYS_CD,
    SUBJ_AREA_NM,
    LAST_BAL_DT,
    TLRNC_CD,
    PREV_ISSUE_CRCTD_IN,
    TRGT_RUN_CYC_NO,
    CRT_DTM,
    LAST_UPDT_DTM,
    USER_ID
  )
  VALUES (
    S.TRGT_SYS_CD,
    S.SUBJ_AREA_NM,
    S.LAST_BAL_DT,
    S.TLRNC_CD,
    S.PREV_ISSUE_CRCTD_IN,
    S.TRGT_RUN_CYC_NO,
    S.CRT_DTM,
    S.LAST_UPDT_DTM,
    S.USER_ID
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)