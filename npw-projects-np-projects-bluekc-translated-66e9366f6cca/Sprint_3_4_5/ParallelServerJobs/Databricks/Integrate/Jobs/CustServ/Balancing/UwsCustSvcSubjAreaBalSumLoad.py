# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 11/01/07 16:40:40 Batch  14550_60068 PROMOTE bckcetl ids20 dsadm rc for brent
# MAGIC ^1_1 11/01/07 16:19:50 Batch  14550_58802 INIT bckcett testIDS30 dsadm rc for brent
# MAGIC ^1_2 10/29/07 15:58:44 Batch  14547_57529 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_2 10/29/07 15:35:38 Batch  14547_56143 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 10/29/07 14:48:31 Batch  14547_53315 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    FctsCustSvcBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job loads the SUBJECT_AREA_BAL_SUM table after the balancing control job is done for a given Subject Area              
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               6/26/2007          3264                              Originally Programmed                           devlIDS30                  Steph Goddard             9/14/07


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

jdbc_url, jdbc_props = get_db_config(get_widget_value('uws_secret_name',''))
UWSOwner = get_widget_value('UWSOwner','')
Target = get_widget_value('Target','')
Subject = get_widget_value('Subject','')
CurrDate = get_widget_value('CurrDate','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
UWSAcct = get_widget_value('UWSAcct','')

df_TBL_BAL_SUM_TblBalExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, TBL_BAL_SUM.SUBJ_AREA_NM, TBL_BAL_SUM.BAL_DT, TBL_BAL_SUM.ROW_CT_TLRNC_CD FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' ORDER BY TBL_BAL_SUM.BAL_DT ASC"
    )
    .load()
)

df_TBL_BAL_SUM_CustSvcCt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT TBL_BAL_SUM.TRGT_SYS_CD, TBL_BAL_SUM.SUBJ_AREA_NM, COUNT(*) AS ROW_COUNT FROM {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM WHERE TBL_BAL_SUM.TRGT_SYS_CD = '{Target}' AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}' GROUP BY TBL_BAL_SUM.TRGT_SYS_CD, TBL_BAL_SUM.SUBJ_AREA_NM"
    )
    .load()
)

df_dedup_CustSvcCt = dedup_sort(
    df_TBL_BAL_SUM_CustSvcCt,
    ["TRGT_SYS_CD","SUBJ_AREA_NM"],
    []
)

df_join = df_TBL_BAL_SUM_TblBalExtr.alias("TblBalExtr").join(
    df_dedup_CustSvcCt.alias("CustSvcCtLoad"),
    [
        F.col("TblBalExtr.TRGT_SYS_CD") == F.col("CustSvcCtLoad.TRGT_SYS_CD"),
        F.col("TblBalExtr.SUBJ_AREA_NM") == F.col("CustSvcCtLoad.SUBJ_AREA_NM")
    ],
    "left"
)

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

rows_join = df_join.collect()
output_rows = []
svPrevCount = 0
svTempTlrncCd_old = ""
svTempRslvIn_old = "Y"
svTempRslvIncd_old = ""

for r in rows_join:
    row_count_value = 0
    if r["ROW_COUNT"] is not None:
        row_count_value = r["ROW_COUNT"]
    bal_dt_str = ""
    if r["BAL_DT"] is not None:
        bal_dt_str = str(r["BAL_DT"])
    svCurrTlrncCd = ""
    if r["ROW_CT_TLRNC_CD"] is not None:
        svCurrTlrncCd = r["ROW_CT_TLRNC_CD"]
    svCurrCount = svPrevCount + 1
    if bal_dt_str[0:10] != CurrDate:
        svTempTlrncCd_new = "NEXT ROW"
    else:
        if svCurrTlrncCd == "BAL" and svTempTlrncCd_old not in ("OUT","IN"):
            svTempTlrncCd_new = "BAL"
        elif svCurrTlrncCd == "BAL" and svTempTlrncCd_old == "IN":
            svTempTlrncCd_new = "IN"
        elif svCurrTlrncCd == "IN" and svTempTlrncCd_old != "OUT":
            svTempTlrncCd_new = "IN"
        else:
            svTempTlrncCd_new = "OUT"
    if bal_dt_str[0:10] == CurrDate:
        svTempRslvIn_new = svTempRslvIn_old
    else:
        if svCurrTlrncCd == "BAL" and svTempRslvIncd_old != "OUT":
            svTempRslvIn_new = "Y"
        elif svCurrTlrncCd == "IN" and svTempRslvIncd_old != "OUT":
            svTempRslvIn_new = "Y"
        else:
            svTempRslvIn_new = "N"
    if bal_dt_str[0:10] == CurrDate:
        svTempRslvIncd_new = "NA"
    else:
        if svCurrTlrncCd == "BAL" and svTempRslvIncd_old not in ("OUT","IN"):
            svTempRslvIncd_new = "BAL"
        elif svCurrTlrncCd == "BAL" and svTempRslvIncd_old == "IN":
            svTempRslvIncd_new = "IN"
        elif svCurrTlrncCd == "IN" and svTempRslvIncd_old != "OUT":
            svTempRslvIncd_new = "IN"
        else:
            svTempRslvIncd_new = "OUT"
    svPrevCount = svCurrCount
    svTempTlrncCd_old = svTempTlrncCd_new
    svTempRslvIn_old = svTempRslvIn_new
    svTempRslvIncd_old = svTempRslvIncd_new
    if svPrevCount == row_count_value:
        output_rows.append(
            Row(
                TRGT_SYS_CD=Target,
                SUBJ_AREA_NM=Subject,
                LAST_BAL_DT=CurrDate,
                TLRNC_CD=svTempTlrncCd_new,
                PREV_ISSUE_CRCTD_IN=svTempRslvIn_new,
                TRGT_RUN_CYC_NO=ExtrRunCycle,
                CRT_DTM=CurrDate,
                LAST_UPDT_DTM=CurrDate,
                USER_ID=UWSAcct
            )
        )

df_SubjAreaBalLoad = spark.createDataFrame(output_rows, schema_SubjAreaBalLoad)
df_SubjAreaBalLoad = df_SubjAreaBalLoad.select(
    "TRGT_SYS_CD",
    "SUBJ_AREA_NM",
    "LAST_BAL_DT",
    "TLRNC_CD",
    "PREV_ISSUE_CRCTD_IN",
    "TRGT_RUN_CYC_NO",
    "CRT_DTM",
    "LAST_UPDT_DTM",
    "USER_ID"
)

df_SubjAreaBalLoad = df_SubjAreaBalLoad.withColumn("TRGT_SYS_CD", F.rpad(F.col("TRGT_SYS_CD"), F.lit(<...>), F.lit(" ")))
df_SubjAreaBalLoad = df_SubjAreaBalLoad.withColumn("SUBJ_AREA_NM", F.rpad(F.col("SUBJ_AREA_NM"), F.lit(<...>), F.lit(" ")))
df_SubjAreaBalLoad = df_SubjAreaBalLoad.withColumn("TLRNC_CD", F.rpad(F.col("TLRNC_CD"), F.lit(<...>), F.lit(" ")))
df_SubjAreaBalLoad = df_SubjAreaBalLoad.withColumn("PREV_ISSUE_CRCTD_IN", F.rpad(F.col("PREV_ISSUE_CRCTD_IN"), F.lit(1), F.lit(" ")))
df_SubjAreaBalLoad = df_SubjAreaBalLoad.withColumn("USER_ID", F.rpad(F.col("USER_ID"), F.lit(<...>), F.lit(" ")))

spark.sql(f"DROP TABLE IF EXISTS STAGING.UwsCustSvcSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp")
df_SubjAreaBalLoad.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", "STAGING.UwsCustSvcSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp").mode("overwrite").save()

merge_sql = f"""
MERGE INTO {UWSOwner}.SUBJ_AREA_BAL_SUM AS T
USING STAGING.UwsCustSvcSubjAreaBalSumLoad_SUBJ_AREA_BAL_SUM_temp AS S
ON T.TRGT_SYS_CD=S.TRGT_SYS_CD AND T.SUBJ_AREA_NM=S.SUBJ_AREA_NM
WHEN MATCHED THEN UPDATE SET
  T.LAST_BAL_DT=S.LAST_BAL_DT,
  T.TLRNC_CD=S.TLRNC_CD,
  T.PREV_ISSUE_CRCTD_IN=S.PREV_ISSUE_CRCTD_IN,
  T.TRGT_RUN_CYC_NO=S.TRGT_RUN_CYC_NO,
  T.CRT_DTM=S.CRT_DTM,
  T.LAST_UPDT_DTM=S.LAST_UPDT_DTM,
  T.USER_ID=S.USER_ID
WHEN NOT MATCHED THEN
  INSERT (TRGT_SYS_CD,SUBJ_AREA_NM,LAST_BAL_DT,TLRNC_CD,PREV_ISSUE_CRCTD_IN,TRGT_RUN_CYC_NO,CRT_DTM,LAST_UPDT_DTM,USER_ID)
  VALUES (S.TRGT_SYS_CD,S.SUBJ_AREA_NM,S.LAST_BAL_DT,S.TLRNC_CD,S.PREV_ISSUE_CRCTD_IN,S.TRGT_RUN_CYC_NO,S.CRT_DTM,S.LAST_UPDT_DTM,S.USER_ID);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)