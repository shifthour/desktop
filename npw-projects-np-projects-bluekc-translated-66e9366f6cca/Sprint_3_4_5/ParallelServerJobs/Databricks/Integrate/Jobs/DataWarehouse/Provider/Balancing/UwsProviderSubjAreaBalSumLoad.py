# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:     Both FctsProvBalCntl and DeaProvBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job loads the SUBJECT_AREA_BAL_SUM table after the balancing control job is done for a given Subject Area              
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               05/24/2007          3264                              Originally Programmed                           devlIDS30                Steph Goddard             9/6/07             
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Pooja Sunkara                   09/12/2014         5345                            Rewrite in  parallel                                    IntegrateWrhsDevl    Kalyan Neelam              2015-01-07


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
Target = get_widget_value('Target','')
Subject = get_widget_value('Subject','')
CurrDate = get_widget_value('CurrDate','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
UWSAcct = get_widget_value('UWSAcct','')

jdbc_url, jdbc_props = get_db_config(uws_secret_name)

sql_UWS_TBL_BAL_SUM_In = f"""
SELECT
  TBL_BAL_SUM.TRGT_SYS_CD,
  TBL_BAL_SUM.SUBJ_AREA_NM,
  TBL_BAL_SUM.BAL_DT,
  TBL_BAL_SUM.ROW_CT_TLRNC_CD
FROM
  {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM
WHERE
  TBL_BAL_SUM.TRGT_SYS_CD = '{Target}'
  AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}'
ORDER BY
  TBL_BAL_SUM.BAL_DT ASC
"""

df_UWS_TBL_BAL_SUM_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_UWS_TBL_BAL_SUM_In)
    .load()
)

sql_UWS_TBL_BAL_SUM_Ref = f"""
SELECT
  TBL_BAL_SUM.TRGT_SYS_CD,
  TBL_BAL_SUM.SUBJ_AREA_NM,
  COUNT(*) AS ROW_COUNT
FROM
  {UWSOwner}.TBL_BAL_SUM TBL_BAL_SUM
WHERE
  TBL_BAL_SUM.TRGT_SYS_CD = '{Target}'
  AND TBL_BAL_SUM.SUBJ_AREA_NM = '{Subject}'
GROUP BY
  TBL_BAL_SUM.TRGT_SYS_CD,
  TBL_BAL_SUM.SUBJ_AREA_NM
"""

df_UWS_TBL_BAL_SUM_Ref = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_UWS_TBL_BAL_SUM_Ref)
    .load()
)

df_Lkup = (
    df_UWS_TBL_BAL_SUM_In.alias("lnk_UwsProviderSubjAreaBalSumLoad_In")
    .join(
        df_UWS_TBL_BAL_SUM_Ref.alias("ref_balSum"),
        [
            F.col("lnk_UwsProviderSubjAreaBalSumLoad_In.TRGT_SYS_CD")
            == F.col("ref_balSum.TRGT_SYS_CD"),
            F.col("lnk_UwsProviderSubjAreaBalSumLoad_In.SUBJ_AREA_NM")
            == F.col("ref_balSum.SUBJ_AREA_NM"),
        ],
        "left",
    )
)

df_LkupOut = df_Lkup.select(
    F.col("lnk_UwsProviderSubjAreaBalSumLoad_In.TRGT_SYS_CD").alias("TRGT_SYS_CD"),
    F.col("lnk_UwsProviderSubjAreaBalSumLoad_In.SUBJ_AREA_NM").alias("SUBJ_AREA_NM"),
    F.col("lnk_UwsProviderSubjAreaBalSumLoad_In.BAL_DT").alias("BAL_DT"),
    F.col("lnk_UwsProviderSubjAreaBalSumLoad_In.ROW_CT_TLRNC_CD").alias("ROW_CT_TLRNC_CD"),
    F.col("ref_balSum.ROW_COUNT").alias("ROW_COUNT")
)

df_LkupOut_coalesced = df_LkupOut.coalesce(1).orderBy("BAL_DT")
df_collect = df_LkupOut_coalesced.collect()

svPrevCount = 0
svTempTlrncCd = ""
svTempRslvIn = ""
svTempRslvIncd = ""

selectedRow = None

for row in df_collect:
    if row.BAL_DT is None:
        dt10 = ""
    else:
        dt10 = str(row.BAL_DT)[0:10]

    svCurrTlrncCd = row.ROW_CT_TLRNC_CD
    svCurrCount = svPrevCount + 1

    if dt10 != CurrDate:
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

    if dt10 == CurrDate:
        new_svTempRslvIn = "Y"
    else:
        if svCurrTlrncCd == "BAL" and svTempRslvIncd != "OUT":
            new_svTempRslvIn = "Y"
        elif svCurrTlrncCd == "IN" and svTempRslvIncd != "OUT":
            new_svTempRslvIn = "Y"
        else:
            new_svTempRslvIn = "N"

    if dt10 == CurrDate:
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

    svTempTlrncCd = new_svTempTlrncCd
    svTempRslvIn = new_svTempRslvIn
    svTempRslvIncd = new_svTempRslvIncd
    svPrevCount = svCurrCount

    if svPrevCount == row.ROW_COUNT:
        selectedRow = (
            Target,
            Subject,
            CurrDate,
            svTempTlrncCd,
            svTempRslvIn,
            ExtrRunCycle,
            CurrDate,
            CurrDate,
            UWSAcct,
        )
        break

schema = StructType(
    [
        StructField("TRGT_SYS_CD", StringType(), True),
        StructField("SUBJ_AREA_NM", StringType(), True),
        StructField("LAST_BAL_DT", StringType(), True),
        StructField("TLRNC_CD", StringType(), True),
        StructField("PREV_ISSUE_CRCTD_IN", StringType(), True),
        StructField("TRGT_RUN_CYC_NO", StringType(), True),
        StructField("CRT_DTM", StringType(), True),
        StructField("LAST_UPDT_DTM", StringType(), True),
        StructField("USER_ID", StringType(), True),
    ]
)

rdd_final = spark.sparkContext.emptyRDD()

if selectedRow is not None:
    rdd_final = spark.sparkContext.parallelize([selectedRow])

df_enriched = spark.createDataFrame(rdd_final, schema)

df_enriched = df_enriched.withColumn("LAST_BAL_DT", F.to_timestamp(F.col("LAST_BAL_DT"), "yyyy-MM-dd"))
df_enriched = df_enriched.withColumn("CRT_DTM", F.to_timestamp(F.col("CRT_DTM"), "yyyy-MM-dd"))
df_enriched = df_enriched.withColumn("LAST_UPDT_DTM", F.to_timestamp(F.col("LAST_UPDT_DTM"), "yyyy-MM-dd"))
df_enriched = df_enriched.withColumn(
    "PREV_ISSUE_CRCTD_IN",
    F.rpad(F.col("PREV_ISSUE_CRCTD_IN"), 1, " ")
)

temp_table = "STAGING.UwsProviderSubjAreaBalSumLoad_UWS_SUBJ_AREA_BAL_SUM_Out_temp"

drop_sql = f"DROP TABLE IF EXISTS {temp_table}"
execute_dml(drop_sql, jdbc_url, jdbc_props)

df_enriched.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", temp_table).mode("overwrite").save()

merge_sql = f"""
MERGE INTO {UWSOwner}.SUBJ_AREA_BAL_SUM AS T
USING {temp_table} AS S
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