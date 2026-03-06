# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 11/28/07 11:03:55 Batch  14577_39840 PROMOTE bckcetl ids20 dsadm bls forhs
# MAGIC ^1_1 11/28/07 10:51:03 Batch  14577_39066 INIT bckcett testIDS30 dsadm bls for hs
# MAGIC ^1_1 11/27/07 14:16:37 Batch  14576_51408 PROMOTE bckcett testIDS30 u03651 steph for Hugh
# MAGIC ^1_1 11/27/07 14:06:57 Batch  14576_50821 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                  10/15/2007        3028                              Originally Programmed                           devlIDS30                    Steph Goddard            10/18/2007

# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
TrgtTable = get_widget_value('TrgtTable','')
SrcTable = get_widget_value('SrcTable','')
NotifyMessage = get_widget_value('NotifyMessage','')
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
SubjectArea = get_widget_value('SubjectArea','')
CurrentDateParam = get_widget_value('CurrentDate','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

sql_Compare = f"""
SELECT
 TRGT.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK,
 TRGT.APL_ID as TRGT_APL_ID,
 TRGT.SEQ_NO as TRGT_SEQ_NO,
 TRGT.APL_LVL_LTR_STYLE_CD_SK as TRGT_APL_LVL_LTR_STYLE_CD_SK,
 TRGT.LTR_SEQ_NO as TRGT_LTR_SEQ_NO,
 TRGT.LTR_DEST_ID as TRGT_LTR_DEST_ID,
 SRC.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK,
 SRC.APL_ID as SRC_APL_ID,
 SRC.SEQ_NO as SRC_SEQ_NO,
 SRC.APL_LVL_LTR_STYLE_CD_SK as SRC_APL_LVL_LTR_STYLE_CD_SK,
 SRC.LTR_SEQ_NO as SRC_LTR_SEQ_NO,
 SRC.LTR_DEST_ID as SRC_LTR_DEST_ID
FROM {IDSOwner}.{TrgtTable} AS TRGT
FULL OUTER JOIN {IDSOwner}.{SrcTable} AS SRC
  ON TRGT.SRC_SYS_CD_SK=SRC.SRC_SYS_CD_SK
  AND TRGT.APL_ID=SRC.APL_ID
  AND TRGT.SEQ_NO=SRC.SEQ_NO
  AND TRGT.LTR_SEQ_NO=SRC.LTR_SEQ_NO
  AND TRGT.LTR_DEST_ID=SRC.LTR_DEST_ID
  AND TRGT.APL_LVL_LTR_STYLE_CD_SK=SRC.APL_LVL_LTR_STYLE_CD_SK
WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_Compare = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_Compare)
    .load()
)

df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

w = Window.orderBy(F.lit(1))
df_CompareWithLookup = df_Compare.alias("Missing").join(
    df_SRC_SYS_CD.alias("SRC_SYS_CD"),
    F.col("Missing.TRGT_SRC_SYS_CD_SK") == F.col("SRC_SYS_CD.CD_MPPNG_SK"),
    "left"
).withColumn("_rownum", F.row_number().over(w))

df_Research = df_CompareWithLookup.filter(
    F.col("TRGT_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_APL_ID").isNull()
    | F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("SRC_APL_ID").isNull()
    | F.col("TRGT_SEQ_NO").isNull()
    | F.col("SRC_SEQ_NO").isNull()
    | F.col("TRGT_LTR_SEQ_NO").isNull()
    | F.col("SRC_LTR_SEQ_NO").isNull()
    | F.col("TRGT_LTR_DEST_ID").isNull()
    | F.col("SRC_LTR_DEST_ID").isNull()
    | F.col("TRGT_APL_LVL_LTR_STYLE_CD_SK").isNull()
    | F.col("SRC_APL_LVL_LTR_STYLE_CD_SK").isNull()
).select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_APL_ID",
    "SRC_SRC_SYS_CD_SK",
    "SRC_APL_ID",
    "TRGT_SEQ_NO",
    "TRGT_LTR_SEQ_NO",
    "TRGT_LTR_DEST_ID",
    "TRGT_APL_LVL_LTR_STYLE_CD_SK",
    "SRC_SEQ_NO",
    "SRC_LTR_SEQ_NO",
    "SRC_LTR_DEST_ID",
    "SRC_APL_LVL_LTR_STYLE_CD_SK"
)

df_Notify = df_CompareWithLookup.filter(
    (F.col("_rownum") == 1) & (F.col("_rownum") > F.lit(ToleranceCd).cast("int"))
).select(
    F.lit(NotifyMessage).alias("NOTIFICATION")
)

df_ROW_CNT = df_CompareWithLookup.filter(F.col("_rownum") == 1).select(
    F.col("TRGT_CD").alias("SRC_SYS_CD")
)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/{SrcTable}.{TrgtTable}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_NotifyFinal = df_Notify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))
write_files(
    df_NotifyFinal,
    f"{adls_path}/balancing/notify/AppealsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_ROW_CNT1 = df_ROW_CNT
df_ROW_CNT1 = df_ROW_CNT1.withColumn("svTlrnc", F.lit(ToleranceCd).cast("int"))
df_ROW_CNT1 = df_ROW_CNT1.withColumn("svSrcCnt", GetSrcCount(F.lit(IDSOwner), F.lit("<...>"), F.lit("<...>"), F.lit("<...>"), F.lit(SrcTable)))
df_ROW_CNT1 = df_ROW_CNT1.withColumn("svTrgtCnt", GetTrgtCount(F.lit(IDSOwner), F.lit("<...>"), F.lit("<...>"), F.lit("<...>"), F.lit(TrgtTable), F.lit(ExtrRunCycle)))
df_ROW_CNT1 = df_ROW_CNT1.withColumn("Diff", F.col("svTrgtCnt") - F.col("svSrcCnt"))
df_ROW_CNT1 = df_ROW_CNT1.withColumn(
    "svTlrncCd",
    F.when(F.col("Diff") == 0, F.lit("BAL"))
     .when(F.abs(F.col("Diff")) > F.col("svTlrnc"), F.lit("OUT"))
     .otherwise(F.lit("IN"))
)
df_ROW_CNT1 = df_ROW_CNT1.withColumn("_rownum2", F.row_number().over(Window.orderBy(F.lit(1))))

df_ROW_CNT_OUT = df_ROW_CNT1.filter(F.col("_rownum2") == 1).select(
    F.lit("IDS").alias("TRGT_SYS_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(SubjectArea).alias("SUBJ_AREA_NM"),
    F.lit(TrgtTable).alias("TRGT_TBL_NM"),
    F.col("svTrgtCnt").alias("TRGT_CT"),
    F.col("svSrcCnt").alias("SRC_CT"),
    F.col("Diff").alias("DIFF_CT"),
    F.col("svTlrncCd").alias("TLRNC_CD"),
    F.col("svTlrnc").alias("TLRNC_AMT"),
    F.col("svTlrnc").alias("TLRNC_MULT_AMT"),
    F.lit("DAILY").alias("FREQ_CD"),
    F.lit(ExtrRunCycle).alias("TRGT_RUN_CYC_NO"),
    F.lit(" ").alias("CRCTN_NOTE"),
    F.lit(CurrentDateParam).alias("CRT_DTM"),
    F.lit(CurrentDateParam).alias("LAST_UPDT_DTM"),
    F.lit(UWSOwner).alias("USER_ID")
)

df_TBL_BAL_SUM = df_ROW_CNT1.filter(F.col("_rownum2") == 1).select(
    F.lit("IDS").alias("TRGT_SYS_CD"),
    F.lit(SubjectArea).alias("SUBJ_AREA_NM"),
    F.lit(TrgtTable).alias("TRGT_TBL_NM"),
    F.lit(CurrentDateParam).alias("BAL_DT"),
    F.col("svTlrncCd").alias("ROW_CT_TLRNC_CD"),
    F.lit("NA").alias("CLMN_SUM_TLRNC_CD"),
    F.lit("NA").alias("ROW_TO_ROW_TLRNC_CD"),
    F.lit("NA").alias("RI_TLRNC_CD"),
    F.lit("NA").alias("RELSHP_CLMN_SUM_TLRNC_CD"),
    F.lit("NA").alias("CRS_FOOT_TLRNC_CD"),
    F.lit("NA").alias("HIST_RSNBL_TLRNC_CD"),
    F.lit(ExtrRunCycle).alias("TRGT_RUN_CYC_NO"),
    F.lit(CurrentDateParam).alias("CRT_DTM"),
    F.lit(CurrentDateParam).alias("LAST_UPDT_DTM"),
    F.lit(UWSOwner).alias("USER_ID")
)

uws_jdbc_url, uws_jdbc_props = get_db_config(uws_secret_name)

execute_dml(f"DROP TABLE IF EXISTS STAGING.AplLvlLtrRowCntBal_TBL_BAL_SUM_temp", uws_jdbc_url, uws_jdbc_props)
df_TBL_BAL_SUM.write.format("jdbc") \
    .option("url", uws_jdbc_url) \
    .options(**uws_jdbc_props) \
    .option("dbtable", "STAGING.AplLvlLtrRowCntBal_TBL_BAL_SUM_temp") \
    .mode("append") \
    .save()

merge_sql_TBL_BAL_SUM = f"""
MERGE INTO {UWSOwner}.TBL_BAL_SUM AS T
USING STAGING.AplLvlLtrRowCntBal_TBL_BAL_SUM_temp AS S
ON 1=2
WHEN NOT MATCHED THEN
INSERT (
 TRGT_SYS_CD,
 SUBJ_AREA_NM,
 TRGT_TBL_NM,
 BAL_DT,
 ROW_CT_TLRNC_CD,
 CLMN_SUM_TLRNC_CD,
 ROW_TO_ROW_TLRNC_CD,
 RI_TLRNC_CD,
 RELSHP_CLMN_SUM_TLRNC_CD,
 CRS_FOOT_TLRNC_CD,
 HIST_RSNBL_TLRNC_CD,
 TRGT_RUN_CYC_NO,
 CRT_DTM,
 LAST_UPDT_DTM,
 USER_ID
)
VALUES (
 S.TRGT_SYS_CD,
 S.SUBJ_AREA_NM,
 S.TRGT_TBL_NM,
 S.BAL_DT,
 S.ROW_CT_TLRNC_CD,
 S.CLMN_SUM_TLRNC_CD,
 S.ROW_TO_ROW_TLRNC_CD,
 S.RI_TLRNC_CD,
 S.RELSHP_CLMN_SUM_TLRNC_CD,
 S.CRS_FOOT_TLRNC_CD,
 S.HIST_RSNBL_TLRNC_CD,
 S.TRGT_RUN_CYC_NO,
 S.CRT_DTM,
 S.LAST_UPDT_DTM,
 S.USER_ID
)
;
"""
execute_dml(merge_sql_TBL_BAL_SUM, uws_jdbc_url, uws_jdbc_props)

df_ROW_CT_DTL = df_ROW_CNT_OUT.select(
    "TRGT_SYS_CD",
    "SRC_SYS_CD",
    "SUBJ_AREA_NM",
    "TRGT_TBL_NM",
    "TRGT_CT",
    "SRC_CT",
    "DIFF_CT",
    "TLRNC_CD",
    "TLRNC_AMT",
    "TLRNC_MULT_AMT",
    "FREQ_CD",
    "TRGT_RUN_CYC_NO",
    "CRCTN_NOTE",
    "CRT_DTM",
    "LAST_UPDT_DTM",
    "USER_ID"
)

df_ROW_CT_DTL = df_ROW_CT_DTL.withColumn("TRGT_SYS_CD", F.rpad(F.col("TRGT_SYS_CD"), <...>, " ")) \
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " ")) \
    .withColumn("SUBJ_AREA_NM", F.rpad(F.col("SUBJ_AREA_NM"), <...>, " ")) \
    .withColumn("TRGT_TBL_NM", F.rpad(F.col("TRGT_TBL_NM"), <...>, " ")) \
    .withColumn("TLRNC_CD", F.rpad(F.col("TLRNC_CD"), <...>, " ")) \
    .withColumn("FREQ_CD", F.rpad(F.col("FREQ_CD"), <...>, " ")) \
    .withColumn("CRCTN_NOTE", F.rpad(F.col("CRCTN_NOTE"), <...>, " ")) \
    .withColumn("USER_ID", F.rpad(F.col("USER_ID"), <...>, " "))

write_files(
    df_ROW_CT_DTL,
    f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

schema_ROW_CT_DTL = StructType([
    StructField("TRGT_SYS_CD", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("SUBJ_AREA_NM", StringType(), False),
    StructField("TRGT_TBL_NM", StringType(), False),
    StructField("TRGT_CT", IntegerType(), False),
    StructField("SRC_CT", IntegerType(), False),
    StructField("DIFF_CT", IntegerType(), False),
    StructField("TLRNC_CD", StringType(), False),
    StructField("TLRNC_AMT", IntegerType(), False),
    StructField("TLRNC_MULT_AMT", DecimalType(), False),
    StructField("FREQ_CD", StringType(), False),
    StructField("TRGT_RUN_CYC_NO", IntegerType(), False),
    StructField("CRCTN_NOTE", StringType(), True),
    StructField("CRT_DTM", TimestampType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("USER_ID", StringType(), False)
])

df_ROW_CT_DTLIn = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("header", False)
    .option("quote", "\"")
    .schema(schema_ROW_CT_DTL)
    .load(f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat")
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.AplLvlLtrRowCntBal_ROW_CT_DTL_temp", uws_jdbc_url, uws_jdbc_props)
df_ROW_CT_DTLIn.write.format("jdbc") \
    .option("url", uws_jdbc_url) \
    .options(**uws_jdbc_props) \
    .option("dbtable", "STAGING.AplLvlLtrRowCntBal_ROW_CT_DTL_temp") \
    .mode("append") \
    .save()

merge_sql_ROW_CT_DTL = f"""
MERGE INTO {UWSOwner}.ROW_CT_DTL AS T
USING STAGING.AplLvlLtrRowCntBal_ROW_CT_DTL_temp AS S
ON 1=2
WHEN NOT MATCHED THEN
INSERT (
 TRGT_SYS_CD,
 SRC_SYS_CD,
 SUBJ_AREA_NM,
 TRGT_TBL_NM,
 TRGT_CT,
 SRC_CT,
 DIFF_CT,
 TLRNC_CD,
 TLRNC_AMT,
 TLRNC_MULT_AMT,
 FREQ_CD,
 TRGT_RUN_CYC_NO,
 CRCTN_NOTE,
 CRT_DTM,
 LAST_UPDT_DTM,
 USER_ID
)
VALUES (
 S.TRGT_SYS_CD,
 S.SRC_SYS_CD,
 S.SUBJ_AREA_NM,
 S.TRGT_TBL_NM,
 S.TRGT_CT,
 S.SRC_CT,
 S.DIFF_CT,
 S.TLRNC_CD,
 S.TLRNC_AMT,
 S.TLRNC_MULT_AMT,
 S.FREQ_CD,
 S.TRGT_RUN_CYC_NO,
 S.CRCTN_NOTE,
 S.CRT_DTM,
 S.LAST_UPDT_DTM,
 S.USER_ID
)
;
"""
execute_dml(merge_sql_ROW_CT_DTL, uws_jdbc_url, uws_jdbc_props)