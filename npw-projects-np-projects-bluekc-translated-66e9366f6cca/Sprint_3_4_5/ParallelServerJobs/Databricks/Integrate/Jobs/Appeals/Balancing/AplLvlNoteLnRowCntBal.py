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
# MAGIC Bhoomi Dasari                  10/15/2007        3028                              Originally Programmed                           devlIDS30                    Steph Goddard             10/18/2007

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit, row_number, rpad, abs as abs_, when
from pyspark.sql import Window
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
NotifyMessage = get_widget_value('NotifyMessage','\\"ROW COUNT BALANCING FACETS - IDS APPEAL LEVEL NOTE LINE OUT OF TOLERANCE\\"')
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
SubjectArea = get_widget_value('SubjectArea','Appeals')
CurrentDate = get_widget_value('CurrentDate','2007-09-14 10:00:00')

threshold_int = 0
try:
    threshold_int = int(ToleranceCd)
except:
    pass

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_Compare = (
"""SELECT TRGT.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK,
       TRGT.APL_ID as TRGT_APL_ID,
       SRC.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK,
       SRC.APL_ID as SRC_APL_ID,
       TRGT.LVL_SEQ_NO as TRGT_LVL_SEQ_NO,
       TRGT.NOTE_SEQ_NO as TRGT_NOTE_SEQ_NO,
       TRGT.NOTE_DEST_ID as TRGT_NOTE_DEST_ID,
       SRC.LVL_SEQ_NO as SRC_LVL_SEQ_NO,
       SRC.NOTE_SEQ_NO as SRC_NOTE_SEQ_NO,
       SRC.NOTE_DEST_ID as SRC_NOTE_DEST_ID,
       TRGT.LN_SEQ_NO as TRGT_LN_SEQ_NO,
       SRC.LN_SEQ_NO as SRC_LN_SEQ_NO
  FROM """ + f"{IDSOwner}.{TrgtTable}" + """ AS TRGT
       FULL OUTER JOIN """ + f"{IDSOwner}.{SrcTable}" + """ AS SRC
       ON TRGT.SRC_SYS_CD_SK=SRC.SRC_SYS_CD_SK
       AND TRGT.APL_ID = SRC.APL_ID
       AND TRGT.LVL_SEQ_NO = SRC.LVL_SEQ_NO
       AND TRGT.NOTE_SEQ_NO = SRC.NOTE_SEQ_NO
       AND TRGT.NOTE_DEST_ID = SRC.NOTE_DEST_ID
       AND TRGT.LN_SEQ_NO = SRC.LN_SEQ_NO
 WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK  >= """ + f"{ExtrRunCycle}" + """
 
""" + f"{IDSOwner}.{TrgtTable}" + """ AS TRGT
"""
)

df_Compare = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_Compare)
    .load()
)

df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

df_TransformLogic_joined = df_Compare.alias("Missing").join(
    df_SRC_SYS_CD.alias("SRC_SYS_CD"),
    col("Missing.TRGT_SRC_SYS_CD_SK") == col("SRC_SYS_CD.CD_MPPNG_SK"),
    "left"
)

window_spec = Window.orderBy(lit(1))
df_TransformLogic_with_rn = df_TransformLogic_joined.withColumn("_row_num", row_number().over(window_spec))

df_Research = df_TransformLogic_with_rn.filter(
    (col("Missing.TRGT_SRC_SYS_CD_SK").isNull()) |
    (col("Missing.TRGT_APL_ID").isNull()) |
    (col("Missing.SRC_SRC_SYS_CD_SK").isNull()) |
    (col("Missing.SRC_APL_ID").isNull()) |
    (col("Missing.TRGT_LVL_SEQ_NO").isNull()) |
    (col("Missing.SRC_LVL_SEQ_NO").isNull()) |
    (col("Missing.TRGT_NOTE_SEQ_NO").isNull()) |
    (col("Missing.SRC_NOTE_SEQ_NO").isNull()) |
    (col("Missing.TRGT_NOTE_DEST_ID").isNull()) |
    (col("Missing.SRC_NOTE_DEST_ID").isNull()) |
    (col("Missing.TRGT_LN_SEQ_NO").isNull()) |
    (col("Missing.SRC_LN_SEQ_NO").isNull())
).select(
    col("Missing.TRGT_SRC_SYS_CD_SK"),
    col("Missing.TRGT_APL_ID"),
    col("Missing.SRC_SRC_SYS_CD_SK"),
    col("Missing.SRC_APL_ID"),
    col("Missing.TRGT_LVL_SEQ_NO"),
    col("Missing.TRGT_NOTE_SEQ_NO"),
    col("Missing.TRGT_NOTE_DEST_ID"),
    col("Missing.SRC_LVL_SEQ_NO"),
    col("Missing.SRC_NOTE_SEQ_NO"),
    col("Missing.SRC_NOTE_DEST_ID"),
    col("Missing.TRGT_LN_SEQ_NO"),
    col("Missing.SRC_LN_SEQ_NO")
)

df_Notify_candidate = df_TransformLogic_with_rn.filter(
    (col("_row_num") == 1) & (col("_row_num") > threshold_int)
)

df_Notify = df_Notify_candidate.select(
    rpad(lit(NotifyMessage), 70, " ").alias("NOTIFICATION")
)

df_ROW_CNT_candidate = df_TransformLogic_with_rn.filter(col("_row_num") == 1)

df_ROW_CNT = df_ROW_CNT_candidate.select(
    col("SRC_SYS_CD.TRGT_CD").alias("SRC_SYS_CD")
)

write_files(
    df_Research.select(
        "TRGT_SRC_SYS_CD_SK",
        "TRGT_APL_ID",
        "SRC_SRC_SYS_CD_SK",
        "SRC_APL_ID",
        "TRGT_LVL_SEQ_NO",
        "TRGT_NOTE_SEQ_NO",
        "TRGT_NOTE_DEST_ID",
        "SRC_LVL_SEQ_NO",
        "SRC_NOTE_SEQ_NO",
        "SRC_NOTE_DEST_ID",
        "TRGT_LN_SEQ_NO",
        "SRC_LN_SEQ_NO"
    ),
    f"{adls_path}/balancing/research/{SrcTable}.{TrgtTable}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_Notify.select(
        rpad(col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
    ),
    f"{adls_path}/balancing/notify/AppealsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

svTlrnc = threshold_int
svSrcCnt = GetSrcCount(IDSOwner, <...>, <...>, <...>, SrcTable)
svTrgtCnt = GetTrgtCount(IDSOwner, <...>, <...>, <...>, TrgtTable, ExtrRunCycle)
DiffVal = svTrgtCnt - svSrcCnt
svTlrncCdVal = "BAL" if DiffVal == 0 else ("OUT" if abs(DiffVal) > svTlrnc else "IN")

window_spec_tl1 = Window.orderBy(lit(1))
df_ROW_CNT_with_rn_tl1 = df_ROW_CNT.withColumn("_row_num", row_number().over(window_spec_tl1))

df_ROW_CNT_tl1 = df_ROW_CNT_with_rn_tl1.filter(col("_row_num") == 1)

df_ROW_CNT_out = df_ROW_CNT_tl1.select(
    rpad(lit("IDS"), <...>, " ").alias("TRGT_SYS_CD"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(lit(SubjectArea), <...>, " ").alias("SUBJ_AREA_NM"),
    rpad(lit(TrgtTable), <...>, " ").alias("TRGT_TBL_NM"),
    lit(svTrgtCnt).alias("TRGT_CT"),
    lit(svSrcCnt).alias("SRC_CT"),
    lit(DiffVal).alias("DIFF_CT"),
    rpad(lit(svTlrncCdVal), <...>, " ").alias("TLRNC_CD"),
    lit(svTlrnc).alias("TLRNC_AMT"),
    lit(svTlrnc).alias("TLRNC_MULT_AMT"),
    rpad(lit("DAILY"), <...>, " ").alias("FREQ_CD"),
    lit(ExtrRunCycle).cast("int").alias("TRGT_RUN_CYC_NO"),
    rpad(lit(" "), <...>, " ").alias("CRCTN_NOTE"),
    lit(CurrentDate).cast("timestamp").alias("CRT_DTM"),
    lit(CurrentDate).cast("timestamp").alias("LAST_UPDT_DTM"),
    rpad(lit("<...>"), <...>, " ").alias("USER_ID")
)

df_TBL_BAL_SUM = df_ROW_CNT_tl1.select(
    rpad(lit("IDS"), <...>, " ").alias("TRGT_SYS_CD"),
    rpad(lit(SubjectArea), <...>, " ").alias("SUBJ_AREA_NM"),
    rpad(lit(TrgtTable), <...>, " ").alias("TRGT_TBL_NM"),
    lit(CurrentDate).cast("timestamp").alias("BAL_DT"),
    rpad(lit(svTlrncCdVal), <...>, " ").alias("ROW_CT_TLRNC_CD"),
    rpad(lit("NA"), <...>, " ").alias("CLMN_SUM_TLRNC_CD"),
    rpad(lit("NA"), <...>, " ").alias("ROW_TO_ROW_TLRNC_CD"),
    rpad(lit("NA"), <...>, " ").alias("RI_TLRNC_CD"),
    rpad(lit("NA"), <...>, " ").alias("RELSHP_CLMN_SUM_TLRNC_CD"),
    rpad(lit("NA"), <...>, " ").alias("CRS_FOOT_TLRNC_CD"),
    rpad(lit("NA"), <...>, " ").alias("HIST_RSNBL_TLRNC_CD"),
    lit(ExtrRunCycle).cast("int").alias("TRGT_RUN_CYC_NO"),
    lit(CurrentDate).cast("timestamp").alias("CRT_DTM"),
    lit(CurrentDate).cast("timestamp").alias("LAST_UPDT_DTM"),
    rpad(lit("<...>"), <...>, " ").alias("USER_ID")
)

write_files(
    df_ROW_CNT_out.select(
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
    ),
    f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_ROW_CT_DTL = df_ROW_CNT_out.select(
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

jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)

spark.sql(f"DROP TABLE IF EXISTS STAGING.AplLvlNoteLnRowCntBal_TBL_BAL_SUM_temp")
(
    df_TBL_BAL_SUM
    .write
    .format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("dbtable", "STAGING.AplLvlNoteLnRowCntBal_TBL_BAL_SUM_temp")
    .mode("overwrite")
    .save()
)

merge_sql_tbl_bal_sum = (
"MERGE INTO " + f"{UWSOwner}.TBL_BAL_SUM" + " AS T "
"USING STAGING.AplLvlNoteLnRowCntBal_TBL_BAL_SUM_temp AS S "
"ON 1=0 "
"WHEN NOT MATCHED THEN INSERT "
"(TRGT_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, BAL_DT, ROW_CT_TLRNC_CD, CLMN_SUM_TLRNC_CD, ROW_TO_ROW_TLRNC_CD, RI_TLRNC_CD, RELSHP_CLMN_SUM_TLRNC_CD, CRS_FOOT_TLRNC_CD, HIST_RSNBL_TLRNC_CD, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID) "
"VALUES "
"(S.TRGT_SYS_CD, S.SUBJ_AREA_NM, S.TRGT_TBL_NM, S.BAL_DT, S.ROW_CT_TLRNC_CD, S.CLMN_SUM_TLRNC_CD, S.ROW_TO_ROW_TLRNC_CD, S.RI_TLRNC_CD, S.RELSHP_CLMN_SUM_TLRNC_CD, S.CRS_FOOT_TLRNC_CD, S.HIST_RSNBL_TLRNC_CD, S.TRGT_RUN_CYC_NO, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID);"
)

execute_dml(merge_sql_tbl_bal_sum, jdbc_url_uws, jdbc_props_uws)

spark.sql(f"DROP TABLE IF EXISTS STAGING.AplLvlNoteLnRowCntBal_ROW_CT_DTL_temp")
(
    df_ROW_CT_DTL
    .write
    .format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("dbtable", "STAGING.AplLvlNoteLnRowCntBal_ROW_CT_DTL_temp")
    .mode("overwrite")
    .save()
)

merge_sql_row_ct_dtl = (
"MERGE INTO " + f"{UWSOwner}.ROW_CT_DTL" + " AS T "
"USING STAGING.AplLvlNoteLnRowCntBal_ROW_CT_DTL_temp AS S "
"ON 1=0 "
"WHEN NOT MATCHED THEN INSERT "
"(TRGT_SYS_CD, SRC_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, TRGT_CT, SRC_CT, DIFF_CT, TLRNC_CD, TLRNC_AMT, TLRNC_MULT_AMT, FREQ_CD, TRGT_RUN_CYC_NO, CRCTN_NOTE, CRT_DTM, LAST_UPDT_DTM, USER_ID) "
"VALUES "
"(S.TRGT_SYS_CD, S.SRC_SYS_CD, S.SUBJ_AREA_NM, S.TRGT_TBL_NM, S.TRGT_CT, S.SRC_CT, S.DIFF_CT, S.TLRNC_CD, S.TLRNC_AMT, S.TLRNC_MULT_AMT, S.FREQ_CD, S.TRGT_RUN_CYC_NO, S.CRCTN_NOTE, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID);"
)

execute_dml(merge_sql_row_ct_dtl, jdbc_url_uws, jdbc_props_uws)