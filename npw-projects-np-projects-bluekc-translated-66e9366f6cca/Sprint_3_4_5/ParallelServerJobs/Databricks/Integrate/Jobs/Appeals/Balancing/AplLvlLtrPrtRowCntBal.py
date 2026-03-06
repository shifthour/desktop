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
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
ids_owner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
TrgtTable = get_widget_value('TrgtTable','')
SrcTable = get_widget_value('SrcTable','')
NotifyMessage = get_widget_value('NotifyMessage','\\"ROW COUNT BALANCING FACETS - IDS APPEAL LEVEL LETTER PRINT OUT OF TOLERANCE\\"')
uws_owner = get_widget_value('UWSOwner','')
SubjectArea = get_widget_value('SubjectArea','Appeals')
CurrentDate = get_widget_value('CurrentDate','2007-09-14 10:00:00')

ids_secret_name = get_widget_value('ids_secret_name','')
uws_secret_name = get_widget_value('uws_secret_name','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)

IDSDB = get_widget_value('IDSDB','')
IDSAcct = get_widget_value('IDSAcct','')
IDSPW = get_widget_value('IDSPW','')
UWSAcct = get_widget_value('UWSAcct','')

tol_val = 0
if ToleranceCd.isdigit():
    tol_val = int(ToleranceCd)

extract_query = (
    f"SELECT TRGT.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK,TRGT.APL_ID as TRGT_APL_ID,"
    f"SRC.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK,SRC.APL_ID as SRC_APL_ID,"
    f"TRGT.SEQ_NO as TRGT_SEQ_NO,TRGT.LTR_SEQ_NO as TRGT_LTR_SEQ_NO,"
    f"TRGT.LTR_DEST_ID as TRGT_LTR_DEST_ID,TRGT.PRT_SEQ_NO as TRGT_PRT_SEQ_NO,"
    f"SRC.SEQ_NO as SRC_SEQ_NO,SRC.LTR_SEQ_NO as SRC_LTR_SEQ_NO,"
    f"SRC.LTR_DEST_ID as SRC_LTR_DEST_ID,SRC.PRT_SEQ_NO as SRC_PRT_SEQ_NO "
    f"FROM {ids_owner}.{TrgtTable} AS TRGT "
    f"FULL OUTER JOIN {ids_owner}.{SrcTable} AS SRC "
    f"ON TRGT.SRC_SYS_CD_SK=SRC.SRC_SYS_CD_SK "
    f"AND TRGT.APL_ID = SRC.APL_ID "
    f"AND TRGT.SEQ_NO = SRC.SEQ_NO "
    f"AND TRGT.LTR_SEQ_NO = SRC.LTR_SEQ_NO "
    f"AND TRGT.LTR_DEST_ID = SRC.LTR_DEST_ID "
    f"AND TRGT.PRT_SEQ_NO = SRC.PRT_SEQ_NO "
    f"WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}"
)

df_Compare = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

w = Window.orderBy(F.lit(1))
df_missing_withRN = df_Compare.withColumn("_rownum", F.row_number().over(w))

df_transform = df_missing_withRN.alias("Missing").join(
    df_SRC_SYS_CD.alias("SRC_SYS_CD"),
    F.col("Missing.TRGT_SRC_SYS_CD_SK") == F.col("SRC_SYS_CD.CD_MPPNG_SK"),
    how="left"
)

df_Research = df_transform.filter(
    F.col("Missing.TRGT_SRC_SYS_CD_SK").isNull()
    | F.col("Missing.TRGT_APL_ID").isNull()
    | F.col("Missing.SRC_SRC_SYS_CD_SK").isNull()
    | F.col("Missing.SRC_APL_ID").isNull()
    | F.col("Missing.TRGT_SEQ_NO").isNull()
    | F.col("Missing.SRC_SEQ_NO").isNull()
    | F.col("Missing.TRGT_LTR_SEQ_NO").isNull()
    | F.col("Missing.SRC_LTR_SEQ_NO").isNull()
    | F.col("Missing.TRGT_LTR_DEST_ID").isNull()
    | F.col("Missing.SRC_LTR_DEST_ID").isNull()
    | F.col("Missing.TRGT_PRT_SEQ_NO").isNull()
    | F.col("Missing.SRC_PRT_SEQ_NO").isNull()
).select(
    F.col("Missing.TRGT_SRC_SYS_CD_SK").alias("TRGT_SRC_SYS_CD_SK"),
    F.col("Missing.TRGT_APL_ID").alias("TRGT_APL_ID"),
    F.col("Missing.SRC_SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
    F.col("Missing.SRC_APL_ID").alias("SRC_APL_ID"),
    F.col("Missing.TRGT_SEQ_NO").alias("TRGT_SEQ_NO"),
    F.col("Missing.TRGT_LTR_SEQ_NO").alias("TRGT_LTR_SEQ_NO"),
    F.col("Missing.TRGT_LTR_DEST_ID").alias("TRGT_LTR_DEST_ID"),
    F.col("Missing.TRGT_PRT_SEQ_NO").alias("TRGT_PRT_SEQ_NO"),
    F.col("Missing.SRC_SEQ_NO").alias("SRC_SEQ_NO"),
    F.col("Missing.SRC_LTR_SEQ_NO").alias("SRC_LTR_SEQ_NO"),
    F.col("Missing.SRC_LTR_DEST_ID").alias("SRC_LTR_DEST_ID"),
    F.col("Missing.SRC_PRT_SEQ_NO").alias("SRC_PRT_SEQ_NO")
)

df_Research_path = f"{adls_path}/balancing/research/{SrcTable}.{TrgtTable}.dat.{RunID}"
write_files(
    df_Research,
    df_Research_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Notify = df_missing_withRN.filter(
    (F.col("_rownum") == 1) & (F.col("_rownum") > tol_val)
).select(
    F.lit(NotifyMessage).alias("NOTIFICATION")
)

df_Notify = df_Notify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))

df_Notify_path = f"{adls_path}/balancing/notify/AppealsBalancingNotification.dat"
write_files(
    df_Notify,
    df_Notify_path,
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_ROW_CNT = df_transform.filter(F.col("_rownum") == 1).select(
    F.col("SRC_SYS_CD.TRGT_CD").alias("SRC_SYS_CD")
)

w2 = Window.orderBy(F.lit(1))
df_ROW_CNT_in_withRN = df_ROW_CNT.withColumn("_rownum", F.row_number().over(w2))

df_TransformLogic1_temp = (
    df_ROW_CNT_in_withRN
    .withColumn("svTlrnc", F.lit(tol_val))
    .withColumn("svSrcCnt", GetSrcCount(ids_owner, IDSDB, IDSAcct, IDSPW, SrcTable))
    .withColumn("svTrgtCnt", GetTrgtCount(ids_owner, IDSDB, IDSAcct, IDSPW, TrgtTable, ExtrRunCycle))
    .withColumn("Diff", F.col("svTrgtCnt") - F.col("svSrcCnt"))
    .withColumn(
        "svTlrncCd",
        F.when(F.col("Diff") == 0, F.lit("BAL"))
        .when(F.abs(F.col("Diff")) > F.col("svTlrnc"), F.lit("OUT"))
        .otherwise(F.lit("IN"))
    )
)

df_ROW_CNT_OUT = df_TransformLogic1_temp.filter(F.col("_rownum") == 1).select(
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
    space(1).alias("CRCTN_NOTE"),
    F.lit(CurrentDate).alias("CRT_DTM"),
    F.lit(CurrentDate).alias("LAST_UPDT_DTM"),
    F.lit(UWSAcct).alias("USER_ID")
)

df_TBL_BAL_SUM = df_TransformLogic1_temp.filter(F.col("_rownum") == 1).select(
    F.lit("IDS").alias("TRGT_SYS_CD"),
    F.lit(SubjectArea).alias("SUBJ_AREA_NM"),
    F.lit(TrgtTable).alias("TRGT_TBL_NM"),
    F.lit(CurrentDate).alias("BAL_DT"),
    F.col("svTlrncCd").alias("ROW_CT_TLRNC_CD"),
    F.lit("NA").alias("CLMN_SUM_TLRNC_CD"),
    F.lit("NA").alias("ROW_TO_ROW_TLRNC_CD"),
    F.lit("NA").alias("RI_TLRNC_CD"),
    F.lit("NA").alias("RELSHP_CLMN_SUM_TLRNC_CD"),
    F.lit("NA").alias("CRS_FOOT_TLRNC_CD"),
    F.lit("NA").alias("HIST_RSNBL_TLRNC_CD"),
    F.lit(ExtrRunCycle).alias("TRGT_RUN_CYC_NO"),
    F.lit(CurrentDate).alias("CRT_DTM"),
    F.lit(CurrentDate).alias("LAST_UPDT_DTM"),
    F.lit(UWSAcct).alias("USER_ID")
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.AplLvlLtrPrtRowCntBal_TBL_BAL_SUM_temp", jdbc_url_uws, jdbc_props_uws)
df_TBL_BAL_SUM.write.format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", "STAGING.AplLvlLtrPrtRowCntBal_TBL_BAL_SUM_temp") \
    .mode("overwrite") \
    .save()

merge_sql_tbl_bal_sum = f"""
MERGE INTO {uws_owner}.TBL_BAL_SUM AS T
USING STAGING.AplLvlLtrPrtRowCntBal_TBL_BAL_SUM_temp AS S
ON (1=0)
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
);
"""
execute_dml(merge_sql_tbl_bal_sum, jdbc_url_uws, jdbc_props_uws)

df_ROW_CNT_OUT_write = df_ROW_CNT_OUT.select(
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

df_ROW_CNT_OUT_path = f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat"
write_files(
    df_ROW_CNT_OUT_write,
    df_ROW_CNT_OUT_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

schema_ROW_CT = StructType([
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

df_ROW_CT_UPDATE_FILE = (
    spark.read.format("csv")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_ROW_CT)
    .load(df_ROW_CNT_OUT_path)
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.AplLvlLtrPrtRowCntBal_ROW_CT_DTL_temp", jdbc_url_uws, jdbc_props_uws)
df_ROW_CT_UPDATE_FILE.write.format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", "STAGING.AplLvlLtrPrtRowCntBal_ROW_CT_DTL_temp") \
    .mode("overwrite") \
    .save()

merge_sql_row_ct_dtl = f"""
MERGE INTO {uws_owner}.ROW_CT_DTL AS T
USING STAGING.AplLvlLtrPrtRowCntBal_ROW_CT_DTL_temp AS S
ON (1=0)
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
);
"""
execute_dml(merge_sql_row_ct_dtl, jdbc_url_uws, jdbc_props_uws)