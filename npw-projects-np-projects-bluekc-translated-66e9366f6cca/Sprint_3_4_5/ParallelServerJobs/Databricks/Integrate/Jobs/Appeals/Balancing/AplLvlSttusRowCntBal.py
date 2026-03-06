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
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DecimalType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
TrgtTable = get_widget_value('TrgtTable','')
SrcTable = get_widget_value('SrcTable','')
NotifyMessage = get_widget_value('NotifyMessage','\\"ROW COUNT BALANCING FACETS - IDS APPEAL LEVEL STATUS OUT OF TOLERANCE\\"')
SubjectArea = get_widget_value('SubjectArea','Appeals')
CurrentDate = get_widget_value('CurrentDate','2007-09-14 10:00:00')
UWSAcct = get_widget_value('UWSAcct','')  # Added to satisfy references to $UWSAcct

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
ExtrRunCycleInt = int(ExtrRunCycle) if ExtrRunCycle else 0
extract_query_Compare = f"""SELECT TRGT.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK,
TRGT.APL_ID as TRGT_APL_ID,
SRC.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK,
SRC.APL_ID as SRC_APL_ID,
TRGT.SEQ_NO as TRGT_SEQ_NO,
TRGT.APL_LVL_SEQ_NO as TRGT_APL_LVL_SEQ_NO,
SRC.SEQ_NO as SRC_SEQ_NO,
SRC.APL_LVL_SEQ_NO as SRC_APL_LVL_SEQ_NO
FROM {IDSOwner}.{TrgtTable} AS TRGT
FULL OUTER JOIN {IDSOwner}.{SrcTable} AS SRC
ON TRGT.SRC_SYS_CD_SK=SRC.SRC_SYS_CD_SK
AND TRGT.APL_ID = SRC.APL_ID
AND TRGT.SEQ_NO = SRC.SEQ_NO
AND TRGT.APL_LVL_SEQ_NO = SRC.APL_LVL_SEQ_NO
WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK  >= {ExtrRunCycleInt}

{IDSOwner}.{TrgtTable} AS TRGT
"""
df_Compare = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_Compare)
    .load()
)

df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

w0 = Window.orderBy(F.lit(1))
df_joined = (
    df_Compare.alias("Missing")
    .join(df_SRC_SYS_CD.alias("SRC_SYS_CD"), F.col("Missing.TRGT_SRC_SYS_CD_SK") == F.col("SRC_SYS_CD.CD_MPPNG_SK"), "left")
    .withColumn("INROWNUM", F.row_number().over(w0))
)

df_Research = df_joined.filter(
    F.col("TRGT_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_APL_ID").isNull()
    | F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("SRC_APL_ID").isNull()
    | F.col("TRGT_SEQ_NO").isNull()
    | F.col("SRC_SEQ_NO").isNull()
    | F.col("TRGT_APL_LVL_SEQ_NO").isNull()
    | F.col("SRC_APL_LVL_SEQ_NO").isNull()
).select(
    F.col("TRGT_SRC_SYS_CD_SK"),
    F.col("TRGT_APL_ID"),
    F.col("SRC_SRC_SYS_CD_SK"),
    F.col("SRC_APL_ID"),
    F.col("TRGT_SEQ_NO"),
    F.col("SRC_SEQ_NO"),
    F.col("TRGT_APL_LVL_SEQ_NO"),
    F.col("SRC_APL_LVL_SEQ_NO")
)

df_Notify = df_joined.filter(
    (F.col("INROWNUM") == 1) & (F.col("INROWNUM") > F.lit(int(ToleranceCd) if ToleranceCd else 0))
).select(
    F.lit(NotifyMessage).alias("NOTIFICATION")
)

df_Notify = df_Notify.select(
    F.rpad(F.col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)

df_ROW_CNT = df_joined.filter(F.col("INROWNUM") == 1).select(
    F.col("SRC_SYS_CD.TRGT_CD").alias("SRC_SYS_CD")
)

df_ResearchFile = df_Research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_APL_ID",
    "SRC_SRC_SYS_CD_SK",
    "SRC_APL_ID",
    "TRGT_SEQ_NO",
    "SRC_SEQ_NO",
    "TRGT_APL_LVL_SEQ_NO",
    "SRC_APL_LVL_SEQ_NO"
)
write_files(
    df_ResearchFile,
    f"{adls_path}/balancing/research/{SrcTable}.{TrgtTable}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/AppealsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

svTlrnc = ToleranceCd
svSrcCnt = GetSrcCount(IDSOwner, <...>, <...>, <...>, SrcTable)
svTrgtCnt = GetTrgtCount(IDSOwner, <...>, <...>, <...>, TrgtTable, ExtrRunCycle)
Diff = svTrgtCnt - svSrcCnt
if Diff == 0:
    tmp_cd = 'BAL'
elif abs(Diff) > int(svTlrnc) if svTlrnc else 0:
    tmp_cd = 'OUT'
else:
    tmp_cd = 'IN'
svTlrncCd = tmp_cd

w1 = Window.orderBy(F.lit(1))
df_rowcnt_staged = df_ROW_CNT.withColumn("INROWNUM", F.row_number().over(w1))

df_ROW_CNT_OUT = df_rowcnt_staged.filter(F.col("INROWNUM") == 1).select(
    F.lit("IDS").alias("TRGT_SYS_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(SubjectArea).alias("SUBJ_AREA_NM"),
    F.lit(TrgtTable).alias("TRGT_TBL_NM"),
    F.lit(svTrgtCnt).alias("TRGT_CT"),
    F.lit(svSrcCnt).alias("SRC_CT"),
    F.lit(Diff).alias("DIFF_CT"),
    F.lit(svTlrncCd).alias("TLRNC_CD"),
    F.lit(svTlrnc).cast("int").alias("TLRNC_AMT"),
    F.lit(svTlrnc).cast("int").alias("TLRNC_MULT_AMT"),
    F.lit("DAILY").alias("FREQ_CD"),
    F.lit(ExtrRunCycle).cast("int").alias("TRGT_RUN_CYC_NO"),
    F.lit(space(1)).alias("CRCTN_NOTE"),
    F.lit(CurrentDate).alias("CRT_DTM"),
    F.lit(CurrentDate).alias("LAST_UPDT_DTM"),
    F.lit(UWSAcct).alias("USER_ID")
)

df_TBL_BAL_SUM = df_rowcnt_staged.filter(F.col("INROWNUM") == 1).select(
    F.lit("IDS").alias("TRGT_SYS_CD"),
    F.lit(SubjectArea).alias("SUBJ_AREA_NM"),
    F.lit(TrgtTable).alias("TRGT_TBL_NM"),
    F.lit(CurrentDate).alias("BAL_DT"),
    F.lit(svTlrncCd).alias("ROW_CT_TLRNC_CD"),
    F.lit("NA").alias("CLMN_SUM_TLRNC_CD"),
    F.lit("NA").alias("ROW_TO_ROW_TLRNC_CD"),
    F.lit("NA").alias("RI_TLRNC_CD"),
    F.lit("NA").alias("RELSHP_CLMN_SUM_TLRNC_CD"),
    F.lit("NA").alias("CRS_FOOT_TLRNC_CD"),
    F.lit("NA").alias("HIST_RSNBL_TLRNC_CD"),
    F.lit(ExtrRunCycle).cast("int").alias("TRGT_RUN_CYC_NO"),
    F.lit(CurrentDate).alias("CRT_DTM"),
    F.lit(CurrentDate).alias("LAST_UPDT_DTM"),
    F.lit(UWSAcct).alias("USER_ID")
)

df_ROW_CT_UPDATE_FILE = df_ROW_CNT_OUT.select(
    F.rpad(F.col("TRGT_SYS_CD"), 255, " ").alias("TRGT_SYS_CD"),
    F.rpad(F.col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("SUBJ_AREA_NM"), 255, " ").alias("SUBJ_AREA_NM"),
    F.rpad(F.col("TRGT_TBL_NM"), 255, " ").alias("TRGT_TBL_NM"),
    F.col("TRGT_CT").cast(IntegerType()).alias("TRGT_CT"),
    F.col("SRC_CT").cast(IntegerType()).alias("SRC_CT"),
    F.col("DIFF_CT").cast(IntegerType()).alias("DIFF_CT"),
    F.rpad(F.col("TLRNC_CD"), 255, " ").alias("TLRNC_CD"),
    F.col("TLRNC_AMT").cast(IntegerType()).alias("TLRNC_AMT"),
    F.col("TLRNC_MULT_AMT").cast(DecimalType(10, 0)).alias("TLRNC_MULT_AMT"),
    F.rpad(F.col("FREQ_CD"), 255, " ").alias("FREQ_CD"),
    F.col("TRGT_RUN_CYC_NO").cast(IntegerType()).alias("TRGT_RUN_CYC_NO"),
    F.rpad(F.col("CRCTN_NOTE"), 255, " ").alias("CRCTN_NOTE"),
    F.col("CRT_DTM").cast(TimestampType()).alias("CRT_DTM"),
    F.col("LAST_UPDT_DTM").cast(TimestampType()).alias("LAST_UPDT_DTM"),
    F.rpad(F.col("USER_ID"), 255, " ").alias("USER_ID")
)
write_files(
    df_ROW_CT_UPDATE_FILE,
    f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

row_ct_schema = StructType([
    StructField("TRGT_SYS_CD", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("SUBJ_AREA_NM", StringType(), False),
    StructField("TRGT_TBL_NM", StringType(), False),
    StructField("TRGT_CT", IntegerType(), False),
    StructField("SRC_CT", IntegerType(), False),
    StructField("DIFF_CT", IntegerType(), False),
    StructField("TLRNC_CD", StringType(), False),
    StructField("TLRNC_AMT", IntegerType(), False),
    StructField("TLRNC_MULT_AMT", DecimalType(10, 0), False),
    StructField("FREQ_CD", StringType(), False),
    StructField("TRGT_RUN_CYC_NO", IntegerType(), False),
    StructField("CRCTN_NOTE", StringType(), True),
    StructField("CRT_DTM", TimestampType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("USER_ID", StringType(), False)
])
df_ROW_CT_UPDATE_FILE_read = (
    spark.read
    .option("sep", ",")
    .option("quote", "\"")
    .schema(row_ct_schema)
    .csv(f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat")
)

df_ROW_CT_DTL = df_ROW_CT_UPDATE_FILE_read

jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)

execute_dml(f"DROP TABLE IF EXISTS STAGING.AplLvlSttusRowCntBal_TBL_BAL_SUM_temp", jdbc_url_uws, jdbc_props_uws)
df_TBL_BAL_SUM.write \
    .format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", "STAGING.AplLvlSttusRowCntBal_TBL_BAL_SUM_temp") \
    .mode("overwrite") \
    .save()
merge_sql_tbl_bal_sum = f"""
MERGE {UWSOwner}.TBL_BAL_SUM AS T
USING STAGING.AplLvlSttusRowCntBal_TBL_BAL_SUM_temp AS S
ON 1=0
WHEN NOT MATCHED THEN
  INSERT (TRGT_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, BAL_DT, ROW_CT_TLRNC_CD, CLMN_SUM_TLRNC_CD, ROW_TO_ROW_TLRNC_CD, RI_TLRNC_CD, RELSHP_CLMN_SUM_TLRNC_CD, CRS_FOOT_TLRNC_CD, HIST_RSNBL_TLRNC_CD, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID)
  VALUES (S.TRGT_SYS_CD, S.SUBJ_AREA_NM, S.TRGT_TBL_NM, S.BAL_DT, S.ROW_CT_TLRNC_CD, S.CLMN_SUM_TLRNC_CD, S.ROW_TO_ROW_TLRNC_CD, S.RI_TLRNC_CD, S.RELSHP_CLMN_SUM_TLRNC_CD, S.CRS_FOOT_TLRNC_CD, S.HIST_RSNBL_TLRNC_CD, S.TRGT_RUN_CYC_NO, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID);
"""
execute_dml(merge_sql_tbl_bal_sum, jdbc_url_uws, jdbc_props_uws)

execute_dml(f"DROP TABLE IF EXISTS STAGING.AplLvlSttusRowCntBal_ROW_CT_DTL_temp", jdbc_url_uws, jdbc_props_uws)
df_ROW_CT_DTL.write \
    .format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", "STAGING.AplLvlSttusRowCntBal_ROW_CT_DTL_temp") \
    .mode("overwrite") \
    .save()
merge_sql_row_ct_dtl = f"""
MERGE {UWSOwner}.ROW_CT_DTL AS T
USING STAGING.AplLvlSttusRowCntBal_ROW_CT_DTL_temp AS S
ON 1=0
WHEN NOT MATCHED THEN
  INSERT (TRGT_SYS_CD, SRC_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, TRGT_CT, SRC_CT, DIFF_CT, TLRNC_CD, TLRNC_AMT, TLRNC_MULT_AMT, FREQ_CD, TRGT_RUN_CYC_NO, CRCTN_NOTE, CRT_DTM, LAST_UPDT_DTM, USER_ID)
  VALUES (S.TRGT_SYS_CD, S.SRC_SYS_CD, S.SUBJ_AREA_NM, S.TRGT_TBL_NM, S.TRGT_CT, S.SRC_CT, S.DIFF_CT, S.TLRNC_CD, S.TLRNC_AMT, S.TLRNC_MULT_AMT, S.FREQ_CD, S.TRGT_RUN_CYC_NO, S.CRCTN_NOTE, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID);
"""
execute_dml(merge_sql_row_ct_dtl, jdbc_url_uws, jdbc_props_uws)