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
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# All required widgets/parameters (including database secret parameters)
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
CurrentDate_Prm = get_widget_value('CurrentDate','')

# 1) Compare (DB2Connector) reading from IDS database
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT 
  TRGT.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK,
  TRGT.APL_ID as TRGT_APL_ID,
  SRC.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK,
  SRC.APL_ID as SRC_APL_ID,
  TRGT.LVL_SEQ_NO as TRGT_LVL_SEQ_NO,
  TRGT.NOTE_SEQ_NO as TRGT_NOTE_SEQ_NO,
  TRGT.NOTE_DEST_ID as TRGT_NOTE_DEST_ID,
  SRC.LVL_SEQ_NO as SRC_LVL_SEQ_NO,
  SRC.NOTE_SEQ_NO as SRC_NOTE_SEQ_NO,
  SRC.NOTE_DEST_ID as SRC_NOTE_DEST_ID
FROM {IDSOwner}.{TrgtTable} AS TRGT
FULL OUTER JOIN {IDSOwner}.{SrcTable} AS SRC
   ON TRGT.SRC_SYS_CD_SK=SRC.SRC_SYS_CD_SK
  AND TRGT.APL_ID=SRC.APL_ID
  AND TRGT.LVL_SEQ_NO=SRC.LVL_SEQ_NO
  AND TRGT.NOTE_SEQ_NO=SRC.NOTE_SEQ_NO
  AND TRGT.NOTE_DEST_ID=SRC.NOTE_DEST_ID
WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""
df_Compare = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# 2) SRC_SYS_CD (CHashedFileStage) - Scenario C => read from parquet
df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

# 3) TransformLogic (CTransformerStage)
#    Join (left) on Missing.TRGT_SRC_SYS_CD_SK = SRC_SYS_CD.CD_MPPNG_SK
df_TransformLogic = df_Compare.alias("Missing").join(
    df_SRC_SYS_CD.alias("SRC_SYS_CD"),
    F.col("Missing.TRGT_SRC_SYS_CD_SK") == F.col("SRC_SYS_CD.CD_MPPNG_SK"),
    "left"
)

# We add a row number for constraints using @INROWNUM
df_TransformLogic_temp = df_TransformLogic.withColumn(
    "ds_row_num",
    F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
)

# Research link constraint:
df_Research = df_TransformLogic_temp.filter(
    F.col("Missing.TRGT_SRC_SYS_CD_SK").isNull()
    | F.col("Missing.TRGT_APL_ID").isNull()
    | F.col("Missing.SRC_SRC_SYS_CD_SK").isNull()
    | F.col("Missing.SRC_APL_ID").isNull()
    | F.col("Missing.TRGT_LVL_SEQ_NO").isNull()
    | F.col("Missing.SRC_LVL_SEQ_NO").isNull()
    | F.col("Missing.TRGT_NOTE_SEQ_NO").isNull()
    | F.col("Missing.SRC_NOTE_SEQ_NO").isNull()
    | F.col("Missing.TRGT_NOTE_DEST_ID").isNull()
    | F.col("Missing.SRC_NOTE_DEST_ID").isNull()
).select(
    F.col("Missing.TRGT_SRC_SYS_CD_SK").alias("TRGT_SRC_SYS_CD_SK"),
    F.col("Missing.TRGT_APL_ID").alias("TRGT_APL_ID"),
    F.col("Missing.SRC_SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
    F.col("Missing.SRC_APL_ID").alias("SRC_APL_ID"),
    F.col("Missing.TRGT_LVL_SEQ_NO").alias("TRGT_LVL_SEQ_NO"),
    F.col("Missing.TRGT_NOTE_SEQ_NO").alias("TRGT_NOTE_SEQ_NO"),
    F.col("Missing.TRGT_NOTE_DEST_ID").alias("TRGT_NOTE_DEST_ID"),
    F.col("Missing.SRC_LVL_SEQ_NO").alias("SRC_LVL_SEQ_NO"),
    F.col("Missing.SRC_NOTE_SEQ_NO").alias("SRC_NOTE_SEQ_NO"),
    F.col("Missing.SRC_NOTE_DEST_ID").alias("SRC_NOTE_DEST_ID")
)

# Notify link constraint: @INROWNUM = 1 AND @INROWNUM > ToleranceCd
# Convert ToleranceCd to int for comparison
tolerance_val = 0
try:
    tolerance_val = int(ToleranceCd)
except:
    pass
df_Notify = df_TransformLogic_temp.filter(
    (F.col("ds_row_num") == 1) & (F.col("ds_row_num") > tolerance_val)
).select(
    # NOTIFICATION is char(70)
    rpad(F.lit(NotifyMessage), 70, " ").alias("NOTIFICATION")
)

# ROW_CNT link constraint: @INROWNUM = 1
df_RowCnt = df_TransformLogic_temp.filter(
    F.col("ds_row_num") == 1
).select(
    F.col("SRC_SYS_CD.TRGT_CD").alias("SRC_SYS_CD")
)

# 4) ResearchFile (CSeqFileStage)
# Write df_Research => balancing/research/<SrcTable>.<TrgtTable>.dat.<RunID>
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

# 5) ErrorNotificationFile (CSeqFileStage)
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

# 6) TransformLogic1 (CTransformerStage)
#    Stage variables to compute Tolerance, Source count, Target count, difference, code
#    We assume GetSrcCount, GetTrgtCount are already defined UDFs/routines.
df_TransformLogic1 = (
    df_RowCnt
    .withColumn("svTlrnc", F.lit(ToleranceCd))
    .withColumn("svSrcCnt", GetSrcCount(IDSOwner, <...>, <...>, <...>, SrcTable))
    .withColumn("svTrgtCnt", GetTrgtCount(IDSOwner, <...>, <...>, <...>, TrgtTable, ExtrRunCycle))
    .withColumn("Diff", F.col("svTrgtCnt") - F.col("svSrcCnt"))
    .withColumn(
        "svTlrncCd",
        F.when(F.col("Diff") == 0, F.lit("BAL"))
         .otherwise(
             F.when(F.abs(F.col("Diff")) > F.col("svTlrnc"), F.lit("OUT"))
              .otherwise(F.lit("IN"))
         )
    )
)

# Two output links from TransformLogic1:

# a) ROW_CNT_OUT => ROW_CT_UPDATE_FILE
#    Constraint: @INROWNUM = 1 (we only have one row at this point)
#    Map columns
#    Some columns are varchar/char, but no length provided => use 255
#    A few are numeric => no rpad
df_ROW_CNT_OUT = df_TransformLogic1.select(
    rpad(F.lit("IDS"), 255, " ").alias("TRGT_SYS_CD"),
    rpad(F.col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    rpad(F.lit(SubjectArea), 255, " ").alias("SUBJ_AREA_NM"),
    rpad(F.lit(TrgtTable), 255, " ").alias("TRGT_TBL_NM"),
    F.col("svTrgtCnt").cast(IntegerType()).alias("TRGT_CT"),
    F.col("svSrcCnt").cast(IntegerType()).alias("SRC_CT"),
    F.col("Diff").cast(IntegerType()).alias("DIFF_CT"),
    rpad(F.col("svTlrncCd"), 255, " ").alias("TLRNC_CD"),
    F.col("svTlrnc").cast(IntegerType()).alias("TLRNC_AMT"),
    F.col("svTlrnc").cast(DecimalType(10,2)).alias("TLRNC_MULT_AMT"),
    rpad(F.lit("DAILY"), 255, " ").alias("FREQ_CD"),
    F.lit(ExtrRunCycle).cast(IntegerType()).alias("TRGT_RUN_CYC_NO"),
    rpad(F.lit(" "), 255, " ").alias("CRCTN_NOTE"),
    F.lit(CurrentDate_Prm).cast(TimestampType()).alias("CRT_DTM"),
    F.lit(CurrentDate_Prm).cast(TimestampType()).alias("LAST_UPDT_DTM"),
    rpad(F.lit(get_widget_value('$UWSAcct','')), 255, " ").alias("USER_ID")
)

# b) TBL_BAL_SUM => TBL_BAL_SUM (CODBCStage)
#    Constraint: @INROWNUM = 1
df_TBL_BAL_SUM = df_TransformLogic1.select(
    rpad(F.lit("IDS"), 255, " ").alias("TRGT_SYS_CD"),
    rpad(F.lit(SubjectArea), 255, " ").alias("SUBJ_AREA_NM"),
    rpad(F.lit(TrgtTable), 255, " ").alias("TRGT_TBL_NM"),
    rpad(F.lit(CurrentDate_Prm), 255, " ").alias("BAL_DT"),
    rpad(F.col("svTlrncCd"), 255, " ").alias("ROW_CT_TLRNC_CD"),
    rpad(F.lit("NA"), 255, " ").alias("CLMN_SUM_TLRNC_CD"),
    rpad(F.lit("NA"), 255, " ").alias("ROW_TO_ROW_TLRNC_CD"),
    rpad(F.lit("NA"), 255, " ").alias("RI_TLRNC_CD"),
    rpad(F.lit("NA"), 255, " ").alias("RELSHP_CLMN_SUM_TLRNC_CD"),
    rpad(F.lit("NA"), 255, " ").alias("CRS_FOOT_TLRNC_CD"),
    rpad(F.lit("NA"), 255, " ").alias("HIST_RSNBL_TLRNC_CD"),
    rpad(F.lit(ExtrRunCycle), 255, " ").alias("TRGT_RUN_CYC_NO"),
    rpad(F.lit(CurrentDate_Prm), 255, " ").alias("CRT_DTM"),
    rpad(F.lit(CurrentDate_Prm), 255, " ").alias("LAST_UPDT_DTM"),
    rpad(F.lit(get_widget_value('$UWSAcct','')), 255, " ").alias("USER_ID")
)

# 7) ROW_CT_UPDATE_FILE (CSeqFileStage) => Write df_ROW_CNT_OUT => #SrcTable##TrgtTable#RowCnt.dat
write_files(
    df_ROW_CNT_OUT,
    f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Also from ROW_CT_UPDATE_FILE, there is an OutputPin => ROW_CT_DTL => CODBCStage
# But first, the DS job re-reads the same file; define schema to avoid inferschema
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
    StructField("TLRNC_MULT_AMT", DecimalType(10,2), False),
    StructField("FREQ_CD", StringType(), False),
    StructField("TRGT_RUN_CYC_NO", IntegerType(), False),
    StructField("CRCTN_NOTE", StringType(), True),
    StructField("CRT_DTM", TimestampType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("USER_ID", StringType(), False)
])

df_ROW_CT_DTL_input = (
    spark.read
    .option("header", False)
    .option("sep", ",")
    .option("quote", "\"")
    .schema(row_ct_schema)
    .csv(f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat")
)

# 8) TBL_BAL_SUM => CODBCStage => Insert
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)
execute_dml(f"DROP TABLE IF EXISTS STAGING.AplLvlNoteRowCntBal_TBL_BAL_SUM_temp", jdbc_url_uws, jdbc_props_uws)
df_TBL_BAL_SUM.write \
    .format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", "STAGING.AplLvlNoteRowCntBal_TBL_BAL_SUM_temp") \
    .mode("errorifexists") \
    .save()
insert_sql_tbl_bal_sum = f"""
INSERT INTO {UWSOwner}.TBL_BAL_SUM(
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
SELECT
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
FROM STAGING.AplLvlNoteRowCntBal_TBL_BAL_SUM_temp
"""
execute_dml(insert_sql_tbl_bal_sum, jdbc_url_uws, jdbc_props_uws)

# 9) ROW_CT_DTL => CODBCStage => Insert
execute_dml(f"DROP TABLE IF EXISTS STAGING.AplLvlNoteRowCntBal_ROW_CT_DTL_temp", jdbc_url_uws, jdbc_props_uws)
df_ROW_CT_DTL_input.write \
    .format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", "STAGING.AplLvlNoteRowCntBal_ROW_CT_DTL_temp") \
    .mode("errorifexists") \
    .save()
insert_sql_row_ct_dtl = f"""
INSERT INTO {UWSOwner}.ROW_CT_DTL(
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
SELECT
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
FROM STAGING.AplLvlNoteRowCntBal_ROW_CT_DTL_temp
"""
execute_dml(insert_sql_row_ct_dtl, jdbc_url_uws, jdbc_props_uws)