# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   Balances Facets to IDS with 2 additional key values in addition to SRC_SYS_CD_SK and CLM_ID
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Oliver Nielsen                  10/16/2007          3264                              Originally Programmed                           devlIDS30                   
# MAGIC Oliver Nielsen                   09/22/2008       Prod Supp                   Added IDSDSN param to function call
# MAGIC                                                                                                       for GetTrgtCnt and GetSrcCnt                    devlIDSnew                Steph Goddard             09/26/2008
# MAGIC 
# MAGIC Manasa Andru                2016-12-20         TFS - 15963              Changed the metadata of the field              IntegrateDev2               Jag Yelavarthi              2016-12-20
# MAGIC                                                                                                 TRGT_EXTRA_COLUMN2 from Integer 10 
# MAGIC                                                                                                  to Char 4 in the TransformLogic Stage.
# MAGIC 
# MAGIC Shanmugam A 	        2017-03-02         5321                     Modified SQL to add alias for column in  	          IntegrateDev2           Jag Yelavarthi              2017-03-07
# MAGIC 						Compare stage

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
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    TimestampType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameters (including secret_name parameters per instructions)
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
TrgtTable = get_widget_value('TrgtTable','')
SrcTable = get_widget_value('SrcTable','')
NotifyMessage = get_widget_value('NotifyMessage','')
UWSOwner = get_widget_value('UWSOwner','')
SubjectArea = get_widget_value('SubjectArea','')
CurrentDateParam = get_widget_value('CurrentDate','')
ExtraColumn = get_widget_value('ExtraColumn','')
ExtraColumn2 = get_widget_value('ExtraColumn2','')

ids_secret_name = get_widget_value('ids_secret_name','')
uws_secret_name = get_widget_value('uws_secret_name','')

# --------------------------------------------------------------------------------
# Compare (DB2Connector) - Reading from IDS database
# --------------------------------------------------------------------------------
jdbc_url_compare, jdbc_props_compare = get_db_config(ids_secret_name)

extract_query_compare = f"""
SELECT
  TRGT.SRC_SYS_CD_SK,
  TRGT.CLM_ID,
  SRC.SRC_SYS_CD_SK AS SRC_SYS_CD_SK_1,
  SRC.CLM_ID AS CLM_ID_1,
  TRGT.{ExtraColumn} AS TRGT_EXTRA_COLUMN,
  TRGT.{ExtraColumn2} AS TRGT_EXTRA_COLUMN2
FROM {IDSOwner}.{TrgtTable} TRGT
FULL OUTER JOIN {IDSOwner}.{SrcTable} SRC
  ON TRGT.SRC_SYS_CD_SK = SRC.SRC_SYS_CD_SK
  AND TRGT.CLM_ID = SRC.CLM_ID
  AND TRGT.{ExtraColumn} = SRC.{ExtraColumn}
  AND TRGT.{ExtraColumn2} = SRC.{ExtraColumn2}
WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
"""

df_Compare_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_compare)
    .options(**jdbc_props_compare)
    .option("query", extract_query_compare)
    .load()
)

# Rename columns so that downstream references (e.g. Missing.TRGT_SRC_SYS_CD_SK) match.
df_Compare = (
    df_Compare_raw
    .withColumnRenamed("SRC_SYS_CD_SK", "TRGT_SRC_SYS_CD_SK")
    .withColumnRenamed("CLM_ID", "TRGT_CLM_ID")
    .withColumnRenamed("SRC_SYS_CD_SK_1", "SRC_SRC_SYS_CD_SK")
    .withColumnRenamed("CLM_ID_1", "SRC_CLM_ID")
)

# --------------------------------------------------------------------------------
# SRC_SYS_CD (CHashedFileStage scenario C - reading parquet)
# --------------------------------------------------------------------------------
df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

# --------------------------------------------------------------------------------
# TransformLogic (CTransformerStage)
# --------------------------------------------------------------------------------
# Join df_Compare (alias "Missing") to df_SRC_SYS_CD (alias "SRC_SYS_CD") on left join
df_Joined = df_Compare.alias("Missing").join(
    df_SRC_SYS_CD.alias("SRC_SYS_CD"),
    F.col("Missing.TRGT_SRC_SYS_CD_SK") == F.col("SRC_SYS_CD.CD_MPPNG_SK"),
    "left"
)

# Add row_number to replicate @INROWNUM usage
windowSpec = Window.orderBy(F.lit(1))
df_Joined = df_Joined.withColumn("row_number", F.row_number().over(windowSpec))

# Research link constraint:
# "ISNULL(Missing.TRGT_SRC_SYS_CD_SK) OR ISNULL(Missing.TRGT_CLM_ID) OR
#  ISNULL(Missing.SRC_SRC_SYS_CD_SK) OR ISNULL(Missing.SRC_CLM_ID) OR
#  ISNULL(Missing.TRGT_EXTRA_COLUMN) OR ISNULL(Missing.TRGT_EXTRA_COLUMN2)"
df_Research_filter = (
    (F.col("Missing.TRGT_SRC_SYS_CD_SK").isNull()) |
    (F.col("Missing.TRGT_CLM_ID").isNull()) |
    (F.col("Missing.SRC_SRC_SYS_CD_SK").isNull()) |
    (F.col("Missing.SRC_CLM_ID").isNull()) |
    (F.col("Missing.TRGT_EXTRA_COLUMN").isNull()) |
    (F.col("Missing.TRGT_EXTRA_COLUMN2").isNull())
)
df_Research_temp = df_Joined.filter(df_Research_filter)
df_Research = df_Research_temp.select(
    F.col("Missing.TRGT_SRC_SYS_CD_SK"),
    F.col("Missing.TRGT_CLM_ID"),
    F.col("Missing.SRC_SRC_SYS_CD_SK"),
    F.col("Missing.SRC_CLM_ID"),
    F.col("Missing.TRGT_EXTRA_COLUMN").alias("TRGT_EXTRA_COLMN"),
    F.rpad(F.col("Missing.TRGT_EXTRA_COLUMN2"), 4, " ").alias("TRGT_EXTRA_COLUMN2")
)

# Notify link constraint:
# "@INROWNUM = 1 And @INROWNUM > ToleranceCd"
# Translate ToleranceCd to integer for numeric comparison.
tol_int = int(ToleranceCd) if ToleranceCd.isdigit() else 0
df_Notify_temp = df_Joined.filter(
    (F.col("row_number") == 1) &
    (F.col("row_number") > F.lit(tol_int))
)
df_Notify = df_Notify_temp.select(
    F.rpad(F.lit(NotifyMessage), 70, " ").alias("NOTIFICATION")
)

# ROW_CNT link constraint:
# "@INROWNUM = 1"
df_ROW_CNT_temp = df_Joined.filter(F.col("row_number") == 1)
df_ROW_CNT = df_ROW_CNT_temp.select(
    F.col("SRC_SYS_CD.TRGT_CD").alias("SRC_SYS_CD")
)

# --------------------------------------------------------------------------------
# ResearchFile (CSeqFileStage) - Write research file
# --------------------------------------------------------------------------------
write_files(
    df_Research.select(
        "TRGT_SRC_SYS_CD_SK",
        "TRGT_CLM_ID",
        "SRC_SRC_SYS_CD_SK",
        "SRC_CLM_ID",
        "TRGT_EXTRA_COLMN",
        "TRGT_EXTRA_COLUMN2"
    ),
    f"{adls_path}/balancing/research/{SrcTable}.{TrgtTable}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# ErrorNotificationFile (CSeqFileStage) - Append notify file
# --------------------------------------------------------------------------------
df_Notify_final = df_Notify.select(
    F.rpad(F.col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)

write_files(
    df_Notify_final,
    f"{adls_path}/balancing/notify/ClaimBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Copy_of_TransformLogic (CTransformerStage)
# Stage Variables:
#   svTlrnc   = ToleranceCd
#   svSrcCnt  = GetSrcCount($IDSOwner, $IDSDSN, $IDSAcct, $IDSPW, SrcTable)
#   svTrgtCnt = GetTrgtCount($IDSOwner, $IDSDSN, $IDSAcct, $IDSPW, TrgtTable, ExtrRunCycle)
#   Diff      = svTrgtCnt - svSrcCnt
#   svTlrncCd = IF Diff=0 THEN 'BAL' ELSE IF ABS(Diff)>svTlrnc THEN 'OUT' ELSE 'IN'
# --------------------------------------------------------------------------------

collected_row_cnt = df_ROW_CNT.collect()
svTlrnc = int(ToleranceCd) if ToleranceCd.isdigit() else 0
svSrcCnt = GetSrcCount(IDSOwner, <...>, <...>, <...>, SrcTable)
svTrgtCnt = GetTrgtCount(IDSOwner, <...>, <...>, <...>, TrgtTable, ExtrRunCycle)
Diff = svTrgtCnt - svSrcCnt
if Diff == 0:
    svTlrncCd = "BAL"
elif abs(Diff) > svTlrnc:
    svTlrncCd = "OUT"
else:
    svTlrncCd = "IN"

if len(collected_row_cnt) > 0:
    row_val = collected_row_cnt[0]
    src_sys_cd_val = row_val["SRC_SYS_CD"]
else:
    src_sys_cd_val = None

# ROW_CNT_OUT columns
# TRGT_SYS_CD, SRC_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, TRGT_CT, SRC_CT, DIFF_CT,
# TLRNC_CD, TLRNC_AMT, TLRNC_MULT_AMT, FREQ_CD, TRGT_RUN_CYC_NO, CRCTN_NOTE,
# CRT_DTM, LAST_UPDT_DTM, USER_ID
row_cnt_data = [(
    "IDS",                    # TRGT_SYS_CD
    src_sys_cd_val,          # SRC_SYS_CD
    SubjectArea,             # SUBJ_AREA_NM
    TrgtTable,               # TRGT_TBL_NM
    svTrgtCnt,               # TRGT_CT
    svSrcCnt,                # SRC_CT
    Diff,                    # DIFF_CT
    svTlrncCd,               # TLRNC_CD
    svTlrnc,                 # TLRNC_AMT
    svTlrnc,                 # TLRNC_MULT_AMT
    "DAILY",                 # FREQ_CD
    int(ExtrRunCycle) if ExtrRunCycle.isdigit() else 0,  # TRGT_RUN_CYC_NO
    " ",                     # CRCTN_NOTE
    current_timestamp(),     # CRT_DTM
    current_timestamp(),     # LAST_UPDT_DTM
    "<...>"                  # USER_ID  (unknown parameter in job)
)]
schema_cnt_out = [
    StructField("TRGT_SYS_CD", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("SUBJ_AREA_NM", StringType(), True),
    StructField("TRGT_TBL_NM", StringType(), True),
    StructField("TRGT_CT", IntegerType(), True),
    StructField("SRC_CT", IntegerType(), True),
    StructField("DIFF_CT", IntegerType(), True),
    StructField("TLRNC_CD", StringType(), True),
    StructField("TLRNC_AMT", IntegerType(), True),
    StructField("TLRNC_MULT_AMT", IntegerType(), True),
    StructField("FREQ_CD", StringType(), True),
    StructField("TRGT_RUN_CYC_NO", IntegerType(), True),
    StructField("CRCTN_NOTE", StringType(), True),
    StructField("CRT_DTM", TimestampType(), True),
    StructField("LAST_UPDT_DTM", TimestampType(), True),
    StructField("USER_ID", StringType(), True)
]
df_copy_of_transformlogic_ROW_CNT_out = spark.createDataFrame(row_cnt_data, schema=StructType(schema_cnt_out))

# TBL_BAL_SUM columns
# TRGT_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, BAL_DT, ROW_CT_TLRNC_CD, CLMN_SUM_TLRNC_CD,
# ROW_TO_ROW_TLRNC_CD, RI_TLRNC_CD, RELSHP_CLMN_SUM_TLRNC_CD, CRS_FOOT_TLRNC_CD,
# HIST_RSNBL_TLRNC_CD, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID
tbl_bal_sum_data = [(
    "IDS",                       # TRGT_SYS_CD
    SubjectArea,                 # SUBJ_AREA_NM
    TrgtTable,                   # TRGT_TBL_NM
    CurrentDateParam,            # BAL_DT (job uses 'CurrentDate' param)
    svTlrncCd,                   # ROW_CT_TLRNC_CD
    "NA",                        # CLMN_SUM_TLRNC_CD
    "NA",                        # ROW_TO_ROW_TLRNC_CD
    "NA",                        # RI_TLRNC_CD
    "NA",                        # RELSHP_CLMN_SUM_TLRNC_CD
    "NA",                        # CRS_FOOT_TLRNC_CD
    "NA",                        # HIST_RSNBL_TLRNC_CD
    int(ExtrRunCycle) if ExtrRunCycle.isdigit() else 0,  # TRGT_RUN_CYC_NO
    current_timestamp(),         # CRT_DTM
    current_timestamp(),         # LAST_UPDT_DTM
    "<...>"                      # USER_ID (unknown parameter in job)
)]
schema_tbl_bal_sum = [
    StructField("TRGT_SYS_CD", StringType(), True),
    StructField("SUBJ_AREA_NM", StringType(), True),
    StructField("TRGT_TBL_NM", StringType(), True),
    StructField("BAL_DT", StringType(), True),
    StructField("ROW_CT_TLRNC_CD", StringType(), True),
    StructField("CLMN_SUM_TLRNC_CD", StringType(), True),
    StructField("ROW_TO_ROW_TLRNC_CD", StringType(), True),
    StructField("RI_TLRNC_CD", StringType(), True),
    StructField("RELSHP_CLMN_SUM_TLRNC_CD", StringType(), True),
    StructField("CRS_FOOT_TLRNC_CD", StringType(), True),
    StructField("HIST_RSNBL_TLRNC_CD", StringType(), True),
    StructField("TRGT_RUN_CYC_NO", IntegerType(), True),
    StructField("CRT_DTM", TimestampType(), True),
    StructField("LAST_UPDT_DTM", TimestampType(), True),
    StructField("USER_ID", StringType(), True)
]
df_copy_of_transformlogic_TBL_BAL_SUM = spark.createDataFrame(tbl_bal_sum_data, schema=StructType(schema_tbl_bal_sum))

# --------------------------------------------------------------------------------
# TBL_BAL_SUM (CODBCStage) - Write to DB (Append)
# --------------------------------------------------------------------------------
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)

(
    df_copy_of_transformlogic_TBL_BAL_SUM
    .write
    .mode("append")
    .format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("dbtable", f"{UWSOwner}.TBL_BAL_SUM")
    .save()
)

# --------------------------------------------------------------------------------
# ROW_CT_UPDATE_FILE (CSeqFileStage) - Write .dat file
# --------------------------------------------------------------------------------
df_ROW_CT_UPDATE_FILE = df_copy_of_transformlogic_ROW_CNT_out.select(
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

# Read the same file back per the stage design (ReadFileProperties)
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
    StructField("TLRNC_MULT_AMT", DecimalType(10, 2), False),
    StructField("FREQ_CD", StringType(), False),
    StructField("TRGT_RUN_CYC_NO", IntegerType(), False),
    StructField("CRCTN_NOTE", StringType(), True),
    StructField("CRT_DTM", TimestampType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("USER_ID", StringType(), False)
])

df_ROW_CT_DTL = (
    spark.read
    .option("header", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_ROW_CT_DTL)
    .csv(f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat")
)

# --------------------------------------------------------------------------------
# ROW_CT_DTL (CODBCStage) - Write to DB (Append)
# --------------------------------------------------------------------------------
(
    df_ROW_CT_DTL
    .write
    .mode("append")
    .format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("dbtable", f"{UWSOwner}.ROW_CT_DTL")
    .save()
)