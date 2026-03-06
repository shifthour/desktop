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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
TrgtTable = get_widget_value('TrgtTable','')
SrcTable = get_widget_value('SrcTable','')
NotifyMessage = get_widget_value('NotifyMessage','')
UWSOwner = get_widget_value('UWSOwner','')
SubjectArea = get_widget_value('SubjectArea','')
CurrentDate = get_widget_value('CurrentDate','')
ids_secret_name = get_widget_value('ids_secret_name','')
uws_secret_name = get_widget_value('uws_secret_name','')

# Convert ToleranceCd to integer if needed
try:
    toleranceInt = int(ToleranceCd)
except:
    toleranceInt = 0

# ----------------------------------------------------------------------------
# Stage: Compare (DB2Connector) reading from IDS
# ----------------------------------------------------------------------------
jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)
extract_query_Compare = (
    f"SELECT TRGT.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK, "
    f"TRGT.APL_ID as TRGT_APL_ID, "
    f"SRC.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK, "
    f"SRC.APL_ID as SRC_APL_ID, "
    f"TRGT.APL_LINK_ID as TRGT_APL_LINK_ID, "
    f"SRC.APL_LINK_ID as SRC_APL_LINK_ID "
    f"FROM {IDSOwner}.{TrgtTable} AS TRGT "
    f"FULL OUTER JOIN {IDSOwner}.{SrcTable} AS SRC "
    f"ON TRGT.SRC_SYS_CD_SK = SRC.SRC_SYS_CD_SK "
    f"AND TRGT.APL_ID = SRC.APL_ID "
    f"AND TRGT.APL_LINK_ID = SRC.APL_LINK_ID "
    f"WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}"
)
df_Compare = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query_Compare)
    .load()
)
df_Missing = df_Compare

# ----------------------------------------------------------------------------
# Stage: SRC_SYS_CD (CHashedFileStage) Scenario C => read parquet
# ----------------------------------------------------------------------------
df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

# ----------------------------------------------------------------------------
# Stage: TransformLogic (CTransformerStage)
# Primary link = df_Missing
# Lookup link = df_SRC_SYS_CD, left join on (TRGT_SRC_SYS_CD_SK == CD_MPPNG_SK)
# ----------------------------------------------------------------------------
df_TransformLogic = df_Missing.alias("Missing").join(
    df_SRC_SYS_CD.alias("SRC_SYS_CD"),
    F.col("Missing.TRGT_SRC_SYS_CD_SK") == F.col("SRC_SYS_CD.CD_MPPNG_SK"),
    "left"
)

# Output Pin 1: "Research"
df_Research = df_TransformLogic.filter(
    F.col("TRGT_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_APL_ID").isNull()
    | F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("SRC_APL_ID").isNull()
    | F.col("SRC_APL_LINK_ID").isNull()
    | F.col("TRGT_APL_LINK_ID").isNull()
).select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_APL_ID",
    "SRC_SRC_SYS_CD_SK",
    "SRC_APL_ID",
    "TRGT_APL_LINK_ID",
    "SRC_APL_LINK_ID"
)

# Output Pin 2: "Notify" (@INROWNUM = 1 And @INROWNUM > ToleranceCd)
windowSpec_TransformLogic = Window.orderBy(F.lit(1))
df_indexed_TransformLogic = df_TransformLogic.withColumn(
    "_rownum", F.row_number().over(windowSpec_TransformLogic)
)
df_Notify = df_indexed_TransformLogic.filter(
    (F.col("_rownum") == 1) & (F.col("_rownum") > F.lit(toleranceInt))
).select(
    F.lit(NotifyMessage).alias("NOTIFICATION")
)

# Output Pin 3: "ROW_CNT" (@INROWNUM = 1)
df_ROW_CNT = df_indexed_TransformLogic.filter(
    F.col("_rownum") == 1
).select(
    F.col("SRC_SYS_CD.TRGT_CD").alias("SRC_SYS_CD")
)

# ----------------------------------------------------------------------------
# Stage: ResearchFile (CSeqFileStage)
# Write df_Research to file
# File path => balancing/research => #SrcTable#.#TrgtTable#.dat.#RunID#
# ----------------------------------------------------------------------------
df_ResearchFile = df_Research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_APL_ID",
    "SRC_SRC_SYS_CD_SK",
    "SRC_APL_ID",
    "TRGT_APL_LINK_ID",
    "SRC_APL_LINK_ID"
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

# ----------------------------------------------------------------------------
# Stage: ErrorNotificationFile (CSeqFileStage)
# Write df_Notify to file => balancing/notify/AppealsBalancingNotification.dat
# append mode
# ----------------------------------------------------------------------------
# The column is char(70), so apply rpad( ,70,' ')
df_NotifyFile = df_Notify.select(
    F.rpad(F.col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)
write_files(
    df_NotifyFile,
    f"{adls_path}/balancing/notify/AppealsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------
# Stage: TransformLogic1 (CTransformerStage)
# Stage variables (global logic):
# ----------------------------------------------------------------------------
svTlrnc = ToleranceCd
svSrcCnt = GetSrcCount(IDSOwner, <...>, <...>, <...>, SrcTable)
svTrgtCnt = GetTrgtCount(IDSOwner, <...>, <...>, <...>, TrgtTable, ExtrRunCycle)
Diff = svTrgtCnt - svSrcCnt
if Diff == 0:
    svTlrncCd = "BAL"
elif abs(Diff) > int(svTlrnc):
    svTlrncCd = "OUT"
else:
    svTlrncCd = "IN"

# The input pin is df_ROW_CNT => single row. We add columns per stage variables.
# Constraint: @INROWNUM=1 => we already have single row.
df_ROW_CNT1 = df_ROW_CNT.limit(1)

df_enriched = df_ROW_CNT1 \
    .withColumn("TRGT_SYS_CD", F.lit("IDS")) \
    .withColumn("SUBJ_AREA_NM", F.lit(SubjectArea)) \
    .withColumn("TRGT_TBL_NM", F.lit(TrgtTable)) \
    .withColumn("TRGT_CT", F.lit(svTrgtCnt)) \
    .withColumn("SRC_CT", F.lit(svSrcCnt)) \
    .withColumn("DIFF_CT", F.lit(Diff)) \
    .withColumn("TLRNC_CD", F.lit(svTlrncCd)) \
    .withColumn("TLRNC_AMT", F.lit(svTlrnc)) \
    .withColumn("TLRNC_MULT_AMT", F.lit(svTlrnc)) \
    .withColumn("FREQ_CD", F.lit("DAILY")) \
    .withColumn("TRGT_RUN_CYC_NO", F.lit(ExtrRunCycle).cast("int")) \
    .withColumn("CRCTN_NOTE", F.lit(" ")) \
    .withColumn("CRT_DTM", F.lit(CurrentDate)) \
    .withColumn("LAST_UPDT_DTM", F.lit(CurrentDate)) \
    .withColumn("USER_ID", F.lit(<...>))

# Output Pin 1: ROW_CNT_OUT
df_ROW_CNT_OUT = df_enriched.select(
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

# Output Pin 2: TBL_BAL_SUM
df_TBL_BAL_SUM = df_enriched.select(
    F.col("TRGT_SYS_CD").alias("TRGT_SYS_CD"),
    F.col("SUBJ_AREA_NM").alias("SUBJ_AREA_NM"),
    F.col("TRGT_TBL_NM").alias("TRGT_TBL_NM"),
    F.lit(CurrentDate).alias("BAL_DT"),
    F.lit(svTlrncCd).alias("ROW_CT_TLRNC_CD"),
    F.lit("NA").alias("CLMN_SUM_TLRNC_CD"),
    F.lit("NA").alias("ROW_TO_ROW_TLRNC_CD"),
    F.lit("NA").alias("RI_TLRNC_CD"),
    F.lit("NA").alias("RELSHP_CLMN_SUM_TLRNC_CD"),
    F.lit("NA").alias("CRS_FOOT_TLRNC_CD"),
    F.lit("NA").alias("HIST_RSNBL_TLRNC_CD"),
    F.col("TRGT_RUN_CYC_NO").alias("TRGT_RUN_CYC_NO"),
    F.lit(CurrentDate).alias("CRT_DTM"),
    F.lit(CurrentDate).alias("LAST_UPDT_DTM"),
    F.lit(<...>).alias("USER_ID")
)

# ----------------------------------------------------------------------------
# Stage: TBL_BAL_SUM (CODBCStage) => Unconditional insert
# Use MERGE with ON 1=0 to replicate the insert
# ----------------------------------------------------------------------------
jdbc_url_UWS, jdbc_props_UWS = get_db_config(uws_secret_name)
temp_table_TBL_BAL_SUM = "STAGING.AplLinkRowCntBal_TBL_BAL_SUM_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_TBL_BAL_SUM}", jdbc_url_UWS, jdbc_props_UWS)

# For DB final output, apply rpad to string columns if needed (unknown lengths => use 255).
df_TBL_BAL_SUM_db = df_TBL_BAL_SUM.select(
    F.rpad(F.col("TRGT_SYS_CD"), 255, " ").alias("TRGT_SYS_CD"),
    F.rpad(F.col("SUBJ_AREA_NM"), 255, " ").alias("SUBJ_AREA_NM"),
    F.rpad(F.col("TRGT_TBL_NM"), 255, " ").alias("TRGT_TBL_NM"),
    "BAL_DT",
    F.rpad(F.col("ROW_CT_TLRNC_CD"), 255, " ").alias("ROW_CT_TLRNC_CD"),
    F.rpad(F.lit("NA"), 255, " ").alias("CLMN_SUM_TLRNC_CD"),
    F.rpad(F.lit("NA"), 255, " ").alias("ROW_TO_ROW_TLRNC_CD"),
    F.rpad(F.lit("NA"), 255, " ").alias("RI_TLRNC_CD"),
    F.rpad(F.lit("NA"), 255, " ").alias("RELSHP_CLMN_SUM_TLRNC_CD"),
    F.rpad(F.lit("NA"), 255, " ").alias("CRS_FOOT_TLRNC_CD"),
    F.rpad(F.lit("NA"), 255, " ").alias("HIST_RSNBL_TLRNC_CD"),
    "TRGT_RUN_CYC_NO",
    "CRT_DTM",
    "LAST_UPDT_DTM",
    F.rpad(F.col("USER_ID"), 255, " ").alias("USER_ID")
)

df_TBL_BAL_SUM_db.write.jdbc(
    url=jdbc_url_UWS,
    table=temp_table_TBL_BAL_SUM,
    mode="overwrite",
    properties=jdbc_props_UWS
)

merge_sql_TBL_BAL_SUM = f"""
MERGE {UWSOwner}.TBL_BAL_SUM AS T
USING {temp_table_TBL_BAL_SUM} AS S
ON 1=0
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
execute_dml(merge_sql_TBL_BAL_SUM, jdbc_url_UWS, jdbc_props_UWS)

# ----------------------------------------------------------------------------
# Stage: ROW_CT_UPDATE_FILE (CSeqFileStage)
# Write df_ROW_CNT_OUT, then read same file, finally next stage ROW_CT_DTL
# ----------------------------------------------------------------------------
df_ROW_CT_UPDATE_FILE = df_ROW_CNT_OUT.select(
    F.rpad(F.col("TRGT_SYS_CD"), 255, " ").alias("TRGT_SYS_CD"),
    F.rpad(F.col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("SUBJ_AREA_NM"), 255, " ").alias("SUBJ_AREA_NM"),
    F.rpad(F.col("TRGT_TBL_NM"), 255, " ").alias("TRGT_TBL_NM"),
    "TRGT_CT",
    "SRC_CT",
    "DIFF_CT",
    F.rpad(F.col("TLRNC_CD"), 255, " ").alias("TLRNC_CD"),
    "TLRNC_AMT",
    "TLRNC_MULT_AMT",
    F.rpad(F.col("FREQ_CD"), 255, " ").alias("FREQ_CD"),
    "TRGT_RUN_CYC_NO",
    F.rpad(F.col("CRCTN_NOTE"), 255, " ").alias("CRCTN_NOTE"),
    "CRT_DTM",
    "LAST_UPDT_DTM",
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

# Read the same file (ROW_CT_UPDATE_FILE) to feed ROW_CT_DTL stage
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
    StructField("TLRNC_MULT_AMT", DecimalType(38, 10), False),
    StructField("FREQ_CD", StringType(), False),
    StructField("TRGT_RUN_CYC_NO", IntegerType(), False),
    StructField("CRCTN_NOTE", StringType(), True),
    StructField("CRT_DTM", TimestampType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("USER_ID", StringType(), False)
])
df_ROW_CT_DTL = spark.read.csv(
    path=f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat",
    sep=",",
    quote="\"",
    header=False,
    schema=schema_ROW_CT_DTL
)

# ----------------------------------------------------------------------------
# Stage: ROW_CT_DTL (CODBCStage) => Insert => replicate with MERGE ON 1=0
# ----------------------------------------------------------------------------
temp_table_ROW_CT_DTL = "STAGING.AplLinkRowCntBal_ROW_CT_DTL_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_ROW_CT_DTL}", jdbc_url_UWS, jdbc_props_UWS)

# For final DB output, rpad any string columns again if needed
df_ROW_CT_DTL_db = df_ROW_CT_DTL.select(
    F.rpad(F.col("TRGT_SYS_CD"), 255, " ").alias("TRGT_SYS_CD"),
    F.rpad(F.col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("SUBJ_AREA_NM"), 255, " ").alias("SUBJ_AREA_NM"),
    F.rpad(F.col("TRGT_TBL_NM"), 255, " ").alias("TRGT_TBL_NM"),
    "TRGT_CT",
    "SRC_CT",
    "DIFF_CT",
    F.rpad(F.col("TLRNC_CD"), 255, " ").alias("TLRNC_CD"),
    "TLRNC_AMT",
    "TLRNC_MULT_AMT",
    F.rpad(F.col("FREQ_CD"), 255, " ").alias("FREQ_CD"),
    "TRGT_RUN_CYC_NO",
    F.rpad(F.col("CRCTN_NOTE"), 255, " ").alias("CRCTN_NOTE"),
    "CRT_DTM",
    "LAST_UPDT_DTM",
    F.rpad(F.col("USER_ID"), 255, " ").alias("USER_ID")
)

df_ROW_CT_DTL_db.write.jdbc(
    url=jdbc_url_UWS,
    table=temp_table_ROW_CT_DTL,
    mode="overwrite",
    properties=jdbc_props_UWS
)

merge_sql_ROW_CT_DTL = f"""
MERGE {UWSOwner}.ROW_CT_DTL AS T
USING {temp_table_ROW_CT_DTL} AS S
ON 1=0
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
execute_dml(merge_sql_ROW_CT_DTL, jdbc_url_UWS, jdbc_props_UWS)