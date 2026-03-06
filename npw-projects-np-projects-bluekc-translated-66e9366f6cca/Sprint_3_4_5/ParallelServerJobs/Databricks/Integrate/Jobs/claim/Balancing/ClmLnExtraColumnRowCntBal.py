# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_4 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 10/02/08 15:44:02 Batch  14886_56722 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_3 10/02/08 15:37:31 Batch  14886_56255 INIT bckcett devlIDSnew dsadm bls for on
# MAGIC ^1_1 10/01/08 10:51:59 Batch  14885_39126 INIT bckcett devlIDSnew u03651 steffy
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/24/07 14:13:49 Batch  14542_51233 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/24/07 14:09:22 Batch  14542_50966 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_2 10/19/07 07:32:17 Batch  14537_27150 PROMOTE bckcett testIDS30 u10913 Ollie move from devl to test
# MAGIC ^1_2 10/19/07 07:30:04 Batch  14537_27018 INIT bckcett devlIDS30 u10913 Ollie move from devl to test
# MAGIC ^1_1 10/17/07 10:10:27 Batch  14535_36634 INIT bckcett devlIDS30 u03651 steffy to test for Ollie
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   Balancing table to Target table compare with ONE extra key column in addition to CLM_ID, SRC_SYS_CD_SK, CLM_LN_SEQ_NO
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Oliver Nielsen                  10/16/2007          3264                              Originally Programmed                           devlIDS30       
# MAGIC Oliver Nielsen                  09/26/2008        Prod Supp                  Added $IDSDSN to GetSrc and GetTrgt      devlIDSnew               Steph Goddard            09/26/2008

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
from pyspark.sql.functions import col, lit, when, row_number, abs
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
TrgtTable = get_widget_value('TrgtTable','')
SrcTable = get_widget_value('SrcTable','')
NotifyMessage = get_widget_value('NotifyMessage','\\"ROW COUNT BALANCING FACETS - IDS CLAIM ATTACHMENT OUT OF TOLERANCE\\"')
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
SubjectArea = get_widget_value('SubjectArea','Claims')
CurrentDate = get_widget_value('CurrentDate','2007-09-14 10:00:00')
ExtraColumn = get_widget_value('ExtraColumn','')

# ---------------------------------------------------------------------
# Stage: Compare (DB2Connector) - Reading from IDS database
# ---------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
  TRGT.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
  TRGT.CLM_ID AS TRGT_CLM_ID,
  SRC.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
  SRC.CLM_ID AS SRC_CLM_ID,
  TRGT.{ExtraColumn} AS TRGT_EXTRA_COLUMN
FROM {IDSOwner}.{TrgtTable} AS TRGT
FULL OUTER JOIN {IDSOwner}.{SrcTable} AS SRC
  ON TRGT.SRC_SYS_CD_SK=SRC.SRC_SYS_CD_SK
     AND TRGT.CLM_ID=SRC.CLM_ID
     AND TRGT.CLM_LN_SEQ_NO=SRC.CLM_LN_SEQ_NO
     AND SRC.{ExtraColumn}=TRGT.{ExtraColumn}
WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
-- {IDSOwner}.{TrgtTable} AS TRGT (line from original job)
"""

df_Compare = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# ---------------------------------------------------------------------
# Stage: SRC_SYS_CD (CHashedFileStage) - Scenario C read as Parquet
# ---------------------------------------------------------------------
df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

# ---------------------------------------------------------------------
# Stage: TransformLogic (CTransformerStage)
#   Input links:
#     - Missing (primary, from df_Compare)
#     - SRC_SYS_CD (lookup, from df_SRC_SYS_CD), left join
#   Output links:
#     - Research
#     - Notify
#     - ROW_CNT
# ---------------------------------------------------------------------
df_Joined = df_Compare.alias("Missing").join(
    df_SRC_SYS_CD.alias("SRC_SYS_CD"),
    col("Missing.TRGT_SRC_SYS_CD_SK") == col("SRC_SYS_CD.CD_MPPNG_SK"),
    "left"
)

# For row number logic
w_transform = Window.orderBy(lit(1))
df_Joined_with_row = df_Joined.withColumn("_row_num", row_number().over(w_transform))

# Research link constraint
df_Research = df_Joined.filter(
    col("TRGT_SRC_SYS_CD_SK").isNull()
    | col("TRGT_CLM_ID").isNull()
    | col("SRC_SRC_SYS_CD_SK").isNull()
    | col("SRC_CLM_ID").isNull()
    | col("TRGT_EXTRA_COLUMN").isNull()
).select(
    col("TRGT_SRC_SYS_CD_SK").alias("TRGT_SRC_SYS_CD_SK"),
    col("TRGT_CLM_ID").alias("TRGT_CLM_ID"),
    col("SRC_SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
    col("SRC_CLM_ID").alias("SRC_CLM_ID"),
    col("TRGT_EXTRA_COLUMN").alias("TRGT_EXTRA_COLUMN")
)

# Notify link constraint: @INROWNUM = 1 And @INROWNUM > ToleranceCd
# Replicate row number and filter logic
df_Notify = df_Joined_with_row.filter(
    (col("_row_num") == 1) & (col("_row_num") > lit(ToleranceCd))
).select(
    # NOTIFICATION (char(70)) => must rpad to 70
    F.rpad(lit(NotifyMessage), 70, " ").alias("NOTIFICATION")
)

# ROW_CNT link constraint: @INROWNUM = 1
df_ROW_CNT = df_Joined_with_row.filter(
    col("_row_num") == 1
).select(
    col("SRC_SYS_CD.TRGT_CD").alias("SRC_SYS_CD")
)

# ---------------------------------------------------------------------
# Stage: ResearchFile (CSeqFileStage) - Write
# ---------------------------------------------------------------------
write_files(
    df_Research.select(
        "TRGT_SRC_SYS_CD_SK",
        "TRGT_CLM_ID",
        "SRC_SRC_SYS_CD_SK",
        "SRC_CLM_ID",
        "TRGT_EXTRA_COLUMN"
    ),
    f"{adls_path}/balancing/research/{SrcTable}.{TrgtTable}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)

# ---------------------------------------------------------------------
# Stage: ErrorNotificationFile (CSeqFileStage) - Append
# ---------------------------------------------------------------------
write_files(
    df_Notify.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/ClaimBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)

# ---------------------------------------------------------------------
# Stage: Copy_of_TransformLogic (CTransformerStage)
#   Input: df_ROW_CNT
#   Output: ROW_CNT_OUT, TBL_BAL_SUM
# ---------------------------------------------------------------------
df_Copy_of_TransformLogic_temp = df_ROW_CNT.withColumn(
    "svTlrnc", lit(ToleranceCd)
).withColumn(
    "svSrcCnt", F.expr("GetSrcCount(IDSOwner,<...>,<...>,<...>,SrcTable)")
).withColumn(
    "svTrgtCnt", F.expr("GetTrgtCount(IDSOwner,<...>,<...>,<...>,TrgtTable,ExtrRunCycle)")
).withColumn(
    "Diff", col("svTrgtCnt") - col("svSrcCnt")
).withColumn(
    "svTlrncCd",
    when(col("Diff") == lit(0), lit("BAL"))
    .when(abs(col("Diff")) > col("svTlrnc"), lit("OUT"))
    .otherwise(lit("IN"))
)

w_copy = Window.orderBy(lit(1))
df_Copy_of_TransformLogic_temp2 = df_Copy_of_TransformLogic_temp.withColumn(
    "_row_num", row_number().over(w_copy)
)

df_ROW_CNT_OUT = df_Copy_of_TransformLogic_temp2.filter(
    col("_row_num") == 1
).select(
    lit("IDS").alias("TRGT_SYS_CD"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    lit(SubjectArea).alias("SUBJ_AREA_NM"),
    lit(TrgtTable).alias("TRGT_TBL_NM"),
    col("svTrgtCnt").alias("TRGT_CT"),
    col("svSrcCnt").alias("SRC_CT"),
    col("Diff").alias("DIFF_CT"),
    col("svTlrncCd").alias("TLRNC_CD"),
    col("svTlrnc").alias("TLRNC_AMT"),
    col("svTlrnc").alias("TLRNC_MULT_AMT"),
    lit("DAILY").alias("FREQ_CD"),
    lit(ExtrRunCycle).alias("TRGT_RUN_CYC_NO"),
    F.expr("space(1)").alias("CRCTN_NOTE"),
    current_timestamp().alias("CRT_DTM"),
    current_timestamp().alias("LAST_UPDT_DTM"),
    lit("<...>").alias("USER_ID")
)

df_TBL_BAL_SUM = df_Copy_of_TransformLogic_temp2.filter(
    col("_row_num") == 1
).select(
    lit("IDS").alias("TRGT_SYS_CD"),
    lit(SubjectArea).alias("SUBJ_AREA_NM"),
    lit(TrgtTable).alias("TRGT_TBL_NM"),
    lit(CurrentDate).alias("BAL_DT"),
    col("svTlrncCd").alias("ROW_CT_TLRNC_CD"),
    lit("NA").alias("CLMN_SUM_TLRNC_CD"),
    lit("NA").alias("ROW_TO_ROW_TLRNC_CD"),
    lit("NA").alias("RI_TLRNC_CD"),
    lit("NA").alias("RELSHP_CLMN_SUM_TLRNC_CD"),
    lit("NA").alias("CRS_FOOT_TLRNC_CD"),
    lit("NA").alias("HIST_RSNBL_TLRNC_CD"),
    lit(ExtrRunCycle).alias("TRGT_RUN_CYC_NO"),
    current_timestamp().alias("CRT_DTM"),
    current_timestamp().alias("LAST_UPDT_DTM"),
    lit("<...>").alias("USER_ID")
)

# ---------------------------------------------------------------------
# Stage: TBL_BAL_SUM (CODBCStage) - Insert into #$UWSOwner#.TBL_BAL_SUM
#   Implemented via STAGING merge
# ---------------------------------------------------------------------
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)
temp_table_TBL_BAL_SUM = "STAGING.ClmLnExtraColumnRowCntBal_TBL_BAL_SUM_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_TBL_BAL_SUM}", jdbc_url_uws, jdbc_props_uws)

df_TBL_BAL_SUM.write \
    .format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", temp_table_TBL_BAL_SUM) \
    .mode("overwrite") \
    .save()

merge_sql_TBL_BAL_SUM = f"""
MERGE INTO {UWSOwner}.TBL_BAL_SUM AS T
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
  )
;
"""
execute_dml(merge_sql_TBL_BAL_SUM, jdbc_url_uws, jdbc_props_uws)

# ---------------------------------------------------------------------
# Stage: ROW_CT_UPDATE_FILE (CSeqFileStage)
#   1) Write df_ROW_CNT_OUT to #SrcTable##TrgtTable#RowCnt.dat
#   2) Then read the same file
#   3) OutputPin => ROW_CT_DTL -> Next Stage
# ---------------------------------------------------------------------
write_files(
    df_ROW_CNT_OUT.select(
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
    quote='"',
    nullValue=None
)

# Reading back the same file
schema_ROW_CT_UPDATE_FILE = StructType([
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

df_ROW_CT_UPDATE_FILE_out = (
    spark.read.format("csv")
    .option("header", False)
    .option("quote", '"')
    .schema(schema_ROW_CT_UPDATE_FILE)
    .load(f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat")
)

# ---------------------------------------------------------------------
# Stage: ROW_CT_DTL (CODBCStage)
#   Insert into #$UWSOwner#.ROW_CT_DTL
# ---------------------------------------------------------------------
temp_table_ROW_CT_DTL = "STAGING.ClmLnExtraColumnRowCntBal_ROW_CT_DTL_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_ROW_CT_DTL}", jdbc_url_uws, jdbc_props_uws)

df_ROW_CT_UPDATE_FILE_out.write \
    .format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", temp_table_ROW_CT_DTL) \
    .mode("overwrite") \
    .save()

merge_sql_ROW_CT_DTL = f"""
MERGE INTO {UWSOwner}.ROW_CT_DTL AS T
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
  )
;
"""
execute_dml(merge_sql_ROW_CT_DTL, jdbc_url_uws, jdbc_props_uws)