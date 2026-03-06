# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/11/09 15:13:12 Batch  15046_54866 PROMOTE bckcetl ids20 dsadm rc for ralph 
# MAGIC ^1_1 03/11/09 14:40:56 Batch  15046_52880 INIT bckcett testIDScur dsadm rc for ralph 
# MAGIC ^1_4 03/09/09 11:24:23 Batch  15044_41067 PROMOTE bckcett testIDScur u03651 steph for Ralph
# MAGIC ^1_4 03/09/09 11:01:22 Batch  15044_39685 INIT bckcett devlIDScur u03651 steffy
# MAGIC ^1_3 03/06/09 14:38:31 Batch  15041_52722 INIT bckcett devlIDScur u08717 Brent for Ralph
# MAGIC ^1_2 03/05/09 20:44:06 Batch  15040_74649 INIT bckcett devlIDScur u03651 steffy
# MAGIC ^1_1 03/05/09 14:08:04 Batch  15040_50886 INIT bckcett devlIDScur u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC Â© Copyright 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Load Balancing Tables into IDS
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #                  Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                   -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Bhoomi D               2009-02-27         3863                                    Originally Programmed                                                                  devlIDScur                    Steph Goddard         03/04/2009

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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit, isnull, when, row_number, abs as spark_abs, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameter definitions
RunID = get_widget_value("RunID","20090305")
ToleranceCd = get_widget_value("ToleranceCd","0")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","")
TrgtTable = get_widget_value("TrgtTable","")
SrcTable = get_widget_value("SrcTable","")
NotifyMessage = get_widget_value("NotifyMessage","")
UWSOwner = get_widget_value("UWSOwner","")
uws_secret_name = get_widget_value("uws_secret_name","")
SubjectArea = get_widget_value("SubjectArea","")
CurrentDate = get_widget_value("CurrentDate","")

# Additional parameters inferred from stage variables
IDSDSN = get_widget_value("IDSDSN","")
IDSAcct = get_widget_value("IDSAcct","")
IDSPW = get_widget_value("IDSPW","")
UWSAcct = get_widget_value("UWSAcct","")

# ----------------------------------------------------------------------------------------------------
# Stage: Compare (DB2Connector reading from IDS)
jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)
extract_query_Compare = f"""
SELECT 
  TRGT.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK,
  TRGT.SUB_UNIQ_KEY as TRGT_SUB_UNIQ_KEY,
  SRC.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK,
  SRC.SUB_UNIQ_KEY as SRC_SUB_UNIQ_KEY
FROM {IDSOwner}.{TrgtTable} AS TRGT
FULL OUTER JOIN {IDSOwner}.{SrcTable} AS SRC
  ON TRGT.SRC_SYS_CD_SK=SRC.SRC_SYS_CD_SK
  AND TRGT.SUB_UNIQ_KEY=SRC.SUB_UNIQ_KEY
WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
"""

df_Compare = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query_Compare)
    .load()
)

# ----------------------------------------------------------------------------------------------------
# Stage: SRC_SYS_CD (CHashedFileStage) - Scenario C => Read from parquet
df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

# ----------------------------------------------------------------------------------------------------
# Stage: TransformLogic (CTransformerStage)
# Primary link join with left lookup
df_Missing = df_Compare.alias("Missing").join(
    df_SRC_SYS_CD.alias("SRC_SYS_CD"),
    col("Missing.TRGT_SRC_SYS_CD_SK") == col("SRC_SYS_CD.CD_MPPNG_SK"),
    "left"
)

# Research output
df_TransformLogic_Research = df_Missing.filter(
    (isnull(col("Missing.TRGT_SRC_SYS_CD_SK"))) |
    (isnull(col("Missing.TRGT_SUB_UNIQ_KEY"))) |
    (isnull(col("Missing.SRC_SRC_SYS_CD_SK"))) |
    (isnull(col("Missing.SRC_SUB_UNIQ_KEY")))
).select(
    col("Missing.TRGT_SRC_SYS_CD_SK").alias("TRGT_SRC_SYS_CD_SK"),
    col("Missing.TRGT_SUB_UNIQ_KEY").alias("TRGT_SUB_UNIQ_KEY"),
    col("Missing.SRC_SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
    col("Missing.SRC_SUB_UNIQ_KEY").alias("SRC_SUB_UNIQ_KEY")
)

# Prepare row numbering for row-based constraints
w_TransformLogic = Window.orderBy(lit(1))
df_indexed_TransformLogic = df_Missing.withColumn("rownum", row_number().over(w_TransformLogic))

# Notify output
df_TransformLogic_Notify = df_indexed_TransformLogic.filter(
    (col("rownum") == 1) & (col("rownum") > lit(ToleranceCd).cast("int"))
).select(
    rpad(lit(NotifyMessage), 70, " ").alias("NOTIFICATION")
)

# ROW_CNT output
df_TransformLogic_ROW_CNT = df_indexed_TransformLogic.filter(col("rownum") == 1).select(
    col("SRC_SYS_CD.TRGT_CD").alias("SRC_SYS_CD")
)

# ----------------------------------------------------------------------------------------------------
# Stage: ResearchFile (CSeqFileStage) - write .dat file
researchFilePath = f"{adls_path}/balancing/research/{SrcTable}.{TrgtTable}.dat.{RunID}"
df_research_out = df_TransformLogic_Research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_SUB_UNIQ_KEY",
    "SRC_SRC_SYS_CD_SK",
    "SRC_SUB_UNIQ_KEY"
)
write_files(
    df_research_out,
    researchFilePath,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------------------------------
# Stage: ErrorNotificationFile (CSeqFileStage) - write .dat file
errorNotifyPath = f"{adls_path}/balancing/notify/ClaimBalancingNotification.dat"
df_error_notify_out = df_TransformLogic_Notify.select("NOTIFICATION")
write_files(
    df_error_notify_out,
    errorNotifyPath,
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------------------------------
# Stage: Copy_of_TransformLogic (CTransformerStage)
# We have stage variables to compute
svTlrnc = lit(ToleranceCd).cast("int")
svSrcCnt_val = GetSrcCount(IDSOwner, IDSDSN, IDSAcct, IDSPW, SrcTable)
svTrgtCnt_val = GetTrgtCount(IDSOwner, IDSDSN, IDSAcct, IDSPW, TrgtTable, ExtrRunCycle)
df_enriched_indexed = df_TransformLogic_ROW_CNT.withColumn("rownum", row_number().over(Window.orderBy(lit(1))))
df_enriched = (
    df_enriched_indexed.filter(col("rownum") == 1)
    .withColumn("svTlrnc", svTlrnc)
    .withColumn("svSrcCnt", lit(svSrcCnt_val))
    .withColumn("svTrgtCnt", lit(svTrgtCnt_val))
    .withColumn("Diff", col("svTrgtCnt") - col("svSrcCnt"))
    .withColumn("DiffAbs", spark_abs(col("Diff")))
    .withColumn(
        "svTlrncCd",
        when(col("Diff").eqNullSafe(lit(0)), lit("BAL"))
        .when(col("DiffAbs") > col("svTlrnc"), lit("OUT"))
        .otherwise(lit("IN"))
    )
    .withColumn("svCurDTM", current_timestamp())
)

# ROW_CNT_OUT
df_ROW_CNT_OUT = df_enriched.select(
    rpad(lit("IDS"), <...>, " ").alias("TRGT_SYS_CD"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(lit(SubjectArea), <...>, " ").alias("SUBJ_AREA_NM"),
    rpad(lit(TrgtTable), <...>, " ").alias("TRGT_TBL_NM"),
    col("svTrgtCnt").alias("TRGT_CT"),
    col("svSrcCnt").alias("SRC_CT"),
    col("Diff").alias("DIFF_CT"),
    rpad(col("svTlrncCd"), <...>, " ").alias("TLRNC_CD"),
    col("svTlrnc").alias("TLRNC_AMT"),
    col("svTlrnc").cast("decimal(38,10)").alias("TLRNC_MULT_AMT"),
    rpad(lit("DAILY"), <...>, " ").alias("FREQ_CD"),
    col("ExtrRunCycle").cast("int").alias("TRGT_RUN_CYC_NO"),
    rpad(lit(" "), <...>, " ").alias("CRCTN_NOTE"),
    col("svCurDTM").alias("CRT_DTM"),
    col("svCurDTM").alias("LAST_UPDT_DTM"),
    rpad(lit(UWSAcct), <...>, " ").alias("USER_ID")
)

# TBL_BAL_SUM
df_TBL_BAL_SUM = df_enriched.select(
    rpad(lit("IDS"), <...>, " ").alias("TRGT_SYS_CD"),
    rpad(lit(SubjectArea), <...>, " ").alias("SUBJ_AREA_NM"),
    rpad(lit(TrgtTable), <...>, " ").alias("TRGT_TBL_NM"),
    rpad(lit(CurrentDate), <...>, " ").alias("BAL_DT"),
    rpad(col("svTlrncCd"), <...>, " ").alias("ROW_CT_TLRNC_CD"),
    rpad(lit("NA"), <...>, " ").alias("CLMN_SUM_TLRNC_CD"),
    rpad(lit("NA"), <...>, " ").alias("ROW_TO_ROW_TLRNC_CD"),
    rpad(lit("NA"), <...>, " ").alias("RI_TLRNC_CD"),
    rpad(lit("NA"), <...>, " ").alias("RELSHP_CLMN_SUM_TLRNC_CD"),
    rpad(lit("NA"), <...>, " ").alias("CRS_FOOT_TLRNC_CD"),
    rpad(lit("NA"), <...>, " ").alias("HIST_RSNBL_TLRNC_CD"),
    col("ExtrRunCycle").cast("int").alias("TRGT_RUN_CYC_NO"),
    col("svCurDTM").alias("CRT_DTM"),
    col("svCurDTM").alias("LAST_UPDT_DTM"),
    rpad(lit(UWSAcct), <...>, " ").alias("USER_ID")
)

# ----------------------------------------------------------------------------------------------------
# Stage: TBL_BAL_SUM (CODBCStage) => write to #$UWSOwner#.TBL_BAL_SUM
jdbc_url_UWS, jdbc_props_UWS = get_db_config(uws_secret_name)
temp_table_name_tbl_bal_sum = "STAGING.AhySubRowCntBal_TBL_BAL_SUM_temp"
drop_sql_tbl_bal_sum = f"DROP TABLE IF EXISTS {temp_table_name_tbl_bal_sum}"
execute_dml(drop_sql_tbl_bal_sum, jdbc_url_UWS, jdbc_props_UWS)

df_TBL_BAL_SUM.write \
    .jdbc(url=jdbc_url_UWS, table=temp_table_name_tbl_bal_sum, mode="overwrite", properties=jdbc_props_UWS)

merge_sql_tbl_bal_sum = f"""
MERGE INTO {UWSOwner}.TBL_BAL_SUM AS T
USING {temp_table_name_tbl_bal_sum} AS S
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
execute_dml(merge_sql_tbl_bal_sum, jdbc_url_UWS, jdbc_props_UWS)

# ----------------------------------------------------------------------------------------------------
# Stage: ROW_CT_UPDATE_FILE (CSeqFileStage)
rowCtFilePath = f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat"
df_row_ct_out = df_ROW_CNT_OUT.select(
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
    df_row_ct_out,
    rowCtFilePath,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Also note the stage can read the same file in potential subsequent usage; we adhere to the instructions.

# OutputPin => ROW_CT_DTL => next stage "ROW_CT_DTL"
df_ROW_CT_DTL = df_row_ct_out.select(
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

# ----------------------------------------------------------------------------------------------------
# Stage: ROW_CT_DTL (CODBCStage)
temp_table_name_row_ct_dtl = "STAGING.AhySubRowCntBal_ROW_CT_DTL_temp"
drop_sql_row_ct_dtl = f"DROP TABLE IF EXISTS {temp_table_name_row_ct_dtl}"
execute_dml(drop_sql_row_ct_dtl, jdbc_url_UWS, jdbc_props_UWS)

df_ROW_CT_DTL.write \
    .jdbc(url=jdbc_url_UWS, table=temp_table_name_row_ct_dtl, mode="overwrite", properties=jdbc_props_UWS)

merge_sql_row_ct_dtl = f"""
MERGE INTO {UWSOwner}.ROW_CT_DTL AS T
USING {temp_table_name_row_ct_dtl} AS S
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
execute_dml(merge_sql_row_ct_dtl, jdbc_url_UWS, jdbc_props_UWS)