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
# MAGIC ^1_1 10/29/07 14:28:38 Batch  14547_52124 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/29/07 14:19:25 Batch  14547_51567 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/25/07 12:50:39 Batch  14543_46244 PROMOTE bckcett testIDS30 u10913 Ollie
# MAGIC ^1_1 10/25/07 12:49:05 Batch  14543_46147 INIT bckcett devlIDS30 u10913 Ollie
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
# MAGIC Oliver Nielsen                   09/22/2008    Prod Supp                     Added IDSDSN to job and to function
# MAGIC                                                                                                       call for GetSrcCnt and GetTrgtCnt             devlIDSnew                Steph Goddard              09/26/2008

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
TrgtTable = get_widget_value('TrgtTable','')
SrcTable = get_widget_value('SrcTable','')
NotifyMessage = get_widget_value('NotifyMessage','\\"ROW COUNT BALANCING FACETS - IDS CLAIM ATTACHMENT OUT OF TOLERANCE\\"')
UWSOwner = get_widget_value('UWSOwner','')
SubjectArea = get_widget_value('SubjectArea','Claims')
CurrentDate = get_widget_value('CurrentDate','2007-09-14 10:00:00')
ExtraColumn = get_widget_value('ExtraColumn','')
ids_secret_name = get_widget_value('ids_secret_name','')
uws_secret_name = get_widget_value('uws_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# -- Stage: Compare (DB2Connector) --------------------------------------------------
extract_query_Compare = f"""
SELECT
  TRGT.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
  TRGT.CLM_ID AS TRGT_CLM_ID,
  TRGT.CLM_LN_SEQ_NO AS TRGT_CLM_LN_SEQ_NO,
  SRC.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
  SRC.CLM_ID AS SRC_CLM_ID,
  SRC.CLM_LN_SEQ_NO AS SRC_CLM_LN_SEQ_NO,
  TRGT.{ExtraColumn} AS TRGT_EXTRA_COLUMN,
  SRC.{ExtraColumn} AS SRC_EXTRA_COLUMN
FROM {IDSOwner}.{TrgtTable} AS TRGT
FULL OUTER JOIN {IDSOwner}.{SrcTable} AS SRC
  ON TRGT.SRC_SYS_CD_SK = SRC.SRC_SYS_CD_SK
  AND TRGT.CLM_ID = SRC.CLM_ID
  AND TRGT.CLM_LN_SEQ_NO = SRC.CLM_LN_SEQ_NO
  AND SRC.{ExtraColumn} = TRGT.{ExtraColumn}
WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
"""

df_Compare = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Compare)
    .load()
)

# -- Stage: SRC_SYS_CD (CHashedFileStage) - Scenario C --------------------------------
df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

# -- Stage: TransformLogic (CTransformerStage) ---------------------------------------
# Join (Primary Link df_Compare as "Missing") with (Lookup Link df_SRC_SYS_CD as "SRC_SYS_CD")
df_missing_join = df_Compare.alias("Missing").join(
    df_SRC_SYS_CD.alias("SRC_SYS_CD"),
    F.col("Missing.TRGT_SRC_SYS_CD_SK") == F.col("SRC_SYS_CD.CD_MPPNG_SK"),
    "left"
)

window_spec_transformlogic = Window.orderBy(F.lit(1))
dfTransformLogic = df_missing_join.withColumn("_row_num", F.row_number().over(window_spec_transformlogic))

# Output link: Research
dfResearch = dfTransformLogic.filter(
    (F.col("Missing.TRGT_SRC_SYS_CD_SK").isNull()) |
    (F.col("Missing.TRGT_CLM_ID").isNull()) |
    (F.col("Missing.SRC_SRC_SYS_CD_SK").isNull()) |
    (F.col("Missing.SRC_CLM_ID").isNull()) |
    (F.col("Missing.TRGT_EXTRA_COLUMN").isNull()) |
    (F.col("Missing.SRC_EXTRA_COLUMN").isNull())
).select(
    F.col("Missing.TRGT_SRC_SYS_CD_SK").alias("TRGT_SRC_SYS_CD_SK"),
    F.col("Missing.TRGT_CLM_ID").alias("TRGT_CLM_ID"),
    F.col("Missing.TRGT_CLM_LN_SEQ_NO").alias("TRGT_CLM_LN_SEQ_NO"),
    F.col("Missing.SRC_SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
    F.col("Missing.SRC_CLM_ID").alias("SRC_CLM_ID"),
    F.col("Missing.SRC_CLM_LN_SEQ_NO").alias("SRC_CLM_LN_SEQ_NO"),
    F.col("Missing.TRGT_EXTRA_COLUMN").alias("TRGT_EXTRA_COLUMN"),
    F.col("Missing.SRC_EXTRA_COLUMN").alias("SRC_EXTRA_COLUMN")
)

# Output link: Notify
dfNotify = dfTransformLogic.filter(
    (F.col("_row_num") == 1) &
    (F.col("_row_num") > F.lit(ToleranceCd).cast("int"))
).select(
    F.lit(NotifyMessage).alias("NOTIFICATION")
)

# For the final output column (type char(70)), apply rpad:
dfNotify = dfNotify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))

# Output link: ROW_CNT
dfROW_CNT = dfTransformLogic.filter(F.col("_row_num") == 1).select(
    F.col("SRC_SYS_CD.TRGT_CD").alias("SRC_SYS_CD")
)

# -- Stage: ResearchFile (CSeqFileStage) ---------------------------------------------
write_files(
    dfResearch.select(
        "TRGT_SRC_SYS_CD_SK",
        "TRGT_CLM_ID",
        "TRGT_CLM_LN_SEQ_NO",
        "SRC_SRC_SYS_CD_SK",
        "SRC_CLM_ID",
        "SRC_CLM_LN_SEQ_NO",
        "TRGT_EXTRA_COLUMN",
        "SRC_EXTRA_COLUMN"
    ),
    f"{adls_path}/balancing/research/{SrcTable}.{TrgtTable}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# -- Stage: ErrorNotificationFile (CSeqFileStage) ------------------------------------
write_files(
    dfNotify.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/ClaimBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# -- Stage: Copy_of_TransformLogic (CTransformerStage) --------------------------------
window_spec_copytransform = Window.orderBy(F.lit(1))
dfCopyOfTransformLogic_input = dfROW_CNT.withColumn("_row_num", F.row_number().over(window_spec_copytransform))

df_enriched = (
    dfCopyOfTransformLogic_input
    .withColumn("svTlrnc", F.lit(ToleranceCd).cast("int"))
    .withColumn("svSrcCnt", F.lit(GetSrcCount(IDSOwner, <...>, <...>, <...>, SrcTable)))
    .withColumn("svTrgtCnt", F.lit(GetTrgtCount(IDSOwner, <...>, <...>, <...>, TrgtTable, ExtrRunCycle)))
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

dfROW_CNT_OUT = df_enriched.filter(F.col("_row_num") == 1).select(
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
    F.lit(ExtrRunCycle).cast("int").alias("TRGT_RUN_CYC_NO"),
    F.lit(" ").alias("CRCTN_NOTE"),
    current_timestamp().alias("CRT_DTM"),
    current_timestamp().alias("LAST_UPDT_DTM"),
    F.lit("<...>").alias("USER_ID")
)

dfTBL_BAL_SUM = df_enriched.filter(F.col("_row_num") == 1).select(
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
    F.lit(ExtrRunCycle).cast("int").alias("TRGT_RUN_CYC_NO"),
    current_timestamp().alias("CRT_DTM"),
    current_timestamp().alias("LAST_UPDT_DTM"),
    F.lit("<...>").alias("USER_ID")
)

# -- Stage: TBL_BAL_SUM (CODBCStage) - write via MERGE (always INSERT) ----------------
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)

execute_dml(f"DROP TABLE IF EXISTS STAGING.ClmLnOvrdRowCntBal_TBL_BAL_SUM_temp", jdbc_url_uws, jdbc_props_uws)

dfTBL_BAL_SUM.write \
    .format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", "STAGING.ClmLnOvrdRowCntBal_TBL_BAL_SUM_temp") \
    .mode("overwrite") \
    .save()

merge_sql_tbl_bal_sum = f"""
MERGE {UWSOwner}.TBL_BAL_SUM AS T
USING STAGING.ClmLnOvrdRowCntBal_TBL_BAL_SUM_temp AS S
ON (1=0)
WHEN NOT MATCHED THEN
  INSERT
  (
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
  VALUES
  (
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

# -- Stage: ROW_CT_UPDATE_FILE (CSeqFileStage) ----------------------------------------
dfROW_CT_UPDATE_FILE = dfROW_CNT_OUT.select(
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
    dfROW_CT_UPDATE_FILE,
    f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Read back the same .dat file for the next stage
schema_ROW_CT_UPDATE_FILE_read = StructType([
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

dfROW_CT_UPDATE_FILE_read = (
    spark.read
    .schema(schema_ROW_CT_UPDATE_FILE_read)
    .option("header", False)
    .option("quote", "\"")
    .option("sep", ",")
    .csv(f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat")
)

# -- Stage: ROW_CT_DTL (CODBCStage) - write via MERGE (always INSERT) -----------------
execute_dml(f"DROP TABLE IF EXISTS STAGING.ClmLnOvrdRowCntBal_ROW_CT_DTL_temp", jdbc_url_uws, jdbc_props_uws)

dfROW_CT_UPDATE_FILE_read.write \
    .format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", "STAGING.ClmLnOvrdRowCntBal_ROW_CT_DTL_temp") \
    .mode("overwrite") \
    .save()

merge_sql_row_ct_dtl = f"""
MERGE {UWSOwner}.ROW_CT_DTL AS T
USING STAGING.ClmLnOvrdRowCntBal_ROW_CT_DTL_temp AS S
ON (1=0)
WHEN NOT MATCHED THEN
  INSERT
  (
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
  VALUES
  (
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