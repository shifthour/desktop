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
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC ======================================================================================================================================
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ====================================================================================================================================== 
# MAGIC Manasa Andru              20174-01-30           TFS - 2626                    Originally Programmed                            IntegrateNewDevl        Kalyan Neelam             2014-02-03

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


# Retrieve parameter values
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
CurrentDate = get_widget_value('CurrentDate','')

# Compare Stage (DB2Connector - read from IDS)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_compare = (
    f"SELECT "
    f"TRGT.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK,"
    f"TRGT.CLM_ID as TRGT_CLM_ID,"
    f"TRGT.CLM_LN_SEQ_NO as TRGT_CLM_LN_SEQ_NO,"
    f"SRC.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK,"
    f"SRC.CLM_ID as SRC_CLM_ID,"
    f"SRC.CLM_LN_SEQ_NO as SRC_CLM_LN_SEQ_NO "
    f"FROM {IDSOwner}.{TrgtTable} AS TRGT "
    f"FULL OUTER JOIN {IDSOwner}.{SrcTable} AS SRC "
    f"ON TRGT.SRC_SYS_CD_SK=SRC.SRC_SYS_CD_SK "
    f"AND TRGT.CLM_ID = SRC.CLM_ID "
    f"AND TRGT.CLM_LN_SEQ_NO = SRC.CLM_LN_SEQ_NO "
    f"WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle} "
    f"ORDER BY SRC.SRC_SYS_CD_SK"
)
df_Compare = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_compare)
    .load()
)

# SRC_SYS_CD (CHashedFileStage) - scenario C => read from HF as parquet
df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

# TransformLogic (CTransformerStage) - join and produce 3 outputs: Research, Notify, and ROW_CNT
toleranceCd_int = int(ToleranceCd) if ToleranceCd else 0
df_joined = df_Compare.alias("Missing").join(
    df_SRC_SYS_CD.alias("SRC_SYS_CD"),
    F.col("Missing.TRGT_SRC_SYS_CD_SK") == F.col("SRC_SYS_CD.CD_MPPNG_SK"),
    "left"
)
windowSpec = Window.orderBy(F.lit(1))
df_joined = df_joined.withColumn("row_number", F.row_number().over(windowSpec))

# Research output
df_Research = df_joined.filter(
    F.col("Missing.TRGT_SRC_SYS_CD_SK").isNull() |
    F.col("Missing.TRGT_CLM_ID").isNull() |
    F.col("Missing.TRGT_CLM_LN_SEQ_NO").isNull() |
    F.col("Missing.SRC_SRC_SYS_CD_SK").isNull() |
    F.col("Missing.SRC_CLM_ID").isNull() |
    F.col("Missing.SRC_CLM_LN_SEQ_NO").isNull()
).select(
    F.col("Missing.TRGT_SRC_SYS_CD_SK").alias("TRGT_SRC_SYS_CD_SK"),
    F.col("Missing.TRGT_CLM_ID").alias("TRGT_CLM_ID"),
    F.col("Missing.TRGT_CLM_LN_SEQ_NO").alias("TRGT_CLM_LN_SEQ_NO"),
    F.col("Missing.SRC_SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
    F.col("Missing.SRC_CLM_ID").alias("SRC_CLM_ID"),
    F.col("Missing.SRC_CLM_LN_SEQ_NO").alias("SRC_CLM_LN_SEQ_NO")
)

# Notify output
df_Notify = df_joined.filter(
    (F.col("row_number") == 1) & (F.col("row_number") > F.lit(toleranceCd_int))
).select(
    F.lit(NotifyMessage).alias("NOTIFICATION")
)
# For the NOTIFICATION field which is char(70):
df_Notify = df_Notify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))

# ROW_CNT output
df_ROW_CNT = df_joined.filter(
    F.col("row_number") == 1
).select(
    F.col("SRC_SYS_CD.TRGT_CD").alias("SRC_SYS_CD")
)

# ResearchFile (CSeqFileStage) - write
write_files(
    df_Research.select(
        "TRGT_SRC_SYS_CD_SK",
        "TRGT_CLM_ID",
        "TRGT_CLM_LN_SEQ_NO",
        "SRC_SRC_SYS_CD_SK",
        "SRC_CLM_ID",
        "SRC_CLM_LN_SEQ_NO"
    ),
    f"{adls_path}/balancing/research/{SrcTable}.{TrgtTable}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ErrorNotificationFile (CSeqFileStage) - write
write_files(
    df_Notify.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/DentalClaimLineBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Transform (CTransformerStage)
# Stage variables as columns
df_stage = df_ROW_CNT.alias("ROW_CNT")

windowSpec2 = Window.orderBy(F.lit(1))
df_stage = df_stage.withColumn("row_number", F.row_number().over(windowSpec2))

df_stage = df_stage.withColumn("svTlrnc", F.lit(ToleranceCd))
df_stage = df_stage.withColumn("svSrcCnt", F.lit(GetSrcCount(IDSOwner, <...>, <...>, <...>, F.lit(SrcTable))))
df_stage = df_stage.withColumn("svTrgtCnt", F.lit(GetTrgtCount(IDSOwner, <...>, <...>, <...>, F.lit(TrgtTable), F.lit(ExtrRunCycle))))
df_stage = df_stage.withColumn("Diff", F.col("svTrgtCnt") - F.col("svSrcCnt"))
df_stage = df_stage.withColumn(
    "svTlrncCd",
    F.when(F.col("Diff") == 0, F.lit("BAL"))
     .otherwise(
       F.when(F.abs(F.col("Diff")) > F.col("svTlrnc"), F.lit("OUT"))
        .otherwise(F.lit("IN"))
     )
)

df_ROW_CNT_OUT = df_stage.filter(F.col("row_number") == 1).select(
    F.lit("IDS").alias("TRGT_SYS_CD"),
    F.col("ROW_CNT.SRC_SYS_CD").alias("SRC_SYS_CD"),
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
    current_timestamp().alias("CRT_DTM"),
    current_timestamp().alias("LAST_UPDT_DTM"),
    F.lit(get_widget_value('UWSAcct','')).alias("USER_ID")
)

df_TBL_BAL_SUM = df_stage.filter(F.col("row_number") == 1).select(
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
    current_timestamp().alias("CRT_DTM"),
    current_timestamp().alias("LAST_UPDT_DTM"),
    F.lit(get_widget_value('UWSAcct','')).alias("USER_ID")
)

# TBL_BAL_SUM (CODBCStage) => Insert (via MERGE with no PK => ON 1=0)
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)
execute_dml(f"DROP TABLE IF EXISTS STAGING.DntlClmLnRowCntBal_TBL_BAL_SUM_temp", jdbc_url_uws, jdbc_props_uws)
df_TBL_BAL_SUM.write.jdbc(
    url=jdbc_url_uws,
    table="STAGING.DntlClmLnRowCntBal_TBL_BAL_SUM_temp",
    mode="overwrite",
    properties=jdbc_props_uws
)
merge_sql_tbl_bal_sum = f"""
MERGE INTO {UWSOwner}.TBL_BAL_SUM AS T
USING STAGING.DntlClmLnRowCntBal_TBL_BAL_SUM_temp AS S
ON 1=0
WHEN NOT MATCHED THEN
INSERT (
  T.TRGT_SYS_CD,
  T.SUBJ_AREA_NM,
  T.TRGT_TBL_NM,
  T.BAL_DT,
  T.ROW_CT_TLRNC_CD,
  T.CLMN_SUM_TLRNC_CD,
  T.ROW_TO_ROW_TLRNC_CD,
  T.RI_TLRNC_CD,
  T.RELSHP_CLMN_SUM_TLRNC_CD,
  T.CRS_FOOT_TLRNC_CD,
  T.HIST_RSNBL_TLRNC_CD,
  T.TRGT_RUN_CYC_NO,
  T.CRT_DTM,
  T.LAST_UPDT_DTM,
  T.USER_ID
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

# ROW_CT_UPDATE_FILE (CSeqFileStage) => write ROW_CNT_OUT to file
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
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Read the same file back (schema required)
row_ct_update_schema = StructType([
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
df_ROW_CT_UPDATE_FILE = spark.read.csv(
    path=f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat",
    schema=row_ct_update_schema,
    sep=",",
    header=False,
    quote="\"",
    inferSchema=False
)

# ROW_CT_DTL (CODBCStage) => Insert (via MERGE with no PK => ON 1=0)
execute_dml(f"DROP TABLE IF EXISTS STAGING.DntlClmLnRowCntBal_ROW_CT_DTL_temp", jdbc_url_uws, jdbc_props_uws)
df_ROW_CT_UPDATE_FILE.write.jdbc(
    url=jdbc_url_uws,
    table="STAGING.DntlClmLnRowCntBal_ROW_CT_DTL_temp",
    mode="overwrite",
    properties=jdbc_props_uws
)
merge_sql_row_ct_dtl = f"""
MERGE INTO {UWSOwner}.ROW_CT_DTL AS T
USING STAGING.DntlClmLnRowCntBal_ROW_CT_DTL_temp AS S
ON 1=0
WHEN NOT MATCHED THEN
INSERT (
  T.TRGT_SYS_CD,
  T.SRC_SYS_CD,
  T.SUBJ_AREA_NM,
  T.TRGT_TBL_NM,
  T.TRGT_CT,
  T.SRC_CT,
  T.DIFF_CT,
  T.TLRNC_CD,
  T.TLRNC_AMT,
  T.TLRNC_MULT_AMT,
  T.FREQ_CD,
  T.TRGT_RUN_CYC_NO,
  T.CRCTN_NOTE,
  T.CRT_DTM,
  T.LAST_UPDT_DTM,
  T.USER_ID
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