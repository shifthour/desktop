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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# PARAMETERS
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
TrgtTable = get_widget_value('TrgtTable','')
SrcTable = get_widget_value('SrcTable','')
NotifyMessage = get_widget_value('NotifyMessage','\\"ROW COUNT BALANCING FACETS - IDS APPEAL LEVEL RVWR OUT OF TOLERANCE\\"')
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
SubjectArea = get_widget_value('SubjectArea','Appeals')
CurrentDate = get_widget_value('CurrentDate','2007-09-14 10:00:00')

# STAGE 1: Compare (DB2Connector) - Read from IDS database
jdbc_url_compare, jdbc_props_compare = get_db_config(ids_secret_name)
extract_query_Compare = (
    f"SELECT TRGT.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK, TRGT.APL_ID as TRGT_APL_ID, "
    f"SRC.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK, SRC.APL_ID as SRC_APL_ID, "
    f"TRGT.APL_LVL_SEQ_NO as TRGT_APL_LVL_SEQ_NO, TRGT.APL_RVWR_ID as TRGT_APL_RVWR_ID, "
    f"SRC.APL_LVL_SEQ_NO as SRC_APL_LVL_SEQ_NO, SRC.APL_RVWR_ID as SRC_APL_RVWR_ID, "
    f"TRGT.EFF_DT_SK as TRGT_EFF_DT_SK, SRC.EFF_DT_SK as SRC_EFF_DT_SK "
    f"FROM {IDSOwner}.{TrgtTable} AS TRGT FULL OUTER JOIN {IDSOwner}.{SrcTable} AS SRC "
    f"ON TRGT.SRC_SYS_CD_SK = SRC.SRC_SYS_CD_SK "
    f"AND TRGT.APL_ID = SRC.APL_ID "
    f"AND TRGT.APL_LVL_SEQ_NO = SRC.APL_LVL_SEQ_NO "
    f"AND TRGT.APL_RVWR_ID = SRC.APL_RVWR_ID "
    f"AND TRGT.EFF_DT_SK = SRC.EFF_DT_SK "
    f"WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}"
)
df_Compare = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_compare)
    .options(**jdbc_props_compare)
    .option("query", extract_query_Compare)
    .load()
)

# STAGE 2: SRC_SYS_CD (CHashedFileStage) - Scenario C => read from parquet
df_SRC_SYS_CD = (
    spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")
    .select(
        "CD_MPPNG_SK",
        "TRGT_CD",
        "TRGT_CD_NM",
        "SRC_CD",
        "SRC_CD_NM"
    )
)

# STAGE 3: TransformLogic (CTransformerStage)
# Join df_Compare (alias "Missing") with df_SRC_SYS_CD (alias "SRC_SYS_CD") - left join
ToleranceCdInt = F.lit(F.col("t").cast("int"))  # placeholder usage below; corrected further down
# Instead, parse ToleranceCd once as an integer:
try:
    ToleranceCdIntValue = int(ToleranceCd)
except:
    ToleranceCdIntValue = 0  # fallback if not integer

df_TransformLogic_Enriched = (
    df_Compare.alias("Missing")
    .join(df_SRC_SYS_CD.alias("SRC_SYS_CD"),
          F.col("Missing.TRGT_SRC_SYS_CD_SK") == F.col("SRC_SYS_CD.CD_MPPNG_SK"),
          "left")
    .withColumn("_row_num", F.row_number().over(Window.orderBy(F.lit(1))))
)

# 3a) Research link
df_Research = df_TransformLogic_Enriched.filter(
    F.col("Missing.TRGT_SRC_SYS_CD_SK").isNull()
    | F.col("Missing.TRGT_APL_ID").isNull()
    | F.col("Missing.SRC_SRC_SYS_CD_SK").isNull()
    | F.col("Missing.SRC_APL_ID").isNull()
    | F.col("Missing.TRGT_APL_LVL_SEQ_NO").isNull()
    | F.col("Missing.SRC_APL_LVL_SEQ_NO").isNull()
    | F.col("Missing.TRGT_APL_RVWR_ID").isNull()
    | F.col("Missing.SRC_APL_RVWR_ID").isNull()
    | F.col("Missing.TRGT_EFF_DT_SK").isNull()
    | F.col("Missing.SRC_EFF_DT_SK").isNull()
).select(
    F.col("Missing.TRGT_SRC_SYS_CD_SK").alias("TRGT_SRC_SYS_CD_SK"),
    F.col("Missing.TRGT_APL_ID").alias("TRGT_APL_ID"),
    F.col("Missing.SRC_SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
    F.col("Missing.SRC_APL_ID").alias("SRC_APL_ID"),
    F.col("Missing.TRGT_APL_LVL_SEQ_NO").alias("TRGT_APL_LVL_SEQ_NO"),
    F.col("Missing.TRGT_APL_RVWR_ID").alias("TRGT_APL_RVWR_ID"),
    F.col("Missing.SRC_APL_LVL_SEQ_NO").alias("SRC_APL_LVL_SEQ_NO"),
    F.col("Missing.SRC_APL_RVWR_ID").alias("SRC_APL_RVWR_ID"),
    # TRGT_EFF_DT_SK char(10)
    F.rpad(F.col("Missing.TRGT_EFF_DT_SK").cast(StringType()), 10, " ").alias("TRGT_EFF_DT_SK"),
    # SRC_EFF_DT_SK char(10)
    F.rpad(F.col("Missing.SRC_EFF_DT_SK").cast(StringType()), 10, " ").alias("SRC_EFF_DT_SK")
)

# 3b) Notify link
df_Notify = df_TransformLogic_Enriched.filter(
    (F.col("_row_num") == 1) & (F.lit(1) > F.lit(ToleranceCdIntValue))
).select(
    F.rpad(F.lit(NotifyMessage), 70, " ").alias("NOTIFICATION")
)

# 3c) ROW_CNT link
df_ROW_CNT = df_TransformLogic_Enriched.filter(
    F.col("_row_num") == 1
).select(
    F.col("SRC_SYS_CD.TRGT_CD").alias("SRC_SYS_CD")
)

# STAGE 4: ResearchFile (CSeqFileStage) - write df_Research
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

# STAGE 5: ErrorNotificationFile (CSeqFileStage) - write df_Notify
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

# STAGE 6: TransformLogic1 (CTransformerStage)
# Define stage variables as Python variables
try:
    svTlrnc = int(ToleranceCd)
except:
    svTlrnc = 0
svSrcCnt = GetSrcCount(IDSOwner, <...>, <...>, <...>, SrcTable)
svTrgtCnt = GetTrgtCount(IDSOwner, <...>, <...>, <...>, TrgtTable, ExtrRunCycle)
Diff = svTrgtCnt - svSrcCnt
absDiff = abs(Diff)
if Diff == 0:
    svTlrncCd = "BAL"
elif absDiff > svTlrnc:
    svTlrncCd = "OUT"
else:
    svTlrncCd = "IN"

df_TransformLogic1_in = df_ROW_CNT.withColumn(
    "_row_num", F.row_number().over(Window.orderBy(F.lit(1)))
)

df_TransformLogic1_filtered = df_TransformLogic1_in.filter(F.col("_row_num") == 1)

# 6a) ROW_CNT_OUT link
df_ROW_CNT_OUT = df_TransformLogic1_filtered.select(
    # All columns as per output definition, applying rpad if varchar/char length known or <...> if unknown
    F.rpad(F.lit("IDS"), <...>, " ").alias("TRGT_SYS_CD"),
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.lit(SubjectArea), <...>, " ").alias("SUBJ_AREA_NM"),
    F.rpad(F.lit(TrgtTable), <...>, " ").alias("TRGT_TBL_NM"),
    F.col(F.lit(svTrgtCnt)).alias("TRGT_CT"),
    F.col(F.lit(svSrcCnt)).alias("SRC_CT"),
    F.col(F.lit(Diff)).alias("DIFF_CT"),
    F.rpad(F.lit(svTlrncCd), <...>, " ").alias("TLRNC_CD"),
    F.col(F.lit(svTlrnc)).alias("TLRNC_AMT"),
    F.col(F.lit(svTlrnc)).alias("TLRNC_MULT_AMT"),
    F.rpad(F.lit("DAILY"), <...>, " ").alias("FREQ_CD"),
    F.col(F.lit(ExtrRunCycle)).alias("TRGT_RUN_CYC_NO"),
    F.rpad(F.lit(" "), <...>, " ").alias("CRCTN_NOTE"),
    F.lit(CurrentDate).cast(TimestampType()).alias("CRT_DTM"),
    F.lit(CurrentDate).cast(TimestampType()).alias("LAST_UPDT_DTM"),
    F.rpad(F.lit("<...>"), <...>, " ").alias("USER_ID")
)

# 6b) TBL_BAL_SUM link
df_TBL_BAL_SUM = df_TransformLogic1_filtered.select(
    F.rpad(F.lit("IDS"), <...>, " ").alias("TRGT_SYS_CD"),
    F.rpad(F.lit(SubjectArea), <...>, " ").alias("SUBJ_AREA_NM"),
    F.rpad(F.lit(TrgtTable), <...>, " ").alias("TRGT_TBL_NM"),
    F.lit(CurrentDate).cast(TimestampType()).alias("BAL_DT"),
    F.rpad(F.lit(svTlrncCd), <...>, " ").alias("ROW_CT_TLRNC_CD"),
    F.rpad(F.lit("NA"), <...>, " ").alias("CLMN_SUM_TLRNC_CD"),
    F.rpad(F.lit("NA"), <...>, " ").alias("ROW_TO_ROW_TLRNC_CD"),
    F.rpad(F.lit("NA"), <...>, " ").alias("RI_TLRNC_CD"),
    F.rpad(F.lit("NA"), <...>, " ").alias("RELSHP_CLMN_SUM_TLRNC_CD"),
    F.rpad(F.lit("NA"), <...>, " ").alias("CRS_FOOT_TLRNC_CD"),
    F.rpad(F.lit("NA"), <...>, " ").alias("HIST_RSNBL_TLRNC_CD"),
    F.col(F.lit(ExtrRunCycle)).alias("TRGT_RUN_CYC_NO"),
    F.lit(CurrentDate).cast(TimestampType()).alias("CRT_DTM"),
    F.lit(CurrentDate).cast(TimestampType()).alias("LAST_UPDT_DTM"),
    F.rpad(F.lit("<...>"), <...>, " ").alias("USER_ID")
)

# STAGE 7: TBL_BAL_SUM (CODBCStage) => Insert into #$UWSOwner#.TBL_BAL_SUM
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)

spark.sql(f"DROP TABLE IF EXISTS STAGING.AplLvlRvwrRowCntBal_TBL_BAL_SUM_temp")
df_TBL_BAL_SUM.write \
    .mode("overwrite") \
    .format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", "STAGING.AplLvlRvwrRowCntBal_TBL_BAL_SUM_temp") \
    .save()

insert_sql_tbl_bal_sum = (
    f"INSERT INTO {UWSOwner}.TBL_BAL_SUM "
    f"(TRGT_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, BAL_DT, ROW_CT_TLRNC_CD, CLMN_SUM_TLRNC_CD, ROW_TO_ROW_TLRNC_CD, RI_TLRNC_CD, RELSHP_CLMN_SUM_TLRNC_CD, CRS_FOOT_TLRNC_CD, HIST_RSNBL_TLRNC_CD, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID) "
    f"SELECT TRGT_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, BAL_DT, ROW_CT_TLRNC_CD, CLMN_SUM_TLRNC_CD, ROW_TO_ROW_TLRNC_CD, RI_TLRNC_CD, RELSHP_CLMN_SUM_TLRNC_CD, CRS_FOOT_TLRNC_CD, HIST_RSNBL_TLRNC_CD, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID "
    f"FROM STAGING.AplLvlRvwrRowCntBal_TBL_BAL_SUM_temp"
)
execute_dml(insert_sql_tbl_bal_sum, jdbc_url_uws, jdbc_props_uws)

# STAGE 8: ROW_CT_UPDATE_FILE (CSeqFileStage)
# 8a) Write df_ROW_CNT_OUT to balancing/<SrcTable><TrgtTable>RowCnt.dat
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

# 8b) Read that same file back into a dataframe
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
    StructField("TLRNC_MULT_AMT", DecimalType(38,10), False),
    StructField("FREQ_CD", StringType(), False),
    StructField("TRGT_RUN_CYC_NO", IntegerType(), False),
    StructField("CRCTN_NOTE", StringType(), True),
    StructField("CRT_DTM", TimestampType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("USER_ID", StringType(), False)
])

df_ROW_CT_DTL_input = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_ROW_CT_UPDATE_FILE)
    .csv(f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat")
)

# STAGE 9: ROW_CT_DTL (CODBCStage) => Insert into #$UWSOwner#.ROW_CT_DTL
spark.sql(f"DROP TABLE IF EXISTS STAGING.AplLvlRvwrRowCntBal_ROW_CT_DTL_temp")
df_ROW_CT_DTL_input.write \
    .mode("overwrite") \
    .format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", "STAGING.AplLvlRvwrRowCntBal_ROW_CT_DTL_temp") \
    .save()

insert_sql_row_ct_dtl = (
    f"INSERT INTO {UWSOwner}.ROW_CT_DTL "
    f"(TRGT_SYS_CD, SRC_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, TRGT_CT, SRC_CT, DIFF_CT, TLRNC_CD, TLRNC_AMT, TLRNC_MULT_AMT, FREQ_CD, TRGT_RUN_CYC_NO, CRCTN_NOTE, CRT_DTM, LAST_UPDT_DTM, USER_ID) "
    f"SELECT TRGT_SYS_CD, SRC_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, TRGT_CT, SRC_CT, DIFF_CT, TLRNC_CD, TLRNC_AMT, TLRNC_MULT_AMT, FREQ_CD, TRGT_RUN_CYC_NO, CRCTN_NOTE, CRT_DTM, LAST_UPDT_DTM, USER_ID "
    f"FROM STAGING.AplLvlRvwrRowCntBal_ROW_CT_DTL_temp"
)
execute_dml(insert_sql_row_ct_dtl, jdbc_url_uws, jdbc_props_uws)