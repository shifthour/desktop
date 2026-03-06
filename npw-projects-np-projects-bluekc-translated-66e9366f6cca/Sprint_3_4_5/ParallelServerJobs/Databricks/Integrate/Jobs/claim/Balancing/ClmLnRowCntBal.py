# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
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
# MAGIC ^1_1 10/25/07 00:02:22 Batch  14543_149 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/25/07 00:01:03 Batch  14543_71 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/24/07 23:58:49 Batch  14542_86339 PROMOTE bckcett testIDS30 u10913 Ollie
# MAGIC ^1_1 10/24/07 23:57:41 Batch  14542_86263 INIT bckcett devlIDS30 u10913 Ollie
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
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Oliver Nielsen                  10/13/2007          3264                              Originally Programmed                           devlIDS30                   
# MAGIC Oliver Nielsen                  9/26/2008            Prod Supp                  Added IDSDSN to GetSrcCnt and
# MAGIC                                                                                                        GetTrgtCnt Routine calls                           devlIDSnew                 Steph Goddard             09/26/2008
# MAGIC Karthik Chintalaphani       09/01/2012       4784                           Added Order By clause to the 
# MAGIC                                                                                                       source query                                            IntegrateWrhsDevl        Bhoomi Dasari         10/20/2012

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
from pyspark.sql.functions import col, lit, when, abs, row_number, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
TrgtTable = get_widget_value('TrgtTable','')
SrcTable = get_widget_value('SrcTable','')
NotifyMessage = get_widget_value('NotifyMessage','')
UWSOwner = get_widget_value('$UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
SubjectArea = get_widget_value('SubjectArea','')
CurrentDate = get_widget_value('CurrentDate','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT TRGT.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK,
       TRGT.CLM_ID as TRGT_CLM_ID,
       SRC.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK,
       SRC.CLM_ID as SRC_CLM_ID
FROM {IDSOwner}.{TrgtTable} AS TRGT
FULL OUTER JOIN {IDSOwner}.{SrcTable} AS SRC
  ON TRGT.SRC_SYS_CD_SK = SRC.SRC_SYS_CD_SK
  AND TRGT.CLM_ID = SRC.CLM_ID
  AND TRGT.CLM_LN_SEQ_NO = SRC.CLM_LN_SEQ_NO
WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}
ORDER BY SRC.SRC_SYS_CD_SK
"""
df_Compare = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_Missing = df_Compare

df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

w_TransformLogic = Window.orderBy(lit(1))
df_TransformLogic_joined = df_Missing.alias("Missing").join(
    df_SRC_SYS_CD.alias("SRC_SYS_CD"),
    col("Missing.TRGT_SRC_SYS_CD_SK") == col("SRC_SYS_CD.CD_MPPNG_SK"),
    "left"
).withColumn("_row_num", row_number().over(w_TransformLogic))

df_Research = df_TransformLogic_joined.filter(
    (col("Missing.TRGT_SRC_SYS_CD_SK").isNull())
    | (col("Missing.TRGT_CLM_ID").isNull())
    | (col("Missing.SRC_SRC_SYS_CD_SK").isNull())
    | (col("Missing.SRC_CLM_ID").isNull())
).select(
    col("Missing.TRGT_SRC_SYS_CD_SK").alias("TRGT_SRC_SYS_CD_SK"),
    col("Missing.TRGT_CLM_ID").alias("TRGT_CLM_ID"),
    col("Missing.SRC_SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
    col("Missing.SRC_CLM_ID").alias("SRC_CLM_ID")
)

df_Notify = df_TransformLogic_joined.filter(
    (col("_row_num") == 1) & (col("_row_num") > lit(ToleranceCd).cast("int"))
).select(
    rpad(lit(NotifyMessage), 70, " ").alias("NOTIFICATION")
)

df_ROW_CNT = df_TransformLogic_joined.filter(col("_row_num") == 1).select(
    col("SRC_SYS_CD.TRGT_CD").alias("SRC_SYS_CD")
)

df_Research_write = df_Research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_CLM_ID",
    "SRC_SRC_SYS_CD_SK",
    "SRC_CLM_ID"
)
write_files(
    df_Research_write,
    f"{adls_path}/balancing/research/{SrcTable}.{TrgtTable}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Notify_write = df_Notify.select(
    rpad(col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)
write_files(
    df_Notify_write,
    f"{adls_path}/balancing/notify/ClaimBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

w_CopyLogic = Window.orderBy(lit(1))
df_Copy_of_TransformLogic_pre = df_ROW_CNT.withColumn("_row_num", row_number().over(w_CopyLogic))
df_Copy_of_TransformLogic = df_Copy_of_TransformLogic_pre \
    .withColumn("svTlrnc", lit(ToleranceCd).cast("int")) \
    .withColumn("svSrcCnt", GetSrcCount(lit(IDSOwner), lit("<...>"), lit("<...>"), lit("<...>"), lit(SrcTable))) \
    .withColumn("svTrgtCnt", GetTrgtCount(lit(IDSOwner), lit("<...>"), lit("<...>"), lit("<...>"), lit(TrgtTable), lit(ExtrRunCycle))) \
    .withColumn("Diff", col("svTrgtCnt") - col("svSrcCnt")) \
    .withColumn("svTlrncCd",
                when(col("Diff") == 0, lit("BAL"))
                .when(abs(col("Diff")) > col("svTlrnc"), lit("OUT"))
                .otherwise(lit("IN")))

df_ROW_CNT_OUT = df_Copy_of_TransformLogic.filter(col("_row_num") == 1).select(
    rpad(lit("IDS"), 30, " ").alias("TRGT_SYS_CD"),
    rpad(col("SRC_SYS_CD"), 30, " ").alias("SRC_SYS_CD"),
    rpad(lit(SubjectArea), 30, " ").alias("SUBJ_AREA_NM"),
    rpad(lit(TrgtTable), 30, " ").alias("TRGT_TBL_NM"),
    col("svTrgtCnt").alias("TRGT_CT"),
    col("svSrcCnt").alias("SRC_CT"),
    col("Diff").alias("DIFF_CT"),
    rpad(col("svTlrncCd"), 30, " ").alias("TLRNC_CD"),
    col("svTlrnc").alias("TLRNC_AMT"),
    col("svTlrnc").alias("TLRNC_MULT_AMT"),
    rpad(lit("DAILY"), 30, " ").alias("FREQ_CD"),
    lit(ExtrRunCycle).cast("int").alias("TRGT_RUN_CYC_NO"),
    rpad(lit(" "), 30, " ").alias("CRCTN_NOTE"),
    current_timestamp().alias("CRT_DTM"),
    current_timestamp().alias("LAST_UPDT_DTM"),
    rpad(lit("<...>"), 30, " ").alias("USER_ID")
)

df_TBL_BAL_SUM = df_Copy_of_TransformLogic.filter(col("_row_num") == 1).select(
    rpad(lit("IDS"), 30, " ").alias("TRGT_SYS_CD"),
    rpad(lit(SubjectArea), 30, " ").alias("SUBJ_AREA_NM"),
    rpad(lit(TrgtTable), 30, " ").alias("TRGT_TBL_NM"),
    current_date().alias("BAL_DT"),
    rpad(col("svTlrncCd"), 30, " ").alias("ROW_CT_TLRNC_CD"),
    rpad(lit("NA"), 30, " ").alias("CLMN_SUM_TLRNC_CD"),
    rpad(lit("NA"), 30, " ").alias("ROW_TO_ROW_TLRNC_CD"),
    rpad(lit("NA"), 30, " ").alias("RI_TLRNC_CD"),
    rpad(lit("NA"), 30, " ").alias("RELSHP_CLMN_SUM_TLRNC_CD"),
    rpad(lit("NA"), 30, " ").alias("CRS_FOOT_TLRNC_CD"),
    rpad(lit("NA"), 30, " ").alias("HIST_RSNBL_TLRNC_CD"),
    lit(ExtrRunCycle).cast("int").alias("TRGT_RUN_CYC_NO"),
    current_timestamp().alias("CRT_DTM"),
    current_timestamp().alias("LAST_UPDT_DTM"),
    rpad(lit("<...>"), 30, " ").alias("USER_ID")
)

jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)
execute_dml(f"DROP TABLE IF EXISTS STAGING.ClmLnRowCntBal_TBL_BAL_SUM_temp", jdbc_url_uws, jdbc_props_uws)
df_TBL_BAL_SUM.write.format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", "STAGING.ClmLnRowCntBal_TBL_BAL_SUM_temp") \
    .mode("overwrite") \
    .save()
merge_sql_tbl_bal_sum = f"""
MERGE INTO {UWSOwner}.TBL_BAL_SUM AS T
USING STAGING.ClmLnRowCntBal_TBL_BAL_SUM_temp AS S
ON 1=0
WHEN NOT MATCHED THEN
INSERT (TRGT_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, BAL_DT, ROW_CT_TLRNC_CD, CLMN_SUM_TLRNC_CD, ROW_TO_ROW_TLRNC_CD, RI_TLRNC_CD, RELSHP_CLMN_SUM_TLRNC_CD, CRS_FOOT_TLRNC_CD, HIST_RSNBL_TLRNC_CD, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID)
VALUES (S.TRGT_SYS_CD, S.SUBJ_AREA_NM, S.TRGT_TBL_NM, S.BAL_DT, S.ROW_CT_TLRNC_CD, S.CLMN_SUM_TLRNC_CD, S.ROW_TO_ROW_TLRNC_CD, S.RI_TLRNC_CD, S.RELSHP_CLMN_SUM_TLRNC_CD, S.CRS_FOOT_TLRNC_CD, S.HIST_RSNBL_TLRNC_CD, S.TRGT_RUN_CYC_NO, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID);
"""
execute_dml(merge_sql_tbl_bal_sum, jdbc_url_uws, jdbc_props_uws)

df_ROW_CT_OUT_write = df_ROW_CNT_OUT.select(
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
    df_ROW_CT_OUT_write,
    f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

schema_ROW_CT_UPDATE_FILE = StructType([
    StructField("TRGT_SYS_CD", StringType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("SUBJ_AREA_NM", StringType(), nullable=False),
    StructField("TRGT_TBL_NM", StringType(), nullable=False),
    StructField("TRGT_CT", IntegerType(), nullable=False),
    StructField("SRC_CT", IntegerType(), nullable=False),
    StructField("DIFF_CT", IntegerType(), nullable=False),
    StructField("TLRNC_CD", StringType(), nullable=False),
    StructField("TLRNC_AMT", IntegerType(), nullable=False),
    StructField("TLRNC_MULT_AMT", DecimalType(38, 10), nullable=False),
    StructField("FREQ_CD", StringType(), nullable=False),
    StructField("TRGT_RUN_CYC_NO", IntegerType(), nullable=False),
    StructField("CRCTN_NOTE", StringType(), nullable=True),
    StructField("CRT_DTM", TimestampType(), nullable=False),
    StructField("LAST_UPDT_DTM", TimestampType(), nullable=False),
    StructField("USER_ID", StringType(), nullable=False)
])
df_ROW_CT_UPDATE_FILE_read = spark.read \
    .option("sep", ",") \
    .option("quote", "\"") \
    .schema(schema_ROW_CT_UPDATE_FILE) \
    .csv(f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat")

execute_dml(f"DROP TABLE IF EXISTS STAGING.ClmLnRowCntBal_ROW_CT_DTL_temp", jdbc_url_uws, jdbc_props_uws)
df_ROW_CT_UPDATE_FILE_read.write.format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", "STAGING.ClmLnRowCntBal_ROW_CT_DTL_temp") \
    .mode("overwrite") \
    .save()
merge_sql_row_ct_dtl = f"""
MERGE INTO {UWSOwner}.ROW_CT_DTL AS T
USING STAGING.ClmLnRowCntBal_ROW_CT_DTL_temp AS S
ON 1=0
WHEN NOT MATCHED THEN
INSERT (TRGT_SYS_CD, SRC_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, TRGT_CT, SRC_CT, DIFF_CT, TLRNC_CD, TLRNC_AMT, TLRNC_MULT_AMT, FREQ_CD, TRGT_RUN_CYC_NO, CRCTN_NOTE, CRT_DTM, LAST_UPDT_DTM, USER_ID)
VALUES (S.TRGT_SYS_CD, S.SRC_SYS_CD, S.SUBJ_AREA_NM, S.TRGT_TBL_NM, S.TRGT_CT, S.SRC_CT, S.DIFF_CT, S.TLRNC_CD, S.TLRNC_AMT, S.TLRNC_MULT_AMT, S.FREQ_CD, S.TRGT_RUN_CYC_NO, S.CRCTN_NOTE, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID);
"""
execute_dml(merge_sql_row_ct_dtl, jdbc_url_uws, jdbc_props_uws)