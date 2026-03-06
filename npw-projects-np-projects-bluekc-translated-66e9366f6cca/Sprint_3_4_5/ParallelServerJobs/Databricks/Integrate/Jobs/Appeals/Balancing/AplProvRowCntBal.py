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
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
TrgtTable = get_widget_value('TrgtTable','')
SrcTable = get_widget_value('SrcTable','')
NotifyMessage = get_widget_value('NotifyMessage','\\"ROW COUNT BALANCING FACETS - IDS APPEAL PROVIDER OUT OF TOLERANCE\\"')
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
SubjectArea = get_widget_value('SubjectArea','Appeals')
CurrentDateParam = get_widget_value('CurrentDate','2007-09-14 10:00:00')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_compare = (
    f"SELECT TRGT.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK, TRGT.APL_ID as TRGT_APL_ID, "
    f"SRC.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK, SRC.APL_ID as SRC_APL_ID, "
    f"TRGT.SEQ_NO as TRGT_SEQ_NO, SRC.SEQ_NO as SRC_SEQ_NO "
    f"FROM {IDSOwner}.{TrgtTable} AS TRGT "
    f"FULL OUTER JOIN {IDSOwner}.{SrcTable} AS SRC "
    f"ON TRGT.SRC_SYS_CD_SK=SRC.SRC_SYS_CD_SK "
    f"AND TRGT.APL_ID = SRC.APL_ID "
    f"AND TRGT.SEQ_NO = SRC.SEQ_NO "
    f"WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}"
)
df_Compare = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_compare)
    .load()
)
df_Missing = df_Compare

df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

temp = (
    df_Missing.alias("Missing")
    .join(
        df_SRC_SYS_CD.alias("SRC_SYS_CD"),
        on=[F.col("Missing.TRGT_SRC_SYS_CD_SK") == F.col("SRC_SYS_CD.CD_MPPNG_SK")],
        how="left"
    )
    .withColumn("_row_num", F.row_number().over(Window.orderBy(F.lit("1"))))
)

df_Research = (
    temp.filter(
        F.col("Missing.TRGT_SRC_SYS_CD_SK").isNull()
        | F.col("Missing.TRGT_APL_ID").isNull()
        | F.col("Missing.SRC_SRC_SYS_CD_SK").isNull()
        | F.col("Missing.SRC_APL_ID").isNull()
    )
    .select(
        F.col("Missing.TRGT_SRC_SYS_CD_SK").alias("TRGT_SRC_SYS_CD_SK"),
        F.col("Missing.TRGT_APL_ID").alias("TRGT_APL_ID"),
        F.col("Missing.SRC_SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
        F.col("Missing.SRC_APL_ID").alias("SRC_APL_ID")
    )
)

try:
    tolerance_cd_int = int(ToleranceCd)
except:
    tolerance_cd_int = 0

df_Notify = (
    temp.filter((F.col("_row_num") == 1) & (F.col("_row_num") > tolerance_cd_int))
    .select(F.lit(NotifyMessage).alias("NOTIFICATION"))
)

df_Row_cnt = (
    temp.filter(F.col("_row_num") == 1)
    .select(F.col("SRC_SYS_CD.TRGT_CD").alias("SRC_SYS_CD"))
)

df_Research_select = df_Research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_APL_ID",
    "SRC_SRC_SYS_CD_SK",
    "SRC_APL_ID"
)
write_files(
    df_Research_select,
    f"{adls_path}/balancing/research/{SrcTable}.{TrgtTable}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_Notify_rpad = df_Notify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))
df_Notify_select = df_Notify_rpad.select("NOTIFICATION")
write_files(
    df_Notify_select,
    f"{adls_path}/balancing/notify/AppealsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

temp1 = df_Row_cnt.withColumn("_row_num", F.row_number().over(Window.orderBy(F.lit("1"))))
temp1 = temp1.withColumn("svTlrnc", F.lit(ToleranceCd))
temp1 = temp1.withColumn("svSrcCnt", GetSrcCount(IDSOwner, <...>, <...>, <...>, SrcTable))
temp1 = temp1.withColumn("svTrgtCnt", GetTrgtCount(IDSOwner, <...>, <...>, <...>, TrgtTable, ExtrRunCycle))
temp1 = temp1.withColumn("Diff", F.col("svTrgtCnt") - F.col("svSrcCnt"))
temp1 = temp1.withColumn(
    "svTlrncCd",
    F.when(F.col("Diff") == 0, F.lit("BAL"))
    .when(F.abs(F.col("Diff")) > F.col("svTlrnc"), F.lit("OUT"))
    .otherwise(F.lit("IN"))
)

df_ROW_CNT_OUT = temp1.filter(F.col("_row_num") == 1).select(
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
    F.lit(ExtrRunCycle).alias("TRGT_RUN_CYC_NO"),
    F.lit(" ").alias("CRCTN_NOTE"),
    F.lit(CurrentDateParam).alias("CRT_DTM"),
    F.lit(CurrentDateParam).alias("LAST_UPDT_DTM"),
    F.lit("<...>").alias("USER_ID")
)

df_TBL_BAL_SUM = temp1.filter(F.col("_row_num") == 1).select(
    F.lit("IDS").alias("TRGT_SYS_CD"),
    F.lit(SubjectArea).alias("SUBJ_AREA_NM"),
    F.lit(TrgtTable).alias("TRGT_TBL_NM"),
    F.lit(CurrentDateParam).alias("BAL_DT"),
    F.col("svTlrncCd").alias("ROW_CT_TLRNC_CD"),
    F.lit("NA").alias("CLMN_SUM_TLRNC_CD"),
    F.lit("NA").alias("ROW_TO_ROW_TLRNC_CD"),
    F.lit("NA").alias("RI_TLRNC_CD"),
    F.lit("NA").alias("RELSHP_CLMN_SUM_TLRNC_CD"),
    F.lit("NA").alias("CRS_FOOT_TLRNC_CD"),
    F.lit("NA").alias("HIST_RSNBL_TLRNC_CD"),
    F.lit(ExtrRunCycle).alias("TRGT_RUN_CYC_NO"),
    F.lit(CurrentDateParam).alias("CRT_DTM"),
    F.lit(CurrentDateParam).alias("LAST_UPDT_DTM"),
    F.lit("<...>").alias("USER_ID")
)

jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)
temp_table_tbl_bal_sum = "STAGING.AplProvRowCntBal_TBL_BAL_SUM_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_tbl_bal_sum}", jdbc_url_uws, jdbc_props_uws)

df_TBL_BAL_SUM.write.format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", temp_table_tbl_bal_sum) \
    .mode("append") \
    .save()

merge_sql_tbl_bal_sum = f"""
MERGE INTO {UWSOwner}.TBL_BAL_SUM AS T
USING {temp_table_tbl_bal_sum} AS S
ON 1=0
WHEN MATCHED THEN
  UPDATE SET
    T.TRGT_SYS_CD = S.TRGT_SYS_CD,
    T.SUBJ_AREA_NM = S.SUBJ_AREA_NM,
    T.TRGT_TBL_NM = S.TRGT_TBL_NM,
    T.BAL_DT = S.BAL_DT,
    T.ROW_CT_TLRNC_CD = S.ROW_CT_TLRNC_CD,
    T.CLMN_SUM_TLRNC_CD = S.CLMN_SUM_TLRNC_CD,
    T.ROW_TO_ROW_TLRNC_CD = S.ROW_TO_ROW_TLRNC_CD,
    T.RI_TLRNC_CD = S.RI_TLRNC_CD,
    T.RELSHP_CLMN_SUM_TLRNC_CD = S.RELSHP_CLMN_SUM_TLRNC_CD,
    T.CRS_FOOT_TLRNC_CD = S.CRS_FOOT_TLRNC_CD,
    T.HIST_RSNBL_TLRNC_CD = S.HIST_RSNBL_TLRNC_CD,
    T.TRGT_RUN_CYC_NO = S.TRGT_RUN_CYC_NO,
    T.CRT_DTM = S.CRT_DTM,
    T.LAST_UPDT_DTM = S.LAST_UPDT_DTM,
    T.USER_ID = S.USER_ID
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
""".strip()
execute_dml(merge_sql_tbl_bal_sum, jdbc_url_uws, jdbc_props_uws)

df_ROW_CT_UPDATE_FILE_select = df_ROW_CNT_OUT.select(
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
    df_ROW_CT_UPDATE_FILE_select,
    f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

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
    StructField("TLRNC_MULT_AMT", DecimalType(10, 0), False),
    StructField("FREQ_CD", StringType(), False),
    StructField("TRGT_RUN_CYC_NO", IntegerType(), False),
    StructField("CRCTN_NOTE", StringType(), True),
    StructField("CRT_DTM", TimestampType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("USER_ID", StringType(), False)
])
df_ROW_CT_DTL = (
    spark.read
    .option("delimiter", ",")
    .option("quote", '"')
    .schema(schema_ROW_CT_DTL)
    .csv(f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat")
)

temp_table_ROW_CT_DTL = "STAGING.AplProvRowCntBal_ROW_CT_DTL_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_ROW_CT_DTL}", jdbc_url_uws, jdbc_props_uws)

df_ROW_CT_DTL.write.format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", temp_table_ROW_CT_DTL) \
    .mode("append") \
    .save()

merge_sql_row_ct_dtl = f"""
MERGE INTO {UWSOwner}.ROW_CT_DTL AS T
USING {temp_table_ROW_CT_DTL} AS S
ON 1=0
WHEN MATCHED THEN
  UPDATE SET
    T.TRGT_SYS_CD = S.TRGT_SYS_CD,
    T.SRC_SYS_CD = S.SRC_SYS_CD,
    T.SUBJ_AREA_NM = S.SUBJ_AREA_NM,
    T.TRGT_TBL_NM = S.TRGT_TBL_NM,
    T.TRGT_CT = S.TRGT_CT,
    T.SRC_CT = S.SRC_CT,
    T.DIFF_CT = S.DIFF_CT,
    T.TLRNC_CD = S.TLRNC_CD,
    T.TLRNC_AMT = S.TLRNC_AMT,
    T.TLRNC_MULT_AMT = S.TLRNC_MULT_AMT,
    T.FREQ_CD = S.FREQ_CD,
    T.TRGT_RUN_CYC_NO = S.TRGT_RUN_CYC_NO,
    T.CRCTN_NOTE = S.CRCTN_NOTE,
    T.CRT_DTM = S.CRT_DTM,
    T.LAST_UPDT_DTM = S.LAST_UPDT_DTM,
    T.USER_ID = S.USER_ID
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
""".strip()
execute_dml(merge_sql_row_ct_dtl, jdbc_url_uws, jdbc_props_uws)