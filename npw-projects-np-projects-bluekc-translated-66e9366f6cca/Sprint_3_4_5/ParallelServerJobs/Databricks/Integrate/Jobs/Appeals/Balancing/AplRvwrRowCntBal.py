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
# MAGIC Bhoomi Dasari                  10/15/2007        3028                              Originally Programmed                           devlIDS30                    Steph Goddard             10/18/07

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
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
UWSOwner = get_widget_value('UWSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
TrgtTable = get_widget_value('TrgtTable','')
SrcTable = get_widget_value('SrcTable','')
ToleranceCd = get_widget_value('ToleranceCd','')
NotifyMessage = get_widget_value('NotifyMessage','')
SubjectArea = get_widget_value('SubjectArea','')
CurrentDate = get_widget_value('CurrentDate','')
RunID = get_widget_value('RunID','')

jdbc_url_compare, jdbc_props_compare = get_db_config(ids_secret_name)
compare_query = (
    f"SELECT TRGT.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK, "
    f"TRGT.APL_RVWR_ID as TRGT_APL_RVWR_ID, "
    f"SRC.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK, "
    f"SRC.APL_RVWR_ID as SRC_APL_RVWR_ID "
    f"FROM {IDSOwner}.{TrgtTable} AS TRGT "
    f"FULL OUTER JOIN {IDSOwner}.{SrcTable} AS SRC "
    f"ON TRGT.SRC_SYS_CD_SK=SRC.SRC_SYS_CD_SK AND TRGT.APL_RVWR_ID = SRC.APL_RVWR_ID "
    f"WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}"
)
df_Compare = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_compare)
    .options(**jdbc_props_compare)
    .option("query", compare_query)
    .load()
)
df_Missing = df_Compare

df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

df_TransformLogic_joined = df_Missing.alias("Missing").join(
    df_SRC_SYS_CD.alias("SRC_SYS_CD"),
    on=(F.col("Missing.TRGT_SRC_SYS_CD_SK") == F.col("SRC_SYS_CD.CD_MPPNG_SK")),
    how="left"
)

df_Research = df_TransformLogic_joined.filter(
    F.col("Missing.TRGT_SRC_SYS_CD_SK").isNull()
    | F.col("Missing.TRGT_APL_RVWR_ID").isNull()
    | F.col("Missing.SRC_SRC_SYS_CD_SK").isNull()
    | F.col("Missing.SRC_APL_RVWR_ID").isNull()
).select(
    F.col("Missing.TRGT_SRC_SYS_CD_SK").alias("TRGT_SRC_SYS_CD_SK"),
    F.col("Missing.TRGT_APL_RVWR_ID").alias("TRGT_APL_RVWR_ID"),
    F.col("Missing.SRC_SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
    F.col("Missing.SRC_APL_RVWR_ID").alias("SRC_APL_RVWR_ID")
)

window_no_partition = Window.orderBy(F.lit(1))
df_TransformLogic_joined_withRN = df_TransformLogic_joined.withColumn("_row_num_", F.row_number().over(window_no_partition))

df_Notify_temp = df_TransformLogic_joined_withRN.filter(
    (F.col("_row_num_") == 1) & (F.col("_row_num_") > F.lit(ToleranceCd))
).select(
    F.lit(NotifyMessage).alias("NOTIFICATION")
)
df_Notify = df_Notify_temp.select(
    F.rpad(F.col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)

df_ROW_CNT = df_TransformLogic_joined_withRN.filter(F.col("_row_num_") == 1).select(
    F.col("SRC_SYS_CD.TRGT_CD").alias("SRC_SYS_CD")
)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/{SrcTable}.{TrgtTable}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/AppealsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_TransformLogic1_withRN = df_ROW_CNT.withColumn("_row_num_", F.row_number().over(window_no_partition))
df_enriched = df_TransformLogic1_withRN
df_enriched = df_enriched.withColumn("svTlrnc", F.lit(ToleranceCd))
df_enriched = df_enriched.withColumn(
    "svSrcCnt",
    GetSrcCount(
        F.lit(IDSOwner),
        F.lit("<...>"),
        F.lit("<...>"),
        F.lit("<...>"),
        F.lit(SrcTable)
    )
)
df_enriched = df_enriched.withColumn(
    "svTrgtCnt",
    GetTrgtCount(
        F.lit(IDSOwner),
        F.lit("<...>"),
        F.lit("<...>"),
        F.lit("<...>"),
        F.lit(TrgtTable),
        F.lit(ExtrRunCycle)
    )
)
df_enriched = df_enriched.withColumn("Diff", F.col("svTrgtCnt") - F.col("svSrcCnt"))
df_enriched = df_enriched.withColumn(
    "svTlrncCd",
    F.when(F.col("Diff") == 0, F.lit("BAL"))
     .when(F.abs(F.col("Diff")) > F.col("svTlrnc"), F.lit("OUT"))
     .otherwise(F.lit("IN"))
)

df_ROW_CNT_OUT_temp = df_enriched.filter(F.col("_row_num_") == 1).select(
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
    F.lit(CurrentDate).alias("CRT_DTM"),
    F.lit(CurrentDate).alias("LAST_UPDT_DTM"),
    F.lit("<...>").alias("USER_ID")
)

df_ROW_CNT_OUT = df_ROW_CNT_OUT_temp.select(
    F.rpad(F.col("TRGT_SYS_CD"), F.lit(<...>), " ").alias("TRGT_SYS_CD"),
    F.rpad(F.col("SRC_SYS_CD"), F.lit(<...>), " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("SUBJ_AREA_NM"), F.lit(<...>), " ").alias("SUBJ_AREA_NM"),
    F.rpad(F.col("TRGT_TBL_NM"), F.lit(<...>), " ").alias("TRGT_TBL_NM"),
    F.col("TRGT_CT").alias("TRGT_CT"),
    F.col("SRC_CT").alias("SRC_CT"),
    F.col("DIFF_CT").alias("DIFF_CT"),
    F.rpad(F.col("TLRNC_CD"), F.lit(<...>), " ").alias("TLRNC_CD"),
    F.col("TLRNC_AMT").alias("TLRNC_AMT"),
    F.col("TLRNC_MULT_AMT").alias("TLRNC_MULT_AMT"),
    F.rpad(F.col("FREQ_CD"), F.lit(<...>), " ").alias("FREQ_CD"),
    F.col("TRGT_RUN_CYC_NO").alias("TRGT_RUN_CYC_NO"),
    F.rpad(F.col("CRCTN_NOTE"), F.lit(<...>), " ").alias("CRCTN_NOTE"),
    F.col("CRT_DTM").alias("CRT_DTM"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.rpad(F.col("USER_ID"), F.lit(<...>), " ").alias("USER_ID")
)

df_TBL_BAL_SUM_temp = df_enriched.filter(F.col("_row_num_") == 1).select(
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
    F.lit(CurrentDate).alias("CRT_DTM"),
    F.lit(CurrentDate).alias("LAST_UPDT_DTM"),
    F.lit("<...>").alias("USER_ID")
)

df_TBL_BAL_SUM = df_TBL_BAL_SUM_temp.select(
    F.rpad(F.col("TRGT_SYS_CD"), F.lit(<...>), " ").alias("TRGT_SYS_CD"),
    F.rpad(F.col("SUBJ_AREA_NM"), F.lit(<...>), " ").alias("SUBJ_AREA_NM"),
    F.rpad(F.col("TRGT_TBL_NM"), F.lit(<...>), " ").alias("TRGT_TBL_NM"),
    F.col("BAL_DT").alias("BAL_DT"),
    F.rpad(F.col("ROW_CT_TLRNC_CD"), F.lit(<...>), " ").alias("ROW_CT_TLRNC_CD"),
    F.rpad(F.col("CLMN_SUM_TLRNC_CD"), F.lit(<...>), " ").alias("CLMN_SUM_TLRNC_CD"),
    F.rpad(F.col("ROW_TO_ROW_TLRNC_CD"), F.lit(<...>), " ").alias("ROW_TO_ROW_TLRNC_CD"),
    F.rpad(F.col("RI_TLRNC_CD"), F.lit(<...>), " ").alias("RI_TLRNC_CD"),
    F.rpad(F.col("RELSHP_CLMN_SUM_TLRNC_CD"), F.lit(<...>), " ").alias("RELSHP_CLMN_SUM_TLRNC_CD"),
    F.rpad(F.col("CRS_FOOT_TLRNC_CD"), F.lit(<...>), " ").alias("CRS_FOOT_TLRNC_CD"),
    F.rpad(F.col("HIST_RSNBL_TLRNC_CD"), F.lit(<...>), " ").alias("HIST_RSNBL_TLRNC_CD"),
    F.col("TRGT_RUN_CYC_NO").alias("TRGT_RUN_CYC_NO"),
    F.col("CRT_DTM").alias("CRT_DTM"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.rpad(F.col("USER_ID"), F.lit(<...>), " ").alias("USER_ID")
)

write_files(
    df_ROW_CNT_OUT,
    f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

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
    StructField("TLRNC_MULT_AMT", DecimalType(10,0), False),
    StructField("FREQ_CD", StringType(), False),
    StructField("TRGT_RUN_CYC_NO", IntegerType(), False),
    StructField("CRCTN_NOTE", StringType(), True),
    StructField("CRT_DTM", TimestampType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("USER_ID", StringType(), False)
])

df_ROW_CT_UPDATE_FILE = spark.read.csv(
    path=f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat",
    schema=schema_ROW_CT_UPDATE_FILE,
    sep=",",
    header=False,
    quote='"',
    escape='"'
)
df_ROW_CT_DTL = df_ROW_CT_UPDATE_FILE

drop_sql_tbl_bal_sum = "DROP TABLE IF EXISTS STAGING.AplRvwrRowCntBal_TBL_BAL_SUM_temp"
execute_dml(drop_sql_tbl_bal_sum, jdbc_url_compare, jdbc_props_compare)
df_TBL_BAL_SUM.write \
    .format("jdbc") \
    .option("url", jdbc_url_compare) \
    .options(**jdbc_props_compare) \
    .option("dbtable", "STAGING.AplRvwrRowCntBal_TBL_BAL_SUM_temp") \
    .mode("overwrite") \
    .save()

insert_sql_tbl_bal_sum = (
    f"INSERT INTO {UWSOwner}.TBL_BAL_SUM (TRGT_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, BAL_DT, ROW_CT_TLRNC_CD, CLMN_SUM_TLRNC_CD, "
    f"ROW_TO_ROW_TLRNC_CD, RI_TLRNC_CD, RELSHP_CLMN_SUM_TLRNC_CD, CRS_FOOT_TLRNC_CD, HIST_RSNBL_TLRNC_CD, TRGT_RUN_CYC_NO, "
    f"CRT_DTM, LAST_UPDT_DTM, USER_ID) "
    f"SELECT TRGT_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, BAL_DT, ROW_CT_TLRNC_CD, CLMN_SUM_TLRNC_CD, ROW_TO_ROW_TLRNC_CD, "
    f"RI_TLRNC_CD, RELSHP_CLMN_SUM_TLRNC_CD, CRS_FOOT_TLRNC_CD, HIST_RSNBL_TLRNC_CD, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID "
    f"FROM STAGING.AplRvwrRowCntBal_TBL_BAL_SUM_temp"
)
execute_dml(insert_sql_tbl_bal_sum, jdbc_url_compare, jdbc_props_compare)

drop_sql_row_ct_dtl = "DROP TABLE IF EXISTS STAGING.AplRvwrRowCntBal_ROW_CT_DTL_temp"
execute_dml(drop_sql_row_ct_dtl, jdbc_url_compare, jdbc_props_compare)
df_ROW_CT_DTL.write \
    .format("jdbc") \
    .option("url", jdbc_url_compare) \
    .options(**jdbc_props_compare) \
    .option("dbtable", "STAGING.AplRvwrRowCntBal_ROW_CT_DTL_temp") \
    .mode("overwrite") \
    .save()

insert_sql_row_ct_dtl = (
    f"INSERT INTO {UWSOwner}.ROW_CT_DTL (TRGT_SYS_CD, SRC_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, TRGT_CT, SRC_CT, DIFF_CT, "
    f"TLRNC_CD, TLRNC_AMT, TLRNC_MULT_AMT, FREQ_CD, TRGT_RUN_CYC_NO, CRCTN_NOTE, CRT_DTM, LAST_UPDT_DTM, USER_ID) "
    f"SELECT TRGT_SYS_CD, SRC_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, TRGT_CT, SRC_CT, DIFF_CT, TLRNC_CD, TLRNC_AMT, "
    f"TLRNC_MULT_AMT, FREQ_CD, TRGT_RUN_CYC_NO, CRCTN_NOTE, CRT_DTM, LAST_UPDT_DTM, USER_ID "
    f"FROM STAGING.AplRvwrRowCntBal_ROW_CT_DTL_temp"
)
execute_dml(insert_sql_row_ct_dtl, jdbc_url_compare, jdbc_props_compare)