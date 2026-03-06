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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType, IntegerType, DecimalType, TimestampType
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
NotifyMessage = get_widget_value('NotifyMessage','\\"ROW COUNT BALANCING FACETS - IDS APPEAL ACTVTY CONTACT LEVEL OUT OF TOLERANCE\\"')
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
SubjectArea = get_widget_value('SubjectArea','Appeals')
CurrentDate = get_widget_value('CurrentDate','2007-09-14 10:00:00')

# -- Stage: Compare (DB2Connector) --
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_Compare = (
    f"SELECT "
    f"TRGT.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK, "
    f"TRGT.APL_ID as TRGT_APL_ID, "
    f"SRC.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK, "
    f"SRC.APL_ID as SRC_APL_ID, "
    f"TRGT.SEQ_NO as TRGT_SEQ_NO, "
    f"SRC.SEQ_NO as SRC_SEQ_NO "
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
    .option("query", extract_query_Compare)
    .load()
)

# -- Stage: SRC_SYS_CD (CHashedFileStage), Scenario C => read parquet --
df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet").select(
    "CD_MPPNG_SK",
    "TRGT_CD",
    "TRGT_CD_NM",
    "SRC_CD",
    "SRC_CD_NM"
)

# -- Stage: TransformLogic (CTransformerStage) --
# Join df_Compare (primary) with df_SRC_SYS_CD (lookup link with left join)
df_TransformLogic = (
    df_Compare.alias("Missing")
    .join(
        df_SRC_SYS_CD.alias("SRC_SYS_CD"),
        F.col("Missing.TRGT_SRC_SYS_CD_SK") == F.col("SRC_SYS_CD.CD_MPPNG_SK"),
        "left"
    )
)

# Add row number for constraints that reference @INROWNUM
wTransformLogic = Window.orderBy(F.lit(1))
df_TransformLogicWithRowNum = df_TransformLogic.withColumn("row_num", F.row_number().over(wTransformLogic))

# -- Output link: Research (Constraint) --
df_Research = df_TransformLogicWithRowNum.filter(
    F.col("TRGT_SRC_SYS_CD_SK").isNull() |
    F.col("TRGT_APL_ID").isNull() |
    F.col("SRC_SRC_SYS_CD_SK").isNull() |
    F.col("SRC_APL_ID").isNull() |
    F.col("SRC_SEQ_NO").isNull() |
    F.col("TRGT_SEQ_NO").isNull()
).select(
    F.col("TRGT_SRC_SYS_CD_SK").alias("TRGT_SRC_SYS_CD_SK"),
    F.col("TRGT_APL_ID").alias("TRGT_APL_ID"),
    F.col("SRC_SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
    F.col("SRC_APL_ID").alias("SRC_APL_ID"),
    F.col("TRGT_SEQ_NO").alias("TRGT_SEQ_NO"),
    F.col("SRC_SEQ_NO").alias("SRC_SEQ_NO")
)

# -- Output link: Notify (Constraint) => "@INROWNUM = 1 And @INROWNUM > ToleranceCd" --
df_Notify = df_TransformLogicWithRowNum.filter(
    (F.col("row_num") == 1) & (F.col("row_num") > F.lit(ToleranceCd).cast("int"))
).select(
    F.lit(NotifyMessage).alias("NOTIFICATION")
)

# -- Output link: ROW_CNT (Constraint) => "@INROWNUM = 1" --
df_ROW_CNT = df_TransformLogicWithRowNum.filter(F.col("row_num") == 1).select(
    F.col("TRGT_CD").alias("SRC_SYS_CD")
)

# -- Stage: ResearchFile (CSeqFileStage) => write df_Research --
df_Research_Write = df_Research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_APL_ID",
    "SRC_SRC_SYS_CD_SK",
    "SRC_APL_ID",
    "TRGT_SEQ_NO",
    "SRC_SEQ_NO"
)
write_files(
    df_Research_Write,
    f"{adls_path}/balancing/research/{SrcTable}.{TrgtTable}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# -- Stage: ErrorNotificationFile (CSeqFileStage) => write df_Notify --
# Column NOTIFICATION is char(70)
df_NotifyToWrite = df_Notify.select(
    F.rpad(F.col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)
write_files(
    df_NotifyToWrite,
    f"{adls_path}/balancing/notify/AppealsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# -- Stage: TransformLogic1 (CTransformerStage) --
# Input: df_ROW_CNT (primary link)
# Define stage variables as columns
df_TransformLogic1 = (
    df_ROW_CNT
    .withColumn("svTlrnc", F.lit(ToleranceCd).cast("int"))
    .withColumn("svSrcCnt", F.expr("GetSrcCount(<...>,<...>,<...>,<...>, SrcTable)"))  # user-defined function
    .withColumn("svTrgtCnt", F.expr("GetTrgtCount(<...>,<...>,<...>,<...>, TrgtTable, ExtrRunCycle)"))  # user-defined function
    .withColumn("Diff", F.col("svTrgtCnt") - F.col("svSrcCnt"))
    .withColumn(
        "svTlrncCd",
        F.when(F.col("Diff") == 0, "BAL")
         .when(F.abs(F.col("Diff")) > F.col("svTlrnc"), "OUT")
         .otherwise("IN")
    )
)

wTransformLogic1 = Window.orderBy(F.lit(1))
df_TransformLogic1WithRowNum = df_TransformLogic1.withColumn("row_num", F.row_number().over(wTransformLogic1))

# Output link: ROW_CNT_OUT (Constraint => @INROWNUM=1)
df_ROW_CNT_OUT = df_TransformLogic1WithRowNum.filter(F.col("row_num") == 1).select(
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
    F.lit(CurrentDate).alias("CRT_DTM"),
    F.lit(CurrentDate).alias("LAST_UPDT_DTM"),
    F.lit("<...>").alias("USER_ID")  # $UWSAcct is unknown
)

# Output link: TBL_BAL_SUM (Constraint => @INROWNUM=1)
df_TBL_BAL_SUM = df_TransformLogic1WithRowNum.filter(F.col("row_num") == 1).select(
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
    F.lit(CurrentDate).alias("CRT_DTM"),
    F.lit(CurrentDate).alias("LAST_UPDT_DTM"),
    F.lit("<...>").alias("USER_ID")  # $UWSAcct is unknown
)

# -- Stage: TBL_BAL_SUM (CODBCStage) => Merge/Insert into #$UWSOwner#.TBL_BAL_SUM
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)
temp_table_TBL_BAL_SUM = "STAGING.AplACLRowCntBal_TBL_BAL_SUM_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_TBL_BAL_SUM}", jdbc_url_uws, jdbc_props_uws)
df_TBL_BAL_SUM.write \
    .format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", temp_table_TBL_BAL_SUM) \
    .mode("append") \
    .save()
merge_sql_TBL_BAL_SUM = (
    f"MERGE INTO {UWSOwner}.TBL_BAL_SUM AS T "
    f"USING {temp_table_TBL_BAL_SUM} AS S ON 1=0 "
    f"WHEN NOT MATCHED THEN INSERT ("
    f"TRGT_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, BAL_DT, ROW_CT_TLRNC_CD, CLMN_SUM_TLRNC_CD, ROW_TO_ROW_TLRNC_CD, RI_TLRNC_CD, RELSHP_CLMN_SUM_TLRNC_CD, CRS_FOOT_TLRNC_CD, HIST_RSNBL_TLRNC_CD, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID"
    f") VALUES ("
    f"S.TRGT_SYS_CD, S.SUBJ_AREA_NM, S.TRGT_TBL_NM, S.BAL_DT, S.ROW_CT_TLRNC_CD, S.CLMN_SUM_TLRNC_CD, S.ROW_TO_ROW_TLRNC_CD, S.RI_TLRNC_CD, S.RELSHP_CLMN_SUM_TLRNC_CD, S.CRS_FOOT_TLRNC_CD, S.HIST_RSNBL_TLRNC_CD, S.TRGT_RUN_CYC_NO, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID"
    f");"
)
execute_dml(merge_sql_TBL_BAL_SUM, jdbc_url_uws, jdbc_props_uws)

# -- Stage: ROW_CT_UPDATE_FILE (CSeqFileStage) => writes df_ROW_CNT_OUT
df_ROW_CT_UPDATE_FILE = df_ROW_CNT_OUT.select(
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

# Pass-through link => ROW_CT_DTL
df_ROW_CT_DTL = df_ROW_CT_UPDATE_FILE

# -- Stage: ROW_CT_DTL (CODBCStage) => Merge/Insert into #$UWSOwner#.ROW_CT_DTL
temp_table_ROW_CT_DTL = "STAGING.AplACLRowCntBal_ROW_CT_DTL_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_ROW_CT_DTL}", jdbc_url_uws, jdbc_props_uws)
df_ROW_CT_DTL.write \
    .format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", temp_table_ROW_CT_DTL) \
    .mode("append") \
    .save()
merge_sql_ROW_CT_DTL = (
    f"MERGE INTO {UWSOwner}.ROW_CT_DTL AS T "
    f"USING {temp_table_ROW_CT_DTL} AS S ON 1=0 "
    f"WHEN NOT MATCHED THEN INSERT ("
    f"TRGT_SYS_CD, SRC_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, TRGT_CT, SRC_CT, DIFF_CT, TLRNC_CD, TLRNC_AMT, TLRNC_MULT_AMT, FREQ_CD, TRGT_RUN_CYC_NO, CRCTN_NOTE, CRT_DTM, LAST_UPDT_DTM, USER_ID"
    f") VALUES ("
    f"S.TRGT_SYS_CD, S.SRC_SYS_CD, S.SUBJ_AREA_NM, S.TRGT_TBL_NM, S.TRGT_CT, S.SRC_CT, S.DIFF_CT, S.TLRNC_CD, S.TLRNC_AMT, S.TLRNC_MULT_AMT, S.FREQ_CD, S.TRGT_RUN_CYC_NO, S.CRCTN_NOTE, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID"
    f");"
)
execute_dml(merge_sql_ROW_CT_DTL, jdbc_url_uws, jdbc_props_uws)