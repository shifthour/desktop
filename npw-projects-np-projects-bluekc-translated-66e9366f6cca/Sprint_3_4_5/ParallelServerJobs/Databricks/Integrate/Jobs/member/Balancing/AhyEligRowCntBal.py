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
# MAGIC Bhoomi D               2009-02-27         3863                                    Originally Programmed                                                                  devlIDScur                   Steph Goddard          03/0/2009

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql.window import Window
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
SubjectArea = get_widget_value('SubjectArea','Membership')
CurrentDate = get_widget_value('CurrentDate','2007-09-14 10:00:00')

# --------------------------------------------------------------------------------
# Stage: Compare (DB2Connector) - Read from IDS
# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    f"SELECT TRGT.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,"
    f"TRGT.CLNT_SRC_ID AS TRGT_CLNT_SRC_ID,"
    f"SRC.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,"
    f"SRC.CLNT_SRC_ID AS SRC_CLNT_SRC_ID,"
    f"TRGT.CLNT_GRP_NO AS TRGT_CLNT_GRP_NO,"
    f"SRC.CLNT_GRP_NO AS SRC_CLNT_GRP_NO,"
    f"TRGT.CLNT_SUBGRP_NO AS TRGT_CLNT_SUBGRP_NO,"
    f"SRC.CLNT_SUBGRP_NO AS SRC_CLNT_SUBGRP_NO,"
    f"TRGT.EFF_DT_SK AS TRGT_EFF_DT_SK,"
    f"SRC.EFF_DT_SK AS SRC_EFF_DT_SK,"
    f"TRGT.CLS_ID AS TRGT_CLS_ID,"
    f"SRC.CLS_ID AS SRC_CLS_ID "
    f"FROM {IDSOwner}.{TrgtTable} AS TRGT "
    f"FULL OUTER JOIN {IDSOwner}.{SrcTable} AS SRC "
    f"ON TRGT.SRC_SYS_CD_SK = SRC.SRC_SYS_CD_SK "
    f"AND TRGT.CLNT_SRC_ID = SRC.CLNT_SRC_ID "
    f"AND TRGT.CLNT_GRP_NO = SRC.CLNT_GRP_NO "
    f"AND TRGT.CLNT_SUBGRP_NO = SRC.CLNT_SUBGRP_NO "
    f"AND TRGT.EFF_DT_SK = SRC.EFF_DT_SK "
    f"AND TRGT.CLS_ID = SRC.CLS_ID "
    f"WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}"
)
df_missing = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: SRC_SYS_CD (CHashedFileStage) - Scenario C => read parquet
# --------------------------------------------------------------------------------
df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

# --------------------------------------------------------------------------------
# Stage: TransformLogic (CTransformerStage)
#   Inputs: df_missing (primary), df_SRC_SYS_CD (lookup, left join on Missing.TRGT_SRC_SYS_CD_SK=SRC_SYS_CD.CD_MPPNG_SK)
#   Outputs: Research, Notify, ROW_CNT
# --------------------------------------------------------------------------------
joined_cols = [
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_CLNT_SRC_ID",
    "TRGT_CLNT_GRP_NO",
    "TRGT_CLNT_SUBGRP_NO",
    "TRGT_EFF_DT_SK",
    "TRGT_CLS_ID",
    "SRC_SRC_SYS_CD_SK",
    "SRC_CLNT_SRC_ID",
    "SRC_CLNT_GRP_NO",
    "SRC_CLNT_SUBGRP_NO",
    "SRC_EFF_DT_SK",
    "SRC_CLS_ID",
    "CD_MPPNG_SK",      # from lookup
    "TRGT_CD",
    "TRGT_CD_NM",
    "SRC_CD",
    "SRC_CD_NM"
]

df_joined = (
    df_missing.alias("Missing")
    .join(
        df_SRC_SYS_CD.alias("SRC_SYS_CD"),
        F.col("Missing.TRGT_SRC_SYS_CD_SK") == F.col("SRC_SYS_CD.CD_MPPNG_SK"),
        "left",
    )
)

window_spec = Window.orderBy(F.lit(1))
df_joined_with_rn = df_joined.withColumn("row_num", F.row_number().over(window_spec))

df_research = df_joined_with_rn.filter(
    (F.col("TRGT_SRC_SYS_CD_SK").isNull())
    | (F.col("TRGT_CLNT_SRC_ID").isNull())
    | (F.col("SRC_SRC_SYS_CD_SK").isNull())
    | (F.col("SRC_CLNT_SRC_ID").isNull())
    | (F.col("TRGT_CLNT_GRP_NO").isNull())
    | (F.col("SRC_CLNT_GRP_NO").isNull())
    | (F.col("TRGT_CLNT_SUBGRP_NO").isNull())
    | (F.col("SRC_CLNT_SUBGRP_NO").isNull())
    | (F.col("TRGT_EFF_DT_SK").isNull())
    | (F.col("SRC_EFF_DT_SK").isNull())
    | (F.col("TRGT_CLS_ID").isNull())
    | (F.col("SRC_CLS_ID").isNull())
)

df_notify = df_joined_with_rn.filter(
    (F.col("row_num") == 1)
    & (F.col("row_num") > F.lit(ToleranceCd).cast("int"))
)

df_row_cnt = df_joined_with_rn.filter(
    (F.col("row_num") == 1)
)

# Research output columns (with rpad for char columns)
df_Research_final = (
    df_research.select(
        F.col("TRGT_SRC_SYS_CD_SK"),
        F.col("TRGT_CLNT_SRC_ID"),
        F.col("TRGT_CLNT_GRP_NO"),
        F.col("TRGT_CLNT_SUBGRP_NO"),
        F.rpad(F.col("TRGT_EFF_DT_SK").cast(StringType()), 10, " ").alias("TRGT_EFF_DT_SK"),
        F.col("TRGT_CLS_ID"),
        F.col("SRC_SRC_SYS_CD_SK"),
        F.col("SRC_CLNT_SRC_ID"),
        F.col("SRC_CLNT_GRP_NO"),
        F.col("SRC_CLNT_SUBGRP_NO"),
        F.rpad(F.col("SRC_EFF_DT_SK").cast(StringType()), 10, " ").alias("SRC_EFF_DT_SK"),
        F.col("SRC_CLS_ID")
    )
)

# Notify output columns (one column, char(70))
df_Notify_final = df_notify.select(
    F.rpad(F.lit(NotifyMessage), 70, " ").alias("NOTIFICATION")
)

# ROW_CNT output columns
df_ROW_CNT_final = df_row_cnt.select(
    F.col("TRGT_CD").alias("SRC_SYS_CD")
)

# --------------------------------------------------------------------------------
# Stage: ResearchFile (CSeqFileStage) - Write df_Research_final
# --------------------------------------------------------------------------------
write_files(
    df_Research_final,
    f"{adls_path}/balancing/research/{SrcTable}.{TrgtTable}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: ErrorNotificationFile (CSeqFileStage) - Write df_Notify_final
# --------------------------------------------------------------------------------
write_files(
    df_Notify_final,
    f"{adls_path}/balancing/notify/AhyEligBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: Copy_of_TransformLogic (CTransformerStage)
#   Input: df_ROW_CNT_final
#   Stage variables: svTlrnc, svSrcCnt, svTrgtCnt, Diff, svTlrncCd, svCurDTM
# --------------------------------------------------------------------------------
svTlrnc = ToleranceCd
svSrcCnt = GetSrcCount(IDSOwner, <...>, <...>, <...>, SrcTable)
svTrgtCnt = GetTrgtCount(IDSOwner, <...>, <...>, <...>, TrgtTable, ExtrRunCycle)
Diff = svTrgtCnt - svSrcCnt
if Diff == 0:
    svTlrncCd = 'BAL'
else:
    if abs(Diff) > float(svTlrnc):
        svTlrncCd = 'OUT'
    else:
        svTlrncCd = 'IN'
svCurDTM = current_timestamp()

df_row_cnt_with_rn = df_ROW_CNT_final.withColumn("row_num", F.row_number().over(Window.orderBy(F.lit(1))))

df_row_cnt_out = df_row_cnt_with_rn.filter(F.col("row_num") == 1).select(
    F.lit("IDS").alias("TRGT_SYS_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(SubjectArea).alias("SUBJ_AREA_NM"),
    F.lit(TrgtTable).alias("TRGT_TBL_NM"),
    F.lit(svTrgtCnt).alias("TRGT_CT"),
    F.lit(svSrcCnt).alias("SRC_CT"),
    F.lit(Diff).alias("DIFF_CT"),
    F.lit(svTlrncCd).alias("TLRNC_CD"),
    F.lit(svTlrnc).alias("TLRNC_AMT"),
    F.lit(svTlrnc).alias("TLRNC_MULT_AMT"),
    F.lit("DAILY").alias("FREQ_CD"),
    F.lit(ExtrRunCycle).alias("TRGT_RUN_CYC_NO"),
    F.lit(" ").alias("CRCTN_NOTE"),
    F.lit(svCurDTM).alias("CRT_DTM"),
    F.lit(svCurDTM).alias("LAST_UPDT_DTM"),
    F.lit("<...>").alias("USER_ID")  # $UWSAcct unknown
)

df_tbl_bal_sum = df_row_cnt_with_rn.filter(F.col("row_num") == 1).select(
    F.lit("IDS").alias("TRGT_SYS_CD"),
    F.lit(SubjectArea).alias("SUBJ_AREA_NM"),
    F.lit(TrgtTable).alias("TRGT_TBL_NM"),
    F.lit(CurrentDate).alias("BAL_DT"),
    F.lit(svTlrncCd).alias("ROW_CT_TLRNC_CD"),
    F.lit("NA").alias("CLMN_SUM_TLRNC_CD"),
    F.lit("NA").alias("ROW_TO_ROW_TLRNC_CD"),
    F.lit("NA").alias("RI_TLRNC_CD"),
    F.lit("NA").alias("RELSHP_CLMN_SUM_TLRNC_CD"),
    F.lit("NA").alias("CRS_FOOT_TLRNC_CD"),
    F.lit("NA").alias("HIST_RSNBL_TLRNC_CD"),
    F.lit(ExtrRunCycle).alias("TRGT_RUN_CYC_NO"),
    F.lit(svCurDTM).alias("CRT_DTM"),
    F.lit(svCurDTM).alias("LAST_UPDT_DTM"),
    F.lit("<...>").alias("USER_ID")  # $UWSAcct unknown
)

# --------------------------------------------------------------------------------
# Stage: TBL_BAL_SUM (CODBCStage) - Merge as insert-only into #$UWSOwner#.TBL_BAL_SUM
# --------------------------------------------------------------------------------
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)
temp_table_tbl_bal_sum = "STAGING.AhyEligRowCntBal_TBL_BAL_SUM_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_tbl_bal_sum}", jdbc_url_uws, jdbc_props_uws)

df_tbl_bal_sum.write.format("jdbc").option("url", jdbc_url_uws).options(**jdbc_props_uws)\
    .option("dbtable", temp_table_tbl_bal_sum).mode("overwrite").save()

merge_sql_tbl_bal_sum = (
    f"MERGE INTO {UWSOwner}.TBL_BAL_SUM AS T "
    f"USING {temp_table_tbl_bal_sum} AS S ON 1=0 "
    f"WHEN NOT MATCHED THEN INSERT ("
    f"[TRGT_SYS_CD], [SUBJ_AREA_NM], [TRGT_TBL_NM], [BAL_DT], [ROW_CT_TLRNC_CD], [CLMN_SUM_TLRNC_CD],"
    f"[ROW_TO_ROW_TLRNC_CD], [RI_TLRNC_CD], [RELSHP_CLMN_SUM_TLRNC_CD], [CRS_FOOT_TLRNC_CD], [HIST_RSNBL_TLRNC_CD],"
    f"[TRGT_RUN_CYC_NO], [CRT_DTM], [LAST_UPDT_DTM], [USER_ID]"
    f") VALUES ("
    f"S.[TRGT_SYS_CD], S.[SUBJ_AREA_NM], S.[TRGT_TBL_NM], S.[BAL_DT], S.[ROW_CT_TLRNC_CD], S.[CLMN_SUM_TLRNC_CD],"
    f"S.[ROW_TO_ROW_TLRNC_CD], S.[RI_TLRNC_CD], S.[RELSHP_CLMN_SUM_TLRNC_CD], S.[CRS_FOOT_TLRNC_CD], S.[HIST_RSNBL_TLRNC_CD],"
    f"S.[TRGT_RUN_CYC_NO], S.[CRT_DTM], S.[LAST_UPDT_DTM], S.[USER_ID]"
    f");"
)
execute_dml(merge_sql_tbl_bal_sum, jdbc_url_uws, jdbc_props_uws)

# --------------------------------------------------------------------------------
# Stage: ROW_CT_UPDATE_FILE (CSeqFileStage) - Write df_row_cnt_out, then read it back
# --------------------------------------------------------------------------------
df_ROW_CNT_OUT_final = df_row_cnt_out.select(
    F.col("TRGT_SYS_CD"),
    F.col("SRC_SYS_CD"),
    F.col("SUBJ_AREA_NM"),
    F.col("TRGT_TBL_NM"),
    F.col("TRGT_CT"),
    F.col("SRC_CT"),
    F.col("DIFF_CT"),
    F.col("TLRNC_CD"),
    F.col("TLRNC_AMT"),
    F.col("TLRNC_MULT_AMT"),
    F.col("FREQ_CD"),
    F.col("TRGT_RUN_CYC_NO"),
    F.col("CRCTN_NOTE"),
    F.col("CRT_DTM"),
    F.col("LAST_UPDT_DTM"),
    F.col("USER_ID")
)

write_files(
    df_ROW_CNT_OUT_final,
    f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

schema_ROW_CT_DTL = StructType([
    StructField("TRGT_SYS_CD", StringType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("SUBJ_AREA_NM", StringType(), nullable=False),
    StructField("TRGT_TBL_NM", StringType(), nullable=False),
    StructField("TRGT_CT", IntegerType(), nullable=False),
    StructField("SRC_CT", IntegerType(), nullable=False),
    StructField("DIFF_CT", IntegerType(), nullable=False),
    StructField("TLRNC_CD", StringType(), nullable=False),
    StructField("TLRNC_AMT", IntegerType(), nullable=False),
    StructField("TLRNC_MULT_AMT", DecimalType(38,10), nullable=False),
    StructField("FREQ_CD", StringType(), nullable=False),
    StructField("TRGT_RUN_CYC_NO", IntegerType(), nullable=False),
    StructField("CRCTN_NOTE", StringType(), nullable=True),
    StructField("CRT_DTM", TimestampType(), nullable=False),
    StructField("LAST_UPDT_DTM", TimestampType(), nullable=False),
    StructField("USER_ID", StringType(), nullable=False)
])

df_ROW_CT_DTL = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", '"')
    .option("header", "false")
    .schema(schema_ROW_CT_DTL)
    .load(f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat")
)

# --------------------------------------------------------------------------------
# Stage: ROW_CT_DTL (CODBCStage) - Merge as insert-only into #$UWSOwner#.ROW_CT_DTL
# --------------------------------------------------------------------------------
temp_table_row_ct_dtl = "STAGING.AhyEligRowCntBal_ROW_CT_DTL_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_row_ct_dtl}", jdbc_url_uws, jdbc_props_uws)

df_ROW_CT_DTL.write.format("jdbc").option("url", jdbc_url_uws).options(**jdbc_props_uws)\
    .option("dbtable", temp_table_row_ct_dtl).mode("overwrite").save()

merge_sql_row_ct_dtl = (
    f"MERGE INTO {UWSOwner}.ROW_CT_DTL AS T "
    f"USING {temp_table_row_ct_dtl} AS S ON 1=0 "
    f"WHEN NOT MATCHED THEN INSERT ("
    f"[TRGT_SYS_CD], [SRC_SYS_CD], [SUBJ_AREA_NM], [TRGT_TBL_NM], [TRGT_CT], [SRC_CT], [DIFF_CT], [TLRNC_CD], [TLRNC_AMT], "
    f"[TLRNC_MULT_AMT], [FREQ_CD], [TRGT_RUN_CYC_NO], [CRCTN_NOTE], [CRT_DTM], [LAST_UPDT_DTM], [USER_ID]"
    f") VALUES ("
    f"S.[TRGT_SYS_CD], S.[SRC_SYS_CD], S.[SUBJ_AREA_NM], S.[TRGT_TBL_NM], S.[TRGT_CT], S.[SRC_CT], S.[DIFF_CT], S.[TLRNC_CD], S.[TLRNC_AMT], "
    f"S.[TLRNC_MULT_AMT], S.[FREQ_CD], S.[TRGT_RUN_CYC_NO], S.[CRCTN_NOTE], S.[CRT_DTM], S.[LAST_UPDT_DTM], S.[USER_ID]"
    f");"
)
execute_dml(merge_sql_row_ct_dtl, jdbc_url_uws, jdbc_props_uws)