# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/11/09 15:13:12 Batch  15046_54866 PROMOTE bckcetl ids20 dsadm rc for ralph 
# MAGIC ^1_1 03/11/09 14:40:56 Batch  15046_52880 INIT bckcett testIDScur dsadm rc for ralph 
# MAGIC ^1_6 03/09/09 11:24:23 Batch  15044_41067 PROMOTE bckcett testIDScur u03651 steph for Ralph
# MAGIC ^1_6 03/09/09 11:01:22 Batch  15044_39685 INIT bckcett devlIDScur u03651 steffy
# MAGIC ^1_5 03/06/09 14:38:31 Batch  15041_52722 INIT bckcett devlIDScur u08717 Brent for Ralph
# MAGIC ^1_4 03/05/09 20:44:06 Batch  15040_74649 INIT bckcett devlIDScur u03651 steffy
# MAGIC ^1_3 03/05/09 13:26:54 Batch  15040_48421 INIT bckcett devlIDScur u03651 steffy
# MAGIC ^1_2 02/23/09 10:29:31 Batch  15030_37773 INIT bckcett devlIDScur u03651 steffy
# MAGIC ^1_1 02/23/09 10:20:32 Batch  15030_37236 INIT bckcett devlIDScur u03651 steffy
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
# MAGIC Ralph Tucker          2009-02-04       3660 - MSP                        Originally Programmed                                                                  devlIDS                           Steph Goddard          02/12/2009

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


# Retrieve parameters (including special handling for $<database>Owner)
RunID = get_widget_value('RunID','20090305')
ToleranceCd = get_widget_value('ToleranceCd','0')
IDSOwner = get_widget_value('$IDSOwner','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','100')
TrgtTable = get_widget_value('TrgtTable','MBR_ATCHMT')
SrcTable = get_widget_value('SrcTable','B_MBR_ATCHMT')
NotifyMessage = get_widget_value('NotifyMessage','"ROW COUNT BALANCING FACETS - IDS CLAIM ATTACHMENT OUT OF TOLERANCE"')
UWSOwner = get_widget_value('$UWSOwner','')
SubjectArea = get_widget_value('SubjectArea','Membership')
CurrentDate = get_widget_value('CurrentDate','2007-09-14 10:00:00')

# Add the extra secrets for databases found in parameter names
ids_secret_name = get_widget_value('ids_secret_name','')
uws_secret_name = get_widget_value('uws_secret_name','')

# ----------------------------------------------------------------------------------------------------
# Stage: Compare (DB2Connector) -> (Missing Link)
# ----------------------------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_Compare = (
    f"SELECT TRGT.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK,"
    f"TRGT.MBR_UNIQ_KEY as TRGT_MBR_UNIQ_KEY,"
    f"SRC.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK,"
    f"SRC.MBR_UNIQ_KEY as SRC_MBR_UNIQ_KEY,"
    f"TRGT.MBR_ATCHMT_ID as TRGT_MBR_ATCHMT_ID,"
    f"SRC.MBR_ATCHMT_ID as SRC_MBR_ATCHMT_ID,"
    f"TRGT.MBR_ATCHMT_DTM as TRGT_MBR_ATCHMT_DTM,"
    f"SRC.MBR_ATCHMT_DTM as SRC_MBR_ATCHMT_DTM,"
    f"TRGT.MBR_ATCHMT_DEST_ID_DTM as TRGT_MBR_ATCHMT_DEST_ID_DTM,"
    f"SRC.MBR_ATCHMT_DEST_ID_DTM as SRC_MBR_ATCHMT_DEST_ID_DTM,"
    f"TRGT.MBR_ATCHMT_SEQ_NO as TRGT_MBR_ATCHMT_SEQ_NO,"
    f"SRC.MBR_ATCHMT_SEQ_NO as SRC_MBR_ATCHMT_SEQ_NO "
    f"FROM {IDSOwner}.{TrgtTable} AS TRGT "
    f"FULL OUTER JOIN {IDSOwner}.{SrcTable} AS SRC "
    f"ON TRGT.SRC_SYS_CD_SK=SRC.SRC_SYS_CD_SK "
    f"AND TRGT.MBR_UNIQ_KEY = SRC.MBR_UNIQ_KEY "
    f"and TRGT.MBR_ATCHMT_ID = SRC.MBR_ATCHMT_ID "
    f"and TRGT.MBR_ATCHMT_DTM = SRC.MBR_ATCHMT_DTM "
    f"and TRGT.MBR_ATCHMT_DEST_ID_DTM = SRC.MBR_ATCHMT_DEST_ID_DTM "
    f"and TRGT.MBR_ATCHMT_SEQ_NO = SRC.MBR_ATCHMT_SEQ_NO "
    f"WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}"
)
df_Compare = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_Compare)
    .load()
)
df_Missing = df_Compare

# ----------------------------------------------------------------------------------------------------
# Stage: SRC_SYS_CD (CHashedFileStage) -> (SRC_SYS_CD Link)
# Scenario C: Read from Parquet
# ----------------------------------------------------------------------------------------------------
df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

# ----------------------------------------------------------------------------------------------------
# Stage: TransformLogic (CTransformerStage)
# PrimaryLink: Missing
# LookupLink: SRC_SYS_CD (left join on Missing.TRGT_SRC_SYS_CD_SK = SRC_SYS_CD.CD_MPPNG_SK)
# OutputPins: Research, Notify, ROW_CNT
# ----------------------------------------------------------------------------------------------------
df_TransformLogic = (
    df_Missing.alias("Missing")
    .join(
        df_SRC_SYS_CD.alias("SRC_SYS_CD"),
        F.col("Missing.TRGT_SRC_SYS_CD_SK") == F.col("SRC_SYS_CD.CD_MPPNG_SK"),
        "left"
    )
)

w = Window.orderBy(F.lit(1))
df_TransformLogic = df_TransformLogic.withColumn("__rownum", F.row_number().over(w))

# Research constraint: 
# ISNULL(Missing.TRGT_SRC_SYS_CD_SK) OR ISNULL(Missing.TRGT_MBR_UNIQ_KEY) OR ... 
research_filter = (
    F.col("TRGT_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_MBR_UNIQ_KEY").isNull()
    | F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("SRC_MBR_UNIQ_KEY").isNull()
    | F.col("TRGT_MBR_ATCHMT_ID").isNull()
    | F.col("SRC_MBR_ATCHMT_ID").isNull()
    | F.col("TRGT_MBR_ATCHMT_DTM").isNull()
    | F.col("SRC_MBR_ATCHMT_DTM").isNull()
    | F.col("TRGT_MBR_ATCHMT_DEST_ID_DTM").isNull()
    | F.col("SRC_MBR_ATCHMT_DEST_ID_DTM").isNull()
    | F.col("TRGT_MBR_ATCHMT_SEQ_NO").isNull()
    | F.col("SRC_MBR_ATCHMT_SEQ_NO").isNull()
)
df_Research = df_TransformLogic.filter(research_filter).select(
    F.col("TRGT_SRC_SYS_CD_SK").alias("TRGT_SRC_SYS_CD_SK"),
    F.col("TRGT_MBR_UNIQ_KEY").alias("TRGT_MBR_UNIQ_KEY"),
    F.col("TRGT_MBR_ATCHMT_ID").alias("TRGT_MBR_ATCHMT_ID"),
    F.col("TRGT_MBR_ATCHMT_DTM").alias("TRGT_MBR_ATCHMT_DTM"),
    F.col("TRGT_MBR_ATCHMT_DEST_ID_DTM").alias("TRGT_MBR_ATCHMT_DEST_ID_DTM"),
    F.col("TRGT_MBR_ATCHMT_SEQ_NO").alias("TRGT_MBR_ATCHMT_SEQ_NO"),
    F.col("SRC_SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
    F.col("SRC_MBR_UNIQ_KEY").alias("SRC_MBR_UNIQ_KEY"),
    F.col("SRC_MBR_ATCHMT_ID").alias("SRC_MBR_ATCHMT_ID"),
    F.col("SRC_MBR_ATCHMT_DTM").alias("SRC_MBR_ATCHMT_DTM"),
    F.col("SRC_MBR_ATCHMT_DEST_ID_DTM").alias("SRC_MBR_ATCHMT_DEST_ID_DTM"),
    F.col("SRC_MBR_ATCHMT_SEQ_NO").alias("SRC_MBR_ATCHMT_SEQ_NO")
)

# Notify constraint: @INROWNUM = 1 AND @INROWNUM > ToleranceCd
# We interpret rownum=1 and 1> Tolerance
df_Notify = df_TransformLogic.filter(
    (F.col("__rownum") == 1)
    & (F.col("__rownum") > F.lit(int(ToleranceCd)))
).select(
    F.lit(NotifyMessage).alias("NOTIFICATION")
)

# ROW_CNT constraint: @INROWNUM=1
df_ROW_CNT = df_TransformLogic.filter(F.col("__rownum") == 1).select(
    F.col("TRGT_CD").alias("SRC_SYS_CD")
)

# ----------------------------------------------------------------------------------------------------
# Stage: ResearchFile (CSeqFileStage) - writes df_Research
# ----------------------------------------------------------------------------------------------------
write_files(
    df_Research.select(
        "TRGT_SRC_SYS_CD_SK",
        "TRGT_MBR_UNIQ_KEY",
        "TRGT_MBR_ATCHMT_ID",
        "TRGT_MBR_ATCHMT_DTM",
        "TRGT_MBR_ATCHMT_DEST_ID_DTM",
        "TRGT_MBR_ATCHMT_SEQ_NO",
        "SRC_SRC_SYS_CD_SK",
        "SRC_MBR_UNIQ_KEY",
        "SRC_MBR_ATCHMT_ID",
        "SRC_MBR_ATCHMT_DTM",
        "SRC_MBR_ATCHMT_DEST_ID_DTM",
        "SRC_MBR_ATCHMT_SEQ_NO"
    ),
    f"{adls_path}/balancing/research/{SrcTable}.{TrgtTable}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------------------------------
# Stage: ErrorNotificationFile (CSeqFileStage) - writes df_Notify
# ----------------------------------------------------------------------------------------------------
df_Notify_out = df_Notify.select(
    F.rpad(F.col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)
write_files(
    df_Notify_out,
    f"{adls_path}/balancing/notify/MbrAtchmtBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------------------------------
# Stage: Copy_of_TransformLogic (CTransformerStage)
# Uses Stage Variables: ToleranceCd, GetSrcCount, GetTrgtCount, etc.
# ----------------------------------------------------------------------------------------------------
svTlrnc = ToleranceCd
svSrcCnt = GetSrcCount(IDSOwner, <...>, <...>, <...>, SrcTable)
svTrgtCnt = GetTrgtCount(IDSOwner, <...>, <...>, <...>, TrgtTable, ExtrRunCycle)
DiffVal = svTrgtCnt - svSrcCnt
# If Diff=0 => 'BAL'; If abs(Diff)> Tolerance => 'OUT'; else 'IN'
cond_abs = abs(DiffVal)
if DiffVal == 0:
    svTlrncCdVal = "BAL"
else:
    if cond_abs > int(svTlrnc):
        svTlrncCdVal = "OUT"
    else:
        svTlrncCdVal = "IN"

# FORMAT.DATE(@DATE, 'DATE', 'CURRENT', 'SYBTIMESTAMP') => current_timestamp()
svCurDTMVal = current_timestamp()

# Input df_ROW_CNT is primary link
df_ROW_CNT_in = df_ROW_CNT

# Output pin: ROW_CNT_OUT => constraint @INROWNUM=1 (already single row)
df_ROW_CNT_OUT = df_ROW_CNT_in.select(
    F.lit("IDS").alias("TRGT_SYS_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(SubjectArea).alias("SUBJ_AREA_NM"),
    F.lit(TrgtTable).alias("TRGT_TBL_NM"),
    F.lit(svTrgtCnt).alias("TRGT_CT"),
    F.lit(svSrcCnt).alias("SRC_CT"),
    F.lit(DiffVal).alias("DIFF_CT"),
    F.lit(svTlrncCdVal).alias("TLRNC_CD"),
    F.lit(svTlrnc).alias("TLRNC_AMT"),
    F.lit(svTlrnc).alias("TLRNC_MULT_AMT"),
    F.lit("DAILY").alias("FREQ_CD"),
    F.lit(ExtrRunCycle).alias("TRGT_RUN_CYC_NO"),
    F.lit(" ").alias("CRCTN_NOTE"),
    F.lit(svCurDTMVal).alias("CRT_DTM"),
    F.lit(svCurDTMVal).alias("LAST_UPDT_DTM"),
    F.lit("<...>").alias("USER_ID")  # $UWSAcct not defined in parameters
)

# Output pin: TBL_BAL_SUM => constraint @INROWNUM=1
df_TBL_BAL_SUM = df_ROW_CNT_in.select(
    F.lit("IDS").alias("TRGT_SYS_CD"),
    F.lit(SubjectArea).alias("SUBJ_AREA_NM"),
    F.lit(TrgtTable).alias("TRGT_TBL_NM"),
    F.lit(CurrentDate).alias("BAL_DT"),
    F.lit(svTlrncCdVal).alias("ROW_CT_TLRNC_CD"),
    F.lit("NA").alias("CLMN_SUM_TLRNC_CD"),
    F.lit("NA").alias("ROW_TO_ROW_TLRNC_CD"),
    F.lit("NA").alias("RI_TLRNC_CD"),
    F.lit("NA").alias("RELSHP_CLMN_SUM_TLRNC_CD"),
    F.lit("NA").alias("CRS_FOOT_TLRNC_CD"),
    F.lit("NA").alias("HIST_RSNBL_TLRNC_CD"),
    F.lit(ExtrRunCycle).alias("TRGT_RUN_CYC_NO"),
    F.lit(svCurDTMVal).alias("CRT_DTM"),
    F.lit(svCurDTMVal).alias("LAST_UPDT_DTM"),
    F.lit("<...>").alias("USER_ID")
)

# ----------------------------------------------------------------------------------------------------
# Stage: TBL_BAL_SUM (CODBCStage) -> Insert into #$UWSOwner#.TBL_BAL_SUM
# Use staging table approach
# ----------------------------------------------------------------------------------------------------
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)
temp_table_TBL_BAL_SUM = f"STAGING.MbrAtchmtRowCntBal_TBL_BAL_SUM_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_TBL_BAL_SUM}", jdbc_url_uws, jdbc_props_uws)

df_TBL_BAL_SUM.write \
    .format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", temp_table_TBL_BAL_SUM) \
    .mode("append") \
    .save()

insert_sql_TBL_BAL_SUM = (
    f"INSERT INTO {UWSOwner}.TBL_BAL_SUM("
    f"TRGT_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, BAL_DT, ROW_CT_TLRNC_CD, CLMN_SUM_TLRNC_CD, "
    f"ROW_TO_ROW_TLRNC_CD, RI_TLRNC_CD, RELSHP_CLMN_SUM_TLRNC_CD, CRS_FOOT_TLRNC_CD, "
    f"HIST_RSNBL_TLRNC_CD, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID"
    f") "
    f"SELECT TRGT_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, BAL_DT, ROW_CT_TLRNC_CD, CLMN_SUM_TLRNC_CD, "
    f"ROW_TO_ROW_TLRNC_CD, RI_TLRNC_CD, RELSHP_CLMN_SUM_TLRNC_CD, CRS_FOOT_TLRNC_CD, "
    f"HIST_RSNBL_TLRNC_CD, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID "
    f"FROM {temp_table_TBL_BAL_SUM}"
)
execute_dml(insert_sql_TBL_BAL_SUM, jdbc_url_uws, jdbc_props_uws)

# ----------------------------------------------------------------------------------------------------
# Stage: ROW_CT_UPDATE_FILE (CSeqFileStage)
# Writes df_ROW_CNT_OUT -> #SrcTable##TrgtTable#RowCnt.dat
# ----------------------------------------------------------------------------------------------------
df_ROW_CNT_OUT_select = df_ROW_CNT_OUT.select(
    F.rpad(F.col("TRGT_SYS_CD"), <...>, " ").alias("TRGT_SYS_CD"),
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("SUBJ_AREA_NM"), <...>, " ").alias("SUBJ_AREA_NM"),
    F.rpad(F.col("TRGT_TBL_NM"), <...>, " ").alias("TRGT_TBL_NM"),
    F.col("TRGT_CT").cast(IntegerType()).alias("TRGT_CT"),
    F.col("SRC_CT").cast(IntegerType()).alias("SRC_CT"),
    F.col("DIFF_CT").cast(IntegerType()).alias("DIFF_CT"),
    F.rpad(F.col("TLRNC_CD"), <...>, " ").alias("TLRNC_CD"),
    F.col("TLRNC_AMT").cast(IntegerType()).alias("TLRNC_AMT"),
    F.col("TLRNC_MULT_AMT").cast(DecimalType(38,10)).alias("TLRNC_MULT_AMT"),
    F.rpad(F.col("FREQ_CD"), <...>, " ").alias("FREQ_CD"),
    F.col("TRGT_RUN_CYC_NO").cast(IntegerType()).alias("TRGT_RUN_CYC_NO"),
    F.rpad(F.col("CRCTN_NOTE"), <...>, " ").alias("CRCTN_NOTE"),
    F.col("CRT_DTM").cast(TimestampType()).alias("CRT_DTM"),
    F.col("LAST_UPDT_DTM").cast(TimestampType()).alias("LAST_UPDT_DTM"),
    F.rpad(F.col("USER_ID"), <...>, " ").alias("USER_ID")
)

write_files(
    df_ROW_CNT_OUT_select,
    f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# The output pin "ROW_CT_DTL" simply passes the same data forward.
df_ROW_CT_DTL = df_ROW_CNT_OUT_select

# ----------------------------------------------------------------------------------------------------
# Stage: ROW_CT_DTL (CODBCStage) -> Insert into #$UWSOwner#.ROW_CT_DTL
# Use staging table approach
# ----------------------------------------------------------------------------------------------------
temp_table_ROW_CT_DTL = f"STAGING.MbrAtchmtRowCntBal_ROW_CT_DTL_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_ROW_CT_DTL}", jdbc_url_uws, jdbc_props_uws)

df_ROW_CT_DTL.write \
    .format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", temp_table_ROW_CT_DTL) \
    .mode("append") \
    .save()

insert_sql_ROW_CT_DTL = (
    f"INSERT INTO {UWSOwner}.ROW_CT_DTL("
    f"TRGT_SYS_CD, SRC_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, TRGT_CT, SRC_CT, DIFF_CT, "
    f"TLRNC_CD, TLRNC_AMT, TLRNC_MULT_AMT, FREQ_CD, TRGT_RUN_CYC_NO, CRCTN_NOTE, CRT_DTM, "
    f"LAST_UPDT_DTM, USER_ID"
    f") "
    f"SELECT TRGT_SYS_CD, SRC_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, TRGT_CT, SRC_CT, DIFF_CT, "
    f"TLRNC_CD, TLRNC_AMT, TLRNC_MULT_AMT, FREQ_CD, TRGT_RUN_CYC_NO, CRCTN_NOTE, CRT_DTM, "
    f"LAST_UPDT_DTM, USER_ID "
    f"FROM {temp_table_ROW_CT_DTL}"
)
execute_dml(insert_sql_ROW_CT_DTL, jdbc_url_uws, jdbc_props_uws)