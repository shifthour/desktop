# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
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
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Oliver Nielsen                  07/01/2007          3264                              Originally Programmed                           devlIDS30       
# MAGIC Oliver Nielsen                  09/22/2008    Prod Supp                     Added IDSDSN to job and to function
# MAGIC                                                                                                       call for GetSrcCnt and GetTrgtCnt             devlIDSnew                Steph Goddard             09/26/2008
# MAGIC Karthik Chintalaphani       09/01/2012       4784                           Added Order By clause to the 
# MAGIC                                                                                                       source query                                            IntegrateWrhsDevl       Bhoomi Dasari         10/20/2012
# MAGIC 
# MAGIC Manasa Andru                 2014-04-11     TFS - 8267              Added FORMAT.DATE to the BAL_DT          IntegrateNewDevl          Kalyan Neelam             2014-04-14
# MAGIC                                                                                            field in the TABLE_BAL_SUM link of the Transform

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
from pyspark.sql.types import IntegerType, StringType, TimestampType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

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
UWSAcct = get_widget_value('UWSAcct','')  # Not declared in job parameters, but referenced in expressions.

# =======================================================================================
# STAGE: Compare (DB2Connector) - Reading from IDS database
# =======================================================================================
compare_query = (
    f"SELECT TRGT.SRC_SYS_CD_SK as TRGT_SRC_SYS_CD_SK,"
    f"TRGT.CLM_ID as TRGT_CLM_ID,"
    f"SRC.SRC_SYS_CD_SK as SRC_SRC_SYS_CD_SK,"
    f"SRC.CLM_ID as SRC_CLM_ID "
    f"FROM {IDSOwner}.{TrgtTable} AS TRGT "
    f"FULL OUTER JOIN {IDSOwner}.{SrcTable} AS SRC "
    f"ON TRGT.SRC_SYS_CD_SK=SRC.SRC_SYS_CD_SK "
    f"AND TRGT.CLM_ID = SRC.CLM_ID "
    f"WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle} "
    f"ORDER BY SRC.SRC_SYS_CD_SK"
)
jdbc_url_compare, jdbc_props_compare = get_db_config(ids_secret_name)
df_Compare = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_compare)
    .options(**jdbc_props_compare)
    .option("query", compare_query)
    .load()
)

# This output link is "Missing" → passes directly into TransformLogic
df_missing = df_Compare

# =======================================================================================
# STAGE: SRC_SYS_CD (CHashedFileStage) - Scenario C: read parquet in place of hashed file
# =======================================================================================
df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")
df_SRC_SYS_CD = df_SRC_SYS_CD.select(
    "CD_MPPNG_SK",
    "TRGT_CD",
    "TRGT_CD_NM",
    "SRC_CD",
    "SRC_CD_NM"
)

# =======================================================================================
# STAGE: TransformLogic (CTransformerStage)
# =======================================================================================
# Primary link: df_missing (alias "Missing")
# Lookup link: df_SRC_SYS_CD (alias "SRC_SYS_CD"), left join on Missing.TRGT_SRC_SYS_CD_SK = SRC_SYS_CD.CD_MPPNG_SK
df_TransformLogic_1 = df_missing.alias("Missing").join(
    df_SRC_SYS_CD.alias("SRC_SYS_CD"),
    F.col("Missing.TRGT_SRC_SYS_CD_SK") == F.col("SRC_SYS_CD.CD_MPPNG_SK"),
    how="left"
)

# Create row numbers to emulate @INROWNUM usage
w_tl = Window.orderBy(F.lit(1))
df_TransformLogic_2 = df_TransformLogic_1.withColumn("row_num", F.row_number().over(w_tl))

# ----------------------------------------------------------
# Output pin: Research (Constraint):
# ISNULL(Missing.TRGT_SRC_SYS_CD_SK) OR ISNULL(Missing.TRGT_CLM_ID) 
# OR ISNULL(Missing.SRC_SRC_SYS_CD_SK) OR ISNULL(Missing.SRC_CLM_ID)
# ----------------------------------------------------------
df_Research = df_TransformLogic_2.filter(
    F.col("Missing.TRGT_SRC_SYS_CD_SK").isNull()
    | F.col("Missing.TRGT_CLM_ID").isNull()
    | F.col("Missing.SRC_SRC_SYS_CD_SK").isNull()
    | F.col("Missing.SRC_CLM_ID").isNull()
).select(
    F.col("Missing.TRGT_SRC_SYS_CD_SK").alias("TRGT_SRC_SYS_CD_SK"),
    F.col("Missing.TRGT_CLM_ID").alias("TRGT_CLM_ID"),
    F.col("Missing.SRC_SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
    F.col("Missing.SRC_CLM_ID").alias("SRC_CLM_ID")
)

# ----------------------------------------------------------
# Output pin: Notify (Constraint):
# @INROWNUM = 1 And @INROWNUM > ToleranceCd
# ----------------------------------------------------------
# Interpret ToleranceCd as integer for filter comparison
tol_int = F.lit(F.round(F.lit(ToleranceCd).cast(IntegerType()), 0))
df_Notify = df_TransformLogic_2.filter(
    (F.col("row_num") == 1) & (F.col("row_num") > tol_int)
).select(
    # NOTIFICATION is char(70)
    F.rpad(F.lit(NotifyMessage), 70, " ").alias("NOTIFICATION")
)

# ----------------------------------------------------------
# Output pin: ROW_CNT (Constraint):
# @INROWNUM = 1
# Columns: SRC_SYS_CD <- SRC_SYS_CD.TRGT_CD
# ----------------------------------------------------------
df_ROW_CNT = df_TransformLogic_2.filter(F.col("row_num") == 1).select(
    F.col("SRC_SYS_CD.TRGT_CD").alias("SRC_SYS_CD")
)

# =======================================================================================
# STAGE: ResearchFile (CSeqFileStage) - Write research output
# =======================================================================================
df_Research_final = df_Research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_CLM_ID",
    "SRC_SRC_SYS_CD_SK",
    "SRC_CLM_ID"
)
write_files(
    df_Research_final,
    f"{adls_path}/balancing/research/{SrcTable}.{TrgtTable}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# =======================================================================================
# STAGE: ErrorNotificationFile (CSeqFileStage) - Write notify output
# =======================================================================================
df_Notify_final = df_Notify.select(
    F.rpad(F.col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)
write_files(
    df_Notify_final,
    f"{adls_path}/balancing/notify/ClaimBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# =======================================================================================
# STAGE: Transform (CTransformerStage)
# =======================================================================================
# Input is df_ROW_CNT from prior transform logic.
# Emulate stage variables.
# svTlrnc = ToleranceCd
# svSrcCnt = GetSrcCount($IDSOwner, $IDSDSN, $IDSAcct, $IDSPW, SrcTable)
# svTrgtCnt = GetTrgtCount($IDSOwner, $IDSDSN, $IDSAcct, $IDSPW, TrgtTable, ExtrRunCycle)
# Diff = svTrgtCnt - svSrcCnt
# svTlrncCd = IF Diff=0 THEN 'BAL' ELSE IF ABS(Diff) > svTlrnc THEN 'OUT' ELSE 'IN'
#
# Each output pin references @INROWNUM=1 again, so we row-number again.

w_transform2 = Window.orderBy(F.lit(1))
df_Transform_1 = df_ROW_CNT.withColumn("row_num", F.row_number().over(w_transform2))

df_enriched = (
    df_Transform_1
    .withColumn("svTlrnc", F.lit(ToleranceCd))
    .withColumn("svSrcCnt", F.expr("GetSrcCount(IDSOwner, <IDSDSN>, <IDSAcct>, <IDSPW>, SrcTable)"))  # user-defined
    .withColumn("svTrgtCnt", F.expr("GetTrgtCount(IDSOwner, <IDSDSN>, <IDSAcct>, <IDSPW>, TrgtTable, ExtrRunCycle)"))
    .withColumn("Diff", F.col("svTrgtCnt") - F.col("svSrcCnt"))
    .withColumn(
        "svTlrncCd",
        F.when(F.col("Diff") == 0, F.lit("BAL"))
         .when(F.abs(F.col("Diff")) > F.col("svTlrnc").cast(IntegerType()), F.lit("OUT"))
         .otherwise(F.lit("IN"))
    )
)

# ----------------------------------------------------------------------------------------
# Output pin: ROW_CNT_OUT (Constraint: @INROWNUM = 1)
# Columns in order:
# 1) TRGT_SYS_CD => 'IDS'
# 2) SRC_SYS_CD => ROW_CNT.SRC_SYS_CD
# 3) SUBJ_AREA_NM => SubjectArea
# 4) TRGT_TBL_NM => TrgtTable
# 5) TRGT_CT => svTrgtCnt
# 6) SRC_CT => svSrcCnt
# 7) DIFF_CT => Diff
# 8) TLRNC_CD => svTlrncCd
# 9) TLRNC_AMT => svTlrnc
# 10) TLRNC_MULT_AMT => svTlrnc
# 11) FREQ_CD => 'DAILY'
# 12) TRGT_RUN_CYC_NO => ExtrRunCycle
# 13) CRCTN_NOTE => ' '
# 14) CRT_DTM => current_timestamp()
# 15) LAST_UPDT_DTM => current_timestamp()
# 16) USER_ID => $UWSAcct
# ----------------------------------------------------------------------------------------
df_ROW_CNT_OUT = df_enriched.filter(F.col("row_num") == 1).select(
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
    F.lit(ExtrRunCycle).cast(IntegerType()).alias("TRGT_RUN_CYC_NO"),
    F.lit(" ").alias("CRCTN_NOTE"),
    current_timestamp().alias("CRT_DTM"),
    current_timestamp().alias("LAST_UPDT_DTM"),
    F.lit(UWSAcct).alias("USER_ID")
)

# ----------------------------------------------------------------------------------------
# Output pin: TBL_BAL_SUM (Constraint: @INROWNUM = 1)
# Columns in order:
# TRGT_SYS_CD => 'IDS'
# SUBJ_AREA_NM => SubjectArea
# TRGT_TBL_NM => TrgtTable
# BAL_DT => current_timestamp()
# ROW_CT_TLRNC_CD => svTlrncCd
# CLMN_SUM_TLRNC_CD => 'NA'
# ROW_TO_ROW_TLRNC_CD => 'NA'
# RI_TLRNC_CD => 'NA'
# RELSHP_CLMN_SUM_TLRNC_CD => 'NA'
# CRS_FOOT_TLRNC_CD => 'NA'
# HIST_RSNBL_TLRNC_CD => 'NA'
# TRGT_RUN_CYC_NO => ExtrRunCycle
# CRT_DTM => current_timestamp()
# LAST_UPDT_DTM => current_timestamp()
# USER_ID => $UWSAcct
# ----------------------------------------------------------------------------------------
df_TBL_BAL_SUM = df_enriched.filter(F.col("row_num") == 1).select(
    F.lit("IDS").alias("TRGT_SYS_CD"),
    F.lit(SubjectArea).alias("SUBJ_AREA_NM"),
    F.lit(TrgtTable).alias("TRGT_TBL_NM"),
    current_timestamp().alias("BAL_DT"),
    F.col("svTlrncCd").alias("ROW_CT_TLRNC_CD"),
    F.lit("NA").alias("CLMN_SUM_TLRNC_CD"),
    F.lit("NA").alias("ROW_TO_ROW_TLRNC_CD"),
    F.lit("NA").alias("RI_TLRNC_CD"),
    F.lit("NA").alias("RELSHP_CLMN_SUM_TLRNC_CD"),
    F.lit("NA").alias("CRS_FOOT_TLRNC_CD"),
    F.lit("NA").alias("HIST_RSNBL_TLRNC_CD"),
    F.lit(ExtrRunCycle).cast(IntegerType()).alias("TRGT_RUN_CYC_NO"),
    current_timestamp().alias("CRT_DTM"),
    current_timestamp().alias("LAST_UPDT_DTM"),
    F.lit(UWSAcct).alias("USER_ID")
)

# =======================================================================================
# STAGE: TBL_BAL_SUM (CODBCStage)
# DataStage had a simple INSERT, but per instructions we do a MERGE (upsert).
# We'll write df_TBL_BAL_SUM to STAGING.ClmRowCntBal_TBL_BAL_SUM_temp, then MERGE into UWSOwner.TBL_BAL_SUM.
# No primary key is specified, so we use ON 1=0 to force all rows to insert.
# =======================================================================================
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)
temp_table_TBL_BAL_SUM = f"STAGING.ClmRowCntBal_TBL_BAL_SUM_temp"

# Drop temp table if exists
drop_sql_TBL_BAL_SUM = f"DROP TABLE IF EXISTS {temp_table_TBL_BAL_SUM}"
execute_dml(drop_sql_TBL_BAL_SUM, jdbc_url_uws, jdbc_props_uws)

# Create temp table by writing the DataFrame
df_TBL_BAL_SUM.write \
    .format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", temp_table_TBL_BAL_SUM) \
    .mode("overwrite") \
    .save()

merge_sql_TBL_BAL_SUM = (
    f"MERGE INTO {UWSOwner}.TBL_BAL_SUM AS T "
    f"USING {temp_table_TBL_BAL_SUM} AS S "
    f"ON (1=0) "
    f"WHEN NOT MATCHED THEN INSERT ("
    f"TRGT_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, BAL_DT, ROW_CT_TLRNC_CD, CLMN_SUM_TLRNC_CD, "
    f"ROW_TO_ROW_TLRNC_CD, RI_TLRNC_CD, RELSHP_CLMN_SUM_TLRNC_CD, CRS_FOOT_TLRNC_CD, HIST_RSNBL_TLRNC_CD, "
    f"TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID"
    f") VALUES ("
    f"S.TRGT_SYS_CD, S.SUBJ_AREA_NM, S.TRGT_TBL_NM, S.BAL_DT, S.ROW_CT_TLRNC_CD, S.CLMN_SUM_TLRNC_CD, "
    f"S.ROW_TO_ROW_TLRNC_CD, S.RI_TLRNC_CD, S.RELSHP_CLMN_SUM_TLRNC_CD, S.CRS_FOOT_TLRNC_CD, S.HIST_RSNBL_TLRNC_CD, "
    f"S.TRGT_RUN_CYC_NO, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID"
    f");"
)
execute_dml(merge_sql_TBL_BAL_SUM, jdbc_url_uws, jdbc_props_uws)

# =======================================================================================
# STAGE: ROW_CT_UPDATE_FILE (CSeqFileStage)
# Input pin: ROW_CNT_OUT → write to #SrcTable##TrgtTable#RowCnt.dat
# Then output link "ROW_CT_DTL" passes the same columns to next stage
# =======================================================================================
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
# The stage also offers an output pin "ROW_CT_DTL" with same columns:
df_ROW_CT_DTL = df_ROW_CT_UPDATE_FILE

# =======================================================================================
# STAGE: ROW_CT_DTL (CODBCStage)
# Again per instructions, use a MERGE approach for an all-row insert
# =======================================================================================
temp_table_ROW_CT_DTL = f"STAGING.ClmRowCntBal_ROW_CT_DTL_temp"
drop_sql_ROW_CT_DTL = f"DROP TABLE IF EXISTS {temp_table_ROW_CT_DTL}"
execute_dml(drop_sql_ROW_CT_DTL, jdbc_url_uws, jdbc_props_uws)

df_ROW_CT_DTL.write \
    .format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", temp_table_ROW_CT_DTL) \
    .mode("overwrite") \
    .save()

merge_sql_ROW_CT_DTL = (
    f"MERGE INTO {UWSOwner}.ROW_CT_DTL AS T "
    f"USING {temp_table_ROW_CT_DTL} AS S "
    f"ON (1=0) "
    f"WHEN NOT MATCHED THEN INSERT ("
    f"TRGT_SYS_CD, SRC_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, TRGT_CT, SRC_CT, DIFF_CT, TLRNC_CD, "
    f"TLRNC_AMT, TLRNC_MULT_AMT, FREQ_CD, TRGT_RUN_CYC_NO, CRCTN_NOTE, CRT_DTM, LAST_UPDT_DTM, USER_ID"
    f") VALUES ("
    f"S.TRGT_SYS_CD, S.SRC_SYS_CD, S.SUBJ_AREA_NM, S.TRGT_TBL_NM, S.TRGT_CT, S.SRC_CT, S.DIFF_CT, S.TLRNC_CD, "
    f"S.TLRNC_AMT, S.TLRNC_MULT_AMT, S.FREQ_CD, S.TRGT_RUN_CYC_NO, S.CRCTN_NOTE, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID"
    f");"
)
execute_dml(merge_sql_ROW_CT_DTL, jdbc_url_uws, jdbc_props_uws)