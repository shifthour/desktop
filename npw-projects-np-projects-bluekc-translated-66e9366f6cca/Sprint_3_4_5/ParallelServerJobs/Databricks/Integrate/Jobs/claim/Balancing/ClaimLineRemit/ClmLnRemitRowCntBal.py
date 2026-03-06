# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 04/24/09 09:45:30 Batch  15090_35139 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_1 04/24/09 09:36:12 Batch  15090_34576 INIT bckcett testIDS dsadm bls for sa
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                                 Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                          ----------------------------------   ---------------------------------    -------------------------   
# MAGIC SAndrew                       2009-03-20        IAD ProdSupport                Originally Programmed                                                               devlIDS                         Steph Goddard            04/09/2009

# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC hf_etrnl_cd_mppng contains all code mapping entries.   Created in job ???
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Retrieve all parameter values
RunID = get_widget_value('RunID','100')
ToleranceCd = get_widget_value('ToleranceCd','0')
ExtrRunCycle = get_widget_value('ExtrRunCycle','100')
TrgtTable = get_widget_value('TrgtTable','CLM_LN_REMIT')
SrcTable = get_widget_value('SrcTable','CLM_LN_REMIT')
NotifyMessage = get_widget_value('NotifyMessage','\\"ROW COUNT BALANCING FACETS - IDS CLAIM LINE REMIT  ATTACHMENT OUT OF TOLERANCE\\"')
SubjectArea = get_widget_value('SubjectArea','ClaimLineRemit')
CurrentDate = get_widget_value('CurrentDate','2007-09-14 10:00:00')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')

# --------------------------------------------------------------------------------
# Stage: Compare (DB2Connector) - Reading from IDS database
# --------------------------------------------------------------------------------
jdbc_url_Compare, jdbc_props_Compare = get_db_config(ids_secret_name)
extract_query_Compare = (
    f"SELECT TRGT.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK, "
    f"       TRGT.CLM_ID AS TRGT_CLM_ID, "
    f"       SRC.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK, "
    f"       SRC.CLM_ID AS SRC_CLM_ID "
    f"  FROM {IDSOwner}.{TrgtTable} AS TRGT "
    f"  FULL OUTER JOIN {IDSOwner}.{SrcTable} AS SRC "
    f"       ON TRGT.SRC_SYS_CD_SK = SRC.SRC_SYS_CD_SK "
    f"      AND TRGT.CLM_ID = SRC.CLM_ID "
    f" WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle}"
)

df_Compare = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_Compare)
    .options(**jdbc_props_Compare)
    .option("query", extract_query_Compare)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: hf_etrnl_cd_mppng (CHashedFileStage) - Scenario C (read as Parquet)
# --------------------------------------------------------------------------------
df_hf_etrnl_cd_mppng = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

# --------------------------------------------------------------------------------
# Stage: determineMissing (CTransformerStage)
# Join (primary link df_Compare) LEFT JOIN (lookup link df_hf_etrnl_cd_mppng)
# --------------------------------------------------------------------------------
df_joined = (
    df_Compare.alias("Missing")
    .join(
        df_hf_etrnl_cd_mppng.alias("SRC_SYS_CD"),
        F.col("Missing.TRGT_SRC_SYS_CD_SK") == F.col("SRC_SYS_CD.CD_MPPNG_SK"),
        "left"
    )
)

# Add a row number to handle constraints that reference @INROWNUM
windowSpec = Window.orderBy(F.lit(1))
df_joined_rn = df_joined.withColumn("rownum", F.row_number().over(windowSpec))

# Output link: Research
# Constraint: ISNULL(Missing.TRGT_SRC_SYS_CD_SK)=@TRUE OR ISNULL(Missing.TRGT_CLM_ID)=@TRUE OR ISNULL(Missing.SRC_SRC_SYS_CD_SK)=@TRUE OR ISNULL(Missing.SRC_CLM_ID)=@TRUE
df_Research = (
    df_joined_rn.filter(
        F.col("TRGT_SRC_SYS_CD_SK").isNull()
        | F.col("TRGT_CLM_ID").isNull()
        | F.col("SRC_SRC_SYS_CD_SK").isNull()
        | F.col("SRC_CLM_ID").isNull()
    )
    .select(
        "TRGT_SRC_SYS_CD_SK",
        "TRGT_CLM_ID",
        "SRC_SRC_SYS_CD_SK",
        "SRC_CLM_ID"
    )
)

# Output link: Notify
# Constraint: @INROWNUM=1 And @INROWNUM > ToleranceCd
df_Notify = (
    df_joined_rn
    .filter(
        (F.col("rownum") == 1)
        & (F.col("rownum") > F.lit(ToleranceCd).cast("int"))
    )
    .select(
        F.lit(NotifyMessage).alias("NOTIFICATION")
    )
)

# Output link: ROW_CNT
# Constraint: @INROWNUM = 1
df_ROW_CNT = (
    df_joined_rn
    .filter(F.col("rownum") == 1)
    .select(
        F.col("SRC_SYS_CD.TRGT_CD").alias("SRC_SYS_CD"),
        F.lit(None).alias("CRT_DTM")  # Temporary placeholder, will overwrite with correct expression below
    )
)

# The expression for "CRT_DTM" is FORMAT.DATE(@DATE, 'DATE', 'CURRENT', 'SYBTIMESTAMP') => current_timestamp()
df_ROW_CNT = df_ROW_CNT.withColumn("CRT_DTM", current_timestamp())

# --------------------------------------------------------------------------------
# Stage: ResearchFile (CSeqFileStage) - write df_Research
# File: balancing/research/#SrcTable#.#TrgtTable#.dat.#RunID#
# --------------------------------------------------------------------------------
research_file_path = f"{adls_path}/balancing/research/{SrcTable}.{TrgtTable}.dat.{RunID}"
# No types specified as char/varchar here, so no rpad is applied.
write_files(
    df_Research.select(
        "TRGT_SRC_SYS_CD_SK",
        "TRGT_CLM_ID",
        "SRC_SRC_SYS_CD_SK",
        "SRC_CLM_ID"
    ),
    research_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: ErrorNotificationFile (CSeqFileStage) - write df_Notify
# File: balancing/notify/ClmLnRemitBalancingNotification.dat (append)
# --------------------------------------------------------------------------------
notify_file_path = f"{adls_path}/balancing/notify/ClmLnRemitBalancingNotification.dat"
write_files(
    df_Notify.select("NOTIFICATION"),
    notify_file_path,
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: CntRowsOnTbls (CTransformerStage)
# Input: df_ROW_CNT
# Stage variables with user-defined functions
# --------------------------------------------------------------------------------
# We assume we have only one row in df_ROW_CNT. We add columns (stage variables).
_tmp_df = df_ROW_CNT
_tmp_df = _tmp_df.withColumn("svTlrnc", F.lit(ToleranceCd).cast("int"))
_tmp_df = _tmp_df.withColumn("svSrcCnt", F.lit(GetSrcCount(IDSOwner, <...>, <...>, <...>, SrcTable)))
_tmp_df = _tmp_df.withColumn("svTrgtCnt", F.lit(GetTrgtCount(IDSOwner, <...>, <...>, <...>, TrgtTable, ExtrRunCycle)))
_tmp_df = _tmp_df.withColumn("Diff", F.col("svTrgtCnt") - F.col("svSrcCnt"))
_tmp_df = _tmp_df.withColumn(
    "svTlrncCd",
    F.when(F.col("Diff") == 0, F.lit("BAL"))
     .when(F.abs(F.col("Diff")) > F.col("svTlrnc"), F.lit("OUT"))
     .otherwise(F.lit("IN"))
)

windowSpec2 = Window.orderBy(F.lit(1))
df_CntRowsOnTbls = _tmp_df.withColumn("rownum_2", F.row_number().over(windowSpec2))

# Output link: ROW_CNT_OUT => "ROW_CT_UPDATE_FILE"
df_ROW_CNT_OUT = (
    df_CntRowsOnTbls
    .filter(F.col("rownum_2") == 1)
    .select(
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
        F.col("CRT_DTM").alias("CRT_DTM"),
        F.col("CRT_DTM").alias("LAST_UPDT_DTM"),
        F.lit("<...>").alias("USER_ID")  # $UWSAcct unknown
    )
)

# Output link: TBL_BAL_SUM
df_TBL_BAL_SUM = (
    df_CntRowsOnTbls
    .filter(F.col("rownum_2") == 1)
    .select(
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
        F.col("CRT_DTM").alias("CRT_DTM"),
        F.col("CRT_DTM").alias("LAST_UPDT_DTM"),
        F.lit("<...>").alias("USER_ID")  # $UWSAcct unknown
    )
)

# --------------------------------------------------------------------------------
# Stage: ROW_CT_UPDATE_FILE (CSeqFileStage) - write df_ROW_CNT_OUT
# File: balancing/#SrcTable##TrgtTable#RowCnt.dat
# --------------------------------------------------------------------------------
row_cnt_file_path = f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat"
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
    row_cnt_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: TBL_BAL_SUM (CODBCStage) - Merge/Insert into #$UWSOwner#.TBL_BAL_SUM
# --------------------------------------------------------------------------------
jdbc_url_TBL_BAL_SUM, jdbc_props_TBL_BAL_SUM = get_db_config(uws_secret_name)

# Create a temporary table in STAGING schema
merge_temp_table_TBL_BAL_SUM = f"STAGING.ClmLnRemitRowCntBal_TBL_BAL_SUM_temp"
execute_dml(f"DROP TABLE IF EXISTS {merge_temp_table_TBL_BAL_SUM}", jdbc_url_TBL_BAL_SUM, jdbc_props_TBL_BAL_SUM)

# Write df_TBL_BAL_SUM to the temp table
(
    df_TBL_BAL_SUM
    .write
    .format("jdbc")
    .option("url", jdbc_url_TBL_BAL_SUM)
    .options(**jdbc_props_TBL_BAL_SUM)
    .option("dbtable", merge_temp_table_TBL_BAL_SUM)
    .mode("overwrite")
    .save()
)

# Perform a merge that mimics pure insert
merge_sql_TBL_BAL_SUM = (
    f"MERGE INTO {UWSOwner}.TBL_BAL_SUM AS T "
    f"USING {merge_temp_table_TBL_BAL_SUM} AS S "
    f"ON 1=0 "
    f"WHEN NOT MATCHED THEN INSERT ("
    f"  TRGT_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, BAL_DT, ROW_CT_TLRNC_CD, CLMN_SUM_TLRNC_CD, "
    f"  ROW_TO_ROW_TLRNC_CD, RI_TLRNC_CD, RELSHP_CLMN_SUM_TLRNC_CD, CRS_FOOT_TLRNC_CD, HIST_RSNBL_TLRNC_CD, "
    f"  TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID"
    f") VALUES ("
    f"  S.TRGT_SYS_CD, S.SUBJ_AREA_NM, S.TRGT_TBL_NM, S.BAL_DT, S.ROW_CT_TLRNC_CD, S.CLMN_SUM_TLRNC_CD, "
    f"  S.ROW_TO_ROW_TLRNC_CD, S.RI_TLRNC_CD, S.RELSHP_CLMN_SUM_TLRNC_CD, S.CRS_FOOT_TLRNC_CD, S.HIST_RSNBL_TLRNC_CD, "
    f"  S.TRGT_RUN_CYC_NO, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID"
    f");"
)
execute_dml(merge_sql_TBL_BAL_SUM, jdbc_url_TBL_BAL_SUM, jdbc_props_TBL_BAL_SUM)

# --------------------------------------------------------------------------------
# Stage: ROW_CT_UPDATE_FILE read again for next link (since there's an OutputPin: ROW_CT_DTL)
# We define the schema, then read the file
# --------------------------------------------------------------------------------
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
    StructField("TLRNC_MULT_AMT", DecimalType(10,2), False),
    StructField("FREQ_CD", StringType(), False),
    StructField("TRGT_RUN_CYC_NO", IntegerType(), False),
    StructField("CRCTN_NOTE", StringType(), True),
    StructField("CRT_DTM", TimestampType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("USER_ID", StringType(), False)
])

df_ROW_CT_UPDATE_FILE_read = (
    spark.read
    .format("csv")
    .option("quote", '"')
    .option("header", "false")
    .schema(schema_ROW_CT_UPDATE_FILE)
    .load(row_cnt_file_path)
)

# --------------------------------------------------------------------------------
# Stage: ROW_CT_DTL (CODBCStage) - Merge/Insert into #$UWSOwner#.ROW_CT_DTL
# --------------------------------------------------------------------------------
df_ROW_CT_DTL = df_ROW_CT_UPDATE_FILE_read.select(
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

jdbc_url_ROW_CT_DTL, jdbc_props_ROW_CT_DTL = get_db_config(uws_secret_name)
merge_temp_table_ROW_CT_DTL = f"STAGING.ClmLnRemitRowCntBal_ROW_CT_DTL_temp"
execute_dml(f"DROP TABLE IF EXISTS {merge_temp_table_ROW_CT_DTL}", jdbc_url_ROW_CT_DTL, jdbc_props_ROW_CT_DTL)

(
    df_ROW_CT_DTL
    .write
    .format("jdbc")
    .option("url", jdbc_url_ROW_CT_DTL)
    .options(**jdbc_props_ROW_CT_DTL)
    .option("dbtable", merge_temp_table_ROW_CT_DTL)
    .mode("overwrite")
    .save()
)

merge_sql_ROW_CT_DTL = (
    f"MERGE INTO {UWSOwner}.ROW_CT_DTL AS T "
    f"USING {merge_temp_table_ROW_CT_DTL} AS S "
    f"ON 1=0 "
    f"WHEN NOT MATCHED THEN INSERT ("
    f"  TRGT_SYS_CD, SRC_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, TRGT_CT, SRC_CT, DIFF_CT, TLRNC_CD, "
    f"  TLRNC_AMT, TLRNC_MULT_AMT, FREQ_CD, TRGT_RUN_CYC_NO, CRCTN_NOTE, CRT_DTM, LAST_UPDT_DTM, USER_ID"
    f") VALUES ("
    f"  S.TRGT_SYS_CD, S.SRC_SYS_CD, S.SUBJ_AREA_NM, S.TRGT_TBL_NM, S.TRGT_CT, S.SRC_CT, S.DIFF_CT, S.TLRNC_CD, "
    f"  S.TLRNC_AMT, S.TLRNC_MULT_AMT, S.FREQ_CD, S.TRGT_RUN_CYC_NO, S.CRCTN_NOTE, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID"
    f");"
)
execute_dml(merge_sql_ROW_CT_DTL, jdbc_url_ROW_CT_DTL, jdbc_props_ROW_CT_DTL)