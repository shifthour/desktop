# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By: FctsMedMgtCntl
# MAGIC 
# MAGIC 
# MAGIC Process :  Case Mgt Status Fkey Process  which is processed and loaded to CASE_MGT_COST  table
# MAGIC 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                              		  Project/                                                                                                    	Code                  Date
# MAGIC Developer         	Date             	 Altiris #     	Change Description                                                   	Reviewer            Reviewed
# MAGIC -----------------------  	-------------------   	-------------   	-----------------------------------------------------------------------------   	-------------------------  -------------------
# MAGIC Sravya Gorla           2019-09-01          US115013                       Case Mgt Cost FKey Process        		                Jaideep Mankala   09/24/2019

# MAGIC FKEY failures are written into this flat file.
# MAGIC Code lookups operation to get the needed FKEY values
# MAGIC This is a load ready file that will go into Load job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Retrieve job parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')
RunDateTime = get_widget_value('RunDateTime','')  # not used downstream but retrieved per instructions

# JDBC configuration for IDS database
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# ----------------------------------------------------------------------------
# STAGE: CASE_MGT (DB2ConnectorPX)
# ----------------------------------------------------------------------------
extract_query_CASE_MGT = f"""
Select distinct 
CM.CASE_MGT_ID,
CM.CASE_MGT_SK
 from {IDSOwner}.CASE_MGT CM, {IDSOwner}.CD_MPPNG CD
WHERE CD.CD_MPPNG_SK = CM.SRC_SYS_CD_SK 
AND CD.TRGT_CD = 'FACETS'
"""
df_CASE_MGT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_CASE_MGT)
    .load()
)

# ----------------------------------------------------------------------------
# STAGE: CASE_COST (DB2ConnectorPX)
# ----------------------------------------------------------------------------
extract_query_CASE_COST = f"""
Select DISTINCT 
SRC_CD, 
CD_MPPNG_SK 
from {IDSOwner}.CD_MPPNG 
where 
TRGT_DOMAIN_NM = 'COST SAVINGS TYPE' 
and TRGT_CLCTN_CD = 'IDS' 
and SRC_SYS_CD = 'FACETS'
AND SRC_DOMAIN_NM = 'COST SAVINGS TYPE' 
AND SRC_CLCTN_CD = 'FACETS DBO'
"""
df_CASE_COST = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_CASE_COST)
    .load()
)

# ----------------------------------------------------------------------------
# STAGE: seq_CASE_MGT_COST_PKEY (PxSequentialFile) - READ
# ----------------------------------------------------------------------------
schema_seq_CASE_MGT_COST_PKEY = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), nullable=False),
    StructField("FIRST_RECYC_TS", TimestampType(), nullable=False),
    StructField("CASE_MGT_ID", StringType(), nullable=False),
    StructField("SRC_SYS_CD_1", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CST_SEQ_NO", IntegerType(), nullable=False),
    StructField("CASE_MGT_CST_LOG_SK", IntegerType(), nullable=False),
    StructField("CST_INPT_USER_ID", StringType(), nullable=False),
    StructField("CST_INPT_DT", TimestampType(), nullable=False),
    StructField("FROM_DT", TimestampType(), nullable=False),
    StructField("TO_DT", TimestampType(), nullable=False),
    StructField("ACTL_CST_AMT", DecimalType(38,10), nullable=False),
    StructField("PROJ_CST_AMT", DecimalType(38,10), nullable=False),
    StructField("CMCS_MCTR_COST", StringType(), nullable=False)
])

file_path_seq_CASE_MGT_COST_PKEY = f"{adls_path}/key/CASE_MGT_CST.{SrcSysCd}.pkey.{RunID}.dat"
df_seq_CASE_MGT_COST_PKEY = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .schema(schema_seq_CASE_MGT_COST_PKEY)
    .load(file_path_seq_CASE_MGT_COST_PKEY)
)

# ----------------------------------------------------------------------------
# STAGE: INP_USER (DB2ConnectorPX)
# ----------------------------------------------------------------------------
extract_query_INP_USER = f"""
Select distinct 
USER_SK ,
USER_ID
 from {IDSOwner}.APP_USER
WHERE USER_SK NOT IN (0,1)
"""
df_INP_USER = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_INP_USER)
    .load()
)

# ----------------------------------------------------------------------------
# STAGE: lkp_Code_SKs (PxLookup)
# Primary link: df_seq_CASE_MGT_COST_PKEY
# Lookup links: df_CASE_MGT, df_CASE_COST, df_INP_USER
# ----------------------------------------------------------------------------
df_lkp_Code_SKs = (
    df_seq_CASE_MGT_COST_PKEY.alias("A")
    .join(
        df_CASE_MGT.alias("B"),
        F.col("A.CASE_MGT_ID") == F.col("B.CASE_MGT_ID"),
        "left"
    )
    .join(
        df_CASE_COST.alias("C"),
        F.col("A.CMCS_MCTR_COST") == F.col("C.SRC_CD"),
        "left"
    )
    .join(
        df_INP_USER.alias("D"),
        (F.col("A.CST_INPT_USER_ID") == F.col("D.USER_ID")),
        "left"
    )
    .select(
        F.col("A.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("A.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("A.CASE_MGT_ID").alias("CASE_MGT_ID"),
        F.col("A.SRC_SYS_CD_1").alias("SRC_SYS_CD_1"),
        F.col("A.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("A.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("A.CST_SEQ_NO").alias("CST_SEQ_NO"),
        F.col("A.CASE_MGT_CST_LOG_SK").alias("CASE_MGT_CST_LOG_SK"),
        F.col("A.CST_INPT_USER_ID").alias("CST_INPT_USER_ID"),
        F.col("A.CST_INPT_DT").alias("CST_INPT_DT"),
        F.col("A.FROM_DT").alias("FROM_DT"),
        F.col("A.TO_DT").alias("TO_DT"),
        F.col("A.ACTL_CST_AMT").alias("ACTL_CST_AMT"),
        F.col("A.PROJ_CST_AMT").alias("PROJ_CST_AMT"),
        F.col("A.CMCS_MCTR_COST").alias("CMCS_MCTR_COST"),
        F.col("D.USER_SK").alias("COST_INPUT_USER_SK"),
        F.col("B.CASE_MGT_SK").alias("CASE_MGT_SK"),
        F.col("C.CD_MPPNG_SK").alias("CST_TYP_CD_SK")
    )
)

# ----------------------------------------------------------------------------
# STAGE: xfm_CheckLkpResults (CTransformerStage)
# ----------------------------------------------------------------------------
df_xfm_CheckLkpResults = (
    df_lkp_Code_SKs
    .withColumn(
        "svCaseMgtLkpFailCheck",
        F.when(
            (F.col("CASE_MGT_SK").isNull()) & (trim(F.col("CASE_MGT_ID")) != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svCaseTypeCdLkpFailCheck",
        F.when(
            (F.col("CST_TYP_CD_SK").isNull()) & (F.col("CMCS_MCTR_COST") != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svUsUsLkupFailCheck",
        F.when(
            (F.col("COST_INPUT_USER_SK").isNull()) & (F.col("CST_INPT_USER_ID") != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    # Bring forward columns needed in output or failures
    .withColumn("PRI_NAT_KEY_STRING", F.col("PRI_NAT_KEY_STRING"))
    .withColumn("FIRST_RECYC_TS", F.col("FIRST_RECYC_TS"))
)

# Lnk_SttsFkey_Main (no constraint)
df_Lnk_SttsFkey_Main = df_xfm_CheckLkpResults.select(
    F.col("CASE_MGT_CST_LOG_SK").alias("CASE_MGT_CST_LOG_SK"),
    F.col("CASE_MGT_ID").alias("CASE_MGT_ID"),
    F.col("CST_SEQ_NO").alias("CST_SEQ_NO"),
    F.col("SRC_SYS_CD_1").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("CASE_MGT_SK").isNull(), F.lit(0)).otherwise(F.col("CASE_MGT_SK")).alias("CASE_MGT_SK"),
    F.col("CST_INPT_USER_ID").alias("CST_INPT_USER_ID"),
    F.col("CST_INPT_DT").alias("CST_INPT_DT"),
    F.col("FROM_DT").alias("FROM_DT"),
    F.col("TO_DT").alias("TO_DT"),
    F.col("ACTL_CST_AMT").alias("ACTL_CST_AMT"),
    F.col("PROJ_CST_AMT").alias("PROJ_CST_AMT"),
    F.when(F.col("COST_INPUT_USER_SK").isNull(), F.lit(0)).otherwise(F.col("COST_INPUT_USER_SK")).alias("COST_INPUT_USER_SK"),
    F.when(F.col("CST_TYP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("CST_TYP_CD_SK")).alias("CST_TYP_CD_SK")
)

# Lnk_CMSttsUNK (constraint: first row only)
w = Window.orderBy(F.lit(1))
df_Lnk_CMSttsUNK_temp = df_xfm_CheckLkpResults.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
df_Lnk_CMSttsUNK = df_Lnk_CMSttsUNK_temp.select(
    F.lit(0).alias("CASE_MGT_CST_LOG_SK"),
    F.lit("UNK").alias("CASE_MGT_ID"),
    F.lit(0).alias("CST_SEQ_NO"),
    F.col("SRC_SYS_CD_1").alias("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CASE_MGT_SK"),
    F.lit("0").alias("CST_INPT_USER_ID"),
    F.lit("1753-01-01 00:00:00").cast(TimestampType()).alias("CST_INPT_DT"),
    F.lit("1753-01-01 00:00:00").cast(TimestampType()).alias("FROM_DT"),
    F.lit("1753-01-01 00:00:00").cast(TimestampType()).alias("TO_DT"),
    F.lit(0.0).alias("ACTL_CST_AMT"),
    F.lit(0.0).alias("PROJ_CST_AMT"),
    F.lit(0).alias("COST_INPUT_USER_SK"),
    F.lit(0).alias("CST_TYP_CD_SK")
)

# Lnk_CMstts_NA (constraint: first row only)
df_Lnk_CMstts_NA_temp = df_xfm_CheckLkpResults.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
df_Lnk_CMstts_NA = df_Lnk_CMstts_NA_temp.select(
    F.lit(1).alias("CASE_MGT_CST_LOG_SK"),
    F.lit("NA").alias("CASE_MGT_ID"),
    F.lit(1).alias("CST_SEQ_NO"),
    F.col("SRC_SYS_CD_1").alias("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CASE_MGT_SK"),
    F.lit("1").alias("CST_INPT_USER_ID"),
    F.lit("1753-01-01 00:00:00").cast(TimestampType()).alias("CST_INPT_DT"),
    F.lit("1753-01-01 00:00:00").cast(TimestampType()).alias("FROM_DT"),
    F.lit("1753-01-01 00:00:00").cast(TimestampType()).alias("TO_DT"),
    F.lit(0.0).alias("ACTL_CST_AMT"),
    F.lit(0.0).alias("PROJ_CST_AMT"),
    F.lit(1).alias("COST_INPUT_USER_SK"),
    F.lit(1).alias("CST_TYP_CD_SK")
)

# LNk_CaseMgt_Fail (svCaseMgtLkpFailCheck = 'Y')
df_Lnk_CaseMgt_Fail = df_xfm_CheckLkpResults.filter(F.col("svCaseMgtLkpFailCheck") == 'Y').select(
    F.col("CASE_MGT_CST_LOG_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_1").alias("SRC_SYS_CD_1"),
    F.lit("IdsCaseMgtCostFkey").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("CASE_MGT").alias("PHYSCL_FILE_NM"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CASE_MGT_ID")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# Lnk_CaseReasn_Fail (svCaseTypeCdLkpFailCheck = 'Y')
df_Lnk_CaseReasn_Fail = df_xfm_CheckLkpResults.filter(F.col("svCaseTypeCdLkpFailCheck") == 'Y').select(
    F.col("CASE_MGT_CST_LOG_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_1").alias("SRC_SYS_CD_1"),
    F.lit("IdsCaseMgtCostFkey").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CMCS_MCTR_COST")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# Lnk_UsUs_Fail (svUsUsLkupFailCheck = 'Y')
df_Lnk_UsUs_Fail = df_xfm_CheckLkpResults.filter(F.col("svUsUsLkupFailCheck") == 'Y').select(
    F.col("CASE_MGT_CST_LOG_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_1").alias("SRC_SYS_CD_1"),
    F.lit("IdsCaseMgtCostFkey").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("APP_USER").alias("PHYSCL_FILE_NM"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CST_INPT_USER_ID")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# ----------------------------------------------------------------------------
# STAGE: fnl_NA_UNK_Streams (PxFunnel)
# ----------------------------------------------------------------------------
common_cols_fnl_NA_UNK = [
    "CASE_MGT_CST_LOG_SK",
    "CASE_MGT_ID",
    "CST_SEQ_NO",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CASE_MGT_SK",
    "COST_INPUT_USER_SK",
    "CST_TYP_CD_SK",
    "CST_INPT_DT",
    "FROM_DT",
    "TO_DT",
    "ACTL_CST_AMT",
    "PROJ_CST_AMT",
    "CST_INPT_USER_ID"
]

df_fnl_NA_UNK_Streams = (
    df_Lnk_SttsFkey_Main.select(common_cols_fnl_NA_UNK)
    .unionByName(df_Lnk_CMSttsUNK.select(common_cols_fnl_NA_UNK))
    .unionByName(df_Lnk_CMstts_NA.select(common_cols_fnl_NA_UNK))
)

# ----------------------------------------------------------------------------
# STAGE: seq_CASE_MGT_COST_FKEY (PxSequentialFile) - WRITE
# ----------------------------------------------------------------------------
# Apply rpad for varchar columns with unknown length
df_fnl_NA_UNK_Streams_rpad = (
    df_fnl_NA_UNK_Streams
    .withColumn("CASE_MGT_ID", F.rpad(F.col("CASE_MGT_ID"), <...>, " "))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("CST_INPT_USER_ID", F.rpad(F.col("CST_INPT_USER_ID"), <...>, " "))
)

df_fnl_NA_UNK_Streams_final = df_fnl_NA_UNK_Streams_rpad.select(common_cols_fnl_NA_UNK)

write_files(
    df_fnl_NA_UNK_Streams_final,
    f"{adls_path}/load/CASE_MGT_CST.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# ----------------------------------------------------------------------------
# STAGE: FnlFkeyFailures (PxFunnel)
# ----------------------------------------------------------------------------
common_cols_fnl_fail = [
    "PRI_SK",
    "PRI_NAT_KEY_STRING",
    "SRC_SYS_CD_1",
    "JOB_NM",
    "ERROR_TYP",
    "PHYSCL_FILE_NM",
    "FRGN_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "JOB_EXCTN_SK"
]

df_FnlFkeyFailures = (
    df_Lnk_CaseReasn_Fail.select(common_cols_fnl_fail)
    .unionByName(df_Lnk_CaseMgt_Fail.select(common_cols_fnl_fail))
    .unionByName(df_Lnk_UsUs_Fail.select(common_cols_fnl_fail))
)

# ----------------------------------------------------------------------------
# STAGE: seq_FkeyFailedFile (PxSequentialFile) - WRITE
# ----------------------------------------------------------------------------
df_FnlFkeyFailures_rpad = (
    df_FnlFkeyFailures
    .withColumn("PRI_NAT_KEY_STRING", F.rpad(F.col("PRI_NAT_KEY_STRING"), <...>, " "))
    .withColumn("SRC_SYS_CD_1", F.rpad(F.col("SRC_SYS_CD_1"), <...>, " "))
    .withColumn("JOB_NM", F.rpad(F.col("JOB_NM"), <...>, " "))
    .withColumn("ERROR_TYP", F.rpad(F.col("ERROR_TYP"), <...>, " "))
    .withColumn("PHYSCL_FILE_NM", F.rpad(F.col("PHYSCL_FILE_NM"), <...>, " "))
    .withColumn("FRGN_NAT_KEY_STRING", F.rpad(F.col("FRGN_NAT_KEY_STRING"), <...>, " "))
)

df_FnlFkeyFailures_final = df_FnlFkeyFailures_rpad.select(common_cols_fnl_fail)

write_files(
    df_FnlFkeyFailures_final,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.IdsCaseMgtCostFkey.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)