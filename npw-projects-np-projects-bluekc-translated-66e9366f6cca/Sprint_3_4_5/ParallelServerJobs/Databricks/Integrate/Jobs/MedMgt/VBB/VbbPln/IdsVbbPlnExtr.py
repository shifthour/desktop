# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY: IhmfConstituentVbbExtrSeq
# MAGIC  
# MAGIC PROCESSING:   Extracts data from Ihmf Constituent database and creates a key file for VBB_PLN
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam        2013-05-13     4963 VBB Phase III    Initial Programming                                                                       IntegrateNewDevl        Bhoomi Dasari           5/16/2013

# MAGIC IDS VBB_PLN Extract
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../../shared_containers/PrimaryKey/VbbPlnPK
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
VBBOwner = get_widget_value('VBBOwner','')
vbb_secret_name = get_widget_value('vbb_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','IHMFCNSTTNT')
SrcSysCdSk = get_widget_value('SrcSysCdSk','-1979267011')
RunId = get_widget_value('RunId','100')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
LastRunDtm = get_widget_value('LastRunDtm','2012-04-26 00:00:00.000')
CurrDate = get_widget_value('CurrDate','2013-04-26')
Environment = get_widget_value('Environment','Test')

# IHMFCONSTITUENT stage - CODBCStage
jdbc_url, jdbc_props = get_db_config(vbb_secret_name)

# Extract link
extract_query_1 = f"""
SELECT
    TZIM_HIPL_HEALTH_PLAN.HIPL_ID,
    TZIM_HIPL_HEALTH_PLAN.CDTP_CODE_TYPE_CATEGORY,
    TZIM_HIPL_HEALTH_PLAN.CDVL_VALUE_CATEGORY,
    TZIM_HIPL_HEALTH_PLAN.HIPL_NAME,
    TZIM_HIPL_HEALTH_PLAN.HIPL_DESC,
    TZIM_HIPL_HEALTH_PLAN.HIPL_YEAR,
    TZIM_HIPL_HEALTH_PLAN.HIPL_YEAR_IND,
    TZIM_HIPL_HEALTH_PLAN.HIPL_START_DT,
    TZIM_HIPL_HEALTH_PLAN.HIPL_END_DT,
    TZIM_HIPL_HEALTH_PLAN.HIPL_REENROLL_IND,
    TZIM_HIPL_HEALTH_PLAN.HIPL_ACTIVATED_IND,
    TZIM_HIPL_HEALTH_PLAN.HIPL_MEMBER_COMPARE_IND,
    TZIM_HIPL_HEALTH_PLAN.HIPL_TERMS,
    TZIM_HIPL_HEALTH_PLAN.HIPL_TERM_DT,
    TZIM_HIPL_HEALTH_PLAN.CDTP_CODE_TYPE_COVERAGE,
    TZIM_HIPL_HEALTH_PLAN.CDVL_VALUE_COVERAGE,
    TZIM_HIPL_HEALTH_PLAN.CDTP_CODE_TYPE_TERM_REASON,
    TZIM_HIPL_HEALTH_PLAN.CDVL_VALUE_TERM_REASON,
    TZIM_HIPL_HEALTH_PLAN.HIPL_CREATE_DT,
    TZIM_HIPL_HEALTH_PLAN.HIPL_CREATE_APP_USER,
    TZIM_HIPL_HEALTH_PLAN.HIPL_UPDATE_DT,
    TZIM_HIPL_HEALTH_PLAN.HIPL_UPDATE_DB_USER,
    TZIM_HIPL_HEALTH_PLAN.HIPL_UPDATE_APP_USER,
    TZIM_HIPL_HEALTH_PLAN.HIPL_UPDATE_SEQ
FROM {VBBOwner}.TZIM_HIPL_HEALTH_PLAN TZIM_HIPL_HEALTH_PLAN
WHERE
    TZIM_HIPL_HEALTH_PLAN.HIPL_ACTIVATED_IND in ('1', '2')
    AND TZIM_HIPL_HEALTH_PLAN.HIPL_UPDATE_DT > '{LastRunDtm}'
"""

df_IHMFCONSTITUENT_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_1)
    .load()
)

# COM link
extract_query_2 = f"""
SELECT 
    cast(LTRIM(RTRIM(TZIM_INPR_INCENTIVE_PGM.HIPL_ID)) as integer)
  as HIPL_ID
FROM {VBBOwner}.TZIM_INPR_INCENTIVE_PGM TZIM_INPR_INCENTIVE_PGM
WHERE
  TZIM_INPR_INCENTIVE_PGM.CDVL_VALUE_FUNCTION = 'COM'
"""

df_IHMFCONSTITUENT_COM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

# IDENT_COM link
extract_query_3 = f"""
SELECT
    cast(LTRIM(RTRIM(TZIM_INPR_INCENTIVE_PGM.HIPL_ID)) as integer)
  as HIPL_ID
FROM {VBBOwner}.TZIM_INPR_INCENTIVE_PGM TZIM_INPR_INCENTIVE_PGM
WHERE
  TZIM_INPR_INCENTIVE_PGM.CDVL_VALUE_FUNCTION IN ('AST', 'CAD', 'CHF', 'COP', 'DBT', 'DPN', 'HTN', 'METX', 'SMK')
"""

df_IHMFCONSTITUENT_IDENTCOM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_3)
    .load()
)

# Trim stage (CTransformerStage)
df_Trim = (
    df_IHMFCONSTITUENT_Extract
    .withColumn(
        "svHiplId",
        F.when(
            (F.col("HIPL_ID").isNull())
            | (trim(strip_field(F.col("HIPL_ID"))) == ''),
            F.lit("N")
        ).otherwise(F.lit("Y"))
    )
    .withColumn(
        "svYearIndcheck1",
        F.when(
            (trim(strip_field(F.col("HIPL_YEAR_IND"))) == '1')
            & (
                (F.col("HIPL_YEAR").isNull())
                | (trim(strip_field(F.col("HIPL_YEAR"))) == '')
            ),
            F.lit("N")
        ).otherwise(F.lit("Y"))
    )
    .withColumn(
        "svYearIndCheck0",
        F.when(
            (trim(strip_field(F.col("HIPL_YEAR_IND"))) == '0')
            & (
                (F.col("HIPL_START_DT").isNull())
                | (trim(strip_field(F.col("HIPL_START_DT"))) == '')
                | (F.col("HIPL_END_DT").isNull())
                | (trim(strip_field(F.col("HIPL_END_DT"))) == '')
            ),
            F.lit("N")
        ).otherwise(F.lit("Y"))
    )
    .withColumn(
        "svActivateInd",
        F.when(
            (trim(strip_field(F.col("HIPL_ACTIVATED_IND"))) == '0')
            & (
                (F.col("HIPL_TERM_DT").isNull())
                | (trim(strip_field(F.col("HIPL_TERM_DT"))) == '')
                | (F.col("CDVL_VALUE_TERM_REASON").isNull())
                | (trim(strip_field(F.col("CDVL_VALUE_TERM_REASON"))) == '')
            ),
            F.lit("N")
        ).otherwise(F.lit("Y"))
    )
    .withColumn(
        "svGoodRecord",
        F.when(
            (F.col("svHiplId") == 'Y')
            & (F.col("svYearIndcheck1") == 'Y')
            & (F.col("svYearIndCheck0") == 'Y')
            & (F.col("svActivateInd") == 'Y'),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

# PlanData link (constraint: svGoodRecord = 'Y')
df_TrimPlanData = df_Trim.filter(F.col("svGoodRecord") == 'Y').select(
    trim(strip_field(F.col("HIPL_ID"))).alias("HIPL_ID"),
    F.upper(trim(strip_field(F.col("CDVL_VALUE_CATEGORY")))).alias("CDVL_VALUE_CATEGORY"),
    F.upper(trim(strip_field(F.col("HIPL_NAME")))).alias("HIPL_NAME"),
    trim(strip_field(F.col("HIPL_YEAR"))).alias("HIPL_YEAR"),
    trim(strip_field(F.col("HIPL_YEAR_IND"))).alias("HIPL_YEAR_IND"),
    trim(strip_field(F.col("HIPL_START_DT"))).alias("HIPL_START_DT"),
    trim(strip_field(F.col("HIPL_END_DT"))).alias("HIPL_END_DT"),
    trim(strip_field(F.col("HIPL_TERM_DT"))).alias("HIPL_TERM_DT"),
    F.upper(trim(strip_field(F.col("CDVL_VALUE_COVERAGE")))).alias("CDVL_VALUE_COVERAGE"),
    F.upper(trim(strip_field(F.col("CDVL_VALUE_TERM_REASON")))).alias("CDVL_VALUE_TERM_REASON"),
    trim(strip_field(F.col("HIPL_CREATE_DT"))).alias("HIPL_CREATE_DT"),
    trim(strip_field(F.col("HIPL_UPDATE_DT"))).alias("HIPL_UPDATE_DT")
)

# Error link (constraint: svGoodRecord = 'N')
df_TrimError = df_Trim.filter(F.col("svGoodRecord") == 'N').select(
    "HIPL_ID",
    "CDTP_CODE_TYPE_CATEGORY",
    "CDVL_VALUE_CATEGORY",
    "HIPL_NAME",
    "HIPL_DESC",
    "HIPL_YEAR",
    "HIPL_YEAR_IND",
    "HIPL_START_DT",
    "HIPL_END_DT",
    "HIPL_REENROLL_IND",
    "HIPL_ACTIVATED_IND",
    "HIPL_MEMBER_COMPARE_IND",
    "HIPL_TERMS",
    "HIPL_TERM_DT",
    "CDTP_CODE_TYPE_COVERAGE",
    "CDVL_VALUE_COVERAGE",
    "CDTP_CODE_TYPE_TERM_REASON",
    "CDVL_VALUE_TERM_REASON",
    "HIPL_CREATE_DT",
    "HIPL_CREATE_APP_USER",
    "HIPL_UPDATE_DT",
    "HIPL_UPDATE_DB_USER",
    "HIPL_UPDATE_APP_USER",
    "HIPL_UPDATE_SEQ"
)

# Error_File (CSeqFileStage)
error_file_path = f"{adls_path_publish}/external/VBB_PLAN_ErrorsOut_{RunId}.dat"
write_files(
    df_TrimError,
    error_file_path,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# hf_vbbpln_vbbmdlcd_com (CHashedFileStage) - Scenario A
# Deduplicate on key column HIPL_ID
df_COM_lkup = df_IHMFCONSTITUENT_COM.dropDuplicates(["HIPL_ID"])

# hf_vbbpln_vbbmdlcd_identcom (CHashedFileStage) - Scenario A
# Deduplicate on key column HIPL_ID
df_IdentCom_lkup = df_IHMFCONSTITUENT_IDENTCOM.dropDuplicates(["HIPL_ID"])

# Transformer_6
df_Transformer6_joined = (
    df_TrimPlanData.alias("PlanData")
    .join(df_COM_lkup.alias("COM_lkup"), F.col("PlanData.HIPL_ID") == F.col("COM_lkup.HIPL_ID"), "left")
    .join(df_IdentCom_lkup.alias("IdentCom_lkup"), F.col("PlanData.HIPL_ID") == F.col("IdentCom_lkup.HIPL_ID"), "left")
    .withColumn("svHiplStartDt", F.col("PlanData.HIPL_START_DT").substr(F.lit(1), F.lit(10)))
    .withColumn("svHiplEndDt", F.col("PlanData.HIPL_END_DT").substr(F.lit(1), F.lit(10)))
    .withColumn("svHiplTermDt", F.col("PlanData.HIPL_TERM_DT").substr(F.lit(1), F.lit(10)))
    .withColumn("svHiplCreateDt", format_date(F.col("PlanData.HIPL_CREATE_DT"), 'SYBASE', 'TIMESTAMP', 'DB2TIMESTAMP'))
    .withColumn("svHiplUpdateDt", format_date(F.col("PlanData.HIPL_UPDATE_DT"), 'SYBASE', 'TIMESTAMP', 'DB2TIMESTAMP'))
)

df_Transformer6_AllCol = df_Transformer6_joined.select(
    F.col("PlanData.HIPL_ID").alias("VBB_PLN_UNIQ_KEY"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit('I'), 10, ' ').alias("INSRT_UPDT_CD"),
    F.rpad(F.lit('N'), 1, ' ').alias("DISCARD_IN"),
    F.rpad(F.lit('Y'), 1, ' ').alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD_1"),
    F.concat(trim(F.col("PlanData.HIPL_ID")), F.lit(';'), F.lit(SrcSysCd)).alias("PRI_KEY_STRING"),
    F.lit(0).alias("VBB_PLN_SK"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        (F.col("COM_lkup.HIPL_ID").isNotNull())
        & (F.col("IdentCom_lkup.HIPL_ID").isNotNull()),
        F.lit("MEMBER ENGAGEMENT")
    ).otherwise(F.lit("COST BARRIER")).alias("VBB_MDL_CD"),
    F.when(
        (F.col("PlanData.CDVL_VALUE_CATEGORY").isNull())
        | (trim(F.col("PlanData.CDVL_VALUE_CATEGORY")) == ''),
        F.lit("NA")
    ).otherwise(F.col("PlanData.CDVL_VALUE_CATEGORY")).alias("VBB_PLN_CAT_CD"),
    F.when(
        (F.col("PlanData.CDVL_VALUE_COVERAGE").isNull())
        | (trim(F.col("PlanData.CDVL_VALUE_COVERAGE")) == ''),
        F.lit("NA")
    ).otherwise(F.col("PlanData.CDVL_VALUE_COVERAGE")).alias("VBB_PLN_PROD_CAT_CD"),
    F.when(
        (F.col("PlanData.CDVL_VALUE_TERM_REASON").isNull())
        | (trim(F.col("PlanData.CDVL_VALUE_TERM_REASON")) == ''),
        F.lit("NA")
    ).otherwise(F.col("PlanData.CDVL_VALUE_TERM_REASON")).alias("VBB_PLN_TERM_RSN_CD"),
    F.rpad(
        F.when(F.col("PlanData.HIPL_YEAR_IND") == '1', F.lit('Y')).otherwise(F.lit('N')),
        1, ' '
    ).alias("VBB_PLN_STRT_YR_IN"),
    F.when(F.col("PlanData.HIPL_YEAR_IND") == '1', F.col("PlanData.HIPL_YEAR").cast(IntegerType())).otherwise(F.lit(0)).alias("VBB_PLN_STRT_YR_NO"),
    F.rpad(
        F.when(
            F.col("PlanData.HIPL_YEAR_IND") == '0',
            F.when(
                (F.col("svHiplStartDt").isNull()) | (trim(F.col("svHiplStartDt")) == ''),
                F.lit("1753-01-01")
            ).otherwise(F.col("svHiplStartDt"))
        ).otherwise(F.lit("1753-01-01")),
        10, ' '
    ).alias("VBB_PLN_STRT_DT_SK"),
    F.rpad(
        F.when(
            F.col("PlanData.HIPL_YEAR_IND") == '0',
            F.when(
                (F.col("svHiplEndDt").isNull()) | (trim(F.col("svHiplEndDt")) == ''),
                F.lit("1753-01-01")
            ).otherwise(F.col("svHiplEndDt"))
        ).otherwise(F.lit("1753-01-01")),
        10, ' '
    ).alias("VBB_PLN_END_DT_SK"),
    F.rpad(
        F.when(
            (F.col("svHiplTermDt").isNull()) | (trim(F.col("svHiplTermDt")) == ''),
            F.lit("2199-12-31")
        ).otherwise(F.col("svHiplTermDt")),
        10, ' '
    ).alias("VBB_PLN_TERM_DT_SK"),
    F.col("svHiplCreateDt").alias("SRC_SYS_CRT_DTM"),
    F.col("svHiplUpdateDt").alias("SRC_SYS_UPDT_DTM"),
    F.when(
        (F.col("PlanData.HIPL_NAME").isNull()) | (trim(F.col("PlanData.HIPL_NAME")) == ''),
        F.lit("UNK")
    ).otherwise(F.col("PlanData.HIPL_NAME")).alias("VBB_PLN_NM")
)

df_Transformer6_Transform = df_Transformer6_joined.select(
    F.col("PlanData.HIPL_ID").alias("VBB_PLN_UNIQ_KEY"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

df_Transformer6_Snapshot = df_Transformer6_joined.select(
    F.col("PlanData.HIPL_ID").alias("VBB_PLN_UNIQ_KEY"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

# B_VBB_PLN (CSeqFileStage)
b_vbb_pln_path = f"{adls_path}/load/B_VBB_PLN.dat"
write_files(
    df_Transformer6_Snapshot.select("VBB_PLN_UNIQ_KEY", "SRC_SYS_CD_SK"),
    b_vbb_pln_path,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# VbbPlnPK (Shared Container)
params = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}
df_VbbPlnPK_Key = VbbPlnPK(df_Transformer6_AllCol, df_Transformer6_Transform, params)

# Sequential_File_26 (CSeqFileStage)
seq_file_26_path = f"{adls_path}/key/IhmfConstituentVbbPlnExtr.VbbPln.dat.{RunId}"
df_seq_file_26 = df_VbbPlnPK_Key.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "VBB_PLN_SK",
    "VBB_PLN_UNIQ_KEY",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "VBB_MDL_CD",
    "VBB_PLN_CAT_CD",
    "VBB_PLN_PROD_CAT_CD",
    "VBB_PLN_TERM_RSN_CD",
    "VBB_PLN_STRT_YR_IN",
    "VBB_PLN_STRT_YR_NO",
    "VBB_PLN_STRT_DT_SK",
    "VBB_PLN_END_DT_SK",
    "VBB_PLN_TERM_DT_SK",
    "SRC_SYS_CRT_DTM",
    "SRC_SYS_UPDT_DTM",
    "VBB_PLN_NM"
)

write_files(
    df_seq_file_26,
    seq_file_26_path,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)