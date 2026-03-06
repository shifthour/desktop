# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     IdsIndvBePopHlthPgmEnrFExtr
# MAGIC 
# MAGIC CALLED BY:  IdsIndvBePopHlthPgmExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:  Extract data from IDS table INDV_BE_POP_HLTH_PGM_ENR_TRANS to create file to load into EDW table INDV_BE_POP_HLTH_PGM_ENR_F
# MAGIC                              
# MAGIC PROCESSING:   Runs daily after the IDS job which populates INDV_BE_POP_HLTH_PGM_ENR_TRANS.  Extracts new rows and rows updated since the last run of this job.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #                       Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kimberly Doty           07-20-2010       #4297 - Alineo Phase II         Original programming                                                                   EnterpriseNewDevl         Steph Goddard          07/22/2010
# MAGIC 
# MAGIC Kimberly Doty           07-23-2010       #4297 - Alineo Phase II         Added hf_cae_mbr_drvr to eliminate warnings when using         EnterpriseNewDevl         Steph Goddard           07/30/2010
# MAGIC                                                                                                        a direct extract from the table; added default values for all 
# MAGIC                                                                                                        CD/NM pairs
# MAGIC 
# MAGIC Karthik Chintalapani  03-10-2015       5157                                     Added 3 new columns   PGM_ORIG_SRC_SYS_CD,                    EnterpriseCurDevl         Kalyan Neelam           2015-03-18                              
# MAGIC                                                                                                             PGM_ORIG_SRC_SYS_NM,
# MAGIC                                                                                                             PGM_ORIG_SRC_SYS_CD_SK          
# MAGIC Jaideep Mankala      2019-06-11       US110616                             Added new filter source system code sk to extract facets data      EnterpriseDev2	   Abhiram Dasarathy      2019-06-12

# MAGIC IDS extract to populate EDW table INDV_BE_POP_HLTH_PGM_ENR_F
# MAGIC Hash File used in IdsIndvBePopHlthPgmDiagFExtr
# MAGIC IDS Individual BE Population Health Program Enrollment F Extract
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrentDate = get_widget_value('CurrentDate','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# --------------------------------------------------------------------------------
# Read from "hf_cdma_codes" (CHashedFileStage scenario C) as parquet
# --------------------------------------------------------------------------------
df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

# --------------------------------------------------------------------------------
# Stage: IDS_INDV_BE_POP_HLTH_PGM_ENR_TRANS (DB2Connector - read from IDS)
# --------------------------------------------------------------------------------
extract_query_IDS_INDV_BE_POP_HLTH_PGM_ENR_TRANS = f"""
SELECT 
INDV_BE_KEY,
POP_HLTH_PGM_ENR_ID,
SRC_SYS_CD_SK, 
ENR_DENIED_RSN_CD_SK, 
ENR_PROD_CD_SK, 
ENR_SVRTY_CD_SK, 
PGM_CLOSE_RSN_CD_SK,
PGM_SCRN_STTUS_CD_SK, 
PGM_SRC_CD_SK, 
SCRN_RQST_PROD_CD_SK, 
SCRN_RQST_SVRTY_CD_SK, 
PGM_ENR_CRT_DT_SK, 
PGM_CLOSE_DT_SK AS PGM_CLSD_DT_SK, 
PGM_ENR_DT_SK, 
PGM_RQST_DT_SK, 
PGM_SCRN_DT_SK, 
PGM_STRT_DT_SK, 
ROW_EFF_DT_SK, 
ENRED_BY_USER_ID, 
ENR_ASG_TO_USER_ID, 
SCRN_BY_USER_ID, 
LAST_UPDT_RUN_CYC_EXCTN_SK,
ENR_POP_HLTH_PGM_ID,
SCRN_POP_HLTH_PGM_ID,
SCRN_ASG_TO_USER_ID,
PGM_ORIG_SRC_SYS_CD_SK,
SRC_SYS_LAST_UPDT_DT_SK
FROM {IDSOwner}.INDV_BE_POP_HLTH_PGM_ENR_TRANS
WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
  AND ROW_TERM_DT_SK = '2199-12-31'
  AND SRC_SYS_CD_SK IN ({SrcSysCdSk})
"""
df_IDS_INDV_BE_POP_HLTH_PGM_ENR_TRANS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_INDV_BE_POP_HLTH_PGM_ENR_TRANS)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: P_CAE_MBR_DRVR_And_Other_Data (DB2Connector - read from IDS)
# --------------------------------------------------------------------------------
extract_query_P_CAE_MBR_DRVR_And_Other_Data = f"""
SELECT 
drvr.INDV_BE_KEY,
drvr.MBR_SK,
drvr.GRP_SK,
grp.GRP_ID,
mbrEnr.MBR_UNIQ_KEY,
prod.PROD_SH_NM_SK,
prodSN.PROD_SH_NM
FROM
{IDSOwner}.P_CAE_MBR_DRVR drvr,
{IDSOwner}.GRP grp,
{IDSOwner}.MBR_ENR mbrEnr,
{IDSOwner}.PROD prod,
{IDSOwner}.PROD_SH_NM prodSN,
{IDSOwner}.CD_MPPNG cd
WHERE drvr.PRI_IN = 'Y'
  AND drvr.GRP_SK = grp.GRP_SK
  AND drvr.MBR_SK = mbrEnr.MBR_SK
  AND mbrEnr.ELIG_IN = 'Y'
  AND (mbrEnr.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = cd.CD_MPPNG_SK AND cd.TRGT_CD = 'MED')
  AND (mbrEnr.EFF_DT_SK <= '{CurrentDate}' AND '{CurrentDate}' <= mbrEnr.TERM_DT_SK)
  AND mbrEnr.PROD_SK = prod.PROD_SK
  AND prod.PROD_SH_NM_SK = prodSN.PROD_SH_NM_SK
"""
df_P_CAE_MBR_DRVR_And_Other_Data = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_P_CAE_MBR_DRVR_And_Other_Data)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: hf_cae_mbr_drvr (CHashedFileStage) - Scenario A intermediate hashed file
# Deduplicate by key columns: INDV_BE_KEY
# --------------------------------------------------------------------------------
df_cae_mbr_drvr = dedup_sort(
    df_P_CAE_MBR_DRVR_And_Other_Data,
    partition_cols=["INDV_BE_KEY"],
    sort_cols=[]
)

# --------------------------------------------------------------------------------
# Stage: hf_indv_be_pop_hlth_pgm_enr_lkup (CHashedFileStage) - Scenario B read from dummy table
# We'll read from dummy table "dummy_hf_indv_be_pop_hlth_pgm_enr" in IDS
# This has columns: POP_HLTH_PGM_ENR_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, CRT_RUN_CYC_EXCTN_DT_SK, INDV_BE_POP_HLTH_PGM_ENR_SK
# --------------------------------------------------------------------------------
dummy_table_name_hf_indv_be_pop_hlth_pgm_enr = "IDS.dummy_hf_indv_be_pop_hlth_pgm_enr"
extract_query_indv_be_pop_hlth_lkup = """
SELECT
POP_HLTH_PGM_ENR_ID,
SRC_SYS_CD,
CRT_RUN_CYC_EXCTN_SK,
CRT_RUN_CYC_EXCTN_DT_SK,
INDV_BE_POP_HLTH_PGM_ENR_SK
FROM IDS.dummy_hf_indv_be_pop_hlth_pgm_enr
"""
df_dummy_indv_be_pop_hlth_pgm_enr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_indv_be_pop_hlth_lkup)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage) - chain all lookups
# --------------------------------------------------------------------------------
df_BusinessRules = (
    df_IDS_INDV_BE_POP_HLTH_PGM_ENR_TRANS.alias("Pgm_Data_Out")
    .join(
        df_hf_cdma_codes.alias("src_sys_cd_lkup"),
        F.col("Pgm_Data_Out.SRC_SYS_CD_SK") == F.col("src_sys_cd_lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("enr_denied_rsn_cd_lkup"),
        F.col("Pgm_Data_Out.ENR_DENIED_RSN_CD_SK") == F.col("enr_denied_rsn_cd_lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("enr_prod_cd_lkup"),
        F.col("Pgm_Data_Out.ENR_PROD_CD_SK") == F.col("enr_prod_cd_lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("enr_svrty_cd_lkup"),
        F.col("Pgm_Data_Out.ENR_SVRTY_CD_SK") == F.col("enr_svrty_cd_lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("close_rsn_cd_lkup"),
        F.col("Pgm_Data_Out.PGM_CLOSE_RSN_CD_SK") == F.col("close_rsn_cd_lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("scrn_sttus_cd_lkup"),
        F.col("Pgm_Data_Out.PGM_SCRN_STTUS_CD_SK") == F.col("scrn_sttus_cd_lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("pgm_src_cd_lkup"),
        F.col("Pgm_Data_Out.PGM_SRC_CD_SK") == F.col("pgm_src_cd_lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("scrn_rqst_prod_cd_lkup"),
        F.col("Pgm_Data_Out.SCRN_RQST_PROD_CD_SK") == F.col("scrn_rqst_prod_cd_lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("scrn_rqst_svrty_cd_lkup"),
        F.col("Pgm_Data_Out.SCRN_RQST_SVRTY_CD_SK") == F.col("scrn_rqst_svrty_cd_lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cae_mbr_drvr.alias("primacy"),
        F.col("Pgm_Data_Out.INDV_BE_KEY") == F.col("primacy.INDV_BE_KEY"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("Pgm_Orig_Src_Sys_Nm_lkup"),
        F.col("Pgm_Data_Out.PGM_ORIG_SRC_SYS_CD_SK") == F.col("Pgm_Orig_Src_Sys_Nm_lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("Pgm_Orig_Src_Sys_Cd_Lkup"),
        F.col("Pgm_Data_Out.PGM_ORIG_SRC_SYS_CD_SK") == F.col("Pgm_Orig_Src_Sys_Cd_Lkup.CD_MPPNG_SK"),
        "left"
    )
)

df_BusinessRules_key = df_BusinessRules.select(
    F.lit(None).alias("INDV_BE_POP_HLTH_PGM_ENR_SK"),  # Placeholder, set in next transformer
    F.col("Pgm_Data_Out.POP_HLTH_PGM_ENR_ID").alias("POP_HLTH_PGM_ENR_ID"),
    F.when(F.col("src_sys_cd_lkup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("src_sys_cd_lkup.TRGT_CD")).alias("SRC_SYS_CD"),
    F.when(F.col("primacy.GRP_SK").isNull(), F.lit(0)).otherwise(F.col("primacy.GRP_SK")).alias("PRI_GRP_SK"),
    F.when(F.col("primacy.MBR_SK").isNull(), F.lit(0)).otherwise(F.col("primacy.MBR_SK")).alias("PRI_MBR_SK"),
    F.when(F.col("primacy.PROD_SH_NM_SK").isNull(), F.lit(0)).otherwise(F.col("primacy.PROD_SH_NM_SK")).alias("PRI_PROD_SH_NM_SK"),
    F.when(F.col("enr_denied_rsn_cd_lkup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("enr_denied_rsn_cd_lkup.TRGT_CD")).alias("ENR_DENIED_RSN_CD"),
    F.when(F.col("enr_denied_rsn_cd_lkup.TRGT_CD_NM").isNull(), F.lit("UNKNOWN")).otherwise(F.col("enr_denied_rsn_cd_lkup.TRGT_CD_NM")).alias("ENR_DENIED_RSN_NM"),
    F.when(F.col("enr_prod_cd_lkup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("enr_prod_cd_lkup.TRGT_CD")).alias("ENR_PROD_CD"),
    F.when(F.col("enr_prod_cd_lkup.TRGT_CD_NM").isNull(), F.lit("UNKNOWN")).otherwise(F.col("enr_prod_cd_lkup.TRGT_CD_NM")).alias("ENR_PROD_NM"),
    F.when(F.col("enr_svrty_cd_lkup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("enr_svrty_cd_lkup.TRGT_CD")).alias("ENR_SVRTY_CD"),
    F.when(F.col("enr_svrty_cd_lkup.TRGT_CD_NM").isNull(), F.lit("UNKNOWN")).otherwise(F.col("enr_svrty_cd_lkup.TRGT_CD_NM")).alias("ENR_SVRTY_NM"),
    F.when(F.col("close_rsn_cd_lkup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("close_rsn_cd_lkup.TRGT_CD")).alias("PGM_CLOSE_RSN_CD"),
    F.when(F.col("close_rsn_cd_lkup.TRGT_CD_NM").isNull(), F.lit("UNKNOWN")).otherwise(F.col("close_rsn_cd_lkup.TRGT_CD_NM")).alias("PGM_CLOSE_RSN_NM"),
    F.when(F.col("scrn_sttus_cd_lkup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("scrn_sttus_cd_lkup.TRGT_CD")).alias("PGM_SCRN_STTUS_CD"),
    F.when(F.col("scrn_sttus_cd_lkup.TRGT_CD_NM").isNull(), F.lit("UNKNOWN")).otherwise(F.col("scrn_sttus_cd_lkup.TRGT_CD_NM")).alias("PGM_SCRN_STTUS_NM"),
    F.when(F.col("pgm_src_cd_lkup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("pgm_src_cd_lkup.TRGT_CD")).alias("PGM_SRC_CD"),
    F.when(F.col("pgm_src_cd_lkup.TRGT_CD_NM").isNull(), F.lit("UNKNOWN")).otherwise(F.col("pgm_src_cd_lkup.TRGT_CD_NM")).alias("PGM_SRC_NM"),
    F.when(F.col("scrn_rqst_prod_cd_lkup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("scrn_rqst_prod_cd_lkup.TRGT_CD")).alias("SCRN_RQST_PROD_CD"),
    F.when(F.col("scrn_rqst_prod_cd_lkup.TRGT_CD_NM").isNull(), F.lit("UNKNOWN")).otherwise(F.col("scrn_rqst_prod_cd_lkup.TRGT_CD_NM")).alias("SCRN_RQST_PROD_NM"),
    F.when(F.col("scrn_rqst_svrty_cd_lkup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("scrn_rqst_svrty_cd_lkup.TRGT_CD")).alias("SCRN_RQST_SVRTY_CD"),
    F.when(F.col("scrn_rqst_svrty_cd_lkup.TRGT_CD_NM").isNull(), F.lit("UNKNOWN")).otherwise(F.col("scrn_rqst_svrty_cd_lkup.TRGT_CD_NM")).alias("SCRN_RQST_SVRTY_NM"),
    F.col("Pgm_Data_Out.PGM_ENR_CRT_DT_SK").alias("ENR_CRT_DT_SK"),
    F.col("Pgm_Data_Out.PGM_CLSD_DT_SK").alias("PGM_CLSD_DT_SK"),
    F.col("Pgm_Data_Out.PGM_ENR_DT_SK").alias("PGM_ENR_DT_SK"),
    F.col("Pgm_Data_Out.PGM_RQST_DT_SK").alias("PGM_RQST_DT_SK"),
    F.col("Pgm_Data_Out.PGM_SCRN_DT_SK").alias("PGM_SCRN_DT_SK"),
    F.col("Pgm_Data_Out.PGM_STRT_DT_SK").alias("PGM_STRT_DT_SK"),
    F.col("Pgm_Data_Out.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("Pgm_Data_Out.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.when(
        F.col("primacy.MBR_UNIQ_KEY").isNull(), F.lit(0)
    ).otherwise(
        F.when(F.length(F.col("primacy.MBR_UNIQ_KEY"))==0, F.lit(0)).otherwise(F.col("primacy.MBR_UNIQ_KEY"))
    ).alias("PRI_MBR_UNIQ_KEY"),
    F.col("Pgm_Data_Out.ENRED_BY_USER_ID").alias("ENRED_BY_USER_ID"),
    F.col("Pgm_Data_Out.ENR_ASG_TO_USER_ID").alias("ENR_ASG_TO_USER_ID"),
    F.col("Pgm_Data_Out.ENR_POP_HLTH_PGM_ID").alias("ENR_POP_HLTH_PGM_ID"),
    F.when(
        F.col("primacy.GRP_ID").isNull(), F.lit("UNK")
    ).otherwise(
        F.when(F.length(F.col("primacy.GRP_ID"))==0, F.lit("UNK")).otherwise(F.col("primacy.GRP_ID"))
    ).alias("PRI_GRP_ID"),
    F.when(
        F.col("primacy.PROD_SH_NM").isNull(), F.lit("UNK")
    ).otherwise(
        F.when(F.length(F.col("primacy.PROD_SH_NM"))==0, F.lit("UNK")).otherwise(F.col("primacy.PROD_SH_NM"))
    ).alias("PRI_PROD_SH_NM"),
    F.col("Pgm_Data_Out.SCRN_BY_USER_ID").alias("SCRN_BY_USER_ID"),
    F.col("Pgm_Data_Out.SCRN_ASG_TO_USER_ID").alias("SCRN_ASG_TO_USER_ID"),
    F.col("Pgm_Data_Out.SCRN_POP_HLTH_PGM_ID").alias("SCRN_POP_HLTH_PGM_ID"),
    F.col("Pgm_Data_Out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Pgm_Data_Out.ENR_DENIED_RSN_CD_SK").alias("ENR_DENIED_RSN_CD_SK"),
    F.col("Pgm_Data_Out.ENR_PROD_CD_SK").alias("ENR_PROD_CD_SK"),
    F.col("Pgm_Data_Out.ENR_SVRTY_CD_SK").alias("ENR_SVRTY_CD_SK"),
    F.col("Pgm_Data_Out.PGM_CLOSE_RSN_CD_SK").alias("PGM_CLOSE_RSN_CD_SK"),
    F.col("Pgm_Data_Out.PGM_SCRN_STTUS_CD_SK").alias("PGM_SCRN_STTUS_CD_SK"),
    F.col("Pgm_Data_Out.PGM_SRC_CD_SK").alias("PGM_SRC_CD_SK"),
    F.col("Pgm_Data_Out.SCRN_RQST_SVRTY_CD_SK").alias("SCRN_RQST_SVRTY_CD_SK"),
    F.when(F.col("Pgm_Orig_Src_Sys_Cd_Lkup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("Pgm_Orig_Src_Sys_Cd_Lkup.TRGT_CD")).alias("PGM_ORIG_SRC_SYS_CD"),
    F.when(F.col("Pgm_Orig_Src_Sys_Nm_lkup.TRGT_CD_NM").isNull(), F.lit("UNKNOWN")).otherwise(F.col("Pgm_Orig_Src_Sys_Nm_lkup.TRGT_CD_NM")).alias("PGM_ORIG_SRC_SYS_NM"),
    F.col("Pgm_Data_Out.PGM_ORIG_SRC_SYS_CD_SK").alias("PGM_ORIG_SRC_SYS_CD_SK")
)

# --------------------------------------------------------------------------------
# Stage: PrimaryKey (CTransformerStage) reading from "BusinessRules_key" and lookup "hf_indv_be_pop_hlth_pgm_enr_lkup" (dummy table)
# We replicate the logic for: 
#  svPrimarySK = if lkup.INDV_BE_POP_HLTH_PGM_ENR_SK isNull => call KeyMgtGetNextValueConcurrent => SurrogateKeyGen
#  else => lkup.INDV_BE_POP_HLTH_PGM_ENR_SK
#  similarly for svCrtRunCycExctnSK, svCrtRunCycExctnDateSK
# Then produce 3 outputs: rows, DefaultNA, DefaultUNK
# We'll do a left join with df_dummy_indv_be_pop_hlth_pgm_enr
# --------------------------------------------------------------------------------
df_primarykey_raw = (
    df_BusinessRules_key.alias("key")
    .join(
        df_dummy_indv_be_pop_hlth_pgm_enr.alias("lkup"),
        [
            F.col("key.POP_HLTH_PGM_ENR_ID") == F.col("lkup.POP_HLTH_PGM_ENR_ID"),
            F.col("key.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD")
        ],
        "left"
    )
    .select(
        F.when(F.col("lkup.INDV_BE_POP_HLTH_PGM_ENR_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.INDV_BE_POP_HLTH_PGM_ENR_SK")).alias("svPrimarySK"),
        F.col("key.*"),
        F.when(F.col("lkup.INDV_BE_POP_HLTH_PGM_ENR_SK").isNull(), F.col("CurrRunCycle")).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK")).alias("svCrtRunCycExctnSK"),
        F.when(F.col("lkup.INDV_BE_POP_HLTH_PGM_ENR_SK").isNull(), F.col("CurrentDate")).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_DT_SK")).alias("svCrtRunCycExctnDateSK")
    )
)

# Now apply SurrogateKeyGen to fill svPrimarySK if null
df_enriched = df_primarykey_raw
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svPrimarySK',<schema>,<secret_name>)

df_primarykey_transformed = df_enriched.select(
    F.col("svPrimarySK").alias("INDV_BE_POP_HLTH_PGM_ENR_SK"),
    F.col("POP_HLTH_PGM_ENR_ID"),
    F.col("SRC_SYS_CD"),
    F.col("svCrtRunCycExctnDateSK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    current_date().alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PRI_GRP_SK"),
    F.col("PRI_MBR_SK"),
    F.col("PRI_PROD_SH_NM_SK"),
    F.when(F.col("ENR_DENIED_RSN_CD").isNull(), F.lit("UNK")).otherwise(F.col("ENR_DENIED_RSN_CD")).alias("ENR_DENIED_RSN_CD"),
    F.when(F.col("ENR_DENIED_RSN_NM").isNull(), F.lit("UNKNOWN")).otherwise(F.col("ENR_DENIED_RSN_NM")).alias("ENR_DENIED_RSN_NM"),
    F.when(F.col("ENR_PROD_CD").isNull(), F.lit("UNK")).otherwise(F.col("ENR_PROD_CD")).alias("ENR_PROD_CD"),
    F.when(F.col("ENR_PROD_NM").isNull(), F.lit("UNKNOWN")).otherwise(F.col("ENR_PROD_NM")).alias("ENR_PROD_NM"),
    F.when(F.col("ENR_SVRTY_CD").isNull(), F.lit("UNK")).otherwise(F.col("ENR_SVRTY_CD")).alias("ENR_SVRTY_CD"),
    F.when(F.col("ENR_SVRTY_NM").isNull(), F.lit("UNKNOWN")).otherwise(F.col("ENR_SVRTY_NM")).alias("ENR_SVRTY_NM"),
    F.when(F.col("PGM_CLOSE_RSN_CD").isNull(), F.lit("UNK")).otherwise(F.col("PGM_CLOSE_RSN_CD")).alias("PGM_CLOSE_RSN_CD"),
    F.when(F.col("PGM_CLOSE_RSN_NM").isNull(), F.lit("UNKNOWN")).otherwise(F.col("PGM_CLOSE_RSN_NM")).alias("PGM_CLOSE_RSN_NM"),
    F.when(F.col("PGM_SCRN_STTUS_CD").isNull(), F.lit("UNK")).otherwise(F.col("PGM_SCRN_STTUS_CD")).alias("PGM_SCRN_STTUS_CD"),
    F.when(F.col("PGM_SCRN_STTUS_NM").isNull(), F.lit("UNKNOWN")).otherwise(F.col("PGM_SCRN_STTUS_NM")).alias("PGM_SCRN_STTUS_NM"),
    F.when(F.col("PGM_SRC_CD").isNull(), F.lit("UNK")).otherwise(F.col("PGM_SRC_CD")).alias("PGM_SRC_CD"),
    F.when(F.col("PGM_SRC_NM").isNull(), F.lit("UNKNOWN")).otherwise(F.col("PGM_SRC_NM")).alias("PGM_SRC_NM"),
    F.when(F.col("SCRN_RQST_PROD_CD").isNull(), F.lit("UNK")).otherwise(F.col("SCRN_RQST_PROD_CD")).alias("SCRN_RQST_PROD_CD"),
    F.when(F.col("SCRN_RQST_PROD_NM").isNull(), F.lit("UNKNOWN")).otherwise(F.col("SCRN_RQST_PROD_NM")).alias("SCRN_RQST_PROD_NM"),
    F.when(F.col("SCRN_RQST_SVRTY_CD").isNull(), F.lit("UNK")).otherwise(F.col("SCRN_RQST_SVRTY_CD")).alias("SCRN_RQST_SVRTY_CD"),
    F.when(F.col("SCRN_RQST_SVRTY_NM").isNull(), F.lit("UNKNOWN")).otherwise(F.col("SCRN_RQST_SVRTY_NM")).alias("SCRN_RQST_SVRTY_NM"),
    F.col("ENR_CRT_DT_SK"),
    F.col("PGM_CLSD_DT_SK"),
    F.col("PGM_ENR_DT_SK"),
    F.col("PGM_RQST_DT_SK"),
    F.col("PGM_SCRN_DT_SK"),
    F.col("PGM_STRT_DT_SK"),
    F.col("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("INDV_BE_KEY"),
    F.col("PRI_MBR_UNIQ_KEY"),
    F.col("ENRED_BY_USER_ID"),
    F.col("ENR_ASG_TO_USER_ID"),
    F.col("ENR_POP_HLTH_PGM_ID"),
    F.col("PRI_GRP_ID"),
    F.col("PRI_PROD_SH_NM"),
    F.col("SCRN_BY_USER_ID"),
    F.col("SCRN_ASG_TO_USER_ID"),
    F.col("SCRN_POP_HLTH_PGM_ID"),
    F.col("svCrtRunCycExctnSK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ENR_DENIED_RSN_CD_SK"),
    F.col("ENR_PROD_CD_SK"),
    F.col("ENR_SVRTY_CD_SK"),
    F.col("PGM_CLOSE_RSN_CD_SK"),
    F.col("PGM_SCRN_STTUS_CD_SK"),
    F.col("PGM_SRC_CD_SK"),
    F.col("SCRN_RQST_SVRTY_CD_SK"),
    F.col("PGM_ORIG_SRC_SYS_CD"),
    F.col("PGM_ORIG_SRC_SYS_NM"),
    F.col("PGM_ORIG_SRC_SYS_CD_SK")
)

# Now replicate the multiple output links "rows", "DefaultNA", "DefaultUNK".
# "rows": no constraint => all records
df_primarykey_rows = df_primarykey_transformed

# "DefaultNA": constraint "@INROWNUM=1" => produce a single row with NA. We force an empty or single row for union
df_primarykey_DefaultNA = df_primarykey_transformed.limit(1).select(
    F.lit(1).alias("INDV_BE_POP_HLTH_PGM_ENR_SK"),
    F.lit("NA").alias("POP_HLTH_PGM_ENR_ID"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("PRI_GRP_SK"),
    F.lit(1).alias("PRI_MBR_SK"),
    F.lit(1).alias("PRI_PROD_SH_NM_SK"),
    F.lit("NA").alias("ENR_DENIED_RSN_CD"),
    F.lit("NA").alias("ENR_DENIED_RSN_NM"),
    F.lit("NA").alias("ENR_PROD_CD"),
    F.lit("NA").alias("ENR_PROD_NM"),
    F.lit("NA").alias("ENR_SVRTY_CD"),
    F.lit("NA").alias("ENR_SVRTY_NM"),
    F.lit("NA").alias("PGM_CLOSE_RSN_CD"),
    F.lit("NA").alias("PGM_CLOSE_RSN_NM"),
    F.lit("NA").alias("PGM_SCRN_STTUS_CD"),
    F.lit("NA").alias("PGM_SCRN_STTUS_NM"),
    F.lit("NA").alias("PGM_SRC_CD"),
    F.lit("NA").alias("PGM_SRC_NM"),
    F.lit("NA").alias("SCRN_RQST_PROD_CD"),
    F.lit("NA").alias("SCRN_RQST_PROD_NM"),
    F.lit("NA").alias("SCRN_RQST_SVRTY_CD"),
    F.lit("NA").alias("SCRN_RQST_SVRTY_NM"),
    F.lit("1753-01-01").alias("ENR_CRT_DT_SK"),
    F.lit("1753-01-01").alias("PGM_CLSD_DT_SK"),
    F.lit("1753-01-01").alias("PGM_ENR_DT_SK"),
    F.lit("1753-01-01").alias("PGM_RQST_DT_SK"),
    F.lit("1753-01-01").alias("PGM_SCRN_DT_SK"),
    F.lit("1753-01-01").alias("PGM_STRT_DT_SK"),
    F.lit("1753-01-01").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.lit(1).alias("INDV_BE_KEY"),
    F.lit(1).alias("PRI_MBR_UNIQ_KEY"),
    F.lit("NA").alias("ENRED_BY_USER_ID"),
    F.lit("NA").alias("ENR_ASG_TO_USER_ID"),
    F.lit("NA").alias("ENR_POP_HLTH_PGM_ID"),
    F.lit("NA").alias("PRI_GRP_ID"),
    F.lit("NA").alias("PRI_PROD_SH_NM"),
    F.lit("NA").alias("SCRN_BY_USER_ID"),
    F.lit("NA").alias("SCRN_ASG_TO_USER_ID"),
    F.lit("NA").alias("SCRN_POP_HLTH_PGM_ID"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("ENR_DENIED_RSN_CD_SK"),
    F.lit(1).alias("ENR_PROD_CD_SK"),
    F.lit(1).alias("ENR_SVRTY_CD_SK"),
    F.lit(1).alias("PGM_CLOSE_RSN_CD_SK"),
    F.lit(1).alias("PGM_SCRN_STTUS_CD_SK"),
    F.lit(1).alias("PGM_SRC_CD_SK"),
    F.lit(1).alias("SCRN_RQST_SVRTY_CD_SK"),
    F.lit("NA").alias("PGM_ORIG_SRC_SYS_CD"),
    F.lit("NA").alias("PGM_ORIG_SRC_SYS_NM"),
    F.lit(1).alias("PGM_ORIG_SRC_SYS_CD_SK")
)

# "DefaultUNK": constraint "@INROWNUM=1" => single row with UNK
df_primarykey_DefaultUNK = df_primarykey_transformed.limit(1).select(
    F.lit(0).alias("INDV_BE_POP_HLTH_PGM_ENR_SK"),
    F.lit("UNK").alias("POP_HLTH_PGM_ENR_ID"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("PRI_GRP_SK"),
    F.lit(0).alias("PRI_MBR_SK"),
    F.lit(0).alias("PRI_PROD_SH_NM_SK"),
    F.lit("UNK").alias("ENR_DENIED_RSN_CD"),
    F.lit("UNK").alias("ENR_DENIED_RSN_NM"),
    F.lit("UNK").alias("ENR_PROD_CD"),
    F.lit("UNK").alias("ENR_PROD_NM"),
    F.lit("UNK").alias("ENR_SVRTY_CD"),
    F.lit("UNK").alias("ENR_SVRTY_NM"),
    F.lit("UNK").alias("PGM_CLOSE_RSN_CD"),
    F.lit("UNK").alias("PGM_CLOSE_RSN_NM"),
    F.lit("UNK").alias("PGM_SCRN_STTUS_CD"),
    F.lit("UNK").alias("PGM_SCRN_STTUS_NM"),
    F.lit("UNK").alias("PGM_SRC_CD"),
    F.lit("UNK").alias("PGM_SRC_NM"),
    F.lit("UNK").alias("SCRN_RQST_PROD_CD"),
    F.lit("UNK").alias("SCRN_RQST_PROD_NM"),
    F.lit("UNK").alias("SCRN_RQST_SVRTY_CD"),
    F.lit("UNK").alias("SCRN_RQST_SVRTY_NM"),
    F.lit("1753-01-01").alias("ENR_CRT_DT_SK"),
    F.lit("1753-01-01").alias("PGM_CLSD_DT_SK"),
    F.lit("1753-01-01").alias("PGM_ENR_DT_SK"),
    F.lit("1753-01-01").alias("PGM_RQST_DT_SK"),
    F.lit("1753-01-01").alias("PGM_SCRN_DT_SK"),
    F.lit("1753-01-01").alias("PGM_STRT_DT_SK"),
    F.lit("1753-01-01").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.lit(0).alias("INDV_BE_KEY"),
    F.lit(0).alias("PRI_MBR_UNIQ_KEY"),
    F.lit("UNK").alias("ENRED_BY_USER_ID"),
    F.lit("UNK").alias("ENR_ASG_TO_USER_ID"),
    F.lit("UNK").alias("ENR_POP_HLTH_PGM_ID"),
    F.lit("UNK").alias("PRI_GRP_ID"),
    F.lit("UNK").alias("PRI_PROD_SH_NM"),
    F.lit("UNK").alias("SCRN_BY_USER_ID"),
    F.lit("UNK").alias("SCRN_ASG_TO_USER_ID"),
    F.lit("UNK").alias("SCRN_POP_HLTH_PGM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("ENR_DENIED_RSN_CD_SK"),
    F.lit(0).alias("ENR_PROD_CD_SK"),
    F.lit(0).alias("ENR_SVRTY_CD_SK"),
    F.lit(0).alias("PGM_CLOSE_RSN_CD_SK"),
    F.lit(0).alias("PGM_SCRN_STTUS_CD_SK"),
    F.lit(0).alias("PGM_SRC_CD_SK"),
    F.lit(0).alias("SCRN_RQST_SVRTY_CD_SK"),
    F.lit("UNK").alias("PGM_ORIG_SRC_SYS_CD"),
    F.lit("UNK").alias("PGM_ORIG_SRC_SYS_NM"),
    F.lit(0).alias("PGM_ORIG_SRC_SYS_CD_SK")
)

df_link_collector_138 = df_primarykey_rows.unionByName(df_primarykey_DefaultNA).unionByName(df_primarykey_DefaultUNK)

# --------------------------------------------------------------------------------
# Stage: hf_indv_be_pop_hlth_pgm_enr_updt (CHashedFileStage) - Scenario B write to dummy table
# We must upsert into dummy_hf_indv_be_pop_hlth_pgm_enr with PK(POP_HLTH_PGM_ENR_ID, SRC_SYS_CD)
# Columns to update: POP_HLTH_PGM_ENR_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, CRT_RUN_CYC_EXCTN_DT_SK, INDV_BE_POP_HLTH_PGM_ENR_SK
# --------------------------------------------------------------------------------
df_updt = df_primarykey_transformed.select(
    F.col("POP_HLTH_PGM_ENR_ID"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("INDV_BE_POP_HLTH_PGM_ENR_SK")
)

# Create a temp staging table
execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsIndvBePopHlthPgmEnrFExtr_hf_indv_be_pop_hlth_pgm_enr_updt", jdbc_url_ids, jdbc_props_ids)
df_updt.write.jdbc(
    url=jdbc_url_ids,
    table="STAGING.IdsIndvBePopHlthPgmEnrFExtr_hf_indv_be_pop_hlth_pgm_enr_updt_temp",
    mode="overwrite",
    properties=jdbc_props_ids
)

merge_sql_hf_indv_be_pop_hlth_pgm_enr_updt = f"""
MERGE INTO {dummy_table_name_hf_indv_be_pop_hlth_pgm_enr} AS T
USING STAGING.IdsIndvBePopHlthPgmEnrFExtr_hf_indv_be_pop_hlth_pgm_enr_updt_temp AS S
ON T.POP_HLTH_PGM_ENR_ID = S.POP_HLTH_PGM_ENR_ID
AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN UPDATE SET
  T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
  T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
  T.INDV_BE_POP_HLTH_PGM_ENR_SK = S.INDV_BE_POP_HLTH_PGM_ENR_SK
WHEN NOT MATCHED THEN INSERT
(
  POP_HLTH_PGM_ENR_ID,
  SRC_SYS_CD,
  CRT_RUN_CYC_EXCTN_SK,
  CRT_RUN_CYC_EXCTN_DT_SK,
  INDV_BE_POP_HLTH_PGM_ENR_SK
)
VALUES
(
  S.POP_HLTH_PGM_ENR_ID,
  S.SRC_SYS_CD,
  S.CRT_RUN_CYC_EXCTN_SK,
  S.CRT_RUN_CYC_EXCTN_DT_SK,
  S.INDV_BE_POP_HLTH_PGM_ENR_SK
);
"""
execute_dml(merge_sql_hf_indv_be_pop_hlth_pgm_enr_updt, jdbc_url_ids, jdbc_props_ids)

# --------------------------------------------------------------------------------
# Stage: Link_Collector_138 => df_link_collector_138 is already the union
# --------------------------------------------------------------------------------
# Stage: IndvBePopHlthPgmEnrF (CSeqFileStage) => Write to .dat file with CSV rules
# Must preserve column order and do rpad for char/varchar columns with known length
# --------------------------------------------------------------------------------

final_cols = [
    ("INDV_BE_POP_HLTH_PGM_ENR_SK", None),
    ("POP_HLTH_PGM_ENR_ID", None),
    ("SRC_SYS_CD", None),
    ("CRT_RUN_CYC_EXCTN_DT_SK", 10),
    ("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10),
    ("PRI_GRP_SK", None),
    ("PRI_MBR_SK", None),
    ("PRI_PROD_SH_NM_SK", None),
    ("ENR_DENIED_RSN_CD", None),
    ("ENR_DENIED_RSN_NM", None),
    ("ENR_PROD_CD", None),
    ("ENR_PROD_NM", None),
    ("ENR_SVRTY_CD", None),
    ("ENR_SVRTY_NM", None),
    ("PGM_CLOSE_RSN_CD", None),
    ("PGM_CLOSE_RSN_NM", None),
    ("PGM_SCRN_STTUS_CD", None),
    ("PGM_SCRN_STTUS_NM", None),
    ("PGM_SRC_CD", None),
    ("PGM_SRC_NM", None),
    ("SCRN_RQST_PROD_CD", None),
    ("SCRN_RQST_PROD_NM", None),
    ("SCRN_RQST_SVRTY_CD", None),
    ("SCRN_RQST_SVRTY_NM", None),
    ("ENR_CRT_DT_SK", 10),
    ("PGM_CLSD_DT_SK", 10),
    ("PGM_ENR_DT_SK", 10),
    ("PGM_RQST_DT_SK", 10),
    ("PGM_SCRN_DT_SK", 10),
    ("PGM_STRT_DT_SK", 10),
    ("SRC_SYS_LAST_UPDT_DT_SK", 10),
    ("INDV_BE_KEY", None),
    ("PRI_MBR_UNIQ_KEY", None),
    ("ENRED_BY_USER_ID", None),
    ("ENR_ASG_TO_USER_ID", None),
    ("ENR_POP_HLTH_PGM_ID", None),
    ("PRI_GRP_ID", None),
    ("PRI_PROD_SH_NM", None),
    ("SCRN_BY_USER_ID", None),
    ("SCRN_ASG_TO_USER_ID", None),
    ("SCRN_POP_HLTH_PGM_ID", None),
    ("CRT_RUN_CYC_EXCTN_SK", None),
    ("LAST_UPDT_RUN_CYC_EXCTN_SK", None),
    ("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", None),
    ("ENR_DENIED_RSN_CD_SK", None),
    ("ENR_PROD_CD_SK", None),
    ("ENR_SVRTY_CD_SK", None),
    ("PGM_CLOSE_RSN_CD_SK", None),
    ("PGM_SCRN_STTUS_CD_SK", None),
    ("PGM_SRC_CD_SK", None),
    ("SCRN_RQST_SVRTY_CD_SK", None),
    ("PGM_ORIG_SRC_SYS_CD", None),
    ("PGM_ORIG_SRC_SYS_NM", None),
    ("PGM_ORIG_SRC_SYS_CD_SK", None)
]

df_final = df_link_collector_138
for c, length_val in final_cols:
    if length_val is not None:
        df_final = df_final.withColumn(c, F.rpad(F.col(c).cast(StringType()), length_val, " "))
    else:
        # If it's declared as char or varchar without length, we skip rpad because we have no length
        pass

df_final = df_final.select([F.col(colName) for colName, _ in final_cols])

write_files(
    df_final,
    f"{adls_path}/load/INDV_BE_POP_HLTH_PGM_ENR_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)