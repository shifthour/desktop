# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC DESCRIPTION:  
# MAGIC 
# MAGIC   Pulls data from IDS Facets Audit table MBR_AUDIT into EDW table MBR_AUDIT_D
# MAGIC       
# MAGIC INPUTS:
# MAGIC 	
# MAGIC                 MBR_AUDIT
# MAGIC                 MBR
# MAGIC                 SUB
# MAGIC                 GRP
# MAGIC                 APP_USER
# MAGIC   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC              Naren Garapaty   11/01/2006  ---    Originally Programmed
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC 
# MAGIC O. Nielsen                7/25/2008        Facets 4.5.1           Change MBR_OTHR_CAR_POL_ID to VarChar(40)                  devlEDWnew                 Steph Goddard          08/25/2008
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               07/25/2013        5114                              Create Load File for EDW Table MBR_COB_AUDIT_D                    EnterpriseWhseDevl      Jag Yelavarthi             2013-10-22
# MAGIC 
# MAGIC Balkarn Gill               11/20/2013        5114                             NA and UNK changes are made as per the DG standards                EnterpriseWrhsDevl

# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwMbrCobAuditDExtr
# MAGIC Read from source table MBR_COB_AUDIT .  Apply Run Cycle filters when applicable to get just the needed rows forward
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC MBR_COB_AUDIT_ACTN_CD_SK,
# MAGIC MBR_COB_LAST_VER_METH_CD_SK,
# MAGIC MBR_COB_OTHR_CAR_ID_CD_SK,
# MAGIC MBR_COB_PAYMT_PRTY_CD_SK,
# MAGIC MBR_COB_TERM_RSN_CD_SK,
# MAGIC MBR_COB_TYP_CD_SK
# MAGIC Write MBR_COB_AUDIT_D Data into a Sequential file for Load Job IdsEdwMbrAuditDLoad.
# MAGIC Add Defaults and Null Handling.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve job parameters
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
IDSRunCycle = get_widget_value('IDSRunCycle', '')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate', '')
CurrRunCycle = get_widget_value('CurrRunCycle', '')

# --------------------------------------------------------------------------------
# Stage: db2_MBR_COB_AUDIT_in (DB2ConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url_db2_MBR_COB_AUDIT_in, jdbc_props_db2_MBR_COB_AUDIT_in = get_db_config(ids_secret_name)

extract_query_db2_MBR_COB_AUDIT_in = f"""
SELECT 
MBR_COB_AUDIT.MBR_COB_AUDIT_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
MBR_COB_AUDIT.MBR_COB_AUDIT_ROW_ID,
MBR_COB_AUDIT.CRT_RUN_CYC_EXCTN_SK,
MBR_COB_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK,
MBR_COB_AUDIT.MBR_SK,
MBR_COB_AUDIT.SRC_SYS_CRT_USER_SK,
MBR_COB_AUDIT.MBR_COB_AUDIT_ACTN_CD_SK,
MBR_COB_AUDIT.MBR_COB_LAST_VER_METH_CD_SK,
MBR_COB_AUDIT.MBR_COB_OTHR_CAR_ID_CD_SK,
MBR_COB_AUDIT.MBR_COB_PAYMT_PRTY_CD_SK,
MBR_COB_AUDIT.MBR_COB_TERM_RSN_CD_SK,
MBR_COB_AUDIT.MBR_COB_TYP_CD_SK,
MBR_COB_AUDIT.MBR_UNIQ_KEY,
MBR_COB_AUDIT.COB_LTR_TRGR_DT_SK,
MBR_COB_AUDIT.EFF_DT_SK,
MBR_COB_AUDIT.LACK_OF_COB_INFO_STRT_DT_SK,
MBR_COB_AUDIT.SRC_SYS_CRT_DT_SK,
MBR_COB_AUDIT.TERM_DT_SK,
MBR_COB_AUDIT.LAST_VERIFIER_TX,
MBR_COB_AUDIT.OTHR_CAR_POL_ID,
APP_USER.USER_ID,
GRP.GRP_SK,
GRP.GRP_ID,
GRP.GRP_UNIQ_KEY,
GRP.GRP_NM,
MBR.MBR_SFX_NO,
SUB.SUB_SK
FROM {IDSOwner}.MBR_COB_AUDIT MBR_COB_AUDIT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON MBR_COB_AUDIT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK,
{IDSOwner}.MBR MBR,
{IDSOwner}.SUB SUB,
{IDSOwner}.GRP GRP,
{IDSOwner}.APP_USER APP_USER
WHERE 
MBR_COB_AUDIT.MBR_SK=MBR.MBR_SK
AND MBR.SUB_SK=SUB.SUB_SK
AND SUB.GRP_SK=GRP.GRP_SK
AND MBR_COB_AUDIT.SRC_SYS_CRT_USER_SK=APP_USER.USER_SK
AND MBR_COB_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
"""

df_db2_MBR_COB_AUDIT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_MBR_COB_AUDIT_in)
    .options(**jdbc_props_db2_MBR_COB_AUDIT_in)
    .option("query", extract_query_db2_MBR_COB_AUDIT_in)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_CD_MPPNG_Extr (DB2ConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url_db2_CD_MPPNG_Extr, jdbc_props_db2_CD_MPPNG_Extr = get_db_config(ids_secret_name)

extract_query_db2_CD_MPPNG_Extr = f"""
SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM 
from {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CD_MPPNG_Extr)
    .options(**jdbc_props_db2_CD_MPPNG_Extr)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: Cpy_Mppng_Cd (PxCopy)
# --------------------------------------------------------------------------------
# This copy stage outputs the same three columns to multiple links.
df_Cpy_Mppng_Cd = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# Prepare multiple references (aliases) for the lookup stage
df_Ref_TermRsnCd_Lkup = df_Cpy_Mppng_Cd
df_Ref_LastVerMethCd_Lkp = df_Cpy_Mppng_Cd
df_Ref_AuditActnCd_Lkup = df_Cpy_Mppng_Cd
df_Ref_PaymtPartyCd_Lkup = df_Cpy_Mppng_Cd
df_Ref_OthrCarIdCdLkupOthrCarIdCd_Lkup = df_Cpy_Mppng_Cd
df_Ref_TypCdCd_Lkup = df_Cpy_Mppng_Cd

# --------------------------------------------------------------------------------
# Stage: lkp_Codes (PxLookup)
# --------------------------------------------------------------------------------

df_lkp_Codes = (
    df_db2_MBR_COB_AUDIT_in.alias("lnk_IdsEdwMbrCobAuditDExtr_InAbc")
    .join(
        df_Ref_AuditActnCd_Lkup.alias("Ref_AuditActnCd_Lkup"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.MBR_COB_AUDIT_ACTN_CD_SK")
        == col("Ref_AuditActnCd_Lkup.CD_MPPNG_SK"),
        how="left"
    )
    .join(
        df_Ref_LastVerMethCd_Lkp.alias("Ref_LastVerMethCd_Lkp"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.MBR_COB_LAST_VER_METH_CD_SK")
        == col("Ref_LastVerMethCd_Lkp.CD_MPPNG_SK"),
        how="left"
    )
    .join(
        df_Ref_OthrCarIdCdLkupOthrCarIdCd_Lkup.alias("Ref_OthrCarIdCdLkupOthrCarIdCd_Lkup"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.MBR_COB_OTHR_CAR_ID_CD_SK")
        == col("Ref_OthrCarIdCdLkupOthrCarIdCd_Lkup.CD_MPPNG_SK"),
        how="left"
    )
    .join(
        df_Ref_PaymtPartyCd_Lkup.alias("Ref_PaymtPartyCd_Lkup"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.MBR_COB_PAYMT_PRTY_CD_SK")
        == col("Ref_PaymtPartyCd_Lkup.CD_MPPNG_SK"),
        how="left"
    )
    .join(
        df_Ref_TermRsnCd_Lkup.alias("Ref_TermRsnCd_Lkup"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.MBR_COB_TERM_RSN_CD_SK")
        == col("Ref_TermRsnCd_Lkup.CD_MPPNG_SK"),
        how="left"
    )
    .join(
        df_Ref_TypCdCd_Lkup.alias("Ref_TypCdCd_Lkup"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.MBR_COB_TYP_CD_SK")
        == col("Ref_TypCdCd_Lkup.CD_MPPNG_SK"),
        how="left"
    )
    .select(
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.MBR_COB_AUDIT_SK").alias("MBR_COB_AUDIT_SK"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.MBR_COB_AUDIT_ROW_ID").alias("MBR_COB_AUDIT_ROW_ID"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.GRP_SK").alias("GRP_SK"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.MBR_SK").alias("MBR_SK"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.SUB_SK").alias("SUB_SK"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.GRP_ID").alias("GRP_ID"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.GRP_NM").alias("GRP_NM"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
        col("Ref_AuditActnCd_Lkup.TRGT_CD").alias("MBR_COB_AUDIT_ACTN_CD"),
        col("Ref_AuditActnCd_Lkup.TRGT_CD_NM").alias("MBR_COB_AUDIT_ACTN_NM"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.EFF_DT_SK").alias("MBR_COB_EFF_DT_SK"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.LACK_OF_COB_INFO_STRT_DT_SK").alias("MBR_COB_LACK_OF_COB_INFO_STRT_DT_SK"),
        col("Ref_LastVerMethCd_Lkp.TRGT_CD").alias("MBR_COB_LAST_VER_METH_CD"),
        col("Ref_LastVerMethCd_Lkp.TRGT_CD_NM").alias("MBR_COB_LAST_VER_METH_NM"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.LAST_VERIFIER_TX").alias("MBR_COB_LAST_VERIFIER_TX"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.COB_LTR_TRGR_DT_SK").alias("COB_LTR_TRGR_DT_SK"),
        col("Ref_OthrCarIdCdLkupOthrCarIdCd_Lkup.TRGT_CD").alias("MBR_COB_OTHR_CAR_ID_CD"),
        col("Ref_OthrCarIdCdLkupOthrCarIdCd_Lkup.TRGT_CD_NM").alias("MBR_COB_OTHER_CAR_ID_NM"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.OTHR_CAR_POL_ID").alias("MBR_COB_OTHR_CAR_POL_ID"),
        col("Ref_PaymtPartyCd_Lkup.TRGT_CD").alias("MBR_COB_PAYMT_PRTY_CD"),
        col("Ref_PaymtPartyCd_Lkup.TRGT_CD_NM").alias("MBR_COB_PAYMT_PRTY_NM"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.TERM_DT_SK").alias("MBR_COB_TERM_DT_SK"),
        col("Ref_TermRsnCd_Lkup.TRGT_CD").alias("MBR_COB_TERM_RSN_CD"),
        col("Ref_TermRsnCd_Lkup.TRGT_CD_NM").alias("MBR_COB_TERM_RSN_TRGT_CD_NM"),
        col("Ref_TypCdCd_Lkup.TRGT_CD").alias("MBR_COB_TYP_CD"),
        col("Ref_TypCdCd_Lkup.TRGT_CD_NM").alias("MBR_COB_TYP_NM"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.MBR_SFX_NO").alias("MBR_SFX_NO"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.USER_ID").alias("SRC_SYS_CRT_USER_ID"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.MBR_COB_AUDIT_ACTN_CD_SK").alias("MBR_COB_AUDIT_ACTN_CD_SK"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.MBR_COB_LAST_VER_METH_CD_SK").alias("MBR_COB_LAST_VER_METH_CD_SK"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.MBR_COB_OTHR_CAR_ID_CD_SK").alias("MBR_COB_OTHR_CAR_ID_CD_SK"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.MBR_COB_PAYMT_PRTY_CD_SK").alias("MBR_COB_PAYMT_PRTY_CD_SK"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.MBR_COB_TERM_RSN_CD_SK").alias("MBR_COB_TERM_RSN_CD_SK"),
        col("lnk_IdsEdwMbrCobAuditDExtr_InAbc.MBR_COB_TYP_CD_SK").alias("MBR_COB_TYP_CD_SK")
    )
)

# --------------------------------------------------------------------------------
# Stage: xfm_BusinessLogic (CTransformerStage)
# --------------------------------------------------------------------------------
# We produce three output links: lnk_Main, UNK, NA.

# 1) lnk_Main: Filter MBR_COB_AUDIT_SK not in {0,1}; plus column transformations
df_xfm_BusinessLogic_lnk_Main_filtered = df_lkp_Codes.filter(
    (col("MBR_COB_AUDIT_SK") != 0) & (col("MBR_COB_AUDIT_SK") != 1)
)

df_xfm_BusinessLogic_lnk_Main = df_xfm_BusinessLogic_lnk_Main_filtered.select(
    col("MBR_COB_AUDIT_SK").alias("MBR_COB_AUDIT_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("MBR_COB_AUDIT_ROW_ID").alias("MBR_COB_AUDIT_ROW_ID"),
    lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("GRP_SK").alias("GRP_SK"),
    col("MBR_SK").alias("MBR_SK"),
    col("SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    col("SUB_SK").alias("SUB_SK"),
    col("GRP_ID").alias("GRP_ID"),
    col("GRP_NM").alias("GRP_NM"),
    col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    when(trim(col("MBR_COB_AUDIT_ACTN_CD")) == '', lit("UNK")).otherwise(col("MBR_COB_AUDIT_ACTN_CD")).alias("MBR_COB_AUDIT_ACTN_CD"),
    when(trim(col("MBR_COB_AUDIT_ACTN_NM")) == '', lit("UNK")).otherwise(col("MBR_COB_AUDIT_ACTN_NM")).alias("MBR_COB_AUDIT_ACTN_NM"),
    col("MBR_COB_EFF_DT_SK").alias("MBR_COB_EFF_DT_SK"),
    col("MBR_COB_LACK_OF_COB_INFO_STRT_DT_SK").alias("MBR_COB_LACK_INFO_STRT_DT_SK"),
    when(trim(col("MBR_COB_LAST_VER_METH_CD")) == '', lit("UNK")).otherwise(col("MBR_COB_LAST_VER_METH_CD")).alias("MBR_COB_LAST_VER_METH_CD"),
    when(trim(col("MBR_COB_LAST_VER_METH_NM")) == '', lit("UNK")).otherwise(col("MBR_COB_LAST_VER_METH_NM")).alias("MBR_COB_LAST_VER_METH_NM"),
    col("MBR_COB_LAST_VERIFIER_TX").alias("MBR_COB_LAST_VERIFIER_TX"),
    col("COB_LTR_TRGR_DT_SK").alias("MBR_COB_LTR_TRGR_DT_SK"),
    when(trim(col("MBR_COB_OTHR_CAR_ID_CD")) == '', lit("UNK")).otherwise(col("MBR_COB_OTHR_CAR_ID_CD")).alias("MBR_COB_OTHR_CAR_ID_CD"),
    when(trim(col("MBR_COB_OTHER_CAR_ID_NM")) == '', lit("UNK")).otherwise(col("MBR_COB_OTHER_CAR_ID_NM")).alias("MBR_COB_OTHR_CAR_ID_NM"),
    col("MBR_COB_OTHR_CAR_POL_ID").alias("MBR_COB_OTHR_CAR_POL_ID"),
    when(trim(col("MBR_COB_PAYMT_PRTY_CD")) == '', lit("UNK")).otherwise(col("MBR_COB_PAYMT_PRTY_CD")).alias("MBR_COB_PAYMT_PRTY_CD"),
    when(trim(col("MBR_COB_PAYMT_PRTY_NM")) == '', lit("UNK")).otherwise(col("MBR_COB_PAYMT_PRTY_NM")).alias("MBR_COB_PAYMT_PRTY_NM"),
    col("MBR_COB_TERM_DT_SK").alias("MBR_COB_TERM_DT_SK"),
    when(trim(col("MBR_COB_TERM_RSN_CD")) == '', lit("UNK")).otherwise(col("MBR_COB_TERM_RSN_CD")).alias("MBR_COB_TERM_RSN_CD"),
    when(trim(col("MBR_COB_TERM_RSN_TRGT_CD_NM")) == '', lit("UNK")).otherwise(col("MBR_COB_TERM_RSN_TRGT_CD_NM")).alias("MBR_COB_TERM_RSN_NM"),
    when(trim(col("MBR_COB_TYP_CD")) == '', lit("UNK")).otherwise(col("MBR_COB_TYP_CD")).alias("MBR_COB_TYP_CD"),
    when(trim(col("MBR_COB_TYP_NM")) == '', lit("UNK")).otherwise(col("MBR_COB_TYP_NM")).alias("MBR_COB_TYP_NM"),
    col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    col("SRC_SYS_CRT_USER_ID").alias("SRC_SYS_CRT_USER_ID"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MBR_COB_AUDIT_ACTN_CD_SK").alias("MBR_COB_AUDIT_ACTN_CD_SK"),
    col("MBR_COB_LAST_VER_METH_CD_SK").alias("MBR_COB_LAST_VER_METH_CD_SK"),
    col("MBR_COB_OTHR_CAR_ID_CD_SK").alias("MBR_COB_OTHR_CAR_ID_CD_SK"),
    col("MBR_COB_PAYMT_PRTY_CD_SK").alias("MBR_COB_PAYMT_PRTY_CD_SK"),
    col("MBR_COB_TERM_RSN_CD_SK").alias("MBR_COB_TERM_RSN_CD_SK"),
    col("MBR_COB_TYP_CD_SK").alias("MBR_COB_TYP_CD_SK")
)

# 2) UNK link: single row if “((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1”
# We will create exactly one row matching the "WhereExpression" definitions.
data_UNK = [(
    0,                  # MBR_COB_AUDIT_SK
    'UNK',              # SRC_SYS_CD
    'UNK',              # MBR_COB_AUDIT_ROW_ID
    '1753-01-01',       # CRT_RUN_CYC_EXCTN_DT_SK
    '1753-01-01',       # LAST_UPDT_RUN_CYC_EXCTN_DT_SK
    0,                  # GRP_SK
    0,                  # MBR_SK
    0,                  # SRC_SYS_CRT_USER_SK
    0,                  # SUB_SK
    'UNK',              # GRP_ID
    'UNK',              # GRP_NM
    0,                  # GRP_UNIQ_KEY
    'UNK',              # MBR_COB_AUDIT_ACTN_CD
    'UNK',              # MBR_COB_AUDIT_ACTN_NM
    '1753-01-01',       # MBR_COB_EFF_DT_SK
    '1753-01-01',       # MBR_COB_LACK_INFO_STRT_DT_SK
    'UNK',              # MBR_COB_LAST_VER_METH_CD
    'UNK',              # MBR_COB_LAST_VER_METH_NM
    '',                 # MBR_COB_LAST_VERIFIER_TX
    '1753-01-01',       # MBR_COB_LTR_TRGR_DT_SK
    'UNK',              # MBR_COB_OTHR_CAR_ID_CD
    'UNK',              # MBR_COB_OTHR_CAR_ID_NM
    'UNK',              # MBR_COB_OTHR_CAR_POL_ID
    'UNK',              # MBR_COB_PAYMT_PRTY_CD
    'UNK',              # MBR_COB_PAYMT_PRTY_NM
    '1753-01-01',       # MBR_COB_TERM_DT_SK
    'UNK',              # MBR_COB_TERM_RSN_CD
    'UNK',              # MBR_COB_TERM_RSN_NM
    'UNK',              # MBR_COB_TYP_CD
    'UNK',              # MBR_COB_TYP_NM
    None,               # MBR_SFX_NO
    0,                  # MBR_UNIQ_KEY
    '1753-01-01',       # SRC_SYS_CRT_DT_SK
    'UNK',              # SRC_SYS_CRT_USER_ID
    100,                # CRT_RUN_CYC_EXCTN_SK
    100,                # LAST_UPDT_RUN_CYC_EXCTN_SK
    0,                  # MBR_COB_AUDIT_ACTN_CD_SK
    0,                  # MBR_COB_LAST_VER_METH_CD_SK
    0,                  # MBR_COB_OTHR_CAR_ID_CD_SK
    0,                  # MBR_COB_PAYMT_PRTY_CD_SK
    0,                  # MBR_COB_TERM_RSN_CD_SK
    0                   # MBR_COB_TYP_CD_SK
)]
schema_common = StructType([
    StructField("MBR_COB_AUDIT_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("MBR_COB_AUDIT_ROW_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("GRP_SK", IntegerType(), True),
    StructField("MBR_SK", IntegerType(), True),
    StructField("SRC_SYS_CRT_USER_SK", IntegerType(), True),
    StructField("SUB_SK", IntegerType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("GRP_NM", StringType(), True),
    StructField("GRP_UNIQ_KEY", IntegerType(), True),
    StructField("MBR_COB_AUDIT_ACTN_CD", StringType(), True),
    StructField("MBR_COB_AUDIT_ACTN_NM", StringType(), True),
    StructField("MBR_COB_EFF_DT_SK", StringType(), True),
    StructField("MBR_COB_LACK_INFO_STRT_DT_SK", StringType(), True),
    StructField("MBR_COB_LAST_VER_METH_CD", StringType(), True),
    StructField("MBR_COB_LAST_VER_METH_NM", StringType(), True),
    StructField("MBR_COB_LAST_VERIFIER_TX", StringType(), True),
    StructField("MBR_COB_LTR_TRGR_DT_SK", StringType(), True),
    StructField("MBR_COB_OTHR_CAR_ID_CD", StringType(), True),
    StructField("MBR_COB_OTHR_CAR_ID_NM", StringType(), True),
    StructField("MBR_COB_OTHR_CAR_POL_ID", StringType(), True),
    StructField("MBR_COB_PAYMT_PRTY_CD", StringType(), True),
    StructField("MBR_COB_PAYMT_PRTY_NM", StringType(), True),
    StructField("MBR_COB_TERM_DT_SK", StringType(), True),
    StructField("MBR_COB_TERM_RSN_CD", StringType(), True),
    StructField("MBR_COB_TERM_RSN_NM", StringType(), True),
    StructField("MBR_COB_TYP_CD", StringType(), True),
    StructField("MBR_COB_TYP_NM", StringType(), True),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("SRC_SYS_CRT_DT_SK", StringType(), True),
    StructField("SRC_SYS_CRT_USER_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("MBR_COB_AUDIT_ACTN_CD_SK", IntegerType(), True),
    StructField("MBR_COB_LAST_VER_METH_CD_SK", IntegerType(), True),
    StructField("MBR_COB_OTHR_CAR_ID_CD_SK", IntegerType(), True),
    StructField("MBR_COB_PAYMT_PRTY_CD_SK", IntegerType(), True),
    StructField("MBR_COB_TERM_RSN_CD_SK", IntegerType(), True),
    StructField("MBR_COB_TYP_CD_SK", IntegerType(), True)
])

df_xfm_BusinessLogic_UNK = spark.createDataFrame(data_UNK, schema_common)

# 3) NA link: similarly, one row with the “WhereExpression” definitions
data_NA = [(
    1,                  # MBR_COB_AUDIT_SK
    'NA',               # SRC_SYS_CD
    'NA',               # MBR_COB_AUDIT_ROW_ID
    '1753-01-01',       # CRT_RUN_CYC_EXCTN_DT_SK
    '1753-01-01',       # LAST_UPDT_RUN_CYC_EXCTN_DT_SK
    1,                  # GRP_SK
    1,                  # MBR_SK
    1,                  # SRC_SYS_CRT_USER_SK
    1,                  # SUB_SK
    'NA',               # GRP_ID
    'NA',               # GRP_NM
    1,                  # GRP_UNIQ_KEY
    'NA',               # MBR_COB_AUDIT_ACTN_CD
    'NA',               # MBR_COB_AUDIT_ACTN_NM
    '1753-01-01',       # MBR_COB_EFF_DT_SK
    '1753-01-01',       # MBR_COB_LACK_INFO_STRT_DT_SK
    'NA',               # MBR_COB_LAST_VER_METH_CD
    'NA',               # MBR_COB_LAST_VER_METH_NM
    '',                 # MBR_COB_LAST_VERIFIER_TX
    '1753-01-01',       # MBR_COB_LTR_TRGR_DT_SK
    'NA',               # MBR_COB_OTHR_CAR_ID_CD
    'NA',               # MBR_COB_OTHR_CAR_ID_NM
    'NA',               # MBR_COB_OTHR_CAR_POL_ID
    'NA',               # MBR_COB_PAYMT_PRTY_CD
    'NA',               # MBR_COB_PAYMT_PRTY_NM
    '1753-01-01',       # MBR_COB_TERM_DT_SK
    'NA',               # MBR_COB_TERM_RSN_CD
    'NA',               # MBR_COB_TERM_RSN_NM
    'NA',               # MBR_COB_TYP_CD
    'NA',               # MBR_COB_TYP_NM
    None,               # MBR_SFX_NO
    1,                  # MBR_UNIQ_KEY
    '1753-01-01',       # SRC_SYS_CRT_DT_SK
    'NA',               # SRC_SYS_CRT_USER_ID
    100,                # CRT_RUN_CYC_EXCTN_SK
    100,                # LAST_UPDT_RUN_CYC_EXCTN_SK
    1,                  # MBR_COB_AUDIT_ACTN_CD_SK
    1,                  # MBR_COB_LAST_VER_METH_CD_SK
    1,                  # MBR_COB_OTHR_CAR_ID_CD_SK
    1,                  # MBR_COB_PAYMT_PRTY_CD_SK
    1,                  # MBR_COB_TERM_RSN_CD_SK
    1                   # MBR_COB_TYP_CD_SK
)]
df_xfm_BusinessLogic_NA = spark.createDataFrame(data_NA, schema_common)

# --------------------------------------------------------------------------------
# Stage: fnl_dataLinks (PxFunnel)
# --------------------------------------------------------------------------------
# Union of NA, UNK, and lnk_Main in that order
df_fnl_dataLinks = df_xfm_BusinessLogic_NA.unionByName(df_xfm_BusinessLogic_UNK).unionByName(df_xfm_BusinessLogic_lnk_Main)

# --------------------------------------------------------------------------------
# Stage: seq_MBR_COB_AUDIT_D_Load (PxSequentialFile)
# --------------------------------------------------------------------------------
# Final select with rpad for char columns, ensuring column order matches the funnel's output schema.

df_final = df_fnl_dataLinks.select(
    col("MBR_COB_AUDIT_SK"),
    col("SRC_SYS_CD"),
    col("MBR_COB_AUDIT_ROW_ID"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("GRP_SK"),
    col("MBR_SK"),
    col("SRC_SYS_CRT_USER_SK"),
    col("SUB_SK"),
    col("GRP_ID"),
    col("GRP_NM"),
    col("GRP_UNIQ_KEY"),
    col("MBR_COB_AUDIT_ACTN_CD"),
    col("MBR_COB_AUDIT_ACTN_NM"),
    rpad(col("MBR_COB_EFF_DT_SK"), 10, " ").alias("MBR_COB_EFF_DT_SK"),
    rpad(col("MBR_COB_LACK_INFO_STRT_DT_SK"), 10, " ").alias("MBR_COB_LACK_INFO_STRT_DT_SK"),
    col("MBR_COB_LAST_VER_METH_CD"),
    col("MBR_COB_LAST_VER_METH_NM"),
    col("MBR_COB_LAST_VERIFIER_TX"),
    rpad(col("MBR_COB_LTR_TRGR_DT_SK"), 10, " ").alias("MBR_COB_LTR_TRGR_DT_SK"),
    col("MBR_COB_OTHR_CAR_ID_CD"),
    col("MBR_COB_OTHR_CAR_ID_NM"),
    col("MBR_COB_OTHR_CAR_POL_ID"),
    col("MBR_COB_PAYMT_PRTY_CD"),
    col("MBR_COB_PAYMT_PRTY_NM"),
    rpad(col("MBR_COB_TERM_DT_SK"), 10, " ").alias("MBR_COB_TERM_DT_SK"),
    col("MBR_COB_TERM_RSN_CD"),
    col("MBR_COB_TERM_RSN_NM"),
    col("MBR_COB_TYP_CD"),
    col("MBR_COB_TYP_NM"),
    rpad(col("MBR_SFX_NO"), 2, " ").alias("MBR_SFX_NO"),
    col("MBR_UNIQ_KEY"),
    rpad(col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    rpad(col("SRC_SYS_CRT_USER_ID"), 10, " ").alias("SRC_SYS_CRT_USER_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MBR_COB_AUDIT_ACTN_CD_SK"),
    col("MBR_COB_LAST_VER_METH_CD_SK"),
    col("MBR_COB_OTHR_CAR_ID_CD_SK"),
    col("MBR_COB_PAYMT_PRTY_CD_SK"),
    col("MBR_COB_TERM_RSN_CD_SK"),
    col("MBR_COB_TYP_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/MBR_COB_AUDIT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)