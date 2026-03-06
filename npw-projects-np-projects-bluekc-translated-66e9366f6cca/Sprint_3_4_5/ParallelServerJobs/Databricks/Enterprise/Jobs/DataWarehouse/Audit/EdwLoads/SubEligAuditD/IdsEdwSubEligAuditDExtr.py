# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC DESCRIPTION:  
# MAGIC 
# MAGIC   Pulls data from IDS Facets Audit table SUB_ELIG_AUDIT into EDW table SUB_ELIG_AUDIT_D
# MAGIC       
# MAGIC INPUTS:
# MAGIC 	
# MAGIC                 SUB_ELIG_AUDIT
# MAGIC                 CLS_PLN
# MAGIC                 GRP
# MAGIC                 EXCD
# MAGIC                 APP_USER
# MAGIC   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                Parikshith Chada    10/30/2006  ---    Originally Programmed
# MAGIC 
# MAGIC Developer                Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed      
# MAGIC ------------------              --------------------	------------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------   
# MAGIC Kalyan Neelam         2011-11-14               TTR-456                Updated Extract SQL, retreiving CLS_PLN_SK from                    EnterpriseNewDevl           sandrew                   2011-11-17
# MAGIC                                                                                                 SUB_ELIG_AUDIT table, also removed join to the SUB_ELIG table
# MAGIC 
# MAGIC 
# MAGIC Balkarn Gill               07/30/2013        5114                              Create Load File for EDW Table SUB_ELIG_AUDIT_D                EnterpriseWhseDevl     Jag Yelavarthi          2013-10-22        
# MAGIC 
# MAGIC Balkarn Gill               11/20/2013        5114                             NA and UNK changes are made as per the DG standards                EnterpriseWrhsDevl

# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwSubEligAuditDExtr
# MAGIC Read from source table SUB_ELIG_AUDIT .  Apply Run Cycle filters when applicable to get just the needed rows forward
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC SUB_ELIG_AUDIT_ACTN_CD_SK,
# MAGIC SUB_ELIG_CLS_PROD_CAT_CD_SK,
# MAGIC SUB_ELIG_TYP_CD_SK
# MAGIC Write SUB_ELIG_AUDIT_D Data into a Sequential file for Load Job IdsEdwSubEligAuditDLoad.
# MAGIC Add Defaults and Null Handling.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_SUB_ELIG_AUDIT_in = f"""
SELECT distinct
SUB_ELIG_AUDIT.SUB_ELIG_AUDIT_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
SUB_ELIG_AUDIT.SUB_ELIG_AUDIT_ROW_ID,
SUB_ELIG_AUDIT.CRT_RUN_CYC_EXCTN_SK,
SUB_ELIG_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK,
SUB_ELIG_AUDIT.EXCD_SK,
SUB_ELIG_AUDIT.SRC_SYS_CRT_USER_SK,
SUB_ELIG_AUDIT.SUB_SK,
SUB_ELIG_AUDIT.SUB_ELIG_AUDIT_ACTN_CD_SK,
SUB_ELIG_AUDIT.SUB_ELIG_CLS_PROD_CAT_CD_SK,
SUB_ELIG_AUDIT.SUB_ELIG_TYP_CD_SK,
SUB_ELIG_AUDIT.VOID_IN,
SUB_ELIG_AUDIT.EFF_DT_SK,
SUB_ELIG_AUDIT.SRC_SYS_CRT_DT_SK,
SUB_ELIG_AUDIT.SUB_UNIQ_KEY,
SUB_ELIG_AUDIT.CLS_PLN_SK,
CLS_PLN.CLS_PLN_ID,
GRP.GRP_SK,
GRP.GRP_ID,
GRP.GRP_UNIQ_KEY,
GRP.GRP_NM,
EXCD.EXCD_ID,
APP_USER.USER_ID
FROM {IDSOwner}.SUB_ELIG_AUDIT SUB_ELIG_AUDIT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
  ON SUB_ELIG_AUDIT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK,
{IDSOwner}.SUB SUB,
{IDSOwner}.CLS_PLN CLS_PLN,
{IDSOwner}.GRP GRP,
{IDSOwner}.EXCD EXCD,
{IDSOwner}.APP_USER APP_USER
WHERE SUB_ELIG_AUDIT.SUB_SK=SUB.SUB_SK
  AND SUB.GRP_SK=GRP.GRP_SK
  AND SUB_ELIG_AUDIT.CLS_PLN_SK=CLS_PLN.CLS_PLN_SK
  AND SUB_ELIG_AUDIT.SRC_SYS_CRT_USER_SK=APP_USER.USER_SK
  AND SUB_ELIG_AUDIT.EXCD_SK=EXCD.EXCD_SK
  AND (SUB_ELIG_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle} OR (SUB_ELIG_AUDIT.SUB_ELIG_AUDIT_SK = 0 OR SUB_ELIG_AUDIT.SUB_ELIG_AUDIT_SK = 1))
"""

df_db2_SUB_ELIG_AUDIT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_SUB_ELIG_AUDIT_in)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = f"""
SELECT 
 CD_MPPNG_SK,
 COALESCE(TRGT_CD,'UNK') TRGT_CD,
 COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_Ref_SubEligAuditActnCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_SubEligTypCd_Lkup = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_SubEligClsProdCatCd_Lkup = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_Codes = (
    df_db2_SUB_ELIG_AUDIT_in.alias("lnk_IdsEdwSubEligAuditDExtr_InAbc")
    .join(
        df_Ref_SubEligAuditActnCd_Lkp.alias("Ref_SubEligAuditActnCd_Lkp"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.SUB_ELIG_AUDIT_ACTN_CD_SK") 
        == F.col("Ref_SubEligAuditActnCd_Lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_SubEligClsProdCatCd_Lkup.alias("Ref_SubEligClsProdCatCd_Lkup"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.SUB_ELIG_CLS_PROD_CAT_CD_SK") 
        == F.col("Ref_SubEligClsProdCatCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_SubEligTypCd_Lkup.alias("Ref_SubEligTypCd_Lkup"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.SUB_ELIG_TYP_CD_SK") 
        == F.col("Ref_SubEligTypCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.SUB_ELIG_AUDIT_SK").alias("SUB_ELIG_AUDIT_SK"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.SUB_ELIG_AUDIT_ROW_ID").alias("SUB_ELIG_AUDIT_ROW_ID"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.EXCD_SK").alias("EXCD_SK"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.GRP_SK").alias("GRP_SK"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.SUB_SK").alias("SUB_SK"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.CLS_PLN_ID").alias("CLS_PLN_ID"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.EXCD_ID").alias("EXCD_ID"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.GRP_ID").alias("GRP_ID"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.GRP_NM").alias("GRP_NM"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.USER_ID").alias("SRC_SYS_CRT_USER_ID"),
        F.col("Ref_SubEligAuditActnCd_Lkp.TRGT_CD").alias("SUB_ELIG_AUDIT_ACTN_CD"),
        F.col("Ref_SubEligAuditActnCd_Lkp.TRGT_CD_NM").alias("SUB_ELIG_AUDIT_ACTN_NM"),
        F.col("Ref_SubEligClsProdCatCd_Lkup.TRGT_CD").alias("SUB_ELIG_CLS_PROD_CD"),
        F.col("Ref_SubEligClsProdCatCd_Lkup.TRGT_CD_NM").alias("SUB_ELIG_CLS_PROD_NM"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.EFF_DT_SK").alias("SUB_ELIG_EFF_DT_SK"),
        F.col("Ref_SubEligTypCd_Lkup.TRGT_CD").alias("SUB_ELIG_TYP_CD"),
        F.col("Ref_SubEligTypCd_Lkup.TRGT_CD_NM").alias("SUB_ELIG_TYP_NM"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.VOID_IN").alias("SUB_ELIG_VOID_IN"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.SUB_ELIG_AUDIT_ACTN_CD_SK").alias("SUB_ELIG_AUDIT_ACTN_CD_SK"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.SUB_ELIG_CLS_PROD_CAT_CD_SK").alias("SUB_ELIG_CLS_PROD_CAT_CD_SK"),
        F.col("lnk_IdsEdwSubEligAuditDExtr_InAbc.SUB_ELIG_TYP_CD_SK").alias("SUB_ELIG_TYP_CD_SK")
    )
)

df_lkp_Codes = df_lkp_Codes.withColumn(
    "myRowNum",
    F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
)

df_main = (
    df_lkp_Codes
    .filter((F.col("SUB_ELIG_AUDIT_SK") != 0) & (F.col("SUB_ELIG_AUDIT_SK") != 1))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrRunCycleDate))
    .withColumn("MBR_SFX_NO", F.lit("00"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
)

df_UNK = df_lkp_Codes.filter(F.col("myRowNum") == 1).select(
    F.lit(0).alias("SUB_ELIG_AUDIT_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("SUB_ELIG_AUDIT_ROW_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("CLS_PLN_SK"),
    F.lit(0).alias("EXCD_SK"),
    F.lit(0).alias("GRP_SK"),
    F.lit(0).alias("SRC_SYS_CRT_USER_SK"),
    F.lit(0).alias("SUB_SK"),
    F.lit("UNK").alias("CLS_PLN_ID"),
    F.lit("UNK").alias("EXCD_ID"),
    F.lit("UNK").alias("GRP_ID"),
    F.lit("UNK").alias("GRP_NM"),
    F.lit(0).alias("GRP_UNIQ_KEY"),
    F.lit(None).alias("MBR_SFX_NO"),
    F.lit("1753-01-01").alias("SRC_SYS_CRT_DT_SK"),
    F.lit("UNK").alias("SRC_SYS_CRT_USER_ID"),
    F.lit("UNK").alias("SUB_ELIG_AUDIT_ACTN_CD"),
    F.lit("UNK").alias("SUB_ELIG_AUDIT_ACTN_NM"),
    F.lit("UNK").alias("SUB_ELIG_CLS_PROD_CAT_CD"),
    F.lit("UNK").alias("SUB_ELIG_CLS_PROD_CAT_NM"),
    F.lit("1753-01-01").alias("SUB_ELIG_EFF_DT_SK"),
    F.lit("UNK").alias("SUB_ELIG_TYP_CD"),
    F.lit("UNK").alias("SUB_ELIG_TYP_NM"),
    F.lit("N").alias("SUB_ELIG_VOID_IN"),
    F.lit(0).alias("SUB_UNIQ_KEY"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("SUB_ELIG_AUDIT_ACTN_CD_SK"),
    F.lit(0).alias("SUB_ELIG_CLS_PROD_CAT_CD_SK"),
    F.lit(0).alias("SUB_ELIG_TYP_CD_SK")
)

df_NA = df_lkp_Codes.filter(F.col("myRowNum") == 1).select(
    F.lit(1).alias("SUB_ELIG_AUDIT_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("SUB_ELIG_AUDIT_ROW_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("CLS_PLN_SK"),
    F.lit(1).alias("EXCD_SK"),
    F.lit(1).alias("GRP_SK"),
    F.lit(1).alias("SRC_SYS_CRT_USER_SK"),
    F.lit(1).alias("SUB_SK"),
    F.lit("NA").alias("CLS_PLN_ID"),
    F.lit("NA").alias("EXCD_ID"),
    F.lit("NA").alias("GRP_ID"),
    F.lit("NA").alias("GRP_NM"),
    F.lit(1).alias("GRP_UNIQ_KEY"),
    F.lit(None).alias("MBR_SFX_NO"),
    F.lit("1753-01-01").alias("SRC_SYS_CRT_DT_SK"),
    F.lit("NA").alias("SRC_SYS_CRT_USER_ID"),
    F.lit("NA").alias("SUB_ELIG_AUDIT_ACTN_CD"),
    F.lit("NA").alias("SUB_ELIG_AUDIT_ACTN_NM"),
    F.lit("NA").alias("SUB_ELIG_CLS_PROD_CAT_CD"),
    F.lit("NA").alias("SUB_ELIG_CLS_PROD_CAT_NM"),
    F.lit("1753-01-01").alias("SUB_ELIG_EFF_DT_SK"),
    F.lit("NA").alias("SUB_ELIG_TYP_CD"),
    F.lit("NA").alias("SUB_ELIG_TYP_NM"),
    F.lit("N").alias("SUB_ELIG_VOID_IN"),
    F.lit(1).alias("SUB_UNIQ_KEY"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("SUB_ELIG_AUDIT_ACTN_CD_SK"),
    F.lit(1).alias("SUB_ELIG_CLS_PROD_CAT_CD_SK"),
    F.lit(1).alias("SUB_ELIG_TYP_CD_SK")
)

df_fnl_dataLinks = df_NA.unionByName(df_UNK).unionByName(df_main)

df_final = df_fnl_dataLinks.select(
    F.col("SUB_ELIG_AUDIT_SK"),
    F.col("SRC_SYS_CD"),
    F.col("SUB_ELIG_AUDIT_ROW_ID"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLS_PLN_SK"),
    F.col("EXCD_SK"),
    F.col("GRP_SK"),
    F.col("SRC_SYS_CRT_USER_SK"),
    F.col("SUB_SK"),
    F.col("CLS_PLN_ID"),
    F.rpad(F.col("EXCD_ID"), 4, " ").alias("EXCD_ID"),
    F.col("GRP_ID"),
    F.col("GRP_NM"),
    F.col("GRP_UNIQ_KEY"),
    F.col("MBR_SFX_NO"),
    F.rpad(F.col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    F.rpad(F.col("SRC_SYS_CRT_USER_ID"), 10, " ").alias("SRC_SYS_CRT_USER_ID"),
    F.col("SUB_ELIG_AUDIT_ACTN_CD"),
    F.col("SUB_ELIG_AUDIT_ACTN_NM"),
    F.col("SUB_ELIG_CLS_PROD_CAT_CD"),
    F.col("SUB_ELIG_CLS_PROD_CAT_NM"),
    F.rpad(F.col("SUB_ELIG_EFF_DT_SK"), 10, " ").alias("SUB_ELIG_EFF_DT_SK"),
    F.col("SUB_ELIG_TYP_CD"),
    F.col("SUB_ELIG_TYP_NM"),
    F.rpad(F.col("SUB_ELIG_VOID_IN"), 1, " ").alias("SUB_ELIG_VOID_IN"),
    F.col("SUB_UNIQ_KEY"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SUB_ELIG_AUDIT_ACTN_CD_SK"),
    F.col("SUB_ELIG_CLS_PROD_CAT_CD_SK"),
    F.col("SUB_ELIG_TYP_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/SUB_ELIG_AUDIT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)