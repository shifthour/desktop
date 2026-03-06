# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC DESCRIPTION:  
# MAGIC 
# MAGIC   Pulls data from IDS Facets Audit table MBR_PCP_AUDIT into EDW table MBR_PCP_AUDIT_D
# MAGIC       
# MAGIC INPUTS:
# MAGIC 	
# MAGIC                 MBR_PCP_AUDIT
# MAGIC                 GRP
# MAGIC                 MBR
# MAGIC                 PROV
# MAGIC                 APP_USER
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                Parikshith Chada    10/31/2006  ---    Originally Programmed
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               07/11/2013        5114                              Create Load File for EDW Table MBR_PCP_AUDIT_D                      EnterpriseWhseDevl    Jag Yelavarthi               2013-10-22
# MAGIC 
# MAGIC Balkarn Gill               11/20/2013        5114                             NA and UNK changes are made as per the DG standards                   EnterpriseWrhsDevl

# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwMbrPcpAuditDExtr
# MAGIC Read from source table MBR_PCP_AUDIT .  Apply Run Cycle filters when applicable to get just the needed rows forward
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC MBR_PCP_AUDIT_ACTN_CD_SK
# MAGIC Write MBR_AUDIT_D Data into a Sequential file for Load Job IdsEdwMbrPcpAuditDLoad.
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

df_db2_MBR_PCP_AUDIT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT
  MBR_PCP_AUDIT.MBR_PCP_AUDIT_SK,
  COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
  MBR_PCP_AUDIT.MBR_PCP_AUDIT_ROW_ID,
  MBR_PCP_AUDIT.CRT_RUN_CYC_EXCTN_SK,
  MBR_PCP_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK,
  MBR_PCP_AUDIT.MBR_SK,
  PROV.PROV_SK,
  MBR_PCP_AUDIT.SRC_SYS_CRT_USER_SK,
  MBR_PCP_AUDIT.MBR_PCP_AUDIT_ACTN_CD_SK,
  MBR_PCP_AUDIT.SRC_SYS_CRT_DT_SK,
  MBR_PCP_AUDIT.MBR_UNIQ_KEY,
  GRP.GRP_SK,
  GRP.GRP_ID,
  GRP.GRP_UNIQ_KEY,
  GRP.GRP_NM,
  MBR.MBR_SFX_NO,
  PROV.PROV_ID,
  APP_USER.USER_ID,
  SUB.SUB_SK
FROM {IDSOwner}.MBR_PCP_AUDIT MBR_PCP_AUDIT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
  ON MBR_PCP_AUDIT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK,
  {IDSOwner}.MBR MBR,
  {IDSOwner}.SUB SUB,
  {IDSOwner}.PROV PROV,
  {IDSOwner}.GRP GRP,
  {IDSOwner}.APP_USER APP_USER
WHERE
  MBR_PCP_AUDIT.MBR_SK = MBR.MBR_SK
  AND MBR.SUB_SK = SUB.SUB_SK
  AND SUB.GRP_SK = GRP.GRP_SK
  AND MBR_PCP_AUDIT.PROV_SK = PROV.PROV_SK
  AND MBR_PCP_AUDIT.SRC_SYS_CRT_USER_SK = APP_USER.USER_SK
  AND MBR_PCP_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
"""
    )
    .load()
)

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT CD_MPPNG_SK, COALESCE(TRGT_CD,'UNK') TRGT_CD, COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM FROM {IDSOwner}.CD_MPPNG"
    )
    .load()
)

df_lkp_Codes = (
    df_db2_MBR_PCP_AUDIT_in.alias("lnk_IdsEdwMbrPcpAuditDExtr_InAbc")
    .join(
        df_db2_CD_MPPNG_Extr.alias("Ref_pcp_audit_Lkp"),
        F.col("lnk_IdsEdwMbrPcpAuditDExtr_InAbc.MBR_PCP_AUDIT_ACTN_CD_SK")
        == F.col("Ref_pcp_audit_Lkp.CD_MPPNG_SK"),
        "left",
    )
    .select(
        F.col("lnk_IdsEdwMbrPcpAuditDExtr_InAbc.MBR_PCP_AUDIT_SK").alias("MBR_PCP_AUDIT_SK"),
        F.col("lnk_IdsEdwMbrPcpAuditDExtr_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwMbrPcpAuditDExtr_InAbc.MBR_PCP_AUDIT_ROW_ID").alias("MBR_PCP_AUDIT_ROW_ID"),
        F.col("lnk_IdsEdwMbrPcpAuditDExtr_InAbc.GRP_SK").alias("GRP_SK"),
        F.col("lnk_IdsEdwMbrPcpAuditDExtr_InAbc.MBR_SK").alias("MBR_SK"),
        F.col("lnk_IdsEdwMbrPcpAuditDExtr_InAbc.PROV_SK").alias("PROV_SK"),
        F.col("lnk_IdsEdwMbrPcpAuditDExtr_InAbc.SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
        F.col("lnk_IdsEdwMbrPcpAuditDExtr_InAbc.SUB_SK").alias("SUB_SK"),
        F.col("lnk_IdsEdwMbrPcpAuditDExtr_InAbc.GRP_ID").alias("GRP_ID"),
        F.col("lnk_IdsEdwMbrPcpAuditDExtr_InAbc.GRP_NM").alias("GRP_NM"),
        F.col("lnk_IdsEdwMbrPcpAuditDExtr_InAbc.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
        F.col("Ref_pcp_audit_Lkp.TRGT_CD").alias("MBR_PCP_AUDIT_ACTN_CD"),
        F.col("Ref_pcp_audit_Lkp.TRGT_CD_NM").alias("MBR_PCP_AUDIT_ACTN_NM"),
        F.col("lnk_IdsEdwMbrPcpAuditDExtr_InAbc.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("lnk_IdsEdwMbrPcpAuditDExtr_InAbc.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("lnk_IdsEdwMbrPcpAuditDExtr_InAbc.PROV_ID").alias("PROV_ID"),
        F.col("lnk_IdsEdwMbrPcpAuditDExtr_InAbc.SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
        F.col("lnk_IdsEdwMbrPcpAuditDExtr_InAbc.USER_ID").alias("SRC_SYS_CRT_USER_ID"),
        F.col("lnk_IdsEdwMbrPcpAuditDExtr_InAbc.MBR_PCP_AUDIT_ACTN_CD_SK").alias("MBR_PCP_AUDIT_ACTN_CD_SK"),
    )
)

df_xfm_temp = df_lkp_Codes.withColumn(
    "rownum", F.row_number().over(Window.orderBy(F.lit(1)))
)

df_main = (
    df_lkp_Codes.filter(
        (F.col("MBR_PCP_AUDIT_SK") != 0) & (F.col("MBR_PCP_AUDIT_SK") != 1)
    )
    .select(
        F.col("MBR_PCP_AUDIT_SK").alias("MBR_PCP_AUDIT_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("MBR_PCP_AUDIT_ROW_ID").alias("MBR_PCP_AUDIT_ROW_ID"),
        F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("GRP_SK").alias("GRP_SK"),
        F.col("MBR_SK").alias("MBR_SK"),
        F.col("PROV_SK").alias("PROV_SK"),
        F.col("SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
        F.col("SUB_SK").alias("SUB_SK"),
        F.col("GRP_ID").alias("GRP_ID"),
        F.col("GRP_NM").alias("GRP_NM"),
        F.col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
        F.col("MBR_PCP_AUDIT_ACTN_CD").alias("MBR_PCP_AUDIT_ACTN_CD"),
        F.col("MBR_PCP_AUDIT_ACTN_NM").alias("MBR_PCP_AUDIT_ACTN_NM"),
        F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("PROV_ID").alias("PROV_ID"),
        F.col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
        F.col("SRC_SYS_CRT_USER_ID").alias("SRC_SYS_CRT_USER_ID"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("MBR_PCP_AUDIT_ACTN_CD_SK").alias("MBR_PCP_AUDIT_ACTN_CD_SK"),
    )
)

df_na = (
    df_xfm_temp.filter(F.col("rownum") == 1)
    .select(
        F.lit(1).alias("MBR_PCP_AUDIT_SK"),
        F.lit("NA").alias("SRC_SYS_CD"),
        F.lit("NA").alias("MBR_PCP_AUDIT_ROW_ID"),
        F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(1).alias("GRP_SK"),
        F.lit(1).alias("MBR_SK"),
        F.lit(1).alias("PROV_SK"),
        F.lit(1).alias("SRC_SYS_CRT_USER_SK"),
        F.lit(1).alias("SUB_SK"),
        F.lit("NA").alias("GRP_ID"),
        F.lit("NA").alias("GRP_NM"),
        F.lit(1).alias("GRP_UNIQ_KEY"),
        F.lit("NA").alias("MBR_PCP_AUDIT_ACTN_CD"),
        F.lit("NA").alias("MBR_PCP_AUDIT_ACTN_NM"),
        F.lit("").alias("MBR_SFX_NO"),
        F.lit(1).alias("MBR_UNIQ_KEY"),
        F.lit("NA").alias("PROV_ID"),
        F.lit("1753-01-01").alias("SRC_SYS_CRT_DT_SK"),
        F.lit("NA").alias("SRC_SYS_CRT_USER_ID"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("MBR_PCP_AUDIT_ACTN_CD_SK"),
    )
)

df_unk = (
    df_xfm_temp.filter(F.col("rownum") == 1)
    .select(
        F.lit(0).alias("MBR_PCP_AUDIT_SK"),
        F.lit("UNK").alias("SRC_SYS_CD"),
        F.lit("UNK").alias("MBR_PCP_AUDIT_ROW_ID"),
        F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(0).alias("GRP_SK"),
        F.lit(0).alias("MBR_SK"),
        F.lit(0).alias("PROV_SK"),
        F.lit(0).alias("SRC_SYS_CRT_USER_SK"),
        F.lit(0).alias("SUB_SK"),
        F.lit("UNK").alias("GRP_ID"),
        F.lit("UNK").alias("GRP_NM"),
        F.lit(0).alias("GRP_UNIQ_KEY"),
        F.lit("UNK").alias("MBR_PCP_AUDIT_ACTN_CD"),
        F.lit("UNK").alias("MBR_PCP_AUDIT_ACTN_NM"),
        F.lit("").alias("MBR_SFX_NO"),
        F.lit(0).alias("MBR_UNIQ_KEY"),
        F.lit("UNK").alias("PROV_ID"),
        F.lit("1753-01-01").alias("SRC_SYS_CRT_DT_SK"),
        F.lit("UNK").alias("SRC_SYS_CRT_USER_ID"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("MBR_PCP_AUDIT_ACTN_CD_SK"),
    )
)

df_fnl_dataLinks = df_na.unionByName(df_unk).unionByName(df_main)

df_fnl_dataLinks_final = df_fnl_dataLinks.select(
    F.col("MBR_PCP_AUDIT_SK"),
    F.col("SRC_SYS_CD"),
    F.col("MBR_PCP_AUDIT_ROW_ID"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("GRP_SK"),
    F.col("MBR_SK"),
    F.col("PROV_SK"),
    F.col("SRC_SYS_CRT_USER_SK"),
    F.col("SUB_SK"),
    F.col("GRP_ID"),
    F.col("GRP_NM"),
    F.col("GRP_UNIQ_KEY"),
    F.col("MBR_PCP_AUDIT_ACTN_CD"),
    F.col("MBR_PCP_AUDIT_ACTN_NM"),
    F.rpad(F.col("MBR_SFX_NO"), 2, " ").alias("MBR_SFX_NO"),
    F.col("MBR_UNIQ_KEY"),
    F.col("PROV_ID"),
    F.rpad(F.col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    F.rpad(F.col("SRC_SYS_CRT_USER_ID"), 10, " ").alias("SRC_SYS_CRT_USER_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_PCP_AUDIT_ACTN_CD_SK"),
)

write_files(
    df_fnl_dataLinks_final,
    f"{adls_path}/load/MBR_PCP_AUDIT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)