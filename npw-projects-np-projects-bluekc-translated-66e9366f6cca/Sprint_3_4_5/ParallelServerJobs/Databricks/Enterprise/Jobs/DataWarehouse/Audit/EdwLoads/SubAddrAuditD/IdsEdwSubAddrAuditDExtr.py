# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC DESCRIPTION:  
# MAGIC 
# MAGIC   Pulls data from IDS Facets Audit table SUB_ADDR_AUDIT into EDW table SUB_ADDR_AUDIT_D
# MAGIC       
# MAGIC INPUTS:
# MAGIC 	
# MAGIC                 SUB_ADDR_AUDIT
# MAGIC                 SUB_ADDR
# MAGIC                 GRP
# MAGIC                 APP_USER
# MAGIC   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                Parikshith Chada    11/01/2006  ---    Originally Programmed
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               07/29/2013        5114                              Create Load File for EDW Table SUB_ADDR_AUDIT_D                   EnterpriseWhseDevl     Jag Yelavarthi              2013-10-22
# MAGIC 
# MAGIC Balkarn Gill               11/20/2013        5114                              NA and UNK changes are made as per the DG standards                 EnterpriseWrhsDevl
# MAGIC 
# MAGIC Manasa Andru          2014-08-20         TFS - 8036                    Added SQL conditions in the DB2 Step to extract                               EnterpriseNewDevl      Kalyan Neelam              2014-08-26
# MAGIC                                                                                                               MBR_SFX_NO from MBR table.
# MAGIC 
# MAGIC Manasa Andru          2014-09-18         TFS - 9727                    Modified the SQL(joined the SUB_ADDR table using                         EnterpriseNewDevl      Kalyan Neelam              2014-09-23
# MAGIC                                                                                                     LEFT  OUTER JOIN) to avoid dropping the records

# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwSubAddrAuditDExtr
# MAGIC Read from source table SUB_ADDR_AUDIT .  Apply Run Cycle filters when applicable to get just the needed rows forward
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC SUB_ADDR_AUDIT_ACTN_CD_SK,
# MAGIC SUB_ADDR_CD_SK,
# MAGIC SUB_ADDR_ST_CD_SK,
# MAGIC SUB_ADDR_CTRY_CD_SK
# MAGIC Write SUB_ADDR_AUDIT_D Data into a Sequential file for Load Job IdsEdwSubAddrAuditDLoad.
# MAGIC Add Defaults and Null Handling.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
IDSRunCycle = get_widget_value('IDSRunCycle', '')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate', '')
CurrRunCycle = get_widget_value('CurrRunCycle', '')

# --------------------------------------------------------------------------------
# Read from db2_SUB_ADDR_AUDIT_in (DB2ConnectorPX, Database=IDS)
# --------------------------------------------------------------------------------
query_db2_SUB_ADDR_AUDIT_in = f"""SELECT distinct
SUB_ADDR_AUDIT.SUB_ADDR_AUDIT_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
SUB_ADDR_AUDIT.SUB_ADDR_AUDIT_ROW_ID,
SUB_ADDR_AUDIT.CRT_RUN_CYC_EXCTN_SK,
SUB_ADDR_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK,
SUB_ADDR_AUDIT.SRC_SYS_CRT_USER_SK,
SUB_ADDR_AUDIT.SUB_SK,
SUB_ADDR_AUDIT.SUB_ADDR_AUDIT_ACTN_CD_SK,
SUB_ADDR_AUDIT.SUB_ADDR_CD_SK,
SUB_ADDR_AUDIT.SRC_SYS_CRT_DT_SK,
SUB_ADDR_AUDIT.SUB_UNIQ_KEY,
SUB_ADDR_AUDIT.ADDR_LN_1,
SUB_ADDR_AUDIT.ADDR_LN_2,
SUB_ADDR_AUDIT.ADDR_LN_3,
SUB_ADDR_AUDIT.CITY_NM,
SUB_ADDR_AUDIT.SUB_ADDR_ST_CD_SK,
SUB_ADDR_AUDIT.POSTAL_CD,
SUB_ADDR_AUDIT.CNTY_NM,
SUB_ADDR_AUDIT.SUB_ADDR_CTRY_CD_SK,
COALESCE(SUB_ADDR.PHN_NO,'UNK') PHN_NO,
APP_USER.USER_ID,
GRP.GRP_SK,
GRP.GRP_ID,
GRP.GRP_UNIQ_KEY,
GRP.GRP_NM,
MSN.MBR_SFX_NO
FROM {IDSOwner}.SUB_ADDR_AUDIT SUB_ADDR_AUDIT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
  ON SUB_ADDR_AUDIT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
LEFT OUTER JOIN
(
  SELECT
    MBR1.SUB_SK,
    MBR1.MBR_SFX_NO
  FROM
    {IDSOwner}.MBR MBR1,
    {IDSOwner}.CD_MPPNG CD1
  WHERE
    MBR1.MBR_RELSHP_CD_SK = CD1.CD_MPPNG_SK
    AND CD1.TRGT_CD = 'SUB'
) MSN
  ON SUB_ADDR_AUDIT.SUB_SK = MSN.SUB_SK
LEFT OUTER JOIN {IDSOwner}.SUB_ADDR SUB_ADDR
  ON (SUB_ADDR_AUDIT.SUB_SK=SUB_ADDR.SUB_SK
      AND SUB_ADDR_AUDIT.SUB_ADDR_CD_SK=SUB_ADDR.SUB_ADDR_CD_SK),
{IDSOwner}.SUB SUB,
{IDSOwner}.MBR MBR,
{IDSOwner}.GRP GRP,
{IDSOwner}.APP_USER APP_USER
WHERE SUB_ADDR_AUDIT.SUB_SK=SUB.SUB_SK
  AND SUB.GRP_SK=GRP.GRP_SK
  AND SUB_ADDR_AUDIT.SUB_SK=MBR.SUB_SK
  AND SUB_ADDR_AUDIT.SRC_SYS_CRT_USER_SK=APP_USER.USER_SK
  AND SUB_ADDR_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}"""

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_db2_SUB_ADDR_AUDIT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_SUB_ADDR_AUDIT_in)
    .load()
)

# --------------------------------------------------------------------------------
# Read from db2_CD_MPPNG_Extr (DB2ConnectorPX, Database=IDS)
# --------------------------------------------------------------------------------
query_db2_CD_MPPNG_Extr = f"""SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_CD_MPPNG_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# Cpy_Mppng_Cd (PxCopy)
# Generate 4 copies from df_db2_CD_MPPNG_Extr
# --------------------------------------------------------------------------------
df_Ref_audit_actn_cd_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_sub_addr_cd_Lkup = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_Ctry_Cd_Lkup = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_st_cd_Lkup = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# --------------------------------------------------------------------------------
# lkp_Codes (PxLookup) - multiple left joins
# --------------------------------------------------------------------------------
df_in = df_db2_SUB_ADDR_AUDIT_in.alias("in")
df_audit = df_Ref_audit_actn_cd_Lkp.alias("audit")
df_addr = df_Ref_sub_addr_cd_Lkup.alias("addr")
df_st = df_Ref_st_cd_Lkup.alias("st")
df_ctry = df_Ref_Ctry_Cd_Lkup.alias("ctry")

df_lkp_joined = (
    df_in
    .join(df_audit, F.col("in.SUB_ADDR_AUDIT_ACTN_CD_SK") == F.col("audit.CD_MPPNG_SK"), how="left")
    .join(df_addr, F.col("in.SUB_ADDR_CD_SK") == F.col("addr.CD_MPPNG_SK"), how="left")
    .join(df_st, F.col("in.SUB_ADDR_ST_CD_SK") == F.col("st.CD_MPPNG_SK"), how="left")
    .join(df_ctry, F.col("in.SUB_ADDR_CTRY_CD_SK") == F.col("ctry.CD_MPPNG_SK"), how="left")
)

df_lkp_Codes = df_lkp_joined.select(
    F.col("in.SUB_ADDR_AUDIT_SK").alias("SUB_ADDR_AUDIT_SK"),
    F.col("in.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("in.SUB_ADDR_AUDIT_ROW_ID").alias("SUB_ADDR_AUDIT_ROW_ID"),
    F.col("in.GRP_SK").alias("GRP_SK"),
    F.col("in.SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    F.col("in.SUB_SK").alias("SUB_SK"),
    F.col("in.GRP_ID").alias("GRP_ID"),
    F.col("in.GRP_NM").alias("GRP_NM"),
    F.col("in.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.col("in.SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    F.col("in.USER_ID").alias("SRC_SYS_CRT_USER_ID"),
    F.col("audit.TRGT_CD").alias("SUB_ADDR_AUDIT_ACTN_CD"),
    F.col("audit.TRGT_CD_NM").alias("SUB_ADDR_AUDIT_ACTN_NM"),
    F.col("addr.TRGT_CD").alias("SUB_ADDR_CD"),
    F.col("addr.TRGT_CD_NM").alias("SUB_ADDR_NM"),
    F.col("in.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("in.ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("in.ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("in.CITY_NM").alias("SUB_ADDR_CITY_NM"),
    F.col("st.TRGT_CD").alias("SUB_ADDR_ST_CD"),
    F.col("st.TRGT_CD_NM").alias("SUB_ADDR_ST_NM"),
    F.col("in.POSTAL_CD").alias("POSTAL_CD"),
    F.col("in.CNTY_NM").alias("SUB_ADDR_CNTY_NM"),
    F.col("ctry.TRGT_CD").alias("SUB_ADDR_CTRY_CD"),
    F.col("ctry.TRGT_CD_NM").alias("SUB_ADDR_CTRY_NM"),
    F.col("in.PHN_NO").alias("SUB_ADDR_PHN_NO"),
    F.col("in.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("in.SUB_ADDR_AUDIT_ACTN_CD_SK").alias("SUB_ADDR_AUDIT_ACTN_CD_SK"),
    F.col("in.SUB_ADDR_CD_SK").alias("SUB_ADDR_CD_SK"),
    F.col("in.SUB_ADDR_ST_CD_SK").alias("SUB_ADDR_ST_CD_SK"),
    F.col("in.SUB_ADDR_CTRY_CD_SK").alias("SUB_ADDR_CTRY_CD_SK"),
    F.col("in.MBR_SFX_NO").alias("MBR_SFX_NO")
)

# --------------------------------------------------------------------------------
# xfm_BusinessLogic (CTransformerStage)
# Three output links: lnk_Main, UNK, NA
# --------------------------------------------------------------------------------

# 1) Prepare a global row number to handle the condition for UNK/NA:
w = Window.orderBy(F.lit(1))
df_lkp_Codes_withRn = df_lkp_Codes.withColumn("GLOBAL_RN", F.row_number().over(w))

# 2) lnk_Main => filter: SUB_ADDR_AUDIT_SK <> 0 and <> 1
df_main = df_lkp_Codes.filter(
    (F.col("SUB_ADDR_AUDIT_SK") != 0) & (F.col("SUB_ADDR_AUDIT_SK") != 1)
).select(
    F.col("SUB_ADDR_AUDIT_SK").alias("SUB_ADDR_AUDIT_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SUB_ADDR_AUDIT_ROW_ID").alias("SUB_ADDR_AUDIT_ROW_ID"),
    F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    F.col("SRC_SYS_CRT_USER_ID").alias("SRC_SYS_CRT_USER_ID"),
    F.col("SUB_ADDR_AUDIT_ACTN_CD").alias("SUB_ADDR_AUDIT_ACTN_CD"),
    F.col("SUB_ADDR_AUDIT_ACTN_NM").alias("SUB_ADDR_AUDIT_ACTN_NM"),
    F.col("SUB_ADDR_CD").alias("SUB_ADDR_CD"),
    F.col("SUB_ADDR_NM").alias("SUB_ADDR_NM"),
    F.col("ADDR_LN_1").alias("SUB_ADDR_LN_1"),
    F.col("ADDR_LN_2").alias("SUB_ADDR_LN_2"),
    F.col("ADDR_LN_3").alias("SUB_ADDR_LN_3"),
    F.col("SUB_ADDR_CITY_NM").alias("SUB_ADDR_CITY_NM"),
    F.col("SUB_ADDR_ST_CD").alias("SUB_ADDR_ST_CD"),
    F.col("SUB_ADDR_ST_NM").alias("SUB_ADDR_ST_NM"),
    # SUB_ADDR_ZIP_CD_5 => Trim(POSTAL_CD[1,5])
    F.ltrim(F.rtrim(F.col("POSTAL_CD").substr(1, 5))).alias("SUB_ADDR_ZIP_CD_5"),
    # SUB_ADDR_ZIP_CD_4 => Trim(POSTAL_CD[6,4])
    F.ltrim(F.rtrim(F.col("POSTAL_CD").substr(6, 4))).alias("SUB_ADDR_ZIP_CD_4"),
    F.col("SUB_ADDR_CNTY_NM").alias("SUB_ADDR_CNTY_NM"),
    F.col("SUB_ADDR_CTRY_CD").alias("SUB_ADDR_CTRY_CD"),
    F.col("SUB_ADDR_CTRY_NM").alias("SUB_ADDR_CTRY_NM"),
    F.col("SUB_ADDR_PHN_NO").alias("SUB_ADDR_PHN_NO"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK_tmp"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK_tmp"),
    F.col("SUB_ADDR_AUDIT_ACTN_CD_SK").alias("SUB_ADDR_AUDIT_ACTN_CD_SK"),
    F.col("SUB_ADDR_CD_SK").alias("SUB_ADDR_CD_SK"),
    F.col("SUB_ADDR_ST_CD_SK").alias("SUB_ADDR_ST_CD_SK"),
    F.col("SUB_ADDR_CTRY_CD_SK").alias("SUB_ADDR_CTRY_CD_SK"),
)

# Replace the temporary columns for cycle_exctn_sk to keep full naming distinct in the select
df_main = df_main.select(
    F.col("SUB_ADDR_AUDIT_SK"),
    F.col("SRC_SYS_CD"),
    F.col("SUB_ADDR_AUDIT_ROW_ID"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("GRP_SK"),
    F.col("SRC_SYS_CRT_USER_SK"),
    F.col("SUB_SK"),
    F.col("GRP_ID"),
    F.col("GRP_NM"),
    F.col("GRP_UNIQ_KEY"),
    F.col("MBR_SFX_NO"),
    F.col("SRC_SYS_CRT_DT_SK"),
    F.col("SRC_SYS_CRT_USER_ID"),
    F.col("SUB_ADDR_AUDIT_ACTN_CD"),
    F.col("SUB_ADDR_AUDIT_ACTN_NM"),
    F.col("SUB_ADDR_CD"),
    F.col("SUB_ADDR_NM"),
    F.col("SUB_ADDR_LN_1"),
    F.col("SUB_ADDR_LN_2"),
    F.col("SUB_ADDR_LN_3"),
    F.col("SUB_ADDR_CITY_NM"),
    F.col("SUB_ADDR_ST_CD"),
    F.col("SUB_ADDR_ST_NM"),
    F.col("SUB_ADDR_ZIP_CD_5"),
    F.col("SUB_ADDR_ZIP_CD_4"),
    F.col("SUB_ADDR_CNTY_NM"),
    F.col("SUB_ADDR_CTRY_CD"),
    F.col("SUB_ADDR_CTRY_NM"),
    F.col("SUB_ADDR_PHN_NO"),
    F.col("SUB_UNIQ_KEY"),
    F.col("CRT_RUN_CYC_EXCTN_SK_tmp").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK_tmp").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SUB_ADDR_AUDIT_ACTN_CD_SK"),
    F.col("SUB_ADDR_CD_SK"),
    F.col("SUB_ADDR_ST_CD_SK"),
    F.col("SUB_ADDR_CTRY_CD_SK")
)

# 3) UNK => condition: ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1
#    Generate a single-row DataFrame with literal values
df_UNK_intermediate = df_lkp_Codes_withRn.filter(F.col("GLOBAL_RN") == 1)
df_UNK = df_UNK_intermediate.select(
    F.lit(0).alias("SUB_ADDR_AUDIT_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("SUB_ADDR_AUDIT_ROW_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("GRP_SK"),
    F.lit(0).alias("SRC_SYS_CRT_USER_SK"),
    F.lit(0).alias("SUB_SK"),
    F.lit("UNK").alias("GRP_ID"),
    F.lit("UNK").alias("GRP_NM"),
    F.lit(0).alias("GRP_UNIQ_KEY"),
    F.lit(None).alias("MBR_SFX_NO"),
    F.lit("1753-01-01").alias("SRC_SYS_CRT_DT_SK"),
    F.lit("UNK").alias("SRC_SYS_CRT_USER_ID"),
    F.lit("UNK").alias("SUB_ADDR_AUDIT_ACTN_CD"),
    F.lit("UNK").alias("SUB_ADDR_AUDIT_ACTN_NM"),
    F.lit("UNK").alias("SUB_ADDR_CD"),
    F.lit("UNK").alias("SUB_ADDR_NM"),
    F.lit(None).alias("SUB_ADDR_LN_1"),
    F.lit(None).alias("SUB_ADDR_LN_2"),
    F.lit(None).alias("SUB_ADDR_LN_3"),
    F.lit("UNK").alias("SUB_ADDR_CITY_NM"),
    F.lit("UNK").alias("SUB_ADDR_ST_CD"),
    F.lit("UNK").alias("SUB_ADDR_ST_NM"),
    F.lit("").alias("SUB_ADDR_ZIP_CD_5"),
    F.lit("").alias("SUB_ADDR_ZIP_CD_4"),
    F.lit("UNK").alias("SUB_ADDR_CNTY_NM"),
    F.lit("UNK").alias("SUB_ADDR_CTRY_CD"),
    F.lit("UNK").alias("SUB_ADDR_CTRY_NM"),
    F.lit(None).alias("SUB_ADDR_PHN_NO"),
    F.lit("UNK").alias("SUB_UNIQ_KEY"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("SUB_ADDR_AUDIT_ACTN_CD_SK"),
    F.lit(0).alias("SUB_ADDR_CD_SK"),
    F.lit(0).alias("SUB_ADDR_ST_CD_SK"),
    F.lit(0).alias("SUB_ADDR_CTRY_CD_SK")
)

# 4) NA => condition: ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1
#    Also a single-row DataFrame with different literal values
df_NA_intermediate = df_lkp_Codes_withRn.filter(F.col("GLOBAL_RN") == 1)
df_NA = df_NA_intermediate.select(
    F.lit(1).alias("SUB_ADDR_AUDIT_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("SUB_ADDR_AUDIT_ROW_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("GRP_SK"),
    F.lit(1).alias("SRC_SYS_CRT_USER_SK"),
    F.lit(1).alias("SUB_SK"),
    F.lit("NA").alias("GRP_ID"),
    F.lit("NA").alias("GRP_NM"),
    F.lit(1).alias("GRP_UNIQ_KEY"),
    F.lit(None).alias("MBR_SFX_NO"),
    F.lit("1753-01-01").alias("SRC_SYS_CRT_DT_SK"),
    F.lit("NA").alias("SRC_SYS_CRT_USER_ID"),
    F.lit("NA").alias("SUB_ADDR_AUDIT_ACTN_CD"),
    F.lit("NA").alias("SUB_ADDR_AUDIT_ACTN_NM"),
    F.lit("NA").alias("SUB_ADDR_CD"),
    F.lit("NA").alias("SUB_ADDR_NM"),
    F.lit(None).alias("SUB_ADDR_LN_1"),
    F.lit(None).alias("SUB_ADDR_LN_2"),
    F.lit(None).alias("SUB_ADDR_LN_3"),
    F.lit("NA").alias("SUB_ADDR_CITY_NM"),
    F.lit("NA").alias("SUB_ADDR_ST_CD"),
    F.lit("NA").alias("SUB_ADDR_ST_NM"),
    F.lit("").alias("SUB_ADDR_ZIP_CD_5"),
    F.lit("").alias("SUB_ADDR_ZIP_CD_4"),
    F.lit("NA").alias("SUB_ADDR_CNTY_NM"),
    F.lit("NA").alias("SUB_ADDR_CTRY_CD"),
    F.lit("NA").alias("SUB_ADDR_CTRY_NM"),
    F.lit(None).alias("SUB_ADDR_PHN_NO"),
    F.lit(1).alias("SUB_UNIQ_KEY"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("SUB_ADDR_AUDIT_ACTN_CD_SK"),
    F.lit(1).alias("SUB_ADDR_CD_SK"),
    F.lit(1).alias("SUB_ADDR_ST_CD_SK"),
    F.lit(1).alias("SUB_ADDR_CTRY_CD_SK")
)

# --------------------------------------------------------------------------------
# fnl_dataLinks (PxFunnel) - union of NA, UNK, and lnk_Main
# --------------------------------------------------------------------------------
df_fnl_dataLinks = df_NA.unionByName(df_UNK).unionByName(df_main)

# --------------------------------------------------------------------------------
# seq_SUB_ADDR_AUDIT_D_Load (PxSequentialFile)
# Write to a .dat file with specified properties
# Column re-order with rpad for char columns in final output
# --------------------------------------------------------------------------------
df_final = df_fnl_dataLinks.select(
    F.col("SUB_ADDR_AUDIT_SK"),
    F.col("SRC_SYS_CD"),
    F.col("SUB_ADDR_AUDIT_ROW_ID"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("GRP_SK"),
    F.col("SRC_SYS_CRT_USER_SK"),
    F.col("SUB_SK"),
    F.col("GRP_ID"),
    F.col("GRP_NM"),
    F.col("GRP_UNIQ_KEY"),
    F.rpad(F.col("MBR_SFX_NO"), 2, " ").alias("MBR_SFX_NO"),
    F.rpad(F.col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    F.col("SRC_SYS_CRT_USER_ID"),
    F.col("SUB_ADDR_AUDIT_ACTN_CD"),
    F.col("SUB_ADDR_AUDIT_ACTN_NM"),
    F.col("SUB_ADDR_CD"),
    F.col("SUB_ADDR_NM"),
    F.col("SUB_ADDR_LN_1"),
    F.col("SUB_ADDR_LN_2"),
    F.col("SUB_ADDR_LN_3"),
    F.col("SUB_ADDR_CITY_NM"),
    F.col("SUB_ADDR_ST_CD"),
    F.col("SUB_ADDR_ST_NM"),
    F.rpad(F.col("SUB_ADDR_ZIP_CD_5"), 5, " ").alias("SUB_ADDR_ZIP_CD_5"),
    F.rpad(F.col("SUB_ADDR_ZIP_CD_4"), 4, " ").alias("SUB_ADDR_ZIP_CD_4"),
    F.col("SUB_ADDR_CNTY_NM"),
    F.col("SUB_ADDR_CTRY_CD"),
    F.col("SUB_ADDR_CTRY_NM"),
    F.col("SUB_ADDR_PHN_NO"),
    F.col("SUB_UNIQ_KEY"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SUB_ADDR_AUDIT_ACTN_CD_SK"),
    F.col("SUB_ADDR_CD_SK"),
    F.col("SUB_ADDR_ST_CD_SK"),
    F.col("SUB_ADDR_CTRY_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/SUB_ADDR_AUDIT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)