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
# MAGIC MODIFICATIONS:
# MAGIC              Naren Garapaty   10/30/2006  ---    Originally Programmed
# MAGIC              Parikshith Chada  11/13/2006  -----  Modified, added two new fields to the job 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               07/11/2013        5114                              Create Load File for EDW Table MBR_AUDIT_D                             EnterpriseWhseDevl      Jag Yelavarthi              2013-10-22
# MAGIC 
# MAGIC Balkarn Gill               11/20/2013        5114                             NA and UNK changes are made as per the DG standards                EnterpriseWrhsDevl

# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwMbrAuditDExtr
# MAGIC Read from source table MBR_AUDIT .  Apply Run Cycle filters when applicable to get just the needed rows forward
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC MBR_AUDIT_ACTN_CD_SK,
# MAGIC MBR_GNDR_CD_SK
# MAGIC Write MBR_AUDIT_D Data into a Sequential file for Load Job IdsEdwMbrAuditDLoad.
# MAGIC Add Defaults and Null Handling.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_MBR_AUDIT_in = f"""
SELECT 
MBR_AUDIT.MBR_AUDIT_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
MBR_AUDIT.MBR_AUDIT_ROW_ID,
MBR_AUDIT.CRT_RUN_CYC_EXCTN_SK,
MBR_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK,
MBR_AUDIT.MBR_SK,
MBR_AUDIT.SRC_SYS_CRT_USER_SK,
MBR_AUDIT.MBR_AUDIT_ACTN_CD_SK,
MBR_AUDIT.MBR_GNDR_CD_SK,
MBR_AUDIT.SCRD_IN,
MBR_AUDIT.BRTH_DT_SK,
MBR_AUDIT.PREX_COND_EFF_DT_SK,
MBR_AUDIT.SRC_SYS_CRT_DT_SK,
MBR_AUDIT.MBR_UNIQ_KEY,
MBR_AUDIT.FIRST_NM,
MBR_AUDIT.MIDINIT,
MBR_AUDIT.LAST_NM,
MBR_AUDIT.SSN,
GRP.GRP_SK,
GRP.GRP_ID,
GRP.GRP_UNIQ_KEY,
GRP.GRP_NM,
APP_USER.USER_ID,
MBR.MBR_SFX_NO,
SUB.SUB_SK
FROM {IDSOwner}.MBR_AUDIT MBR_AUDIT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON MBR_AUDIT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK,
{IDSOwner}.MBR MBR,
{IDSOwner}.SUB SUB,
{IDSOwner}.GRP GRP,
{IDSOwner}.APP_USER APP_USER
WHERE MBR_AUDIT.MBR_SK = MBR.MBR_SK
  AND MBR.SUB_SK = SUB.SUB_SK
  AND SUB.GRP_SK = GRP.GRP_SK
  AND MBR_AUDIT.SRC_SYS_CRT_USER_SK = APP_USER.USER_SK
  AND MBR_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
"""
df_db2_MBR_AUDIT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_MBR_AUDIT_in)
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

# --------------------------------------------------------------------------------
# Cpy_Mppng_Cd (PxCopy) creates two output links with identical columns
df_Cpy_Mppng_Cd_Ref_MbrGndrCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Cpy_Mppng_Cd_Ref_MbrAuditActnCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# --------------------------------------------------------------------------------
# lkp_Codes (PxLookup) has a primary link and two left joins
df_lkp_joined = (
    df_db2_MBR_AUDIT_in.alias("lnk_IdsEdwMbrAuditDExtr_InAbc")
    .join(
        df_Cpy_Mppng_Cd_Ref_MbrGndrCd_Lkp.alias("Ref_MbrGndrCd_Lkp"),
        F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.MBR_GNDR_CD_SK") == F.col("Ref_MbrGndrCd_Lkp.CD_MPPNG_SK"),
        how="left"
    )
    .join(
        df_Cpy_Mppng_Cd_Ref_MbrAuditActnCd_Lkp.alias("Ref_MbrAuditActnCd_Lkp"),
        F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.MBR_AUDIT_ACTN_CD_SK") == F.col("Ref_MbrAuditActnCd_Lkp.CD_MPPNG_SK"),
        how="left"
    )
)

df_lkp_Codes_out = df_lkp_joined.select(
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.MBR_AUDIT_SK").alias("MBR_AUDIT_SK"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.MBR_AUDIT_ROW_ID").alias("MBR_AUDIT_ROW_ID"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.GRP_SK").alias("GRP_SK"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.MBR_SK").alias("MBR_SK"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.SUB_SK").alias("SUB_SK"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.GRP_ID").alias("GRP_ID"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.GRP_NM").alias("GRP_NM"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.MIDINIT").alias("MBR_MIDINIT"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.LAST_NM").alias("MBR_LAST_NM"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    F.col("Ref_MbrGndrCd_Lkp.TRGT_CD").alias("MBR_GNDR_CD"),
    F.col("Ref_MbrGndrCd_Lkp.TRGT_CD_NM").alias("MBR_GNDR_NM"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.PREX_COND_EFF_DT_SK").alias("MBR_PREX_COND_EFF_DT_SK"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.SCRD_IN").alias("MBR_SCRD_IN"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.SSN").alias("MBR_SSN"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Ref_MbrAuditActnCd_Lkp.TRGT_CD").alias("MBR_AUDIT_CD"),
    F.col("Ref_MbrAuditActnCd_Lkp.TRGT_CD_NM").alias("MBR_AUDIT_NM"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.USER_ID").alias("SRC_SYS_CRT_USER_ID"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.MBR_AUDIT_ACTN_CD_SK").alias("MBR_AUDIT_ACTN_CD_SK"),
    F.col("lnk_IdsEdwMbrAuditDExtr_InAbc.MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK")
)

# --------------------------------------------------------------------------------
# xfm_BusinessLogic (CTransformerStage) produces three output links

# 1) lnk_Main
df_xfm_BusinessLogic_lnk_Main_base = df_lkp_Codes_out.filter(
    (F.col("MBR_AUDIT_SK") != 0) & (F.col("MBR_AUDIT_SK") != 1)
)
df_xfm_BusinessLogic_lnk_Main = (
    df_xfm_BusinessLogic_lnk_Main_base
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrRunCycleDate))
    .withColumn(
        "MBR_MIDINIT",
        F.when(
            trim(
                F.when(F.col("MBR_MIDINIT").isNotNull(), F.col("MBR_MIDINIT")).otherwise(F.lit(""))
            ) == F.lit(","),
            F.lit(None)
        ).otherwise(F.col("MBR_MIDINIT"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
)

# 2) NA (constraint = row 1 only), columns set to WhereExpression
windowSpecNA = Window.orderBy(F.lit(1))
df_with_rn_NA = df_lkp_Codes_out.withColumn("temp_rn", F.row_number().over(windowSpecNA))
df_xfm_BusinessLogic_NA_base = df_with_rn_NA.filter(F.col("temp_rn") == 1)
df_xfm_BusinessLogic_NA = (
    df_xfm_BusinessLogic_NA_base
    .withColumn("MBR_AUDIT_SK", F.lit(1))
    .withColumn("SRC_SYS_CD", F.lit("NA"))
    .withColumn("MBR_AUDIT_ROW_ID", F.lit("NA"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
    .withColumn("GRP_SK", F.lit(1))
    .withColumn("MBR_SK", F.lit(1))
    .withColumn("SRC_SYS_CRT_USER_SK", F.lit(1))
    .withColumn("SUB_SK", F.lit(1))
    .withColumn("GRP_ID", F.lit("NA"))
    .withColumn("GRP_NM", F.lit("NA"))
    .withColumn("GRP_UNIQ_KEY", F.lit(1))
    .withColumn("MBR_FIRST_NM", F.lit("NA"))
    .withColumn("MBR_MIDINIT", F.lit(None))
    .withColumn("MBR_LAST_NM", F.lit("NA"))
    .withColumn("MBR_BRTH_DT_SK", F.lit("1753-01-01"))
    .withColumn("MBR_GNDR_CD", F.lit("NA"))
    .withColumn("MBR_GNDR_NM", F.lit("NA"))
    .withColumn("MBR_PREX_COND_EFF_DT_SK", F.lit("1753-01-01"))
    .withColumn("MBR_SCRD_IN", F.lit("N"))
    .withColumn("MBR_SSN", F.lit(""))
    .withColumn("MBR_SFX_NO", F.lit(None))
    .withColumn("MBR_UNIQ_KEY", F.lit(1))
    .withColumn("MBR_AUDIT_CD", F.lit("NA"))
    .withColumn("MBR_AUDIT_NM", F.lit("NA"))
    .withColumn("SRC_SYS_CRT_DT_SK", F.lit("1753-01-01"))
    .withColumn("SRC_SYS_CRT_USER_ID", F.lit("NA"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("MBR_AUDIT_ACTN_CD_SK", F.lit(1))
    .withColumn("MBR_GNDR_CD_SK", F.lit(1))
)

# 3) UNK (constraint = row 1 only), columns set to WhereExpression
windowSpecUNK = Window.orderBy(F.lit(1))
df_with_rn_UNK = df_lkp_Codes_out.withColumn("temp_rn", F.row_number().over(windowSpecUNK))
df_xfm_BusinessLogic_UNK_base = df_with_rn_UNK.filter(F.col("temp_rn") == 1)
df_xfm_BusinessLogic_UNK = (
    df_xfm_BusinessLogic_UNK_base
    .withColumn("MBR_AUDIT_SK", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("UNK"))
    .withColumn("MBR_AUDIT_ROW_ID", F.lit("UNK"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
    .withColumn("GRP_SK", F.lit(0))
    .withColumn("MBR_SK", F.lit(0))
    .withColumn("SRC_SYS_CRT_USER_SK", F.lit(0))
    .withColumn("SUB_SK", F.lit(0))
    .withColumn("GRP_ID", F.lit("UNK"))
    .withColumn("GRP_NM", F.lit("UNK"))
    .withColumn("GRP_UNIQ_KEY", F.lit(0))
    .withColumn("MBR_FIRST_NM", F.lit("UNK"))
    .withColumn("MBR_MIDINIT", F.lit(None))
    .withColumn("MBR_LAST_NM", F.lit("UNK"))
    .withColumn("MBR_BRTH_DT_SK", F.lit("1753-01-01"))
    .withColumn("MBR_GNDR_CD", F.lit("UNK"))
    .withColumn("MBR_GNDR_NM", F.lit("UNK"))
    .withColumn("MBR_PREX_COND_EFF_DT_SK", F.lit("1753-01-01"))
    .withColumn("MBR_SCRD_IN", F.lit("N"))
    .withColumn("MBR_SSN", F.lit(""))
    .withColumn("MBR_SFX_NO", F.lit(None))
    .withColumn("MBR_UNIQ_KEY", F.lit(0))
    .withColumn("MBR_AUDIT_CD", F.lit("UNK"))
    .withColumn("MBR_AUDIT_NM", F.lit("UNK"))
    .withColumn("SRC_SYS_CRT_DT_SK", F.lit("1753-01-01"))
    .withColumn("SRC_SYS_CRT_USER_ID", F.lit("UNK"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("MBR_AUDIT_ACTN_CD_SK", F.lit(0))
    .withColumn("MBR_GNDR_CD_SK", F.lit(0))
)

# --------------------------------------------------------------------------------
# fnl_dataLinks (PxFunnel) => union of NA, lnk_Main, UNK in that order
df_fnl_dataLinks = df_xfm_BusinessLogic_NA.select(df_lkp_Codes_out.columns).unionByName(
    df_xfm_BusinessLogic_lnk_Main.select(df_lkp_Codes_out.columns), allowMissingColumns=True
).unionByName(
    df_xfm_BusinessLogic_UNK.select(df_lkp_Codes_out.columns), allowMissingColumns=True
)

# --------------------------------------------------------------------------------
# The funnel output columns (same order). We apply any char-length padding
# Checking final schema from funnel stage:
final_columns = [
    ("MBR_AUDIT_SK", None),
    ("SRC_SYS_CD", None),
    ("MBR_AUDIT_ROW_ID", None),
    ("CRT_RUN_CYC_EXCTN_DT_SK", 10),  # char(10)
    ("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10),  # char(10)
    ("GRP_SK", None),
    ("MBR_SK", None),
    ("SRC_SYS_CRT_USER_SK", None),
    ("SUB_SK", None),
    ("GRP_ID", None),
    ("GRP_NM", None),
    ("GRP_UNIQ_KEY", None),
    ("MBR_FIRST_NM", None),
    ("MBR_MIDINIT", 1),  # char(1)
    ("MBR_LAST_NM", None),
    ("MBR_BRTH_DT_SK", 10),  # char(10)
    ("MBR_GNDR_CD", None),
    ("MBR_GNDR_NM", None),
    ("MBR_PREX_COND_EFF_DT_SK", 10),  # char(10)
    ("MBR_SCRD_IN", 1),  # char(1)
    ("MBR_SSN", None),
    ("MBR_SFX_NO", 2),   # originally char(2)
    ("MBR_UNIQ_KEY", None),
    ("MBR_AUDIT_ACTN_CD", None),
    ("MBR_AUDIT_ACTN_NM", None),
    ("SRC_SYS_CRT_DT_SK", 10),  # char(10)
    ("SRC_SYS_CRT_USER_ID", None),
    ("CRT_RUN_CYC_EXCTN_SK", None),
    ("LAST_UPDT_RUN_CYC_EXCTN_SK", None),
    ("MBR_AUDIT_ACTN_CD_SK", None),
    ("MBR_GNDR_CD_SK", None)
]

df_fnl_padded = df_fnl_dataLinks
for col_name, length_val in final_columns:
    if length_val is not None:
        df_fnl_padded = df_fnl_padded.withColumn(
            col_name,
            F.when(
                F.col(col_name).isNotNull(),
                F.rpad(F.col(col_name).cast(StringType()), length_val, " ")
            ).otherwise(F.col(col_name))
        )

df_final = df_fnl_padded.select([F.col(c[0]) for c in final_columns])

# --------------------------------------------------------------------------------
# seq_MBR_AUDIT_D_Load (PxSequentialFile) => write to MBR_AUDIT_D.dat under "load"
output_file_path = f"{adls_path}/load/MBR_AUDIT_D.dat"
write_files(
    df_final,
    output_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)