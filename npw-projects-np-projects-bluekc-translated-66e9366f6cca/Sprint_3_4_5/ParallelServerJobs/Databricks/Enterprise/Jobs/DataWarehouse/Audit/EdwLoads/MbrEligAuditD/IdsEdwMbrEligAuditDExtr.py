# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC DESCRIPTION:  
# MAGIC 
# MAGIC   Pulls data from IDS Facets Audit table MBR_ELIG_AUDIT into EDW table MBR_ELIG_AUDIT_D
# MAGIC       
# MAGIC INPUTS:
# MAGIC 	
# MAGIC                 MBR_ELIG_AUDIT
# MAGIC                 MBR
# MAGIC                 SUB
# MAGIC                 GRP
# MAGIC                 EXCD
# MAGIC                 SUB_ELIG
# MAGIC                 APP_USER
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC              Naren Garapaty   31/10/2006  ---    Originally Programmed
# MAGIC 
# MAGIC Developer                Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed      
# MAGIC ------------------              --------------------	------------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------   
# MAGIC Kalyan Neelam         2011-11-14               TTR-456                Updated Extract SQL, retreiving CLS_PLN_SK from                    EnterpriseNewDevl           SAndrew                2011-11-17
# MAGIC                                                                                                 MBR_ELIG_AUDIT table, also removed join to the SUB_ELIG table
# MAGIC                                                                                                 
# MAGIC 
# MAGIC Balkarn Gill               07/26/2013        5114                            Create Load File for EDW Table MBR_ELIG_AUDIT_D             EnterpriseWhseDevl        Jag Yelavarthi         2013-10-22
# MAGIC 
# MAGIC Balkarn Gill               11/20/2013        5114                             NA and UNK changes are made as per the DG standards                EnterpriseWrhsDevl

# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwMbrEligAuditDExtr
# MAGIC Read from source table MBR_ELIG_AUDIT .  Apply Run Cycle filters when applicable to get just the needed rows forward
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC MBR_ELIG_AUDIT_ACTN_CD_SK,
# MAGIC MBR_ELIG_CLS_PROD_CAT_CD_SK,
# MAGIC MBR_ELIG_TYP_CD_SK,
# MAGIC Write MBR_ELIG_AUDIT_D Data into a Sequential file for Load Job IdsEdwMbrEligAuditDLoad.
# MAGIC Add Defaults and Null Handling.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.window import Window
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve job parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

# --------------------------------------------------------------------------------
# db2_MBR_ELIG_AUDIT_in (DB2ConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_MBR_ELIG_AUDIT_in = f"""
SELECT distinct
    MBR_ELIG_AUDIT.MBR_ELIG_AUDIT_SK,
    COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
    MBR_ELIG_AUDIT.MBR_ELIG_AUDIT_ROW_ID,
    MBR_ELIG_AUDIT.CRT_RUN_CYC_EXCTN_SK,
    MBR_ELIG_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK,
    MBR_ELIG_AUDIT.EXCD_SK,
    MBR_ELIG_AUDIT.MBR_SK,
    MBR_ELIG_AUDIT.SRC_SYS_CRT_USER_SK,
    MBR_ELIG_AUDIT.MBR_ELIG_AUDIT_ACTN_CD_SK,
    MBR_ELIG_AUDIT.MBR_ELIG_CLS_PROD_CAT_CD_SK,
    MBR_ELIG_AUDIT.MBR_ELIG_TYP_CD_SK,
    MBR_ELIG_AUDIT.VOID_IN,
    MBR_ELIG_AUDIT.EFF_DT_SK,
    MBR_ELIG_AUDIT.SRC_SYS_CRT_DT_SK,
    MBR_ELIG_AUDIT.MBR_UNIQ_KEY,
    MBR_ELIG_AUDIT.CLS_PLN_SK,
    APP_USER.USER_ID,
    CLS_PLN.CLS_PLN_ID,
    EXCD.EXCD_ID,
    MBR.MBR_SFX_NO,
    GRP.GRP_SK,
    GRP.GRP_ID,
    GRP.GRP_UNIQ_KEY,
    GRP.GRP_NM,
    MBR.SUB_SK
FROM {IDSOwner}.MBR_ELIG_AUDIT MBR_ELIG_AUDIT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
       ON MBR_ELIG_AUDIT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.MBR MBR,
     {IDSOwner}.GRP GRP,
     {IDSOwner}.CLS_PLN CLS_PLN,
     {IDSOwner}.EXCD EXCD,
     {IDSOwner}.APP_USER APP_USER
WHERE MBR_ELIG_AUDIT.MBR_SK=MBR.MBR_SK
  AND MBR.SUB_SK=SUB.SUB_SK
  AND MBR_ELIG_AUDIT.CLS_PLN_SK=CLS_PLN.CLS_PLN_SK
  AND MBR_ELIG_AUDIT.EXCD_SK=EXCD.EXCD_SK
  AND SUB.GRP_SK=GRP.GRP_SK
  AND MBR_ELIG_AUDIT.SRC_SYS_CRT_USER_SK=APP_USER.USER_SK
  AND MBR_ELIG_AUDIT.MBR_UNIQ_KEY=MBR.MBR_UNIQ_KEY
  AND (
       MBR_ELIG_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
       OR (
           MBR_ELIG_AUDIT.MBR_ELIG_AUDIT_SK = 0
           OR MBR_ELIG_AUDIT.MBR_ELIG_AUDIT_SK = 1
          )
      )
"""
df_db2_MBR_ELIG_AUDIT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_MBR_ELIG_AUDIT_in)
    .load()
)

# --------------------------------------------------------------------------------
# db2_CD_MPPNG_Extr (DB2ConnectorPX)
# --------------------------------------------------------------------------------
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
# Cpy_Mppng_Cd (PxCopy)
# --------------------------------------------------------------------------------
df_Ref_MbrEligAuditActnCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_MbrEligClsProdCatCd_Lkup = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_MbrEligTypCd_Lkup = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# --------------------------------------------------------------------------------
# lkp_Codes (PxLookup)
# --------------------------------------------------------------------------------
df_lkp_Codes = (
    df_db2_MBR_ELIG_AUDIT_in.alias("p")
    .join(
        df_Ref_MbrEligAuditActnCd_Lkp.alias("l1"),
        F.col("p.MBR_ELIG_AUDIT_ACTN_CD_SK") == F.col("l1.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_MbrEligClsProdCatCd_Lkup.alias("l2"),
        F.col("p.MBR_ELIG_CLS_PROD_CAT_CD_SK") == F.col("l2.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_MbrEligTypCd_Lkup.alias("l3"),
        F.col("p.MBR_ELIG_TYP_CD_SK") == F.col("l3.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("p.MBR_ELIG_AUDIT_SK").alias("MBR_ELIG_AUDIT_SK"),
        F.col("p.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("p.MBR_ELIG_AUDIT_ROW_ID").alias("MBR_ELIG_AUDIT_ROW_ID"),
        F.col("p.CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.col("p.EXCD_SK").alias("EXCD_SK"),
        F.col("p.GRP_SK").alias("GRP_SK"),
        F.col("p.MBR_SK").alias("MBR_SK"),
        F.col("p.SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
        F.col("p.SUB_SK").alias("SUB_SK"),
        F.col("p.CLS_PLN_ID").alias("CLS_PLN_ID"),
        F.col("p.EXCD_ID").alias("EXCD_ID"),
        F.col("p.GRP_ID").alias("GRP_ID"),
        F.col("p.GRP_NM").alias("GRP_NM"),
        F.col("p.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
        F.col("l1.TRGT_CD").alias("MBR_ELIG_AUDIT_ACTN_CD"),
        F.col("l1.TRGT_CD_NM").alias("MBR_ELIG_AUDIT_ACTN_NM"),
        F.col("l2.TRGT_CD").alias("MBR_ELIG_AUDIT_CLS_PROD_CAT_CD"),
        F.col("l2.TRGT_CD_NM").alias("MBR_ELIG_AUDIT_CLS_PROD_CAT_NM"),
        F.col("p.EFF_DT_SK").alias("MBR_ELIG_EFF_DT_SK"),
        F.col("l3.TRGT_CD").alias("MBR_ELIG_TYP_CD"),
        F.col("l3.TRGT_CD_NM").alias("MBR_ELIG_TYP_NM"),
        F.col("p.VOID_IN").alias("MBR_ELIG_VOID_IN"),
        F.col("p.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("p.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("p.SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
        F.col("p.USER_ID").alias("SRC_SYS_CRT_USER_ID"),
        F.col("p.MBR_ELIG_AUDIT_ACTN_CD_SK").alias("MBR_ELIG_AUDIT_ACTN_CD_SK"),
        F.col("p.MBR_ELIG_CLS_PROD_CAT_CD_SK").alias("MBR_ELIG_CLS_PROD_CAT_CD_SK"),
        F.col("p.MBR_ELIG_TYP_CD_SK").alias("MBR_ELIG_TYP_CD_SK")
    )
)

# --------------------------------------------------------------------------------
# xfm_BusinessLogic (CTransformerStage)
# --------------------------------------------------------------------------------
df_enriched = df_lkp_Codes

# Add row_number to replicate Transformer constraints that reference @INROWNUM etc.
w = Window.orderBy(F.lit(1))
df_enriched2 = df_enriched.withColumn("_row_num", F.row_number().over(w))

# lnk_Main constraint: MBR_ELIG_AUDIT_SK <> 0 And MBR_ELIG_AUDIT_SK <> 1
df_main = df_enriched2.filter(
    (F.col("MBR_ELIG_AUDIT_SK") != 0) & (F.col("MBR_ELIG_AUDIT_SK") != 1)
).select(
    F.col("MBR_ELIG_AUDIT_SK").alias("MBR_ELIG_AUDIT_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_ELIG_AUDIT_ROW_ID").alias("MBR_ELIG_AUDIT_ROW_ID"),
    F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("EXCD_SK").alias("EXCD_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("EXCD_ID").alias("EXCD_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.col("MBR_ELIG_AUDIT_ACTN_CD").alias("MBR_ELIG_AUDIT_ACTN_CD"),
    F.col("MBR_ELIG_AUDIT_ACTN_NM").alias("MBR_ELIG_AUDIT_ACTN_NM"),
    F.col("MBR_ELIG_AUDIT_CLS_PROD_CAT_CD").alias("MBR_ELIG_CLS_PROD_CAT_CD"),
    F.col("MBR_ELIG_AUDIT_CLS_PROD_CAT_NM").alias("MBR_ELIG_CLS_PROD_CAT_NM"),
    F.col("MBR_ELIG_EFF_DT_SK").alias("MBR_ELIG_EFF_DT_SK"),
    F.col("MBR_ELIG_TYP_CD").alias("MBR_ELIG_TYP_CD"),
    F.col("MBR_ELIG_TYP_NM").alias("MBR_ELIG_TYP_NM"),
    F.col("MBR_ELIG_VOID_IN").alias("MBR_ELIG_VOID_IN"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    F.col("SRC_SYS_CRT_USER_ID").alias("SRC_SYS_CRT_USER_ID"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_ELIG_AUDIT_ACTN_CD_SK").alias("MBR_ELIG_AUDIT_ACTN_CD_SK"),
    F.col("MBR_ELIG_CLS_PROD_CAT_CD_SK").alias("MBR_ELIG_CLS_PROD_CAT_CD_SK"),
    F.col("MBR_ELIG_TYP_CD_SK").alias("MBR_ELIG_TYP_CD_SK")
)

# "UNK" constraint: ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1 => effectively first row
df_UNK = df_enriched2.filter(F.col("_row_num") == 1).select(
    F.lit(0).alias("MBR_ELIG_AUDIT_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("MBR_ELIG_AUDIT_ROW_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("CLS_PLN_SK"),
    F.lit(0).alias("EXCD_SK"),
    F.lit(0).alias("GRP_SK"),
    F.lit(0).alias("MBR_SK"),
    F.lit(0).alias("SRC_SYS_CRT_USER_SK"),
    F.lit(0).alias("SUB_SK"),
    F.lit("UNK").alias("CLS_PLN_ID"),
    F.lit("UNK").alias("EXCD_ID"),
    F.lit("UNK").alias("GRP_ID"),
    F.lit("UNK").alias("GRP_NM"),
    F.lit(0).alias("GRP_UNIQ_KEY"),
    F.lit("UNK").alias("MBR_ELIG_AUDIT_ACTN_CD"),
    F.lit("UNK").alias("MBR_ELIG_AUDIT_ACTN_NM"),
    F.lit("UNK").alias("MBR_ELIG_CLS_PROD_CAT_CD"),
    F.lit("UNK").alias("MBR_ELIG_CLS_PROD_CAT_NM"),
    F.lit("1753-01-01").alias("MBR_ELIG_EFF_DT_SK"),
    F.lit("UNK").alias("MBR_ELIG_TYP_CD"),
    F.lit("UNK").alias("MBR_ELIG_TYP_NM"),
    F.lit("N").alias("MBR_ELIG_VOID_IN"),
    F.lit(None).alias("MBR_SFX_NO"),
    F.lit(0).alias("MBR_UNIQ_KEY"),
    F.lit("1753-01-01").alias("SRC_SYS_CRT_DT_SK"),
    F.lit("UNK").alias("SRC_SYS_CRT_USER_ID"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("MBR_ELIG_AUDIT_ACTN_CD_SK"),
    F.lit(0).alias("MBR_ELIG_CLS_PROD_CAT_CD_SK"),
    F.lit(0).alias("MBR_ELIG_TYP_CD_SK")
)

# "NA" constraint: also first row
df_NA = df_enriched2.filter(F.col("_row_num") == 1).select(
    F.lit(1).alias("MBR_ELIG_AUDIT_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("MBR_ELIG_AUDIT_ROW_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("CLS_PLN_SK"),
    F.lit(1).alias("EXCD_SK"),
    F.lit(1).alias("GRP_SK"),
    F.lit(1).alias("MBR_SK"),
    F.lit(1).alias("SRC_SYS_CRT_USER_SK"),
    F.lit(1).alias("SUB_SK"),
    F.lit("NA").alias("CLS_PLN_ID"),
    F.lit("NA").alias("EXCD_ID"),
    F.lit("NA").alias("GRP_ID"),
    F.lit("NA").alias("GRP_NM"),
    F.lit(1).alias("GRP_UNIQ_KEY"),
    F.lit("NA").alias("MBR_ELIG_AUDIT_ACTN_CD"),
    F.lit("NA").alias("MBR_ELIG_AUDIT_ACTN_NM"),
    F.lit("NA").alias("MBR_ELIG_CLS_PROD_CAT_CD"),
    F.lit("NA").alias("MBR_ELIG_CLS_PROD_CAT_NM"),
    F.lit("1753-01-01").alias("MBR_ELIG_EFF_DT_SK"),
    F.lit("NA").alias("MBR_ELIG_TYP_CD"),
    F.lit("NA").alias("MBR_ELIG_TYP_NM"),
    F.lit("N").alias("MBR_ELIG_VOID_IN"),
    F.lit(None).alias("MBR_SFX_NO"),
    F.lit(1).alias("MBR_UNIQ_KEY"),
    F.lit("1753-01-01").alias("SRC_SYS_CRT_DT_SK"),
    F.lit("NA").alias("SRC_SYS_CRT_USER_ID"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("MBR_ELIG_AUDIT_ACTN_CD_SK"),
    F.lit(1).alias("MBR_ELIG_CLS_PROD_CAT_CD_SK"),
    F.lit(1).alias("MBR_ELIG_TYP_CD_SK")
)

# --------------------------------------------------------------------------------
# fnl_dataLinks (PxFunnel)
# Funnel order: NA, UNK, lnk_Main
# --------------------------------------------------------------------------------
df_fnl_dataLinks = (
    df_NA.unionByName(df_UNK)
    .unionByName(df_main)
)

# --------------------------------------------------------------------------------
# seq_MBR_ELIG_AUDIT_D_Load (PxSequentialFile)
# Write to MBR_ELIG_AUDIT_D.dat
# --------------------------------------------------------------------------------

# Apply rpad where columns have SqlType=char or varchar in the job:
df_fnl_dataLinks_rpad = (
    df_fnl_dataLinks
    .withColumn("EXCD_ID", F.rpad(F.col("EXCD_ID"), 4, " "))
    .withColumn("MBR_ELIG_EFF_DT_SK", F.rpad(F.col("MBR_ELIG_EFF_DT_SK"), 10, " "))
    .withColumn("MBR_ELIG_VOID_IN", F.rpad(F.col("MBR_ELIG_VOID_IN"), 1, " "))
    .withColumn("MBR_SFX_NO", F.rpad(F.col("MBR_SFX_NO"), 2, " "))
    .withColumn("SRC_SYS_CRT_DT_SK", F.rpad(F.col("SRC_SYS_CRT_DT_SK"), 10, " "))
    .withColumn("SRC_SYS_CRT_USER_ID", F.rpad(F.col("SRC_SYS_CRT_USER_ID"), 10, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
)

# Select columns in final order
final_columns = [
    "MBR_ELIG_AUDIT_SK",
    "SRC_SYS_CD",
    "MBR_ELIG_AUDIT_ROW_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CLS_PLN_SK",
    "EXCD_SK",
    "GRP_SK",
    "MBR_SK",
    "SRC_SYS_CRT_USER_SK",
    "SUB_SK",
    "CLS_PLN_ID",
    "EXCD_ID",
    "GRP_ID",
    "GRP_NM",
    "GRP_UNIQ_KEY",
    "MBR_ELIG_AUDIT_ACTN_CD",
    "MBR_ELIG_AUDIT_ACTN_NM",
    "MBR_ELIG_CLS_PROD_CAT_CD",
    "MBR_ELIG_CLS_PROD_CAT_NM",
    "MBR_ELIG_EFF_DT_SK",
    "MBR_ELIG_TYP_CD",
    "MBR_ELIG_TYP_NM",
    "MBR_ELIG_VOID_IN",
    "MBR_SFX_NO",
    "MBR_UNIQ_KEY",
    "SRC_SYS_CRT_DT_SK",
    "SRC_SYS_CRT_USER_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_ELIG_AUDIT_ACTN_CD_SK",
    "MBR_ELIG_CLS_PROD_CAT_CD_SK",
    "MBR_ELIG_TYP_CD_SK"
]
df_final = df_fnl_dataLinks_rpad.select(*final_columns)

write_files(
    df_final,
    f"{adls_path}/load/MBR_ELIG_AUDIT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)