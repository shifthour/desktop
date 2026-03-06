# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC DESCRIPTION:  
# MAGIC 
# MAGIC   Pulls data from IDS Facets Audit table SUB_AUDIT into EDW table SUB_AUDIT_D
# MAGIC       
# MAGIC INPUTS:
# MAGIC 	
# MAGIC                 SUB_AUDIT
# MAGIC                 SUB
# MAGIC                 GRP
# MAGIC                 APP_USER
# MAGIC   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                Bhoomi Dasari    10/30/2006  ---    Originally Programmed
# MAGIC                Parikshith Chada  11/13/2006 ----   Modifications done , added additional columns to target
# MAGIC 
# MAGIC                                                                                                                                                                                                                    
# MAGIC DEVELOPER                         DATE                 PROJECT                                                DESCRIPTION                                            DATASTAGE                                            CODE                            REVIEW
# MAGIC                                                                                                                                                                                                               ENVIRONMENT                                       REVIEWER                   DATE                                              
# MAGIC ------------------------------------------   ----------------------      ------------------------------------------------                ---------------------------------------------------------------     ---------------------------------------------------              ------------------------------          --------------------
# MAGIC Rajasekhar Mangalampally    2013-09-05            5114                                                         Original Programming                                   EnterpriseWrhsDevl                                Jag Yelavarthi                  2013-10-11
# MAGIC                                                                                                                                           (Server to Parallel Conversion)
# MAGIC 
# MAGIC Balkarn Gill                                   11/20/2013        5114                               NA and UNK changes are made as per the DG standards                 EnterpriseWrhsDevl

# MAGIC This is Source extract data from an IDS table and apply Code denormalization, create a load ready file.
# MAGIC 
# MAGIC Job Name:
# MAGIC IdsEdwSubAuditDExtr
# MAGIC 
# MAGIC Table:
# MAGIC SUB_AUDIT_D
# MAGIC Read from source table SUB_AUDIT, GRP and APP_USER from IDS: 
# MAGIC Pull records based on LAST_UPDT_RUN_CYC_EXTN_SK
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write SUB_AUDIT_D Data into a Sequential file for Load Job.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Add Defaults and Null Handling
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC 1) SUB_AUDIT_ACTN_CD_SK
# MAGIC 2) SUB_FMLY_CNTR_CD_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, regexp_replace, rpad
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_1 = f"""
SELECT 
SUB_AUDIT.SUB_AUDIT_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
SUB_AUDIT.SRC_SYS_CD_SK,
SUB_AUDIT.SUB_AUDIT_ROW_ID,
SUB_AUDIT.SRC_SYS_CRT_USER_SK,
SUB_AUDIT.SUB_SK,
SUB_AUDIT.SUB_AUDIT_ACTN_CD_SK,
SUB_AUDIT.SUB_FMLY_CNTR_CD_SK,
SUB_AUDIT.SCRD_IN,
SUB_AUDIT.ORIG_EFF_DT_SK,
SUB_AUDIT.RETR_DT_SK,
SUB_AUDIT.SRC_SYS_CRT_DT_SK,
SUB_AUDIT.SUB_UNIQ_KEY,
SUB_AUDIT.SUB_FIRST_NM,
SUB_AUDIT.SUB_MIDINIT,
SUB_AUDIT.SUB_LAST_NM,
SUB_AUDIT.SUB_AUDIT_HIRE_DT_TX,
SUB_AUDIT.SUB_AUDIT_RCVD_DT_TX,
SUB_AUDIT.SUB_AUDIT_STTUS_TX,
GRP.GRP_SK,
GRP.GRP_ID,
GRP.GRP_UNIQ_KEY,
GRP.GRP_NM,
APP_USER.USER_ID
FROM {IDSOwner}.SUB_AUDIT SUB_AUDIT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON SUB_AUDIT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK,
{IDSOwner}.SUB SUB,
{IDSOwner}.GRP GRP,
{IDSOwner}.APP_USER APP_USER
WHERE
SUB_AUDIT.SUB_SK=SUB.SUB_SK AND
SUB.GRP_SK=GRP.GRP_SK AND
SUB_AUDIT.SRC_SYS_CRT_USER_SK=APP_USER.USER_SK AND
SUB_AUDIT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
"""

df_db2_SUB_AUDIT_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_1)
    .load()
)

extract_query_2 = f"SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM from {IDSOwner}.CD_MPPNG"

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

df_cpy_cd_mppnig_Refsub_fmly_cntr_CDLkp = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppnig_Refsub_audit_actn_lkup_CDLkp = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lnk_IdsEdwSubAuditDExtr_InABC = df_db2_SUB_AUDIT_D_in.alias("lnk_IdsEdwSubAuditDExtr_InABC")
df_Refsub_audit_actn_lkup_CDLkp = df_cpy_cd_mppnig_Refsub_audit_actn_lkup_CDLkp.alias("Refsub_audit_actn_lkup_CDLkp")
df_Refsub_fmly_cntr_CDLkp = df_cpy_cd_mppnig_Refsub_fmly_cntr_CDLkp.alias("Refsub_fmly_cntr_CDLkp")

df_lkp_CodesLkpData_out = (
    df_lnk_IdsEdwSubAuditDExtr_InABC
    .join(
        df_Refsub_audit_actn_lkup_CDLkp,
        col("lnk_IdsEdwSubAuditDExtr_InABC.SUB_AUDIT_ACTN_CD_SK") == col("Refsub_audit_actn_lkup_CDLkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Refsub_fmly_cntr_CDLkp,
        col("lnk_IdsEdwSubAuditDExtr_InABC.SUB_FMLY_CNTR_CD_SK") == col("Refsub_fmly_cntr_CDLkp.CD_MPPNG_SK"),
        "left"
    )
    .select(
        col("lnk_IdsEdwSubAuditDExtr_InABC.SUB_AUDIT_SK").alias("SUB_AUDIT_SK"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.SUB_AUDIT_ROW_ID").alias("SUB_AUDIT_ROW_ID"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.GRP_SK").alias("GRP_SK"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.SUB_SK").alias("SUB_SK"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.GRP_ID").alias("GRP_ID"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.GRP_NM").alias("GRP_NM"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.USER_ID").alias("SRC_SYS_CRT_USER_ID"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.SUB_MIDINIT").alias("SUB_MIDINIT"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.SUB_LAST_NM").alias("SUB_LAST_NM"),
        col("Refsub_audit_actn_lkup_CDLkp.TRGT_CD").alias("SUB_AUDIT_ACTN_CD"),
        col("Refsub_audit_actn_lkup_CDLkp.TRGT_CD_NM").alias("SUB_AUDIT_ACTN_NM"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.SUB_AUDIT_HIRE_DT_TX").alias("SUB_AUDIT_HIRE_DT_TX"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.SUB_AUDIT_RCVD_DT_TX").alias("SUB_AUDIT_RCVD_DT_TX"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.SUB_AUDIT_STTUS_TX").alias("SUB_AUDIT_STTUS_TX"),
        col("Refsub_fmly_cntr_CDLkp.TRGT_CD").alias("SUB_FMLY_CNTR_CD"),
        col("Refsub_fmly_cntr_CDLkp.TRGT_CD_NM").alias("SUB_FMLY_CNTR_NM"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.ORIG_EFF_DT_SK").alias("SUB_ORIG_EFF_DT_SK"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.RETR_DT_SK").alias("SUB_RETR_DT_SK"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.SCRD_IN").alias("SUB_SCRD_IN"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.SUB_AUDIT_ACTN_CD_SK").alias("SUB_AUDIT_ACTN_CD_SK"),
        col("lnk_IdsEdwSubAuditDExtr_InABC.SUB_FMLY_CNTR_CD_SK").alias("SUB_FMLY_CNTR_CD_SK")
    )
)

df_xfm_BusinessLogic_SubAuditDMainExtr = (
    df_lkp_CodesLkpData_out
    .filter((col("SUB_AUDIT_SK") != 0) & (col("SUB_AUDIT_SK") != 1))
    .select(
        col("SUB_AUDIT_SK").alias("SUB_AUDIT_SK"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("SUB_AUDIT_ROW_ID").alias("SUB_AUDIT_ROW_ID"),
        lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        col("GRP_SK").alias("GRP_SK"),
        col("SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
        col("SUB_SK").alias("SUB_SK"),
        col("GRP_ID").alias("GRP_ID"),
        col("GRP_NM").alias("GRP_NM"),
        col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
        lit("00").alias("MBR_SFX_NO"),
        col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
        col("SRC_SYS_CRT_USER_ID").alias("SRC_SYS_CRT_USER_ID"),
        col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
        regexp_replace(col("SUB_MIDINIT"), ",", "").alias("SUB_MIDINIT"),
        col("SUB_LAST_NM").alias("SUB_LAST_NM"),
        col("SUB_AUDIT_ACTN_CD").alias("SUB_AUDIT_ACTN_CD"),
        col("SUB_AUDIT_ACTN_NM").alias("SUB_AUDIT_ACTN_NM"),
        col("SUB_AUDIT_HIRE_DT_TX").alias("SUB_AUDIT_HIRE_DT_TX"),
        col("SUB_AUDIT_RCVD_DT_TX").alias("SUB_AUDIT_RCVD_DT_TX"),
        col("SUB_AUDIT_STTUS_TX").alias("SUB_AUDIT_STTUS_TX"),
        col("SUB_FMLY_CNTR_CD").alias("SUB_FMLY_CNTR_CD"),
        col("SUB_FMLY_CNTR_NM").alias("SUB_FMLY_CNTR_NM"),
        col("SUB_ORIG_EFF_DT_SK").alias("SUB_ORIG_EFF_DT_SK"),
        col("SUB_RETR_DT_SK").alias("SUB_RETR_DT_SK"),
        col("SUB_SCRD_IN").alias("SUB_SCRD_IN"),
        col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("SUB_AUDIT_ACTN_CD_SK").alias("SUB_AUDIT_ACTN_CD_SK"),
        col("SUB_FMLY_CNTR_CD_SK").alias("SUB_FMLY_CNTR_CD_SK")
    )
)

df_xfm_BusinessLogic_UNK = spark.createDataFrame(
    [
      (
        "0","UNK","UNK","1753-01-01","1753-01-01","0","0","0","UNK","UNK","0",None,
        "1753-01-01","UNK","UNK",None,"UNK","UNK","UNK","","","",
        "UNK","UNK","1753-01-01","1753-01-01","N","0","100","100","0","0"
      )
    ],
    [
      "SUB_AUDIT_SK","SRC_SYS_CD","SUB_AUDIT_ROW_ID","CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
      "GRP_SK","SRC_SYS_CRT_USER_SK","SUB_SK","GRP_ID","GRP_NM","GRP_UNIQ_KEY","MBR_SFX_NO",
      "SRC_SYS_CRT_DT_SK","SRC_SYS_CRT_USER_ID","SUB_FIRST_NM","SUB_MIDINIT","SUB_LAST_NM","SUB_AUDIT_ACTN_CD",
      "SUB_AUDIT_ACTN_NM","SUB_AUDIT_HIRE_DT_TX","SUB_AUDIT_RCVD_DT_TX","SUB_AUDIT_STTUS_TX","SUB_FMLY_CNTR_CD",
      "SUB_FMLY_CNTR_NM","SUB_ORIG_EFF_DT_SK","SUB_RETR_DT_SK","SUB_SCRD_IN","SUB_UNIQ_KEY",
      "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","SUB_AUDIT_ACTN_CD_SK","SUB_FMLY_CNTR_CD_SK"
    ]
)

df_xfm_BusinessLogic_NA = spark.createDataFrame(
    [
      (
        "1","NA","NA","1753-01-01","1753-01-01","1","1","1","NA","NA","1",None,
        "1753-01-01","NA","NA",None,"NA","NA","NA","","","",
        "NA","NA","1753-01-01","1753-01-01","N","1","100","100","1","1"
      )
    ],
    [
      "SUB_AUDIT_SK","SRC_SYS_CD","SUB_AUDIT_ROW_ID","CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
      "GRP_SK","SRC_SYS_CRT_USER_SK","SUB_SK","GRP_ID","GRP_NM","GRP_UNIQ_KEY","MBR_SFX_NO",
      "SRC_SYS_CRT_DT_SK","SRC_SYS_CRT_USER_ID","SUB_FIRST_NM","SUB_MIDINIT","SUB_LAST_NM","SUB_AUDIT_ACTN_CD",
      "SUB_AUDIT_ACTN_NM","SUB_AUDIT_HIRE_DT_TX","SUB_AUDIT_RCVD_DT_TX","SUB_AUDIT_STTUS_TX","SUB_FMLY_CNTR_CD",
      "SUB_FMLY_CNTR_NM","SUB_ORIG_EFF_DT_SK","SUB_RETR_DT_SK","SUB_SCRD_IN","SUB_UNIQ_KEY",
      "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","SUB_AUDIT_ACTN_CD_SK","SUB_FMLY_CNTR_CD_SK"
    ]
)

df_Funnel_26 = (
    df_xfm_BusinessLogic_SubAuditDMainExtr
    .unionByName(df_xfm_BusinessLogic_UNK)
    .unionByName(df_xfm_BusinessLogic_NA)
)

df_final = df_Funnel_26.select(
    col("SUB_AUDIT_SK"),
    col("SRC_SYS_CD"),
    col("SUB_AUDIT_ROW_ID"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("GRP_SK"),
    col("SRC_SYS_CRT_USER_SK"),
    col("SUB_SK"),
    col("GRP_ID"),
    col("GRP_NM"),
    col("GRP_UNIQ_KEY"),
    col("MBR_SFX_NO"),
    rpad(col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    col("SRC_SYS_CRT_USER_ID"),
    col("SUB_FIRST_NM"),
    rpad(col("SUB_MIDINIT"), 1, " ").alias("SUB_MIDINIT"),
    col("SUB_LAST_NM"),
    col("SUB_AUDIT_ACTN_CD"),
    col("SUB_AUDIT_ACTN_NM"),
    col("SUB_AUDIT_HIRE_DT_TX"),
    col("SUB_AUDIT_RCVD_DT_TX"),
    col("SUB_AUDIT_STTUS_TX"),
    col("SUB_FMLY_CNTR_CD"),
    col("SUB_FMLY_CNTR_NM"),
    rpad(col("SUB_ORIG_EFF_DT_SK"), 10, " ").alias("SUB_ORIG_EFF_DT_SK"),
    rpad(col("SUB_RETR_DT_SK"), 10, " ").alias("SUB_RETR_DT_SK"),
    rpad(col("SUB_SCRD_IN"), 1, " ").alias("SUB_SCRD_IN"),
    col("SUB_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("SUB_AUDIT_ACTN_CD_SK"),
    col("SUB_FMLY_CNTR_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/SUB_AUDIT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)