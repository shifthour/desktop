# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  EdwVbbExtrSeq
# MAGIC 
# MAGIC PROCESSING: Extracts data from IDS MBR_VBB_CMPNT_ENR table and creates a load file for EDW MBR_VBB_CMPNT_ENR_F
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #                  Change Description                               Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------      ---------------------------------------------------------       ----------------------------------     ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2013-05-11    4963 VBB Phase III           Initial Programming                                    EnterpriseNewdevl        Bhoomi Dasari              7/9/2013

# MAGIC EDW MBR_VBB_CMPNT_ENR extract from IDS
# MAGIC Business Rules that determine Edw Output
# MAGIC These Hashed files are created by IdsMbrVbbGrpClsClsPlnHahsCreateExtr and used in IdsMbrVbbCmpntEnrFExtr and IdsMbrVbbPlnEnrFExtr. Cleared in IdsMbrVbbCmpntEnrFExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')
EDWRunCycle = get_widget_value('EDWRunCycle','0')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','2007-12-12')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""SELECT MBR_VBB_CMPNT_ENR.MBR_VBB_CMPNT_ENR_SK,
MBR_VBB_CMPNT_ENR.MBR_UNIQ_KEY,
MBR_VBB_CMPNT_ENR.VBB_CMPNT_UNIQ_KEY,
MBR_VBB_CMPNT_ENR.SRC_SYS_CD_SK,
MBR_VBB_CMPNT_ENR.CRT_RUN_CYC_EXCTN_SK,
MBR_VBB_CMPNT_ENR.LAST_UPDT_RUN_CYC_EXCTN_SK,
MBR_VBB_CMPNT_ENR.CLS_SK,
MBR_VBB_CMPNT_ENR.CLS_PLN_SK,
MBR_VBB_CMPNT_ENR.GRP_SK,
MBR_VBB_CMPNT_ENR.MBR_SK,
MBR_VBB_CMPNT_ENR.MBR_VBB_PLN_ENR_SK,
MBR_VBB_CMPNT_ENR.VBB_CMPNT_SK,
MBR_VBB_CMPNT_ENR.MBR_VBB_CMPNT_ENR_METH_CD_SK,
MBR_VBB_CMPNT_ENR.MBR_VBB_CMPNT_STTUS_CD_SK,
MBR_VBB_CMPNT_ENR.MBR_VBB_CMPNT_TERM_METH_CD_SK,
MBR_VBB_CMPNT_ENR.MBR_VBB_CMPNT_TERM_RSN_CD_SK,
MBR_VBB_CMPNT_ENR.MBR_CNTCT_IN,
MBR_VBB_CMPNT_ENR.MBR_CNTCT_LAST_UPDT_DT_SK,
MBR_VBB_CMPNT_ENR.MBR_VBB_CMPNT_CMPLTN_DT_SK,
MBR_VBB_CMPNT_ENR.MBR_VBB_CMPNT_ENR_DT_SK,
MBR_VBB_CMPNT_ENR.MBR_VBB_CMPNT_TERM_DT_SK,
MBR_VBB_CMPNT_ENR.SRC_SYS_CRT_DTM,
MBR_VBB_CMPNT_ENR.SRC_SYS_UPDT_DTM,
MBR_VBB_CMPNT_ENR.VBB_VNDR_UNIQ_KEY,
MBR_VBB_CMPNT_ENR.VNDR_PGM_SEQ_NO,
MBR_VBB_CMPNT_ENR.MBR_CMPLD_ACHV_LVL_CT,
MBR_VBB_CMPNT_ENR.TRZ_MBR_UNVRS_ID
FROM {IDSOwner}.MBR_VBB_CMPNT_ENR MBR_VBB_CMPNT_ENR
WHERE MBR_VBB_CMPNT_ENR.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}

{IDSOwner}.WELNS_CLS
"""

df_MBR_VBB_CMPNT_ENR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")
df_hf_mbrvbbenr_clsplnid = spark.read.parquet(f"{adls_path}/hf_mbrvbbenr_clsplnid.parquet")
df_hf_mbrvbbenr_clsid = spark.read.parquet(f"{adls_path}/hf_mbrvbbenr_clsid.parquet")
df_hf_mbrvbbenr_grpid = spark.read.parquet(f"{adls_path}/hf_mbrvbbenr_grpid.parquet")

df_BussinessLogic = (
    df_MBR_VBB_CMPNT_ENR.alias("Extract")
    .join(df_hf_cdma_codes.alias("Src_sys_cd"), F.col("Extract.SRC_SYS_CD_SK") == F.col("Src_sys_cd.CD_MPPNG_SK"), "left")
    .join(df_hf_cdma_codes.alias("Cmpnt_Enr_meth_cd"), F.col("Extract.MBR_VBB_CMPNT_ENR_METH_CD_SK") == F.col("Cmpnt_Enr_meth_cd.CD_MPPNG_SK"), "left")
    .join(df_hf_cdma_codes.alias("Cmpnt_Term_Meth_Cd"), F.col("Extract.MBR_VBB_CMPNT_TERM_METH_CD_SK") == F.col("Cmpnt_Term_Meth_Cd.CD_MPPNG_SK"), "left")
    .join(df_hf_cdma_codes.alias("Cmpnt_Sttus_Cd"), F.col("Extract.MBR_VBB_CMPNT_STTUS_CD_SK") == F.col("Cmpnt_Sttus_Cd.CD_MPPNG_SK"), "left")
    .join(df_hf_cdma_codes.alias("Cmpnt_Term_Rsn_Cd"), F.col("Extract.MBR_VBB_CMPNT_TERM_RSN_CD_SK") == F.col("Cmpnt_Term_Rsn_Cd.CD_MPPNG_SK"), "left")
    .join(df_hf_mbrvbbenr_grpid.alias("grp"), F.col("Extract.GRP_SK") == F.col("grp.GRP_SK"), "left")
    .join(df_hf_mbrvbbenr_clsid.alias("cls"), F.col("Extract.CLS_SK") == F.col("cls.CLS_SK"), "left")
    .join(df_hf_mbrvbbenr_clsplnid.alias("cls_pln"), F.col("Extract.CLS_PLN_SK") == F.col("cls_pln.CLS_PLN_SK"), "left")
)

df_MBR_VBB_CMPNT_ENR_F = df_BussinessLogic.select(
    F.col("Extract.MBR_VBB_CMPNT_ENR_SK").alias("MBR_VBB_CMPNT_ENR_SK"),
    F.col("Extract.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Extract.VBB_CMPNT_UNIQ_KEY").alias("VBB_CMPNT_UNIQ_KEY"),
    F.when(F.col("Src_sys_cd.TRGT_CD").isNull(), "UNK").otherwise(F.col("Src_sys_cd.TRGT_CD")).alias("SRC_SYS_CD"),
    F.rpad(F.lit(CurrRunCycleDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.lit(CurrRunCycleDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Extract.CLS_SK").alias("CLS_SK"),
    F.col("Extract.CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("Extract.GRP_SK").alias("GRP_SK"),
    F.col("Extract.MBR_SK").alias("MBR_SK"),
    F.col("Extract.MBR_VBB_PLN_ENR_SK").alias("MBR_VBB_PLN_ENR_SK"),
    F.col("Extract.VBB_CMPNT_SK").alias("VBB_CMPNT_SK"),
    F.when(F.col("Cmpnt_Enr_meth_cd.TRGT_CD").isNull(), "UNK").otherwise(F.col("Cmpnt_Enr_meth_cd.TRGT_CD")).alias("MBR_VBB_CMPNT_ENR_METH_CD"),
    F.when(F.col("Cmpnt_Enr_meth_cd.TRGT_CD_NM").isNull(), "UNK").otherwise(F.col("Cmpnt_Enr_meth_cd.TRGT_CD_NM")).alias("MBR_VBB_CMPNT_ENR_METH_NM"),
    F.when(F.col("Cmpnt_Sttus_Cd.TRGT_CD").isNull(), "UNK").otherwise(F.col("Cmpnt_Sttus_Cd.TRGT_CD")).alias("MBR_VBB_CMPNT_STTUS_CD"),
    F.when(F.col("Cmpnt_Sttus_Cd.TRGT_CD_NM").isNull(), "UNK").otherwise(F.col("Cmpnt_Sttus_Cd.TRGT_CD_NM")).alias("MBR_VBB_CMPNT_STTUS_NM"),
    F.when(F.col("Cmpnt_Term_Meth_Cd.TRGT_CD").isNull(), "UNK").otherwise(F.col("Cmpnt_Term_Meth_Cd.TRGT_CD")).alias("MBR_VBB_CMPNT_TERM_METH_CD"),
    F.when(F.col("Cmpnt_Term_Meth_Cd.TRGT_CD_NM").isNull(), "UNK").otherwise(F.col("Cmpnt_Term_Meth_Cd.TRGT_CD_NM")).alias("MBR_VBB_CMPNT_TERM_METH_NM"),
    F.when(F.col("Cmpnt_Term_Rsn_Cd.TRGT_CD").isNull(), "UNK").otherwise(F.col("Cmpnt_Term_Rsn_Cd.TRGT_CD")).alias("MBR_VBB_CMPNT_TERM_RSN_CD"),
    F.when(F.col("Cmpnt_Term_Rsn_Cd.TRGT_CD_NM").isNull(), "UNK").otherwise(F.col("Cmpnt_Term_Rsn_Cd.TRGT_CD_NM")).alias("MBR_VBB_CMPNT_TERM_RSN_NM"),
    F.rpad(F.col("Extract.MBR_CNTCT_IN"), 1, " ").alias("MBR_CNTCT_IN"),
    F.rpad(F.col("Extract.MBR_CNTCT_LAST_UPDT_DT_SK"), 10, " ").alias("MBR_CNTCT_LAST_UPDT_DT_SK"),
    F.rpad(F.col("Extract.MBR_VBB_CMPNT_CMPLTN_DT_SK"), 10, " ").alias("MBR_VBB_CMPNT_CMPLTN_DT_SK"),
    F.rpad(F.col("Extract.MBR_VBB_CMPNT_ENR_DT_SK"), 10, " ").alias("MBR_VBB_CMPNT_ENR_DT_SK"),
    F.rpad(F.col("Extract.MBR_VBB_CMPNT_TERM_DT_SK"), 10, " ").alias("MBR_VBB_CMPNT_TERM_DT_SK"),
    F.col("Extract.SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
    F.col("Extract.SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
    F.col("Extract.MBR_CMPLD_ACHV_LVL_CT").alias("MBR_CMPLD_ACHV_LVL_CT"),
    F.col("Extract.VBB_VNDR_UNIQ_KEY").alias("VBB_VNDR_UNIQ_KEY"),
    F.col("Extract.VNDR_PGM_SEQ_NO").alias("VNDR_PGM_SEQ_NO"),
    F.rpad(F.when(F.col("cls.CLS_ID").isNull(), "UNK").otherwise(F.col("cls.CLS_ID")), <...>, " ").alias("CLS_ID"),
    F.rpad(F.when(F.col("cls_pln.CLS_PLN_ID").isNull(), "UNK").otherwise(F.col("cls_pln.CLS_PLN_ID")), <...>, " ").alias("CLS_PLN_ID"),
    F.rpad(F.when(F.col("grp.GRP_ID").isNull(), "UNK").otherwise(F.col("grp.GRP_ID")), <...>, " ").alias("GRP_ID"),
    F.col("Extract.TRZ_MBR_UNVRS_ID").alias("TRZ_MBR_UNVRS_ID"),
    F.col("EDWRunCycle").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("EDWRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Extract.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Extract.MBR_VBB_CMPNT_ENR_METH_CD_SK").alias("MBR_VBB_CMPNT_ENR_METH_CD_SK"),
    F.col("Extract.MBR_VBB_CMPNT_STTUS_CD_SK").alias("MBR_VBB_CMPNT_STTUS_CD_SK"),
    F.col("Extract.MBR_VBB_CMPNT_TERM_METH_CD_SK").alias("MBR_VBB_CMPNT_TERM_METH_CD_SK"),
    F.col("Extract.MBR_VBB_CMPNT_TERM_RSN_CD_SK").alias("MBR_VBB_CMPNT_TERM_RSN_CD_SK")
)

write_files(
    df_MBR_VBB_CMPNT_ENR_F,
    f"{adls_path}/load/MBR_VBB_CMPNT_ENR_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)