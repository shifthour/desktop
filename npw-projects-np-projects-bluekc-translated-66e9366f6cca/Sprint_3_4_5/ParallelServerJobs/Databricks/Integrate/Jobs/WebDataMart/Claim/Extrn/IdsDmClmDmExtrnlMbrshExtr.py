# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC                    IDS Product Data Mart Deduct Component extract from IDS to Data Mart.  
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                     Extracts from IDS and pulls to the Data Marts.  Perform no changes with the data.
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project               Code Reviewer                      Date Reviewed
# MAGIC ---------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------              ---------------------------------               -------------------------
# MAGIC Ralph Tucker                 2008-10-27       Labor Accts - 3648                New Etl                                                                              devlIDSnew                             Steph Goddard                      11/07/2008                
# MAGIC Archana Palivela            09/14/2013      5114                                      Original Programming(Server to Parallel)                             IntegrateWrhsDevl                      Jag Yelavarthi                        2013-11-30
# MAGIC Jag Yelavarthi                2014-01-13        #5114                                         MBR_MID_INIT - Applied logic if the content of this             IntegrateWrhsDevl 
# MAGIC                                                                                                               column is just a comma change                 
# MAGIC                                                                                                               it to a Space
# MAGIC Hugh Sisson                   2023-03-08       Prod Supp                             AND EXTMBR.MBR_BRTH_DT_SK <> 'UNK'

# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC MBR_GNDR_CD_SK
# MAGIC MBR_RELSHP_CD_SK
# MAGIC Write CLM_DM_CLM_EXTRNL_MBRSH Data into a Sequential file for Load Job IdsDmClmDmExtrnlMbrshLoad.
# MAGIC Read all the Data from IDS CLM_EXTRNL_MBRSH Table; .
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsDmClmDmExtrnlMbrshExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
DataMartRunCycle = get_widget_value('DataMartRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_CLM_EXTRNL_MBRSH_in = f"""
SELECT
EXTMBR.CLM_ID,
COALESCE(MAP.TRGT_CD,'UNK') SRC_SYS_CD,
MBR_BRTH_DT_SK,
MBR_GNDR_CD_SK,
MBR_RELSHP_CD_SK,
MBR_FIRST_NM,
MBR_MIDINIT,
MBR_LAST_NM,
SUB_ID,
SUB_FIRST_NM,
SUB_MIDINIT,
SUB_LAST_NM,
SUBMT_SUB_ID,
ACTL_SUB_ID
FROM
{IDSOwner}.CLM_EXTRNL_MBRSH EXTMBR,
{IDSOwner}.W_WEBDM_ETL_DRVR DRVR,
{IDSOwner}.CD_MPPNG MAP
WHERE EXTMBR.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
  AND EXTMBR.CLM_ID = DRVR.CLM_ID
  AND EXTMBR.SRC_SYS_CD_SK = MAP.CD_MPPNG_SK
  AND EXTMBR.MBR_BRTH_DT_SK <> 'UNK'
"""

df_db2_CLM_EXTRNL_MBRSH_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_EXTRNL_MBRSH_in)
    .load()
)

extract_query_DB2_MbrRelsh_In = f"""
SELECT
CD_MPPNG_SK,
SRC_CD,
TRGT_CD
FROM {IDSOwner}.CD_MPPNG
WHERE SRC_DOMAIN_NM = 'MEMBER RELATIONSHIP ITS'
"""

df_DB2_MbrRelsh_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_MbrRelsh_In)
    .load()
)

extract_query_DB2_Mbr_Gndr_Ib = f"""
SELECT
CD_MPPNG_SK,
TRGT_CD
FROM
{IDSOwner}.CD_MPPNG
WHERE SRC_DOMAIN_NM = 'GENDER'
"""

df_DB2_Mbr_Gndr_Ib = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_Mbr_Gndr_Ib)
    .load()
)

df_lkp_Codes = (
    df_db2_CLM_EXTRNL_MBRSH_in.alias("lnk_IdsDmClmDmExtrnlMbrshExtr_InABC")
    .join(
        df_DB2_MbrRelsh_In.alias("Ref_MbrRelshpcd"),
        F.col("lnk_IdsDmClmDmExtrnlMbrshExtr_InABC.MBR_RELSHP_CD_SK") == F.col("Ref_MbrRelshpcd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_DB2_Mbr_Gndr_Ib.alias("Ref_MbrGndrCdLkup"),
        F.col("lnk_IdsDmClmDmExtrnlMbrshExtr_InABC.MBR_GNDR_CD_SK") == F.col("Ref_MbrGndrCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsDmClmDmExtrnlMbrshExtr_InABC.CLM_ID").alias("CLM_ID"),
        F.col("lnk_IdsDmClmDmExtrnlMbrshExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsDmClmDmExtrnlMbrshExtr_InABC.SUB_ID").alias("CLM_EXTRNL_MBRSH_SUB_ID"),
        F.col("Ref_MbrRelshpcd.TRGT_CD").alias("CLM_EXTRNL_MBRSH_MBR_RELSHP_CD"),
        F.col("lnk_IdsDmClmDmExtrnlMbrshExtr_InABC.MBR_FIRST_NM").alias("CLM_EXTRNL_MBRSH_MBR_FIRST_NM"),
        F.col("lnk_IdsDmClmDmExtrnlMbrshExtr_InABC.MBR_MIDINIT").alias("CLM_EXTRNL_MBRSH_MBR_MIDINIT"),
        F.col("lnk_IdsDmClmDmExtrnlMbrshExtr_InABC.MBR_LAST_NM").alias("CLM_EXTRNL_MBRSH_MBR_LAST_NM"),
        F.col("lnk_IdsDmClmDmExtrnlMbrshExtr_InABC.MBR_BRTH_DT_SK").alias("CLM_EXTRNL_MBRSH_MBR_BRTH_DT"),
        F.col("Ref_MbrGndrCdLkup.TRGT_CD").alias("CLM_EXTRNL_MBRSH_MBR_GNDR_CD"),
        F.col("lnk_IdsDmClmDmExtrnlMbrshExtr_InABC.SUB_FIRST_NM").alias("CLM_EXTRNL_MBRSH_SUB_FIRST_NM"),
        F.col("lnk_IdsDmClmDmExtrnlMbrshExtr_InABC.SUB_MIDINIT").alias("CLM_EXTRNL_MBRSH_SUB_MIDINIT"),
        F.col("lnk_IdsDmClmDmExtrnlMbrshExtr_InABC.SUB_LAST_NM").alias("CLM_EXTRNL_MBRSH_SUB_LAST_NM"),
        F.col("lnk_IdsDmClmDmExtrnlMbrshExtr_InABC.ACTL_SUB_ID").alias("CLM_EXTRNL_MBRSH_ACTL_SUB_ID"),
        F.col("lnk_IdsDmClmDmExtrnlMbrshExtr_InABC.SUBMT_SUB_ID").alias("CLM_EXTRNL_MBRSH_SUBMT_SUB_ID")
    )
)

df_xfrm_BusinessLogic = (
    df_lkp_Codes
    .withColumn(
        "SRC_SYS_CD",
        F.when(F.trim(F.col("SRC_SYS_CD")) == "", F.lit("UNK")).otherwise(F.col("SRC_SYS_CD"))
    )
    .withColumn("CLM_ID", F.col("CLM_ID"))
    .withColumn("CLM_EXTRNL_MBRSH_SUB_ID", F.col("CLM_EXTRNL_MBRSH_SUB_ID"))
    .withColumn(
        "CLM_EXTRNL_MBRSH_MBR_RELSHP_CD",
        F.when(
            (F.trim(F.col("CLM_EXTRNL_MBRSH_MBR_RELSHP_CD")) == "") | (F.col("CLM_EXTRNL_MBRSH_MBR_RELSHP_CD").isNull()),
            F.lit("UNK")
        ).otherwise(F.col("CLM_EXTRNL_MBRSH_MBR_RELSHP_CD"))
    )
    .withColumn("CLM_EXTRNL_MBRSH_MBR_FIRST_NM", F.col("CLM_EXTRNL_MBRSH_MBR_FIRST_NM"))
    .withColumn(
        "CLM_EXTRNL_MBRSH_MBR_MIDINIT",
        F.when(
            F.trim(F.col("CLM_EXTRNL_MBRSH_MBR_MIDINIT")) == ",",
            F.lit(" ")
        ).otherwise(F.col("CLM_EXTRNL_MBRSH_MBR_MIDINIT"))
    )
    .withColumn("CLM_EXTRNL_MBRSH_MBR_LAST_NM", F.col("CLM_EXTRNL_MBRSH_MBR_LAST_NM"))
    .withColumn(
        "CLM_EXTRNL_MBRSH_MBR_BRTH_DT",
        F.to_timestamp(F.col("CLM_EXTRNL_MBRSH_MBR_BRTH_DT"), "yyyy-MM-dd")
    )
    .withColumn(
        "CLM_EXTRNL_MBRSH_MBR_GNDR_CD",
        F.when(
            (F.trim(F.col("CLM_EXTRNL_MBRSH_MBR_GNDR_CD")) == "") | (F.col("CLM_EXTRNL_MBRSH_MBR_GNDR_CD").isNull()),
            F.lit("UNKGNDR ")
        ).otherwise(F.col("CLM_EXTRNL_MBRSH_MBR_GNDR_CD"))
    )
    .withColumn("CLM_EXTRNL_MBRSH_SUB_FIRST_NM", F.col("CLM_EXTRNL_MBRSH_SUB_FIRST_NM"))
    .withColumn(
        "CLM_EXTRNL_MBRSH_SUB_MIDINIT",
        trim(F.col("CLM_EXTRNL_MBRSH_SUB_MIDINIT"), F.lit(","), F.lit("A"))
    )
    .withColumn("CLM_EXTRNL_MBRSH_SUB_LAST_NM", F.col("CLM_EXTRNL_MBRSH_SUB_LAST_NM"))
    .withColumn("CLM_EXTRNL_MBRSH_ACTL_SUB_ID", F.col("CLM_EXTRNL_MBRSH_ACTL_SUB_ID"))
    .withColumn(
        "CLM_EXTRNL_MBRSH_SUBMT_SUB_ID",
        F.when(F.col("CLM_EXTRNL_MBRSH_SUBMT_SUB_ID").isNull(), F.lit(" ")).otherwise(F.col("CLM_EXTRNL_MBRSH_SUBMT_SUB_ID"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_NO", F.lit(DataMartRunCycle))
)

df_final = (
    df_xfrm_BusinessLogic
    .withColumn(
        "CLM_EXTRNL_MBRSH_SUB_ID",
        F.rpad(F.col("CLM_EXTRNL_MBRSH_SUB_ID"), 14, " ")
    )
    .withColumn(
        "CLM_EXTRNL_MBRSH_MBR_MIDINIT",
        F.rpad(F.col("CLM_EXTRNL_MBRSH_MBR_MIDINIT"), 1, " ")
    )
    .withColumn(
        "CLM_EXTRNL_MBRSH_MBR_BRTH_DT",
        F.date_format(F.col("CLM_EXTRNL_MBRSH_MBR_BRTH_DT"), "yyyy-MM-dd")
    )
    .withColumn(
        "CLM_EXTRNL_MBRSH_MBR_BRTH_DT",
        F.rpad(F.col("CLM_EXTRNL_MBRSH_MBR_BRTH_DT"), 10, " ")
    )
    .withColumn(
        "CLM_EXTRNL_MBRSH_SUB_MIDINIT",
        F.rpad(F.col("CLM_EXTRNL_MBRSH_SUB_MIDINIT"), 1, " ")
    )
    .select(
        "SRC_SYS_CD",
        "CLM_ID",
        "CLM_EXTRNL_MBRSH_SUB_ID",
        "CLM_EXTRNL_MBRSH_MBR_RELSHP_CD",
        "CLM_EXTRNL_MBRSH_MBR_FIRST_NM",
        "CLM_EXTRNL_MBRSH_MBR_MIDINIT",
        "CLM_EXTRNL_MBRSH_MBR_LAST_NM",
        "CLM_EXTRNL_MBRSH_MBR_BRTH_DT",
        "CLM_EXTRNL_MBRSH_MBR_GNDR_CD",
        "CLM_EXTRNL_MBRSH_SUB_FIRST_NM",
        "CLM_EXTRNL_MBRSH_SUB_MIDINIT",
        "CLM_EXTRNL_MBRSH_SUB_LAST_NM",
        "CLM_EXTRNL_MBRSH_ACTL_SUB_ID",
        "CLM_EXTRNL_MBRSH_SUBMT_SUB_ID",
        "LAST_UPDT_RUN_CYC_NO"
    )
)

write_files(
    df_final,
    f"{adls_path}/load/CLM_DM_CLM_EXTRNL_MBRSH.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)