# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC IdsCustSvcDriverExtr - pulls ImpactPro & McSource information to load into the IDS driver table.
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This pulls ImpactPro & McSource information to load into the IDS driver table 'W_MBR_RISK_DRVR'.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                       Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                5/29/2008     3044                                Originally Programmed                          devlEDW                     Steph Goddard             06/02/2008                   
# MAGIC Kalyan Neelam               7/23/2010     4297                                Added new sources IHM & Alineo        EnterpriseNewDevl      Steph Goddard            07/26/2010  
# MAGIC Kalyan Neelam                2013-07-11     5056 FEP                   Added new source IDEA                          EnterpriseNewDevl      Bhoomi Dasari             7/24/2013
# MAGIC 
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               11/08/2013        5114                              Create Load File for IDS W_MBR_RISK_DRVR                             EnterpriseWhseDevl        Bhoomi Dasari             12/22/2013
# MAGIC Santosh Bokka         01/03/2014       4917 PCMH                   Added TREO Source                                                                       EnterpriseNewDevl          Kalyan Neelam             2014-01-07
# MAGIC Santosh Bokka         01/16/2014       4917 PCMH                   Added BCBSKC Source                                                                   EnterpriseNewDevl         Bhoomi Dasari             1/17/2014
# MAGIC 
# MAGIC Krishnakanth             10/13/2016       30001                            Added COBALTTALON Source                                                       EnterpriseDev2                 Jag Yelavarthi             2016-11-16
# MAGIC     Manivannan
# MAGIC 
# MAGIC Reddy Sanam            03/09/2023      US574919                   In the stage "db2_COBALTTALON_MbrRisk_in" changed the 
# MAGIC                                                                                                  value from COBALTTALON to MEDINSIGHTS                                                EnterpriseDev2      Goutham Kalidindi     2023-03-09

# MAGIC Job Name: IdsIdsMbrRiskDriverExtr
# MAGIC This load file drive the IDS extracts from each claim table.
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
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSFilePath = get_widget_value('IDSFilePath','')
ImpProRunCycle = get_widget_value('ImpProRunCycle','')
McSourceRunCycle = get_widget_value('McSourceRunCycle','')
IHMRunCycle = get_widget_value('IHMRunCycle','')
AlineoRunCycle = get_widget_value('AlineoRunCycle','')
IDEARunCycle = get_widget_value('IDEARunCycle','')
TREORunCycle = get_widget_value('TREORunCycle','')
BcbskcRunCycle = get_widget_value('BcbskcRunCycle','')
COBALTTALONRunCycle = get_widget_value('COBALTTALONRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_IDEA_MbrRisk_in = f"""
SELECT cs.SRC_SYS_CD_SK,
       cs.MBR_UNIQ_KEY,
       cs.RISK_CAT_ID,
       cs.PRCS_YR_MO_SK,
       cs.LAST_UPDT_RUN_CYC_EXCTN_SK,
       cd.TRGT_CD
FROM {IDSOwner}.MBR_RISK_MESR cs,
     {IDSOwner}.CD_MPPNG cd
WHERE cs.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'IDEA'
  AND cs.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDEARunCycle}
"""
df_db2_IDEA_MbrRisk_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_IDEA_MbrRisk_in)
    .load()
)

extract_query_db2_Alineo_MbrRisk_in = f"""
SELECT cs.SRC_SYS_CD_SK,
       cs.MBR_UNIQ_KEY,
       cs.RISK_CAT_ID,
       cs.PRCS_YR_MO_SK,
       cs.LAST_UPDT_RUN_CYC_EXCTN_SK,
       cd.TRGT_CD
FROM {IDSOwner}.MBR_RISK_MESR cs,
     {IDSOwner}.CD_MPPNG cd
WHERE cs.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'ALINEO'
  AND cs.LAST_UPDT_RUN_CYC_EXCTN_SK >= {AlineoRunCycle}
"""
df_db2_Alineo_MbrRisk_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Alineo_MbrRisk_in)
    .load()
)

extract_query_db2_IHM_MbrRisk_in = f"""
SELECT cs.SRC_SYS_CD_SK,
       cs.MBR_UNIQ_KEY,
       cs.RISK_CAT_ID,
       cs.PRCS_YR_MO_SK,
       cs.LAST_UPDT_RUN_CYC_EXCTN_SK,
       cd.TRGT_CD
FROM {IDSOwner}.MBR_RISK_MESR cs,
     {IDSOwner}.CD_MPPNG cd
WHERE cs.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'IHMANALYTICS'
  AND cs.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IHMRunCycle}
"""
df_db2_IHM_MbrRisk_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_IHM_MbrRisk_in)
    .load()
)

extract_query_db2_McSource_MbrRisk_in = f"""
SELECT cs.SRC_SYS_CD_SK,
       cs.MBR_UNIQ_KEY,
       cs.RISK_CAT_ID,
       cs.PRCS_YR_MO_SK,
       cs.LAST_UPDT_RUN_CYC_EXCTN_SK,
       cd.TRGT_CD
FROM {IDSOwner}.MBR_RISK_MESR cs,
     {IDSOwner}.CD_MPPNG cd
WHERE cs.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'MCSOURCE'
  AND cs.LAST_UPDT_RUN_CYC_EXCTN_SK >= {McSourceRunCycle}
"""
df_db2_McSource_MbrRisk_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_McSource_MbrRisk_in)
    .load()
)

extract_query_db2_ImpPro_MbrRisk_in = f"""
SELECT cs.SRC_SYS_CD_SK,
       cs.MBR_UNIQ_KEY,
       cs.RISK_CAT_ID,
       cs.PRCS_YR_MO_SK,
       cs.LAST_UPDT_RUN_CYC_EXCTN_SK,
       cd.TRGT_CD
FROM {IDSOwner}.MBR_RISK_MESR cs,
     {IDSOwner}.CD_MPPNG cd
WHERE cs.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'IMP'
  AND cs.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ImpProRunCycle}
"""
df_db2_ImpPro_MbrRisk_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_ImpPro_MbrRisk_in)
    .load()
)

extract_query_db2_TREO_MbrRisk_in = f"""
SELECT cs.SRC_SYS_CD_SK,
       cs.MBR_UNIQ_KEY,
       cs.RISK_CAT_ID,
       cs.PRCS_YR_MO_SK,
       cs.LAST_UPDT_RUN_CYC_EXCTN_SK,
       cd.TRGT_CD
FROM {IDSOwner}.MBR_RISK_MESR cs,
     {IDSOwner}.CD_MPPNG cd
WHERE cs.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'TREO'
  AND cs.LAST_UPDT_RUN_CYC_EXCTN_SK >= {TREORunCycle}
"""
df_db2_TREO_MbrRisk_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_TREO_MbrRisk_in)
    .load()
)

extract_query_db2_BCBSKC_MbrRisk_in = f"""
SELECT cs.SRC_SYS_CD_SK,
       cs.MBR_UNIQ_KEY,
       cs.RISK_CAT_ID,
       cs.PRCS_YR_MO_SK,
       cs.LAST_UPDT_RUN_CYC_EXCTN_SK,
       cd.TRGT_CD
FROM {IDSOwner}.MBR_RISK_MESR cs,
     {IDSOwner}.CD_MPPNG cd
WHERE cs.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'BCBSKC'
  AND cs.LAST_UPDT_RUN_CYC_EXCTN_SK >= {BcbskcRunCycle}
"""
df_db2_BCBSKC_MbrRisk_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_BCBSKC_MbrRisk_in)
    .load()
)

extract_query_db2_COBALTTALON_MbrRisk_in = f"""
SELECT cs.SRC_SYS_CD_SK,
       cs.MBR_UNIQ_KEY,
       cs.RISK_CAT_ID,
       cs.PRCS_YR_MO_SK,
       cs.LAST_UPDT_RUN_CYC_EXCTN_SK,
       cd.TRGT_CD
FROM {IDSOwner}.MBR_RISK_MESR cs,
     {IDSOwner}.CD_MPPNG cd
WHERE cs.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
  AND cd.TRGT_CD = 'MEDINSIGHTS'
  AND cs.LAST_UPDT_RUN_CYC_EXCTN_SK >= {COBALTTALONRunCycle}
"""
df_db2_COBALTTALON_MbrRisk_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_COBALTTALON_MbrRisk_in)
    .load()
)

df_fnl_Drvr = (
    df_db2_IDEA_MbrRisk_in.select("SRC_SYS_CD_SK","MBR_UNIQ_KEY","RISK_CAT_ID","PRCS_YR_MO_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","TRGT_CD")
    .unionByName(df_db2_Alineo_MbrRisk_in.select("SRC_SYS_CD_SK","MBR_UNIQ_KEY","RISK_CAT_ID","PRCS_YR_MO_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","TRGT_CD"))
    .unionByName(df_db2_IHM_MbrRisk_in.select("SRC_SYS_CD_SK","MBR_UNIQ_KEY","RISK_CAT_ID","PRCS_YR_MO_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","TRGT_CD"))
    .unionByName(df_db2_McSource_MbrRisk_in.select("SRC_SYS_CD_SK","MBR_UNIQ_KEY","RISK_CAT_ID","PRCS_YR_MO_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","TRGT_CD"))
    .unionByName(df_db2_ImpPro_MbrRisk_in.select("SRC_SYS_CD_SK","MBR_UNIQ_KEY","RISK_CAT_ID","PRCS_YR_MO_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","TRGT_CD"))
    .unionByName(df_db2_TREO_MbrRisk_in.select("SRC_SYS_CD_SK","MBR_UNIQ_KEY","RISK_CAT_ID","PRCS_YR_MO_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","TRGT_CD"))
    .unionByName(df_db2_BCBSKC_MbrRisk_in.select("SRC_SYS_CD_SK","MBR_UNIQ_KEY","RISK_CAT_ID","PRCS_YR_MO_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","TRGT_CD"))
    .unionByName(df_db2_COBALTTALON_MbrRisk_in.select("SRC_SYS_CD_SK","MBR_UNIQ_KEY","RISK_CAT_ID","PRCS_YR_MO_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","TRGT_CD"))
)

df_xfm_BusinessLogic = (
    df_fnl_Drvr
    .withColumn(
        "SRC_SYS_CD",
        F.when(trim(F.col("TRGT_CD")) == '', F.lit("UNK")).otherwise(F.col("TRGT_CD"))
    )
    .withColumn(
        "PRCS_YR_MO_SK",
        F.rpad(F.col("PRCS_YR_MO_SK"), 6, " ")
    )
    .select(
        F.col("SRC_SYS_CD_SK"),
        F.col("MBR_UNIQ_KEY"),
        F.col("RISK_CAT_ID"),
        F.col("PRCS_YR_MO_SK"),
        F.col("SRC_SYS_CD"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

df_final = df_xfm_BusinessLogic.select(
    "SRC_SYS_CD_SK",
    "MBR_UNIQ_KEY",
    "RISK_CAT_ID",
    "PRCS_YR_MO_SK",
    "SRC_SYS_CD",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

write_files(
    df_final,
    f"{adls_path}/load/W_MBR_RISK_DRVR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)