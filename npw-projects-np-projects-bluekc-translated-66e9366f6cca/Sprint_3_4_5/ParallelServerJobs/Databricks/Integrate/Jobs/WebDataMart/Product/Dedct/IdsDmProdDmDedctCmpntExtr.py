# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
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
# MAGIC Ralph Tucker                  2008-10-27       Labor Accts - 3648                New Etl                                                                              devlIDSnew                             Steph Goddard                      11/07/2008                
# MAGIC   
# MAGIC Srikanth Mettpalli             06/06/2013          5114                                 Original Programming(Server to Parallel)                             IntegrateWrhsDevl                   Jag Yelavarthi                        2013-08-11
# MAGIC 
# MAGIC Manasa Andru                 03/26/2014        TFS - 3990                 Enhanced SQL Logic in the db2_DEDCT_CMPNT_in step.      IntegrateNewDevl                     Kalyan Neelam                        2014-04-01

# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC DEDCT_CMPNT_ACCUM_PERD_CD_SK
# MAGIC DEDCT_CMPNT_OOP_USAGE_CD_SK
# MAGIC DEDCT_CMPNT_TYP_CD_SK
# MAGIC Write PROD_DM_DEDCT_CMPNT Data into a Sequential file for Load Job IdsDmProdDmDedctCmpntLoad.
# MAGIC Read all the Data from IDS DEDCT_CMPNT Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsDmProdDmDedctCmpntExtr
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
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','100')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_db2_DEDCT_CMPNT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT\nDEDCT_CMPNT_SK,\nCOALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,\nDEDCT_CMPNT_ID,\nACCUM_NO,\nDEDCT_CMPNT_ACCUM_PERD_CD_SK,\nDEDCT_CMPNT_OOP_USAGE_CD_SK,\nDEDCT_CMPNT_TYP_CD_SK,\nSTOP_LOSS_IN,\nFMLY_DEDCT_AMT,\nFMLY_DEDCT_CAROVR_AMT,\nMBR_DEDCT_AMT,\nMBR_DEDCT_CAROVR_AMT,\nFMLY_DEDCT_CAROVR_PRSN_CT,\nFMLY_DEDCT_PRSN_CT,\nREL_ACCUM_NO,\nDEDCT_CMPNT_DESC\nFROM "
        + IDSOwner
        + ".DEDCT_CMPNT D\nLEFT JOIN "
        + IDSOwner
        + ".CD_MPPNG CD\nON D.SRC_SYS_CD_SK = CD.CD_MPPNG_SK\nWHERE\nDEDCT_CMPNT_SK NOT IN (0,1)"
    )
    .load()
)

df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM from "
        + IDSOwner
        + ".CD_MPPNG"
    )
    .load()
)

df_cpy_cd_mppng_ref_dedctcmpntoopusagecdlkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
)

df_cpy_cd_mppng_ref_dedctcmpntaccumperdcdlkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
)

df_cpy_cd_mppng_ref_dedctcmpnttypcdlkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
)

df_lkp_Codes = (
    df_db2_DEDCT_CMPNT_in.alias("lnk_IdsDmProdDmDedctCmpntExtr_InABC")
    .join(
        df_cpy_cd_mppng_ref_dedctcmpntoopusagecdlkup.alias("Ref_DedctCmpntOopUsageCdLkup"),
        F.col("lnk_IdsDmProdDmDedctCmpntExtr_InABC.DEDCT_CMPNT_OOP_USAGE_CD_SK")
        == F.col("Ref_DedctCmpntOopUsageCdLkup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_cpy_cd_mppng_ref_dedctcmpntaccumperdcdlkup.alias("Ref_DedctCmpntAccumPerdCdLkup"),
        F.col("lnk_IdsDmProdDmDedctCmpntExtr_InABC.DEDCT_CMPNT_ACCUM_PERD_CD_SK")
        == F.col("Ref_DedctCmpntAccumPerdCdLkup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_cpy_cd_mppng_ref_dedctcmpnttypcdlkup.alias("Ref_DedctCmpntTypCdLkup"),
        F.col("lnk_IdsDmProdDmDedctCmpntExtr_InABC.DEDCT_CMPNT_TYP_CD_SK")
        == F.col("Ref_DedctCmpntTypCdLkup.CD_MPPNG_SK"),
        "left",
    )
    .select(
        F.col("lnk_IdsDmProdDmDedctCmpntExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsDmProdDmDedctCmpntExtr_InABC.ACCUM_NO").alias("ACCUM_NO"),
        F.col("lnk_IdsDmProdDmDedctCmpntExtr_InABC.DEDCT_CMPNT_ID").alias("DEDCT_CMPNT_ID"),
        F.col("Ref_DedctCmpntAccumPerdCdLkup.TRGT_CD").alias("DEDCT_CMPNT_ACCUM_PERD_CD"),
        F.col("Ref_DedctCmpntAccumPerdCdLkup.TRGT_CD_NM").alias("DEDCT_CMPNT_ACCUM_PERD_NM"),
        F.col("Ref_DedctCmpntOopUsageCdLkup.TRGT_CD").alias("DEDCT_CMPNT_OOP_USAGE_CD"),
        F.col("Ref_DedctCmpntOopUsageCdLkup.TRGT_CD_NM").alias("DEDCT_CMPNT_OOP_USAGE_NM"),
        F.col("Ref_DedctCmpntTypCdLkup.TRGT_CD").alias("DEDCT_CMPNT_TYP_CD"),
        F.col("Ref_DedctCmpntTypCdLkup.TRGT_CD_NM").alias("DEDCT_CMPNT_TYP_NM"),
        F.col("lnk_IdsDmProdDmDedctCmpntExtr_InABC.STOP_LOSS_IN").alias("STOP_LOSS_IN"),
        F.col("lnk_IdsDmProdDmDedctCmpntExtr_InABC.FMLY_DEDCT_AMT").alias("FMLY_DEDCT_AMT"),
        F.col("lnk_IdsDmProdDmDedctCmpntExtr_InABC.FMLY_DEDCT_CAROVR_AMT").alias("FMLY_DEDCT_CAROVR_AMT"),
        F.col("lnk_IdsDmProdDmDedctCmpntExtr_InABC.MBR_DEDCT_AMT").alias("MBR_DEDCT_AMT"),
        F.col("lnk_IdsDmProdDmDedctCmpntExtr_InABC.MBR_DEDCT_CAROVR_AMT").alias("MBR_DEDCT_CAROVR_AMT"),
        F.col("lnk_IdsDmProdDmDedctCmpntExtr_InABC.FMLY_DEDCT_CAROVR_PRSN_CT").alias("FMLY_DEDCT_CAROVR_PRSN_CT"),
        F.col("lnk_IdsDmProdDmDedctCmpntExtr_InABC.FMLY_DEDCT_PRSN_CT").alias("FMLY_DEDCT_PRSN_CT"),
        F.col("lnk_IdsDmProdDmDedctCmpntExtr_InABC.REL_ACCUM_NO").alias("REL_ACCUM_NO"),
        F.col("lnk_IdsDmProdDmDedctCmpntExtr_InABC.DEDCT_CMPNT_DESC").alias("DEDCT_CMPNT_DESC"),
    )
)

df_xfrm_BusinessLogic = df_lkp_Codes.select(
    F.when(trim(F.col("SRC_SYS_CD")) == "", "UNK").otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("ACCUM_NO").alias("ACCUM_NO"),
    F.col("DEDCT_CMPNT_ID").alias("DEDCT_CMPNT_ID"),
    F.when(trim(F.col("DEDCT_CMPNT_ACCUM_PERD_CD")) == "", "").otherwise(F.col("DEDCT_CMPNT_ACCUM_PERD_CD")).alias("DEDCT_CMPNT_ACCUM_PERD_CD"),
    F.when(trim(F.col("DEDCT_CMPNT_ACCUM_PERD_NM")) == "", "").otherwise(F.col("DEDCT_CMPNT_ACCUM_PERD_NM")).alias("DEDCT_CMPNT_ACCUM_PERD_NM"),
    F.when(trim(F.col("DEDCT_CMPNT_OOP_USAGE_CD")) == "", "").otherwise(F.col("DEDCT_CMPNT_OOP_USAGE_CD")).alias("DEDCT_CMPNT_OOP_USAGE_CD"),
    F.when(trim(F.col("DEDCT_CMPNT_OOP_USAGE_NM")) == "", "").otherwise(F.col("DEDCT_CMPNT_OOP_USAGE_NM")).alias("DEDCT_CMPNT_OOP_USAGE_NM"),
    F.when(trim(F.col("DEDCT_CMPNT_TYP_CD")) == "", "").otherwise(F.col("DEDCT_CMPNT_TYP_CD")).alias("DEDCT_CMPNT_TYP_CD"),
    F.when(trim(F.col("DEDCT_CMPNT_TYP_NM")) == "", "").otherwise(F.col("DEDCT_CMPNT_TYP_NM")).alias("DEDCT_CMPNT_TYP_NM"),
    F.col("STOP_LOSS_IN").alias("STOP_LOSS_IN"),
    F.col("FMLY_DEDCT_AMT").alias("FMLY_DEDCT_AMT"),
    F.col("FMLY_DEDCT_CAROVR_AMT").alias("FMLY_DEDCT_CAROVR_AMT"),
    F.col("MBR_DEDCT_AMT").alias("MBR_DEDCT_AMT"),
    F.col("MBR_DEDCT_CAROVR_AMT").alias("MBR_DEDCT_CAROVR_AMT"),
    F.col("FMLY_DEDCT_CAROVR_PRSN_CT").alias("FMLY_DEDCT_CAROVR_PRSN_CT"),
    F.col("FMLY_DEDCT_PRSN_CT").alias("FMLY_DEDCT_PRSN_CT"),
    F.col("REL_ACCUM_NO").alias("REL_ACCUM_NO"),
    F.col("DEDCT_CMPNT_DESC").alias("DEDCT_CMPNT_DESC"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_NO"),
)

df_final = df_xfrm_BusinessLogic.withColumn(
    "STOP_LOSS_IN", F.rpad(F.col("STOP_LOSS_IN"), 1, " ")
).select(
    "SRC_SYS_CD",
    "ACCUM_NO",
    "DEDCT_CMPNT_ID",
    "DEDCT_CMPNT_ACCUM_PERD_CD",
    "DEDCT_CMPNT_ACCUM_PERD_NM",
    "DEDCT_CMPNT_OOP_USAGE_CD",
    "DEDCT_CMPNT_OOP_USAGE_NM",
    "DEDCT_CMPNT_TYP_CD",
    "DEDCT_CMPNT_TYP_NM",
    "STOP_LOSS_IN",
    "FMLY_DEDCT_AMT",
    "FMLY_DEDCT_CAROVR_AMT",
    "MBR_DEDCT_AMT",
    "MBR_DEDCT_CAROVR_AMT",
    "FMLY_DEDCT_CAROVR_PRSN_CT",
    "FMLY_DEDCT_PRSN_CT",
    "REL_ACCUM_NO",
    "DEDCT_CMPNT_DESC",
    "LAST_UPDT_RUN_CYC_NO",
)

write_files(
    df_final,
    f"{adls_path}/load/PROD_DM_DEDCT_CMPNT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)