# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY        
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                     Extracts from IDS SUB_LVL_AGNT and loads the DataMart table MBRSH_DM_SUB_LVL_AGNT
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #       Change Description                                                  Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------           --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam        2010-10-28         3346                      Original Programming                                                IntegrateNewDevl          Steph Goddard          11/04/2010
# MAGIC 
# MAGIC Nagesh Bandi         2013-07-25          #5114                  Original Programming(Server to Parallel)                    IntegrateWhseDevl        Peter Marshall            10/22/2013

# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC SUB_LVL_AGNT_ROLE_TYP_CD_SK
# MAGIC SUB_SK
# MAGIC GRP_SK
# MAGIC Write SUB_LVL_AGNT Data into a Sequential file for Load Job IdsDmMbrshDmSubLvlAgntLoad.
# MAGIC Read all the Data from IDS SUB_LVL_AGNT Table.
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: IdsDmMbrshDmSubLvlAgntExtr
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
CurrRunCycle = get_widget_value('CurrRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_SUB_LVL_AGNT_Extr = f"""
SELECT
  SUB_LVL_AGNT.AGNT_ID,
  SUB_LVL_AGNT.CLS_PLN_ID,
  SUB_LVL_AGNT.SUB_UNIQ_KEY,
  COALESCE(CD.TRGT_CD,'UNK') AS SRC_SYS_CD,
  SUB_LVL_AGNT.EFF_DT_SK,
  SUB_LVL_AGNT.SUB_LVL_AGNT_ROLE_TYP_CD_SK,
  SUB_LVL_AGNT.TERM_DT_SK,
  SUB_LVL_AGNT.GRP_SK,
  SUB_LVL_AGNT.SUB_SK
FROM {IDSOwner}.SUB_LVL_AGNT SUB_LVL_AGNT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
  ON SUB_LVL_AGNT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE SUB_LVL_AGNT_SK NOT IN (0,1)
"""

df_db2_SUB_LVL_AGNT_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_SUB_LVL_AGNT_Extr)
    .load()
)

extract_query_db2_CD_MPPNG_in = f"""
SELECT
  CD_MPPNG_SK,
  COALESCE(TRGT_CD,'UNK') AS TRGT_CD,
  COALESCE(TRGT_CD_NM,'UNK') AS TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

extract_query_Grp_Extr = f"""
SELECT
  GRP_SK,
  GRP_ID
FROM {IDSOwner}.GRP
"""

df_Grp_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Grp_Extr)
    .load()
)

extract_query_Sub_Extr = f"""
SELECT
  SUB_SK,
  SUB_ID
FROM {IDSOwner}.SUB
"""

df_Sub_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Sub_Extr)
    .load()
)

df_lkp_Codes = (
    df_db2_SUB_LVL_AGNT_Extr.alias("lnk_IdsDmMbrshDmSubLvlAgntExtr_InABC")
    .join(
        df_db2_CD_MPPNG_in.alias("Sub_Lvl_Agnt_Role_Typ_Cd_lkp"),
        F.col("lnk_IdsDmMbrshDmSubLvlAgntExtr_InABC.SUB_LVL_AGNT_ROLE_TYP_CD_SK")
        == F.col("Sub_Lvl_Agnt_Role_Typ_Cd_lkp.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Grp_Extr.alias("Grp_Id_lkp"),
        F.col("lnk_IdsDmMbrshDmSubLvlAgntExtr_InABC.GRP_SK") == F.col("Grp_Id_lkp.GRP_SK"),
        "left",
    )
    .join(
        df_Sub_Extr.alias("Sub_Id_Extr"),
        F.col("lnk_IdsDmMbrshDmSubLvlAgntExtr_InABC.SUB_SK") == F.col("Sub_Id_Extr.SUB_SK"),
        "left",
    )
    .select(
        F.col("lnk_IdsDmMbrshDmSubLvlAgntExtr_InABC.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("lnk_IdsDmMbrshDmSubLvlAgntExtr_InABC.CLS_PLN_ID").alias("CLS_PLN_ID"),
        F.col("lnk_IdsDmMbrshDmSubLvlAgntExtr_InABC.AGNT_ID").alias("AGNT_ID"),
        F.col("Sub_Lvl_Agnt_Role_Typ_Cd_lkp.TRGT_CD").alias("SUB_LVL_AGNT_ROLE_TYP_CD"),
        F.col("lnk_IdsDmMbrshDmSubLvlAgntExtr_InABC.EFF_DT_SK").alias("EFF_DT"),
        F.col("lnk_IdsDmMbrshDmSubLvlAgntExtr_InABC.TERM_DT_SK").alias("TERM_DT"),
        F.col("lnk_IdsDmMbrshDmSubLvlAgntExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Grp_Id_lkp.GRP_ID").alias("GRP_ID"),
        F.col("Sub_Id_Extr.SUB_ID").alias("SUB_ID"),
    )
)

df_xfrm_BusinessLogic = (
    df_lkp_Codes
    .withColumn(
        "SUB_LVL_AGNT_ROLE_TYP_CD",
        F.when(
            F.trim(F.col("SUB_LVL_AGNT_ROLE_TYP_CD")) == "",
            F.lit("UNK")
        ).otherwise(F.col("SUB_LVL_AGNT_ROLE_TYP_CD"))
    )
    .withColumn(
        "EFF_DT",
        F.to_timestamp(
            F.concat(F.col("EFF_DT"), F.lit(" 00:00:00.000")),
            "yyyy-MM-dd HH:mm:ss.SSS"
        )
    )
    .withColumn(
        "TERM_DT",
        F.to_timestamp(
            F.concat(F.col("TERM_DT"), F.lit(" 00:00:00.000")),
            "yyyy-MM-dd HH:mm:ss.SSS"
        )
    )
    .withColumn(
        "SRC_SYS_CD",
        F.when(
            F.trim(F.col("SRC_SYS_CD")) == "",
            F.lit("UNK")
        ).otherwise(F.col("SRC_SYS_CD"))
    )
    .withColumn(
        "GRP_ID",
        F.when(
            F.trim(F.col("GRP_ID")) == "",
            F.lit("UNK")
        ).otherwise(F.col("GRP_ID"))
    )
    .withColumn(
        "SUB_ID",
        F.when(
            F.trim(F.col("SUB_ID")) == "",
            F.lit("UNK")
        ).otherwise(F.col("SUB_ID"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_NO", F.lit(CurrRunCycle))
    .withColumn("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(IDSRunCycle))
)

df_final = (
    df_xfrm_BusinessLogic
    .select(
        F.col("SUB_UNIQ_KEY"),
        F.col("CLS_PLN_ID"),
        F.col("AGNT_ID"),
        F.col("SUB_LVL_AGNT_ROLE_TYP_CD"),
        F.rpad(F.date_format(F.col("EFF_DT"), "yyyy-MM-dd"), 10, " ").alias("EFF_DT"),
        F.rpad(F.date_format(F.col("TERM_DT"), "yyyy-MM-dd"), 10, " ").alias("TERM_DT"),
        F.col("SRC_SYS_CD"),
        F.col("GRP_ID"),
        F.col("SUB_ID"),
        F.col("LAST_UPDT_RUN_CYC_NO"),
        F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    )
)

write_files(
    df_final,
    f"{adls_path}/load/MBRSH_DM_SUB_LVL_AGNT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)