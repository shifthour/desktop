# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   IDS - read all records from IDS SUB_LVL_AGNT, supply the Source System Code and Agent Role from CDMA
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date               Project/Altiris #         Change Description                                                                                                    Development Project    Code Reviewer            Date Reviewed
# MAGIC ---------------------------      -------------------    -----------------------------------         -------------------------------------------------------------------------------------------                                      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Sharon Andrew         08/10/2005-                                              Originally Programmed
# MAGIC Sharon Andrew         12/12/2005-                                              Changed outfile file name from SUB_LVL_AGNT_I.dat   to    SUB_LVL_AGNT_HIST_I.dat
# MAGIC Suzanne Saylor         04/12/2006 -                                             changed > #BeginCycle# to >=, added RunCycle parameter, set LAST_RUN_CYC_EXCTN_SK to RunCycle
# MAGIC Sharon Anderw         07/20/2007                                                Per BlueJira 134 - Added GroupSK to the table - also table EDW SUB_LVL_AGNT_I    
# MAGIC                                                                                                     Took out EDW Parameters.   This implies sequencer change.
# MAGIC Pooja Sunkara          07/18/2013        5114                               Converted job from server to parallel version.                                                            EnterpriseWrhsDevl      Peter Marshall              9/4/2013

# MAGIC Read data from source table SUB_LVL_AGNT_I.
# MAGIC Extracts all data from IDS reference table CD_MPPNG.
# MAGIC 
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC GRP_REL_ENTY_TERM_RSN_CD_SK
# MAGIC GRP_REL_ENTY_TYP_CD_SK
# MAGIC GRP_REL_ENTY_CAT_CD_SK
# MAGIC SRC_SYS_CD_SK
# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK and LAST_UPDT_RUN_CYC_EXCTN_DT_SK
# MAGIC Write SUB_LVL_AGNT_I Data into a Sequential file for Load Ready Job.
# MAGIC Job name:
# MAGIC IdsEdwSubLvlAgntIExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_db2_SUB_LVL_AGNT_I_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT SUB_LVL_AGNT_SK,AGNT_ID,CLS_PLN_ID,SUB_UNIQ_KEY,SRC_SYS_CD_SK,EFF_DT_SK,SUB_LVL_AGNT_ROLE_TYP_CD_SK,TERM_DT_SK,CRT_RUN_CYC_EXCTN_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,AGNT_SK,CLS_PLN_SK,SUB_SK,GRP_SK FROM {IDSOwner}.SUB_LVL_AGNT SUB_LVL_AGNT"
    )
    .load()
)

df_db2_CD_MPPNG1_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM FROM {IDSOwner}.CD_MPPNG"
    )
    .load()
)

df_Copy_CdMppng_ref_AgentRole = df_db2_CD_MPPNG1_in.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Copy_CdMppng_ref_SrcSysCd = df_db2_CD_MPPNG1_in.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_CdmaCodes = (
    df_db2_SUB_LVL_AGNT_I_in.alias("Ink_IdsEdwSubLvlAgntIExtr_InABC")
    .join(
        df_Copy_CdMppng_ref_AgentRole.alias("ref_AgentRole"),
        col("Ink_IdsEdwSubLvlAgntIExtr_InABC.SUB_LVL_AGNT_ROLE_TYP_CD_SK")
        == col("ref_AgentRole.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Copy_CdMppng_ref_SrcSysCd.alias("ref_SrcSysCd"),
        col("Ink_IdsEdwSubLvlAgntIExtr_InABC.SRC_SYS_CD_SK") == col("ref_SrcSysCd.CD_MPPNG_SK"),
        "left",
    )
)

df_lnk_CodesLkpData_out = df_lkp_CdmaCodes.select(
    col("Ink_IdsEdwSubLvlAgntIExtr_InABC.SUB_LVL_AGNT_SK").alias("SUB_LVL_AGNT_SK"),
    col("Ink_IdsEdwSubLvlAgntIExtr_InABC.AGNT_ID").alias("AGNT_ID"),
    col("Ink_IdsEdwSubLvlAgntIExtr_InABC.CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("Ink_IdsEdwSubLvlAgntIExtr_InABC.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("Ink_IdsEdwSubLvlAgntIExtr_InABC.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("Ink_IdsEdwSubLvlAgntIExtr_InABC.EFF_DT_SK").alias("EFF_DT_SK"),
    col("Ink_IdsEdwSubLvlAgntIExtr_InABC.SUB_LVL_AGNT_ROLE_TYP_CD_SK").alias("SUB_LVL_AGNT_ROLE_TYP_CD_SK"),
    col("Ink_IdsEdwSubLvlAgntIExtr_InABC.TERM_DT_SK").alias("TERM_DT_SK"),
    col("Ink_IdsEdwSubLvlAgntIExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Ink_IdsEdwSubLvlAgntIExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Ink_IdsEdwSubLvlAgntIExtr_InABC.AGNT_SK").alias("AGNT_SK"),
    col("Ink_IdsEdwSubLvlAgntIExtr_InABC.CLS_PLN_SK").alias("CLS_PLN_SK"),
    col("Ink_IdsEdwSubLvlAgntIExtr_InABC.GRP_SK").alias("GRP_SK"),
    col("Ink_IdsEdwSubLvlAgntIExtr_InABC.SUB_SK").alias("SUB_SK"),
    col("ref_SrcSysCd.TRGT_CD").alias("SRC_SYS_CD"),
    col("ref_AgentRole.TRGT_CD").alias("SUB_LVL_AGNT_ROLE_TYP_CD"),
    col("ref_AgentRole.TRGT_CD_NM").alias("SUB_LVL_AGNT_ROLE_TYP_NM")
)

df_xfrm_businessLogic = (
    df_lnk_CodesLkpData_out.withColumn(
        "SRC_SYS_CD",
        when(
            (col("SRC_SYS_CD").isNull()) | (trim(col("SRC_SYS_CD")) == ""),
            lit("UNK"),
        ).otherwise(col("SRC_SYS_CD")),
    )
    .withColumn(
        "SUB_LVL_AGNT_ROLE_TYP_CD",
        when(
            (col("SUB_LVL_AGNT_ROLE_TYP_CD").isNull())
            | (trim(col("SUB_LVL_AGNT_ROLE_TYP_CD")) == ""),
            lit("UNK"),
        ).otherwise(col("SUB_LVL_AGNT_ROLE_TYP_CD")),
    )
    .withColumn(
        "SUB_LVL_AGNT_ROLE_TYP_NM",
        when(
            (col("SUB_LVL_AGNT_ROLE_TYP_NM").isNull())
            | (trim(col("SUB_LVL_AGNT_ROLE_TYP_NM")) == ""),
            lit("UNK"),
        ).otherwise(col("SUB_LVL_AGNT_ROLE_TYP_NM")),
    )
    .withColumnRenamed("EFF_DT_SK", "SUB_LVL_AGNT_EFF_DT_SK")
    .withColumnRenamed("TERM_DT_SK", "SUB_LVL_AGNT_TERM_DT_SK")
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
)

df_IdsEdwSubLvlAgntIExtr_OutABC = df_xfrm_businessLogic.select(
    "SUB_LVL_AGNT_SK",
    "SRC_SYS_CD",
    "AGNT_ID",
    "CLS_PLN_ID",
    "SUB_UNIQ_KEY",
    "SUB_LVL_AGNT_ROLE_TYP_CD",
    "SUB_LVL_AGNT_EFF_DT_SK",
    "SUB_LVL_AGNT_TERM_DT_SK",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "AGNT_SK",
    "CLS_PLN_SK",
    "GRP_SK",
    "SUB_SK",
    "SUB_LVL_AGNT_ROLE_TYP_NM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SUB_LVL_AGNT_ROLE_TYP_CD_SK"
)

df_IdsEdwSubLvlAgntIExtr_OutABC = (
    df_IdsEdwSubLvlAgntIExtr_OutABC.withColumn(
        "SUB_LVL_AGNT_EFF_DT_SK",
        rpad(col("SUB_LVL_AGNT_EFF_DT_SK"), 10, " "),
    )
    .withColumn(
        "SUB_LVL_AGNT_TERM_DT_SK",
        rpad(col("SUB_LVL_AGNT_TERM_DT_SK"), 10, " "),
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "),
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "),
    )
)

write_files(
    df_IdsEdwSubLvlAgntIExtr_OutABC,
    f"{adls_path}/load/SUB_LVL_AGNT_I.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)