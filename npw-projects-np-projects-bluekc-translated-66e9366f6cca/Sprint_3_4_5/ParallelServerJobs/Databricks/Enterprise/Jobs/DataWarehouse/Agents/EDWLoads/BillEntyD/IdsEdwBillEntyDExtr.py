# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                     PROJECT                REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC               Ralph Tucker - 12/16/2005  -  New Program
# MAGIC 
# MAGIC 
# MAGIC Archana Palivela             11/10/2013        5114                           Originally Programmed (In Parallel)                                                EnterpriseWhseDevl         Jag Yelavarthi            2013-12-24

# MAGIC Job name: IdsEdwEvtApptDExtr 
# MAGIC 
# MAGIC 
# MAGIC Job Extract data from IDS table and apply Fkey and Code denormalization, create a load ready file.
# MAGIC Read data from source table BILL_ENTY
# MAGIC Write BILL_ENTY_D Data into a Sequential file for Load Ready Job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameter parsing
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
IdsRunCycle = get_widget_value('IdsRunCycle','')

# Configure JDBC connection
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Stage: db2_BILL_ENTY_Extr
extract_query_db2_BILL_ENTY_Extr = """SELECT
BILL_ENTY.BILL_ENTY_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
BILL_ENTY.BILL_ENTY_UNIQ_KEY,
BILL_ENTY.GRP_SK,
BILL_ENTY.SUBGRP_SK,
BILL_ENTY.SUB_SK,
BILL_ENTY.BILL_ENTY_LVL_CD_SK,
BILL_ENTY.BILL_ENTY_LVL_UNIQ_KEY
FROM {IDSOwner}.BILL_ENTY BILL_ENTY
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON BILL_ENTY.SRC_SYS_CD_SK = CD.CD_MPPNG_SK""".format(IDSOwner=IDSOwner)

df_db2_BILL_ENTY_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_BILL_ENTY_Extr)
    .load()
)

# Stage: db2_GRP_Extr
extract_query_db2_GRP_Extr = "SELECT GRP.GRP_SK,GRP.GRP_UNIQ_KEY FROM {IDSOwner}.GRP GRP".format(IDSOwner=IDSOwner)
df_db2_GRP_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_GRP_Extr)
    .load()
)

# Stage: db2_SUBGRP_Extr
extract_query_db2_SUBGRP_Extr = """SELECT
SUBGRP.SUBGRP_SK,
SUBGRP.SUBGRP_UNIQ_KEY
FROM {IDSOwner}.SUBGRP SUBGRP""".format(IDSOwner=IDSOwner)
df_db2_SUBGRP_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_SUBGRP_Extr)
    .load()
)

# Stage: db2_SUB_Extr
extract_query_db2_SUB_Extr = """SELECT
SUB.SUB_SK,
SUB.SUB_UNIQ_KEY
FROM {IDSOwner}.SUB SUB""".format(IDSOwner=IDSOwner)
df_db2_SUB_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_SUB_Extr)
    .load()
)

# Stage: db2_CD_MPPNG_Extr
extract_query_db2_CD_MPPNG_Extr = """SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG""".format(IDSOwner=IDSOwner)
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

# Stage: lkp_Codes (PxLookup)
df_lkp_Codes = (
    df_db2_BILL_ENTY_Extr.alias("Ink_IdsEdwBillEntyDExtr_InABC")
    .join(
        df_db2_GRP_Extr.alias("Lnk_Grp_Out"),
        F.col("Ink_IdsEdwBillEntyDExtr_InABC.GRP_SK") == F.col("Lnk_Grp_Out.GRP_SK"),
        "left",
    )
    .join(
        df_db2_CD_MPPNG_Extr.alias("Lnk_CdMppng_Out"),
        F.col("Ink_IdsEdwBillEntyDExtr_InABC.BILL_ENTY_LVL_CD_SK")
        == F.col("Lnk_CdMppng_Out.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_db2_SUB_Extr.alias("Lnk_Sub_Out"),
        F.col("Ink_IdsEdwBillEntyDExtr_InABC.SUB_SK") == F.col("Lnk_Sub_Out.SUB_SK"),
        "left",
    )
    .join(
        df_db2_SUBGRP_Extr.alias("Lnk_SubGrp_Out"),
        F.col("Ink_IdsEdwBillEntyDExtr_InABC.SUBGRP_SK")
        == F.col("Lnk_SubGrp_Out.SUBGRP_SK"),
        "left",
    )
    .select(
        F.col("Ink_IdsEdwBillEntyDExtr_InABC.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
        F.col("Ink_IdsEdwBillEntyDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Ink_IdsEdwBillEntyDExtr_InABC.BILL_ENTY_UNIQ_KEY").alias(
            "BILL_ENTY_UNIQ_KEY"
        ),
        F.col("Lnk_CdMppng_Out.TRGT_CD").alias("BILL_ENTY_LVL_CD"),
        F.col("Lnk_CdMppng_Out.TRGT_CD_NM").alias("BILL_ENTY_LVL_NM"),
        F.col("Ink_IdsEdwBillEntyDExtr_InABC.BILL_ENTY_LVL_UNIQ_KEY").alias(
            "BILL_ENTY_LVL_UNIQ_KEY"
        ),
        F.col("Lnk_Grp_Out.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
        F.col("Lnk_SubGrp_Out.SUBGRP_UNIQ_KEY").alias("SUBGRP_UNIQ_KEY"),
        F.col("Lnk_Sub_Out.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("Ink_IdsEdwBillEntyDExtr_InABC.BILL_ENTY_LVL_CD_SK").alias(
            "BILL_ENTY_LVL_CD_SK"
        ),
        F.col("Ink_IdsEdwBillEntyDExtr_InABC.GRP_SK").alias("GRP_SK"),
        F.col("Ink_IdsEdwBillEntyDExtr_InABC.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("Ink_IdsEdwBillEntyDExtr_InABC.SUB_SK").alias("SUB_SK"),
    )
)

# Stage: xmf_businessLogic (CTransformerStage)
# Link: Lnk_Main_Out -> constraint = (BILL_ENTY_SK <> 0 AND BILL_ENTY_SK <> 1)
df_lnk_Main_Out = (
    df_lkp_Codes.filter((F.col("BILL_ENTY_SK") != 0) & (F.col("BILL_ENTY_SK") != 1))
    .select(
        F.col("BILL_ENTY_SK").alias("BILL_ENTY_SK"),
        F.when(F.trim(F.col("SRC_SYS_CD")) == "", F.lit("UNK"))
        .otherwise(F.col("SRC_SYS_CD"))
        .alias("SRC_SYS_CD"),
        F.col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
        F.rpad(F.lit(EDWRunCycleDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.rpad(F.lit(EDWRunCycleDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.when(F.trim(F.col("BILL_ENTY_LVL_CD")) == "", F.lit("UNK"))
        .otherwise(F.col("BILL_ENTY_LVL_CD"))
        .alias("BILL_ENTY_LVL_CD"),
        F.when(F.trim(F.col("BILL_ENTY_LVL_NM")) == "", F.lit("UNK"))
        .otherwise(F.col("BILL_ENTY_LVL_NM"))
        .alias("BILL_ENTY_LVL_NM"),
        F.col("BILL_ENTY_LVL_UNIQ_KEY").alias("BILL_ENTY_LVL_UNIQ_KEY"),
        F.when(F.col("GRP_UNIQ_KEY").isNull(), F.lit(0))
        .otherwise(F.col("GRP_UNIQ_KEY"))
        .alias("GRP_UNIQ_KEY"),
        F.when(F.col("SUBGRP_UNIQ_KEY").isNull(), F.lit(0))
        .otherwise(F.col("SUBGRP_UNIQ_KEY"))
        .alias("SUBGRP_UNIQ_KEY"),
        F.when(F.col("SUB_UNIQ_KEY").isNull(), F.lit(0))
        .otherwise(F.col("SUB_UNIQ_KEY"))
        .alias("SUB_UNIQ_KEY"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("BILL_ENTY_LVL_CD_SK").alias("BILL_ENTY_LVL_CD_SK"),
        F.col("GRP_SK").alias("GRP_SK"),
        F.col("SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("SUB_SK").alias("SUB_SK"),
    )
)

# Link: Lnk_UNK_Out -> single row with specific constants
df_lnk_UNK_Out = spark.createDataFrame(
    [
        (
            0,         # BILL_ENTY_SK
            "UNK",     # SRC_SYS_CD
            0,         # BILL_ENTY_UNIQ_KEY
            "1753-01-01",  # CRT_RUN_CYC_EXCTN_DT_SK
            "1753-01-01",  # LAST_UPDT_RUN_CYC_EXCTN_DT_SK
            "UNK",     # BILL_ENTY_LVL_CD
            "UNK",     # BILL_ENTY_LVL_NM
            0,         # BILL_ENTY_LVL_UNIQ_KEY
            0,         # GRP_UNIQ_KEY
            0,         # SUBGRP_UNIQ_KEY
            0,         # SUB_UNIQ_KEY
            100,       # CRT_RUN_CYC_EXCTN_SK
            100,       # LAST_UPDT_RUN_CYC_EXCTN_SK
            0,         # BILL_ENTY_LVL_CD_SK
            0,         # GRP_SK
            0,         # SUBGRP_SK
            0,         # SUB_SK
        )
    ],
    [
        "BILL_ENTY_SK",
        "SRC_SYS_CD",
        "BILL_ENTY_UNIQ_KEY",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "BILL_ENTY_LVL_CD",
        "BILL_ENTY_LVL_NM",
        "BILL_ENTY_LVL_UNIQ_KEY",
        "GRP_UNIQ_KEY",
        "SUBGRP_UNIQ_KEY",
        "SUB_UNIQ_KEY",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "BILL_ENTY_LVL_CD_SK",
        "GRP_SK",
        "SUBGRP_SK",
        "SUB_SK",
    ],
)
df_lnk_UNK_Out = df_lnk_UNK_Out.select(
    F.col("BILL_ENTY_SK"),
    F.col("SRC_SYS_CD"),
    F.col("BILL_ENTY_UNIQ_KEY"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias(
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK"
    ),
    F.col("BILL_ENTY_LVL_CD"),
    F.col("BILL_ENTY_LVL_NM"),
    F.col("BILL_ENTY_LVL_UNIQ_KEY"),
    F.col("GRP_UNIQ_KEY"),
    F.col("SUBGRP_UNIQ_KEY"),
    F.col("SUB_UNIQ_KEY"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BILL_ENTY_LVL_CD_SK"),
    F.col("GRP_SK"),
    F.col("SUBGRP_SK"),
    F.col("SUB_SK"),
)

# Link: Lnk_NA_Out -> single row with specific constants
df_lnk_NA_Out = spark.createDataFrame(
    [
        (
            1,         # BILL_ENTY_SK
            "NA",      # SRC_SYS_CD
            1,         # BILL_ENTY_UNIQ_KEY
            "1753-01-01",  # CRT_RUN_CYC_EXCTN_DT_SK
            "1753-01-01",  # LAST_UPDT_RUN_CYC_EXCTN_DT_SK
            "NA",      # BILL_ENTY_LVL_CD
            "NA",      # BILL_ENTY_LVL_NM
            1,         # BILL_ENTY_LVL_UNIQ_KEY
            1,         # GRP_UNIQ_KEY
            1,         # SUBGRP_UNIQ_KEY
            1,         # SUB_UNIQ_KEY
            100,       # CRT_RUN_CYC_EXCTN_SK
            100,       # LAST_UPDT_RUN_CYC_EXCTN_SK
            1,         # BILL_ENTY_LVL_CD_SK
            1,         # GRP_SK
            1,         # SUBGRP_SK
            1,         # SUB_SK
        )
    ],
    [
        "BILL_ENTY_SK",
        "SRC_SYS_CD",
        "BILL_ENTY_UNIQ_KEY",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "BILL_ENTY_LVL_CD",
        "BILL_ENTY_LVL_NM",
        "BILL_ENTY_LVL_UNIQ_KEY",
        "GRP_UNIQ_KEY",
        "SUBGRP_UNIQ_KEY",
        "SUB_UNIQ_KEY",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "BILL_ENTY_LVL_CD_SK",
        "GRP_SK",
        "SUBGRP_SK",
        "SUB_SK",
    ],
)
df_lnk_NA_Out = df_lnk_NA_Out.select(
    F.col("BILL_ENTY_SK"),
    F.col("SRC_SYS_CD"),
    F.col("BILL_ENTY_UNIQ_KEY"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias(
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK"
    ),
    F.col("BILL_ENTY_LVL_CD"),
    F.col("BILL_ENTY_LVL_NM"),
    F.col("BILL_ENTY_LVL_UNIQ_KEY"),
    F.col("GRP_UNIQ_KEY"),
    F.col("SUBGRP_UNIQ_KEY"),
    F.col("SUB_UNIQ_KEY"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BILL_ENTY_LVL_CD_SK"),
    F.col("GRP_SK"),
    F.col("SUBGRP_SK"),
    F.col("SUB_SK"),
)

# Stage: Fnl_Main (PxFunnel) -> union these three outputs
df_Fnl_Main = (
    df_lnk_Main_Out.unionByName(df_lnk_UNK_Out)
    .unionByName(df_lnk_NA_Out)
    .select(
        "BILL_ENTY_SK",
        "SRC_SYS_CD",
        "BILL_ENTY_UNIQ_KEY",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "BILL_ENTY_LVL_CD",
        "BILL_ENTY_LVL_NM",
        "BILL_ENTY_LVL_UNIQ_KEY",
        "GRP_UNIQ_KEY",
        "SUBGRP_UNIQ_KEY",
        "SUB_UNIQ_KEY",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "BILL_ENTY_LVL_CD_SK",
        "GRP_SK",
        "SUBGRP_SK",
        "SUB_SK",
    )
)

# Stage: Seq_BILL_ENTY_D_Load (PxSequentialFile) -> write to ADLS
write_files(
    df_Fnl_Main,
    f"{adls_path}/load/BILL_ENTY_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None,
)