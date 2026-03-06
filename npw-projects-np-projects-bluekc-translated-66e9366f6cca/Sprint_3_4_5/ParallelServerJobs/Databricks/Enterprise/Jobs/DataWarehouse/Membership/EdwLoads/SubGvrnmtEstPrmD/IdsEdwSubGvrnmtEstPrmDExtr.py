# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2014 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsEdwSubGvrnmtEstPrmDExtr
# MAGIC 
# MAGIC Called By:  EdwMbrNoDriverSeq
# MAGIC 
# MAGIC DESCRIPTION:  Extracts from IDS SUB_GVRNMT_EST_PRM to flatfile SUB_GVRNMT_EST_PRM_D.dat.  Only records with the most recent LAST_UPDT_RUN_CYC_EXCTN_SK are pulled.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC The following IDS table is used:
# MAGIC SUB_GVRNMT_EST_PRM
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC OUTPUTS: 
# MAGIC                     Flatfile: SUB_GVRNMT_EST_PRM_D.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description                                                                              Development Environment       Cope Reviewer               Review Date
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------    -------------------------------------------   -----------------------------------       --------------------
# MAGIC 2014-02-17     Santosh Bokka         Original Programming                                                                              EnterpriseNewDevl                 Bhoomi Dasari                 3/27/2014

# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK and LAST_UPDT_RUN_CYC_EXCTN_DT_SK
# MAGIC Job name:
# MAGIC IdsEdwSubGvrnmtEstPrmDExtr
# MAGIC Pulls from IDS SUB_GVRNMT_EST_PRM to load to file SUB_GVRNMT_EST_PRM_D.dat
# MAGIC Write SUB_GVRNMT_EST_PRM_D Data into a Sequential file for Load Ready Job.
# MAGIC Extract updated SUB_GVRNMT_EST_PRM IDS Data
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate', '')
EDWRunCycle = get_widget_value('EDWRunCycle', '')
IDSRunCycle = get_widget_value('IDSRunCycle', '')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_SUB_GVRNMT_EST_PRM_in = (
    f"SELECT DISTINCT\n"
    f"\tSUB_GVRNMT_EST_PRM_ALIAS.SUB_GVRNMT_EST_PRM_SK,\n"
    f"\tSUB_GVRNMT_EST_PRM_ALIAS.SUB_UNIQ_KEY,\n"
    f"\tSUB_GVRNMT_EST_PRM_ALIAS.CLS_PLN_PROD_CAT_CD,\n"
    f"\tSUB_GVRNMT_EST_PRM_ALIAS.EFF_DT_SK,\n"
    f"\tSUB_GVRNMT_EST_PRM_ALIAS.SRC_SYS_CD_SK,\n"
    f"\tSUB_GVRNMT_EST_PRM_ALIAS.CRT_RUN_CYC_EXCTN_SK,\n"
    f"\tSUB_GVRNMT_EST_PRM_ALIAS.LAST_UPDT_RUN_CYC_EXCTN_SK,\n"
    f"\tSUB_GVRNMT_EST_PRM_ALIAS.SUB_SK,\n"
    f"\tSUB_GVRNMT_EST_PRM_ALIAS.CLS_PLN_PROD_CAT_CD_SK,\n"
    f"\tSUB_GVRNMT_EST_PRM_ALIAS.TERM_DT_SK,\n"
    f"\tSUB_GVRNMT_EST_PRM_ALIAS.EST_PRM_AMT,\n"
    f"COALESCE(CD.TRGT_CD,'UNK') AS SRC_SYS_CD\n"
    f"FROM {IDSOwner}.SUB_GVRNMT_EST_PRM AS SUB_GVRNMT_EST_PRM_ALIAS\n"
    f"LEFT JOIN {IDSOwner}.CD_MPPNG CD\n"
    f"ON SUB_GVRNMT_EST_PRM_ALIAS.SRC_SYS_CD_SK = CD.CD_MPPNG_SK\n"
    f"WHERE SUB_GVRNMT_EST_PRM_ALIAS.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}"
)

df_db2_SUB_GVRNMT_EST_PRM_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_SUB_GVRNMT_EST_PRM_in)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = (
    f"SELECT CD_MPPNG_SK, COALESCE(TRGT_CD,'UNK') TRGT_CD, COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM "
    f"FROM {IDSOwner}.CD_MPPNG"
)

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_lkp_Codes = df_db2_SUB_GVRNMT_EST_PRM_in.alias("lnk_IdsEdwSubGvrnmtEstPrmDExtr_In").join(
    df_db2_CD_MPPNG_Extr.alias("lnk_CdMppng_Out"),
    F.col("lnk_IdsEdwSubGvrnmtEstPrmDExtr_In.CLS_PLN_PROD_CAT_CD_SK") == F.col("lnk_CdMppng_Out.CD_MPPNG_SK"),
    how="left"
)

df_lkp_Codes_sel = df_lkp_Codes.select(
    F.col("lnk_IdsEdwSubGvrnmtEstPrmDExtr_In.SUB_GVRNMT_EST_PRM_SK").alias("SUB_GVRNMT_EST_PRM_SK"),
    F.col("lnk_IdsEdwSubGvrnmtEstPrmDExtr_In.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("lnk_IdsEdwSubGvrnmtEstPrmDExtr_In.CLS_PLN_PROD_CAT_CD").alias("CLS_PLN_PROD_CAT_CD"),
    F.col("lnk_IdsEdwSubGvrnmtEstPrmDExtr_In.EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("lnk_IdsEdwSubGvrnmtEstPrmDExtr_In.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnk_IdsEdwSubGvrnmtEstPrmDExtr_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_IdsEdwSubGvrnmtEstPrmDExtr_In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_IdsEdwSubGvrnmtEstPrmDExtr_In.SUB_SK").alias("SUB_SK"),
    F.col("lnk_IdsEdwSubGvrnmtEstPrmDExtr_In.CLS_PLN_PROD_CAT_CD_SK").alias("CLS_PLN_PROD_CAT_CD_SK"),
    F.col("lnk_IdsEdwSubGvrnmtEstPrmDExtr_In.TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("lnk_IdsEdwSubGvrnmtEstPrmDExtr_In.EST_PRM_AMT").alias("EST_PRM_AMT"),
    F.col("lnk_IdsEdwSubGvrnmtEstPrmDExtr_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_CdMppng_Out.TRGT_CD_NM").alias("CLS_PLN_PROD_CAT_NM")
)

df_lnk_IdsEdwSubGvrnmtEstPrmDExtr_Main = df_lkp_Codes_sel.filter(
    (F.col("SUB_GVRNMT_EST_PRM_SK") != 0) & (F.col("SUB_GVRNMT_EST_PRM_SK") != 1)
).select(
    F.col("SUB_GVRNMT_EST_PRM_SK").alias("SUB_GVRNMT_EST_PRM_SK"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("CLS_PLN_PROD_CAT_CD").alias("CLS_PLN_PROD_CAT_CD"),
    F.col("EFF_DT_SK").alias("SUB_GVRNMT_EST_PRM_EFF_DT_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.when(F.col("CLS_PLN_PROD_CAT_NM").isNull(), F.lit("UNK")).otherwise(F.col("CLS_PLN_PROD_CAT_NM")).alias("CLS_PLN_PROD_CAT_NM"),
    F.col("TERM_DT_SK").alias("SUB_GVRNMT_EST_PRM_TERM_DT_SK"),
    F.col("EST_PRM_AMT").alias("EST_PRM_AMT"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLS_PLN_PROD_CAT_CD_SK").alias("CLS_PLN_PROD_CAT_CD_SK")
)

df_UNKRow = spark.createDataFrame(
    [
        (
            0,
            0,
            'UNK',
            '1753-01-01',
            'UNK',
            '1753-01-01',
            EDWRunCycleDate,
            0,
            'UNK',
            '2199-31-12',
            0,
            100,
            EDWRunCycle,
            0
        )
    ],
    [
        "SUB_GVRNMT_EST_PRM_SK",
        "SUB_UNIQ_KEY",
        "CLS_PLN_PROD_CAT_CD",
        "SUB_GVRNMT_EST_PRM_EFF_DT_SK",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "SUB_SK",
        "CLS_PLN_PROD_CAT_NM",
        "SUB_GVRNMT_EST_PRM_TERM_DT_SK",
        "EST_PRM_AMT",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLS_PLN_PROD_CAT_CD_SK"
    ]
)

df_NARow = spark.createDataFrame(
    [
        (
            1,
            1,
            'NA',
            '1753-01-01',
            'NA',
            '1753-01-01',
            EDWRunCycleDate,
            1,
            'NA',
            '2199-31-12',
            1,
            100,
            EDWRunCycle,
            1
        )
    ],
    [
        "SUB_GVRNMT_EST_PRM_SK",
        "SUB_UNIQ_KEY",
        "CLS_PLN_PROD_CAT_CD",
        "SUB_GVRNMT_EST_PRM_EFF_DT_SK",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "SUB_SK",
        "CLS_PLN_PROD_CAT_NM",
        "SUB_GVRNMT_EST_PRM_TERM_DT_SK",
        "EST_PRM_AMT",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLS_PLN_PROD_CAT_CD_SK"
    ]
)

final_columns = [
    "SUB_GVRNMT_EST_PRM_SK",
    "SUB_UNIQ_KEY",
    "CLS_PLN_PROD_CAT_CD",
    "SUB_GVRNMT_EST_PRM_EFF_DT_SK",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "SUB_SK",
    "CLS_PLN_PROD_CAT_NM",
    "SUB_GVRNMT_EST_PRM_TERM_DT_SK",
    "EST_PRM_AMT",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLS_PLN_PROD_CAT_CD_SK"
]

df_fnl_dataLinks = (
    df_lnk_IdsEdwSubGvrnmtEstPrmDExtr_Main.select(final_columns)
    .unionByName(df_UNKRow.select(final_columns))
    .unionByName(df_NARow.select(final_columns))
)

df_seq_SUB_GVRNMT_EST_PRM_D_csv_load = df_fnl_dataLinks.select(
    F.col("SUB_GVRNMT_EST_PRM_SK").alias("SUB_GVRNMT_EST_PRM_SK"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("CLS_PLN_PROD_CAT_CD").alias("CLS_PLN_PROD_CAT_CD"),
    F.rpad(F.col("SUB_GVRNMT_EST_PRM_EFF_DT_SK"), 10, " ").alias("SUB_GVRNMT_EST_PRM_EFF_DT_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("CLS_PLN_PROD_CAT_NM").alias("CLS_PLN_PROD_CAT_NM"),
    F.rpad(F.col("SUB_GVRNMT_EST_PRM_TERM_DT_SK"), 10, " ").alias("SUB_GVRNMT_EST_PRM_TERM_DT_SK"),
    F.col("EST_PRM_AMT").alias("EST_PRM_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLS_PLN_PROD_CAT_CD_SK").alias("CLS_PLN_PROD_CAT_CD_SK")
)

write_files(
    df_seq_SUB_GVRNMT_EST_PRM_D_csv_load,
    f"{adls_path}/load/SUB_GVRNMT_EST_PRM_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)