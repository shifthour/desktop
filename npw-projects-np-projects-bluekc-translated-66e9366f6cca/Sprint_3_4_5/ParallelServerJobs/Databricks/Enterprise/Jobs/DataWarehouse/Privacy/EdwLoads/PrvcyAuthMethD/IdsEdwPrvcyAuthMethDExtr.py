# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC PROCESSING: Extracts data from IDS PRVCY_AUTH_METH
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                2/6/2007           CDS Sunset/3279          Originally Programmed                     devlEDW10                     Steph Goddard            3/29/07
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               12/02/2013        5114                              Create Load File for EDW Table PRVCY_AUTH_METH_D                EnterpriseWhseDevl   Peter Marshall             1/6/2014

# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwPrvcyAuthMethDExtr
# MAGIC Read from source table PRVCY_AUTH_METH.
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC PRVCY_AUTH_METH_TYP_CD_SK
# MAGIC Write MRKR_CAT_D Data into a Sequential file for Load Job IdsEdwPrvcyAuthMethDLoad
# MAGIC Add Defaults and Null Handling.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

# db2_PRVCY_AUTH_METH_in
jdbc_url_db2_PRVCY_AUTH_METH_in, jdbc_props_db2_PRVCY_AUTH_METH_in = get_db_config(ids_secret_name)
extract_query_db2_PRVCY_AUTH_METH_in = f"""
SELECT 
AUTH.PRVCY_AUTH_METH_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
AUTH.PRVCY_AUTH_ID,
AUTH.CRT_RUN_CYC_EXCTN_SK,
AUTH.LAST_UPDT_RUN_CYC_EXCTN_SK,
AUTH.PRVCY_AUTH_METH_TYP_CD_SK,
AUTH.AUTH_METH_DESC
FROM {IDSOwner}.PRVCY_AUTH_METH AUTH
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON AUTH.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
"""
df_db2_PRVCY_AUTH_METH_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_PRVCY_AUTH_METH_in)
    .options(**jdbc_props_db2_PRVCY_AUTH_METH_in)
    .option("query", extract_query_db2_PRVCY_AUTH_METH_in)
    .load()
)

# db2_CD_MPPNG_Extr
jdbc_url_db2_CD_MPPNG_Extr, jdbc_props_db2_CD_MPPNG_Extr = get_db_config(ids_secret_name)
extract_query_db2_CD_MPPNG_Extr = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CD_MPPNG_Extr)
    .options(**jdbc_props_db2_CD_MPPNG_Extr)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

# lkp_Codes (PxLookup)
df_lkp_Codes = (
    df_db2_PRVCY_AUTH_METH_in.alias("lnk_IdsEdwPrvcyAuthMethDExtr_InAbc")
    .join(
        df_db2_CD_MPPNG_Extr.alias("lkp_MajPrctcCatCd_ref"),
        F.col("lnk_IdsEdwPrvcyAuthMethDExtr_InAbc.PRVCY_AUTH_METH_TYP_CD_SK")
        == F.col("lkp_MajPrctcCatCd_ref.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwPrvcyAuthMethDExtr_InAbc.PRVCY_AUTH_METH_SK").alias("PRVCY_AUTH_METH_SK"),
        F.col("lnk_IdsEdwPrvcyAuthMethDExtr_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwPrvcyAuthMethDExtr_InAbc.PRVCY_AUTH_ID").alias("PRVCY_AUTH_ID"),
        F.col("lnk_IdsEdwPrvcyAuthMethDExtr_InAbc.AUTH_METH_DESC").alias("AUTH_METH_DESC"),
        F.col("lkp_MajPrctcCatCd_ref.TRGT_CD").alias("PRVCY_AUTH_METH_TYP_CD"),
        F.col("lkp_MajPrctcCatCd_ref.TRGT_CD_NM").alias("PRVCY_AUTH_METH_TYP_NM"),
        F.col("lnk_IdsEdwPrvcyAuthMethDExtr_InAbc.PRVCY_AUTH_METH_TYP_CD_SK").alias("PRVCY_AUTH_METH_TYP_CD_SK")
    )
)

# xfm_BusinessLogic
# 1) Main link
df_xfm_main = (
    df_lkp_Codes.filter(
        (F.col("PRVCY_AUTH_METH_SK") != 0) & (F.col("PRVCY_AUTH_METH_SK") != 1)
    )
    .withColumn("PRVCY_AUTH_METH_SK", F.col("PRVCY_AUTH_METH_SK"))
    .withColumn("SRC_SYS_CD", F.col("SRC_SYS_CD"))
    .withColumn("PRVCY_AUTH_ID", F.col("PRVCY_AUTH_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrRunCycleDate))
    .withColumn("PRVCY_AUTH_METH_DESC", F.col("AUTH_METH_DESC"))
    .withColumn(
        "PRVCY_AUTH_METH_TYP_CD",
        F.when(
            F.col("PRVCY_AUTH_METH_TYP_CD").isNull()
            | (F.length(trim(F.col("PRVCY_AUTH_METH_TYP_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("PRVCY_AUTH_METH_TYP_CD"))
    )
    .withColumn(
        "PRVCY_AUTH_METH_TYP_NM",
        F.when(
            F.col("PRVCY_AUTH_METH_TYP_NM").isNull()
            | (F.length(trim(F.col("PRVCY_AUTH_METH_TYP_NM"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("PRVCY_AUTH_METH_TYP_NM"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn("PRVCY_AUTH_METH_TYP_CD_SK", F.col("PRVCY_AUTH_METH_TYP_CD_SK"))
    .select(
        "PRVCY_AUTH_METH_SK",
        "SRC_SYS_CD",
        "PRVCY_AUTH_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "PRVCY_AUTH_METH_DESC",
        "PRVCY_AUTH_METH_TYP_CD",
        "PRVCY_AUTH_METH_TYP_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PRVCY_AUTH_METH_TYP_CD_SK"
    )
)

# 2) NA link: a single row
data_na = [(
    1,
    "NA",
    "NA",
    "1753-01-01",
    "1753-01-01",
    "",
    "NA",
    "NA",
    100,
    100,
    1
)]
df_xfm_na = spark.createDataFrame(
    data_na,
    [
        "PRVCY_AUTH_METH_SK",
        "SRC_SYS_CD",
        "PRVCY_AUTH_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "PRVCY_AUTH_METH_DESC",
        "PRVCY_AUTH_METH_TYP_CD",
        "PRVCY_AUTH_METH_TYP_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PRVCY_AUTH_METH_TYP_CD_SK"
    ]
)

# 3) UNK link: a single row
data_unk = [(
    0,
    "UNK",
    "UNK",
    "1753-01-01",
    "1753-01-01",
    "",
    "UNK",
    "UNK",
    100,
    100,
    0
)]
df_xfm_unk = spark.createDataFrame(
    data_unk,
    [
        "PRVCY_AUTH_METH_SK",
        "SRC_SYS_CD",
        "PRVCY_AUTH_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "PRVCY_AUTH_METH_DESC",
        "PRVCY_AUTH_METH_TYP_CD",
        "PRVCY_AUTH_METH_TYP_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PRVCY_AUTH_METH_TYP_CD_SK"
    ]
)

# fnl_dataLinks (PxFunnel)
df_fnl_dataLinks = (
    df_xfm_na
    .unionByName(df_xfm_main)
    .unionByName(df_xfm_unk)
)

# Prepare final DataFrame with rpad for char(10) columns
df_final = (
    df_fnl_dataLinks
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
    )
    .select(
        "PRVCY_AUTH_METH_SK",
        "SRC_SYS_CD",
        "PRVCY_AUTH_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "PRVCY_AUTH_METH_DESC",
        "PRVCY_AUTH_METH_TYP_CD",
        "PRVCY_AUTH_METH_TYP_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PRVCY_AUTH_METH_TYP_CD_SK"
    )
)

# seq_PRVCY_AUTH_METH_D_Load (PxSequentialFile)
write_files(
    df_final,
    f"{adls_path}/load/PRVCY_AUTH_METH_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)