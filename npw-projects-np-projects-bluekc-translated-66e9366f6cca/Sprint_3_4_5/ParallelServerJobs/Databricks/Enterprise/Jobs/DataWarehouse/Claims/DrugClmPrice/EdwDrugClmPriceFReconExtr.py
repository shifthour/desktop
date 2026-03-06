# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                  ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Sri Nannpaneni               2019-11-25            6131                          Initial Programming                                             EnterpriseDev2              Kalyan Neelam             2019-11-26

# MAGIC count validation 
# MAGIC CLM_F. COUNT(Claim SK) = DRUG_CLM_PRICE_F COUNT( DRUG_CLM_SK)
# MAGIC This parallel job is called in EdwDrugClmPriceFReconCntl sequence
# MAGIC CLM_SK Row level validation 
# MAGIC CLM_F.CLM_ID = DRUG_CLM_PRICE_F.CLM_ID
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, length, rpad, trim
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','2019-12-02')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query = f"SELECT Distinct CLM_SK ,CLM_ID FROM {EDWOwner}.DRUG_CLM_PRICE_F"
df_db2_DRUG_CLM_PRICE_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_db2_DRUG_CLM_PRICE_F = df_db2_DRUG_CLM_PRICE_F.select("CLM_SK","CLM_ID")

df_cp_DRUG_CLM_PRICE_F_out1 = df_db2_DRUG_CLM_PRICE_F.select(
    col("CLM_SK").alias("CLM_SK"),
    col("CLM_ID").alias("DRUG_CLM_PRICE_F_CLM_ID")
)

df_cp_DRUG_CLM_PRICE_F_out2 = df_db2_DRUG_CLM_PRICE_F.select(
    col("CLM_SK").alias("CLM_SK")
)

row_count = df_cp_DRUG_CLM_PRICE_F_out2.count()
df_Xfm_DRUG_CLM_PRICE_F_Counts = spark.createDataFrame(
    [(row_count, "1")],
    ["DRUG_CLM_PRICE_F_CNT","KEY"]
)

extract_query = f"SELECT Distinct CLM_SK, CLM_ID FROM {EDWOwner}.CLM_F where SRC_SYS_CD = 'OPTUMRX'"
df_db2_CLM_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_db2_CLM_F = df_db2_CLM_F.select("CLM_SK","CLM_ID")

df_cp_CLM_F_out1 = df_db2_CLM_F.select(
    col("CLM_SK").alias("CLM_SK")
)

df_cp_CLM_F_out2 = df_db2_CLM_F.select(
    col("CLM_SK").alias("CLM_SK"),
    col("CLM_ID").alias("CLM_F_CLM_ID")
)

df_Jn_ClmSk = df_cp_DRUG_CLM_PRICE_F_out1.alias("Lnk_DrugClmPriceF_In").join(
    df_cp_CLM_F_out2.alias("Lnk_ClmF_Ids"),
    col("Lnk_DrugClmPriceF_In.CLM_SK") == col("Lnk_ClmF_Ids.CLM_SK"),
    "full"
).select(
    col("Lnk_DrugClmPriceF_In.CLM_SK").alias("leftRec_CLM_SK"),
    col("Lnk_DrugClmPriceF_In.DRUG_CLM_PRICE_F_CLM_ID").alias("DRUG_CLM_PRICE_F_CLM_ID"),
    col("Lnk_ClmF_Ids.CLM_SK").alias("rightRec_CLM_SK"),
    col("Lnk_ClmF_Ids.CLM_F_CLM_ID").alias("CLM_F_CLM_ID")
)

df_xfm_Logic_ClmIds_init = df_Jn_ClmSk.withColumn(
    "REASON",
    when(length(trim(col("leftRec_CLM_SK"))) == 1, lit("DRUG_CLM_PRICE_F is Out of Balance with CLM_F"))
    .when(length(trim(col("rightRec_CLM_SK"))) == 1, lit("CLM_F is Out of Balance with DRUG_CLM_PRICE_F"))
    .otherwise(lit(""))
)

df_xfm_Logic_ClmIds_filtered = df_xfm_Logic_ClmIds_init.filter(
    (length(trim(col("leftRec_CLM_SK"))) == 1) | (length(trim(col("rightRec_CLM_SK"))) == 1)
)

df_xfm_Logic_ClmIds = df_xfm_Logic_ClmIds_filtered.select(
    col("rightRec_CLM_SK").alias("CLM_F_CLM_SK"),
    col("CLM_F_CLM_ID").alias("CLM_F_CLM_ID"),
    col("DRUG_CLM_PRICE_F_CLM_ID").alias("DRUG_CLM_PRICE_F_CLM_ID"),
    col("leftRec_CLM_SK").alias("DRUG_CLM_PRICE_F_CLM_SK"),
    col("REASON").alias("REASON")
)

df_final_1 = (
    df_xfm_Logic_ClmIds
    .withColumn("CLM_F_CLM_SK", rpad("CLM_F_CLM_SK", 50, " "))
    .withColumn("CLM_F_CLM_ID", rpad("CLM_F_CLM_ID", 50, " "))
    .withColumn("DRUG_CLM_PRICE_F_CLM_ID", rpad("DRUG_CLM_PRICE_F_CLM_ID", 50, " "))
    .withColumn("DRUG_CLM_PRICE_F_CLM_SK", rpad("DRUG_CLM_PRICE_F_CLM_SK", 50, " "))
    .withColumn("REASON", rpad("REASON", 50, " "))
    .select("CLM_F_CLM_SK","CLM_F_CLM_ID","DRUG_CLM_PRICE_F_CLM_ID","DRUG_CLM_PRICE_F_CLM_SK","REASON")
)

write_files(
    df_final_1,
    f"{adls_path_publish}/external/OptumRx_ClmSpreadPrice_Recon_Claims.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

row_count_2 = df_cp_CLM_F_out1.count()
df_Xfm_CLM_F_Counts = spark.createDataFrame(
    [(row_count_2, "1")],
    ["CML_F_COUNT","KEY"]
)

df_Lkp_Key = df_Xfm_DRUG_CLM_PRICE_F_Counts.alias("Lnk_DrugClmPriceF_Counts_Out").join(
    df_Xfm_CLM_F_Counts.alias("Lnk_clmF_Counts_Out"),
    col("Lnk_DrugClmPriceF_Counts_Out.KEY") == col("Lnk_clmF_Counts_Out.KEY"),
    "inner"
).select(
    col("Lnk_DrugClmPriceF_Counts_Out.DRUG_CLM_PRICE_F_CNT").alias("DRUG_CLM_PRICE_F_CNT"),
    col("Lnk_clmF_Counts_Out.CML_F_COUNT").alias("CML_F_COUNT")
)

df_xfm_Logic_Clmcounts = df_Lkp_Key.select(
    col("CML_F_COUNT").alias("CML_F_COUNT"),
    col("DRUG_CLM_PRICE_F_CNT").alias("DRUG_CLM_PRICE_F_CNT"),
    (col("CML_F_COUNT") - col("DRUG_CLM_PRICE_F_CNT")).alias("CNT_DIFF")
)

df_final_2 = (
    df_xfm_Logic_Clmcounts
    .withColumn("CML_F_COUNT", rpad("CML_F_COUNT", 50, " "))
    .withColumn("DRUG_CLM_PRICE_F_CNT", rpad("DRUG_CLM_PRICE_F_CNT", 50, " "))
    .withColumn("CNT_DIFF", rpad("CNT_DIFF", 50, " "))
    .select("CML_F_COUNT","DRUG_CLM_PRICE_F_CNT","CNT_DIFF")
)

write_files(
    df_final_2,
    f"{adls_path_publish}/external/OptumRx_ClmSpreadPrice_Recon_Count.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)