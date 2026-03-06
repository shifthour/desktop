# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClmHitListExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Read IDS Claim hit list and retrieve needed information from IDS claim table.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	IDS - CLM
# MAGIC                 IDS - CD_MPPNG
# MAGIC                 /edw/update/IdsClmHitList.dat
# MAGIC HASH FILES:  none
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  none
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Read IDS Claim hit list from /edw/update directory and retieive additional information from IDS claim and code mapping table.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:    Load file for IDS    - W_EDW_ETL_DRVR 
# MAGIC                                           EDW - W_CLM_DEL
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description                                                                                              Project                                Environment                         Code Reviewer         Data Reviewed
# MAGIC ------------------      ----------------------------     -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC 2006-03-24      Brent Leland            Original Programming.
# MAGIC 2006-06-16      BJ Luce                   added CLM_SK in select for reference lookup to match columns
# MAGIC                                                         corrected the order of columns in both W_EDW_ETL_DRVR and W_CLM_DEL outfiles
# MAGIC 2006-11-27      Ralph Tucker           Added PCA Claim Driver & PCA Claim Delete file
# MAGIC 
# MAGIC 2013-10-18     Pooja Sunkara         Rewrite in Parallel                                                                                                   5114                                   EnterpriseWrhsDevl            Peter Marshall           12/24/2013
# MAGIC 
# MAGIC 2014-01-28     Archana Palivela      Changed the Lookup type from Normal to Sparse for                                             5114                                   EnterpriseWrhsDevl             Jag Yelavarthi          2014-01-28
# MAGIC                                                                                                                 Performance efficiency

# MAGIC This load file drive the IDS extracts from each claim table.
# MAGIC This load file drive the IDS PCA extracts from each claim table.
# MAGIC This load file drive the EDW claim table deletes.
# MAGIC This load file drive the EDW PCA claim table deletes.
# MAGIC Job name:
# MAGIC IdsEdwClmHitListExtr
# MAGIC Process Hit List of Claims from IDS
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSFilePath = get_widget_value('IDSFilePath','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_seq_IdsClmHitList_csv = StructType([
    StructField("CLM_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CLAIM_ID", StringType(), nullable=False)
])

df_seq_IdsClmHitList_csv = (
    spark.read
    .option("header", False)
    .option("sep", ",")
    .option("quote", "^")
    .option("escape", "^")
    .option("nullValue", None)
    .schema(schema_seq_IdsClmHitList_csv)
    .csv(f"{adls_path}/update/IdsClmHitList.dat")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_CLM_in = f"""
SELECT
    cl.CLM_SK,
    cl.SRC_SYS_CD_SK,
    COALESCE(cd.TRGT_CD,'UNK') AS SRC_SYS_CD,
    cl.CLM_ID,
    cl.LAST_UPDT_RUN_CYC_EXCTN_SK,
    cd2.TRGT_CD AS PCA_TYP_CD
FROM {IDSOwner}.CLM cl
JOIN {IDSOwner}.CD_MPPNG cd
    ON cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
JOIN {IDSOwner}.CD_MPPNG cd2
    ON cl.PCA_TYP_CD_SK = cd2.CD_MPPNG_SK
"""

df_db2_CLM_in = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_in)
    .load()
)

df_Lkup_CLM = (
    df_seq_IdsClmHitList_csv.alias("primary")
    .join(
        df_db2_CLM_in.alias("ref_CLM_SK"),
        F.col("primary.CLM_SK") == F.col("ref_CLM_SK.CLM_SK"),
        how="left"
    )
    .select(
        F.col("ref_CLM_SK.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("ref_CLM_SK.CLM_ID").alias("CLM_ID"),
        F.col("ref_CLM_SK.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("ref_CLM_SK.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ref_CLM_SK.PCA_TYP_CD").alias("PCA_TYP_CD")
    )
)

# xmf_businessLogic equivalent
condition_norm = (
    F.col("CLM_ID").isNotNull() &
    F.col("PCA_TYP_CD").isin("MEDPCA", "NA")
)
condition_pca = (
    F.col("CLM_ID").isNotNull() &
    F.col("PCA_TYP_CD").isin(
        "MEDPCA", "RUNOUT", "PCA", "EMPWBNF", "EXCPT", "RUNINMED",
        "RUNINRX", "PCARXONLY", "PCAMEDONLY"
    )
)

df_lnk_IdsDriver_OutABC = df_Lkup_CLM.filter(condition_norm).select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "SRC_SYS_CD",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

df_lnk_EdwDelete_OutABC = df_Lkup_CLM.filter(condition_norm).select(
    "SRC_SYS_CD",
    "CLM_ID",
    "SRC_SYS_CD_SK"
)

df_lnk_EdwPcaDelete_OutABC = df_Lkup_CLM.filter(condition_pca).select(
    "SRC_SYS_CD",
    "CLM_ID",
    "SRC_SYS_CD_SK"
)

df_lnk_IdsPcaDriver_OutABC = df_Lkup_CLM.filter(condition_pca).select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "SRC_SYS_CD",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

write_files(
    df_lnk_IdsDriver_OutABC,
    f"{adls_path}/file{IDSFilePath}/load/W_EDW_ETL_DRVR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

write_files(
    df_lnk_EdwDelete_OutABC,
    f"{adls_path}/load/W_CLM_DEL.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

write_files(
    df_lnk_EdwPcaDelete_OutABC,
    f"{adls_path}/load/W_PCA_CLM_DEL.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

write_files(
    df_lnk_IdsPcaDriver_OutABC,
    f"{adls_path}/file{IDSFilePath}/load/W_EDW_PCA_ETL_DRVR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)