# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2020 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY :  OptumDrugLandSeq
# MAGIC  
# MAGIC DESCRIPTION:  Join extract file DRUG_CLM_ACCUM_IMPCT_prep.dat (created in job OptumClmLandExtr that extracts from the daily claims file) to table K_CLM (already created for table CLM in OptumDrugLandSeq) to get the surrogate key values for columns DRUG_CLM_SK and CRT_RUN_CYC_EXCTN_SK and write to file DRUG_CLM_ACCUM_IMPCT.dat.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                     Date               Project                 Change Description                                                                                                   Development Project    Code Reviewer               Date Reviewed
# MAGIC --------------------------          --------------------    ----------------------      ------------------------------------------------------------------------------------------------------------------------------    ---------------------------------    ------------------------------------    ----------------------------              
# MAGIC Bill Schroeder              07/14/2023     US-586570          Original Programming                                                                                                 IntegrateDev2               Jeyaprasanna                  2023-08-14

# MAGIC Join extract file DRUG_CLM_ACCUM_IMPCT_prep.dat to table K_CLM (already created for table CLM) to get the surrogate key values for columns DRUG_CLM_SK and CRT_RUN_CYC_EXCTN_SK and write to file DRUG_CLM_ACCUM_IMPCT.dat for loading IDS table.
# MAGIC Lookup in CD_MPPNG to validate CLIENTDEF3 (NCP_COUPON_TYP_CD) and retrieve NCP_COUPON_TYP_NM.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import col, when, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_CD_MPPNG = """SELECT 
SRC_CD, TRGT_CD_NM, CD_MPPNG_SK as SRC_CD_SK
FROM #$IDSOwner#.CD_MPPNG
WHERE SRC_CLCTN_CD='OPTUMRX'
  AND SRC_SYS_CD='OPTUMRX'
  AND SRC_DOMAIN_NM='NCP COUPON TYPE'
  AND TRGT_CLCTN_CD='IDS'
  AND TRGT_DOMAIN_NM='NCP COUPON TYPE'
union
SELECT
SRC_CD, TRGT_CD_NM, cast(CD_MPPNG_SK as integer) as SRC_CD_SK
FROM #$IDSOwner#.CD_MPPNG
WHERE CD_MPPNG_SK='1'
"""

df_db2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG)
    .load()
)

schema_DRUG_CLM_ACCUM_IMPCT_prep = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CLIENTDEF3", StringType(), False),
    StructField("CCAA_COUPON_AMT", DecimalType(38,10), False),
    StructField("FMLY_ACCUM_DEDCT_AMT", DecimalType(38,10), False),
    StructField("FMLY_ACCUM_OOP_AMT", DecimalType(38,10), False),
    StructField("INDV_ACCUM_DEDCT_AMT", DecimalType(38,10), False),
    StructField("INDV_ACCUM_OOP_AMT", DecimalType(38,10), False),
    StructField("INDV_APLD_DEDCT_AMT", DecimalType(38,10), False),
    StructField("INDV_APLD_OOP_AMT", DecimalType(38,10), False)
])

df_DRUG_CLM_ACCUM_IMPCT_prep = (
    spark.read.format("csv")
    .schema(schema_DRUG_CLM_ACCUM_IMPCT_prep)
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .load(f"{adls_path}/load/DRUG_CLM_ACCUM_IMPCT_prep.dat")
)

execute_dml("DROP TABLE IF EXISTS STAGING.OptumDrugClmAccumImpctExtr_db2_K_CLM_temp", jdbc_url, jdbc_props)

(
    df_DRUG_CLM_ACCUM_IMPCT_prep.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.OptumDrugClmAccumImpctExtr_db2_K_CLM_temp")
    .mode("append")
    .save()
)

extract_query_db2_K_CLM = f"""SELECT 
a.SRC_SYS_CD_SK as Ref_db2_K_CLM_SRC_SYS_CD_SK,
a.CLM_ID as Ref_db2_K_CLM_CLM_ID,
a.CRT_RUN_CYC_EXCTN_SK as Ref_db2_K_CLM_CRT_RUN_CYC_EXCTN_SK,
a.CLM_SK as Ref_db2_K_CLM_CLM_SK,
b.SRC_SYS_CD_SK as Lnk_Accum_extr_SRC_SYS_CD_SK,
b.CLM_ID as Lnk_Accum_extr_CLM_ID,
b.LAST_UPDT_RUN_CYC_EXCTN_SK as Lnk_Accum_extr_LAST_UPDT_RUN_CYC_EXCTN_SK,
b.CLIENTDEF3 as Lnk_Accum_extr_CLIENTDEF3,
b.CCAA_COUPON_AMT as Lnk_Accum_extr_CCAA_COUPON_AMT,
b.FMLY_ACCUM_DEDCT_AMT as Lnk_Accum_extr_FMLY_ACCUM_DEDCT_AMT,
b.FMLY_ACCUM_OOP_AMT as Lnk_Accum_extr_FMLY_ACCUM_OOP_AMT,
b.INDV_ACCUM_DEDCT_AMT as Lnk_Accum_extr_INDV_ACCUM_DEDCT_AMT,
b.INDV_ACCUM_OOP_AMT as Lnk_Accum_extr_INDV_ACCUM_OOP_AMT,
b.INDV_APLD_DEDCT_AMT as Lnk_Accum_extr_INDV_APLD_DEDCT_AMT,
b.INDV_APLD_OOP_AMT as Lnk_Accum_extr_INDV_APLD_OOP_AMT
FROM #$IDSOwner#.K_CLM a
JOIN STAGING.OptumDrugClmAccumImpctExtr_db2_K_CLM_temp b
   ON a.SRC_SYS_CD_SK = b.SRC_SYS_CD_SK
   AND a.CLM_ID = b.CLM_ID
"""

df_joined_db2_K_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_K_CLM)
    .load()
)

df_Lkp_K_CLM = df_joined_db2_K_CLM.select(
    col("Ref_db2_K_CLM_CLM_SK").alias("DRUG_CLM_SK"),
    col("Lnk_Accum_extr_SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("Lnk_Accum_extr_CLM_ID").alias("CLM_ID"),
    col("Ref_db2_K_CLM_CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Lnk_Accum_extr_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Lnk_Accum_extr_CCAA_COUPON_AMT").alias("CCAA_COUPON_AMT"),
    col("Lnk_Accum_extr_FMLY_ACCUM_DEDCT_AMT").alias("FMLY_ACCUM_DEDCT_AMT"),
    col("Lnk_Accum_extr_FMLY_ACCUM_OOP_AMT").alias("FMLY_ACCUM_OOP_AMT"),
    col("Lnk_Accum_extr_INDV_ACCUM_DEDCT_AMT").alias("INDV_ACCUM_DEDCT_AMT"),
    col("Lnk_Accum_extr_INDV_ACCUM_OOP_AMT").alias("INDV_ACCUM_OOP_AMT"),
    col("Lnk_Accum_extr_INDV_APLD_DEDCT_AMT").alias("INDV_APLD_DEDCT_AMT"),
    col("Lnk_Accum_extr_INDV_APLD_OOP_AMT").alias("INDV_APLD_OOP_AMT"),
    col("Lnk_Accum_extr_CLIENTDEF3").alias("CLIENTDEF3")
)

df_Lkp_Src_Cd = df_Lkp_K_CLM.join(
    df_db2_CD_MPPNG.alias("Ref_db2_CD_MPPNG"),
    df_Lkp_K_CLM["CLIENTDEF3"] == df_db2_CD_MPPNG["SRC_CD"],
    "left"
)

df_Lkp_Src_Cd_select = df_Lkp_Src_Cd.select(
    col("DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    df_db2_CD_MPPNG["SRC_CD"].alias("SRC_CD"),
    df_db2_CD_MPPNG["TRGT_CD_NM"].alias("TRGT_CD_NM"),
    col("CCAA_COUPON_AMT").alias("CCAA_COUPON_AMT"),
    col("FMLY_ACCUM_DEDCT_AMT").alias("FMLY_ACCUM_DEDCT_AMT"),
    col("FMLY_ACCUM_OOP_AMT").alias("FMLY_ACCUM_OOP_AMT"),
    col("INDV_ACCUM_DEDCT_AMT").alias("INDV_ACCUM_DEDCT_AMT"),
    col("INDV_ACCUM_OOP_AMT").alias("INDV_ACCUM_OOP_AMT"),
    col("INDV_APLD_DEDCT_AMT").alias("INDV_APLD_DEDCT_AMT"),
    col("INDV_APLD_OOP_AMT").alias("INDV_APLD_OOP_AMT"),
    df_db2_CD_MPPNG["SRC_CD_SK"].alias("SRC_CD_SK")
)

df_Trans_Accum = df_Lkp_Src_Cd_select.select(
    col("DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    trim(col("SRC_CD_SK")).alias("NCP_COUPON_TYP_CD_SK"),
    when(
        (trim(col("SRC_CD")) == lit("99")) & (col("CCAA_COUPON_AMT") > lit(0)),
        lit("Y")
    ).otherwise(lit("N")).alias("CCAA_APLD_IN"),
    col("CCAA_COUPON_AMT").alias("CCAA_COUPON_AMT"),
    col("FMLY_ACCUM_DEDCT_AMT").alias("FMLY_ACCUM_DEDCT_AMT"),
    col("FMLY_ACCUM_OOP_AMT").alias("FMLY_ACCUM_OOP_AMT"),
    col("INDV_ACCUM_DEDCT_AMT").alias("INDV_ACCUM_DEDCT_AMT"),
    col("INDV_ACCUM_OOP_AMT").alias("INDV_ACCUM_OOP_AMT"),
    col("INDV_APLD_DEDCT_AMT").alias("INDV_APLD_DEDCT_AMT"),
    col("INDV_APLD_OOP_AMT").alias("INDV_APLD_OOP_AMT")
)

df_final = (
    df_Trans_Accum
    .withColumn("CLM_ID", rpad(col("CLM_ID"), <...>, " "))
    .withColumn("CCAA_APLD_IN", rpad(col("CCAA_APLD_IN"), 1, " "))
)

df_final_select = df_final.select(
    "DRUG_CLM_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "NCP_COUPON_TYP_CD_SK",
    "CCAA_APLD_IN",
    "CCAA_COUPON_AMT",
    "FMLY_ACCUM_DEDCT_AMT",
    "FMLY_ACCUM_OOP_AMT",
    "INDV_ACCUM_DEDCT_AMT",
    "INDV_ACCUM_OOP_AMT",
    "INDV_APLD_DEDCT_AMT",
    "INDV_APLD_OOP_AMT"
)

write_files(
    df_final_select,
    f"{adls_path}/load/DRUG_CLM_ACCUM_IMPCT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="",
    nullValue=None
)