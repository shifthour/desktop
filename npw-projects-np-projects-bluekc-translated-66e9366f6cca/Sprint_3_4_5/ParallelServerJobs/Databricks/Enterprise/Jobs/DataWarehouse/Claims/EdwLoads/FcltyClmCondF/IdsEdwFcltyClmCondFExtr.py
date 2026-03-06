# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsEdwFcltyClmCondFExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from Ids - FCLTY_CLM_COND taable and loads Edw - FCLTY_CLM_COND_F table
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	
# MAGIC                 IDS -   FCLTY_CLM_COND
# MAGIC                            CLM
# MAGIC                           
# MAGIC 
# MAGIC   TRANSFORMS:  
# MAGIC                   TRIM and NullCheck all the fields that are used in lookups. Rest are direct maps.
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                 
# MAGIC OUTPUTS: 
# MAGIC                     Load File to be used the Load Job
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC =========================================================================================================================================================================                                                                                                                                                                                                                  
# MAGIC DEVELOPER                         DATE                 PROJECT                                                DESCRIPTION                                            DATASTAGE                                            CODE                            REVIEW
# MAGIC                                                                                                                                                                                                               ENVIRONMENT                                       REVIEWER                   DATE                                              
# MAGIC =========================================================================================================================================================================
# MAGIC Manasa Andru                     2014-12-17          TFS - 9802                                            Original Programming                                   EnterpriseNewDevl                                        Kalyan Neelam              2014-12-29

# MAGIC This is Source extract data from an IDS table and apply Code denormalization, create a load ready file.
# MAGIC 
# MAGIC Job Name:
# MAGIC IdsEdwFcltyClmCondFExtr
# MAGIC 
# MAGIC Table:
# MAGIC FCLTY_CLM_COND_F
# MAGIC Read from source table FCLTY_CLM_COND
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write FCLTY_CLM_COND_F Data into a Sequential file for Load Job.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Add Defaults and Null Handling
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC 1) CLM_ID
# MAGIC 2) SRC_SYS_CD_SK
# MAGIC Lookup on Reference CLM
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import Row
from pyspark.sql.functions import col, lit, when, length, trim, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

# Assuming SparkSession is already defined as spark and the helper functions are in the namespace

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# db2_FCLTY_CLM_COND_in
extract_query_db2_FCLTY_CLM_COND_in = f"""
SELECT 
 FCC.FCLTY_CLM_COND_SK,
 FCC.SRC_SYS_CD_SK,
 FCC.CLM_ID,
 FCC.FCLTY_CLM_COND_SEQ_NO,
 FCC.CRT_RUN_CYC_EXCTN_SK,
 FCC.LAST_UPDT_RUN_CYC_EXCTN_SK,
 FCC.FCLTY_CLM_SK,
 FCC.FCLTY_CLM_COND_CD_SK,
 FCC.FCLTY_CLM_SK as CLM_SK
FROM 
 {IDSOwner}.FCLTY_CLM_COND FCC,
 {IDSOwner}.W_EDW_ETL_DRVR dr
WHERE
 FCC.SRC_SYS_CD_SK = dr.SRC_SYS_CD_SK
 AND FCC.CLM_ID = dr.CLM_ID
"""
df_db2_FCLTY_CLM_COND_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_FCLTY_CLM_COND_in)
    .load()
)

# db2_CD_MPPNG_Extr
extract_query_db2_CD_MPPNG_Extr = f"""
SELECT 
 CD_MPPNG_SK,
 COALESCE(TRGT_CD,'UNK') AS TRGT_CD,
 COALESCE(TRGT_CD_NM,'UNK') AS TRGT_CD_NM
FROM 
 {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

# cpy (PxCopy)
df_cpy = df_db2_CD_MPPNG_Extr

df_Ref_SrcSysCdLkup = df_cpy.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_FcltyClmCondLkup = df_cpy.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# lkp_Codes (PxLookup)
df_lkp_Codes_intermediate = (
    df_db2_FCLTY_CLM_COND_in.alias("lnk_CodesLkpData_out")
    .join(
        df_Ref_FcltyClmCondLkup.alias("Ref_FcltyClmCondLkup"),
        col("lnk_CodesLkpData_out.FCLTY_CLM_COND_CD_SK") == col("Ref_FcltyClmCondLkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_SrcSysCdLkup.alias("Ref_SrcSysCdLkup"),
        col("lnk_CodesLkpData_out.SRC_SYS_CD_SK") == col("Ref_SrcSysCdLkup.CD_MPPNG_SK"),
        "left"
    )
)

df_lkp_Codes = df_lkp_Codes_intermediate.select(
    col("lnk_CodesLkpData_out.FCLTY_CLM_COND_SK").alias("FCLTY_CLM_COND_SK"),
    col("lnk_CodesLkpData_out.CLM_ID").alias("CLM_ID"),
    col("lnk_CodesLkpData_out.FCLTY_CLM_COND_SEQ_NO").alias("FCLTY_CLM_COND_SEQ_NO"),
    col("Ref_SrcSysCdLkup.TRGT_CD").alias("SRC_SYS_CD"),
    col("lnk_CodesLkpData_out.CLM_SK").alias("CLM_SK"),
    col("lnk_CodesLkpData_out.FCLTY_CLM_SK").alias("FCLTY_CLM_SK"),
    col("Ref_FcltyClmCondLkup.TRGT_CD").alias("FCLTY_CLM_COND_CD"),
    col("Ref_FcltyClmCondLkup.TRGT_CD_NM").alias("FCLTY_CLM_COND_NM"),
    col("lnk_CodesLkpData_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_CodesLkpData_out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lnk_CodesLkpData_out.FCLTY_CLM_COND_CD_SK").alias("FCLTY_CLM_COND_CD_SK")
)

# xfm_BusinessLogic (CTransformerStage)

# Main path
df_main = (
    df_lkp_Codes
    .withColumn(
        "SRC_SYS_CD",
        when(
            col("SRC_SYS_CD").isNull() | (length(trim(col("SRC_SYS_CD"))) == 0),
            lit("UNK")
        ).otherwise(col("SRC_SYS_CD"))
    )
    .withColumn(
        "CLM_SK",
        when(
            col("CLM_SK").isNull() | (col("CLM_SK") == ''),
            lit(0)
        ).otherwise(col("CLM_SK"))
    )
    .withColumn(
        "FCLTY_CLM_COND_CD",
        when(
            col("FCLTY_CLM_COND_CD").isNull() | (length(trim(col("FCLTY_CLM_COND_CD"))) == 0),
            lit("UNK")
        ).otherwise(col("FCLTY_CLM_COND_CD"))
    )
    .withColumn(
        "FCLTY_CLM_COND_NM",
        when(
            col("FCLTY_CLM_COND_NM").isNull() | (length(trim(col("FCLTY_CLM_COND_NM"))) == 0),
            lit("UNK")
        ).otherwise(col("FCLTY_CLM_COND_NM"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
)

df_FcltyClmCondFMainExtr = df_main.filter(
    (col("FCLTY_CLM_COND_SK") != 0) & (col("FCLTY_CLM_COND_SK") != 1)
)

# UNK link: single hard-coded row
schema_unk = StructType([
    StructField("FCLTY_CLM_COND_SK", IntegerType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("FCLTY_CLM_COND_SEQ_NO", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CLM_SK", IntegerType(), True),
    StructField("FCLTY_CLM_SK", IntegerType(), True),
    StructField("FCLTY_CLM_COND_CD", StringType(), True),
    StructField("FCLTY_CLM_COND_NM", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("FCLTY_CLM_COND_CD_SK", IntegerType(), True)
])
df_unk = spark.createDataFrame(
    [
        (
            0, "UNK", 0, "UNK", "1753-01-01", "1753-01-01", 0, 0, "UNK", "UNK", 100, 100, 0
        )
    ],
    schema_unk
)

# NA link: single hard-coded row
schema_na = StructType([
    StructField("FCLTY_CLM_COND_SK", IntegerType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("FCLTY_CLM_COND_SEQ_NO", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CLM_SK", IntegerType(), True),
    StructField("FCLTY_CLM_SK", IntegerType(), True),
    StructField("FCLTY_CLM_COND_CD", StringType(), True),
    StructField("FCLTY_CLM_COND_NM", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("FCLTY_CLM_COND_CD_SK", IntegerType(), True)
])
df_na = spark.createDataFrame(
    [
        (
            1, "NA", 1, "NA", "1753-01-01", "1753-01-01", 1, 1, "NA", "NA", 100, 100, 1
        )
    ],
    schema_na
)

# Funnel_26 (PxFunnel) => union of the three sets
common_cols = [
    "FCLTY_CLM_COND_SK",
    "CLM_ID",
    "FCLTY_CLM_COND_SEQ_NO",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CLM_SK",
    "FCLTY_CLM_SK",
    "FCLTY_CLM_COND_CD",
    "FCLTY_CLM_COND_NM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "FCLTY_CLM_COND_CD_SK"
]
df_FcltyClmCondFMainExtr_sel = df_FcltyClmCondFMainExtr.select(common_cols)
df_unk_sel = df_unk.select(common_cols)
df_na_sel = df_na.select(common_cols)

df_funnel_26 = (
    df_FcltyClmCondFMainExtr_sel
    .union(df_unk_sel)
    .union(df_na_sel)
)

# Apply rpad for char(10) columns
df_funnel_26 = (
    df_funnel_26
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
)

df_final = df_funnel_26.select(common_cols)

# seq_FCLTY_CLM_COND_F_csv_load (PxSequentialFile)
write_files(
    df_final,
    f"{adls_path}/load/FCLTY_CLM_COND_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)