# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsEdwFcltyClmValFExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from Ids - FCLTY_CLM_VAL taable and loads Edw - FCLTY_CLM_VAL_F table
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	
# MAGIC                 IDS -   FCLTY_CLM_VAL
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
# MAGIC Manasa Andru                     2014-12-17          TFS - 9802                                            Original Programming                                   EnterpriseNewDevl                                       Kalyan Neelam               2014-12-29

# MAGIC This is Source extract data from an IDS table and apply Code denormalization, create a load ready file.
# MAGIC 
# MAGIC Job Name:
# MAGIC IdsEdwFcltyClmValFExtr
# MAGIC 
# MAGIC Table:
# MAGIC FCLTY_CLM_VAL_F
# MAGIC Read from source table FCLTY_CLM_VAL
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write FCLTY_CLM_VAL_F Data into a Sequential file for Load Job.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Add Defaults and Null Handling
# MAGIC Lookup on Reference CLM
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_FCLTY_CLM_VAL_in = f"""SELECT 
FCV.FCLTY_CLM_VAL_SK,
FCV.SRC_SYS_CD_SK,
FCV.CLM_ID,
FCV.FCLTY_CLM_VAL_SEQ_NO,
FCV.CRT_RUN_CYC_EXCTN_SK,
FCV.LAST_UPDT_RUN_CYC_EXCTN_SK,
FCV.FCLTY_CLM_SK,
FCV.FCLTY_CLM_VAL_CD_SK,
FCV.VAL_AMT,
FCV.VAL_UNIT_CT,
FCV.FCLTY_CLM_SK as CLM_SK
FROM {IDSOwner}.FCLTY_CLM_VAL FCV,
{IDSOwner}.W_EDW_ETL_DRVR dr
WHERE
FCV.SRC_SYS_CD_SK = dr.SRC_SYS_CD_SK
AND FCV.CLM_ID = dr.CLM_ID
"""
df_db2_FCLTY_CLM_VAL_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_FCLTY_CLM_VAL_in)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = f"""SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') as TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') as TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_cpy = df_db2_CD_MPPNG_Extr
df_Ref_FcltyClmValLkup = df_cpy
df_Ref_SrcSysCdLkup = df_cpy

df_lkp_Codes = (
    df_db2_FCLTY_CLM_VAL_in.alias("lnk_CodesLkpData_out")
    .join(
        df_Ref_FcltyClmValLkup.alias("Ref_FcltyClmValLkup"),
        F.col("lnk_CodesLkpData_out.FCLTY_CLM_VAL_CD_SK") == F.col("Ref_FcltyClmValLkup.CD_MPPNG_SK"),
        how="left"
    )
    .join(
        df_Ref_SrcSysCdLkup.alias("Ref_SrcSysCdLkup"),
        F.col("lnk_CodesLkpData_out.SRC_SYS_CD_SK") == F.col("Ref_SrcSysCdLkup.CD_MPPNG_SK"),
        how="left"
    )
    .select(
        F.col("lnk_CodesLkpData_out.FCLTY_CLM_VAL_SK").alias("FCLTY_CLM_VAL_SK"),
        F.col("lnk_CodesLkpData_out.CLM_ID").alias("CLM_ID"),
        F.col("lnk_CodesLkpData_out.FCLTY_CLM_VAL_SEQ_NO").alias("FCLTY_CLM_VAL_SEQ_NO"),
        F.col("Ref_SrcSysCdLkup.TRGT_CD").alias("SRC_SYS_CD"),
        F.col("lnk_CodesLkpData_out.CLM_SK").alias("CLM_SK"),
        F.col("lnk_CodesLkpData_out.FCLTY_CLM_SK").alias("FCLTY_CLM_SK"),
        F.col("Ref_FcltyClmValLkup.TRGT_CD").alias("FCLTY_CLM_VAL_CD"),
        F.col("Ref_FcltyClmValLkup.TRGT_CD_NM").alias("FCLTY_CLM_VAL_NM"),
        F.col("lnk_CodesLkpData_out.VAL_AMT").alias("VAL_AMT"),
        F.col("lnk_CodesLkpData_out.VAL_UNIT_CT").alias("VAL_UNIT_CT"),
        F.col("lnk_CodesLkpData_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_CodesLkpData_out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_CodesLkpData_out.FCLTY_CLM_VAL_CD_SK").alias("FCLTY_CLM_VAL_CD_SK"),
    )
)

df_xfm_BusinessLogic = (
    df_lkp_Codes
    .withColumn(
        "SRC_SYS_CD",
        F.when(
            (F.col("SRC_SYS_CD").isNull()) | (F.length(trim(F.col("SRC_SYS_CD"))) == 0),
            F.lit("UNK")
        ).otherwise(F.col("SRC_SYS_CD"))
    )
    .withColumn(
        "CLM_SK",
        F.when(
            (F.col("CLM_SK").isNull()) | (F.length(trim(F.col("CLM_SK"))) == 0),
            F.lit(0)
        ).otherwise(F.col("CLM_SK"))
    )
    .withColumn(
        "FCLTY_CLM_VAL_CD",
        F.when(
            (F.col("FCLTY_CLM_VAL_CD").isNull()) | (F.length(trim(F.col("FCLTY_CLM_VAL_CD"))) == 0),
            F.lit("UNK")
        ).otherwise(F.col("FCLTY_CLM_VAL_CD"))
    )
    .withColumn(
        "FCLTY_CLM_VAL_NM",
        F.when(
            (F.col("FCLTY_CLM_VAL_NM").isNull()) | (F.length(trim(F.col("FCLTY_CLM_VAL_NM"))) == 0),
            F.lit("UNK")
        ).otherwise(F.col("FCLTY_CLM_VAL_NM"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
)

df_FcltyClmValFMainExtr = df_xfm_BusinessLogic.filter(
    (F.col("FCLTY_CLM_VAL_SK") != 0) & (F.col("FCLTY_CLM_VAL_SK") != 1)
)

df_UNK = spark.createDataFrame(
    [
        (
            0, "UNK", 0, "UNK", "1753-01-01", "1753-01-01",
            0, 0, "UNK", "UNK", 0, 0, 100, 100, 0
        )
    ],
    [
        "FCLTY_CLM_VAL_SK","CLM_ID","FCLTY_CLM_VAL_SEQ_NO","SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CLM_SK","FCLTY_CLM_SK","FCLTY_CLM_VAL_CD","FCLTY_CLM_VAL_NM",
        "VAL_AMT","VAL_UNIT_CT","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK",
        "FCLTY_CLM_VAL_CD_SK"
    ]
)

df_NA = spark.createDataFrame(
    [
        (
            1, "NA", 1, "NA", "1753-01-01", "1753-01-01",
            1, 1, "NA", "NA", 0, 0, 100, 100, 1
        )
    ],
    [
        "FCLTY_CLM_VAL_SK","CLM_ID","FCLTY_CLM_VAL_SEQ_NO","SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CLM_SK","FCLTY_CLM_SK","FCLTY_CLM_VAL_CD","FCLTY_CLM_VAL_NM",
        "VAL_AMT","VAL_UNIT_CT","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK",
        "FCLTY_CLM_VAL_CD_SK"
    ]
)

df_funnel_26 = (
    df_FcltyClmValFMainExtr.select(df_UNK.columns)
    .union(df_UNK)
    .union(df_NA)
)

df_final = df_funnel_26.select(
    "FCLTY_CLM_VAL_SK",
    "CLM_ID",
    "FCLTY_CLM_VAL_SEQ_NO",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CLM_SK",
    "FCLTY_CLM_SK",
    "FCLTY_CLM_VAL_CD",
    "FCLTY_CLM_VAL_NM",
    "VAL_AMT",
    "VAL_UNIT_CT",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "FCLTY_CLM_VAL_CD_SK"
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
)

write_files(
    df_final,
    f"{adls_path}/load/FCLTY_CLM_VAL_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)