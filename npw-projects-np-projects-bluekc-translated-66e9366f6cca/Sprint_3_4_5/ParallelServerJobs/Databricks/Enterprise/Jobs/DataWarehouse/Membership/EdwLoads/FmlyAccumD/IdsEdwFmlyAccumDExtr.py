# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                          Code                   Date
# MAGIC Developer           Date              Altiris #        Change Description                                                                                     Reviewer            Reviewed
# MAGIC -------------------------  -------------------   ----------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Bhoomi Dasari    11/27/2007   Mbrshp       Originally Programmed                                                                                  Steph Goddard   11/28/2007  
# MAGIC SAndrew            2009-06-26     TTR539     Added IDS run cycle to extract                                                                    Steph Goddard   06/16/2009
# MAGIC Hugh Sisson      2012-08-10     TTR435      Added FMLY_CAROVR_AMT to end of table                                             SAndrew               2012-08-20    
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Archana Palivela 2013-07-11 P5114            Originally Programmed(In parallel)    
# MAGIC 
# MAGIC Karthik Chintalapani   2016-11-09     5634    Added new columns PLN_YR_EFF_DT and          EnterpriseDev2           Kalyan Neelam       2016-11-28
# MAGIC                                                                      PLN_YR_END_DT

# MAGIC Job name: IdsEdwFmlyAccumDExtr
# MAGIC 
# MAGIC 
# MAGIC Job Extract data from IDS table and apply Fkey and Code denormalization, create a load ready file.
# MAGIC Read data from source table FMLY_ACCUM
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC FMLY_ACCUM_TYP_CD
# MAGIC Write FMLY_ACCUM_D Data into a Sequential file for Load Ready Job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, when, length, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

# Stage: db2_IdsEdwFmlyAccumD_Extr
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_db2_IdsEdwFmlyAccumD_Extr = f"""SELECT 
FMLY_ACCUM.FMLY_ACCUM_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
FMLY_ACCUM.SUB_UNIQ_KEY,
FMLY_ACCUM.PROD_ACCUM_ID,
FMLY_ACCUM.FMLY_ACCUM_TYP_CD_SK,
FMLY_ACCUM.ACCUM_NO,
FMLY_ACCUM.YR_NO,
FMLY_ACCUM.GRP_SK,
FMLY_ACCUM.SUB_SK,
FMLY_ACCUM.ACCUM_AMT,
FMLY_ACCUM.CAROVR_AMT,
FMLY_ACCUM.PLN_YR_EFF_DT,
FMLY_ACCUM.PLN_YR_END_DT
FROM {IDSOwner}.FMLY_ACCUM FMLY_ACCUM
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON FMLY_ACCUM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE FMLY_ACCUM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
"""
df_db2_IdsEdwFmlyAccumD_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_IdsEdwFmlyAccumD_Extr)
    .load()
)
df_Ink_IdsEdwFmlyAccumDExtr_InABC = df_db2_IdsEdwFmlyAccumD_Extr

# Stage: db2_CD_MPPNG_Extr
extract_query_db2_CD_MPPNG_Extr = f"""SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)
df_Ref_FmlyAccumTyp = df_db2_CD_MPPNG_Extr

# Stage: db2_ACCUM
extract_query_db2_ACCUM = f"SELECT * FROM {IDSOwner}.ACCUM"
df_db2_ACCUM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_ACCUM)
    .load()
)
df_Lkp_Accum_Desc = df_db2_ACCUM

# Stage: lkp_Codes (PxLookup)
df_lkp_Codes = (
    df_Ink_IdsEdwFmlyAccumDExtr_InABC.alias("p")
    .join(df_Ref_FmlyAccumTyp.alias("f"), col("p.FMLY_ACCUM_TYP_CD_SK") == col("f.CD_MPPNG_SK"), "left")
    .join(
        df_Lkp_Accum_Desc.alias("a"),
        [
            col("p.ACCUM_NO") == col("a.ACCUM_NO"),
            col("p.FMLY_ACCUM_TYP_CD_SK") == col("a.ACCUM_TYP_CD_SK")
        ],
        "left"
    )
    .select(
        col("p.FMLY_ACCUM_SK").alias("FMLY_ACCUM_SK"),
        col("p.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("p.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        col("p.PROD_ACCUM_ID").alias("PROD_ACCUM_ID"),
        col("f.TRGT_CD").alias("FMLY_ACCUM_TYP_CD"),
        col("f.TRGT_CD_NM").alias("FMLY_ACCUM_TYP_DESC"),
        col("p.ACCUM_NO").alias("FMLY_ACCUM_NO"),
        col("p.YR_NO").alias("FMLY_ACCUM_YR_NO"),
        col("p.GRP_SK").alias("GRP_SK"),
        col("p.SUB_SK").alias("SUB_SK"),
        col("p.ACCUM_AMT").alias("FMLY_ACCUM_AMT"),
        col("p.FMLY_ACCUM_TYP_CD_SK").alias("FMLY_ACCUM_TYP_CD_SK"),
        col("p.CAROVR_AMT").alias("FMLY_CAROVR_AMT"),
        col("p.PLN_YR_EFF_DT").alias("PLN_YR_EFF_DT"),
        col("p.PLN_YR_END_DT").alias("PLN_YR_END_DT"),
        col("a.ACCUM_DESC").alias("ACCUM_DESC")
    )
)
df_lnk_CodesLkpData_out = df_lkp_Codes

# Stage: xmf_businessLogic (CTransformerStage)
df_xmf_businessLogic = (
    df_lnk_CodesLkpData_out
    .withColumn("SRC_SYS_CD", when(trim(col("SRC_SYS_CD")) == "", lit("UNK")).otherwise(col("SRC_SYS_CD")))
    .withColumn("FMLY_ACCUM_TYP_CD", when(trim(col("FMLY_ACCUM_TYP_CD")) == "", lit("UNK")).otherwise(col("FMLY_ACCUM_TYP_CD")))
    .withColumn("FMLY_ACCUM_TYP_DESC", when(trim(col("FMLY_ACCUM_TYP_DESC")) == "", lit("UNK")).otherwise(col("FMLY_ACCUM_TYP_DESC")))
    .withColumn("FMLY_CAROVR_AMT", when(col("FMLY_CAROVR_AMT").isNull() | (length(col("FMLY_CAROVR_AMT")) == 0), lit(0)).otherwise(col("FMLY_CAROVR_AMT")))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
)

df_xmf_businessLogic_final = (
    df_xmf_businessLogic
    .withColumnRenamed("ACCUM_DESC", "PROD_ACCUM_DESC")
    .select(
        "FMLY_ACCUM_SK",
        "SRC_SYS_CD",
        "SUB_UNIQ_KEY",
        "PROD_ACCUM_ID",
        "FMLY_ACCUM_TYP_CD",
        "FMLY_ACCUM_NO",
        "FMLY_ACCUM_YR_NO",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "GRP_SK",
        "SUB_SK",
        "FMLY_ACCUM_AMT",
        "FMLY_ACCUM_TYP_DESC",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "FMLY_ACCUM_TYP_CD_SK",
        "FMLY_CAROVR_AMT",
        "PLN_YR_EFF_DT",
        "PLN_YR_END_DT",
        "PROD_ACCUM_DESC"
    )
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
)

# Stage: Seq_FMLY_ACCUM_D_Load (PxSequentialFile)
write_files(
    df_xmf_businessLogic_final,
    f"{adls_path}/load/FMLY_ACCUM_D.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)