# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:       FctsIdsWorkCompClmSeq
# MAGIC 
# MAGIC PROCESSING:   To load WORK_COMPNSTN_CLM_LN_DSALW with missing EXCD_SK's from extract job FctsIdsWorkCompClmLnDsalwExtr
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                     Development       Code                        Date 
# MAGIC Developer           Date             Project                                                                  Change Description                                                        Project\(9)                Reviewer                 Reviewed       
# MAGIC ------------------------   ------------------   --------------------------------------------------------------------------   ------------------------------------------------------------------------------------   -------------------------   -----------------------------   ------------------       
# MAGIC Kailashnath J      2017-03-15\(9)5628 WORK_COMPNSTN_CLM_LN_DSALW   Original Programming\(9)\(9)\(9)                    Integratedev2      Hugh Sisson            2017-03-21            
# MAGIC                                                 (Facets to IDS) ETL Report

# MAGIC This job loads WORK_COMPNSTN_CLM_LN_DSALW with missing EXCD_SK's from extract job FctsIdsWorkCompClmLnDsalwExtr
# MAGIC Excd Rej File with missing Excd_Sk's created in FctsIdsWorkCompClmLnDsalwExtr
# MAGIC This jobs runs only after EXCD table with missing EXCD_SK's is loaded.
# MAGIC Seq. file to load into the DB2 table B_WORK_COMPNSTN_CLM_LN_DSALW for Balancing Report in Apend Mode.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


RunCycle = get_widget_value('RunCycle','')
Env = get_widget_value('Env','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
WorkCompOwner = get_widget_value('WorkCompOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# ----------------------------------------------------------------------------
# Stage: Seq_Excd_Rej (PxSequentialFile)
# ----------------------------------------------------------------------------
schema_Seq_Excd_Rej = StructType([
    StructField("EXCD_ID", StringType(), True),
    StructField("CLCL_ID", StringType(), True),
    StructField("CDML_SEQ_NO", IntegerType(), True),
    StructField("CDMD_TYPE", StringType(), True),
    StructField("CDMD_DISALL_AMT", DecimalType(38, 10), True),
    StructField("TRGT_CD", StringType(), True),
    StructField("CD_MPPNG_SK", IntegerType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True)
])

df_Seq_Excd_Rej = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .schema(schema_Seq_Excd_Rej)
    .load(f"{adls_path_publish}/external/IDS_EXCD_REJ.dat")
)

# ----------------------------------------------------------------------------
# Stage: IDS_EXCD (DB2ConnectorPX) - Database: IDS
# ----------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT DISTINCT
    Trim(EXCD_ID) AS EXCD_ID,
    EXCD_SK
FROM
    {IDSOwner}.EXCD
"""
df_IDS_EXCD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: Lkp_Excd (PxLookup)
# ----------------------------------------------------------------------------
df_Lkp_Excd = (
    df_Seq_Excd_Rej.alias("Lnk_Excd_Rej")
    .join(
        df_IDS_EXCD.alias("Lnk_IDS_Excd"),
        F.col("Lnk_Excd_Rej.EXCD_ID") == F.col("Lnk_IDS_Excd.EXCD_ID"),
        "inner"
    )
    .select(
        F.col("Lnk_Excd_Rej.CLCL_ID").alias("CLCL_ID"),
        F.col("Lnk_Excd_Rej.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        F.col("Lnk_Excd_Rej.CDMD_TYPE").alias("CDMD_TYPE"),
        F.col("Lnk_Excd_Rej.CDMD_DISALL_AMT").alias("CDMD_DISALL_AMT"),
        F.col("Lnk_Excd_Rej.TRGT_CD").alias("TRGT_CD"),
        F.col("Lnk_Excd_Rej.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
        F.col("Lnk_Excd_Rej.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_IDS_Excd.EXCD_SK").alias("EXCD_SK")
    )
)

# ----------------------------------------------------------------------------
# Stage: Trns_Excd (CTransformerStage)
#    Output link 1 -> Lnk_IDS_EXCD -> WORK_COMPNSTN_CLM_LN_DSALW
#    Output link 2 -> Lnk_B_WorkCompnstnClmLnDsalw -> B_WORK_COMPNSTN_CLM_LN_DSALW
# ----------------------------------------------------------------------------
df_Trns_Excd_1 = (
    df_Lkp_Excd.alias("Lnk_Trns_Excd")
    .withColumn("RunCycle", F.lit(RunCycle))
    .withColumn("SrcSysCd", F.lit(SrcSysCd))
    .select(
        F.col("Lnk_Trns_Excd.CLCL_ID").alias("CLM_ID"),
        F.col("Lnk_Trns_Excd.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("Lnk_Trns_Excd.TRGT_CD").alias("CLM_LN_DSALW_TYP_CD"),
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        F.when(
            F.col("Lnk_Trns_Excd.CRT_RUN_CYC_EXCTN_SK").isNull() | (F.col("Lnk_Trns_Excd.CRT_RUN_CYC_EXCTN_SK") == 0),
            F.col("RunCycle")
        ).otherwise(F.col("Lnk_Trns_Excd.CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("RunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_Trns_Excd.EXCD_SK").alias("CLM_LN_DSALW_EXCD_SK"),
        F.when(
            F.col("Lnk_Trns_Excd.CD_MPPNG_SK").isNull() | (F.col("Lnk_Trns_Excd.CD_MPPNG_SK") == ""),
            F.lit(1)
        ).otherwise(F.col("Lnk_Trns_Excd.CD_MPPNG_SK")).alias("CLM_LN_DSALW_TYP_CAT_CD_SK"),
        F.col("Lnk_Trns_Excd.CDMD_DISALL_AMT").alias("DSALW_AMT")
    )
)

df_Trns_Excd_2 = (
    df_Lkp_Excd.alias("Lnk_Trns_Excd")
    .withColumn("SrcSysCd", F.lit(SrcSysCd))
    .select(
        F.col("Lnk_Trns_Excd.CLCL_ID").alias("CLM_ID"),
        F.col("Lnk_Trns_Excd.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("Lnk_Trns_Excd.TRGT_CD").alias("CLM_LN_DSALW_TYP_CD"),
        F.col("SrcSysCd").alias("SRC_SYS_CD")
    )
)

# Apply rpad for char columns in final frames:
df_Trns_Excd_1 = df_Trns_Excd_1.withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 12, " "))
df_Trns_Excd_2 = df_Trns_Excd_2.withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 12, " "))

# ----------------------------------------------------------------------------
# Stage: WORK_COMPNSTN_CLM_LN_DSALW (DB2ConnectorPX) - Database: IDS
# Upsert into #$WorkCompOwner#.WORK_COMPNSTN_CLM_LN_DSALW
# ----------------------------------------------------------------------------
table_name = "STAGING.FctsIdsWorkCompExcdSkClmLnDsalwLoad_WORK_COMPNSTN_CLM_LN_DSALW_temp"

execute_dml(f"DROP TABLE IF EXISTS {table_name}", jdbc_url, jdbc_props)

df_Trns_Excd_1.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", table_name).mode("append").save()

merge_sql = f"""
MERGE INTO {WorkCompOwner}.WORK_COMPNSTN_CLM_LN_DSALW AS T
USING {table_name} AS S
ON
    T.CLM_ID = S.CLM_ID
    AND T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO
    AND T.CLM_LN_DSALW_TYP_CD = S.CLM_LN_DSALW_TYP_CD
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.CLM_LN_DSALW_EXCD_SK = S.CLM_LN_DSALW_EXCD_SK,
    T.CLM_LN_DSALW_TYP_CAT_CD_SK = S.CLM_LN_DSALW_TYP_CAT_CD_SK,
    T.DSALW_AMT = S.DSALW_AMT
WHEN NOT MATCHED THEN
  INSERT (
    CLM_ID,
    CLM_LN_SEQ_NO,
    CLM_LN_DSALW_TYP_CD,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK,
    LAST_UPDT_RUN_CYC_EXCTN_SK,
    CLM_LN_DSALW_EXCD_SK,
    CLM_LN_DSALW_TYP_CAT_CD_SK,
    DSALW_AMT
  )
  VALUES (
    S.CLM_ID,
    S.CLM_LN_SEQ_NO,
    S.CLM_LN_DSALW_TYP_CD,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    S.CLM_LN_DSALW_EXCD_SK,
    S.CLM_LN_DSALW_TYP_CAT_CD_SK,
    S.DSALW_AMT
  );
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# ----------------------------------------------------------------------------
# Stage: B_WORK_COMPNSTN_CLM_LN_DSALW (PxSequentialFile)
# ----------------------------------------------------------------------------
write_files(
    df_Trns_Excd_2.select("CLM_ID", "CLM_LN_SEQ_NO", "CLM_LN_DSALW_TYP_CD", "SRC_SYS_CD"),
    f"{adls_path}/load/B_WORK_COMPNSTN_CLM_LN_DSALW.dat",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)