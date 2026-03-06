# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Rama Kamjula         09/20/2013          5114                             Original Programming                                                                             IntegrateWrhsDevl   Peter Marshall               10/22/2013     
# MAGIC Kalyan Neelam        2013-11-18           5234 Compass               Added new column CMPSS_COV_IN, ALNO_HOME_PG_ID             IntegrateNewDevl      Bhoomi Dasari              12/09/2013       
# MAGIC Karthik Chintalapani  2013-01-21         5131                            Updated the Datatype  for  column ALNO_HOME_PG_ID 
# MAGIC                                                                                                  from CHAR to VARCHAR                                                                       IntegrateNewDevl        Kalyan Neelam            2014-01-28
# MAGIC 
# MAGIC Karthik Chintalapani  2013-01-21         5108                             Added new  column  PCMH_IN
# MAGIC                                                                                                                                                                                                                 IntegrateNewDevl         Kalyan Neelam            2014-03-31
# MAGIC 
# MAGIC Abhiram Dasarathy	2016-12-06         5217 Dental Network	 Added DNTL_NTWRK_IN field to the end of the columns		IntegrateDev2               Kalyan Neelam            2016-12-06

# MAGIC Job Name:  IdsIdsPMbrSuplmtBnfLoad
# MAGIC 
# MAGIC Loads dat file into P_MBR_SUPLMT_BNF table.
# MAGIC Read Load File created in the IdsIdsPMbrSuplmtBnfExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Load Method: Replace
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_seq_P_MBR_SUPLMT_BNF = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("MNTL_HLTH_VNDR_ID", StringType(), True),
    StructField("PBM_VNDR_CD", StringType(), True),
    StructField("WELNS_BNF_LVL_CD", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("WELNS_BNF_VNDR_ID", StringType(), True),
    StructField("EAP_BNF_LVL_CD", StringType(), True),
    StructField("VSN_BNF_VNDR_ID", StringType(), True),
    StructField("VSN_RTN_EXAM_IN", StringType(), True),
    StructField("VSN_HRDWR_IN", StringType(), True),
    StructField("VSN_OUT_OF_NTWK_EXAM_IN", StringType(), True),
    StructField("OTHR_CAR_PRI_MED_IN", StringType(), True),
    StructField("VBB_ENR_IN", StringType(), True),
    StructField("VBB_MDL_CD", StringType(), True),
    StructField("CMPSS_COV_IN", StringType(), True),
    StructField("ALNO_HOME_PG_ID", StringType(), True),
    StructField("PCMH_IN", StringType(), True),
    StructField("DNTL_RWRD_IN", StringType(), True)
])

df_seq_P_MBR_SUPLMT_BNF = (
    spark.read.format("csv")
    .schema(schema_seq_P_MBR_SUPLMT_BNF)
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .load(f"{adls_path}/load/P_MBR_SUPLMT_BNF.dat")
)

df_cpy_forBuffer = df_seq_P_MBR_SUPLMT_BNF.select(
    col("SRC_SYS_CD_SK"),
    col("MBR_UNIQ_KEY"),
    col("MNTL_HLTH_VNDR_ID"),
    col("PBM_VNDR_CD"),
    col("WELNS_BNF_LVL_CD"),
    col("LAST_UPDT_RUN_CYC_NO"),
    col("WELNS_BNF_VNDR_ID"),
    col("EAP_BNF_LVL_CD"),
    col("VSN_BNF_VNDR_ID"),
    col("VSN_RTN_EXAM_IN"),
    col("VSN_HRDWR_IN"),
    col("VSN_OUT_OF_NTWK_EXAM_IN"),
    col("OTHR_CAR_PRI_MED_IN"),
    col("VBB_ENR_IN"),
    col("VBB_MDL_CD"),
    col("CMPSS_COV_IN"),
    col("ALNO_HOME_PG_ID"),
    col("PCMH_IN"),
    col("DNTL_RWRD_IN")
)

df_db2_P_MBR_SUPLMT_BNF = df_cpy_forBuffer.select(
    col("SRC_SYS_CD_SK"),
    col("MBR_UNIQ_KEY"),
    rpad(col("MNTL_HLTH_VNDR_ID"), <...>, " ").alias("MNTL_HLTH_VNDR_ID"),
    rpad(col("PBM_VNDR_CD"), <...>, " ").alias("PBM_VNDR_CD"),
    rpad(col("WELNS_BNF_LVL_CD"), <...>, " ").alias("WELNS_BNF_LVL_CD"),
    col("LAST_UPDT_RUN_CYC_NO"),
    rpad(col("WELNS_BNF_VNDR_ID"), <...>, " ").alias("WELNS_BNF_VNDR_ID"),
    rpad(col("EAP_BNF_LVL_CD"), <...>, " ").alias("EAP_BNF_LVL_CD"),
    rpad(col("VSN_BNF_VNDR_ID"), <...>, " ").alias("VSN_BNF_VNDR_ID"),
    rpad(col("VSN_RTN_EXAM_IN"), 1, " ").alias("VSN_RTN_EXAM_IN"),
    rpad(col("VSN_HRDWR_IN"), 1, " ").alias("VSN_HRDWR_IN"),
    rpad(col("VSN_OUT_OF_NTWK_EXAM_IN"), 1, " ").alias("VSN_OUT_OF_NTWK_EXAM_IN"),
    rpad(col("OTHR_CAR_PRI_MED_IN"), 1, " ").alias("OTHR_CAR_PRI_MED_IN"),
    rpad(col("VBB_ENR_IN"), 1, " ").alias("VBB_ENR_IN"),
    rpad(col("VBB_MDL_CD"), <...>, " ").alias("VBB_MDL_CD"),
    rpad(col("CMPSS_COV_IN"), 1, " ").alias("CMPSS_COV_IN"),
    rpad(col("ALNO_HOME_PG_ID"), <...>, " ").alias("ALNO_HOME_PG_ID"),
    rpad(col("PCMH_IN"), 1, " ").alias("PCMH_IN"),
    rpad(col("DNTL_RWRD_IN"), 1, " ").alias("DNTL_RWRD_IN")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsIdsPMbrSuplmtBnfLoad_db2_P_MBR_SUPLMT_BNF_temp", jdbc_url, jdbc_props)

(
    df_db2_P_MBR_SUPLMT_BNF.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.IdsIdsPMbrSuplmtBnfLoad_db2_P_MBR_SUPLMT_BNF_temp")
    .mode("overwrite")
    .save()
)

execute_dml(f"TRUNCATE TABLE {IDSOwner}.P_MBR_SUPLMT_BNF", jdbc_url, jdbc_props)

merge_sql = f"""
MERGE INTO {IDSOwner}.P_MBR_SUPLMT_BNF AS T
USING STAGING.IdsIdsPMbrSuplmtBnfLoad_db2_P_MBR_SUPLMT_BNF_temp AS S
ON T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
   AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
WHEN MATCHED THEN
  UPDATE SET
    T.MNTL_HLTH_VNDR_ID = S.MNTL_HLTH_VNDR_ID,
    T.PBM_VNDR_CD = S.PBM_VNDR_CD,
    T.WELNS_BNF_LVL_CD = S.WELNS_BNF_LVL_CD,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
    T.WELNS_BNF_VNDR_ID = S.WELNS_BNF_VNDR_ID,
    T.EAP_BNF_LVL_CD = S.EAP_BNF_LVL_CD,
    T.VSN_BNF_VNDR_ID = S.VSN_BNF_VNDR_ID,
    T.VSN_RTN_EXAM_IN = S.VSN_RTN_EXAM_IN,
    T.VSN_HRDWR_IN = S.VSN_HRDWR_IN,
    T.VSN_OUT_OF_NTWK_EXAM_IN = S.VSN_OUT_OF_NTWK_EXAM_IN,
    T.OTHR_CAR_PRI_MED_IN = S.OTHR_CAR_PRI_MED_IN,
    T.VBB_ENR_IN = S.VBB_ENR_IN,
    T.VBB_MDL_CD = S.VBB_MDL_CD,
    T.CMPSS_COV_IN = S.CMPSS_COV_IN,
    T.ALNO_HOME_PG_ID = S.ALNO_HOME_PG_ID,
    T.PCMH_IN = S.PCMH_IN,
    T.DNTL_RWRD_IN = S.DNTL_RWRD_IN
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD_SK,
    MBR_UNIQ_KEY,
    MNTL_HLTH_VNDR_ID,
    PBM_VNDR_CD,
    WELNS_BNF_LVL_CD,
    LAST_UPDT_RUN_CYC_NO,
    WELNS_BNF_VNDR_ID,
    EAP_BNF_LVL_CD,
    VSN_BNF_VNDR_ID,
    VSN_RTN_EXAM_IN,
    VSN_HRDWR_IN,
    VSN_OUT_OF_NTWK_EXAM_IN,
    OTHR_CAR_PRI_MED_IN,
    VBB_ENR_IN,
    VBB_MDL_CD,
    CMPSS_COV_IN,
    ALNO_HOME_PG_ID,
    PCMH_IN,
    DNTL_RWRD_IN
  )
  VALUES (
    S.SRC_SYS_CD_SK,
    S.MBR_UNIQ_KEY,
    S.MNTL_HLTH_VNDR_ID,
    S.PBM_VNDR_CD,
    S.WELNS_BNF_LVL_CD,
    S.LAST_UPDT_RUN_CYC_NO,
    S.WELNS_BNF_VNDR_ID,
    S.EAP_BNF_LVL_CD,
    S.VSN_BNF_VNDR_ID,
    S.VSN_RTN_EXAM_IN,
    S.VSN_HRDWR_IN,
    S.VSN_OUT_OF_NTWK_EXAM_IN,
    S.OTHR_CAR_PRI_MED_IN,
    S.VBB_ENR_IN,
    S.VBB_MDL_CD,
    S.CMPSS_COV_IN,
    S.ALNO_HOME_PG_ID,
    S.PCMH_IN,
    S.DNTL_RWRD_IN
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

reject_schema = StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])
df_lnk_IdsIdsPMbrSuplmtBnfLoad_RejABC = spark.createDataFrame([], reject_schema)

write_files(
    df_lnk_IdsIdsPMbrSuplmtBnfLoad_RejABC,
    f"{adls_path}/load/P_MBR_SUPLMT_BNF_Rej.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)