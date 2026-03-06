# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Nagesh Bandi         08/05/2013          5114                             Movies Data from IDS to  MBRSH_DM_MBR_PRI_CARE_PROV      IntegrateWrhsDevl     Peter Marshall               10/23/2013

# MAGIC Job Name: IdsDmMbrshDmMbrPriCareProvLoad
# MAGIC Read Load File created in the IdsDmMbrshDmMbrPriCareProvExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the MBRSH_DM_MBR_PRI_CARE_PROV  Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_lMBRSH_DM_MBR_PRI_CARE_PROV_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("MBR_PCP_TYP_CD", StringType(), True),
    StructField("MBR_PCP_EFF_DT", TimestampType(), True),
    StructField("MBR_PCP_TERM_RSN_CD", StringType(), True),
    StructField("MBR_PCP_TERM_RSN_NM", StringType(), True),
    StructField("MBR_PCP_TYP_NM", StringType(), True),
    StructField("MBR_PCP_TERM_DT", TimestampType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("GRP_UNIQ_KEY", IntegerType(), True),
    StructField("GRP_NM", StringType(), True),
    StructField("PROV_ID", StringType(), True),
    StructField("PROV_NM", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True),
    StructField("PROV_PRI_PRCTC_ADDR_PHN_NO", StringType(), True),
    StructField("PROV_PRI_PRCTC_ADDR_PHN_NO_EXT", StringType(), True)
])

df_seq_lMBRSH_DM_MBR_PRI_CARE_PROV_csv_load = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .schema(schema_seq_lMBRSH_DM_MBR_PRI_CARE_PROV_csv_load)
    .load(f"{adls_path}/load/MBRSH_DM_MBR_PRI_CARE_PROV.dat")
)

df_cpy_forBuffer = df_seq_lMBRSH_DM_MBR_PRI_CARE_PROV_csv_load.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_PCP_TYP_CD").alias("MBR_PCP_TYP_CD"),
    F.col("MBR_PCP_EFF_DT").alias("MBR_PCP_EFF_DT"),
    F.col("MBR_PCP_TERM_RSN_CD").alias("MBR_PCP_TERM_RSN_CD"),
    F.col("MBR_PCP_TERM_RSN_NM").alias("MBR_PCP_TERM_RSN_NM"),
    F.col("MBR_PCP_TYP_NM").alias("MBR_PCP_TYP_NM"),
    F.col("MBR_PCP_TERM_DT").alias("MBR_PCP_TERM_DT"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("PROV_NM").alias("PROV_NM"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    F.col("PROV_PRI_PRCTC_ADDR_PHN_NO").alias("PROV_PRI_PRCTC_ADDR_PHN_NO"),
    F.col("PROV_PRI_PRCTC_ADDR_PHN_NO_EXT").alias("PROV_PRI_PRCTC_ADDR_PHN_NO_EXT")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

spark.sql("DROP TABLE IF EXISTS STAGING.IdsDmMbrshDmMbrPriCareProvLoad_Odbc_MBRSH_DM_MBR_PRI_CARE_PROV_out_temp")

df_cpy_forBuffer.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmMbrshDmMbrPriCareProvLoad_Odbc_MBRSH_DM_MBR_PRI_CARE_PROV_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.MBRSH_DM_MBR_PRI_CARE_PROV AS T
USING STAGING.IdsDmMbrshDmMbrPriCareProvLoad_Odbc_MBRSH_DM_MBR_PRI_CARE_PROV_out_temp AS S
ON 
(
    T.SRC_SYS_CD = S.SRC_SYS_CD 
    AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
    AND T.MBR_PCP_TYP_CD = S.MBR_PCP_TYP_CD
    AND T.MBR_PCP_EFF_DT = S.MBR_PCP_EFF_DT
)
WHEN MATCHED THEN
  UPDATE SET
    T.MBR_PCP_TERM_RSN_CD = S.MBR_PCP_TERM_RSN_CD,
    T.MBR_PCP_TERM_RSN_NM = S.MBR_PCP_TERM_RSN_NM,
    T.MBR_PCP_TYP_NM = S.MBR_PCP_TYP_NM,
    T.MBR_PCP_TERM_DT = S.MBR_PCP_TERM_DT,
    T.GRP_ID = S.GRP_ID,
    T.GRP_UNIQ_KEY = S.GRP_UNIQ_KEY,
    T.GRP_NM = S.GRP_NM,
    T.PROV_ID = S.PROV_ID,
    T.PROV_NM = S.PROV_NM,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
    T.PROV_PRI_PRCTC_ADDR_PHN_NO = S.PROV_PRI_PRCTC_ADDR_PHN_NO,
    T.PROV_PRI_PRCTC_ADDR_PHN_NO_EXT = S.PROV_PRI_PRCTC_ADDR_PHN_NO_EXT
WHEN NOT MATCHED THEN
  INSERT 
  (
    SRC_SYS_CD,
    MBR_UNIQ_KEY,
    MBR_PCP_TYP_CD,
    MBR_PCP_EFF_DT,
    MBR_PCP_TERM_RSN_CD,
    MBR_PCP_TERM_RSN_NM,
    MBR_PCP_TYP_NM,
    MBR_PCP_TERM_DT,
    GRP_ID,
    GRP_UNIQ_KEY,
    GRP_NM,
    PROV_ID,
    PROV_NM,
    LAST_UPDT_RUN_CYC_NO,
    PROV_PRI_PRCTC_ADDR_PHN_NO,
    PROV_PRI_PRCTC_ADDR_PHN_NO_EXT
  )
  VALUES
  (
    S.SRC_SYS_CD,
    S.MBR_UNIQ_KEY,
    S.MBR_PCP_TYP_CD,
    S.MBR_PCP_EFF_DT,
    S.MBR_PCP_TERM_RSN_CD,
    S.MBR_PCP_TERM_RSN_NM,
    S.MBR_PCP_TYP_NM,
    S.MBR_PCP_TERM_DT,
    S.GRP_ID,
    S.GRP_UNIQ_KEY,
    S.GRP_NM,
    S.PROV_ID,
    S.PROV_NM,
    S.LAST_UPDT_RUN_CYC_NO,
    S.PROV_PRI_PRCTC_ADDR_PHN_NO,
    S.PROV_PRI_PRCTC_ADDR_PHN_NO_EXT
  );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_Odbc_MBRSH_DM_MBR_PRI_CARE_PROV_out_rej = spark.createDataFrame([], StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
]))

df_Odbc_MBRSH_DM_MBR_PRI_CARE_PROV_out_rej = df_Odbc_MBRSH_DM_MBR_PRI_CARE_PROV_out_rej.select(
    F.rpad(F.col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    F.rpad(F.col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_Odbc_MBRSH_DM_MBR_PRI_CARE_PROV_out_rej,
    f"{adls_path}/load/MBRSH_DM_MBR_PRI_CARE_PROV_Rej.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)