# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name : IdsHedisMesrGapDplyPKey
# MAGIC 
# MAGIC Called By: IdsMbrHedisMesrGapCntl
# MAGIC 
# MAGIC                    
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Goutham Kalidindi             2024-09-10        US-628396                                 Orig Development                                        IntegrateDev2        Reddy Sanam             09-30-2024

# MAGIC Create Primary Key for HEDIS_MESR table
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Left Outer Join Here
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Land into Seq File for the FKEY job
# MAGIC Table K_HEDIS_MESR_GAP_DPLY.Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value("RunID","")
RunDate = get_widget_value("RunDate","")
RunCycExctnSK_param = get_widget_value("RunCycExctnSK","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
SrcSysCd = get_widget_value("SrcSysCd","")
IDSDB = get_widget_value("IDSDB","")
IDSAcct = get_widget_value("IDSAcct","")
IDSPW = get_widget_value("IDSPW","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT HEDIS_MESR_GAP_DPLY_SK,HEDIS_MESR_NM,HEDIS_SUB_MESR_NM,HEDIS_MBR_BUCKET_ID, MOD_ID, HEDIS_MESR_GAP_DPLY_MOD_EFF_DT,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.K_HEDIS_MESR_GAP_DPLY"
df_db2_K_HEDIS_MESR_GAP_DPLY_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

schema_HEDIS_MESR_GAP_DPLY = StructType([
    StructField("HEDIS_MESR_NM", StringType(), False),
    StructField("HEDIS_SUB_MESR_NM", StringType(), False),
    StructField("HEDIS_MBR_BUCKET_ID", StringType(), False),
    StructField("MOD_ID", StringType(), False),
    StructField("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT", TimestampType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("HEDIS_MESR_GAP_DPLY_MOD_TERM_DT", TimestampType(), False),
    StructField("HEDIS_MESR_GAP_MOD_PRTY_NO", IntegerType(), False),
    StructField("HEDIS_MESR_ABBR_ID", StringType(), False),
    StructField("HEDIS_MESR_GAP_MOD_CAT_ID", StringType(), False),
    StructField("HEDIS_MESR_GAP_MOD_DPLY_TX", StringType(), False),
    StructField("HEDIS_MESR_GAP_MOD_SCRIPT_TX", StringType(), False),
    StructField("HEDIS_MESR_MOD_KEY_ID", StringType(), False)
])
df_HEDIS_MESR_GAP_DPLY = (
    spark.read
    .option("delimiter", "^")
    .option("header", False)
    .option("quote", "\"")
    .option("escape", "\"")
    .option("nullValue", None)
    .schema(schema_HEDIS_MESR_GAP_DPLY)
    .csv(f"{adls_path}/verified/K_HEDIS_MESR_GAP_DPLY.dat")
)

df_lnkRemDupDataIn = df_HEDIS_MESR_GAP_DPLY.select(
    F.col("HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
    F.col("HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
    F.col("HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
    F.col("MOD_ID").alias("MOD_ID"),
    F.col("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT").alias("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_lnkFullDataJnIn = df_HEDIS_MESR_GAP_DPLY.select(
    F.col("HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
    F.col("HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
    F.col("HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
    F.col("MOD_ID").alias("MOD_ID"),
    F.col("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT").alias("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT"),
    F.col("HEDIS_MESR_GAP_DPLY_MOD_TERM_DT").alias("HEDIS_MESR_GAP_DPLY_MOD_TERM_DT"),
    F.col("HEDIS_MESR_GAP_MOD_PRTY_NO").alias("HEDIS_MESR_GAP_MOD_PRTY_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("HEDIS_MESR_ABBR_ID").alias("HEDIS_MESR_ABBR_ID"),
    F.col("HEDIS_MESR_GAP_MOD_CAT_ID").alias("HEDIS_MESR_GAP_MOD_CAT_ID"),
    F.col("HEDIS_MESR_GAP_MOD_DPLY_TX").alias("HEDIS_MESR_GAP_MOD_DPLY_TX"),
    F.col("HEDIS_MESR_GAP_MOD_SCRIPT_TX").alias("HEDIS_MESR_GAP_MOD_SCRIPT_TX"),
    F.col("HEDIS_MESR_MOD_KEY_ID").alias("HEDIS_MESR_MOD_KEY_ID")
)

df_lnkRemDupDataOut = dedup_sort(
    df_lnkRemDupDataIn,
    [
        "HEDIS_MESR_NM",
        "HEDIS_SUB_MESR_NM",
        "HEDIS_MBR_BUCKET_ID",
        "MOD_ID",
        "HEDIS_MESR_GAP_DPLY_MOD_EFF_DT",
        "SRC_SYS_CD"
    ],
    []
)

df_join_jn_K_MESR_GAP_DPLY = (
    df_lnkRemDupDataOut.alias("lnkRemDupDataOut")
    .join(
        df_db2_K_HEDIS_MESR_GAP_DPLY_In.alias("Extr"),
        on=[
            F.col("lnkRemDupDataOut.HEDIS_MESR_NM") == F.col("Extr.HEDIS_MESR_NM"),
            F.col("lnkRemDupDataOut.HEDIS_SUB_MESR_NM") == F.col("Extr.HEDIS_SUB_MESR_NM"),
            F.col("lnkRemDupDataOut.HEDIS_MBR_BUCKET_ID") == F.col("Extr.HEDIS_MBR_BUCKET_ID"),
            F.col("lnkRemDupDataOut.MOD_ID") == F.col("Extr.MOD_ID"),
            F.col("lnkRemDupDataOut.HEDIS_MESR_GAP_DPLY_MOD_EFF_DT") == F.col("Extr.HEDIS_MESR_GAP_DPLY_MOD_EFF_DT"),
            F.col("lnkRemDupDataOut.SRC_SYS_CD") == F.col("Extr.SRC_SYS_CD")
        ],
        how="left"
    )
)

df_JoinOut = df_join_jn_K_MESR_GAP_DPLY.select(
    F.col("lnkRemDupDataOut.HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
    F.col("lnkRemDupDataOut.HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
    F.col("lnkRemDupDataOut.HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
    F.col("lnkRemDupDataOut.MOD_ID").alias("MOD_ID"),
    F.col("lnkRemDupDataOut.HEDIS_MESR_GAP_DPLY_MOD_EFF_DT").alias("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT"),
    F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Extr.HEDIS_MESR_GAP_DPLY_SK").alias("HEDIS_MESR_GAP_DPLY_SK"),
    F.col("Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

df_xfm = (
    df_JoinOut
    .withColumn("original_HEDIS_MESR_GAP_DPLY_SK", F.col("HEDIS_MESR_GAP_DPLY_SK"))
    .withColumn(
        "isNew",
        (F.col("HEDIS_MESR_GAP_DPLY_SK").isNull()) | (F.col("HEDIS_MESR_GAP_DPLY_SK") == 0)
    )
    .withColumn(
        "HEDIS_MESR_GAP_DPLY_SK",
        F.when(
            (F.col("HEDIS_MESR_GAP_DPLY_SK").isNull()) | (F.col("HEDIS_MESR_GAP_DPLY_SK") == 0),
            F.lit(None)
        ).otherwise(F.col("HEDIS_MESR_GAP_DPLY_SK"))
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        F.when(
            (F.col("original_HEDIS_MESR_GAP_DPLY_SK").isNull()) | (F.col("original_HEDIS_MESR_GAP_DPLY_SK") == 0),
            F.lit(RunCycExctnSK_param)
        ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
    )
)

df_enriched = SurrogateKeyGen(df_xfm, <DB sequence name>, "HEDIS_MESR_GAP_DPLY_SK", <schema>, <secret_name>)

df_New = (
    df_enriched
    .filter(F.col("isNew") == True)
    .select(
        F.col("HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
        F.col("HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
        F.col("HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
        F.col("MOD_ID").alias("MOD_ID"),
        F.col("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT").alias("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("HEDIS_MESR_GAP_DPLY_SK").alias("HEDIS_MESR_GAP_DPLY_SK")
    )
)

df_lnkPKEYxfmOut = df_enriched.select(
    F.col("HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
    F.col("HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
    F.col("HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
    F.col("MOD_ID").alias("MOD_ID"),
    F.col("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT").alias("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycExctnSK_param).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("HEDIS_MESR_GAP_DPLY_SK").alias("HEDIS_MESR_GAP_DPLY_SK")
)

drop_sql = "DROP TABLE IF EXISTS STAGING.IdsHedisMesrGapDplyPKey_db2_K_HEDIS_MESR_GAP_DPLY_Load_temp"
execute_dml(drop_sql, jdbc_url, jdbc_props)

df_New.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsHedisMesrGapDplyPKey_db2_K_HEDIS_MESR_GAP_DPLY_Load_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.K_HEDIS_MESR_GAP_DPLY AS T
USING STAGING.IdsHedisMesrGapDplyPKey_db2_K_HEDIS_MESR_GAP_DPLY_Load_temp AS S
ON T.HEDIS_MESR_GAP_DPLY_SK = S.HEDIS_MESR_GAP_DPLY_SK
WHEN MATCHED THEN
  UPDATE SET
    T.HEDIS_MESR_NM = S.HEDIS_MESR_NM,
    T.HEDIS_SUB_MESR_NM = S.HEDIS_SUB_MESR_NM,
    T.HEDIS_MBR_BUCKET_ID = S.HEDIS_MBR_BUCKET_ID,
    T.MOD_ID = S.MOD_ID,
    T.HEDIS_MESR_GAP_DPLY_MOD_EFF_DT = S.HEDIS_MESR_GAP_DPLY_MOD_EFF_DT,
    T.SRC_SYS_CD = S.SRC_SYS_CD,
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT (
    HEDIS_MESR_NM,
    HEDIS_SUB_MESR_NM,
    HEDIS_MBR_BUCKET_ID,
    MOD_ID,
    HEDIS_MESR_GAP_DPLY_MOD_EFF_DT,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK,
    HEDIS_MESR_GAP_DPLY_SK
  )
  VALUES (
    S.HEDIS_MESR_NM,
    S.HEDIS_SUB_MESR_NM,
    S.HEDIS_MBR_BUCKET_ID,
    S.MOD_ID,
    S.HEDIS_MESR_GAP_DPLY_MOD_EFF_DT,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.HEDIS_MESR_GAP_DPLY_SK
  );
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_jn_PKEYs = (
    df_lnkFullDataJnIn.alias("lnkFullDataJnIn")
    .join(
        df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
        on=[
            F.col("lnkFullDataJnIn.HEDIS_SUB_MESR_NM") == F.col("lnkPKEYxfmOut.HEDIS_SUB_MESR_NM"),
            F.col("lnkFullDataJnIn.HEDIS_MESR_NM") == F.col("lnkPKEYxfmOut.HEDIS_MESR_NM"),
            F.col("lnkFullDataJnIn.HEDIS_MBR_BUCKET_ID") == F.col("lnkPKEYxfmOut.HEDIS_MBR_BUCKET_ID"),
            F.col("lnkFullDataJnIn.MOD_ID") == F.col("lnkPKEYxfmOut.MOD_ID"),
            F.col("lnkFullDataJnIn.HEDIS_MESR_GAP_DPLY_MOD_EFF_DT") == F.col("lnkPKEYxfmOut.HEDIS_MESR_GAP_DPLY_MOD_EFF_DT"),
            F.col("lnkFullDataJnIn.SRC_SYS_CD") == F.col("lnkPKEYxfmOut.SRC_SYS_CD")
        ],
        how="inner"
    )
)

df_Pkey_Out = df_jn_PKEYs.select(
    F.col("lnkPKEYxfmOut.HEDIS_MESR_GAP_DPLY_SK").alias("HEDIS_MESR_GAP_DPLY_SK"),
    F.col("lnkFullDataJnIn.HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
    F.col("lnkFullDataJnIn.HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
    F.col("lnkFullDataJnIn.HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
    F.col("lnkFullDataJnIn.MOD_ID").alias("MOD_ID"),
    F.col("lnkFullDataJnIn.HEDIS_MESR_GAP_DPLY_MOD_EFF_DT").alias("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnkFullDataJnIn.HEDIS_MESR_GAP_DPLY_MOD_TERM_DT").alias("HEDIS_MESR_GAP_DPLY_MOD_TERM_DT"),
    F.col("lnkFullDataJnIn.HEDIS_MESR_GAP_MOD_PRTY_NO").alias("HEDIS_MESR_GAP_MOD_PRTY_NO"),
    F.col("lnkFullDataJnIn.HEDIS_MESR_ABBR_ID").alias("HEDIS_MESR_ABBR_ID"),
    F.col("lnkFullDataJnIn.HEDIS_MESR_GAP_MOD_CAT_ID").alias("HEDIS_MESR_GAP_MOD_CAT_ID"),
    F.col("lnkFullDataJnIn.HEDIS_MESR_GAP_MOD_DPLY_TX").alias("HEDIS_MESR_GAP_MOD_DPLY_TX"),
    F.col("lnkFullDataJnIn.HEDIS_MESR_GAP_MOD_SCRIPT_TX").alias("HEDIS_MESR_GAP_MOD_SCRIPT_TX"),
    F.col("lnkFullDataJnIn.HEDIS_MESR_MOD_KEY_ID").alias("HEDIS_MESR_MOD_KEY_ID")
)

df_final = df_Pkey_Out.select(
    F.col("HEDIS_MESR_GAP_DPLY_SK"),
    F.rpad(F.col("HEDIS_MESR_NM"), <...>, " ").alias("HEDIS_MESR_NM"),
    F.rpad(F.col("HEDIS_SUB_MESR_NM"), <...>, " ").alias("HEDIS_SUB_MESR_NM"),
    F.rpad(F.col("HEDIS_MBR_BUCKET_ID"), <...>, " ").alias("HEDIS_MBR_BUCKET_ID"),
    F.rpad(F.col("MOD_ID"), <...>, " ").alias("MOD_ID"),
    F.col("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT"),
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("HEDIS_MESR_GAP_DPLY_MOD_TERM_DT"),
    F.col("HEDIS_MESR_GAP_MOD_PRTY_NO"),
    F.rpad(F.col("HEDIS_MESR_ABBR_ID"), <...>, " ").alias("HEDIS_MESR_ABBR_ID"),
    F.rpad(F.col("HEDIS_MESR_GAP_MOD_CAT_ID"), <...>, " ").alias("HEDIS_MESR_GAP_MOD_CAT_ID"),
    F.rpad(F.col("HEDIS_MESR_GAP_MOD_DPLY_TX"), <...>, " ").alias("HEDIS_MESR_GAP_MOD_DPLY_TX"),
    F.rpad(F.col("HEDIS_MESR_GAP_MOD_SCRIPT_TX"), <...>, " ").alias("HEDIS_MESR_GAP_MOD_SCRIPT_TX"),
    F.rpad(F.col("HEDIS_MESR_MOD_KEY_ID"), <...>, " ").alias("HEDIS_MESR_MOD_KEY_ID")
)

write_files(
    df_final,
    f"{adls_path}/key/HEDIS_MESR_GAP_DPLY_Fkey.dat",
    delimiter="^",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)