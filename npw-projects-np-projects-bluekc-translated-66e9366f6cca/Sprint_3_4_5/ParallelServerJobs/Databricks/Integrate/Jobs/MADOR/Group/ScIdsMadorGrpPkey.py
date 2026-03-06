# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Sethuraman Rajendran     03/30/2018      5839 - MADOR                Originally Programmed                            IntegrateDev2            Jaideep Mankala        04/13/2018 / Kalyan Neelam 2018-06-14

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Land into Seq File for the FKEY job
# MAGIC Left Outer Join Here
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Table GRP_MA_DOR.Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

IDSRunCycle = get_widget_value('IDSRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSDB = get_widget_value('IDSDB','')
IDSAcct = get_widget_value('IDSAcct','')
IDSPW = get_widget_value('IDSPW','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_db2_K_GRP_MA_DOR_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT GRP_ID, TAX_YR, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, GRP_MA_DOR_SK FROM {IDSOwner}.K_GRP_MA_DOR")
    .load()
)

schema_for_Group_Ds = StructType([
    StructField("GRP_ID", StringType(), True),
    StructField("TAX_YR", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("AS_OF_DTM", StringType(), True),
    StructField("GRP_NM", StringType(), True),
    StructField("GRP_CNTCT_FIRST_NM", StringType(), True),
    StructField("GRP_CNTCT_MIDINIT", StringType(), True),
    StructField("GRP_CNTCT_LAST_NM", StringType(), True),
    StructField("GRP_ADDR_LN_1", StringType(), True),
    StructField("GRP_ADDR_LN_2", StringType(), True),
    StructField("GRP_ADDR_LN_3", StringType(), True),
    StructField("GRP_ADDR_CITY_NM", StringType(), True),
    StructField("GRP_ADDR_ZIP_CD_5", StringType(), True),
    StructField("GRP_ADDR_ZIP_CD_4", StringType(), True),
    StructField("GRP_ST_CD", StringType(), True)
])
df_Group_Ds = spark.read.schema(schema_for_Group_Ds).parquet(f"{adls_path}/ds/GRP_MA_DOR_.{SrcSysCd}.extr.{RunID}.parquet")

df_lnkRemDupDataIn = df_Group_Ds.select(
    col("GRP_ID").alias("GRP_ID"),
    col("TAX_YR").alias("TAX_YR"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_lnkFullDataJnIn = df_Group_Ds.select(
    col("GRP_ID").alias("GRP_ID"),
    col("TAX_YR").alias("TAX_YR"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("AS_OF_DTM").alias("AS_OF_DTM"),
    col("GRP_NM").alias("GRP_NM"),
    col("GRP_CNTCT_FIRST_NM").alias("GRP_CNTCT_FIRST_NM"),
    col("GRP_CNTCT_MIDINIT").alias("GRP_CNTCT_MIDINIT"),
    col("GRP_CNTCT_LAST_NM").alias("GRP_CNTCT_LAST_NM"),
    col("GRP_ADDR_LN_1").alias("GRP_ADDR_LN_1"),
    col("GRP_ADDR_LN_2").alias("GRP_ADDR_LN_2"),
    col("GRP_ADDR_LN_3").alias("GRP_ADDR_LN_3"),
    col("GRP_ADDR_CITY_NM").alias("GRP_ADDR_CITY_NM"),
    col("GRP_ADDR_ZIP_CD_5").alias("GRP_ADDR_ZIP_CD_5"),
    col("GRP_ADDR_ZIP_CD_4").alias("GRP_ADDR_ZIP_CD_4"),
    col("GRP_ST_CD").alias("GRP_ST_CD")
)

df_rdp_NaturalKeys = dedup_sort(
    df_lnkRemDupDataIn,
    ["GRP_ID","TAX_YR","SRC_SYS_CD"],
    []
)

df_jn_Grp = (
    df_rdp_NaturalKeys.alias("lnkRemDupDataOut")
    .join(
        df_db2_K_GRP_MA_DOR_In.alias("Extr"),
        (
            col("lnkRemDupDataOut.GRP_ID")==col("Extr.GRP_ID")
            & col("lnkRemDupDataOut.TAX_YR")==col("Extr.TAX_YR")
            & col("lnkRemDupDataOut.SRC_SYS_CD")==col("Extr.SRC_SYS_CD")
        ),
        "left"
    )
    .select(
        col("lnkRemDupDataOut.GRP_ID").alias("GRP_ID"),
        col("lnkRemDupDataOut.TAX_YR").alias("TAX_YR"),
        col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("Extr.GRP_MA_DOR_SK").alias("GRP_MA_DOR_SK")
    )
)

df_enriched = df_jn_Grp.withColumn(
    "svGrpMadorSK",
    when(
        col("GRP_MA_DOR_SK").isNull() | (col("GRP_MA_DOR_SK")==0),
        lit(None)
    ).otherwise(col("GRP_MA_DOR_SK"))
)
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svGrpMadorSK',<schema>,<secret_name>)

df_New = (
    df_enriched
    .filter((col("GRP_MA_DOR_SK").isNull()) | (col("GRP_MA_DOR_SK")==0))
    .select(
        col("GRP_ID").alias("GRP_ID"),
        col("TAX_YR").alias("TAX_YR"),
        current_timestamp().alias("PRCS_DTM"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        col("svGrpMadorSK").alias("GRP_MA_DOR_SK")
    )
)

df_lnkPKEYxfmOut = df_enriched.select(
    col("GRP_ID").alias("GRP_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("TAX_YR").alias("TAX_YR"),
    when(
        col("GRP_MA_DOR_SK").isNull() | (col("GRP_MA_DOR_SK")==0),
        lit(IDSRunCycle)
    ).otherwise(col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svGrpMadorSK").alias("GRP_MA_DOR_SK")
)

jdbc_url_load, jdbc_props_load = get_db_config(ids_secret_name)
temp_table_name = "STAGING.ScIdsMadorGrpPkey_db2_K_GRP_MA_DOR_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url_load, jdbc_props_load)
(
    df_New.write.format("jdbc")
    .option("url", jdbc_url_load)
    .options(**jdbc_props_load)
    .option("dbtable", temp_table_name)
    .mode("overwrite")
    .save()
)
merge_sql_db2_K_GRP_MA_DOR_Load = f"""
MERGE INTO {IDSOwner}.K_GRP_MA_DOR AS T
USING {temp_table_name} AS S
ON
    T.GRP_ID = S.GRP_ID
    AND T.TAX_YR = S.TAX_YR
    AND T.PRCS_DTM = S.PRCS_DTM
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
    UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.GRP_MA_DOR_SK = S.GRP_MA_DOR_SK
WHEN NOT MATCHED THEN
    INSERT (GRP_ID, TAX_YR, PRCS_DTM, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, GRP_MA_DOR_SK)
    VALUES (S.GRP_ID, S.TAX_YR, S.PRCS_DTM, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.GRP_MA_DOR_SK);
"""
execute_dml(merge_sql_db2_K_GRP_MA_DOR_Load, jdbc_url_load, jdbc_props_load)

df_jn_PKEYs = (
    df_lnkFullDataJnIn.alias("lnkFullDataJnIn")
    .join(
        df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
        (
            col("lnkFullDataJnIn.GRP_ID") == col("lnkPKEYxfmOut.GRP_ID")
            & (col("lnkFullDataJnIn.SRC_SYS_CD") == col("lnkPKEYxfmOut.SRC_SYS_CD"))
            & (col("lnkFullDataJnIn.TAX_YR") == col("lnkPKEYxfmOut.TAX_YR"))
        ),
        "inner"
    )
    .select(
        col("lnkFullDataJnIn.GRP_ID").alias("GRP_ID"),
        col("lnkFullDataJnIn.TAX_YR").alias("TAX_YR"),
        col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnkFullDataJnIn.AS_OF_DTM").alias("AS_OF_DTM"),
        col("lnkFullDataJnIn.GRP_NM").alias("GRP_NM"),
        col("lnkFullDataJnIn.GRP_CNTCT_FIRST_NM").alias("GRP_CNTCT_FIRST_NM"),
        col("lnkFullDataJnIn.GRP_CNTCT_MIDINIT").alias("GRP_CNTCT_MIDINIT"),
        col("lnkFullDataJnIn.GRP_CNTCT_LAST_NM").alias("GRP_CNTCT_LAST_NM"),
        col("lnkFullDataJnIn.GRP_ADDR_LN_1").alias("GRP_ADDR_LN_1"),
        col("lnkFullDataJnIn.GRP_ADDR_LN_2").alias("GRP_ADDR_LN_2"),
        col("lnkFullDataJnIn.GRP_ADDR_LN_3").alias("GRP_ADDR_LN_3"),
        col("lnkFullDataJnIn.GRP_ADDR_CITY_NM").alias("GRP_ADDR_CITY_NM"),
        col("lnkFullDataJnIn.GRP_ADDR_ZIP_CD_5").alias("GRP_ADDR_ZIP_CD_5"),
        col("lnkFullDataJnIn.GRP_ADDR_ZIP_CD_4").alias("GRP_ADDR_ZIP_CD_4"),
        col("lnkFullDataJnIn.GRP_ST_CD").alias("GRP_ST_CD"),
        col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("lnkPKEYxfmOut.GRP_MA_DOR_SK").alias("GRP_MA_DOR_SK")
    )
)

df_seq_GRP_MA_DOR_Pkey = df_jn_PKEYs.select(
    col("GRP_ID").alias("GRP_ID"),
    rpad(col("TAX_YR"),4," ").alias("TAX_YR"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("AS_OF_DTM").alias("AS_OF_DTM"),
    col("GRP_NM").alias("GRP_NM"),
    col("GRP_CNTCT_FIRST_NM").alias("GRP_CNTCT_FIRST_NM"),
    rpad(col("GRP_CNTCT_MIDINIT"),1," ").alias("GRP_CNTCT_MIDINIT"),
    col("GRP_CNTCT_LAST_NM").alias("GRP_CNTCT_LAST_NM"),
    col("GRP_ADDR_LN_1").alias("GRP_ADDR_LN_1"),
    col("GRP_ADDR_LN_2").alias("GRP_ADDR_LN_2"),
    col("GRP_ADDR_LN_3").alias("GRP_ADDR_LN_3"),
    col("GRP_ADDR_CITY_NM").alias("GRP_ADDR_CITY_NM"),
    rpad(col("GRP_ADDR_ZIP_CD_5"),5," ").alias("GRP_ADDR_ZIP_CD_5"),
    rpad(col("GRP_ADDR_ZIP_CD_4"),4," ").alias("GRP_ADDR_ZIP_CD_4"),
    col("GRP_ST_CD").alias("GRP_ST_CD"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("GRP_MA_DOR_SK").alias("GRP_MA_DOR_SK")
)

write_files(
    df_seq_GRP_MA_DOR_Pkey,
    f"{adls_path}/key/GRP_MA_DOR.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)