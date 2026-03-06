# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name : IdsHedisRvwSetPKey
# MAGIC 
# MAGIC Called By: GDITIdsMbrHedisMesrYRMOExtCntl
# MAGIC 
# MAGIC                    
# MAGIC PROCESSING:
# MAGIC 
# MAGIC Applies Transformation rules for MBR_HEDIS_MESR_YR_MO
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Akhila M                               2018-03-23        30001                                 Orig Development                                         IntegrateDev2             Jaideep Mankala         03/30/2018   
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Karthik C                          2021-05-22       360427                                          Added new fielf VNDR_STD_FMT_IN               IntegrateDev2           Jaideep Mankala         05/24/2021

# MAGIC This Dataset created in the GDITIdsMbrHedisMesrYrMoXfrm
# MAGIC Create Primary Key for HEDIS_RVW_SET table
# MAGIC Balancing Load file
# MAGIC Final Load file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
RunCycExctnSK = get_widget_value('RunCycExctnSK','')
SrcSysCd = get_widget_value('SrcSysCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_DB2_K_HEDIS_RVW_SET = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT HEDIS_RVW_SET_NM, HEDIS_RVW_SET_STRT_DT, HEDIS_RVW_SET_END_DT, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, HEDIS_RVW_SET_SK FROM {IDSOwner}.K_HEDIS_RVW_SET"
    )
    .load()
)

df_Ds_Rvw_Set = spark.read.parquet(
    f"{adls_path}/ds/HEDIS_RVW_SET.xfrm.{RunID}.parquet"
)

df_Cpy_Data = df_Ds_Rvw_Set.select(
    F.col("HEDIS_RVW_SET_NM").alias("HEDIS_RVW_SET_NM"),
    F.col("HEDIS_RVW_SET_STRT_DT").alias("HEDIS_RVW_SET_STRT_DT"),
    F.col("HEDIS_RVW_SET_END_DT").alias("HEDIS_RVW_SET_END_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("HEDIS_RVW_SET_STRT_MO_NO").alias("HEDIS_RVW_SET_STRT_MO_NO"),
    F.col("HEDIS_RVW_SET_STRT_YR_NO").alias("HEDIS_RVW_SET_STRT_YR_NO"),
    F.col("HEDIS_RVW_SET_END_MO_NO").alias("HEDIS_RVW_SET_END_MO_NO"),
    F.col("HEDIS_RVW_SET_END_YR_NO").alias("HEDIS_RVW_SET_END_YR_NO")
)

df_Rdp_Key = dedup_sort(
    df_Cpy_Data,
    ["HEDIS_RVW_SET_NM","HEDIS_RVW_SET_STRT_DT","HEDIS_RVW_SET_END_DT","SRC_SYS_CD"],
    []
)

df_Jn_Key = df_Rdp_Key.alias("Lnk_K_TableJoin").join(
    df_DB2_K_HEDIS_RVW_SET.alias("Lnk_KHedisRvwSet"),
    (
        (F.col("Lnk_K_TableJoin.HEDIS_RVW_SET_NM") == F.col("Lnk_KHedisRvwSet.HEDIS_RVW_SET_NM")) &
        (F.col("Lnk_K_TableJoin.HEDIS_RVW_SET_STRT_DT") == F.col("Lnk_KHedisRvwSet.HEDIS_RVW_SET_STRT_DT")) &
        (F.col("Lnk_K_TableJoin.HEDIS_RVW_SET_END_DT") == F.col("Lnk_KHedisRvwSet.HEDIS_RVW_SET_END_DT")) &
        (F.col("Lnk_K_TableJoin.SRC_SYS_CD") == F.col("Lnk_KHedisRvwSet.SRC_SYS_CD"))
    ),
    how="left"
).select(
    F.col("Lnk_K_TableJoin.HEDIS_RVW_SET_NM").alias("HEDIS_RVW_SET_NM"),
    F.col("Lnk_K_TableJoin.HEDIS_RVW_SET_STRT_DT").alias("HEDIS_RVW_SET_STRT_DT"),
    F.col("Lnk_K_TableJoin.HEDIS_RVW_SET_END_DT").alias("HEDIS_RVW_SET_END_DT"),
    F.col("Lnk_K_TableJoin.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_K_TableJoin.HEDIS_RVW_SET_STRT_MO_NO").alias("HEDIS_RVW_SET_STRT_MO_NO"),
    F.col("Lnk_K_TableJoin.HEDIS_RVW_SET_STRT_YR_NO").alias("HEDIS_RVW_SET_STRT_YR_NO"),
    F.col("Lnk_K_TableJoin.HEDIS_RVW_SET_END_MO_NO").alias("HEDIS_RVW_SET_END_MO_NO"),
    F.col("Lnk_K_TableJoin.HEDIS_RVW_SET_END_YR_NO").alias("HEDIS_RVW_SET_END_YR_NO"),
    F.col("Lnk_KHedisRvwSet.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_KHedisRvwSet.HEDIS_RVW_SET_SK").alias("HEDIS_RVW_SET_SK")
)

df_TransformerInput = df_Jn_Key.withColumn(
    "isNew", F.col("HEDIS_RVW_SET_SK").isNull()
)

df_enriched = SurrogateKeyGen(
    df_TransformerInput,
    <DB sequence name>,
    "HEDIS_RVW_SET_SK",
    <schema>,
    <secret_name>
)

df_enriched_final = df_enriched.select(
    F.col("HEDIS_RVW_SET_SK").alias("HEDIS_RVW_SET_SK"),
    F.col("HEDIS_RVW_SET_NM").alias("HEDIS_RVW_SET_NM"),
    F.col("HEDIS_RVW_SET_STRT_DT").alias("HEDIS_RVW_SET_STRT_DT"),
    F.col("HEDIS_RVW_SET_END_DT").alias("HEDIS_RVW_SET_END_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.when(F.col("isNew"), RunCycExctnSK).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycExctnSK).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("HEDIS_RVW_SET_STRT_MO_NO").alias("HEDIS_RVW_SET_STRT_MO_NO"),
    F.col("HEDIS_RVW_SET_STRT_YR_NO").alias("HEDIS_RVW_SET_STRT_YR_NO"),
    F.col("HEDIS_RVW_SET_END_MO_NO").alias("HEDIS_RVW_SET_END_MO_NO"),
    F.col("HEDIS_RVW_SET_END_YR_NO").alias("HEDIS_RVW_SET_END_YR_NO"),
    F.when(
        F.substr(F.col("HEDIS_RVW_SET_NM"), 10, 3) == 'YTD',
        'Y'
    ).when(
        F.substr(F.col("HEDIS_RVW_SET_NM"), 10, 3) == 'CYR',
        'Y'
    ).otherwise('N').alias("VNDR_STD_FMT_IN"),
    F.col("isNew").alias("isNew")
)

df_All = df_enriched_final.drop("isNew")

df_New = df_enriched_final.filter(F.col("isNew")).select(
    F.col("HEDIS_RVW_SET_NM").alias("HEDIS_RVW_SET_NM"),
    F.col("HEDIS_RVW_SET_STRT_DT").alias("HEDIS_RVW_SET_STRT_DT"),
    F.col("HEDIS_RVW_SET_END_DT").alias("HEDIS_RVW_SET_END_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("HEDIS_RVW_SET_SK").alias("HEDIS_RVW_SET_SK")
)

df_Lnk_BalanceOut = df_Jn_Key.select(
    F.col("HEDIS_RVW_SET_NM"),
    F.col("HEDIS_RVW_SET_STRT_DT"),
    F.col("HEDIS_RVW_SET_END_DT"),
    F.col("SRC_SYS_CD")
)

schema_balance_out = StructType([
    StructField("HEDIS_RVW_SET_NM", StringType(), True),
    StructField("HEDIS_RVW_SET_STRT_DT", StringType(), True),
    StructField("HEDIS_RVW_SET_END_DT", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True)
])

write_files(
    df_Lnk_BalanceOut.select("HEDIS_RVW_SET_NM","HEDIS_RVW_SET_STRT_DT","HEDIS_RVW_SET_END_DT","SRC_SYS_CD"),
    f"{adls_path}/load/B_HEDIS_RVW_SET.{RunID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.IdsHedisRvwSetPKey_db2_K_HEDIS_RVW_SET_Load_temp",
    jdbc_url,
    jdbc_props
)

df_db2_K_HEDIS_RVW_SET_Load = df_New

df_db2_K_HEDIS_RVW_SET_Load.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsHedisRvwSetPKey_db2_K_HEDIS_RVW_SET_Load_temp") \
    .mode("overwrite") \
    .save()

merge_sql = (
    f"MERGE INTO {IDSOwner}.K_HEDIS_RVW_SET AS D "
    f"USING STAGING.IdsHedisRvwSetPKey_db2_K_HEDIS_RVW_SET_Load_temp AS S "
    f"ON D.HEDIS_RVW_SET_NM = S.HEDIS_RVW_SET_NM "
    f"AND D.HEDIS_RVW_SET_STRT_DT = S.HEDIS_RVW_SET_STRT_DT "
    f"AND D.HEDIS_RVW_SET_END_DT = S.HEDIS_RVW_SET_END_DT "
    f"AND D.SRC_SYS_CD = S.SRC_SYS_CD "
    f"WHEN MATCHED THEN UPDATE SET D.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK, D.HEDIS_RVW_SET_SK = S.HEDIS_RVW_SET_SK "
    f"WHEN NOT MATCHED THEN INSERT (HEDIS_RVW_SET_NM,HEDIS_RVW_SET_STRT_DT,HEDIS_RVW_SET_END_DT,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,HEDIS_RVW_SET_SK) "
    f"VALUES (S.HEDIS_RVW_SET_NM,S.HEDIS_RVW_SET_STRT_DT,S.HEDIS_RVW_SET_END_DT,S.SRC_SYS_CD,S.CRT_RUN_CYC_EXCTN_SK,S.HEDIS_RVW_SET_SK);"
)

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_funnel = StructType([
    StructField("HEDIS_RVW_SET_SK", StringType(), True),
    StructField("HEDIS_RVW_SET_NM", StringType(), True),
    StructField("HEDIS_RVW_SET_STRT_DT", StringType(), True),
    StructField("HEDIS_RVW_SET_END_DT", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("HEDIS_RVW_SET_STRT_MO_NO", StringType(), True),
    StructField("HEDIS_RVW_SET_STRT_YR_NO", StringType(), True),
    StructField("HEDIS_RVW_SET_END_MO_NO", StringType(), True),
    StructField("HEDIS_RVW_SET_END_YR_NO", StringType(), True),
    StructField("VNDR_STD_FMT_IN", StringType(), True)
])

df_Lnk_NA = spark.createDataFrame(
    [
        (
            "1", 
            "NA", 
            "1753-01-01", 
            "1753-01-01", 
            "NA", 
            "100", 
            "100", 
            "1", 
            "1", 
            "1", 
            "1",
            "N"
        )
    ],
    schema_funnel
)

df_Lnk_UNK = spark.createDataFrame(
    [
        (
            "0", 
            "UNK", 
            "1753-01-01", 
            "1753-01-01", 
            "UNK", 
            "100", 
            "100", 
            "0", 
            "0", 
            "0", 
            "0",
            "N"
        )
    ],
    schema_funnel
)

df_All_fnl = df_All.select(
    F.col("HEDIS_RVW_SET_SK").cast(StringType()),
    F.col("HEDIS_RVW_SET_NM").cast(StringType()),
    F.col("HEDIS_RVW_SET_STRT_DT").cast(StringType()),
    F.col("HEDIS_RVW_SET_END_DT").cast(StringType()),
    F.col("SRC_SYS_CD").cast(StringType()),
    F.col("CRT_RUN_CYC_EXCTN_SK").cast(StringType()),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").cast(StringType()),
    F.col("HEDIS_RVW_SET_STRT_MO_NO").cast(StringType()),
    F.col("HEDIS_RVW_SET_STRT_YR_NO").cast(StringType()),
    F.col("HEDIS_RVW_SET_END_MO_NO").cast(StringType()),
    F.col("HEDIS_RVW_SET_END_YR_NO").cast(StringType()),
    F.col("VNDR_STD_FMT_IN").cast(StringType())
)

df_fnl_all = df_Lnk_NA.unionByName(df_Lnk_UNK).unionByName(df_All_fnl)

df_fnl_all_rpad = df_fnl_all.withColumn(
    "VNDR_STD_FMT_IN",
    F.rpad(F.col("VNDR_STD_FMT_IN"), 1, " ")
)

write_files(
    df_fnl_all_rpad.select(
        "HEDIS_RVW_SET_SK",
        "HEDIS_RVW_SET_NM",
        "HEDIS_RVW_SET_STRT_DT",
        "HEDIS_RVW_SET_END_DT",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "HEDIS_RVW_SET_STRT_MO_NO",
        "HEDIS_RVW_SET_STRT_YR_NO",
        "HEDIS_RVW_SET_END_MO_NO",
        "HEDIS_RVW_SET_END_YR_NO",
        "VNDR_STD_FMT_IN"
    ),
    f"{adls_path}/load/HEDIS_RVW_SET.{SrcSysCd}.{RunID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)