# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Job : FctsNhssCdXrefPKey
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project          Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------         -------------------------------       -------------------------
# MAGIC Goutham Kalidindi             08/03/2020            US-241932                        New Programming                                                          IntegrateDev2                     Kalyan Neelam              2020-08-13
# MAGIC Goutham Kalidindi             09/01/2020       INC0657939                       Replaced $IDSFilePath parameter with                                  IntegrateDev2                Kalyan Neelam              2020-09-01
# MAGIC                                                                                                                $FilePath

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC Job checks for the existing primary key in the k table and creates sk for new records
# MAGIC Left Outer Join
# MAGIC New Pkey is Generated in the Transformer stage, A DB sequenser is used to generate the Key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, when, coalesce, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

schema_Extr_NHSS_RTRN_CD = StructType([
    StructField("NHSS_RTRN_CD", StringType(), True),
    StructField("NHSS_EXCD_ID", StringType(), True),
    StructField("NHSS_WMDS_NMBR", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True)
])
df_Extr_NHSS_RTRN_CD = (
    spark.read
    .option("delimiter", "|")
    .option("header", "false")
    .schema(schema_Extr_NHSS_RTRN_CD)
    .csv(f"{adls_path}/verified/NHSS_RTRN_CD.dat")
    .select(
        col("NHSS_RTRN_CD"),
        col("NHSS_EXCD_ID"),
        col("NHSS_WMDS_NMBR"),
        col("SRC_SYS_CD")
    )
)

df_Cpy_Nhss_cd = df_Extr_NHSS_RTRN_CD

df_Lnk_Rm_Dup = df_Cpy_Nhss_cd.select(
    col("NHSS_RTRN_CD"),
    col("SRC_SYS_CD")
)

df_Lnk_Full_data_Nhss = df_Cpy_Nhss_cd.select(
    col("NHSS_RTRN_CD"),
    col("NHSS_EXCD_ID"),
    col("NHSS_WMDS_NMBR"),
    col("SRC_SYS_CD")
)

df_Lnk_Rm_Dup_data = dedup_sort(
    df_Lnk_Rm_Dup,
    ["NHSS_RTRN_CD", "SRC_SYS_CD"],
    []
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT NHSS_RTRN_CD, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, NHSS_RTRN_CD_SK FROM {IDSOwner}.K_NHSS_RTRN_CD"
df_db2_K_NHSS_RTRN_CD_read = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Jn_Left_Natural_Keys = (
    df_Lnk_Rm_Dup_data.alias("Lnk_Rm_Dup_data")
    .join(
        df_db2_K_NHSS_RTRN_CD_read.alias("Lnk_K_read"),
        (
            (col("Lnk_Rm_Dup_data.NHSS_RTRN_CD") == col("Lnk_K_read.NHSS_RTRN_CD")) &
            (col("Lnk_Rm_Dup_data.SRC_SYS_CD") == col("Lnk_K_read.SRC_SYS_CD"))
        ),
        "left"
    )
    .select(
        col("Lnk_Rm_Dup_data.NHSS_RTRN_CD").alias("NHSS_RTRN_CD"),
        col("Lnk_Rm_Dup_data.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("Lnk_K_read.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("Lnk_K_read.NHSS_RTRN_CD_SK").alias("NHSS_RTRN_CD_SK")
    )
)

df_enriched = (
    df_Jn_Left_Natural_Keys
    .withColumn("nullOrZero", (col("NHSS_RTRN_CD_SK").isNull()) | (col("NHSS_RTRN_CD_SK") == lit(0)))
    .withColumn("temp_NHSS_RTRN_CD_SK", when(col("nullOrZero"), lit(None)).otherwise(col("NHSS_RTRN_CD_SK")))
)
df_enriched = SurrogateKeyGen(df_enriched, "<DB sequence name>", "temp_NHSS_RTRN_CD_SK", "<schema>", "<secret_name>")
df_enriched = df_enriched.withColumn(
    "NHSS_RTRN_CD_SK",
    coalesce(col("temp_NHSS_RTRN_CD_SK"), col("NHSS_RTRN_CD_SK"))
)

df_lnk_Load_K = df_enriched.filter(col("nullOrZero") == True).select(
    col("NHSS_RTRN_CD"),
    col("SRC_SYS_CD"),
    lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    col("NHSS_RTRN_CD_SK")
)

df_Lnk_In_Rt = (
    df_enriched
    .withColumn("CRT_RUN_CYC_EXCTN_SK_2", when(col("nullOrZero"), lit(IDSRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK")))
    .select(
        col("NHSS_RTRN_CD_SK").alias("NHSS_RTRN_CD_SK"),
        col("NHSS_RTRN_CD").alias("NHSS_RTRN_CD"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("CRT_RUN_CYC_EXCTN_SK_2").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.FctsNhssCdXrefPKey_db2_K_NHSS_RTRN_CD_write_temp", jdbc_url, jdbc_props)
df_lnk_Load_K.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", "STAGING.FctsNhssCdXrefPKey_db2_K_NHSS_RTRN_CD_write_temp").mode("append").save()

merge_sql = f"""
MERGE INTO {IDSOwner}.K_NHSS_RTRN_CD as T
USING STAGING.FctsNhssCdXrefPKey_db2_K_NHSS_RTRN_CD_write_temp as S
ON T.NHSS_RTRN_CD = S.NHSS_RTRN_CD AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN UPDATE SET 
 T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
 T.NHSS_RTRN_CD_SK = S.NHSS_RTRN_CD_SK
WHEN NOT MATCHED THEN INSERT (NHSS_RTRN_CD,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,NHSS_RTRN_CD_SK)
VALUES (S.NHSS_RTRN_CD,S.SRC_SYS_CD,S.CRT_RUN_CYC_EXCTN_SK,S.NHSS_RTRN_CD_SK);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_Jn_Left = (
    df_Lnk_Full_data_Nhss.alias("Lnk_Full_data_Nhss")
    .join(
        df_Lnk_In_Rt.alias("Lnk_In_Rt"),
        (
            (col("Lnk_Full_data_Nhss.NHSS_RTRN_CD") == col("Lnk_In_Rt.NHSS_RTRN_CD")) &
            (col("Lnk_Full_data_Nhss.SRC_SYS_CD") == col("Lnk_In_Rt.SRC_SYS_CD"))
        ),
        "inner"
    )
    .select(
        col("Lnk_In_Rt.NHSS_RTRN_CD_SK").alias("NHSS_RTRN_CD_SK"),
        col("Lnk_Full_data_Nhss.NHSS_RTRN_CD").alias("NHSS_RTRN_CD"),
        col("Lnk_Full_data_Nhss.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("Lnk_In_Rt.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("Lnk_In_Rt.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("Lnk_Full_data_Nhss.NHSS_EXCD_ID").alias("NHSS_EXPL_CD"),
        col("Lnk_Full_data_Nhss.NHSS_WMDS_NMBR").alias("NHSS_WARN_MSG_NO")
    )
)

df_final = df_Jn_Left.withColumn("NHSS_EXPL_CD", rpad(col("NHSS_EXPL_CD"), 3, " "))
write_files(
    df_final.select(
        "NHSS_RTRN_CD_SK",
        "NHSS_RTRN_CD",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "NHSS_EXPL_CD",
        "NHSS_WARN_MSG_NO"
    ),
    f"{adls_path}/load/NHSS_RTRN_CD.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=" "
)