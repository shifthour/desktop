# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Shiva Devagiri    2013-06-24       5114                              Original Programming                                                                                EnterpriseWrhsDevl        Jag Yelavarthi               2013-08-30
# MAGIC                                                                                      (Server to Parallel Conversion)

# MAGIC Load file created in the previous job will be loaded into the target table here. 
# MAGIC 
# MAGIC Load Type; "Update then Insert"
# MAGIC Read Load File created in the EdwProvDirDmProvAddrExtr Job
# MAGIC Load rejects are redirected into a Reject file
# MAGIC Copy For Buffer
# MAGIC Job Name: 
# MAGIC EdwProvDirDmProvAddrLoad
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from pyspark.sql.functions import col, rpad, lit
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


WebProvDirOwner = get_widget_value('WebProvDirOwner','')
webprovdir_secret_name = get_widget_value('webprovdir_secret_name','')
ProvDirArraySize = get_widget_value('ProvDirArraySize','')
ProvDirRecordCount = get_widget_value('ProvDirRecordCount','')

schema_seq_PROV_DIR_DM_PROV_ADDR_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PROV_ADDR_ID", StringType(), False),
    StructField("PROV_ADDR_TYP_CD", StringType(), False),
    StructField("PROV_ADDR_EFF_DT", TimestampType(), False),
    StructField("PROV_ADDR_TERM_DT", TimestampType(), False),
    StructField("PROV_ADDR_HCAP_IN", StringType(), False),
    StructField("PROV_ADDR_PDX_24_HR_IN", StringType(), False),
    StructField("PROV_ADDR_LN_1", StringType(), True),
    StructField("PROV_ADDR_LN_2", StringType(), True),
    StructField("PROV_ADDR_LN_3", StringType(), True),
    StructField("PROV_ADDR_CITY_NM", StringType(), True),
    StructField("PROV_ADDR_ST_CD", StringType(), True),
    StructField("PROV_ADDR_ST_NM", StringType(), True),
    StructField("PROV_ADDR_ZIP_CD_5", StringType(), True),
    StructField("PROV_ADDR_ZIP_CD_4", StringType(), True),
    StructField("PROV_ADDR_CNTY_NM", StringType(), True),
    StructField("PROV_ADDR_PHN_NO", StringType(), True),
    StructField("PROV_ADDR_PHN_NO_EXT", StringType(), True),
    StructField("PROV_ADDR_FAX_NO", StringType(), True),
    StructField("PROV_ADDR_FAX_NO_EXT", StringType(), True),
    StructField("PROV_ADDR_EMAIL_ADDR_TX", StringType(), True),
    StructField("PROV_ADDR_LAT_TX", DoubleType(), False),
    StructField("PROV_ADDR_LONG_TX", DoubleType(), False),
    StructField("PROV_ADDR_GEO_ACES_RTRN_CD_TX", StringType(), False),
    StructField("LAST_UPDT_DT", TimestampType(), False)
])

df_seq_PROV_DIR_DM_PROV_ADDR_csv_load = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("nullValue", "\"\"")
    .schema(schema_seq_PROV_DIR_DM_PROV_ADDR_csv_load)
    .load(f"{adls_path}/load/PROV_DIR_DM_PROV_ADDR.dat")
)

df_cpy_forBuffer = df_seq_PROV_DIR_DM_PROV_ADDR_csv_load

df_odbc_PROV_DIR_DM_PROV_ADDR_out = df_cpy_forBuffer.select(
    col("SRC_SYS_CD"),
    col("PROV_ADDR_ID"),
    col("PROV_ADDR_TYP_CD"),
    col("PROV_ADDR_EFF_DT"),
    col("PROV_ADDR_TERM_DT"),
    col("PROV_ADDR_HCAP_IN"),
    col("PROV_ADDR_PDX_24_HR_IN"),
    col("PROV_ADDR_LN_1"),
    col("PROV_ADDR_LN_2"),
    col("PROV_ADDR_LN_3"),
    col("PROV_ADDR_CITY_NM"),
    col("PROV_ADDR_ST_CD"),
    col("PROV_ADDR_ST_NM"),
    col("PROV_ADDR_ZIP_CD_5"),
    col("PROV_ADDR_ZIP_CD_4"),
    col("PROV_ADDR_CNTY_NM"),
    col("PROV_ADDR_PHN_NO"),
    col("PROV_ADDR_PHN_NO_EXT"),
    col("PROV_ADDR_FAX_NO"),
    col("PROV_ADDR_FAX_NO_EXT"),
    col("PROV_ADDR_EMAIL_ADDR_TX"),
    col("PROV_ADDR_LAT_TX"),
    col("PROV_ADDR_LONG_TX"),
    col("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    col("LAST_UPDT_DT")
)

df_odbc_PROV_DIR_DM_PROV_ADDR_out = (
    df_odbc_PROV_DIR_DM_PROV_ADDR_out
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), 255, " "))
    .withColumn("PROV_ADDR_ID", rpad(col("PROV_ADDR_ID"), 255, " "))
    .withColumn("PROV_ADDR_TYP_CD", rpad(col("PROV_ADDR_TYP_CD"), 255, " "))
    .withColumn("PROV_ADDR_HCAP_IN", rpad(col("PROV_ADDR_HCAP_IN"), 1, " "))
    .withColumn("PROV_ADDR_PDX_24_HR_IN", rpad(col("PROV_ADDR_PDX_24_HR_IN"), 1, " "))
    .withColumn("PROV_ADDR_LN_1", rpad(col("PROV_ADDR_LN_1"), 255, " "))
    .withColumn("PROV_ADDR_LN_2", rpad(col("PROV_ADDR_LN_2"), 255, " "))
    .withColumn("PROV_ADDR_LN_3", rpad(col("PROV_ADDR_LN_3"), 255, " "))
    .withColumn("PROV_ADDR_CITY_NM", rpad(col("PROV_ADDR_CITY_NM"), 255, " "))
    .withColumn("PROV_ADDR_ST_CD", rpad(col("PROV_ADDR_ST_CD"), 255, " "))
    .withColumn("PROV_ADDR_ST_NM", rpad(col("PROV_ADDR_ST_NM"), 255, " "))
    .withColumn("PROV_ADDR_ZIP_CD_5", rpad(col("PROV_ADDR_ZIP_CD_5"), 5, " "))
    .withColumn("PROV_ADDR_ZIP_CD_4", rpad(col("PROV_ADDR_ZIP_CD_4"), 4, " "))
    .withColumn("PROV_ADDR_CNTY_NM", rpad(col("PROV_ADDR_CNTY_NM"), 255, " "))
    .withColumn("PROV_ADDR_PHN_NO", rpad(col("PROV_ADDR_PHN_NO"), 255, " "))
    .withColumn("PROV_ADDR_PHN_NO_EXT", rpad(col("PROV_ADDR_PHN_NO_EXT"), 255, " "))
    .withColumn("PROV_ADDR_FAX_NO", rpad(col("PROV_ADDR_FAX_NO"), 255, " "))
    .withColumn("PROV_ADDR_FAX_NO_EXT", rpad(col("PROV_ADDR_FAX_NO_EXT"), 255, " "))
    .withColumn("PROV_ADDR_EMAIL_ADDR_TX", rpad(col("PROV_ADDR_EMAIL_ADDR_TX"), 255, " "))
    .withColumn("PROV_ADDR_GEO_ACES_RTRN_CD_TX", rpad(col("PROV_ADDR_GEO_ACES_RTRN_CD_TX"), 255, " "))
)

jdbc_url, jdbc_props = get_db_config(webprovdir_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.EdwDmProvDirDmProvAddrLoad_odbc_PROV_DIR_DM_PROV_ADDR_out_temp",
    jdbc_url,
    jdbc_props
)

df_odbc_PROV_DIR_DM_PROV_ADDR_out.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwDmProvDirDmProvAddrLoad_odbc_PROV_DIR_DM_PROV_ADDR_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {WebProvDirOwner}.PROV_DIR_DM_PROV_ADDR AS T
USING STAGING.EdwDmProvDirDmProvAddrLoad_odbc_PROV_DIR_DM_PROV_ADDR_out_temp AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD AND
    T.PROV_ADDR_ID = S.PROV_ADDR_ID AND
    T.PROV_ADDR_TYP_CD = S.PROV_ADDR_TYP_CD AND
    T.PROV_ADDR_EFF_DT = S.PROV_ADDR_EFF_DT
WHEN MATCHED THEN
    UPDATE SET
        T.SRC_SYS_CD = S.SRC_SYS_CD,
        T.PROV_ADDR_ID = S.PROV_ADDR_ID,
        T.PROV_ADDR_TYP_CD = S.PROV_ADDR_TYP_CD,
        T.PROV_ADDR_EFF_DT = S.PROV_ADDR_EFF_DT,
        T.PROV_ADDR_TERM_DT = S.PROV_ADDR_TERM_DT,
        T.PROV_ADDR_HCAP_IN = S.PROV_ADDR_HCAP_IN,
        T.PROV_ADDR_PDX_24_HR_IN = S.PROV_ADDR_PDX_24_HR_IN,
        T.PROV_ADDR_LN_1 = S.PROV_ADDR_LN_1,
        T.PROV_ADDR_LN_2 = S.PROV_ADDR_LN_2,
        T.PROV_ADDR_LN_3 = S.PROV_ADDR_LN_3,
        T.PROV_ADDR_CITY_NM = S.PROV_ADDR_CITY_NM,
        T.PROV_ADDR_ST_CD = S.PROV_ADDR_ST_CD,
        T.PROV_ADDR_ST_NM = S.PROV_ADDR_ST_NM,
        T.PROV_ADDR_ZIP_CD_5 = S.PROV_ADDR_ZIP_CD_5,
        T.PROV_ADDR_ZIP_CD_4 = S.PROV_ADDR_ZIP_CD_4,
        T.PROV_ADDR_CNTY_NM = S.PROV_ADDR_CNTY_NM,
        T.PROV_ADDR_PHN_NO = S.PROV_ADDR_PHN_NO,
        T.PROV_ADDR_PHN_NO_EXT = S.PROV_ADDR_PHN_NO_EXT,
        T.PROV_ADDR_FAX_NO = S.PROV_ADDR_FAX_NO,
        T.PROV_ADDR_FAX_NO_EXT = S.PROV_ADDR_FAX_NO_EXT,
        T.PROV_ADDR_EMAIL_ADDR_TX = S.PROV_ADDR_EMAIL_ADDR_TX,
        T.PROV_ADDR_LAT_TX = S.PROV_ADDR_LAT_TX,
        T.PROV_ADDR_LONG_TX = S.PROV_ADDR_LONG_TX,
        T.PROV_ADDR_GEO_ACES_RTRN_CD_TX = S.PROV_ADDR_GEO_ACES_RTRN_CD_TX,
        T.LAST_UPDT_DT = S.LAST_UPDT_DT
WHEN NOT MATCHED THEN
    INSERT
    (
        SRC_SYS_CD,
        PROV_ADDR_ID,
        PROV_ADDR_TYP_CD,
        PROV_ADDR_EFF_DT,
        PROV_ADDR_TERM_DT,
        PROV_ADDR_HCAP_IN,
        PROV_ADDR_PDX_24_HR_IN,
        PROV_ADDR_LN_1,
        PROV_ADDR_LN_2,
        PROV_ADDR_LN_3,
        PROV_ADDR_CITY_NM,
        PROV_ADDR_ST_CD,
        PROV_ADDR_ST_NM,
        PROV_ADDR_ZIP_CD_5,
        PROV_ADDR_ZIP_CD_4,
        PROV_ADDR_CNTY_NM,
        PROV_ADDR_PHN_NO,
        PROV_ADDR_PHN_NO_EXT,
        PROV_ADDR_FAX_NO,
        PROV_ADDR_FAX_NO_EXT,
        PROV_ADDR_EMAIL_ADDR_TX,
        PROV_ADDR_LAT_TX,
        PROV_ADDR_LONG_TX,
        PROV_ADDR_GEO_ACES_RTRN_CD_TX,
        LAST_UPDT_DT
    )
    VALUES
    (
        S.SRC_SYS_CD,
        S.PROV_ADDR_ID,
        S.PROV_ADDR_TYP_CD,
        S.PROV_ADDR_EFF_DT,
        S.PROV_ADDR_TERM_DT,
        S.PROV_ADDR_HCAP_IN,
        S.PROV_ADDR_PDX_24_HR_IN,
        S.PROV_ADDR_LN_1,
        S.PROV_ADDR_LN_2,
        S.PROV_ADDR_LN_3,
        S.PROV_ADDR_CITY_NM,
        S.PROV_ADDR_ST_CD,
        S.PROV_ADDR_ST_NM,
        S.PROV_ADDR_ZIP_CD_5,
        S.PROV_ADDR_ZIP_CD_4,
        S.PROV_ADDR_CNTY_NM,
        S.PROV_ADDR_PHN_NO,
        S.PROV_ADDR_PHN_NO_EXT,
        S.PROV_ADDR_FAX_NO,
        S.PROV_ADDR_FAX_NO_EXT,
        S.PROV_ADDR_EMAIL_ADDR_TX,
        S.PROV_ADDR_LAT_TX,
        S.PROV_ADDR_LONG_TX,
        S.PROV_ADDR_GEO_ACES_RTRN_CD_TX,
        S.LAST_UPDT_DT
    );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_rej = StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])
df_seq_PROV_DIR_DM_PROV_ADDRcsv_rej = spark.createDataFrame([], schema_rej)

write_files(
    df_seq_PROV_DIR_DM_PROV_ADDRcsv_rej,
    f"{adls_path}/load/PROV_DIR_DM_PROV_ADDR_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue="\"\""
)