# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY
# MAGIC 
# MAGIC DESCRIPTION:   Takes the CRF file and applies the foreign key and creates the load file for PROV_ADDR
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC   ***************************************************************************************************
# MAGIC   ************** NOTE NOTE NOTE NOTE  **************************************************
# MAGIC    **************************************************************************************************
# MAGIC   ****  Logging is turned off for the lookup for code lookups for the following  ***********
# MAGIC    *****   because specs say if not found then 'NA' -   **************************************
# MAGIC   ****  Fields are:  PROV_ADDR_CNTY_CLS_CD_SK and  ******************************
# MAGIC    ********      PROV_ADDR_METRORURAL_COV_CD_SK  ****************************
# MAGIC    *************************************************************************************************
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                               Change Description                  Development Project      Code Reviewer          Date Reviewed       
# MAGIC --------------------            --------------------     ------------------------                              ----------------------------------------           --------------------------------       -------------------------------   ----------------------------       
# MAGIC Giri Mallavaram        10/16/2019      6131 PBM Replacement               Initial Programming                       IntegrateDevl                  Kalyan Neelam          2019-11-20

# MAGIC Read PROV_ADDR for foreign key look-up in a common record format
# MAGIC Perform foreign-key and code translation via corresponding lookup routines
# MAGIC Capture records generating translation errors to be uploaded to the IDS recycle table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DecimalType,
    DoubleType
)
from pyspark.sql.functions import (
    col,
    lit,
    when,
    length,
    rpad
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile","")
Logging = get_widget_value("Logging","")
RunId = get_widget_value("RunId","")

schema_IdsProvAddrExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("PROV_ADDR_SK", IntegerType(), nullable=False),
    StructField("PROV_ADDR_ID", StringType(), nullable=False),
    StructField("PROV_ADDR_TYP_CD", StringType(), nullable=False),
    StructField("EFF_DT", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=True),
    StructField("PROV_ADDR_CNTY_CLS_CD", StringType(), nullable=False),
    StructField("PROV_ADDR_GEO_ACES_RTRN_CD_TX", StringType(), nullable=False),
    StructField("PROV_ADDR_METRORURAL_COV_CD", StringType(), nullable=False),
    StructField("PROV_ADDR_TERM_RSN_CD", StringType(), nullable=False),
    StructField("HCAP_IN", StringType(), nullable=False),
    StructField("PDX_24_HR_IN", StringType(), nullable=False),
    StructField("PRCTC_LOC_IN", StringType(), nullable=False),
    StructField("PROV_ADDR_DIR_IN", StringType(), nullable=False),
    StructField("TERM_DT", StringType(), nullable=False),
    StructField("ADDR_LN_1", StringType(), nullable=True),
    StructField("ADDR_LN_2", StringType(), nullable=True),
    StructField("ADDR_LN_3", StringType(), nullable=True),
    StructField("CITY_NM", StringType(), nullable=True),
    StructField("PROV_ADDR_ST_CD", StringType(), nullable=False),
    StructField("POSTAL_CD", StringType(), nullable=True),
    StructField("CNTY_NM", StringType(), nullable=True),
    StructField("PHN_NO", StringType(), nullable=True),
    StructField("PHN_NO_EXT", StringType(), nullable=True),
    StructField("FAX_NO", StringType(), nullable=True),
    StructField("FAX_NO_EXT", StringType(), nullable=True),
    StructField("EMAIL_ADDR_TX", StringType(), nullable=True),
    StructField("LAT_TX", DoubleType(), nullable=True),
    StructField("LONG_TX", DoubleType(), nullable=True),
    StructField("PRAD_TYPE_MAIL", StringType(), nullable=False),
    StructField("PROV2_EFF_DT", StringType(), nullable=False)
])

df_IdsProvAddrExtr = (
    spark.read
    .csv(
        path=f"{adls_path}/key/{InFile}",
        schema=schema_IdsProvAddrExtr,
        sep=",",
        quote="\"",
        header=False
    )
)

df_enriched = (
    df_IdsProvAddrExtr
    .withColumn("ErrCount", GetFkeyErrorCnt(col("PROV_ADDR_SK")))
    .withColumn("svEffDt", GetFkeyDate(lit("IDS"), col("PROV_ADDR_SK"), col("EFF_DT"), Logging))
    .withColumn("svSrcSysCd", GetFkeyCodes(lit("IDS"), col("PROV_ADDR_SK"), lit("SOURCE SYSTEM"), col("SRC_SYS_CD"), Logging))
    .withColumn("svProvAddrCntyClsCd", GetFkeyCodes(lit("FACETS"), col("PROV_ADDR_SK"), lit("PROVIDER ADDRESS COUNTY CLASSIFICATION"), col("PROV_ADDR_CNTY_CLS_CD"), Logging))
    .withColumn("svProvAddrMetroRuralCovCd", GetFkeyCodes(lit("FACETS"), col("PROV_ADDR_SK"), lit("PROVIDER ADDRESS METRO RURAL COVERAGE"), col("PROV_ADDR_METRORURAL_COV_CD"), Logging))
    .withColumn("svProvAddrStCd", GetFkeyCodes(lit("FACETS"), col("PROV_ADDR_SK"), lit("STATE"), col("PROV_ADDR_ST_CD"), Logging))
    .withColumn("svProvAddrTermRsnCd", GetFkeyCodes(lit("FACETS"), col("PROV_ADDR_SK"), lit("PROVIDER ADDRESS TERMINATION REASON"), col("PROV_ADDR_TERM_RSN_CD"), Logging))
    .withColumn("svProvAddrTypCd", GetFkeyCodes(col("SRC_SYS_CD"), col("PROV_ADDR_SK"), lit("PROVIDER ADDRESS TYPE"), col("PROV_ADDR_TYP_CD"), Logging))
    .withColumn(
        "MailProvAddrSK",
        when(col("SRC_SYS_CD") == "NABP", col("PROV_ADDR_SK"))
        .otherwise(
            when(col("PROV2_EFF_DT") == "NA", lit(1))
            .otherwise(GetFkeyProvAddr(col("SRC_SYS_CD"), col("PROV_ADDR_SK"), col("PROV_ADDR_ID"), col("PRAD_TYPE_MAIL"), col("PROV2_EFF_DT"), Logging))
        )
    )
    .withColumn("svTermDt", GetFkeyDate(lit("IDS"), col("PROV_ADDR_SK"), col("TERM_DT"), Logging))
    .withColumn("PassThru", col("PASS_THRU_IN"))
)

df_fkey = (
    df_enriched
    .filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .select(
        col("PROV_ADDR_SK").alias("PROV_ADDR_SK"),
        col("svSrcSysCd").alias("SRC_SYS_CD_SK"),
        col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        col("svProvAddrTypCd").alias("PROV_ADDR_TYP_CD_SK"),
        col("svEffDt").alias("PROV_ADDR_EFF_DT_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("MailProvAddrSK").alias("MAIL_PROV_ADRESS_SK"),
        col("svProvAddrCntyClsCd").alias("PROV_ADDR_CNTY_CLS_CD_SK"),
        col("PROV_ADDR_GEO_ACES_RTRN_CD_TX").alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
        col("svProvAddrMetroRuralCovCd").alias("PROV_ADDR_METRORURAL_COV_CD_SK"),
        col("svProvAddrTermRsnCd").alias("PROV_ADDR_TERM_RSN_CD_SK"),
        col("HCAP_IN").alias("HCAP_IN"),
        col("PDX_24_HR_IN").alias("PDX_24_HR_IN"),
        col("PRCTC_LOC_IN").alias("PRCTC_LOC_IN"),
        col("PROV_ADDR_DIR_IN").alias("PROV_ADDR_DIR_IN"),
        col("svTermDt").alias("TERM_DT_SK"),
        col("ADDR_LN_1").alias("ADDR_LN_1"),
        col("ADDR_LN_2").alias("ADDR_LN_2"),
        col("ADDR_LN_3").alias("ADDR_LN_3"),
        col("CITY_NM").alias("CITY_NM"),
        col("svProvAddrStCd").alias("PROV_ADDR_ST_CD_SK"),
        col("POSTAL_CD").alias("POSTAL_CD"),
        col("CNTY_NM").alias("CNTY_NM"),
        when(length(trim(col("PHN_NO"))) == 0, lit(" ")).otherwise(col("PHN_NO")).alias("PHN_NO"),
        col("PHN_NO_EXT").alias("PHN_NO_EXT"),
        col("FAX_NO").alias("FAX_NO"),
        col("FAX_NO_EXT").alias("FAX_NO_EXT"),
        col("EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
        col("LAT_TX").alias("LAT_TX"),
        col("LONG_TX").alias("LONG_TX")
    )
)

df_recycle = (
    df_enriched
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("PROV_ADDR_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("PROV_ADDR_SK").alias("PROV_ADDR_SK"),
        col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
        col("EFF_DT").alias("EFF_DT"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("PROV_ADDR_CNTY_CLS_CD").alias("PROV_ADDR_CNTY_CLS_CD"),
        col("PROV_ADDR_GEO_ACES_RTRN_CD_TX").alias("PROV_ADDR_GEO_ACES_RTRN_CD"),
        col("PROV_ADDR_METRORURAL_COV_CD").alias("PROV_ADDR_METRORURAL_COV_CD"),
        col("PROV_ADDR_TERM_RSN_CD").alias("PROV_ADDR_TERM_RSN_CD"),
        col("HCAP_IN").alias("HCAP_IN"),
        col("PDX_24_HR_IN").alias("PDX_24_HR_IN"),
        col("PRCTC_LOC_IN").alias("PRCTC_LOC_IN"),
        col("PROV_ADDR_DIR_IN").alias("PROV_ADDR_DIR_IN"),
        col("TERM_DT").alias("TERM_DT"),
        col("ADDR_LN_1").alias("ADDR_LN_1"),
        col("ADDR_LN_2").alias("ADDR_LN_2"),
        col("ADDR_LN_3").alias("ADDR_LN_3"),
        col("CITY_NM").alias("CITY_NM"),
        col("PROV_ADDR_ST_CD").alias("PROV_ADDR_ST_CD"),
        col("POSTAL_CD").alias("POSTAL_CD"),
        col("CNTY_NM").alias("CNTY_NM"),
        col("PHN_NO").alias("PHN_NO"),
        col("PHN_NO_EXT").alias("PHN_NO_EXT"),
        col("FAX_NO").alias("FAX_NO"),
        col("FAX_NO_EXT").alias("FAX_NO_EXT"),
        col("EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
        col("LAT_TX").alias("LAT_TX"),
        col("LONG_TX").alias("LONG_TX"),
        col("PRAD_TYPE_MAIL").alias("PRAD_TYPE_MAIL"),
        col("PROV2_EFF_DT").alias("PROV2_EFF_DT")
    )
)

write_files(
    df_recycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_defaultNA = spark.range(1).select(
    lit(1).alias("PROV_ADDR_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit("NA").alias("PROV_ADDR_ID"),
    lit(1).alias("PROV_ADDR_TYP_CD_SK"),
    lit("NA").alias("PROV_ADDR_EFF_DT_SK"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("MAIL_PROV_ADRESS_SK"),
    lit(1).alias("PROV_ADDR_CNTY_CLS_CD_SK"),
    lit("NA").alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    lit(1).alias("PROV_ADDR_METRORURAL_COV_CD_SK"),
    lit(1).alias("PROV_ADDR_TERM_RSN_CD_SK"),
    lit("X").alias("HCAP_IN"),
    lit("X").alias("PDX_24_HR_IN"),
    lit("X").alias("PRCTC_LOC_IN"),
    lit("X").alias("PROV_ADDR_DIR_IN"),
    lit("NA").alias("TERM_DT_SK"),
    lit("NA").alias("ADDR_LN_1"),
    lit("NA").alias("ADDR_LN_2"),
    lit("NA").alias("ADDR_LN_3"),
    lit("NA").alias("CITY_NM"),
    lit(1).alias("PROV_ADDR_ST_CD_SK"),
    lit("NA").alias("POSTAL_CD"),
    lit("NA").alias("CNTY_NM"),
    lit("NA").alias("PHN_NO"),
    lit("NA").alias("PHN_NO_EXT"),
    lit("NA").alias("FAX_NO"),
    lit("NA").alias("FAX_NO_EXT"),
    lit("NA").alias("EMAIL_ADDR_TX"),
    lit(1).alias("LAT_TX"),
    lit(1).alias("LONG_TX")
)

df_defaultUNK = spark.range(1).select(
    lit(0).alias("PROV_ADDR_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit("UNK").alias("PROV_ADDR_ID"),
    lit(0).alias("PROV_ADDR_TYP_CD_SK"),
    lit("UNK").alias("PROV_ADDR_EFF_DT_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("MAIL_PROV_ADRESS_SK"),
    lit(0).alias("PROV_ADDR_CNTY_CLS_CD_SK"),
    lit("UNK").alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    lit(0).alias("PROV_ADDR_METRORURAL_COV_CD_SK"),
    lit(0).alias("PROV_ADDR_TERM_RSN_CD_SK"),
    lit("X").alias("HCAP_IN"),
    lit("X").alias("PDX_24_HR_IN"),
    lit("X").alias("PRCTC_LOC_IN"),
    lit("X").alias("PROV_ADDR_DIR_IN"),
    lit("UNK").alias("TERM_DT_SK"),
    lit("UNK").alias("ADDR_LN_1"),
    lit("UNK").alias("ADDR_LN_2"),
    lit("UNK").alias("ADDR_LN_3"),
    lit("UNK").alias("CITY_NM"),
    lit(0).alias("PROV_ADDR_ST_CD_SK"),
    lit("UNK").alias("POSTAL_CD"),
    lit("UNK").alias("CNTY_NM"),
    lit("UNK").alias("PHN_NO"),
    lit("UNK").alias("PHN_NO_EXT"),
    lit("UNK").alias("FAX_NO"),
    lit("UNK").alias("FAX_NO_EXT"),
    lit("UNK").alias("EMAIL_ADDR_TX"),
    lit(0).alias("LAT_TX"),
    lit(0).alias("LONG_TX")
)

df_collector = df_fkey.unionByName(df_defaultNA).unionByName(df_defaultUNK)

df_final = df_collector.select(
    col("PROV_ADDR_SK"),
    col("SRC_SYS_CD_SK"),
    rpad(col("PROV_ADDR_ID"), 10, " ").alias("PROV_ADDR_ID"),
    col("PROV_ADDR_TYP_CD_SK"),
    rpad(col("PROV_ADDR_EFF_DT_SK"), 10, " ").alias("PROV_ADDR_EFF_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MAIL_PROV_ADRESS_SK"),
    col("PROV_ADDR_CNTY_CLS_CD_SK"),
    col("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    col("PROV_ADDR_METRORURAL_COV_CD_SK"),
    col("PROV_ADDR_TERM_RSN_CD_SK"),
    rpad(col("HCAP_IN"), 1, " ").alias("HCAP_IN"),
    rpad(col("PDX_24_HR_IN"), 1, " ").alias("PDX_24_HR_IN"),
    rpad(col("PRCTC_LOC_IN"), 1, " ").alias("PRCTC_LOC_IN"),
    rpad(col("PROV_ADDR_DIR_IN"), 1, " ").alias("PROV_ADDR_DIR_IN"),
    rpad(col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
    col("ADDR_LN_1"),
    col("ADDR_LN_2"),
    col("ADDR_LN_3"),
    col("CITY_NM"),
    col("PROV_ADDR_ST_CD_SK"),
    rpad(col("POSTAL_CD"), 11, " ").alias("POSTAL_CD"),
    col("CNTY_NM"),
    col("PHN_NO"),
    col("PHN_NO_EXT"),
    rpad(col("FAX_NO"), 20, " ").alias("FAX_NO"),
    col("FAX_NO_EXT"),
    col("EMAIL_ADDR_TX"),
    col("LAT_TX"),
    col("LONG_TX")
)

write_files(
    df_final,
    f"{adls_path}/load/PROV_ADDR.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)