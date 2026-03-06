# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2015 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                  Extracts data from EyeMed Provider from claim file and loads into a DataSets
# MAGIC 
# MAGIC Called by: EyeMedIDSProvExtrSeq
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                           Development Project               Code Reviewer               Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                    ----------------------------------              ---------------------------------       -------------------------
# MAGIC Goutham K                             2020-09-23       US-261580              Original Programming.                                                                     IntegrateDev2                        Kalyan Neelam               2020-10-26  
# MAGIC 
# MAGIC Sunitha Ganta                      2020-11-11         US-261580             Removed column PROV_NTWK_ID - No down stream impact     IntegrateDev2                          Reddy Sanam                2020-11-16
# MAGIC 
# MAGIC Goutham K                          2021-02-24          US-356916            Added Taxonomy field from source file                                             IntegrateDev2                         Jeyaprasanna                2021-02-25
# MAGIC 
# MAGIC Goutham K                          2021-05-15         US-366403            New Provider file Change to include Loc and Svc loc id                  IntegrateDev1                          Jeyaprasanna                2021-05-24

# MAGIC Job name: EyeMedIProvExtr
# MAGIC Remove Duplicate Prov Ids
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
EyeMedClmFile = get_widget_value('EyeMedClmFile','')

schema_EyeMed_Provider = StructType([
    StructField("SK", StringType(), True),
    StructField("PROV_ID", StringType(), True),
    StructField("LAST_NM", StringType(), True),
    StructField("FIRST_NM", StringType(), True),
    StructField("MIDINIT", StringType(), True),
    StructField("PROF_DSGTN", StringType(), True),
    StructField("PRI_SPEC_DESC", StringType(), True),
    StructField("GNDR", StringType(), True),
    StructField("LIC_NO", StringType(), True),
    StructField("LIC_ST", StringType(), True),
    StructField("TAX_ID", StringType(), True),
    StructField("LOC_ID", StringType(), True),
    StructField("LOC_SVC_ID", StringType(), True),
    StructField("LOC_NM", StringType(), True),
    StructField("MAIL_NM", StringType(), True),
    StructField("ADDR_LN_1", StringType(), True),
    StructField("ADDR_LN_2", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("ST", StringType(), True),
    StructField("ZIP_CD_5", StringType(), True),
    StructField("ZIP_CD_4", StringType(), True),
    StructField("CTRY", StringType(), True),
    StructField("LOC_PHN_NO", StringType(), True),
    StructField("LOC_FAX_NO", StringType(), True),
    StructField("LOC_HCAP_ACES_IN", StringType(), True),
    StructField("NTNL_PROV_ID", StringType(), True),
    StructField("LOC_EMAIL", StringType(), True),
    StructField("PROV_WEBSITE", StringType(), True),
    StructField("FRGN_LANG_1", StringType(), True),
    StructField("FRGN_LANG_2", StringType(), True),
    StructField("FRGN_LANG_3", StringType(), True),
    StructField("BRD_CRTFD_IN", StringType(), True),
    StructField("BRD_CERT_NM", StringType(), True),
    StructField("ACPTNG_NEW_PATN_IN", StringType(), True),
    StructField("TXNMY_CD", StringType(), True),
    StructField("EFF_DT", StringType(), True),
    StructField("TRM_DT", StringType(), True)
])

df_EyeMed_Provider = (
    spark.read.format("csv")
    .option("header", "true")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_EyeMed_Provider)
    .load(f"{adls_path_raw}/landing/{EyeMedClmFile}")
)

df_ClmProv = (
    df_EyeMed_Provider
    .where(
        (F.col("PROV_ID").isNotNull()) &
        (trim(F.col("PROV_ID")) != "")
    )
    .select(
        F.concat(
            trim(F.col("PROV_ID")),
            trim(F.col("LOC_ID")),
            trim(F.col("LOC_SVC_ID")),
            F.substring(trim(F.col("TAX_ID")), -4, 4)
        ).alias("PROV_ID"),
        trim(F.col("NTNL_PROV_ID")).alias("PROV_NPI"),
        F.lit("NA").alias("TAX_ENTY_NPI"),
        trim(F.col("FIRST_NM")).alias("PROV_FIRST_NM"),
        trim(F.col("LAST_NM")).alias("PROV_LAST_NM"),
        F.lit("NA").alias("BUS_NM"),
        trim(F.col("ADDR_LN_1")).alias("PROV_ADDR"),
        trim(F.col("ADDR_LN_2")).alias("PROV_ADDR_2"),
        trim(F.col("CITY")).alias("PROV_CITY"),
        trim(F.col("ST")).alias("PROV_ST"),
        trim(F.col("ZIP_CD_5")).alias("PROV_ZIP"),
        trim(F.col("ZIP_CD_4")).alias("PROV_ZIP_PLUS_4"),
        trim(F.col("PROF_DSGTN")).alias("PROF_DSGTN"),
        trim(F.col("TAX_ID")).alias("TAX_ENTY_ID"),
        trim(F.col("TXNMY_CD")).alias("TXNMY_CD"),
        F.col("LOC_ID").alias("LOC_ID"),
        F.col("LOC_SVC_ID").alias("LOC_SVC_ID"),
        F.col("LOC_PHN_NO").alias("LOC_PHN_NO"),
        F.col("LOC_FAX_NO").alias("LOC_FAX_NO"),
        F.col("LOC_EMAIL").alias("LOC_EMAIL"),
        F.col("LOC_HCAP_ACES_IN").alias("LOC_HCAP_ACES_IN"),
        F.date_format(
            F.to_date(trim(F.col("TRM_DT")), "MM/dd/yyyy"),
            "yyyy-MM-dd"
        ).alias("EFF_DT"),
        F.concat(trim(F.col("LOC_ID")), trim(F.col("LOC_SVC_ID"))).alias("PROV_ADDR_ID")
    )
)

df_Prov_Addr_In = (
    df_EyeMed_Provider
    .select(
        F.concat(
            trim(F.col("PROV_ID")),
            trim(F.col("LOC_ID")),
            trim(F.col("LOC_SVC_ID")),
            F.substring(trim(F.col("TAX_ID")), -4, 4)
        ).alias("PROV_ID"),
        trim(F.col("NTNL_PROV_ID")).alias("PROV_NPI"),
        F.lit("NA").alias("TAX_ENTY_NPI"),
        trim(F.col("FIRST_NM")).alias("PROV_FIRST_NM"),
        trim(F.col("LAST_NM")).alias("PROV_LAST_NM"),
        F.lit("NA").alias("BUS_NM"),
        trim(F.col("ADDR_LN_1")).alias("PROV_ADDR"),
        trim(F.col("ADDR_LN_2")).alias("PROV_ADDR_2"),
        trim(F.col("CITY")).alias("PROV_CITY"),
        trim(F.col("ST")).alias("PROV_ST"),
        trim(F.col("ZIP_CD_5")).alias("PROV_ZIP"),
        trim(F.col("ZIP_CD_4")).alias("PROV_ZIP_PLUS_4"),
        trim(F.col("PROF_DSGTN")).alias("PROF_DSGTN"),
        trim(F.col("TAX_ID")).alias("TAX_ENTY_ID"),
        F.lit("NA").alias("TXNMY_CD"),
        trim(F.col("LOC_ID")).alias("LOC_ID"),
        trim(F.col("LOC_SVC_ID")).alias("LOC_SVC_ID"),
        trim(F.col("LOC_PHN_NO")).alias("LOC_PHN_NO"),
        trim(F.col("LOC_FAX_NO")).alias("LOC_FAX_NO"),
        trim(F.col("LOC_EMAIL")).alias("LOC_EMAIL"),
        trim(F.col("LOC_HCAP_ACES_IN")).alias("LOC_HCAP_ACES_IN"),
        trim(F.col("CTRY")).alias("CNTY")
    )
)

df_RemDup_Prov = dedup_sort(
    df_ClmProv,
    ["PROV_ID"],
    [("PROV_ID", "A")]
)

df_RemDup_ProvOut = df_RemDup_Prov.select(
    F.col("PROV_ID"),
    F.col("PROV_NPI"),
    F.col("TAX_ENTY_NPI"),
    F.col("PROV_FIRST_NM"),
    F.col("PROV_LAST_NM"),
    F.col("BUS_NM"),
    F.col("PROV_ADDR"),
    F.col("PROV_ADDR_2"),
    F.col("PROV_CITY"),
    F.col("PROV_ST"),
    F.col("PROV_ZIP"),
    F.col("PROV_ZIP_PLUS_4"),
    F.col("PROF_DSGTN"),
    F.col("TAX_ENTY_ID"),
    F.col("TXNMY_CD"),
    F.col("EFF_DT"),
    F.col("PROV_ADDR_ID")
)

df_ds_PROV_EYEMED = df_RemDup_ProvOut
for c in df_ds_PROV_EYEMED.columns:
    df_ds_PROV_EYEMED = df_ds_PROV_EYEMED.withColumn(c, F.rpad(F.col(c), <...>, " "))

write_files(
    df_ds_PROV_EYEMED,
    f"{adls_path}/ds/PROV.{SrcSysCd}.extr.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_RemDup_ProvLoc = dedup_sort(
    df_Prov_Addr_In,
    ["PROV_ID", "LOC_SVC_ID", "LOC_ID"],
    [
        ("PROV_ID", "A"),
        ("LOC_SVC_ID", "A"),
        ("LOC_ID", "A")
    ]
)

df_RemDup_ProvLocOut = df_RemDup_ProvLoc.select(
    F.col("PROV_ID"),
    F.col("PROV_NPI"),
    F.col("TAX_ENTY_NPI"),
    F.col("PROV_FIRST_NM"),
    F.col("PROV_LAST_NM"),
    F.col("BUS_NM"),
    F.col("PROV_ADDR"),
    F.col("PROV_ADDR_2"),
    F.col("PROV_CITY"),
    F.col("PROV_ST"),
    F.col("PROV_ZIP"),
    F.col("PROV_ZIP_PLUS_4"),
    F.col("PROF_DSGTN"),
    F.col("TAX_ENTY_ID"),
    F.col("TXNMY_CD"),
    F.col("LOC_ID"),
    F.col("LOC_SVC_ID"),
    F.col("LOC_PHN_NO"),
    F.col("LOC_FAX_NO"),
    F.col("LOC_EMAIL"),
    F.col("LOC_HCAP_ACES_IN"),
    F.col("CNTY")
)

df_ds_PROV_EYEMED_ADDR = df_RemDup_ProvLocOut
for c in df_ds_PROV_EYEMED_ADDR.columns:
    df_ds_PROV_EYEMED_ADDR = df_ds_PROV_EYEMED_ADDR.withColumn(c, F.rpad(F.col(c), <...>, " "))

write_files(
    df_ds_PROV_EYEMED_ADDR,
    f"{adls_path}/ds/PROV_ADDR.{SrcSysCd}.extr.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)