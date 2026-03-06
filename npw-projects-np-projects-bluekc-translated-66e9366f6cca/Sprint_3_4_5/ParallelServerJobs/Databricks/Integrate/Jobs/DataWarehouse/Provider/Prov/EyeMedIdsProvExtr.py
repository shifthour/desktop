# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2015 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                  Extracts data from EyeMed Provider from claim file and loads into a DataSets
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                           Development Project               Code Reviewer               Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                    ----------------------------------              ---------------------------------       -------------------------
# MAGIC Madhavan B                    2018-03-19            5744                                   Initial Programming                                                                 IntegrateDev2                      Kalyan Neelam                 2018-04-04
# MAGIC 
# MAGIC Goutham K                      2021-05-20         US-366403            New Provider file Change to include Loc and Svc loc id                         IntegrateDev1                     Jeyaprasanna                   2021-05-24

# MAGIC Job name: EyeMedIdsProvExtr
# MAGIC Remove Duplicate Prov Ids
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, concat, substring, length, rpad
from pyspark.sql.functions import trim as pyspark_trim
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")
SrcSysCd = get_widget_value("SrcSysCd", "")
RunID = get_widget_value("RunID", "")
EyeMedClmFile = get_widget_value("EyeMedClmFile", "")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT PROV_ID,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,PROV_SK FROM {IDSOwner}.K_PROV"
df_db2_K_PROV_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

schema_EYEMED_CLM = StructType([
    StructField("RCRD_TYP", StringType(), True),
    StructField("ADJ_VOID_FLAG", StringType(), True),
    StructField("EYEMED_GRP_ID", StringType(), True),
    StructField("EYEMED_SUBGRP_ID", StringType(), True),
    StructField("BILL_TYP_IN", StringType(), True),
    StructField("CLM_NO", StringType(), True),
    StructField("LN_CTR", StringType(), True),
    StructField("DT_OF_SVC", StringType(), True),
    StructField("INVC_NO", StringType(), True),
    StructField("INVC_DT", StringType(), True),
    StructField("BILL_AMT", StringType(), True),
    StructField("FFS_ADM_FEE", StringType(), True),
    StructField("RTL_AMT", StringType(), True),
    StructField("MBR_OOP", StringType(), True),
    StructField("THIRD_PARTY_DSCNT", StringType(), True),
    StructField("COPAY_AMT", StringType(), True),
    StructField("COV_AMT", StringType(), True),
    StructField("FLR_1", StringType(), True),
    StructField("FLR_2", StringType(), True),
    StructField("NTWK_IN", StringType(), True),
    StructField("SVC_CD", StringType(), True),
    StructField("SVC_DESC", StringType(), True),
    StructField("MOD_CD_1", StringType(), True),
    StructField("MOD_CD_2", StringType(), True),
    StructField("MOD_CD_3", StringType(), True),
    StructField("MOD_CD_4", StringType(), True),
    StructField("MOD_CD_5", StringType(), True),
    StructField("MOD_CD_6", StringType(), True),
    StructField("MOD_CD_7", StringType(), True),
    StructField("MOD_CD_8", StringType(), True),
    StructField("ICD_CD_SET", StringType(), True),
    StructField("DIAG_CD_1", StringType(), True),
    StructField("DIAG_CD_2", StringType(), True),
    StructField("DIAG_CD_3", StringType(), True),
    StructField("DIAG_CD_4", StringType(), True),
    StructField("DIAG_CD_5", StringType(), True),
    StructField("DIAG_CD_6", StringType(), True),
    StructField("DIAG_CD_7", StringType(), True),
    StructField("DIAG_CD_8", StringType(), True),
    StructField("PATN_ID", StringType(), True),
    StructField("PATN_SSN", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_MIDINIT", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("PATN_GNDR", StringType(), True),
    StructField("PATN_FMLY_RELSHP", StringType(), True),
    StructField("PATN_DOB", StringType(), True),
    StructField("PATN_ADDR", StringType(), True),
    StructField("PATN_ADDR_2", StringType(), True),
    StructField("PATN_CITY", StringType(), True),
    StructField("PATN_ST", StringType(), True),
    StructField("PATN_ZIP", StringType(), True),
    StructField("PATN_ZIP4", StringType(), True),
    StructField("CLNT_GRP_NO", StringType(), True),
    StructField("CO_CD", StringType(), True),
    StructField("DIV_CD", StringType(), True),
    StructField("LOC_CD", StringType(), True),
    StructField("CLNT_RPTNG_1", StringType(), True),
    StructField("CLNT_RPTNG_2", StringType(), True),
    StructField("CLNT_RPTNG_3", StringType(), True),
    StructField("CLNT_RPTNG_4", StringType(), True),
    StructField("CLNT_RPTNG_5", StringType(), True),
    StructField("CLS_PLN_ID", StringType(), True),
    StructField("SUB_ID", StringType(), True),
    StructField("SUB_SSN", StringType(), True),
    StructField("SUB_FIRST_NM", StringType(), True),
    StructField("SUB_MIDINIT", StringType(), True),
    StructField("SUB_LAST_NM", StringType(), True),
    StructField("SUB_GNDR", StringType(), True),
    StructField("SUB_DOB", StringType(), True),
    StructField("SUB_ADDR", StringType(), True),
    StructField("SUB_ADDR_2", StringType(), True),
    StructField("SUB_CITY", StringType(), True),
    StructField("SUB_ST", StringType(), True),
    StructField("SUB_ZIP", StringType(), True),
    StructField("SUB_ZIP_PLUS_4", StringType(), True),
    StructField("PROV_ID", StringType(), True),
    StructField("PROV_NPI", StringType(), True),
    StructField("TAX_ENTY_NPI", StringType(), True),
    StructField("PROV_FIRST_NM", StringType(), True),
    StructField("PROV_LAST_NM", StringType(), True),
    StructField("BUS_NM", StringType(), True),
    StructField("PROV_ADDR", StringType(), True),
    StructField("PROV_ADDR_2", StringType(), True),
    StructField("PROV_CITY", StringType(), True),
    StructField("PROV_ST", StringType(), True),
    StructField("PROV_ZIP", StringType(), True),
    StructField("PROV_ZIP_PLUS_4", StringType(), True),
    StructField("PROF_DSGTN", StringType(), True),
    StructField("TAX_ENTY_ID", StringType(), True),
    StructField("TXNMY_CD", StringType(), True),
    StructField("CLM_RCVD_DT", StringType(), True),
    StructField("ADJDCT_DT", StringType(), True),
    StructField("CHK_DT", StringType(), True),
    StructField("DENIAL_RSN_CD", StringType(), True),
    StructField("SVC_TYP", StringType(), True),
    StructField("UNIT_OF_SVC", StringType(), True),
    StructField("LOC_ID", StringType(), True),
    StructField("SVC_LOC_ID", StringType(), True),
    StructField("FLR", StringType(), True)
])

df_EYEMED_CLM = (
    spark.read
    .option("sep", ",")
    .option("header", "false")
    .option("quote", "\u0000")
    .schema(schema_EYEMED_CLM)
    .csv(f"{adls_path_raw}/landing/{EyeMedClmFile}")
)

df_ClmProv = (
    df_EYEMED_CLM
    .filter(
        (col("PROV_ID").isNotNull()) &
        (pyspark_trim(col("PROV_ID")) != "")
    )
    .select(
        concat(
            pyspark_trim(col("PROV_ID")),
            lit(":"),
            pyspark_trim(col("LOC_ID")),
            lit(":"),
            pyspark_trim(col("SVC_LOC_ID")),
            lit(":"),
            substring(
                pyspark_trim(col("TAX_ENTY_ID")),
                length(pyspark_trim(col("TAX_ENTY_ID"))) - 3,
                4
            )
        ).alias("PROV_ID"),
        pyspark_trim(col("PROV_NPI")).alias("PROV_NPI"),
        pyspark_trim(col("TAX_ENTY_NPI")).alias("TAX_ENTY_NPI"),
        pyspark_trim(col("PROV_FIRST_NM")).alias("PROV_FIRST_NM"),
        pyspark_trim(col("PROV_LAST_NM")).alias("PROV_LAST_NM"),
        pyspark_trim(col("BUS_NM")).alias("BUS_NM"),
        pyspark_trim(col("PROV_ADDR")).alias("PROV_ADDR"),
        pyspark_trim(col("PROV_ADDR_2")).alias("PROV_ADDR_2"),
        pyspark_trim(col("PROV_CITY")).alias("PROV_CITY"),
        pyspark_trim(col("PROV_ST")).alias("PROV_ST"),
        pyspark_trim(col("PROV_ZIP")).alias("PROV_ZIP"),
        pyspark_trim(col("PROV_ZIP_PLUS_4")).alias("PROV_ZIP_PLUS_4"),
        pyspark_trim(col("PROF_DSGTN")).alias("PROF_DSGTN"),
        pyspark_trim(col("TAX_ENTY_ID")).alias("TAX_ENTY_ID"),
        pyspark_trim(col("TXNMY_CD")).alias("TXNMY_CD"),
        concat(
            pyspark_trim(col("LOC_ID")),
            lit(":"),
            pyspark_trim(col("SVC_LOC_ID"))
        ).alias("PROV_ADDR_ID")
    )
)

df_RemDup_Prov = dedup_sort(df_ClmProv, ["PROV_ID"], [])

df_Dataset_Prov_in = df_RemDup_Prov.select(
    col("PROV_ID"),
    col("PROV_NPI"),
    col("TAX_ENTY_NPI"),
    col("PROV_FIRST_NM"),
    col("PROV_LAST_NM"),
    col("BUS_NM"),
    col("PROV_ADDR"),
    col("PROV_ADDR_2"),
    col("PROV_CITY"),
    col("PROV_ST"),
    col("PROV_ZIP"),
    col("PROV_ZIP_PLUS_4"),
    col("PROF_DSGTN"),
    col("TAX_ENTY_ID"),
    col("TXNMY_CD"),
    col("PROV_ADDR_ID")
)

df_Jnr_Prov = df_Dataset_Prov_in.alias("Dataset_Prov_in").join(
    df_db2_K_PROV_In.alias("lnk_KProv_extr"),
    on=[col("Dataset_Prov_in.PROV_ID") == col("lnk_KProv_extr.PROV_ID")],
    how="left"
)

df_AllProv = df_Jnr_Prov.select(
    col("Dataset_Prov_in.PROV_ID").alias("PROV_ID"),
    col("Dataset_Prov_in.PROV_NPI").alias("PROV_NPI"),
    col("Dataset_Prov_in.TAX_ENTY_NPI").alias("TAX_ENTY_NPI"),
    col("Dataset_Prov_in.PROV_FIRST_NM").alias("PROV_FIRST_NM"),
    col("Dataset_Prov_in.PROV_LAST_NM").alias("PROV_LAST_NM"),
    col("Dataset_Prov_in.BUS_NM").alias("BUS_NM"),
    col("Dataset_Prov_in.PROV_ADDR").alias("PROV_ADDR"),
    col("Dataset_Prov_in.PROV_ADDR_2").alias("PROV_ADDR_2"),
    col("Dataset_Prov_in.PROV_CITY").alias("PROV_CITY"),
    col("Dataset_Prov_in.PROV_ST").alias("PROV_ST"),
    col("Dataset_Prov_in.PROV_ZIP").alias("PROV_ZIP"),
    col("Dataset_Prov_in.PROV_ZIP_PLUS_4").alias("PROV_ZIP_PLUS_4"),
    col("Dataset_Prov_in.PROF_DSGTN").alias("PROF_DSGTN"),
    col("Dataset_Prov_in.TAX_ENTY_ID").alias("TAX_ENTY_ID"),
    col("Dataset_Prov_in.TXNMY_CD").alias("TXNMY_CD"),
    col("Dataset_Prov_in.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("lnk_KProv_extr.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_KProv_extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_KProv_extr.PROV_SK").alias("PROV_SK")
)

df_Only_Nee_Prov = (
    df_AllProv
    .filter(
        (pyspark_trim(col("PROV_SK")) == "") |
        (col("PROV_SK").isNull())
    )
    .select(
        col("PROV_ID"),
        col("PROV_NPI"),
        col("TAX_ENTY_NPI"),
        col("PROV_FIRST_NM"),
        col("PROV_LAST_NM"),
        col("BUS_NM"),
        col("PROV_ADDR"),
        col("PROV_ADDR_2"),
        col("PROV_CITY"),
        col("PROV_ST"),
        col("PROV_ZIP"),
        col("PROV_ZIP_PLUS_4"),
        col("PROF_DSGTN"),
        col("TAX_ENTY_ID"),
        col("TXNMY_CD"),
        col("PROV_ADDR_ID")
    )
)

df_final = (
    df_Only_Nee_Prov
    .withColumn("PROV_ID", rpad(col("PROV_ID"), 200, " "))
    .withColumn("PROV_NPI", rpad(col("PROV_NPI"), 200, " "))
    .withColumn("TAX_ENTY_NPI", rpad(col("TAX_ENTY_NPI"), 200, " "))
    .withColumn("PROV_FIRST_NM", rpad(col("PROV_FIRST_NM"), 200, " "))
    .withColumn("PROV_LAST_NM", rpad(col("PROV_LAST_NM"), 200, " "))
    .withColumn("BUS_NM", rpad(col("BUS_NM"), 200, " "))
    .withColumn("PROV_ADDR", rpad(col("PROV_ADDR"), 200, " "))
    .withColumn("PROV_ADDR_2", rpad(col("PROV_ADDR_2"), 200, " "))
    .withColumn("PROV_CITY", rpad(col("PROV_CITY"), 200, " "))
    .withColumn("PROV_ST", rpad(col("PROV_ST"), 2, " "))
    .withColumn("PROV_ZIP", rpad(col("PROV_ZIP"), 200, " "))
    .withColumn("PROV_ZIP_PLUS_4", rpad(col("PROV_ZIP_PLUS_4"), 200, " "))
    .withColumn("PROF_DSGTN", rpad(col("PROF_DSGTN"), 200, " "))
    .withColumn("TAX_ENTY_ID", rpad(col("TAX_ENTY_ID"), 200, " "))
    .withColumn("TXNMY_CD", rpad(col("TXNMY_CD"), 200, " "))
    .withColumn("PROV_ADDR_ID", rpad(col("PROV_ADDR_ID"), 200, " "))
)

write_files(
    df_final,
    f"{adls_path}/ds/PROV.{SrcSysCd}.extr.{RunID}.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)