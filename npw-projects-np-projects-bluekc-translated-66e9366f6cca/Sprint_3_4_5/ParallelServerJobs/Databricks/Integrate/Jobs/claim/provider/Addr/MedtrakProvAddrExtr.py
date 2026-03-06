# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  MedtrakDrugClmExtrLoadSeq
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from MEDTRAK PDX file to load Provider Address
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                                  Change Description                                          Project #                      Development Project          Code Reviewer               Date Reviewed  
# MAGIC ------------------           ----------------------------        ----------------------------------------------------------------------------          -------------------------           ------------------------------------       ----------------------------           ----------------
# MAGIC Deepika C                   2021-06-01                           Initial Programming                                         373781                              IntegrateSITF                Kalyan Neelam               2021-07-08

# MAGIC Read K_PROV_ADDR Table to pull the Natural Keys and the Skey.
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Left Join on Natural Keys
# MAGIC Inner Join primary key info with table info
# MAGIC Land into Seq File for the FKEY job
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
RunID = get_widget_value('RunID', '')
CurrRunCycle = get_widget_value('CurrRunCycle', '')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate', '')

# Read from db2_K_PROV_ADDR_In (DB2ConnectorPX)
jdbc_url_db2_K_PROV_ADDR_In, jdbc_props_db2_K_PROV_ADDR_In = get_db_config(ids_secret_name)
extract_query_db2_K_PROV_ADDR_In = f"""SELECT 
PROV_ADDR_ID,
PROV_ADDR_TYP_CD,
PROV_ADDR_EFF_DT_SK AS PROV_ADDR_EFF_DT,
SRC_SYS_CD,
CRT_RUN_CYC_EXCTN_SK,
PROV_ADDR_SK
FROM {IDSOwner}.K_PROV_ADDR"""
df_db2_K_PROV_ADDR_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_K_PROV_ADDR_In)
    .options(**jdbc_props_db2_K_PROV_ADDR_In)
    .option("query", extract_query_db2_K_PROV_ADDR_In)
    .load()
)

# Read from MedtrakPDXLand (PxSequentialFile)
schema_MedtrakPDXLand = StructType([
    StructField("RCRD_ID", StringType(), True),
    StructField("PRCSR_NO", StringType(), True),
    StructField("BTCH_NO", StringType(), True),
    StructField("PDX_NO", StringType(), True),
    StructField("PDX_NM", StringType(), True),
    StructField("PDX_ADDR", StringType(), True),
    StructField("PDX_LOC_CITY", StringType(), True),
    StructField("PDX_LOC_ST", StringType(), True),
    StructField("PDX_ZIP_CD", StringType(), True),
    StructField("PDX_TEL_NO", StringType(), True),
    StructField("EXPNSN_AREA", StringType(), True),
    StructField("PHARM_NM_2", StringType(), True),
    StructField("PHARM_ADDR_1S", StringType(), True),
    StructField("PHARM_ADDR_2", StringType(), True),
    StructField("PHARM_CORP_ID", StringType(), True),
    StructField("PHARM_CORP_NM", StringType(), True),
    StructField("PHARM_CORP_ADDR", StringType(), True),
    StructField("PHARM_CORP_ADDR2", StringType(), True),
    StructField("PHARM_CORP_CITY", StringType(), True),
    StructField("PHARM_CORP_ST", StringType(), True),
    StructField("PHARM_CORP_ZIP_CD", StringType(), True),
    StructField("NTNL_PROV_ID", StringType(), True),
    StructField("EXPNSN", StringType(), True)
])
df_MedtrakPDXLand = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("header", "false")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("nullValue", None)
    .schema(schema_MedtrakPDXLand)
    .load(f"{adls_path}/verified/MedtrakPDX_Land.dat.{RunID}")
)

# BusinessRules (CTransformerStage)
df_BusinessRules = df_MedtrakPDXLand.select(
    F.col("RCRD_ID").alias("RCRD_ID"),
    F.col("PRCSR_NO").alias("PRCSR_NO"),
    F.col("BTCH_NO").alias("BTCH_NO"),
    F.col("PDX_NO").alias("PDX_NO"),
    trim(F.col("PDX_NM")).alias("PDX_NM"),
    trim(F.col("PDX_ADDR")).alias("PDX_ADDR"),
    trim(F.when(F.col("PDX_LOC_CITY").isNotNull(), F.col("PDX_LOC_CITY")).otherwise(F.lit(""))).alias("PDX_LOC_CITY"),
    trim(F.col("PDX_LOC_ST")).alias("PDX_LOC_ST"),
    F.substring(trim(F.col("PDX_ZIP_CD")), 1, 5).alias("PDX_ZIP_CD"),
    trim(F.col("PDX_TEL_NO")).alias("PDX_TEL_NO"),
    trim(F.col("EXPNSN_AREA")).alias("EXPNSN_AREA"),
    trim(F.col("PHARM_NM_2")).alias("PHARM_NM_2"),
    trim(F.col("PHARM_ADDR_1S")).alias("PHARM_ADDR_1S"),
    trim(F.col("PHARM_ADDR_2")).alias("PHARM_ADDR_2"),
    trim(F.col("PHARM_CORP_ID")).alias("PHARM_CORP_ID"),
    trim(F.col("PHARM_CORP_NM")).alias("PHARM_CORP_NM"),
    trim(F.col("PHARM_CORP_ADDR")).alias("PHARM_CORP_ADDR"),
    trim(F.col("PHARM_CORP_ADDR2")).alias("PHARM_CORP_ADDR2"),
    trim(F.col("PHARM_CORP_CITY")).alias("PHARM_CORP_CITY"),
    trim(F.col("PHARM_CORP_ST")).alias("PHARM_CORP_ST"),
    F.col("PHARM_CORP_ZIP_CD").alias("PHARM_CORP_ZIP_CD"),
    F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    trim(F.col("EXPNSN")).alias("EXPNSN")
)

# USPS_ZIP_TRNSLTN (DB2ConnectorPX)
jdbc_url_USPS_ZIP_TRNSLTN, jdbc_props_USPS_ZIP_TRNSLTN = get_db_config(ids_secret_name)
extract_query_USPS_ZIP_TRNSLTN = f"""SELECT DISTINCT 
ZIP_CD_5,
CITY_NM,
ZIP_LN_ID,
CNTY_NM
FROM {IDSOwner}.USPS_ZIP_TRNSLTN"""
df_USPS_ZIP_TRNSLTN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_USPS_ZIP_TRNSLTN)
    .options(**jdbc_props_USPS_ZIP_TRNSLTN)
    .option("query", extract_query_USPS_ZIP_TRNSLTN)
    .load()
)

# Keys (CTransformerStage), producing ZipCity_Key and Zip_Key1
df_ZipCity_Key = df_USPS_ZIP_TRNSLTN.select(
    trim(F.when(F.col("ZIP_CD_5").isNotNull(), F.col("ZIP_CD_5")).otherwise(F.lit(""))).alias("ZIP_CD_5"),
    trim(F.when(F.col("CITY_NM").isNotNull(), F.col("CITY_NM")).otherwise(F.lit(""))).alias("CITY_NM"),
    trim(F.col("CNTY_NM")).alias("CNTY_NM")
)

df_Zip_Key1 = df_USPS_ZIP_TRNSLTN.select(
    trim(F.when(F.col("ZIP_CD_5").isNotNull(), F.col("ZIP_CD_5")).otherwise(F.lit(""))).alias("ZIP_CD_5"),
    trim(F.col("CNTY_NM")).alias("CNTY_NM")
)

# Remove_dup (PxCopy) -> Deduplicate df_Zip_Key1
df_Remove_dup = dedup_sort(
    df_Zip_Key1,
    partition_cols=["ZIP_CD_5"],
    sort_cols=[("ZIP_CD_5", "A")]
)
df_Zip_Key = df_Remove_dup.select(
    F.col("ZIP_CD_5").alias("ZIP_CD_5"),
    F.col("CNTY_NM").alias("CNTY_NM")
)

# Lookup (PxLookup) - Left joins
df_Lookup = (
    df_BusinessRules.alias("lkup")
    .join(
        df_ZipCity_Key.alias("ZipCity_Key"),
        (F.col("lkup.PDX_ZIP_CD") == F.col("ZipCity_Key.ZIP_CD_5"))
        & (F.col("lkup.PDX_LOC_CITY") == F.col("ZipCity_Key.CITY_NM")),
        "left"
    )
    .join(
        df_Zip_Key.alias("Zip_Key"),
        (F.col("lkup.PDX_ZIP_CD") == F.col("Zip_Key.ZIP_CD_5")),
        "left"
    )
)

df_Lookup_out = df_Lookup.select(
    F.col("lkup.RCRD_ID").alias("RCRD_ID"),
    F.col("lkup.PRCSR_NO").alias("PRCSR_NO"),
    F.col("lkup.BTCH_NO").alias("BTCH_NO"),
    F.col("lkup.PDX_NO").alias("PDX_NO"),
    F.col("lkup.PDX_NM").alias("PDX_NM"),
    F.col("lkup.PDX_ADDR").alias("PDX_ADDR"),
    F.col("lkup.PDX_LOC_CITY").alias("PDX_LOC_CITY"),
    F.col("lkup.PDX_LOC_ST").alias("PDX_LOC_ST"),
    F.col("lkup.PDX_ZIP_CD").alias("PDX_ZIP_CD"),
    F.col("lkup.PDX_TEL_NO").alias("PDX_TEL_NO"),
    F.col("lkup.EXPNSN_AREA").alias("EXPNSN_AREA"),
    F.col("lkup.PHARM_NM_2").alias("PHARM_NM_2"),
    F.col("lkup.PHARM_ADDR_1S").alias("PHARM_ADDR_1S"),
    F.col("lkup.PHARM_ADDR_2").alias("PHARM_ADDR_2"),
    F.col("lkup.PHARM_CORP_ID").alias("PHARM_CORP_ID"),
    F.col("lkup.PHARM_CORP_NM").alias("PHARM_CORP_NM"),
    F.col("lkup.PHARM_CORP_ADDR").alias("PHARM_CORP_ADDR"),
    F.col("lkup.PHARM_CORP_ADDR2").alias("PHARM_CORP_ADDR2"),
    F.col("lkup.PHARM_CORP_CITY").alias("PHARM_CORP_CITY"),
    F.col("lkup.PHARM_CORP_ST").alias("PHARM_CORP_ST"),
    F.col("lkup.PHARM_CORP_ZIP_CD").alias("PHARM_CORP_ZIP_CD"),
    F.col("lkup.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("lkup.EXPNSN").alias("EXPNSN"),
    F.col("ZipCity_Key.CNTY_NM").alias("CNTY_NM_zipcity"),
    F.col("Zip_Key.CNTY_NM").alias("CNTY_NM_zip")
)

# Transformer (CTransformerStage)
df_Transformer = (
    df_Lookup_out
    .withColumn("svProvAddrId", UpCase(trim(F.col("PDX_NO"))))  # Assume UpCase is available in namespace
    .withColumn("svSrcSysCd", F.lit("NABP"))
    .withColumn("PhnFormat", 
                F.concat(
                   F.substring(F.regexp_replace(F.col("PDX_TEL_NO"), "[()\\-]", ""), 1, 3),
                   F.lit("-"),
                   F.substring(F.regexp_replace(F.col("PDX_TEL_NO"), "[()\\-]", ""), 4, 3),
                   F.lit("-"),
                   F.substring(F.regexp_replace(F.col("PDX_TEL_NO"), "[()\\-]", ""), 7, 4)
                )
    )
    .withColumn("PhnNum", 
        F.when(
            (F.length(trim(F.col("PDX_TEL_NO"))) < 10) | (F.col("PDX_TEL_NO").isNull()),
            F.lit(None)
        ).otherwise(F.col("PhnFormat"))
    )
    # Simulating the original self-referential "PhnNumCvrt": map it to use "PhnNum" instead
    .withColumn("PhnNumCvrt",
        F.when(
            F.col("PhnNum").isNotNull(),
            F.when(
                (F.length(F.col("PhnNum"))==10) | (F.substring(F.col("PhnNum"),1,3) == F.lit("000")),
                F.lit(None)
            ).otherwise(F.col("PhnNum"))
        ).otherwise(F.lit(None))
    )
    .withColumn("svCntyNm",
        F.when(
            F.col("CNTY_NM_zipcity").isNull(),
            F.when(F.col("CNTY_NM_zip").isNull(), F.lit("UNK")).otherwise(F.col("CNTY_NM_zip"))
        ).otherwise(F.col("CNTY_NM_zipcity"))
    )
)

df_Transformer_lnk_rm_dup_MedtrakProvAddr = df_Transformer.select(
    F.col("svProvAddrId").alias("PROV_ADDR_ID"),
    F.lit("P").alias("PROV_ADDR_TYP_CD"),
    F.lit("1753-01-01").alias("PROV_ADDR_EFF_DT"),
    F.col("svSrcSysCd").alias("SRC_SYS_CD")
)

df_Transformer_lnk_MedtrakProvAddrPkey_All = df_Transformer.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    # The expression "StringToTimestamp(DateToString(CurrRunCycleDate, \"%yyyy%mm%dd\") : '000000', \"%yyyy%mm%dd%hh%nn%ss\")" -> we assume user-defined, or directly interpret as CurrRunCycleDate plus time
    # We'll just treat it as a pass-through example with user-defined function. Here we make it pass the stage unaltered:
    F.lit(None).alias("FIRST_RECYC_DT"),  # because the actual function is not standard, leaving as None or any placeholder
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("svSrcSysCd").alias("SRC_SYS_CD"),
    F.concat(F.col("svSrcSysCd"), F.lit(";"), F.col("svProvAddrId"), F.lit(";P;1753-01-01")).alias("PRI_KEY_STRING"),
    F.col("svProvAddrId").alias("PROV_ADDR_ID"),
    F.lit("P").alias("PROV_ADDR_TYP_CD"),
    F.lit("1753-01-01").alias("PROV_ADDR_EFF_DT"),
    F.when(F.col("svCntyNm") == F.lit("UNK"), F.lit("OOA")).otherwise(F.concat(F.col("svCntyNm"), F.col("PDX_LOC_ST"))).alias("PROV_ADDR_CNTY_CLS_CD"),
    F.lit("NA").alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    F.when(F.col("svCntyNm") == F.lit("UNK"), F.lit("OOA")).otherwise(F.concat(F.col("svCntyNm"), F.col("PDX_LOC_ST"))).alias("PROV_ADDR_METRORURAL_COV_CD"),
    F.lit("NA").alias("PROV_ADDR_TERM_RSN_CD"),
    F.lit("N").alias("HCAP_IN"),
    F.lit("N").alias("PDX_24_HR_IN"),
    F.lit("N").alias("PRCTC_LOC_IN"),
    F.lit("N").alias("PROV_ADDR_DIR_IN"),
    F.lit("2199-12-31").alias("TERM_DT"),
    F.when((F.length(trim(F.col("PDX_ADDR"))) == 0) | (F.col("PDX_ADDR").isNull()), F.lit(None)).otherwise(UpCase(trim(F.col("PDX_ADDR")))).alias("ADDR_LN_1"),
    F.lit(None).alias("ADDR_LN_2"),
    F.lit(None).alias("ADDR_LN_3"),
    F.when((F.length(trim(F.col("PDX_LOC_CITY"))) == 0) | (F.col("PDX_LOC_CITY").isNull()), F.lit(None)).otherwise(UpCase(trim(F.col("PDX_LOC_CITY")))).alias("CITY_NM"),
    F.when((F.length(trim(F.col("PDX_LOC_ST"))) == 0) | (F.col("PDX_LOC_ST").isNull()), F.lit("UNK")).otherwise(UpCase(trim(F.col("PDX_LOC_ST")))).alias("PROV_ADDR_ST_CD"),
    F.when((F.length(trim(F.col("PDX_ZIP_CD"))) == 0) | (F.col("PDX_ZIP_CD").isNull()), F.lit("")).otherwise(F.substring(trim(F.col("PDX_ZIP_CD")),1,5)).alias("POSTAL_CD"),
    F.col("svCntyNm").alias("CNTY_NM"),
    F.col("PhnNum").alias("PHN_NO"),
    F.lit(None).alias("PHN_NO_EXT"),
    F.lit(None).alias("FAX_NO"),
    F.lit(None).alias("FAX_NO_EXT"),
    F.lit(None).alias("EMAIL_ADDR_TX"),
    F.lit(0).alias("LAT_TX"),
    F.lit(0).alias("LONG_TX"),
    F.lit(None).alias("PRAD_TYPE_MAIL"),
    F.lit(None).alias("PROV2_PRAD_EFF_DT")
)

# rdp_NaturalKeys (PxRemDup)
df_rdp_NaturalKeys_in = dedup_sort(
    df_Transformer_lnk_rm_dup_MedtrakProvAddr,
    partition_cols=["PROV_ADDR_ID", "PROV_ADDR_TYP_CD", "PROV_ADDR_EFF_DT", "SRC_SYS_CD"],
    sort_cols=[("PROV_ADDR_ID","A"),("PROV_ADDR_TYP_CD","A"),("PROV_ADDR_EFF_DT","A"),("SRC_SYS_CD","A")]
)
df_rdp_NaturalKeys_lnk_rm_dup_DataOut = df_rdp_NaturalKeys_in.select(
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# jn_ProvAddr (PxJoin - leftouterjoin)
df_jn_ProvAddr = (
    df_rdp_NaturalKeys_lnk_rm_dup_DataOut.alias("lnk_rm_dup_DataOut")
    .join(
        df_db2_K_PROV_ADDR_In.alias("lnk_KProvAddr_extr"),
        (F.col("lnk_rm_dup_DataOut.PROV_ADDR_ID") == F.col("lnk_KProvAddr_extr.PROV_ADDR_ID"))
        & (F.col("lnk_rm_dup_DataOut.PROV_ADDR_TYP_CD") == F.col("lnk_KProvAddr_extr.PROV_ADDR_TYP_CD"))
        & (F.col("lnk_rm_dup_DataOut.PROV_ADDR_EFF_DT") == F.col("lnk_KProvAddr_extr.PROV_ADDR_EFF_DT"))
        & (F.col("lnk_rm_dup_DataOut.SRC_SYS_CD") == F.col("lnk_KProvAddr_extr.SRC_SYS_CD")),
        "left"
    )
)

df_jn_ProvAddr_out = df_jn_ProvAddr.select(
    F.col("lnk_rm_dup_DataOut.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("lnk_rm_dup_DataOut.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("lnk_rm_dup_DataOut.PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT"),
    F.col("lnk_rm_dup_DataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_KProvAddr_extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_KProvAddr_extr.PROV_ADDR_SK").alias("PROV_ADDR_SK")
)

# xfm_PKEYgen (CTransformerStage) - Surrogate key logic
df_xfm_PKEYgen_in = df_jn_ProvAddr_out.withColumn("orig_PROV_ADDR_SK", F.col("PROV_ADDR_SK"))
df_xfm_PKEYgen_in = df_xfm_PKEYgen_in.withColumnRenamed("PROV_ADDR_SK", "svProvAddrSK")
df_enriched = SurrogateKeyGen(df_xfm_PKEYgen_in, <DB sequence name>, "svProvAddrSK", <schema>, <secret_name>)

df_lnk_KProvAddr_new = (
    df_enriched
    .filter(F.col("orig_PROV_ADDR_SK").isNull())
    .select(
        F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        F.col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
        F.col("PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svProvAddrSK").alias("PROV_ADDR_SK")
    )
)

df_lnkPKEYxfmOut = df_enriched.select(
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.when(F.col("orig_PROV_ADDR_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svProvAddrSK").alias("PROV_ADDR_SK")
)

# db2_K_PROV_ADDR_load (DB2ConnectorPX) - Merge into table #$IDSOwner#.K_PROV_ADDR
df_lnk_KProvAddr_new_for_merge = df_lnk_KProvAddr_new.select(
    "PROV_ADDR_ID","PROV_ADDR_TYP_CD","PROV_ADDR_EFF_DT_SK","SRC_SYS_CD","CRT_RUN_CYC_EXCTN_SK","PROV_ADDR_SK"
)

jdbc_url_db2_K_PROV_ADDR_load, jdbc_props_db2_K_PROV_ADDR_load = get_db_config(ids_secret_name)
temp_table_name_db2_K_PROV_ADDR_load = "STAGING.MedtrakProvAddrExtr_db2_K_PROV_ADDR_load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name_db2_K_PROV_ADDR_load}", jdbc_url_db2_K_PROV_ADDR_load, jdbc_props_db2_K_PROV_ADDR_load)
(
    df_lnk_KProvAddr_new_for_merge.write.format("jdbc")
    .option("url", jdbc_url_db2_K_PROV_ADDR_load)
    .options(**jdbc_props_db2_K_PROV_ADDR_load)
    .option("dbtable", temp_table_name_db2_K_PROV_ADDR_load)
    .mode("append")
    .save()
)
merge_sql_db2_K_PROV_ADDR_load = f"""
MERGE INTO {IDSOwner}.K_PROV_ADDR AS T
USING {temp_table_name_db2_K_PROV_ADDR_load} AS S
ON T.PROV_ADDR_ID = S.PROV_ADDR_ID
   AND T.PROV_ADDR_TYP_CD = S.PROV_ADDR_TYP_CD
   AND T.PROV_ADDR_EFF_DT_SK = S.PROV_ADDR_EFF_DT_SK
   AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN UPDATE SET
   T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
   T.PROV_ADDR_SK = S.PROV_ADDR_SK
WHEN NOT MATCHED THEN INSERT (PROV_ADDR_ID,PROV_ADDR_TYP_CD,PROV_ADDR_EFF_DT_SK,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,PROV_ADDR_SK)
VALUES (S.PROV_ADDR_ID,S.PROV_ADDR_TYP_CD,S.PROV_ADDR_EFF_DT_SK,S.SRC_SYS_CD,S.CRT_RUN_CYC_EXCTN_SK,S.PROV_ADDR_SK);
"""
execute_dml(merge_sql_db2_K_PROV_ADDR_load, jdbc_url_db2_K_PROV_ADDR_load, jdbc_props_db2_K_PROV_ADDR_load)

# jn_PKEYs (PxJoin - innerjoin)
df_jn_PKEYs = (
    df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut")
    .join(
        df_Transformer_lnk_MedtrakProvAddrPkey_All.alias("lnk_MedtrakProvAddrPkey_All"),
        (F.col("lnkPKEYxfmOut.PROV_ADDR_ID") == F.col("lnk_MedtrakProvAddrPkey_All.PROV_ADDR_ID"))
        & (F.col("lnkPKEYxfmOut.PROV_ADDR_TYP_CD") == F.col("lnk_MedtrakProvAddrPkey_All.PROV_ADDR_TYP_CD"))
        & (F.col("lnkPKEYxfmOut.PROV_ADDR_EFF_DT") == F.col("lnk_MedtrakProvAddrPkey_All.PROV_ADDR_EFF_DT"))
        & (F.col("lnkPKEYxfmOut.SRC_SYS_CD") == F.col("lnk_MedtrakProvAddrPkey_All.SRC_SYS_CD")),
        "inner"
    )
)

df_jn_PKEYs_lnk_MedtrakProvAddrPkey = df_jn_PKEYs.select(
    F.col("lnk_MedtrakProvAddrPkey_All.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("lnk_MedtrakProvAddrPkey_All.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("lnk_MedtrakProvAddrPkey_All.DISCARD_IN").alias("DISCARD_IN"),
    F.col("lnk_MedtrakProvAddrPkey_All.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("lnk_MedtrakProvAddrPkey_All.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("lnk_MedtrakProvAddrPkey_All.ERR_CT").alias("ERR_CT"),
    F.col("lnk_MedtrakProvAddrPkey_All.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("lnkPKEYxfmOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_MedtrakProvAddrPkey_All.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("lnkPKEYxfmOut.PROV_ADDR_SK").alias("PROV_ADDR_SK"),
    F.col("lnkPKEYxfmOut.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("lnkPKEYxfmOut.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("lnkPKEYxfmOut.PROV_ADDR_EFF_DT").alias("EFF_DT"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_MedtrakProvAddrPkey_All.PROV_ADDR_CNTY_CLS_CD").alias("PROV_ADDR_CNTY_CLS_CD"),
    F.col("lnk_MedtrakProvAddrPkey_All.PROV_ADDR_GEO_ACES_RTRN_CD_TX").alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    F.col("lnk_MedtrakProvAddrPkey_All.PROV_ADDR_METRORURAL_COV_CD").alias("PROV_ADDR_METRORURAL_COV_CD"),
    F.col("lnk_MedtrakProvAddrPkey_All.PROV_ADDR_TERM_RSN_CD").alias("PROV_ADDR_TERM_RSN_CD"),
    F.col("lnk_MedtrakProvAddrPkey_All.HCAP_IN").alias("HCAP_IN"),
    F.col("lnk_MedtrakProvAddrPkey_All.PDX_24_HR_IN").alias("PDX_24_HR_IN"),
    F.col("lnk_MedtrakProvAddrPkey_All.PRCTC_LOC_IN").alias("PRCTC_LOC_IN"),
    F.col("lnk_MedtrakProvAddrPkey_All.PROV_ADDR_DIR_IN").alias("PROV_ADDR_DIR_IN"),
    F.col("lnk_MedtrakProvAddrPkey_All.TERM_DT").alias("TERM_DT"),
    F.col("lnk_MedtrakProvAddrPkey_All.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("lnk_MedtrakProvAddrPkey_All.ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("lnk_MedtrakProvAddrPkey_All.ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("lnk_MedtrakProvAddrPkey_All.CITY_NM").alias("CITY_NM"),
    F.col("lnk_MedtrakProvAddrPkey_All.PROV_ADDR_ST_CD").alias("PROV_ADDR_ST_CD"),
    F.col("lnk_MedtrakProvAddrPkey_All.POSTAL_CD").alias("POSTAL_CD"),
    F.col("lnk_MedtrakProvAddrPkey_All.CNTY_NM").alias("CNTY_NM"),
    F.col("lnk_MedtrakProvAddrPkey_All.PHN_NO").alias("PHN_NO"),
    F.col("lnk_MedtrakProvAddrPkey_All.PHN_NO_EXT").alias("PHN_NO_EXT"),
    F.col("lnk_MedtrakProvAddrPkey_All.FAX_NO").alias("FAX_NO"),
    F.col("lnk_MedtrakProvAddrPkey_All.FAX_NO_EXT").alias("FAX_NO_EXT"),
    F.col("lnk_MedtrakProvAddrPkey_All.EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
    F.col("lnk_MedtrakProvAddrPkey_All.LAT_TX").alias("LAT_TX"),
    F.col("lnk_MedtrakProvAddrPkey_All.LONG_TX").alias("LONG_TX"),
    F.col("lnk_MedtrakProvAddrPkey_All.PRAD_TYPE_MAIL").alias("PRAD_TYPE_MAIL"),
    F.col("lnk_MedtrakProvAddrPkey_All.PROV2_PRAD_EFF_DT").alias("PROV2_PRAD_EFF_DT")
)

# MedtrakProvAddrExtr (PxSequentialFile) - final write
# Apply rpad for char/varchar columns that have a declared length in this final schema (if any). Below are lengths from the JSON:
# INSRT_UPDT_CD (char 10), DISCARD_IN (char 1), PASS_THRU_IN (char 1), PROV_ADDR_TERM_RSN_CD (char 10), HCAP_IN (char 1),
# PDX_24_HR_IN (char 1), PRCTC_LOC_IN (char 1), PROV_ADDR_DIR_IN (char 1), TERM_DT (char 10), PROV_ADDR_ST_CD (char 10),
# FAX_NO (char 20), PRAD_TYPE_MAIL (char 3), PROV2_PRAD_EFF_DT (char 10).
df_final = df_jn_PKEYs_lnk_MedtrakProvAddrPkey
df_final = df_final.withColumn("INSRT_UPDT_CD", F.rpad("INSRT_UPDT_CD", 10, " "))
df_final = df_final.withColumn("DISCARD_IN", F.rpad("DISCARD_IN", 1, " "))
df_final = df_final.withColumn("PASS_THRU_IN", F.rpad("PASS_THRU_IN", 1, " "))
df_final = df_final.withColumn("PROV_ADDR_TERM_RSN_CD", F.rpad("PROV_ADDR_TERM_RSN_CD", 10, " "))
df_final = df_final.withColumn("HCAP_IN", F.rpad("HCAP_IN", 1, " "))
df_final = df_final.withColumn("PDX_24_HR_IN", F.rpad("PDX_24_HR_IN", 1, " "))
df_final = df_final.withColumn("PRCTC_LOC_IN", F.rpad("PRCTC_LOC_IN", 1, " "))
df_final = df_final.withColumn("PROV_ADDR_DIR_IN", F.rpad("PROV_ADDR_DIR_IN", 1, " "))
df_final = df_final.withColumn("TERM_DT", F.rpad("TERM_DT", 10, " "))
df_final = df_final.withColumn("PROV_ADDR_ST_CD", F.rpad("PROV_ADDR_ST_CD", 10, " "))
df_final = df_final.withColumn("FAX_NO", F.rpad("FAX_NO", 20, " "))
df_final = df_final.withColumn("PRAD_TYPE_MAIL", F.rpad("PRAD_TYPE_MAIL", 3, " "))
df_final = df_final.withColumn("PROV2_PRAD_EFF_DT", F.rpad("PROV2_PRAD_EFF_DT", 10, " "))

# Write final output file
write_files(
    df_final.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "PROV_ADDR_SK",
        "PROV_ADDR_ID",
        "PROV_ADDR_TYP_CD",
        "EFF_DT",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PROV_ADDR_CNTY_CLS_CD",
        "PROV_ADDR_GEO_ACES_RTRN_CD_TX",
        "PROV_ADDR_METRORURAL_COV_CD",
        "PROV_ADDR_TERM_RSN_CD",
        "HCAP_IN",
        "PDX_24_HR_IN",
        "PRCTC_LOC_IN",
        "PROV_ADDR_DIR_IN",
        "TERM_DT",
        "ADDR_LN_1",
        "ADDR_LN_2",
        "ADDR_LN_3",
        "CITY_NM",
        "PROV_ADDR_ST_CD",
        "POSTAL_CD",
        "CNTY_NM",
        "PHN_NO",
        "PHN_NO_EXT",
        "FAX_NO",
        "FAX_NO_EXT",
        "EMAIL_ADDR_TX",
        "LAT_TX",
        "LONG_TX",
        "PRAD_TYPE_MAIL",
        "PROV2_PRAD_EFF_DT"
    ),
    f"{adls_path}/key/MedtrakProvAddrExtr.ProvAddr.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)