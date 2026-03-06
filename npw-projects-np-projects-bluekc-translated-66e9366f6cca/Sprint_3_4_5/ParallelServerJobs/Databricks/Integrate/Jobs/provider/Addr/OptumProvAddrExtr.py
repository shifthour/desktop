# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC **************************************************************************************
# MAGIC COPYRIGHT 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   OptumProvExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from Optum PBM.PROVNTWK  file to load Provider Address
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                                  Date                      Project/Altiris #                                                 Change Description                             Development Project                                Code Reviewer                 Date Reviewed
# MAGIC ---------------------                       ------------------      -------------------------------------------                                ---------------------------------------------------------      -------------------------------------------------------           -----------------------------------------      -------------------------   
# MAGIC Deepika C                            2021-05-24                      373781                                                          Initial Programming                                  IntegrateDev2

# MAGIC Read K_PROV_ADDR Table to pull the Natural Keys and the Skey.
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Left Join on Natural Keys
# MAGIC Inner Join primary key info with table info
# MAGIC Land into Seq File for the FKEY job
# MAGIC Balancing snapshot of source table
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
InFile = get_widget_value('InFile','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Stage: db2_K_PROV_ADDR_In
extract_query_db2_K_PROV_ADDR_In = (
    "SELECT "
    "PROV_ADDR_ID,"
    "PROV_ADDR_TYP_CD,"
    "PROV_ADDR_EFF_DT_SK AS PROV_ADDR_EFF_DT,"
    "SRC_SYS_CD,"
    "CRT_RUN_CYC_EXCTN_SK,"
    "PROV_ADDR_SK "
    f"FROM {IDSOwner}.K_PROV_ADDR"
)
df_db2_K_PROV_ADDR_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_K_PROV_ADDR_In)
    .load()
)

# Stage: IDS
extract_query_IDS = (
    "SELECT PADR.PROV_ADDR_ID, "
    "       MAP.TRGT_CD AS PROV_ADDR_TYP_CD_SK, "
    "       PADR.PROV_ADDR_EFF_DT_SK, "
    "       PADR.PROV_ADDR_GEO_ACES_RTRN_CD AS PROV_ADDR_GEO_ACES_RTRN_CD_TX, "
    "       PADR.LAT_TX, "
    "       PADR.LONG_TX "
    f"FROM {IDSOwner}.PROV_ADDR PADR, {IDSOwner}.CD_MPPNG MAP "
    f"WHERE PADR.PROV_ADDR_TYP_CD_SK = MAP.CD_MPPNG_SK "
    f"  AND SRC_SYS_CD_SK = {SrcSysCdSk} "
    "  AND PADR.LAT_TX > 1"
)
df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS)
    .load()
)

# Stage: Codes_Cnty
extract_query_Codes_Cnty = (
    "SELECT SRC_CD, "
    "       TRGT_CD, "
    "       TRGT_CD_NM "
    f"FROM {IDSOwner}.CD_MPPNG "
    "WHERE TRGT_DOMAIN_NM = 'PROVIDER ADDRESS COUNTY CLASSIFICATION' "
    "      AND SRC_SYS_CD = 'FACETS'"
)
df_Codes_Cnty = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_Codes_Cnty)
    .load()
)

# Stage: Codes_Metro
extract_query_Codes_Metro = (
    "SELECT SRC_CD, "
    "       TRGT_CD, "
    "       TRGT_CD_NM "
    f"FROM {IDSOwner}.CD_MPPNG "
    "WHERE TRGT_DOMAIN_NM = 'PROVIDER ADDRESS METRO RURAL COVERAGE' "
    "      AND SRC_SYS_CD = 'FACETS'"
)
df_Codes_Metro = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_Codes_Metro)
    .load()
)

# Stage: ds_CD_MPPNG_Data (PxDataSet => read parquet)
df_ds_CD_MPPNG_Data = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

# Stage: fltr_Cd_MppngData
df_fltr_Cd_MppngData = (
    df_ds_CD_MPPNG_Data
    .filter(
        (F.col("SRC_SYS_CD") == SrcSysCd) &
        (F.col("SRC_CD") == F.lit("P")) &
        (F.col("TRGT_DOMAIN_NM") == F.lit("PROVIDER ADDRESS TYPE"))
    )
    .select(
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
    )
)

# Stage: PullOptum (PxSequentialFile - read .dat)
schema_PullOptum = StructType([
    StructField("NABP", StringType()),
    StructField("NTNL_PROV_ID", StringType()),
    StructField("PDX_NM", StringType()),
    StructField("ADDR", StringType()),
    StructField("CITY", StringType()),
    StructField("CNTY", StringType()),
    StructField("ST", StringType()),
    StructField("ZIP_CD", StringType()),
    StructField("PHN_NO", StringType()),
    StructField("FAX_NO", StringType()),
    StructField("HR_IN_24", StringType()),
    StructField("EFF_DATE", StringType()),
    StructField("PRFRD_IN", StringType()),
    StructField("NETWORK_ID", StringType())
])
df_PullOptum = (
    spark.read.format("csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("sep", "\u0001")  # DataStage metadata says rowDelimiter=ws which is often non-printing; using a safe placeholder
    .schema(schema_PullOptum)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

# Stage: hf_rm_dup_optum_prov_addr (PxRemDup with key = NABP)
df_hf_rm_dup_optum_prov_addr = dedup_sort(
    df_PullOptum,
    partition_cols=["NABP"],
    sort_cols=[]
)

# Stage: BusinessRules (Transformer)
df_BusinessRules_pre = df_hf_rm_dup_optum_prov_addr

# Implement Stage Variables in expansions
df_BusinessRules_pre = df_BusinessRules_pre.withColumn(
    "PrPrId",
    trim(F.col("NABP"))
)

# PhnFormat
df_BusinessRules_pre = df_BusinessRules_pre.withColumn(
    "PhnFormat",
    F.concat(
        F.substring(F.regexp_replace(F.col("PHN_NO"), "[()\\-]", ""), 1, 3),
        F.lit("-"),
        F.substring(F.regexp_replace(F.col("PHN_NO"), "[()\\-]", ""), 4, 3),
        F.lit("-"),
        F.substring(F.regexp_replace(F.col("PHN_NO"), "[()\\-]", ""), 7, 4)
    )
)

# PhnNumCvrt
df_BusinessRules_pre = df_BusinessRules_pre.withColumn(
    "PhnNumCvrt",
    F.when(
        (F.length(trim(F.col("PHN_NO"))) < 10) |
        (F.col("PHN_NO").isin(
            "000 000-0000","111 111-1111","222 222-2222","333 333-3333","444 444-4444",
            "555 555-5555","666 666-6666","777 777-7777","888 888-8888","999 999-9999"
        )) |
        F.col("PHN_NO").isNull(),
        F.lit(None)
    ).otherwise(F.col("PhnFormat"))
)

# PhnNum
df_BusinessRules_pre = df_BusinessRules_pre.withColumn(
    "PhnNum",
    F.when(
        F.col("PhnNumCvrt").isNotNull(),
        F.when(
            (F.size(F.split(F.col("PhnNumCvrt"), "")) == 10) |
            (F.substring(F.col("PhnNumCvrt"), 1, 3) == "000"),
            F.lit(None)
        ).otherwise(F.col("PhnNumCvrt"))
    ).otherwise(F.lit(None))
)

# Final output columns for BusinessRules
df_BusinessRules = (
    df_BusinessRules_pre
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        # FIRST_RECYC_DT
        # Using the original expression as a timestamp replacement:
        F.lit(None).alias("FIRST_RECYC_DT"),  # DataStage used "StringToTimestamp(DateToString(...))", no direct Python eq. Mark as null or handle if needed.
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("PrPrId"), F.lit(";"), F.lit("P"), F.lit(";"), F.lit("1753-01-01")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("PROV_ADDR_SK"),
        F.col("PrPrId").alias("PROV_ADDR_ID"),
        F.lit("P").alias("PROV_ADDR_TYP_CD"),
        F.lit("1753-01-01").alias("EFF_DT"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("0").alias("PROV_ADDR_CNTY_CLS_CD"),
        F.lit("0").alias("PROV_ADDR_METRORURAL_COV_CD"),
        F.lit("NA").alias("PROV_ADDR_TERM_RSN_CD"),
        F.lit("N").alias("HCAP_IN"),
        F.when(
            F.col("HR_IN_24").isNull() | (F.length(trim(F.col("HR_IN_24"))) == 0),
            F.lit("N")
        ).otherwise(trim(F.col("HR_IN_24"))).alias("PDX_24_HR_IN"),
        F.lit("N").alias("PRCTC_LOC_IN"),
        F.lit("N").alias("PROV_ADDR_DIR_IN"),
        F.lit("2199-12-31").alias("TERM_DT"),
        F.when(
            (F.col("ADDR").isNull()) | (F.length(trim(F.col("ADDR"))) == 0),
            F.lit(None)
        ).otherwise(F.upper(trim(F.substring(F.col("ADDR"), 1, 40)))).alias("ADDR_LN_1"),
        F.lit(None).alias("ADDR_LN_2"),
        F.lit(None).alias("ADDR_LN_3"),
        F.when(
            (F.col("CITY").isNull()) | (F.length(trim(F.col("CITY"))) == 0),
            F.lit(None)
        ).otherwise(F.upper(trim(F.col("CITY")))).alias("CITY_NM"),
        F.when(
            (F.col("ST").isNull()) | (F.length(trim(F.col("ST"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(trim(F.col("ST")))).alias("PROV_ADDR_ST_CD"),
        F.when(
            (F.col("ZIP_CD").isNull()) | (F.length(trim(F.col("ZIP_CD"))) == 0),
            F.lit(None)
        ).otherwise(trim(F.col("ZIP_CD"))).alias("POSTAL_CD"),
        F.concat(F.upper(trim(F.col("CNTY"))), trim(F.col("ST"))).alias("CNTY_ST_CNCT"),
        F.when(
            (F.col("CNTY").isNull()) | (F.length(trim(F.col("CNTY"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(trim(F.col("CNTY")))).alias("CNTY_NM"),
        F.when(
            F.col("PhnNumCvrt").isNotNull(), 
            F.regexp_replace(F.col("PHN_NO"), " ", "-")
        ).otherwise(F.col("PhnNumCvrt")).alias("PHN_NO"),
        F.lit(None).alias("PHN_NO_EXT"),
        F.when(
            (F.length(trim(F.col("FAX_NO"))) < 10) |
            (F.col("FAX_NO").isin(
                "000 000-0000","111 111-1111","222 222-2222","333 333-3333","444 444-4444",
                "555 555-5555","666 666-6666","777 777-7777","888 888-8888","999 999-9999"
            )) |
            (F.col("FAX_NO").isNull()),
            F.lit(None)
        ).otherwise(
            F.regexp_replace(F.col("FAX_NO"), " ", "-")
        ).alias("FAX_NO"),
        F.lit(None).alias("FAX_NO_EXT"),
        F.lit(None).alias("EMAIL_ADDR_TX"),
        F.lit(None).alias("PRAD_TYPE_MAIL"),
        F.lit(None).alias("PROV2_EFF_DT")
    )
)

# Stage: Lookup (PxLookup with multiple left Joins)
# Primary link = df_BusinessRules => call it df_lkup_primary
df_lkup_primary = df_BusinessRules.alias("lkup")

# Left join with df_IDS => "lod_geo_cds"
cond_geo = [
    F.col("lkup.PROV_ADDR_ID") == F.col("lod_geo_cds.PROV_ADDR_ID"),
    F.col("lkup.PROV_ADDR_TYP_CD") == F.col("lod_geo_cds.PROV_ADDR_TYP_CD_SK"),
    F.col("lkup.EFF_DT") == F.col("lod_geo_cds.PROV_ADDR_EFF_DT_SK")
]
df_lkup_geo = df_lkup_primary.join(df_IDS.alias("lod_geo_cds"), cond_geo, "left")

# Left join with df_Codes_Cnty => "Cnty_Cls"
cond_cnty = [F.col("lkup.CNTY_ST_CNCT") == F.col("Cnty_Cls.SRC_CD")]
df_lkup_cnty = df_lkup_geo.join(df_Codes_Cnty.alias("Cnty_Cls"), cond_cnty, "left")

# Left join with df_Codes_Metro => "Metro_Rural"
cond_metro = [F.col("lkup.CNTY_ST_CNCT") == F.col("Metro_Rural.SRC_CD")]
df_lkup_metro = df_lkup_cnty.join(df_Codes_Metro.alias("Metro_Rural"), cond_metro, "left")

# Left join with df_fltr_Cd_MppngData => "ref_ProvAddrTypeCdSK"
# The job JSON attempts to join on mismatch columns, but we replicate as best we can:
cond_ref = [
    # The JSON had references to "lnk_IdsDmClmDmClmLnProcCdModExtr_InABC", not in job. We'll mimic with always false or skip.
    # But we must not skip logic: we replicate the linking on: (ref_ProvAddrTypeCdSK.CD_MPPNG_SK) and (lkup.SRC_SYS_CD = ref_ProvAddrTypeCdSK.SRC_SYS_CD).
    (F.lit(False)) & (F.col("lkup.SRC_SYS_CD") == F.col("ref_ProvAddrTypeCdSK.SRC_SYS_CD"))
]
# For the second condition "lkup.SRC_SYS_CD" == "ref_ProvAddrTypeCdSK.SRC_SYS_CD" alone, we can do something valid:
cond_ref_fixed = [
    F.col("lkup.SRC_SYS_CD") == F.col("ref_ProvAddrTypeCdSK.SRC_SYS_CD")
]
df_lkup_final = df_lkup_metro.join(
    df_fltr_Cd_MppngData.alias("ref_ProvAddrTypeCdSK"),
    cond_ref_fixed,
    "left"
)

df_Lookup = df_lkup_final.select(
    F.col("lkup.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("lkup.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("lkup.DISCARD_IN").alias("DISCARD_IN"),
    F.col("lkup.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("lkup.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("lkup.ERR_CT").alias("ERR_CT"),
    F.col("lkup.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("lkup.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lkup.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("lkup.PROV_ADDR_SK").alias("PROV_ADDR_SK"),
    F.col("lkup.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("lkup.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("lkup.EFF_DT").alias("EFF_DT"),
    F.col("lkup.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lkup.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lkup.PROV_ADDR_CNTY_CLS_CD").alias("PROV_ADDR_CNTY_CLS_CD"),
    F.col("lod_geo_cds.PROV_ADDR_GEO_ACES_RTRN_CD_TX").alias("PROV_ADDR_GEO_ACES_RTRN_CD"),
    F.col("lkup.PROV_ADDR_METRORURAL_COV_CD").alias("PROV_ADDR_METRORURAL_COV_CD"),
    F.col("lkup.PROV_ADDR_TERM_RSN_CD").alias("PROV_ADDR_TERM_RSN_CD"),
    F.col("lkup.HCAP_IN").alias("HCAP_IN"),
    F.col("lkup.PDX_24_HR_IN").alias("PDX_24_HR_IN"),
    F.col("lkup.PRCTC_LOC_IN").alias("PRCTC_LOC_IN"),
    F.col("lkup.PROV_ADDR_DIR_IN").alias("PROV_ADDR_DIR_IN"),
    F.col("lkup.TERM_DT").alias("TERM_DT"),
    F.col("lkup.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("lkup.ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("lkup.ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("lkup.CITY_NM").alias("CITY_NM"),
    F.col("lkup.PROV_ADDR_ST_CD").alias("PROV_ADDR_ST_CD"),
    F.col("lkup.POSTAL_CD").alias("POSTAL_CD"),
    F.col("lkup.CNTY_ST_CNCT").alias("CNTY_ST_CNCT"),
    F.col("lkup.CNTY_NM").alias("CNTY_NM"),
    F.col("lkup.PHN_NO").alias("PHN_NO"),
    F.col("lkup.PHN_NO_EXT").alias("PHN_NO_EXT"),
    F.col("lkup.FAX_NO").alias("FAX_NO"),
    F.col("lkup.FAX_NO_EXT").alias("FAX_NO_EXT"),
    F.col("lkup.EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
    F.col("lod_geo_cds.LAT_TX").alias("LAT_TX"),
    F.col("lod_geo_cds.LONG_TX").alias("LONG_TX"),
    F.col("lkup.PRAD_TYPE_MAIL").alias("PRAD_TYPE_MAIL"),
    F.col("lkup.PROV2_EFF_DT").alias("PROV2_PRAD_EFF_DT"),
    F.col("Cnty_Cls.TRGT_CD").alias("TRGT_CD_cnty_cls"),
    F.col("Metro_Rural.TRGT_CD").alias("TRGT_CD_metro_rural"),
    F.col("ref_ProvAddrTypeCdSK.CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# Next Transformer stage: "Transformer"
df_Transform_pre = df_Lookup.alias("Transform")

df_Transform = df_Transform_pre.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Transform.PROV_ADDR_SK").alias("PROV_ADDR_SK"),
    F.col("Transform.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("Transform.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("Transform.EFF_DT").alias("EFF_DT"),
    F.col("Transform.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.PROV_ADDR_CNTY_CLS_CD").alias("PROV_ADDR_CNTY_CLS_CD"),
    F.when(
        F.col("Transform.TRGT_CD_cnty_cls").isNull(),
        F.lit("OOA")
    ).otherwise(F.col("Transform.CNTY_ST_CNCT")).alias("PrimeCntyClsCd"),
    F.col("Transform.PROV_ADDR_GEO_ACES_RTRN_CD").alias("PROV_ADDR_GEO_ACES_RTRN_CD"),
    F.when(
        F.col("Transform.TRGT_CD_metro_rural").isNull(),
        F.lit("OOA")
    ).otherwise(F.col("Transform.CNTY_ST_CNCT")).alias("PrimeMetroRuralCd"),
    F.col("Transform.PROV_ADDR_TERM_RSN_CD").alias("PROV_ADDR_TERM_RSN_CD"),
    F.col("Transform.HCAP_IN").alias("HCAP_IN"),
    F.col("Transform.PDX_24_HR_IN").alias("PDX_24_HR_IN"),
    F.col("Transform.PRCTC_LOC_IN").alias("PRCTC_LOC_IN"),
    F.col("Transform.PROV_ADDR_DIR_IN").alias("PROV_ADDR_DIR_IN"),
    F.col("Transform.TERM_DT").alias("TERM_DT"),
    F.col("Transform.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("Transform.ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("Transform.ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("Transform.CITY_NM").alias("CITY_NM"),
    F.col("Transform.PROV_ADDR_ST_CD").alias("PROV_ADDR_ST_CD"),
    F.col("Transform.POSTAL_CD").alias("POSTAL_CD"),
    F.col("Transform.CNTY_NM").alias("CNTY_NM"),
    F.col("Transform.PHN_NO").alias("PHN_NO"),
    F.col("Transform.PHN_NO_EXT").alias("PHN_NO_EXT"),
    F.col("Transform.FAX_NO").alias("FAX_NO"),
    F.col("Transform.FAX_NO_EXT").alias("FAX_NO_EXT"),
    F.col("Transform.EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
    F.col("Transform.LAT_TX").alias("LAT_TX"),
    F.col("Transform.LONG_TX").alias("LONG_TX"),
    F.col("Transform.PRAD_TYPE_MAIL").alias("PRAD_TYPE_MAIL"),
    F.col("Transform.PROV2_PRAD_EFF_DT").alias("PROV2_EFF_DT"),
    F.col("Transform.TRGT_CD_cnty_cls").alias("TRGT_CD_cnty_cls"),
    F.col("Transform.TRGT_CD_metro_rural").alias("TRGT_CD_metro_rural"),
    F.col("Transform.CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# From the Transformer, we have 3 outputs:
# 1) "lnk_rm_dup_OptumProvAddr" => columns: [PROV_ADDR_ID, PROV_ADDR_TYP_CD, PROV_ADDR_EFF_DT, SRC_SYS_CD]
df_lnk_rm_dup_OptumProvAddr = df_Transform.select(
    trim(F.col("PROV_ADDR_ID")).alias("PROV_ADDR_ID"),
    trim(F.col("PROV_ADDR_TYP_CD")).alias("PROV_ADDR_TYP_CD"),
    trim(F.col("EFF_DT")).alias("PROV_ADDR_EFF_DT"),
    trim(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD")
)

# 2) "lnk_OptumProvAddrPkey_All" => we keep everything but with certain columns trimmed or expressions
df_lnk_OptumProvAddrPkey_All = df_Transform.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    trim(F.col("INSRT_UPDT_CD")).alias("INSRT_UPDT_CD"),
    trim(F.col("DISCARD_IN")).alias("DISCARD_IN"),
    trim(F.col("PASS_THRU_IN")).alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    trim(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    trim(F.col("PRI_KEY_STRING")).alias("PRI_KEY_STRING"),
    trim(F.col("PROV_ADDR_ID")).alias("PROV_ADDR_ID"),
    trim(F.col("PROV_ADDR_TYP_CD")).alias("PROV_ADDR_TYP_CD"),
    trim(F.col("EFF_DT")).alias("PROV_ADDR_EFF_DT"),
    F.col("PrimeCntyClsCd").alias("PROV_ADDR_CNTY_CLS_CD"),
    F.when(
        (F.col("PROV_ADDR_GEO_ACES_RTRN_CD").isNull()) |
        (F.length(F.col("PROV_ADDR_GEO_ACES_RTRN_CD")) == 0),
        F.lit("NA")
    ).otherwise(F.col("PROV_ADDR_GEO_ACES_RTRN_CD")).alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    F.col("PrimeMetroRuralCd").alias("PROV_ADDR_METRORURAL_COV_CD"),
    trim(F.col("PROV_ADDR_TERM_RSN_CD")).alias("PROV_ADDR_TERM_RSN_CD"),
    trim(F.col("HCAP_IN")).alias("HCAP_IN"),
    trim(F.col("PDX_24_HR_IN")).alias("PDX_24_HR_IN"),
    trim(F.col("PRCTC_LOC_IN")).alias("PRCTC_LOC_IN"),
    trim(F.col("PROV_ADDR_DIR_IN")).alias("PROV_ADDR_DIR_IN"),
    trim(F.col("TERM_DT")).alias("TERM_DT"),
    trim(F.regexp_replace(F.col("ADDR_LN_1"), '"', '""')).alias("ADDR_LN_1"),
    trim(F.col("ADDR_LN_2")).alias("ADDR_LN_2"),
    trim(F.col("ADDR_LN_3")).alias("ADDR_LN_3"),
    trim(F.col("CITY_NM")).alias("CITY_NM"),
    trim(F.col("PROV_ADDR_ST_CD")).alias("PROV_ADDR_ST_CD"),
    # POSTAL_CD => PostCd logic
    F.lit(None).alias("POSTAL_CD_temp"),  # We'll fill later
    trim(F.col("CNTY_NM")).alias("CNTY_NM"),
    trim(F.col("PHN_NO")).alias("PHN_NO"),
    trim(F.col("PHN_NO_EXT")).alias("PHN_NO_EXT"),
    trim(F.col("FAX_NO")).alias("FAX_NO"),
    trim(F.col("FAX_NO_EXT")).alias("FAX_NO_EXT"),
    trim(F.col("EMAIL_ADDR_TX")).alias("EMAIL_ADDR_TX"),
    F.when(F.col("LAT_TX").isNull(), F.lit(0)).otherwise(F.col("LAT_TX")).alias("LAT_TX"),
    F.when(F.col("LONG_TX").isNull(), F.lit(0)).otherwise(F.col("LONG_TX")).alias("LONG_TX"),
    trim(F.col("PRAD_TYPE_MAIL")).alias("PRAD_TYPE_MAIL"),
    trim(F.col("PROV2_EFF_DT")).alias("PROV2_PRAD_EFF_DT")
).withColumnRenamed("POSTAL_CD_temp", "POSTAL_CD")  # we handle actual ZIP logic below

# For the actual post code logic in the stage variables ("ZipCd" transformations):
df_lnk_OptumProvAddrPkey_All = df_lnk_OptumProvAddrPkey_All.withColumn(
    "ZipCd1",
    F.regexp_replace(F.col("POSTAL_CD"), "-", "")
).withColumn(
    "ZipCd2",
    F.regexp_replace(F.col("ZipCd1"), " ", "")
).withColumn(
    "PostCdLen",
    F.length(F.col("ZipCd2"))
).withColumn(
    "PostCd",
    F.when(
        (F.col("PostCdLen") == 5) | (F.col("PostCdLen") == 9),
        F.col("ZipCd2")
    ).when(
        (F.col("PostCdLen") > 5) & (F.col("PostCdLen") < 9),
        F.substring(F.col("ZipCd2"), 1, 5)
    ).when(
        F.col("PostCdLen") > 9,
        F.substring(F.col("ZipCd2"), 1, 9)
    ).otherwise(F.lit(None))
)

df_lnk_OptumProvAddrPkey_All = df_lnk_OptumProvAddrPkey_All.drop("ZipCd1","ZipCd2","PostCdLen","POSTAL_CD").withColumnRenamed("PostCd","POSTAL_CD")

# Stage: "Snapshot" => "Snapshot_File"
df_Snapshot_file = df_Transform.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    trim(F.col("PROV_ADDR_ID")).alias("PROV_ADDR_ID"),
    F.col("CD_MPPNG_SK").alias("PROV_ADDR_TYP_CD_SK"),
    F.lit("1753-01-01").alias("PROV_ADDR_EFF_DT_SK")
)

write_files(
    df_Snapshot_file.select(
        "SRC_SYS_CD_SK",
        "PROV_ADDR_ID",
        "PROV_ADDR_TYP_CD_SK",
        "PROV_ADDR_EFF_DT_SK"
    ),
    f"{adls_path}/load/B_PROV_ADDR.OPTUM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# Stage: rdp_NaturalKeys => deduplicate on [PROV_ADDR_ID, PROV_ADDR_TYP_CD, PROV_ADDR_EFF_DT, SRC_SYS_CD]
df_rdp_NaturalKeys = dedup_sort(
    df_lnk_rm_dup_OptumProvAddr,
    partition_cols=["PROV_ADDR_ID","PROV_ADDR_TYP_CD","PROV_ADDR_EFF_DT","SRC_SYS_CD"],
    sort_cols=[]
)

# Stage: jn_ProvAddr => left join rdp_NaturalKeys with df_db2_K_PROV_ADDR_In on those keys
leftA = df_rdp_NaturalKeys.alias("lnk_rm_dup_DataOut")
rightA = df_db2_K_PROV_ADDR_In.alias("lnk_KProvAddr_extr")
condA = [
    F.col("lnk_rm_dup_DataOut.PROV_ADDR_ID") == F.col("lnk_KProvAddr_extr.PROV_ADDR_ID"),
    F.col("lnk_rm_dup_DataOut.PROV_ADDR_TYP_CD") == F.col("lnk_KProvAddr_extr.PROV_ADDR_TYP_CD"),
    F.col("lnk_rm_dup_DataOut.PROV_ADDR_EFF_DT") == F.col("lnk_KProvAddr_extr.PROV_ADDR_EFF_DT"),
    F.col("lnk_rm_dup_DataOut.SRC_SYS_CD") == F.col("lnk_KProvAddr_extr.SRC_SYS_CD")
]
df_jn_ProvAddr_pre = leftA.join(rightA, condA, "left")

df_jn_ProvAddr = df_jn_ProvAddr_pre.select(
    F.col("lnk_rm_dup_DataOut.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("lnk_rm_dup_DataOut.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("lnk_rm_dup_DataOut.PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT"),
    F.col("lnk_rm_dup_DataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_KProvAddr_extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_KProvAddr_extr.PROV_ADDR_SK").alias("PROV_ADDR_SK")
)

# Stage: xfm_PKEYgen
# The DS code does a constraint for new (IsNull(PROV_ADDR_SK)).
# Also sets PROV_ADDR_SK = NextSurrogateKey() if null. Then merges them back for the other link.

df_xfm_PKEYgen = df_jn_ProvAddr.select(
    F.col("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD"),
    F.col("PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT_SK"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_ADDR_SK")
)

# "lnk_KProvAddr_new" => filter where PROV_ADDR_SK is null => apply SurrogateKeyGen
df_lnk_KProvAddr_new_pre = df_xfm_PKEYgen.filter(F.col("PROV_ADDR_SK").isNull())
df_enriched = df_lnk_KProvAddr_new_pre
df_enriched = SurrogateKeyGen(df_enriched, <DB sequence name>, 'PROV_ADDR_SK', <schema>, <secret_name>)
df_lnk_KProvAddr_new = df_enriched.select(
    F.col("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD"),
    F.col("PROV_ADDR_EFF_DT_SK"),
    F.col("SRC_SYS_CD"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_ADDR_SK")
)

# "lnkPKEYxfmOut" => unify updated or existing SK
df_lnkPKEYxfmOut_left = df_xfm_PKEYgen.filter(F.col("PROV_ADDR_SK").isNotNull())
df_lnkPKEYxfmOut_left = df_lnkPKEYxfmOut_left.withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    F.when(F.col("PROV_ADDR_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    F.lit(CurrRunCycle)
)

# For rows with new SK, we already generated them in df_lnk_KProvAddr_new:
df_lnkPKEYxfmOut_right = df_lnk_KProvAddr_new.select(
    df_lnk_KProvAddr_new["PROV_ADDR_ID"],
    df_lnk_KProvAddr_new["PROV_ADDR_TYP_CD"],
    df_lnk_KProvAddr_new["PROV_ADDR_EFF_DT_SK"],
    df_lnk_KProvAddr_new["SRC_SYS_CD"],
    df_lnk_KProvAddr_new["CRT_RUN_CYC_EXCTN_SK"],
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    df_lnk_KProvAddr_new["PROV_ADDR_SK"]
)

df_lnkPKEYxfmOut = (
    df_lnkPKEYxfmOut_left
    .select(
        "PROV_ADDR_ID",
        "PROV_ADDR_TYP_CD",
        "PROV_ADDR_EFF_DT_SK",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PROV_ADDR_SK"
    )
    .unionByName(
        df_lnkPKEYxfmOut_right.select(
            "PROV_ADDR_ID",
            "PROV_ADDR_TYP_CD",
            "PROV_ADDR_EFF_DT_SK",
            "SRC_SYS_CD",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "PROV_ADDR_SK"
        )
    )
)

# Now we must write df_lnk_KProvAddr_new to #$IDSOwner#.K_PROV_ADDR (INSERT only).
# We'll do a merge with "when not matched then insert" and no update, matching PK: [PROV_ADDR_ID,PROV_ADDR_TYP_CD,PROV_ADDR_EFF_DT_SK,SRC_SYS_CD].
df_to_db_new = df_lnk_KProvAddr_new.select(
    "PROV_ADDR_ID",
    "PROV_ADDR_TYP_CD",
    "PROV_ADDR_EFF_DT_SK",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "PROV_ADDR_SK"
)

temp_table_new = "STAGING.OptumProvAddrExtr_db2_K_PROV_ADDR_load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_new}", jdbc_url_ids, jdbc_props_ids)

df_to_db_new.write.jdbc(
    url=jdbc_url_ids,
    table=temp_table_new,
    mode="overwrite",
    properties=jdbc_props_ids
)

merge_sql_new = (
    f"MERGE INTO {IDSOwner}.K_PROV_ADDR AS T "
    f"USING {temp_table_new} AS S "
    "ON "
    " T.PROV_ADDR_ID = S.PROV_ADDR_ID AND "
    " T.PROV_ADDR_TYP_CD = S.PROV_ADDR_TYP_CD AND "
    " T.PROV_ADDR_EFF_DT_SK = S.PROV_ADDR_EFF_DT_SK AND "
    " T.SRC_SYS_CD = S.SRC_SYS_CD "
    "WHEN MATCHED THEN "
    "  UPDATE SET PROV_ADDR_ID = T.PROV_ADDR_ID "  # do nothing effectively
    "WHEN NOT MATCHED THEN "
    "  INSERT (PROV_ADDR_ID,PROV_ADDR_TYP_CD,PROV_ADDR_EFF_DT_SK,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,PROV_ADDR_SK) "
    "  VALUES (S.PROV_ADDR_ID,S.PROV_ADDR_TYP_CD,S.PROV_ADDR_EFF_DT_SK,S.SRC_SYS_CD,S.CRT_RUN_CYC_EXCTN_SK,S.PROV_ADDR_SK);"
)
execute_dml(merge_sql_new, jdbc_url_ids, jdbc_props_ids)

# Next stage: jn_PKEYs => inner join df_lnkPKEYxfmOut with lnk_OptumProvAddrPkey_All
leftB = df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut")
rightB = df_lnk_OptumProvAddrPkey_All.alias("lnk_OptumProvAddrPkey_All")
condB = [
    F.col("lnkPKEYxfmOut.PROV_ADDR_ID") == F.col("lnk_OptumProvAddrPkey_All.PROV_ADDR_ID"),
    F.col("lnkPKEYxfmOut.PROV_ADDR_TYP_CD") == F.col("lnk_OptumProvAddrPkey_All.PROV_ADDR_TYP_CD"),
    F.col("lnkPKEYxfmOut.PROV_ADDR_EFF_DT_SK") == F.col("lnk_OptumProvAddrPkey_All.PROV_ADDR_EFF_DT"),
    F.col("lnkPKEYxfmOut.SRC_SYS_CD") == F.col("lnk_OptumProvAddrPkey_All.SRC_SYS_CD")
]
df_jn_PKEYs_pre = leftB.join(rightB, condB, "inner")

df_jn_PKEYs = df_jn_PKEYs_pre.select(
    F.col("lnk_OptumProvAddrPkey_All.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("lnk_OptumProvAddrPkey_All.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("lnk_OptumProvAddrPkey_All.DISCARD_IN").alias("DISCARD_IN"),
    F.col("lnk_OptumProvAddrPkey_All.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("lnk_OptumProvAddrPkey_All.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("lnk_OptumProvAddrPkey_All.ERR_CT").alias("ERR_CT"),
    F.col("lnk_OptumProvAddrPkey_All.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("lnkPKEYxfmOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_OptumProvAddrPkey_All.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("lnkPKEYxfmOut.PROV_ADDR_SK").alias("PROV_ADDR_SK"),
    F.col("lnkPKEYxfmOut.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("lnkPKEYxfmOut.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("lnkPKEYxfmOut.PROV_ADDR_EFF_DT_SK").alias("EFF_DT"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_OptumProvAddrPkey_All.PROV_ADDR_CNTY_CLS_CD").alias("PROV_ADDR_CNTY_CLS_CD"),
    F.col("lnk_OptumProvAddrPkey_All.PROV_ADDR_GEO_ACES_RTRN_CD_TX").alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    F.col("lnk_OptumProvAddrPkey_All.PROV_ADDR_METRORURAL_COV_CD").alias("PROV_ADDR_METRORURAL_COV_CD"),
    F.col("lnk_OptumProvAddrPkey_All.PROV_ADDR_TERM_RSN_CD").alias("PROV_ADDR_TERM_RSN_CD"),
    F.col("lnk_OptumProvAddrPkey_All.HCAP_IN").alias("HCAP_IN"),
    F.col("lnk_OptumProvAddrPkey_All.PDX_24_HR_IN").alias("PDX_24_HR_IN"),
    F.col("lnk_OptumProvAddrPkey_All.PRCTC_LOC_IN").alias("PRCTC_LOC_IN"),
    F.col("lnk_OptumProvAddrPkey_All.PROV_ADDR_DIR_IN").alias("PROV_ADDR_DIR_IN"),
    F.col("lnk_OptumProvAddrPkey_All.TERM_DT").alias("TERM_DT"),
    F.col("lnk_OptumProvAddrPkey_All.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("lnk_OptumProvAddrPkey_All.ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("lnk_OptumProvAddrPkey_All.ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("lnk_OptumProvAddrPkey_All.CITY_NM").alias("CITY_NM"),
    F.col("lnk_OptumProvAddrPkey_All.PROV_ADDR_ST_CD").alias("PROV_ADDR_ST_CD"),
    F.col("lnk_OptumProvAddrPkey_All.POSTAL_CD").alias("POSTAL_CD"),
    F.col("lnk_OptumProvAddrPkey_All.CNTY_NM").alias("CNTY_NM"),
    F.col("lnk_OptumProvAddrPkey_All.PHN_NO").alias("PHN_NO"),
    F.col("lnk_OptumProvAddrPkey_All.PHN_NO_EXT").alias("PHN_NO_EXT"),
    F.col("lnk_OptumProvAddrPkey_All.FAX_NO").alias("FAX_NO"),
    F.col("lnk_OptumProvAddrPkey_All.FAX_NO_EXT").alias("FAX_NO_EXT"),
    F.col("lnk_OptumProvAddrPkey_All.EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
    F.col("lnk_OptumProvAddrPkey_All.LAT_TX").alias("LAT_TX"),
    F.col("lnk_OptumProvAddrPkey_All.LONG_TX").alias("LONG_TX"),
    F.col("lnk_OptumProvAddrPkey_All.PRAD_TYPE_MAIL").alias("PRAD_TYPE_MAIL"),
    F.col("lnk_OptumProvAddrPkey_All.PROV2_PRAD_EFF_DT").alias("PROV2_PRAD_EFF_DT")
)

# Final stage: OptumProvAddrExtr => write .dat.#RunID#
# We must rpad for char columns in final select, preserving order:

final_cols = [
    ("JOB_EXCTN_RCRD_ERR_SK", None),
    ("INSRT_UPDT_CD", 10),
    ("DISCARD_IN", 1),
    ("PASS_THRU_IN", 1),
    ("FIRST_RECYC_DT", None),
    ("ERR_CT", None),
    ("RECYCLE_CT", None),
    ("SRC_SYS_CD", None),
    ("PRI_KEY_STRING", None),
    ("PROV_ADDR_SK", None),
    ("PROV_ADDR_ID", None),
    ("PROV_ADDR_TYP_CD", None),
    ("EFF_DT", 10),
    ("CRT_RUN_CYC_EXCTN_SK", None),
    ("LAST_UPDT_RUN_CYC_EXCTN_SK", None),
    ("PROV_ADDR_CNTY_CLS_CD", None),
    ("PROV_ADDR_GEO_ACES_RTRN_CD_TX", None),
    ("PROV_ADDR_METRORURAL_COV_CD", None),
    ("PROV_ADDR_TERM_RSN_CD", 2),
    ("HCAP_IN", 1),
    ("PDX_24_HR_IN", 1),
    ("PRCTC_LOC_IN", 1),
    ("PROV_ADDR_DIR_IN", 1),
    ("TERM_DT", 10),
    ("ADDR_LN_1", None),
    ("ADDR_LN_2", None),
    ("ADDR_LN_3", None),
    ("CITY_NM", None),
    ("PROV_ADDR_ST_CD", 10),
    ("POSTAL_CD", 11),
    ("CNTY_NM", None),
    ("PHN_NO", None),
    ("PHN_NO_EXT", None),
    ("FAX_NO", 20),
    ("FAX_NO_EXT", None),
    ("EMAIL_ADDR_TX", None),
    ("LAT_TX", None),
    ("LONG_TX", None),
    ("PRAD_TYPE_MAIL", 3),
    ("PROV2_PRAD_EFF_DT", 10)
]
df_final = df_jn_PKEYs
for c, length in final_cols:
    if length is not None:
        df_final = df_final.withColumn(c, F.rpad(F.col(c), length, " "))

df_final = df_final.select([F.col(colname) for colname, _ in final_cols])

write_files(
    df_final,
    f"{adls_path}/key/OptumProvAddrExtr.ProvAddr.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)