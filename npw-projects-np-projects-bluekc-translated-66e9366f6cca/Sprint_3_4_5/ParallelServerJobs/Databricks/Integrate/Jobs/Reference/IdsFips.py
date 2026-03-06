# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  IdsReferenceSeq
# MAGIC 
# MAGIC PROCESSING:   \(9)Extract County and State Fips Code information from Facets for IDS.  This job is run monthly.  If the state, county, and couty FIPs numer (logical key) do not change, do not include in 
# MAGIC \(9)the final output file.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      \(9)Change Description                                        \(9)Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      \(9)-----------------------------------------------------------------------         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Laurel Kindley          2007-02-26       Project 3279  \(9)\(9)Original Programming.                                             \(9)CDS Sunset                    Steph Goddard          02/28/2007
# MAGIC 
# MAGIC Pooja Sunkara         2014-05-01       5345                              Added Trim function to generate unique SK's
# MAGIC                                                                                               loading into IDS Fips table(TFS#1403)                     IntegrateWrhsDevl           Jag Yelavarthi           2014-05-02
# MAGIC 
# MAGIC Anoop Nair              2022-03-08      S2S Remediation            Added FACETS DSN Connection parameters      IntegrateDev5      Manasa Andru        2022-04-28

# MAGIC FIPS extract for IDS
# MAGIC Pull fips data already in IDS
# MAGIC Pull state and couty fips data from Facets.  Put in a hash file for removing duplicates.
# MAGIC Pull state abbreviations for lookup.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, length, upper, rpad
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
FacetsOwner = get_widget_value("FacetsOwner","")
facets_secret_name = get_widget_value("facets_secret_name","")
CurrRunCycle = get_widget_value("CurrRunCycle","")
RunID = get_widget_value("RunID","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT
  CD_MPPNG.TRGT_CD as TRGT_CD,
  CD_MPPNG.CD_MPPNG_SK as CD_MPPNG_SK
FROM {IDSOwner}.cd_mppng CD_MPPNG
WHERE
  CD_MPPNG.SRC_SYS_CD = 'FACETS'
  AND CD_MPPNG.TRGT_CLCTN_CD = 'IDS'
  AND CD_MPPNG.SRC_DOMAIN_NM = 'STATE'
  AND CD_MPPNG.CD_MPPNG_STTUS_CD = 'MAPPED'
"""
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_hf_ids_fips_st_cd = dedup_sort(
    df_CD_MPPNG,
    ["TRGT_CD"],
    [("TRGT_CD", "A")]
)

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"""
SELECT
  MCZT_STATE_ABBREV,
  MCZT_COUNTY_NO,
  MCZT_COUNTY_NAME
FROM {FacetsOwner}.CER_MCZT_ZIP_TRANS
"""
df_Facets = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_hf_ids_fips = dedup_sort(
    df_Facets,
    ["MCZT_STATE_ABBREV", "MCZT_COUNTY_NO", "MCZT_COUNTY_NAME"],
    [("MCZT_STATE_ABBREV", "A"), ("MCZT_COUNTY_NO", "A"), ("MCZT_COUNTY_NAME", "A")]
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT
  FIPS.ST_CD as ST_CD,
  FIPS.CNTY_FIPS_NO as CNTY_FIPS_NO,
  FIPS.CNTY_NM as CNTY_NM,
  FIPS.FIPS_SK as FIPS_SK,
  FIPS.CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,
  FIPS.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,
  FIPS.ST_CD_SK as ST_CD_SK
FROM {IDSOwner}.fips FIPS
"""
df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Trim = (
    df_IDS
    .withColumn("ST_CD", trim(col("ST_CD")))
    .withColumn("CNTY_FIPS_NO", trim(col("CNTY_FIPS_NO")))
    .withColumn("CNTY_NM", trim(col("CNTY_NM")))
    .withColumn("FIPS_SK", col("FIPS_SK"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", col("CRT_RUN_CYC_EXCTN_SK"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", col("LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn("ST_CD_SK", col("ST_CD_SK"))
)

df_hf_ids_fips_main_pull = dedup_sort(
    df_Trim,
    ["ST_CD", "CNTY_FIPS_NO", "CNTY_NM"],
    [("ST_CD", "A"), ("CNTY_FIPS_NO", "A"), ("CNTY_NM", "A")]
)

df_joined = (
    df_hf_ids_fips
    .alias("fips_out")
    .join(
        df_hf_ids_fips_st_cd.alias("st_cd_lkup"),
        col("fips_out.MCZT_STATE_ABBREV") == col("st_cd_lkup.TRGT_CD"),
        "left"
    )
    .join(
        df_hf_ids_fips_main_pull.alias("ids_fips_lkup"),
        [
            trim(col("fips_out.MCZT_STATE_ABBREV")) == col("ids_fips_lkup.ST_CD"),
            trim(col("fips_out.MCZT_COUNTY_NO")) == col("ids_fips_lkup.CNTY_FIPS_NO"),
            trim(col("fips_out.MCZT_COUNTY_NAME")) == col("ids_fips_lkup.CNTY_NM")
        ],
        "left"
    )
)

df_Business_Rules = (
    df_joined
    .withColumn(
        "vNew",
        when(
            (
                col("ids_fips_lkup.ST_CD").isNull()
                | (length(trim(col("ids_fips_lkup.ST_CD"))) == 0)
            )
            & (
                col("ids_fips_lkup.CNTY_FIPS_NO").isNull()
                | (length(trim(col("ids_fips_lkup.CNTY_FIPS_NO"))) == 0)
            )
            & (
                col("ids_fips_lkup.CNTY_NM").isNull()
                | (length(trim(col("ids_fips_lkup.CNTY_NM"))) == 0)
            ),
            "Y"
        ).otherwise("N")
    )
    .withColumn(
        "vUpdState",
        when(
            col("ids_fips_lkup.ST_CD").isNull()
            | (length(trim(col("ids_fips_lkup.ST_CD"))) == 0),
            "Y"
        ).otherwise("N")
    )
    .withColumn(
        "vUpdCntyNo",
        when(
            col("ids_fips_lkup.CNTY_FIPS_NO").isNull()
            | (length(trim(col("ids_fips_lkup.CNTY_FIPS_NO"))) == 0),
            "Y"
        ).otherwise("N")
    )
    .withColumn(
        "vUpdCntyNm",
        when(
            col("ids_fips_lkup.CNTY_NM").isNull()
            | (length(trim(col("ids_fips_lkup.CNTY_NM"))) == 0),
            "Y"
        ).otherwise("N")
    )
    .withColumn(
        "vUpdate",
        when(
            (
                (col("vUpdState") == "Y")
                | (col("vUpdState") == "Y")
                | (col("vUpdCntyNm") == "Y")
            ),
            "Y"
        ).otherwise("N")
    )
    .withColumn(
        "FIPS_SK_temp",
        when(col("vNew") == "N", col("ids_fips_lkup.FIPS_SK")).otherwise(None)
    )
    .withColumn(
        "ST_CD",
        when(
            col("vUpdState") == "Y",
            upper(trim(col("fips_out.MCZT_STATE_ABBREV")))
        ).otherwise(col("ids_fips_lkup.ST_CD"))
    )
    .withColumn(
        "CNTY_FIPS_NO",
        when(
            col("vUpdCntyNo") == "Y",
            trim(col("fips_out.MCZT_COUNTY_NO"))
        ).otherwise(col("ids_fips_lkup.CNTY_FIPS_NO"))
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        when(col("vNew") == "Y", CurrRunCycle).otherwise(col("ids_fips_lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        col("CurrRunCycle")
    )
    .withColumn(
        "ST_CD_SK",
        when(
            col("st_cd_lkup.CD_MPPNG_SK").isNull(),
            1
        ).otherwise(col("st_cd_lkup.CD_MPPNG_SK"))
    )
    .withColumn(
        "CNTY_NM",
        when(
            col("vUpdCntyNm") == "Y",
            upper(trim(col("fips_out.MCZT_COUNTY_NAME")))
        ).otherwise(col("ids_fips_lkup.CNTY_NM"))
    )
)

df_Business_Rules_filtered = df_Business_Rules.filter(
    (col("vNew") == "Y") | (col("vUpdate") == "Y")
)

df_enriched = (
    df_Business_Rules_filtered
    .withColumn("FIPS_SK", col("FIPS_SK_temp"))
    .drop("FIPS_SK_temp")
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"FIPS_SK",<schema>,<secret_name>)

df_final = (
    df_enriched
    .withColumn("ST_CD", rpad(col("ST_CD"), 2, " "))
    .withColumn("CNTY_FIPS_NO", rpad(col("CNTY_FIPS_NO"), 3, " "))
    .withColumn("CNTY_NM", rpad(col("CNTY_NM"), 25, " "))
    .select(
        "FIPS_SK",
        "ST_CD",
        "CNTY_FIPS_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ST_CD_SK",
        "CNTY_NM"
    )
)

write_files(
    df_final,
    f"{adls_path}/load/FIPS.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)