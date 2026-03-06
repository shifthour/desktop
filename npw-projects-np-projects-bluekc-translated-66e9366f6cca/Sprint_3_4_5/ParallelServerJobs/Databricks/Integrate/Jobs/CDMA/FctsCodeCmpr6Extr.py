# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Job Name:  FctsCodeCmpr6Extr
# MAGIC 
# MAGIC Processing:  Read Facets code tables and compare to CDMA code sets.  Capture differences for emailing metadata analyst.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer	Date		Project/Altiris #		Change Description					Development Project		Reviewer		Review Date
# MAGIC =========================================================================================================================================================================================
# MAGIC Saikiran Subbagari	2018-10-11	Production Support		Original Programming				IntegrateDev2		Abhiram Dasarathy	2018-11-06
# MAGIC Saikiran Subbagari	2018-12-19	Production Support		Join on SRC_DMN_NM				IntegrateDev2		Abhiram Dasarathy	2018-12-26
# MAGIC Prabhu ES	2022-03-17	S2S                          		MSSQL ODBC conn params added       			IntegrateDev5		Ken Bradmon	2022-05-02

# MAGIC Code Domains
# MAGIC 
# MAGIC SUBSCRIBER FAMILY INDICATOR
# MAGIC PRODUCT COMPONENT TYPE
# MAGIC CLASS PLAN DETAIL COVERING PROVIDER SET PREFIX
# MAGIC CAPITATION FUND ACCOUNTING CATEGORY
# MAGIC Compare Facets codes to CDMA and log extra codes in Facets.
# MAGIC 
# MAGIC This process runs in development weekdays and emails the output to Metadata Support
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, length, upper, lit, rpad, asc
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
FacetsOwner = get_widget_value('$FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
TempTable = get_widget_value('TempTable','')

schema_CD_MPPNG_Ext = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("SRC_DMN_NM", StringType(), True),
    StructField("SRC_LKUP_VAL", StringType(), True),
    StructField("Dummy", StringType(), True)
])

df_CD_MPPNG_Ext = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", "|")
    .schema(schema_CD_MPPNG_Ext)
    .load(f"{adls_path}/load/CDMA_CD_MPPNG.txt")
)

df_lnk_Sub_Fam_in = df_CD_MPPNG_Ext.select(
    col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    col("Dummy").alias("Dummy")
)

df_lnk_Prod_Cmpnt_in = df_CD_MPPNG_Ext.select(
    col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    col("Dummy").alias("Dummy")
)

df_lnk_Cov_Prov_Pfxr_in = df_CD_MPPNG_Ext.select(
    col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    col("Dummy").alias("Dummy")
)

df_lnk_Cap_Fund_Acct_Cat_in = df_CD_MPPNG_Ext.select(
    col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    col("Dummy").alias("Dummy")
)

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"SELECT DISTINCT MEPE_FI FROM {FacetsOwner}.CMC_MEPE_PRCS_ELIG"
df_CMC_MEPE_PRCS_ELIG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfm_trm_MEPE_FI = (
    df_CMC_MEPE_PRCS_ELIG
    .filter(length(trim(col("MEPE_FI"))) > 0)
    .select(
        upper(trim(col("MEPE_FI"))).alias("SRC_LKUP_VAL"),
        lit("SUBSCRIBER FAMILY INDICATOR").alias("SRC_DMN_NM")
    )
)

df_join_Sub_Fam = (
    df_xfm_trm_MEPE_FI.alias("lnk_xfm_trm_MEPE_FI_out")
    .join(
        df_lnk_Sub_Fam_in.alias("lnk_Sub_Fam_in"),
        (
            (col("lnk_xfm_trm_MEPE_FI_out.SRC_LKUP_VAL") == col("lnk_Sub_Fam_in.SRC_LKUP_VAL"))
            & (col("lnk_xfm_trm_MEPE_FI_out.SRC_DMN_NM") == col("lnk_Sub_Fam_in.SRC_DMN_NM"))
        ),
        "left"
    )
    .select(
        col("lnk_xfm_trm_MEPE_FI_out.SRC_LKUP_VAL").alias("MEPE_FI"),
        col("lnk_Sub_Fam_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_Sub_Fam_in.Dummy").alias("Dummy")
    )
)

df_xfm_Sub_Fam = (
    df_join_Sub_Fam
    .filter(trim(col("Dummy")) != "1")
    .select(
        (lit("|") + col("MEPE_FI") + lit("|")).alias("Code"),
        upper(col("MEPE_FI")).alias("Description"),
        lit("FACETS").alias("Source_System"),
        lit("SUBSCRIBER FAMILY INDICATOR").alias("Domain")
    )
)

extract_query = f"SELECT DISTINCT PDBC_TYPE, PDPT_DESC FROM {FacetsOwner}.CMC_PDPT_DESC pd"
df_CMC_PDPT_DESC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfm_trim_PDBC_TYPE = (
    df_CMC_PDPT_DESC
    .filter(length(trim(col("PDBC_TYPE"))) > 0)
    .select(
        upper(trim(col("PDBC_TYPE"))).alias("SRC_LKUP_VAL"),
        col("PDPT_DESC").alias("PDBC_DESC"),
        lit("PRODUCT COMPONENT TYPE").alias("SRC_DMN_NM")
    )
)

df_join_Prod_Cmpnt = (
    df_xfm_trim_PDBC_TYPE.alias("lnk_xfm_trm_PDBC_TYPE_out")
    .join(
        df_lnk_Prod_Cmpnt_in.alias("lnk_Prod_Cmpnt_in"),
        (
            (col("lnk_xfm_trm_PDBC_TYPE_out.SRC_LKUP_VAL") == col("lnk_Prod_Cmpnt_in.SRC_LKUP_VAL"))
            & (col("lnk_xfm_trm_PDBC_TYPE_out.SRC_DMN_NM") == col("lnk_Prod_Cmpnt_in.SRC_DMN_NM"))
        ),
        "left"
    )
    .select(
        col("lnk_xfm_trm_PDBC_TYPE_out.SRC_LKUP_VAL").alias("PDBC_TYPE"),
        col("lnk_Prod_Cmpnt_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_xfm_trm_PDBC_TYPE_out.PDBC_DESC").alias("PDBC_DESC"),
        col("lnk_Prod_Cmpnt_in.Dummy").alias("Dummy")
    )
)

df_xfm_Prod_Cmpnt = (
    df_join_Prod_Cmpnt
    .filter(trim(col("Dummy")) != "1")
    .select(
        (lit("|") + col("PDBC_TYPE") + lit("|")).alias("Code"),
        upper(col("PDBC_DESC")).alias("Description"),
        lit("FACETS").alias("Source_System"),
        lit("PRODUCT COMPONENT TYPE").alias("Domain")
    )
)

extract_query = f"SELECT DISTINCT PDBC_PFX, PDPX_DESC FROM {FacetsOwner}.CMC_PDPX_DESC pd WHERE pd.PDBC_TYPE = 'CVST'"
df_CMC_CSPI_CS_PLAN3 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfm_trim_PDBC_PFX = (
    df_CMC_CSPI_CS_PLAN3
    .filter(length(trim(col("PDBC_PFX"))) > 0)
    .select(
        upper(trim(col("PDBC_PFX"))).alias("SRC_LKUP_VAL"),
        col("PDPX_DESC").alias("PDBC_DESC"),
        lit("CLASS PLAN DETAIL COVERING PROVIDER SET PREFIX").alias("SRC_DMN_NM")
    )
)

df_join_Cov_Prov_Pfx = (
    df_xfm_trim_PDBC_PFX.alias("lnk_xfm_trm_PDBC_PFX_out")
    .join(
        df_lnk_Cov_Prov_Pfxr_in.alias("lnk_Cov_Prov_Pfxr_in"),
        (
            (col("lnk_xfm_trm_PDBC_PFX_out.SRC_LKUP_VAL") == col("lnk_Cov_Prov_Pfxr_in.SRC_LKUP_VAL"))
            & (col("lnk_xfm_trm_PDBC_PFX_out.SRC_DMN_NM") == col("lnk_Cov_Prov_Pfxr_in.SRC_DMN_NM"))
        ),
        "left"
    )
    .select(
        col("lnk_xfm_trm_PDBC_PFX_out.SRC_LKUP_VAL").alias("PDBC_PFX"),
        col("lnk_Cov_Prov_Pfxr_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_xfm_trm_PDBC_PFX_out.PDBC_DESC").alias("PDBC_DESC"),
        col("lnk_Cov_Prov_Pfxr_in.Dummy").alias("Dummy")
    )
)

df_xfm_Cov_Prov_Pfx = (
    df_join_Cov_Prov_Pfx
    .filter(trim(col("Dummy")) != "1")
    .select(
        (lit("|") + col("PDBC_PFX") + lit("|")).alias("Code"),
        upper(col("PDBC_DESC")).alias("Description"),
        lit("FACETS").alias("Source_System"),
        lit("CLASS PLAN DETAIL COVERING PROVIDER SET PREFIX").alias("Domain")
    )
)

extract_query = f"SELECT DISTINCT CRFD_ACCT_CAT FROM {FacetsOwner}.CMC_CRFD_FUND_DEFN"
df_CMC_CRFD_FUND_DEFN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfm_CRFD_ACCT_CAT = (
    df_CMC_CRFD_FUND_DEFN
    .filter(length(trim(col("CRFD_ACCT_CAT"))) > 0)
    .select(
        upper(trim(col("CRFD_ACCT_CAT"))).alias("SRC_LKUP_VAL"),
        lit("CAPITATION FUND ACCOUNTING CATEGORY").alias("SRC_DMN_NM")
    )
)

df_join_Cap_Fund_Acct_Cat = (
    df_xfm_CRFD_ACCT_CAT.alias("lnk_xfm_trm_CRFD_ACCT_CAT_out")
    .join(
        df_lnk_Cap_Fund_Acct_Cat_in.alias("lnk_Cap_Fund_Acct_Cat_in"),
        (
            (col("lnk_xfm_trm_CRFD_ACCT_CAT_out.SRC_LKUP_VAL") == col("lnk_Cap_Fund_Acct_Cat_in.SRC_LKUP_VAL"))
            & (col("lnk_xfm_trm_CRFD_ACCT_CAT_out.SRC_DMN_NM") == col("lnk_Cap_Fund_Acct_Cat_in.SRC_DMN_NM"))
        ),
        "left"
    )
    .select(
        col("lnk_xfm_trm_CRFD_ACCT_CAT_out.SRC_LKUP_VAL").alias("CRFD_ACCT_CAT"),
        col("lnk_Cap_Fund_Acct_Cat_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_Cap_Fund_Acct_Cat_in.Dummy").alias("Dummy")
    )
)

df_xfm_Cap_Fund_Acct_Cat = (
    df_join_Cap_Fund_Acct_Cat
    .filter(trim(col("Dummy")) != "1")
    .select(
        (lit("|") + col("CRFD_ACCT_CAT") + lit("|")).alias("Code"),
        upper(col("CRFD_ACCT_CAT")).alias("Description"),
        lit("FACETS").alias("Source_System"),
        lit("CAPITATION FUND ACCOUNTING CATEGORY").alias("Domain")
    )
)

df_fl_New_Codes = (
    df_xfm_Sub_Fam.select("Code", "Description", "Source_System", "Domain")
    .unionByName(df_xfm_Prod_Cmpnt.select("Code", "Description", "Source_System", "Domain"))
    .unionByName(df_xfm_Cov_Prov_Pfx.select("Code", "Description", "Source_System", "Domain"))
    .unionByName(df_xfm_Cap_Fund_Acct_Cat.select("Code", "Description", "Source_System", "Domain"))
    .sort(["Domain", "Code"], ascending=[True, True])
)

df_final = df_fl_New_Codes.select(
    col("Code"),
    rpad(col("Description"), 80, " ").alias("Description"),
    col("Source_System"),
    rpad(col("Domain"), 80, " ").alias("Domain")
)

write_files(
    df_final,
    f"{adls_path}/load/processed/CDMA_Code_Diff.{RunID}.txt",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)