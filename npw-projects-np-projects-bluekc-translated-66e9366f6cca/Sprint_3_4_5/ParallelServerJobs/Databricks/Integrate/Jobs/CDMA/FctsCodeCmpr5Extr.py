# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Read Facets code tables and compare to CDMA code sets.  Capture differences for emailing metadata analyst.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer	Date		Project/Altiris #		Change Description					Development Project		Reviewer			Review Date
# MAGIC =========================================================================================================================================================================================
# MAGIC Saikiran SubbagarI	2018-10-11	Production Support		Original Programming				IntegrateDev2		Abhiram Dasarathy		2018-11-06
# MAGIC Saikiran Subbagari	2018-12-19	Production Support		Join on SRC_DMN_NM				IntegrateDev2		Abhiram Dasarathy		2018-12-26
# MAGIC Prabhu ES	2022-02-24	S2S Remediation		MSSQL connection parameters added			IntegrateDev5		Ken Bradmon		2022-05-02

# MAGIC Code Domains
# MAGIC 
# MAGIC PLACE OF SERVICE
# MAGIC SIC NAICS RISK CLASSIFICATION
# MAGIC FACILITY CLAIM OCCURRENCE
# MAGIC FACILITY CLAIM PROCESSING VALUE
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, length, rpad, lit
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
TempTable = get_widget_value('TempTable','')

schema_cd_mppng_ext = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("SRC_DMN_NM", StringType(), True),
    StructField("SRC_LKUP_VAL", StringType(), True),
    StructField("Dummy", IntegerType(), True)
])

df_CD_MPPNG_Ext = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", "|")
    .schema(schema_cd_mppng_ext)
    .load(f"{adls_path}/load/CDMA_CD_MPPNG.txt")
)

df_cp_CD_MPPNG = df_CD_MPPNG_Ext

df_lnk_Place_of_Service_in = df_cp_CD_MPPNG.select(
    col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    col("Dummy").alias("Dummy")
)

df_lnk_NAICS_RISK_in = df_cp_CD_MPPNG.select(
    col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    col("Dummy").alias("Dummy")
)

df_lnk_Fclty_Occr_in = df_cp_CD_MPPNG.select(
    col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    col("Dummy").alias("Dummy")
)

df_lnk_Fclty_Val_in = df_cp_CD_MPPNG.select(
    col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    col("Dummy").alias("Dummy")
)

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"SELECT DISTINCT PSCD_ID, PSCD_DESC FROM {FacetsOwner}.CMC_PSCD_POS_DESC"
df_CMC_PSCD_POS_DESC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfm_trm_PSCD_ID = (
    df_CMC_PSCD_POS_DESC
    .filter(length(trim(col("PSCD_ID"))) > 0)
    .select(
        UpCase(trim(col("PSCD_ID"))).alias("SRC_LKUP_VAL"),
        col("PSCD_DESC").alias("PSCD_DESC"),
        lit("PLACE OF SERVICE").alias("SRC_DMN_NM")
    )
)

df_join_Place_of_Service = (
    df_xfm_trm_PSCD_ID.alias("lnk_xfm_trm_PSCD_ID_out")
    .join(
        df_lnk_Place_of_Service_in.alias("lnk_Place_of_Service_in"),
        (
            (col("lnk_xfm_trm_PSCD_ID_out.SRC_LKUP_VAL") == col("lnk_Place_of_Service_in.SRC_LKUP_VAL"))
            & (col("lnk_xfm_trm_PSCD_ID_out.SRC_DMN_NM") == col("lnk_Place_of_Service_in.SRC_DMN_NM"))
        ),
        "left"
    )
    .select(
        col("lnk_xfm_trm_PSCD_ID_out.SRC_LKUP_VAL").alias("PSCD_ID"),
        col("lnk_Place_of_Service_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_xfm_trm_PSCD_ID_out.PSCD_DESC").alias("PSCD_DESC"),
        col("lnk_Place_of_Service_in.Dummy").alias("Dummy")
    )
)

df_xfm_Place_of_Service = (
    df_join_Place_of_Service
    .filter(trim(col("Dummy").cast("string")) != "1")
    .select(
        (lit("|") + col("PSCD_ID") + lit("|")).alias("Code"),
        UpCase(col("PSCD_DESC")).alias("Description"),
        lit("FACETS").alias("Source_System"),
        lit("PLACE OF SERVICE").alias("Domain")
    )
)

extract_query = f"SELECT DISTINCT PAGR_SIC FROM {FacetsOwner}.CMC_PAGR_PARENT_GR"
df_CMC_PAGR_PARENT_GR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfm_trim_PAGR_SIC = (
    df_CMC_PAGR_PARENT_GR
    .filter(length(trim(col("PAGR_SIC"))) > 0)
    .select(
        UpCase(trim(col("PAGR_SIC"))).alias("SRC_LKUP_VAL"),
        lit("SIC NAICS RISK CLASSIFICATION").alias("SRC_DMN_NM")
    )
)

df_join_NAICS_RISK = (
    df_xfm_trim_PAGR_SIC.alias("lnk_xfm_trm_PAGR_SIC_out")
    .join(
        df_lnk_NAICS_RISK_in.alias("lnk_NAICS_RISK_in"),
        (
            (col("lnk_xfm_trm_PAGR_SIC_out.SRC_LKUP_VAL") == col("lnk_NAICS_RISK_in.SRC_LKUP_VAL"))
            & (col("lnk_xfm_trm_PAGR_SIC_out.SRC_DMN_NM") == col("lnk_NAICS_RISK_in.SRC_DMN_NM"))
        ),
        "left"
    )
    .select(
        col("lnk_xfm_trm_PAGR_SIC_out.SRC_LKUP_VAL").alias("PAGR_SIC"),
        col("lnk_NAICS_RISK_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_NAICS_RISK_in.Dummy").alias("Dummy")
    )
)

df_xfm_NAICS_RISK = (
    df_join_NAICS_RISK
    .filter(trim(col("Dummy").cast("string")) != "1")
    .select(
        (lit("|") + col("PAGR_SIC") + lit("|")).alias("Code"),
        UpCase(col("PAGR_SIC")).alias("Description"),
        lit("FACETS").alias("Source_System"),
        lit("SIC NAICS RISK CLASSIFICATION").alias("Domain")
    )
)

extract_query = f"SELECT DISTINCT CLHO_OCC_CODE FROM {FacetsOwner}.CMC_CLHO_OCC_CODE"
df_CMC_CLHO_OCC_CODE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfm_trim_CLHO_OCC_CODE = (
    df_CMC_CLHO_OCC_CODE
    .filter(length(trim(col("CLHO_OCC_CODE"))) > 0)
    .select(
        UpCase(trim(col("CLHO_OCC_CODE"))).alias("SRC_LKUP_VAL"),
        lit("FACILITY CLAIM OCCURRENCE").alias("SRC_DMN_NM")
    )
)

df_join_Fclty_Occr = (
    df_xfm_trim_CLHO_OCC_CODE.alias("lnk_xfm_trm_CLHO_OCC_CODE_out")
    .join(
        df_lnk_Fclty_Occr_in.alias("lnk_Fclty_Occr_in"),
        (
            (col("lnk_xfm_trm_CLHO_OCC_CODE_out.SRC_LKUP_VAL") == col("lnk_Fclty_Occr_in.SRC_LKUP_VAL"))
            & (col("lnk_xfm_trm_CLHO_OCC_CODE_out.SRC_DMN_NM") == col("lnk_Fclty_Occr_in.SRC_DMN_NM"))
        ),
        "left"
    )
    .select(
        col("lnk_xfm_trm_CLHO_OCC_CODE_out.SRC_LKUP_VAL").alias("CLHO_OCC_CODE"),
        col("lnk_Fclty_Occr_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_Fclty_Occr_in.Dummy").alias("Dummy")
    )
)

df_xfm_Fclty_Occr = (
    df_join_Fclty_Occr
    .filter(trim(col("Dummy").cast("string")) != "1")
    .select(
        (lit("|") + col("CLHO_OCC_CODE") + lit("|")).alias("Code"),
        UpCase(col("CLHO_OCC_CODE")).alias("Description"),
        lit("FACETS").alias("Source_System"),
        lit("FACILITY CLAIM OCCURRENCE").alias("Domain")
    )
)

extract_query = f"SELECT DISTINCT CLVC_CODE FROM {FacetsOwner}.CMC_CLVC_VAL_CODE"
df_CMC_CLVC_VAL_CODE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfm_CLVC_CODE = (
    df_CMC_CLVC_VAL_CODE
    .filter(length(trim(col("CLVC_CODE"))) > 0)
    .select(
        UpCase(trim(col("CLVC_CODE"))).alias("SRC_LKUP_VAL"),
        lit("FACILITY CLAIM PROCESSING VALUE").alias("SRC_DMN_NM")
    )
)

df_join_Fclty_Val = (
    df_xfm_CLVC_CODE.alias("lnk_xfm_trm_CLVC_CODE_out")
    .join(
        df_lnk_Fclty_Val_in.alias("lnk_Fclty_Val_in"),
        (
            (col("lnk_xfm_trm_CLVC_CODE_out.SRC_LKUP_VAL") == col("lnk_Fclty_Val_in.SRC_LKUP_VAL"))
            & (col("lnk_xfm_trm_CLVC_CODE_out.SRC_DMN_NM") == col("lnk_Fclty_Val_in.SRC_DMN_NM"))
        ),
        "left"
    )
    .select(
        col("lnk_xfm_trm_CLVC_CODE_out.SRC_LKUP_VAL").alias("CLVC_CODE"),
        col("lnk_Fclty_Val_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_Fclty_Val_in.Dummy").alias("Dummy")
    )
)

df_xfm_Fclty_Val = (
    df_join_Fclty_Val
    .filter(trim(col("Dummy").cast("string")) != "1")
    .select(
        (lit("|") + col("CLVC_CODE") + lit("|")).alias("Code"),
        UpCase(col("CLVC_CODE")).alias("Description"),
        lit("FACETS").alias("Source_System"),
        lit("FACILITY CLAIM PROCESSING VALUE").alias("Domain")
    )
)

df_fl_New_Codes = (
    df_xfm_Place_of_Service.select("Code", "Description", "Source_System", "Domain")
    .unionByName(df_xfm_NAICS_RISK.select("Code", "Description", "Source_System", "Domain"))
    .unionByName(df_xfm_Fclty_Occr.select("Code", "Description", "Source_System", "Domain"))
    .unionByName(df_xfm_Fclty_Val.select("Code", "Description", "Source_System", "Domain"))
    .orderBy("Domain", "Code")
)

df_fl_New_Codes_final = df_fl_New_Codes.select(
    col("Code"),
    rpad(col("Description"), 80, " ").alias("Description"),
    col("Source_System"),
    rpad(col("Domain"), 80, " ").alias("Domain")
)

write_files(
    df_fl_New_Codes_final,
    f"{adls_path}/load/processed/CDMA_Code_Diff.{RunID}.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)