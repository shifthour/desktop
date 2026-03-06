# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Job Name:  FctsCodeCmpr8Extr
# MAGIC 
# MAGIC Processing: Read Facets code tables and compare to CDMA code sets.  Capture differences for emailing metadata analyst.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer	Date		Project/Altiris #		Change Description					Development Project		Reviewer		Review Date
# MAGIC =========================================================================================================================================================================================
# MAGIC Saikiran Subbagari	2018-10-11	Production Support		Original Programming				IntegrateDev2		Abhiram Dasarathy	2018-11-06
# MAGIC Saikiran Subbagari	2018-12-19	Production Support		Join on SRC_DMN_NM				IntegrateDev2		Abhiram Dasarathy	2018-12-26
# MAGIC Prabhu ES	2022-02-24	S2S Remediation     		MSSQL connection parameters added  			IntegrateDev5		Ken Bradmon	2022-05-02

# MAGIC Code Domains
# MAGIC 
# MAGIC LIMIT COMPONENT ACCUMULATOR CODE
# MAGIC LIMIT COMPONENT PROCESSING
# MAGIC CLAIM LETTER TYPE
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
from pyspark.sql import DataFrame, functions as F, types as T
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


# PARAMETERS
RunID = get_widget_value('RunID', '')
FacetsOwner = get_widget_value('FacetsOwner', '')
facets_secret_name = get_widget_value('facets_secret_name', '')
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
TempTable = get_widget_value('TempTable', '')

# STAGE: CD_MPPNG_Ext (PxSequentialFile)
schema_CD_MPPNG_Ext = StructType([
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("SRC_DMN_NM", StringType(), nullable=False),
    StructField("SRC_LKUP_VAL", StringType(), nullable=False),
    StructField("Dummy", IntegerType(), nullable=False)
])
df_CD_MPPNG_Ext = (
    spark.read.format("csv")
    .schema(schema_CD_MPPNG_Ext)
    .option("header", False)
    .option("delimiter", "|")
    .load(f"{adls_path}/load/CDMA_CD_MPPNG.txt")
)

# STAGE: cp_CD_MPPNG (PxCopy)
df_cp_CD_MPPNG_in = df_CD_MPPNG_Ext

df_lnk_Grp_Bill_Lvl_in = df_cp_CD_MPPNG_in.select(
    F.col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    F.col("Dummy").alias("Dummy")
)
df_lnk_Ltr_Type_in = df_cp_CD_MPPNG_in.select(
    F.col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    F.col("Dummy").alias("Dummy")
)
df_lnk_Ltlt_Rule_in = df_cp_CD_MPPNG_in.select(
    F.col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    F.col("Dummy").alias("Dummy")
)

# STAGE: CMC_LTLT_LIMIT (ODBCConnectorPX)
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"SELECT DISTINCT LTLT_CAT as SRC_LKUP_VAL FROM {FacetsOwner}.CMC_LTLT_LIMIT"
df_CMC_LTLT_LIMIT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# STAGE: xfm_trm_GRGR_BILL_LEVEL (CTransformerStage)
df_xfm_trm_GRGR_BILL_LEVEL_out = (
    df_CMC_LTLT_LIMIT
    .withColumn("SRC_LKUP_VAL", upCase(trim(F.col("SRC_LKUP_VAL"))))
    .withColumn("SRC_DMN_NM", F.lit("LIMIT COMPONENT ACCUMULATOR CODE"))
)

# STAGE: join_Grp_Bill_Lvl (PxJoin) - leftouter join on (SRC_LKUP_VAL, SRC_DMN_NM)
df_join_Grp_Bill_Lvl = (
    df_xfm_trm_GRGR_BILL_LEVEL_out.alias("lnk_xfm_trm_GRGR_BILL_LEVEL_out")
    .join(
        df_lnk_Grp_Bill_Lvl_in.alias("lnk_Grp_Bill_Lvl_in"),
        on=[
            F.col("lnk_xfm_trm_GRGR_BILL_LEVEL_out.SRC_LKUP_VAL") == F.col("lnk_Grp_Bill_Lvl_in.SRC_LKUP_VAL"),
            F.col("lnk_xfm_trm_GRGR_BILL_LEVEL_out.SRC_DMN_NM") == F.col("lnk_Grp_Bill_Lvl_in.SRC_DMN_NM")
        ],
        how="left"
    )
    .select(
        F.col("lnk_xfm_trm_GRGR_BILL_LEVEL_out.SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
        F.col("lnk_Grp_Bill_Lvl_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_Grp_Bill_Lvl_in.Dummy").alias("Dummy")
    )
)

# STAGE: xfm_Grp_Bill_Lvl (CTransformerStage)
df_xfm_Grp_Bill_Lvl_out = (
    df_join_Grp_Bill_Lvl
    .filter(trim(F.col("Dummy")) != "1")
    .withColumn("Code", F.lit("|") + F.col("SRC_LKUP_VAL") + F.lit("|"))
    .withColumn("Description", upCase(F.col("SRC_LKUP_VAL")))
    .withColumn("Source_System", F.lit("FACETS"))
    .withColumn("Domain", F.lit("LIMIT COMPONENT ACCUMULATOR CODE"))
    .select("Code", "Description", "Source_System", "Domain")
)

# STAGE: CER_ATLT_LETTER_D (ODBCConnectorPX)
jdbc_url_cer_atlt_letter_d, jdbc_props_cer_atlt_letter_d = get_db_config(facets_secret_name)
extract_query_cer_atlt_letter_d = f"SELECT DISTINCT ATLD_ID FROM {FacetsOwner}.CER_ATLT_LETTER_D"
df_CER_ATLT_LETTER_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_cer_atlt_letter_d)
    .options(**jdbc_props_cer_atlt_letter_d)
    .option("query", extract_query_cer_atlt_letter_d)
    .load()
)

# STAGE: xfm_trim_ATLD_ID (CTransformerStage)
df_xfm_trim_ATLD_ID_out = (
    df_CER_ATLT_LETTER_D
    .filter(F.length(trim(F.col("ATLD_ID"))) > 0)
    .withColumn("SRC_LKUP_VAL", upCase(trim(F.col("ATLD_ID"))))
    .withColumn("SRC_DMN_NM", F.lit("CLAIM LETTER TYPE"))
)

# STAGE: join_Ltr_Type (PxJoin) - leftouter join on (SRC_LKUP_VAL, SRC_DMN_NM)
df_join_Ltr_Type = (
    df_xfm_trim_ATLD_ID_out.alias("lnk_xfm_trm_ATLD_ID_out")
    .join(
        df_lnk_Ltr_Type_in.alias("lnk_Ltr_Type_in"),
        on=[
            F.col("lnk_xfm_trm_ATLD_ID_out.SRC_LKUP_VAL") == F.col("lnk_Ltr_Type_in.SRC_LKUP_VAL"),
            F.col("lnk_xfm_trm_ATLD_ID_out.SRC_DMN_NM") == F.col("lnk_Ltr_Type_in.SRC_DMN_NM")
        ],
        how="left"
    )
    .select(
        F.col("lnk_xfm_trm_ATLD_ID_out.SRC_LKUP_VAL").alias("ATLD_ID"),
        F.col("lnk_Ltr_Type_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_Ltr_Type_in.Dummy").alias("Dummy")
    )
)

# STAGE: xfm_Ltr_Type (CTransformerStage)
df_xfm_Ltr_Type_out = (
    df_join_Ltr_Type
    .filter(trim(F.col("Dummy")) != "1")
    .withColumn("Code", F.lit("|") + F.col("ATLD_ID") + F.lit("|"))
    .withColumn("Description", upCase(F.col("ATLD_ID")) + F.lit(" --  Letter Description Not Found"))
    .withColumn("Source_System", F.lit("FACETS"))
    .withColumn("Domain", F.lit("CLAIM LETTER TYPE"))
    .select("Code", "Description", "Source_System", "Domain")
)

# STAGE: CMC_LTLT_RULE (ODBCConnectorPX)
jdbc_url_cmcltlt_rule, jdbc_props_cmcltlt_rule = get_db_config(facets_secret_name)
extract_query_cmcltlt_rule = f"SELECT DISTINCT LTLT_RULE as SRC_LKUP_VAL FROM {FacetsOwner}.CMC_LTLT_LIMIT"
df_CMC_LTLT_RULE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_cmcltlt_rule)
    .options(**jdbc_props_cmcltlt_rule)
    .option("query", extract_query_cmcltlt_rule)
    .load()
)

# STAGE: xfm_trm_LTLT_RULE (CTransformerStage)
df_xfm_trm_LTLT_RULE_out = (
    df_CMC_LTLT_RULE
    .withColumn("SRC_LKUP_VAL", upCase(trim(F.col("SRC_LKUP_VAL"))))
    .withColumn("SRC_DMN_NM", F.lit("LIMIT COMPONENT PROCESSING"))
)

# STAGE: join_Ltlt_Rule (PxJoin) - leftouter join on (SRC_LKUP_VAL, SRC_DMN_NM)
df_join_Ltlt_Rule = (
    df_xfm_trm_LTLT_RULE_out.alias("lnk_xfm_trm_LTLT_RULE_out")
    .join(
        df_lnk_Ltlt_Rule_in.alias("lnk_Ltlt_Rule_in"),
        on=[
            F.col("lnk_xfm_trm_LTLT_RULE_out.SRC_LKUP_VAL") == F.col("lnk_Ltlt_Rule_in.SRC_LKUP_VAL"),
            F.col("lnk_xfm_trm_LTLT_RULE_out.SRC_DMN_NM") == F.col("lnk_Ltlt_Rule_in.SRC_DMN_NM")
        ],
        how="left"
    )
    .select(
        F.col("lnk_xfm_trm_LTLT_RULE_out.SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
        F.col("lnk_Ltlt_Rule_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_Ltlt_Rule_in.Dummy").alias("Dummy")
    )
)

# STAGE: xfm_Ltlt_Rule (CTransformerStage)
df_xfm_Ltlt_Rule_out = (
    df_join_Ltlt_Rule
    .filter(trim(F.col("Dummy")) != "1")
    .withColumn("Code", F.lit("|") + F.col("SRC_LKUP_VAL") + F.lit("|"))
    .withColumn("Description", upCase(F.col("SRC_LKUP_VAL")))
    .withColumn("Source_System", F.lit("FACETS"))
    .withColumn("Domain", F.lit("LIMIT COMPONENT PROCESSING"))
    .select("Code", "Description", "Source_System", "Domain")
)

# STAGE: fl_New_Codes (PxFunnel) - sortfunnel with key Domain ASC, Code ASC
df_fl_New_Codes = (
    df_xfm_Grp_Bill_Lvl_out.unionByName(df_xfm_Ltr_Type_out)
    .unionByName(df_xfm_Ltlt_Rule_out)
    .orderBy(["Domain", "Code"], ascending=[True, True])
)

# STAGE: CDMA_Code_Diff (PxSequentialFile)
# Apply RPAD for columns with char type
df_final = (
    df_fl_New_Codes
    .withColumn("Description", rpad(F.col("Description"), 80, " "))
    .withColumn("Domain", rpad(F.col("Domain"), 80, " "))
    .select("Code", "Description", "Source_System", "Domain")
)

write_files(
    df_final,
    f"{adls_path}/load/processed/CDMA_Code_Diff.{RunID}.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)