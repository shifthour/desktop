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
# MAGIC Developer	Date		Project/Altiris #		Change Description					Development Project		Reviewer		Review Date
# MAGIC =========================================================================================================================================================================================
# MAGIC Saikiran SubbagarI	2018-10-11	Production Support		Original Programming				IntegrateDev2		Abhiram Dasarathy	2018-11-06
# MAGIC Saikiran Subbagari	2018-12-19	Production Support  		Join on SRC_DMN_NM				IntegrateDev2		Abhiram Dasarathy	2018-12-26
# MAGIC Prabhu ES	2022-02-24	S2S Remediation     		MSSQL connection parameters added   			IntegrateDev5		Ken Bradmon	2022-05-02

# MAGIC Code Domains
# MAGIC 
# MAGIC CLASS PLAN AGE CALCULATION METHOD
# MAGIC CLASS PLAN CONT ENR BREAK
# MAGIC CLASS PLAN FAMILY CONTRACT
# MAGIC CLASS PLAN RATE GUARANTEE
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
from pyspark.sql.functions import col, lit, length, trim, upper, concat, rpad
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
TempTable = get_widget_value('TempTable','')

# Schema and read for CD_MPPNG_Ext (PxSequentialFile)
schema_CD_MPPNG_Ext = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("SRC_DMN_NM", StringType(), True),
    StructField("SRC_LKUP_VAL", StringType(), True),
    StructField("Dummy", IntegerType(), True)
])
df_CD_MPPNG_Ext = (
    spark.read.format("csv")
    .option("sep", "|")
    .option("header", False)
    .option("inferSchema", False)
    .schema(schema_CD_MPPNG_Ext)
    .load(f"{adls_path}/load/CDMA_CD_MPPNG.txt")
)

# cp_CD_MPPNG (PxCopy) outputs
df_lnk_Cls_Pln_Prod_Cd_in = df_CD_MPPNG_Ext.select(
    col("SRC_LKUP_VAL"),
    col("SRC_SYS_CD"),
    col("SRC_DMN_NM"),
    col("Dummy")
)
df_lnk_Prov_Prefix_in = df_CD_MPPNG_Ext.select(
    col("SRC_LKUP_VAL"),
    col("SRC_SYS_CD"),
    col("SRC_DMN_NM"),
    col("Dummy")
)
df_lnk_Ntwk_Set_Prefix_in = df_CD_MPPNG_Ext.select(
    col("SRC_LKUP_VAL"),
    col("SRC_SYS_CD"),
    col("SRC_DMN_NM"),
    col("Dummy")
)
df_lnk_Cls_Pln_Rate_Grnt_in = df_CD_MPPNG_Ext.select(
    col("SRC_LKUP_VAL"),
    col("SRC_SYS_CD"),
    col("SRC_DMN_NM"),
    col("Dummy")
)

# CMC_CSPI_CS_PLAN (ODBCConnectorPX)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_CMC_CSPI_CS_PLAN = f"SELECT DISTINCT CSPI_AGE_CALC_METH FROM {FacetsOwner}.CMC_CSPI_CS_PLAN"
df_CMC_CSPI_CS_PLAN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_CSPI_CS_PLAN)
    .load()
)

# xfm_trm_CSPI_AGE_CALC_METH (CTransformerStage)
df_xfm_trm_CSPI_AGE_CALC_METH = df_CMC_CSPI_CS_PLAN.filter(length(trim(col("CSPI_AGE_CALC_METH"))) > 0).select(
    upper(trim(col("CSPI_AGE_CALC_METH"))).alias("SRC_LKUP_VAL"),
    lit("CLASS PLAN AGE CALCULATION METHOD").alias("SRC_DMN_NM")
)

# join_Cls_Pln_Prod_Cd (PxJoin)
df_join_Cls_Pln_Prod_Cd = (
    df_xfm_trm_CSPI_AGE_CALC_METH.alias("lnk_xfm_trm_CSPI_AGE_CALC_METH_out")
    .join(
        df_lnk_Cls_Pln_Prod_Cd_in.alias("lnk_Cls_Pln_Prod_Cd_in"),
        [
            col("lnk_xfm_trm_CSPI_AGE_CALC_METH_out.SRC_LKUP_VAL")
            == col("lnk_Cls_Pln_Prod_Cd_in.SRC_LKUP_VAL"),
            col("lnk_xfm_trm_CSPI_AGE_CALC_METH_out.SRC_DMN_NM")
            == col("lnk_Cls_Pln_Prod_Cd_in.SRC_DMN_NM"),
        ],
        "left",
    )
    .select(
        col("lnk_xfm_trm_CSPI_AGE_CALC_METH_out.SRC_LKUP_VAL").alias("CSPI_AGE_CALC_METH"),
        col("lnk_Cls_Pln_Prod_Cd_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_Cls_Pln_Prod_Cd_in.Dummy").alias("Dummy")
    )
)

# xfm_Cls_Pln_Prod_Cd (CTransformerStage)
df_xfm_Cls_Pln_Prod_Cd = df_join_Cls_Pln_Prod_Cd.filter(trim(col("Dummy")) != "1").select(
    concat(lit("|"), col("CSPI_AGE_CALC_METH"), lit("|")).alias("Code"),
    upper(col("CSPI_AGE_CALC_METH")).alias("Description"),
    lit("FACETS").alias("Source_System"),
    lit("CLASS PLAN AGE CALCULATION METHOD").alias("Domain")
)

# CMC_CSPI_CS_PLAN2 (ODBCConnectorPX)
extract_query_CMC_CSPI_CS_PLAN2 = f"SELECT DISTINCT CSPI_HEDIS_CEBREAK FROM {FacetsOwner}.CMC_CSPI_CS_PLAN"
df_CMC_CSPI_CS_PLAN2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_CSPI_CS_PLAN2)
    .load()
)

# xfm_trim_CSPI_HEDIS_CEBREAK (CTransformerStage)
df_xfm_trim_CSPI_HEDIS_CEBREAK = df_CMC_CSPI_CS_PLAN2.filter(length(trim(col("CSPI_HEDIS_CEBREAK"))) > 0).select(
    upper(trim(col("CSPI_HEDIS_CEBREAK"))).alias("SRC_LKUP_VAL"),
    lit("CLASS PLAN CONT ENR BREAK").alias("SRC_DMN_NM")
)

# join_Prov_Prefix (PxJoin)
df_join_Prov_Prefix = (
    df_xfm_trim_CSPI_HEDIS_CEBREAK.alias("lnk_xfm_trm_CSPI_HEDIS_CEBREAK_out")
    .join(
        df_lnk_Prov_Prefix_in.alias("lnk_Prov_Prefix_in"),
        [
            col("lnk_xfm_trm_CSPI_HEDIS_CEBREAK_out.SRC_LKUP_VAL")
            == col("lnk_Prov_Prefix_in.SRC_LKUP_VAL"),
            col("lnk_xfm_trm_CSPI_HEDIS_CEBREAK_out.SRC_DMN_NM")
            == col("lnk_Prov_Prefix_in.SRC_DMN_NM"),
        ],
        "left",
    )
    .select(
        col("lnk_xfm_trm_CSPI_HEDIS_CEBREAK_out.SRC_LKUP_VAL").alias("CSPI_HEDIS_CEBREAK"),
        col("lnk_Prov_Prefix_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_Prov_Prefix_in.Dummy").alias("Dummy")
    )
)

# xfm_Prov_Prefix (CTransformerStage)
df_xfm_Prov_Prefix = df_join_Prov_Prefix.filter(trim(col("Dummy")) != "1").select(
    concat(lit("|"), col("CSPI_HEDIS_CEBREAK"), lit("|")).alias("Code"),
    upper(col("CSPI_HEDIS_CEBREAK")).alias("Description"),
    lit("FACETS").alias("Source_System"),
    lit("CLASS PLAN CONT ENR BREAK").alias("Domain")
)

# CMC_CSPI_CS_PLAN3 (ODBCConnectorPX)
extract_query_CMC_CSPI_CS_PLAN3 = f"SELECT DISTINCT CSPI_FI FROM {FacetsOwner}.CMC_CSPI_CS_PLAN"
df_CMC_CSPI_CS_PLAN3 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_CSPI_CS_PLAN3)
    .load()
)

# xfm_trim_CSPI_FI (CTransformerStage)
df_xfm_trim_CSPI_FI = df_CMC_CSPI_CS_PLAN3.filter(length(trim(col("CSPI_FI"))) > 0).select(
    upper(trim(col("CSPI_FI"))).alias("SRC_LKUP_VAL"),
    lit("CLASS PLAN FAMILY CONTRACT").alias("SRC_DMN_NM")
)

# join_Ntwk_Set_Prefix (PxJoin)
df_join_Ntwk_Set_Prefix = (
    df_xfm_trim_CSPI_FI.alias("lnk_xfm_trm_CSPI_FI_out")
    .join(
        df_lnk_Ntwk_Set_Prefix_in.alias("lnk_Ntwk_Set_Prefix_in"),
        [
            col("lnk_xfm_trm_CSPI_FI_out.SRC_LKUP_VAL")
            == col("lnk_Ntwk_Set_Prefix_in.SRC_LKUP_VAL"),
            col("lnk_xfm_trm_CSPI_FI_out.SRC_DMN_NM")
            == col("lnk_Ntwk_Set_Prefix_in.SRC_DMN_NM"),
        ],
        "left",
    )
    .select(
        col("lnk_xfm_trm_CSPI_FI_out.SRC_LKUP_VAL").alias("CSPI_FI"),
        col("lnk_Ntwk_Set_Prefix_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_Ntwk_Set_Prefix_in.Dummy").alias("Dummy")
    )
)

# xfm_Ntwk_Set_Prefix (CTransformerStage)
df_xfm_Ntwk_Set_Prefix = df_join_Ntwk_Set_Prefix.filter(trim(col("Dummy")) != "1").select(
    concat(lit("|"), col("CSPI_FI"), lit("|")).alias("Code"),
    upper(col("CSPI_FI")).alias("Description"),
    lit("FACETS").alias("Source_System"),
    lit("CLASS PLAN FAMILY CONTRACT").alias("Domain")
)

# CMC_CSPI_CS_PLAN5 (ODBCConnectorPX)
extract_query_CMC_CSPI_CS_PLAN5 = f"SELECT DISTINCT CSPI_GUAR_IND FROM {FacetsOwner}.CMC_CSPI_CS_PLAN"
df_CMC_CSPI_CS_PLAN5 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_CSPI_CS_PLAN5)
    .load()
)

# xfm_CSPI_GUAR_IND (CTransformerStage)
df_xfm_CSPI_GUAR_IND = df_CMC_CSPI_CS_PLAN5.filter(length(trim(col("CSPI_GUAR_IND"))) > 0).select(
    upper(trim(col("CSPI_GUAR_IND"))).alias("SRC_LKUP_VAL"),
    lit("CLASS PLAN RATE GUARANTEE").alias("SRC_DMN_NM")
)

# join_Cls_Pln_Rate_Grnt (PxJoin)
df_join_Cls_Pln_Rate_Grnt = (
    df_xfm_CSPI_GUAR_IND.alias("lnk_xfm_trm_CSPI_GUAR_IND_out")
    .join(
        df_lnk_Cls_Pln_Rate_Grnt_in.alias("lnk_Cls_Pln_Rate_Grnt_in"),
        [
            col("lnk_xfm_trm_CSPI_GUAR_IND_out.SRC_LKUP_VAL")
            == col("lnk_Cls_Pln_Rate_Grnt_in.SRC_LKUP_VAL"),
            col("lnk_xfm_trm_CSPI_GUAR_IND_out.SRC_DMN_NM")
            == col("lnk_Cls_Pln_Rate_Grnt_in.SRC_DMN_NM"),
        ],
        "left",
    )
    .select(
        col("lnk_xfm_trm_CSPI_GUAR_IND_out.SRC_LKUP_VAL").alias("CSPI_GUAR_IND"),
        col("lnk_Cls_Pln_Rate_Grnt_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_Cls_Pln_Rate_Grnt_in.Dummy").alias("Dummy")
    )
)

# xfm_Cls_Pln_Rate_Grnt (CTransformerStage)
df_xfm_Cls_Pln_Rate_Grnt = df_join_Cls_Pln_Rate_Grnt.filter(trim(col("Dummy")) != "1").select(
    concat(lit("|"), col("CSPI_GUAR_IND"), lit("|")).alias("Code"),
    upper(col("CSPI_GUAR_IND")).alias("Description"),
    lit("FACETS").alias("Source_System"),
    lit("CLASS PLAN RATE GUARANTEE").alias("Domain")
)

# fl_New_Codes (PxFunnel)
df_fl_New_Codes = (
    df_xfm_Cls_Pln_Prod_Cd.unionByName(df_xfm_Prov_Prefix, allowMissingColumns=False)
    .unionByName(df_xfm_Ntwk_Set_Prefix, allowMissingColumns=False)
    .unionByName(df_xfm_Cls_Pln_Rate_Grnt, allowMissingColumns=False)
)

df_fl_New_Codes = df_fl_New_Codes.sort(col("Domain").asc(), col("Code").asc())

# CDMA_Code_Diff (PxSequentialFile) => final write
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
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)