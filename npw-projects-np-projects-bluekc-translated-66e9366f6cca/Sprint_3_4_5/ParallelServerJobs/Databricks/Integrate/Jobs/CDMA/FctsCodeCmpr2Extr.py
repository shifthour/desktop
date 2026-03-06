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
# MAGIC Saikiran Subbagari	2018-10-11	Production Support		Original Programming				IntegrateDev2		Abhiram Dasarathy	2018-11-06
# MAGIC Saikiran Subbagari	2018-12-19	Production Support  	Join on SRC_DMN_NM                         				IntegrateDev2
# MAGIC Prabhu ES	2022-02-24	S2S Remediation     	MSSQL connection parameters added   				IntegrateDev5
# MAGIC Brent Leland	03-26-2022	Syb 2 SQL               	Added column alias  as GPAI_SP_STOP_IND			IntegrateDev5		Ken Bradmon	2022-05-02

# MAGIC Code Domains
# MAGIC 
# MAGIC CLASS PLAN COVERAGE RULE EVENT TYPE
# MAGIC CLASS PLAN COVERAGE RULE WAIT PERIOD START
# MAGIC CLASS PLAN COVERAGE RULE WAIT PERIOD BEGIN
# MAGIC CLASS PLAN COVERAGE RULE WAIT PERIOD TYPE
# MAGIC CLASS PLAN PRODUCT CATEGORY
# MAGIC CLASS PLAN DETAIL COVERING PROVIDER SET PREFIX
# MAGIC CLASS PLAN DETAIL NETWORK SET PREFIX
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
from pyspark.sql.functions import col, length, upper, lit, rpad, when
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

facets_secret_name = get_widget_value('facets_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
FacetsOwner = get_widget_value('FacetsOwner','')
IDSOwner = get_widget_value('IDSOwner','')
TempTable = get_widget_value('TempTable','')

# ---------------------------------------------------------------
# Stage: CD_MPPNG_Ext  (PxSequentialFile)
# ---------------------------------------------------------------
schema_CD_MPPNG_Ext = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("SRC_DMN_NM", StringType(), True),
    StructField("SRC_LKUP_VAL", StringType(), True),
    StructField("Dummy", IntegerType(), True)
])
df_CD_MPPNG_Ext = (
    spark.read.format("csv")
    .option("sep", "|")
    .option("quote", "\u0000")
    .schema(schema_CD_MPPNG_Ext)
    .load(f"{adls_path}/load/CDMA_CD_MPPNG.txt")
)

# ---------------------------------------------------------------
# Stage: cp_CD_MPPNG  (PxCopy)
# ---------------------------------------------------------------
df_Pln_Cov_Rule_Evt_in = df_CD_MPPNG_Ext.select(
    col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    col("Dummy").alias("Dummy")
)

df_Cls_Pln_Cov_Rule_Wait_Strt_in = df_CD_MPPNG_Ext.select(
    col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    col("Dummy").alias("Dummy")
)

df_Cls_Pln_Cov_Rule_Wait_Beg_in = df_CD_MPPNG_Ext.select(
    col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    col("Dummy").alias("Dummy")
)

df_Cls_Pln_Cov_Rule_Wait_Type_in = df_CD_MPPNG_Ext.select(
    col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    col("Dummy").alias("Dummy")
)

df_Cls_Pln_Prod_Cd_in = df_CD_MPPNG_Ext.select(
    col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    col("Dummy").alias("Dummy")
)

df_Prov_Prefix_in = df_CD_MPPNG_Ext.select(
    col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    col("Dummy").alias("Dummy")
)

df_Ntwk_Set_Prefix_in = df_CD_MPPNG_Ext.select(
    col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    col("Dummy").alias("Dummy")
)

# ---------------------------------------------------------------
# Stage: CMC_GPAI_GRP_ADM (ODBCConnectorPX)
# ---------------------------------------------------------------
jdbc_url_CMC_GPAI_GRP_ADM, jdbc_props_CMC_GPAI_GRP_ADM = get_db_config(facets_secret_name)
extract_query_CMC_GPAI_GRP_ADM = (
    f"SELECT DISTINCT GPAI_SP_STOP_IND as GPAI_STOP_IND "
    f"FROM {FacetsOwner}.CMC_GPAI_GRP_ADM "
    f"UNION "
    f"SELECT DISTINCT GPAI_DEP_STOP_IND as GPAI_STOP_IND "
    f"FROM {FacetsOwner}.CMC_GPAI_GRP_ADM "
    f"UNION "
    f"SELECT DISTINCT GPAI_STU_STOP_IND as GPAI_STOP_IND "
    f"FROM {FacetsOwner}.CMC_GPAI_GRP_ADM "
    f"UNION "
    f"SELECT DISTINCT GPAI_SB_STOP_IND as GPAI_STOP_IND "
    f"FROM {FacetsOwner}.CMC_GPAI_GRP_ADM"
)
df_CMC_GPAI_GRP_ADM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CMC_GPAI_GRP_ADM)
    .options(**jdbc_props_CMC_GPAI_GRP_ADM)
    .option("query", extract_query_CMC_GPAI_GRP_ADM)
    .load()
)

# ---------------------------------------------------------------
# Stage: xfm_trim_GPAI_ADM (CTransformerStage)
# ---------------------------------------------------------------
df_xfm_trim_GPAI_ADM = (
    df_CMC_GPAI_GRP_ADM
    .filter(length(trim(col("GPAI_STOP_IND"))) > 0)
    .select(
        upper(trim(col("GPAI_STOP_IND"))).alias("SRC_LKUP_VAL"),
        lit("CLASS PLAN COVERAGE RULE EVENT TYPE").alias("SRC_DMN_NM")
    )
)

# ---------------------------------------------------------------
# Stage: join_Pln_Cov_Rule_Evt (PxJoin)
#   Left join on (SRC_LKUP_VAL, SRC_DMN_NM)
# ---------------------------------------------------------------
df_join_Pln_Cov_Rule_Evt = (
    df_xfm_trim_GPAI_ADM.alias("L")
    .join(
        df_Pln_Cov_Rule_Evt_in.alias("R"),
        (col("L.SRC_LKUP_VAL") == col("R.SRC_LKUP_VAL"))
        & (col("L.SRC_DMN_NM") == col("R.SRC_DMN_NM")),
        "left"
    )
    .select(
        col("L.SRC_LKUP_VAL").alias("GPAI_STOP_IND"),
        col("R.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("R.Dummy").alias("Dummy")
    )
)

# ---------------------------------------------------------------
# Stage: xfm_Pln_Cov_Rule_Evt (CTransformerStage)
# ---------------------------------------------------------------
df_xfm_Pln_Cov_Rule_Evt = (
    df_join_Pln_Cov_Rule_Evt
    .filter(col("Dummy") != 1)
    .select(
        (lit("|") + col("GPAI_STOP_IND") + lit("|")).alias("Code"),
        upper(col("GPAI_STOP_IND")).alias("Description"),
        lit("FACETS").alias("Source_System"),
        lit("CLASS PLAN COVERAGE RULE EVENT TYPE").alias("Domain")
    )
)

# ---------------------------------------------------------------
# Stage: CMC_GPAI_GRP_ADM2 (ODBCConnectorPX)
# ---------------------------------------------------------------
jdbc_url_CMC_GPAI_GRP_ADM2, jdbc_props_CMC_GPAI_GRP_ADM2 = get_db_config(facets_secret_name)
extract_query_CMC_GPAI_GRP_ADM2 = (
    f"SELECT DISTINCT GPAI_WT_PER_IND "
    f"FROM {FacetsOwner}.CMC_GPAI_GRP_ADM"
)
df_CMC_GPAI_GRP_ADM2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CMC_GPAI_GRP_ADM2)
    .options(**jdbc_props_CMC_GPAI_GRP_ADM2)
    .option("query", extract_query_CMC_GPAI_GRP_ADM2)
    .load()
)

# ---------------------------------------------------------------
# Stage: xfm_trim_GPAI_ADM2 (CTransformerStage)
# ---------------------------------------------------------------
df_xfm_trim_GPAI_ADM2 = (
    df_CMC_GPAI_GRP_ADM2
    .filter(length(trim(col("GPAI_WT_PER_IND"))) > 0)
    .select(
        upper(trim(col("GPAI_WT_PER_IND"))).alias("SRC_LKUP_VAL"),
        lit("CLASS PLAN COVERAGE RULE WAIT PERIOD START").alias("SRC_DMN_NM")
    )
)

# ---------------------------------------------------------------
# Stage: join_Cls_Pln_Cov_Rule_Wait_Strt (PxJoin)
# ---------------------------------------------------------------
df_join_Cls_Pln_Cov_Rule_Wait_Strt = (
    df_xfm_trim_GPAI_ADM2.alias("L")
    .join(
        df_Cls_Pln_Cov_Rule_Wait_Strt_in.alias("R"),
        (col("L.SRC_LKUP_VAL") == col("R.SRC_LKUP_VAL"))
        & (col("L.SRC_DMN_NM") == col("R.SRC_DMN_NM")),
        "left"
    )
    .select(
        col("L.SRC_LKUP_VAL").alias("GPAI_WT_PER_IND"),
        col("R.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("R.Dummy").alias("Dummy")
    )
)

# ---------------------------------------------------------------
# Stage: xfm_Cls_Pln_Cov_Rule_Wait_Strt (CTransformerStage)
# ---------------------------------------------------------------
df_xfm_Cls_Pln_Cov_Rule_Wait_Strt = (
    df_join_Cls_Pln_Cov_Rule_Wait_Strt
    .filter(col("Dummy") != 1)
    .select(
        (lit("|") + col("GPAI_WT_PER_IND") + lit("|")).alias("Code"),
        upper(col("GPAI_WT_PER_IND")).alias("Description"),
        lit("FACETS").alias("Source_System"),
        lit("CLASS PLAN COVERAGE RULE WAIT PERIOD START").alias("Domain")
    )
)

# ---------------------------------------------------------------
# Stage: CMC_GPAI_GRP_ADM3 (ODBCConnectorPX)
# ---------------------------------------------------------------
jdbc_url_CMC_GPAI_GRP_ADM3, jdbc_props_CMC_GPAI_GRP_ADM3 = get_db_config(facets_secret_name)
extract_query_CMC_GPAI_GRP_ADM3 = (
    f"SELECT DISTINCT GPAI_CVG_BEGIN "
    f"FROM {FacetsOwner}.CMC_GPAI_GRP_ADM"
)
df_CMC_GPAI_GRP_ADM3 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CMC_GPAI_GRP_ADM3)
    .options(**jdbc_props_CMC_GPAI_GRP_ADM3)
    .option("query", extract_query_CMC_GPAI_GRP_ADM3)
    .load()
)

# ---------------------------------------------------------------
# Stage: xfm_trim_GPAI_ADM3 (CTransformerStage)
# ---------------------------------------------------------------
df_xfm_trim_GPAI_ADM3 = (
    df_CMC_GPAI_GRP_ADM3
    .filter(length(trim(col("GPAI_CVG_BEGIN"))) > 0)
    .select(
        upper(trim(col("GPAI_CVG_BEGIN"))).alias("SRC_LKUP_VAL"),
        lit("CLASS PLAN COVERAGE RULE WAIT PERIOD BEGIN").alias("SRC_DMN_NM")
    )
)

# ---------------------------------------------------------------
# Stage: join_Cls_Pln_Cov_Rule_Wait_Beg (PxJoin)
# ---------------------------------------------------------------
df_join_Cls_Pln_Cov_Rule_Wait_Beg = (
    df_xfm_trim_GPAI_ADM3.alias("L")
    .join(
        df_Cls_Pln_Cov_Rule_Wait_Beg_in.alias("R"),
        (col("L.SRC_LKUP_VAL") == col("R.SRC_LKUP_VAL"))
        & (col("L.SRC_DMN_NM") == col("R.SRC_DMN_NM")),
        "left"
    )
    .select(
        col("L.SRC_LKUP_VAL").alias("GPAI_CVG_BEGIN"),
        col("R.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("R.Dummy").alias("Dummy")
    )
)

# ---------------------------------------------------------------
# Stage: xfm_Cls_Pln_Cov_Rule_Wait_Beg (CTransformerStage)
# ---------------------------------------------------------------
df_xfm_Cls_Pln_Cov_Rule_Wait_Beg = (
    df_join_Cls_Pln_Cov_Rule_Wait_Beg
    .filter(col("Dummy") != 1)
    .select(
        (lit("|") + col("GPAI_CVG_BEGIN") + lit("|")).alias("Code"),
        upper(col("GPAI_CVG_BEGIN")).alias("Description"),
        lit("FACETS").alias("Source_System"),
        lit("CLASS PLAN COVERAGE RULE WAIT PERIOD BEGIN").alias("Domain")
    )
)

# ---------------------------------------------------------------
# Stage: CMC_GPAI_GRP_ADM4 (ODBCConnectorPX)
# ---------------------------------------------------------------
jdbc_url_CMC_GPAI_GRP_ADM4, jdbc_props_CMC_GPAI_GRP_ADM4 = get_db_config(facets_secret_name)
extract_query_CMC_GPAI_GRP_ADM4 = (
    f"SELECT DISTINCT GPAI_WT_PER_TYPE "
    f"FROM {FacetsOwner}.CMC_GPAI_GRP_ADM"
)
df_CMC_GPAI_GRP_ADM4 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CMC_GPAI_GRP_ADM4)
    .options(**jdbc_props_CMC_GPAI_GRP_ADM4)
    .option("query", extract_query_CMC_GPAI_GRP_ADM4)
    .load()
)

# ---------------------------------------------------------------
# Stage: xfm_trim_GPAI_ADM4 (CTransformerStage)
# ---------------------------------------------------------------
df_xfm_trim_GPAI_ADM4 = (
    df_CMC_GPAI_GRP_ADM4
    .filter(length(trim(col("GPAI_WT_PER_TYPE"))) > 0)
    .select(
        upper(trim(col("GPAI_WT_PER_TYPE"))).alias("SRC_LKUP_VAL"),
        lit("CLASS PLAN COVERAGE RULE WAIT PERIOD TYPE").alias("SRC_DMN_NM")
    )
)

# ---------------------------------------------------------------
# Stage: join_Cls_Pln_Cov_Rule_Wait_Type (PxJoin)
# ---------------------------------------------------------------
df_join_Cls_Pln_Cov_Rule_Wait_Type = (
    df_xfm_trim_GPAI_ADM4.alias("L")
    .join(
        df_Cls_Pln_Cov_Rule_Wait_Type_in.alias("R"),
        (col("L.SRC_LKUP_VAL") == col("R.SRC_LKUP_VAL"))
        & (col("L.SRC_DMN_NM") == col("R.SRC_DMN_NM")),
        "left"
    )
    .select(
        col("L.SRC_LKUP_VAL").alias("GPAI_WT_PER_TYPE"),
        col("R.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("R.Dummy").alias("Dummy")
    )
)

# ---------------------------------------------------------------
# Stage: xfm_Cls_Pln_Cov_Rule_Wait_Type (CTransformerStage)
# ---------------------------------------------------------------
df_xfm_Cls_Pln_Cov_Rule_Wait_Type = (
    df_join_Cls_Pln_Cov_Rule_Wait_Type
    .filter(col("Dummy") != 1)
    .select(
        (lit("|") + col("GPAI_WT_PER_TYPE") + lit("|")).alias("Code"),
        upper(col("GPAI_WT_PER_TYPE")).alias("Description"),
        lit("FACETS").alias("Source_System"),
        lit("CLASS PLAN COVERAGE RULE WAIT PERIOD TYPE").alias("Domain")
    )
)

# ---------------------------------------------------------------
# Stage: CMC_CSPI_CS_PLAN (ODBCConnectorPX)
# ---------------------------------------------------------------
jdbc_url_CMC_CSPI_CS_PLAN, jdbc_props_CMC_CSPI_CS_PLAN = get_db_config(facets_secret_name)
extract_query_CMC_CSPI_CS_PLAN = (
    f"SELECT DISTINCT CSPD_CAT FROM {FacetsOwner}.CMC_CSPI_CS_PLAN"
)
df_CMC_CSPI_CS_PLAN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CMC_CSPI_CS_PLAN)
    .options(**jdbc_props_CMC_CSPI_CS_PLAN)
    .option("query", extract_query_CMC_CSPI_CS_PLAN)
    .load()
)

# ---------------------------------------------------------------
# Stage: xfm_trm_CSPD_CAT (CTransformerStage)
# ---------------------------------------------------------------
df_xfm_trm_CSPD_CAT = (
    df_CMC_CSPI_CS_PLAN
    .filter(length(trim(col("CSPD_CAT"))) > 0)
    .select(
        upper(trim(col("CSPD_CAT"))).alias("SRC_LKUP_VAL"),
        lit("CLASS PLAN PRODUCT CATEGORY").alias("SRC_DMN_NM")
    )
)

# ---------------------------------------------------------------
# Stage: join_Cls_Pln_Prod_Cd (PxJoin)
# ---------------------------------------------------------------
df_join_Cls_Pln_Prod_Cd = (
    df_xfm_trm_CSPD_CAT.alias("L")
    .join(
        df_Cls_Pln_Prod_Cd_in.alias("R"),
        (col("L.SRC_LKUP_VAL") == col("R.SRC_LKUP_VAL"))
        & (col("L.SRC_DMN_NM") == col("R.SRC_DMN_NM")),
        "left"
    )
    .select(
        col("L.SRC_LKUP_VAL").alias("CSPD_CAT"),
        col("R.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("R.Dummy").alias("Dummy")
    )
)

# ---------------------------------------------------------------
# Stage: xfm_Cls_Pln_Prod_Cd (CTransformerStage)
# ---------------------------------------------------------------
df_xfm_Cls_Pln_Prod_Cd = (
    df_join_Cls_Pln_Prod_Cd
    .filter(col("Dummy") != 1)
    .select(
        (lit("|") + col("CSPD_CAT") + lit("|")).alias("Code"),
        upper(col("CSPD_CAT")).alias("Description"),
        lit("FACETS").alias("Source_System"),
        lit("CLASS PLAN PRODUCT CATEGORY").alias("Domain")
    )
)

# ---------------------------------------------------------------
# Stage: CMC_CSPI_CS_PLAN2 (ODBCConnectorPX)
# ---------------------------------------------------------------
jdbc_url_CMC_CSPI_CS_PLAN2, jdbc_props_CMC_CSPI_CS_PLAN2 = get_db_config(facets_secret_name)
extract_query_CMC_CSPI_CS_PLAN2 = (
    f"SELECT DISTINCT CVST_PFX FROM {FacetsOwner}.CMC_CSPI_CS_PLAN"
)
df_CMC_CSPI_CS_PLAN2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CMC_CSPI_CS_PLAN2)
    .options(**jdbc_props_CMC_CSPI_CS_PLAN2)
    .option("query", extract_query_CMC_CSPI_CS_PLAN2)
    .load()
)

# ---------------------------------------------------------------
# Stage: xfm_trim_CVST_PFX (CTransformerStage)
# ---------------------------------------------------------------
df_xfm_trim_CVST_PFX = (
    df_CMC_CSPI_CS_PLAN2
    .filter(length(trim(col("CVST_PFX"))) > 0)
    .select(
        upper(trim(col("CVST_PFX"))).alias("SRC_LKUP_VAL"),
        lit("CLASS PLAN DETAIL NETWORK SET PREFIX").alias("SRC_DMN_NM")
    )
)

# ---------------------------------------------------------------
# Stage: join_Prov_Prefix (PxJoin)
# ---------------------------------------------------------------
df_join_Prov_Prefix = (
    df_xfm_trim_CVST_PFX.alias("L")
    .join(
        df_Prov_Prefix_in.alias("R"),
        (col("L.SRC_LKUP_VAL") == col("R.SRC_LKUP_VAL"))
        & (col("L.SRC_DMN_NM") == col("R.SRC_DMN_NM")),
        "left"
    )
    .select(
        col("L.SRC_LKUP_VAL").alias("CVST_PFX"),
        col("R.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("R.Dummy").alias("Dummy")
    )
)

# ---------------------------------------------------------------
# Stage: xfm_Prov_Prefix (CTransformerStage)
# ---------------------------------------------------------------
df_xfm_Prov_Prefix = (
    df_join_Prov_Prefix
    .filter(col("Dummy") != 1)
    .select(
        (lit("|") + col("CVST_PFX") + lit("|")).alias("Code"),
        upper(col("CVST_PFX")).alias("Description"),
        lit("FACETS").alias("Source_System"),
        lit("CLASS PLAN DETAIL NETWORK SET PREFIX").alias("Domain")
    )
)

# ---------------------------------------------------------------
# Stage: CMC_CSPI_CS_PLAN3 (ODBCConnectorPX)
# ---------------------------------------------------------------
jdbc_url_CMC_CSPI_CS_PLAN3, jdbc_props_CMC_CSPI_CS_PLAN3 = get_db_config(facets_secret_name)
extract_query_CMC_CSPI_CS_PLAN3 = (
    f"SELECT DISTINCT pd.PDBC_PFX, pd.PDPX_DESC "
    f"FROM {FacetsOwner}.CMC_PDPX_DESC pd "
    f"WHERE pd.PDBC_TYPE = 'NWST'"
)
df_CMC_CSPI_CS_PLAN3 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CMC_CSPI_CS_PLAN3)
    .options(**jdbc_props_CMC_CSPI_CS_PLAN3)
    .option("query", extract_query_CMC_CSPI_CS_PLAN3)
    .load()
)

# ---------------------------------------------------------------
# Stage: xfm_trim_PDBC_PFX (CTransformerStage)
# ---------------------------------------------------------------
df_xfm_trim_PDBC_PFX = (
    df_CMC_CSPI_CS_PLAN3
    .filter(length(trim(col("PDBC_PFX"))) > 0)
    .select(
        upper(trim(col("PDBC_PFX"))).alias("SRC_LKUP_VAL"),
        col("PDPX_DESC").alias("PDPX_DESC"),
        lit("CLASS PLAN DETAIL NETWORK SET PREFIX").alias("SRC_DMN_NM")
    )
)

# ---------------------------------------------------------------
# Stage: join_Ntwk_Set_Prefix (PxJoin)
# ---------------------------------------------------------------
df_join_Ntwk_Set_Prefix = (
    df_xfm_trim_PDBC_PFX.alias("L")
    .join(
        df_Ntwk_Set_Prefix_in.alias("R"),
        (col("L.SRC_LKUP_VAL") == col("R.SRC_LKUP_VAL"))
        & (col("L.SRC_DMN_NM") == col("R.SRC_DMN_NM")),
        "left"
    )
    .select(
        col("L.SRC_LKUP_VAL").alias("PDBC_PFX"),
        col("L.PDPX_DESC").alias("PDPX_DESC"),
        col("R.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("R.Dummy").alias("Dummy")
    )
)

# ---------------------------------------------------------------
# Stage: xfm_Ntwk_Set_Prefix (CTransformerStage)
# ---------------------------------------------------------------
df_xfm_Ntwk_Set_Prefix = (
    df_join_Ntwk_Set_Prefix
    .filter(col("Dummy") != 1)
    .select(
        (lit("|") + col("PDBC_PFX") + lit("|")).alias("Code"),
        upper(col("PDPX_DESC")).alias("Description"),
        lit("FACETS").alias("Source_System"),
        lit("CLASS PLAN DETAIL NETWORK SET PREFIX").alias("Domain")
    )
)

# ---------------------------------------------------------------
# Stage: fl_New_Codes (PxFunnel)
# ---------------------------------------------------------------
df_fl_New_Codes = (
    df_xfm_Pln_Cov_Rule_Evt
    .unionByName(df_xfm_Cls_Pln_Cov_Rule_Wait_Strt, allowMissingColumns=True)
    .unionByName(df_xfm_Cls_Pln_Cov_Rule_Wait_Beg, allowMissingColumns=True)
    .unionByName(df_xfm_Cls_Pln_Cov_Rule_Wait_Type, allowMissingColumns=True)
    .unionByName(df_xfm_Cls_Pln_Prod_Cd, allowMissingColumns=True)
    .unionByName(df_xfm_Prov_Prefix, allowMissingColumns=True)
    .unionByName(df_xfm_Ntwk_Set_Prefix, allowMissingColumns=True)
)

# The funnel output columns: [Code, Description, Source_System, Domain]

# ---------------------------------------------------------------
# Stage: CDMA_Code_Diff (PxSequentialFile) -- writing output
# ---------------------------------------------------------------
# rpad columns that are char(80) as indicated
df_fl_New_Codes_final = (
    df_fl_New_Codes
    .select(
        col("Code").alias("Code"),
        rpad(col("Description"), 80, " ").alias("Description"),
        col("Source_System").alias("Source_System"),
        rpad(col("Domain"), 80, " ").alias("Domain")
    )
)

write_files(
    df_fl_New_Codes_final,
    f"{adls_path}/load/processed/CDMA_Code_Diff.{RunID}.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)