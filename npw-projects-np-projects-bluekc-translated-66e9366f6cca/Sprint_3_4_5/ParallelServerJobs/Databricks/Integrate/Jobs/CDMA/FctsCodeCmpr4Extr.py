# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Job Name:  FctsCodeCmpr4Extr
# MAGIC 
# MAGIC Read Facets code tables and compare to CDMA code sets.  Capture differences for emailing metadata analyst.
# MAGIC This process runs in development weekdays and emails the output to Metadata Support
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer	Date		Project/Altiris #		Change Description					Development Project		Reviewer		Review Date
# MAGIC =========================================================================================================================================================================================
# MAGIC Saikiran Subbagari	2018-10-11	Production Support		Original Programming				IntegrateDev2		Abhiram Dasarathy	2018-11-06
# MAGIC Saikiran Subbagari	2018-12-19              	Production Support		Join on SRC_DMN_NM				IntegrateDev2		Abhiram Dasarathy	2018-12-26
# MAGIC Prabhu ES	2022-02-24              	S2S Remediation		MSSQL connection parameters added   			IntegrateDev5		Ken Bradmon	2022-05-02

# MAGIC Code Domains
# MAGIC 
# MAGIC GROUP BILLING LEVEL
# MAGIC CLAIM NON PARTICIPATING PROVIDER PREFIX
# MAGIC CLAIM PROCESSING CONTROL AGENT PREFIX
# MAGIC CLAIM SERVICE DEFINITION PREFIX
# MAGIC SERVICE TYPE
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
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value("RunID","")
FacetsOwner = get_widget_value("FacetsOwner","")
facets_secret_name = get_widget_value("facets_secret_name","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
TempTable = get_widget_value("TempTable","")

# Read from CD_MPPNG_Ext (PxSequentialFile)
schema_CD_MPPNG_Ext = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("SRC_DMN_NM", StringType(), False),
    StructField("SRC_LKUP_VAL", StringType(), False),
    StructField("Dummy", IntegerType(), False)
])
df_CD_MPPNG_Ext = (
    spark.read.format("csv")
    .option("sep", "|")
    .option("quote", None)
    .schema(schema_CD_MPPNG_Ext)
    .load(f"{adls_path}/load/CDMA_CD_MPPNG.txt")
)

# cp_CD_MPPNG (PxCopy) - Split into multiple outputs
df_cp_CD_MPPNG_Grp_Bill_Lvl_in = df_CD_MPPNG_Ext.select(
    F.col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    F.col("Dummy").alias("Dummy")
)
df_cp_CD_MPPNG_Service_Type_in = df_CD_MPPNG_Ext.select(
    F.col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    F.col("Dummy").alias("Dummy")
)
df_cp_CD_MPPNG_NonPartProv_in = df_CD_MPPNG_Ext.select(
    F.col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    F.col("Dummy").alias("Dummy")
)
df_cp_CD_MPPNG_NonPcagProv_in = df_CD_MPPNG_Ext.select(
    F.col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    F.col("Dummy").alias("Dummy")
)
df_cp_CD_MPPNG_NonSdefProv_in = df_CD_MPPNG_Ext.select(
    F.col("SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SRC_DMN_NM").alias("SRC_DMN_NM"),
    F.col("Dummy").alias("Dummy")
)

# CMC_GRGR_GROUP (ODBCConnectorPX)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query = f"SELECT DISTINCT GRGR_BILL_LEVEL FROM {FacetsOwner}.CMC_GRGR_GROUP"
df_CMC_GRGR_GROUP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query)
    .load()
)

# xfm_trm_GRGR_BILL_LEVEL (CTransformerStage)
df_xfm_trm_GRGR_BILL_LEVEL = df_CMC_GRGR_GROUP.filter(
    F.length(trim(F.col("GRGR_BILL_LEVEL"))) > 0
).select(
    UpCase(trim(F.col("GRGR_BILL_LEVEL"))).alias("SRC_LKUP_VAL"),
    F.lit("GROUP BILLING LEVEL").alias("SRC_DMN_NM")
)

# join_Grp_Bill_Lvl (PxJoin, left outer join on SRC_LKUP_VAL, SRC_DMN_NM)
df_join_Grp_Bill_Lvl = (
    df_xfm_trm_GRGR_BILL_LEVEL.alias("lnk_xfm_trm_GRGR_BILL_LEVEL_out")
    .join(
        df_cp_CD_MPPNG_Grp_Bill_Lvl_in.alias("lnk_Grp_Bill_Lvl_in"),
        (
            (F.col("lnk_xfm_trm_GRGR_BILL_LEVEL_out.SRC_LKUP_VAL") 
             == F.col("lnk_Grp_Bill_Lvl_in.SRC_LKUP_VAL"))
            & 
            (F.col("lnk_xfm_trm_GRGR_BILL_LEVEL_out.SRC_DMN_NM") 
             == F.col("lnk_Grp_Bill_Lvl_in.SRC_DMN_NM"))
        ),
        "left"
    )
    .select(
        F.col("lnk_xfm_trm_GRGR_BILL_LEVEL_out.SRC_LKUP_VAL").alias("GRGR_BILL_LEVEL"),
        F.col("lnk_Grp_Bill_Lvl_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_Grp_Bill_Lvl_in.Dummy").alias("Dummy")
    )
)

# xfm_Grp_Bill_Lvl (CTransformerStage)
df_xfm_Grp_Bill_Lvl = df_join_Grp_Bill_Lvl.filter(F.col("Dummy") != 1)
df_xfm_Grp_Bill_Lvl = df_xfm_Grp_Bill_Lvl.select(
    F.concat(F.lit("|"), F.col("GRGR_BILL_LEVEL"), F.lit("|")).alias("Code"),
    UpCase(F.col("GRGR_BILL_LEVEL")).alias("Description"),
    F.lit("FACETS").alias("Source_System"),
    F.lit("GROUP BILLING LEVEL").alias("Domain")
)

# CMC_PDPX_DESC (ODBCConnectorPX) -- NPPR
extract_query_nppr = (
    f"SELECT DISTINCT PDBC_PFX, PDPX_DESC, PDBC_TYPE "
    f"FROM {FacetsOwner}.CMC_PDPX_DESC WHERE PDBC_TYPE IN ('NPPR')"
)
df_CMC_PDPX_DESC_nppr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_nppr)
    .load()
)

# xfm_trim_PDBC_PFX (CTransformerStage) -- NPPR
df_xfm_trim_PDBC_PFX_nppr = df_CMC_PDPX_DESC_nppr.filter(
    F.length(trim(F.col("PDBC_PFX"))) > 0
).select(
    UpCase(trim(F.col("PDBC_PFX"))).alias("SRC_LKUP_VAL"),
    F.col("PDPX_DESC").alias("PDPX_DESC"),
    F.col("PDBC_TYPE").alias("PDBC_TYPE"),
    F.lit("CLAIM NON PARTICIPATING PROVIDER PREFIX").alias("SRC_DMN_NM")
)

# join_NonPartProv (PxJoin, left outer join)
df_join_NonPartProv = (
    df_xfm_trim_PDBC_PFX_nppr.alias("lnk_xfm_trm_PDBC_PFX_out")
    .join(
        df_cp_CD_MPPNG_NonPartProv_in.alias("lnk_NonPartProv_in"),
        (
            (F.col("lnk_xfm_trm_PDBC_PFX_out.SRC_LKUP_VAL") 
             == F.col("lnk_NonPartProv_in.SRC_LKUP_VAL"))
            & 
            (F.col("lnk_xfm_trm_PDBC_PFX_out.SRC_DMN_NM") 
             == F.col("lnk_NonPartProv_in.SRC_DMN_NM"))
        ),
        "left"
    )
    .select(
        F.col("lnk_xfm_trm_PDBC_PFX_out.SRC_LKUP_VAL").alias("PDBC_PFX"),
        F.col("lnk_NonPartProv_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_xfm_trm_PDBC_PFX_out.PDPX_DESC").alias("PDPX_DESC"),
        F.col("lnk_xfm_trm_PDBC_PFX_out.PDBC_TYPE").alias("PDBC_TYPE"),
        F.col("lnk_NonPartProv_in.Dummy").alias("Dummy")
    )
)

# xfm_NonPartProv (CTransformerStage)
df_xfm_NonPartProv = df_join_NonPartProv.filter(F.col("Dummy") != 1)
df_xfm_NonPartProv = df_xfm_NonPartProv.select(
    F.concat(F.lit("|"), F.col("PDBC_PFX"), F.lit("|")).alias("Code"),
    UpCase(F.col("PDPX_DESC")).alias("Description"),
    F.lit("FACETS").alias("Source_System"),
    F.lit("CLAIM NON PARTICIPATING PROVIDER PREFIX").alias("Domain")
)

# CMC_PDPX_DESC (ODBCConnectorPX) -- PCAG
extract_query_pcag = (
    f"SELECT DISTINCT PDBC_PFX, PDPX_DESC, PDBC_TYPE "
    f"FROM {FacetsOwner}.CMC_PDPX_DESC WHERE PDBC_TYPE IN ('PCAG')"
)
df_CMC_PCAG_DESC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_pcag)
    .load()
)

# xfm_trim_PCAG_PFX (CTransformerStage)
df_xfm_trim_PCAG_PFX = df_CMC_PCAG_DESC.filter(
    F.length(trim(F.col("PDBC_PFX"))) > 0
).select(
    UpCase(trim(F.col("PDBC_PFX"))).alias("SRC_LKUP_VAL"),
    F.col("PDPX_DESC").alias("PDPX_DESC"),
    F.col("PDBC_TYPE").alias("PDBC_TYPE"),
    F.lit("CLAIM PROCESSING CONTROL AGENT PREFIX").alias("SRC_DMN_NM")
)

# join_NonPcagProv
df_join_NonPcagProv = (
    df_xfm_trim_PCAG_PFX.alias("lnk_xfm_trm_PCAG_PFX_out")
    .join(
        df_cp_CD_MPPNG_NonPcagProv_in.alias("lnk_NonPcagProv_in"),
        (
            (F.col("lnk_xfm_trm_PCAG_PFX_out.SRC_LKUP_VAL") 
             == F.col("lnk_NonPcagProv_in.SRC_LKUP_VAL"))
            & 
            (F.col("lnk_xfm_trm_PCAG_PFX_out.SRC_DMN_NM") 
             == F.col("lnk_NonPcagProv_in.SRC_DMN_NM"))
        ),
        "left"
    )
    .select(
        F.col("lnk_xfm_trm_PCAG_PFX_out.SRC_LKUP_VAL").alias("PDBC_PFX"),
        F.col("lnk_NonPcagProv_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_xfm_trm_PCAG_PFX_out.PDPX_DESC").alias("PDPX_DESC"),
        F.col("lnk_xfm_trm_PCAG_PFX_out.PDBC_TYPE").alias("PDBC_TYPE"),
        F.col("lnk_NonPcagProv_in.Dummy").alias("Dummy")
    )
)

# xfm_NonPcagProv
df_xfm_NonPcagProv = df_join_NonPcagProv.filter(F.col("Dummy") != 1)
df_xfm_NonPcagProv = df_xfm_NonPcagProv.select(
    F.concat(F.lit("|"), F.col("PDBC_PFX"), F.lit("|")).alias("Code"),
    UpCase(F.col("PDPX_DESC")).alias("Description"),
    F.lit("FACETS").alias("Source_System"),
    F.lit("CLAIM PROCESSING CONTROL AGENT PREFIX").alias("Domain")
)

# CMC_PDPX_DESC (ODBCConnectorPX) -- SEDF
extract_query_sdef = (
    f"SELECT DISTINCT PDBC_PFX, PDPX_DESC, PDBC_TYPE "
    f"FROM {FacetsOwner}.CMC_PDPX_DESC WHERE PDBC_TYPE IN ('SEDF')"
)
df_CMC_SDEF_DESC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_sdef)
    .load()
)

# xfm_trim_SDEF_PFX
df_xfm_trim_SDEF_PFX = df_CMC_SDEF_DESC.filter(
    F.length(trim(F.col("PDBC_PFX"))) > 0
).select(
    UpCase(trim(F.col("PDBC_PFX"))).alias("SRC_LKUP_VAL"),
    F.col("PDPX_DESC").alias("PDPX_DESC"),
    F.col("PDBC_TYPE").alias("PDBC_TYPE"),
    F.lit("CLAIM SERVICE DEFINITION PREFIX").alias("SRC_DMN_NM")
)

# join_NonSdefProv
df_join_NonSdefProv = (
    df_xfm_trim_SDEF_PFX.alias("lnk_xfm_trm_SDEF_PFX_out")
    .join(
        df_cp_CD_MPPNG_NonSdefProv_in.alias("lnk_NonSdefProv_in"),
        (
            (F.col("lnk_xfm_trm_SDEF_PFX_out.SRC_LKUP_VAL") 
             == F.col("lnk_NonSdefProv_in.SRC_LKUP_VAL"))
            & 
            (F.col("lnk_xfm_trm_SDEF_PFX_out.SRC_DMN_NM") 
             == F.col("lnk_NonSdefProv_in.SRC_DMN_NM"))
        ),
        "left"
    )
    .select(
        F.col("lnk_xfm_trm_SDEF_PFX_out.SRC_LKUP_VAL").alias("PDBC_PFX"),
        F.col("lnk_NonSdefProv_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_xfm_trm_SDEF_PFX_out.PDPX_DESC").alias("PDPX_DESC"),
        F.col("lnk_xfm_trm_SDEF_PFX_out.PDBC_TYPE").alias("PDBC_TYPE"),
        F.col("lnk_NonSdefProv_in.Dummy").alias("Dummy")
    )
)

# xfm_NonSdefProv
df_xfm_NonSdefProv = df_join_NonSdefProv.filter(F.col("Dummy") != 1)
df_xfm_NonSdefProv = df_xfm_NonSdefProv.select(
    F.concat(F.lit("|"), F.col("PDBC_PFX"), F.lit("|")).alias("Code"),
    UpCase(F.col("PDPX_DESC")).alias("Description"),
    F.lit("FACETS").alias("Source_System"),
    F.lit("CLAIM SERVICE DEFINITION PREFIX").alias("Domain")
)

# CMC_SEDS_SE_DESC (ODBCConnectorPX)
extract_query_seds_se_desc = (
    f"SELECT DISTINCT SEDS_TYPE, SEDS_DESC FROM {FacetsOwner}.CMC_SEDS_SE_DESC"
)
df_CMC_SEDS_SE_DESC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_seds_se_desc)
    .load()
)

# xfm_trim_SEDS_TYPE
df_xfm_trim_SEDS_TYPE = df_CMC_SEDS_SE_DESC.filter(
    F.length(trim(F.col("SEDS_TYPE"))) > 0
).select(
    UpCase(trim(F.col("SEDS_TYPE"))).alias("SRC_LKUP_VAL"),
    F.col("SEDS_DESC").alias("SEDS_DESC"),
    F.lit("SERVICE TYPE").alias("SRC_DMN_NM")
)

# join_Service_Type
df_join_Service_Type = (
    df_xfm_trim_SEDS_TYPE.alias("lnk_xfm_trm_SEDS_TYPE_out")
    .join(
        df_cp_CD_MPPNG_Service_Type_in.alias("lnk_Service_Type_in"),
        (
            (F.col("lnk_xfm_trm_SEDS_TYPE_out.SRC_LKUP_VAL") 
             == F.col("lnk_Service_Type_in.SRC_LKUP_VAL"))
            & 
            (F.col("lnk_xfm_trm_SEDS_TYPE_out.SRC_DMN_NM") 
             == F.col("lnk_Service_Type_in.SRC_DMN_NM"))
        ),
        "left"
    )
    .select(
        F.col("lnk_xfm_trm_SEDS_TYPE_out.SRC_LKUP_VAL").alias("SEDS_TYPE"),
        F.col("lnk_xfm_trm_SEDS_TYPE_out.SEDS_DESC").alias("SEDS_DESC"),
        F.col("lnk_Service_Type_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_Service_Type_in.Dummy").alias("Dummy")
    )
)

# xfm_Service_Type
df_xfm_Service_Type = df_join_Service_Type.filter(F.col("Dummy") != 1)
df_xfm_Service_Type = df_xfm_Service_Type.select(
    F.concat(F.lit("|"), F.col("SEDS_TYPE"), F.lit("|")).alias("Code"),
    UpCase(F.col("SEDS_DESC")).alias("Description"),
    F.lit("FACETS").alias("Source_System"),
    F.lit("SERVICE TYPE").alias("Domain")
)

# fl_New_Codes (PxFunnel)
df_fl_New_Codes = (
    df_xfm_Grp_Bill_Lvl
    .unionByName(df_xfm_Service_Type)
    .unionByName(df_xfm_NonPartProv)
    .unionByName(df_xfm_NonPcagProv)
    .unionByName(df_xfm_NonSdefProv)
)

# CDMA_Code_Diff (PxSequentialFile) - write final output
df_final = df_fl_New_Codes.select("Code", "Description", "Source_System", "Domain")

df_final = df_final.withColumn("Description", F.rpad(F.col("Description"), 80, " "))
df_final = df_final.withColumn("Domain", F.rpad(F.col("Domain"), 80, " "))

write_files(
    df_final,
    f"{adls_path}/load/processed/CDMA_Code_Diff.{RunID}.txt",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)