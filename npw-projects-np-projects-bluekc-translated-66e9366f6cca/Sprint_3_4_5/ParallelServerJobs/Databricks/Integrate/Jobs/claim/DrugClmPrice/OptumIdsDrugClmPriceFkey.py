# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                                                                                             DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                                   DESCRIPTION                                                                                                                                                    ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC Sri Nannapaneni        10-22-2019        Spread price                             Perform Foreign Key and code mapping Lookups to populate DRUG_CLM_PRICE File                                      IntegrateDev2             Sharon Andrew              11/11/2019
# MAGIC Sri Nannapaneni         11-21-2-19                                                         updated based on new mapping                                                                                                                                                              Kalyan Neelam             2019-11-26
# MAGIC Rekha Radhakrishna  2020-01-30                                                       Mapped new fields SELL_INCNTV_FEE_AMT,BUY_INCNTV_FEE_AMT                                                           IntegrateDev2            Kalyan Neelam             2020-02-06
# MAGIC                                                                                                             ,SELL_OTHR_PAYOR_AMT,SELL_OTHR_AMT and SPREAD_INCNTV_ FEE_AMT
# MAGIC 
# MAGIC Rekha Radhakrishna    2020-02-10    6131- PBM Replacement         Mapped fields  CLT2PSTAX and CLT2FSTAX to calculate BUY_SLS_TAX_AMT                                               IntegrateDev2            Kalyan Neelam             2020-02-10     
# MAGIC                                                                                                              and changed SPREAD_SLS_TAX_AMT calculation
# MAGIC 
# MAGIC Velmani Kondappan     2020-04-02   6131- PBM Replacement           Removed the errror warnings coming from Lkup_Fkey lookup                                                                                                                  Kalyan Neelam            2020-04-09
# MAGIC 
# MAGIC Rekha Radhakrishna   2020-10-14     6131 - PBM Replacement         parameterized input source file                                                                                                                         IntegrateDev2                   Sravya Gorla	2020-12-09
# MAGIC Geetanjali Rajendran    2021-06-03        PBM PhaseII                              Perform code mapping lookup to populate DRUG_TYPE_CD_SK and FRMLRY lookup          IntegrateDev2		Abhiram Dasarathy	2021-06-23
# MAGIC                                                                                                                                    to populate FRMLRY_SK 
# MAGIC 
# MAGIC Arpitha V                      2023-11-07       US 600305                              Perform code mapping lookup to populate PLN_DRUG_STTUS_CD_SK                                                        IntegrateDevB               Jeyaprasanna           2024-01-03
# MAGIC 
# MAGIC Ashok kumar B            2024-02-01        US 608682                        Added  PLAN_TYPE  to the DrugClmPrice Landing file                                                                                        IntegrateDev2                 Jeyaprasanna            2024-03-14

# MAGIC OptumIdsDrugClmPriceFkey
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    IntegerType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
UWSOwner = get_widget_value("UWSOwner","")
uws_secret_name = get_widget_value("uws_secret_name","")
SrcSysCd = get_widget_value("SrcSysCd","")
RunID = get_widget_value("RunID","")
IDSRunCycle = get_widget_value("IDSRunCycle","")
CurrentDate = get_widget_value("CurrentDate","")

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)

# ------------------------------------------------------------------------------------------------
# db2_ds_FRMLRY (DB2ConnectorPX) => Read from IDS
# ------------------------------------------------------------------------------------------------
query_db2_ds_FRMLRY = f"SELECT DISTINCT FRMLRY_ID, FRMLRY_SK FROM {IDSOwner}.FRMLRY"
df_db2_ds_FRMLRY = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_ds_FRMLRY)
    .load()
)

# ------------------------------------------------------------------------------------------------
# db2_ds_DRUG_CLM (DB2ConnectorPX) => Read from IDS, removing the WHERE referencing ORCHESTRATE
# for subsequent join in Lkup_DrugClm
# ------------------------------------------------------------------------------------------------
query_db2_ds_DRUG_CLM = (
    f"SELECT DRUG_CLM_SK, "
    f"SRC_SYS_CD_SK, "
    f"CLM_ID as CLM_ID2, "
    f"CRT_RUN_CYC_EXCTN_SK, "
    f"LAST_UPDT_RUN_CYC_EXCTN_SK, "
    f"DISPNS_FEE_AMT, "
    f"INGR_CST_CHRGD_AMT, "
    f"GNRC_DRUG_IN, "
    f"SLS_TAX_AMT "
    f"FROM {IDSOwner}.DRUG_CLM"
)
df_db2_ds_DRUG_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_ds_DRUG_CLM)
    .load()
)

# ------------------------------------------------------------------------------------------------
# db2_CD_MPPNG_SK (DB2ConnectorPX) => Read from IDS
# ------------------------------------------------------------------------------------------------
query_db2_CD_MPPNG_SK = (
    f"SELECT DISTINCT "
    f"CD_MPPNG_SK, "
    f"SRC_CD, "
    f"SRC_CD_NM, "
    f"SRC_CLCTN_CD, "
    f"SRC_DRVD_LKUP_VAL, "
    f"SRC_DOMAIN_NM, "
    f"SRC_SYS_CD, "
    f"TRGT_CD, "
    f"TRGT_CD_NM, "
    f"TRGT_CLCTN_CD, "
    f"TRGT_DOMAIN_NM "
    f"FROM {IDSOwner}.CD_MPPNG"
)
df_db2_CD_MPPNG_SK = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_CD_MPPNG_SK)
    .load()
)

# ------------------------------------------------------------------------------------------------
# fltr_Data (PxFilter) => produces 9 outputs
# ------------------------------------------------------------------------------------------------
df_Lnk_SRC_SYS_CD_SK = df_db2_CD_MPPNG_SK.filter(
    (F.col("SRC_SYS_CD") == SrcSysCd)
    & (F.col("SRC_DOMAIN_NM") == "SOURCE SYSTEM")
    & (F.col("SRC_CLCTN_CD") == SrcSysCd)
    & (F.col("TRGT_DOMAIN_NM") == "SOURCE SYSTEM")
    & (F.col("TRGT_CLCTN_CD") == "IDS")
).select(
    F.col("CD_MPPNG_SK"),
    F.col("SRC_SYS_CD")
)

df_Lnk_BUY_CST_SRC_CD_SK = df_db2_CD_MPPNG_SK.filter(
    (F.col("SRC_SYS_CD") == SrcSysCd)
    & (F.col("SRC_DOMAIN_NM") == "BUY COST SOURCE")
    & (F.col("SRC_CLCTN_CD") == SrcSysCd)
    & (F.col("TRGT_DOMAIN_NM") == "BUY COST SOURCE")
    & (F.col("TRGT_CLCTN_CD") == "IDS")
).select(
    F.col("CD_MPPNG_SK"),
    F.col("SRC_CD")
)

df_Lnk_BUY_CST_TYP_CD_SK = df_db2_CD_MPPNG_SK.filter(
    (F.col("SRC_SYS_CD") == SrcSysCd)
    & (F.col("SRC_DOMAIN_NM") == "DRUG COST TYPE")
    & (F.col("SRC_CLCTN_CD") == SrcSysCd)
    & (F.col("TRGT_DOMAIN_NM") == "DRUG COST TYPE")
    & (F.col("TRGT_CLCTN_CD") == "IDS")
).select(
    F.col("CD_MPPNG_SK"),
    F.col("SRC_CD")
)

df_Lnk_BUY_PRICE_TYP_CD_SK = df_db2_CD_MPPNG_SK.filter(
    (F.col("SRC_SYS_CD") == SrcSysCd)
    & (F.col("SRC_DOMAIN_NM") == "DRUG PRICE TYPE")
    & (F.col("SRC_CLCTN_CD") == SrcSysCd)
    & (F.col("TRGT_DOMAIN_NM") == "DRUG PRICE TYPE")
    & (F.col("TRGT_CLCTN_CD") == "IDS")
).select(
    F.col("CD_MPPNG_SK"),
    F.col("SRC_CD")
)

df_Lnk_CST_TYP_CD_SK = df_db2_CD_MPPNG_SK.filter(
    (F.col("SRC_SYS_CD") == SrcSysCd)
    & (F.col("SRC_DOMAIN_NM") == "DRUG COST TYPE")
    & (F.col("SRC_CLCTN_CD") == SrcSysCd)
    & (F.col("TRGT_DOMAIN_NM") == "DRUG COST TYPE")
    & (F.col("TRGT_CLCTN_CD") == "IDS")
).select(
    F.col("CD_MPPNG_SK"),
    F.col("SRC_CD")
)

df_Lnk_GNRC_OVRD_CD_SK = df_db2_CD_MPPNG_SK.filter(
    (F.col("SRC_SYS_CD") == SrcSysCd)
    & (F.col("SRC_DOMAIN_NM") == "DRUG GENERIC OVERRIDE")
    & (F.col("SRC_CLCTN_CD") == SrcSysCd)
    & (F.col("TRGT_DOMAIN_NM") == "DRUG GENERIC OVERRIDE")
    & (F.col("TRGT_CLCTN_CD") == "IDS")
).select(
    F.col("CD_MPPNG_SK"),
    F.col("SRC_CD")
)

df_Lnk_PDX_NTWK_QLFR_CD_SK = df_db2_CD_MPPNG_SK.filter(
    (F.col("SRC_SYS_CD") == SrcSysCd)
    & (F.col("SRC_DOMAIN_NM") == "PHARMACY NETWORK QUALIFIER")
    & (F.col("SRC_CLCTN_CD") == SrcSysCd)
    & (F.col("TRGT_DOMAIN_NM") == "PHARMACY NETWORK QUALIFIER")
    & (F.col("TRGT_CLCTN_CD") == "IDS")
).select(
    F.col("CD_MPPNG_SK"),
    F.col("SRC_CD")
)

df_Lnk_DRUG_TYP_CD_SK = df_db2_CD_MPPNG_SK.filter(
    (F.col("SRC_SYS_CD") == SrcSysCd)
    & (F.col("SRC_DOMAIN_NM") == "DRUG TYPE")
    & (F.col("SRC_CLCTN_CD") == SrcSysCd)
    & (F.col("TRGT_DOMAIN_NM") == "DRUG TYPE")
    & (F.col("TRGT_CLCTN_CD") == "IDS")
).select(
    F.col("CD_MPPNG_SK"),
    F.col("SRC_CD")
)

df_Lnk_PLN_DRUG_STTUS_CD = df_db2_CD_MPPNG_SK.filter(
    (F.col("SRC_SYS_CD") == SrcSysCd)
    & (F.col("SRC_DOMAIN_NM") == "PLAN DRUG STATUS")
    & (F.col("SRC_CLCTN_CD") == SrcSysCd)
    & (F.col("TRGT_DOMAIN_NM") == "PLAN DRUG STATUS")
    & (F.col("TRGT_CLCTN_CD") == "IDS")
).select(
    F.col("SRC_CD"),
    F.col("CD_MPPNG_SK")
)

# ------------------------------------------------------------------------------------------------
# Seq_W_DRUG_CLM_PRICE (PxSequentialFile) => Read delimited file
# ------------------------------------------------------------------------------------------------
schema_Seq_W_DRUG_CLM_PRICE = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), True),
    StructField("RXCLMNBR", StringType(), True),
    StructField("CLMSEQNBR", StringType(), True),
    StructField("CLAIMSTS", StringType(), True),
    StructField("ORGPDSBMDT", StringType(), True),
    StructField("METRICQTY", StringType(), True),
    StructField("GPINUMBER", StringType(), True),
    StructField("GENINDOVER", StringType(), True),
    StructField("CPQSPCPRG", StringType(), True),
    StructField("CPQSPCPGIN", StringType(), True),
    StructField("FNLPLANCDE", StringType(), True),
    StructField("FNLPLANDTE", StringType(), True),
    StructField("FORMLRFLAG", StringType(), True),
    StructField("AWPUNITCST", StringType(), True),
    StructField("WACUNITCST", StringType(), True),
    StructField("CTYPEUCOST", StringType(), True),
    StructField("PROQTY", StringType(), True),
    StructField("CLTCOSTTYP", StringType(), True),
    StructField("CLTPRCTYPE", StringType(), True),
    StructField("CLTRATE", StringType(), True),
    StructField("CLTTYPUCST", StringType(), True),
    StructField("CLT2INGRCST", StringType(), True),
    StructField("CLT2DISPFEE", StringType(), True),
    StructField("CLT2COSTSRC", StringType(), True),
    StructField("CLT2COSTTYP", StringType(), True),
    StructField("CLT2PRCTYPE", StringType(), True),
    StructField("CLT2RATE", StringType(), True),
    StructField("RXNETWRKQL", StringType(), True),
    StructField("CLT2SLSTAX", StringType(), True),
    StructField("CLT2PSTAX", StringType(), True),
    StructField("CLT2FSTAX", StringType(), True),
    StructField("CLT2DUEAMT", StringType(), False),
    StructField("CLT2OTHAMT", StringType(), True),
    StructField("CLTINCENTV", StringType(), True),
    StructField("CALINCENTV2", StringType(), True),
    StructField("CLTOTHPAYA", StringType(), True),
    StructField("CLTTOTHAMT", StringType(), True),
    StructField("CLNTDEF2", StringType(), True),
    StructField("TIER_ID", StringType(), True),
    StructField("DRUG_TYP_CD", StringType(), True),
    StructField("PLN_DRUG_STTUS_CD", StringType(), True),
    StructField("PLANTYPE", StringType(), True)
])

file_path_Seq_W_DRUG_CLM_PRICE = f"{adls_path_raw}/landing/DrugClmPrice_Land_{SrcSysCd}.dat.{RunID}"
df_Seq_W_DRUG_CLM_PRICE = (
    spark.read.format("csv")
    .option("header", True)
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_Seq_W_DRUG_CLM_PRICE)
    .load(file_path_Seq_W_DRUG_CLM_PRICE)
)

# ------------------------------------------------------------------------------------------------
# xfm_DrugClmPriceSrc (CTransformerStage)
# ------------------------------------------------------------------------------------------------
df_xfm_DrugClmPriceSrc_stagevars = (
    df_Seq_W_DRUG_CLM_PRICE
    .withColumn(
        "StageVar",
        F.regexp_replace(F.col("ORGPDSBMDT"), r'^(0+)', '')
    )
    .withColumn(
        "SVClmId",
        F.when(
            (F.trim(F.col("CLAIMSTS")) == "X"),
            F.trim(F.col("RXCLMNBR")) + F.trim(F.col("CLMSEQNBR")) + F.lit("R")
        ).otherwise(
            F.trim(F.col("RXCLMNBR")) + F.trim(F.col("CLMSEQNBR"))
        )
    )
)

df_xfm_src_results = (
    df_xfm_DrugClmPriceSrc_stagevars
    .withColumn("SRC_SYS_CD", F.col("SRC_SYS_CD"))
    .withColumn("SRC_SYS_CD_SK", F.col("SRC_SYS_CD_SK"))
    .withColumn("CLM_ID", F.col("CLM_ID"))
    .withColumn("RXCLMNBR", F.trim(F.col("RXCLMNBR")))
    .withColumn("CLMSEQNBR", F.trim(F.col("CLMSEQNBR")))
    .withColumn("CLAIMSTS", F.trim(F.col("CLAIMSTS")))
    .withColumn(
        "ORGPDSBMDT",
        F.when(
            (F.length(F.trim(F.col("ORGPDSBMDT"))) == 0)
            | (F.trim(F.col("ORGPDSBMDT")) == "0")
            | (F.col("ORGPDSBMDT").isNull()),
            F.lit("1753-01-01")
        ).otherwise(F.trim(F.col("ORGPDSBMDT")))
    )
    .withColumn("METRICQTY", F.trim(F.col("METRICQTY")))
    .withColumn("GPINUMBER", F.trim(F.col("GPINUMBER")))
    .withColumn("GENINDOVER", F.trim(F.col("GENINDOVER")))
    .withColumn("CPQSPCPRG", F.trim(F.col("CPQSPCPRG")))
    .withColumn("CPQSPCPGIN", F.trim(F.col("CPQSPCPGIN")))
    .withColumn("FNLPLANCDE", F.trim(F.col("FNLPLANCDE")))
    .withColumn(
        "FNLPLANDTE",
        F.when(
            (F.length(F.trim(F.col("FNLPLANDTE"))) == 0)
            | (F.trim(F.col("FNLPLANDTE")) == "0"),
            F.lit("1753-01-01")
        ).otherwise(F.trim(F.col("FNLPLANDTE")))
    )
    .withColumn("FORMLRFLAG", F.trim(F.col("FORMLRFLAG")))
    .withColumn("AWPUNITCST", F.trim(F.col("AWPUNITCST")))
    .withColumn("WACUNITCST", F.trim(F.col("WACUNITCST")))
    .withColumn("CTYPEUCOST", F.trim(F.col("CTYPEUCOST")))
    .withColumn("PROQTY", F.trim(F.col("PROQTY")))
    .withColumn("CLTCOSTTYP", trim(F.col("CLTCOSTTYP")))
    .withColumn("CLTPRCTYPE", F.trim(F.col("CLTPRCTYPE")))
    .withColumn("CLTRATE", F.trim(F.col("CLTRATE")))
    .withColumn("CLTTYPUCST", F.trim(F.col("CLTTYPUCST")))
    .withColumn("CLT2INGRCST", F.trim(F.col("CLT2INGRCST")))
    .withColumn("CLT2DISPFEE", F.trim(F.col("CLT2DISPFEE")))
    .withColumn("CLT2COSTSRC", F.trim(F.col("CLT2COSTSRC")))
    .withColumn("CLT2COSTTYP", F.trim(F.col("CLT2COSTTYP")))
    .withColumn("CLT2PRCTYPE", F.trim(F.col("CLT2PRCTYPE")))
    .withColumn("CLT2RATE", F.trim(F.col("CLT2RATE")))
    .withColumn("RXNETWRKQL", trim(F.col("RXNETWRKQL")))
    .withColumn("CLT2SLSTAX", F.col("CLT2SLSTAX").cast(DecimalType(38,10)))
    .withColumn("CLT2PSTAX", F.col("CLT2PSTAX").cast(DecimalType(38,10)))
    .withColumn("CLT2FSTAX", F.col("CLT2FSTAX").cast(DecimalType(38,10)))
    .withColumn("CLT2DUEAMT", F.col("CLT2DUEAMT").cast(DecimalType(38,10)))
    .withColumn("CLT2OTHAMT", F.col("CLT2OTHAMT").cast(DecimalType(38,10)))
    .withColumn("CLTINCENTV", F.col("CLTINCENTV").cast(DecimalType(38,10)))
    .withColumn("CALINCENTV2", F.col("CALINCENTV2").cast(DecimalType(38,10)))
    .withColumn("CLTOTHPAYA", F.col("CLTOTHPAYA").cast(DecimalType(38,10)))
    .withColumn("CLTOTHAMT", F.col("CLTTOTHAMT").cast(DecimalType(38,10)))
    .withColumn("CLNTDEF2", F.col("CLNTDEF2"))
    .withColumn("TIER_ID", F.col("TIER_ID"))
    .withColumn("DRUG_TYP_CD", F.col("DRUG_TYP_CD"))
    .withColumn("PLN_DRUG_STTUS_CD", F.col("PLN_DRUG_STTUS_CD"))
    .withColumn("PLANTYPE", F.col("PLANTYPE"))
)

# ------------------------------------------------------------------------------------------------
# Lkup_DrugClm (PxLookup) => primary link df_xfm_src_results, lookup link df_db2_ds_DRUG_CLM (inner join)
# ------------------------------------------------------------------------------------------------
df_Lkup_DrugClm = (
    df_xfm_src_results.alias("p")
    .join(
        df_db2_ds_DRUG_CLM.alias("l"),
        (
            (F.col("p.CLM_ID") == F.col("l.CLM_ID2")) &
            (F.col("p.SRC_SYS_CD_SK") == F.col("l.SRC_SYS_CD_SK"))
        ),
        "inner"
    )
    .select(
        F.col("l.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
        F.col("l.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("p.CLM_ID").alias("CLM_ID"),
        F.col("p.RXCLMNBR").alias("RXCLMNBR"),
        F.col("p.CLMSEQNBR").alias("CLMSEQNBR"),
        F.col("p.CLAIMSTS").alias("CLAIMSTS"),
        F.col("p.ORGPDSBMDT").alias("ORGPDSBMDT"),
        F.col("p.METRICQTY").alias("METRICQTY"),
        F.col("p.GPINUMBER").alias("GPINUMBER"),
        F.col("p.GENINDOVER").alias("GENINDOVER"),
        F.col("p.CPQSPCPRG").alias("CPQSPCPRG"),
        F.col("p.CPQSPCPGIN").alias("CPQSPCPGIN"),
        F.col("p.FNLPLANCDE").alias("FNLPLANCDE"),
        F.col("p.FNLPLANDTE").alias("FNLPLANDTE"),
        F.col("p.FORMLRFLAG").alias("FORMLRFLAG"),
        F.col("p.AWPUNITCST").alias("AWPUNITCST"),
        F.col("p.WACUNITCST").alias("WACUNITCST"),
        F.col("p.CTYPEUCOST").alias("CTYPEUCOST"),
        F.col("p.PROQTY").alias("PROQTY"),
        F.col("p.CLTCOSTTYP").alias("CLTCOSTTYP"),
        F.col("p.CLTPRCTYPE").alias("CLTPRCTYPE"),
        F.col("p.CLTRATE").alias("CLTRATE"),
        F.col("p.CLTTYPUCST").alias("CLTTYPUCST"),
        F.col("p.CLT2INGRCST").alias("CLT2INGRCST"),
        F.col("p.CLT2DISPFEE").alias("CLT2DISPFEE"),
        F.col("p.CLT2COSTSRC").alias("CLT2COSTSRC"),
        F.col("p.CLT2COSTTYP").alias("CLT2COSTTYP"),
        F.col("p.CLT2PRCTYPE").alias("CLT2PRCTYPE"),
        F.col("p.CLT2RATE").alias("CLT2RATE"),
        F.col("p.RXNETWRKQL").alias("RXNETWRKQL"),
        F.col("p.CLT2SLSTAX").alias("CLT2SLSTAX"),
        F.col("p.CLT2PSTAX").alias("CLT2PSTAX"),
        F.col("p.CLT2FSTAX").alias("CLT2FSTAX"),
        F.col("p.CLT2DUEAMT").alias("CLT2DUEAMT"),
        F.col("p.CLT2OTHAMT").alias("CLT2OTHAMT"),
        F.col("l.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("l.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("l.DISPNS_FEE_AMT").alias("DISPNS_FEE_AMT"),
        F.col("l.INGR_CST_CHRGD_AMT").alias("INGR_CST_CHRGD_AMT"),
        F.col("l.GNRC_DRUG_IN").alias("GNRC_DRUG_IN"),
        F.col("l.SLS_TAX_AMT").alias("SLS_TAX_AMT"),
        F.col("p.CLTINCENTV").alias("CLTINCENTV"),
        F.col("p.CALINCENTV2").alias("CALINCENTV2"),
        F.col("p.CLTOTHPAYA").alias("CLTOTHPAYA"),
        F.col("p.CLTOTHAMT").alias("CLTOTHAMT"),
        F.col("p.CLNTDEF2").alias("CLNTDEF2"),
        F.col("p.TIER_ID").alias("TIER_ID"),
        F.col("p.DRUG_TYP_CD").alias("DRUG_TYP_CD"),
        F.col("p.PLN_DRUG_STTUS_CD").alias("PLN_DRUG_STTUS_CD"),
        F.col("p.PLANTYPE").alias("PLANTYPE")
    )
)

# ------------------------------------------------------------------------------------------------
# SRC_DOMAIN_TRNSLTN (ODBCConnectorPX) => Read from UWS
# ------------------------------------------------------------------------------------------------
query_SRC_DOMAIN_TRNSLTN = (
    f"SELECT SRC_DOMAIN_TX, TRGT_DOMAIN_TX "
    f"FROM {UWSOwner}.SRC_DOMAIN_TRNSLTN "
    f"WHERE EFF_DT_SK <= '{CurrentDate}' "
    f"AND TERM_DT_SK >= '{CurrentDate}' "
    f"AND SRC_SYS_CD = '{SrcSysCd}' "
    f"AND DOMAIN_ID = 'PLAN_DRUG_ST'"
)
df_SRC_DOMAIN_TRNSLTN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("query", query_SRC_DOMAIN_TRNSLTN)
    .load()
)

# ------------------------------------------------------------------------------------------------
# xfm_Src_Domain_Trnsltn (CTransformerStage)
# ------------------------------------------------------------------------------------------------
df_xfm_Src_Domain_Trnsltn = df_SRC_DOMAIN_TRNSLTN.select(
    F.col("SRC_DOMAIN_TX").alias("SRC_DOMAIN_TX"),
    F.col("TRGT_DOMAIN_TX").alias("TRGT_DOMAIN_TX")
)

# ------------------------------------------------------------------------------------------------
# Lkp_SrcDomain (PxLookup):
#   Primary link => df_xfm_Src_Domain_Trnsltn
#   Lookup link => df_Lnk_PLN_DRUG_STTUS_CD (left join)
#   Condition => TRGT_DOMAIN_TX = SRC_CD
# ------------------------------------------------------------------------------------------------
df_Lkp_SrcDomain = (
    df_xfm_Src_Domain_Trnsltn.alias("p")
    .join(
        df_Lnk_PLN_DRUG_STTUS_CD.alias("l"),
        F.col("p.TRGT_DOMAIN_TX") == F.col("l.SRC_CD"),
        "left"
    )
    .select(
        F.col("l.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
        F.col("p.SRC_DOMAIN_TX").alias("SRC_DOMAIN_TX")
    )
)

# ------------------------------------------------------------------------------------------------
# Lkup_Fkey (PxLookup) => primary link = df_Lkup_DrugClm, multiple left lookups from fltr_Data + Lkp_SrcDomain
# ------------------------------------------------------------------------------------------------
df_Lkup_Fkey = (
    df_Lkup_DrugClm.alias("p")
    .join(df_Lnk_SRC_SYS_CD_SK.alias("s0"), F.col("p.SRC_SYS_CD_SK") == F.col("s0.CD_MPPNG_SK"), "left")
    .join(df_Lnk_BUY_CST_SRC_CD_SK.alias("s1"), F.col("p.CLT2COSTSRC") == F.col("s1.SRC_CD"), "left")
    .join(df_Lnk_BUY_CST_TYP_CD_SK.alias("s2"), F.col("p.CLT2COSTTYP") == F.col("s2.SRC_CD"), "left")
    .join(df_Lnk_BUY_PRICE_TYP_CD_SK.alias("s3"), F.col("p.CLT2PRCTYPE") == F.col("s3.SRC_CD"), "left")
    .join(df_Lnk_CST_TYP_CD_SK.alias("s4"), F.col("p.CLTCOSTTYP") == F.col("s4.SRC_CD"), "left")
    .join(df_Lnk_GNRC_OVRD_CD_SK.alias("s5"), F.col("p.GENINDOVER") == F.col("s5.SRC_CD"), "left")
    .join(df_Lnk_PDX_NTWK_QLFR_CD_SK.alias("s6"), F.col("p.RXNETWRKQL") == F.col("s6.SRC_CD"), "left")
    .join(df_Lnk_DRUG_TYP_CD_SK.alias("s7"), F.col("p.DRUG_TYP_CD") == F.col("s7.SRC_CD"), "left")
    .join(df_Lkp_SrcDomain.alias("s8"), F.col("p.PLN_DRUG_STTUS_CD") == F.col("s8.SRC_DOMAIN_TX"), "left")
    .select(
        F.col("p.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
        F.col("p.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("p.CLM_ID").alias("CLM_ID"),
        F.col("s1.CD_MPPNG_SK").alias("BUY_CST_SRC_CD_SK"),
        F.col("s2.CD_MPPNG_SK").alias("BUY_CST_TYP_CD_SK"),
        F.col("s3.CD_MPPNG_SK").alias("BUY_PRICE_TYP_CD_SK"),
        F.col("s4.CD_MPPNG_SK").alias("CST_TYP_CD_SK"),
        F.col("s5.CD_MPPNG_SK").alias("GNRC_OVRD_CD_SK"),
        F.col("s6.CD_MPPNG_SK").alias("PDX_NTWK_QLFR_CD_SK"),
        F.col("p.RXCLMNBR").alias("RXCLMNBR"),
        F.col("p.CLMSEQNBR").alias("CLMSEQNBR"),
        F.col("p.CLAIMSTS").alias("CLAIMSTS"),
        F.col("p.ORGPDSBMDT").alias("ORGPDSBMDT"),
        F.col("p.METRICQTY").alias("METRICQTY"),
        F.col("p.GPINUMBER").alias("GPINUMBER"),
        F.col("p.GENINDOVER").alias("GENINDOVER"),
        F.col("p.CPQSPCPRG").alias("CPQSPCPRG"),
        F.col("p.CPQSPCPGIN").alias("CPQSPCPGIN"),
        F.col("p.FNLPLANCDE").alias("FNLPLANCDE"),
        F.col("p.FNLPLANDTE").alias("FNLPLANDTE"),
        F.col("p.FORMLRFLAG").alias("FORMLRFLAG"),
        F.col("p.AWPUNITCST").alias("AWPUNITCST"),
        F.col("p.WACUNITCST").alias("WACUNITCST"),
        F.col("p.CTYPEUCOST").alias("CTYPEUCOST"),
        F.col("p.PROQTY").alias("PROQTY"),
        F.col("p.CLTCOSTTYP").alias("CLTCOSTTYP"),
        F.col("p.CLTPRCTYPE").alias("CLTPRCTYPE"),
        F.col("p.CLTRATE").alias("CLTRATE"),
        F.col("p.CLTTYPUCST").alias("CLTTYPUCST"),
        F.col("p.CLT2INGRCST").alias("CLT2INGRCST"),
        F.col("p.CLT2DISPFEE").alias("CLT2DISPFEE"),
        F.col("p.CLT2COSTSRC").alias("CLT2COSTSRC"),
        F.col("p.CLT2COSTTYP").alias("CLT2COSTTYP"),
        F.col("p.CLT2PRCTYPE").alias("CLT2PRCTYPE"),
        F.col("p.CLT2RATE").alias("CLT2RATE"),
        F.col("p.RXNETWRKQL").alias("RXNETWRKQL"),
        F.col("p.CLT2SLSTAX").alias("CLT2SLSTAX"),
        F.col("p.CLT2PSTAX").alias("CLT2PSTAX"),
        F.col("p.CLT2FSTAX").alias("CLT2FSTAX"),
        F.col("p.CLT2DUEAMT").alias("CLT2DUEAMT"),
        F.col("p.CLT2OTHAMT").alias("CLT2OTHAMT"),
        F.col("p.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("p.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("p.DISPNS_FEE_AMT").alias("DISPNS_FEE_AMT"),
        F.col("p.INGR_CST_CHRGD_AMT").alias("INGR_CST_CHRGD_AMT"),
        F.col("p.GNRC_DRUG_IN").alias("GNRC_DRUG_IN"),
        F.col("p.SLS_TAX_AMT").alias("SLS_TAX_AMT"),
        F.col("p.CLTINCENTV").alias("CLTINCENTV"),
        F.col("p.CALINCENTV2").alias("CALINCENTV2"),
        F.col("p.CLTOTHPAYA").alias("CLTOTHPAYA"),
        F.col("p.CLTOTHAMT").alias("CLTOTHAMT"),
        F.col("p.CLNTDEF2").alias("CLNTDEF2"),
        F.col("p.TIER_ID").alias("TIER_ID"),
        F.col("s7.CD_MPPNG_SK").alias("DRUG_TYP_CD_SK"),
        F.col("s8.CD_MPPNG_SK").alias("PLN_DRUG_STTUS_CD_SK"),
        F.col("p.PLANTYPE").alias("PLANTYPE")
    )
)

# ------------------------------------------------------------------------------------------------
# Lkup_Frmly (PxLookup) => primary link = df_Lkup_Fkey, lookup link = df_db2_ds_FRMLRY (left join)
#   condition => CLNTDEF2 = FRMLRY_ID
# ------------------------------------------------------------------------------------------------
df_Lkup_Frmly = (
    df_Lkup_Fkey.alias("p")
    .join(
        df_db2_ds_FRMLRY.alias("l"),
        F.col("p.CLNTDEF2") == F.col("l.FRMLRY_ID"),
        "left"
    )
    .select(
        F.col("p.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
        F.col("p.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("p.CLM_ID").alias("CLM_ID"),
        F.col("p.BUY_CST_SRC_CD_SK").alias("BUY_CST_SRC_CD_SK"),
        F.col("p.BUY_CST_TYP_CD_SK").alias("BUY_CST_TYP_CD_SK"),
        F.col("p.BUY_PRICE_TYP_CD_SK").alias("BUY_PRICE_TYP_CD_SK"),
        F.col("p.CST_TYP_CD_SK").alias("CST_TYP_CD_SK"),
        F.col("p.GNRC_OVRD_CD_SK").alias("GNRC_OVRD_CD_SK"),
        F.col("p.PDX_NTWK_QLFR_CD_SK").alias("PDX_NTWK_QLFR_CD_SK"),
        F.col("p.RXCLMNBR").alias("RXCLMNBR"),
        F.col("p.CLMSEQNBR").alias("CLMSEQNBR"),
        F.col("p.CLAIMSTS").alias("CLAIMSTS"),
        F.col("p.ORGPDSBMDT").alias("ORGPDSBMDT"),
        F.col("p.METRICQTY").alias("METRICQTY"),
        F.col("p.GPINUMBER").alias("GPINUMBER"),
        F.col("p.GENINDOVER").alias("GENINDOVER"),
        F.col("p.CPQSPCPRG").alias("CPQSPCPRG"),
        F.col("p.CPQSPCPGIN").alias("CPQSPCPGIN"),
        F.col("p.FNLPLANCDE").alias("FNLPLANCDE"),
        F.col("p.FNLPLANDTE").alias("FNLPLANDTE"),
        F.col("p.FORMLRFLAG").alias("FORMLRFLAG"),
        F.col("p.AWPUNITCST").alias("AWPUNITCST"),
        F.col("p.WACUNITCST").alias("WACUNITCST"),
        F.col("p.CTYPEUCOST").alias("CTYPEUCOST"),
        F.col("p.PROQTY").alias("PROQTY"),
        F.col("p.CLTCOSTTYP").alias("CLTCOSTTYP"),
        F.col("p.CLTPRCTYPE").alias("CLTPRCTYPE"),
        F.col("p.CLTRATE").alias("CLTRATE"),
        F.col("p.CLTTYPUCST").alias("CLTTYPUCST"),
        F.col("p.CLT2INGRCST").alias("CLT2INGRCST"),
        F.col("p.CLT2DISPFEE").alias("CLT2DISPFEE"),
        F.col("p.CLT2COSTSRC").alias("CLT2COSTSRC"),
        F.col("p.CLT2COSTTYP").alias("CLT2COSTTYP"),
        F.col("p.CLT2PRCTYPE").alias("CLT2PRCTYPE"),
        F.col("p.CLT2RATE").alias("CLT2RATE"),
        F.col("p.RXNETWRKQL").alias("RXNETWRKQL"),
        F.col("p.CLT2SLSTAX").alias("CLT2SLSTAX"),
        F.col("p.CLT2PSTAX").alias("CLT2PSTAX"),
        F.col("p.CLT2FSTAX").alias("CLT2FSTAX"),
        F.col("p.CLT2DUEAMT").alias("CLT2DUEAMT"),
        F.col("p.CLT2OTHAMT").alias("CLT2OTHAMT"),
        F.col("p.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("p.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("p.DISPNS_FEE_AMT").alias("DISPNS_FEE_AMT"),
        F.col("p.INGR_CST_CHRGD_AMT").alias("INGR_CST_CHRGD_AMT"),
        F.col("p.GNRC_DRUG_IN").alias("GNRC_DRUG_IN"),
        F.col("p.SLS_TAX_AMT").alias("SLS_TAX_AMT"),
        F.col("p.CLTINCENTV").alias("CLTINCENTV"),
        F.col("p.CALINCENTV2").alias("CALINCENTV2"),
        F.col("p.CLTOTHPAYA").alias("CLTOTHPAYA"),
        F.col("p.CLTOTHAMT").alias("CLTOTHAMT"),
        F.col("l.FRMLRY_SK").alias("FRMLRY_SK"),
        F.col("p.TIER_ID").alias("TIER_ID"),
        F.col("p.DRUG_TYP_CD_SK").alias("DRUG_TYP_CD_SK"),
        F.col("p.PLN_DRUG_STTUS_CD_SK").alias("PLN_DRUG_STTUS_CD_SK"),
        F.col("p.PLANTYPE").alias("PLANTYPE")
    )
)

# ------------------------------------------------------------------------------------------------
# xfm_CheckLkpResults (CTransformerStage)
# ------------------------------------------------------------------------------------------------
df_xfm_CheckLkpResults_stagevars = (
    df_Lkup_Frmly
    .withColumn(
        "SVDate1",
        F.regexp_replace(F.col("FNLPLANDTE"), "-", "")
    )
    .withColumn(
        "SVDate2",
        F.substring(F.col("SVDate1"), 1, 1)
    )
    .withColumn(
        "SVDate3",
        F.when(
            F.col("SVDate2") == "1",
            F.lit("20") + F.substring(F.col("SVDate1"), 2, 6)
        ).otherwise(
            F.lit("19") + F.substring(F.col("SVDate1"), 2, 6)
        )
    )
    .withColumn(
        "SVDrugTypCd",
        F.when(F.trim(F.col("GNRC_DRUG_IN")) == "Y", F.lit("GENERIC")).otherwise(F.lit("BRAND"))
    )
    .withColumn(
        "SVBuySlsTaxAmt",
        (F.col("CLT2PSTAX") + F.col("CLT2FSTAX"))
    )
)

df_xfm_CheckLkpResults = (
    df_xfm_CheckLkpResults_stagevars
    .withColumn(
        "DRUG_CLM_SK",
        F.col("DRUG_CLM_SK")
    )
    .withColumn(
        "SRC_SYS_CD_SK",
        F.col("SRC_SYS_CD_SK")
    )
    .withColumn(
        "CLM_ID",
        F.trim(F.col("CLM_ID"))
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        F.lit(IDSRunCycle)
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        F.lit(IDSRunCycle)
    )
    .withColumn(
        "BUY_CST_SRC_CD_SK",
        F.col("BUY_CST_SRC_CD_SK")
    )
    .withColumn(
        "GNRC_OVRD_CD_SK",
        F.col("GNRC_OVRD_CD_SK")
    )
    .withColumn(
        "PDX_NTWK_QLFR_CD_SK",
        F.col("PDX_NTWK_QLFR_CD_SK")
    )
    .withColumn(
        "FRMLRY_PROTOCOL_IN",
        F.col("FORMLRFLAG")
    )
    .withColumn(
        "RECON_IN",
        F.lit(None).cast(StringType())
    )
    .withColumn(
        "SPEC_PGM_IN",
        F.col("CPQSPCPGIN")
    )
    .withColumn(
        "FINL_PLN_EFF_DT_SK",
        F.when(
            F.col("FNLPLANDTE") == "0000000",
            F.expr("StringToDate('1753-01-01', '%yyyy-%mm-%dd')")
        ).otherwise(
            F.expr(
                "StringToDate(SVDate3[1,4] || '-' || SVDate3[5,2] || '-' || SVDate3[7,2], '%yyyy-%mm-%dd')"
            )
        )
    )
    .withColumn(
        "ORIG_PD_TRANS_SUBMT_DT_SK",
        F.when(
            F.col("ORGPDSBMDT") == "00000000",
            F.expr("StringToDate('1753-01-01', '%yyyy-%mm-%dd')")
        ).otherwise(
            F.expr(
                "StringToDate(substring(ORGPDSBMDT,1,4) || '-' || substring(ORGPDSBMDT,5,2) || '-' || substring(ORGPDSBMDT,7,2), '%yyyy-%mm-%dd')"
            )
        )
    )
    .withColumn(
        "PRORTD_DISPNS_QTY",
        F.col("PROQTY")
    )
    .withColumn(
        "SUBMT_DISPNS_METRIC_QTY",
        F.col("METRICQTY")
    )
    .withColumn(
        "AVG_WHLSL_PRICE_UNIT_CST_AMT",
        F.col("AWPUNITCST").cast(DecimalType(38, 10))
    )
    .withColumn(
        "WHLSL_ACQSTN_CST_UNIT_CST_AMT",
        F.col("WACUNITCST").cast(DecimalType(38, 10))
    )
    .withColumn(
        "BUY_DISPNS_FEE_AMT",
        F.col("CLT2DISPFEE")
    )
    .withColumn(
        "BUY_INGR_CST_AMT",
        F.col("CLT2INGRCST")
    )
    .withColumn(
        "BUY_RATE_PCT",
        F.col("CLT2RATE")
    )
    .withColumn(
        "BUY_SLS_TAX_AMT",
        F.col("SVBuySlsTaxAmt")
    )
    .withColumn(
        "BUY_TOT_DUE_AMT",
        F.col("CLT2DUEAMT")
    )
    .withColumn(
        "BUY_TOT_OTHR_AMT",
        F.col("CLT2OTHAMT")
    )
    .withColumn(
        "CST_TYP_UNIT_CST_AMT",
        F.col("CTYPEUCOST")
    )
    .withColumn(
        "INVC_TOT_DUE_AMT",
        F.lit("0.00").cast(DecimalType(38, 10))
    )
    .withColumn(
        "SELL_CST_TYP_UNIT_CST_AMT",
        F.col("CLTTYPUCST")
    )
    .withColumn(
        "SELL_RATE_PCT",
        F.trim(F.col("CLTRATE"))
    )
    .withColumn(
        "SPREAD_DISPNS_FEE_AMT",
        F.when(
            F.col("CLT2DISPFEE").isNull(),
            F.lit(0)
        ).otherwise(
            F.when(
                F.col("CLT2DISPFEE") == 0,
                F.col("CLT2DISPFEE")
            ).otherwise(
                F.col("DISPNS_FEE_AMT") - F.col("CLT2DISPFEE")
            )
        )
    )
    .withColumn(
        "SPREAD_INGR_CST_AMT",
        F.when(
            F.col("CLT2INGRCST").isNull(),
            F.lit(0)
        ).otherwise(
            F.when(
                F.col("CLT2INGRCST") == 0,
                F.col("CLT2INGRCST")
            ).otherwise(
                F.col("INGR_CST_CHRGD_AMT") - F.col("CLT2INGRCST")
            )
        )
    )
    .withColumn(
        "SPREAD_SLS_TAX_AMT",
        F.col("SLS_TAX_AMT") - F.col("SVBuySlsTaxAmt")
    )
    .withColumn(
        "BUY_CST_TYP_ID",
        F.trim(F.col("CLT2COSTTYP"))
    )
    .withColumn(
        "BUY_PRICE_TYP_ID",
        F.trim(F.col("CLT2PRCTYPE"))
    )
    .withColumn(
        "CST_TYP_ID",
        F.trim(F.col("CLTCOSTTYP"))
    )
    .withColumn(
        "DRUG_TYP_ID",
        F.col("SVDrugTypCd")
    )
    .withColumn(
        "FINL_PLN_ID",
        F.trim(F.col("FNLPLANCDE"))
    )
    .withColumn(
        "GNRC_PROD_ID",
        F.trim(F.col("GPINUMBER"))
    )
    .withColumn(
        "SELL_PRICE_TYP_ID",
        F.trim(F.col("CLTPRCTYPE"))
    )
    .withColumn(
        "SPEC_PGM_ID",
        F.trim(F.col("CPQSPCPRG"))
    )
    .withColumn(
        "BUY_INCNTV_FEE_AMT",
        F.col("CALINCENTV2")
    )
    .withColumn(
        "SELL_INCNTV_FEE_AMT",
        F.col("CLTINCENTV")
    )
    .withColumn(
        "SPREAD_INCNTV_FEE_AMT",
        F.col("CLTINCENTV") - F.col("CALINCENTV2")
    )
    .withColumn(
        "SELL_OTHR_AMT",
        F.col("CLTOTHAMT")
    )
    .withColumn(
        "SELL_OTHR_PAYOR_AMT",
        F.col("CLTOTHPAYA")
    )
    .withColumn(
        "FRMLRY_SK",
        F.col("FRMLRY_SK")
    )
    .withColumn(
        "TIER_ID",
        F.col("TIER_ID")
    )
    .withColumn(
        "DRUG_TYP_CD_SK",
        F.col("DRUG_TYP_CD_SK")
    )
    .withColumn(
        "PLN_DRUG_STTUS_CD_SK",
        F.col("PLN_DRUG_STTUS_CD_SK")
    )
    .withColumn(
        "DRUG_PLN_TYP_ID",
        F.col("PLANTYPE")
    )
)

# ------------------------------------------------------------------------------------------------
# Seq_Drug_Clm_Price_Fkey (PxSequentialFile) => final write
# Must preserve column order exactly, rpad() for char/varchar
# ------------------------------------------------------------------------------------------------

# Based on the final columns and their types/length:
#  1) DRUG_CLM_SK (not char/varchar -> no rpad)
#  2) SRC_SYS_CD_SK (not char/varchar -> no rpad)
#  3) CLM_ID (originally varchar, no length => use rpad 256)
#  4) CRT_RUN_CYC_EXCTN_SK (no rpad)
#  5) LAST_UPDT_RUN_CYC_EXCTN_SK (no rpad)
#  6) BUY_CST_SRC_CD_SK (no rpad)
#  7) GNRC_OVRD_CD_SK (no rpad)
#  8) PDX_NTWK_QLFR_CD_SK (no rpad)
#  9) FRMLRY_PROTOCOL_IN => char(1) => rpad length=1
# 10) RECON_IN => char(1) => rpad length=1
# 11) SPEC_PGM_IN => char(1) => rpad length=1
# 12) FINL_PLN_EFF_DT_SK => char(10) => rpad length=10
# 13) ORIG_PD_TRANS_SUBMT_DT_SK => char(10) => rpad length=10
# 14) PRORTD_DISPNS_QTY (numeric -> no rpad)
# 15) SUBMT_DISPNS_METRIC_QTY (numeric -> no rpad)
# 16) AVG_WHLSL_PRICE_UNIT_CST_AMT (decimal -> no rpad)
# 17) WHLSL_ACQSTN_CST_UNIT_CST_AMT (decimal -> no rpad)
# 18) BUY_DISPNS_FEE_AMT (decimal -> no rpad)
# 19) BUY_INGR_CST_AMT (decimal -> no rpad)
# 20) BUY_RATE_PCT (decimal -> no rpad)
# 21) BUY_SLS_TAX_AMT (decimal -> no rpad)
# 22) BUY_TOT_DUE_AMT (decimal -> no rpad)
# 23) BUY_TOT_OTHR_AMT (decimal -> no rpad)
# 24) CST_TYP_UNIT_CST_AMT (decimal -> no rpad)
# 25) INVC_TOT_DUE_AMT (decimal -> no rpad)
# 26) SELL_CST_TYP_UNIT_CST_AMT (decimal -> no rpad)
# 27) SELL_RATE_PCT (varchar? -> rpad(256))
# 28) SPREAD_DISPNS_FEE_AMT (decimal -> no rpad)
# 29) SPREAD_INGR_CST_AMT (decimal -> no rpad)
# 30) SPREAD_SLS_TAX_AMT (decimal -> no rpad)
# 31) BUY_CST_TYP_ID => (varchar? -> rpad(256))
# 32) BUY_PRICE_TYP_ID => (varchar? -> rpad(256))
# 33) CST_TYP_ID => (varchar? -> rpad(256))
# 34) DRUG_TYP_ID => (varchar? -> rpad(256))
# 35) FINL_PLN_ID => (varchar? -> rpad(256))
# 36) GNRC_PROD_ID => (varchar? -> rpad(256))
# 37) SELL_PRICE_TYP_ID => (varchar? -> rpad(256))
# 38) SPEC_PGM_ID => (varchar? -> rpad(256))
# 39) BUY_INCNTV_FEE_AMT (decimal -> no rpad)
# 40) SELL_INCNTV_FEE_AMT (decimal -> no rpad)
# 41) SPREAD_INCNTV_FEE_AMT (decimal -> no rpad)
# 42) SELL_OTHR_AMT (decimal -> no rpad)
# 43) SELL_OTHR_PAYOR_AMT (decimal -> no rpad)
# 44) FRMLRY_SK (no rpad)
# 45) TIER_ID => (varchar? -> rpad(256))
# 46) DRUG_TYP_CD_SK (no rpad)
# 47) PLN_DRUG_STTUS_CD_SK (no rpad)
# 48) DRUG_PLN_TYP_ID => char(8) => rpad(8)

df_final = df_xfm_CheckLkpResults.select(
    F.col("DRUG_CLM_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID"), 256, " ").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BUY_CST_SRC_CD_SK"),
    F.col("GNRC_OVRD_CD_SK"),
    F.col("PDX_NTWK_QLFR_CD_SK"),
    F.rpad(F.col("FRMLRY_PROTOCOL_IN"), 1, " ").alias("FRMLRY_PROTOCOL_IN"),
    F.rpad(F.col("RECON_IN"), 1, " ").alias("RECON_IN"),
    F.rpad(F.col("SPEC_PGM_IN"), 1, " ").alias("SPEC_PGM_IN"),
    F.rpad(F.col("FINL_PLN_EFF_DT_SK"), 10, " ").alias("FINL_PLN_EFF_DT_SK"),
    F.rpad(F.col("ORIG_PD_TRANS_SUBMT_DT_SK"), 10, " ").alias("ORIG_PD_TRANS_SUBMT_DT_SK"),
    F.col("PRORTD_DISPNS_QTY"),
    F.col("SUBMT_DISPNS_METRIC_QTY"),
    F.col("AVG_WHLSL_PRICE_UNIT_CST_AMT"),
    F.col("WHLSL_ACQSTN_CST_UNIT_CST_AMT"),
    F.col("BUY_DISPNS_FEE_AMT"),
    F.col("BUY_INGR_CST_AMT"),
    F.col("BUY_RATE_PCT"),
    F.col("BUY_SLS_TAX_AMT"),
    F.col("BUY_TOT_DUE_AMT"),
    F.col("BUY_TOT_OTHR_AMT"),
    F.col("CST_TYP_UNIT_CST_AMT"),
    F.col("INVC_TOT_DUE_AMT"),
    F.col("SELL_CST_TYP_UNIT_CST_AMT"),
    F.rpad(F.col("SELL_RATE_PCT"), 256, " ").alias("SELL_RATE_PCT"),
    F.col("SPREAD_DISPNS_FEE_AMT"),
    F.col("SPREAD_INGR_CST_AMT"),
    F.col("SPREAD_SLS_TAX_AMT"),
    F.rpad(F.col("BUY_CST_TYP_ID"), 256, " ").alias("BUY_CST_TYP_ID"),
    F.rpad(F.col("BUY_PRICE_TYP_ID"), 256, " ").alias("BUY_PRICE_TYP_ID"),
    F.rpad(F.col("CST_TYP_ID"), 256, " ").alias("CST_TYP_ID"),
    F.rpad(F.col("DRUG_TYP_ID"), 256, " ").alias("DRUG_TYP_ID"),
    F.rpad(F.col("FINL_PLN_ID"), 256, " ").alias("FINL_PLN_ID"),
    F.rpad(F.col("GNRC_PROD_ID"), 256, " ").alias("GNRC_PROD_ID"),
    F.rpad(F.col("SELL_PRICE_TYP_ID"), 256, " ").alias("SELL_PRICE_TYP_ID"),
    F.rpad(F.col("SPEC_PGM_ID"), 256, " ").alias("SPEC_PGM_ID"),
    F.col("BUY_INCNTV_FEE_AMT"),
    F.col("SELL_INCNTV_FEE_AMT"),
    F.col("SPREAD_INCNTV_FEE_AMT"),
    F.col("SELL_OTHR_AMT"),
    F.col("SELL_OTHR_PAYOR_AMT"),
    F.col("FRMLRY_SK"),
    F.rpad(F.col("TIER_ID"), 256, " ").alias("TIER_ID"),
    F.col("DRUG_TYP_CD_SK"),
    F.col("PLN_DRUG_STTUS_CD_SK"),
    F.rpad(F.col("DRUG_PLN_TYP_ID"), 8, " ").alias("DRUG_PLN_TYP_ID")
)

write_files(
    df_final,
    f"{adls_path}/load/DRUG_CLM_PRICE.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)