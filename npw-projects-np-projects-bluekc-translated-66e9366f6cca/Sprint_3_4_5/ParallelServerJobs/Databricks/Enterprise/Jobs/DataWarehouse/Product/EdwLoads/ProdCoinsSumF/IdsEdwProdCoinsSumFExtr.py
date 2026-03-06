# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               12/06/2007          3044                              Originally Programmed                           devlEDW10                          
# MAGIC 
# MAGIC Archana Palivela     05/30/2013        5114                                         Originally Programmed  (In Parallel)          EnterpriseWhseDevl Bhoomi Dasari            8/11/2013
# MAGIC 
# MAGIC Aishwarya                03/15/2016        5600                                        DRUG_NPRFR_SPEC_COINS_PCT       EnterpriseDev2           Jag Yelavarthi           2016-04-06
# MAGIC                                                                                                            columnn added in the target

# MAGIC Job name: IdsEdwProdCoinsSumFExtr
# MAGIC 
# MAGIC 
# MAGIC Job Extract data from IDS table and apply Fkey and Code denormalization, create a load ready file.
# MAGIC Code SK lookups for Denormalization
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write this into a Dataset if the data goes into a PKEY job otherwise write this info into a Sequential file
# MAGIC 
# MAGIC Please use Metadata available in Table definitions to support data Lineage.
# MAGIC Read data from source table PROD_CMPNT
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit, when, length, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_db2_RX1N_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
        SELECT 
            BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
            BNF_SUM_DTL.COINS_PCT_AMT
        FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
             {IDSOwner}.CD_MPPNG CD_MPPNG
        WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
          AND CD_MPPNG.TRGT_CD = 'RX1N'
    """)
    .load()
)

df_db2_RX1G_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
        SELECT
            BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
            BNF_SUM_DTL.COINS_PCT_AMT
        FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
             {IDSOwner}.CD_MPPNG CD_MPPNG
        WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
          AND CD_MPPNG.TRGT_CD = 'RX1G'
    """)
    .load()
)

df_db2_PROD_CMPNT_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
        SELECT
            COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
            PROD_CMPNT.PROD_ID,
            PROD_CMPNT.PROD_CMPNT_EFF_DT_SK,
            PROD_CMPNT.PROD_SK,
            PROD_CMPNT.PROD_CMPNT_TERM_DT_SK,
            PROD_CMPNT.PROD_CMPNT_PFX_ID
        FROM {IDSOwner}.PROD_CMPNT PROD_CMPNT
        INNER JOIN {IDSOwner}.CD_MPPNG CD_MPPING
            ON PROD_CMPNT.PROD_CMPNT_TYP_CD_SK = CD_MPPING.CD_MPPNG_SK
        LEFT OUTER JOIN {IDSOwner}.CD_MPPNG CD
            ON PROD_CMPNT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
        WHERE CD_MPPING.TRGT_CD = 'BSBS'
        ORDER BY
            SRC_SYS_CD,
            PROD_ID,
            PROD_CMPNT_EFF_DT_SK
    """)
    .load()
)

df_db2_RX1P_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
        SELECT 
            BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
            BNF_SUM_DTL.COINS_PCT_AMT
        FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
             {IDSOwner}.CD_MPPNG CD_MPPNG
        WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
          AND CD_MPPNG.TRGT_CD = 'RX1P'
    """)
    .load()
)

df_db2_RX1X_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
        SELECT
            BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
            BNF_SUM_DTL.COINS_PCT_AMT
        FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
             {IDSOwner}.CD_MPPNG CD_MPPNG
        WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
          AND CD_MPPNG.TRGT_CD = 'RX1X'
    """)
    .load()
)

df_db2_HO1C_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
        SELECT
            BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
            BNF_SUM_DTL.COINS_PCT_AMT
        FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
             {IDSOwner}.CD_MPPNG CD_MPPNG
        WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
          AND CD_MPPNG.TRGT_CD = 'HOIC'
    """)
    .load()
)

df_db2_COO_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
        SELECT
            BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
            BNF_SUM_DTL.COINS_PCT_AMT
        FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
             {IDSOwner}.CD_MPPNG CD_MPPNG
        WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
          AND CD_MPPNG.TRGT_CD = 'COO'
    """)
    .load()
)

df_db2_COI_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
        SELECT
            BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
            BNF_SUM_DTL.COINS_PCT_AMT
        FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
             {IDSOwner}.CD_MPPNG CD_MPPNG
        WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
          AND CD_MPPNG.TRGT_CD = 'COI'
    """)
    .load()
)

df_db2_RTNS_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
        SELECT
            BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
            BNF_SUM_DTL.COINS_PCT_AMT
        FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
             {IDSOwner}.CD_MPPNG CD_MPPNG
        WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
          AND CD_MPPNG.TRGT_CD = 'RTNS'
    """)
    .load()
)

df_db2_RxSpExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
        SELECT
            BNF_SUM_DTL.PROD_CMPNT_PFX_ID,
            BNF_SUM_DTL.COINS_PCT_AMT
        FROM {IDSOwner}.BNF_SUM_DTL BNF_SUM_DTL,
             {IDSOwner}.CD_MPPNG CD_MPPNG
        WHERE BNF_SUM_DTL.BNF_SUM_DTL_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
          AND CD_MPPNG.TRGT_CD = 'RXSP'
          AND CD_MPPNG.SRC_SYS_CD = 'FACETS'
          AND CD_MPPNG.SRC_DOMAIN_NM = 'CAPITATION COPAYMENT TYPE'
          AND CD_MPPNG.SRC_CLCTN_CD = 'FACETS DBO'
          AND CD_MPPNG.TRGT_DOMAIN_NM = 'CAPITATION COPAYMENT TYPE'
          AND CD_MPPNG.TRGT_CLCTN_CD = 'IDS'
    """)
    .load()
)

df_lkp_FKeys = (
    df_db2_PROD_CMPNT_Extr.alias("primary")
    .join(df_db2_RX1G_Extr.alias("rxig"), col("primary.PROD_CMPNT_PFX_ID") == col("rxig.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_RX1P_Extr.alias("rx1p"), col("primary.PROD_CMPNT_PFX_ID") == col("rx1p.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_RX1N_Extr.alias("rx1n"), col("primary.PROD_CMPNT_PFX_ID") == col("rx1n.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_RX1X_Extr.alias("rx1x"), col("primary.PROD_CMPNT_PFX_ID") == col("rx1x.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_HO1C_Extr.alias("ho1c"), col("primary.PROD_CMPNT_PFX_ID") == col("ho1c.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_COO_Extr.alias("coo"), col("primary.PROD_CMPNT_PFX_ID") == col("coo.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_COI_Extr.alias("coi"), col("primary.PROD_CMPNT_PFX_ID") == col("coi.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_RTNS_Extr.alias("rtns"), col("primary.PROD_CMPNT_PFX_ID") == col("rtns.PROD_CMPNT_PFX_ID"), "left")
    .join(df_db2_RxSpExtr.alias("rxsp"), col("primary.PROD_CMPNT_PFX_ID") == col("rxsp.PROD_CMPNT_PFX_ID"), "left")
    .select(
        col("primary.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("primary.PROD_ID").alias("PROD_ID"),
        col("primary.PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
        col("primary.PROD_SK").alias("PROD_SK"),
        col("primary.PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
        col("rxig.COINS_PCT_AMT").alias("DRUG_GNRC_COINS_PCT"),
        col("rx1n.COINS_PCT_AMT").alias("DRUG_NM_BRND_COINS_PCT"),
        col("rx1p.COINS_PCT_AMT").alias("DRUG_PRFRD_COINS_PCT"),
        col("rx1x.COINS_PCT_AMT").alias("DRUG_NPRFR_COINS_PCT"),
        col("ho1c.COINS_PCT_AMT").alias("IN_HOSP_IN_OUT_NTWK_COINS_PCT"),
        col("coi.COINS_PCT_AMT").alias("IN_NTWK_COINS_PCT"),
        col("coo.COINS_PCT_AMT").alias("OUT_NTWK_COINS_PCT"),
        col("rtns.COINS_PCT_AMT").alias("RTN_IN_NTWK_COINS_PCT"),
        col("rxsp.COINS_PCT_AMT").alias("DRUG_NPRFR_SPEC_COINS_PCT"),
        col("primary.PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID")
    )
)

df_lnk_MainData_in = (
    df_lkp_FKeys
    .filter(
        (col("SRC_SYS_CD") != 'UNK') &
        (col("PROD_ID") != 'UNK') &
        (col("PROD_CMPNT_EFF_DT_SK") != '1753-01-01') &
        (col("SRC_SYS_CD") != 'NA') &
        (col("PROD_ID") != 'NA') &
        (col("PROD_CMPNT_EFF_DT_SK") != '1753-01-01')
    )
    .select(
        lit(0).alias("PROD_COINS_SUM_SK"),
        when(trim(col("SRC_SYS_CD")) == "", "UNK").otherwise(col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
        col("PROD_ID").alias("PROD_ID"),
        col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
        lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        col("PROD_SK").alias("PROD_SK"),
        col("PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
        when(length(trim(col("PROD_CMPNT_PFX_ID"))) == 0, lit(None)).otherwise(col("DRUG_GNRC_COINS_PCT")).alias("DRUG_GNRC_COINS_PCT"),
        when(length(trim(col("PROD_CMPNT_PFX_ID"))) == 0, lit(None)).otherwise(col("DRUG_NM_BRND_COINS_PCT")).alias("DRUG_NM_BRND_COINS_PCT"),
        when(length(trim(col("PROD_CMPNT_PFX_ID"))) == 0, lit(None)).otherwise(col("DRUG_PRFRD_COINS_PCT")).alias("DRUG_PRFRD_COINS_PCT"),
        when(length(trim(col("PROD_CMPNT_PFX_ID"))) == 0, lit(None)).otherwise(col("DRUG_NPRFR_COINS_PCT")).alias("DRUG_NPRFR_COINS_PCT"),
        when(length(trim(col("PROD_CMPNT_PFX_ID"))) == 0, lit(None)).otherwise(col("IN_HOSP_IN_OUT_NTWK_COINS_PCT")).alias("IN_HOSP_IN_OUT_NTWK_COINS_PCT"),
        when(length(trim(col("PROD_CMPNT_PFX_ID"))) == 0, lit(None)).otherwise(col("IN_NTWK_COINS_PCT").cast("integer")).alias("IN_NTWK_COINS_PCT"),
        when(length(trim(col("PROD_CMPNT_PFX_ID"))) == 0, lit(None)).otherwise(col("OUT_NTWK_COINS_PCT")).alias("OUT_NTWK_COINS_PCT"),
        when(length(trim(col("PROD_CMPNT_PFX_ID"))) == 0, lit(None)).otherwise(col("RTN_IN_NTWK_COINS_PCT")).alias("RTN_IN_NTWK_COINS_PCT"),
        lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("DRUG_NPRFR_SPEC_COINS_PCT").alias("DRUG_NPRFR_SPEC_COINS_PCT")
    )
)

df_UNK_Row = spark.createDataFrame(
    [
        (
            0,
            'UNK',
            'UNK',
            '1753-01-01',
            '1753-01-01',
            EDWRunCycleDate,
            0,
            '1753-01-01',
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            100,
            EDWRunCycle,
            None
        )
    ],
    StructType([
        StructField("PROD_COINS_SUM_SK", IntegerType(), True),
        StructField("SRC_SYS_CD", StringType(), True),
        StructField("PROD_ID", StringType(), True),
        StructField("PROD_CMPNT_EFF_DT_SK", StringType(), True),
        StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
        StructField("PROD_SK", IntegerType(), True),
        StructField("PROD_CMPNT_TERM_DT_SK", StringType(), True),
        StructField("DRUG_GNRC_COINS_PCT", DoubleType(), True),
        StructField("DRUG_NM_BRND_COINS_PCT", DoubleType(), True),
        StructField("DRUG_PRFRD_COINS_PCT", DoubleType(), True),
        StructField("DRUG_NPRFR_COINS_PCT", DoubleType(), True),
        StructField("IN_HOSP_IN_OUT_NTWK_COINS_PCT", DoubleType(), True),
        StructField("IN_NTWK_COINS_PCT", DoubleType(), True),
        StructField("OUT_NTWK_COINS_PCT", DoubleType(), True),
        StructField("RTN_IN_NTWK_COINS_PCT", DoubleType(), True),
        StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("DRUG_NPRFR_SPEC_COINS_PCT", DoubleType(), True),
    ])
)

df_NA_Row = spark.createDataFrame(
    [
        (
            1,
            'NA',
            'NA',
            '1753-01-01',
            '1753-01-01',
            EDWRunCycleDate,
            1,
            '1753-01-01',
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            100,
            EDWRunCycle,
            None
        )
    ],
    StructType([
        StructField("PROD_COINS_SUM_SK", IntegerType(), True),
        StructField("SRC_SYS_CD", StringType(), True),
        StructField("PROD_ID", StringType(), True),
        StructField("PROD_CMPNT_EFF_DT_SK", StringType(), True),
        StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
        StructField("PROD_SK", IntegerType(), True),
        StructField("PROD_CMPNT_TERM_DT_SK", StringType(), True),
        StructField("DRUG_GNRC_COINS_PCT", DoubleType(), True),
        StructField("DRUG_NM_BRND_COINS_PCT", DoubleType(), True),
        StructField("DRUG_PRFRD_COINS_PCT", DoubleType(), True),
        StructField("DRUG_NPRFR_COINS_PCT", DoubleType(), True),
        StructField("IN_HOSP_IN_OUT_NTWK_COINS_PCT", DoubleType(), True),
        StructField("IN_NTWK_COINS_PCT", DoubleType(), True),
        StructField("OUT_NTWK_COINS_PCT", DoubleType(), True),
        StructField("RTN_IN_NTWK_COINS_PCT", DoubleType(), True),
        StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("DRUG_NPRFR_SPEC_COINS_PCT", DoubleType(), True),
    ])
)

df_fnl_Pord_Coins_Sum_F = df_UNK_Row.unionByName(df_NA_Row).unionByName(df_lnk_MainData_in)

df_PROD_COINS_SUM_F = df_fnl_Pord_Coins_Sum_F.select(
    col("PROD_COINS_SUM_SK"),
    col("SRC_SYS_CD"),
    col("PROD_ID"),
    rpad(col("PROD_CMPNT_EFF_DT_SK"), 10, " ").alias("PROD_CMPNT_EFF_DT_SK"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("PROD_SK"),
    rpad(col("PROD_CMPNT_TERM_DT_SK"), 10, " ").alias("PROD_CMPNT_TERM_DT_SK"),
    col("DRUG_GNRC_COINS_PCT"),
    col("DRUG_NM_BRND_COINS_PCT"),
    col("DRUG_PRFRD_COINS_PCT"),
    col("DRUG_NPRFR_COINS_PCT"),
    col("IN_HOSP_IN_OUT_NTWK_COINS_PCT"),
    col("IN_NTWK_COINS_PCT"),
    col("OUT_NTWK_COINS_PCT"),
    col("RTN_IN_NTWK_COINS_PCT"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("DRUG_NPRFR_SPEC_COINS_PCT")
)

write_files(
    df_PROD_COINS_SUM_F,
    f"{adls_path}/ds/PROD_COINS_SUM_F.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)