# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY: IdsMbd110
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Select Ids MBR_ENR rows for P table creation
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Ralph Tucker          06/15/2010      4113                      IntegrateNewDevl                                                                       IntegrateNewDevl            Steph Goddard          06/23/2010
# MAGIC Steph Goddard        07/08/2010     4113                      changed to extract current product/member and compare          RebuildIntNewDevl
# MAGIC                                                                                       with prior to only select prior records that have current membership
# MAGIC 
# MAGIC 
# MAGIC Manasa Andru         10/6/2013       TFS-1275              Changed the job to two extracts to get rid of warnings                 IntegrateNewDevl            Kalyan Neelam          2013-10-09
# MAGIC 
# MAGIC Manasa Andru          2/14/2014      TFS - 8026            Updated the field LMT_AMT as per the new mapping rule.         IntegrateCurDevl               Kalyan Neelam         2014-02-19
# MAGIC 
# MAGIC Karthik Chintalapani   2016-11-14       5634                   Added new columns PLN_YR_EFF_DT and  PLN_YR_END_DT   IntegrateDev2             Kalyan Neelam          2016-11-28
# MAGIC 
# MAGIC Shanmugam A \(9)    2017-03-02         5321               SQL needs to be changed to alias '#CurrYear#' \(9)               IntegrateDev2              Jag Yelavarthi            2017-03-07

# MAGIC Apply business logic
# MAGIC Load File
# MAGIC Extract for current year - extract product data for current year, only extract membership if the member is CURRENT
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DecimalType,
    DateType
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ./Manual_Shared_Containers_Path/ <shared_container_name>  # (No shared containers actually referenced, placeholder per instructions)
# COMMAND ----------

# Parameters
CurrRunCycle = get_widget_value('CurrRunCycle','100')
CurrDate = get_widget_value('CurrDate','2010-07-14')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrYear = get_widget_value('CurrYear','2010')
PrevYear = get_widget_value('PrevYear','2009')
YrEndDate = get_widget_value('YrEndDate','2009-12-31')
ProdCmpntTypCdSk = get_widget_value('ProdCmpntTypCdSk','844281128')
ClsPlnProdCatCdMedSk = get_widget_value('ClsPlnProdCatCdMedSk','1949')
ClsPlnProdCatCdDntlSk = get_widget_value('ClsPlnProdCatCdDntlSk','1946')

# Read from hashed file (scenario C) -> as parquet
df_Cd_Mppng = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

# Create separate aliases for lookups (same underlying DataFrame)
df_refNtwkTypCd = df_Cd_Mppng.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM")
)
df_refDtlTypCd = df_Cd_Mppng.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM")
)
df_refSrcSysCd = df_Cd_Mppng.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM")
)

# Read PMbrLmtPriorEnroll (CSeqFileStage) from landing
schema_PMbrLmtPriorEnroll = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("PROD_ACCUM_ID", StringType(), nullable=False),
    StructField("ACCUM_NO", IntegerType(), nullable=False),
    StructField("BNF_SUM_DTL_NTWK_TYP_CD_SK", IntegerType(), nullable=False),
    StructField("BNF_SUM_DTL_TYP_CD_SK", IntegerType(), nullable=False),
    StructField("BLUEKC_DPLY_IN", StringType(), nullable=False),
    StructField("MBR_360_DPLY_IN", StringType(), nullable=False),
    StructField("EXTRNL_DPLY_ACCUM_DESC", StringType(), nullable=False),
    StructField("INTRNL_DPLY_ACCUM_DESC", StringType(), nullable=False),
    StructField("LMT_AMT", DecimalType(38,10), nullable=False),
    StructField("STOPLOSS_AMT", DecimalType(38,10), nullable=False),
    StructField("PROD_SK", IntegerType(), nullable=False),
    StructField("YR_NO", IntegerType(), nullable=False),
    StructField("PLN_YR_EFF_DT", DateType(), nullable=True),
    StructField("PLN_YR_END_DT", DateType(), nullable=True)
])

df_PMbrLmtPriorEnroll = (
    spark.read
    .format("csv")
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_PMbrLmtPriorEnroll)
    .load(f"{adls_path_raw}/landing/PMbrLmtPriorEnroll.dat")
)

# Read from IDS database (DB2Connector)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT
       ENR.SRC_SYS_CD_SK,
       ENR.MBR_UNIQ_KEY,
       PAC.PROD_ACCUM_ID,
       BNF.ACCUM_NO,
       BNF.BNF_SUM_DTL_NTWK_TYP_CD_SK,
       BNF.BNF_SUM_DTL_TYP_CD_SK,
       PAC.BLUEKC_DPLY_IN,
       PAC.MBR_360_DPLY_IN,
       PAC.EXTRNL_DPLY_ACCUM_DESC,
       PAC.INTRNL_DPLY_ACCUM_DESC,
       BNF.LMT_AMT,
       BNF.STOPLOSS_AMT,
       PC.PROD_SK,
       {CurrYear} AS YR_NO,
       CSPD.PLN_BEG_DT_MO_DAY
FROM
       {IDSOwner}.P_ACCUM_CTL PAC,
       {IDSOwner}.BNF_SUM_DTL BNF,
       {IDSOwner}.PROD_CMPNT PC,
       {IDSOwner}.MBR_ENR ENR,
       {IDSOwner}.W_MBR_ACCUM ENRDRVR,
       {IDSOwner}.CLS_PLN_DTL CSPD
Where PAC.BNF_SUM_DTL_ACCUM_LVL_CD = 'INDV'
  AND (PAC.MBR_360_DPLY_IN = 'Y' or PAC.BLUEKC_DPLY_IN = 'Y')
  AND PAC.ACCUM_NO = BNF.ACCUM_NO
  AND (BNF.STOPLOSS_AMT > 0 OR BNF.LMT_AMT > 0)
  AND BNF.PROD_CMPNT_PFX_ID = PC.PROD_CMPNT_PFX_ID
  AND PC.PROD_CMPNT_TYP_CD_SK = {ProdCmpntTypCdSk}
  AND ENR.EFF_DT_SK <= '{CurrDate}'
  AND ENR.PROD_SK = PC.PROD_SK
  AND ENR.ELIG_IN = 'Y'
  AND ENRDRVR.TERM_DT_SK = ENR.TERM_DT_SK
  AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK IN ({ClsPlnProdCatCdMedSk},{ClsPlnProdCatCdDntlSk})
  AND ENRDRVR.MBR_UNIQ_KEY = ENR.MBR_UNIQ_KEY
  AND ENRDRVR.AS_OF_DT_SK BETWEEN PC.PROD_CMPNT_EFF_DT_SK AND PC.PROD_CMPNT_TERM_DT_SK
  AND PAC.PROD_ACCUM_ID = ENRDRVR.PROD_ACCUM_ID
  AND CSPD.GRP_SK=ENR.GRP_SK
  AND CSPD.CLS_PLN_SK=ENR.CLS_PLN_SK
  AND CSPD.CLS_SK=ENR.CLS_SK
  AND CSPD.PROD_SK=ENR.PROD_SK
  AND CSPD.EFF_DT_SK<='{CurrDate}'
  AND CSPD.TERM_DT_SK>='{CurrDate}'
"""
df_CurrEnroll = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# XfmCurrYear: compute stage variables and output
df_XfmCurrYear = df_CurrEnroll.withColumn(
    "svFutrYear",
    (F.lit(CurrYear).cast(IntegerType()) + F.lit(1)).cast(StringType())
).withColumn(
    "svPlnEffDt",
    F.when(
        F.length(F.col("PLN_BEG_DT_MO_DAY")) == 3,
        F.lit(CurrYear) + F.lit("-0") +
        F.substring(F.col("PLN_BEG_DT_MO_DAY"), 1, 1) + F.lit("-") +
        F.substring(F.col("PLN_BEG_DT_MO_DAY"), 2, 2)
    ).otherwise(
        F.lit(CurrYear) + F.lit("-") +
        F.substring(F.col("PLN_BEG_DT_MO_DAY"), 1, 2) + F.lit("-") +
        F.substring(F.col("PLN_BEG_DT_MO_DAY"), 3, 2)
    )
).withColumn(
    "svPlnEndDt",
    F.when(
        F.length(F.col("PLN_BEG_DT_MO_DAY")) == 3,
        F.col("svFutrYear") + F.lit("-0") +
        F.substring(F.col("PLN_BEG_DT_MO_DAY"), 1, 1) + F.lit("-") +
        F.substring(F.col("PLN_BEG_DT_MO_DAY"), 2, 2)
    ).otherwise(
        F.col("svFutrYear") + F.lit("-") +
        F.substring(F.col("PLN_BEG_DT_MO_DAY"), 1, 2) + F.lit("-") +
        F.substring(F.col("PLN_BEG_DT_MO_DAY"), 3, 2)
    )
)

# Select for output link "CpCurrEnroll" -> "CurrentRecs"
df_CurrentRecs = df_XfmCurrYear.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("PROD_ACCUM_ID").alias("PROD_ACCUM_ID"),
    F.col("ACCUM_NO").alias("ACCUM_NO"),
    F.col("BNF_SUM_DTL_NTWK_TYP_CD_SK").alias("BNF_SUM_DTL_NTWK_TYP_CD_SK"),
    F.col("BNF_SUM_DTL_TYP_CD_SK").alias("BNF_SUM_DTL_TYP_CD_SK"),
    F.col("BLUEKC_DPLY_IN").alias("BLUEKC_DPLY_IN"),
    F.col("MBR_360_DPLY_IN").alias("MBR_360_DPLY_IN"),
    F.col("EXTRNL_DPLY_ACCUM_DESC").alias("EXTRNL_DPLY_ACCUM_DESC"),
    F.col("INTRNL_DPLY_ACCUM_DESC").alias("INTRNL_DPLY_ACCUM_DESC"),
    F.col("LMT_AMT").alias("LMT_AMT"),
    F.col("STOPLOSS_AMT").alias("STOPLOSS_AMT"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("YR_NO").alias("YR_NO"),
    F.col("svPlnEffDt").alias("PLN_YR_EFF_DT"),
    # FIND.DATE(...) call for PLN_YR_END_DT
    FIND.DATE(F.col("svPlnEndDt"), F.lit("-1"), F.lit("D"), F.lit("X"), F.lit("CCYY-MM-DD")).alias("PLN_YR_END_DT")
)

# Write to PMbrLmtCurrEnroll.dat (landing)
write_files(
    df_CurrentRecs,
    f"{adls_path_raw}/landing/PMbrLmtCurrEnroll.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# The same dataframe is also for the output link "CurrentRecords"
df_CurrentRecords = df_CurrentRecs

# Link_Collector_81 merges (union) CurrentRecords + PrRecs
df_CurrRecsOut = df_CurrentRecords.unionByName(df_PMbrLmtPriorEnroll)

# Trans3: left joins with refNtwkTypCd, refDtlTypCd, refSrcSysCd
df_Trans3 = (
    df_CurrRecsOut
    .alias("CurrRecsOut")
    .join(
        df_refNtwkTypCd.alias("refNtwkTypCd"),
        F.col("CurrRecsOut.BNF_SUM_DTL_NTWK_TYP_CD_SK") == F.col("refNtwkTypCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_refDtlTypCd.alias("refDtlTypCd"),
        F.col("CurrRecsOut.BNF_SUM_DTL_TYP_CD_SK") == F.col("refDtlTypCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_refSrcSysCd.alias("refSrcSysCd"),
        F.col("CurrRecsOut.SRC_SYS_CD_SK") == F.col("refSrcSysCd.CD_MPPNG_SK"),
        "left"
    )
)

# Build DSLink26 columns
df_DSLink26 = df_Trans3.select(
    F.when(F.col("refSrcSysCd.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("refSrcSysCd.TRGT_CD")).alias("SRC_SYS_CD"),
    F.col("CurrRecsOut.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("CurrRecsOut.YR_NO").alias("YR_NO"),
    F.col("CurrRecsOut.PROD_ACCUM_ID").alias("PROD_ACCUM_ID"),
    F.col("CurrRecsOut.ACCUM_NO").alias("ACCUM_NO"),
    F.when(F.col("refNtwkTypCd.TRGT_CD").isNull(), F.lit(" ")).otherwise(F.col("refNtwkTypCd.TRGT_CD")).alias("BNF_SUM_DTL_NTWK_TYP_CD"),
    F.when(F.col("refNtwkTypCd.TRGT_CD_NM").isNull(), F.lit(" ")).otherwise(F.col("refNtwkTypCd.TRGT_CD_NM")).alias("BNF_SUM_DTL_NTWK_TYP_NM"),
    F.when(F.col("refDtlTypCd.TRGT_CD").isNull(), F.lit(" ")).otherwise(F.col("refDtlTypCd.TRGT_CD")).alias("BNF_SUM_DTL_TYP_CD"),
    F.when(F.col("refDtlTypCd.TRGT_CD_NM").isNull(), F.lit(" ")).otherwise(F.col("refDtlTypCd.TRGT_CD_NM")).alias("BNF_SUM_DTL_TYP_NM"),
    F.col("CurrRecsOut.BLUEKC_DPLY_IN").alias("BLUEKC_DPLY_IN"),
    F.col("CurrRecsOut.MBR_360_DPLY_IN").alias("MBR_360_DPLY_IN"),
    F.col("CurrRecsOut.EXTRNL_DPLY_ACCUM_DESC").alias("EXTRNL_DPLY_ACCUM_DESC"),
    F.col("CurrRecsOut.INTRNL_DPLY_ACCUM_DESC").alias("INTRNL_DPLY_ACCUM_DESC"),
    F.col("CurrRecsOut.LMT_AMT").alias("LMT_AMT"),
    F.col("CurrRecsOut.STOPLOSS_AMT").alias("STOPLOSS_AMT"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_NO"),
    F.col("CurrRecsOut.PLN_YR_EFF_DT").alias("PLN_YR_EFF_DT"),
    F.col("CurrRecsOut.PLN_YR_END_DT").alias("PLN_YR_END_DT")
)

# BusinessRules
df_BusinessRules = df_DSLink26.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("YR_NO").alias("YR_NO"),
    F.col("PROD_ACCUM_ID").alias("PROD_ACCUM_ID"),
    F.col("ACCUM_NO").alias("ACCUM_NO"),
    F.col("BNF_SUM_DTL_NTWK_TYP_CD").alias("BNF_SUM_DTL_NTWK_TYP_CD"),
    F.col("BNF_SUM_DTL_NTWK_TYP_NM").alias("BNF_SUM_DTL_NTWK_TYP_NM"),
    F.col("BNF_SUM_DTL_TYP_CD").alias("BNF_SUM_DTL_TYP_CD"),
    F.col("BNF_SUM_DTL_TYP_NM").alias("BNF_SUM_DTL_TYP_NM"),
    F.when(
        F.col("ACCUM_NO").isin("999","107","109","203"),
        F.when(F.col("LMT_AMT")>0, F.col("LMT_AMT")).otherwise(F.lit(0))
    ).otherwise(
        F.when(
            F.col("ACCUM_NO").isin("1","3","5","6","7"),
            F.when(F.col("STOPLOSS_AMT")>0, F.col("STOPLOSS_AMT")).otherwise(F.lit(0))
        ).otherwise(F.lit(0))
    ).alias("LMT_AMT"),
    F.col("BLUEKC_DPLY_IN").alias("BLUEKC_DPLY_IN"),
    F.col("MBR_360_DPLY_IN").alias("MBR_360_DPLY_IN"),
    F.col("EXTRNL_DPLY_ACCUM_DESC").alias("EXTRNL_DPLY_ACCUM_DESC"),
    F.col("INTRNL_DPLY_ACCUM_DESC").alias("INTRNL_DPLY_ACCUM_DESC"),
    F.col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    F.col("PLN_YR_EFF_DT").alias("PLN_YR_EFF_DT"),
    F.col("PLN_YR_END_DT").alias("PLN_YR_END_DT")
)

# Final output to P_MBR_LMT.dat
# Add rpad for char/varchar columns that have known length=1
df_final = df_BusinessRules.withColumn(
    "BLUEKC_DPLY_IN",
    F.rpad(F.col("BLUEKC_DPLY_IN"), 1, " ")
).withColumn(
    "MBR_360_DPLY_IN",
    F.rpad(F.col("MBR_360_DPLY_IN"), 1, " ")
)

write_files(
    df_final.select(
        "SRC_SYS_CD",
        "MBR_UNIQ_KEY",
        "YR_NO",
        "PROD_ACCUM_ID",
        "ACCUM_NO",
        "BNF_SUM_DTL_NTWK_TYP_CD",
        "BNF_SUM_DTL_NTWK_TYP_NM",
        "BNF_SUM_DTL_TYP_CD",
        "BNF_SUM_DTL_TYP_NM",
        "LMT_AMT",
        "BLUEKC_DPLY_IN",
        "MBR_360_DPLY_IN",
        "EXTRNL_DPLY_ACCUM_DESC",
        "INTRNL_DPLY_ACCUM_DESC",
        "LAST_UPDT_RUN_CYC_NO",
        "PLN_YR_EFF_DT",
        "PLN_YR_END_DT"
    ),
    f"{adls_path}/load/P_MBR_LMT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)