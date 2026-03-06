# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from IDS & EDW and loads to EDW
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                     Development          Code                        Date 
# MAGIC Developer            Date                   Project/Altiris #            Change Description                                                                          Project                    Reviewer                  Reviewed       
# MAGIC ------------------          --------------------       ------------------------            ------------------------------------------------------------------------------------                   -----------------------        -------------------------       ---------------------------   
# MAGIC Bhoomi                09/30/2009          3500                           Originally Programmed                                                                   devlEDWnew          Steph Goddard        10/12/2009
# MAGIC 
# MAGIC Bhoomi                01/12/2010         Prod Supp                   Added new logic to VNDR_ID lkup where making                      EnterpriseWrhsDevl  Steph Goddard         01/30/2010
# MAGIC                                                                                             RATE.EFF_DT_SK <= BeginDtPrMnth AND
# MAGIC                                                                                             RATE.TERM_DT_SK >= BeginDtPrMnth
# MAGIC 
# MAGIC RajMangalampally    2013-08-30        5114                            Original Programming                                                                 EnterpriseWrhsDevl  Peter Marshall         12/10/2013
# MAGIC                                                                                               (Server to Parallel Conversion)

# MAGIC This is Source extract data from an EDW table and apply Code denormalization, create a load ready file.
# MAGIC 
# MAGIC Job Name:
# MAGIC EdwEdwBnfVndrRemitSumFExtr
# MAGIC 
# MAGIC Table:
# MAGIC BNF_VNDR_REMIT_SUM_F
# MAGIC Lookup Keys
# MAGIC 
# MAGIC 1) BNF_SUM_DTL_TYP_CD
# MAGIC 
# MAGIC 2) BNF_SUM_DTL_SK
# MAGIC Lookup Keys
# MAGIC 
# MAGIC 1) BNF_SUM_DTL_TYP_CD
# MAGIC 
# MAGIC 2) BNF_SUM_DTL_SK
# MAGIC 
# MAGIC 3)ACTIVITY_YR_MO_SK
# MAGIC Write Data in to Data Set for Primary Key Job EdwEdwBnfVndrRemitSumFPkey
# MAGIC Read From the Source EDW Table MBR_ORIG_CT_F and MBR_D
# MAGIC Add Defaults and Null Handling
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name> 
# COMMAND ----------

EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
BeginDtPrMnth = get_widget_value('BeginDtPrMnth','')
BeginPrMnth = get_widget_value('BeginPrMnth','')
EndDtPrMnth = get_widget_value('EndDtPrMnth','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# db2_BNF_VNDR_ID_in
sql_db2_BNF_VNDR_ID_in = f"""
SELECT distinct
 RATE.BNF_VNDR_ID,
 RATE.BNF_VNDR_REMIT_RATE,
 RATE.BNF_SUM_DTL_TYP_CD,
 DTL.BNF_SUM_DTL_TYP_CD_SK,
 RATE.CRT_RUN_CYC_EXCTN_SK,
 RATE.LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {EDWOwner}.BNF_VNDR_REMIT_RATE_D RATE,
     {EDWOwner}.BNF_SUM_DTL_D DTL
WHERE DTL.BNF_SUM_DTL_TYP_CD IN ('DNOA', 'DNoA')
  AND RATE.BNF_SUM_DTL_TYP_CD = DTL.BNF_SUM_DTL_TYP_CD
  AND RATE.EFF_DT_SK <= '{BeginDtPrMnth}'
  AND RATE.TERM_DT_SK >= '{BeginDtPrMnth}'
"""
df_db2_BNF_VNDR_ID_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_db2_BNF_VNDR_ID_in)
    .load()
)

# db2_BNF_VNDR_Sk_in
sql_db2_BNF_VNDR_Sk_in = f"""
SELECT
 VNDR.BNF_VNDR_ID,
 VNDR.BNF_VNDR_SK,
 VNDR.BNF_VNDR_NM
FROM {EDWOwner}.BNF_VNDR_D VNDR,
     {EDWOwner}.BNF_VNDR_REMIT_RATE_D RATE
WHERE RATE.BNF_VNDR_ID = VNDR.BNF_VNDR_ID
"""
df_db2_BNF_VNDR_Sk_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_db2_BNF_VNDR_Sk_in)
    .load()
)

# db2_Prod_in
sql_db2_Prod_in = f"""
SELECT
 PROD.PROD_ID,
 DTL.BNF_SUM_DTL_TYP_CD,
 CMPNT.PROD_CMPNT_TYP_CD,
 CMPNT.PROD_CMPNT_EFF_DT_SK,
 CMPNT.PROD_CMPNT_TERM_DT_SK,
 PROD.PROD_EFF_DT_SK,
 PROD.PROD_TERM_DT_SK
FROM {EDWOwner}.PROD_D PROD,
     {EDWOwner}.PROD_CMPNT_D CMPNT,
     {EDWOwner}.BNF_SUM_DTL_D DTL
WHERE PROD.SRC_SYS_CD = CMPNT.SRC_SYS_CD
  AND CMPNT.SRC_SYS_CD = DTL.SRC_SYS_CD
  AND PROD.PROD_ID = CMPNT.PROD_ID
  AND CMPNT.PROD_CMPNT_PFX_ID = DTL.PROD_CMPNT_PFX_ID
  AND CMPNT.PROD_CMPNT_TYP_CD = 'BSBS'
  AND DTL.BNF_SUM_DTL_TYP_CD IN ('DNOA', 'DNoA')
  AND PROD.PROD_EFF_DT_SK <= '{BeginDtPrMnth}'
  AND PROD.PROD_TERM_DT_SK >= '{EndDtPrMnth}'
  AND CMPNT.PROD_CMPNT_EFF_DT_SK <= '{BeginDtPrMnth}'
  AND CMPNT.PROD_CMPNT_TERM_DT_SK >= '{EndDtPrMnth}'
"""
df_db2_Prod_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_db2_Prod_in)
    .load()
)

# db2_MBR_ORG_CNT_in
sql_db2_MBR_ORG_CNT_in = f"""
SELECT
 ORIG.PROD_ID,
 ORIG.ACTVTY_YR_MO_SK,
 ORIG.MBR_CT,
 ORIG.MBR_ORIG_CT_SK,
 ORIG.MBR_UNIQ_KEY,
 ORIG.MBR_SK,
 ORIG.MBR_PRSN_CT,
 MBR.MBR_HOME_ADDR_CNTY_NM || MBR.MBR_HOME_ADDR_ST_CD as ST_CD_NM
FROM {EDWOwner}.MBR_ORIG_CT_F ORIG,
     {EDWOwner}.MBR_D MBR
WHERE MBR.MBR_UNIQ_KEY = ORIG.MBR_UNIQ_KEY
  AND ORIG.ACTVTY_YR_MO_SK = '{BeginPrMnth}'
  AND ORIG.MBR_CT > 0.5
  AND MBR.MBR_HOME_ADDR_CNTY_NM || MBR.MBR_HOME_ADDR_ST_CD NOT IN
      (
       'JOHNSONKS','ATCHISONKS','DOUGLASKS','LEAVENWORTHKS','MIAMIKS','JACKSONMO','CLAYMO','PLATTEMO','BUCHANANMO',
       'LAFAYETTEMO','CLINTONMO','CASSMO','JOHNSONMO','RAYMO','ATCHISONMO','HOLTMO','NODAWAYMO','WORTHMO','GENTRYMO',
       'DEKALBMO','DAVIESMO','HARRISONMO','LIVINGSTONMO','GRUNDRYMO','MERCERMO','ANDREWMO','CARROLLMO','CALDWELLMO',
       'BATESMO','VERNONMO','SALINEMO'
      )
"""
df_db2_MBR_ORG_CNT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_db2_MBR_ORG_CNT_in)
    .load()
)

# lkp_MbrProdId (PxLookup)
df_lkp_MbrProdId = df_db2_MBR_ORG_CNT_in.alias("lnk_MbrOrgCntExtr_In").join(
    df_db2_Prod_in.alias("Ref_BnfVndrRemitRateProId"),
    F.col("lnk_MbrOrgCntExtr_In.PROD_ID") == F.col("Ref_BnfVndrRemitRateProId.PROD_ID"),
    "inner"
).select(
    F.col("lnk_MbrOrgCntExtr_In.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lnk_MbrOrgCntExtr_In.PROD_ID").alias("PROD_ID"),
    F.col("Ref_BnfVndrRemitRateProId.BNF_SUM_DTL_TYP_CD").alias("BNF_SUM_DTL_TYP_CD"),
    F.col("lnk_MbrOrgCntExtr_In.ACTVTY_YR_MO_SK").alias("ACTVTY_YR_MO_SK"),
    F.col("lnk_MbrOrgCntExtr_In.MBR_PRSN_CT").alias("MBR_PRSN_CT")
)

# xfm_BusineesLogic2 (CTransformerStage)
df_xfm_BusineesLogic2 = df_lkp_MbrProdId.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.when(
        F.col("BNF_SUM_DTL_TYP_CD").isNull() | (trim("BNF_SUM_DTL_TYP_CD") == ""),
        F.lit("NA")
    ).otherwise(F.col("BNF_SUM_DTL_TYP_CD")).alias("BNF_SUM_DTL_TYP_CD"),
    F.col("ACTVTY_YR_MO_SK").alias("ACTVTY_YR_MO_SK"),
    F.col("MBR_PRSN_CT").alias("MBR_PRSN_CT")
)

# agg_sum_max (PxAggregator)
df_agg_sum_max = df_xfm_BusineesLogic2.groupBy("BNF_SUM_DTL_TYP_CD").agg(
    F.max("ACTVTY_YR_MO_SK").alias("ACTVTY_YR_MO_SK"),
    F.sum("MBR_PRSN_CT").alias("MBR_PRSN_CT")
)

# lkp_BnfSumDtl (PxLookup) with three inputs:
df_lkp_BnfSumDtl_pre = df_db2_BNF_VNDR_ID_in.alias("lnk_BnfSumDtlDExtr_in").join(
    df_db2_BNF_VNDR_Sk_in.alias("Ref_BnfVndrRemitDtl_VndrSk"),
    F.col("lnk_BnfSumDtlDExtr_in.BNF_VNDR_ID") == F.col("Ref_BnfVndrRemitDtl_VndrSk.BNF_VNDR_ID"),
    "inner"
)

df_lkp_BnfSumDtl = df_lkp_BnfSumDtl_pre.join(
    df_agg_sum_max.alias("lnk_BnfVndrRemitSumLkup"),
    F.col("lnk_BnfSumDtlDExtr_in.BNF_SUM_DTL_TYP_CD") == F.col("lnk_BnfVndrRemitSumLkup.BNF_SUM_DTL_TYP_CD"),
    "inner"
).select(
    F.col("lnk_BnfSumDtlDExtr_in.BNF_VNDR_ID").alias("BNF_VNDR_ID"),
    F.col("Ref_BnfVndrRemitDtl_VndrSk.BNF_VNDR_ID").alias("BNF_VNDR_ID_1"),
    F.col("Ref_BnfVndrRemitDtl_VndrSk.BNF_VNDR_SK").alias("BNF_VNDR_SK"),
    F.col("Ref_BnfVndrRemitDtl_VndrSk.BNF_VNDR_NM").alias("BNF_VNDR_NM"),
    F.col("lnk_BnfVndrRemitSumLkup.MBR_PRSN_CT").alias("MBR_PRSN_CT"),
    F.col("lnk_BnfSumDtlDExtr_in.BNF_VNDR_REMIT_RATE").alias("BNF_VNDR_REMIT_RATE"),
    F.col("lnk_BnfSumDtlDExtr_in.BNF_SUM_DTL_TYP_CD").alias("BNF_SUM_DTL_TYP_CD"),
    F.col("lnk_BnfVndrRemitSumLkup.BNF_SUM_DTL_TYP_CD").alias("BNF_SUM_DTL_TYP_CD_1"),
    F.col("lnk_BnfSumDtlDExtr_in.BNF_SUM_DTL_TYP_CD_SK").alias("BNF_SUM_DTL_TYP_CD_SK"),
    F.col("lnk_BnfVndrRemitSumLkup.ACTVTY_YR_MO_SK").alias("ACTVTY_YR_MO_SK")
)

# xfm_BusinessLogic (CTransformerStage)
df_with_svMbrCnt = df_lkp_BnfSumDtl.withColumn(
    "svMbrCnt",
    F.when(F.col("MBR_PRSN_CT").isNull(), F.lit(0)).otherwise(F.col("MBR_PRSN_CT").cast(IntegerType()))
)

df_main = df_with_svMbrCnt.select(
    F.lit(0).alias("BNF_VNDR_REMIT_SUM_SK"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.col("BNF_VNDR_ID").alias("BNF_VNDR_ID"),
    F.col("BNF_SUM_DTL_TYP_CD").alias("BNF_SUM_DTL_TYP_CD"),
    F.when(
        F.col("BNF_SUM_DTL_TYP_CD_1").isNull() | (trim("BNF_SUM_DTL_TYP_CD_1") == ""),
        F.lit("0")
    ).otherwise(F.col("ACTVTY_YR_MO_SK")).alias("BNF_VNDR_REMIT_COV_YR_MO"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.when(
        F.col("BNF_VNDR_ID_1").isNull() | (trim("BNF_VNDR_ID_1") == ""),
        F.lit(0)
    ).otherwise(F.col("BNF_VNDR_SK")).alias("BNF_VNDR_SK"),
    F.when(
        F.col("MBR_PRSN_CT").isNull() | (trim("MBR_PRSN_CT") == ""),
        F.lit(0)
    ).otherwise(F.col("BNF_VNDR_REMIT_RATE") * F.col("MBR_PRSN_CT")).alias("BNF_VNDR_REMIT_AMT"),
    F.col("BNF_VNDR_REMIT_RATE").alias("BNF_VNDR_REMIT_RATE"),
    F.when(
        F.col("BNF_SUM_DTL_TYP_CD_1").isNull() | (trim("BNF_SUM_DTL_TYP_CD_1") == ""),
        F.lit(0)
    ).otherwise(F.col("svMbrCnt")).alias("BNF_VNDR_MBR_CT"),
    F.when(
        F.col("BNF_VNDR_ID_1").isNull() | (trim("BNF_VNDR_ID_1") == ""),
        F.lit("NA")
    ).otherwise(F.col("BNF_VNDR_NM")).alias("BNF_VNDR_NM"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BNF_SUM_DTL_TYP_CD_SK").alias("BNF_SUM_DTL_TYP_CD_SK")
)

df_unk = spark.createDataFrame(
    [
        (
            0, "UNK", "UNK", "UNK", "175301", "1753-01-01", "1753-01-01",
            0, 0, 0, 0, "UNK", 100, 100, 0
        )
    ],
    [
        "BNF_VNDR_REMIT_SUM_SK",
        "SRC_SYS_CD",
        "BNF_VNDR_ID",
        "BNF_SUM_DTL_TYP_CD",
        "BNF_VNDR_REMIT_COV_YR_MO",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "BNF_VNDR_SK",
        "BNF_VNDR_REMIT_AMT",
        "BNF_VNDR_REMIT_RATE",
        "BNF_VNDR_MBR_CT",
        "BNF_VNDR_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "BNF_SUM_DTL_TYP_CD_SK"
    ]
)

df_na = spark.createDataFrame(
    [
        (
            1, "NA", "NA", "NA", "175301", "1753-01-01", "1753-01-01",
            1, 0, 0, 0, "NA", 100, 100, 1
        )
    ],
    [
        "BNF_VNDR_REMIT_SUM_SK",
        "SRC_SYS_CD",
        "BNF_VNDR_ID",
        "BNF_SUM_DTL_TYP_CD",
        "BNF_VNDR_REMIT_COV_YR_MO",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "BNF_VNDR_SK",
        "BNF_VNDR_REMIT_AMT",
        "BNF_VNDR_REMIT_RATE",
        "BNF_VNDR_MBR_CT",
        "BNF_VNDR_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "BNF_SUM_DTL_TYP_CD_SK"
    ]
)

df_fnl = df_main.unionByName(df_unk).unionByName(df_na)

df_fnl_output = df_fnl.select(
    F.col("BNF_VNDR_REMIT_SUM_SK"),
    F.col("SRC_SYS_CD"),
    F.col("BNF_VNDR_ID"),
    F.col("BNF_SUM_DTL_TYP_CD"),
    F.rpad(F.col("BNF_VNDR_REMIT_COV_YR_MO"), 6, " ").alias("BNF_VNDR_REMIT_COV_YR_MO"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("BNF_VNDR_SK"),
    F.col("BNF_VNDR_REMIT_AMT"),
    F.col("BNF_VNDR_REMIT_RATE"),
    F.col("BNF_VNDR_MBR_CT"),
    F.col("BNF_VNDR_NM"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BNF_SUM_DTL_TYP_CD_SK")
)

write_files(
    df_fnl_output,
    "BNF_VNDR_REMIT_SUM_F.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)