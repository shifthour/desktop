# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from IDS & EDW tables and loads to EDW
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                     Development          Code                        Date 
# MAGIC Developer            Date                   Project/Altiris #            Change Description                                                                          Project                    Reviewer                  Reviewed       
# MAGIC ------------------          --------------------       ------------------------            ------------------------------------------------------------------------------------                   -----------------------        -------------------------       ---------------------------   
# MAGIC Bhoomi                09/30/2009          3500                           Originally Programmed                                                                   devlEDWnew           Steph Goddard      10/12/2009
# MAGIC 
# MAGIC Bhoomi                01/12/2010         Prod Supp                   Added new logic to VNDR_ID lkup where making                      EnterpriseWrhsDevl   Steph Goddard         01/30/2010
# MAGIC                                                                                             RATE.EFF_DT_SK <= BeginDtPrMnth AND
# MAGIC                                                                                             RATE.TERM_DT_SK >= BeginDtPrMnth
# MAGIC 
# MAGIC Raj Mangalampally  08/24/2013    5114                            Original Programming                                                                    EnterpriseWrhsDevl   Peter Marshall          12/10/2013
# MAGIC                                                                                            (Server to Parallel Conversion)

# MAGIC This is Source extract data from an EDW table and apply Code denormalization, create a load ready file.
# MAGIC 
# MAGIC Job Name:
# MAGIC EdwEdwBnfVndrRemitDtlFExtr
# MAGIC 
# MAGIC Table:
# MAGIC BNF_VNDR_REMIT_DTL_F
# MAGIC Lookup Keys 
# MAGIC 
# MAGIC 1) PROD_ID
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
# MAGIC Write Data in to Data Set for Primary Key Job EdwEdwBnfVndrRemitDtlFPkey
# MAGIC Lookup Keys 
# MAGIC 
# MAGIC 1) BNF_VNDR_ID
# MAGIC Read From the Source EDW Table MBR_ORIG_CT_F and MBR_D and Pull Records Based on LAST_UPDT_RUN_CYC_EXCTN_SK
# MAGIC Add Defaults and Null Handling
# MAGIC Add Defaults, Null Handling and Business Rules
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
BeginDtPrMnth = get_widget_value('BeginDtPrMnth','')
BeginPrMnth = get_widget_value('BeginPrMnth','')
EndDtPrMnth = get_widget_value('EndDtPrMnth','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# Stage: db2_BNF_VNDR_REMIT_SUM_sk
extract_query = f"""
SELECT
  RATE.BNF_SUM_DTL_TYP_CD,
  RATE.BNF_VNDR_ID,
  RATE.BNF_VNDR_REMIT_COV_YR_MO,
  RATE.BNF_VNDR_REMIT_SUM_SK
FROM {EDWOwner}.BNF_VNDR_REMIT_SUM_F RATE
"""
df_db2_BNF_VNDR_REMIT_SUM_sk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: db2_BNF_SUM_DTL_D_in
extract_query = f"""
SELECT DISTINCT
  RATE.BNF_SUM_DTL_TYP_CD,
  DTL.BNF_SUM_DTL_SK,
  RATE.BNF_VNDR_ID
FROM {EDWOwner}.BNF_VNDR_REMIT_RATE_D RATE,
     {EDWOwner}.BNF_SUM_DTL_D DTL
WHERE DTL.BNF_SUM_DTL_TYP_CD IN ('DNOA', 'DNoA')
  AND RATE.BNF_SUM_DTL_TYP_CD = DTL.BNF_SUM_DTL_TYP_CD
  AND RATE.EFF_DT_SK <= '{BeginDtPrMnth}'
  AND RATE.TERM_DT_SK >= '{EndDtPrMnth}'
"""
df_db2_BNF_SUM_DTL_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: db2_BNF_VNDR_D_in
extract_query = f"""
SELECT
  VNDR.BNF_VNDR_ID,
  VNDR.BNF_VNDR_SK,
  VNDR.BNF_VNDR_NM
FROM {EDWOwner}.BNF_VNDR_D VNDR,
     {EDWOwner}.BNF_VNDR_REMIT_RATE_D RATE
WHERE RATE.BNF_VNDR_ID = VNDR.BNF_VNDR_ID
;
"""
df_db2_BNF_VNDR_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: lkp_BnfVndrId (PxLookup)
p = df_db2_BNF_SUM_DTL_D_in.alias("p")
l = df_db2_BNF_VNDR_D_in.alias("l")
df_lkp_BnfVndrId = (
    p.join(l, p["BNF_VNDR_ID"] == l["BNF_VNDR_ID"], "inner")
    .select(
        p["BNF_SUM_DTL_TYP_CD"].alias("BNF_SUM_DTL_TYP_CD"),
        p["BNF_SUM_DTL_SK"].alias("BNF_SUM_DTL_SK"),
        p["BNF_VNDR_ID"].alias("BNF_VNDR_ID"),
    )
)

# Stage: db2_Prod_in
extract_query = f"""
SELECT
  PROD.PROD_ID,
  DTL.BNF_SUM_DTL_TYP_CD,
  CMPNT.PROD_CMPNT_TYP_CD,
  CMPNT.PROD_CMPNT_EFF_DT_SK,
  CMPNT.PROD_CMPNT_TERM_DT_SK,
  PROD.PROD_EFF_DT_SK,
  PROD.PROD_TERM_DT_SK,
  DTL.BNF_SUM_DTL_TYP_CD_SK,
  DTL.BNF_SUM_DTL_SK
FROM {EDWOwner}.PROD_D PROD,
     {EDWOwner}.PROD_CMPNT_D CMPNT,
     {EDWOwner}.BNF_SUM_DTL_D DTL
WHERE PROD.SRC_SYS_CD = CMPNT.SRC_SYS_CD
  AND CMPNT.SRC_SYS_CD = DTL.SRC_SYS_CD
  AND PROD.PROD_ID = CMPNT.PROD_ID
  AND CMPNT.PROD_CMPNT_PFX_ID = DTL.PROD_CMPNT_PFX_ID
  AND CMPNT.PROD_CMPNT_TYP_CD = 'BSBS'
  AND DTL.BNF_SUM_DTL_TYP_CD IN ('DNOA','DNoA')
  AND PROD.PROD_EFF_DT_SK <= '{BeginDtPrMnth}'
  AND PROD.PROD_TERM_DT_SK >= '{EndDtPrMnth}'
  AND CMPNT.PROD_CMPNT_EFF_DT_SK <= '{BeginDtPrMnth}'
  AND CMPNT.PROD_CMPNT_TERM_DT_SK >= '{EndDtPrMnth}'
;
"""
df_db2_Prod_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: db2_MBR_ORG_CNT_in
extract_query = f"""
SELECT
  ORIG.PROD_ID,
  ORIG.ACTVTY_YR_MO_SK,
  ORIG.MBR_CT,
  ORIG.MBR_ORIG_CT_SK,
  ORIG.MBR_UNIQ_KEY,
  ORIG.MBR_SK,
  ORIG.MBR_PRSN_CT,
  MBR.MBR_HOME_ADDR_ZIP_CD_5,
  MBR.MBR_HOME_ADDR_CNTY_NM || MBR.MBR_HOME_ADDR_ST_CD AS ST_CD_NM
FROM {EDWOwner}.MBR_ORIG_CT_F ORIG,
     {EDWOwner}.MBR_D MBR
WHERE MBR.MBR_UNIQ_KEY = ORIG.MBR_UNIQ_KEY
  AND ORIG.ACTVTY_YR_MO_SK = '{BeginPrMnth}'
  AND ORIG.MBR_CT > 0.5
  AND (MBR.MBR_HOME_ADDR_CNTY_NM || MBR.MBR_HOME_ADDR_ST_CD) NOT IN
      ('JOHNSONKS','ATCHISONKS','DOUGLASKS','LEAVENWORTHKS','MIAMIKS','JACKSONMO',
       'CLAYMO','PLATTEMO','BUCHANANMO','LAFAYETTEMO','CLINTONMO','CASSMO',
       'JOHNSONMO','RAYMO','ATCHISONMO','HOLTMO','NODAWAYMO','WORTHMO',
       'GENTRYMO','DEKALBMO','DAVIESMO','HARRISONMO','LIVINGSTONMO','GRUNDRYMO',
       'MERCERMO','ANDREWMO','CARROLLMO','CALDWELLMO','BATESMO','VERNONMO',
       'SALINEMO')
;
"""
df_db2_MBR_ORG_CNT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: lkp_MbrProdId (PxLookup)
p = df_db2_MBR_ORG_CNT_in.alias("p")
l = df_db2_Prod_in.alias("l")
df_lkp_MbrProdId = (
    p.join(l, p["PROD_ID"] == l["PROD_ID"], "inner")
    .select(
        l["BNF_SUM_DTL_TYP_CD"].alias("BNF_SUM_DTL_TYP_CD"),
        p["MBR_UNIQ_KEY"].alias("MBR_UNIQ_KEY"),
        p["PROD_ID"].alias("PROD_ID"),
        p["ACTVTY_YR_MO_SK"].alias("ACTVTY_YR_MO_SK"),
        p["MBR_PRSN_CT"].alias("MBR_PRSN_CT"),
        p["MBR_HOME_ADDR_ZIP_CD_5"].alias("MBR_HOME_ADDR_ZIP_CD_5"),
        p["MBR_SK"].alias("MBR_SK"),
        p["MBR_ORIG_CT_SK"].alias("MBR_ORIG_CT_SK"),
        l["BNF_SUM_DTL_TYP_CD_SK"].alias("BNF_SUM_DTL_TYP_CD_SK"),
        l["BNF_SUM_DTL_SK"].alias("BNF_SUM_DTL_SK"),
    )
)

# Stage: xfm_businessLogic2 (CTransformerStage)
df_xfm_businessLogic2 = (
    df_lkp_MbrProdId
    .withColumn(
        "BNF_SUM_DTL_TYP_CD",
        F.when(
            (F.col("BNF_SUM_DTL_TYP_CD").isNull()) | (trim(F.col("BNF_SUM_DTL_TYP_CD")) == ""),
            F.lit("NA")
        ).otherwise(F.col("BNF_SUM_DTL_TYP_CD"))
    )
    .withColumn(
        "BNF_SUM_DTL_TYP_CD_SK",
        F.when(F.col("BNF_SUM_DTL_TYP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("BNF_SUM_DTL_TYP_CD_SK"))
    )
    .withColumn(
        "BNF_SUM_DTL_SK",
        F.when(F.col("BNF_SUM_DTL_SK").isNull(), F.lit(0)).otherwise(F.col("BNF_SUM_DTL_SK"))
    )
    .select(
        "BNF_SUM_DTL_TYP_CD",
        "MBR_UNIQ_KEY",
        "PROD_ID",
        "ACTVTY_YR_MO_SK",
        "MBR_PRSN_CT",
        "MBR_HOME_ADDR_ZIP_CD_5",
        "MBR_SK",
        "MBR_ORIG_CT_SK",
        "BNF_SUM_DTL_TYP_CD_SK",
        "BNF_SUM_DTL_SK",
    )
)

# Stage: lkp_BnfSumDtl (PxLookup), left join with df_lkp_BnfVndrId
p = df_xfm_businessLogic2.alias("p")
l = df_lkp_BnfVndrId.alias("l")
df_lkp_BnfSumDtl = (
    p.join(
        l,
        [
            p["BNF_SUM_DTL_TYP_CD"] == l["BNF_SUM_DTL_TYP_CD"],
            p["BNF_SUM_DTL_SK"] == l["BNF_SUM_DTL_SK"],
        ],
        "left"
    )
    .select(
        l["BNF_VNDR_ID"].alias("BNF_VNDR_ID"),
        p["BNF_SUM_DTL_TYP_CD"].alias("BNF_SUM_DTL_TYP_CD"),
        p["ACTVTY_YR_MO_SK"].alias("ACTVTY_YR_MO_SK"),
        p["MBR_UNIQ_KEY"].alias("MBR_UNIQ_KEY"),
        p["MBR_SK"].alias("MBR_SK"),
        p["MBR_ORIG_CT_SK"].alias("MBR_ORIG_CT_SK"),
        p["MBR_HOME_ADDR_ZIP_CD_5"].alias("MBR_HOME_ADDR_ZIP_CD_5"),
        p["PROD_ID"].alias("PROD_ID"),
        p["BNF_SUM_DTL_TYP_CD_SK"].alias("BNF_SUM_DTL_TYP_CD_SK"),
    )
)

# Stage: xfm_businessLogic3 (CTransformerStage)
df_xfm_businessLogic3 = (
    df_lkp_BnfSumDtl
    .withColumn(
        "BNF_VNDR_ID",
        F.when(
            (F.col("BNF_VNDR_ID").isNull()) | (trim(F.col("BNF_VNDR_ID")) == ""),
            F.lit("NA")
        ).otherwise(F.col("BNF_VNDR_ID"))
    )
    .select(
        "BNF_VNDR_ID",
        "BNF_SUM_DTL_TYP_CD",
        "ACTVTY_YR_MO_SK",
        "MBR_UNIQ_KEY",
        "MBR_SK",
        "MBR_ORIG_CT_SK",
        "MBR_HOME_ADDR_ZIP_CD_5",
        "PROD_ID",
        "BNF_SUM_DTL_TYP_CD_SK",
    )
)

# Stage: lkp_BnfVndrRemitSum (PxLookup), left join with df_db2_BNF_VNDR_REMIT_SUM_sk
p = df_xfm_businessLogic3.alias("p")
l = df_db2_BNF_VNDR_REMIT_SUM_sk.alias("l")
df_lkp_BnfVndrRemitSum = (
    p.join(
        l,
        [
            p["BNF_SUM_DTL_TYP_CD"] == l["BNF_SUM_DTL_TYP_CD"],
            p["BNF_VNDR_ID"] == l["BNF_VNDR_ID"],
            p["ACTVTY_YR_MO_SK"] == l["BNF_VNDR_REMIT_COV_YR_MO"],
        ],
        "left"
    )
    .select(
        p["BNF_VNDR_ID"].alias("BNF_VNDR_ID"),
        p["BNF_SUM_DTL_TYP_CD"].alias("BNF_SUM_DTL_TYP_CD"),
        p["ACTVTY_YR_MO_SK"].alias("ACTVTY_YR_MO_SK"),
        p["MBR_UNIQ_KEY"].alias("MBR_UNIQ_KEY"),
        l["BNF_VNDR_REMIT_SUM_SK"].alias("BNF_VNDR_REMIT_SUM_SK"),
        p["MBR_SK"].alias("MBR_SK"),
        p["MBR_ORIG_CT_SK"].alias("MBR_ORIG_CT_SK"),
        p["MBR_HOME_ADDR_ZIP_CD_5"].alias("MBR_HOME_ADDR_ZIP_CD_5"),
        p["PROD_ID"].alias("PROD_ID"),
        p["BNF_SUM_DTL_TYP_CD_SK"].alias("BNF_SUM_DTL_TYP_CD_SK"),
    )
)

# Stage: xfm_BusinessLogic (CTransformerStage) => multiple outputs
# We create one DataFrame with an extra row number column:
window_spec = Window.orderBy(F.lit(1))
df_with_row = df_lkp_BnfVndrRemitSum.withColumn("rowNo", F.row_number().over(window_spec))

# Output 1 (lnk_BnfRemitRateDtlFExtr_Out) => all rows
df_xfm_BusinessLogic_out1 = df_with_row.select(
    F.lit(0).alias("BNF_VNDR_REMIT_DTL_SK"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.col("BNF_VNDR_ID").alias("BNF_VNDR_ID"),
    F.col("BNF_SUM_DTL_TYP_CD").alias("BNF_SUM_DTL_TYP_CD"),
    F.col("ACTVTY_YR_MO_SK").alias("BNF_VNDR_REMIT_COV_YR_MO"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.when(F.col("BNF_VNDR_REMIT_SUM_SK").isNull(), F.lit(0)).otherwise(F.col("BNF_VNDR_REMIT_SUM_SK")).alias("BNF_VNDR_REMIT_SUM_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MBR_ORIG_CT_SK").alias("MBR_ORIG_CT_SK"),
    F.col("MBR_HOME_ADDR_ZIP_CD_5").alias("MBR_HOME_ADDR_ZIP_CD_5"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BNF_SUM_DTL_TYP_CD_SK").alias("BNF_SUM_DTL_TYP_CD_SK"),
)

# Output 2 (UNK) => rowNo=1 only
df_xfm_BusinessLogic_out2 = (
    df_with_row.filter(F.col("rowNo") == 1)
    .select(
        F.lit(0).alias("BNF_VNDR_REMIT_DTL_SK"),
        F.lit("UNK").alias("SRC_SYS_CD"),
        F.lit("UNK").alias("BNF_VNDR_ID"),
        F.lit("UNK").alias("BNF_SUM_DTL_TYP_CD"),
        F.lit("175301").alias("BNF_VNDR_REMIT_COV_YR_MO"),
        F.lit("0").alias("MBR_UNIQ_KEY"),
        F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(0).alias("BNF_VNDR_REMIT_SUM_SK"),
        F.lit(0).alias("MBR_SK"),
        F.lit(0).alias("MBR_ORIG_CT_SK"),
        F.lit("").alias("MBR_HOME_ADDR_ZIP_CD_5"),
        F.lit("UNK").alias("PROD_ID"),
        F.lit("100").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit("100").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("BNF_SUM_DTL_TYP_CD_SK"),
    )
)

# Output 3 (NA) => rowNo=1 only
df_xfm_BusinessLogic_out3 = (
    df_with_row.filter(F.col("rowNo") == 1)
    .select(
        F.lit(1).alias("BNF_VNDR_REMIT_DTL_SK"),
        F.lit("NA").alias("SRC_SYS_CD"),
        F.lit("NA").alias("BNF_VNDR_ID"),
        F.lit("NA").alias("BNF_SUM_DTL_TYP_CD"),
        F.lit("175301").alias("BNF_VNDR_REMIT_COV_YR_MO"),
        F.lit("1").alias("MBR_UNIQ_KEY"),
        F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(1).alias("BNF_VNDR_REMIT_SUM_SK"),
        F.lit(1).alias("MBR_SK"),
        F.lit(1).alias("MBR_ORIG_CT_SK"),
        F.lit("").alias("MBR_HOME_ADDR_ZIP_CD_5"),
        F.lit("NA").alias("PROD_ID"),
        F.lit("100").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit("100").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("BNF_SUM_DTL_TYP_CD_SK"),
    )
)

# Funnel Stage: fnl_UNK_NA => union
df_fnl_UNK_NA = df_xfm_BusinessLogic_out1.unionByName(df_xfm_BusinessLogic_out2).unionByName(df_xfm_BusinessLogic_out3)

# Final Stage: ds_BNF_VNDR_REMIT_DTL_F => write to parquet
# Apply rpad to char columns and maintain column order
df_final = df_fnl_UNK_NA.select(
    F.col("BNF_VNDR_REMIT_DTL_SK"),
    F.col("SRC_SYS_CD"),
    F.col("BNF_VNDR_ID"),
    F.col("BNF_SUM_DTL_TYP_CD"),
    F.rpad(F.col("BNF_VNDR_REMIT_COV_YR_MO"), 6, " ").alias("BNF_VNDR_REMIT_COV_YR_MO"),
    F.col("MBR_UNIQ_KEY"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("BNF_VNDR_REMIT_SUM_SK"),
    F.col("MBR_SK"),
    F.col("MBR_ORIG_CT_SK"),
    F.rpad(F.col("MBR_HOME_ADDR_ZIP_CD_5"), 5, " ").alias("MBR_HOME_ADDR_ZIP_CD_5"),
    F.col("PROD_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BNF_SUM_DTL_TYP_CD_SK"),
)

write_files(
    df_final,
    "BNF_VNDR_REMIT_DTL_F.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)