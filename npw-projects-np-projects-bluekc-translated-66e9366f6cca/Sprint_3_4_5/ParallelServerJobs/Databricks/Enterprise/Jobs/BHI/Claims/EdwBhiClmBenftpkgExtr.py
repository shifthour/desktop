# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2016 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdwBhiClmBenftpkgExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Extract Benefit package data to be sent to BCBSA for BHI (Blue Health Intelligence)
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  EdwBhiClmExtrSeq
# MAGIC 
# MAGIC HASH FILES:  
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                    Project/Ticket #\(9)           Change Description\(9)\(9)\(9)              Development Project\(9)      Code Reviewer\(9)        Date Reviewed       
# MAGIC ------------------                    -------------------            ---------------------------\(9)           -----------------------------------------------------------------------            --------------------------------\(9)      -------------------------         -------------------------    
# MAGIC Aishwarya                      2016-07-18              5604 BHI                                      Original programming                                                   EnterpriseDevl                 Kalyan Neelam          2016-07-19
# MAGIC Aishwarya                      2016-09-01              5604                                              Derivation modified for MED_BNF_RMBRMT            EnterpriseDevl                 Kalyan Neelam          2016-09-12
# MAGIC                                                                                                                                                
# MAGIC Mohan Karnati               2020-07-07             US-243135                 Including High Performance n/w  in product short name 
# MAGIC                                                                                                                             while  extracting in the source query                           EnterpriseDev1              Hugh Sisson               2020-08-18
# MAGIC 
# MAGIC Tamannakumari            2024-02-29               US- 612200                Include additional BMADVH and BMADVP in source query
# MAGIC                                                                                                                  (where PROD_SH_NM = 'BMADVH', 'BMADVP')                  EnterpriseDev1             Jeyaprasanna              2024-03-07

# MAGIC Control File is created with file name and respective counts, write mode is set as append to capture information for all BHI files
# MAGIC Remove duplicate on PROD_CMPNT_PFX_ID since there are duplicates on all amounts
# MAGIC BHI Benefit Package Extract
# MAGIC Create fixed length data and provide monthly file to BHI
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
CurrDate = get_widget_value('CurrDate','')
StartDate = get_widget_value('StartDate','')
EndDate = get_widget_value('EndDate','')
ProdIn = get_widget_value('ProdIn','')
LastRunCycEnd = get_widget_value('LastRunCycEnd','')

# --------------------------------------------------------------------------------
# Benefit_Pkg (DB2ConnectorPX) - Reading from EDW
# --------------------------------------------------------------------------------
jdbc_url_1, jdbc_props_1 = get_db_config(edw_secret_name)
extract_query_1 = f"""
SELECT
pcmpnt.PROD_CMPNT_EFF_DT_SK,
pcmpnt.PROD_CMPNT_PFX_ID,
pcmpnt.PROD_CMPNT_TERM_DT_SK,
prd.PROD_SK,
pcmpnt.PROD_ID
FROM
{EDWOwner}.PROD_CMPNT_D pcmpnt,
{EDWOwner}.PROD_D prd
WHERE
pcmpnt.PROD_SK = prd.PROD_SK
AND pcmpnt.PROD_CMPNT_TYP_CD = 'BSBS'
AND pcmpnt.PROD_CMPNT_EFF_DT_SK <= '{LastRunCycEnd}'
AND pcmpnt.PROD_CMPNT_TERM_DT_SK >= DATEADD(MONTH, -14, CAST('{StartDate}' AS DATE))
AND prd.PROD_SH_NM in ('PC','PCB','BLUE-ACCESS','BLUE-SELECT','BCARE','BLUESELECT+ ','HP','BMADVH','BMADVP')
AND prd.PROD_EFF_DT_SK <= '{LastRunCycEnd}'
AND prd.PROD_TERM_DT_SK >= DATEADD(MONTH, -14, CAST('{StartDate}' AS DATE))
AND LOWER(prd.PROD_ID)<>prd.PROD_ID
"""
df_Benefit_Pkg = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_1)
    .options(**jdbc_props_1)
    .option("query", extract_query_1)
    .load()
)

# --------------------------------------------------------------------------------
# Prod_Bnf (DB2ConnectorPX) - Reading from EDW
# --------------------------------------------------------------------------------
jdbc_url_2, jdbc_props_2 = get_db_config(edw_secret_name)
extract_query_2 = f"""
SELECT DISTINCT
bsum.PROD_CMPNT_PFX_ID,
bsum.BNF_SUM_DTL_TYP_CD,
bsum.BNF_SUM_DTL_COINS_PCT_AMT,
bsum.BNF_SUM_DTL_COPAY_AMT,
bsum.BNF_SUM_DTL_DEDCT_AMT,
bsum.BNF_SUM_DTL_STOPLOSS_AMT
FROM
{EDWOwner}.BNF_SUM_DTL_D bsum
WHERE
 bsum.BNF_SUM_DTL_TYP_CD in
 ('DEDI','DIO','DEDF','DFO','OOPI','IOI','IOPI','OPIO','IOO','IOPO','FOI','IOFI','OOPF','FOO','IOFO','OFIO',
 'OV','OVP','OVS','HOI','OPH','ER','ERO','MCPC','COI','HOIC','COO')
"""
df_Prod_Bnf = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_2)
    .options(**jdbc_props_2)
    .option("query", extract_query_2)
    .load()
)

# --------------------------------------------------------------------------------
# Fltr_Bnf_Pkg (PxFilter) - Split into multiple outputs
# --------------------------------------------------------------------------------
df_Indv_Dedc_in_rmd = df_Prod_Bnf.filter("BNF_SUM_DTL_TYP_CD = 'DEDI'").select(
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_DEDCT_AMT").alias("BNF_SUM_DTL_DEDCT_AMT")
)
df_Indv_Dedc_out_rmd = df_Prod_Bnf.filter("BNF_SUM_DTL_TYP_CD = 'DIO'").select(
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_DEDCT_AMT").alias("BNF_SUM_DTL_DEDCT_AMT")
)
df_Fmly_Dedc_in_rmd = df_Prod_Bnf.filter("BNF_SUM_DTL_TYP_CD = 'DEDF'").select(
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_DEDCT_AMT").alias("BNF_SUM_DTL_DEDCT_AMT")
)
df_Fmly_Dedc_out_rmd = df_Prod_Bnf.filter("BNF_SUM_DTL_TYP_CD = 'DFO'").select(
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_DEDCT_AMT").alias("BNF_SUM_DTL_DEDCT_AMT")
)
df_Inv_oop_in_rmd = df_Prod_Bnf.filter("BNF_SUM_DTL_TYP_CD in ('OOPI','IOI','IOPI')").select(
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_STOPLOSS_AMT").alias("BNF_SUM_DTL_STOPLOSS_AMT")
)
df_Inv_oop_out_rmd = df_Prod_Bnf.filter("BNF_SUM_DTL_TYP_CD in ('OPIO','IOO','IOPO')").select(
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_STOPLOSS_AMT").alias("BNF_SUM_DTL_STOPLOSS_AMT")
)
df_Fmly_oop_in_rmd = df_Prod_Bnf.filter("BNF_SUM_DTL_TYP_CD in ('FOI','IOFI','OOPF')").select(
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_STOPLOSS_AMT").alias("BNF_SUM_DTL_STOPLOSS_AMT")
)
df_Fmly_oop_out_rmd = df_Prod_Bnf.filter("BNF_SUM_DTL_TYP_CD in ('FOO','IOFO','OFIO')").select(
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_STOPLOSS_AMT").alias("BNF_SUM_DTL_STOPLOSS_AMT")
)
df_Pcp_copay_rmd = df_Prod_Bnf.filter("BNF_SUM_DTL_TYP_CD in ('OV','OVP')").select(
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_COPAY_AMT").alias("BNF_SUM_DTL_COPAY_AMT")
)
df_Splst_copay_rmd = df_Prod_Bnf.filter("BNF_SUM_DTL_TYP_CD = 'OVS'").select(
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_COPAY_AMT").alias("BNF_SUM_DTL_COPAY_AMT")
)
df_Ip_fclty_copay_rmd = df_Prod_Bnf.filter("BNF_SUM_DTL_TYP_CD = 'HOI'").select(
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_COPAY_AMT").alias("BNF_SUM_DTL_COPAY_AMT")
)
df_Op_fclty_copay_rmd = df_Prod_Bnf.filter("BNF_SUM_DTL_TYP_CD = 'OPH'").select(
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_COPAY_AMT").alias("BNF_SUM_DTL_COPAY_AMT")
)
df_Er_copay_in_rmd = df_Prod_Bnf.filter("BNF_SUM_DTL_TYP_CD = 'ER'").select(
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_STOPLOSS_AMT").alias("BNF_SUM_DTL_STOPLOSS_AMT")
)
df_Er_copay_out_rmd = df_Prod_Bnf.filter("BNF_SUM_DTL_TYP_CD = 'ERO'").select(
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_STOPLOSS_AMT").alias("BNF_SUM_DTL_STOPLOSS_AMT")
)
df_Advd_img_copay_rmd = df_Prod_Bnf.filter("BNF_SUM_DTL_TYP_CD = 'MCPC'").select(
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_COPAY_AMT").alias("BNF_SUM_DTL_COPAY_AMT")
)
df_Splst_coins_in_rmd = df_Prod_Bnf.filter("BNF_SUM_DTL_TYP_CD = 'COI'").select(
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_COINS_PCT_AMT").alias("BNF_SUM_DTL_COINS_PCT_AMT")
)
df_Splst_coins_out_rmd = df_Prod_Bnf.filter("BNF_SUM_DTL_TYP_CD = 'COO'").select(
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_COINS_PCT_AMT").alias("BNF_SUM_DTL_COINS_PCT_AMT")
)
df_Ip_fclty_coins_rmd = df_Prod_Bnf.filter("BNF_SUM_DTL_TYP_CD = 'HOIC'").select(
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_COINS_PCT_AMT").alias("BNF_SUM_DTL_COINS_PCT_AMT")
)

# --------------------------------------------------------------------------------
# PxRemDup stages (dedup_sort function)
# --------------------------------------------------------------------------------
df_op_fclty_copay_rmd_dedup = dedup_sort(df_Op_fclty_copay_rmd, ["PROD_CMPNT_PFX_ID"], [])
df_op_fclty_copay = df_op_fclty_copay_rmd_dedup.select(
    F.col("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_COPAY_AMT")
)

df_inv_oop_out_rmd_dedup = dedup_sort(df_Inv_oop_out_rmd, ["PROD_CMPNT_PFX_ID"], [])
df_inv_oop_out = df_inv_oop_out_rmd_dedup.select(
    F.col("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_STOPLOSS_AMT")
)

df_Indv_Dedc_in_rmd_dedup = dedup_sort(df_Indv_Dedc_in_rmd, ["PROD_CMPNT_PFX_ID"], [])
df_Indv_Dedc_in = df_Indv_Dedc_in_rmd_dedup.select(
    F.col("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_DEDCT_AMT")
)

df_Fmly_Dedc_in_rmd_dedup = dedup_sort(df_Fmly_Dedc_in_rmd, ["PROD_CMPNT_PFX_ID"], [])
df_Fmly_Dedc_in = df_Fmly_Dedc_in_rmd_dedup.select(
    F.col("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_DEDCT_AMT")
)

df_Pcp_copay_rmd_dedup = dedup_sort(df_Pcp_copay_rmd, ["PROD_CMPNT_PFX_ID"], [])
df_pcp_copay = df_Pcp_copay_rmd_dedup.select(
    F.col("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_COPAY_AMT")
)

df_Ip_fclty_coins_rmd_dedup = dedup_sort(df_Ip_fclty_coins_rmd, ["PROD_CMPNT_PFX_ID"], [])
df_ip_fclty_coins = df_Ip_fclty_coins_rmd_dedup.select(
    F.col("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_COINS_PCT_AMT")
)

df_Inv_oop_in_rmd_dedup = dedup_sort(df_Inv_oop_in_rmd, ["PROD_CMPNT_PFX_ID"], [])
df_inv_oop_in = df_Inv_oop_in_rmd_dedup.select(
    F.col("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_STOPLOSS_AMT")
)

df_Splst_coins_in_rmd_dedup = dedup_sort(df_Splst_coins_in_rmd, ["PROD_CMPNT_PFX_ID"], [])
df_splst_coins_in = df_Splst_coins_in_rmd_dedup.select(
    F.col("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_COINS_PCT_AMT")
)

df_Splst_copay_rmd_dedup = dedup_sort(df_Splst_copay_rmd, ["PROD_CMPNT_PFX_ID"], [])
df_splst_copay = df_Splst_copay_rmd_dedup.select(
    F.col("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_COPAY_AMT")
)

df_Er_copay_in_rmd_dedup = dedup_sort(df_Er_copay_in_rmd, ["PROD_CMPNT_PFX_ID"], [])
df_er_copay_in = df_Er_copay_in_rmd_dedup.select(
    F.col("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_STOPLOSS_AMT")
)

df_Fmly_Dedc_out_rmd_dedup = dedup_sort(df_Fmly_Dedc_out_rmd, ["PROD_CMPNT_PFX_ID"], [])
df_fmly_dedc_out = df_Fmly_Dedc_out_rmd_dedup.select(
    F.col("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_DEDCT_AMT")
)

df_Indv_Dedc_out_rmd_dedup = dedup_sort(df_Indv_Dedc_out_rmd, ["PROD_CMPNT_PFX_ID"], [])
df_indv_dedc_out = df_Indv_Dedc_out_rmd_dedup.select(
    F.col("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_DEDCT_AMT")
)

df_Advd_img_copay_rmd_dedup = dedup_sort(df_Advd_img_copay_rmd, ["PROD_CMPNT_PFX_ID"], [])
df_advd_img_copay = df_Advd_img_copay_rmd_dedup.select(
    F.col("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_COPAY_AMT")
)

df_Fmly_oop_in_rmd_dedup = dedup_sort(df_Fmly_oop_in_rmd, ["PROD_CMPNT_PFX_ID"], [])
df_fmly_oop_in = df_Fmly_oop_in_rmd_dedup.select(
    F.col("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_STOPLOSS_AMT")
)

df_Splst_coins_out_rmd_dedup = dedup_sort(df_Splst_coins_out_rmd, ["PROD_CMPNT_PFX_ID"], [])
df_splst_coins_out = df_Splst_coins_out_rmd_dedup.select(
    F.col("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_COINS_PCT_AMT")
)

df_Er_copay_out_rmd_dedup = dedup_sort(df_Er_copay_out_rmd, ["PROD_CMPNT_PFX_ID"], [])
df_er_copay_out = df_Er_copay_out_rmd_dedup.select(
    F.col("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_STOPLOSS_AMT")
)

df_Ip_fclty_copay_rmd_dedup = dedup_sort(df_Ip_fclty_copay_rmd, ["PROD_CMPNT_PFX_ID"], [])
df_ip_fclty_copay = df_Ip_fclty_copay_rmd_dedup.select(
    F.col("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_COPAY_AMT")
)

df_Fmly_oop_out_rmd_dedup = dedup_sort(df_Fmly_oop_out_rmd, ["PROD_CMPNT_PFX_ID"], [])
df_fmly_oop_out = df_Fmly_oop_out_rmd_dedup.select(
    F.col("PROD_CMPNT_PFX_ID"),
    F.col("BNF_SUM_DTL_STOPLOSS_AMT")
)

# --------------------------------------------------------------------------------
# Lkp_Bnf_Pkg (PxLookup) - Multi-join
# --------------------------------------------------------------------------------
df_Lkp_Bnf_Pkg = (
    df_Benefit_Pkg.alias("trns_1")
    .join(df_inv_oop_out.alias("Inv_oop_out"), F.col("trns_1.PROD_CMPNT_PFX_ID") == F.col("Inv_oop_out.PROD_CMPNT_PFX_ID"), "left")
    .join(df_op_fclty_copay.alias("Op_fclty_copay"), F.col("trns_1.PROD_CMPNT_PFX_ID") == F.col("Op_fclty_copay.PROD_CMPNT_PFX_ID"), "left")
    .join(df_Indv_Dedc_in.alias("Indv_Dedc_in"), F.col("trns_1.PROD_CMPNT_PFX_ID") == F.col("Indv_Dedc_in.PROD_CMPNT_PFX_ID"), "left")
    .join(df_Fmly_Dedc_in.alias("Fmly_Dedc_in"), F.col("trns_1.PROD_CMPNT_PFX_ID") == F.col("Fmly_Dedc_in.PROD_CMPNT_PFX_ID"), "left")
    .join(df_pcp_copay.alias("Pcp_copay"), F.col("trns_1.PROD_CMPNT_PFX_ID") == F.col("Pcp_copay.PROD_CMPNT_PFX_ID"), "left")
    .join(df_ip_fclty_coins.alias("Ip_fclty_coins"), F.col("trns_1.PROD_CMPNT_PFX_ID") == F.col("Ip_fclty_coins.PROD_CMPNT_PFX_ID"), "left")
    .join(df_inv_oop_in.alias("Inv_oop_in"), F.col("trns_1.PROD_CMPNT_PFX_ID") == F.col("Inv_oop_in.PROD_CMPNT_PFX_ID"), "left")
    .join(df_splst_coins_in.alias("Splst_coins_in"), F.col("trns_1.PROD_CMPNT_PFX_ID") == F.col("Splst_coins_in.PROD_CMPNT_PFX_ID"), "left")
    .join(df_splst_copay.alias("Splst_copay"), F.col("trns_1.PROD_CMPNT_PFX_ID") == F.col("Splst_copay.PROD_CMPNT_PFX_ID"), "left")
    .join(df_er_copay_in.alias("Er_copay_in"), F.col("trns_1.PROD_CMPNT_PFX_ID") == F.col("Er_copay_in.PROD_CMPNT_PFX_ID"), "left")
    .join(df_indv_dedc_out.alias("Indv_Dedc_out"), F.col("trns_1.PROD_CMPNT_PFX_ID") == F.col("Indv_Dedc_out.PROD_CMPNT_PFX_ID"), "left")
    .join(df_advd_img_copay.alias("Advd_img_copay"), F.col("trns_1.PROD_CMPNT_PFX_ID") == F.col("Advd_img_copay.PROD_CMPNT_PFX_ID"), "left")
    .join(df_fmly_oop_in.alias("Fmly_oop_in"), F.col("trns_1.PROD_CMPNT_PFX_ID") == F.col("Fmly_oop_in.PROD_CMPNT_PFX_ID"), "left")
    .join(df_fmly_dedc_out.alias("Fmly_Dedc_out"), F.col("trns_1.PROD_CMPNT_PFX_ID") == F.col("Fmly_Dedc_out.PROD_CMPNT_PFX_ID"), "left")
    .join(df_splst_coins_out.alias("Splst_coins_out"), F.col("trns_1.PROD_CMPNT_PFX_ID") == F.col("Splst_coins_out.PROD_CMPNT_PFX_ID"), "left")
    .join(df_er_copay_out.alias("Er_copay_out"), F.col("trns_1.PROD_CMPNT_PFX_ID") == F.col("Er_copay_out.PROD_CMPNT_PFX_ID"), "left")
    .join(df_ip_fclty_copay.alias("Ip_fclty_copay"), F.col("trns_1.PROD_CMPNT_PFX_ID") == F.col("Ip_fclty_copay.PROD_CMPNT_PFX_ID"), "left")
    .join(df_fmly_oop_out.alias("Fmly_oop_out"), F.col("trns_1.PROD_CMPNT_PFX_ID") == F.col("Fmly_oop_out.PROD_CMPNT_PFX_ID"), "left")
)

df_Bnf_Pkg_Trns_in = df_Lkp_Bnf_Pkg.select(
    F.col("trns_1.PROD_ID").alias("PROD_ID"),
    F.col("trns_1.PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("trns_1.PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    F.col("trns_1.PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
    F.col("Indv_Dedc_in.BNF_SUM_DTL_DEDCT_AMT").alias("INDV_DEDCT_AMT_In"),
    F.col("Indv_Dedc_out.BNF_SUM_DTL_DEDCT_AMT").alias("INDV_DEDCT_AMT_Out"),
    F.col("Fmly_Dedc_in.BNF_SUM_DTL_DEDCT_AMT").alias("FMLY_DEDCT_AMT_In"),
    F.col("Fmly_Dedc_out.BNF_SUM_DTL_DEDCT_AMT").alias("FMLY_DEDCT_AMT_Out"),
    F.col("Inv_oop_in.BNF_SUM_DTL_STOPLOSS_AMT").alias("INDV_OOP_MAX_AMT_In"),
    F.col("Inv_oop_out.BNF_SUM_DTL_STOPLOSS_AMT").alias("INDV_OOP_MAX_Out"),
    F.col("Fmly_oop_in.BNF_SUM_DTL_STOPLOSS_AMT").alias("FMLY_OOP_In"),
    F.col("Fmly_oop_out.BNF_SUM_DTL_STOPLOSS_AMT").alias("FMLY_OOP_Out"),
    F.col("Pcp_copay.BNF_SUM_DTL_COPAY_AMT").alias("PCP_COPAY_AMT"),
    F.col("Splst_copay.BNF_SUM_DTL_COPAY_AMT").alias("SPLST_PHYS_COPAY_AMT"),
    F.col("Ip_fclty_copay.BNF_SUM_DTL_COPAY_AMT").alias("IP_FCLTY_COPAY_AMT"),
    F.col("Op_fclty_copay.BNF_SUM_DTL_COPAY_AMT").alias("OP_FCLTY_COPAY_AMT"),
    F.col("Er_copay_in.BNF_SUM_DTL_STOPLOSS_AMT").alias("ER_COPAY_AMT_In"),
    F.col("Er_copay_out.BNF_SUM_DTL_STOPLOSS_AMT").alias("ER_COPAY_AMT_Out"),
    F.col("Advd_img_copay.BNF_SUM_DTL_COPAY_AMT").alias("ADVD_IMG_COPAY_AMT"),
    F.col("Splst_coins_in.BNF_SUM_DTL_COINS_PCT_AMT").alias("SPLST_PHYS_COINS_In"),
    F.col("Splst_coins_out.BNF_SUM_DTL_COINS_PCT_AMT").alias("SPLST_PHYS_COINS_Out"),
    F.col("Ip_fclty_coins.BNF_SUM_DTL_COINS_PCT_AMT").alias("IP_FCLTY_COINS_AMT")
)

# --------------------------------------------------------------------------------
# Trns_Bnft_pkg (CTransformerStage) - Two output links
# --------------------------------------------------------------------------------
# Out_ntwk
df_Out_ntwk = df_Bnf_Pkg_Trns_in.select(
    Padstring(F.col("PROD_ID"), ' ', 15).alias("_tmp_for_padstring_PROD_"),  # so we can reuse below
    F.col("INDV_DEDCT_AMT_Out").alias("_tmp_1"),
    F.col("FMLY_DEDCT_AMT_Out").alias("_tmp_2"),
    F.col("INDV_OOP_MAX_Out").alias("_tmp_3"),
    F.col("FMLY_OOP_Out").alias("_tmp_4"),
    F.col("PCP_COPAY_AMT").alias("_tmp_5"),
    F.col("SPLST_PHYS_COPAY_AMT").alias("_tmp_6"),
    F.col("IP_FCLTY_COPAY_AMT").alias("_tmp_7"),
    F.col("OP_FCLTY_COPAY_AMT").alias("_tmp_8"),
    F.col("ER_COPAY_AMT_Out").alias("_tmp_9"),
    F.col("ADVD_IMG_COPAY_AMT").alias("_tmp_10"),
    F.col("SPLST_PHYS_COINS_Out").alias("_tmp_11"),
    F.col("IP_FCLTY_COINS_AMT").alias("_tmp_12"),
    F.col("PROD_CMPNT_EFF_DT_SK").alias("_tmp_13"),
    F.col("PROD_CMPNT_TERM_DT_SK").alias("_tmp_14"),
    F.col("PROD_ID").alias("_tmp_15")
).select(
    F.lit("240").alias("BHI_HOME_PLN_ID"),
    Padstring(F.col("_tmp_15"), ' ', 15).alias("BNF_PCKG_ID"),
    F.lit("OON").alias("MED_BNF_RMBRMT"),
    If(
       Left(F.col("_tmp_1"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 10 - Len(DecimalToString(Trim(F.col("_tmp_1"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_1"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 10 - Len(DecimalToString(Trim(Trim(F.col("_tmp_1"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_1"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("INDV_DEDCT_AMT"),
    F.lit("+0000000000").alias("INDV_PLUS_1_DEDCT_AMT"),
    F.lit("+0000000000").alias("INDV_PLUS_2_DEDCT_AMT"),
    If(
       Left(F.col("_tmp_2"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 10 - Len(DecimalToString(Trim(F.col("_tmp_2"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_2"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 10 - Len(DecimalToString(Trim(Trim(F.col("_tmp_2"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_2"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("FMLY_DEDCT_AMT"),
    F.lit("N").alias("EMBD_DEDCT_IN"),
    If(
       Left(F.col("_tmp_3"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 10 - Len(DecimalToString(Trim(F.col("_tmp_3"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_3"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 10 - Len(DecimalToString(Trim(Trim(F.col("_tmp_3"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_3"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("INDV_OOP_MAX_AMT"),
    F.lit("+0000000000").alias("INDV_PLUS_1_OOP_MAX_AMT"),
    F.lit("+0000000000").alias("INDV_PLUS_2_OOP_MAX_AMT"),
    If(
       Left(F.col("_tmp_4"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 10 - Len(DecimalToString(Trim(F.col("_tmp_4"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_4"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 10 - Len(DecimalToString(Trim(Trim(F.col("_tmp_4"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_4"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("FMLY_OOP_MAX_AMT"),
    F.lit("N").alias("EMBD_OOP_MAX_AMT_IN"),
    If(
       Left(F.col("_tmp_5"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 10 - Len(DecimalToString(Trim(F.col("_tmp_5"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_5"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 10 - Len(DecimalToString(Trim(Trim(F.col("_tmp_5"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_5"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("PCP_COPAY_AMT"),
    If(
       Left(F.col("_tmp_6"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 10 - Len(DecimalToString(Trim(F.col("_tmp_6"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_6"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 10 - Len(DecimalToString(Trim(Trim(F.col("_tmp_6"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_6"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("SPLST_PHYS_COPAY_AMT"),
    F.lit("N").alias("SPLST_PHYS_COPAY_IN"),
    F.lit("+0000000000").alias("SPLST_PHYS_AVG_COPAY_AMT"),
    If(
       Left(F.col("_tmp_7"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 10 - Len(DecimalToString(Trim(F.col("_tmp_7"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_7"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 10 - Len(DecimalToString(Trim(Trim(F.col("_tmp_7"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_7"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("IP_FCLTY_COPAY_AMT"),
    If(
       Left(F.col("_tmp_8"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 10 - Len(DecimalToString(Trim(F.col("_tmp_8"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_8"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 10 - Len(DecimalToString(Trim(Trim(F.col("_tmp_8"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_8"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("OP_FCLTY_COPAY_AMT"),
    If(
       Left(F.col("_tmp_9"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 10 - Len(DecimalToString(Trim(F.col("_tmp_9"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_9"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 10 - Len(DecimalToString(Trim(Trim(F.col("_tmp_9"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_9"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("ER_COPAY_AMT"),
    If(
       Left(F.col("_tmp_10"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 10 - Len(DecimalToString(Trim(F.col("_tmp_10"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_10"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 10 - Len(DecimalToString(Trim(Trim(F.col("_tmp_10"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_10"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("ADVD_IMG_COPAY_AMT"),
    F.lit("N").alias("ADVD_IMG_COPAY_IN"),
    F.lit("+0000000000").alias("ADVD_IMG_AVG_COPAY_AMT"),
    If(
       Left(F.col("_tmp_11"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 5 - Len(DecimalToString(Trim(F.col("_tmp_11"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_11"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 5 - Len(DecimalToString(Trim(Trim(F.col("_tmp_11"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_11"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("SPLST_PHYS_COINS_PCT"),
    F.lit("N").alias("SPLST_PHYS_COINS_IN"),
    F.lit("+00000").alias("SPLST_PHYS_AVG_COINS_PCT"),
    If(
       Left(F.col("_tmp_12"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 5 - Len(DecimalToString(Trim(F.col("_tmp_12"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_12"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 5 - Len(DecimalToString(Trim(Trim(F.col("_tmp_12"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_12"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("IP_FCLTY_COINS_PCT"),
    If(
       Left(F.col("_tmp_11"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 5 - Len(DecimalToString(Trim(F.col("_tmp_11"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_11"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 5 - Len(DecimalToString(Trim(Trim(F.col("_tmp_11"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_11"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("OP_FCLTY_COINS_PCT"),
    If(
       Left(F.col("_tmp_11"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 5 - Len(DecimalToString(Trim(F.col("_tmp_11"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_11"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 5 - Len(DecimalToString(Trim(Trim(F.col("_tmp_11"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_11"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("ER_COINS_PCT"),
    If(
       Left(F.col("_tmp_11"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 5 - Len(DecimalToString(Trim(F.col("_tmp_11"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_11"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 5 - Len(DecimalToString(Trim(Trim(F.col("_tmp_11"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_11"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("ADVD_IMG_COINS_PCT"),
    F.lit("N").alias("ADVD_IMG_COINS_IN"),
    F.lit("+00000").alias("ADVD_IMG_AVG_COINS_PCT"),
    F.lit("+0000000000").alias("COINS_MAX_AMT"),
    Trim(F.col("_tmp_13"), "-", "A").alias("EFF_DT"),
    Trim(F.col("_tmp_14"), "-", "A").alias("EXPRTN_DT")
)

# In_ntwk
df_In_ntwk = df_Bnf_Pkg_Trns_in.select(
    Padstring(F.col("PROD_ID"), ' ', 15).alias("_tmp_for_padstring_PROD_"), 
    F.col("INDV_DEDCT_AMT_In").alias("_tmp_1"),
    F.col("FMLY_DEDCT_AMT_In").alias("_tmp_2"),
    F.col("INDV_OOP_MAX_AMT_In").alias("_tmp_3"),
    F.col("FMLY_OOP_In").alias("_tmp_4"),
    F.col("PCP_COPAY_AMT").alias("_tmp_5"),
    F.col("SPLST_PHYS_COPAY_AMT").alias("_tmp_6"),
    F.col("IP_FCLTY_COPAY_AMT").alias("_tmp_7"),
    F.col("OP_FCLTY_COPAY_AMT").alias("_tmp_8"),
    F.col("ER_COPAY_AMT_In").alias("_tmp_9"),
    F.col("ADVD_IMG_COPAY_AMT").alias("_tmp_10"),
    F.col("SPLST_PHYS_COINS_In").alias("_tmp_11"),
    F.col("IP_FCLTY_COINS_AMT").alias("_tmp_12"),
    F.col("PROD_CMPNT_EFF_DT_SK").alias("_tmp_13"),
    F.col("PROD_CMPNT_TERM_DT_SK").alias("_tmp_14"),
    F.col("PROD_ID").alias("_tmp_15")
).select(
    F.lit("240").alias("BHI_HOME_PLN_ID"),
    Padstring(F.col("_tmp_15"), ' ', 15).alias("BNF_PCKG_ID"),
    F.lit("IN10").alias("MED_BNF_RMBRMT"),
    If(
       Left(F.col("_tmp_1"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 10 - Len(DecimalToString(Trim(F.col("_tmp_1"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_1"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 10 - Len(DecimalToString(Trim(Trim(F.col("_tmp_1"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_1"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("INDV_DEDCT_AMT"),
    F.lit("+0000000000").alias("INDV_PLUS_1_DEDCT_AMT"),
    F.lit("+0000000000").alias("INDV_PLUS_2_DEDCT_AMT"),
    If(
       Left(F.col("_tmp_2"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 10 - Len(DecimalToString(Trim(F.col("_tmp_2"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_2"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 10 - Len(DecimalToString(Trim(Trim(F.col("_tmp_2"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_2"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("FMLY_DEDCT_AMT"),
    F.lit("N").alias("EMBD_DEDCT_IN"),
    If(
       Left(F.col("_tmp_3"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 10 - Len(DecimalToString(Trim(F.col("_tmp_3"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_3"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 10 - Len(DecimalToString(Trim(Trim(F.col("_tmp_3"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_3"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("INDV_OOP_MAX_AMT"),
    F.lit("+0000000000").alias("INDV_PLUS_1_OOP_MAX_AMT"),
    F.lit("+0000000000").alias("INDV_PLUS_2_OOP_MAX_AMT"),
    If(
       Left(F.col("_tmp_4"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 10 - Len(DecimalToString(Trim(F.col("_tmp_4"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_4"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 10 - Len(DecimalToString(Trim(Trim(F.col("_tmp_4"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_4"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("FMLY_OOP_MAX_AMT"),
    F.lit("N").alias("EMBD_OOP_MAX_AMT_IN"),
    If(
       Left(F.col("_tmp_5"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 10 - Len(DecimalToString(Trim(F.col("_tmp_5"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_5"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 10 - Len(DecimalToString(Trim(Trim(F.col("_tmp_5"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_5"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("PCP_COPAY_AMT"),
    If(
       Left(F.col("_tmp_6"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 10 - Len(DecimalToString(Trim(F.col("_tmp_6"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_6"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 10 - Len(DecimalToString(Trim(Trim(F.col("_tmp_6"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_6"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("SPLST_PHYS_COPAY_AMT"),
    F.lit("N").alias("SPLST_PHYS_COPAY_IN"),
    F.lit("+0000000000").alias("SPLST_PHYS_AVG_COPAY_AMT"),
    If(
       Left(F.col("_tmp_7"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 10 - Len(DecimalToString(Trim(F.col("_tmp_7"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_7"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 10 - Len(DecimalToString(Trim(Trim(F.col("_tmp_7"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_7"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("IP_FCLTY_COPAY_AMT"),
    If(
       Left(F.col("_tmp_8"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 10 - Len(DecimalToString(Trim(F.col("_tmp_8"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_8"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 10 - Len(DecimalToString(Trim(Trim(F.col("_tmp_8"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_8"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("OP_FCLTY_COPAY_AMT"),
    If(
       Left(F.col("_tmp_9"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 10 - Len(DecimalToString(Trim(F.col("_tmp_9"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_9"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 10 - Len(DecimalToString(Trim(Trim(F.col("_tmp_9"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_9"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("ER_COPAY_AMT"),
    If(
       Left(F.col("_tmp_10"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 10 - Len(DecimalToString(Trim(F.col("_tmp_10"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_10"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 10 - Len(DecimalToString(Trim(Trim(F.col("_tmp_10"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_10"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("ADVD_IMG_COPAY_AMT"),
    F.lit("N").alias("ADVD_IMG_COPAY_IN"),
    F.lit("+0000000000").alias("ADVD_IMG_AVG_COPAY_AMT"),
    If(
       Left(F.col("_tmp_11"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 5 - Len(DecimalToString(Trim(F.col("_tmp_11"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_11"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 5 - Len(DecimalToString(Trim(Trim(F.col("_tmp_11"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_11"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("SPLST_PHYS_COINS_PCT"),
    F.lit("N").alias("SPLST_PHYS_COINS_IN"),
    F.lit("+00000").alias("SPLST_PHYS_AVG_COINS_PCT"),
    If(
       Left(F.col("_tmp_12"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 5 - Len(DecimalToString(Trim(F.col("_tmp_12"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_12"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 5 - Len(DecimalToString(Trim(Trim(F.col("_tmp_12"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_12"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("IP_FCLTY_COINS_PCT"),
    If(
       Left(F.col("_tmp_11"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 5 - Len(DecimalToString(Trim(F.col("_tmp_11"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_11"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 5 - Len(DecimalToString(Trim(Trim(F.col("_tmp_11"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_11"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("OP_FCLTY_COINS_PCT"),
    If(
       Left(F.col("_tmp_11"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 5 - Len(DecimalToString(Trim(F.col("_tmp_11"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_11"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 5 - Len(DecimalToString(Trim(Trim(F.col("_tmp_11"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_11"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("ER_COINS_PCT"),
    If(
       Left(F.col("_tmp_11"), 1) != '-',
       Concat(
          F.lit('+'),
          Str('0', 5 - Len(DecimalToString(Trim(F.col("_tmp_11"), '.', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(F.col("_tmp_11"), '.', 'A'), "fix_zero,suppress_zero")
       ),
       Concat(
          F.lit('-'),
          Str('0', 5 - Len(DecimalToString(Trim(Trim(F.col("_tmp_11"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero"))),
          DecimalToString(Trim(Trim(F.col("_tmp_11"), '.', 'A'), '-', 'A'), "fix_zero,suppress_zero")
       )
    ).alias("ADVD_IMG_COINS_PCT"),
    F.lit("N").alias("ADVD_IMG_COINS_IN"),
    F.lit("+00000").alias("ADVD_IMG_AVG_COINS_PCT"),
    F.lit("+0000000000").alias("COINS_MAX_AMT"),
    Trim(F.col("_tmp_13"), "-", "A").alias("EFF_DT"),
    Trim(F.col("_tmp_14"), "-", "A").alias("EXPRTN_DT")
)

# --------------------------------------------------------------------------------
# Fnl_in_out (PxFunnel)
# --------------------------------------------------------------------------------
df_Fnl_in_out = df_Out_ntwk.unionByName(df_In_ntwk, allowMissingColumns=True).select(
    "BHI_HOME_PLN_ID",
    "BNF_PCKG_ID",
    "MED_BNF_RMBRMT",
    "INDV_DEDCT_AMT",
    "INDV_PLUS_1_DEDCT_AMT",
    "INDV_PLUS_2_DEDCT_AMT",
    "FMLY_DEDCT_AMT",
    "EMBD_DEDCT_IN",
    "INDV_OOP_MAX_AMT",
    "INDV_PLUS_1_OOP_MAX_AMT",
    "INDV_PLUS_2_OOP_MAX_AMT",
    "FMLY_OOP_MAX_AMT",
    "EMBD_OOP_MAX_AMT_IN",
    "PCP_COPAY_AMT",
    "SPLST_PHYS_COPAY_AMT",
    "SPLST_PHYS_COPAY_IN",
    "SPLST_PHYS_AVG_COPAY_AMT",
    "IP_FCLTY_COPAY_AMT",
    "OP_FCLTY_COPAY_AMT",
    "ER_COPAY_AMT",
    "ADVD_IMG_COPAY_AMT",
    "ADVD_IMG_COPAY_IN",
    "ADVD_IMG_AVG_COPAY_AMT",
    "SPLST_PHYS_COINS_PCT",
    "SPLST_PHYS_COINS_IN",
    "SPLST_PHYS_AVG_COINS_PCT",
    "IP_FCLTY_COINS_PCT",
    "OP_FCLTY_COINS_PCT",
    "ER_COINS_PCT",
    "ADVD_IMG_COINS_PCT",
    "ADVD_IMG_COINS_IN",
    "ADVD_IMG_AVG_COINS_PCT",
    "COINS_MAX_AMT",
    "EFF_DT",
    "EXPRTN_DT"
)

# --------------------------------------------------------------------------------
# Trans (CTransformerStage) -> two outputs: Extract (benefit_package_ref), Count (Aggregator)
# --------------------------------------------------------------------------------
df_Trans = df_Fnl_in_out.select(
    F.col("BHI_HOME_PLN_ID"),
    F.col("BNF_PCKG_ID"),
    F.col("MED_BNF_RMBRMT"),
    F.col("INDV_DEDCT_AMT"),
    F.col("INDV_PLUS_1_DEDCT_AMT"),
    F.col("INDV_PLUS_2_DEDCT_AMT"),
    F.col("FMLY_DEDCT_AMT"),
    F.col("EMBD_DEDCT_IN"),
    F.col("INDV_OOP_MAX_AMT"),
    F.col("INDV_PLUS_1_OOP_MAX_AMT"),
    F.col("INDV_PLUS_2_OOP_MAX_AMT"),
    F.col("FMLY_OOP_MAX_AMT"),
    F.col("EMBD_OOP_MAX_AMT_IN"),
    F.col("PCP_COPAY_AMT"),
    F.col("SPLST_PHYS_COPAY_AMT"),
    F.col("SPLST_PHYS_COPAY_IN"),
    F.col("SPLST_PHYS_AVG_COPAY_AMT"),
    F.col("IP_FCLTY_COPAY_AMT"),
    F.col("OP_FCLTY_COPAY_AMT"),
    F.col("ER_COPAY_AMT"),
    F.col("ADVD_IMG_COPAY_AMT"),
    F.col("ADVD_IMG_COPAY_IN"),
    F.col("ADVD_IMG_AVG_COPAY_AMT"),
    F.col("SPLST_PHYS_COINS_PCT"),
    F.col("SPLST_PHYS_COINS_IN"),
    F.col("SPLST_PHYS_AVG_COINS_PCT"),
    F.col("IP_FCLTY_COINS_PCT"),
    F.col("OP_FCLTY_COINS_PCT"),
    F.col("ER_COINS_PCT"),
    F.col("ADVD_IMG_COINS_PCT"),
    F.col("ADVD_IMG_COINS_IN"),
    F.col("ADVD_IMG_AVG_COINS_PCT"),
    F.col("COINS_MAX_AMT"),
    F.col("EFF_DT"),
    F.col("EXPRTN_DT")
)

df_Extract = df_Trans  # Output link "Extract"
df_Count = df_Trans.select(
    F.col("BHI_HOME_PLN_ID")
)  # Output link "Count"

# --------------------------------------------------------------------------------
# benefit_package_ref (PxSequentialFile)
# --------------------------------------------------------------------------------
write_files(
    df_Extract.select(
       "BHI_HOME_PLN_ID",
       "BNF_PCKG_ID",
       "MED_BNF_RMBRMT",
       "INDV_DEDCT_AMT",
       "INDV_PLUS_1_DEDCT_AMT",
       "INDV_PLUS_2_DEDCT_AMT",
       "FMLY_DEDCT_AMT",
       "EMBD_DEDCT_IN",
       "INDV_OOP_MAX_AMT",
       "INDV_PLUS_1_OOP_MAX_AMT",
       "INDV_PLUS_2_OOP_MAX_AMT",
       "FMLY_OOP_MAX_AMT",
       "EMBD_OOP_MAX_AMT_IN",
       "PCP_COPAY_AMT",
       "SPLST_PHYS_COPAY_AMT",
       "SPLST_PHYS_COPAY_IN",
       "SPLST_PHYS_AVG_COPAY_AMT",
       "IP_FCLTY_COPAY_AMT",
       "OP_FCLTY_COPAY_AMT",
       "ER_COPAY_AMT",
       "ADVD_IMG_COPAY_AMT",
       "ADVD_IMG_COPAY_IN",
       "ADVD_IMG_AVG_COPAY_AMT",
       "SPLST_PHYS_COINS_PCT",
       "SPLST_PHYS_COINS_IN",
       "SPLST_PHYS_AVG_COINS_PCT",
       "IP_FCLTY_COINS_PCT",
       "OP_FCLTY_COINS_PCT",
       "ER_COINS_PCT",
       "ADVD_IMG_COINS_PCT",
       "ADVD_IMG_COINS_IN",
       "ADVD_IMG_AVG_COINS_PCT",
       "COINS_MAX_AMT",
       "EFF_DT",
       "EXPRTN_DT"
    ),
    f"{adls_path_publish}/external/benefit_package_ref",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)

# --------------------------------------------------------------------------------
# Aggregator (PxAggregator) - grouping by BHI_HOME_PLN_ID, RecCount
# --------------------------------------------------------------------------------
df_Aggregator = df_Count.groupBy("BHI_HOME_PLN_ID").agg(F.count("*").alias("COUNT"))
df_Control_Count = df_Aggregator.select(
    F.col("BHI_HOME_PLN_ID").alias("BHI_HOME_PLN_ID"),
    F.col("COUNT").alias("COUNT")
)

# --------------------------------------------------------------------------------
# Trns_cntrl (CTransformerStage)
# --------------------------------------------------------------------------------
df_Trns_cntrl = df_Control_Count.select(
    F.col("BHI_HOME_PLN_ID").alias("_tmp_bhi"),
    F.col("COUNT").alias("_tmp_count")
).select(
    F.col("_tmp_bhi").alias("BHI_HOME_PLN_ID"),
    Padstring(F.lit("BENEFIT_PACKAGE_REF"), ' ', 30).alias("EXTR_NM"),
    Trim(F.col("StartDate"), "-", "A").alias("MIN_CLM_PROCESSED_DT"),
    Trim(F.col("EndDate"), "-", "A").alias("MAX_CLM_PRCS_DT"),
    Trim(F.col("CurrDate"), "-", "A").alias("SUBMSN_DT"),
    Concat(Str('0', 10 - Len(F.col("_tmp_count"))), F.col("_tmp_count")).alias("RCRD_CT"),
    If(
       Left(F.lit("00000000000000"), 1) != '-',
       Concat(F.lit('+'), F.lit("00000000000000")),
       F.lit("000000000000000")
    ).alias("TOT_SUBMT_AMT"),
    If(
       Left(F.lit("00000000000000"), 1) != '-',
       Concat(F.lit('+'), F.lit("00000000000000")),
       F.lit("000000000000000")
    ).alias("TOT_NONCOV_AMT"),
    If(
       Left(F.lit("00000000000000"), 1) != '-',
       Concat(F.lit('+'), F.lit("00000000000000")),
       F.lit("000000000000000")
    ).alias("TOT_ALW_AMT"),
    If(
       Left(F.lit("00000000000000"), 1) != '-',
       Concat(F.lit('+'), F.lit("00000000000000")),
       F.lit("000000000000000")
    ).alias("TOT_PD_AMT"),
    If(
       Left(F.lit("00000000000000"), 1) != '-',
       Concat(F.lit('+'), F.lit("00000000000000")),
       F.lit("000000000000000")
    ).alias("TOT_COB_TPL_AMT"),
    If(
       Left(F.lit("00000000000000"), 1) != '-',
       Concat(F.lit('+'), F.lit("00000000000000")),
       F.lit("000000000000000")
    ).alias("TOT_COINS_AMT"),
    If(
       Left(F.lit("00000000000000"), 1) != '-',
       Concat(F.lit('+'), F.lit("00000000000000")),
       F.lit("000000000000000")
    ).alias("TOT_COPAY_AMT"),
    If(
       Left(F.lit("00000000000000"), 1) != '-',
       Concat(F.lit('+'), F.lit("00000000000000")),
       F.lit("000000000000000")
    ).alias("TOT_DEDCT_AMT"),
    If(
       Left(F.lit("00000000000000"), 1) != '-',
       Concat(F.lit('+'), F.lit("00000000000000")),
       F.lit("000000000000000")
    ).alias("TOT_FFS_EQVLNT_AMT")
)

# --------------------------------------------------------------------------------
# submission_control (PxSequentialFile)
# --------------------------------------------------------------------------------
write_files(
    df_Trns_cntrl.select(
        "BHI_HOME_PLN_ID",
        "EXTR_NM",
        "MIN_CLM_PROCESSED_DT",
        "MAX_CLM_PRCS_DT",
        "SUBMSN_DT",
        "RCRD_CT",
        "TOT_SUBMT_AMT",
        "TOT_NONCOV_AMT",
        "TOT_ALW_AMT",
        "TOT_PD_AMT",
        "TOT_COB_TPL_AMT",
        "TOT_COINS_AMT",
        "TOT_COPAY_AMT",
        "TOT_DEDCT_AMT",
        "TOT_FFS_EQVLNT_AMT"
    ),
    f"{adls_path_publish}/external/submission_control",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)