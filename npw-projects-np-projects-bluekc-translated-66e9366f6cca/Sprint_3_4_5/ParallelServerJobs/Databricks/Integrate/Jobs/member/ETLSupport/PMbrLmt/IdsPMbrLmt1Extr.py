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
# MAGIC Manasa Andru         10/6/2013       TFS-1275              Changed the job to two extracts to get rid of warnings                 IntegrateNewDevl            Kalyan neelam           2013-10-09
# MAGIC 
# MAGIC 
# MAGIC Karthik Chintalapani   2016-11-14       5634                   Added new columns PLN_YR_EFF_DT and  PLN_YR_END_DT   IntegrateDev2             Kalyan Neelam          2016-11-28
# MAGIC 
# MAGIC Shanmugam A \(9) 2017-03-02         5321                   SQL needs to be changed to alias '#PrevYear#'                            IntegrateDev2               Jag Yelavarthi            2017-03-07

# MAGIC Extract for prior year - extract product data for prior year
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, length, substring, concat, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

jdbc_url = None
jdbc_props = None

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
       {PrevYear} AS YR_NO,
       CSPD.PLN_BEG_DT_MO_DAY
FROM [{IDSOwner}].P_ACCUM_CTL PAC,
     [{IDSOwner}].BNF_SUM_DTL BNF,
     [{IDSOwner}].PROD_CMPNT PC,
     [{IDSOwner}].MBR_ENR ENR,
     [{IDSOwner}].W_MBR_ACCUM ENRDRVR,
     [{IDSOwner}].CLS_PLN_DTL  CSPD
WHERE PAC.BNF_SUM_DTL_ACCUM_LVL_CD = 'INDV'
  AND (PAC.MBR_360_DPLY_IN = 'Y' or PAC.BLUEKC_DPLY_IN = 'Y')
  AND PAC.ACCUM_NO = BNF.ACCUM_NO
  AND (BNF.STOPLOSS_AMT > 0 OR BNF.LMT_AMT > 0)
  AND BNF.PROD_CMPNT_PFX_ID = PC.PROD_CMPNT_PFX_ID
  AND PC.PROD_CMPNT_TYP_CD_SK = {ProdCmpntTypCdSk}
  AND CSPD.GRP_SK=ENR.GRP_SK
  AND CSPD.CLS_PLN_SK=ENR.CLS_PLN_SK
  AND CSPD.CLS_SK=ENR.CLS_SK
  AND CSPD.PROD_SK=ENR.PROD_SK
  AND CSPD.EFF_DT_SK<='{PrevYear}-12-31'
  AND CSPD.TERM_DT_SK>='{PrevYear}-01-01'
  AND ENR.EFF_DT_SK <= '{YrEndDate}'
  AND ENR.PROD_SK = PC.PROD_SK
  AND ENR.ELIG_IN = 'Y'
  AND ENRDRVR.TERM_DT_SK = ENR.TERM_DT_SK
  AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK IN ({ClsPlnProdCatCdMedSk},{ClsPlnProdCatCdDntlSk})
  AND ENRDRVR.MBR_UNIQ_KEY = ENR.MBR_UNIQ_KEY
  AND ENRDRVR.AS_OF_DT_SK BETWEEN PC.PROD_CMPNT_EFF_DT_SK AND PC.PROD_CMPNT_TERM_DT_SK
  AND PAC.PROD_ACCUM_ID = ENRDRVR.PROD_ACCUM_ID
"""

df_WExtractLink = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Xfm_PriorYear = df_WExtractLink.withColumn(
    "svPlnEffDt",
    when(
        length(col("PLN_BEG_DT_MO_DAY")) == 3,
        concat(lit(PrevYear), lit("-0"), substring("PLN_BEG_DT_MO_DAY", 1, 1), lit("-"), substring("PLN_BEG_DT_MO_DAY", 2, 2))
    ).otherwise(
        concat(lit(PrevYear), lit("-"), substring("PLN_BEG_DT_MO_DAY", 1, 2), lit("-"), substring("PLN_BEG_DT_MO_DAY", 3, 2))
    )
).withColumn(
    "svPlnEndDt",
    when(
        length(col("PLN_BEG_DT_MO_DAY")) == 3,
        concat(lit(CurrYear), lit("-0"), substring("PLN_BEG_DT_MO_DAY", 1, 1), lit("-"), substring("PLN_BEG_DT_MO_DAY", 2, 2))
    ).otherwise(
        concat(lit(CurrYear), lit("-"), substring("PLN_BEG_DT_MO_DAY", 1, 2), lit("-"), substring("PLN_BEG_DT_MO_DAY", 3, 2))
    )
).withColumn(
    "PLN_YR_END_DT",
    FIND.DATE(col("svPlnEndDt"), lit("-1"), lit("D"), lit("X"), lit("CCYY-MM-DD"))
)

df_PriorEnroll = df_Xfm_PriorYear.select(
    col("SRC_SYS_CD_SK"),
    col("MBR_UNIQ_KEY"),
    col("PROD_ACCUM_ID"),
    col("ACCUM_NO"),
    col("BNF_SUM_DTL_NTWK_TYP_CD_SK"),
    col("BNF_SUM_DTL_TYP_CD_SK"),
    rpad(col("BLUEKC_DPLY_IN"), 1, " ").alias("BLUEKC_DPLY_IN"),
    rpad(col("MBR_360_DPLY_IN"), 1, " ").alias("MBR_360_DPLY_IN"),
    col("EXTRNL_DPLY_ACCUM_DESC"),
    col("INTRNL_DPLY_ACCUM_DESC"),
    col("LMT_AMT"),
    col("STOPLOSS_AMT"),
    col("PROD_SK"),
    col("YR_NO"),
    col("svPlnEffDt").alias("PLN_YR_EFF_DT"),
    col("PLN_YR_END_DT").alias("PLN_YR_END_DT")
)

write_files(
    df_PriorEnroll,
    f"{adls_path_raw}/landing/PMbrLmtPriorEnroll.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)