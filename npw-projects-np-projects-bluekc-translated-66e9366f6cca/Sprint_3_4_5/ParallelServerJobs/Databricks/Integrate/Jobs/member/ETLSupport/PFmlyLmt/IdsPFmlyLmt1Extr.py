# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY: IdsMbrLmtExtrSeq
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
# MAGIC Manasa Andru         10/6/2013       TFS-1275              Changed the job to two extracts to get rid of warnings                 IntegrateNewDevl           Kalyan Neelam           2013-10-09
# MAGIC 
# MAGIC Karthik Chintalapani   2016-11-14       5634                   Added new columns PLN_YR_EFF_DT and  PLN_YR_END_DT   IntegrateDev2            Kalyan Neelam           2016-11-28
# MAGIC 
# MAGIC Shanmugam A \(9) 2017-03-02         5321                    SQL needs to be changed to alias '#PrevYear#' \(9)              IntegrateDev2              Jag Yelavarthi              2017-03-07

# MAGIC Extract for prevous year - extract product data for previous year
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','100')
CurrDate = get_widget_value('CurrDate','2010-07-19')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrYear = get_widget_value('CurrYear','2010')
PrevYear = get_widget_value('PrevYear','2009')
YrEndDate = get_widget_value('YrEndDate','2009-12-31')
ProdCmpntTypCdSk = get_widget_value('ProdCmpntTypCdSk','844281128')
ClsPlnProdCatCdMedSk = get_widget_value('ClsPlnProdCatCdMedSk','1949')
ClsPlnProdCatCdDntlSk = get_widget_value('ClsPlnProdCatCdDntlSk','1946')
MbrRelshpCdSk = get_widget_value('MbrRelshpCdSk','1975')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
       ENR.SRC_SYS_CD_SK,
       SUB.SUB_UNIQ_KEY,
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
FROM
      {IDSOwner}.P_ACCUM_CTL PAC,
      {IDSOwner}.BNF_SUM_DTL BNF,
      {IDSOwner}.PROD_CMPNT PC,
      {IDSOwner}.MBR_ENR ENR,
      {IDSOwner}.W_MBR_ACCUM ENRDRVR,
      {IDSOwner}.SUB SUB,
      {IDSOwner}.MBR MBR,
      {IDSOwner}.CLS_PLN_DTL CSPD
Where PAC.BNF_SUM_DTL_ACCUM_LVL_CD = 'FMLY'
  AND (PAC.MBR_360_DPLY_IN = 'Y' or PAC.BLUEKC_DPLY_IN = 'Y')
  AND PAC.ACCUM_NO = BNF.ACCUM_NO
  AND (BNF.STOPLOSS_AMT > 0 OR BNF.LMT_AMT > 0)
  AND BNF.PROD_CMPNT_PFX_ID = PC.PROD_CMPNT_PFX_ID
  AND PC.PROD_CMPNT_TYP_CD_SK = {ProdCmpntTypCdSk}
  AND ENR.EFF_DT_SK <= '{YrEndDate}'
  AND CSPD.GRP_SK=ENR.GRP_SK
  AND CSPD.CLS_PLN_SK=ENR.CLS_PLN_SK
  AND CSPD.CLS_SK=ENR.CLS_SK
  AND CSPD.PROD_SK=ENR.PROD_SK
  AND CSPD.EFF_DT_SK<='{PrevYear}-12-31'
  AND CSPD.TERM_DT_SK>='{PrevYear}-01-01'
  AND ENR.PROD_SK = PC.PROD_SK
  AND ENR.ELIG_IN = 'Y'
  AND ENRDRVR.TERM_DT_SK = ENR.TERM_DT_SK
  AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK IN ({ClsPlnProdCatCdMedSk},{ClsPlnProdCatCdDntlSk})
  AND ENRDRVR.MBR_UNIQ_KEY = ENR.MBR_UNIQ_KEY
  AND ENRDRVR.AS_OF_DT_SK BETWEEN PC.PROD_CMPNT_EFF_DT_SK AND PC.PROD_CMPNT_TERM_DT_SK
  AND PAC.PROD_ACCUM_ID = ENRDRVR.PROD_ACCUM_ID
  AND MBR.MBR_SK = ENR.MBR_SK
  AND MBR.SUB_SK = SUB.SUB_SK
  AND MBR.MBR_RELSHP_CD_SK = {MbrRelshpCdSk}
"""

df_Extract_Link = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_XfmPrior = (
    df_Extract_Link
    .withColumn(
        "svPlnEffDt",
        F.when(
            F.length(F.col("PLN_BEG_DT_MO_DAY")) == 3,
            F.concat(
                F.lit(PrevYear), F.lit('-'), F.lit('0'),
                F.substring(F.col("PLN_BEG_DT_MO_DAY"), 1, 1),
                F.lit('-'),
                F.substring(F.col("PLN_BEG_DT_MO_DAY"), 2, 2)
            )
        ).otherwise(
            F.concat(
                F.lit(PrevYear), F.lit('-'),
                F.substring(F.col("PLN_BEG_DT_MO_DAY"), 1, 2),
                F.lit('-'),
                F.substring(F.col("PLN_BEG_DT_MO_DAY"), 3, 2)
            )
        )
    )
    .withColumn(
        "svPlnEndDt",
        F.when(
            F.length(F.col("PLN_BEG_DT_MO_DAY")) == 3,
            F.concat(
                F.lit(CurrYear), F.lit('-'), F.lit('0'),
                F.substring(F.col("PLN_BEG_DT_MO_DAY"), 1, 1),
                F.lit('-'),
                F.substring(F.col("PLN_BEG_DT_MO_DAY"), 2, 2)
            )
        ).otherwise(
            F.concat(
                F.lit(CurrYear), F.lit('-'),
                F.substring(F.col("PLN_BEG_DT_MO_DAY"), 1, 2),
                F.lit('-'),
                F.substring(F.col("PLN_BEG_DT_MO_DAY"), 3, 2)
            )
        )
    )
)

df_PriorEnrollOut = df_XfmPrior.withColumn(
    "PLN_YR_END_DT",
    FIND_DATE(
        F.col("svPlnEndDt"),
        "-1",
        "D",
        "X",
        "CCYY-MM-DD"
    )
).withColumnRenamed(
    "svPlnEffDt",
    "PLN_YR_EFF_DT"
)

df_PriorEnrollOut = df_PriorEnrollOut.withColumn(
    "BLUEKC_DPLY_IN",
    F.rpad(F.col("BLUEKC_DPLY_IN"), 1, " ")
).withColumn(
    "MBR_360_DPLY_IN",
    F.rpad(F.col("MBR_360_DPLY_IN"), 1, " ")
)

df_PriorEnrollOut = df_PriorEnrollOut.select(
    "SRC_SYS_CD_SK",
    "SUB_UNIQ_KEY",
    "PROD_ACCUM_ID",
    "ACCUM_NO",
    "BNF_SUM_DTL_NTWK_TYP_CD_SK",
    "BNF_SUM_DTL_TYP_CD_SK",
    "BLUEKC_DPLY_IN",
    "MBR_360_DPLY_IN",
    "EXTRNL_DPLY_ACCUM_DESC",
    "INTRNL_DPLY_ACCUM_DESC",
    "LMT_AMT",
    "STOPLOSS_AMT",
    "PROD_SK",
    "YR_NO",
    "PLN_YR_EFF_DT",
    "PLN_YR_END_DT"
)

write_files(
    df_PriorEnrollOut,
    f"{adls_path_raw}/landing/PFmlyLmtPriorEnroll.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)