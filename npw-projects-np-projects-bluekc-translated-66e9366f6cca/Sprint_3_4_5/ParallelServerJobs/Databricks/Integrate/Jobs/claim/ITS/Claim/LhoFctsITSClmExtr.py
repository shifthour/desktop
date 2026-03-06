# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2005 Blue Cross/Blue Shield of Kansas City
# MAGIC  Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION: Runs membership extract,  transform, primary key jobs.
# MAGIC 
# MAGIC JOB NAME:  LhoFctsITSClmExtr
# MAGIC Calling job:LhoFctsClmExtr1Seq
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 10/13/2004     O. Nielsen                Originally programmed
# MAGIC 02/07/2005     S  Andrew                IDS 3.0
# MAGIC 10/25/2005 -   BJ Luce                   TT 4388 - default Transmission status code to NA if blank or null
# MAGIC 02/15/2006     S Goddard               added transform, pkey for sequencer
# MAGIC          BJ Luce            03/20/2006     add hf_clm_nasco_dup_bypass, identifies claims that are nasco dups. If the claim is on the file, a row is not generated for it in IDS. However, an R row will be build for it if the status if '91'
# MAGIC 03/29/2006     S Goddard               added constraint to check for ITSHome or ITSHost claim
# MAGIC  04/13/2006    Brent Leland       Changed key hash file from hf_clm back to original temporaily         
# MAGIC 06/16/2006     S Goddard              change made to pull admin amount from original claim since it's not passed on to adjustment claims - production support project 313 task tracker 4551
# MAGIC                                                        mapping changes by Charlie Russell
# MAGIC 08/25/2006     S Goddard               pulled admin fee for home and host claims adjustment/reversal claims from IDS, amount is not in Facets except on original claim
# MAGIC                                                            - production support 313 changes mapped by Charlie Russell
# MAGIC                                                         deleted surcharge file - nothing is ever matched from here, this is now a separate monthly update process
# MAGIC                                                         To calculate access fee amount on adjustments - compare last two characters of SCCF number with last two characters of Claim ID to 
# MAGIC                                                             determine whether it's a void only vs. a void/reissue.  A void only should have 0, a void/reissue will have the access fee amount.
# MAGIC Sanderw  12/08/2006   Project 1756  - Reversal logix added for new status codes 89 and  and 99
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                                                            Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     ----------------------------------------------------------------------------                                               ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi D              03/20/2008             Added five new fields                                                                                         3255                 IDSCurDevl                         Steph Goddard          03/31/2008
# MAGIC 
# MAGIC SAndrew               04/17/2008             Changed business rule for Investigative days = Null if 0 on facets
# MAGIC                                                              Changed business rule for Investigative end dt to equal 2199-12-31 if it is 
# MAGIC                                                                         1753-01-01 on facets                                                                               3255                 IDSCurDevl                         Steph Goddard          04/21/2008
# MAGIC                              04/25/2008             changed criteria as to what was an ITS Home by adding test for ITS_SUB_TYPE of HT
# MAGIC                                                               changed CMC_CLMI_MISC to also test for all ITS SUB TYPES S,H,E,T
# MAGIC Bhoomi Dasari       2008-08-07              Changed primary key process from hash file to DB2 table                                  3567(Primary Key)  devlIDS                          Steph Goddard           08/15/2008
# MAGIC  
# MAGIC Bhoomi Dasari       2009-04-09             Updated logic in "DISP_FMT_PD_DT_SK", "SRCHRG_AMT" and                  4039                devlIDS                                 Steph Goddard          04/30/2009
# MAGIC                                                              "SRPLS_AMT"  
# MAGIC 
# MAGIC Manasa Andru      2014-10-17             Added SUPLMT_DSCNT_AMT field at the end.                                             TFS - 9580         IntegrateCurDevl                Kalyan Neelam             2014-10-22
# MAGIC 
# MAGIC Manasa Andru      2014-11-20            Added scale of 2 to the SUPLMT_DSCNT_AMT field.                                 TFS - 9580 PostProd Fix  IdsCurDevl               Kalyan Neelam             2014-11-20
# MAGIC 
# MAGIC Reddy Sanam      2020-11-13            Mapped 'LUMERIS' based on source system code param
# MAGIC                                                           in the BusinessRules transformer                                                                                                IntegrateDev2                                                 Manasa Andru           2020-11-13
# MAGIC Prabhu ES           2022-03-29            MSSQL ODBC conn params added                                                                S2S                     IntegrateDev5\(9)\(9)Ken Bradmon\(9)2022-06-10

# MAGIC This container is used in:
# MAGIC ITSClmExtr
# MAGIC NascoITSClmTrns
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC ITS Admin Amount is only carried on original claim, not on adjustments.  Need to get it from the original claim on IDS
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, length, substring, trim, expr, date_format, rpad
from pyspark.sql.types import *
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

 
# MAGIC %run ../../../../../shared_containers/PrimaryKey/ITSClmPK
# COMMAND ----------

DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

extract_query_ids = f"""
SELECT
  CLM.CLM_ID,
  CLM.SRC_SYS_CD_SK,
  ITS.ADM_FEE_AMT
FROM {IDSOwner}.ITS_CLM ITS,
     {IDSOwner}.CLM CLM,
     {IDSOwner}.CD_MPPNG
WHERE SRC_CD = 'FACETS'
  AND CLM.SRC_SYS_CD_SK = CD_MPPNG_SK
  AND CLM.ADJ_FROM_CLM_SK = ITS.ITS_CLM_SK
"""

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids)
    .load()
)

extract_query_cmc_clmi_misc = f"""
SELECT
  MISC.CLCL_ID,
  MISC.CLMI_ITS_FEE,
  MISC.CLMI_ITS_ADM_AMT,
  MISC.CLMI_ITS_AMT,
  MISC.CLMI_ITS_SCCF_NO,
  MISC.CLMI_CFA_DISP_DT,
  MISC.CLMI_ITS_CUR_STS,
  MISC.CLMI_ITS_SUB_TYPE,
  MISC.CLMI_INVEST_IND,
  MISC.CLMI_INVEST_DAYS,
  MISC.CLMI_INVEST_BEG_DT,
  MISC.CLMI_INVEST_END_DT,
  MISC.CLMI_SURCHG_AMT,
  CLM.CLCL_PAID_DT,
  MISC.CLMI_SUPP_DISC_AMT
FROM {FacetsOwner}.CMC_CLMI_MISC MISC,
     {FacetsOwner}.CMC_CLCL_CLAIM CLM,
     tempdb..{DriverTable} TMP
WHERE TMP.CLM_ID = MISC.CLCL_ID
  AND CLM.CLCL_ID = MISC.CLCL_ID
  AND (
       SUBSTRING(MISC.CLCL_ID, 6,1) = 'H'
       OR SUBSTRING(MISC.CLCL_ID,6,2) = 'RH'
       OR SUBSTRING(MISC.CLCL_ID,6,1) = 'K'
       OR SUBSTRING(MISC.CLCL_ID,6,1) = 'G'
       OR MISC.CLMI_ITS_SUB_TYPE IN ('S','H','E','T')
      )
"""

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
df_CMC_CLMI_MISC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_cmc_clmi_misc)
    .load()
)

df_its_clm_info = df_CMC_CLMI_MISC.select(
    expr("strip_field(CLCL_ID)").alias("CLCL_ID"),
    col("CLMI_ITS_FEE").alias("CLMI_ITS_FEE"),
    col("CLMI_ITS_ADM_AMT").alias("CLMI_ITS_ADM_AMT"),
    col("CLMI_ITS_AMT").alias("CLMI_ITS_AMT"),
    expr("strip_field(CLMI_ITS_SCCF_NO)").alias("CLMI_ITS_SCCF_NO"),
    col("CLMI_CFA_DISP_DT").alias("CLMI_CFA_DISP_DT"),
    expr("strip_field(CLMI_ITS_CUR_STS)").alias("CLMI_ITS_CUR_STS"),
    expr("strip_field(CLMI_ITS_SUB_TYPE)").alias("CLMI_ITS_SUB_TYPE"),
    expr("strip_field(CLMI_INVEST_IND)").alias("CLMI_INVEST_IND"),
    trim(col("CLMI_INVEST_DAYS")).alias("CLMI_INVEST_DAYS"),
    date_format(col("CLMI_INVEST_BEG_DT"), "yyyy-MM-dd").alias("CLMI_INVEST_BEG_DT"),
    date_format(col("CLMI_INVEST_END_DT"), "yyyy-MM-dd").alias("CLMI_INVEST_END_DT"),
    col("CLMI_SURCHG_AMT").alias("CLMI_SURCHG_AMT"),
    date_format(col("CLCL_PAID_DT"), "yyyy-MM-dd").alias("CLCL_PAID_DT"),
    expr("CASE WHEN CLMI_SUPP_DISC_AMT IS NULL OR length(CLMI_SUPP_DISC_AMT)=0 THEN 0 ELSE CLMI_SUPP_DISC_AMT END").alias("CLMI_SUPP_DISC_AMT")
)

df_determine_adjust = (
    df_its_clm_info
    .withColumn("ITSHost", when((col("CLMI_ITS_SUB_TYPE")=="S") | (col("CLMI_ITS_SUB_TYPE")=="E"), "Y").otherwise("N"))
    .withColumn("ITSHome", when(
        (substring(col("CLCL_ID"),6,1).isin("H","K","G")) |
        (substring(col("CLCL_ID"),6,2)=="RH") |
        (col("CLMI_ITS_SUB_TYPE").isin("H","T")), "Y").otherwise("N"))
    .withColumn("AdjClaim", when(substring(col("CLCL_ID"),-2,2)!="00","Y").otherwise("N"))
    .withColumn("SCCFAdj", substring(col("CLMI_ITS_SCCF_NO"), -2, 2))
    .withColumn("ClmIdDetAdj", when(substring(col("CLCL_ID"),-1,1)=="R", substring(col("CLCL_ID"),-3,3)).otherwise(substring(col("CLCL_ID"),-2,2)))
    .withColumn("ClmIDAdj", when(substring(col("ClmIdDetAdj"),-1,1)=="R", substring(col("ClmIdDetAdj"),1,2)).otherwise(col("ClmIdDetAdj")))
)

df_no_adjust = df_determine_adjust.filter(
    ((col("ITSHome")=="Y") | (col("ITSHost")=="Y")) & (col("AdjClaim")=="N")
)

df_no_adjust_enriched = df_no_adjust.select(
    trim(col("CLCL_ID")).alias("CLM_ID"),
    lit(CurrDate).alias("EXTRACT_TIMESTAMP"),
    trim(col("CLMI_ITS_CUR_STS")).alias("TRNSMSN_SRC_CD"),
    col("CLMI_CFA_DISP_DT").alias("CFA_DISP_PD_DT"),
    when(col("CLMI_SURCHG_AMT")!=0, col("CLCL_PAID_DT")).otherwise(lit("NA")).alias("DISP_FMT_PF_DT"),
    when(col("SCCFAdj")==col("ClmIDAdj"), col("CLMI_ITS_FEE")).otherwise(lit(0.0)).alias("ACES_FEE_AMT"),
    col("CLMI_ITS_ADM_AMT").alias("ADM_FEE_AMT"),
    col("CLMI_ITS_AMT").alias("DRG_AMT"),
    col("CLMI_SURCHG_AMT").alias("SRCRG_AMT"),
    lit(0.0).alias("SRPLS_AMT"),
    trim(col("CLMI_ITS_SCCF_NO")).alias("SCCF_NO"),
    when((col("CLMI_INVEST_IND").isNull()) | (length(trim(col("CLMI_INVEST_IND")))==0), lit("NA"))
      .otherwise(trim(col("CLMI_INVEST_IND"))).alias("CLMI_INVEST_IND"),
    when((col("CLMI_INVEST_DAYS").isNull()) | (length(trim(col("CLMI_INVEST_DAYS")))==0), lit("NA"))
      .otherwise(trim(col("CLMI_INVEST_DAYS"))).alias("CLMI_INVEST_DAYS"),
    when((col("CLMI_INVEST_BEG_DT").isNull()) | (length(col("CLMI_INVEST_BEG_DT"))==0), lit("1753-01-01"))
      .otherwise(col("CLMI_INVEST_BEG_DT")).alias("CLMI_INVEST_BEG_DT"),
    when((col("CLMI_INVEST_END_DT").isNull()) | (length(col("CLMI_INVEST_END_DT"))==0), lit("2199-12-31"))
      .otherwise(col("CLMI_INVEST_END_DT")).alias("CLMI_INVEST_END_DT"),
    col("CLMI_SUPP_DISC_AMT").alias("SUPLMT_DSCNT_AMT")
)

df_needs_admin = df_determine_adjust.filter(
    ((col("ITSHome")=="Y") | (col("ITSHost")=="Y")) & (col("AdjClaim")=="Y")
)

df_needs_admin_enriched = df_needs_admin.select(
    expr("substring(CLCL_ID,1,length(trim(CLCL_ID))-2) || '00'").alias("ADJ_FROM_CLM_ID"),
    col("CLCL_ID").alias("CLM_ID"),
    lit(CurrDate).alias("EXTRACT_TIMESTAMP"),
    trim(col("CLMI_ITS_CUR_STS")).alias("TRNSMSN_SRC_CD"),
    col("CLMI_CFA_DISP_DT").alias("CFA_DISP_PD_DT"),
    when(col("CLMI_SURCHG_AMT")!=0, col("CLCL_PAID_DT")).otherwise(lit("NA")).alias("DISP_FMT_PF_DT"),
    when(col("SCCFAdj")==col("ClmIDAdj"), col("CLMI_ITS_FEE")).otherwise(lit(0.0)).alias("ACES_FEE_AMT"),
    lit(0.0).alias("ADM_FEE_AMT"),
    col("CLMI_ITS_AMT").alias("DRG_AMT"),
    col("CLMI_SURCHG_AMT").alias("SRCRG_AMT"),
    lit(0.0).alias("SRPLS_AMT"),
    trim(col("CLMI_ITS_SCCF_NO")).alias("SCCF_NO"),
    when((col("CLMI_INVEST_IND").isNull()) | (length(trim(col("CLMI_INVEST_IND")))==0), lit("NA"))
      .otherwise(trim(col("CLMI_INVEST_IND"))).alias("CLMI_INVEST_IND"),
    when((col("CLMI_INVEST_DAYS").isNull()) | (length(trim(col("CLMI_INVEST_DAYS")))==0), lit("NA"))
      .otherwise(trim(col("CLMI_INVEST_DAYS"))).alias("CLMI_INVEST_DAYS"),
    when((col("CLMI_INVEST_BEG_DT").isNull()) | (length(col("CLMI_INVEST_BEG_DT"))==0), lit("1753-01-01"))
      .otherwise(col("CLMI_INVEST_BEG_DT")).alias("CLMI_INVEST_BEG_DT"),
    when((col("CLMI_INVEST_END_DT").isNull()) | (length(col("CLMI_INVEST_END_DT"))==0), lit("2199-12-31"))
      .otherwise(col("CLMI_INVEST_END_DT")).alias("CLMI_INVEST_END_DT"),
    col("CLMI_SUPP_DISC_AMT").alias("SUPLMT_DSCNT_AMT")
)

df_add_admin_amt_joined = (
    df_needs_admin_enriched.alias("needs_admin")
    .join(
        df_IDS.alias("Extract"),
        col("needs_admin.ADJ_FROM_CLM_ID") == col("Extract.CLM_ID"),
        "left"
    )
)

df_adjust = df_add_admin_amt_joined.select(
    col("needs_admin.CLM_ID").alias("CLM_ID"),
    col("needs_admin.EXTRACT_TIMESTAMP").alias("EXTRACT_TIMESTAMP"),
    col("needs_admin.TRNSMSN_SRC_CD").alias("TRNSMSN_SRC_CD"),
    col("needs_admin.CFA_DISP_PD_DT").alias("CFA_DISP_PD_DT"),
    col("needs_admin.DISP_FMT_PF_DT").alias("DISP_FMT_PF_DT"),
    col("needs_admin.ACES_FEE_AMT").alias("ACES_FEE_AMT"),
    when(col("Extract.ADM_FEE_AMT").isNull(), lit(0.0))
      .otherwise(col("Extract.ADM_FEE_AMT") * -1).alias("ADM_FEE_AMT"),
    col("needs_admin.DRG_AMT").alias("DRG_AMT"),
    col("needs_admin.SRCRG_AMT").alias("SRCRG_AMT"),
    col("needs_admin.SRPLS_AMT").alias("SRPLS_AMT"),
    col("needs_admin.SCCF_NO").alias("SCCF_NO"),
    col("needs_admin.CLMI_INVEST_IND").alias("CLMI_INVEST_IND"),
    col("needs_admin.CLMI_INVEST_DAYS").alias("CLMI_INVEST_DAYS"),
    col("needs_admin.CLMI_INVEST_BEG_DT").alias("CLMI_INVEST_BEG_DT"),
    col("needs_admin.CLMI_INVEST_END_DT").alias("CLMI_INVEST_END_DT"),
    col("needs_admin.SUPLMT_DSCNT_AMT").alias("SUPLMT_DSCNT_AMT")
)

common_collector_schema = [
    "CLM_ID",
    "EXTRACT_TIMESTAMP",
    "TRNSMSN_SRC_CD",
    "CFA_DISP_PD_DT",
    "DISP_FMT_PF_DT",
    "ACES_FEE_AMT",
    "ADM_FEE_AMT",
    "DRG_AMT",
    "SRCRG_AMT",
    "SRPLS_AMT",
    "SCCF_NO",
    "CLMI_INVEST_IND",
    "CLMI_INVEST_DAYS",
    "CLMI_INVEST_BEG_DT",
    "CLMI_INVEST_END_DT",
    "SUPLMT_DSCNT_AMT"
]

df_no_adjust_enriched_2 = df_no_adjust_enriched.select(common_collector_schema)
df_adjust_2 = df_adjust.select(common_collector_schema)
df_collector1 = df_no_adjust_enriched_2.unionByName(df_adjust_2)

df_BusinessRules_in = df_collector1.alias("Strip").withColumn("SrcSysCdSk", lit(SrcSysCdSk)) \
    .withColumn("svSrcSysCd", when(col("SrcSysCdSk")==-1951780915, "LUMERIS").otherwise("FACETS"))

df_BusinessRules_joined = (
    df_BusinessRules_in
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        trim(col("Strip.CLM_ID")) == col("fcts_reversals.CLCL_ID"),
        "left"
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        col("Strip.CLM_ID") == col("nasco_dup_lkup.CLM_ID"),
        "left"
    )
)

df_Trans = df_BusinessRules_joined.filter(col("nasco_dup_lkup.CLM_ID").isNull())
df_reversals = df_BusinessRules_joined.filter(
    (col("fcts_reversals.CLCL_ID").isNotNull()) &
    (col("fcts_reversals.CLCL_CUR_STS").isin("89","91","99"))
)

df_Trans_out = df_Trans.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("svSrcSysCd").alias("SRC_SYS_CD"),
    expr("svSrcSysCd || ';' || Strip.CLM_ID").alias("PRI_KEY_STRING"),
    lit(0).alias("ITS_CLM_SK"),
    col("Strip.CLM_ID").alias("CLM_ID"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(length(trim(col("Strip.TRNSMSN_SRC_CD")))==0, lit("NA"))
      .otherwise(col("Strip.TRNSMSN_SRC_CD")).alias("TRNSMSN_SRC_CD"),
    when(col("Strip.CFA_DISP_PD_DT").isNull() | (length(trim(col("Strip.CFA_DISP_PD_DT")))==0), lit("UNK"))
      .otherwise(expr("substring(trim(Strip.CFA_DISP_PD_DT),1,10)")).alias("CFA_DISP_PD_DT"),
    col("Strip.DISP_FMT_PF_DT").alias("DISP_FMT_PD_DT_SK"),
    col("Strip.ACES_FEE_AMT").alias("ACES_FEE_AMT"),
    col("Strip.ADM_FEE_AMT").alias("ADM_FEE_AMT"),
    col("Strip.DRG_AMT").alias("DRG_AMT"),
    col("Strip.SRCRG_AMT").alias("SRCHRG_AMT"),
    col("Strip.SRPLS_AMT").alias("SRPLS_AMT"),
    when(length(trim(col("Strip.SCCF_NO")))==0, lit("NA"))
      .otherwise(col("Strip.SCCF_NO")).alias("SCCF_NO"),
    col("Strip.CLMI_INVEST_IND").alias("CLMI_INVEST_IND"),
    when(col("Strip.CLMI_INVEST_DAYS")!=0, col("Strip.CLMI_INVEST_DAYS"))
      .otherwise(lit(None)).alias("CLMI_INVEST_DAYS"),
    col("Strip.CLMI_INVEST_BEG_DT").alias("CLMI_INVEST_BEG_DT"),
    when(col("Strip.CLMI_INVEST_END_DT")=="1753-01-01", lit("2199-12-31"))
      .otherwise(col("Strip.CLMI_INVEST_END_DT")).alias("CLMI_INVEST_END_DT"),
    col("Strip.SUPLMT_DSCNT_AMT").alias("SUPLMT_DSCNT_AMT")
)

df_reversals_out = df_reversals.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("svSrcSysCd").alias("SRC_SYS_CD"),
    expr("svSrcSysCd || ';' || trim(Strip.CLM_ID) || 'R'").alias("PRI_KEY_STRING"),
    lit(0).alias("ITS_CLM_SK"),
    expr("trim(Strip.CLM_ID) || 'R'").alias("CLM_ID"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(length(trim(col("Strip.TRNSMSN_SRC_CD")))==0, lit("NA"))
      .otherwise(col("Strip.TRNSMSN_SRC_CD")).alias("TRNSMSN_SRC_CD"),
    when(col("Strip.CFA_DISP_PD_DT").isNull() | (length(trim(col("Strip.CFA_DISP_PD_DT")))==0), lit("UNK"))
      .otherwise(expr("substring(trim(Strip.CFA_DISP_PD_DT),1,10)")).alias("CFA_DISP_PD_DT"),
    col("Strip.DISP_FMT_PF_DT").alias("DISP_FMT_PD_DT_SK"),
    expr("-1 * Strip.ACES_FEE_AMT").alias("ACES_FEE_AMT"),
    expr("-1 * Strip.ADM_FEE_AMT").alias("ADM_FEE_AMT"),
    expr("-1 * Strip.DRG_AMT").alias("DRG_AMT"),
    expr("-1 * Strip.SRCRG_AMT").alias("SRCHRG_AMT"),
    expr("-1 * Strip.SRPLS_AMT").alias("SRPLS_AMT"),
    when(length(trim(col("Strip.SCCF_NO")))==0, lit("NA"))
      .otherwise(col("Strip.SCCF_NO")).alias("SCCF_NO"),
    col("Strip.CLMI_INVEST_IND").alias("CLMI_INVEST_IND"),
    when(col("Strip.CLMI_INVEST_DAYS")!=0, col("Strip.CLMI_INVEST_DAYS"))
      .otherwise(lit(None)).alias("CLMI_INVEST_DAYS"),
    col("Strip.CLMI_INVEST_BEG_DT").alias("CLMI_INVEST_BEG_DT"),
    when(col("Strip.CLMI_INVEST_END_DT")=="1753-01-01", lit("2199-12-31"))
      .otherwise(col("Strip.CLMI_INVEST_END_DT")).alias("CLMI_INVEST_END_DT"),
    expr("-1 * Strip.SUPLMT_DSCNT_AMT").alias("SUPLMT_DSCNT_AMT")
)

common_collector2_schema = [
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "ITS_CLM_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "TRNSMSN_SRC_CD",
    "CFA_DISP_PD_DT",
    "DISP_FMT_PD_DT_SK",
    "ACES_FEE_AMT",
    "ADM_FEE_AMT",
    "DRG_AMT",
    "SRCHRG_AMT",
    "SRPLS_AMT",
    "SCCF_NO",
    "CLMI_INVEST_IND",
    "CLMI_INVEST_DAYS",
    "CLMI_INVEST_BEG_DT",
    "CLMI_INVEST_END_DT",
    "SUPLMT_DSCNT_AMT"
]

df_Trans_out2 = df_Trans_out.select(common_collector2_schema)
df_reversals_out2 = df_reversals_out.select(common_collector2_schema)
df_collector2 = df_reversals_out2.unionByName(df_Trans_out2)

df_snapShot = df_collector2

params = {
    "CurrRunCycle": CurrRunCycle
}
df_ITSClmPK_out = ITSClmPK(df_snapShot, params)

df_final = df_ITSClmPK_out.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("ITS_CLM_SK"),
    col("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("TRNSMSN_SRC_CD"), 2, " ").alias("TRNSMSN_SRC_CD"),
    rpad(col("CFA_DISP_PD_DT"), 10, " ").alias("CFA_DISP_PD_DT"),
    rpad(col("DISP_FMT_PD_DT_SK"), 10, " ").alias("DISP_FMT_PD_DT_SK"),
    col("ACES_FEE_AMT"),
    col("ADM_FEE_AMT"),
    col("DRG_AMT"),
    col("SRCHRG_AMT"),
    col("SRPLS_AMT"),
    rpad(col("SCCF_NO"), 17, " ").alias("SCCF_NO"),
    rpad(col("CLMI_INVEST_IND"), 1, " ").alias("CLMI_INVEST_IND"),
    col("CLMI_INVEST_DAYS"),
    col("CLMI_INVEST_BEG_DT"),
    col("CLMI_INVEST_END_DT"),
    col("SUPLMT_DSCNT_AMT")
)

write_files(
    df_final,
    f"{adls_path}/key/LhoFctsITSClmExtr.LhoFctsITSClm.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)