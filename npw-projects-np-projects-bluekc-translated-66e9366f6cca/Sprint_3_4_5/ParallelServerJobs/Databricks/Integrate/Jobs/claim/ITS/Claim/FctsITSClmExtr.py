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
# MAGIC Prabhu ES            2022-02-28            MSSQL connection parameters added                                                          S2S Remediation   IntegrateDev5                   Kalyan Neelam             2022-06-10

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
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, lit, when, date_format, length, coalesce, to_timestamp, expr, isnan
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# ------------------------------------------------------------------------------
# JOB: FctsITSClmExtr
# ------------------------------------------------------------------------------

# 1) Retrieve all required parameter values (including secret_name parameters).
DriverTable = get_widget_value('DriverTable','TMP_IDS_CLAIM')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','20090410')
CurrDate = get_widget_value('CurrDate','2009-04-10')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105838')

# ------------------------------------------------------------------------------
# 2) Read Hashed Files (Scenario C) as Parquet
# ------------------------------------------------------------------------------

# -- hf_clm_fcts_reversals
schema_hf_clm_fcts_reversals = StructType([
    StructField("CLCL_ID", StringType(), False),
    StructField("CLCL_CUR_STS", StringType(), False),
    StructField("CLCL_PAID_DT", StringType(), False),
    StructField("CLCL_ID_ADJ_TO", StringType(), False),
    StructField("CLCL_ID_ADJ_FROM", StringType(), False)
])
df_hf_clm_fcts_reversals = spark.read.schema(schema_hf_clm_fcts_reversals).parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

# -- clm_nasco_dup_bypass
schema_clm_nasco_dup_bypass = StructType([
    StructField("CLM_ID", StringType(), False)
])
df_clm_nasco_dup_bypass = spark.read.schema(schema_clm_nasco_dup_bypass).parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# ------------------------------------------------------------------------------
# 3) Read from CMC_CLMI_MISC (ODBCConnector) using FacetsOwner
# ------------------------------------------------------------------------------
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

extract_query_cmc_clmi_misc = (
    f"SELECT\n"
    f"MISC.CLCL_ID,\n"
    f"MISC.CLMI_ITS_FEE,\n"
    f"MISC.CLMI_ITS_ADM_AMT,\n"
    f"MISC.CLMI_ITS_AMT,\n"
    f"MISC.CLMI_ITS_SCCF_NO,\n"
    f"MISC.CLMI_CFA_DISP_DT,\n"
    f"MISC.CLMI_ITS_CUR_STS,\n"
    f"MISC.CLMI_ITS_SUB_TYPE,\n"
    f"MISC.CLMI_INVEST_IND,\n"
    f"MISC.CLMI_INVEST_DAYS,\n"
    f"MISC.CLMI_INVEST_BEG_DT,\n"
    f"MISC.CLMI_INVEST_END_DT,\n"
    f"MISC.CLMI_SURCHG_AMT,\n"
    f"CLM.CLCL_PAID_DT,\n"
    f"MISC.CLMI_SUPP_DISC_AMT\n"
    f"FROM {FacetsOwner}.CMC_CLMI_MISC MISC,\n"
    f"     {FacetsOwner}.CMC_CLCL_CLAIM CLM,\n"
    f"     tempdb..#{DriverTable} TMP\n"
    f"WHERE TMP.CLM_ID = MISC.CLCL_ID\n"
    f"  AND CLM.CLCL_ID = MISC.CLCL_ID\n"
    f"  AND (\n"
    f"     SUBSTRING(MISC.CLCL_ID, 6,1) = 'H'\n"
    f"     OR SUBSTRING(MISC.CLCL_ID,6,2) = 'RH'\n"
    f"     OR SUBSTRING(MISC.CLCL_ID,6,1) = 'K'\n"
    f"     OR SUBSTRING(MISC.CLCL_ID,6,1) = 'G'\n"
    f"     OR MISC.CLMI_ITS_SUB_TYPE IN ('S','H','E','T')\n"
    f"  )"
)

df_CMC_CLMI_MISC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_cmc_clmi_misc)
    .load()
)

# ------------------------------------------------------------------------------
# 4) StripField Stage (Transformer) => Output link "its_clm_info"
#    Using user-defined strip_field(...) and date transformations
# ------------------------------------------------------------------------------
df_its_clm_info = (
    df_CMC_CLMI_MISC
    .withColumn("CLCL_ID", strip_field(col("CLCL_ID")))
    .withColumn("CLMI_ITS_FEE", col("CLMI_ITS_FEE"))
    .withColumn("CLMI_ITS_ADM_AMT", col("CLMI_ITS_ADM_AMT"))
    .withColumn("CLMI_ITS_AMT", col("CLMI_ITS_AMT"))
    .withColumn("CLMI_ITS_SCCF_NO", strip_field(col("CLMI_ITS_SCCF_NO")))
    .withColumn("CLMI_CFA_DISP_DT", col("CLMI_CFA_DISP_DT"))
    .withColumn("CLMI_ITS_CUR_STS", strip_field(col("CLMI_ITS_CUR_STS")))
    .withColumn("CLMI_ITS_SUB_TYPE", strip_field(col("CLMI_ITS_SUB_TYPE")))
    .withColumn("CLMI_INVEST_IND", strip_field(col("CLMI_INVEST_IND")))
    .withColumn("CLMI_INVEST_DAYS", trim(col("CLMI_INVEST_DAYS")))
    .withColumn("CLMI_INVEST_BEG_DT", when(
        (col("CLMI_INVEST_BEG_DT").isNull()) | (length(col("CLMI_INVEST_BEG_DT")) == 0),
        lit(None)
    ).otherwise(date_format(col("CLMI_INVEST_BEG_DT"), "yyyy-MM-dd")))
    .withColumn("CLMI_INVEST_END_DT", when(
        (col("CLMI_INVEST_END_DT").isNull()) | (length(col("CLMI_INVEST_END_DT")) == 0),
        lit(None)
    ).otherwise(date_format(col("CLMI_INVEST_END_DT"), "yyyy-MM-dd")))
    .withColumn("CLMI_SURCHG_AMT", col("CLMI_SURCHG_AMT"))
    .withColumn("CLCL_PAID_DT", when(
        (col("CLCL_PAID_DT").isNull()) | (length(col("CLCL_PAID_DT")) == 0),
        lit(None)
    ).otherwise(date_format(col("CLCL_PAID_DT"), "yyyy-MM-dd")))
    .withColumn("CLMI_SUPP_DISC_AMT", when(
        (col("CLMI_SUPP_DISC_AMT").isNull()) | (length(col("CLMI_SUPP_DISC_AMT")) == 0),
        lit("0")
    ).otherwise(col("CLMI_SUPP_DISC_AMT")))
)

# ------------------------------------------------------------------------------
# 5) determine_adjust Stage (Transformer)
#    => Produces two output links: "no_adjust" and "needs_admin"
# ------------------------------------------------------------------------------
df_determine_adjust = (
    df_its_clm_info
    .withColumn("ITSHost", when((col("CLMI_ITS_SUB_TYPE") == "S") | (col("CLMI_ITS_SUB_TYPE") == "E"), lit("Y")).otherwise(lit("N")))
    .withColumn("ITSHome", when(
        (col("CLCL_ID").substr(6,1) == "H") |
        (col("CLCL_ID").substr(6,1) == "K") |
        (col("CLCL_ID").substr(6,1) == "G") |
        (col("CLCL_ID").substr(6,2) == "RH") |
        (col("CLMI_ITS_SUB_TYPE").isin("H","T")),
        lit("Y")
    ).otherwise(lit("N")))
    .withColumn("AdjClaim", when(col("CLCL_ID").substr(-2,2) != "00", lit("Y")).otherwise(lit("N")))
    .withColumn("SCCFAdj", col("CLMI_ITS_SCCF_NO").substr(-2,2))
    .withColumn("ClmIdDetAdj", when(
        col("CLCL_ID").substr(-1,1) == lit("R"), col("CLCL_ID").substr(-3,3)
    ).otherwise(col("CLCL_ID").substr(-2,2)))
    .withColumn("ClmIDAdj", when(
        col("ClmIdDetAdj").substr(-1,1) == lit("R"),
        col("ClmIdDetAdj").substr(1,2)
    ).otherwise(col("ClmIdDetAdj")))
)

# "no_adjust" (constraint: (ITSHome='Y' or ITSHost='Y') and AdjClaim='N')
df_no_adjust = (
    df_determine_adjust
    .filter(
        (
            (col("ITSHome") == lit("Y")) | (col("ITSHost") == lit("Y"))
        ) & (col("AdjClaim") == lit("N"))
    )
    .select(
        trim(col("CLCL_ID")).alias("CLM_ID"),
        lit(CurrDate).alias("EXTRACT_TIMESTAMP"),
        when(length(trim(col("CLMI_ITS_CUR_STS"))) == 0, lit("NA")).otherwise(col("CLMI_ITS_CUR_STS")).alias("TRNSMSN_SRC_CD"),
        col("CLMI_CFA_DISP_DT").alias("CFA_DISP_PD_DT"),
        when(col("CLMI_SURCHG_AMT") != 0, col("CLCL_PAID_DT")).otherwise(lit("NA")).alias("DISP_FMT_PF_DT"),
        when(col("SCCFAdj") == col("ClmIDAdj"), col("CLMI_ITS_FEE")).otherwise(lit("0.00")).alias("ACES_FEE_AMT"),
        col("CLMI_ITS_ADM_AMT").alias("ADM_FEE_AMT"),
        col("CLMI_ITS_AMT").alias("DRG_AMT"),
        col("CLMI_SURCHG_AMT").alias("SRCRG_AMT"),
        lit("0.00").alias("SRPLS_AMT"),
        trim(col("CLMI_ITS_SCCF_NO")).alias("SCCF_NO"),
        when(
            (col("CLMI_INVEST_IND").isNull()) | (length(trim(col("CLMI_INVEST_IND"))) == 0),
            lit("NA")
        ).otherwise(trim(col("CLMI_INVEST_IND"))).alias("CLMI_INVEST_IND"),
        when(
            (col("CLMI_INVEST_DAYS").isNull()) | (length(trim(col("CLMI_INVEST_DAYS"))) == 0),
            lit("NA")
        ).otherwise(trim(col("CLMI_INVEST_DAYS"))).alias("CLMI_INVEST_DAYS"),
        when(
            (col("CLMI_INVEST_BEG_DT").isNull()) | (length(col("CLMI_INVEST_BEG_DT")) == 0),
            lit("1753-01-01")
        ).otherwise(col("CLMI_INVEST_BEG_DT")).alias("CLMI_INVEST_BEG_DT"),
        when(
            (col("CLMI_INVEST_END_DT").isNull()) | (length(col("CLMI_INVEST_END_DT")) == 0),
            lit("2199-12-31")
        ).otherwise(col("CLMI_INVEST_END_DT")).alias("CLMI_INVEST_END_DT"),
        col("CLMI_SUPP_DISC_AMT").alias("SUPLMT_DSCNT_AMT")
    )
)

# "needs_admin" (constraint: (ITSHome='Y' or ITSHost='Y') and AdjClaim='Y')
df_needs_admin = (
    df_determine_adjust
    .filter(
        (
            (col("ITSHome") == lit("Y")) | (col("ITSHost") == lit("Y"))
        ) & (col("AdjClaim") == lit("Y"))
    )
    .select(
        expr("left(CLCL_ID, length(trim(CLCL_ID)) - 2) || '00'").alias("ADJ_FROM_CLM_ID"),
        col("CLCL_ID").alias("CLM_ID"),
        lit(CurrDate).alias("EXTRACT_TIMESTAMP"),
        when(length(trim(col("CLMI_ITS_CUR_STS"))) == 0, lit("NA")).otherwise(col("CLMI_ITS_CUR_STS")).alias("TRNSMSN_SRC_CD"),
        col("CLMI_CFA_DISP_DT").alias("CFA_DISP_PD_DT"),
        when(col("CLMI_SURCHG_AMT") != 0, col("CLCL_PAID_DT")).otherwise(lit("NA")).alias("DISP_FMT_PF_DT"),
        when(col("SCCFAdj") == col("ClmIDAdj"), col("CLMI_ITS_FEE")).otherwise(lit("0.00")).alias("ACES_FEE_AMT"),
        lit("0.00").alias("ADM_FEE_AMT"),
        col("CLMI_ITS_AMT").alias("DRG_AMT"),
        col("CLMI_SURCHG_AMT").alias("SRCRG_AMT"),
        lit("0.00").alias("SRPLS_AMT"),
        trim(col("CLMI_ITS_SCCF_NO")).alias("SCCF_NO"),
        when(
            (col("CLMI_INVEST_IND").isNull()) | (length(trim(col("CLMI_INVEST_IND"))) == 0),
            lit("NA")
        ).otherwise(trim(col("CLMI_INVEST_IND"))).alias("CLMI_INVEST_IND"),
        when(
            (col("CLMI_INVEST_DAYS").isNull()) | (length(trim(col("CLMI_INVEST_DAYS"))) == 0),
            lit("NA")
        ).otherwise(trim(col("CLMI_INVEST_DAYS"))).alias("CLMI_INVEST_DAYS"),
        when(
            (col("CLMI_INVEST_BEG_DT").isNull()) | (length(col("CLMI_INVEST_BEG_DT")) == 0),
            lit("1753-01-01")
        ).otherwise(col("CLMI_INVEST_BEG_DT")).alias("CLMI_INVEST_BEG_DT"),
        when(
            (col("CLMI_INVEST_END_DT").isNull()) | (length(col("CLMI_INVEST_END_DT")) == 0),
            lit("2199-12-31")
        ).otherwise(col("CLMI_INVEST_END_DT")).alias("CLMI_INVEST_END_DT"),
        col("CLMI_SUPP_DISC_AMT").alias("SUPLMT_DSCNT_AMT")
    )
)

# ------------------------------------------------------------------------------
# 6) add_admin_amt Stage (Transformer)
#    - Primary Link: df_needs_admin
#    - Lookup Link: "IDS" with condition "needs_admin.ADJ_FROM_CLM_ID = Extract.CLM_ID"
#    This "IDS" DB2Connector has "WHERE CLM.CLM_ID=?" in the original DS job.
#    => Implement by creating a staging table from df_needs_admin, then join from #$IDSOwner#.ITS_CLM, #$IDSOwner#.CLM, #$IDSOwner#.CD_MPPNG
# ------------------------------------------------------------------------------

# 6a) Write df_needs_admin to a STAGING table so we can join to it.
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
execute_dml(f"DROP TABLE IF EXISTS STAGING.FctsITSClmExtr_add_admin_amt_temp", jdbc_url_ids, jdbc_props_ids)

cols_for_temp = df_needs_admin.columns
# Create the STAGING.FctsITSClmExtr_add_admin_amt_temp table (all columns as strings or best-effort to preserve).
# We must physically write it. Spark can create the table with .save() in an overwrite mode.
df_needs_admin.write.jdbc(
    url=jdbc_url_ids,
    table="STAGING.FctsITSClmExtr_add_admin_amt_temp",
    mode="overwrite",
    properties=jdbc_props_ids
)

# 6b) Read from the DB with left join logic
# We incorporate the original filter: 
# "SELECT CLM.CLM_ID, CLM.SRC_SYS_CD_SK, ITS.ADM_FEE_AMT 
#  FROM #$IDSOwner#.ITS_CLM ITS, #$IDSOwner#.CLM CLM, #$IDSOwner#.CD_MPPNG
#  WHERE CLM.CLM_ID=? AND SRC_CD='FACETS' AND CD_MPPNG_SK=CLM.SRC_SYS_CD_SK AND CLM.ADJ_FROM_CLM_SK=ITS.ITS_CLM_SK"
# replaced ? with temp table join: STAGING.FctsITSClmExtr_add_admin_amt_temp.ADJ_FROM_CLM_ID

extract_query_ids = (
    f"SELECT CLM.CLM_ID AS CLM_ID, "
    f"       CLM.SRC_SYS_CD_SK AS SRC_SYS_CD_SK, "
    f"       ITS.ADM_FEE_AMT AS ADM_FEE_AMT "
    f"FROM {IDSOwner}.ITS_CLM ITS, "
    f"     {IDSOwner}.CLM CLM, "
    f"     {IDSOwner}.CD_MPPNG MPP, "
    f"     STAGING.FctsITSClmExtr_add_admin_amt_temp TMP "
    f"WHERE CLM.CLM_ID = TMP.ADJ_FROM_CLM_ID "
    f"  AND MPP.SRC_CD='FACETS' "
    f"  AND MPP.CD_MPPNG_SK = CLM.SRC_SYS_CD_SK "
    f"  AND CLM.ADJ_FROM_CLM_SK = ITS.ITS_CLM_SK"
)
df_ids_lookup = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids)
    .load()
)

# 6c) Perform the left join in PySpark.
df_add_admin_joined = (
    df_needs_admin.alias("needs_admin")
    .join(df_ids_lookup.alias("Extract"),
          col("needs_admin.ADJ_FROM_CLM_ID") == col("Extract.CLM_ID"),
          "left"
         )
)

# 6d) Produce the output link "adjust"
df_adjust = (
    df_add_admin_joined
    .select(
        col("needs_admin.CLM_ID").alias("CLM_ID"),
        col("needs_admin.EXTRACT_TIMESTAMP").alias("EXTRACT_TIMESTAMP"),
        col("needs_admin.TRNSMSN_SRC_CD").alias("TRNSMSN_SRC_CD"),
        col("needs_admin.CFA_DISP_PD_DT").alias("CFA_DISP_PD_DT"),
        col("needs_admin.DISP_FMT_PF_DT").alias("DISP_FMT_PF_DT"),
        col("needs_admin.ACES_FEE_AMT").alias("ACES_FEE_AMT"),
        when(
            col("Extract.ADM_FEE_AMT").isNull(),
            lit("0.00")
        ).otherwise(col("Extract.ADM_FEE_AMT") * -1).alias("ADM_FEE_AMT"),
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
)

# ------------------------------------------------------------------------------
# 7) collector1 Stage => collects "df_no_adjust" and "df_adjust"
#    Round-Robin approach simply means union with the same schema
# ------------------------------------------------------------------------------
common_cols_collector1 = [
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

df_no_adjust_aligned = df_no_adjust.select(common_cols_collector1)
df_adjust_aligned = df_adjust.select(common_cols_collector1)

df_collector1 = df_no_adjust_aligned.union(df_adjust_aligned)

# ------------------------------------------------------------------------------
# 8) BusinessRules Stage (Transformer)
#    - Primary link: df_collector1 (alias "Strip")
#    - Lookup link 1: df_hf_clm_fcts_reversals (alias "fcts_reversals"),
#      join condition: trim(Strip.CLM_ID) = fcts_reversals.CLCL_ID
#    - Lookup link 2: df_clm_nasco_dup_bypass (alias "nasco_dup_lkup"),
#      join condition: Strip.CLM_ID = nasco_dup_lkup.CLM_ID
#
#    => Then produces two outputs: "Trans" and "reversals"
# ------------------------------------------------------------------------------
# Prepare for the joins
df_collector1_enriched = (
    df_collector1
    .withColumn("CLM_ID_trim", trim(col("CLM_ID")))
    .alias("Strip")
    .join(
        df_hf_clm_fcts_reversals.withColumnRenamed("CLCL_ID","CLCL_ID_hf").alias("fcts_reversals"),
        col("Strip.CLM_ID_trim") == col("fcts_reversals.CLCL_ID_hf"),
        "left"
    )
    .join(
        df_clm_nasco_dup_bypass.withColumnRenamed("CLM_ID","CLM_ID_nasco").alias("nasco_dup_lkup"),
        col("Strip.CLM_ID") == col("nasco_dup_lkup.CLM_ID_nasco"),
        "left"
    )
)

# "Trans" constraint: IsNull(nasco_dup_lkup.CLM_ID) = @TRUE 
# meaning no match => nasco_dup_lkup.CLM_ID_nasco is null
df_businessrules_trans = (
    df_collector1_enriched
    .filter(col("nasco_dup_lkup.CLM_ID_nasco").isNull())
    .select(
        lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        lit("I").alias("INSRT_UPDT_CD"),
        lit("N").alias("DISCARD_IN"),
        lit("Y").alias("PASS_THRU_IN"),
        lit(CurrDate).alias("FIRST_RECYC_DT"),
        lit(0).alias("ERR_CT"),
        lit(0).alias("RECYCLE_CT"),
        lit("FACETS").alias("SRC_SYS_CD"),
        expr("'FACETS' || ';' || Strip.CLM_ID").alias("PRI_KEY_STRING"),
        lit(0).alias("ITS_CLM_SK"),
        col("Strip.CLM_ID").alias("CLM_ID"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        when(length(trim(col("Strip.TRNSMSN_SRC_CD"))) == 0, lit("NA")).otherwise(col("Strip.TRNSMSN_SRC_CD")).alias("TRNSMSN_SRC_CD"),
        when(
            col("Strip.CFA_DISP_PD_DT").isNull() | (length(trim(col("Strip.CFA_DISP_PD_DT"))) == 0),
            lit("UNK")
        ).otherwise(expr("left(trim(Strip.CFA_DISP_PD_DT),10)")).alias("CFA_DISP_PD_DT"),
        col("Strip.DISP_FMT_PF_DT").alias("DISP_FMT_PD_DT_SK"),
        col("Strip.ACES_FEE_AMT").alias("ACES_FEE_AMT"),
        col("Strip.ADM_FEE_AMT").alias("ADM_FEE_AMT"),
        col("Strip.DRG_AMT").alias("DRG_AMT"),
        col("Strip.SRCRG_AMT").alias("SRCHRG_AMT"),
        col("Strip.SRPLS_AMT").alias("SRPLS_AMT"),
        when(length(col("Strip.SCCF_NO")) == 0, lit("NA")).otherwise(col("Strip.SCCF_NO")).alias("SCCF_NO"),
        col("Strip.CLMI_INVEST_IND").alias("CLMI_INVEST_IND"),
        when(col("Strip.CLMI_INVEST_DAYS") != lit("0"), col("Strip.CLMI_INVEST_DAYS")).otherwise(lit(None)).alias("CLMI_INVEST_DAYS"),
        col("Strip.CLMI_INVEST_BEG_DT").alias("CLMI_INVEST_BEG_DT"),
        when(col("Strip.CLMI_INVEST_END_DT") == lit("1753-01-01"), lit("2199-12-31")).otherwise(col("Strip.CLMI_INVEST_END_DT")).alias("CLMI_INVEST_END_DT"),
        col("Strip.SUPLMT_DSCNT_AMT").alias("SUPLMT_DSCNT_AMT")
    )
)

# "reversals" constraint:
#   IsNull(fcts_reversals.CLCL_ID) = @FALSE 
#   and (fcts_reversals.CLCL_CUR_STS in ["89","91","99"])
df_businessrules_reversals = (
    df_collector1_enriched
    .filter(
        (col("fcts_reversals.CLCL_ID_hf").isNotNull()) &
        (
          (col("fcts_reversals.CLCL_CUR_STS") == lit("89")) |
          (col("fcts_reversals.CLCL_CUR_STS") == lit("91")) |
          (col("fcts_reversals.CLCL_CUR_STS") == lit("99"))
        )
    )
    .select(
        lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        lit("I").alias("INSRT_UPDT_CD"),
        lit("N").alias("DISCARD_IN"),
        lit("Y").alias("PASS_THRU_IN"),
        lit(CurrDate).alias("FIRST_RECYC_DT"),
        lit(0).alias("ERR_CT"),
        lit(0).alias("RECYCLE_CT"),
        lit("FACETS").alias("SRC_SYS_CD"),
        expr("'FACETS' || ';' || trim(Strip.CLM_ID) || 'R'").alias("PRI_KEY_STRING"),
        lit(0).alias("ITS_CLM_SK"),
        expr("trim(Strip.CLM_ID) || 'R'").alias("CLM_ID"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        when(length(trim(col("Strip.TRNSMSN_SRC_CD"))) == 0, lit("NA")).otherwise(col("Strip.TRNSMSN_SRC_CD")).alias("TRNSMSN_SRC_CD"),
        when(
            col("Strip.CFA_DISP_PD_DT").isNull() | (length(trim(col("Strip.CFA_DISP_PD_DT"))) == 0),
            lit("UNK")
        ).otherwise(expr("left(trim(Strip.CFA_DISP_PD_DT),10)")).alias("CFA_DISP_PD_DT"),
        col("Strip.DISP_FMT_PF_DT").alias("DISP_FMT_PD_DT_SK"),
        expr("-1 * Strip.ACES_FEE_AMT").alias("ACES_FEE_AMT"),
        expr("-1 * Strip.ADM_FEE_AMT").alias("ADM_FEE_AMT"),
        expr("-1 * Strip.DRG_AMT").alias("DRG_AMT"),
        expr("-1 * Strip.SRCRG_AMT").alias("SRCHRG_AMT"),
        expr("-1 * Strip.SRPLS_AMT").alias("SRPLS_AMT"),
        when(length(trim(col("Strip.SCCF_NO"))) == 0, lit("NA")).otherwise(col("Strip.SCCF_NO")).alias("SCCF_NO"),
        col("Strip.CLMI_INVEST_IND").alias("CLMI_INVEST_IND"),
        when(col("Strip.CLMI_INVEST_DAYS") != lit("0"), col("Strip.CLMI_INVEST_DAYS")).otherwise(lit(None)).alias("CLMI_INVEST_DAYS"),
        col("Strip.CLMI_INVEST_BEG_DT").alias("CLMI_INVEST_BEG_DT"),
        when(col("Strip.CLMI_INVEST_END_DT") == lit("1753-01-01"), lit("2199-12-31")).otherwise(col("Strip.CLMI_INVEST_END_DT")).alias("CLMI_INVEST_END_DT"),
        expr("-1 * Strip.SUPLMT_DSCNT_AMT").alias("SUPLMT_DSCNT_AMT")
    )
)

# ------------------------------------------------------------------------------
# 9) Collector => collects "reversals" and "Trans" 
# ------------------------------------------------------------------------------
common_cols_businessrules_out = [
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
df_businessrules_reversals_aligned = df_businessrules_reversals.select(common_cols_businessrules_out)
df_businessrules_trans_aligned = df_businessrules_trans.select(common_cols_businessrules_out)

df_collector = df_businessrules_reversals_aligned.union(df_businessrules_trans_aligned)

# ------------------------------------------------------------------------------
# 10) SnapShot Stage (Transformer) => produces 2 outputs:
#     "Pkey" (to ITSClmPK Container) and "Snapshot" (to Transformer2)
# ------------------------------------------------------------------------------
# Pass columns directly from df_collector:
df_SnapShot = df_collector.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("ITS_CLM_SK"),
    col("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("TRNSMSN_SRC_CD"),
    col("CFA_DISP_PD_DT"),
    col("DISP_FMT_PD_DT_SK"),
    col("ACES_FEE_AMT"),
    col("ADM_FEE_AMT"),
    col("DRG_AMT"),
    col("SRCHRG_AMT"),
    col("SRPLS_AMT"),
    col("SCCF_NO"),
    col("CLMI_INVEST_IND"),
    col("CLMI_INVEST_DAYS"),
    col("CLMI_INVEST_BEG_DT"),
    col("CLMI_INVEST_END_DT"),
    col("SUPLMT_DSCNT_AMT")
)

# Split to "Pkey" and "Snapshot" as per the link definitions
# "Pkey" includes the same columns plus same expressions:
df_SnapShot_Pkey = df_SnapShot.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("ITS_CLM_SK"),
    col("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("TRNSMSN_SRC_CD"),
    col("CFA_DISP_PD_DT"),
    col("DISP_FMT_PD_DT_SK"),
    col("ACES_FEE_AMT"),
    col("ADM_FEE_AMT"),
    col("DRG_AMT"),
    col("SRCHRG_AMT"),
    col("SRPLS_AMT"),
    col("SCCF_NO"),
    col("CLMI_INVEST_IND"),
    col("CLMI_INVEST_DAYS"),
    col("CLMI_INVEST_BEG_DT"),
    col("CLMI_INVEST_END_DT"),
    col("SUPLMT_DSCNT_AMT")
)

# "Snapshot" has columns -> "CLCL_ID" (char12 pk) from CLM_ID, "SRC_SYS_CD" from SRC_SYS_CD
df_SnapShot_Snapshot = df_SnapShot.select(
    col("CLM_ID").alias("CLCL_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# ------------------------------------------------------------------------------
# 11) Transformer2 => from "Snapshot" => produces output "RowCount"
#     with columns:
#        SRC_SYS_CD_SK (pk) => Expression: SrcSysCdSk
#        CLM_ID => Expression: Snapshot.CLCL_ID
# ------------------------------------------------------------------------------
df_Transformer2 = df_SnapShot_Snapshot.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLCL_ID").alias("CLM_ID")
)

# ------------------------------------------------------------------------------
# 12) B_ITS_CLM => CSeqFileStage => writes "B_ITS_CLM.FACETS.dat.#RunID#"
# ------------------------------------------------------------------------------
# The final columns in B_ITS_CLM per the stage: [SRC_SYS_CD_SK, CLM_ID]
# We must rpad if columns are char/varchar with known length. 
# The metadata does not specify an explicit length for SRC_SYS_CD_SK. 
# For CLM_ID we see up above it was char(12) originally? 
# We'll apply rpad of length 12 for CLM_ID (strict reading from "ClmId => char(12)" in SnapShot earlier).
df_B_ITS_CLM_final = (
    df_Transformer2
    .withColumn("CLM_ID", expr("rpad(CLM_ID, 12, ' ')"))
    .select("SRC_SYS_CD_SK", "CLM_ID")
)

write_files(
    df_B_ITS_CLM_final,
    f"{adls_path}/load/B_ITS_CLM.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ------------------------------------------------------------------------------
# 13) Handle the Shared Container "ITSClmPK" with input = "Pkey", output = "Key"
# ------------------------------------------------------------------------------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/ITSClmPK
# COMMAND ----------

params_ITSClmPK = {
    "CurrRunCycle": CurrRunCycle
}
df_ITSClmPK_out = ITSClmPK(df_SnapShot_Pkey, params_ITSClmPK)

# ------------------------------------------------------------------------------
# 14) ITSClmOut => CSeqFileStage => "FctsITSClmExtr.FctsITSClm.dat.#RunID#"
# ------------------------------------------------------------------------------
# Final columns (from the job definition) are the same as "df_ITSClmPK_out" columns in the order:
#  JOB_EXCTN_RCRD_ERR_SK
#  INSRT_UPDT_CD (char(10))
#  DISCARD_IN (char(1))
#  PASS_THRU_IN (char(1))
#  FIRST_RECYC_DT
#  ERR_CT
#  RECYCLE_CT
#  SRC_SYS_CD
#  PRI_KEY_STRING
#  ITS_CLM_SK
#  CLM_ID
#  CRT_RUN_CYC_EXCTN_SK
#  LAST_UPDT_RUN_CYC_EXCTN_SK
#  TRNSMSN_SRC_CD (char(2))
#  CFA_DISP_PD_DT (char(10))
#  DISP_FMT_PD_DT_SK (char(10))
#  ACES_FEE_AMT
#  ADM_FEE_AMT
#  DRG_AMT
#  SRCHRG_AMT
#  SRPLS_AMT
#  SCCF_NO (char(17))
#  CLMI_INVEST_IND (char(1))
#  CLMI_INVEST_DAYS
#  CLMI_INVEST_BEG_DT
#  CLMI_INVEST_END_DT
#  SUPLMT_DSCNT_AMT

df_ITSClmOut = df_ITSClmPK_out.select(
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
)

# Now apply rpad for those columns with declared char lengths:
df_ITSClmOut_final = (
    df_ITSClmOut
    .withColumn("INSRT_UPDT_CD", expr("rpad(INSRT_UPDT_CD, 10, ' ')"))
    .withColumn("DISCARD_IN", expr("rpad(DISCARD_IN, 1, ' ')"))
    .withColumn("PASS_THRU_IN", expr("rpad(PASS_THRU_IN, 1, ' ')"))
    .withColumn("TRNSMSN_SRC_CD", expr("rpad(TRNSMSN_SRC_CD, 2, ' ')"))
    .withColumn("CFA_DISP_PD_DT", expr("rpad(CFA_DISP_PD_DT, 10, ' ')"))
    .withColumn("DISP_FMT_PD_DT_SK", expr("rpad(DISP_FMT_PD_DT_SK, 10, ' ')"))
    .withColumn("SCCF_NO", expr("rpad(SCCF_NO, 17, ' ')"))
    .withColumn("CLMI_INVEST_IND", expr("rpad(CLMI_INVEST_IND, 1, ' ')"))
)

write_files(
    df_ITSClmOut_final,
    f"{adls_path}/key/FctsITSClmExtr.FctsITSClm.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)