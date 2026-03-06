# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009 - 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Runs query logic from New Directions to mark a claim as a New Directions Claim or not.
# MAGIC   New Directions Claims to HDMS will have special logic on the HDMS pull to zero out some values.
# MAGIC 
# MAGIC   Uses several P table that have no UWS update ability.  Additional row need to be created and loaded through
# MAGIC   production support.
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                           Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                                   --------------------------------       -------------------------------   ----------------------------       
# MAGIC O. Nielsen                 2009-07-09                                     Original Devlopment                                                                                                          devlEDW                      Steph Goddard           07/15/2009
# MAGIC Steph Goddard         06/18/2010    4022 Facets 4.7.1   Added left join for PROV_MSG_D table - prov_msg was moved off PROV                      EnterpriseNewDevl
# MAGIC                                                                                         Removed FilePath and EDWInstance parameters, not used
# MAGIC Brent Leland             05-23-2011     TTR-1162                Removed logic to set mental health indicator based on extract revenue code                 EnterpriseWrhsDevl       SAndrew                  2011-06-01
# MAGIC                                                                                         column returned.
# MAGIC                                                                                         Broke huge SQL logic into multiple SQL lookups.
# MAGIC 
# MAGIC Rick Henry                05-17-2012     4896                        Added pd.PROC_CD_TYP_CD IN ( 'CPT4', 'HCPCS' ) to Proc_Cd extract                      EnterpriseNewDevl                Sharon Adnrew       2012-05-20
# MAGIC 
# MAGIC Pooja Sunkara          10-18-2013      5114                       Rewrite in Parallel                                                                                                              EnterpriseWrhsDevl            Peter Marshall           12/24/2013
# MAGIC 
# MAGIC Pooja Sunkara          2014-01-27    5114 # Daptiv 701    Corrected CLM_LN_MNTL_HLTH_IN column derivation and population                        EnterpriseWrhsDevl           Jag Yelavarthi             2014-01-27
# MAGIC 
# MAGIC Sharon Andrew        2015-11-10    5332 ICD10              ICD10 Diagnosis Codes needs to be tested for now to set the Mental Health Indicator field correctly.                    Kalyan Neelam             2015-11-10
# MAGIC                                                                                        In the ODBC box db2_DIAG_CD -  a union was added to the Select Statement.   
# MAGIC                                                                                       The first Select statement gets the ICD9 Diag Codes, the second select statement gets all of the ICD10 that are mappped to the same ICD9 Diag codes in the first select.
# MAGIC                                                                                       the business has not yet contracted with ICD10 Diagnosis codes to use to set the Mental Health indicator so we will continue to use last years ICD9 Diag Codes.
# MAGIC 
# MAGIC                                                                                         Added field DIAG.DIAG_CD_TYP_CD to the main extract db2_CLM_F_in so that it can be matched to the DIAG CODEs results 
# MAGIC 
# MAGIC                                                                                         Also re-arranged the fields on the main extract so that Claim data, CLaim Line, and other Misc data fields were grouped for easier readibility
# MAGIC 
# MAGIC Manasa Andru         2017-01-22    TFS - 16318             Modified the job by removing and adding several stages to populate the                           EnterpriseDev1             Kalyan Neelam               2017-01-24
# MAGIC                                                                                        field CLM_LN_MNTL_HLTH_IN as per new rule.
# MAGIC 
# MAGIC Manasa Andru         2017-08-03    TFS - 12572 and     Added condition to extract data on LAST_UPDT_RUN_CYC_DT_SK = CurrDate and      EnterpriseDev2
# MAGIC                                                          TFS - 19599           and added condition to exclude claims with SubType of 'IP' from being processed further.
# MAGIC 
# MAGIC Madhavan B           2017-11-09    TFS 20344              Updated the logic for CLM_LN_MNTL_HLTH_IN in Step 5b Outpatient Claims.                  EnterpriseDev3            Kalyan Neelam               2017-11-09
# MAGIC 
# MAGIC Sudeep Reddy       2020-09-22     US-277946             Added business condition to update the Mental Health Billing Indicator                                EnterpriseDev2            Hugh Sisson                   2020-09-23
# MAGIC                                                                                      on the basis of Claim paid date >= '2020-08-01'.
# MAGIC  
# MAGIC Sudeep Reddy       2020-10-23     US-304222                   Added business condition to update the Mental Health Billing Indicator                           EnterpriseDev2           Jeyaprasanna                2020-10-28
# MAGIC                                                                                     on the basis of Claim paid date >= '2020-08-01' according to updated
# MAGIC                                                                                     business logic. I.e Menatal health billing indicator must be 'Y' if Menatl health 
# MAGIC                                                                                     Indicator is 'N'.
# MAGIC 
# MAGIC Goutham Kalidindi  2022-08-25    US-548400            Update Business Logic for Mental Health Billing Indicator                                                       EnterpriseDev1              Reddy Sanam            2022-08-31

# MAGIC Job name:
# MAGIC EdwEdwClmLnMntlHlthlnUpdt
# MAGIC Audit file written to #$FilePath#/load/processed/CLM_NEWDIR_MNTL_HLTH_IN_UPDT.dat.#RunID#
# MAGIC Update Claim and Claim Line New Directions Mental Health Indicator
# MAGIC Select claims from recent load that may be New Directions
# MAGIC If one of the Claim Lines gets the Mental Health Indicator set then the Claim record also get updated.
# MAGIC Perfom Left Join
# MAGIC Key Column:- GRP_ID
# MAGIC Added  Business tranformation on renewal date and Bill effective date
# MAGIC Added Filter:-
# MAGIC GRP_STTUS_CD = 'ACTV'
# MAGIC Added Filter:-
# MAGIC PGM_CD = 'MNDFL'
# MAGIC Derived the logic for Mental health billing indicator
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameter values
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
CurrDate = get_widget_value('CurrDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')

# 1) db2_RVNU_CD_in - DB2ConnectorPX (EDW)
jdbc_url_db2_RVNU_CD_in, jdbc_props_db2_RVNU_CD_in = get_db_config(edw_secret_name)
extract_query_db2_RVNU_CD_in = f"""
SELECT r.RVNU_CD
FROM {EDWOwner}.RVNU_CD_D r,
     {EDWOwner}.P_NEWDIR_RVNU p
WHERE r.RVNU_CD = p.RVNU_CD
"""
df_db2_RVNU_CD_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_RVNU_CD_in)
    .options(**jdbc_props_db2_RVNU_CD_in)
    .option("query", extract_query_db2_RVNU_CD_in)
    .load()
)

# 2) db2_PROC_CD_in - DB2ConnectorPX (EDW)
jdbc_url_db2_PROC_CD_in, jdbc_props_db2_PROC_CD_in = get_db_config(edw_secret_name)
extract_query_db2_PROC_CD_in = f"""
SELECT pd.PROC_CD
FROM {EDWOwner}.PROC_CD_D pd,
     {EDWOwner}.P_NEWDIR_PROC p
WHERE pd.PROC_CD = p.PROC_CD
"""
df_db2_PROC_CD_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_PROC_CD_in)
    .options(**jdbc_props_db2_PROC_CD_in)
    .option("query", extract_query_db2_PROC_CD_in)
    .load()
)

# 3) db2_Edw_GRP_MED_PGM_BILL_D - DB2ConnectorPX (EDW)
jdbc_url_db2_Edw_GRP_MED_PGM_BILL_D, jdbc_props_db2_Edw_GRP_MED_PGM_BILL_D = get_db_config(edw_secret_name)
extract_query_db2_Edw_GRP_MED_PGM_BILL_D = f"""
SELECT 
GRP_ID,
PGM_BILL_EFF_DT, 
GRP_ID AS BILL_GRP_ID, 
PGM_CD as BILL_PGM_CD
from 
{EDWOwner}.GRP_MED_PGM_BILL_D
WHERE 
PGM_CD = 'MNDFL'
"""
df_db2_Edw_GRP_MED_PGM_BILL_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_Edw_GRP_MED_PGM_BILL_D)
    .options(**jdbc_props_db2_Edw_GRP_MED_PGM_BILL_D)
    .option("query", extract_query_db2_Edw_GRP_MED_PGM_BILL_D)
    .load()
)

# 4) db2_Edw_GRP_D - DB2ConnectorPX (EDW)
jdbc_url_db2_Edw_GRP_D, jdbc_props_db2_Edw_GRP_D = get_db_config(edw_secret_name)
extract_query_db2_Edw_GRP_D = f"""
select GRP_D.GRP_ID, GRP_D.GRP_STTUS_CD, GRP_D.GRP_SK,
      CASE WHEN GRP_RNWL_DT_SK <> 'NA'
       THEN CAST (GRP_RNWL_DT_SK AS DATE)
      END AS GRP_RNWL_DT_SK
from {EDWOwner}.GRP_D GRP_D
where GRP_STTUS_CD = 'ACTV'
"""
df_db2_Edw_GRP_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_Edw_GRP_D)
    .options(**jdbc_props_db2_Edw_GRP_D)
    .option("query", extract_query_db2_Edw_GRP_D)
    .load()
)

# Jnl_Grp_id - PxJoin (leftouterjoin on GRP_ID)
df_jnl_grp_id = (
    df_db2_Edw_GRP_D.alias("lnk_GrpD")
    .join(
        df_db2_Edw_GRP_MED_PGM_BILL_D.alias("lnk_GrpMedPgmBillD"),
        F.col("lnk_GrpD.GRP_ID") == F.col("lnk_GrpMedPgmBillD.GRP_ID"),
        how="left"
    )
    .select(
        F.col("lnk_GrpD.GRP_SK").alias("GRP_SK"),
        F.col("lnk_GrpD.GRP_ID").alias("GRP_ID"),
        F.col("lnk_GrpD.GRP_RNWL_DT_SK").alias("GRP_RNWL_DT_SK"),
        F.col("lnk_GrpD.GRP_STTUS_CD").alias("GRP_STTUS_CD"),
        F.col("lnk_GrpMedPgmBillD.BILL_PGM_CD").alias("BILL_PGM_CD"),
        F.col("lnk_GrpMedPgmBillD.BILL_GRP_ID").alias("BILL_GRP_ID"),
        F.col("lnk_GrpMedPgmBillD.PGM_BILL_EFF_DT").alias("PGM_BILL_EFF_DT"),
    )
)

# xfm_business_logic - CTransformerStage
# Create a dataframe with the stage variable svDateBusinessTranformation
df_xfm_business_logic_sv = df_jnl_grp_id.withColumn(
    "svDateBusinessTranformation",
    F.when(
        F.col("BILL_GRP_ID").isNull(),
        F.col("GRP_RNWL_DT_SK")
    ).otherwise(
        F.when(
            F.col("GRP_RNWL_DT_SK") == F.col("PGM_BILL_EFF_DT"),
            F.col("GRP_RNWL_DT_SK")
        ).otherwise(F.col("PGM_BILL_EFF_DT"))
    )
)

# Constraint for output link "lnk_transform_data":
# "TrimLeadingTrailing(( IF IsNotNull((svDateBusinessTranformation)) THEN (svDateBusinessTranformation) ELSE \"\")) <> '' AND IsNotNull(svDateBusinessTranformation)"
# We'll replicate: the StageVariable must not be null or empty after trim. Then select columns
df_lnk_transform_data = (
    df_xfm_business_logic_sv
    .filter(
        (trim(F.col("svDateBusinessTranformation")) != "") &
        (F.col("svDateBusinessTranformation").isNotNull())
    )
    .select(
        F.col("GRP_SK").alias("GRP_SK"),
        F.col("GRP_ID").alias("GRP_ID"),
        F.col("GRP_RNWL_DT_SK").alias("GRP_RNWL_DT_SK"),
        F.col("GRP_STTUS_CD").alias("GRP_STTUS_CD"),
        F.col("BILL_PGM_CD").alias("BILL_PGM_CD"),
        F.col("BILL_GRP_ID").alias("BILL_GRP_ID"),
        F.col("PGM_BILL_EFF_DT").alias("PGM_BILL_EFF_DT"),
        F.col("svDateBusinessTranformation").alias("RESULT_DATE")
    )
)

# db2_PROV_OR1_in
jdbc_url_db2_PROV_OR1_in, jdbc_props_db2_PROV_OR1_in = get_db_config(edw_secret_name)
extract_query_db2_PROV_OR1_in = f"""
SELECT distinct
pd.PROV_SK,
pd.PROV_MSG_CD
FROM
{EDWOwner}.PROV_MSG_D pd,
{EDWOwner}.P_NEWDIR_PROV_MSG pm,
{EDWOwner}.CLM_F ClmF,
{EDWOwner}.CLM_LN_F ClmLnF
WHERE
pd.PROV_MSG_CD = pm.PROV_MSG_CD
AND pd.PROV_SK = ClmF.SVC_PROV_SK
AND ClmF.CLM_SK = ClmLnF.CLM_SK
AND pd.PROV_MSG_EFF_DT_SK <= ClmLnF.CLM_LN_SVC_STRT_DT_SK
AND pd.PROV_MSG_TERM_DT_SK >= ClmLnF.CLM_LN_SVC_STRT_DT_SK
"""
df_db2_PROV_OR1_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_PROV_OR1_in)
    .options(**jdbc_props_db2_PROV_OR1_in)
    .option("query", extract_query_db2_PROV_OR1_in)
    .load()
)

# db2_DRG_in
jdbc_url_db2_DRG_in, jdbc_props_db2_DRG_in = get_db_config(edw_secret_name)
extract_query_db2_DRG_in = f"""
SELECT distinct
DR.DRG_CD,
DR.DRG_METH_CD
FROM {EDWOwner}.P_NEWDIR_DRG P,
     {EDWOwner}.DRG_D DR
WHERE
DR.DRG_CD = P.DRG_CD
and DR.DRG_METH_CD = P.DRG_METH_CD
"""
df_db2_DRG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_DRG_in)
    .options(**jdbc_props_db2_DRG_in)
    .option("query", extract_query_db2_DRG_in)
    .load()
)

# db2_DIAG_CD_in
jdbc_url_db2_DIAG_CD_in, jdbc_props_db2_DIAG_CD_in = get_db_config(edw_secret_name)
extract_query_db2_DIAG_CD_in = f"""
SELECT distinct
DCD.DIAG_CD
FROM 
{EDWOwner}.DIAG_CD_D DCD,
{EDWOwner}.P_NEWDIR_DIAG PD
WHERE
DCD.DIAG_CD = PD.DIAG_CD
and DCD.DIAG_CD_TYP_CD = PD.DIAG_CD_TYP_CD
"""
df_db2_DIAG_CD_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_DIAG_CD_in)
    .options(**jdbc_props_db2_DIAG_CD_in)
    .option("query", extract_query_db2_DIAG_CD_in)
    .load()
)

# db2_PROV_D_in
jdbc_url_db2_PROV_D_in, jdbc_props_db2_PROV_D_in = get_db_config(edw_secret_name)
extract_query_db2_PROV_D_in = f"""
SELECT distinct
PR.PROV_SK,
PR.PROV_SPEC_CD
  FROM {EDWOwner}.PROV_D PR,
       {EDWOwner}.P_NEWDIR_PROV_SPEC p
 WHERE PR.PROV_SPEC_CD = p.PROV_SPEC_CD
       AND PR.SRC_SYS_CD = 'FACETS'

UNION

SELECT PR.PROV_SK,
       PR.PROV_SPEC_CD
  FROM {EDWOwner}.PROV_D PR
 WHERE PR.PROV_SPEC_CD = 'NA'
"""
df_db2_PROV_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_PROV_D_in)
    .options(**jdbc_props_db2_PROV_D_in)
    .option("query", extract_query_db2_PROV_D_in)
    .load()
)

# db2_CLM_F_in
jdbc_url_db2_CLM_F_in, jdbc_props_db2_CLM_F_in = get_db_config(edw_secret_name)
extract_query_db2_CLM_F_in = f"""
Select
ClmF.CLM_ID,
ClmF.GRP_SK,
ClmF.CLM_PD_DT_SK,
ClmLnF.CLM_LN_SEQ_NO,
ClmLnF.CLM_LN_SK,
ClmLnF.SRC_SYS_CD,
ClmF.CLM_SUBTYP_CD,
DrgD.DRG_CD,
RvnuCdD.RVNU_CD,
ProcCdD.PROC_CD,
ProvD.PROV_SK,
ProvD.PROV_SPEC_CD,
DiagCdD.DIAG_CD,
ClmF.CLM_SK
FROM
{EDWOwner}.CLM_F ClmF,
{EDWOwner}.CLM_LN_F ClmLnF,
{EDWOwner}.DRG_D DrgD,
{EDWOwner}.DIAG_CD_D DiagCdD,
{EDWOwner}.RVNU_CD_D RvnuCdD,
{EDWOwner}.PROC_CD_D ProcCdD,
{EDWOwner}.PROV_D ProvD,
{EDWOwner}.PROD_D ProdD
WHERE
ClmF.PROD_SK = ProdD.PROD_SK AND
ClmF.CLM_SK = ClmLnF.CLM_SK AND
ClmF.GNRT_DRG_SK = DrgD.DRG_SK AND
ClmF.DIAG_CD_1_SK = DiagCdD.DIAG_CD_SK AND
ClmLnF.CLM_LN_RVNU_CD_SK = RvnuCdD.RVNU_CD_SK AND
ClmLnF.CLM_LN_PROC_CD_SK = ProcCdD.PROC_CD_SK AND
ClmF.SVC_PROV_SK = ProvD.PROV_SK AND
ClmF.CLM_PD_DT_SK >= '2017-02-01' AND
ClmF.CLM_PD_DT_SK <> 'NA' AND
ClmF.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = '{CurrDate}' AND
ClmF.SRC_SYS_CD = 'FACETS' AND
ClmF.CLM_TYP_CD = 'MED' AND
ProdD.PROD_ABBR = 'BC' AND
ProdD.PROD_SH_NM = 'BCARE' AND
ClmF.CLM_HOST_IN = 'N' AND
ClmF.CLM_STTUS_CD in (Select CLM_STTUS_CD from {EDWOwner}.P_NEWDIR_CLM_STTUS)
"""
df_db2_CLM_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CLM_F_in)
    .options(**jdbc_props_db2_CLM_F_in)
    .option("query", extract_query_db2_CLM_F_in)
    .load()
)

# Transformer_101
df_Transformer_101_sv = df_db2_CLM_F_in.withColumn(
    "svSubTyp",
    F.when(
        (
            ((F.col("CLM_SUBTYP_CD") == "OP") | (F.col("CLM_SUBTYP_CD") == "IP")) &
            (F.col("RVNU_CD") >= "0450") &
            (F.col("RVNU_CD") <= "0459")
        ),
        F.lit("N")
    ).otherwise(F.lit("Y"))
)

# Output link LkupPTbls => constraint: svSubTyp = 'Y'
df_LkupPTbls = (
    df_Transformer_101_sv
    .filter(F.col("svSubTyp") == "Y")
    .select(
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
        F.col("DRG_CD").alias("DRG_CD"),
        F.col("RVNU_CD").alias("RVNU_CD"),
        F.col("PROC_CD").alias("PROC_CD"),
        F.col("PROV_SK").alias("PROV_SK"),
        F.col("PROV_SPEC_CD").alias("PROV_SPEC_CD"),
        F.col("DIAG_CD").alias("DIAG_CD"),
        F.col("GRP_SK").alias("GRP_SK"),
        F.col("CLM_LN_SK").alias("CLM_LN_SK"),
        # CLM_PD_DT_SK is char(10)
        F.col("CLM_PD_DT_SK").alias("CLM_PD_DT_SK")
    )
)

# Output link DSLink154 => constraint: svSubTyp = 'N'
df_DSLink154 = (
    df_Transformer_101_sv
    .filter(F.col("svSubTyp") == "N")
    .select(
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("RVNU_CD").alias("RVNU_CD")
    )
)

# Remove_Duplicates_155 (PxRemDup) - deduplicate by CLM_SK, retain first
df_Remove_Duplicates_155 = dedup_sort(
    df_DSLink154,
    partition_cols=["CLM_SK"],
    sort_cols=[]
)
df_DSLink157 = df_Remove_Duplicates_155.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("RVNU_CD").alias("RVNU_CD_ER")
)

# Lkp_CdmaCodes - PxLookup
# Primary link: df_LkupPTbls => "LkupPTbls"
# Lookup: ref_Drg_cd => left join on DRG_CD
#         ref_Diag_cd => left join on DIAG_CD
#         ref_Prov_msg_cd => left join on PROV_SK
#         ref_Prov_spec_cd => left join on PROV_SK
#         DSLink157 => left join on CLM_SK
# We'll do step by step. Start with primary link as df_LkupPTbls, then chain the left joins.

df_Lkp_CdmaCodes_1 = df_LkupPTbls.alias("LkupPTbls").join(
    df_db2_DRG_in.alias("ref_Drg_cd"),
    on=[F.col("LkupPTbls.DRG_CD") == F.col("ref_Drg_cd.DRG_CD")],
    how="left"
)
df_Lkp_CdmaCodes_2 = df_Lkp_CdmaCodes_1.join(
    df_db2_DIAG_CD_in.alias("ref_Diag_cd"),
    on=[F.col("LkupPTbls.DIAG_CD") == F.col("ref_Diag_cd.DIAG_CD")],
    how="left"
)
df_Lkp_CdmaCodes_3 = df_Lkp_CdmaCodes_2.join(
    df_db2_PROV_OR1_in.alias("ref_Prov_msg_cd"),
    on=[F.col("LkupPTbls.PROV_SK") == F.col("ref_Prov_msg_cd.PROV_SK")],
    how="left"
)
df_Lkp_CdmaCodes_4 = df_Lkp_CdmaCodes_3.join(
    df_db2_PROV_D_in.alias("ref_Prov_spec_cd"),
    on=[F.col("LkupPTbls.PROV_SK") == F.col("ref_Prov_spec_cd.PROV_SK")],
    how="left"
)
df_Lkp_CdmaCodes_5 = df_Lkp_CdmaCodes_4.join(
    df_DSLink157.alias("DSLink157"),
    on=[F.col("LkupPTbls.CLM_SK") == F.col("DSLink157.CLM_SK")],
    how="left"
)

df_Lkp_CdmaCodes_out = df_Lkp_CdmaCodes_5.select(
    F.col("LkupPTbls.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("LkupPTbls.CLM_ID").alias("CLM_ID"),
    F.col("LkupPTbls.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("LkupPTbls.CLM_SK").alias("CLM_SK"),
    F.col("LkupPTbls.PROV_SK").alias("SVC_PROV_SK"),
    F.col("LkupPTbls.RVNU_CD").alias("RVNU_CD"),
    F.col("LkupPTbls.PROC_CD").alias("PROC_CD"),
    F.col("LkupPTbls.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("ref_Prov_msg_cd.PROV_MSG_CD").alias("PROV_MSG_CD"),
    F.col("ref_Prov_spec_cd.PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    F.col("ref_Drg_cd.DRG_METH_CD").alias("DRG_METH_CHK"),
    F.col("ref_Diag_cd.DIAG_CD").alias("DIAG_CD"),
    F.col("DSLink157.RVNU_CD_ER").alias("RVNU_CD_ER"),
    F.col("LkupPTbls.GRP_SK").alias("GRP_SK"),
    F.col("LkupPTbls.CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("LkupPTbls.CLM_PD_DT_SK").alias("CLM_PD_DT_SK")
)

# xfrm_businessLogic
df_xfrm_businessLogic_sv = (
    df_Lkp_CdmaCodes_out
    .withColumn(
        "svProvMesgChk",
        F.when(
            (F.length(trim(F.col("PROV_MSG_CD"))) == 0) | (F.col("PROV_MSG_CD").isNull()),
            F.lit("N")
        ).otherwise(F.lit("Y"))
    )
    .withColumn(
        "svProvSpecChk",
        F.when(
            (F.length(trim(F.col("PROV_SPEC_CD"))) == 0) | (F.col("PROV_SPEC_CD").isNull()),
            F.lit("N")
        ).otherwise(F.lit("Y"))
    )
    .withColumn(
        "svProvChk",
        F.when(
            (F.col("svProvMesgChk") == "Y") | (F.col("svProvSpecChk") == "Y"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svDrgMethChk",
        F.when(
            (F.length(trim(F.col("DRG_METH_CHK"))) == 0) | (F.col("DRG_METH_CHK").isNull()),
            F.lit("N")
        ).otherwise(F.lit("Y"))
    )
    .withColumn(
        "svDiagCdChk",
        F.when(
            (F.length(trim(F.col("DIAG_CD"))) == 0) | (F.col("DIAG_CD").isNull()),
            F.lit("N")
        ).otherwise(F.lit("Y"))
    )
    .withColumn(
        "svDiagChk",
        F.when(
            (F.col("svDrgMethChk") == "Y") | (F.col("svDiagCdChk") == "Y"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svFinalChk",
        F.when(
            (F.col("svProvChk") == "Y") & (F.col("svDiagChk") == "Y"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svERClmChk",
        F.when(
            trim(F.col("RVNU_CD_ER")) == "",
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

# Output link Ink_SubTyp_IP_PR => constraint:
# svFinalChk = 'Y' AND (CLM_SUBTYP_CD = 'IP' OR CLM_SUBTYP_CD = 'PR') AND svERClmChk = 'Y'
df_Ink_SubTyp_IP_PR = (
    df_xfrm_businessLogic_sv
    .filter(
        (F.col("svFinalChk") == "Y") &
        ((F.col("CLM_SUBTYP_CD") == "IP") | (F.col("CLM_SUBTYP_CD") == "PR")) &
        (F.col("svERClmChk") == "Y")
    )
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
        F.col("RVNU_CD").alias("RVNU_CD"),
        F.col("PROC_CD").alias("PROC_CD"),
        F.col("svProvMesgChk").alias("PROV_MSG_CHK"),
        F.col("svProvSpecChk").alias("PROV_SPEC_CHK"),
        F.col("svProvChk").alias("PROV_CHK"),
        F.col("svDrgMethChk").alias("DRG_METH_CHK"),
        F.col("svDiagCdChk").alias("DIAG_CD_CHK"),
        F.col("svDiagChk").alias("DIAG_CHK"),
        F.col("svFinalChk").alias("STEP4_CHK"),
        F.col("GRP_SK").alias("GRP_SK"),
        F.col("CLM_LN_SK").alias("CLM_LN_SK"),
        F.col("CLM_PD_DT_SK").alias("CLM_PD_DT_SK")
    )
)

# Output link Ink_SubTyp_OP => constraint:
# svFinalChk = 'Y' AND CLM_SUBTYP_CD = 'OP' AND svERClmChk = 'Y'
df_Ink_SubTyp_OP = (
    df_xfrm_businessLogic_sv
    .filter(
        (F.col("svFinalChk") == "Y") &
        (F.col("CLM_SUBTYP_CD") == "OP") &
        (F.col("svERClmChk") == "Y")
    )
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
        F.col("RVNU_CD").alias("RVNU_CD"),
        F.col("PROC_CD").alias("PROC_CD"),
        F.col("svProvMesgChk").alias("PROV_MSG_CHK"),
        F.col("svProvSpecChk").alias("PROV_SPEC_CHK"),
        F.col("svProvChk").alias("PROV_CHK"),
        F.col("svDrgMethChk").alias("DRG_METH_CHK"),
        F.col("svDiagCdChk").alias("DIAG_CD_CHK"),
        F.col("svDiagChk").alias("DIAG_CHK"),
        F.col("svFinalChk").alias("STEP4_CHK"),
        F.col("GRP_SK").alias("GRP_SK"),
        F.col("CLM_LN_SK").alias("CLM_LN_SK"),
        F.col("CLM_PD_DT_SK").alias("CLM_PD_DT_SK")
    )
)

# Transformer_122
df_Transformer_122_sv = (
    df_Ink_SubTyp_OP
    .withColumn(
        "sv5b1",
        F.when(
            (
                (
                    (F.col("RVNU_CD").isin("0900","0904","0905","0906","0907","0911","0914","0915","0916","0918","0919","0944","0945"))
                ) &
                (
                    (F.col("PROC_CD").isin("90853","90899","H0010","H0015","H0018","H0035","H2036","S0201","S9475","S9480","S9976"))
                )
            ),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "sv5b2",
        F.when(
            (
                (F.col("RVNU_CD").isin("0912","0913")) &
                (F.col("PROC_CD").isin("90853","90899","H0010","H0015","H0018","H0035","H2036","S0201","S9475","S9480","S9976"))
            ),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "sv5bFinal",
        F.when(
            (F.col("sv5b1") == "Y") | (F.col("sv5b2") == "Y"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

# Output link OPClms => constraint: sv5bFinal = 'Y'
df_OPClms = (
    df_Transformer_122_sv
    .filter(F.col("sv5bFinal") == "Y")
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
        F.col("RVNU_CD").alias("RVNU_CD"),
        F.col("PROC_CD").alias("PROC_CD"),
        F.col("PROV_MSG_CHK").alias("PROV_MSG_CHK"),
        F.col("PROV_SPEC_CHK").alias("PROV_SPEC_CHK"),
        F.col("PROV_CHK").alias("PROV_CHK"),
        F.col("DRG_METH_CHK").alias("DRG_METH_CHK"),
        F.col("DIAG_CD_CHK").alias("DIAG_CD_CHK"),
        F.col("DIAG_CHK").alias("DIAG_CHK"),
        F.col("STEP4_CHK").alias("STEP4_CHK"),
        F.col("GRP_SK").alias("GRP_SK"),
        F.col("CLM_LN_SK").alias("CLM_LN_SK"),
        F.col("CLM_PD_DT_SK").alias("CLM_PD_DT_SK")
    )
)

# Lookup_113
# Primary link: df_Ink_SubTyp_IP_PR
# Lookup: ref_RvnuCd => left on RVNU_CD
#         ref_ProcCd => left on PROC_CD
df_Lookup_113_1 = df_Ink_SubTyp_IP_PR.alias("Ink_SubTyp_IP_PR").join(
    df_db2_RVNU_CD_in.alias("ref_RvnuCd"),
    on=[F.col("Ink_SubTyp_IP_PR.RVNU_CD") == F.col("ref_RvnuCd.RVNU_CD")],
    how="left"
)
df_Lookup_113_2 = df_Lookup_113_1.join(
    df_db2_PROC_CD_in.alias("ref_ProcCd"),
    on=[F.col("Ink_SubTyp_IP_PR.PROC_CD") == F.col("ref_ProcCd.PROC_CD")],
    how="left"
)
df_IpPrClms = df_Lookup_113_2.select(
    F.col("Ink_SubTyp_IP_PR.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Ink_SubTyp_IP_PR.CLM_ID").alias("CLM_ID"),
    F.col("Ink_SubTyp_IP_PR.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("Ink_SubTyp_IP_PR.CLM_SK").alias("CLM_SK"),
    F.col("Ink_SubTyp_IP_PR.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("ref_RvnuCd.RVNU_CD").alias("RVNU_CD"),
    F.col("ref_ProcCd.PROC_CD").alias("PROC_CD"),
    F.col("Ink_SubTyp_IP_PR.PROV_MSG_CHK").alias("PROV_MSG_CHK"),
    F.col("Ink_SubTyp_IP_PR.PROV_SPEC_CHK").alias("PROV_SPEC_CHK"),
    F.col("Ink_SubTyp_IP_PR.PROV_CHK").alias("PROV_CHK"),
    F.col("Ink_SubTyp_IP_PR.DRG_METH_CHK").alias("DRG_METH_CHK"),
    F.col("Ink_SubTyp_IP_PR.DIAG_CD_CHK").alias("DIAG_CD_CHK"),
    F.col("Ink_SubTyp_IP_PR.DIAG_CHK").alias("DIAG_CHK"),
    F.col("Ink_SubTyp_IP_PR.STEP4_CHK").alias("STEP4_CHK"),
    F.col("Ink_SubTyp_IP_PR.GRP_SK").alias("GRP_SK"),
    F.col("Ink_SubTyp_IP_PR.CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("Ink_SubTyp_IP_PR.CLM_PD_DT_SK").alias("CLM_PD_DT_SK")
)

# Transformer_117
df_Transformer_117_sv = (
    df_IpPrClms
    .withColumn(
        "sv5ARvnu",
        F.when(
            (F.length(trim(F.col("RVNU_CD"))) == 0) | (F.col("RVNU_CD").isNull()),
            F.lit("N")
        ).otherwise(F.lit("Y"))
    )
    .withColumn(
        "sv5AProc",
        F.when(
            (F.length(trim(F.col("PROC_CD"))) == 0) | (F.col("PROC_CD").isNull()),
            F.lit("N")
        ).otherwise(F.lit("Y"))
    )
    .withColumn(
        "sv5AFinal",
        F.when(
            (F.col("sv5ARvnu") == "Y") | (F.col("sv5AProc") == "Y"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

df_ClmsIpPr = (
    df_Transformer_117_sv
    .filter(F.col("sv5AFinal") == "Y")
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
        F.col("RVNU_CD").alias("RVNU_CD"),
        F.col("PROC_CD").alias("PROC_CD"),
        F.col("PROV_MSG_CHK").alias("PROV_MSG_CHK"),
        F.col("PROV_SPEC_CHK").alias("PROV_SPEC_CHK"),
        F.col("PROV_CHK").alias("PROV_CHK"),
        F.col("DRG_METH_CHK").alias("DRG_METH_CHK"),
        F.col("DIAG_CD_CHK").alias("DIAG_CD_CHK"),
        F.col("DIAG_CHK").alias("DIAG_CHK"),
        F.col("STEP4_CHK").alias("STEP4_CHK"),
        F.col("GRP_SK").alias("GRP_SK"),
        F.col("CLM_LN_SK").alias("CLM_LN_SK"),
        F.col("CLM_PD_DT_SK").alias("CLM_PD_DT_SK")
    )
)

# Funnel_132 => input from df_ClmsIpPr, df_OPClms
df_Funnel_132 = df_ClmsIpPr.select(*[c for c in df_ClmsIpPr.columns]) \
    .unionByName(
        df_OPClms.select(*[c for c in df_OPClms.columns]),
        allowMissingColumns=True
    )

df_Next = df_Funnel_132.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("RVNU_CD").alias("RVNU_CD"),
    F.col("PROC_CD").alias("PROC_CD"),
    F.col("PROV_MSG_CHK").alias("PROV_MSG_CHK"),
    F.col("PROV_SPEC_CHK").alias("PROV_SPEC_CHK"),
    F.col("PROV_CHK").alias("PROV_CHK"),
    F.col("DRG_METH_CHK").alias("DRG_METH_CHK"),
    F.col("DIAG_CD_CHK").alias("DIAG_CD_CHK"),
    F.col("DIAG_CHK").alias("DIAG_CHK"),
    F.col("STEP4_CHK").alias("STEP4_CHK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("CLM_PD_DT_SK").alias("CLM_PD_DT_SK")
)

# Copy_All => two outputs
df_Ink_EdwEdwClmLnMntlHlthlnUpdtCpy_Out = df_Next.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("CLM_PD_DT_SK").alias("CLM_PD_DT_SK")
)

df_Ink_EdwEdwClmLnMntlHlthlnUpdtSeq_Out = df_Next.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("RVNU_CD").alias("RVNU_CD"),
    F.col("PROC_CD").alias("PROC_CD"),
    F.col("PROV_MSG_CHK").alias("PROV_MSG_CHK"),
    F.col("PROV_SPEC_CHK").alias("PROV_SPEC_CHK"),
    F.col("PROV_CHK").alias("PROV_CHK"),
    F.col("DRG_METH_CHK").alias("DRG_METH_CHK"),
    F.col("DIAG_CD_CHK").alias("DIAG_CD_CHK"),
    F.col("DIAG_CHK").alias("DIAG_CHK"),
    F.col("STEP4_CHK").alias("STEP4_CHK")
)

# seq_CLM_NEWDIR_MNTL_HLTH_IN_UPDT_csv_Load - PxSequentialFile (write)
# Apply rpad for char columns before writing
df_seq_CLM_NEWDIR_MNTL_HLTH_IN_UPDT_csv_Load = df_Ink_EdwEdwClmLnMntlHlthlnUpdtSeq_Out.select(
    F.col("CLM_SK"),
    F.col("SRC_SYS_CD"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_SUBTYP_CD"),
    F.col("RVNU_CD"),
    F.col("PROC_CD"),
    F.rpad(F.col("PROV_MSG_CHK"), 10, " ").alias("PROV_MSG_CHK"),
    F.rpad(F.col("PROV_SPEC_CHK"), 10, " ").alias("PROV_SPEC_CHK"),
    F.rpad(F.col("PROV_CHK"), 10, " ").alias("PROV_CHK"),
    F.rpad(F.col("DRG_METH_CHK"), 10, " ").alias("DRG_METH_CHK"),
    F.rpad(F.col("DIAG_CD_CHK"), 10, " ").alias("DIAG_CD_CHK"),
    F.rpad(F.col("DIAG_CHK"), 10, " ").alias("DIAG_CHK"),
    F.rpad(F.col("STEP4_CHK"), 10, " ").alias("STEP4_CHK")
)

write_files(
    df_seq_CLM_NEWDIR_MNTL_HLTH_IN_UPDT_csv_Load,
    f"{adls_path}/processed/CLM_NEWDIR_MNTL_HLTH_IN_UPDT.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote='"',
    nullValue=None
)

# xfrm_BusinessLogic2
df_xfrm_BusinessLogic2_clm_f = df_Ink_EdwEdwClmLnMntlHlthlnUpdtCpy_Out.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit("Y").alias("CLM_LN_MNTL_HLTH_IN")  # from WhereExpression "'Y'"
)

df_xfrm_BusinessLogic2_lnk_data = df_Ink_EdwEdwClmLnMntlHlthlnUpdtCpy_Out.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.lit("Y").alias("CLM_LN_MNTL_HLTH_IN"),  # from WhereExpression "'Y'"
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("CLM_PD_DT_SK").alias("CLM_PD_DT_SK")
)

# Update_CLM_F_Out - DB2ConnectorPX (merge into {EDWOwner}.CLM_F)
df_Update_CLM_F_Out = df_xfrm_BusinessLogic2_clm_f

# Create temporary table for the merge
temp_table_CLM_F = "STAGING.EdwClmLnMntlHlthlnUpdt_EE_Update_CLM_F_Out_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_CLM_F}", jdbc_url_db2_CLM_F_in, jdbc_props_db2_CLM_F_in)

df_Update_CLM_F_Out.write \
    .format("jdbc") \
    .option("url", jdbc_url_db2_CLM_F_in) \
    .options(**jdbc_props_db2_CLM_F_in) \
    .option("dbtable", temp_table_CLM_F) \
    .mode("overwrite") \
    .save()

merge_sql_CLM_F = f"""
MERGE INTO {EDWOwner}.CLM_F AS T
USING {temp_table_CLM_F} AS S
ON T.CLM_ID = S.CLM_ID
AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
  UPDATE SET
    T.CLM_LN_MNTL_HLTH_IN = S.CLM_LN_MNTL_HLTH_IN
WHEN NOT MATCHED THEN
  INSERT (CLM_ID, SRC_SYS_CD, CLM_LN_MNTL_HLTH_IN)
  VALUES (S.CLM_ID, S.SRC_SYS_CD, S.CLM_LN_MNTL_HLTH_IN);
"""
execute_dml(merge_sql_CLM_F, jdbc_url_db2_CLM_F_in, jdbc_props_db2_CLM_F_in)

# Jni_Grp_sk - PxJoin (innerjoin on GRP_SK)
df_Jni_Grp_sk = (
    df_xfrm_BusinessLogic2_lnk_data.alias("lnk_data")
    .join(
        df_lnk_transform_data.alias("lnk_transform_data"),
        F.col("lnk_data.GRP_SK") == F.col("lnk_transform_data.GRP_SK"),
        how="inner"
    )
    .select(
        F.col("lnk_transform_data.GRP_ID").alias("GRP_ID"),
        F.col("lnk_transform_data.GRP_RNWL_DT_SK").alias("GRP_RNWL_DT_SK"),
        F.col("lnk_transform_data.GRP_STTUS_CD").alias("GRP_STTUS_CD"),
        F.col("lnk_transform_data.BILL_PGM_CD").alias("BILL_PGM_CD"),
        F.col("lnk_transform_data.BILL_GRP_ID").alias("BILL_GRP_ID"),
        F.col("lnk_transform_data.PGM_BILL_EFF_DT").alias("PGM_BILL_EFF_DT"),
        F.col("lnk_transform_data.RESULT_DATE").alias("RESULT_DATE"),
        F.col("lnk_data.CLM_ID").alias("CLM_ID"),
        F.col("lnk_data.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("lnk_data.GRP_SK").alias("GRP_SK"),
        F.col("lnk_data.CLM_LN_SK").alias("CLM_LN_SK"),
        F.col("lnk_data.CLM_PD_DT_SK").alias("CLM_PD_DT_SK"),
        F.col("lnk_data.CLM_LN_MNTL_HLTH_IN").alias("CLM_LN_MNTL_HLTH_IN"),
        F.col("lnk_data.SRC_SYS_CD").alias("SRC_SYS_CD")
    )
)

# xfm_Business_Indicator_logic
df_Business_Indicator_logic_sv = df_Jni_Grp_sk.withColumn(
    "svHealthBillingIndicator",
    F.when(
        trim(F.col("CLM_LN_MNTL_HLTH_IN")) == "N",
        F.lit("Y")
    ).otherwise(
        F.when(
            F.col("CLM_PD_DT_SK") < F.lit("2020-08-01"),
            F.lit("N")
        ).otherwise(
            F.when(
                (F.col("PGM_BILL_EFF_DT").isNull()) & (F.col("CLM_PD_DT_SK").substr(F.lit(1), F.lit(4)) == "2022"),
                F.lit("Y")
            ).otherwise(
                F.when(
                    (F.col("PGM_BILL_EFF_DT").isNull()) & (
                        (F.col("CLM_PD_DT_SK") >= F.concat_ws("-", F.col("CLM_PD_DT_SK").substr(F.lit(1),F.lit(4)), F.col("GRP_RNWL_DT_SK").substr(F.lit(6),F.lit(5)))) &
                        (F.concat_ws("-", F.col("CLM_PD_DT_SK").substr(F.lit(1),F.lit(4)), F.col("GRP_RNWL_DT_SK").substr(F.lit(6),F.lit(5))) >= "2020-08-01")
                    ),
                    F.lit("Y")
                ).otherwise(
                    F.when(
                        (F.col("PGM_BILL_EFF_DT").isNotNull()) &
                        (F.col("CLM_PD_DT_SK") >= F.col("PGM_BILL_EFF_DT")),
                        F.lit("Y")
                    ).otherwise(F.lit("N"))
                )
            )
        )
    )
)

df_clm_ln_f = df_Business_Indicator_logic_sv.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_MNTL_HLTH_IN").alias("CLM_LN_MNTL_HLTH_IN"),
    F.col("svHealthBillingIndicator").alias("CLM_LN_MNTL_HLTH_BILL_IN")
)

# Update_CLM_LN_F_Out - DB2ConnectorPX (merge into {EDWOwner}.CLM_LN_F)
df_Update_CLM_LN_F_Out = df_clm_ln_f

# Create temporary table for the merge
temp_table_CLM_LN_F = "STAGING.EdwClmLnMntlHlthlnUpdt_EE_Update_CLM_LN_F_Out_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_CLM_LN_F}", jdbc_url_db2_CLM_F_in, jdbc_props_db2_CLM_F_in)

df_Update_CLM_LN_F_Out.write \
    .format("jdbc") \
    .option("url", jdbc_url_db2_CLM_F_in) \
    .options(**jdbc_props_db2_CLM_F_in) \
    .option("dbtable", temp_table_CLM_LN_F) \
    .mode("overwrite") \
    .save()

merge_sql_CLM_LN_F = f"""
MERGE INTO {EDWOwner}.CLM_LN_F AS T
USING {temp_table_CLM_LN_F} AS S
ON T.CLM_ID = S.CLM_ID
AND T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO
WHEN MATCHED THEN
  UPDATE SET
    T.CLM_LN_MNTL_HLTH_IN = S.CLM_LN_MNTL_HLTH_IN,
    T.CLM_LN_MNTL_HLTH_BILL_IN = S.CLM_LN_MNTL_HLTH_BILL_IN
WHEN NOT MATCHED THEN
  INSERT (CLM_ID, SRC_SYS_CD, CLM_LN_SEQ_NO, CLM_LN_MNTL_HLTH_IN, CLM_LN_MNTL_HLTH_BILL_IN)
  VALUES (S.CLM_ID, S.SRC_SYS_CD, S.CLM_LN_SEQ_NO, S.CLM_LN_MNTL_HLTH_IN, S.CLM_LN_MNTL_HLTH_BILL_IN);
"""
execute_dml(merge_sql_CLM_LN_F, jdbc_url_db2_CLM_F_in, jdbc_props_db2_CLM_F_in)