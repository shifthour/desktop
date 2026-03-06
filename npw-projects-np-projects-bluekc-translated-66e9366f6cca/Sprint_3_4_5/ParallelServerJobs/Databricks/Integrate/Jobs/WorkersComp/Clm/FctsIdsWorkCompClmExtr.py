# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY: 
# MAGIC 
# MAGIC PROCESSING:     Workers Compensation claims extract from FacetsDbo and load to IDS WORK_COMPNSTN_CLM table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                          Change Description                                                   Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                --------------------     \(9)------------------------                                        -----------------------------------------------------------------------                  ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Akhila Manickavelu\(9)  2016-12-15\(9)5628 WORK_COMPNSTN_CLM \(9)    Original Programming\(9)\(9)\(9)          Integratedev2                          Kalyan Neelam           2017-02-27       
# MAGIC                                                                 (Facets to IDS) ETL Report 
# MAGIC 
# MAGIC Akhila Manickavelu\(9)  2017-05-09\(9)5628 WORK_COMPNSTN_CLM \(9)    Reversal Logic updated\(9)\(9)\(9)          Integratedev2                         Kalyan Neelam            2017-05-22             
# MAGIC                                                                 (Facets to IDS) ETL Report 
# MAGIC 
# MAGIC 
# MAGIC Akhila Manickavelu\(9)  2017-06-08\(9)5628 WORK_COMPNSTN_CLM \(9)    Updated Trim for NTWK_ID in DB2\(9)\(9)          Integratedev2                         Kalyan Neelam            2017-06-12            
# MAGIC                                                                 (Facets to IDS) ETL Report                            to find SK for Blanks
# MAGIC Prabhu ES                 2022-03-02            S2S Remediation                                        MSSQL ODBC conn added and other param changes  IntegrateDev5

# MAGIC This job Extracts the Claims data from Facets tables to generate the load file #Env#1.Facets_Ids_WorkCompClm.dat to be send to IDS WORK_COMPNSTN_CLM table
# MAGIC Join CMC_CLCL_CLAIM , CMC_CDML_CL_LINE, CMC_PDBL_PROD_BILL, CMC_PRPR_PROV, CMC_GRGR_GROUP, CMC_MEME_MEMBER for master extract and Look up on P_SEL_PRCS_CRITR for GRGR_ID
# MAGIC Lookup for the fields to generate FINL_DISP_CD, ROV_SPEC_CD_SK, FNCL_LOB_SK
# MAGIC Seq. file to load into the DB2 table WORK_COMPNSTN_CLM
# MAGIC Seq. file to load into the DB2 table B_WORK_COMPNSTN_CLM for Balancing Report
# MAGIC Reversal Logic
# MAGIC Populate Negative amounts for Reversed Claims
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Retrieve all parameter values
FacetsOwner = get_widget_value('FacetsOwner', '')
facets_secret_name = get_widget_value('facets_secret_name', '')
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
WorkCompOwner = get_widget_value('WorkCompOwner', '')
workcomp_secret_name = get_widget_value('workcomp_secret_name', '')
BCBSOwner = get_widget_value('BCBSOwner', '')
bcbs_secret_name = get_widget_value('bcbs_secret_name', '')
RunCycle = get_widget_value('RunCycle', '')
Env = get_widget_value('Env', '')
RunID = get_widget_value('RunID', '')
DrivTable = get_widget_value('DrivTable', '')
SrcSysCd = get_widget_value('SrcSysCd', '')

# Prepare JDBC configurations for IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Prepare JDBC configurations for Facets (ODBC-like usage as well)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

# Prepare JDBC configurations for WorkCompOwner if needed (also uses IDS DB, same secret if so)
# (No direct writer or merge to WorkCompOwner table in this job, only reading #$WorkCompOwner#.WORK_COMPNSTN_CLM, so we use the IDS secrets)
# But the job references the same IDS DB with #$WorkCompOwner# as schema.

# -----------------------------------------------------------------------------------------------
# Stage: FacetsDB_CdmlInput (ODBCConnectorPX) - READ
# -----------------------------------------------------------------------------------------------
extract_query_FacetsDB_CdmlInput = """SELECT DISTINCT
    CMC_CDML_CL_LINE.CLCL_ID,
    Sum(CMC_CDML_CL_LINE.CDML_CHG_AMT) as CDML_CHG_AMT,
    Sum(CMC_CDML_CL_LINE.CDML_CONSIDER_CHG) as CDML_CONSIDER_CHG,
    Sum(CMC_CDML_CL_LINE.CDML_ALLOW) as CDML_ALLOW,
    Sum(CMC_CDML_CL_LINE.CDML_DED_AMT) as CDML_DED_AMT,
    Sum(CMC_CDML_CL_LINE.CDML_COPAY_AMT) as CDML_COPAY_AMT,
    Sum(CMC_CDML_CL_LINE.CDML_COINS_AMT) as CDML_COINS_AMT,
    Sum(CMC_CDML_CL_LINE.CDML_DISALL_AMT) as CDML_DISALL_AMT,
    Sum(CMC_CDML_CL_LINE.CDML_SB_PYMT_AMT) as CDML_SB_PYMT_AMT,
    Sum(CMC_CDML_CL_LINE.CDML_PR_PYMT_AMT) as CDML_PR_PYMT_AMT
FROM
    {0}.CMC_CDML_CL_LINE CMC_CDML_CL_LINE
    INNER JOIN {0}.CMC_CLCL_CLAIM CMC_CLCL_CLAIM ON CMC_CLCL_CLAIM.CLCL_ID = CMC_CDML_CL_LINE.CLCL_ID
    INNER JOIN tempdb..{1} Drvr ON Drvr.CLM_ID=CMC_CLCL_CLAIM.CLCL_ID
WHERE
    CMC_CLCL_CLAIM.CLCL_CL_TYPE = 'M'
GROUP BY  CMC_CDML_CL_LINE.CLCL_ID
""".format(FacetsOwner, DrivTable)

df_FacetsDB_CdmlInput = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_FacetsDB_CdmlInput)
    .load()
)

# -----------------------------------------------------------------------------------------------
# Stage: WorkCompnstnClm (DB2ConnectorPX) - READ from IDS
# -----------------------------------------------------------------------------------------------
extract_query_WorkCompnstnClm = f"""
SELECT 
  DISTINCT 
  CLM_ID, 
  CRT_RUN_CYC_EXCTN_SK
FROM 
   {WorkCompOwner}.WORK_COMPNSTN_CLM
WHERE
   SRC_SYS_CD='FACETS'
"""

df_WorkCompnstnClm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_WorkCompnstnClm)
    .load()
)

# -----------------------------------------------------------------------------------------------
# Stage: EXPRNC_CAT (DB2ConnectorPX) - READ from IDS
# -----------------------------------------------------------------------------------------------
extract_query_EXPRNC_CAT = f"""
SELECT DISTINCT
    EXPRNC_CAT.EXPRNC_CAT_SK,
    EXPRNC_CAT.EXPRNC_CAT_CD
FROM 
    {IDSOwner}.EXPRNC_CAT EXPRNC_CAT
    INNER JOIN {IDSOwner}.CD_MPPNG CODE ON 
       EXPRNC_CAT.SRC_SYS_CD_SK=CODE.CD_MPPNG_SK
       AND CODE.TRGT_CD='FACETS'
"""

df_EXPRNC_CAT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_EXPRNC_CAT)
    .load()
)

# -----------------------------------------------------------------------------------------------
# Stage: NTWK (DB2ConnectorPX) - READ from IDS
# -----------------------------------------------------------------------------------------------
extract_query_NTWK = f"""
SELECT Trim(NTWK.NTWK_ID) AS NTWK_ID, NTWK.NTWK_SK 
FROM {IDSOwner}.NTWK NTWK
"""

df_NTWK = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_NTWK)
    .load()
)

# -----------------------------------------------------------------------------------------------
# Stage: PROD (DB2ConnectorPX) - READ from IDS
# -----------------------------------------------------------------------------------------------
extract_query_PROD = f"""
SELECT PROD.PROD_ID, PROD.PROD_SK 
FROM {IDSOwner}.PROD PROD
"""

df_PROD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_PROD)
    .load()
)

# -----------------------------------------------------------------------------------------------
# Stage: FacetsDB_Input (ODBCConnectorPX) - READ
# -----------------------------------------------------------------------------------------------
extract_query_FacetsDB_Input = """SELECT DISTINCT
    CMC_CLCL_CLAIM.CLCL_ID,
    CMC_CLCL_CLAIM.GRGR_CK,
    CMC_CLCL_CLAIM.SBSB_CK,
    CMC_CLCL_CLAIM.CLCL_CL_TYPE,
    CMC_CLCL_CLAIM.CLCL_CL_SUB_TYPE,
    CMC_CLCL_CLAIM.CLCL_CUR_STS,
    CMC_CLCL_CLAIM.CLCL_LAST_ACT_DTM,
    CMC_CLCL_CLAIM.CLCL_INPUT_DT,
    CMC_CLCL_CLAIM.CLCL_RECD_DT,
    CMC_CLCL_CLAIM.CLCL_ACPT_DTM,
    CMC_CLCL_CLAIM.CLCL_PAID_DT,
    CMC_CLCL_CLAIM.CLCL_NEXT_REV_DT,
    CMC_CLCL_CLAIM.CLCL_LOW_SVC_DT,
    CMC_CLCL_CLAIM.CLCL_HIGH_SVC_DT,
    CMC_CLCL_CLAIM.CLCL_ID_ADJ_TO,
    CMC_CLCL_CLAIM.CLCL_ID_ADJ_FROM,
    CMC_CLCL_CLAIM.NWNW_ID,
    CMC_CLCL_CLAIM.CLCL_PAY_PR_IND,
    CMC_CLCL_CLAIM.CLCL_ACD_IND,
    CMC_CLCL_CLAIM.CLCL_ACD_STATE,
    CMC_CLCL_CLAIM.CLCL_ACD_DT,
    CMC_CLCL_CLAIM.CLCL_ACD_AMT,
    CMC_CLCL_CLAIM.CLCL_PRPR_ID_REF,
    CMC_CLCL_CLAIM.CLCL_PA_ACCT_NO,
    CMC_CLCL_CLAIM.CLCL_PA_PAID_AMT,
    CMC_CLCL_CLAIM.CLCL_TOT_PAYABLE,
    CMC_CLCL_CLAIM.CLCL_INPUT_METH,
    CMC_CLCL_CLAIM.CLCL_UNABL_FROM_DT,
    CMC_CLCL_CLAIM.CLCL_UNABL_TO_DT,
    CMC_PDBL_PROD_BILL.PDBL_EXP_CAT,
    CMC_PRPR_PROV.PRCF_MCTR_SPEC,
    CMC_PRPR_PROV.PRPR_MCTR_TYPE,
    CMC_CLCL_CLAIM.PDPD_ID,
    CMC_PRPR_PROV.PRPR_ENTITY,
    CMC_PRPR_PROV.PRPR_ID,
    CMC_PDBL_PROD_BILL.PDBL_ACCT_CAT,
    CMC_CLHP_HOSP.CLHP_FAC_TYPE,
    CMC_CLHP_HOSP.CLHP_BILL_CLASS,
    CMC_GRGR_GROUP.GRGR_ID
FROM
    {0}.CMC_CLCL_CLAIM CMC_CLCL_CLAIM
    INNER JOIN tempdb..{1} Drvr ON Drvr.CLM_ID=CMC_CLCL_CLAIM.CLCL_ID
    INNER JOIN {0}.CMC_GRGR_GROUP CMC_GRGR_GROUP
       ON CMC_CLCL_CLAIM.GRGR_CK = CMC_GRGR_GROUP.GRGR_CK
    INNER JOIN {0}.CMC_PRPR_PROV CMC_PRPR_PROV
       ON CMC_CLCL_CLAIM.PRPR_ID = CMC_PRPR_PROV.PRPR_ID
    INNER JOIN (
       {0}.CMC_PDBC_PROD_COMP CMC_PDBC_PROD_COMP
       INNER JOIN {0}.CMC_PDBL_PROD_BILL CMC_PDBL_PROD_BILL
         ON CMC_PDBC_PROD_COMP.PDBC_PFX = CMC_PDBL_PROD_BILL.PDBC_PFX
    )
    ON CMC_CLCL_CLAIM.PDPD_ID = CMC_PDBC_PROD_COMP.PDPD_ID
    LEFT OUTER JOIN {0}.CMC_CLHP_HOSP CMC_CLHP_HOSP
       ON CMC_CLHP_HOSP.CLCL_ID=CMC_CLCL_CLAIM.CLCL_ID
WHERE
   (CMC_CLCL_CLAIM.CLCL_CL_TYPE = 'M' 
    AND CMC_PDBL_PROD_BILL.PDBL_ID In ('MED','MED1'))
   AND CMC_PDBC_PROD_COMP.PDBC_TYPE='PDBL'
   AND CMC_PDBC_PROD_COMP.PDBC_EFF_DT<=CMC_CLCL_CLAIM.CLCL_LOW_SVC_DT
   AND CMC_PDBC_PROD_COMP.PDBC_TERM_DT>=CMC_CLCL_CLAIM.CLCL_LOW_SVC_DT
   AND CMC_PDBL_PROD_BILL.PDBL_EFF_DT<=CMC_CLCL_CLAIM.CLCL_LOW_SVC_DT
   AND CMC_PDBL_PROD_BILL.PDBL_TERM_DT>=CMC_CLCL_CLAIM.CLCL_LOW_SVC_DT
""".format(FacetsOwner, DrivTable)

df_FacetsDB_Input = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_FacetsDB_Input)
    .load()
)

# -----------------------------------------------------------------------------------------------
# Stage: Xfm_Trim (CTransformerStage) with 3 output links
# -----------------------------------------------------------------------------------------------
# Primary input: df_FacetsDB_Input

# Lnk_Facets columns
df_Xfm_Trim_Lnk_Facets = df_FacetsDB_Input.select(
    F.col("CLCL_ID").alias("CLCL_ID"),
    F.col("GRGR_CK").alias("GRGR_CK"),
    F.col("SBSB_CK").alias("SBSB_CK"),
    F.col("CLCL_CL_TYPE").alias("CLCL_CL_TYPE"),
    F.col("CLCL_CUR_STS").alias("CLCL_CUR_STS"),
    F.col("CLCL_LAST_ACT_DTM").alias("CLCL_LAST_ACT_DTM"),
    F.col("CLCL_INPUT_DT").alias("CLCL_INPUT_DT"),
    F.col("CLCL_RECD_DT").alias("CLCL_RECD_DT"),
    F.col("CLCL_ACPT_DTM").alias("CLCL_ACPT_DTM"),
    F.col("CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    F.col("CLCL_NEXT_REV_DT").alias("CLCL_NEXT_REV_DT"),
    F.col("CLCL_LOW_SVC_DT").alias("CLCL_LOW_SVC_DT"),
    F.col("CLCL_HIGH_SVC_DT").alias("CLCL_HIGH_SVC_DT"),
    F.col("CLCL_ID_ADJ_TO").alias("CLCL_ID_ADJ_TO"),
    F.col("CLCL_ID_ADJ_FROM").alias("CLCL_ID_ADJ_FROM"),
    F.when(
        F.trim(
            F.when(F.col("NWNW_ID").isNotNull(), F.col("NWNW_ID")).otherwise(F.lit("")) 
        ) == F.lit(""),
        F.lit("")
    ).otherwise(F.trim(F.col("NWNW_ID"))).alias("NWNW_ID"),
    F.trim(F.col("CLCL_PAY_PR_IND")).alias("CLCL_PAY_PR_IND"),
    F.trim(F.col("CLCL_ACD_IND")).alias("CLCL_ACD_IND"),
    F.trim(F.col("CLCL_ACD_STATE")).alias("CLCL_ACD_STATE"),
    F.col("CLCL_ACD_DT").alias("CLCL_ACD_DT"),
    F.col("CLCL_ACD_AMT").alias("CLCL_ACD_AMT"),
    F.col("CLCL_PRPR_ID_REF").alias("CLCL_PRPR_ID_REF"),
    F.col("CLCL_PA_ACCT_NO").alias("CLCL_PA_ACCT_NO"),
    F.col("CLCL_PA_PAID_AMT").alias("CLCL_PA_PAID_AMT"),
    F.col("CLCL_TOT_PAYABLE").alias("CLCL_TOT_PAYABLE"),
    F.trim(F.col("CLCL_INPUT_METH")).alias("CLCL_INPUT_METH"),
    F.col("CLCL_UNABL_FROM_DT").alias("CLCL_UNABL_FROM_DT"),
    F.col("CLCL_UNABL_TO_DT").alias("CLCL_UNABL_TO_DT"),
    F.when(
        F.trim(
            F.when(F.col("PDBL_EXP_CAT").isNotNull(), F.col("PDBL_EXP_CAT")).otherwise(F.lit(""))
        ) == F.lit(""),
        F.lit("NA")
    ).otherwise(F.col("PDBL_EXP_CAT")).alias("PDBL_EXP_CAT"),
    F.col("PRPR_MCTR_TYPE").alias("PRPR_MCTR_TYPE"),
    F.trim(F.col("PRCF_MCTR_SPEC")).alias("PRCF_MCTR_SPEC"),
    F.trim(F.col("PDPD_ID")).alias("PDPD_ID"),
    F.trim(F.col("PRPR_ENTITY")).alias("PRPR_ENTITY"),
    F.trim(F.col("PDBL_ACCT_CAT")).alias("PDBL_ACCT_CAT"),
    F.when(
        (F.col("CLCL_CL_SUB_TYPE") == F.lit("M")) | (F.col("CLCL_CL_SUB_TYPE") == F.lit("D")),
        F.lit("P")
    ).otherwise(
        F.when(
            (F.col("CLCL_CL_SUB_TYPE") == F.lit("H"))
            & F.col("CLHP_FAC_TYPE").isNotNull()
            & (F.col("CLHP_FAC_TYPE") > F.lit("06")),
            F.lit("O")
        ).otherwise(
            F.when(
                (F.col("CLCL_CL_SUB_TYPE") == F.lit("H"))
                & F.col("CLHP_FAC_TYPE").isNotNull()
                & (F.col("CLHP_FAC_TYPE") < F.lit("07"))
                & F.col("CLHP_BILL_CLASS").isNotNull()
                & (
                    (F.col("CLHP_BILL_CLASS") == F.lit("3"))
                    | (F.col("CLHP_BILL_CLASS") == F.lit("4"))
                ),
                F.lit("O")
            ).otherwise(
                F.when(
                    F.col("CLCL_CL_SUB_TYPE") == F.lit("H"),
                    F.lit("I")
                ).otherwise(F.lit("UNK"))
            )
        )
    ).alias("CLM_SUB_TYPE_SOURCE"),
    F.trim(F.col("PRPR_ID")).alias("PRPR_ID"),
    F.col("GRGR_ID").alias("GRGR_ID")
)

# Lnk_ToOthersLkup columns
df_Xfm_Trim_Lnk_ToOthersLkup = df_FacetsDB_Input.select(
    F.col("CLCL_ID").alias("CLCL_ID_FROM"),
    F.col("CLCL_CUR_STS").alias("CLCL_CUR_STS"),
    F.col("CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    F.col("CLCL_ID_ADJ_TO").alias("CLCL_ID_ADJ_TO"),
    F.col("CLCL_ID_ADJ_FROM").alias("CLCL_ID_ADJ_FROM")
)

# Lnk_ToFindReversal columns
df_Xfm_Trim_Lnk_ToFindReversal = df_FacetsDB_Input.select(
    F.col("CLCL_ID").alias("CLCL_ID"),
    F.col("CLCL_CUR_STS").alias("CLCL_CUR_STS"),
    F.col("CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    F.col("CLCL_LAST_ACT_DTM").alias("CLCL_LAST_ACT_DTM")
)

# -----------------------------------------------------------------------------------------------
# Stage: CD_MPPNG (DB2ConnectorPX) - READ from IDS
# -----------------------------------------------------------------------------------------------
extract_query_CD_MPPNG = f"""
SELECT DISTINCT
   SRC_CD,
   CD_MPPNG_SK,
   SRC_DOMAIN_NM,
   TRGT_DOMAIN_NM
FROM {IDSOwner}.CD_MPPNG
WHERE 
  SRC_SYS_CD = 'FACETS'
  AND SRC_CLCTN_CD = 'FACETS DBO'
  AND SRC_DOMAIN_NM IN ( 'CLAIM ACCIDENT' , 'STATE','CUSTOM DOMAIN MAPPING', 'CLAIM INPUT METHOD', 'CLAIM PAYEE','PROVIDER SPECIALTY','CLAIM STATUS','CUSTOM DOMAIN MAPPING','CLAIM TYPE','PROVIDER TYPE' )
  AND TRGT_CLCTN_CD = 'IDS'
  AND TRGT_DOMAIN_NM  IN ( 'CLAIM ACCIDENT' , 'STATE','CLAIM FINALIZE DISPOSITION', 'CLAIM INPUT METHOD', 'CLAIM PAYEE','PROVIDER SPECIALTY','CLAIM STATUS','CUSTOM DOMAIN MAPPING','CLAIM TYPE','CLAIM SUBTYPE','PROVIDER TYPE')
  AND Trim(SRC_DRVD_LKUP_VAL) <> ''
"""

df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_CD_MPPNG)
    .load()
)

# -----------------------------------------------------------------------------------------------
# Stage: Flt_CdMppng (PxFilter) with multiple outputs
# -----------------------------------------------------------------------------------------------
# We will create separate DataFrames for each filter output link
df_Flt_CdMppng = df_CD_MPPNG

df_Flt_CdMppng_ClmAcci = df_Flt_CdMppng.filter(
    (F.col("SRC_DOMAIN_NM") == "CLAIM ACCIDENT") & (F.col("TRGT_DOMAIN_NM") == "CLAIM ACCIDENT")
)

df_Flt_CdMppng_State = df_Flt_CdMppng.filter(
    (F.col("SRC_DOMAIN_NM") == "STATE") & (F.col("TRGT_DOMAIN_NM") == "STATE")
)

df_Flt_CdMppng_FnlDispos = df_Flt_CdMppng.filter(
    (F.col("SRC_DOMAIN_NM") == "CUSTOM DOMAIN MAPPING") & (F.col("TRGT_DOMAIN_NM") == "CLAIM FINALIZE DISPOSITION")
)

df_Flt_CdMppng_InpMethod = df_Flt_CdMppng.filter(
    (F.col("SRC_DOMAIN_NM") == "CLAIM INPUT METHOD") & (F.col("TRGT_DOMAIN_NM") == "CLAIM INPUT METHOD")
)

df_Flt_CdMppng_ClmPayee = df_Flt_CdMppng.filter(
    (F.col("SRC_DOMAIN_NM") == "CLAIM PAYEE") & (F.col("TRGT_DOMAIN_NM") == "CLAIM PAYEE")
)

df_Flt_CdMppng_ClmStatus = df_Flt_CdMppng.filter(
    (F.col("SRC_DOMAIN_NM") == "CLAIM STATUS") & (F.col("TRGT_DOMAIN_NM") == "CLAIM STATUS")
)

df_Flt_CdMppng_DomainMapng = df_Flt_CdMppng.filter(
    (F.col("SRC_DOMAIN_NM") == "CUSTOM DOMAIN MAPPING") & (F.col("TRGT_DOMAIN_NM") == "CLAIM SUBTYPE")
)

df_Flt_CdMppng_ClmType = df_Flt_CdMppng.filter(
    (F.col("SRC_DOMAIN_NM") == "CLAIM TYPE") & (F.col("TRGT_DOMAIN_NM") == "CLAIM TYPE")
)

df_Flt_CdMppng_Prov_Type = df_Flt_CdMppng.filter(
    (F.col("SRC_DOMAIN_NM") == "PROVIDER TYPE") & (F.col("TRGT_DOMAIN_NM") == "PROVIDER TYPE")
)

# We only select columns needed in each link
df_Flt_CdMppng_ClmAcci = df_Flt_CdMppng_ClmAcci.select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)
df_Flt_CdMppng_State = df_Flt_CdMppng_State.select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)
df_Flt_CdMppng_FnlDispos = df_Flt_CdMppng_FnlDispos.select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)
df_Flt_CdMppng_InpMethod = df_Flt_CdMppng_InpMethod.select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)
df_Flt_CdMppng_ClmPayee = df_Flt_CdMppng_ClmPayee.select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)
df_Flt_CdMppng_ClmStatus = df_Flt_CdMppng_ClmStatus.select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)
df_Flt_CdMppng_DomainMapng = df_Flt_CdMppng_DomainMapng.select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)
df_Flt_CdMppng_ClmType = df_Flt_CdMppng_ClmType.select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)
df_Flt_CdMppng_Prov_Type = df_Flt_CdMppng_Prov_Type.select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# -----------------------------------------------------------------------------------------------
# Stage: FNCL_LOB (DB2ConnectorPX) - READ from IDS
# -----------------------------------------------------------------------------------------------
extract_query_FNCL_LOB = f"""
SELECT 
FNCL_LOB.FNCL_LOB_CD,
FNCL_LOB.SRC_SYS_CD_SK,
FNCL_LOB.FNCL_LOB_SK
FROM {IDSOwner}.FNCL_LOB FNCL_LOB
WHERE FNCL_LOB.SRC_SYS_CD_SK= (
  SELECT CD_MPPNG_SK 
  FROM {IDSOwner}.CD_MPPNG 
  where TRGT_CD='PSI' 
    AND SRC_CD_NM='PEOPLESOFT INPUT'
)
"""

df_FNCL_LOB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_FNCL_LOB)
    .load()
)

# -----------------------------------------------------------------------------------------------
# Stage: ProvSpec (DB2ConnectorPX) - READ from IDS
# -----------------------------------------------------------------------------------------------
extract_query_ProvSpec = f"""
SELECT PROV_SPEC_CD.PROV_SPEC_CD,PROV_SPEC_CD.PROV_SPEC_CD_SK 
FROM {IDSOwner}.PROV_SPEC_CD PROV_SPEC_CD
"""

df_ProvSpec = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ProvSpec)
    .load()
)

# -----------------------------------------------------------------------------------------------
# Stage: Ds_FinalDisp (PxDataSet) - READ from DataSet => read from parquet instead
# -----------------------------------------------------------------------------------------------
# Original file: ds/FinalDispDataset.ds => translate to ds/FinalDispDataset.parquet
df_Ds_FinalDisp = spark.read.parquet(f"{adls_path}/ds/FinalDispDataset.parquet")

# -----------------------------------------------------------------------------------------------
# Stage: CMC_CDOR_LI_OVR (ODBCConnectorPX) - READ
# -----------------------------------------------------------------------------------------------
extract_query_CMC_CDOR_LI_OVR = """SELECT DISTINCT
CMC_CDOR_LI_OVR.CLCL_ID,
CMC_CDOR_LI_OVR.CDML_SEQ_NO,
CMC_CDOR_LI_OVR.CDOR_OR_VALUE
FROM
    {0}.CMC_CDOR_LI_OVR CMC_CDOR_LI_OVR
    INNER JOIN tempdb..{1} Drvr ON Drvr.CLM_ID=CMC_CDOR_LI_OVR.CLCL_ID
WHERE
CMC_CDOR_LI_OVR.CDOR_OR_ID = 'XR'
AND CMC_CDOR_LI_OVR.CDOR_OR_VALUE is not NULL
AND CMC_CDOR_LI_OVR.CDOR_OR_VALUE <> ''
""".format(FacetsOwner, DrivTable)

df_CMC_CDOR_LI_OVR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_CDOR_LI_OVR)
    .load()
)

# -----------------------------------------------------------------------------------------------
# Stage: Rmv_Dups_OR (PxRemDup)
# -----------------------------------------------------------------------------------------------
# KeysThatDefineDuplicates = CLCL_ID
# RetainRecord = first
# Sort by CLCL_ID asc, CDML_SEQ_NO asc
df_CMC_CDOR_LI_OVR_sorted = df_CMC_CDOR_LI_OVR.sort(
    F.col("CLCL_ID").asc(), F.col("CDML_SEQ_NO").asc()
)
df_Rmv_Dups_OR = dedup_sort(
    df_CMC_CDOR_LI_OVR_sorted,
    partition_cols=["CLCL_ID"],
    sort_cols=[("CDML_SEQ_NO","A")]
)

# Output columns
df_Rmv_Dups_OR_out = df_Rmv_Dups_OR.select(
    F.col("CLCL_ID").alias("CLCL_ID"),
    F.col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.col("CDOR_OR_VALUE").alias("CDOR_OR_VALUE")
)

# -----------------------------------------------------------------------------------------------
# Stage: Lkp_DB2DB (PxLookup) with multiple lookup links, primary link = Lnk_Facets
# -----------------------------------------------------------------------------------------------
# Primary Link => df_Xfm_Trim_Lnk_Facets
# Lookup Links => df_FNCL_LOB (left join), df_ProvSpec (left join), df_Ds_FinalDisp (left join), df_Rmv_Dups_OR_out (left join)
df_lkp_db2db_join1 = df_Xfm_Trim_Lnk_Facets.join(
    df_FNCL_LOB,
    on=(df_Xfm_Trim_Lnk_Facets["PDBL_ACCT_CAT"] == df_FNCL_LOB["FNCL_LOB_CD"]),
    how="left"
).withColumnRenamed("FNCL_LOB_SK","FNCL_LOB_SK_lookup")

df_lkp_db2db_join2 = df_lkp_db2db_join1.join(
    df_ProvSpec,
    on=(df_lkp_db2db_join1["PRCF_MCTR_SPEC"] == df_ProvSpec["PROV_SPEC_CD"]),
    how="left"
).withColumnRenamed("PROV_SPEC_CD_SK","PROV_SPEC_CD_SK_lookup")

df_lkp_db2db_join3 = df_lkp_db2db_join2.join(
    df_Ds_FinalDisp,
    on=(df_lkp_db2db_join2["CLCL_ID"] == df_Ds_FinalDisp["CLCL_ID"]),
    how="left"
).withColumnRenamed("FINL_DISP_CD","FINL_DISP_CD_lookup")

df_lkp_db2db_join4 = df_lkp_db2db_join3.join(
    df_Rmv_Dups_OR_out,
    on=(df_lkp_db2db_join3["CLCL_ID"] == df_Rmv_Dups_OR_out["CLCL_ID"]),
    how="left"
).withColumnRenamed("CDOR_OR_VALUE","CDOR_OR_VALUE_lookup")

df_Lkp_DB2DB_out = df_lkp_db2db_join4.select(
    F.col("CLCL_ID"),
    F.col("GRGR_CK"),
    F.col("SBSB_CK"),
    F.col("CLCL_CL_TYPE"),
    F.col("CLCL_CUR_STS"),
    F.col("CLCL_LAST_ACT_DTM"),
    F.col("CLCL_INPUT_DT"),
    F.col("CLCL_RECD_DT"),
    F.col("CLCL_ACPT_DTM"),
    F.col("CLCL_PAID_DT"),
    F.col("CLCL_NEXT_REV_DT"),
    F.col("CLCL_LOW_SVC_DT"),
    F.col("CLCL_HIGH_SVC_DT"),
    F.col("CLCL_ID_ADJ_TO"),
    F.col("CLCL_ID_ADJ_FROM"),
    F.col("NWNW_ID"),
    F.col("CLCL_PAY_PR_IND"),
    F.col("CLCL_ACD_IND"),
    F.col("CLCL_ACD_STATE"),
    F.col("CLCL_ACD_DT"),
    F.col("CLCL_ACD_AMT"),
    F.col("CLCL_PRPR_ID_REF"),
    F.col("CLCL_PA_ACCT_NO"),
    F.col("CLCL_PA_PAID_AMT"),
    F.col("CLCL_TOT_PAYABLE"),
    F.col("CLCL_INPUT_METH"),
    F.col("CLCL_UNABL_FROM_DT"),
    F.col("CLCL_UNABL_TO_DT"),
    F.col("PDBL_EXP_CAT"),
    F.col("PRPR_MCTR_TYPE"),
    F.col("PRCF_MCTR_SPEC"),
    F.col("PDPD_ID"),
    F.col("PRPR_ENTITY"),
    F.col("CLM_SUB_TYPE_SOURCE").alias("CD_MPPNG_SRCLKPVAL"),
    F.col("FNCL_LOB_SK_lookup").alias("FNCL_LOB_SK"),
    F.col("PDBL_ACCT_CAT"),
    F.col("PROV_SPEC_CD_SK_lookup").alias("PROV_SPEC_CD_SK"),
    F.col("PRPR_ID"),
    F.col("GRGR_ID"),
    F.col("FINL_DISP_CD_lookup").alias("FINL_DISP_CD"),
    F.col("CDOR_OR_VALUE_lookup").alias("CDOR_OR_VALUE")
)

# -----------------------------------------------------------------------------------------------
# Stage: Xfm_FnlDisp (CTransformerStage) with 2 outputs (Lnk_Others, Lnk_Reversals)
# -----------------------------------------------------------------------------------------------
df_Xfm_FnlDisp_Lnk_Others = df_Lkp_DB2DB_out.select(
    F.col("CLCL_ID").alias("CLCL_ID"),
    F.col("GRGR_CK").alias("GRGR_CK"),
    F.col("SBSB_CK").alias("SBSB_CK"),
    F.col("CLCL_CL_TYPE").alias("CLCL_CL_TYPE"),
    F.col("CLCL_CUR_STS").alias("CLCL_CUR_STS"),
    F.col("CLCL_LAST_ACT_DTM").alias("CLCL_LAST_ACT_DTM"),
    F.col("CLCL_INPUT_DT").alias("CLCL_INPUT_DT"),
    F.col("CLCL_RECD_DT").alias("CLCL_RECD_DT"),
    F.col("CLCL_ACPT_DTM").alias("CLCL_ACPT_DTM"),
    F.col("CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    F.col("CLCL_NEXT_REV_DT").alias("CLCL_NEXT_REV_DT"),
    F.col("CLCL_LOW_SVC_DT").alias("CLCL_LOW_SVC_DT"),
    F.col("CLCL_HIGH_SVC_DT").alias("CLCL_HIGH_SVC_DT"),
    F.col("CLCL_ID_ADJ_TO").alias("CLCL_ID_ADJ_TO"),
    F.col("CLCL_ID_ADJ_FROM").alias("CLCL_ID_ADJ_FROM"),
    F.col("NWNW_ID").alias("NWNW_ID"),
    F.col("CLCL_PAY_PR_IND").alias("CLCL_PAY_PR_IND"),
    F.col("CLCL_ACD_IND").alias("CLCL_ACD_IND"),
    F.col("CLCL_ACD_STATE").alias("CLCL_ACD_STATE"),
    F.col("CLCL_ACD_DT").alias("CLCL_ACD_DT"),
    F.col("CLCL_ACD_AMT").alias("CLCL_ACD_AMT"),
    F.col("CLCL_PRPR_ID_REF").alias("CLCL_PRPR_ID_REF"),
    F.col("CLCL_PA_ACCT_NO").alias("CLCL_PA_ACCT_NO"),
    F.col("CLCL_PA_PAID_AMT").alias("CLCL_PA_PAID_AMT"),
    F.col("CLCL_TOT_PAYABLE").alias("CLCL_TOT_PAYABLE"),
    F.col("CLCL_INPUT_METH").alias("CLCL_INPUT_METH"),
    F.col("CLCL_UNABL_FROM_DT").alias("CLCL_UNABL_FROM_DT"),
    F.col("CLCL_UNABL_TO_DT").alias("CLCL_UNABL_TO_DT"),
    F.col("PDBL_EXP_CAT").alias("PDBL_EXP_CAT"),
    F.col("PRPR_MCTR_TYPE").alias("PRPR_MCTR_TYPE"),
    F.col("PRCF_MCTR_SPEC").alias("PRCF_MCTR_SPEC"),
    F.col("PDPD_ID").alias("PDPD_ID"),
    F.col("PRPR_ENTITY").alias("PRPR_ENTITY"),
    F.col("CD_MPPNG_SRCLKPVAL").alias("CD_MPPNG_SRCLKPVAL"),
    F.col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("PDBL_ACCT_CAT").alias("PDBL_ACCT_CAT"),
    F.col("PROV_SPEC_CD_SK").alias("PROV_SPEC_CD_SK"),
    F.col("PRPR_ID").alias("PRPR_ID"),
    F.col("GRGR_ID").alias("GRGR_ID"),
    F.when(
        F.trim(F.when(F.col("FINL_DISP_CD").isNotNull(), F.col("FINL_DISP_CD")).otherwise(F.lit(""))) == "",
        F.lit("UNK")
    ).otherwise(F.col("FINL_DISP_CD")).alias("FINL_DISP_CD"),
    F.col("CDOR_OR_VALUE").alias("CDOR_OR_VALUE"),
    F.col("CLCL_ID_ADJ_FROM").alias("CLCL_ID_FROM")  # rename for next stage
)

df_Xfm_FnlDisp_Lnk_Reversals = df_Lkp_DB2DB_out.filter(
    (F.col("CLCL_CUR_STS") == "89") | (F.col("CLCL_CUR_STS") == "91")
).select(
    F.trim(F.col("CLCL_ID")).alias("CLCL_ID"),
    F.col("GRGR_CK").alias("GRGR_CK"),
    F.col("SBSB_CK").alias("SBSB_CK"),
    F.col("CLCL_CL_TYPE").alias("CLCL_CL_TYPE"),
    F.lit("R").alias("CLCL_CUR_STS"),  # WhereExpression => "R"
    F.col("CLCL_LAST_ACT_DTM").alias("CLCL_LAST_ACT_DTM"),
    F.col("CLCL_INPUT_DT").alias("CLCL_INPUT_DT"),
    F.col("CLCL_RECD_DT").alias("CLCL_RECD_DT"),
    F.col("CLCL_ACPT_DTM").alias("CLCL_ACPT_DTM"),
    F.col("CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    F.col("CLCL_NEXT_REV_DT").alias("CLCL_NEXT_REV_DT"),
    F.col("CLCL_LOW_SVC_DT").alias("CLCL_LOW_SVC_DT"),
    F.col("CLCL_HIGH_SVC_DT").alias("CLCL_HIGH_SVC_DT"),
    F.col("CLCL_ID_ADJ_TO").alias("CLCL_ID_ADJ_TO"),
    F.col("CLCL_ID").alias("CLCL_ID_ADJ_FROM"),  # per specs
    F.col("NWNW_ID").alias("NWNW_ID"),
    F.col("CLCL_PAY_PR_IND").alias("CLCL_PAY_PR_IND"),
    F.col("CLCL_ACD_IND").alias("CLCL_ACD_IND"),
    F.col("CLCL_ACD_STATE").alias("CLCL_ACD_STATE"),
    F.col("CLCL_ACD_DT").alias("CLCL_ACD_DT"),
    F.col("CLCL_ACD_AMT").alias("CLCL_ACD_AMT"),
    F.col("CLCL_PRPR_ID_REF").alias("CLCL_PRPR_ID_REF"),
    F.col("CLCL_PA_ACCT_NO").alias("CLCL_PA_ACCT_NO"),
    F.col("CLCL_PA_PAID_AMT").alias("CLCL_PA_PAID_AMT"),
    F.col("CLCL_TOT_PAYABLE").alias("CLCL_TOT_PAYABLE"),
    F.col("CLCL_INPUT_METH").alias("CLCL_INPUT_METH"),
    F.col("CLCL_UNABL_FROM_DT").alias("CLCL_UNABL_FROM_DT"),
    F.col("CLCL_UNABL_TO_DT").alias("CLCL_UNABL_TO_DT"),
    F.col("PDBL_EXP_CAT").alias("PDBL_EXP_CAT"),
    F.col("PRPR_MCTR_TYPE").alias("PRPR_MCTR_TYPE"),
    F.col("PRCF_MCTR_SPEC").alias("PRCF_MCTR_SPEC"),
    F.col("PDPD_ID").alias("PDPD_ID"),
    F.col("PRPR_ENTITY").alias("PRPR_ENTITY"),
    F.col("CD_MPPNG_SRCLKPVAL").alias("CD_MPPNG_SRCLKPVAL"),
    F.col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("PDBL_ACCT_CAT").alias("PDBL_ACCT_CAT"),
    F.col("PROV_SPEC_CD_SK").alias("PROV_SPEC_CD_SK"),
    F.col("PRPR_ID").alias("PRPR_ID"),
    F.col("GRGR_ID").alias("GRGR_ID"),
    F.when(
        F.trim(F.when(F.col("FINL_DISP_CD").isNotNull(), F.col("FINL_DISP_CD")).otherwise(F.lit(""))) == "",
        F.lit("UNK")
    ).otherwise(F.col("FINL_DISP_CD")).alias("FINL_DISP_CD"),
    F.col("CDOR_OR_VALUE").alias("CDOR_OR_VALUE")
)

# -----------------------------------------------------------------------------------------------
# Stage: Lkup_Reversal (PxLookup) => PrimaryLink = Lnk_Reversals, LookupLink = Lnk_ToFindReversal (inner join)
# -----------------------------------------------------------------------------------------------
df_Lkup_Reversal_join = df_Xfm_FnlDisp_Lnk_Reversals.join(
    df_Xfm_Trim_Lnk_ToFindReversal,
    on=(df_Xfm_FnlDisp_Lnk_Reversals["CLCL_ID_ADJ_TO"] == df_Xfm_Trim_Lnk_ToFindReversal["CLCL_ID"]),
    how="inner"
).select(
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_ID"].alias("CLCL_ID"),
    df_Xfm_FnlDisp_Lnk_Reversals["GRGR_CK"].alias("GRGR_CK"),
    df_Xfm_FnlDisp_Lnk_Reversals["SBSB_CK"].alias("SBSB_CK"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_CL_TYPE"].alias("CLCL_CL_TYPE"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_CUR_STS"].alias("CLCL_CUR_STS"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_LAST_ACT_DTM"].alias("CLCL_LAST_ACT_DTM"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_INPUT_DT"].alias("CLCL_INPUT_DT"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_RECD_DT"].alias("CLCL_RECD_DT"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_ACPT_DTM"].alias("CLCL_ACPT_DTM"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_PAID_DT"].alias("CLCL_PAID_DT"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_NEXT_REV_DT"].alias("CLCL_NEXT_REV_DT"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_LOW_SVC_DT"].alias("CLCL_LOW_SVC_DT"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_HIGH_SVC_DT"].alias("CLCL_HIGH_SVC_DT"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_ID_ADJ_TO"].alias("CLCL_ID_ADJ_TO"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_ID_ADJ_FROM"].alias("CLCL_ID_ADJ_FROM"),
    df_Xfm_FnlDisp_Lnk_Reversals["NWNW_ID"].alias("NWNW_ID"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_PAY_PR_IND"].alias("CLCL_PAY_PR_IND"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_ACD_IND"].alias("CLCL_ACD_IND"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_ACD_STATE"].alias("CLCL_ACD_STATE"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_ACD_DT"].alias("CLCL_ACD_DT"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_ACD_AMT"].alias("CLCL_ACD_AMT"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_PRPR_ID_REF"].alias("CLCL_PRPR_ID_REF"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_PA_ACCT_NO"].alias("CLCL_PA_ACCT_NO"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_PA_PAID_AMT"].alias("CLCL_PA_PAID_AMT"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_TOT_PAYABLE"].alias("CLCL_TOT_PAYABLE"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_INPUT_METH"].alias("CLCL_INPUT_METH"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_UNABL_FROM_DT"].alias("CLCL_UNABL_FROM_DT"),
    df_Xfm_FnlDisp_Lnk_Reversals["CLCL_UNABL_TO_DT"].alias("CLCL_UNABL_TO_DT"),
    df_Xfm_FnlDisp_Lnk_Reversals["PDBL_EXP_CAT"].alias("PDBL_EXP_CAT"),
    df_Xfm_FnlDisp_Lnk_Reversals["PRPR_MCTR_TYPE"].alias("PRPR_MCTR_TYPE"),
    df_Xfm_FnlDisp_Lnk_Reversals["PRCF_MCTR_SPEC"].alias("PRCF_MCTR_SPEC"),
    df_Xfm_FnlDisp_Lnk_Reversals["PDPD_ID"].alias("PDPD_ID"),
    df_Xfm_FnlDisp_Lnk_Reversals["PRPR_ENTITY"].alias("PRPR_ENTITY"),
    df_Xfm_FnlDisp_Lnk_Reversals["CD_MPPNG_SRCLKPVAL"].alias("CD_MPPNG_SRCLKPVAL"),
    df_Xfm_FnlDisp_Lnk_Reversals["FNCL_LOB_SK"].alias("FNCL_LOB_SK"),
    df_Xfm_FnlDisp_Lnk_Reversals["PDBL_ACCT_CAT"].alias("PDBL_ACCT_CAT"),
    df_Xfm_FnlDisp_Lnk_Reversals["PROV_SPEC_CD_SK"].alias("PROV_SPEC_CD_SK"),
    df_Xfm_FnlDisp_Lnk_Reversals["PRPR_ID"].alias("PRPR_ID"),
    df_Xfm_FnlDisp_Lnk_Reversals["GRGR_ID"].alias("GRGR_ID"),
    df_Xfm_FnlDisp_Lnk_Reversals["FINL_DISP_CD"].alias("FINL_DISP_CD"),
    df_Xfm_FnlDisp_Lnk_Reversals["CDOR_OR_VALUE"].alias("CDOR_OR_VALUE"),
    df_Xfm_Trim_Lnk_ToFindReversal["CLCL_CUR_STS"].alias("CLCL_CUR_STS_1"),
    df_Xfm_Trim_Lnk_ToFindReversal["CLCL_PAID_DT"].alias("CLCL_PAID_DT_1"),
    df_Xfm_Trim_Lnk_ToFindReversal["CLCL_LAST_ACT_DTM"].alias("CLCL_LAST_ACT_DTM_1")
)

# -----------------------------------------------------------------------------------------------
# Stage: Xfm_Reverse (CTransformerStage) with 2 outputs - Lnk_FromReversal, Lnk_ReversalDs
# -----------------------------------------------------------------------------------------------
df_Xfm_Reverse_Lnk_FromReversal = df_Lkup_Reversal_join.select(
    F.col("CLCL_ID"),
    F.col("GRGR_CK"),
    F.col("SBSB_CK"),
    F.col("CLCL_CL_TYPE"),
    F.trim(F.col("CLCL_CUR_STS")).alias("CLCL_CUR_STS"),
    F.when(
        F.col("CLCL_CUR_STS_1").isNull(),
        F.trim(F.col("CLCL_LAST_ACT_DTM"))
    ).otherwise(
        F.when(
            F.instr(F.lit("02_89_99_91"), F.col("CLCL_CUR_STS_1")) > 0,
            F.col("CLCL_LAST_ACT_DTM_1")
        ).otherwise(
            F.trim(F.col("CLCL_LAST_ACT_DTM"))
        )
    ).alias("CLCL_LAST_ACT_DTM"),
    F.col("CLCL_INPUT_DT"),
    F.col("CLCL_RECD_DT"),
    F.col("CLCL_ACPT_DTM"),
    F.when(
        F.col("CLCL_PAID_DT_1").isNull(),
        F.col("CLCL_PAID_DT")
    ).otherwise(
        F.when(
            F.instr(F.lit("02_89_99_91"), F.col("CLCL_CUR_STS_1")) > 0,
            F.col("CLCL_PAID_DT_1")
        ).otherwise(
            F.col("CLCL_PAID_DT")
        )
    ).alias("CLCL_PAID_DT"),
    F.col("CLCL_NEXT_REV_DT"),
    F.col("CLCL_LOW_SVC_DT"),
    F.col("CLCL_HIGH_SVC_DT"),
    F.col("CLCL_ID_ADJ_TO"),
    F.col("CLCL_ID_ADJ_FROM"),
    F.col("NWNW_ID"),
    F.col("CLCL_PAY_PR_IND"),
    F.col("CLCL_ACD_IND"),
    F.col("CLCL_ACD_STATE"),
    F.col("CLCL_ACD_DT"),
    F.col("CLCL_ACD_AMT"),
    F.col("CLCL_PRPR_ID_REF"),
    F.col("CLCL_PA_ACCT_NO"),
    F.col("CLCL_PA_PAID_AMT"),
    F.col("CLCL_TOT_PAYABLE"),
    F.col("CLCL_INPUT_METH"),
    F.col("CLCL_UNABL_FROM_DT"),
    F.col("CLCL_UNABL_TO_DT"),
    F.col("PDBL_EXP_CAT"),
    F.col("PRPR_MCTR_TYPE"),
    F.col("PRCF_MCTR_SPEC"),
    F.col("PDPD_ID"),
    F.col("PRPR_ENTITY"),
    F.col("CD_MPPNG_SRCLKPVAL"),
    F.col("FNCL_LOB_SK"),
    F.col("PDBL_ACCT_CAT"),
    F.col("PROV_SPEC_CD_SK"),
    F.col("PRPR_ID"),
    F.col("GRGR_ID"),
    F.col("FINL_DISP_CD"),
    F.col("CDOR_OR_VALUE")
)

df_Xfm_Reverse_Lnk_ReversalDs = df_Lkup_Reversal_join.select(
    F.col("CLCL_ID"),
    F.col("CLCL_CUR_STS_1").alias("CLCL_CUR_STS"),
    F.col("CLCL_PAID_DT_1").alias("CLCL_PAID_DT")
)

# -----------------------------------------------------------------------------------------------
# Stage: Ds_ReversalClaim (PxDataSet) - WRITE to .parquet
# -----------------------------------------------------------------------------------------------
df_Ds_ReversalClaim_out = df_Xfm_Reverse_Lnk_ReversalDs.select("CLCL_ID","CLCL_CUR_STS","CLCL_PAID_DT")
write_files(
    df_Ds_ReversalClaim_out,
    f"{adls_path}/ds/Clm_Reversals.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote='"',
    nullValue=None
)

# -----------------------------------------------------------------------------------------------
# Stage: Lkup_Others (PxLookup) => PrimaryLink=Lnk_Others, LookupLink=Lnk_ToOthersLkup (left)
# -----------------------------------------------------------------------------------------------
df_Lkup_Others_join = df_Xfm_FnlDisp_Lnk_Others.join(
    df_Xfm_Trim_Lnk_ToOthersLkup,
    on=(df_Xfm_FnlDisp_Lnk_Others["CLCL_ID_FROM"] == df_Xfm_Trim_Lnk_ToOthersLkup["CLCL_ID_FROM"]),
    how="left"
).select(
    df_Xfm_FnlDisp_Lnk_Others["CLCL_ID"].alias("CLCL_ID"),
    df_Xfm_FnlDisp_Lnk_Others["GRGR_CK"].alias("GRGR_CK"),
    df_Xfm_FnlDisp_Lnk_Others["SBSB_CK"].alias("SBSB_CK"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_CL_TYPE"].alias("CLCL_CL_TYPE"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_CUR_STS"].alias("CLCL_CUR_STS"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_LAST_ACT_DTM"].alias("CLCL_LAST_ACT_DTM"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_INPUT_DT"].alias("CLCL_INPUT_DT"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_RECD_DT"].alias("CLCL_RECD_DT"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_ACPT_DTM"].alias("CLCL_ACPT_DTM"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_PAID_DT"].alias("CLCL_PAID_DT"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_NEXT_REV_DT"].alias("CLCL_NEXT_REV_DT"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_LOW_SVC_DT"].alias("CLCL_LOW_SVC_DT"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_HIGH_SVC_DT"].alias("CLCL_HIGH_SVC_DT"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_ID_ADJ_TO"].alias("CLCL_ID_ADJ_TO"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_ID_ADJ_FROM"].alias("CLCL_ID_ADJ_FROM"),
    df_Xfm_FnlDisp_Lnk_Others["NWNW_ID"].alias("NWNW_ID"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_PAY_PR_IND"].alias("CLCL_PAY_PR_IND"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_ACD_IND"].alias("CLCL_ACD_IND"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_ACD_STATE"].alias("CLCL_ACD_STATE"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_ACD_DT"].alias("CLCL_ACD_DT"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_ACD_AMT"].alias("CLCL_ACD_AMT"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_PRPR_ID_REF"].alias("CLCL_PRPR_ID_REF"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_PA_ACCT_NO"].alias("CLCL_PA_ACCT_NO"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_PA_PAID_AMT"].alias("CLCL_PA_PAID_AMT"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_TOT_PAYABLE"].alias("CLCL_TOT_PAYABLE"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_INPUT_METH"].alias("CLCL_INPUT_METH"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_UNABL_FROM_DT"].alias("CLCL_UNABL_FROM_DT"),
    df_Xfm_FnlDisp_Lnk_Others["CLCL_UNABL_TO_DT"].alias("CLCL_UNABL_TO_DT"),
    df_Xfm_FnlDisp_Lnk_Others["PDBL_EXP_CAT"].alias("PDBL_EXP_CAT"),
    df_Xfm_FnlDisp_Lnk_Others["PRPR_MCTR_TYPE"].alias("PRPR_MCTR_TYPE"),
    df_Xfm_FnlDisp_Lnk_Others["PRCF_MCTR_SPEC"].alias("PRCF_MCTR_SPEC"),
    df_Xfm_FnlDisp_Lnk_Others["PDPD_ID"].alias("PDPD_ID"),
    df_Xfm_FnlDisp_Lnk_Others["PRPR_ENTITY"].alias("PRPR_ENTITY"),
    df_Xfm_FnlDisp_Lnk_Others["CD_MPPNG_SRCLKPVAL"].alias("CD_MPPNG_SRCLKPVAL"),
    df_Xfm_FnlDisp_Lnk_Others["FNCL_LOB_SK"].alias("FNCL_LOB_SK"),
    df_Xfm_FnlDisp_Lnk_Others["PDBL_ACCT_CAT"].alias("PDBL_ACCT_CAT"),
    df_Xfm_FnlDisp_Lnk_Others["PROV_SPEC_CD_SK"].alias("PROV_SPEC_CD_SK"),
    df_Xfm_FnlDisp_Lnk_Others["PRPR_ID"].alias("PRPR_ID"),
    df_Xfm_FnlDisp_Lnk_Others["GRGR_ID"].alias("GRGR_ID"),
    df_Xfm_FnlDisp_Lnk_Others["FINL_DISP_CD"].alias("FINL_DISP_CD"),
    df_Xfm_FnlDisp_Lnk_Others["CDOR_OR_VALUE"].alias("CDOR_OR_VALUE"),
    df_Xfm_Trim_Lnk_ToOthersLkup["CLCL_ID_ADJ_TO"].alias("CLCL_ID_ADJ_TO_Lkup"),
    df_Xfm_Trim_Lnk_ToOthersLkup["CLCL_ID_ADJ_FROM"].alias("CLCL_ID_ADJ_FROM_Lkup"),
    df_Xfm_Trim_Lnk_ToOthersLkup["CLCL_CUR_STS"].alias("CLCL_CUR_STS_Lkup")
)

# -----------------------------------------------------------------------------------------------
# Stage: Xfm_others (CTransformerStage) => output Lnk_fromall
# -----------------------------------------------------------------------------------------------
df_Xfm_others_Lnk_fromall = df_Lkup_Others_join.select(
    F.col("CLCL_ID"),
    F.col("GRGR_CK"),
    F.col("SBSB_CK"),
    F.col("CLCL_CL_TYPE"),
    F.col("CLCL_CUR_STS"),
    F.col("CLCL_LAST_ACT_DTM"),
    F.col("CLCL_INPUT_DT"),
    F.col("CLCL_RECD_DT"),
    F.col("CLCL_ACPT_DTM"),
    F.col("CLCL_PAID_DT"),
    F.col("CLCL_NEXT_REV_DT"),
    F.col("CLCL_LOW_SVC_DT"),
    F.col("CLCL_HIGH_SVC_DT"),
    F.when(
        (F.col("CLCL_ID_ADJ_TO").isNull()) | (F.trim(F.col("CLCL_ID_ADJ_TO")) == ""),
        F.lit("NA")
    ).otherwise(
        F.when(
            (F.trim(F.col("CLCL_ID_ADJ_TO")) == "NA") | (F.trim(F.col("CLCL_ID_ADJ_TO")) == "UNK"),
            F.col("CLCL_ID_ADJ_TO")
        ).otherwise(
            F.when(
                F.col("CLCL_CUR_STS") == "91",
                F.concat(F.trim(F.col("CLCL_ID")), F.lit("R"))
            ).otherwise(
                F.col("CLCL_ID_ADJ_TO")
            )
        )
    ).alias("CLCL_ID_ADJ_TO"),
    F.when(
        (F.col("CLCL_ID_ADJ_FROM").isNull()) | (F.trim(F.col("CLCL_ID_ADJ_FROM")) == ""),
        F.lit("NA")
    ).otherwise(
        F.when(
            (F.trim(F.col("CLCL_ID_ADJ_FROM")) == "NA") | (F.trim(F.col("CLCL_ID_ADJ_FROM")) == "UNK"),
            F.col("CLCL_ID_ADJ_FROM")
        ).otherwise(
            F.when(
                (F.col("CLCL_ID_ADJ_FROM_Lkup").isNotNull()) & (F.col("CLCL_CUR_STS_Lkup") == "91"),
                F.concat(F.trim(F.col("CLCL_ID_ADJ_FROM")), F.lit("R"))
            ).otherwise(
                F.col("CLCL_ID_ADJ_FROM")
            )
        )
    ).alias("CLCL_ID_ADJ_FROM"),
    F.col("NWNW_ID"),
    F.col("CLCL_PAY_PR_IND"),
    F.col("CLCL_ACD_IND"),
    F.col("CLCL_ACD_STATE"),
    F.col("CLCL_ACD_DT"),
    F.col("CLCL_ACD_AMT"),
    F.col("CLCL_PRPR_ID_REF"),
    F.col("CLCL_PA_ACCT_NO"),
    F.col("CLCL_PA_PAID_AMT"),
    F.col("CLCL_TOT_PAYABLE"),
    F.col("CLCL_INPUT_METH"),
    F.col("CLCL_UNABL_FROM_DT"),
    F.col("CLCL_UNABL_TO_DT"),
    F.col("PDBL_EXP_CAT"),
    F.col("PRPR_MCTR_TYPE"),
    F.col("PRCF_MCTR_SPEC"),
    F.col("PDPD_ID"),
    F.col("PRPR_ENTITY"),
    F.col("CD_MPPNG_SRCLKPVAL"),
    F.col("FNCL_LOB_SK"),
    F.col("PDBL_ACCT_CAT"),
    F.col("PROV_SPEC_CD_SK"),
    F.col("PRPR_ID"),
    F.col("GRGR_ID"),
    F.col("FINL_DISP_CD"),
    F.col("CDOR_OR_VALUE")
)

# -----------------------------------------------------------------------------------------------
# Stage: Fnl_All_Reverse (PxFunnel) => input: Lnk_fromall, Lnk_FromReversal
# -----------------------------------------------------------------------------------------------
df_Fnl_All_Reverse = df_Xfm_others_Lnk_fromall.unionByName(df_Xfm_Reverse_Lnk_FromReversal, allowMissingColumns=True)

# -----------------------------------------------------------------------------------------------
# Stage: LkpAllSets (PxLookup) => primary = Lnk_FacetsLkp = df_Fnl_All_Reverse, multiple lookups
#   Lookups from:
#       State (left), ClmAcci (left), LnkWorkCompnstnClm (left), Lnk_ExprncCat (left),
#       Lnk_Ntwk (left), Lnk_Prod (left), FnlDispos (left), InpMethod (left),
#       ClmPayee (left), ClmStatus (left), ClmType (left), DomainMapng (left), Prov_Type (left).
# -----------------------------------------------------------------------------------------------
# We'll apply each lookup sequentially.

# 1) join with WorkCompnstnClm
df_LkpAllSets_1 = df_Fnl_All_Reverse.join(
    df_WorkCompnstnClm,
    on=(df_Fnl_All_Reverse["CLCL_ID"] == df_WorkCompnstnClm["CLM_ID"]),
    how="left"
).withColumnRenamed("CRT_RUN_CYC_EXCTN_SK","CRT_RUN_CYC_EXCTN_SK_lookup")

# 2) join with EXPRNC_CAT
df_LkpAllSets_2 = df_LkpAllSets_1.join(
    df_EXPRNC_CAT,
    on=(df_LkpAllSets_1["PDBL_EXP_CAT"] == df_EXPRNC_CAT["EXPRNC_CAT_CD"]),
    how="left"
).withColumnRenamed("EXPRNC_CAT_SK","EXPRNC_CAT_SK_lookup")

# 3) join with NTWK
df_LkpAllSets_3 = df_LkpAllSets_2.join(
    df_NTWK,
    on=(df_LkpAllSets_2["NWNW_ID"] == df_NTWK["NTWK_ID"]),
    how="left"
).withColumnRenamed("NTWK_SK","NTWK_SK_lookup")

# 4) join with PROD
df_LkpAllSets_4 = df_LkpAllSets_3.join(
    df_PROD,
    on=(df_LkpAllSets_3["PDPD_ID"] == df_PROD["PROD_ID"]),
    how="left"
).withColumnRenamed("PROD_SK","PROD_SK_lookup")

# 5) join with FnlDispos
df_LkpAllSets_5 = df_LkpAllSets_4.join(
    df_Flt_CdMppng_FnlDispos,
    on=(df_LkpAllSets_4["FINL_DISP_CD"] == df_Flt_CdMppng_FnlDispos["SRC_CD"]),
    how="left"
).withColumnRenamed("CD_MPPNG_SK","CD_MPPNG_SK_FnlDisp")

# 6) join with InpMethod
df_LkpAllSets_6 = df_LkpAllSets_5.join(
    df_Flt_CdMppng_InpMethod,
    on=(df_LkpAllSets_5["CLCL_INPUT_METH"] == df_Flt_CdMppng_InpMethod["SRC_CD"]),
    how="left"
).withColumnRenamed("CD_MPPNG_SK","CD_MPPNG_SK_InpMethod")

# 7) join with ClmPayee
df_LkpAllSets_7 = df_LkpAllSets_6.join(
    df_Flt_CdMppng_ClmPayee,
    on=(df_LkpAllSets_6["CLCL_PAY_PR_IND"] == df_Flt_CdMppng_ClmPayee["SRC_CD"]),
    how="left"
).withColumnRenamed("CD_MPPNG_SK","CD_MPPNG_SK_ClmPayee")

# 8) join with ClmStatus
df_LkpAllSets_8 = df_LkpAllSets_7.join(
    df_Flt_CdMppng_ClmStatus,
    on=(df_LkpAllSets_7["CLCL_CUR_STS"] == df_Flt_CdMppng_ClmStatus["SRC_CD"]),
    how="left"
).withColumnRenamed("CD_MPPNG_SK","CD_MPPNG_SK_ClmStatus")

# 9) join with ClmType
df_LkpAllSets_9 = df_LkpAllSets_8.join(
    df_Flt_CdMppng_ClmType,
    on=(df_LkpAllSets_8["CLCL_CL_TYPE"] == df_Flt_CdMppng_ClmType["SRC_CD"]),
    how="left"
).withColumnRenamed("CD_MPPNG_SK","CD_MPPNG_SK_ClmType")

# 10) join with DomainMapng
df_LkpAllSets_10 = df_LkpAllSets_9.join(
    df_Flt_CdMppng_DomainMapng,
    on=(df_LkpAllSets_9["CD_MPPNG_SRCLKPVAL"] == df_Flt_CdMppng_DomainMapng["SRC_CD"]),
    how="left"
).withColumnRenamed("CD_MPPNG_SK","CD_MPPNG_SK_DomainMapng")

# 11) join with Prov_Type
df_LkpAllSets_11 = df_LkpAllSets_10.join(
    df_Flt_CdMppng_Prov_Type,
    on=(df_LkpAllSets_10["PRPR_MCTR_TYPE"] == df_Flt_CdMppng_Prov_Type["SRC_CD"]),
    how="left"
).withColumnRenamed("CD_MPPNG_SK","CD_MPPNG_SK_ProvType")

# 12) join with State
df_LkpAllSets_12 = df_LkpAllSets_11.join(
    df_Flt_CdMppng_State,
    on=(df_LkpAllSets_11["CLCL_ACD_STATE"] == df_Flt_CdMppng_State["SRC_CD"]),
    how="left"
).withColumnRenamed("CD_MPPNG_SK","CD_MPPNG_SK_State")

# 13) join with ClmAcci
df_LkpAllSets_13 = df_LkpAllSets_12.join(
    df_Flt_CdMppng_ClmAcci,
    on=(df_LkpAllSets_12["CLCL_ACD_IND"] == df_Flt_CdMppng_ClmAcci["SRC_CD"]),
    how="left"
).withColumnRenamed("CD_MPPNG_SK","CD_MPPNG_SK_ClmAcci")

# Now select final columns needed for next stage
df_LkpAllSets_out = df_LkpAllSets_13.select(
    F.col("CLCL_ID"),
    F.col("GRGR_CK"),
    F.col("SBSB_CK"),
    F.col("CLCL_CL_TYPE"),
    F.col("CLCL_CUR_STS"),
    F.col("CLCL_LAST_ACT_DTM"),
    F.col("CLCL_INPUT_DT"),
    F.col("CLCL_RECD_DT"),
    F.col("CLCL_ACPT_DTM"),
    F.col("CLCL_PAID_DT"),
    F.col("CLCL_NEXT_REV_DT"),
    F.col("CLCL_LOW_SVC_DT"),
    F.col("CLCL_HIGH_SVC_DT"),
    F.col("CLCL_ID_ADJ_TO"),
    F.col("CLCL_ID_ADJ_FROM"),
    F.col("NWNW_ID"),
    F.col("CLCL_PAY_PR_IND"),
    F.col("CD_MPPNG_SK_ClmAcci").alias("CLM_ACDNT_CD_SK"),
    F.col("CD_MPPNG_SK_State").alias("CLM_ACDNT_ST_CD_SK"),
    F.col("CLCL_ACD_DT"),
    F.col("CLCL_ACD_AMT"),
    F.col("CLCL_PRPR_ID_REF").alias("CLCL_PRPR_ID_REF"),
    F.col("CLCL_PA_ACCT_NO"),
    F.col("CLCL_PA_PAID_AMT"),
    F.col("CLCL_TOT_PAYABLE"),
    F.col("CLCL_INPUT_METH"),
    F.col("CLCL_UNABL_FROM_DT"),
    F.col("CLCL_UNABL_TO_DT"),
    F.col("PDBL_EXP_CAT"),
    F.col("PRPR_MCTR_TYPE"),
    F.col("PRCF_MCTR_SPEC"),
    F.col("CRT_RUN_CYC_EXCTN_SK_lookup").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CD_MPPNG_SK_ClmAcci"),  # just to keep track if needed
    F.col("CD_MPPNG_SK_State"),
    F.col("EXPRNC_CAT_SK_lookup").alias("EXPRNC_CAT_SK"),
    F.col("NTWK_SK_lookup").alias("NTWK_SK"),
    F.col("PROD_SK_lookup").alias("PROD_SK"),
    F.col("CD_MPPNG_SK_InpMethod").alias("CLM_INPT_METH_CD_SK"),
    F.col("CD_MPPNG_SK_ClmPayee").alias("CLM_PAYE_CD_SK"),
    F.col("CD_MPPNG_SK_ClmStatus").alias("CLM_STTUS_CD_SK"),
    F.col("CD_MPPNG_SK_ClmType").alias("CLM_TYP_CD_SK"),
    df_WorkCompnstnClm["CLM_ID"].alias("CLM_ID"),  # from the joined table
    F.col("CD_MPPNG_SK_DomainMapng").alias("CLM_SUBTYP_CD_SK"),
    F.col("FNCL_LOB_SK"),
    F.col("PDBL_ACCT_CAT"),
    F.col("PDPD_ID"),
    F.col("PRPR_ENTITY"),
    F.col("PROV_SPEC_CD_SK"),
    F.col("PRPR_ID"),
    F.col("GRGR_ID"),
    F.col("CD_MPPNG_SK_FnlDisp").alias("FINAL_DISP_CD_SK"),
    F.col("FINL_DISP_CD"),
    F.col("CLCL_ACD_IND"),
    F.col("CLCL_ACD_STATE"),
    F.col("CD_MPPNG_SK_ProvType").alias("CLM_SVC_PROV_TYP_CD_SK"),
    F.col("CDOR_OR_VALUE")
)

# -----------------------------------------------------------------------------------------------
# Stage: Join_CMC_CdmlClLine (PxJoin) => input: Lnk_JoinCdml (df_LkpAllSets_out), Lnk_CdmlFacets (df_FacetsDB_CdmlInput)
#   key: CLCL_ID, operator=innerjoin
# -----------------------------------------------------------------------------------------------
df_Join_CMC_CdmlClLine = df_LkpAllSets_out.join(
    df_FacetsDB_CdmlInput,
    on=[df_LkpAllSets_out["CLCL_ID"] == df_FacetsDB_CdmlInput["CLCL_ID"]],
    how="inner"
).select(
    df_LkpAllSets_out["CLCL_ID"],
    df_LkpAllSets_out["GRGR_CK"],
    df_LkpAllSets_out["SBSB_CK"],
    df_LkpAllSets_out["CLCL_CL_TYPE"],
    df_LkpAllSets_out["CLCL_CUR_STS"],
    df_LkpAllSets_out["CLCL_LAST_ACT_DTM"],
    df_LkpAllSets_out["CLCL_INPUT_DT"],
    df_LkpAllSets_out["CLCL_RECD_DT"],
    df_LkpAllSets_out["CLCL_ACPT_DTM"],
    df_LkpAllSets_out["CLCL_PAID_DT"],
    df_LkpAllSets_out["CLCL_NEXT_REV_DT"],
    df_LkpAllSets_out["CLCL_LOW_SVC_DT"],
    df_LkpAllSets_out["CLCL_HIGH_SVC_DT"],
    df_LkpAllSets_out["CLCL_ID_ADJ_TO"],
    df_LkpAllSets_out["CLCL_ID_ADJ_FROM"],
    df_LkpAllSets_out["NWNW_ID"],
    df_LkpAllSets_out["CLCL_PAY_PR_IND"],
    df_LkpAllSets_out["CLM_ACDNT_CD_SK"],
    df_LkpAllSets_out["CLM_ACDNT_ST_CD_SK"],
    df_LkpAllSets_out["CLCL_ACD_DT"],
    df_LkpAllSets_out["CLCL_ACD_AMT"],
    df_LkpAllSets_out["CLCL_PRPR_ID_REF"],
    df_LkpAllSets_out["CLCL_PA_ACCT_NO"],
    df_LkpAllSets_out["CLCL_PA_PAID_AMT"],
    df_LkpAllSets_out["CLCL_TOT_PAYABLE"],
    df_LkpAllSets_out["CLCL_INPUT_METH"],
    df_LkpAllSets_out["CLCL_UNABL_FROM_DT"],
    df_LkpAllSets_out["CLCL_UNABL_TO_DT"],
    df_LkpAllSets_out["PDBL_EXP_CAT"],
    df_LkpAllSets_out["PRPR_MCTR_TYPE"],
    df_LkpAllSets_out["PRCF_MCTR_SPEC"],
    df_LkpAllSets_out["CRT_RUN_CYC_EXCTN_SK"],
    df_LkpAllSets_out["EXPRNC_CAT_SK"],
    df_LkpAllSets_out["NTWK_SK"],
    df_LkpAllSets_out["PROD_SK"],
    df_LkpAllSets_out["CLM_INPT_METH_CD_SK"],
    df_LkpAllSets_out["CLM_PAYE_CD_SK"],
    df_LkpAllSets_out["CLM_STTUS_CD_SK"],
    df_LkpAllSets_out["CLM_TYP_CD_SK"],
    df_LkpAllSets_out["CLM_ID"],
    df_LkpAllSets_out["CLM_SUBTYP_CD_SK"],
    df_FacetsDB_CdmlInput["CDML_CHG_AMT"].alias("CDML_CHG_AMT"),
    df_FacetsDB_CdmlInput["CDML_CONSIDER_CHG"].alias("CDML_CONSIDER_CHG"),
    df_FacetsDB_CdmlInput["CDML_ALLOW"].alias("CDML_ALLOW"),
    df_FacetsDB_CdmlInput["CDML_DED_AMT"].alias("CDML_DED_AMT"),
    df_FacetsDB_CdmlInput["CDML_COPAY_AMT"].alias("CDML_COPAY_AMT"),
    df_FacetsDB_CdmlInput["CDML_COINS_AMT"].alias("CDML_COINS_AMT"),
    df_FacetsDB_CdmlInput["CDML_DISALL_AMT"].alias("CDML_DISALL_AMT"),
    df_FacetsDB_CdmlInput["CDML_SB_PYMT_AMT"].alias("CDML_SB_PYMT_AMT"),
    df_FacetsDB_CdmlInput["CDML_PR_PYMT_AMT"].alias("CDML_PR_PYMT_AMT"),
    df_LkpAllSets_out["FNCL_LOB_SK"],
    df_LkpAllSets_out["PDBL_ACCT_CAT"],
    df_LkpAllSets_out["PDPD_ID"],
    df_LkpAllSets_out["PRPR_ENTITY"],
    df_LkpAllSets_out["PROV_SPEC_CD_SK"],
    df_LkpAllSets_out["PRPR_ID"],
    df_LkpAllSets_out["GRGR_ID"],
    df_LkpAllSets_out["FINAL_DISP_CD_SK"],
    df_LkpAllSets_out["CLCL_ACD_IND"],
    df_LkpAllSets_out["CLCL_ACD_STATE"],
    df_LkpAllSets_out["CLM_SVC_PROV_TYP_CD_SK"],
    df_LkpAllSets_out["CDOR_OR_VALUE"]
)

# -----------------------------------------------------------------------------------------------
# Stage: BusinessLogic (CTransformerStage) => output LnkTo_WorkCompnstnClm, Lnk_BalRep_WorkCompnstnClm
# -----------------------------------------------------------------------------------------------
df_BusinessLogic_LnkTo_WorkCompnstnClm = df_Join_CMC_CdmlClLine.select(
    F.when(
        F.trim(F.col("CLCL_CUR_STS")) == "R",
        F.col("CLCL_ID") + F.lit("R")
    ).otherwise(
        F.col("CLCL_ID")
    ).alias("CLM_ID"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.when(
        (F.when(F.col("CRT_RUN_CYC_EXCTN_SK").isNotNull(), F.col("CRT_RUN_CYC_EXCTN_SK")).otherwise(F.lit(0))) == 0,
        F.lit(RunCycle).cast("long")
    ).otherwise(
        F.col("CRT_RUN_CYC_EXCTN_SK")
    ).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLCL_ID_ADJ_FROM").alias("ADJ_FROM_CLM"),
    F.col("CLCL_ID_ADJ_TO").alias("ADJ_TO_CLM"),
    F.when(
        F.trim(F.when(F.col("PDBL_EXP_CAT").isNotNull(), F.col("PDBL_EXP_CAT")).otherwise(F.lit(""))) == "",
        F.lit(1)
    ).otherwise(
        F.col("EXPRNC_CAT_SK")
    ).alias("EXPRNC_CAT_SK"),
    F.when(
        F.col("PDPD_ID").isNull() & (F.trim(F.col("PDPD_ID")) == ""),
        F.lit(1)
    ).otherwise(
        F.when(
            F.col("FNCL_LOB_SK").isNull(),
            F.lit(0)
        ).otherwise(
            F.col("FNCL_LOB_SK")
        )
    ).alias("FNCL_LOB_SK"),
    F.col("GRGR_ID").alias("GRP_ID"),
    F.col("NTWK_SK").alias("NTWK_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("SBSB_CK").alias("SUB_UNIQ_KEY"),
    F.when(
        (F.col("CDOR_OR_VALUE").isNull()) | (F.trim(F.col("CDOR_OR_VALUE")) == ""),
        F.lit("")
    ).otherwise(
        F.substring(F.trim(F.col("CDOR_OR_VALUE")),1,20)
    ).alias("WORK_COMP_PAYMT_TYP_ID"),
    F.when(
        F.trim(F.when(F.col("CLCL_ACD_IND").isNotNull(),F.col("CLCL_ACD_IND")).otherwise(F.lit(""))) == "",
        F.lit(1)
    ).otherwise(
        F.col("CLM_ACDNT_CD_SK")
    ).alias("CLM_ACDNT_CD_SK"),
    F.when(
        F.trim(F.when(F.col("CLCL_ACD_STATE").isNotNull(),F.col("CLCL_ACD_STATE")).otherwise(F.lit(""))) == "",
        F.lit(1)
    ).otherwise(
        F.col("CLM_ACDNT_ST_CD_SK")
    ).alias("CLM_ACDNT_ST_CD_SK"),
    F.col("FINAL_DISP_CD_SK").alias("CLM_FINL_DISP_CD_SK"),
    F.col("CLM_INPT_METH_CD_SK").alias("CLM_INPT_METH_CD_SK"),
    F.when(
        F.trim(F.when(F.col("CLCL_PAY_PR_IND").isNotNull(),F.col("CLCL_PAY_PR_IND")).otherwise(F.lit(""))) == "",
        F.lit(0)
    ).otherwise(
        F.col("CLM_PAYE_CD_SK")
    ).alias("CLM_PAYE_CD_SK"),
    F.when(
        F.col("PRPR_ENTITY") == "F",
        F.lit(1)
    ).otherwise(
        F.when(
            F.trim(F.when(F.col("PRPR_ID").isNotNull(),F.col("PRPR_ID")).otherwise(F.lit(""))) == "",
            F.lit(1)
        ).otherwise(
            F.when(
                F.trim(F.when(F.col("PRCF_MCTR_SPEC").isNotNull(),F.col("PRCF_MCTR_SPEC")).otherwise(F.lit(""))) == "",
                F.lit(1)
            ).otherwise(
                F.when(
                    (F.col("PRPR_ENTITY").isin("I","P","G")) & F.col("PROV_SPEC_CD_SK").isNotNull(),
                    F.col("PROV_SPEC_CD_SK")
                ).otherwise(
                    F.lit(0)
                )
            )
        )
    ).alias("CLM_SVC_PROV_SPEC_CD_SK"),
    F.col("CLM_SVC_PROV_TYP_CD_SK").alias("CLM_SVC_PROV_TYP_CD_SK"),
    F.col("CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK"),
    F.col("CLM_SUBTYP_CD_SK").alias("CLM_SUBTYP_CD_SK"),
    F.col("CLM_TYP_CD_SK").alias("CLM_TYP_CD_SK"),
    # Date transformations
    F.call_udf("TimestampToDate", F.col("CLCL_ACD_DT")).alias("ACDNT_DT"),
    F.call_udf("TimestampToDate", F.col("CLCL_INPUT_DT")).alias("INPT_DT"),
    F.call_udf("TimestampToDate", F.col("CLCL_NEXT_REV_DT")).alias("NEXT_RVW_DT"),
    F.call_udf("TimestampToDate", F.col("CLCL_PAID_DT")).alias("PD_DT"),
    F.call_udf("TimestampToDate", F.col("CLCL_ACPT_DTM")).alias("PRCS_DT"),
    F.call_udf("TimestampToDate", F.col("CLCL_RECD_DT")).alias("RCVD_DT"),
    F.call_udf("TimestampToDate", F.col("CLCL_HIGH_SVC_DT")).alias("SVC_END_DT"),
    F.call_udf("TimestampToDate", F.col("CLCL_LOW_SVC_DT")).alias("SVC_STRT_DT"),
    F.call_udf("TimestampToDate", F.col("CLCL_LAST_ACT_DTM")).alias("STTUS_DT"),
    F.call_udf("TimestampToDate", F.col("CLCL_UNABL_FROM_DT")).alias("WORK_UNABLE_BEG_DT"),
    F.call_udf("TimestampToDate", F.col("CLCL_UNABL_TO_DT")).alias("WORK_UNABLE_END_DT"),
    # Monetary transformations
    F.when(
        F.trim(F.col("CLCL_CUR_STS")) == "R",
        -1 * F.col("CLCL_ACD_AMT")
    ).otherwise(
        F.col("CLCL_ACD_AMT")
    ).alias("ACDNT_AMT"),
    F.when(
        F.trim(F.col("CLCL_CUR_STS")) == "R",
        -1 * (F.col("CDML_SB_PYMT_AMT") + F.col("CDML_PR_PYMT_AMT"))
    ).otherwise(
        F.col("CDML_SB_PYMT_AMT") + F.col("CDML_PR_PYMT_AMT")
    ).alias("ACTL_PD_AMT"),
    F.when(
        F.trim(F.col("CLCL_CUR_STS")) == "R",
        -1 * F.col("CDML_ALLOW")
    ).otherwise(
        F.col("CDML_ALLOW")
    ).alias("ALW_AMT"),
    F.when(
        F.trim(F.col("CLCL_CUR_STS")) == "R",
        -1 * F.col("CDML_CHG_AMT")
    ).otherwise(
        F.col("CDML_CHG_AMT")
    ).alias("CHRG_AMT"),
    F.when(
        F.trim(F.col("CLCL_CUR_STS")) == "R",
        -1 * F.col("CDML_COINS_AMT")
    ).otherwise(
        F.col("CDML_COINS_AMT")
    ).alias("COINS_AMT"),
    F.when(
        F.trim(F.col("CLCL_CUR_STS")) == "R",
        -1 * F.col("CDML_CONSIDER_CHG")
    ).otherwise(
        F.col("CDML_CONSIDER_CHG")
    ).alias("CNSD_CHRG_AMT"),
    F.when(
        F.trim(F.col("CLCL_CUR_STS")) == "R",
        -1 * F.col("CDML_COPAY_AMT")
    ).otherwise(
        F.col("CDML_COPAY_AMT")
    ).alias("COPAY_AMT"),
    F.when(
        F.trim(F.col("CLCL_CUR_STS")) == "R",
        -1 * F.col("CDML_DED_AMT")
    ).otherwise(
        F.col("CDML_DED_AMT")
    ).alias("DEDCT_AMT"),
    F.when(
        F.trim(F.col("CLCL_CUR_STS")) == "R",
        -1 * F.col("CDML_DISALL_AMT")
    ).otherwise(
        F.col("CDML_DISALL_AMT")
    ).alias("DSALW_AMT"),
    F.when(
        F.trim(F.col("CLCL_CUR_STS")) == "R",
        -1 * F.col("CLCL_PA_PAID_AMT")
    ).otherwise(
        F.col("CLCL_PA_PAID_AMT")
    ).alias("PATN_PD_AMT"),
    F.when(
        F.trim(F.col("CLCL_CUR_STS")) == "R",
        -1 * F.col("CLCL_TOT_PAYABLE")
    ).otherwise(
        F.col("CLCL_TOT_PAYABLE")
    ).alias("PAYBL_AMT"),
    F.when(
        F.trim(F.col("CLCL_CUR_STS")) == "R",
        -1 * (F.col("CDML_SB_PYMT_AMT") + F.col("CDML_PR_PYMT_AMT"))
    ).otherwise(
        F.col("CDML_SB_PYMT_AMT") + F.col("CDML_PR_PYMT_AMT")
    ).alias("REMIT_SUPRSION_AMT"),
    F.col("CLCL_PA_ACCT_NO").alias("PATN_ACCT_NO"),
    F.col("CLCL_PRPR_ID_REF").alias("RFRNG_PROV_TX")
)

df_BusinessLogic_Lnk_BalRep_WorkCompnstnClm = df_Join_CMC_CdmlClLine.select(
    F.col("CLCL_ID").alias("CLM_ID"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD")
)

# -----------------------------------------------------------------------------------------------
# Stage: WORK_COMPNSTN_CLM (PxSequentialFile) - WRITE
#   File: load/WORK_COMPNSTN_CLM.dat
# -----------------------------------------------------------------------------------------------
# Before writing, apply rpad to any char/varchar columns in the final SELECT to preserve lengths
df_WORK_COMPNSTN_CLM_out = df_BusinessLogic_LnkTo_WorkCompnstnClm.select(
    F.rpad(F.col("CLM_ID").cast(StringType()), 12, " ").alias("CLM_ID"),  # typical max length is unknown, chosen 12 to match char(12) if relevant
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ADJ_FROM_CLM"),
    F.col("ADJ_TO_CLM"),
    F.col("EXPRNC_CAT_SK"),
    F.col("FNCL_LOB_SK"),
    F.col("GRP_ID"),
    F.col("NTWK_SK"),
    F.col("PROD_SK"),
    F.col("SUB_UNIQ_KEY"),
    F.col("WORK_COMP_PAYMT_TYP_ID"),
    F.col("CLM_ACDNT_CD_SK"),
    F.col("CLM_ACDNT_ST_CD_SK"),
    F.col("CLM_FINL_DISP_CD_SK"),
    F.col("CLM_INPT_METH_CD_SK"),
    F.col("CLM_PAYE_CD_SK"),
    F.col("CLM_SVC_PROV_SPEC_CD_SK"),
    F.col("CLM_SVC_PROV_TYP_CD_SK"),
    F.col("CLM_STTUS_CD_SK"),
    F.col("CLM_SUBTYP_CD_SK"),
    F.col("CLM_TYP_CD_SK"),
    F.col("ACDNT_DT"),
    F.col("INPT_DT"),
    F.col("NEXT_RVW_DT"),
    F.col("PD_DT"),
    F.col("PRCS_DT"),
    F.col("RCVD_DT"),
    F.col("SVC_END_DT"),
    F.col("SVC_STRT_DT"),
    F.col("STTUS_DT"),
    F.col("WORK_UNABLE_BEG_DT"),
    F.col("WORK_UNABLE_END_DT"),
    F.col("ACDNT_AMT"),
    F.col("ACTL_PD_AMT"),
    F.col("ALW_AMT"),
    F.col("CHRG_AMT"),
    F.col("COINS_AMT"),
    F.col("CNSD_CHRG_AMT"),
    F.col("COPAY_AMT"),
    F.col("DEDCT_AMT"),
    F.col("DSALW_AMT"),
    F.col("PATN_PD_AMT"),
    F.col("PAYBL_AMT"),
    F.col("REMIT_SUPRSION_AMT"),
    F.col("PATN_ACCT_NO"),
    F.col("RFRNG_PROV_TX")
)

write_files(
    df_WORK_COMPNSTN_CLM_out,
    f"{adls_path}/load/WORK_COMPNSTN_CLM.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)

# -----------------------------------------------------------------------------------------------
# Stage: B_WORK_COMPNSTN_CLM (PxSequentialFile) - WRITE
#   File: load/B_WORK_COMPNSTN_CLM.dat
# -----------------------------------------------------------------------------------------------
df_B_WORK_COMPNSTN_CLM_out = df_BusinessLogic_Lnk_BalRep_WorkCompnstnClm.select(
    F.rpad(F.col("CLM_ID").cast(StringType()), 12, " ").alias("CLM_ID"),
    F.col("SRC_SYS_CD")
)

write_files(
    df_B_WORK_COMPNSTN_CLM_out,
    f"{adls_path}/load/B_WORK_COMPNSTN_CLM.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)