# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsIncomeInvcSubExtr
# MAGIC CALLED BY:   FctsIncomeExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Pulls the Invoice Subscriber information from Facets for Income 
# MAGIC       
# MAGIC    Date is used as a criteria for which records are pulled.   
# MAGIC 
# MAGIC INPUTS:
# MAGIC    CDS_INSB_SB_DETAIL
# MAGIC   
# MAGIC HASH FILES:  hf_invc_sub
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   used the STRIP.FIELD to all of the character fields.
# MAGIC                   The DB2 timestamp value of the audit_ts is converted to a sybase timestamp with the transofrm  FORMAT.DATE
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Output file is created with a temp. name. 
# MAGIC                   
# MAGIC                  This is pulls all records from a BeginDate supplied in parameters.
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC              Ralph Tucker  -  11/03/2005-   Originally Programmed
# MAGIC              SAndrew           -  05/03/2006-   Task Tracker 4531 - USAble Reporting Support.   Added extract of field BLSB_VOL to populate the  INVC_SUB VOL_COV_AMT field.      
# MAGIC               Naren Garapaty - 04/2007        - Added Reversal Logic
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              6/14/2007          3264                              Added Balancing process to the overall                 devlIDS30               Steph Goddard             09/15/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data                        
# MAGIC 
# MAGIC Bhoomi Dasari                 09/30/2008        3567                             Added new primay key contianer and SrcSysCdsk  devlIDS                   Steph Goddard              10/03/2008                       
# MAGIC                                                                                                        and SrcSysCd  
# MAGIC 
# MAGIC Manasa Andru                2016-03-15         TFS - 12308                Added Trim function to the BILL_INVC_ID field in the   IntegrateDev2      Kalyan Neelam               2016-03-16
# MAGIC                                                                                                      svReversalBillInvcId stage variable in the ReversalLogic transformer.
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi             2021-03-24      358186                            Changed Datatype length for field                      IntegrateDev1              Reddy Sanam                2021-04-01
# MAGIC                                                                                                   BLIV_ID
# MAGIC                                                                                                   char(12) to Varchar(15)
# MAGIC 
# MAGIC Prabhu ES                     2022-03-02      S2S Remediation       MSSQL ODBC conn added and other                      IntegrateDev5	         Ken Bradmon	              2022-06-15
# MAGIC                                                                                                   param changes
# MAGIC Reddy Sanam               2022-07-15                                         Changed the shared container for a column
# MAGIC                                                                                                    mapping  Keys.CRT_TS [1,23] to Keys.CRT_TS   IntegrateDev2               Hugh Sisson                   2022-07-15

# MAGIC Hash file hf_invc_sub_allcol cleared
# MAGIC Pulls all Invoice rows from facets CDS_INSB_SB_DETAIL table matching data criteria.
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
facets_secret_name = get_widget_value("facets_secret_name","")
ids_secret_name = get_widget_value("ids_secret_name","")
FacetsOwner = get_widget_value("FacetsOwner","")
IDSOwner = get_widget_value("IDSOwner","")
TmpOutFile = get_widget_value("TmpOutFile","")
CurrRunCycle = get_widget_value("CurrRunCycle","")
DriverTable = get_widget_value("DriverTable","")
RunID = get_widget_value("RunID","")
CurrDate = get_widget_value("CurrDate","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
SrcSysCd = get_widget_value("SrcSysCd","")

# MAGIC %run ../../../../../shared_containers/PrimaryKey/IncmInvcSubPK
# COMMAND ----------

# Read from hashed file hf_invc_rebills (Scenario C -> read parquet)
df_hf_invc_rebills = spark.read.parquet(f"{adls_path}/hf_invc_rebills.parquet")

# Connect to Facets database for CDS_INSB_SB_DETAIL
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_cds_insb_sb_detail = """
SELECT
INSB.BLIV_ID,
INSB.CSPI_ID,
INSB.PDPD_ID,
INSB.PDBL_ID,
INSB.BLSB_COV_FROM_DT,
INSB.BLSB_PREM_TYPE,
INSB.SBSB_CK,
INSB.BLSB_CREATE_DTM,
INSB.BLSB_DISP_CD,
INSB.BLSB_COV_THRU_DT,
INSB.BLSB_PREM_SB,
INSB.GRGR_ID,
INSB.SGSG_CK,
INSB.SGSG_ID,
INSB.CSCS_ID,
INSB.BLSB_FI,
INSB.BLSB_LVS_SB,
INSB.BLSB_LVS_DEP,
INSB.BLSB_PREM_DEP,
INSB.BLSB_COV_DUE_DT,
INSB.BLSB_VOL
FROM  
tempdb..#DriverTable#  TmpInvc,  
#$FacetsOwner#.CDS_INSB_SB_DETAIL INSB
WHERE
TmpInvc.BILL_INVC_ID = INSB.BLIV_ID
AND (INSB.BLSB_PREM_SB<>0 OR INSB.BLSB_PREM_DEP<>0)
"""
df_CDS_INSB_SB_DETAIL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_cds_insb_sb_detail)
    .load()
)

# StripField stage
df_stripfield = (
    df_CDS_INSB_SB_DETAIL
    .withColumn("BLIV_ID", F.regexp_replace(F.col("BLIV_ID"), "[\n\r\t]", ""))
    .withColumn("CSPI_ID", F.regexp_replace(F.col("CSPI_ID"), "[\n\r\t]", ""))
    .withColumn("PDPD_ID", F.regexp_replace(F.col("PDPD_ID"), "[\n\r\t]", ""))
    .withColumn("PDBL_ID", F.regexp_replace(F.col("PDBL_ID"), "[\n\r\t]", ""))
    .withColumn("BLSB_COV_FROM_DT", F.col("BLSB_COV_FROM_DT"))
    .withColumn("BLSB_PREM_TYPE", F.regexp_replace(F.col("BLSB_PREM_TYPE"), "[\n\r\t]", ""))
    .withColumn("SBSB_CK", F.col("SBSB_CK"))
    .withColumn("BLSB_CREATE_DTM", F.col("BLSB_CREATE_DTM"))
    .withColumn("BLSB_DISP_CD", F.regexp_replace(F.col("BLSB_DISP_CD"), "[\n\r\t]", ""))
    .withColumn("BLSB_COV_THRU_DT", F.col("BLSB_COV_THRU_DT"))
    .withColumn("BLSB_PREM_SB", F.col("BLSB_PREM_SB"))
    .withColumn("GRGR_ID", F.regexp_replace(F.col("GRGR_ID"), "[\n\r\t]", ""))
    .withColumn("SGSG_CK", F.col("SGSG_CK"))
    .withColumn("SGSG_ID", F.regexp_replace(F.col("SGSG_ID"), "[\n\r\t]", ""))
    .withColumn("CSCS_ID", F.regexp_replace(F.col("CSCS_ID"), "[\n\r\t]", ""))
    .withColumn("BLSB_FI", F.regexp_replace(F.col("BLSB_FI"), "[\n\r\t]", ""))
    .withColumn("BLSB_LVS_SB", F.col("BLSB_LVS_SB"))
    .withColumn("BLSB_LVS_DEP", F.col("BLSB_LVS_DEP"))
    .withColumn("BLSB_PREM_DEP", F.col("BLSB_PREM_DEP"))
    .withColumn("BLSB_COV_DUE_DT", F.col("BLSB_COV_DUE_DT"))
    .withColumn("BLSB_VOL", F.col("BLSB_VOL"))
)

# BusinessRules stage (transform)
df_businessrules_stagevars = (
    df_stripfield
    .withColumn(
        "svBlsbDispCd",
        F.when(F.length(F.trim(F.col("BLSB_DISP_CD"))) == 0, F.lit("NA"))
         .otherwise(F.trim(F.col("BLSB_DISP_CD")))
    )
    .withColumn("svCovDueDt", F.expr("substring(BLSB_COV_DUE_DT,1,10)"))
    .withColumn("svCovStrtDt", F.expr("substring(BLSB_COV_FROM_DT,1,10)"))
    .withColumn("svCovEndDt", F.expr("substring(BLSB_COV_THRU_DT,1,10)"))
    .withColumn(
        "svCrtDt",
        F.concat(
            F.substring(F.col("BLSB_CREATE_DTM"), 1, 10),
            F.lit(" "),
            F.substring(F.col("BLSB_CREATE_DTM"), 12, 2),
            F.lit(":"),
            F.substring(F.col("BLSB_CREATE_DTM"), 15, 2),
            F.lit(":"),
            F.substring(F.col("BLSB_CREATE_DTM"), 18, 2),
            F.lit("."),
            F.substring(F.col("BLSB_CREATE_DTM"), 21, 6),
        )
    )
)

df_businessrules = (
    df_businessrules_stagevars
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(
            F.lit("FACETS;"),
            F.trim(F.col("BLIV_ID")),
            F.trim(F.col("CSPI_ID")),
            F.trim(F.col("PDPD_ID")),
            F.trim(F.col("PDBL_ID")),
            F.col("svCovDueDt"),
            F.col("svCovStrtDt"),
            F.trim(F.col("BLSB_PREM_TYPE")),
            F.col("SBSB_CK"),
            F.col("svCovEndDt"),
            F.col("svBlsbDispCd")
        )
    )
    .withColumn("INVC_SK", F.lit(0))
    .withColumn("BILL_INVC_ID", F.trim(F.col("BLIV_ID")))
    .withColumn("CLS_PLN_ID", F.trim(F.col("CSPI_ID")))
    .withColumn("PROD_ID", F.trim(F.col("PDPD_ID")))
    .withColumn("PROD_BILL_CMPNT_ID", F.trim(F.col("PDBL_ID")))
    .withColumn("COV_DUE_DT", F.col("svCovDueDt"))
    .withColumn("COV_STRT_DT_SK", F.col("svCovStrtDt"))
    .withColumn("COV_END_DT_SK", F.col("svCovEndDt"))
    .withColumn("INVC_SUB_PRM_TYP_CD", F.col("BLSB_PREM_TYPE"))
    .withColumn("SUB_UNIQ_KEY", F.col("SBSB_CK"))
    .withColumn("CRT_DT", F.col("svCrtDt"))
    .withColumn("INVC_SUB_BILL_DISP_CD", F.col("svBlsbDispCd"))
    .withColumn("CLS_ID", F.trim(F.col("CSCS_ID")))
    .withColumn("CSPI_ID", F.trim(F.col("CSPI_ID")))
    .withColumn("GRP_ID", F.trim(F.col("GRGR_ID")))
    .withColumn("INVC_ID", F.trim(F.col("BLIV_ID")))
    .withColumn("SUBGRP_ID", F.trim(F.col("SGSG_ID")))
    .withColumn(
        "INVC_SUB_FMLY_CNTR_CD",
        F.when(F.trim(F.col("BLSB_FI")) == "*", F.lit("ISB"))
         .otherwise(
            F.when(F.length(F.trim(F.col("BLSB_FI"))) == 0, F.lit("NA"))
             .otherwise(F.trim(F.col("BLSB_FI")))
         )
    )
    .withColumn("DPNDT_PRM_AMT", F.col("BLSB_PREM_DEP"))
    .withColumn("SUB_PRM_AMT", F.col("BLSB_PREM_SB"))
    .withColumn("DPNDT_CT", F.col("BLSB_LVS_DEP"))
    .withColumn("SUB_CT", F.col("BLSB_LVS_SB"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("VOL_COV_AMT", F.col("BLSB_VOL"))
)

# ReversalLogic stage

# Left-join df_hf_invc_rebills as a lookup
df_reversallogic_joined = (
    df_businessrules.alias("ChkReversals")
    .join(
        df_hf_invc_rebills.alias("rebill_lkup"),
        (
            (F.col("ChkReversals.BILL_INVC_ID") == F.col("rebill_lkup.BILL_INVC_ID"))
            & (F.col("ChkReversals.INVC_ID") == F.col("rebill_lkup.BILL_INVC_ID"))
        ),
        how="left"
    )
)

# Split into "NonReversal" and "Reversal"
df_reversallogic_nonreversal = df_reversallogic_joined.filter("rebill_lkup.BILL_INVC_ID IS NULL").select(
    F.col("ChkReversals.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("ChkReversals.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("ChkReversals.DISCARD_IN").alias("DISCARD_IN"),
    F.col("ChkReversals.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("ChkReversals.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ChkReversals.ERR_CT").alias("ERR_CT"),
    F.col("ChkReversals.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("ChkReversals.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("ChkReversals.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("ChkReversals.INVC_SK").alias("INVC_SK"),
    F.col("ChkReversals.BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("ChkReversals.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("ChkReversals.PROD_ID").alias("PROD_ID"),
    F.col("ChkReversals.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("ChkReversals.COV_DUE_DT").alias("COV_DUE_DT"),
    F.col("ChkReversals.COV_STRT_DT_SK").alias("COV_STRT_DT_SK"),
    F.col("ChkReversals.COV_END_DT_SK").alias("COV_END_DT_SK"),
    F.col("ChkReversals.INVC_SUB_PRM_TYP_CD").alias("INVC_SUB_PRM_TYP_CD"),
    F.col("ChkReversals.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("ChkReversals.CRT_DT").alias("CRT_DT"),
    F.col("ChkReversals.INVC_SUB_BILL_DISP_CD").alias("INVC_SUB_BILL_DISP_CD"),
    F.col("ChkReversals.CLS_ID").alias("CLS_ID"),
    F.col("ChkReversals.CSPI_ID").alias("CSPI_ID"),
    F.col("ChkReversals.GRP_ID").alias("GRP_ID"),
    F.col("ChkReversals.INVC_ID").alias("INVC_ID"),
    F.col("ChkReversals.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("ChkReversals.INVC_SUB_FMLY_CNTR_CD").alias("INVC_SUB_FMLY_CNTR_CD"),
    F.col("ChkReversals.DPNDT_PRM_AMT").alias("DPNDT_PRM_AMT"),
    F.col("ChkReversals.SUB_PRM_AMT").alias("SUB_PRM_AMT"),
    F.col("ChkReversals.DPNDT_CT").alias("DPNDT_CT"),
    F.col("ChkReversals.SUB_CT").alias("SUB_CT"),
    F.col("ChkReversals.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("ChkReversals.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ChkReversals.VOL_COV_AMT").alias("VOL_COV_AMT"),
)

df_reversallogic_reversal = df_reversallogic_joined.filter("rebill_lkup.BILL_INVC_ID IS NOT NULL").withColumn(
    "svReversalsString",
    F.concat(
        F.lit("FACETS;"),
        F.trim(F.col("ChkReversals.BILL_INVC_ID")),
        F.lit("R"),
        F.trim(F.col("ChkReversals.CLS_PLN_ID")),
        F.trim(F.col("ChkReversals.PROD_ID")),
        F.trim(F.col("ChkReversals.PROD_BILL_CMPNT_ID")),
        F.col("ChkReversals.COV_DUE_DT"),
        F.col("ChkReversals.COV_STRT_DT_SK"),
        F.trim(F.col("ChkReversals.INVC_SUB_PRM_TYP_CD")),
        F.col("ChkReversals.SUB_UNIQ_KEY"),
        F.col("ChkReversals.CRT_DT"),
        F.col("ChkReversals.INVC_SUB_BILL_DISP_CD"),
    )
).withColumn(
    "svReversalBillInvcId",
    F.concat(F.trim(F.col("ChkReversals.BILL_INVC_ID")), F.lit("R"))
).withColumn(
    "svReversalDpndtAmt",
    F.col("ChkReversals.DPNDT_PRM_AMT") * -1
).withColumn(
    "svReversalSubPrmAmt",
    F.col("ChkReversals.SUB_PRM_AMT") * -1
).withColumn(
    "svReversalDpndtCt",
    F.col("ChkReversals.DPNDT_CT") * -1
).withColumn(
    "svReversalSubCt",
    F.col("ChkReversals.SUB_CT") * -1
).withColumn(
    "svReversalVolCovAmt",
    F.col("ChkReversals.VOL_COV_AMT") * -1
).withColumn(
    "svReversalnvcId",
    F.concat(F.col("ChkReversals.INVC_ID"), F.lit("R"))
)

df_reversallogic_reversal = df_reversallogic_reversal.select(
    F.col("ChkReversals.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("ChkReversals.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("ChkReversals.DISCARD_IN").alias("DISCARD_IN"),
    F.col("ChkReversals.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("ChkReversals.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ChkReversals.ERR_CT").alias("ERR_CT"),
    F.col("ChkReversals.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("ChkReversals.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("svReversalsString").alias("PRI_KEY_STRING"),
    F.col("ChkReversals.INVC_SK").alias("INVC_SK"),
    F.col("svReversalBillInvcId").alias("BILL_INVC_ID"),
    F.col("ChkReversals.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("ChkReversals.PROD_ID").alias("PROD_ID"),
    F.col("ChkReversals.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("ChkReversals.COV_DUE_DT").alias("COV_DUE_DT"),
    F.col("ChkReversals.COV_STRT_DT_SK").alias("COV_STRT_DT_SK"),
    F.col("ChkReversals.COV_END_DT_SK").alias("COV_END_DT_SK"),
    F.col("ChkReversals.INVC_SUB_PRM_TYP_CD").alias("INVC_SUB_PRM_TYP_CD"),
    F.col("ChkReversals.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("ChkReversals.CRT_DT").alias("CRT_DT"),
    F.col("ChkReversals.INVC_SUB_BILL_DISP_CD").alias("INVC_SUB_BILL_DISP_CD"),
    F.col("ChkReversals.CLS_ID").alias("CLS_ID"),
    F.col("ChkReversals.CSPI_ID").alias("CSPI_ID"),
    F.col("ChkReversals.GRP_ID").alias("GRP_ID"),
    F.col("svReversalnvcId").alias("INVC_ID"),
    F.col("ChkReversals.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("ChkReversals.INVC_SUB_FMLY_CNTR_CD").alias("INVC_SUB_FMLY_CNTR_CD"),
    F.col("svReversalDpndtAmt").alias("DPNDT_PRM_AMT"),
    F.col("svReversalSubPrmAmt").alias("SUB_PRM_AMT"),
    F.col("svReversalDpndtCt").alias("DPNDT_CT"),
    F.col("svReversalSubCt").alias("SUB_CT"),
    F.col("ChkReversals.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("ChkReversals.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svReversalVolCovAmt").alias("VOL_COV_AMT"),
)

# ColletRows => union the two dataframes
df_ColletRows = df_reversallogic_nonreversal.unionByName(df_reversallogic_reversal)

# Key stage input => same df_ColletRows
df_Key_input = df_ColletRows

# Generate two outputs from Key stage
df_Key_allcol = df_Key_input.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("INVC_SK"),
    F.col("BILL_INVC_ID"),
    F.col("CLS_PLN_ID"),
    F.col("PROD_ID"),
    F.col("PROD_BILL_CMPNT_ID"),
    F.col("COV_DUE_DT"),
    F.col("COV_STRT_DT_SK"),
    F.col("COV_END_DT_SK"),
    F.col("INVC_SUB_PRM_TYP_CD"),
    F.col("SUB_UNIQ_KEY"),
    F.col("CRT_DT"),
    F.col("INVC_SUB_BILL_DISP_CD"),
    F.col("CLS_ID"),
    F.col("CSPI_ID"),
    F.col("GRP_ID"),
    F.col("INVC_ID"),
    F.col("SUBGRP_ID"),
    F.col("INVC_SUB_FMLY_CNTR_CD"),
    F.col("DPNDT_PRM_AMT"),
    F.col("SUB_PRM_AMT"),
    F.col("DPNDT_CT"),
    F.col("SUB_CT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("VOL_COV_AMT"),
)

df_Key_transform = df_Key_input.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("INVC_SK"),
    F.col("BILL_INVC_ID"),
    F.col("CLS_PLN_ID"),
    F.col("PROD_ID"),
    F.col("PROD_BILL_CMPNT_ID"),
    F.col("COV_DUE_DT"),
    F.col("COV_STRT_DT_SK"),
    F.col("COV_END_DT_SK"),
    F.col("INVC_SUB_PRM_TYP_CD"),
    F.col("SUB_UNIQ_KEY"),
    F.col("CRT_DT"),
    F.col("INVC_SUB_BILL_DISP_CD"),
    F.col("CLS_ID"),
    F.col("CSPI_ID"),
    F.col("GRP_ID"),
    F.col("INVC_ID"),
    F.col("SUBGRP_ID"),
    F.col("INVC_SUB_FMLY_CNTR_CD"),
    F.col("DPNDT_PRM_AMT"),
    F.col("SUB_PRM_AMT"),
    F.col("DPNDT_CT"),
    F.col("SUB_CT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("VOL_COV_AMT"),
)

# Container IncmInvcSubPK
params_incm_invc_sub_pk = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrDate,
    "IDSOwner": IDSOwner
}
df_incm_invc_sub_pk = IncmInvcSubPK(df_Key_allcol, df_Key_transform, params_incm_invc_sub_pk)

# IdsInvcSub => write to sequential file #TmpOutFile# in directory "key"
# No "landing" or "external" in path => use adls_path
df_idsinvcsub_select = df_incm_invc_sub_pk.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "INVC_SK",
    "BILL_INVC_ID",
    "CLS_PLN_ID",
    "PROD_ID",
    "PROD_BILL_CMPNT_ID",
    "COV_DUE_DT",
    "COV_STRT_DT_SK",
    "COV_END_DT_SK",
    "INVC_SUB_PRM_TYP_CD",
    "SUB_UNIQ_KEY",
    "CRT_DT",
    "INVC_SUB_BILL_DISP_CD",
    "CLS_ID",
    "CSPI_ID",
    "GRP_ID",
    "INVC_ID",
    "SUBGRP_ID",
    "INVC_SUB_FMLY_CNTR_CD",
    "DPNDT_PRM_AMT",
    "SUB_PRM_AMT",
    "DPNDT_CT",
    "SUB_CT",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "VOL_COV_AMT",
)

# For columns typed as char/varchar in DataStage, apply rpad to match length
df_idsinvcsub = (
    df_idsinvcsub_select
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CLS_PLN_ID", F.rpad(F.col("CLS_PLN_ID"), 8, " "))
    .withColumn("PROD_ID", F.rpad(F.col("PROD_ID"), 8, " "))
    .withColumn("PROD_BILL_CMPNT_ID", F.rpad(F.col("PROD_BILL_CMPNT_ID"), 4, " "))
    .withColumn("COV_DUE_DT", F.rpad(F.col("COV_DUE_DT"), 10, " "))
    .withColumn("COV_STRT_DT_SK", F.rpad(F.col("COV_STRT_DT_SK"), 10, " "))
    .withColumn("INVC_SUB_PRM_TYP_CD", F.rpad(F.col("INVC_SUB_PRM_TYP_CD"), 1, " "))
    .withColumn("INVC_SUB_BILL_DISP_CD", F.rpad(F.col("INVC_SUB_BILL_DISP_CD"), 1, " "))
    .withColumn("CLS_ID", F.rpad(F.col("CLS_ID"), 4, " "))
    .withColumn("CSPI_ID", F.rpad(F.col("CSPI_ID"), 8, " "))
    .withColumn("GRP_ID", F.rpad(F.col("GRP_ID"), 8, " "))
    .withColumn("SUBGRP_ID", F.rpad(F.col("SUBGRP_ID"), 4, " "))
    .withColumn("INVC_SUB_FMLY_CNTR_CD", F.rpad(F.col("INVC_SUB_FMLY_CNTR_CD"), 1, " "))
)

write_files(
    df_idsinvcsub,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Read from hashed file hf_invc_rebills_rvrsl (same file in scenario C)
df_hf_invc_rebills_rvrsl = spark.read.parquet(f"{adls_path}/hf_invc_rebills.parquet")

# Facets_Source => ODBC read
jdbc_url_facets_2, jdbc_props_facets_2 = get_db_config(facets_secret_name)
extract_query_facets_source = """
SELECT
INSB.BLIV_ID,
INSB.CSPI_ID,
INSB.PDPD_ID,
INSB.PDBL_ID,
INSB.BLSB_COV_DUE_DT,
INSB.BLSB_COV_FROM_DT,
INSB.BLSB_PREM_TYPE,
INSB.SBSB_CK,
INSB.BLSB_CREATE_DTM,
INSB.BLSB_DISP_CD,
INSB.BLSB_PREM_SB
FROM
tempdb..#DriverTable#  TmpInvc,
#$FacetsOwner#.CDS_INSB_SB_DETAIL INSB
WHERE
TmpInvc.BILL_INVC_ID = INSB.BLIV_ID
AND (INSB.BLSB_PREM_SB<>0 OR INSB.BLSB_PREM_DEP<>0)
"""
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets_2)
    .options(**jdbc_props_facets_2)
    .option("query", extract_query_facets_source)
    .load()
)

# SnapshotReversalLogic stage - left join df_hf_invc_rebills_rvrsl as a lookup
df_snapshot_joined = (
    df_Facets_Source.alias("Snapshot")
    .join(
        df_hf_invc_rebills_rvrsl.alias("rebill_lkup"),
        (
            (F.col("Snapshot.BLIV_ID") == F.col("rebill_lkup.BILL_INVC_ID"))
            & (F.col("Snapshot.BLIV_ID") == F.col("rebill_lkup.BILL_INVC_ID"))
        ),
        how="left"
    )
)

# Stage variables
df_snapshot_logic = (
    df_snapshot_joined
    .withColumn(
        "svReversalInvcId",
        F.concat(
            F.trim(F.regexp_replace(F.col("Snapshot.BLIV_ID"), "[\n\r\t]", "")),
            F.lit("R")
        )
    )
    .withColumn(
        "svReversalSubAmt",
        F.col("Snapshot.BLSB_PREM_SB") * -1
    )
)

df_snapshot_logic_nonreversal = df_snapshot_logic.filter("rebill_lkup.BILL_INVC_ID IS NULL").select(
    F.trim(F.regexp_replace(F.col("Snapshot.BLIV_ID"), "[\n\r\t]", "")).alias("BILL_INVC_ID"),
    F.rpad(F.trim(F.regexp_replace(F.col("Snapshot.CSPI_ID"), "[\n\r\t]", "")),8," ").alias("CSPI_ID"),
    F.rpad(F.trim(F.regexp_replace(F.col("Snapshot.PDPD_ID"), "[\n\r\t]", "")),8," ").alias("PDPD_ID"),
    F.rpad(F.trim(F.regexp_replace(F.col("Snapshot.PDBL_ID"), "[\n\r\t]", "")),4," ").alias("PDBL_ID"),
    F.col("Snapshot.BLSB_COV_DUE_DT").alias("BLSB_COV_DUE_DT"),
    F.col("Snapshot.BLSB_COV_FROM_DT").alias("BLSB_COV_FROM_DT"),
    F.rpad(F.regexp_replace(F.col("Snapshot.BLSB_PREM_TYPE"), "[\n\r\t]", ""),1," ").alias("BLSB_PREM_TYPE"),
    F.col("Snapshot.SBSB_CK").alias("SBSB_CK"),
    F.col("Snapshot.BLSB_CREATE_DTM").alias("BLSB_CREATE_DTM"),
    F.rpad(
        F.when(
            F.length(F.trim(F.regexp_replace(F.col("Snapshot.BLSB_DISP_CD"), "[\n\r\t]", ""))) == 0,
            F.lit("NA")
        ).otherwise(F.trim(F.regexp_replace(F.col("Snapshot.BLSB_DISP_CD"), "[\n\r\t]", ""))),
        1, " "
    ).alias("BLSB_DISP_CD"),
    F.col("Snapshot.BLSB_PREM_SB").alias("BLSB_PREM_SB")
)

df_snapshot_logic_reversal = df_snapshot_logic.filter("rebill_lkup.BILL_INVC_ID IS NOT NULL").select(
    F.col("svReversalInvcId").alias("BILL_INVC_ID"),
    F.rpad(F.trim(F.regexp_replace(F.col("Snapshot.CSPI_ID"), "[\n\r\t]", "")),8," ").alias("CSPI_ID"),
    F.rpad(F.trim(F.regexp_replace(F.col("Snapshot.PDPD_ID"), "[\n\r\t]", "")),8," ").alias("PDPD_ID"),
    F.rpad(F.trim(F.regexp_replace(F.col("Snapshot.PDBL_ID"), "[\n\r\t]", "")),4," ").alias("PDBL_ID"),
    F.col("Snapshot.BLSB_COV_DUE_DT").alias("BLSB_COV_DUE_DT"),
    F.col("Snapshot.BLSB_COV_FROM_DT").alias("BLSB_COV_FROM_DT"),
    F.rpad(F.regexp_replace(F.col("Snapshot.BLSB_PREM_TYPE"), "[\n\r\t]", ""),1," ").alias("BLSB_PREM_TYPE"),
    F.col("Snapshot.SBSB_CK").alias("SBSB_CK"),
    F.col("Snapshot.BLSB_CREATE_DTM").alias("BLSB_CREATE_DTM"),
    F.rpad(
        F.when(
            F.length(F.trim(F.regexp_replace(F.col("Snapshot.BLSB_DISP_CD"), "[\n\r\t]", ""))) == 0,
            F.lit("NA")
        ).otherwise(F.trim(F.regexp_replace(F.col("Snapshot.BLSB_DISP_CD"), "[\n\r\t]", ""))),
        1, " "
    ).alias("BLSB_DISP_CD"),
    F.col("svReversalSubAmt").alias("BLSB_PREM_SB")
)

# Collector => union
df_collector_snapshot = df_snapshot_logic_nonreversal.unionByName(df_snapshot_logic_reversal)

# Transform stage after collector
df_transform_input = df_collector_snapshot

df_transform_stagevars = (
    df_transform_input
    .withColumn("svCovDueDt", F.expr("substring(BLSB_COV_DUE_DT,1,10)"))
    .withColumn("svCovDueDtSk", GetFkeyDate("IDS", F.lit(100), F.col("svCovDueDt"), F.lit("X")))
    .withColumn("svCovFromDt", F.expr("substring(BLSB_COV_FROM_DT,1,10)"))
    .withColumn("svCovStrtDtSk", GetFkeyDate("IDS", F.lit(102), F.col("svCovFromDt"), F.lit("X")))
    .withColumn("svInvcSubPrmTypCdSk", GetFkeyCodes("FACETS", F.lit(104), F.lit("INVOICE SUBSCRIBER PREMIUM TYPE"), F.col("BLSB_PREM_TYPE"), F.lit("X")))
    .withColumn("svCrtTs", F.expr("""substring(BLSB_CREATE_DTM,1,10) || ' ' || substring(BLSB_CREATE_DTM,12,2) || ':' || substring(BLSB_CREATE_DTM,15,2) || ':' || substring(BLSB_CREATE_DTM,18,2) || '.' || substring(BLSB_CREATE_DTM,21,6)"""))
    .withColumn("svInvcSubBillDispCdSk", GetFkeyCodes("FACETS", F.lit(106), F.lit("BILLING DISPOSITION"), F.col("BLSB_DISP_CD"), F.lit("X")))
)

df_transform_output = df_transform_stagevars.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("BILL_INVC_ID"),
    F.col("SBSB_CK").alias("SUB_UNIQ_KEY"),
    F.col("CSPI_ID").alias("CLS_PLN_ID"),
    F.col("PDPD_ID").alias("PROD_ID"),
    F.col("PDBL_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("svCovDueDtSk").cast(StringType()).alias("COV_DUE_DT_SK"),
    F.col("svCovStrtDtSk").cast(StringType()).alias("COV_STRT_DT_SK"),
    F.col("svInvcSubPrmTypCdSk").alias("INVC_SUB_PRM_TYP_CD_SK"),
    F.col("svCrtTs").alias("CRT_TS"),
    F.col("svInvcSubBillDispCdSk").alias("INVC_SUB_BILL_DISP_CD_SK"),
    F.col("BLSB_PREM_SB").alias("SUB_PRM_AMT"),
)

# Snapshot_File => write
df_snapshot_file_select = df_transform_output.select(
    "SRC_SYS_CD_SK",
    "BILL_INVC_ID",
    "SUB_UNIQ_KEY",
    "CLS_PLN_ID",
    "PROD_ID",
    "PROD_BILL_CMPNT_ID",
    "COV_DUE_DT_SK",
    "COV_STRT_DT_SK",
    "INVC_SUB_PRM_TYP_CD_SK",
    "CRT_TS",
    "INVC_SUB_BILL_DISP_CD_SK",
    "SUB_PRM_AMT"
)

# For columns typed as char in the design, apply rpad if lengths are specified
df_snapshot_file = (
    df_snapshot_file_select
    .withColumn("CLS_PLN_ID", F.rpad(F.col("CLS_PLN_ID"), 8, " "))
    .withColumn("PROD_ID", F.rpad(F.col("PROD_ID"), 8, " "))
    .withColumn("PROD_BILL_CMPNT_ID", F.rpad(F.col("PROD_BILL_CMPNT_ID"), 4, " "))
    .withColumn("COV_DUE_DT_SK", F.rpad(F.col("COV_DUE_DT_SK"), 10, " "))
    .withColumn("COV_STRT_DT_SK", F.rpad(F.col("COV_STRT_DT_SK"), 10, " "))
)

# Write to B_INVC_SUB.dat in directory "load"
# "load" does not contain "landing" or "external", so use adls_path
write_files(
    df_snapshot_file,
    f"{adls_path}/load/B_INVC_SUB.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)