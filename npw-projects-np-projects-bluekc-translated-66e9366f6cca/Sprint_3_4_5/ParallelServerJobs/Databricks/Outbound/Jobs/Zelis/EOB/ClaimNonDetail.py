# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Extracts Not Detail record data from Facets and generates a data file for RedCard.
# MAGIC 
# MAGIC 
# MAGIC Called By:RedCardEobExtrCntl
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                               Date                 Project/Altiris #      Change Description                                                                      Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                           --------------------     ------------------------      -----------------------------------------------------------------------                               --------------------------------       -------------------------------   ----------------------------       
# MAGIC Raja Gummadi                     2020-12-05            RedCard                     Original Devlopment                                                                OutboundDev3             Jaideep Mankala       12/27/2020
# MAGIC Raja Gummadi                     2021-01-21           RedCard               Added ERR logic to LOBD_ID field                                                OutboundDev3        Jaideep Mankala       01/21/2021
# MAGIC Raja Gummadi                    2021-02-04            343913               Added void claims logic                                                                    OutboundDev3             Jaideep Mankala       02/04/2021
# MAGIC Raja Gummadi                    2021-08-23            377877              Added participating dental network logic                                           OutboundDev3        Jaideep Mankala       08/26/2021
# MAGIC Rojarani Karnati                  2021-12-15            458995               updated logic for OpenField3                                                          OutboundDev3        Raja Gummadi             12/28/2021 
# MAGIC Raja Gummadi                    2022-03-23             S2S                     Sybase to SQL                                                                               OutboundDev5   	Ken Bradmon	2022-05-19
# MAGIC Mrudula Kodali                    2022-07-11        503892                   Updated CNTWK column                                                              OutboundDev3            Jeyaprasanna           2022-08-08
# MAGIC Raja Gummadi                  2025-01-10          615629           Updated openfield4,5 and6 columns
# MAGIC Mrudula Kodali                2025-02-09        615629     Update source sql in EDW_gaps_in_care to pull HEDIS_MESR_MOD_KEY_ID1,2,3 
# MAGIC                                                                                    instead of HEDIS_MESR_ABBR_ID. Made the changes to Remove_Duplicates_91, 
# MAGIC                                                                                    Lookup_19 and Transformer_1 to map the new fields to COPENFLD4,5,6      OutboundDev3            Jeyaprasanna           2024-02-26


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Outbound
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
BCBSOwner = get_widget_value('BCBSOwner','')
FacetsOwner = get_widget_value('FacetsOwner','')
CLMPDDT = get_widget_value('CLMPDDT','')
edw_secret_name = get_widget_value('edw_secret_name','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
facets_secret_name = get_widget_value('facets_secret_name','')

# Read from Sequential_File_14
schema_Sequential_File_14 = StructType([
    StructField("CLCL_ID", StringType(), True),
    StructField("CDOCID", StringType(), True)
])
df_Sequential_File_14 = (
    spark.read.format("csv")
    .schema(schema_Sequential_File_14)
    .option("sep", ",")
    .option("header", False)
    .option("quote", '"')
    .load(f"{adls_path_publish}/external/Zelis_EOB_temp.dat")
)

# DB2_Connector_0
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
extract_query_DB2_Connector_0 = f"SELECT FNCL_LOB_CD,FNCL_FUND_TYP_NM, FNCL_GRP_SIZE_CAT_NM from {EDWOwner}.FNCL_LOB_D"
df_DB2_Connector_0 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_DB2_Connector_0)
    .load()
)

# Remove_Duplicates_36
df_Remove_Duplicates_36_temp = dedup_sort(
    df_DB2_Connector_0,
    ["FNCL_LOB_CD"],
    [("FNCL_LOB_CD","A")]
)
df_Remove_Duplicates_36 = df_Remove_Duplicates_36_temp.select(
    F.col("FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    F.col("FNCL_FUND_TYP_NM").alias("FNCL_FUND_TYP_NM"),
    F.col("FNCL_GRP_SIZE_CAT_NM").alias("FNCL_GRP_SIZE_CAT_NM")
)

# bsdl_typ
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_bsdl_typ = f"""SELECT

CLCL.CLCL_ID,
BSDL.BSDL_TYPE


FROM 

{FacetsOwner}.CMC_CLCL_CLAIM CLCL,
{FacetsOwner}.CMC_PDBC_PROD_COMP COMP, 
{FacetsOwner}.CMC_BSDL_DETAILS BSDL

WHERE

CLCL.PDPD_ID = COMP.PDPD_ID 
AND COMP.PDBC_PFX = BSDL.PDBC_PFX
AND COMP.PDBC_TYPE="BSBS"
AND CLCL.CLCL_CL_TYPE = 'M'
AND BSDL.BSDL_TYPE = 'CLNC'
AND COMP.PDBC_EFF_DT <= CLCL.CLCL_LOW_SVC_DT
AND COMP.PDBC_TERM_DT >= CLCL.CLCL_LOW_SVC_DT
AND CLCL.CLCL_PAID_DT  IN (SELECT (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) FROM 
(SELECT (CASE WHEN MAX(CKPY_PAY_DT)IS NULL THEN (SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_REF_ID IN (SELECT SYIN_REF_ID FROM CER_SYIN_INST WHERE SYIN_INST IN (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD")))   
                             ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_INST IN 
                            (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD"))A)

UNION

SELECT


CLCL.CLCL_ID,
BSDL.BSDL_TYPE


FROM 

{FacetsOwner}.CMC_CLCL_CLAIM CLCL,
{FacetsOwner}.CMC_PDBC_PROD_COMP COMP, 
{FacetsOwner}.CMC_BSDL_DETAILS BSDL,
{FacetsOwner}.CMC_CLCK_CLM_CHECK CCCC 

WHERE

CLCL.PDPD_ID = COMP.PDPD_ID 
AND COMP.PDBC_PFX = BSDL.PDBC_PFX
AND CCCC.CLCL_ID = CLCL.CLCL_ID
AND COMP.PDBC_TYPE="BSBS"
AND CLCL.CLCL_CL_TYPE = 'M'
AND BSDL.BSDL_TYPE = 'CLNC'
AND COMP.PDBC_EFF_DT <= CLCL.CLCL_LOW_SVC_DT
AND COMP.PDBC_TERM_DT >= CLCL.CLCL_LOW_SVC_DT
AND CCCC.CKPY_REF_ID IN (SELECT DISTINCT CBI.CKPY_REF_ID FROM {FacetsOwner}.CMC_BPID_INDIC CBI WHERE CBI.BPID_PRINTED_DT IN (SELECT (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) FROM 
(SELECT (CASE WHEN MAX(CKPY_PAY_DT)IS NULL THEN (SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_REF_ID IN (SELECT SYIN_REF_ID FROM CER_SYIN_INST WHERE SYIN_INST IN (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD")))   
                             ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_INST IN 
                            (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD"))A))
"""
df_bsdl_typ = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_bsdl_typ)
    .load()
)

# Remove_Duplicates_30
df_Remove_Duplicates_30_temp = dedup_sort(
    df_bsdl_typ,
    ["CLCL_ID"],
    [("CLCL_ID","A")]
)
df_Remove_Duplicates_30 = df_Remove_Duplicates_30_temp.select(
    F.col("CLCL_ID").alias("CLCL_ID"),
    F.col("BSDL_TYPE").alias("BSDL_TYPE")
)

# clpp_pr_nm
extract_query_clpp_pr_nm = f"""SELECT

CLCL.CLCL_ID,
PRPR.CLPP_PR_NAME,
PRPR.CLPP_CLM_SUB_ORG


FROM 

{FacetsOwner}.CMC_CLCL_CLAIM CLCL,
{FacetsOwner}.CMC_CLPP_ITS_PROV PRPR


WHERE

CLCL.CLCL_ID = PRPR.CLCL_ID
AND CLCL.CLCL_PAID_DT  IN (SELECT (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) FROM 
(SELECT (CASE WHEN MAX(CKPY_PAY_DT)IS NULL THEN (SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_REF_ID IN (SELECT SYIN_REF_ID FROM CER_SYIN_INST WHERE SYIN_INST IN (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD")))   
                             ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_INST IN 
                            (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD"))A)

UNION

SELECT

CLCL.CLCL_ID,
PRPR.CLPP_PR_NAME,
PRPR.CLPP_CLM_SUB_ORG


FROM 

{FacetsOwner}.CMC_CLCL_CLAIM CLCL,
{FacetsOwner}.CMC_CLPP_ITS_PROV PRPR,
{FacetsOwner}.CMC_CLCK_CLM_CHECK CCCC


WHERE

CLCL.CLCL_ID = PRPR.CLCL_ID
AND  CCCC.CLCL_ID = CLCL.CLCL_ID 
AND CCCC.CKPY_REF_ID IN (SELECT DISTINCT CBI.CKPY_REF_ID FROM {FacetsOwner}.CMC_BPID_INDIC CBI WHERE CBI.BPID_PRINTED_DT IN (SELECT (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) FROM 
(SELECT (CASE WHEN MAX(CKPY_PAY_DT)IS NULL THEN (SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_REF_ID IN (SELECT SYIN_REF_ID FROM CER_SYIN_INST WHERE SYIN_INST IN (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD")))   
                             ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_INST IN 
                            (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD"))A))
"""
df_clpp_pr_nm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_clpp_pr_nm)
    .load()
)

# Copy_2_of_Remove_Duplicates_36
df_Copy_2_of_Remove_Duplicates_36_temp = dedup_sort(
    df_clpp_pr_nm,
    ["CLCL_ID"],
    [("CLCL_ID","A")]
)
df_Copy_2_of_Remove_Duplicates_36 = df_Copy_2_of_Remove_Duplicates_36_temp.select(
    F.col("CLCL_ID").alias("CLCL_ID"),
    F.col("CLPP_PR_NAME").alias("CLPP_PR_NAME"),
    F.col("CLPP_CLM_SUB_ORG").alias("CLPP_CLM_SUB_ORG")
)

# prod_base (BCBSOwner)
jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbs_secret_name)
extract_query_prod_base = f"""SELECT

PROD_PFX_CD,
PROD_NM


FROM 

{BCBSOwner}.PRODUCT_BASE
"""
df_prod_base = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", extract_query_prod_base)
    .load()
)

# Copy_of_Remove_Duplicates_36
df_Copy_of_Remove_Duplicates_36_temp = dedup_sort(
    df_prod_base,
    ["PROD_PFX_CD"],
    [("PROD_PFX_CD","A")]
)
df_Copy_of_Remove_Duplicates_36 = df_Copy_of_Remove_Duplicates_36_temp.select(
    F.col("PROD_PFX_CD").alias("PROD_PFX_CD"),
    F.col("PROD_NM").alias("PROD_NM")
)

# lobd
extract_query_lobd = f"""SELECT

CCCC.CLCL_ID,
CBI.LOBD_ID


FROM 

{FacetsOwner}.CMC_BPID_INDIC CBI,
{FacetsOwner}.CMC_CLCK_CLM_CHECK CCCC


WHERE


CBI.CKPY_REF_ID = CCCC.CKPY_REF_ID AND 
CBI.CKPY_NET_AMT > 0.00 AND
CCCC.CLCK_NET_AMT > 0.00 AND
CBI.CKPY_PAY_DT IN (SELECT (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) FROM 
(SELECT (CASE WHEN MAX(CKPY_PAY_DT)IS NULL THEN (SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_REF_ID IN (SELECT SYIN_REF_ID FROM CER_SYIN_INST WHERE SYIN_INST IN (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD")))   
                             ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_INST IN 
                            (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD"))A)

UNION

SELECT

CCCC.CLCL_ID,
CBI.LOBD_ID


FROM 

{FacetsOwner}.CMC_BPID_INDIC CBI,
{FacetsOwner}.CMC_CLCK_CLM_CHECK CCCC


WHERE


CBI.CKPY_REF_ID = CCCC.CKPY_REF_ID AND 
CBI.CKPY_NET_AMT > 0.00 AND
CCCC.CLCK_NET_AMT > 0.00 AND
CBI.BPID_PRINTED_DT IN (SELECT (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) FROM 
(SELECT (CASE WHEN MAX(CKPY_PAY_DT)IS NULL THEN (SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_REF_ID IN (SELECT SYIN_REF_ID FROM CER_SYIN_INST WHERE SYIN_INST IN (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD")))   
                             ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_INST IN 
                            (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD"))A)
ORDER BY CCCC.CLCL_ID,CBI.LOBD_ID
"""
df_lobd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_lobd)
    .load()
)

# Copy_3_of_Remove_Duplicates_41
df_Copy_3_of_Remove_Duplicates_41_temp = dedup_sort(
    df_lobd,
    ["CLCL_ID"],
    [("CLCL_ID","D")]
)
df_Copy_3_of_Remove_Duplicates_41 = df_Copy_3_of_Remove_Duplicates_41_temp.select(
    F.col("CLCL_ID").alias("CLCL_ID"),
    F.col("LOBD_ID").alias("LOBD_ID")
)

# supressed_claims
extract_query_supressed_claims = f"""SELECT

CLCL.CLCL_ID
 
FROM 

{FacetsOwner}.CMC_CLCL_CLAIM CLCL,
{FacetsOwner}.CMC_CLOR_CL_OVR CLOR


WHERE 

CLCL.CLCL_ID = CLOR.CLCL_ID
AND CLOR.EXCD_ID = 'YXM'
AND CLCL.CLCL_PAID_DT IN (SELECT (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) FROM 
(SELECT (CASE WHEN MAX(CKPY_PAY_DT)ISNULL THEN (SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_REF_ID IN (SELECT SYIN_REF_ID FROM CER_SYIN_INST WHERE SYIN_INST IN (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD")))   
                             ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_INST IN 
                            (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD"))A)

UNION

SELECT

CLCL.CLCL_ID
 
FROM 

{FacetsOwner}.CMC_CLCL_CLAIM CLCL,
{FacetsOwner}.CMC_CLOR_CL_OVR CLOR,
{FacetsOwner}.CMC_CLCK_CLM_CHECK CCCC


WHERE 

CLCL.CLCL_ID = CLOR.CLCL_ID
AND CLOR.EXCD_ID = 'YXM'
AND CCCC.CLCL_ID = CLCL.CLCL_ID
AND CCCC.CKPY_REF_ID IN (SELECT DISTINCT CBI.CKPY_REF_ID FROM {FacetsOwner}.CMC_BPID_INDIC CBI WHERE CBI.BPID_PRINTED_DT IN (SELECT (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) FROM 
(SELECT (CASE WHEN MAX(CKPY_PAY_DT)ISNULL THEN (SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_REF_ID IN (SELECT SYIN_REF_ID FROM CER_SYIN_INST WHERE SYIN_INST IN (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD")))   
                             ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_INST IN 
                            (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD"))A))
"""
df_supressed_claims = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_supressed_claims)
    .load()
)

# rmdupclms3
df_rmdupclms3_temp = dedup_sort(
    df_supressed_claims,
    ["CLCL_ID"],
    [("CLCL_ID","A")]
)
df_rmdupclms3 = df_rmdupclms3_temp.select(
    F.col("CLCL_ID").alias("CLCL_ID")
)

# sybase_cntwk
extract_query_sybase_cntwk = f"""SELECT
CLCL.CLCL_ID,
CMC_CLPP_ITS_PROV.CLPP_CLASS_PROV ,
CLCL.CLCL_PRE_PRICE_IND,
CMC_CDPP_LI_ITS_PR.CDPP_CLASS_PROV

from {FacetsOwner}.CMC_CLCL_CLAIM CLCL
LEFT OUTER JOIN {FacetsOwner}.CMC_CLPP_ITS_PROV CMC_CLPP_ITS_PROV on CLCL.CLCL_ID = CMC_CLPP_ITS_PROV.CLCL_ID
LEFT OUTER JOIN {FacetsOwner}.CMC_CDPP_LI_ITS_PR CMC_CDPP_LI_ITS_PR on CMC_CDPP_LI_ITS_PR.CLCL_ID = CLCL.CLCL_ID
WHERE 1=1
AND CLCL.CLCL_CL_TYPE = 'M'
AND CLCL.CLCL_PAID_DT  IN (SELECT (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) FROM 
(SELECT (CASE WHEN MAX(CKPY_PAY_DT)ISNULL THEN (SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_REF_ID IN (SELECT SYIN_REF_ID FROM CER_SYIN_INST WHERE SYIN_INST IN (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD")))   
                             ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_INST IN 
                            (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD"))A)
"""
df_sybase_cntwk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_sybase_cntwk)
    .load()
)

# rm_cntwk
df_rm_cntwk_temp = dedup_sort(
    df_sybase_cntwk,
    ["CLCL_ID"],
    [("CLCL_ID","A")]
)
df_rm_cntwk = df_rm_cntwk_temp.select(
    F.col("CLCL_ID").alias("CLCL_ID"),
    F.col("CLPP_CLASS_PROV").alias("CLPP_CLASS_PROV"),
    F.col("CLCL_PRE_PRICE_IND").alias("CLCL_PRE_PRICE_IND"),
    F.col("CDPP_CLASS_PROV").alias("CDPP_CLASS_PROV")
)

# EDW_gaps_in_care
extract_query_EDW_gaps_in_care = f"""WITH ModResults AS 
(SELECT DISTINCT
MBR.MBR_UNIQ_KEY,
GAP_DPLY.HEDIS_MESR_MOD_KEY_ID,
GAP_DPLY.HEDIS_MESR_GAP_MOD_PRTY_NO,
ROW_NUMBER() OVER (PARTITION BY MBR.MBR_UNIQ_KEY ORDER BY GAP_DPLY.HEDIS_MESR_GAP_MOD_PRTY_NO) AS row_num

FROM 

{EDWOwner}.MBR_D MBR
INNER JOIN {EDWOwner}.MBR_HEDIS_MESR_YTD_F HEDIS ON MBR.MBR_INDV_BE_KEY = HEDIS.MBR_INDV_BE_KEY 
                         AND HEDIS.CMPLNC_ADMIN_IN <> 1 
                         AND HEDIS.MESR_ELIG_IN ='1'
                         AND HEDIS.EXCL_IN <> 1
                         AND HEDIS.HEDIS_MBR_BUCKET_ID IN ('Default','Total')
INNER JOIN {EDWOwner}.HEDIS_MESR_GAP_DPLY_D GAP_DPLY ON GAP_DPLY.HEDIS_MBR_BUCKET_ID = HEDIS.HEDIS_MBR_BUCKET_ID
                         AND RTRIM(GAP_DPLY.HEDIS_MESR_NM) = RTRIM(HEDIS.HEDIS_MESR_NM)
                         AND RTRIM(GAP_DPLY.HEDIS_SUB_MESR_NM) = RTRIM(HEDIS.HEDIS_SUB_MESR_NM)
                         AND GAP_DPLY.MOD_ID = 'EOB'
                         AND GAP_DPLY.HEDIS_MBR_BUCKET_ID IN ('Default','Total')
                         AND GAP_DPLY.HEDIS_MESR_GAP_DPLY_MOD_EFF_DT <= Current_Date
                         AND GAP_DPLY.HEDIS_MESR_GAP_DPLY_MOD_TERM_DT >= Current_Date
INNER JOIN {EDWOwner}.HEDIS_MESR_GAP_D GAP ON GAP.HEDIS_MESR_ABBR_ID = GAP_DPLY.HEDIS_MESR_ABBR_ID

GROUP BY 
MBR.MBR_UNIQ_KEY,
GAP_DPLY.HEDIS_MESR_MOD_KEY_ID,
GAP_DPLY.HEDIS_MESR_GAP_MOD_PRTY_NO

ORDER BY 
MBR.MBR_UNIQ_KEY,
GAP_DPLY.HEDIS_MESR_GAP_MOD_PRTY_NO)

SELECT 
MBR_UNIQ_KEY,
MAX(CASE WHEN row_num = 1 THEN  HEDIS_MESR_MOD_KEY_ID Else NULL END) AS HEDIS_MESR_MOD_KEY_ID1,
MAX(CASE WHEN row_num = 2 THEN  HEDIS_MESR_MOD_KEY_ID Else NULL END) AS HEDIS_MESR_MOD_KEY_ID2,
MAX(CASE WHEN row_num = 3 THEN  HEDIS_MESR_MOD_KEY_ID Else NULL END) AS HEDIS_MESR_MOD_KEY_ID3,
MAX(CASE WHEN row_num = 1 THEN  HEDIS_MESR_GAP_MOD_PRTY_NO Else NULL END) AS HEDIS_MESR_GAP_MOD_PRTY_NO1,
MAX(CASE WHEN row_num = 2 THEN  HEDIS_MESR_GAP_MOD_PRTY_NO Else NULL END) AS HEDIS_MESR_GAP_MOD_PRTY_NO2,
MAX(CASE WHEN row_num = 3 THEN  HEDIS_MESR_GAP_MOD_PRTY_NO Else NULL END) AS HEDIS_MESR_GAP_MOD_PRTY_NO3
FROM ModResults 
GROUP BY 
MBR_UNIQ_KEY
"""
df_EDW_gaps_in_care = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_EDW_gaps_in_care)
    .load()
)

# Remove_Duplicates_91
df_Remove_Duplicates_91_temp = dedup_sort(
    df_EDW_gaps_in_care,
    ["MBR_UNIQ_KEY"],
    [("MBR_UNIQ_KEY","A")]
)
df_Remove_Duplicates_91 = df_Remove_Duplicates_91_temp.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("HEDIS_MESR_MOD_KEY_ID1").alias("HEDIS_MESR_MOD_KEY_ID1"),
    F.col("HEDIS_MESR_MOD_KEY_ID2").alias("HEDIS_MESR_MOD_KEY_ID2"),
    F.col("HEDIS_MESR_MOD_KEY_ID3").alias("HEDIS_MESR_MOD_KEY_ID3"),
    F.col("HEDIS_MESR_GAP_MOD_PRTY_NO1").alias("HEDIS_MESR_GAP_MOD_PRTY_NO1"),
    F.col("HEDIS_MESR_GAP_MOD_PRTY_NO2").alias("HEDIS_MESR_GAP_MOD_PRTY_NO2"),
    F.col("HEDIS_MESR_GAP_MOD_PRTY_NO3").alias("HEDIS_MESR_GAP_MOD_PRTY_NO3")
)

# Copy_2_of_HProvNm
extract_query_Copy_2_of_HProvNm = f"""SELECT

CLCL.CLCL_ID,
BILL.PDBL_ACCT_CAT


FROM 

{FacetsOwner}.CMC_CLCL_CLAIM CLCL,
{FacetsOwner}.CMC_PDBC_PROD_COMP COMP, 
{FacetsOwner}.CMC_PDBL_PROD_BILL BILL



WHERE


CLCL.PDPD_ID = COMP.PDPD_ID 
AND COMP.PDBC_PFX = BILL.PDBC_PFX
AND COMP.PDBC_TYPE='PDBL'
AND BILL.PDBL_ID="MED1"
AND CLCL.CLCL_CL_TYPE = 'M'
AND CLCL.CLCL_PAID_DT  IN (SELECT (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) FROM 
(SELECT (CASE WHEN MAX(CKPY_PAY_DT)IS NULL THEN (SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_REF_ID IN (SELECT SYIN_REF_ID FROM CER_SYIN_INST WHERE SYIN_INST IN (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID='!CKD')))   
                             ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_INST IN 
                            (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID='!CKD'))A)

UNION

SELECT


CLCL.CLCL_ID,
BILL.PDBL_ACCT_CAT

FROM 

{FacetsOwner}.CMC_CLCL_CLAIM CLCL,
{FacetsOwner}.CMC_PDBC_PROD_COMP COMP, 
{FacetsOwner}.CMC_PDBL_PROD_BILL BILL

WHERE

CLCL.PDPD_ID = COMP.PDPD_ID 
AND COMP.PDBC_PFX = BILL.PDBC_PFX
 AND COMP.PDBC_TYPE='PDBL'
AND BILL.PDBL_ID='DEN1'
AND CLCL.CLCL_CL_TYPE = 'D'
AND CLCL.CLCL_PAID_DT IN (SELECT (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) FROM 
(SELECT (CASE WHEN MAX(CKPY_PAY_DT)ISNULL THEN (SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_REF_ID IN (SELECT SYIN_REF_ID FROM CER_SYIN_INST WHERE SYIN_INST IN (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID='!CKD')))   
                             ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_INST IN 
                            (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID='!CKD'))A)

UNION

SELECT


CLCL.CLCL_ID,
BILL.PDBL_ACCT_CAT


FROM 

{FacetsOwner}.CMC_CLCL_CLAIM CLCL,
{FacetsOwner}.CMC_PDBC_PROD_COMP COMP, 
{FacetsOwner}.CMC_PDBL_PROD_BILL BILL,
{FacetsOwner}.CMC_CLCK_CLM_CHECK CCCC


WHERE


CLCL.PDPD_ID = COMP.PDPD_ID 
AND COMP.PDBC_PFX = BILL.PDBC_PFX
AND CCCC.CLCL_ID = CLCL.CLCL_ID
AND COMP.PDBC_TYPE='PDBL'
AND BILL.PDBL_ID='MED1'
AND CLCL.CLCL_CL_TYPE = 'M'
AND CCCC.CKPY_REF_ID IN (SELECT DISTINCT CBI.CKPY_REF_ID FROM {FacetsOwner}.CMC_BPID_INDIC CBI WHERE CBI.BPID_PRINTED_DT IN (SELECT (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) FROM 
(SELECT (CASE WHEN MAX(CKPY_PAY_DT)ISNULL THEN (SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_REF_ID IN (SELECT SYIN_REF_ID FROM CER_SYIN_INST WHERE SYIN_INST IN (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID='!CKD')))   
                             ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_INST IN 
                            (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID='!CKD'))A))

UNION

SELECT


CLCL.CLCL_ID,
BILL.PDBL_ACCT_CAT

FROM 

{FacetsOwner}.CMC_CLCL_CLAIM CLCL,
{FacetsOwner}.CMC_PDBC_PROD_COMP COMP, 
{FacetsOwner}.CMC_PDBL_PROD_BILL BILL,
{FacetsOwner}.CMC_CLCK_CLM_CHECK CCCC

WHERE

CLCL.PDPD_ID = COMP.PDPD_ID 
AND COMP.PDBC_PFX = BILL.PDBC_PFX
AND CCCC.CLCL_ID = CLCL.CLCL_ID
 AND COMP.PDBC_TYPE='PDBL'
AND BILL.PDBL_ID='DEN1'
AND CLCL.CLCL_CL_TYPE = 'D'
AND CCCC.CKPY_REF_ID IN (SELECT DISTINCT CBI.CKPY_REF_ID FROM {FacetsOwner}.CMC_BPID_INDIC CBI WHERE CBI.BPID_PRINTED_DT IN (SELECT (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) FROM 
(SELECT (CASE WHEN MAX(CKPY_PAY_DT)ISNULL THEN (SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_REF_ID IN (SELECT SYIN_REF_ID FROM CER_SYIN_INST WHERE SYIN_INST IN (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID='!CKD')))   
                             ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_INST IN 
                            (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID='!CKD'))A))
"""
df_Copy_2_of_HProvNm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_Copy_2_of_HProvNm)
    .load()
)

# Remove_Duplicates_33
df_Remove_Duplicates_33_temp = dedup_sort(
    df_Copy_2_of_HProvNm,
    ["CLCL_ID"],
    [("CLCL_ID","A")]
)
df_Remove_Duplicates_33 = df_Remove_Duplicates_33_temp.select(
    F.col("CLCL_ID").alias("CLCL_ID"),
    F.col("PDBL_ACCT_CAT").alias("PDBL_ACCT_CAT")
)

# CLM
extract_query_CLM = f"""SELECT

SBSB.SBSB_ID,
MEME.MEME_SFX,
MEME.MEME_LAST_NAME,
MEME.MEME_FIRST_NAME,
MEME.MEME_MID_INIT,
CLCL.CLCL_ID,
CLCL.CLCL_CL_TYPE,
CLCL.CLCL_PAID_DT,
PRPR.PRPR_NAME,
PRPR.MCTN_ID,
CDML.LOBD_ID,
CLCL.CLCL_PA_ACCT_NO,
GRGR.GRGR_ID,
GRGR.GRGR_NAME,
CLCL.PDPD_ID,
CLCL.CLCL_NTWK_IND,
CLCL.CLCL_RECD_DT,
CCCC.CKPY_REF_ID,
CCCC.CLCK_INT_AMT,
MEME.MEME_CK

FROM 

{FacetsOwner}.CMC_SBSB_SUBSC SBSB,
{FacetsOwner}.CMC_CLCL_CLAIM CLCL,
{FacetsOwner}.CMC_PRPR_PROV PRPR,
{FacetsOwner}.CMC_MEME_MEMBER MEME,
{FacetsOwner}.CMC_CDML_CL_LINE CDML,
{FacetsOwner}.CMC_GRGR_GROUP GRGR,
{FacetsOwner}.CMC_CLCK_CLM_CHECK CCCC


WHERE


SBSB.SBSB_CK = CLCL.SBSB_CK 
AND CLCL.MEME_CK = MEME.MEME_CK
AND MEME.SBSB_CK = SBSB.SBSB_CK
AND CLCL.PRPR_ID = PRPR.PRPR_ID
AND CLCL.CLCL_ID = CDML.CLCL_ID
AND CCCC.CLCL_ID = CLCL.CLCL_ID
AND CLCL.GRGR_CK = GRGR.GRGR_CK
AND CLCL.CLCL_CL_TYPE = 'M'
AND CLCL.CLCL_PAID_DT  IN (SELECT (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) FROM 
(SELECT (CASE WHEN MAX(CKPY_PAY_DT)ISNULL THEN (SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_REF_ID IN (SELECT SYIN_REF_ID FROM CER_SYIN_INST WHERE SYIN_INST IN (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD")))   
                             ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_INST IN 
                            (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD"))A)

UNION

SELECT

SBSB.SBSB_ID,
MEME.MEME_SFX,
MEME.MEME_LAST_NAME,
MEME.MEME_FIRST_NAME,
MEME.MEME_MID_INIT,
CLCL.CLCL_ID,
CLCL.CLCL_CL_TYPE,
CLCL.CLCL_PAID_DT,
PRPR.PRPR_NAME,
PRPR.MCTN_ID,
CDML.LOBD_ID,
CLCL.CLCL_PA_ACCT_NO,
GRGR.GRGR_ID,
GRGR.GRGR_NAME,
CLCL.PDPD_ID,
CLCL.CLCL_NTWK_IND,
CLCL.CLCL_RECD_DT,
CCCC.CKPY_REF_ID,
CCCC.CLCK_INT_AMT,
MEME.MEME_CK


FROM 

{FacetsOwner}.CMC_SBSB_SUBSC SBSB,
{FacetsOwner}.CMC_CLCL_CLAIM CLCL,
{FacetsOwner}.CMC_PRPR_PROV PRPR,
{FacetsOwner}.CMC_MEME_MEMBER MEME,
{FacetsOwner}.CMC_CDDL_CL_LINE CDML,
{FacetsOwner}.CMC_GRGR_GROUP GRGR,
{FacetsOwner}.CMC_CLCK_CLM_CHECK CCCC

WHERE


SBSB.SBSB_CK = CLCL.SBSB_CK 
AND CLCL.MEME_CK = MEME.MEME_CK
AND MEME.SBSB_CK = SBSB.SBSB_CK
AND CLCL.PRPR_ID = PRPR.PRPR_ID
AND CLCL.CLCL_ID = CDML.CLCL_ID
AND CCCC.CLCL_ID = CLCL.CLCL_ID
AND CLCL.GRGR_CK = GRGR.GRGR_CK
AND CLCL.CLCL_CL_TYPE = 'D'
AND CLCL.CLCL_PAID_DT IN (SELECT (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) FROM 
(SELECT (CASE WHEN MAX(CKPY_PAY_DT)ISNULL THEN (SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_REF_ID IN (SELECT SYIN_REF_ID FROM CER_SYIN_INST WHERE SYIN_INST IN (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD")))   
                             ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_INST IN 
                            (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD"))A)
"""
df_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CLM)
    .load()
)

# Remove_Duplicates_11
df_Remove_Duplicates_11_temp = dedup_sort(
    df_CLM,
    ["CLCL_ID"],
    [("CLCL_ID","A")]
)
df_Remove_Duplicates_11 = df_Remove_Duplicates_11_temp.select(
    F.col("SBSB_ID").alias("SBSB_ID"),
    F.col("MEME_SFX").alias("MEME_SFX"),
    F.col("MEME_LAST_NAME").alias("MEME_LAST_NAME"),
    F.col("MEME_FIRST_NAME").alias("MEME_FIRST_NAME"),
    F.col("MEME_MID_INIT").alias("MEME_MID_INIT"),
    F.col("CLCL_ID").alias("CLCL_ID"),
    F.col("CLCL_CL_TYPE").alias("CLCL_CL_TYPE"),
    F.col("CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    F.col("PRPR_NAME").alias("PRPR_NAME"),
    F.col("MCTN_ID").alias("MCTN_ID"),
    F.col("LOBD_ID").alias("LOBD_ID"),
    F.col("CLCL_PA_ACCT_NO").alias("CLCL_PA_ACCT_NO"),
    F.col("GRGR_ID").alias("GRGR_ID"),
    F.col("GRGR_NAME").alias("GRGR_NAME"),
    F.col("PDPD_ID").alias("PDPD_ID"),
    F.col("CLCL_NTWK_IND").alias("CLCL_NTWK_IND"),
    F.col("CLCL_RECD_DT").alias("CLCL_RECD_DT"),
    F.col("CKPY_REF_ID").alias("CKPY_REF_ID"),
    F.col("CLCK_INT_AMT").alias("CLCK_INT_AMT"),
    F.col("MEME_CK").alias("MEME_CK")
)

# Copy_of_CLM
extract_query_Copy_of_CLM = f"""SELECT

SBSB.SBSB_ID,
MEME.MEME_SFX,
MEME.MEME_LAST_NAME,
MEME.MEME_FIRST_NAME,
MEME.MEME_MID_INIT,
CLCL.CLCL_ID,
CLCL.CLCL_CL_TYPE,
CLCL.CLCL_PAID_DT,
PRPR.PRPR_NAME,
PRPR.MCTN_ID,
CDML.LOBD_ID,
CLCL.CLCL_PA_ACCT_NO,
GRGR.GRGR_ID,
GRGR.GRGR_NAME,
CLCL.PDPD_ID,
CLCL.CLCL_NTWK_IND,
CLCL.CLCL_RECD_DT,
CCCC.CKPY_REF_ID,
CCCC.CLCK_INT_AMT,
MEME.MEME_CK

FROM 

{FacetsOwner}.CMC_SBSB_SUBSC SBSB,
{FacetsOwner}.CMC_CLCL_CLAIM CLCL,
{FacetsOwner}.CMC_PRPR_PROV PRPR,
{FacetsOwner}.CMC_MEME_MEMBER MEME,
{FacetsOwner}.CMC_CDML_CL_LINE CDML,
{FacetsOwner}.CMC_GRGR_GROUP GRGR,
{FacetsOwner}.CMC_CLCK_CLM_CHECK CCCC



WHERE


SBSB.SBSB_CK = CLCL.SBSB_CK 
AND CLCL.MEME_CK = MEME.MEME_CK
AND MEME.SBSB_CK = SBSB.SBSB_CK
AND CLCL.PRPR_ID = PRPR.PRPR_ID
AND CLCL.CLCL_ID = CDML.CLCL_ID
AND CCCC.CLCL_ID = CLCL.CLCL_ID
AND CLCL.GRGR_CK = GRGR.GRGR_CK
AND CLCL.CLCL_CL_TYPE = 'M'
AND CCCC.CKPY_REF_ID IN (SELECT DISTINCT CBI.CKPY_REF_ID FROM {FacetsOwner}.CMC_BPID_INDIC CBI WHERE CBI.BPID_PRINTED_DT IN (SELECT (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) FROM 
(SELECT (CASE WHEN MAX(CKPY_PAY_DT)ISNULL THEN (SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_REF_ID IN (SELECT SYIN_REF_ID FROM CER_SYIN_INST WHERE SYIN_INST IN (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD")))   
                             ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_INST IN 
                            (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD"))A))


UNION

SELECT

SBSB.SBSB_ID,
MEME.MEME_SFX,
MEME.MEME_LAST_NAME,
MEME.MEME_FIRST_NAME,
MEME.MEME_MID_INIT,
CLCL.CLCL_ID,
CLCL.CLCL_CL_TYPE,
CLCL.CLCL_PAID_DT,
PRPR.PRPR_NAME,
PRPR.MCTN_ID,
CDML.LOBD_ID,
CLCL.CLCL_PA_ACCT_NO,
GRGR.GRGR_ID,
GRGR.GRGR_NAME,
CLCL.PDPD_ID,
CLCL.CLCL_NTWK_IND,
CLCL.CLCL_RECD_DT,
CCCC.CKPY_REF_ID,
CCCC.CLCK_INT_AMT,
MEME.MEME_CK


FROM 

{FacetsOwner}.CMC_SBSB_SUBSC SBSB,
{FacetsOwner}.CMC_CLCL_CLAIM CLCL,
{FacetsOwner}.CMC_PRPR_PROV PRPR,
{FacetsOwner}.CMC_MEME_MEMBER MEME,
{FacetsOwner}.CMC_CDDL_CL_LINE CDML,
{FacetsOwner}.CMC_GRGR_GROUP GRGR,
{FacetsOwner}.CMC_CLCK_CLM_CHECK CCCC

WHERE


SBSB.SBSB_CK = CLCL.SBSB_CK 
AND CLCL.MEME_CK = MEME.MEME_CK
AND MEME.SBSB_CK = SBSB.SBSB_CK
AND CLCL.PRPR_ID = PRPR.PRPR_ID
AND CLCL.CLCL_ID = CDML.CLCL_ID
AND CCCC.CLCL_ID = CLCL.CLCL_ID
AND CLCL.GRGR_CK = GRGR.GRGR_CK
AND CLCL.CLCL_CL_TYPE = 'D'
AND CCCC.CKPY_REF_ID IN (SELECT DISTINCT CBI.CKPY_REF_ID FROM {FacetsOwner}.CMC_BPID_INDIC CBI WHERE CBI.BPID_PRINTED_DT IN (SELECT (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) FROM 
(SELECT (CASE WHEN MAX(CKPY_PAY_DT)ISNULL THEN (SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_REF_ID IN (SELECT SYIN_REF_ID FROM CER_SYIN_INST WHERE SYIN_INST IN (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD")))   
                             ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_INST IN 
                            (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID="!CKD"))A))
"""
df_Copy_of_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_Copy_of_CLM)
    .load()
)

# Copy_of_Remove_Duplicates_11
df_Copy_of_Remove_Duplicates_11_temp = dedup_sort(
    df_Copy_of_CLM,
    ["CLCL_ID"],
    [("CLCL_ID","A")]
)
df_Copy_of_Remove_Duplicates_11 = df_Copy_of_Remove_Duplicates_11_temp.select(
    F.col("SBSB_ID").alias("SBSB_ID"),
    F.col("MEME_SFX").alias("MEME_SFX"),
    F.col("MEME_LAST_NAME").alias("MEME_LAST_NAME"),
    F.col("MEME_FIRST_NAME").alias("MEME_FIRST_NAME"),
    F.col("MEME_MID_INIT").alias("MEME_MID_INIT"),
    F.col("CLCL_ID").alias("CLCL_ID"),
    F.col("CLCL_CL_TYPE").alias("CLCL_CL_TYPE"),
    F.col("CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    F.col("PRPR_NAME").alias("PRPR_NAME"),
    F.col("MCTN_ID").alias("MCTN_ID"),
    F.col("LOBD_ID").alias("LOBD_ID"),
    F.col("CLCL_PA_ACCT_NO").alias("CLCL_PA_ACCT_NO"),
    F.col("GRGR_ID").alias("GRGR_ID"),
    F.col("GRGR_NAME").alias("GRGR_NAME"),
    F.col("PDPD_ID").alias("PDPD_ID"),
    F.col("CLCL_NTWK_IND").alias("CLCL_NTWK_IND"),
    F.col("CLCL_RECD_DT").alias("CLCL_RECD_DT"),
    F.col("CKPY_REF_ID").alias("CKPY_REF_ID"),
    F.col("CLCK_INT_AMT").alias("CLCK_INT_AMT"),
    F.col("MEME_CK").alias("MEME_CK")
)

# Funnel_52 => union of df_Remove_Duplicates_11 and df_Copy_of_Remove_Duplicates_11
df_Funnel_52 = df_Remove_Duplicates_11.unionByName(df_Copy_of_Remove_Duplicates_11)

# Remove_Duplicates_55
df_Remove_Duplicates_55_temp = dedup_sort(
    df_Funnel_52,
    ["CLCL_ID"],
    [("CLCL_ID","A")]
)
df_Remove_Duplicates_55 = df_Remove_Duplicates_55_temp.select(
    F.col("SBSB_ID").alias("SBSB_ID"),
    F.col("MEME_SFX").alias("MEME_SFX"),
    F.col("MEME_LAST_NAME").alias("MEME_LAST_NAME"),
    F.col("MEME_FIRST_NAME").alias("MEME_FIRST_NAME"),
    F.col("MEME_MID_INIT").alias("MEME_MID_INIT"),
    F.col("CLCL_ID").alias("CLCL_ID"),
    F.col("CLCL_CL_TYPE").alias("CLCL_CL_TYPE"),
    F.col("CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    F.col("PRPR_NAME").alias("PRPR_NAME"),
    F.col("MCTN_ID").alias("MCTN_ID"),
    F.col("LOBD_ID").alias("LOBD_ID"),
    F.col("CLCL_PA_ACCT_NO").alias("CLCL_PA_ACCT_NO"),
    F.col("GRGR_ID").alias("GRGR_ID"),
    F.col("GRGR_NAME").alias("GRGR_NAME"),
    F.col("PDPD_ID").alias("PDPD_ID"),
    F.col("CLCL_NTWK_IND").alias("CLCL_NTWK_IND"),
    F.col("CLCL_RECD_DT").alias("CLCL_RECD_DT"),
    F.col("CKPY_REF_ID").alias("CKPY_REF_ID"),
    F.col("CLCK_INT_AMT").alias("CLCK_INT_AMT"),
    F.col("MEME_CK").alias("MEME_CK")
)

# Transformer_27 => add column "tempMA" = PDPD_ID[1,2]
df_Transformer_27 = df_Remove_Duplicates_55.withColumn(
    "tempMA",
    F.substring("PDPD_ID",1,2)
)

# Lookup_15 => primary link df_Transformer_27 as "lnk_meme_ck"
#   lookup link df_Sequential_File_14 as "DSLink16" (inner join on CLCL_ID)
#   lookup link df_Remove_Duplicates_33 as "pdbl" (left join on CLCL_ID)

df_lookup_15_join_1 = df_Transformer_27.alias("lnk_meme_ck").join(
    df_Sequential_File_14.alias("DSLink16"),
    F.col("lnk_meme_ck.CLCL_ID") == F.col("DSLink16.CLCL_ID"),
    "inner"
)
df_lookup_15_join_2 = df_lookup_15_join_1.join(
    df_Remove_Duplicates_33.alias("pdbl"),
    F.col("lnk_meme_ck.CLCL_ID") == F.col("pdbl.CLCL_ID"),
    "left"
)

df_Lookup_15 = df_lookup_15_join_2.select(
    F.col("lnk_meme_ck.SBSB_ID").alias("SBSB_ID"),
    F.col("lnk_meme_ck.MEME_SFX").alias("MEME_SFX"),
    F.col("lnk_meme_ck.MEME_LAST_NAME").alias("MEME_LAST_NAME"),
    F.col("lnk_meme_ck.MEME_FIRST_NAME").alias("MEME_FIRST_NAME"),
    F.col("lnk_meme_ck.MEME_MID_INIT").alias("MEME_MID_INIT"),
    F.col("lnk_meme_ck.CLCL_ID").alias("CLCL_ID"),
    F.col("lnk_meme_ck.CLCL_CL_TYPE").alias("CLCL_CL_TYPE"),
    F.col("lnk_meme_ck.CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    F.col("lnk_meme_ck.PRPR_NAME").alias("PRPR_NAME"),
    F.col("lnk_meme_ck.MCTN_ID").alias("MCTN_ID"),
    F.col("lnk_meme_ck.LOBD_ID").alias("LOBD_ID"),
    F.col("lnk_meme_ck.CLCL_PA_ACCT_NO").alias("CLCL_PA_ACCT_NO"),
    F.col("lnk_meme_ck.GRGR_ID").alias("GRGR_ID"),
    F.col("lnk_meme_ck.GRGR_NAME").alias("GRGR_NAME"),
    F.col("DSLink16.CDOCID").alias("CDOCID"),
    F.col("lnk_meme_ck.PDPD_ID").alias("PDPD_ID"),
    F.col("lnk_meme_ck.CLCL_NTWK_IND").alias("CLCL_NTWK_IND"),
    F.col("lnk_meme_ck.CLCL_RECD_DT").alias("CLCL_RECD_DT"),
    F.col("lnk_meme_ck.CKPY_REF_ID").alias("CKPY_REF_ID"),
    F.col("lnk_meme_ck.tempMA").alias("tempMA"),
    F.col("pdbl.PDBL_ACCT_CAT").alias("PDBL_ACCT_CAT"),
    F.col("lnk_meme_ck.CLCK_INT_AMT").alias("CLCK_INT_AMT"),
    F.col("lnk_meme_ck.MEME_CK").alias("MEME_CK")
)

# Lookup_19 => 
#   primary link df_Lookup_15 as "ghlkup"
#   left join df_Remove_Duplicates_36 as DSLink2 on ghlkup.PDBL_ACCT_CAT == DSLink2.FNCL_LOB_CD
#   left join df_Copy_2_of_Remove_Duplicates_36 as hProvNm on ghlkup.CLCL_ID == hProvNm.CLCL_ID
#   left join df_Copy_of_Remove_Duplicates_36 as ProdBase on ghlkup.tempMA == ProdBase.PROD_PFX_CD
#   left join df_Remove_Duplicates_30 as DSLink31 on ghlkup.CLCL_ID == DSLink31.CLCL_ID
#   left join df_Copy_3_of_Remove_Duplicates_41 as ckpy_ref on ghlkup.CLCL_ID == ckpy_ref.CLCL_ID
#   left join df_rmdupclms3 as clor on ghlkup.CLCL_ID == clor.CLCL_ID
#   left join df_rm_cntwk as new on ghlkup.CLCL_ID == new.CLCL_ID
#   left join df_Remove_Duplicates_91 as gapsincare on ghlkup.MEME_CK == gapsincare.MBR_UNIQ_KEY

df_lookup_19_join_1 = df_Lookup_15.alias("ghlkup").join(
    df_Remove_Duplicates_36.alias("DSLink2"),
    F.col("ghlkup.PDBL_ACCT_CAT") == F.col("DSLink2.FNCL_LOB_CD"),
    "left"
)
df_lookup_19_join_2 = df_lookup_19_join_1.join(
    df_Copy_2_of_Remove_Duplicates_36.alias("hProvNm"),
    F.col("ghlkup.CLCL_ID") == F.col("hProvNm.CLCL_ID"),
    "left"
)
df_lookup_19_join_3 = df_lookup_19_join_2.join(
    df_Copy_of_Remove_Duplicates_36.alias("ProdBase"),
    F.col("ghlkup.tempMA") == F.col("ProdBase.PROD_PFX_CD"),
    "left"
)
df_lookup_19_join_4 = df_lookup_19_join_3.join(
    df_Remove_Duplicates_30.alias("DSLink31"),
    F.col("ghlkup.CLCL_ID") == F.col("DSLink31.CLCL_ID"),
    "left"
)
df_lookup_19_join_5 = df_lookup_19_join_4.join(
    df_Copy_3_of_Remove_Duplicates_41.alias("ckpy_ref"),
    F.col("ghlkup.CLCL_ID") == F.col("ckpy_ref.CLCL_ID"),
    "left"
)
df_lookup_19_join_6 = df_lookup_19_join_5.join(
    df_rmdupclms3.alias("clor"),
    F.col("ghlkup.CLCL_ID") == F.col("clor.CLCL_ID"),
    "left"
)
df_lookup_19_join_7 = df_lookup_19_join_6.join(
    df_rm_cntwk.alias("new"),
    F.col("ghlkup.CLCL_ID") == F.col("new.CLCL_ID"),
    "left"
)
df_lookup_19_join_8 = df_lookup_19_join_7.join(
    df_Remove_Duplicates_91.alias("gapsincare"),
    F.col("ghlkup.MEME_CK") == F.col("gapsincare.MBR_UNIQ_KEY"),
    "left"
)

df_Lookup_19 = df_lookup_19_join_8.select(
    F.col("ghlkup.SBSB_ID").alias("SBSB_ID"),
    F.col("ghlkup.MEME_SFX").alias("MEME_SFX"),
    F.col("ghlkup.MEME_LAST_NAME").alias("MEME_LAST_NAME"),
    F.col("ghlkup.MEME_FIRST_NAME").alias("MEME_FIRST_NAME"),
    F.col("ghlkup.MEME_MID_INIT").alias("MEME_MID_INIT"),
    F.col("ghlkup.CLCL_ID").alias("CLCL_ID"),
    F.col("ghlkup.CLCL_CL_TYPE").alias("CLCL_CL_TYPE"),
    F.col("ghlkup.CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    F.col("ghlkup.PRPR_NAME").alias("PRPR_NAME"),
    F.col("ghlkup.MCTN_ID").alias("MCTN_ID"),
    F.col("ghlkup.LOBD_ID").alias("LOBD_ID"),
    F.col("ghlkup.CLCL_PA_ACCT_NO").alias("CLCL_PA_ACCT_NO"),
    F.col("ghlkup.GRGR_ID").alias("GRGR_ID"),
    F.col("ghlkup.GRGR_NAME").alias("GRGR_NAME"),
    F.col("ghlkup.CDOCID").alias("CDOCID"),
    F.col("DSLink2.FNCL_FUND_TYP_NM").alias("FNCL_FUND_TYP_NM"),
    F.col("DSLink2.FNCL_GRP_SIZE_CAT_NM").alias("FNCL_GRP_SIZE_CAT_NM"),
    F.col("ghlkup.PDPD_ID").alias("PDPD_ID"),
    F.col("ghlkup.CLCL_NTWK_IND").alias("CLCL_NTWK_IND"),
    F.col("ghlkup.CLCL_RECD_DT").alias("CLCL_RECD_DT"),
    F.col("ghlkup.CKPY_REF_ID").alias("CKPY_REF_ID"),
    F.col("hProvNm.CLPP_PR_NAME").alias("CLPP_PR_NAME"),
    F.col("hProvNm.CLPP_CLM_SUB_ORG").alias("CLPP_CLM_SUB_ORG"),
    F.col("DSLink31.BSDL_TYPE").alias("BSDL_TYPE"),
    F.col("ghlkup.PDBL_ACCT_CAT").alias("PDBL_ACCT_CAT"),
    F.col("ProdBase.PROD_NM").alias("PROD_NM"),
    F.col("ckpy_ref.LOBD_ID").alias("LOBD_ID_1"),
    F.col("ghlkup.CLCK_INT_AMT").alias("CLCK_INT_AMT"),
    F.col("clor.CLCL_ID").alias("CLCL_ID_OTHERFIELD3"),
    F.col("new.CLPP_CLASS_PROV").alias("CLPP_CLASS_PROV"),
    F.col("new.CLCL_PRE_PRICE_IND").alias("CLCL_PRE_PRICE_IND"),
    F.col("new.CDPP_CLASS_PROV").alias("CDPP_CLASS_PROV"),
    F.col("gapsincare.HEDIS_MESR_MOD_KEY_ID1").alias("HEDIS_MESR_MOD_KEY_ID1"),
    F.col("gapsincare.HEDIS_MESR_MOD_KEY_ID2").alias("HEDIS_MESR_MOD_KEY_ID2"),
    F.col("gapsincare.HEDIS_MESR_MOD_KEY_ID3").alias("HEDIS_MESR_MOD_KEY_ID3"),
    F.col("gapsincare.HEDIS_MESR_GAP_MOD_PRTY_NO1").alias("HEDIS_MESR_GAP_MOD_PRTY_NO1"),
    F.col("gapsincare.HEDIS_MESR_GAP_MOD_PRTY_NO2").alias("HEDIS_MESR_GAP_MOD_PRTY_NO2"),
    F.col("gapsincare.HEDIS_MESR_GAP_MOD_PRTY_NO3").alias("HEDIS_MESR_GAP_MOD_PRTY_NO3")
)

# Transformer_1 => final mapping of columns
df_Transformer_1 = df_Lookup_19.withColumn(
    "CRCRDTYP", F.lit("01")
).withColumn(
    "CRCRDVRSN", F.lit("32")
).withColumn(
    "CDOCID_out", F.col("CDOCID")
).withColumn(
    "CCLMEQUENCE", F.lit("1")
).withColumn(
    "CCLMNO", F.col("CLCL_ID")
).withColumn(
    "CENRNM",
    F.when(F.trim(F.col("MEME_MID_INIT")) == "", F.trim(F.col("MEME_FIRST_NAME"))+F.lit(" ")+F.trim(F.col("MEME_LAST_NAME"))) \
     .otherwise(F.trim(F.col("MEME_FIRST_NAME"))+F.lit(" ")+F.trim(F.col("MEME_MID_INIT"))+F.lit(" ")+F.trim(F.col("MEME_LAST_NAME")))
).withColumn(
    "CENRLASTNM", F.lit("")
).withColumn(
    "CENRID",
    F.when(F.length(F.trim(F.col("MEME_SFX"))) == 1,
           F.col("SBSB_ID")+F.lit("0")+F.col("MEME_SFX")
    ).otherwise(F.col("SBSB_ID")+F.col("MEME_SFX"))
).withColumn(
    "CENRSOCL", F.lit("")
).withColumn(
    "CENRPOLNO", F.lit("")
).withColumn(
    "CENRADDR1", F.lit("")
).withColumn(
    "CENRADDR2", F.lit("")
).withColumn(
    "CENRADDR3", F.lit("")
).withColumn(
    "CENRADDR4", F.lit("")
).withColumn(
    "CPATNNM",
    F.when(F.trim(F.col("MEME_MID_INIT")) == "", F.trim(F.col("MEME_FIRST_NAME"))+F.lit(" ")+F.trim(F.col("MEME_LAST_NAME"))) \
     .otherwise(F.trim(F.col("MEME_FIRST_NAME"))+F.lit(" ")+F.trim(F.col("MEME_MID_INIT"))+F.lit(" ")+F.trim(F.col("MEME_LAST_NAME")))
).withColumn(
    "CPATNID", F.lit("")
).withColumn(
    "CPATNACCTNO", F.col("CLCL_PA_ACCT_NO")
).withColumn(
    "CDPNDTNO", F.lit("")
).withColumn(
    "CPATNRELSHP", F.lit("")
).withColumn(
    "CRCVDDT",
    F.col("CLCL_RECD_DT").substr(F.lit(1),F.lit(4)) \
    +F.col("CLCL_RECD_DT").substr(F.lit(6),F.lit(2)) \
    +F.col("CLCL_RECD_DT").substr(F.lit(9),F.lit(2))
).withColumn(
    "CPRCSDT",
    F.col("CLCL_PAID_DT").substr(F.lit(1),F.lit(4)) \
    +F.col("CLCL_PAID_DT").substr(F.lit(6),F.lit(2)) \
    +F.col("CLCL_PAID_DT").substr(F.lit(9),F.lit(2))
).withColumn(
    "CPDDT", F.lit("")
).withColumn(
    "CSVCATESTRT", F.lit("")
).withColumn(
    "CSVCATEEND", F.lit("")
).withColumn(
    "CPRCSRID", F.lit("")
).withColumn(
    "CPRCSRNM", F.lit("")
).withColumn(
    "CCLMYR", F.lit("")
).withColumn(
    "CGRPHIER1",
    F.when(
        (F.trim(F.col("LOBD_ID_1")) == "") | (F.length(F.trim(F.col("LOBD_ID_1"))) == 0),
        F.when(F.trim(F.col("LOBD_ID")) == "ERR", F.lit("")).otherwise(F.col("LOBD_ID"))
    ).otherwise(F.col("LOBD_ID_1"))
).withColumn(
    "CGRPNM1", F.lit("")
).withColumn(
    "CGRPHIER2",
    F.when(F.col("FNCL_FUND_TYP_NM").isNull(), F.lit("")).otherwise(F.col("FNCL_FUND_TYP_NM"))
).withColumn(
    "CGRPNM2", F.lit("")
).withColumn(
    "CGRPHIER3",
    F.when(F.col("FNCL_GRP_SIZE_CAT_NM").isNull(), F.lit("")).otherwise(F.col("FNCL_GRP_SIZE_CAT_NM"))
).withColumn(
    "CGRPNM3", F.lit("")
).withColumn(
    "CGRPHIER4", F.col("PDPD_ID")
).withColumn(
    "CGRPNM4", F.lit("")
).withColumn(
    "CGRPHIER5", F.col("GRGR_ID")
).withColumn(
    "CGRPNM5", F.col("GRGR_NAME")
).withColumn(
    "CGRPHIER6", F.lit("")
).withColumn(
    "CGRPNM6", F.lit("")
).withColumn(
    "CGRPADDR1", F.lit("")
).withColumn(
    "CGRPADDR2", F.lit("")
).withColumn(
    "CGRPADDR3", F.lit("")
).withColumn(
    "CGRPADDR4", F.lit("")
).withColumn(
    "CGRPCITY", F.lit("")
).withColumn(
    "CGRPST", F.lit("")
).withColumn(
    "CGRPZIP", F.lit("")
).withColumn(
    "CGRPZIP4", F.lit("")
).withColumn(
    "CPROVTAXID", F.col("MCTN_ID")
).withColumn(
    "CPROVSUBTAXID", F.lit("")
).withColumn(
    "CALTPROVID", F.lit("")
).withColumn(
    "CNTNLPROVID", F.lit("")
).withColumn(
    "CBILLNPI", F.lit("")
).withColumn(
    "CRENDPHYSNPI", F.lit("")
).withColumn(
    "CPROVNM",
    F.when(
       (F.col("CLCL_ID").substr(F.lit(6),F.lit(1)) == "H"),
       F.when(F.trim(F.col("CLPP_PR_NAME")) != "", F.trim(F.col("CLPP_PR_NAME")))
        .otherwise(F.col("CLPP_CLM_SUB_ORG"))
    ).otherwise(F.col("PRPR_NAME"))
).withColumn(
    "CPROVADDR1", F.lit("")
).withColumn(
    "CPROVADDR2", F.lit("")
).withColumn(
    "CPROVADDR3", F.lit("")
).withColumn(
    "COPENFLD1",
    F.when(
       (F.col("LOBD_ID") == "MVMA") | (F.col("LOBD_ID") == "BAMA"),
       F.col("PROD_NM")
    ).otherwise(F.lit(""))
).withColumn(
    "COPENFLD2",
    F.when(F.col("BSDL_TYPE") == "CLNC", "Y").otherwise("N")
).withColumn(
    "COPENFLD3",
    F.when(F.trim(F.col("CLCL_ID_OTHERFIELD3")) == "", "N").otherwise("Y")
).withColumn(
    "COPENAMT1", F.lit("")
).withColumn(
    "COPENAMT2", F.lit("")
).withColumn(
    "COPENAMT3", F.lit("")
).withColumn(
    "CVCHRNO", F.lit("")
).withColumn(
    "CNTWK",
    F.when(
      (
        ((F.col("CLCL_PRE_PRICE_IND") == "H") | (F.col("CLCL_PRE_PRICE_IND") == "T"))
        & (
          (F.col("CLPP_CLASS_PROV") == "3") | (F.col("CLPP_CLASS_PROV") == "9")
          | (F.col("CLPP_CLASS_PROV") == "E") | (F.col("CLPP_CLASS_PROV") == "G")
        )
      )
      | (
        (F.col("CDPP_CLASS_PROV") == "3") | (F.col("CDPP_CLASS_PROV") == "9")
        | (F.col("CDPP_CLASS_PROV") == "E") | (F.col("CDPP_CLASS_PROV") == "G")
      ),
      "OUT OF NETWORK"
    ).otherwise(
      F.when(
        F.col("CLCL_CL_TYPE") == "M",
        F.when(F.trim(F.col("CLCL_NTWK_IND")) == "I","IN NETWORK")
         .otherwise(
           F.when(
             (F.trim(F.col("CLCL_NTWK_IND")) == "O") | (F.trim(F.col("CLCL_NTWK_IND")) == "P"),
             "OUT OF NETWORK"
           ).otherwise("NETWORK UNAVAILABLE")
         )
      ).otherwise(
        F.when(
          F.col("CLCL_CL_TYPE") == "D",
          F.when(F.trim(F.col("CLCL_NTWK_IND")) == "I","IN NETWORK")
           .otherwise(
             F.when(
               F.trim(F.col("CLCL_NTWK_IND")) == "O",
               "OUT OF NETWORK"
             ).otherwise(
               F.when(
                 F.trim(F.col("CLCL_NTWK_IND")) == "P",
                 "PARTICIPATING"
               ).otherwise("NETWORK UNAVAILABLE")
             )
           )
        ).otherwise("NETWORK UNAVAILABLE")
      )
    )
).withColumn(
    "CBILLCD", F.lit("")
).withColumn(
    "COPENFLD4",
    F.when(F.col("HEDIS_MESR_MOD_KEY_ID1").isNull(), "").otherwise(F.col("HEDIS_MESR_MOD_KEY_ID1"))
).withColumn(
    "COPENFLD5",
    F.when(F.col("HEDIS_MESR_MOD_KEY_ID2").isNull(), "").otherwise(F.col("HEDIS_MESR_MOD_KEY_ID2"))
).withColumn(
    "CCLMRELSTRING", F.col("CLCL_ID")
).withColumn(
    "CENRCITY", F.lit("")
).withColumn(
    "CENRST", F.lit("")
).withColumn(
    "CENRZIP", F.lit("")
).withColumn(
    "CPROVCITY", F.lit("")
).withColumn(
    "CPROVST", F.lit("")
).withColumn(
    "CPROVZIP", F.lit("")
).withColumn(
    "CPPOCNTRNO", F.lit("")
).withColumn(
    "CPATNDTOFBRTH", F.lit("")
).withColumn(
    "CADJS", F.lit("")
).withColumn(
    "CFORMLTRNM", F.lit("")
).withColumn(
    "CFORMLTRID", F.lit("")
).withColumn(
    "COPENFLD6",
    F.when(F.col("HEDIS_MESR_MOD_KEY_ID3").isNull(), "").otherwise(F.col("HEDIS_MESR_MOD_KEY_ID3"))
).withColumn(
    "COPENFLD7", F.lit("")
).withColumn(
    "COPENFLD8", F.lit("")
).withColumn(
    "COPENFLD9", F.lit("")
).withColumn(
    "COPENFLD10", F.lit("")
).withColumn(
    "CCLMFILINGINCD", F.lit("")
).withColumn(
    "CCLMFREQTYPCD", F.lit("")
).withColumn(
    "CCLMTATUSCD", F.lit("")
).withColumn(
    "CCRCTDENRID", F.lit("")
).withColumn(
    "CCRCTDENRNM", F.lit("")
).withColumn(
    "CCRCTDPATNID", F.lit("")
).withColumn(
    "CCRCTDPATNNM", F.lit("")
).withColumn(
    "CCOVEXPRTNDT", F.lit("")
).withColumn(
    "CDRGCD", F.lit("")
).withColumn(
    "CDRGWT", F.lit("")
).withColumn(
    "CINTRST", F.col("CLCK_INT_AMT")
).withColumn(
    "CNCPDPPDXNO", F.lit("")
).withColumn(
    "CPATNPDAMT", F.lit("")
).withColumn(
    "CENRFIRSTNM", F.lit("")
).withColumn(
    "CENRMIDNM", F.lit("")
).withColumn(
    "CPATNFIRSTNM", F.lit("")
).withColumn(
    "CPATNMIDNM", F.lit("")
).withColumn(
    "CPATNLASTNM", F.lit("")
).withColumn(
    "CPROVFIRSTNM", F.lit("")
).withColumn(
    "CPROVMIDNM", F.lit("")
).withColumn(
    "CPROVLASTNM", F.lit("")
).withColumn(
    "CCRCTDENRFIRSTNM", F.lit("")
).withColumn(
    "CCRCTDENRMIDNM", F.lit("")
).withColumn(
    "CCRCTDENRLASTNM", F.lit("")
).withColumn(
    "CCRCTDPATNFIRSTNM", F.lit("")
).withColumn(
    "CCRCTDPATNMIDNM", F.lit("")
).withColumn(
    "CCRCTDPATNLASTNM", F.lit("")
).withColumn(
    "CBNDL", F.lit("")
).withColumn(
    "CNTWKNM", F.lit("")
).withColumn(
    "CALTCLMNO", F.lit("")
).withColumn(
    "COPENFLD11", F.lit("")
).withColumn(
    "COPENFLD12", F.lit("")
).withColumn(
    "COPENFLD13", F.lit("")
).withColumn(
    "COPENFLD14", F.lit("")
).withColumn(
    "COPENFLD15", F.lit("")
).withColumn(
    "CENRSFX", F.lit("")
).withColumn(
    "CPATNUFFIX", F.lit("")
).withColumn(
    "CPROVSFX", F.lit("")
).withColumn(
    "CCCRID1", F.lit("")
).withColumn(
    "CCCRID2", F.lit("")
).withColumn(
    "CCCRID3", F.lit("")
).withColumn(
    "CCCRID4", F.lit("")
).withColumn(
    "CHRABAL", F.lit("")
).withColumn(
    "CCLMORTORDER", F.lit("")
).withColumn(
    "COPENFLD16", F.lit("")
).withColumn(
    "COPENFLD17", F.lit("")
).withColumn(
    "COPENFLD18", F.lit("")
).withColumn(
    "COPENFLD19", F.lit("")
).withColumn(
    "COPENFLD20", F.lit("")
).withColumn(
    "COPENFLD21", F.lit("")
).withColumn(
    "COPENFLD22", F.lit("")
).withColumn(
    "COPENFLD23", F.lit("")
).withColumn(
    "COPENFLD24", F.lit("")
).withColumn(
    "COPENFLD25", F.lit("")
).withColumn(
    "CCLMTYP", F.lit("")
).withColumn(
    "CEPISODICMERGEDUPVAL", F.lit("")
).withColumn(
    "CREISSUESEQ", F.lit("")
).withColumn(
    "CCHKNO", F.lit("")
).withColumn(
    "CCHKAMT", F.lit("")
)

# Select final columns in correct order for Sequential_File_0, then apply rpad for char columns
columns_for_final = [
    ("CRCRDTYP","char",2),
    ("CRCRDVRSN","char",2),
    ("CDOCID_out","char",25),
    ("CCLMEQUENCE","char",6),
    ("CCLMNO","char",25),
    ("CENRNM","char",80),
    ("CENRLASTNM","char",30),
    ("CENRID","char",20),
    ("CENRSOCL","char",9),
    ("CENRPOLNO","char",20),
    ("CENRADDR1","char",35),
    ("CENRADDR2","char",35),
    ("CENRADDR3","char",35),
    ("CENRADDR4","char",35),
    ("CPATNNM","char",80),
    ("CPATNID","char",20),
    ("CPATNACCTNO","char",40),
    ("CDPNDTNO","char",20),
    ("CPATNRELSHP","char",20),
    ("CRCVDDT","char",8),
    ("CPRCSDT","char",8),
    ("CPDDT","char",8),
    ("CSVCATESTRT","char",8),
    ("CSVCATEEND","char",8),
    ("CPRCSRID","char",5),
    ("CPRCSRNM","char",30),
    ("CCLMYR","char",4),
    ("CGRPHIER1","char",15),
    ("CGRPNM1","char",80),
    ("CGRPHIER2","char",15),
    ("CGRPNM2","char",80),
    ("CGRPHIER3","char",15),
    ("CGRPNM3","char",80),
    ("CGRPHIER4","char",15),
    ("CGRPNM4","char",80),
    ("CGRPHIER5","char",15),
    ("CGRPNM5","char",80),
    ("CGRPHIER6","char",15),
    ("CGRPNM6","char",80),
    ("CGRPADDR1","char",80),
    ("CGRPADDR2","char",80),
    ("CGRPADDR3","char",80),
    ("CGRPADDR4","char",80),
    ("CGRPCITY","char",30),
    ("CGRPST","char",2),
    ("CGRPZIP","char",5),
    ("CGRPZIP4","char",4),
    ("CPROVTAXID","char",9),
    ("CPROVSUBTAXID","char",3),
    ("CALTPROVID","char",20),
    ("CNTNLPROVID","char",10),
    ("CBILLNPI","char",10),
    ("CRENDPHYSNPI","char",10),
    ("CPROVNM","char",80),
    ("CPROVADDR1","char",40),
    ("CPROVADDR2","char",40),
    ("CPROVADDR3","char",40),
    ("COPENFLD1","char",50),
    ("COPENFLD2","char",50),
    ("COPENFLD3","char",50),
    ("COPENAMT1","char",15),
    ("COPENAMT2","char",15),
    ("COPENAMT3","char",15),
    ("CVCHRNO","char",16),
    ("CNTWK","char",50),
    ("CBILLCD","char",30),
    ("COPENFLD4","char",60),
    ("COPENFLD5","char",60),
    ("CCLMRELSTRING","char",50),
    ("CENRCITY","char",30),
    ("CENRST","char",2),
    ("CENRZIP","char",5),
    ("CPROVCITY","char",30),
    ("CPROVST","char",2),
    ("CPROVZIP","char",5),
    ("CPPOCNTRNO","char",5),
    ("CPATNDTOFBRTH","char",8),
    ("CADJS","char",15),
    ("CFORMLTRNM","char",40),
    ("CFORMLTRID","char",5),
    ("COPENFLD6","char",50),
    ("COPENFLD7","char",50),
    ("COPENFLD8","char",50),
    ("COPENFLD9","char",50),
    ("COPENFLD10","char",50),
    ("CCLMFILINGINCD","char",2),
    ("CCLMFREQTYPCD","char",1),
    ("CCLMTATUSCD","char",2),
    ("CCRCTDENRID","char",20),
    ("CCRCTDENRNM","char",30),
    ("CCRCTDPATNID","char",20),
    ("CCRCTDPATNNM","char",30),
    ("CCOVEXPRTNDT","char",8),
    ("CDRGCD","char",4),
    ("CDRGWT","char",15),
    ("CINTRST","char",15),
    ("CNCPDPPDXNO","char",15),
    ("CPATNPDAMT","char",15),
    ("CENRFIRSTNM","char",30),
    ("CENRMIDNM","char",30),
    ("CPATNFIRSTNM","char",30),
    ("CPATNMIDNM","char",30),
    ("CPATNLASTNM","char",30),
    ("CPROVFIRSTNM","char",30),
    ("CPROVMIDNM","char",30),
    ("CPROVLASTNM","char",30),
    ("CCRCTDENRFIRSTNM","char",30),
    ("CCRCTDENRMIDNM","char",30),
    ("CCRCTDENRLASTNM","char",30),
    ("CCRCTDPATNFIRSTNM","char",30),
    ("CCRCTDPATNMIDNM","char",30),
    ("CCRCTDPATNLASTNM","char",30),
    ("CBNDL","char",2),
    ("CNTWKNM","char",50),
    ("CALTCLMNO","char",25),
    ("COPENFLD11","char",50),
    ("COPENFLD12","char",50),
    ("COPENFLD13","char",50),
    ("COPENFLD14","char",50),
    ("COPENFLD15","char",50),
    ("CENRSFX","char",30),
    ("CPATNUFFIX","char",30),
    ("CPROVSFX","char",30),
    ("CCCRID1","char",20),
    ("CCCRID2","char",20),
    ("CCCRID3","char",20),
    ("CCCRID4","char",20),
    ("CHRABAL","char",15),
    ("CCLMORTORDER","char",256),
    ("COPENFLD16","char",50),
    ("COPENFLD17","char",50),
    ("COPENFLD18","char",50),
    ("COPENFLD19","char",50),
    ("COPENFLD20","char",50),
    ("COPENFLD21","char",50),
    ("COPENFLD22","char",50),
    ("COPENFLD23","char",50),
    ("COPENFLD24","char",50),
    ("COPENFLD25","char",50),
    ("CCLMTYP","char",3),
    ("CEPISODICMERGEDUPVAL","char",1),
    ("CREISSUESEQ","char",20),
    ("CCHKNO","char",16),
    ("CCHKAMT","char",15)
]

df_final = df_Transformer_1
for col_name, col_type, col_len in columns_for_final:
    if col_type in ("char","varchar"):
        df_final = df_final.withColumn(col_name, F.rpad(F.col(col_name), col_len, " "))

df_Sequential_File_0 = df_final.select([F.col(col[0]) for col in columns_for_final])

write_files(
    df_Sequential_File_0,
    f"{adls_path_publish}/external/Zelis_EOBClaimNonDetail01.dat",
    delimiter="\t",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)