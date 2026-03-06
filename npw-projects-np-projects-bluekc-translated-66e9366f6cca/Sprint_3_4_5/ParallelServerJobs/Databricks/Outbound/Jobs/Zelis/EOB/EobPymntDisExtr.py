# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Payment distribution record data from Facets and generates a data file for RedCard.
# MAGIC 
# MAGIC Job Name:  EobPymntDisExtr
# MAGIC Called By:RedCardEobExtrCntl
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                               Date                 Project/Altiris #      Change Description                                                                      Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                           --------------------     ------------------------      -----------------------------------------------------------------------                               --------------------------------       -------------------------------   ----------------------------       
# MAGIC Raja Gummadi                     2020-12-05            RedCard                     Original Devlopment                                                                OutboundDev3            Jaideep Mankala         12/27/2020
# MAGIC Raja Gummadi                     2021-02-04            343913                      Added void logic                                                                       OutboundDev3               Jaideep Mankala          02/04/2021
# MAGIC 
# MAGIC Megan Conway                  2022-03-10               	                 S2S Remediation - MSSQL connection parameters added	           OutboundDev5		Ken Bradmon	2022-05-19
# MAGIC Arpitha V                            2024-08-06           US 626236           Updated the query to add six new LOBD_ID's in                             OutboundDev3                Jeyaprasanna                2024-08-16
# MAGIC                                                                                                                 CLM,Void stages


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, when, substring, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Outbound
# COMMAND ----------

FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CLMPDDT = get_widget_value('CLMPDDT','')

# Read seq_Chktemp (PxSequentialFile)
schema_seq_Chktemp = StructType([
    StructField("CLCL_ID", StringType(), False),
    StructField("CKPY_REF_ID", StringType(), False),
    StructField("CDOCID", StringType(), False)
])
df_seq_Chktemp = (
    spark.read.format("csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("delimiter", ",")
    .option("quote", '"')
    .schema(schema_seq_Chktemp)
    .load(f"{adls_path_publish}/external/Zelis_EOB_Checktemp.dat")
)

# Read CLM (ODBCConnectorPX)
jdbc_url_clm, jdbc_props_clm = get_db_config(facets_secret_name)
query_CLM = f"""
SELECT 
CBI.CKCK_PAYEE_NAME AS PRPR_NAME,
CCC.CKCK_CK_NO,
CCCC.CKPY_PAY_DT,
CCCC.CLCL_ID,
CBI.CKPY_NET_AMT,
CBI.CKPY_REF_ID,
CBI.LOBD_ID,
CBI.LOBD_NAME
FROM 
{FacetsOwner}.CMC_BPID_INDIC CBI,
{FacetsOwner}.CMC_CKCK_CHECK CCC,
{FacetsOwner}.CMC_CLCK_CLM_CHECK CCCC,
{FacetsOwner}.CMC_PYBA_BANK_ACCT CPBA
WHERE
CBI.CKPY_REF_ID = CCCC.CKPY_REF_ID
AND CCCC.CKPY_REF_ID = CCC.CKPY_REF_ID
AND CBI.PYBA_ID = CPBA.PYBA_ID
AND CBI.LOBD_ID NOT IN ('FEP','FEPB','FEPH','FEPS','FEPV','FEHS','FEHB','FEHV','FEPL','FEPM','FEPT')
AND CBI.CKPY_PAYEE_TYPE IN ('S','A','M')
AND CBI.CKPY_NET_AMT > 0.00
AND CCCC.CLCK_NET_AMT > 0.00
AND CBI.CKPY_PAY_DT  IN (SELECT (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) FROM 
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
"""
df_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_clm)
    .options(**jdbc_props_clm)
    .option("query", query_CLM)
    .load()
)

# Read Void (ODBCConnectorPX)
jdbc_url_void, jdbc_props_void = get_db_config(facets_secret_name)
query_Void = f"""
SELECT 
CBI.CKCK_PAYEE_NAME AS PRPR_NAME,
CBI.BPID_CK_NO AS CKCK_CK_NO,
CBI.BPID_PRINTED_DT AS CKPY_PAY_DT,
CCCC.CLCL_ID,
CBI.CKPY_NET_AMT,
CBI.CKPY_REF_ID,
CBI.LOBD_ID,
CBI.LOBD_NAME
FROM 
{FacetsOwner}.CMC_BPID_INDIC CBI,
{FacetsOwner}.CMC_CLCK_CLM_CHECK CCCC,
{FacetsOwner}.CMC_PYBA_BANK_ACCT CPBA
WHERE
CBI.CKPY_REF_ID = CCCC.CKPY_REF_ID
AND CBI.PYBA_ID = CPBA.PYBA_ID
AND CBI.LOBD_ID NOT IN ('FEP','FEPB','FEPH','FEPS','FEPV','FEHS','FEHB','FEHV','FEPL','FEPM','FEPT')
AND CBI.CKPY_PAYEE_TYPE IN ('S','A','M')
AND CBI.CKPY_NET_AMT > 0.00
AND CCCC.CLCK_NET_AMT > 0.00
AND CBI.BPID_CK_NO <> 0
AND CBI.BPID_PRINTED_DT IN (SELECT (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) FROM 
(SELECT (CASE WHEN MAX(CKPY_PAY_DT)IS NULL THEN (SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_REF_ID IN (SELECT SYIN_REF_ID FROM CER_SYIN_INST WHERE SYIN_INST IN (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID=\"!CKD\")))   
                             ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
                             FROM CMC_BPID_INDIC 
                             WHERE SYIN_INST IN 
                            (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID=\"!CKD\"))A)
"""
df_Void = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_void)
    .options(**jdbc_props_void)
    .option("query", query_Void)
    .load()
)

# Funnel (PxFunnel) => union
df_fnl_recs = (
    df_Void.select(
        col("PRPR_NAME"),
        col("CKCK_CK_NO"),
        col("CKPY_PAY_DT"),
        col("CLCL_ID"),
        col("CKPY_NET_AMT"),
        col("CKPY_REF_ID"),
        col("LOBD_ID"),
        col("LOBD_NAME")
    )
    .unionByName(
        df_CLM.select(
            col("PRPR_NAME"),
            col("CKCK_CK_NO"),
            col("CKPY_PAY_DT"),
            col("CLCL_ID"),
            col("CKPY_NET_AMT"),
            col("CKPY_REF_ID"),
            col("LOBD_ID"),
            col("LOBD_NAME")
        )
    )
)

# xfrm_Map (CTransformerStage)
df_xfrm_Map = df_fnl_recs.select(
    col("PRPR_NAME").alias("PRPR_NAME"),
    col("CKCK_CK_NO").alias("CKCK_CK_NO"),
    col("CKPY_PAY_DT").alias("CKPY_PAY_DT"),
    col("CKPY_NET_AMT").alias("CKPY_NET_AMT"),
    col("CLCL_ID").alias("CLCL_ID"),
    col("CKPY_REF_ID").alias("CKPY_REF_ID"),
    col("LOBD_ID").alias("LOBD_ID"),
    col("LOBD_NAME").alias("LOBD_NAME")
)

# lkp_ID (PxLookup) => inner join with seq_Chktemp
df_lkp_ID = (
    df_xfrm_Map.alias("lnk_meme_ck")
    .join(
        df_seq_Chktemp.alias("lnk_ref"),
        (
            (col("lnk_meme_ck.CLCL_ID") == col("lnk_ref.CLCL_ID")) &
            (col("lnk_meme_ck.CKPY_REF_ID") == col("lnk_ref.CKPY_REF_ID"))
        ),
        "inner"
    )
    .select(
        col("lnk_meme_ck.CLCL_ID").alias("CLCL_ID"),
        col("lnk_meme_ck.PRPR_NAME").alias("PRPR_NAME"),
        col("lnk_ref.CDOCID").alias("CDOCID"),
        col("lnk_meme_ck.CKCK_CK_NO").alias("CKCK_CK_NO"),
        col("lnk_meme_ck.CKPY_PAY_DT").alias("CKPY_PAY_DT"),
        col("lnk_meme_ck.CKPY_NET_AMT").alias("CKPY_NET_AMT"),
        col("lnk_meme_ck.CKPY_REF_ID").alias("CKPY_REF_ID"),
        col("lnk_meme_ck.LOBD_ID").alias("LOBD_ID"),
        col("lnk_meme_ck.LOBD_NAME").alias("LOBD_NAME")
    )
)

# rmdup_ID (PxRemDup)
df_rmdup_pre = df_lkp_ID
df_rmdup_ID = dedup_sort(
    df_rmdup_pre,
    ["CLCL_ID"],
    [("CLCL_ID", "D")]
).select(
    col("PRPR_NAME"),
    col("CDOCID"),
    col("CKCK_CK_NO"),
    col("CKPY_PAY_DT"),
    col("CKPY_NET_AMT"),
    col("CKPY_REF_ID"),
    col("LOBD_ID"),
    col("LOBD_NAME")
)

# xfrm_BusinessRules (CTransformerStage)
df_xfrm_BusinessRules = df_rmdup_ID.select(
    lit("05").alias("CRCRDTYP"),
    lit("02").alias("CRCRDVRSN"),
    col("CDOCID").alias("CDOCID"),
    col("PRPR_NAME").alias("CPAYENM"),
    col("CKPY_NET_AMT").alias("CCHKAMT"),
    when((col("CKPY_NET_AMT") != 0.0) & (col("CKCK_CK_NO") == 0), col("CKPY_REF_ID"))
    .otherwise(col("CKCK_CK_NO")).alias("CCHKNO"),
    (
        substring(col("CKPY_PAY_DT"), 1, 4)
        .concat(substring(col("CKPY_PAY_DT"), 6, 2))
        .concat(substring(col("CKPY_PAY_DT"), 9, 2))
    ).alias("CCHKDT"),
    col("LOBD_ID").alias("COPENFLD1"),
    col("LOBD_NAME").alias("COPENFLD2"),
    lit(" " * 30).alias("COPENFLD3"),
    lit(" " * 30).alias("COPENFLD4"),
    lit(" " * 30).alias("COPENFLD5")
)

# seq_PymntDis (PxSequentialFile) => final select with rpad, write to file
df_seq_PymntDis = df_xfrm_BusinessRules.select(
    rpad(col("CRCRDTYP"), 2, " ").alias("CRCRDTYP"),
    rpad(col("CRCRDVRSN"), 2, " ").alias("CRCRDVRSN"),
    rpad(col("CDOCID"), 25, " ").alias("CDOCID"),
    rpad(col("CPAYENM"), 80, " ").alias("CPAYENM"),
    rpad(col("CCHKAMT"), 15, " ").alias("CCHKAMT"),
    rpad(col("CCHKNO"), 16, " ").alias("CCHKNO"),
    rpad(col("CCHKDT"), 8, " ").alias("CCHKDT"),
    rpad(col("COPENFLD1"), 30, " ").alias("COPENFLD1"),
    rpad(col("COPENFLD2"), 30, " ").alias("COPENFLD2"),
    rpad(col("COPENFLD3"), 30, " ").alias("COPENFLD3"),
    rpad(col("COPENFLD4"), 30, " ").alias("COPENFLD4"),
    rpad(col("COPENFLD5"), 30, " ").alias("COPENFLD5")
)

write_files(
    df_seq_PymntDis,
    f"{adls_path_publish}/external/Zelis_EOBPymntDisExtr05.dat",
    delimiter="\t",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)