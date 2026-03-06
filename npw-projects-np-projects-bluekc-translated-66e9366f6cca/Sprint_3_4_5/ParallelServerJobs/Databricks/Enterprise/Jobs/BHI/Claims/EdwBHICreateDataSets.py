# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdwBHIMbrExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Extract member data to be sent to BCBSA for BHI (Blue Health Intelligence)
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  
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
# MAGIC Developer                 Date                    Project/Ticket #\(9)           Change Description\(9)\(9)\(9)              Development Project\(9)      Code Reviewer\(9)        Date Reviewed       
# MAGIC ------------------               -------------------         ---------------------------\(9)           -----------------------------------------------------------------------            --------------------------------\(9)      -------------------------         -------------------------    
# MAGIC Kimberly Doty            2013-07-22          5115 BHI                               Original programming                                                  EnterpriseNewDevl            Kalyan Neelam         2013-11-11                                
# MAGIC 
# MAGIC Bhoomi Dasari           2014-01-24          5115 BHI                              Changed requierements                                              EnterpriseNewDevl            Kalyan Neelam          2014-01-27
# MAGIC 
# MAGIC Praveen Annam       2014-05-08            5115                                    Modified                                                                       EnterpriseNewdevl

# MAGIC Extract Data into Datasets for Lookup
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
BeginYrMo = get_widget_value('BeginYrMo','')
EndYrMo = get_widget_value('EndYrMo','')
PaidEndYrMo = get_widget_value('PaidEndYrMo','')
edw_secret_name = get_widget_value('edw_secret_name','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query = f"""Select 
CASE 
WHEN clmextr.CLM_EXTRNL_PROV_NM IS NULL
THEN '                                                  '
ELSE clmextr.CLM_EXTRNL_PROV_NM
END
AS CLM_EXTRNL_PROV_NM,
clmextr.CLM_EXTRNL_PROV_SVC_PROV_ID,
clmextr.CLM_EXTRNL_PROV_TAX_ID,
clmextr.CLM_EXTRNL_PROV_ZIP_CD_5,
clmextr.CLM_EXTRNL_PROV_NPI,
CASE 
WHEN clmextr.CLM_EXTRNL_PROV_SVC_PROV_NM IS NULL 
THEN '                                                                           '
ELSE clmextr.CLM_EXTRNL_PROV_SVC_PROV_NM 
END
AS CLM_EXTRNL_PROV_SVC_PROV_NM,
clmextr.CLM_SK,
CASE
 WHEN clmextr.CLM_EXTRNL_PROV_ST_CD  IN('AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY') 
 THEN 'US' 
  WHEN clmextr.CLM_EXTRNL_PROV_ST_CD  IN ('NA','UNK','AA','AE','AP')
  THEN 'UN' 
   ELSE clmextr.CLM_EXTRNL_PROV_ST_CD 
  END
 AS CLM_EXTRNL_PROV_ST_CD
From {EDWOwner}.CLM_EXTRNL_PROV_D clmextr,
        {EDWOwner}.CLM_f clmf
where 
clmextr.CLM_SK = clmf.CLM_SK
AND clmf.CLM_SVC_STRT_YR_MO_SK >= '201001'
AND clmf.CLM_PD_YR_MO_SK BETWEEN '{BeginYrMo}' AND '{PaidEndYrMo}'"""
df_CLM_EXTRNL_PROV_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_BHIClmExtrnProvD = df_CLM_EXTRNL_PROV_D.select(
    "CLM_EXTRNL_PROV_NM",
    "CLM_EXTRNL_PROV_SVC_PROV_ID",
    "CLM_EXTRNL_PROV_TAX_ID",
    "CLM_EXTRNL_PROV_ZIP_CD_5",
    "CLM_EXTRNL_PROV_NPI",
    "CLM_EXTRNL_PROV_SVC_PROV_NM",
    "CLM_SK",
    "CLM_EXTRNL_PROV_ST_CD"
)
write_files(
    df_BHIClmExtrnProvD,
    "BHIClmExtrnProvD.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

extract_query = f"""Select provd.PROV_ID,
provd.CMN_PRCT_FIRST_NM,
provd.CMN_PRCT_LAST_NM,
provd.PROV_ENTY_CD,
CASE 
WHEN provd.PROV_NM IS NULL THEN '                                                  ' 
ELSE provd.PROV_NM
END 
AS PROV_NM,
provd.PROV_NTNL_PROV_ID,
provd.PROV_SPEC_CD,
provd.PROV_TAX_ID,
CASE
 WHEN provd.PROV_PRI_PRCTC_ADDR_ST_CD  IN ('AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY') 
 THEN 'US' 
  WHEN provd.PROV_PRI_PRCTC_ADDR_ST_CD IN ('NA','UNK','AA',' NA','NA ','AE','AP')
   THEN 'UN'
ELSE  provd.PROV_PRI_PRCTC_ADDR_ST_CD
END
 AS PROV_PRI_PRCTC_ADDR_ST_CD,
CASE 
WHEN provd.PROV_PRI_PRCTC_ADDR_ZIP_CD_5 IS NULL 
THEN '     '  
ELSE provd.PROV_PRI_PRCTC_ADDR_ZIP_CD_5 
END 
AS PROV_PRI_PRCTC_ADDR_ZIP_CD_5,
provd.PROV_SK,
provd.PROV_FCLTY_TYP_CD,
provd.PAR_PROV_IN,
provd.CMN_PRCT_ID,
provd.CMN_PRCT_SK
from {EDWOwner}.PROV_D provd
"""
df_Prov_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_Transformer_29 = df_Prov_D.select(
    F.col("PROV_SK").alias("PROV_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("CMN_PRCT_FIRST_NM").alias("CMN_PRCT_FIRST_NM"),
    F.col("CMN_PRCT_LAST_NM").alias("CMN_PRCT_LAST_NM"),
    F.col("PROV_ENTY_CD").alias("PROV_ENTY_CD"),
    F.col("PROV_NM").alias("PROV_NM"),
    F.when(F.col("PROV_NTNL_PROV_ID") == "NA", F.lit("9999999999")).otherwise(F.col("PROV_NTNL_PROV_ID")).alias("PROV_NTNL_PROV_ID"),
    F.col("PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    F.col("PROV_TAX_ID").alias("PROV_TAX_ID"),
    F.col("PROV_PRI_PRCTC_ADDR_ST_CD").alias("PROV_PRI_PRCTC_ADDR_ST_CD"),
    F.col("PROV_PRI_PRCTC_ADDR_ZIP_CD_5").alias("PROV_PRI_PRCTC_ADDR_ZIP_CD_5"),
    F.col("PROV_FCLTY_TYP_CD").alias("PROV_FCLTY_TYP_CD"),
    F.col("PAR_PROV_IN").alias("PAR_PROV_IN"),
    F.col("CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    F.col("CMN_PRCT_SK").alias("CMN_PRCT_SK")
)
df_BHIProvD = df_Transformer_29.select(
    "PROV_SK",
    "PROV_ID",
    "CMN_PRCT_FIRST_NM",
    "CMN_PRCT_LAST_NM",
    "PROV_ENTY_CD",
    "PROV_NM",
    "PROV_NTNL_PROV_ID",
    "PROV_SPEC_CD",
    "PROV_TAX_ID",
    "PROV_PRI_PRCTC_ADDR_ST_CD",
    "PROV_PRI_PRCTC_ADDR_ZIP_CD_5",
    "PROV_FCLTY_TYP_CD",
    "PAR_PROV_IN",
    "CMN_PRCT_ID",
    "CMN_PRCT_SK"
)
write_files(
    df_BHIProvD,
    "BHIProvD.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

extract_query = f"""Select ntwkd.ntwk_sk,
ntwkd.NTWK_ID,
ntwkd.NTWK_SH_NM
From {EDWOwner}.NTWK_D ntwkd
"""
df_NTWK_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_BHINtwkD = df_NTWK_D.select(
    "ntwk_sk",
    "NTWK_ID",
    "NTWK_SH_NM"
)
write_files(
    df_BHINtwkD,
    "BHINtwkD.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

extract_query = f"""Select prmd.PROV_MCARE_SEQ_NO,
prmd.PROV_SK,
prmd.PROV_MCARE_BILL_NO 
From {EDWOwner}.PROV_MCARE_D prmd
"""
df_Prov_Mcare_d = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_BHIProvMedCareD = df_Prov_Mcare_d.select(
    "PROV_MCARE_SEQ_NO",
    "PROV_SK",
    "PROV_MCARE_BILL_NO"
)
write_files(
    df_BHIProvMedCareD,
    "BHIProvMedCareD.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

extract_query = f"""SELECT DISTINCT
   clmDiag.CLM_SK,
   clmDiag.CLM_DIAG_ORDNL_CD,
   diag.DIAG_FMTTED_CD,
   clmdiag.DIAG_CD 
FROM {EDWOwner}.CLM_DIAG_I clmDiag,
           {EDWOwner}.DIAG_CD_D diag,
           {EDWOwner}.CLM_F clmF,
           {EDWOwner}.W_MBR_SEL_DRVR drvr
WHERE drvr.MBR_SK = clmF.MBR_SK
AND clmF.CLM_SK = clmDiag.CLM_SK
AND   clmDiag.DIAG_CD_D_SK = diag.DIAG_CD_SK
AND   clmDiag.CLM_DIAG_ORDNL_CD IN ('A','1','2','3','4','5','6','7','8','9','10','11')
"""
df_CLM_DIAG_DIAG_CD_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_BHIClaimDiag_DiagCd = df_CLM_DIAG_DIAG_CD_D.select(
    "CLM_SK",
    "CLM_DIAG_ORDNL_CD",
    "DIAG_FMTTED_CD",
    "DIAG_CD"
)
write_files(
    df_BHIClaimDiag_DiagCd,
    "BHIClaimDiag_DiagCd.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

extract_query = f"""Select PROC_CD_SK, PROC_CD from {EDWOwner}.PROC_CD_D
"""
df_PROC_CD_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_BHIProccdD = df_PROC_CD_D.select(
    "PROC_CD_SK",
    "PROC_CD"
)
write_files(
    df_BHIProccdD,
    "BHIProccdD.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

extract_query = f"""SELECT 
XREF.PROV_ENTY_CD,
XREF.BHI_PROV_SPEC_CD,
XREF.BHI_PROV_TYP_CD,
XREF.PROV_SPEC_FCLTY_TYP_CD
 FROM
{EDWOwner}.P_BCBSA_BHI_PROV_XREF XREF
"""
df_P_BCBSA_BHI_PROV_XREF = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_BHIPBcbsBhiProvXref = df_P_BCBSA_BHI_PROV_XREF.select(
    "PROV_ENTY_CD",
    "BHI_PROV_SPEC_CD",
    "BHI_PROV_TYP_CD",
    "PROV_SPEC_FCLTY_TYP_CD"
)
write_files(
    df_BHIPBcbsBhiProvXref,
    "BHIPBcbsBhiProvXref.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

extract_query = f"""select 
ZIP_CD,
LAST_UPDT_DT_SK
from  
{EDWOwner}.P_BCBSA_BHI_ZIP
"""
df_P_BCBSA_BHI_ZIP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = f"""select 
 clmf.MBR_SK,
clmf.CLM_SVC_STRT_DT_SK
from {EDWOwner}.CLM_f clmf
where clmf.CLM_SVC_STRT_YR_MO_SK >= '201001'
AND clmf.CLM_PD_YR_MO_SK BETWEEN '{BeginYrMo}' AND '{PaidEndYrMo}'
"""
df_CLM_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = f"""select 
mbrhist.MBR_SK,
CASE
 WHEN mbrhist.MBR_HOME_ADDR_ST_CD  IN('AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY') 
 THEN 'US' 
 WHEN mbrhist.MBR_HOME_ADDR_ST_CD IN ('NA','UNK','AA','AE','AP','  ')
  THEN 'UN'
  ELSE mbrhist.MBR_HOME_ADDR_ST_CD
  END
 AS MBR_HOME_ADDR_ST_CD,
CASE 
WHEN mbrhist.MBR_HOME_ADDR_ZIP_CD_5 IS NULL THEN '     ' ELSE mbrhist.MBR_HOME_ADDR_ZIP_CD_5 END AS MBR_HOME_ADDR_ZIP_CD_5,
mbrhist.EDW_RCRD_STRT_DT_SK,
mbrhist.EDW_RCRD_END_DT_SK
from  {EDWOwner}.MBR_GNRL_LOC_HIST_D mbrhist
"""
df_MBR_GNRL_LOC_HIST_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Join_37 = df_CLM_F.alias("jn2").join(
    df_MBR_GNRL_LOC_HIST_D.alias("join"),
    F.col("jn2.MBR_SK") == F.col("join.MBR_SK"),
    "inner"
)
df_Join_37_select = df_Join_37.select(
    F.col("jn2.MBR_SK").alias("MBR_SK"),
    F.col("jn2.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("join.MBR_HOME_ADDR_ST_CD").alias("MBR_HOME_ADDR_ST_CD"),
    F.col("join.MBR_HOME_ADDR_ZIP_CD_5").alias("MBR_HOME_ADDR_ZIP_CD_5"),
    F.col("join.EDW_RCRD_STRT_DT_SK").alias("EDW_RCRD_STRT_DT_SK"),
    F.col("join.EDW_RCRD_END_DT_SK").alias("EDW_RCRD_END_DT_SK")
)

df_Lookup_33 = df_Join_37_select.alias("Trnss").join(
    df_P_BCBSA_BHI_ZIP.alias("join"),
    (F.col("Trnss.MBR_HOME_ADDR_ZIP_CD_5") == F.col("join.ZIP_CD")) & (F.col("Trnss.MBR_SK") == F.col("join.MBR_SK")),
    "left"
)
df_Lookup_33_select = df_Lookup_33.select(
    F.col("Trnss.MBR_SK").alias("MBR_SK"),
    F.col("Trnss.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("Trnss.MBR_HOME_ADDR_ST_CD").alias("MBR_HOME_ADDR_ST_CD"),
    F.col("join.ZIP_CD").alias("MBR_HOME_ADDR_ZIP_CD_5"),
    F.col("Trnss.EDW_RCRD_STRT_DT_SK").alias("EDW_RCRD_STRT_DT_SK"),
    F.col("Trnss.EDW_RCRD_END_DT_SK").alias("EDW_RCRD_END_DT_SK")
)

df_Transformer_27 = df_Lookup_33_select.withColumn(
    "svDatechk",
    F.when(
        (F.col("CLM_SVC_STRT_DT_SK") >= F.col("EDW_RCRD_STRT_DT_SK")) &
        (F.col("CLM_SVC_STRT_DT_SK") <= F.col("EDW_RCRD_END_DT_SK")),
        F.lit("Y")
    ).otherwise(F.lit("N"))
)
df_Transformer_27_filtered = df_Transformer_27.filter(F.col("svDatechk") == "Y")
df_BHIMbrGnrlLocHist = df_Transformer_27_filtered.select(
    F.col("MBR_SK").alias("MBR_SK"),
    F.when(F.col("MBR_HOME_ADDR_ST_CD").isNull(), F.lit("UN")).otherwise(F.col("MBR_HOME_ADDR_ST_CD")).alias("MBR_HOME_ADDR_ST_CD"),
    F.when(
        F.col("MBR_HOME_ADDR_ZIP_CD_5").isNull() |
        (F.col("MBR_HOME_ADDR_ZIP_CD_5") == "00000") |
        (F.col("MBR_HOME_ADDR_ZIP_CD_5") == "99999") |
        (F.col("MBR_HOME_ADDR_ZIP_CD_5") == " ") |
        (F.length(F.regexp_replace(F.col("MBR_HOME_ADDR_ZIP_CD_5"), "[0-9]", "")) != 0),
        F.lit("     ")
    ).otherwise(F.col("MBR_HOME_ADDR_ZIP_CD_5")).alias("MBR_HOME_ADDR_ZIP_CD_5")
)
write_files(
    df_BHIMbrGnrlLocHist,
    "BHIMbrGnrlLocHist.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)