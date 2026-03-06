# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2020, 2021, 2022, 2023, 2024 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:  This job extracts data from IDS CLM_SUPLMT_DIAG table
# MAGIC 
# MAGIC JOB NAME:  EdwProvClmDiagAllSuplmtExtr
# MAGIC CALLED BY: EdwLhoHistClmDiagAllSuplmCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                  
# MAGIC 
# MAGIC   
# MAGIC MODIFICATIONS:
# MAGIC Developer		Date		Project/Altius #		Change Description							Development Project		Code Reviewer		Date Reviewed  
# MAGIC ================================================================================================================================================================================================================= 
# MAGIC Veerendra Punati		2021-03-09	RA			Original Programming						EnterpriseDev2	       		Abhiram Dasarathy		2021-03-09
# MAGIC Lakshmi Devagiri		2021-04-06	RA			Added Source System Code 'MOSAICLIFECARE'      			EnterpriseDe	       		Jaideep Mankala           	04/07/2021 
# MAGIC Venkata Yama             	2021-10-27 	us438375                		Added Source System Code 'HCA'                                                          	EnterpriseDev2                		Jeyaprasanna              	2021-10-27
# MAGIC Venkata Yama            	2022-01-14 	US480876             		Added NONSTDSUPLMTDATA Source system                                             	EnterpriseDev2			Harsha Ravuri		2022-01-14
# MAGIC Ken Bradmon		2023-08-07	us559895			Added 8 new Providers to the IDS_CLM_SUPLMT_DIAG stage,		EnterpriseDev2                                       Reddy Sanam                           2023-10-27			
# MAGIC 								and the corresponding RunCycles to the job parameters.	
# MAGIC Ken Bradmon		2024-04-18	us-616897		Added new the Providers TRUMAN and CHILDRENSMERCY to the                    EnterpriseDev2                                        Goutham Kalidindi                    2024-04-19
# MAGIC 								IDS_CLM_SUPLMT_DIAG stage, and the corresponding RunCycles to
# MAGIC 								the job parameters.
# MAGIC Harsha Ravuri		2025-05-15	US#649175		Added new provider ASCENTIST to the IDS_CLM_SUPLMT_DIAG stage,	EnterpriseDev2                                       Jeyaprasanna                           2025-05-20
# MAGIC 								 and the corresponding RunCycles to the job parameters.


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ClayplatteRunCycle = get_widget_value('ClayplatteRunCycle','')
EncompassRunCycle = get_widget_value('EncompassRunCycle','')
JayhawkRunCycle = get_widget_value('JayhawkRunCycle','')
LibertyhospRunCycle = get_widget_value('LibertyhospRunCycle','')
MeritasRunCycle = get_widget_value('MeritasRunCycle','')
NorthlandRunCycle = get_widget_value('NorthlandRunCycle','')
OlathemedRunCycle = get_widget_value('OlathemedRunCycle','')
ProvstlukesRunCycle = get_widget_value('ProvstlukesRunCycle','')
SunflowerRunCycle = get_widget_value('SunflowerRunCycle','')
UntdmedgrpRunCycle = get_widget_value('UntdmedgrpRunCycle','')
BarrypointeRunCycle = get_widget_value('BarrypointeRunCycle','')
LeawoodfmlycareRunCycle = get_widget_value('LeawoodfmlycareRunCycle','')
PrimemoRunCycle = get_widget_value('PrimemoRunCycle','')
ProvidenceRunCycle = get_widget_value('ProvidenceRunCycle','')
SpiraRunCycle = get_widget_value('SpiraRunCycle','')
MosaicLifeCareRunCycle = get_widget_value('MosaicLifeCareRunCycle','')
HCARunCycle = get_widget_value('HCARunCycle','')
NONSTDSUPLMTDATARunCycle = get_widget_value('NONSTDSUPLMTDATARunCycle','')
CENTRUSHEALTHRunCycle = get_widget_value('CENTRUSHEALTHRunCycle','')
MOHEALTHRunCycle = get_widget_value('MOHEALTHRunCycle','')
GOLDENVALLEYRunCycle = get_widget_value('GOLDENVALLEYRunCycle','')
JEFFERSONRunCycle = get_widget_value('JEFFERSONRunCycle','')
WESTMOMEDCNTRRunCycle = get_widget_value('WESTMOMEDCNTRRunCycle','')
BLUESPRINGSRunCycle = get_widget_value('BLUESPRINGSRunCycle','')
EXCELSIORRunCycle = get_widget_value('EXCELSIORRunCycle','')
HARRISONVILLERunCycle = get_widget_value('HARRISONVILLERunCycle','')
TRUMANRunCycle = get_widget_value('TRUMANRunCycle','')
CHILDRENSMERCYRunCycle = get_widget_value('CHILDRENSMERCYRunCycle','')
ASCENTISTRunCycle = get_widget_value('ASCENTISTRunCycle','')

EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

# --------------------------------------------------------------------------------
# IDS_CLM_SUPLMT_DIAG (DB2ConnectorPX) - from IDS
# --------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

sql_ids_clm_suplmt_diag = f"""
Select
  CLM_SUPLMT_DIAG_SK, 
  MBR_UNIQ_KEY, 
  NTNL_PROV_ID, 
  CLM_SVC_STRT_DT, 
  DIAG_CD, 
  Replace(DIAG_CD,'.','') DIAG_CD_LKP, 
  DIAG_CD_TYP_CD, 
  SRC_SYS_CD, 
  CRT_RUN_CYC_EXCTN_SK, 
  LAST_UPDT_RUN_CYC_EXCTN_SK, 
  CLM_SRC_SYS_ORIG_CLM_SK, 
  DIAG_CD_SK, 
  MBR_SK, 
  PROV_SK, 
  CLM_DIAG_ORDNL_CD_SK, 
  CLM_PATN_ACCT_NO, 
  CLM_SRC_SYS_ORIG_CLM_ID,
  nvl(CLM_DIAG_ORDNL.CLM_DIAG_ORDNL_CD, 'NA') CLM_DIAG_ORDNL_CD,
  nvl(CLM_DIAG_ORDNL.CLM_DIAG_ORDNL_NM,'NA') CLM_DIAG_ORDNL_NM,
  NVL(DIAG_CD_TYP_CD_SK.DIAG_CD_TYP_CD_SK,1) DIAG_CD_TYP_CD_SK
from {IDSOwner}.CLM_SUPLMT_DIAG CSD
LEFT OUTER JOIN
(
  SELECT 
    CD_MPPNG_SK, 
    TRGT_CD AS CLM_DIAG_ORDNL_CD, 
    TRGT_CD_NM AS CLM_DIAG_ORDNL_NM
  FROM {IDSOwner}.CD_MPPNG
  WHERE TRGT_DOMAIN_NM='DIAGNOSIS ORDINAL'
    AND SRC_SYS_CD IN('PROVSTLUKES','BARRYPOINTE','SPIRA','PRIMEMO','LEAWOODFMLYCARE','CLAYPLATTE','JAYHAWK','ENCOMPASS',
      'LIBERTYHOSP','MERITAS','NORTHLAND','OLATHEMED','PROVIDENCE','SUNFLOWER','UNTDMEDGRP','CHILDRENSMERCY','TRUMAN',
      'HARRISONVILLE','EXCELSIOR','BLUESPRINGS','WESTMOMEDCNTR','JEFFERSON','GOLDENVALLEY','MOHEALTH','CENTRUSHEALTH',
      'NONSTDSUPLMTDATA','HCA','MOSAICLIFECARE')
  GROUP BY CD_MPPNG_SK, TRGT_CD, TRGT_CD_NM
  ORDER BY CD_MPPNG_SK
) CLM_DIAG_ORDNL
ON CSD.CLM_DIAG_ORDNL_CD_SK=CLM_DIAG_ORDNL.CD_MPPNG_SK
LEFT OUTER JOIN
(
  Select 
    CD_MPPNG_SK DIAG_CD_TYP_CD_SK,
    TRGT_CD DIAG_CD_TYP_CD_1
  from {IDSOwner}.CD_MPPNG
  where SRC_CLCTN_CD='FACETS DBO' 
    and TRGT_CLCTN_CD='IDS' 
    and TRGT_DOMAIN_NM='DIAGNOSIS CODE TYPE'
    and SRC_CD in ('9','0')
) DIAG_CD_TYP_CD_SK
on CSD.DIAG_CD_TYP_CD=DIAG_CD_TYP_CD_SK.DIAG_CD_TYP_CD_1
WHERE 
(
   (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ClayplatteRunCycle} AND CSD.SRC_SYS_CD = 'CLAYPLATTE')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {EncompassRunCycle} AND CSD.SRC_SYS_CD = 'ENCOMPASS')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {JayhawkRunCycle} AND CSD.SRC_SYS_CD = 'JAYHAWK')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {LibertyhospRunCycle} AND CSD.SRC_SYS_CD = 'LIBERTYHOSP')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {MeritasRunCycle} AND CSD.SRC_SYS_CD = 'MERITAS')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {NorthlandRunCycle} AND CSD.SRC_SYS_CD = 'NORTHLAND')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {OlathemedRunCycle} AND CSD.SRC_SYS_CD = 'OLATHEMED')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ProvstlukesRunCycle} AND CSD.SRC_SYS_CD = 'PROVSTLUKES')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {SunflowerRunCycle} AND CSD.SRC_SYS_CD = 'SUNFLOWER')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {UntdmedgrpRunCycle} AND CSD.SRC_SYS_CD = 'UNTDMEDGRP')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {BarrypointeRunCycle} AND CSD.SRC_SYS_CD = 'BARRYPOINTE')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {LeawoodfmlycareRunCycle} AND CSD.SRC_SYS_CD = 'LEAWOODFMLYCARE')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {PrimemoRunCycle} AND CSD.SRC_SYS_CD = 'PRIMEMO')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ProvidenceRunCycle} AND CSD.SRC_SYS_CD = 'PROVIDENCE')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {SpiraRunCycle} AND CSD.SRC_SYS_CD = 'SPIRA')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {MosaicLifeCareRunCycle} AND CSD.SRC_SYS_CD = 'MOSAICLIFECARE')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {HCARunCycle} AND CSD.SRC_SYS_CD = 'HCA')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {NONSTDSUPLMTDATARunCycle} AND CSD.SRC_SYS_CD = 'NONSTDSUPLMTDATA')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {CENTRUSHEALTHRunCycle} AND CSD.SRC_SYS_CD = 'CENTRUSHEALTH')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {MOHEALTHRunCycle} AND CSD.SRC_SYS_CD = 'MOHEALTH')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {GOLDENVALLEYRunCycle} AND CSD.SRC_SYS_CD = 'GOLDENVALLEY')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {JEFFERSONRunCycle} AND CSD.SRC_SYS_CD = 'JEFFERSON')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {WESTMOMEDCNTRRunCycle} AND CSD.SRC_SYS_CD = 'WESTMOMEDCNTR')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {BLUESPRINGSRunCycle} AND CSD.SRC_SYS_CD = 'BLUESPRINGS')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {EXCELSIORRunCycle} AND CSD.SRC_SYS_CD = 'EXCELSIOR')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {HARRISONVILLERunCycle} AND CSD.SRC_SYS_CD = 'HARRISONVILLE')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {TRUMANRunCycle} AND CSD.SRC_SYS_CD = 'TRUMAN')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {CHILDRENSMERCYRunCycle} AND CSD.SRC_SYS_CD = 'CHILDRENSMERCY')
   OR (CSD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ASCENTISTRunCycle} AND CSD.SRC_SYS_CD = 'ASCENTIST')
)
"""

df_IDS_CLM_SUPLMT_DIAG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_ids_clm_suplmt_diag)
    .load()
)

# --------------------------------------------------------------------------------
# EDW_DIAG_CD_D_1 (DB2ConnectorPX) - from EDW
# --------------------------------------------------------------------------------
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

sql_edw_diag_cd_d_1 = f"""
select distinct 
  DIAG_CD_TYP_CD, 
  CASE WHEN DIAG_CD_TYP_NM='ICD9' Then 'ICD-9' Else DIAG_CD_TYP_NM End DIAG_CD_TYP_NM
from {EDWOwner}.DIAG_CD_D 
with ur
"""

df_EDW_DIAG_CD_D_1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", sql_edw_diag_cd_d_1)
    .load()
)

# --------------------------------------------------------------------------------
# EDW_PROV_D (DB2ConnectorPX) - from EDW
# --------------------------------------------------------------------------------
sql_edw_prov_d = f"""
select distinct 
  PROV_ID,
  max(PROV_SK) PROV_SK
from {EDWOwner}.PROV_D
group by PROV_ID
with ur
"""

df_EDW_PROV_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", sql_edw_prov_d)
    .load()
)

# --------------------------------------------------------------------------------
# EDW_DIAG_CD_D_2 (DB2ConnectorPX) - from EDW
# --------------------------------------------------------------------------------
sql_edw_diag_cd_d_2 = f"""
select distinct 
  DIAG_CD,
  DIAG_CD_TYP_CD,
  DIAG_CD_DESC
from {EDWOwner}.DIAG_CD_D
with ur
"""

df_EDW_DIAG_CD_D_2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", sql_edw_diag_cd_d_2)
    .load()
)

# --------------------------------------------------------------------------------
# Lkp1 (PxLookup)
# --------------------------------------------------------------------------------
# Primary df: df_IDS_CLM_SUPLMT_DIAG
# Lookup 1: df_EDW_DIAG_CD_D_1 on (DIAG_CD_TYP_CD)
# Lookup 2: df_EDW_PROV_D on (PROV_SK)
# Lookup 3: df_EDW_DIAG_CD_D_2 on (DIAG_CD_LKP, DIAG_CD_TYP_CD)

df_Lkp1 = (
    df_IDS_CLM_SUPLMT_DIAG.alias("p")
    .join(df_EDW_DIAG_CD_D_1.alias("l1"), F.col("p.DIAG_CD_TYP_CD") == F.col("l1.DIAG_CD_TYP_CD"), "left")
    .join(df_EDW_PROV_D.alias("l2"), F.col("p.PROV_SK") == F.col("l2.PROV_SK"), "left")
    .join(
        df_EDW_DIAG_CD_D_2.alias("l3"),
        (F.col("p.DIAG_CD_LKP") == F.col("l3.DIAG_CD"))
        & (F.col("p.DIAG_CD_TYP_CD") == F.col("l3.DIAG_CD_TYP_CD")),
        "left"
    )
    .select(
        F.col("p.CLM_SUPLMT_DIAG_SK").alias("CLM_SUPLMT_DIAG_SK"),
        F.col("p.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("p.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
        F.col("p.CLM_SVC_STRT_DT").alias("CLM_SVC_STRT_DT"),
        F.col("p.DIAG_CD").alias("DIAG_CD"),
        F.col("p.DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD"),
        F.col("p.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("p.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("p.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("p.CLM_SRC_SYS_ORIG_CLM_SK").alias("CLM_SRC_SYS_ORIG_CLM_SK"),
        F.col("p.DIAG_CD_SK").alias("DIAG_CD_SK"),
        F.col("p.MBR_SK").alias("MBR_SK"),
        F.col("p.PROV_SK").alias("PROV_SK"),
        F.col("p.CLM_DIAG_ORDNL_CD_SK").alias("CLM_DIAG_ORDNL_CD_SK"),
        F.col("p.CLM_PATN_ACCT_NO").alias("CLM_PATN_ACCT_NO"),
        F.col("p.CLM_SRC_SYS_ORIG_CLM_ID").alias("CLM_SRC_SYS_ORIG_CLM_ID"),
        F.col("p.CLM_DIAG_ORDNL_CD").alias("CLM_DIAG_ORDNL_CD"),
        F.col("p.CLM_DIAG_ORDNL_NM").alias("CLM_DIAG_ORDNL_NM"),
        F.col("p.DIAG_CD_TYP_CD_SK").alias("DIAG_CD_TYP_CD_SK"),
        F.col("l1.DIAG_CD_TYP_NM").alias("DIAG_CD_TYP_NM"),
        F.col("l2.PROV_ID").alias("PROV_ID"),
        F.col("l3.DIAG_CD_DESC").alias("DIAG_CD_DESC"),
    )
)

# --------------------------------------------------------------------------------
# Xfm (CTransformerStage)
#   We create three output DataFrames:
#   1) lnk_ClmSuplmtDiag_In (filter: CLM_SUPLMT_DIAG_SK <> 0 AND <> 1)
#   2) lnk_ClmSuplmtDiag_NA (single row)
#   3) lnk_ClmSuplmtDiag_UNK (single row)
# --------------------------------------------------------------------------------

df_in = (
    df_Lkp1
    .filter((F.col("CLM_SUPLMT_DIAG_SK") != 0) & (F.col("CLM_SUPLMT_DIAG_SK") != 1))
    .select(
        F.col("CLM_SUPLMT_DIAG_SK").alias("CLM_SUPLMT_DIAG_SK"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
        F.col("CLM_SVC_STRT_DT").alias("CLM_SVC_STRT_DT"),
        F.col("DIAG_CD").alias("DIAG_CD"),
        F.col("DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),   # char(10)
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),  # char(10)
        F.col("CLM_SRC_SYS_ORIG_CLM_SK").alias("CLM_SRC_SYS_ORIG_CLM_SK"),
        F.col("DIAG_CD_SK").alias("DIAG_CD_SK"),
        F.col("MBR_SK").alias("MBR_SK"),
        F.col("PROV_SK").alias("PROV_SK"),
        F.when(
            F.col("CLM_DIAG_ORDNL_CD").isNull() | (trim(F.col("CLM_DIAG_ORDNL_CD")) == ''),
            F.lit("UNK")
        ).otherwise(F.col("CLM_DIAG_ORDNL_CD")).alias("CLM_DIAG_ORDNL_CD"),
        F.when(
            F.col("CLM_DIAG_ORDNL_NM").isNull() | (trim(F.col("CLM_DIAG_ORDNL_NM")) == ''),
            F.lit("UNK")
        ).otherwise(F.col("CLM_DIAG_ORDNL_NM")).alias("CLM_DIAG_ORDNL_NM"),
        F.when(
            F.col("DIAG_CD_DESC").isNull() | (trim(F.col("DIAG_CD_DESC")) == ''),
            F.lit("UNK")
        ).otherwise(F.col("DIAG_CD_DESC")).alias("DIAG_CD_DESC"),
        F.when(
            F.col("DIAG_CD_TYP_NM").isNull() | (trim(F.col("DIAG_CD_TYP_NM")) == ''),
            F.lit("UNK")
        ).otherwise(F.col("DIAG_CD_TYP_NM")).alias("DIAG_CD_TYP_NM"),
        F.col("CLM_PATN_ACCT_NO").alias("CLM_PATN_ACCT_NO"),
        F.col("CLM_SRC_SYS_ORIG_CLM_ID").alias("CLM_SRC_SYS_ORIG_CLM_ID"),
        F.when(
            F.col("PROV_ID").isNull() | (trim(F.col("PROV_ID")) == ''),
            F.lit("UNK")
        ).otherwise(F.col("PROV_ID")).alias("PROV_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CLM_DIAG_ORDNL_CD_SK").alias("CLM_DIAG_ORDNL_CD_SK"),
        F.when(F.col("DIAG_CD_TYP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("DIAG_CD_TYP_CD_SK")).alias("DIAG_CD_TYP_CD_SK")
    )
)

# Construct single-row DataFrames for the "NA" link and the "UNK" link.
# lnk_ClmSuplmtDiag_NA
schema_na = StructType([
    StructField("CLM_SUPLMT_DIAG_SK", IntegerType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("NTNL_PROV_ID", StringType(), True),
    StructField("CLM_SVC_STRT_DT", StringType(), True),
    StructField("DIAG_CD", StringType(), True),
    StructField("DIAG_CD_TYP_CD", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CLM_SRC_SYS_ORIG_CLM_SK", IntegerType(), True),
    StructField("DIAG_CD_SK", IntegerType(), True),
    StructField("MBR_SK", IntegerType(), True),
    StructField("PROV_SK", IntegerType(), True),
    StructField("CLM_DIAG_ORDNL_CD", StringType(), True),
    StructField("CLM_DIAG_ORDNL_NM", StringType(), True),
    StructField("DIAG_CD_DESC", StringType(), True),
    StructField("DIAG_CD_TYP_NM", StringType(), True),
    StructField("CLM_PATN_ACCT_NO", StringType(), True),
    StructField("CLM_SRC_SYS_ORIG_CLM_ID", StringType(), True),
    StructField("PROV_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("CLM_DIAG_ORDNL_CD_SK", IntegerType(), True),
    StructField("DIAG_CD_TYP_CD_SK", IntegerType(), True)
])

df_NA = spark.createDataFrame(
    [
        (
            1,  # CLM_SUPLMT_DIAG_SK
            1,  # MBR_UNIQ_KEY
            'NA',
            '1753-01-01',
            'NA',
            'NA',
            'NA',
            '1753-01-01',
            '1753-01-01',
            1,
            1,
            1,
            1,
            'NA',
            'NA',
            'NA',
            'NA',
            'NA',
            'NA',
            'NA',
            100,
            100,
            100,
            1,
            1
        )
    ],
    schema_na
)

# lnk_ClmSuplmtDiag_UNK
schema_unk = StructType([
    StructField("CLM_SUPLMT_DIAG_SK", IntegerType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("NTNL_PROV_ID", StringType(), True),
    StructField("CLM_SVC_STRT_DT", StringType(), True),
    StructField("DIAG_CD", StringType(), True),
    StructField("DIAG_CD_TYP_CD", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CLM_SRC_SYS_ORIG_CLM_SK", IntegerType(), True),
    StructField("DIAG_CD_SK", IntegerType(), True),
    StructField("MBR_SK", IntegerType(), True),
    StructField("PROV_SK", IntegerType(), True),
    StructField("CLM_DIAG_ORDNL_CD", StringType(), True),
    StructField("CLM_DIAG_ORDNL_NM", StringType(), True),
    StructField("DIAG_CD_DESC", StringType(), True),
    StructField("DIAG_CD_TYP_NM", StringType(), True),
    StructField("CLM_PATN_ACCT_NO", StringType(), True),
    StructField("CLM_SRC_SYS_ORIG_CLM_ID", StringType(), True),
    StructField("PROV_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("CLM_DIAG_ORDNL_CD_SK", IntegerType(), True),
    StructField("DIAG_CD_TYP_CD_SK", IntegerType(), True)
])

df_UNK = spark.createDataFrame(
    [
        (
            0,  # CLM_SUPLMT_DIAG_SK
            0,  # MBR_UNIQ_KEY
            'UNK',
            '1753-01-01',
            'UNK',
            'UNK',
            'UNK',
            '1753-01-01',
            '1753-01-01',
            0,
            0,
            0,
            0,
            'UNK',
            'UNK',
            'UNK',
            'UNK',
            'UNK',
            'UNK',
            'UNK',
            100,
            100,
            100,
            0,
            0
        )
    ],
    schema_unk
)

# --------------------------------------------------------------------------------
# Funnel_All (PxFunnel) - union the three DataFrames
# --------------------------------------------------------------------------------
df_funnel = df_in.unionByName(df_NA).unionByName(df_UNK)

# --------------------------------------------------------------------------------
# Seq_CLM_SUPLMT_DIAG_D (PxSequentialFile) - write to .dat
# --------------------------------------------------------------------------------
# We must preserve the column order and apply rpad for char columns of length 10
df_final = df_funnel.select(
    F.col("CLM_SUPLMT_DIAG_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("NTNL_PROV_ID"),
    F.col("CLM_SVC_STRT_DT"),
    F.col("DIAG_CD"),
    F.col("DIAG_CD_TYP_CD"),
    F.col("SRC_SYS_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLM_SRC_SYS_ORIG_CLM_SK"),
    F.col("DIAG_CD_SK"),
    F.col("MBR_SK"),
    F.col("PROV_SK"),
    F.col("CLM_DIAG_ORDNL_CD"),
    F.col("CLM_DIAG_ORDNL_NM"),
    F.col("DIAG_CD_DESC"),
    F.col("DIAG_CD_TYP_NM"),
    F.col("CLM_PATN_ACCT_NO"),
    F.col("CLM_SRC_SYS_ORIG_CLM_ID"),
    F.col("PROV_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_DIAG_ORDNL_CD_SK"),
    F.col("DIAG_CD_TYP_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/CLM_SUPLMT_DIAG_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)