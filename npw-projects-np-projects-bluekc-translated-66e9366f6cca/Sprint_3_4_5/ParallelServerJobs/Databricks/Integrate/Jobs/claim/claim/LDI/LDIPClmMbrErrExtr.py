# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : BCBSCommonClmErrMbrRecycCntl
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION :  LDI Common claims recycle process
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                  Date                 Project/Altiris #          Change Description                                                           Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                            --------------------         ------------------------          -----------------------------------------------------------------------                   --------------------------------         -------------------------------   ----------------------------      
# MAGIC Jaideep Mankala                    2018-04-04              5828                          Original Programming                                                       IntegrateDev2                  Kalyan Neelam          2018-04-04
# MAGIC Kaushik Kapoor                      2018-06-06              5828                       Adding new Member match steps for handling Babyboy     
# MAGIC                                                                                                               scenario                                                                              IntegrateDev2\(9)Abhiram Dasarathy\(9)    2018-06-13


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    CharType
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/PBMClaimsStep6MemMatch
# COMMAND ----------

# Parameters
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCdErr = get_widget_value('SrcSysCdErr','')
RunID = get_widget_value('RunID','')
RunCycle = get_widget_value('RunCycle','')
RunCycleDate = get_widget_value('RunCycleDate','')
Logging = get_widget_value('Logging','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCd1 = get_widget_value('SrcSysCd1','')
CurrDate = get_widget_value('CurrDate','')

# Read "ErrorFile2" (CSeqFileStage)
schema_ErrorFile2 = StructType([
    StructField("CLM_ID", StringType(), nullable=False)
])
df_ErrorFile2 = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_ErrorFile2)
    .load(f"{adls_path_publish}/external/{SrcSysCd1}_MbrMatch_RejectRecs.dat")
)

# Read "GRP_ID" (DB2Connector, Database=IDS)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_GRP_ID = (
    f"SELECT DISTINCT PBM_GRP_ID, GRP_ID "
    f"FROM {IDSOwner}.P_PBM_GRP_XREF XREF "
    f"WHERE UPPER(XREF.SRC_SYS_CD) = 'LDI' "
    f"AND '{CurrDate}' BETWEEN DATE(XREF.EFF_DT) AND DATE(XREF.TERM_DT)"
)
df_GRP_ID = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_GRP_ID)
    .load()
)

# "hf_ldi_medclm_preproc_grpsklkup" (CHashedFileStage) - Scenario A deduplicate by key PBM_GRP_ID
df_hf_ldi_medclm_preproc_grpsklkup = df_GRP_ID.dropDuplicates(["PBM_GRP_ID"])

# Read "IDS_CLM" (DB2Connector, Database=IDS)
extract_query_IDS_CLM = (
    f"SELECT CLM.CLM_SK "
    f"FROM {IDSOwner}.CLM AS CLM, {IDSOwner}.CD_MPPNG AS CD "
    f"WHERE CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK "
    f"AND CD.TRGT_CD = '{SrcSysCd}' "
    f"AND CLM.GRP_SK = 0"
)
df_IDS_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_CLM)
    .load()
)

# "hf_ldi_rx_ids_clm_recyc" (CHashedFileStage) - Scenario A deduplicate by key CLM_SK
df_hf_ldi_rx_ids_clm_recyc = df_IDS_CLM.dropDuplicates(["CLM_SK"])

# Read "EDW_CLM_F" (DB2Connector, Database=EDW)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
extract_query_EDW_CLM_F = (
    f"SELECT CLM_SK "
    f"FROM {EDWOwner}.CLM_F "
    f"WHERE SRC_SYS_CD = '{SrcSysCd}' "
    f"AND GRP_SK = 0"
)
df_EDW_CLM_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_EDW_CLM_F)
    .load()
)

# "hf_ldi_rx_edw_clm_recyc" (CHashedFileStage) - Scenario A deduplicate by key CLM_SK
df_hf_ldi_rx_edw_clm_recyc = df_EDW_CLM_F.dropDuplicates(["CLM_SK"])

# Read "IDS_MBR" (DB2Connector, Database=IDS)
extract_query_IDS_MBR = (
    f"SELECT DISTINCT "
    f"      MEMBER.MBR_UNIQ_KEY, "
    f"      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MEMBER.FIRST_NM,'-',''),'&',''),'.',''),' ',''),'''',''),'\"',''),',','') as MBR_FIRST_NM, "
    f"      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MEMBER.LAST_NM,'-',''),'&',''),'.',''),' ',''),'''',''),'\"',''),',','') as MBR_LAST_NM, "
    f"      MEMBER.SSN as MBR_SSN, "
    f"      MEMBER.BRTH_DT_SK as MBR_BRTH_DT_SK, "
    f"      MEMBER.GNDR_CD, "
    f"      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(SUBSCRIBER.FIRST_NM,'-',''),'&',''),'.',''),' ',''),'''',''),'\"',''),',','') as SUB_FIRST_NM, "
    f"      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(SUBSCRIBER.LAST_NM,'-',''),'&',''),'.',''),' ',''),'''',''),'\"',''),',','') as SUB_LAST_NM, "
    f"      SUBSCRIBER.SSN as SUB_SSN, "
    f"      SUBSCRIBER.BRTH_DT_SK as SUB_BRTH_DT_SK, "
    f"      MEMBER.GRP_ID, "
    f"      MEMBER.SUB_ID, "
    f"      MEMBER.MBR_SFX_NO, "
    f"      MEMBER.PBM_GRP_ID "
    f"FROM "
    f"(SELECT "
    f"      MBR.MBR_UNIQ_KEY, "
    f"      MBR.FIRST_NM, "
    f"      MBR.LAST_NM, "
    f"      MBR.SSN, "
    f"      MBR.BRTH_DT_SK, "
    f"      CD.TRGT_CD as GNDR_CD, "
    f"      MBR.SUB_SK, "
    f"      GRP.GRP_ID, "
    f"      SUB.SUB_ID, "
    f"      MBR.MBR_SFX_NO, "
    f"      XREF.PBM_GRP_ID "
    f"FROM "
    f"          {IDSOwner}.MBR MBR, "
    f"          {IDSOwner}.SUB SUB, "
    f"          {IDSOwner}.GRP GRP, "
    f"          {IDSOwner}.CD_MPPNG CD, "
    f"          {IDSOwner}.MBR_ENR ENR, "
    f"          {IDSOwner}.CD_MPPNG CD1, "
    f"          {IDSOwner}.P_PBM_GRP_XREF XREF "
    f"WHERE "
    f"             MBR.MBR_SK = ENR.MBR_SK AND "
    f"             MBR.SUB_SK = SUB.SUB_SK AND "
    f"             SUB.GRP_SK = GRP.GRP_SK AND "
    f"             MBR.MBR_GNDR_CD_SK = CD.CD_MPPNG_SK AND "
    f"             ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND "
    f"             GRP.GRP_ID = XREF.GRP_ID AND "
    f"             UPPER(XREF.SRC_SYS_CD) = 'LDI' AND "
    f"             '{CurrDate}' BETWEEN DATE(XREF.EFF_DT) AND DATE(XREF.TERM_DT) AND "
    f"             CD1.TRGT_CD in ('MED','MED1') AND "
    f"             UPPER(MBR.LAST_NM) NOT LIKE '%DO NOT USE%') MEMBER "
    f"LEFT OUTER JOIN "
    f"(SELECT "
    f"      MBR1.MBR_UNIQ_KEY, "
    f"      MBR1.FIRST_NM, "
    f"      MBR1.LAST_NM, "
    f"      MBR1.SSN, "
    f"      MBR1.BRTH_DT_SK, "
    f"      MBR1.SUB_SK "
    f"FROM "
    f"          {IDSOwner}.MBR MBR1, "
    f"          {IDSOwner}.CD_MPPNG CD1 "
    f"WHERE "
    f"            MBR1.MBR_RELSHP_CD_SK = CD1.CD_MPPNG_SK AND "
    f"            CD1.TRGT_CD = 'SUB' AND "
    f"            UPPER(MBR1.LAST_NM) NOT LIKE '%DO NOT USE%') SUBSCRIBER "
    f"ON MEMBER.SUB_SK = SUBSCRIBER.SUB_SK"
)
df_IDS_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_MBR)
    .load()
)

# "Mem_Match" (CTransformerStage) => 6 outputs: Step1, Step2, Step3, Step4, Step5, Step6
# Prepare the base DataFrame for transformation referencing columns with aliases

df_Mem_Match_base = df_IDS_MBR

# Step1
df_Mem_Match_Step1 = (
    df_Mem_Match_base
    .withColumn("MBR_SSN", F.col("MBR_SSN"))
    .withColumn("SUB_SSN", F.col("SUB_SSN"))
    .withColumn("MBR_BRTH_DT_SK", F.col("MBR_BRTH_DT_SK"))
    .withColumn("MBR_LAST_NM", F.upper(F.trim(F.col("MBR_LAST_NM"))).substr(F.lit(1), F.lit(4)))
    .withColumn("MBR_FIRST_NM", F.upper(F.trim(F.col("MBR_FIRST_NM"))).substr(F.lit(1), F.lit(4)))
    .withColumn("GNDR_CD", F.upper(F.trim(F.col("GNDR_CD"))))
    .withColumn("GRP_ID", F.trim(F.col("GRP_ID")))
    .withColumn("MBR_UNIQ_KEY", F.col("MBR_UNIQ_KEY"))
    .withColumn("SUB_FIRST_NM", F.upper(F.trim(F.col("SUB_FIRST_NM"))))
    .withColumn("SUB_LAST_NM", F.upper(F.trim(F.col("SUB_LAST_NM"))))
    .withColumn("SUB_BRTH_DT_SK", F.col("SUB_BRTH_DT_SK"))
    .withColumn("SUB_ID", F.col("SUB_ID"))
    .withColumn("MBR_SFX_NO", F.col("MBR_SFX_NO"))
    .select(
        "MBR_SSN", "SUB_SSN", "MBR_BRTH_DT_SK", "MBR_LAST_NM", "MBR_FIRST_NM",
        "GNDR_CD", "GRP_ID", "MBR_UNIQ_KEY", "SUB_FIRST_NM", "SUB_LAST_NM",
        "SUB_BRTH_DT_SK", "SUB_ID", "MBR_SFX_NO"
    )
)

# Step2
df_Mem_Match_Step2_base = df_Mem_Match_base
df_Mem_Match_Step2 = (
    df_Mem_Match_Step2_base
    .withColumn("MBR_SSN", F.col("MBR_SSN"))
    .withColumn("SUB_SSN", F.col("SUB_SSN"))
    .withColumn("MBR_BRTH_DT_SK", F.col("MBR_BRTH_DT_SK"))
    .withColumn("GRP_ID", F.trim(F.col("GRP_ID")))
    .withColumn("MBR_FIRST_NM", F.upper(F.trim(F.col("MBR_FIRST_NM"))))
    .withColumn("MBR_LAST_NM", F.upper(F.trim(F.col("MBR_LAST_NM"))))
    .withColumn("GNDR_CD", F.upper(F.trim(F.col("GNDR_CD"))))
    .withColumn("SUB_FIRST_NM", F.upper(F.trim(F.col("SUB_FIRST_NM"))))
    .withColumn("SUB_LAST_NM", F.upper(F.trim(F.col("SUB_LAST_NM"))))
    .withColumn("MBR_UNIQ_KEY", F.col("MBR_UNIQ_KEY"))
    .withColumn("SUB_BRTH_DT_SK", F.col("SUB_BRTH_DT_SK"))
    .withColumn("SUB_ID", F.col("SUB_ID"))
    .withColumn("MBR_SFX_NO", F.col("MBR_SFX_NO"))
    .select(
        "MBR_SSN", "SUB_SSN", "MBR_BRTH_DT_SK", "GRP_ID", "MBR_FIRST_NM",
        "MBR_LAST_NM", "GNDR_CD", "SUB_FIRST_NM", "SUB_LAST_NM", "MBR_UNIQ_KEY",
        "SUB_BRTH_DT_SK", "SUB_ID", "MBR_SFX_NO"
    )
)

# Step3
df_Mem_Match_Step3_base = df_Mem_Match_base
df_Mem_Match_Step3 = (
    df_Mem_Match_Step3_base
    .withColumn("MBR_FIRST_NM", F.upper(F.trim(F.col("MBR_FIRST_NM"))).substr(F.lit(1), F.lit(4)))
    .withColumn("MBR_LAST_NM", F.upper(F.trim(F.col("MBR_LAST_NM"))).substr(F.lit(1), F.lit(4)))
    .withColumn("MBR_BRTH_DT_SK", F.col("MBR_BRTH_DT_SK"))
    .withColumn("SUB_SSN", F.col("SUB_SSN"))
    .withColumn("GNDR_CD", F.upper(F.trim(F.col("GNDR_CD"))))
    .withColumn("GRP_ID", F.trim(F.col("GRP_ID")))
    .withColumn("SUB_FIRST_NM", F.upper(F.trim(F.col("SUB_FIRST_NM"))))
    .withColumn("SUB_LAST_NM", F.upper(F.trim(F.col("SUB_LAST_NM"))))
    .withColumn("MBR_UNIQ_KEY", F.col("MBR_UNIQ_KEY"))
    .withColumn("MBR_SSN", F.col("MBR_SSN"))
    .withColumn("SUB_BRTH_DT_SK", F.col("SUB_BRTH_DT_SK"))
    .withColumn("SUB_ID", F.col("SUB_ID"))
    .withColumn("MBR_SFX_NO", F.col("MBR_SFX_NO"))
    .select(
        "MBR_FIRST_NM", "MBR_LAST_NM", "MBR_BRTH_DT_SK", "SUB_SSN", "GNDR_CD",
        "GRP_ID", "SUB_FIRST_NM", "SUB_LAST_NM", "MBR_UNIQ_KEY", "MBR_SSN",
        "SUB_BRTH_DT_SK", "SUB_ID", "MBR_SFX_NO"
    )
)

# Step4
df_Mem_Match_Step4_base = df_Mem_Match_base
df_Mem_Match_Step4 = (
    df_Mem_Match_Step4_base
    .withColumn("MBR_FIRST_NM", F.upper(F.trim(F.col("MBR_FIRST_NM"))).substr(F.lit(1), F.lit(4)))
    .withColumn("MBR_LAST_NM", F.upper(F.trim(F.col("MBR_LAST_NM"))).substr(F.lit(1), F.lit(4)))
    .withColumn("SUB_FIRST_NM", F.upper(F.trim(F.col("SUB_FIRST_NM"))).substr(F.lit(1), F.lit(4)))
    .withColumn("SUB_LAST_NM", F.upper(F.trim(F.col("SUB_LAST_NM"))).substr(F.lit(1), F.lit(4)))
    .withColumn("MBR_BRTH_DT_SK", F.col("MBR_BRTH_DT_SK"))
    .withColumn("GNDR_CD", F.upper(F.trim(F.col("GNDR_CD"))))
    .withColumn("GRP_ID", F.trim(F.col("GRP_ID")))
    .withColumn("SUB_SSN", F.col("SUB_SSN"))
    .withColumn("MBR_UNIQ_KEY", F.col("MBR_UNIQ_KEY"))
    .withColumn("MBR_SSN", F.col("MBR_SSN"))
    .withColumn("SUB_BRTH_DT_SK", F.col("SUB_BRTH_DT_SK"))
    .withColumn("SUB_ID", F.col("SUB_ID"))
    .withColumn("MBR_SFX_NO", F.col("MBR_SFX_NO"))
    .select(
        "MBR_FIRST_NM", "MBR_LAST_NM", "SUB_FIRST_NM", "SUB_LAST_NM",
        "MBR_BRTH_DT_SK", "GNDR_CD", "GRP_ID", "SUB_SSN", "MBR_UNIQ_KEY",
        "MBR_SSN", "SUB_BRTH_DT_SK", "SUB_ID", "MBR_SFX_NO"
    )
)

# Step5 (constraint => MBR_SSN <> SUB_SSN)
df_Mem_Match_Step5 = (
    df_Mem_Match_base
    .filter(F.col("MBR_SSN") != F.col("SUB_SSN"))
    .withColumn("MBR_SSN", F.col("MBR_SSN"))
    .withColumn("SUB_SSN", F.col("SUB_SSN"))
    .withColumn("GRP_ID", F.trim(F.col("GRP_ID")))
    .withColumn("MBR_FIRST_NM", F.upper(F.trim(F.col("MBR_FIRST_NM"))))
    .withColumn("MBR_LAST_NM", F.upper(F.trim(F.col("MBR_LAST_NM"))))
    .withColumn("SUB_FIRST_NM", F.upper(F.trim(F.col("SUB_FIRST_NM"))))
    .withColumn("SUB_LAST_NM", F.upper(F.trim(F.col("SUB_LAST_NM"))))
    .withColumn("MBR_BRTH_DT_SK", F.col("MBR_BRTH_DT_SK"))
    .withColumn("GNDR_CD", F.upper(F.trim(F.col("GNDR_CD"))))
    .withColumn("MBR_UNIQ_KEY", F.col("MBR_UNIQ_KEY"))
    .withColumn("SUB_BRTH_DT_SK", F.col("SUB_BRTH_DT_SK"))
    .withColumn("SUB_ID", F.col("SUB_ID"))
    .withColumn("MBR_SFX_NO", F.col("MBR_SFX_NO"))
    .select(
        "MBR_SSN","SUB_SSN","GRP_ID","MBR_FIRST_NM","MBR_LAST_NM",
        "SUB_FIRST_NM","SUB_LAST_NM","MBR_BRTH_DT_SK","GNDR_CD",
        "MBR_UNIQ_KEY","SUB_BRTH_DT_SK","SUB_ID","MBR_SFX_NO"
    )
)

# Step6
df_Mem_Match_Step6 = (
    df_Mem_Match_base
    .withColumn("SUB_SSN", F.col("SUB_SSN"))
    .withColumn("GRP_ID", F.trim(F.col("GRP_ID")))
    .withColumn("MBR_BRTH_DT_SK", F.col("MBR_BRTH_DT_SK"))
    .withColumn("GNDR_CD", F.upper(F.trim(F.col("GNDR_CD"))))
    .withColumn("MBR_FIRST_NM", F.upper(F.trim(F.col("MBR_FIRST_NM"))))
    .withColumn("MBR_LAST_NM", F.upper(F.trim(F.col("MBR_LAST_NM"))))
    .withColumn("SUB_FIRST_NM", F.upper(F.trim(F.col("SUB_FIRST_NM"))))
    .withColumn("SUB_LAST_NM", F.upper(F.trim(F.col("SUB_LAST_NM"))))
    .withColumn("MBR_UNIQ_KEY", F.col("MBR_UNIQ_KEY"))
    .withColumn("MBR_SSN", F.col("MBR_SSN"))
    .withColumn("SUB_BRTH_DT_SK", F.col("SUB_BRTH_DT_SK"))
    .withColumn("SUB_ID", F.col("SUB_ID"))
    .withColumn("MBR_SFX_NO", F.col("MBR_SFX_NO"))
    .withColumn("PBM_GRP_ID", F.col("PBM_GRP_ID"))
    .select(
        "SUB_SSN","GRP_ID","MBR_BRTH_DT_SK","GNDR_CD","MBR_FIRST_NM",
        "MBR_LAST_NM","SUB_FIRST_NM","SUB_LAST_NM","MBR_UNIQ_KEY",
        "MBR_SSN","SUB_BRTH_DT_SK","SUB_ID","MBR_SFX_NO","PBM_GRP_ID"
    )
)

# "hf_LDI_clm_preproc_mssncssn" (CHashedFileStage) - Scenario A deduplicate by (MBR_SSN, SUB_SSN, GRP_ID)
df_hf_LDI_clm_preproc_mssncssn = df_Mem_Match_Step5.dropDuplicates(["MBR_SSN","SUB_SSN","GRP_ID"])

# "hf_LDI_clm_preproc_mssncssndoblname" - Scenario A deduplicate by:
# keys => MBR_SSN, SUB_SSN, MBR_BRTH_DT_SK, MBR_LAST_NM, MBR_FIRST_NM, GNDR_CD, GRP_ID
df_hf_LDI_clm_preproc_mssncssndoblname = df_Mem_Match_Step1.dropDuplicates([
    "MBR_SSN","SUB_SSN","MBR_BRTH_DT_SK","MBR_LAST_NM","MBR_FIRST_NM","GNDR_CD","GRP_ID"
])

# "hf_LDI_clm_preproc_mfnmmlnmcfnmclnmdobgen" - Scenario A deduplicate by:
# MBR_FIRST_NM, MBR_LAST_NM, SUB_FIRST_NM, SUB_LAST_NM, MBR_BRTH_DT_SK, GNDR_CD, GRP_ID
df_hf_LDI_clm_preproc_mfnmmlnmcfnmclnmdobgen = df_Mem_Match_Step4.dropDuplicates([
    "MBR_FIRST_NM","MBR_LAST_NM","SUB_FIRST_NM","SUB_LAST_NM","MBR_BRTH_DT_SK","GNDR_CD","GRP_ID"
])

# "hf_LDI_clm_preproc_mssncssndob" - Scenario A deduplicate by (MBR_SSN, SUB_SSN, MBR_BRTH_DT_SK, GRP_ID)
df_hf_LDI_clm_preproc_mssncssndob = df_Mem_Match_Step2.dropDuplicates([
    "MBR_SSN","SUB_SSN","MBR_BRTH_DT_SK","GRP_ID"
])

# "hf_LDI_clm_preproc_fnamelnamedobcssngen" - Scenario A deduplicate by:
# MBR_FIRST_NM, MBR_LAST_NM, MBR_BRTH_DT_SK, SUB_SSN, GNDR_CD, GRP_ID
df_hf_LDI_clm_preproc_fnamelnamedobcssngen = df_Mem_Match_Step3.dropDuplicates([
    "MBR_FIRST_NM","MBR_LAST_NM","MBR_BRTH_DT_SK","SUB_SSN","GNDR_CD","GRP_ID"
])

# "PBMClaimsStep6MemMatch" (CContainerStage)
params_PBMClaimsStep6MemMatch = {}
df_PBMClaimsStep6MemMatch = PBMClaimsStep6MemMatch(df_Mem_Match_Step6, params_PBMClaimsStep6MemMatch)

# Read "IDS_P_CLM_ERR" (DB2Connector, Database=IDS)
extract_query_IDS_P_CLM_ERR = (
    f"SELECT "
    f"CLM_SK, CLM_ID, SRC_SYS_CD, CLM_TYP_CD, CLM_SUBTYP_CD, CLM_SVC_STRT_DT_SK, "
    f"SRC_SYS_GRP_PFX, SRC_SYS_GRP_ID, SRC_SYS_GRP_SFX, SUB_SSN, PATN_LAST_NM, PATN_FIRST_NM, "
    f"PATN_GNDR_CD, PATN_BRTH_DT_SK, ERR_CD, ERR_DESC, FEP_MBR_ID, SUB_FIRST_NM, SUB_LAST_NM, "
    f"SRC_SYS_SUB_ID, SRC_SYS_MBR_SFX_NO, GRP_ID, FILE_DT_SK, PATN_SSN "
    f"FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC "
    f"WHERE SRC_SYS_CD = '{SrcSysCdErr}'"
)
df_IDS_P_CLM_ERR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_P_CLM_ERR)
    .load()
)

# "hf_ldi_ids_p_clm_err" (CHashedFileStage) - Scenario A deduplicate by "CLM_SK"
df_hf_ldi_ids_p_clm_err = df_IDS_P_CLM_ERR.dropDuplicates(["CLM_SK"])

# "Trn_Mbr_Mtch" (CTransformerStage) => 2 outputs: Next, Lnk_Err_Clm_To_Hf
df_Trn_Mbr_Mtch_base = df_hf_ldi_ids_p_clm_err
df_Trn_Mbr_Mtch_Next = (
    df_Trn_Mbr_Mtch_base
    .withColumn("CLM_ID", F.col("CLM_ID"))
    .withColumn("SUB_SSN", F.col("SUB_SSN"))
    .withColumn("PATN_LAST_NM", F.col("PATN_LAST_NM"))
    .withColumn("PATN_FIRST_NM", F.col("PATN_FIRST_NM"))
    .withColumn("SUB_FIRST_NM", F.col("SUB_FIRST_NM"))
    .withColumn("SUB_LAST_NM", F.col("SUB_LAST_NM"))
    .withColumn("PATN_DOB", F.col("PATN_BRTH_DT_SK"))
    .withColumn("PATN_SEX", F.trim(F.col("PATN_GNDR_CD")))
    .withColumn("FILL_DT_SK", F.col("CLM_SVC_STRT_DT_SK"))
    .withColumn("PATN_SSN", F.col("PATN_SSN"))
    .withColumn("SRC_SYS_GRP_ID", F.col("SRC_SYS_GRP_ID"))
    .select(
        "CLM_ID","SUB_SSN","PATN_LAST_NM","PATN_FIRST_NM","SUB_FIRST_NM",
        "SUB_LAST_NM","PATN_DOB","PATN_SEX","FILL_DT_SK","PATN_SSN","SRC_SYS_GRP_ID"
    )
)

df_Trn_Mbr_Mtch_ErrClm = (
    df_Trn_Mbr_Mtch_base
    .withColumn("CLM_SK", F.col("CLM_SK"))
    .withColumn("CLM_ID", F.col("CLM_ID"))
    .withColumn("SRC_SYS_CD", F.col("SRC_SYS_CD"))
    .withColumn("CLM_TYP_CD", F.col("CLM_TYP_CD"))
    .withColumn("CLM_SUBTYP_CD", F.col("CLM_SUBTYP_CD"))
    .withColumn("CLM_SVC_STRT_DT_SK", F.col("CLM_SVC_STRT_DT_SK"))
    .withColumn("SRC_SYS_GRP_PFX", F.col("SRC_SYS_GRP_PFX"))
    .withColumn("SRC_SYS_GRP_ID", F.col("SRC_SYS_GRP_ID"))
    .withColumn("SRC_SYS_GRP_SFX", F.col("SRC_SYS_GRP_SFX"))
    .withColumn("SUB_SSN", F.col("SUB_SSN"))
    .withColumn("PATN_LAST_NM", F.col("PATN_LAST_NM"))
    .withColumn("PATN_FIRST_NM", F.col("PATN_FIRST_NM"))
    .withColumn("PATN_GNDR_CD", F.col("PATN_GNDR_CD"))
    .withColumn("PATN_BRTH_DT_SK", F.col("PATN_BRTH_DT_SK"))
    .withColumn("ERR_CD", F.col("ERR_CD"))
    .withColumn("ERR_DESC", F.col("ERR_DESC"))
    .withColumn("FEP_MBR_ID", F.col("FEP_MBR_ID"))
    .withColumn("SUB_FIRST_NM", F.col("SUB_FIRST_NM"))
    .withColumn("SUB_LAST_NM", F.col("SUB_LAST_NM"))
    .withColumn("SRC_SYS_SUB_ID", F.col("SRC_SYS_SUB_ID"))
    .withColumn("SRC_SYS_MBR_SFX_NO", F.col("SRC_SYS_MBR_SFX_NO"))
    .withColumn("GRP_ID", F.col("GRP_ID"))
    .withColumn("FILE_DT_SK", F.col("FILE_DT_SK"))
    .withColumn("PATN_SSN", F.col("PATN_SSN"))
    .select(
        "CLM_SK","CLM_ID","SRC_SYS_CD","CLM_TYP_CD","CLM_SUBTYP_CD","CLM_SVC_STRT_DT_SK",
        "SRC_SYS_GRP_PFX","SRC_SYS_GRP_ID","SRC_SYS_GRP_SFX","SUB_SSN","PATN_LAST_NM",
        "PATN_FIRST_NM","PATN_GNDR_CD","PATN_BRTH_DT_SK","ERR_CD","ERR_DESC","FEP_MBR_ID",
        "SUB_FIRST_NM","SUB_LAST_NM","SRC_SYS_SUB_ID","SRC_SYS_MBR_SFX_NO","GRP_ID",
        "FILE_DT_SK","PATN_SSN"
    )
)

# "hf_ldi_errclm_mbrmatch_land" - Scenario A deduplicate by CLM_SK
df_hf_ldi_errclm_mbrmatch_land = df_Trn_Mbr_Mtch_ErrClm.dropDuplicates(["CLM_SK"])

# "Convert" (CTransformerStage) => 1 output: LDI
df_Convert_base = df_Trn_Mbr_Mtch_Next
df_Convert = (
    df_Convert_base
    .withColumn("svPatnFrstNm", F.trim(F.regexp_replace(F.col("PATN_FIRST_NM"), r"[-&\.\ '\",]", " ")))
    .withColumn("svPatnLastNm", F.trim(F.regexp_replace(F.col("PATN_LAST_NM"), r"[-&\.\ '\",]", " ")))
    .withColumn("svSubFrstNm", F.trim(F.regexp_replace(F.col("SUB_FIRST_NM"), r"[-&\.\ '\",]", " ")))
    .withColumn("svSubLastNm", F.trim(F.regexp_replace(F.col("SUB_LAST_NM"), r"[-&\.\ '\",]", " ")))
    .withColumn("PATN_FIRST_NM", F.upper(F.trim(F.col("svPatnFrstNm"))))
    .withColumn("PATN_LAST_NM", F.upper(F.trim(F.col("svPatnLastNm"))))
    .withColumn("BRTH_DT", F.trim(F.col("PATN_DOB")))
    .withColumn("SEX_CD", F.upper(F.trim(F.col("PATN_SEX"))))
    .withColumn("CARDHLDR_FIRST_NM", F.upper(F.trim(F.col("svSubFrstNm"))))
    .withColumn("CARDHLDR_LAST_NM", F.upper(F.trim(F.col("svSubLastNm"))))
    .withColumn("PATN_SSN", F.trim(F.col("PATN_SSN")))
    .withColumn("CARDHLDR_SSN", F.trim(F.col("SUB_SSN")))
    .withColumn("CLM_ID", F.trim(F.col("CLM_ID")))
    .withColumn("FILL_DT_SK", F.col("FILL_DT_SK"))
    .withColumn("GRP_NO", F.trim(F.col("SRC_SYS_GRP_ID")))
    .select(
        "PATN_FIRST_NM","PATN_LAST_NM","BRTH_DT","SEX_CD","CARDHLDR_FIRST_NM",
        "CARDHLDR_LAST_NM","PATN_SSN","CARDHLDR_SSN","CLM_ID","FILL_DT_SK","GRP_NO"
    )
)

# "MemberMatch" (CTransformerStage) => multiple links: Reject, Land
# Also multiple lookups: Step1_lkp, Step2_lkp, Step3_lkp, Step4_lkp, Step5_lkp, Step6_lkp
# We must join them as left joins. Then define the stage variables, then filter out the rows for constraints.

# Prepare the "LDI" primary link DataFrame
df_MemberMatch_primary = df_Convert

# Step1_lkp => df_hf_LDI_clm_preproc_mssncssndoblname
cond_step1 = [
    F.trim(F.upper(F.col("PATN_SSN"))) == F.col("Step1_lkp.MBR_SSN"),
    F.trim(F.upper(F.col("CARDHLDR_SSN"))) == F.col("Step1_lkp.SUB_SSN"),
    F.col("BRTH_DT") == F.col("Step1_lkp.MBR_BRTH_DT_SK"),
    F.upper(F.trim(F.col("PATN_LAST_NM"))).substr(F.lit(1),F.lit(4)) == F.col("Step1_lkp.MBR_LAST_NM"),
    F.upper(F.trim(F.col("PATN_FIRST_NM"))).substr(F.lit(1),F.lit(4)) == F.col("Step1_lkp.MBR_FIRST_NM"),
    F.upper(F.trim(F.col("SEX_CD"))) == F.col("Step1_lkp.GNDR_CD"),
    F.trim(F.col("GRP_NO")) == F.col("Step1_lkp.GRP_ID")
]
df_MemberMatch_joined_1 = df_MemberMatch_primary.alias("LDI").join(
    df_hf_LDI_clm_preproc_mssncssndoblname.alias("Step1_lkp"),
    cond_step1,
    how="left"
)

# Step2_lkp => df_hf_LDI_clm_preproc_mssncssndob
cond_step2 = [
    F.trim(F.upper(F.col("LDI.PATN_SSN"))) == F.col("Step2_lkp.MBR_SSN"),
    F.trim(F.upper(F.col("LDI.CARDHLDR_SSN"))) == F.col("Step2_lkp.SUB_SSN"),
    F.col("LDI.BRTH_DT") == F.col("Step2_lkp.MBR_BRTH_DT_SK"),
    F.trim(F.col("LDI.GRP_NO")) == F.col("Step2_lkp.GRP_ID")
]
df_MemberMatch_joined_2 = df_MemberMatch_joined_1.join(
    df_hf_LDI_clm_preproc_mssncssndob.alias("Step2_lkp"),
    cond_step2,
    how="left"
)

# Step3_lkp => df_hf_LDI_clm_preproc_fnamelnamedobcssngen
cond_step3 = [
    F.upper(F.trim(F.col("LDI.PATN_FIRST_NM"))).substr(F.lit(1),F.lit(4)) == F.col("Step3_lkp.MBR_FIRST_NM"),
    F.upper(F.trim(F.col("LDI.PATN_LAST_NM"))).substr(F.lit(1),F.lit(4)) == F.col("Step3_lkp.MBR_LAST_NM"),
    F.col("LDI.BRTH_DT") == F.col("Step3_lkp.MBR_BRTH_DT_SK"),
    F.trim(F.upper(F.col("LDI.CARDHLDR_SSN"))) == F.col("Step3_lkp.SUB_SSN"),
    F.upper(F.trim(F.col("LDI.SEX_CD"))) == F.col("Step3_lkp.GNDR_CD"),
    F.trim(F.col("LDI.GRP_NO")) == F.col("Step3_lkp.GRP_ID")
]
df_MemberMatch_joined_3 = df_MemberMatch_joined_2.join(
    df_hf_LDI_clm_preproc_fnamelnamedobcssngen.alias("Step3_lkp"),
    cond_step3,
    how="left"
)

# Step4_lkp => df_hf_LDI_clm_preproc_mfnmmlnmcfnmclnmdobgen
cond_step4 = [
    F.upper(F.trim(F.col("LDI.PATN_FIRST_NM"))).substr(F.lit(1),F.lit(4)) == F.col("Step4_lkp.MBR_FIRST_NM"),
    F.upper(F.trim(F.col("LDI.PATN_LAST_NM"))).substr(F.lit(1),F.lit(4)) == F.col("Step4_lkp.MBR_LAST_NM"),
    F.upper(F.trim(F.col("LDI.CARDHLDR_FIRST_NM"))).substr(F.lit(1),F.lit(4)) == F.col("Step4_lkp.SUB_FIRST_NM"),
    F.upper(F.trim(F.col("LDI.CARDHLDR_LAST_NM"))).substr(F.lit(1),F.lit(4)) == F.col("Step4_lkp.SUB_LAST_NM"),
    F.col("LDI.BRTH_DT") == F.col("Step4_lkp.MBR_BRTH_DT_SK"),
    F.upper(F.trim(F.col("LDI.SEX_CD"))) == F.col("Step4_lkp.GNDR_CD"),
    F.trim(F.col("LDI.GRP_NO")) == F.col("Step4_lkp.GRP_ID")
]
df_MemberMatch_joined_4 = df_MemberMatch_joined_3.join(
    df_hf_LDI_clm_preproc_mfnmmlnmcfnmclnmdobgen.alias("Step4_lkp"),
    cond_step4,
    how="left"
)

# Step5_lkp => df_hf_LDI_clm_preproc_mssncssn
cond_step5 = [
    F.trim(F.upper(F.col("LDI.PATN_SSN"))) == F.col("Step5_lkp.MBR_SSN"),
    F.trim(F.upper(F.col("LDI.CARDHLDR_SSN"))) == F.col("Step5_lkp.SUB_SSN"),
    F.trim(F.col("LDI.GRP_NO")) == F.col("Step5_lkp.GRP_ID")
]
df_MemberMatch_joined_5 = df_MemberMatch_joined_4.join(
    df_hf_LDI_clm_preproc_mssncssn.alias("Step5_lkp"),
    cond_step5,
    how="left"
)

# Step6_lkp => df_PBMClaimsStep6MemMatch
cond_step6 = [
    F.trim(F.col("LDI.CARDHLDR_SSN")) == F.col("Step6_lkp.SUB_SSN"),
    F.trim(F.col("LDI.BRTH_DT")) == F.col("Step6_lkp.MBR_BRTH_DT_SK"),
    F.upper(F.trim(F.col("LDI.SEX_CD"))) == F.col("Step6_lkp.GNDR_CD")
]
df_MemberMatch_joined_6 = df_MemberMatch_joined_5.join(
    df_PBMClaimsStep6MemMatch.alias("Step6_lkp"),
    cond_step6,
    how="left"
)

# Stage variables:
df_MemberMatch_sv = (
    df_MemberMatch_joined_6
    .withColumn(
        "svMbrUniqKey",
        F.when(F.col("Step1_lkp.MBR_UNIQ_KEY").isNotNull(), F.col("Step1_lkp.MBR_UNIQ_KEY"))
         .when(F.col("Step2_lkp.MBR_UNIQ_KEY").isNotNull(), F.col("Step2_lkp.MBR_UNIQ_KEY"))
         .when(F.col("Step3_lkp.MBR_UNIQ_KEY").isNotNull(), F.col("Step3_lkp.MBR_UNIQ_KEY"))
         .when(F.col("Step4_lkp.MBR_UNIQ_KEY").isNotNull(), F.col("Step4_lkp.MBR_UNIQ_KEY"))
         .when(F.col("Step5_lkp.MBR_UNIQ_KEY").isNotNull(), F.col("Step5_lkp.MBR_UNIQ_KEY"))
         .when((F.col("Step6_lkp.MBR_UNIQ_KEY").isNotNull()) & (F.col("Step6_lkp.CNT") == F.lit(1)), F.col("Step6_lkp.MBR_UNIQ_KEY"))
         .otherwise(F.lit("UNK"))
    )
    .withColumn(
        "svSubId",
        F.when(F.col("Step1_lkp.SUB_ID").isNotNull(), F.col("Step1_lkp.SUB_ID"))
         .when(F.col("Step2_lkp.SUB_ID").isNotNull(), F.col("Step2_lkp.SUB_ID"))
         .when(F.col("Step3_lkp.SUB_ID").isNotNull(), F.col("Step3_lkp.SUB_ID"))
         .when(F.col("Step4_lkp.SUB_ID").isNotNull(), F.col("Step4_lkp.SUB_ID"))
         .when(F.col("Step5_lkp.SUB_ID").isNotNull(), F.col("Step5_lkp.SUB_ID"))
         .when(F.col("Step6_lkp.SUB_ID").isNotNull(), F.col("Step5_lkp.SUB_ID"))  # per job expression
         .otherwise(F.lit("UNK"))
    )
    .withColumn(
        "svGrpId",
        F.when(F.col("Step1_lkp.GRP_ID").isNotNull(), F.col("Step1_lkp.GRP_ID"))
         .when(F.col("Step2_lkp.GRP_ID").isNotNull(), F.col("Step2_lkp.GRP_ID"))
         .when(F.col("Step3_lkp.GRP_ID").isNotNull(), F.col("Step3_lkp.GRP_ID"))
         .when(F.col("Step4_lkp.GRP_ID").isNotNull(), F.col("Step4_lkp.GRP_ID"))
         .when(F.col("Step5_lkp.GRP_ID").isNotNull(), F.col("Step5_lkp.GRP_ID"))
         .when(F.col("Step6_lkp.GRP_ID").isNotNull(), F.col("Step5_lkp.GRP_ID"))
         .otherwise(F.lit("UNK"))
    )
)

df_MemberMatch_Reject = df_MemberMatch_sv.filter(F.col("svMbrUniqKey") == F.lit("UNK")).select(
    F.col("LDI.CLM_ID").alias("CLM_ID")
)

df_MemberMatch_Land = df_MemberMatch_sv.filter(F.col("svMbrUniqKey") != F.lit("UNK")).select(
    F.col("LDI.CLM_ID").alias("CLM_ID"),
    F.col("LDI.FILL_DT_SK").alias("FILL_DT_SK"),
    F.expr("CASE WHEN svMbrUniqKey = 'UNK' THEN 0 ELSE svMbrUniqKey END").alias("MBR_UNIQ_KEY")
)

# "Load_ErrorFile2" (CSeqFileStage, write mode "append", path in "external")
# We want to keep the same single column "CLM_ID" (varchar => rpad?). DataStage shows just one column. Let's do rpad for a default length, or 255 if unknown.
df_Load_ErrorFile2_final = df_MemberMatch_Reject.select(
    F.rpad(F.col("CLM_ID"), 255, " ").alias("CLM_ID")
)
write_files(
    df_Load_ErrorFile2_final,
    f"{adls_path_publish}/external/{SrcSysCd1}_MbrMatch_RejectRecs.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# "hf_ldi_mbrmatch_drugenrmatch_dedupe" - Scenario A deduplicate by (CLM_ID, FILL_DT_SK, MBR_UNIQ_KEY)
df_hf_ldi_mbrmatch_drugenrmatch_dedupe = df_MemberMatch_Land.dropDuplicates(["CLM_ID","FILL_DT_SK","MBR_UNIQ_KEY"])

# "W_DRUG_ENR_MATCH" (DB2Connector, Database=IDS) => must do a merge upsert
df_W_DRUG_ENR = df_hf_ldi_mbrmatch_drugenrmatch_dedupe.select("CLM_ID","FILL_DT_SK","MBR_UNIQ_KEY")

temp_table_W_DRUG_ENR = "STAGING.LDIPClmMbrErrExtr_W_DRUG_ENR_MATCH_temp"
spark.sql(f"DROP TABLE IF EXISTS {temp_table_W_DRUG_ENR}")

(
    df_W_DRUG_ENR
    .write
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", temp_table_W_DRUG_ENR)
    .mode("overwrite")
    .save()
)

merge_sql_W_DRUG_ENR = (
    f"MERGE {IDSOwner}.W_DRUG_ENR_MATCH AS T "
    f"USING {temp_table_W_DRUG_ENR} AS S "
    f"ON T.CLM_ID = S.CLM_ID AND T.FILL_DT_SK = S.FILL_DT_SK AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY "
    f"WHEN MATCHED THEN UPDATE SET "
    f"  T.FILL_DT_SK = S.FILL_DT_SK, "
    f"  T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY "
    f"WHEN NOT MATCHED THEN INSERT (CLM_ID,FILL_DT_SK,MBR_UNIQ_KEY) "
    f"VALUES (S.CLM_ID,S.FILL_DT_SK,S.MBR_UNIQ_KEY);"
)
execute_dml(merge_sql_W_DRUG_ENR, jdbc_url_ids, jdbc_props_ids)

# "hf_ldi_errclm_mbrmatch_mbrlkup" (CHashedFileStage) => deduplicate by "CLM_ID"
df_hf_ldi_errclm_mbrmatch_mbrlkup = df_W_DRUG_ENR.select(
    "CLM_ID"  # We'll join later. The stage columns: CLM_ID is PK
).dropDuplicates(["CLM_ID"]).join(
    df_W_DRUG_ENR, ["CLM_ID"], how="inner"
)
# Now add the extra columns from the original stage definition:
df_hf_ldi_errclm_mbrmatch_mbrlkup = df_hf_ldi_errclm_mbrmatch_mbrlkup.select(
    F.col("CLM_ID"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.lit("").alias("GRP_ID"),
    F.lit("").alias("SUB_ID"),
    F.lit(None).cast(CharType(2)).alias("MBR_SFX_NO"),
    F.lit("0").cast(IntegerType()).alias("SUB_UNIQ_KEY"),
    F.lit(None).cast(CharType(10)).alias("EFF_DT_SK")
)

# "Trn_MbrMtch_Lkup" transformer
#  Primary link => "hf_ldi_errclm_mbrmatch_land" => columns => the big set
#  Lookups => "hf_ldi_errclm_mbrmatch_mbrlkup" => "hf_ldi_medclm_preproc_grpsklkup" => "hf_ldi_rx_ids_clm_recyc" => "hf_ldi_rx_edw_clm_recyc"
df_Trn_MbrMtch_Lkup_primary = df_hf_ldi_errclm_mbrmatch_land.alias("Lnk_ErrClm")

df_Trn_MbrMtch_Lkup_lkpMbr = df_hf_ldi_errclm_mbrmatch_mbrlkup.alias("Lnk_Mbr_Lkup")
cond_mbrlkup = [F.col("Lnk_ErrClm.CLM_ID") == F.col("Lnk_Mbr_Lkup.CLM_ID")]
df_join_mbrlkup = df_Trn_MbrMtch_Lkup_primary.join(df_Trn_MbrMtch_Lkup_lkpMbr, cond_mbrlkup, how="left")

df_GrpBase_lkup = df_hf_ldi_medclm_preproc_grpsklkup.alias("GrpBase_lkup")
cond_grpbase = [F.col("Lnk_ErrClm.SRC_SYS_GRP_ID") == F.col("GrpBase_lkup.PBM_GRP_ID")]
df_join_grpbase = df_join_mbrlkup.join(df_GrpBase_lkup, cond_grpbase, how="left")

df_IDS_CLM_lkup = df_hf_ldi_rx_ids_clm_recyc.alias("IDS_CLM")
cond_idsclm = [F.col("Lnk_ErrClm.CLM_SK") == F.col("IDS_CLM.CLM_SK")]
df_join_idsclm = df_join_grpbase.join(df_IDS_CLM_lkup, cond_idsclm, how="left")

df_EDW_CLM_lkup = df_hf_ldi_rx_edw_clm_recyc.alias("EDW_CLM")
cond_edwclm = [F.col("Lnk_ErrClm.CLM_SK") == F.col("EDW_CLM.CLM_SK")]
df_join_edwclm = df_join_idsclm.join(df_EDW_CLM_lkup, cond_edwclm, how="left")

df_final_trn = (
    df_join_edwclm
    .withColumn("svMbrUniqKey", F.when(F.col("Lnk_Mbr_Lkup.MBR_UNIQ_KEY").isNull(), F.lit(0)).otherwise(F.col("Lnk_Mbr_Lkup.MBR_UNIQ_KEY")))
    .withColumn("svGrpId", F.when(F.col("GrpBase_lkup.GRP_ID").isNotNull(), F.col("GrpBase_lkup.GRP_ID"))
                .when(F.col("Lnk_Mbr_Lkup.GRP_ID").isNotNull(), F.col("Lnk_Mbr_Lkup.GRP_ID"))
                .otherwise(
                   F.when(F.col("Lnk_ErrClm.GRP_ID").isNotNull(), F.col("Lnk_Mbr_Lkup.GRP_ID"))
                   .otherwise(F.lit("UNK"))
                )
    )
    .withColumn("svSubId", F.when(F.col("Lnk_Mbr_Lkup.SUB_ID").isNull(), F.lit("0")).otherwise(F.col("Lnk_Mbr_Lkup.SUB_ID")))
    .withColumn("svMbrSfxNo", F.when(F.col("Lnk_Mbr_Lkup.MBR_SFX_NO").isNull(), F.lit(0)).otherwise(F.col("Lnk_Mbr_Lkup.MBR_SFX_NO")))
    .withColumn("svMbrEnrEffDt", F.when(F.col("Lnk_Mbr_Lkup.EFF_DT_SK").isNull(), F.lit("1753-01-01")).otherwise(F.col("Lnk_Mbr_Lkup.EFF_DT_SK")))
    .withColumn("svSubUniqKey", F.when(F.col("Lnk_Mbr_Lkup.SUB_UNIQ_KEY").isNotNull(), F.col("Lnk_Mbr_Lkup.SUB_UNIQ_KEY")).otherwise(F.lit(0)))
    .withColumn(
        "svSrcSysCd",
        F.when(F.col("Lnk_ErrClm.SRC_SYS_CD").isNull() | (F.length(F.trim(F.col("Lnk_ErrClm.SRC_SYS_CD"))) == 0), F.lit("UNK"))
         .when(F.trim(F.col("Lnk_ErrClm.SRC_SYS_CD")).isin(
            "PCT","WELLDYNERX","PCS","CAREMARK","ARGUS","EDC","OT@2","ADOL","MOHSAIC","CAREADVANCE",
            "ESI","OPTUMRX","MCSOURCE","MCAID","MEDTRAK","BCBSSC","BCA","BCBSA","SAVRX","LDI","EYEMED",
            "CVS","LUMERIS","MEDIMPACT"),
            F.lit("FACETS")
         )
         .otherwise(F.col("Lnk_ErrClm.SRC_SYS_CD"))
    )
)

# Outputs from "Trn_MbrMtch_Lkup":
df_W_DRUG_ENR = df_final_trn.filter(F.col("Lnk_Mbr_Lkup.MBR_UNIQ_KEY").isNotNull()).select(
    F.col("Lnk_ErrClm.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_ErrClm.CLM_SVC_STRT_DT_SK").alias("FILL_DT_SK"),
    F.col("Lnk_Mbr_Lkup.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

df_Trn_MbrMtch_Lkup_ErrorClm = df_final_trn.select(
    F.col("Lnk_ErrClm.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_ErrClm.CLM_SK").alias("CLM_SK"),
    F.col("Lnk_ErrClm.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_ErrClm.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("Lnk_ErrClm.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("Lnk_ErrClm.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("Lnk_ErrClm.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    F.col("Lnk_ErrClm.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("Lnk_ErrClm.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    F.col("Lnk_ErrClm.SUB_SSN").alias("SUB_SSN"),
    F.col("Lnk_ErrClm.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("Lnk_ErrClm.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("Lnk_ErrClm.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.col("Lnk_ErrClm.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    F.col("Lnk_ErrClm.ERR_CD").alias("ERR_CD"),
    F.col("Lnk_ErrClm.ERR_DESC").alias("ERR_DESC"),
    F.col("Lnk_ErrClm.FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("Lnk_ErrClm.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("Lnk_ErrClm.SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("Lnk_ErrClm.SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    F.col("Lnk_ErrClm.SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    F.expr("CASE WHEN GrpBase_lkup.GRP_ID IS NOT NULL THEN GrpBase_lkup.GRP_ID WHEN Lnk_Mbr_Lkup.GRP_ID IS NOT NULL THEN Lnk_Mbr_Lkup.GRP_ID WHEN Lnk_ErrClm.GRP_ID IS NOT NULL THEN Lnk_Mbr_Lkup.GRP_ID ELSE 'UNK' END").alias("GRP_ID"),
    F.col("Lnk_ErrClm.FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("Lnk_ErrClm.PATN_SSN").alias("PATN_SSN")
)

df_ids_grp_update = df_final_trn.filter(
    (F.length(F.trim(F.col("Lnk_ErrClm.GRP_ID"))) == 0) | (F.col("Lnk_ErrClm.GRP_ID").isNull())
).filter(
    F.col("GrpBase_lkup.GRP_ID").isNotNull()
).filter(
    F.col("IDS_CLM.CLM_SK").isNotNull()
).select(
    F.col("Lnk_ErrClm.CLM_SK").alias("CLM_SK"),
    F.col("RunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.expr("GetFkeyGrp(svSrcSysCd, Lnk_ErrClm.CLM_SK, svGrpId, Logging)").alias("GRP_SK")
)

df_edw_grp_update = df_final_trn.filter(
    (F.length(F.trim(F.col("Lnk_ErrClm.GRP_ID"))) == 0) | (F.col("Lnk_ErrClm.GRP_ID").isNull())
).filter(
    F.col("GrpBase_lkup.GRP_ID").isNotNull()
).filter(
    F.col("EDW_CLM.CLM_SK").isNotNull()
).select(
    F.col("Lnk_ErrClm.CLM_SK").alias("CLM_SK"),
    F.col("RunCycleDate").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("RunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.expr("GetFkeyGrp(svSrcSysCd, Lnk_ErrClm.CLM_SK, svGrpId, Logging)").alias("GRP_SK"),
    F.col("svGrpId").alias("GRP_ID")
)

df_Trn_MbrMtch_Lkup_Lnk_ErrClmLand = df_final_trn.filter(F.col("Lnk_Mbr_Lkup.MBR_UNIQ_KEY").isNotNull()).select(
    F.col("Lnk_ErrClm.CLM_SK").alias("CLM_SK"),
    F.col("Lnk_ErrClm.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_ErrClm.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_ErrClm.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("Lnk_ErrClm.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("Lnk_ErrClm.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("Lnk_ErrClm.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    F.col("Lnk_ErrClm.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("Lnk_ErrClm.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    F.col("Lnk_ErrClm.SUB_SSN").alias("SUB_SSN"),
    F.col("Lnk_ErrClm.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("Lnk_ErrClm.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("Lnk_ErrClm.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.col("Lnk_ErrClm.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    F.col("svMbrUniqKey").alias("MBR_UNIQ_KEY"),
    F.col("svGrpId").alias("GRP_ID"),
    F.col("svSubId").alias("SUB_ID"),
    F.col("svMbrSfxNo").alias("MBR_SFX_NO"),
    F.col("svSubUniqKey").alias("SUB_UNIQ_KEY"),
    F.col("svMbrEnrEffDt").alias("EFF_DT_SK")
)

# "MedtrakErrClmLand" (CSeqFileStage) => final write
df_MedtrakErrClmLand_final = df_Trn_MbrMtch_Lkup_Lnk_ErrClmLand.select(
    F.rpad(F.col("CLM_SK").cast(StringType()), 10, " ").alias("CLM_SK"),
    F.rpad(F.col("CLM_ID"), 255, " ").alias("CLM_ID"),
    F.rpad(F.col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_TYP_CD"), 255, " ").alias("CLM_TYP_CD"),
    F.rpad(F.col("CLM_SUBTYP_CD"), 255, " ").alias("CLM_SUBTYP_CD"),
    F.rpad(F.col("CLM_SVC_STRT_DT_SK"), 10, " ").alias("CLM_SVC_STRT_DT_SK"),
    F.rpad(F.col("SRC_SYS_GRP_PFX"), 255, " ").alias("SRC_SYS_GRP_PFX"),
    F.rpad(F.col("SRC_SYS_GRP_ID"), 255, " ").alias("SRC_SYS_GRP_ID"),
    F.rpad(F.col("SRC_SYS_GRP_SFX"), 255, " ").alias("SRC_SYS_GRP_SFX"),
    F.rpad(F.col("SUB_SSN"), 255, " ").alias("SUB_SSN"),
    F.rpad(F.col("PATN_LAST_NM"), 255, " ").alias("PATN_LAST_NM"),
    F.rpad(F.col("PATN_FIRST_NM"), 255, " ").alias("PATN_FIRST_NM"),
    F.rpad(F.col("PATN_GNDR_CD"), 255, " ").alias("PATN_GNDR_CD"),
    F.rpad(F.col("PATN_BRTH_DT_SK"), 10, " ").alias("PATN_BRTH_DT_SK"),
    F.rpad(F.col("MBR_UNIQ_KEY").cast(StringType()), 20, " ").alias("MBR_UNIQ_KEY"),
    F.rpad(F.col("GRP_ID"), 255, " ").alias("GRP_ID"),
    F.rpad(F.col("SUB_ID"), 255, " ").alias("SUB_ID"),
    F.rpad(F.col("MBR_SFX_NO").cast(StringType()), 2, " ").alias("MBR_SFX_NO"),
    F.rpad(F.col("SUB_UNIQ_KEY").cast(StringType()), 20, " ").alias("SUB_UNIQ_KEY"),
    F.rpad(F.col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK")
)
write_files(
    df_MedtrakErrClmLand_final,
    f"{adls_path}/verified/{SrcSysCd1}_ErrClm_Landing.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# "W_DRUG_ENR" (CSeqFileStage) => final write => from df_W_DRUG_ENR
df_W_DRUG_ENR_final = df_W_DRUG_ENR.select(
    F.rpad(F.col("CLM_ID"), 255, " ").alias("CLM_ID"),
    F.rpad(F.col("FILL_DT_SK"), 10, " ").alias("FILL_DT_SK"),
    F.rpad(F.col("MBR_UNIQ_KEY").cast(StringType()), 20, " ").alias("MBR_UNIQ_KEY")
)
write_files(
    df_W_DRUG_ENR_final,
    f"{adls_path}/load/W_DRUG_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# "IDS_CLM_Update" (DB2Connector, Database=IDS) => Merge approach on CLM
df_IDS_CLM_Update = df_ids_grp_update.select("CLM_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","GRP_SK")

temp_table_IDS_CLM_Update = "STAGING.LDIPClmMbrErrExtr_IDS_CLM_Update_temp"
spark.sql(f"DROP TABLE IF EXISTS {temp_table_IDS_CLM_Update}")

(
    df_IDS_CLM_Update
    .write
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", temp_table_IDS_CLM_Update)
    .mode("overwrite")
    .save()
)

merge_sql_IDS_CLM_Update = (
    f"MERGE {IDSOwner}.CLM AS T "
    f"USING {temp_table_IDS_CLM_Update} AS S "
    f"ON T.CLM_SK = S.CLM_SK "
    f"WHEN MATCHED THEN UPDATE SET "
    f"T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK, "
    f"T.GRP_SK = S.GRP_SK "
    f"WHEN NOT MATCHED THEN INSERT (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, GRP_SK) "
    f"VALUES (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_SK, S.GRP_SK);"
)
execute_dml(merge_sql_IDS_CLM_Update, jdbc_url_ids, jdbc_props_ids)

# "EDW_CLM_Update" (DB2Connector, Database=EDW) => also do a merge
df_EDW_CLM_Update = df_edw_grp_update.select(
    "CLM_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","GRP_SK","GRP_ID"
)
temp_table_EDW_CLM_Update = "STAGING.LDIPClmMbrErrExtr_EDW_CLM_Update_temp"
spark.sql(f"DROP TABLE IF EXISTS {temp_table_EDW_CLM_Update}")

(
    df_EDW_CLM_Update
    .write
    .format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("dbtable", temp_table_EDW_CLM_Update)
    .mode("overwrite")
    .save()
)

merge_sql_EDW_CLM_Update = (
    f"MERGE {EDWOwner}.CLM_F AS T "
    f"USING {temp_table_EDW_CLM_Update} AS S "
    f"ON T.CLM_SK = S.CLM_SK "
    f"WHEN MATCHED THEN UPDATE SET "
    f"T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK, "
    f"T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK, "
    f"T.GRP_SK = S.GRP_SK, "
    f"T.GRP_ID = S.GRP_ID "
    f"WHEN NOT MATCHED THEN INSERT (CLM_SK,LAST_UPDT_RUN_CYC_EXCTN_DT_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,GRP_SK,GRP_ID) "
    f"VALUES (S.CLM_SK,S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,S.LAST_UPDT_RUN_CYC_EXCTN_SK,S.GRP_SK,S.GRP_ID);"
)
execute_dml(merge_sql_EDW_CLM_Update, jdbc_url_edw, jdbc_props_edw)

# "hf_ldi_errmbrclm_errlkup" => scenario A => deduplicate by "CLM_ID"
df_hf_ldi_errmbrclm_errlkup = df_Trn_MbrMtch_Lkup_ErrorClm.dropDuplicates(["CLM_ID"])

# "SCErrFile" (CTransformerStage) => 2 outputs => ErrFileUpdate, ErrFileReport
df_SCErrFile_base = df_ErrorFile2.alias("RejectRecs")
df_SCErrFile_join = df_SCErrFile_base.join(
    df_hf_ldi_errmbrclm_errlkup.alias("Error"),
    F.col("RejectRecs.CLM_ID") == F.col("Error.CLM_ID"),
    how="left"
)

df_SCErrFile_ErrFileUpdate = (
    df_SCErrFile_join
    .select(
        F.col("Error.CLM_SK").alias("CLM_SK"),
        F.col("Error.CLM_ID").alias("CLM_ID"),
        F.col("Error.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Error.CLM_TYP_CD").alias("CLM_TYP_CD"),
        F.col("Error.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
        F.col("Error.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
        F.col("Error.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
        F.col("Error.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
        F.col("Error.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
        F.col("Error.SUB_SSN").alias("SUB_SSN"),
        F.col("Error.PATN_LAST_NM").alias("PATN_LAST_NM"),
        F.col("Error.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
        F.col("Error.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
        F.col("Error.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
        F.col("Error.ERR_CD").alias("ERR_CD"),
        F.col("Error.ERR_DESC").alias("ERR_DESC"),
        F.col("Error.FEP_MBR_ID").alias("FEP_MBR_ID"),
        F.col("Error.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
        F.col("Error.SUB_LAST_NM").alias("SUB_LAST_NM"),
        F.col("Error.SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
        F.col("Error.SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
        F.col("Error.GRP_ID").alias("GRP_ID"),
        F.col("Error.FILE_DT_SK").alias("FILE_DT_SK"),
        F.col("Error.PATN_SSN").alias("PATN_SSN")
    )
)

df_SCErrFile_ErrFileReport = (
    df_SCErrFile_join
    .select(
        F.col("Error.CLM_ID").alias("CLM_ID"),
        F.col("Error.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Error.CLM_TYP_CD").alias("CLM_TYP_CD"),
        F.col("Error.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
        F.col("Error.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
        F.col("Error.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
        F.col("Error.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
        F.col("Error.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
        F.col("Error.SUB_SSN").alias("SUB_SSN"),
        F.col("Error.PATN_LAST_NM").alias("PATN_LAST_NM"),
        F.col("Error.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
        F.col("Error.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
        F.col("Error.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
        F.col("Error.ERR_CD").alias("ERR_CD"),
        F.col("Error.ERR_DESC").alias("ERR_DESC"),
        F.col("Error.FEP_MBR_ID").alias("FEP_MBR_ID"),
        F.col("Error.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
        F.col("Error.SUB_LAST_NM").alias("SUB_LAST_NM"),
        F.col("Error.SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
        F.col("Error.SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
        F.col("Error.GRP_ID").alias("GRP_ID"),
        F.col("Error.FILE_DT_SK").alias("FILE_DT_SK"),
        F.col("Error.PATN_SSN").alias("PATN_SSN")
    )
)

# "ErrFileUpdate" (CSeqFileStage) => final write
df_ErrFileUpdate_final = df_SCErrFile_ErrFileUpdate.select(
    F.rpad(F.col("CLM_SK").cast(StringType()), 10, " ").alias("CLM_SK"),
    F.rpad(F.col("CLM_ID"), 255, " ").alias("CLM_ID"),
    F.rpad(F.col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_TYP_CD"), 255, " ").alias("CLM_TYP_CD"),
    F.rpad(F.col("CLM_SUBTYP_CD"), 255, " ").alias("CLM_SUBTYP_CD"),
    F.rpad(F.col("CLM_SVC_STRT_DT_SK"), 10, " ").alias("CLM_SVC_STRT_DT_SK"),
    F.rpad(F.col("SRC_SYS_GRP_PFX"), 255, " ").alias("SRC_SYS_GRP_PFX"),
    F.rpad(F.col("SRC_SYS_GRP_ID"), 255, " ").alias("SRC_SYS_GRP_ID"),
    F.rpad(F.col("SRC_SYS_GRP_SFX"), 255, " ").alias("SRC_SYS_GRP_SFX"),
    F.rpad(F.col("SUB_SSN"), 255, " ").alias("SUB_SSN"),
    F.rpad(F.col("PATN_LAST_NM"), 255, " ").alias("PATN_LAST_NM"),
    F.rpad(F.col("PATN_FIRST_NM"), 255, " ").alias("PATN_FIRST_NM"),
    F.rpad(F.col("PATN_GNDR_CD"), 255, " ").alias("PATN_GNDR_CD"),
    F.rpad(F.col("PATN_BRTH_DT_SK"), 10, " ").alias("PATN_BRTH_DT_SK"),
    F.rpad(F.col("ERR_CD"), 255, " ").alias("ERR_CD"),
    F.rpad(F.col("ERR_DESC"), 255, " ").alias("ERR_DESC"),
    F.rpad(F.col("FEP_MBR_ID"), 255, " ").alias("FEP_MBR_ID"),
    F.rpad(F.col("SUB_FIRST_NM"), 255, " ").alias("SUB_FIRST_NM"),
    F.rpad(F.col("SUB_LAST_NM"), 255, " ").alias("SUB_LAST_NM"),
    F.rpad(F.col("SRC_SYS_SUB_ID"), 255, " ").alias("SRC_SYS_SUB_ID"),
    F.rpad(F.col("SRC_SYS_MBR_SFX_NO"), 3, " ").alias("SRC_SYS_MBR_SFX_NO"),
    F.rpad(F.col("GRP_ID"), 255, " ").alias("GRP_ID"),
    F.rpad(F.col("FILE_DT_SK"), 10, " ").alias("FILE_DT_SK"),
    F.rpad(F.col("PATN_SSN"), 255, " ").alias("PATN_SSN")
)
write_files(
    df_ErrFileUpdate_final,
    f"{adls_path}/load/P_CLM_MBRSH_ERR_RECYC_{SrcSysCd1}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# "ErrFileReport" (CSeqFileStage) => final write
df_ErrFileReport_final = df_SCErrFile_ErrFileReport.select(
    F.rpad(F.col("CLM_ID"), 255, " ").alias("CLM_ID"),
    F.rpad(F.col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_TYP_CD"), 255, " ").alias("CLM_TYP_CD"),
    F.rpad(F.col("CLM_SUBTYP_CD"), 255, " ").alias("CLM_SUBTYP_CD"),
    F.rpad(F.col("CLM_SVC_STRT_DT_SK"), 10, " ").alias("CLM_SVC_STRT_DT_SK"),
    F.rpad(F.col("SRC_SYS_GRP_PFX"), 255, " ").alias("SRC_SYS_GRP_PFX"),
    F.rpad(F.col("SRC_SYS_GRP_ID"), 255, " ").alias("SRC_SYS_GRP_ID"),
    F.rpad(F.col("SRC_SYS_GRP_SFX"), 255, " ").alias("SRC_SYS_GRP_SFX"),
    F.rpad(F.col("SUB_SSN"), 255, " ").alias("SUB_SSN"),
    F.rpad(F.col("PATN_LAST_NM"), 255, " ").alias("PATN_LAST_NM"),
    F.rpad(F.col("PATN_FIRST_NM"), 255, " ").alias("PATN_FIRST_NM"),
    F.rpad(F.col("PATN_GNDR_CD"), 255, " ").alias("PATN_GNDR_CD"),
    F.rpad(F.col("PATN_BRTH_DT_SK"), 10, " ").alias("PATN_BRTH_DT_SK"),
    F.rpad(F.col("ERR_CD"), 255, " ").alias("ERR_CD"),
    F.rpad(F.col("ERR_DESC"), 255, " ").alias("ERR_DESC"),
    F.rpad(F.col("FEP_MBR_ID"), 255, " ").alias("FEP_MBR_ID"),
    F.rpad(F.col("SUB_FIRST_NM"), 255, " ").alias("SUB_FIRST_NM"),
    F.rpad(F.col("SUB_LAST_NM"), 255, " ").alias("SUB_LAST_NM"),
    F.rpad(F.col("SRC_SYS_SUB_ID"), 255, " ").alias("SRC_SYS_SUB_ID"),
    F.rpad(F.col("SRC_SYS_MBR_SFX_NO"), 3, " ").alias("SRC_SYS_MBR_SFX_NO"),
    F.rpad(F.col("GRP_ID"), 255, " ").alias("GRP_ID"),
    F.rpad(F.col("FILE_DT_SK"), 10, " ").alias("FILE_DT_SK"),
    F.rpad(F.col("PATN_SSN"), 255, " ").alias("PATN_SSN")
)
write_files(
    df_ErrFileReport_final,
    f"{adls_path_publish}/external/{SrcSysCd1}_MbrMatch_MedClm_ErrorFile_Recycle.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)