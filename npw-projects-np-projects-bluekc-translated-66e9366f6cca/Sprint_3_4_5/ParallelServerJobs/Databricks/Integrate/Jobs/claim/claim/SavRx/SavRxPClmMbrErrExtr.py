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
# MAGIC DESCRIPTION :  SavRx Common claims recycle process
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                  Date                 Project/Altiris #          Change Description                                                           Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                            --------------------         ------------------------          -----------------------------------------------------------------------                   --------------------------------         -------------------------------   ----------------------------      
# MAGIC Jaideep Mankala                    2018-03-18              5828                          Original Programming                                                       IntegrateDev2                  Kalyan Neelam           2018-03-19
# MAGIC Kaushik Kapoor                      2018-06-05              5828                       Adding new member match logic added in the PreProc 
# MAGIC                                                                                                              job for handling BabyBoy and other scenarios                     IntegrateDev2\(9)\(9)Abhiram Dasarathy\(9)    2018-06-13


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, CharType
from pyspark.sql.functions import col, upper, substring, lit, when
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

# We also need trim and current_timestamp, current_date, etc. from user-defined scope (already in namespace).
# Import only what is strictly required.
# MAGIC %run ../../../../../shared_containers/PrimaryKey/PBMClaimsStep6MemMatch

# Retrieve job parameters
EDWOwner = get_widget_value('EDWOwner','')
IDSOwner = get_widget_value('IDSOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCdErr = get_widget_value('SrcSysCdErr','')
RunID = get_widget_value('RunID','')
RunCycle = get_widget_value('RunCycle','')
RunCycleDate = get_widget_value('RunCycleDate','')
Logging = get_widget_value('Logging','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCd1 = get_widget_value('SrcSysCd1','')
RunDate = get_widget_value('RunDate','')

# Prepare schemas and read from the first Sequential File (ErrorFile2)
schema_ErrorFile2 = StructType([
    StructField("CLM_ID", StringType(), nullable=False)
])
df_ErrorFile2 = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("sep", ",")
    .schema(schema_ErrorFile2)
    .load(f"{adls_path_publish}/external/{SrcSysCd1}_MbrMatch_RejectRecs.dat")
)

# GRP_ID (DB2Connector to IDS)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
select_query_GRP_ID = (
    f"SELECT DISTINCT\n"
    f"      PBM_GRP_ID, \n"
    f"      GRP_ID\n"
    f"FROM {IDSOwner}.P_PBM_GRP_XREF XREF\n"
    f"WHERE\n"
    f"             UPPER(XREF.SRC_SYS_CD) = 'SAVRX' AND\n"
    f"             '{RunDate}' BETWEEN CAST(XREF.EFF_DT as DATE) AND CAST(XREF.TERM_DT as DATE)"
)
df_GRP_ID = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", select_query_GRP_ID)
    .load()
)
# hf_savrx_medclm_preproc_grpsklkup (Scenario A) => deduplicate on primary key PBM_GRP_ID
df_GrpBase_lkup = df_GRP_ID.dropDuplicates(["PBM_GRP_ID"])

# IDS_CLM (DB2Connector to IDS)
select_query_IDS_CLM = (
    f"SELECT\n"
    f"CLM.CLM_SK\n"
    f"FROM {IDSOwner}.CLM AS CLM,\n"
    f"     {IDSOwner}.CD_MPPNG AS CD\n"
    f"WHERE CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK\n"
    f"  AND CD.TRGT_CD = '{SrcSysCd}'\n"
    f"  AND CLM.GRP_SK = 0"
)
df_IDS_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", select_query_IDS_CLM)
    .load()
)
# hf_savrx_rx_ids_clm (Scenario A) => deduplicate on CLM_SK
df_IDS_CLM_lkp = df_IDS_CLM.dropDuplicates(["CLM_SK"])

# EDW_CLM_F (DB2Connector to EDW)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
select_query_EDW_CLM_F = (
    f"SELECT\n"
    f"CLM_SK\n"
    f"FROM {EDWOwner}.CLM_F\n"
    f"WHERE SRC_SYS_CD = '{SrcSysCd}'\n"
    f" AND GRP_SK = 0"
)
df_EDW_CLM_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", select_query_EDW_CLM_F)
    .load()
)
# hf_savrx_rx_edw_clm_f (Scenario A) => deduplicate on CLM_SK
df_EDW_CLM_lkp = df_EDW_CLM_F.dropDuplicates(["CLM_SK"])

# IDS_MBR (DB2Connector to IDS)
select_query_IDS_MBR = (
    f"SELECT DISTINCT\n"
    f"      MEMBER.MBR_UNIQ_KEY,\n"
    f"      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MEMBER.FIRST_NM,'-',''),'&',''),'.',''),' ',''),'''',''),'\"',''),',','') AS MBR_FIRST_NM,\n"
    f"      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MEMBER.LAST_NM,'-',''),'&',''),'.',''),' ',''),'''',''),'\"',''),',','') AS MBR_LAST_NM,\n"
    f"      MEMBER.SSN AS MBR_SSN,\n"
    f"      MEMBER.BRTH_DT_SK ,\n"
    f"      MEMBER.GNDR_CD,\n"
    f"       REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(SUBSCRIBER.FIRST_NM,'-',''),'&',''),'.',''),' ',''),'''',''),'\"',''),',','') AS SUB_FIRST_NM,\n"
    f"       REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(SUBSCRIBER.LAST_NM,'-',''),'&',''),'.',''),' ',''),'''',''),'\"',''),',','') AS SUB_LAST_NM,\n"
    f"      SUBSCRIBER.SSN AS SUB_SSN,\n"
    f"      SUBSCRIBER.BRTH_DT_SK AS SUB_BRTH_DT_SK,\n"
    f"      MEMBER.GRP_ID,\n"
    f"      MEMBER.SUB_ID,\n"
    f"      MEMBER.MBR_SFX_NO\n"
    f"FROM\n"
    f"(SELECT \n"
    f"      MBR.MBR_UNIQ_KEY,\n"
    f"      MBR.FIRST_NM,\n"
    f"      MBR.LAST_NM,\n"
    f"      MBR.SSN,\n"
    f"      MBR.BRTH_DT_SK ,\n"
    f"      CD.TRGT_CD GNDR_CD,\n"
    f"      MBR.SUB_SK,\n"
    f"      GRP.GRP_ID,\n"
    f"      SUB.SUB_ID, \n"
    f"      MBR.MBR_SFX_NO\n"
    f"FROM \n"
    f"          {IDSOwner}.MBR MBR,\n"
    f"          {IDSOwner}.SUB SUB,\n"
    f"          {IDSOwner}.GRP GRP,\n"
    f"          {IDSOwner}.CD_MPPNG CD,\n"
    f"          {IDSOwner}.MBR_ENR ENR,\n"
    f"          {IDSOwner}.CD_MPPNG CD1,\n"
    f"          {IDSOwner}.P_PBM_GRP_XREF XREF\n"
    f"WHERE\n"
    f"             MBR.MBR_SK = ENR.MBR_SK AND\n"
    f"             MBR.SUB_SK = SUB.SUB_SK AND\n"
    f"             SUB.GRP_SK = GRP.GRP_SK AND\n"
    f"             MBR.MBR_GNDR_CD_SK = CD.CD_MPPNG_SK AND\n"
    f"             ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND\n"
    f"             GRP.GRP_ID = XREF.GRP_ID AND \n"
    f"             UPPER(XREF.SRC_SYS_CD) = 'SAVRX' AND\n"
    f"             '{RunDate}' BETWEEN CAST(XREF.EFF_DT as DATE) AND CAST(XREF.TERM_DT as DATE) AND\n"
    f"             CD1.TRGT_CD = 'MED' AND\n"
    f"             UPPER(MBR.LAST_NM) NOT LIKE '%DO NOT USE%') MEMBER\n"
    f"LEFT OUTER JOIN\n"
    f"(SELECT \n"
    f"      MBR1.MBR_UNIQ_KEY,\n"
    f"      MBR1.FIRST_NM,\n"
    f"      MBR1.LAST_NM,\n"
    f"      MBR1.SSN,\n"
    f"      MBR1.BRTH_DT_SK,\n"
    f"      MBR1.SUB_SK\n"
    f"FROM\n"
    f"          {IDSOwner}.MBR MBR1,\n"
    f"          {IDSOwner}.CD_MPPNG CD1\n"
    f"WHERE\n"
    f"            MBR1.MBR_RELSHP_CD_SK = CD1.CD_MPPNG_SK AND\n"
    f"            CD1.TRGT_CD = 'SUB' AND\n"
    f"            UPPER(MBR1.LAST_NM) NOT LIKE '%DO NOT USE%') SUBSCRIBER\n"
    f"ON MEMBER.SUB_SK = SUBSCRIBER.SUB_SK"
)
df_IDS_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", select_query_IDS_MBR)
    .load()
)

# Now Transformer_265: It has one input (df_IDS_MBR). We create 6 output DataFrames (Step1..Step6).
# We'll use the column expressions as specified. 
# DataStage indexing "[1,4]" => substring with start=1, length=4 in Spark => substring(..., 1, 4)
df_Transformer_265_Step1 = df_IDS_MBR.select(
    col("SUB_SSN").alias("SUB_SSN"),
    substring(upper(trim(col("MBR_FIRST_NM"))), 1, 4).alias("MBR_FIRST_NM"),
    substring(upper(trim(col("MBR_LAST_NM"))), 1, 4).alias("MBR_LAST_NM"),
    col("BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    upper(trim(col("GNDR_CD"))).alias("GNDR_CD"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("MBR_SSN").alias("MBR_SSN"),
    upper(trim(col("SUB_FIRST_NM"))).alias("SUB_FIRST_NM"),
    upper(trim(col("SUB_LAST_NM"))).alias("SUB_LAST_NM"),
    col("SUB_BRTH_DT_SK").alias("SUB_BRTH_DT_SK"),
    col("GRP_ID").alias("GRP_ID"),
    col("SUB_ID").alias("SUB_ID"),
    col("MBR_SFX_NO").alias("MBR_SFX_NO")
)

df_Transformer_265_Step2 = df_IDS_MBR.select(
    substring(upper(trim(col("MBR_FIRST_NM"))), 1, 4).alias("MBR_FIRST_NM"),
    substring(upper(trim(col("MBR_LAST_NM"))), 1, 4).alias("MBR_LAST_NM"),
    col("BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    upper(trim(col("GNDR_CD"))).alias("GNDR_CD"),
    substring(upper(trim(col("SUB_LAST_NM"))), 1, 4).alias("SUB_LAST_NM"),
    upper(trim(col("SUB_FIRST_NM"))).alias("SUB_FIRST_NM"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("MBR_SSN").alias("MBR_SSN"),
    col("SUB_SSN").alias("SUB_SSN"),
    col("SUB_BRTH_DT_SK").alias("SUB_BRTH_DT_SK"),
    col("GRP_ID").alias("GRP_ID"),
    col("SUB_ID").alias("SUB_ID"),
    col("MBR_SFX_NO").alias("MBR_SFX_NO")
)

df_Transformer_265_Step3 = df_IDS_MBR.select(
    substring(upper(trim(col("MBR_FIRST_NM"))), 1, 4).alias("MBR_FIRST_NM"),
    substring(upper(trim(col("MBR_LAST_NM"))), 1, 4).alias("MBR_LAST_NM"),
    col("BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    substring(upper(trim(col("SUB_FIRST_NM"))), 1, 4).alias("SUB_FIRST_NM"),
    substring(upper(trim(col("SUB_LAST_NM"))), 1, 4).alias("SUB_LAST_NM"),
    col("GNDR_CD").alias("GNDR_CD"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("MBR_SSN").alias("MBR_SSN"),
    col("SUB_SSN").alias("SUB_SSN"),
    col("SUB_BRTH_DT_SK").alias("SUB_BRTH_DT_SK"),
    col("GRP_ID").alias("GRP_ID"),
    col("SUB_ID").alias("SUB_ID"),
    col("MBR_SFX_NO").alias("MBR_SFX_NO")
)

df_Transformer_265_Step4 = df_IDS_MBR.select(
    substring(upper(trim(col("MBR_FIRST_NM"))), 1, 4).alias("MBR_FIRST_NM"),
    substring(upper(trim(col("MBR_LAST_NM"))), 1, 4).alias("MBR_LAST_NM"),
    col("SUB_SSN").alias("SUB_SSN"),
    substring(upper(trim(col("SUB_FIRST_NM"))), 1, 4).alias("SUB_FIRST_NM"),
    substring(upper(trim(col("SUB_LAST_NM"))), 1, 4).alias("SUB_LAST_NM"),
    col("BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    upper(trim(col("GNDR_CD"))).alias("GNDR_CD"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("MBR_SSN").alias("MBR_SSN"),
    col("SUB_BRTH_DT_SK").alias("SUB_BRTH_DT_SK"),
    col("GRP_ID").alias("GRP_ID"),
    col("SUB_ID").alias("SUB_ID"),
    col("MBR_SFX_NO").alias("MBR_SFX_NO")
)

df_Transformer_265_Step5 = df_IDS_MBR.select(
    col("SUB_SSN").alias("SUB_SSN"),
    substring(upper(trim(col("MBR_FIRST_NM"))), 1, 4).alias("MBR_FIRST_NM"),
    col("BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    upper(trim(col("GNDR_CD"))).alias("GNDR_CD"),
    upper(trim(col("MBR_LAST_NM"))).alias("MBR_LAST_NM"),
    upper(trim(col("SUB_FIRST_NM"))).alias("SUB_FIRST_NM"),
    upper(trim(col("SUB_LAST_NM"))).alias("SUB_LAST_NM"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("MBR_SSN").alias("MBR_SSN"),
    col("SUB_BRTH_DT_SK").alias("SUB_BRTH_DT_SK"),
    col("GRP_ID").alias("GRP_ID"),
    col("SUB_ID").alias("SUB_ID"),
    col("MBR_SFX_NO").alias("MBR_SFX_NO")
)

df_Transformer_265_Step6 = df_IDS_MBR.select(
    col("SUB_SSN").alias("SUB_SSN"),
    col("GRP_ID").alias("GRP_ID"),
    col("BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    upper(trim(col("GNDR_CD"))).alias("GNDR_CD"),
    upper(trim(col("MBR_FIRST_NM"))).alias("MBR_FIRST_NM"),
    upper(trim(col("MBR_LAST_NM"))).alias("MBR_LAST_NM"),
    upper(trim(col("SUB_FIRST_NM"))).alias("SUB_FIRST_NM"),
    upper(trim(col("SUB_LAST_NM"))).alias("SUB_LAST_NM"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("MBR_SSN").alias("MBR_SSN"),
    col("SUB_BRTH_DT_SK").alias("SUB_BRTH_DT_SK"),
    col("SUB_ID").alias("SUB_ID"),
    col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    lit("NA").alias("PBM_GRP_ID")  # WhereExpression => 'NA'
)

# PBMClaimsStep6MemMatch (Shared Container). One input, one output.
params_PBMClaimsStep6MemMatch = {}
df_step6_lkp = PBMClaimsStep6MemMatch(df_Transformer_265_Step6, params_PBMClaimsStep6MemMatch)

# Next: each CHashedFileStage from Transformer_265 -> MemberMatch is scenario A => deduplicate on the specified primary key columns:

df_step1_lkp = df_Transformer_265_Step1.dropDuplicates([
    "SUB_SSN","MBR_FIRST_NM","MBR_LAST_NM","MBR_BRTH_DT_SK","GNDR_CD"
])
df_step2_lkp = df_Transformer_265_Step2.dropDuplicates([
    "MBR_FIRST_NM","MBR_LAST_NM","MBR_BRTH_DT_SK","GNDR_CD","SUB_LAST_NM"
])
df_step3_lkp = df_Transformer_265_Step3.dropDuplicates([
    "MBR_FIRST_NM","MBR_LAST_NM","MBR_BRTH_DT_SK","SUB_FIRST_NM","SUB_LAST_NM"
])
df_step4_lkp = df_Transformer_265_Step4.dropDuplicates([
    "MBR_FIRST_NM","MBR_LAST_NM","SUB_SSN","SUB_FIRST_NM","SUB_LAST_NM"
])
df_step5_lkp = df_Transformer_265_Step5.dropDuplicates([
    "SUB_SSN","MBR_FIRST_NM","MBR_BRTH_DT_SK","GNDR_CD"
])

# IDS_P_CLM_ERR (DB2Connector to IDS)
select_query_IDS_P_CLM_ERR = (
    f"SELECT\n"
    f"CLM_SK,\n"
    f"CLM_ID,\n"
    f"SRC_SYS_CD,\n"
    f"CLM_TYP_CD,\n"
    f"CLM_SUBTYP_CD,\n"
    f"CLM_SVC_STRT_DT_SK,\n"
    f"SRC_SYS_GRP_PFX,\n"
    f"SRC_SYS_GRP_ID,\n"
    f"SRC_SYS_GRP_SFX,\n"
    f"SUB_SSN,\n"
    f"PATN_LAST_NM,\n"
    f"PATN_FIRST_NM,\n"
    f"PATN_GNDR_CD,\n"
    f"PATN_BRTH_DT_SK,\n"
    f"ERR_CD,\n"
    f"ERR_DESC,\n"
    f"FEP_MBR_ID,\n"
    f"SUB_FIRST_NM,\n"
    f"SUB_LAST_NM,\n"
    f"SRC_SYS_SUB_ID,\n"
    f"SRC_SYS_MBR_SFX_NO,\n"
    f"GRP_ID,\n"
    f"FILE_DT_SK,\n"
    f"PATN_SSN\n"
    f"FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC\n"
    f"WHERE SRC_SYS_CD = '{SrcSysCdErr}'"
)
df_IDS_P_CLM_ERR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", select_query_IDS_P_CLM_ERR)
    .load()
)
# hf_savrx_ids_p_clm_err => scenario A => deduplicate on CLM_SK
df_IDS_P_CLM_ERR_lkp = df_IDS_P_CLM_ERR.dropDuplicates(["CLM_SK"])

# Trn_Mbr_Mtch
# We create two outputs from df_IDS_P_CLM_ERR_lkp (primary link becomes two):
# Actually this transformer has 1 input that is df_IDS_P_CLM_ERR_lkp => it yields Next + Lnk_Err_Clm_To_Hf
df_Trn_Mbr_Mtch_Next = df_IDS_P_CLM_ERR_lkp.select(
    col("CLM_ID").alias("CLM_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    col("PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    col("SUB_SSN").alias("SUB_SSN"),
    col("PATN_GNDR_CD").alias("PATN_GNDR_CD")
)

df_Trn_Mbr_Mtch_LnkErrClmToHf = df_IDS_P_CLM_ERR_lkp.select(
    col("CLM_SK").alias("CLM_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    col("CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    col("SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    col("SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    col("SUB_SSN").alias("SUB_SSN"),
    col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    col("PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    col("PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    col("ERR_CD").alias("ERR_CD"),
    col("ERR_DESC").alias("ERR_DESC"),
    col("FEP_MBR_ID").alias("FEP_MBR_ID"),
    col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    col("SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    col("SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    col("GRP_ID").alias("GRP_ID"),
    col("FILE_DT_SK").alias("FILE_DT_SK"),
    col("PATN_SSN").alias("PATN_SSN")
)

# hf_savrx_errclm_mbrmatch_land => scenario A => deduplicate on CLM_SK => feed next
df_Lnk_ErrClm = df_Trn_Mbr_Mtch_LnkErrClmToHf.dropDuplicates(["CLM_SK"])

# Transformer_6
# Input is df_Trn_Mbr_Mtch_Next => outputs "SavRx"
df_Transformer_6_SavRx = df_Trn_Mbr_Mtch_Next.select(
    upper(trim(col("PATN_FIRST_NM"))).alias("PATN_FIRST_NM"),  # remove CR/LF/tabs replaced with just removing them
    upper(trim(col("PATN_LAST_NM"))).alias("PATN_LAST_NM"),
    trim(col("PATN_BRTH_DT_SK")).alias("BRTH_DT"),
    trim(col("SUB_SSN")).alias("CARDHLDR_ID_NO"),
    trim(col("SRC_SYS_GRP_ID")).alias("GRP_NO"),
    upper(trim(col("SUB_FIRST_NM"))).alias("CARDHLDR_FIRST_NM"),
    upper(trim(col("SUB_LAST_NM"))).alias("CARDHLDR_LAST_NM"),
    trim(col("SUB_SSN")).alias("CARDHLDR_SSN"),
    trim(col("PATN_GNDR_CD")).alias("GNDR_CD"),
    trim(col("CLM_ID")).alias("CLM_ID"),
    trim(col("CLM_SVC_STRT_DT_SK")).alias("CLM_SVC_STRT_DT_SK")
)

# MemberMatch
# Inputs: "SavRx" (primary), step1_lkp, step2_lkp, step3_lkp, step4_lkp, step5_lkp, step6_lkp
# We replicate the logic of stage variables. Then produce 2 outputs: Land and Reject.

# Construct joined DF with left joins:
df_MemberMatch_joined = (
    df_Transformer_6_SavRx.alias("SavRx")
    .join(df_step1_lkp.alias("Step1_lkp"),
          on=[
              col("SavRx.CARDHLDR_SSN") == col("Step1_lkp.SUB_SSN"),
              substring(upper(trim(col("SavRx.PATN_FIRST_NM"))), 1, 4) == col("Step1_lkp.MBR_FIRST_NM"),
              substring(upper(trim(col("SavRx.PATN_LAST_NM"))), 1, 4) == col("Step1_lkp.MBR_LAST_NM"),
              col("SavRx.BRTH_DT") == col("Step1_lkp.MBR_BRTH_DT_SK"),
              upper(trim(col("SavRx.GNDR_CD"))) == col("Step1_lkp.GNDR_CD")
          ],
          how="left")
    .join(df_step2_lkp.alias("Step2_lkp"),
          on=[
              substring(upper(trim(col("SavRx.PATN_FIRST_NM"))), 1, 4) == col("Step2_lkp.MBR_FIRST_NM"),
              substring(upper(trim(col("SavRx.PATN_LAST_NM"))), 1, 4) == col("Step2_lkp.MBR_LAST_NM"),
              col("SavRx.BRTH_DT") == col("Step2_lkp.MBR_BRTH_DT_SK"),
              upper(trim(col("SavRx.GNDR_CD"))) == col("Step2_lkp.GNDR_CD"),
              substring(upper(trim(col("SavRx.CARDHLDR_LAST_NM"))), 1, 4) == col("Step2_lkp.SUB_LAST_NM"),
              substring(upper(trim(col("SavRx.CARDHLDR_FIRST_NM"))), 1, 4) == col("Step2_lkp.SUB_FIRST_NM")
          ],
          how="left")
    .join(df_step3_lkp.alias("Step3_lkp"),
          on=[
              substring(upper(trim(col("SavRx.PATN_FIRST_NM"))), 1, 4) == col("Step3_lkp.MBR_FIRST_NM"),
              substring(upper(trim(col("SavRx.PATN_LAST_NM"))), 1, 4) == col("Step3_lkp.MBR_LAST_NM"),
              col("SavRx.BRTH_DT") == col("Step3_lkp.MBR_BRTH_DT_SK"),
              substring(upper(trim(col("SavRx.CARDHLDR_FIRST_NM"))), 1, 4) == col("Step3_lkp.SUB_FIRST_NM"),
              substring(upper(trim(col("SavRx.CARDHLDR_LAST_NM"))), 1, 4) == col("Step3_lkp.SUB_LAST_NM")
          ],
          how="left")
    .join(df_step4_lkp.alias("Step4_lkp"),
          on=[
              substring(upper(trim(col("SavRx.PATN_FIRST_NM"))), 1, 4) == col("Step4_lkp.MBR_FIRST_NM"),
              substring(upper(trim(col("SavRx.PATN_LAST_NM"))), 1, 4) == col("Step4_lkp.MBR_LAST_NM"),
              col("SavRx.CARDHLDR_SSN") == col("Step4_lkp.SUB_SSN"),
              substring(upper(trim(col("SavRx.CARDHLDR_FIRST_NM"))), 1, 4) == col("Step4_lkp.SUB_FIRST_NM"),
              substring(upper(trim(col("SavRx.CARDHLDR_LAST_NM"))), 1, 4) == col("Step4_lkp.SUB_LAST_NM")
          ],
          how="left")
    .join(df_step5_lkp.alias("Step5_lkp"),
          on=[
              col("SavRx.CARDHLDR_SSN") == col("Step5_lkp.SUB_SSN"),
              substring(upper(trim(col("SavRx.PATN_FIRST_NM"))), 1, 4) == col("Step5_lkp.MBR_FIRST_NM"),
              col("SavRx.BRTH_DT") == col("Step5_lkp.MBR_BRTH_DT_SK"),
              upper(trim(col("SavRx.GNDR_CD"))) == col("Step5_lkp.GNDR_CD")
          ],
          how="left")
    .join(df_step6_lkp.alias("Step6_lkp"),
          on=[
              col("SavRx.CARDHLDR_SSN") == col("Step6_lkp.SUB_SSN"),
              col("SavRx.BRTH_DT") == col("Step6_lkp.MBR_BRTH_DT_SK"),
              upper(trim(col("SavRx.GNDR_CD"))) == col("Step6_lkp.GNDR_CD")
          ],
          how="left")
)

# Build the logic for stage variables in MemberMatch
# We approximate the "If" logic. "UNK" if not found, else pick the first non-null among them. For SUB_ID, etc.
# Then we have constraints for the output links Land (svMbrUniqKey <> 'UNK') and Reject (svMbrUniqKey = 'UNK').

# Derive the columns:
df_MemberMatch_enriched = df_MemberMatch_joined.withColumn(
    "svMbrUniqKey",
    when(
        col("Step1_lkp.MBR_UNIQ_KEY").isNotNull(), col("Step1_lkp.MBR_UNIQ_KEY")
    ).otherwise(
        when(
            col("Step2_lkp.MBR_UNIQ_KEY").isNotNull(), col("Step2_lkp.MBR_UNIQ_KEY")
        ).otherwise(
            when(
                col("Step3_lkp.MBR_UNIQ_KEY").isNotNull(), col("Step3_lkp.MBR_UNIQ_KEY")
            ).otherwise(
                when(
                    col("Step4_lkp.MBR_UNIQ_KEY").isNotNull(), col("Step4_lkp.MBR_UNIQ_KEY")
                ).otherwise(
                    when(
                        col("Step5_lkp.MBR_UNIQ_KEY").isNotNull(), col("Step5_lkp.MBR_UNIQ_KEY")
                    ).otherwise(
                        when(
                            (col("Step6_lkp.MBR_UNIQ_KEY").isNotNull()) & (col("Step6_lkp.CNT") == lit(1)),
                            col("Step6_lkp.MBR_UNIQ_KEY")
                        ).otherwise(lit("UNK"))
                    )
                )
            )
        )
    )
).withColumn(
    "svSubId",
    when(
        col("Step1_lkp.SUB_ID").isNotNull(), col("Step1_lkp.SUB_ID")
    ).otherwise(
        when(
            col("Step2_lkp.SUB_ID").isNotNull(), col("Step2_lkp.SUB_ID")
        ).otherwise(
            when(
                col("Step3_lkp.SUB_ID").isNotNull(), col("Step3_lkp.SUB_ID")
            ).otherwise(
                when(
                    col("Step4_lkp.SUB_ID").isNotNull(), col("Step4_lkp.SUB_ID")
                ).otherwise(
                    when(
                        col("Step5_lkp.SUB_ID").isNotNull(), col("Step5_lkp.SUB_ID")
                    ).otherwise(
                        when(
                            col("Step6_lkp.SUB_ID").isNotNull(), col("Step6_lkp.SUB_ID")
                        ).otherwise(lit("UNK"))
                    )
                )
            )
        )
    )
).withColumn(
    "svGrpId",
    when(
        col("Step1_lkp.GRP_ID").isNotNull(), col("Step1_lkp.GRP_ID")
    ).otherwise(
        when(
            col("Step2_lkp.GRP_ID").isNotNull(), col("Step2_lkp.GRP_ID")
        ).otherwise(
            when(
                col("Step3_lkp.GRP_ID").isNotNull(), col("Step3_lkp.GRP_ID")
            ).otherwise(
                when(
                    col("Step4_lkp.GRP_ID").isNotNull(), col("Step4_lkp.GRP_ID")
                ).otherwise(
                    when(
                        col("Step5_lkp.GRP_ID").isNotNull(), col("Step5_lkp.GRP_ID")
                    ).otherwise(
                        when(
                            col("Step6_lkp.GRP_ID").isNotNull(), col("Step6_lkp.GRP_ID")
                        ).otherwise(lit("UNK"))
                    )
                )
            )
        )
    )
)

df_MemberMatch_Land = df_MemberMatch_enriched.filter(col("svMbrUniqKey") != lit("UNK")).select(
    col("SavRx.CLM_ID").alias("CLM_ID"),
    col("SavRx.CLM_SVC_STRT_DT_SK").alias("FILL_DT_SK"),
    when(col("svMbrUniqKey") == lit("UNK"), lit(0)).otherwise(col("svMbrUniqKey")).alias("MBR_UNIQ_KEY")
)

df_MemberMatch_Reject = df_MemberMatch_enriched.filter(col("svMbrUniqKey") == lit("UNK")).select(
    col("SavRx.CLM_ID").alias("CLM_ID")
)

# Now write "MemberMatch->Reject" to "Load_ErrorFile2" (CSeqFileStage, append)
# No length info, so we only do a simple select and do not rpad. 
write_files(
    df_MemberMatch_Reject.select("CLM_ID"),
    f"{adls_path_publish}/external/{SrcSysCd1}_MbrMatch_RejectRecs.dat",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# hf_savrx_mbrmatch_drugenrmatch_dedupe => scenario A => deduplicate on (CLM_ID, FILL_DT_SK, MBR_UNIQ_KEY)
df_hf_savrx_mbrmatch_drugenrmatch_dedupe = df_MemberMatch_Land.dropDuplicates(["CLM_ID","FILL_DT_SK","MBR_UNIQ_KEY"])

# W_DRUG_ENR_MATCH (DB2Connector to IDS) - The input is df_hf_savrx_mbrmatch_drugenrmatch_dedupe
# The stage logic: "Before_Sql" does a CALL to truncate, but instructions say we do a merge. 
# However, DataStage has "Connection_WriteMode=Insert, TableAction=Truncate". 
# We will replicate the final table "W_DRUG_ENR_MATCH" in IDS. 
# The stage used a direct insert in DS, but the instructions want a merge. 
# This table has 3 columns: CLM_ID, FILL_DT_SK, MBR_UNIQ_KEY (PK if so). We'll do an upsert by these columns.
tmp_tbl_w_drug_enr = "STAGING.SavRxPClmMbrErrExtr_W_DRUG_ENR_MATCH_temp"
execute_dml(f"DROP TABLE IF EXISTS {tmp_tbl_w_drug_enr}", jdbc_url_ids, jdbc_props_ids)

# Create the temporary table (physical) via spark write -> .option("createTableColumnTypes", ???).
# We can create a small schema for these columns:
df_hf_savrx_mbrmatch_drugenrmatch_dedupe.createOrReplaceTempView("_tmp_wdrug_enr")  # We must not use createOrReplaceTempView? 
# The instructions forbid createOrReplaceTempView. Instead we can write it directly with .jdbc, but we cannot do partial columns easily in pure PySpark without createTableColumnTypes. We will try:
df_temp_w_drug_enr = df_hf_savrx_mbrmatch_drugenrmatch_dedupe.select(
    col("CLM_ID").cast(StringType()),
    col("FILL_DT_SK").cast(StringType()),
    col("MBR_UNIQ_KEY").cast(IntegerType())
)
df_temp_w_drug_enr.write \
    .format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", tmp_tbl_w_drug_enr) \
    .mode("overwrite") \
    .save()

# Now there is no direct DS logic for merges on W_DRUG_ENR_MATCH (the DS job was basically a truncate+insert). We still unify it with a merge to replicate an upsert. 
# We'll do a simple merge that updates the row if match, else insert.
merge_sql_w_drug_enr = f"""
MERGE INTO {IDSOwner}.W_DRUG_ENR_MATCH AS T
USING {tmp_tbl_w_drug_enr} AS S
ON T.CLM_ID = S.CLM_ID
AND T.FILL_DT_SK = S.FILL_DT_SK
AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
WHEN MATCHED THEN
  UPDATE SET
    T.CLM_ID = S.CLM_ID
WHEN NOT MATCHED THEN
  INSERT (CLM_ID, FILL_DT_SK, MBR_UNIQ_KEY)
  VALUES (S.CLM_ID, S.FILL_DT_SK, S.MBR_UNIQ_KEY);
"""
execute_dml(merge_sql_w_drug_enr, jdbc_url_ids, jdbc_props_ids)

# W_DRUG_ENR_MATCH output pin => "MbrOut" => big query => we read with a SELECT from #$IDSOwner#.W_DRUG_ENR_MATCH ??? 
# The job says it actually runs "SELECT ..." from #$IDSOwner#.MBR_ENR etc. with unions => 
# We'll replicate that, following the stage's "SQL" property:

select_query_W_DRUG_ENR_MATCH = (
    f"SELECT \n"
    f"       CLM_ID,\n"
    f"       MBR_UNIQ_KEY,\n"
    f"       GRP_ID,\n"
    f"       SUB_ID, \n"
    f"       MBR_SFX_NO,\n"
    f"       SUB_UNIQ_KEY,\n"
    f"       EFF_DT_SK\n"
    f"FROM (\n"
    f"SELECT \n"
    f"      DRUG.CLM_ID,\n"
    f"      ENR.MBR_UNIQ_KEY,\n"
    f"      MAX(ENR.TERM_DT_SK) TERM_DT_SK,\n"
    f"      MAX(ENR.EFF_DT_SK) EFF_DT_SK,\n"
    f"      GRP.GRP_ID,\n"
    f"      SUB.SUB_ID, \n"
    f"      MBR.MBR_SFX_NO,\n"
    f"      SUB.SUB_UNIQ_KEY,\n"
    f"      1 as Order\n"
    f"FROM\n"
    f"    {IDSOwner}.MBR_ENR ENR,\n"
    f"    {IDSOwner}.CD_MPPNG CD1,\n"
    f"    {IDSOwner}.W_DRUG_ENR_MATCH DRUG,\n"
    f"    {IDSOwner}.PROD PROD,\n"
    f"    {IDSOwner}.MBR MBR,\n"
    f"    {IDSOwner}.SUB SUB,\n"
    f"    {IDSOwner}.GRP GRP\n"
    f"WHERE\n"
    f"     MBR.MBR_SK = ENR.MBR_SK AND\n"
    f"     MBR.SUB_SK = SUB.SUB_SK AND\n"
    f"     SUB.GRP_SK = GRP.GRP_SK AND\n"
    f"     ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND\n"
    f"     CD1.TRGT_CD IN ('MED', 'DNTL') AND\n"
    f"     ENR.ELIG_IN = 'Y' AND\n"
    f"     ENR.MBR_UNIQ_KEY = DRUG.MBR_UNIQ_KEY AND\n"
    f"     ENR.EFF_DT_SK <= DRUG.FILL_DT_SK AND\n"
    f"     ENR.TERM_DT_SK >= DRUG.FILL_DT_SK AND\n"
    f"     ENR.PROD_SK = PROD.PROD_SK\n"
    f"GROUP BY\n"
    f"     DRUG.CLM_ID,\n"
    f"     ENR.MBR_UNIQ_KEY,\n"
    f"     GRP.GRP_ID,\n"
    f"     SUB.SUB_ID, \n"
    f"     MBR.MBR_SFX_NO,\n"
    f"     SUB.SUB_UNIQ_KEY\n"
    f"UNION\n"
    f"SELECT \n"
    f"      DRUG.CLM_ID,\n"
    f"      ENR.MBR_UNIQ_KEY,\n"
    f"      MAX(ENR.TERM_DT_SK) TERM_DT_SK,\n"
    f"      MAX(ENR.EFF_DT_SK) EFF_DT_SK,\n"
    f"      GRP.GRP_ID,\n"
    f"      SUB.SUB_ID, \n"
    f"      MBR.MBR_SFX_NO,\n"
    f"      SUB.SUB_UNIQ_KEY,\n"
    f"      2 as Order\n"
    f"FROM\n"
    f"    {IDSOwner}.MBR_ENR ENR,\n"
    f"    {IDSOwner}.CD_MPPNG CD1,\n"
    f"    {IDSOwner}.W_DRUG_ENR_MATCH DRUG,\n"
    f"    {IDSOwner}.PROD PROD,\n"
    f"    {IDSOwner}.MBR MBR,\n"
    f"    {IDSOwner}.SUB SUB,\n"
    f"    {IDSOwner}.GRP GRP\n"
    f"WHERE\n"
    f"     MBR.MBR_SK = ENR.MBR_SK AND\n"
    f"     MBR.SUB_SK = SUB.SUB_SK AND\n"
    f"     SUB.GRP_SK = GRP.GRP_SK AND\n"
    f"     ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND\n"
    f"     CD1.TRGT_CD IN ('MED', 'DNTL') AND\n"
    f"     ENR.ELIG_IN = 'Y' AND\n"
    f"     ENR.MBR_UNIQ_KEY = DRUG.MBR_UNIQ_KEY AND\n"
    f"     ENR.PROD_SK = PROD.PROD_SK\n"
    f"GROUP BY\n"
    f"     DRUG.CLM_ID,\n"
    f"     ENR.MBR_UNIQ_KEY,\n"
    f"     GRP.GRP_ID,\n"
    f"     SUB.SUB_ID, \n"
    f"     MBR.MBR_SFX_NO,\n"
    f"     SUB.SUB_UNIQ_KEY\n"
    f"UNION\n"
    f"SELECT \n"
    f"      DRUG.CLM_ID,\n"
    f"      ENR.MBR_UNIQ_KEY,\n"
    f"      MAX(ENR.TERM_DT_SK) TERM_DT_SK,\n"
    f"      MAX(ENR.EFF_DT_SK) EFF_DT_SK,\n"
    f"      GRP.GRP_ID,\n"
    f"      SUB.SUB_ID, \n"
    f"      MBR.MBR_SFX_NO,\n"
    f"      SUB.SUB_UNIQ_KEY,\n"
    f"      3 as Order\n"
    f"FROM\n"
    f"    {IDSOwner}.MBR_ENR ENR,\n"
    f"    {IDSOwner}.CD_MPPNG CD1,\n"
    f"    {IDSOwner}.W_DRUG_ENR_MATCH DRUG,\n"
    f"    {IDSOwner}.PROD PROD,\n"
    f"    {IDSOwner}.MBR MBR,\n"
    f"    {IDSOwner}.SUB SUB,\n"
    f"    {IDSOwner}.GRP GRP\n"
    f"WHERE\n"
    f"     MBR.MBR_SK = ENR.MBR_SK AND\n"
    f"     MBR.SUB_SK = SUB.SUB_SK AND\n"
    f"     SUB.GRP_SK = GRP.GRP_SK AND\n"
    f"     ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND\n"
    f"     CD1.TRGT_CD IN ('MED', 'DNTL') AND\n"
    f"     ENR.ELIG_IN = 'N' AND\n"
    f"     ENR.MBR_UNIQ_KEY = DRUG.MBR_UNIQ_KEY AND\n"
    f"     ENR.PROD_SK = PROD.PROD_SK\n"
    f"GROUP BY\n"
    f"     DRUG.CLM_ID,\n"
    f"     ENR.MBR_UNIQ_KEY,\n"
    f"     GRP.GRP_ID,\n"
    f"     SUB.SUB_ID, \n"
    f"     MBR.MBR_SFX_NO,\n"
    f"     SUB.SUB_UNIQ_KEY\n"
    f")\n"
    f"ORDER BY\n"
    f"CLM_ID,\n"
    f"Order DESC,\n"
    f"TERM_DT_SK,\n"
    f"EFF_DT_SK,\n"
    f"MBR_UNIQ_KEY"
)
df_W_DRUG_ENR_MATCH_MbrOut = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", select_query_W_DRUG_ENR_MATCH)
    .load()
)

# hf_savrx_errclm_mbrmatch_mbrlkup => scenario A => deduplicate on CLM_ID => feed next
df_Lnk_Mbr_Lkup = df_W_DRUG_ENR_MATCH_MbrOut.dropDuplicates(["CLM_ID"])

# Trn_MbrMtch_Lkup
# Input is "Lnk_ErrClm" (primary) + multiple lookups: Lnk_Mbr_Lkup, GrpBase_lkup, IDS_CLM, EDW_CLM
df_Trn_MbrMtch_Lkup_joined = (
    df_Lnk_ErrClm.alias("Lnk_ErrClm")
    .join(df_Lnk_Mbr_Lkup.alias("Lnk_Mbr_Lkup"), on=[col("Lnk_ErrClm.CLM_ID") == col("Lnk_Mbr_Lkup.CLM_ID")], how="left")
    .join(df_GrpBase_lkup.alias("GrpBase_lkup"), on=[col("Lnk_ErrClm.SRC_SYS_GRP_ID") == col("GrpBase_lkup.PBM_GRP_ID")], how="left")
    .join(df_IDS_CLM_lkp.alias("IDS_CLM"), on=[col("Lnk_ErrClm.CLM_SK") == col("IDS_CLM.CLM_SK")], how="left")
    .join(df_EDW_CLM_lkp.alias("EDW_CLM"), on=[col("Lnk_ErrClm.CLM_SK") == col("EDW_CLM.CLM_SK")], how="left")
)

df_Trn_MbrMtch_Lkup_enriched = df_Trn_MbrMtch_Lkup_joined.withColumn(
    "svMbrUniqKey",
    when(col("Lnk_Mbr_Lkup.MBR_UNIQ_KEY").isNull(), lit(0)).otherwise(col("Lnk_Mbr_Lkup.MBR_UNIQ_KEY"))
).withColumn(
    "svGrpId",
    when(col("GrpBase_lkup.GRP_ID").isNotNull(), col("GrpBase_lkup.GRP_ID"))
    .otherwise(
        when(col("Lnk_Mbr_Lkup.GRP_ID").isNotNull(), col("Lnk_Mbr_Lkup.GRP_ID"))
        .otherwise(lit("UNK"))
    )
).withColumn(
    "svSubId",
    when(col("Lnk_Mbr_Lkup.SUB_ID").isNull(), lit("0")).otherwise(col("Lnk_Mbr_Lkup.SUB_ID"))
).withColumn(
    "svMbrSfxNo",
    when(col("Lnk_Mbr_Lkup.MBR_SFX_NO").isNull(), lit(0)).otherwise(col("Lnk_Mbr_Lkup.MBR_SFX_NO"))
).withColumn(
    "svMbrEnrEffDt",
    when(col("Lnk_Mbr_Lkup.EFF_DT_SK").isNull(), lit("1753-01-01")).otherwise(col("Lnk_Mbr_Lkup.EFF_DT_SK"))
).withColumn(
    "svSubUniqKey",
    when(col("Lnk_Mbr_Lkup.SUB_UNIQ_KEY").isNotNull(), col("Lnk_Mbr_Lkup.SUB_UNIQ_KEY")).otherwise(lit(0))
).withColumn(
    "svSrcSysCd",
    when(col("Lnk_ErrClm.SRC_SYS_CD").isNull() | (trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("")), lit("UNK"))
    .otherwise(
        when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("PCT"), lit("FACETS"))
        .otherwise(
            when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("WELLDYNERX"), lit("FACETS"))
            .otherwise(
                when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("PCS"), lit("FACETS"))
                .otherwise(
                    when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("CAREMARK"), lit("FACETS"))
                    .otherwise(
                        when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("ARGUS"), lit("FACETS"))
                        .otherwise(
                            when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("EDC"), lit("FACETS"))
                            .otherwise(
                                when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("OT@2"), lit("FACETS"))
                                .otherwise(
                                    when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("ADOL"), lit("FACETS"))
                                    .otherwise(
                                        when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("MOHSAIC"), lit("FACETS"))
                                        .otherwise(
                                            when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("CAREADVANCE"), lit("FACETS"))
                                            .otherwise(
                                                when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("ESI"), lit("FACETS"))
                                                .otherwise(
                                                    when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("OPTUMRX"), lit("FACETS"))
                                                    .otherwise(
                                                        when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("MCSOURCE"), lit("FACETS"))
                                                        .otherwise(
                                                            when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("MCAID"), lit("FACETS"))
                                                            .otherwise(
                                                                when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("MEDTRAK"), lit("FACETS"))
                                                                .otherwise(
                                                                    when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("BCBSSC"), lit("FACETS"))
                                                                    .otherwise(
                                                                        when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("BCA"), lit("FACETS"))
                                                                        .otherwise(
                                                                            when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("BCBSA"), lit("FACETS"))
                                                                            .otherwise(
                                                                                when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("SAVRX"), lit("FACETS"))
                                                                                .otherwise(
                                                                                    when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("LDI"), lit("FACETS"))
                                                                                    .otherwise(
                                                                                        when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("EYEMED"), lit("FACETS"))
                                                                                        .otherwise(
                                                                                            when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("CVS"), lit("FACETS"))
                                                                                            .otherwise(
                                                                                                when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("LUMERIS"), lit("FACETS"))
                                                                                                .otherwise(
                                                                                                    when(trim(col("Lnk_ErrClm.SRC_SYS_CD")) == lit("MEDIMPACT"), lit("FACETS"))
                                                                                                    .otherwise(col("Lnk_ErrClm.SRC_SYS_CD"))
                                                                                                )
                                                                                            )
                                                                                        )
                                                                                    )
                                                                                )
                                                                            )
                                                                        )
                                                                    )
                                                                )
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    )
)

df_Trn_MbrMtch_Lkup_Lnk_WDrugEnr = df_Trn_MbrMtch_Lkup_enriched.filter(col("Lnk_Mbr_Lkup.MBR_UNIQ_KEY").isNotNull()).select(
    col("Lnk_ErrClm.CLM_ID").alias("CLM_ID"),
    col("Lnk_ErrClm.CLM_SVC_STRT_DT_SK").alias("FILL_DT_SK"),
    col("Lnk_Mbr_Lkup.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

df_Trn_MbrMtch_Lkup_Lnk_ErrClmLand = df_Trn_MbrMtch_Lkup_enriched.filter(col("Lnk_Mbr_Lkup.MBR_UNIQ_KEY").isNotNull()).select(
    col("Lnk_ErrClm.CLM_SK").alias("CLM_SK"),
    col("Lnk_ErrClm.CLM_ID").alias("CLM_ID"),
    col("Lnk_ErrClm.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Lnk_ErrClm.CLM_TYP_CD").alias("CLM_TYP_CD"),
    col("Lnk_ErrClm.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    col("Lnk_ErrClm.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    col("Lnk_ErrClm.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    col("Lnk_ErrClm.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    col("Lnk_ErrClm.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    col("Lnk_ErrClm.SUB_SSN").alias("SUB_SSN"),
    col("Lnk_ErrClm.PATN_LAST_NM").alias("PATN_LAST_NM"),
    col("Lnk_ErrClm.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    col("Lnk_ErrClm.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    col("Lnk_ErrClm.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    col("svMbrUniqKey").alias("MBR_UNIQ_KEY"),
    col("svGrpId").alias("GRP_ID"),
    col("svSubId").alias("SUB_ID"),
    col("svMbrSfxNo").alias("MBR_SFX_NO"),
    col("svSubUniqKey").alias("SUB_UNIQ_KEY"),
    col("svMbrEnrEffDt").alias("EFF_DT_SK")
)

df_Trn_MbrMtch_Lkup_ErrorClm = df_Trn_MbrMtch_Lkup_enriched.select(
    col("Lnk_ErrClm.CLM_ID").alias("CLM_ID"),
    col("Lnk_ErrClm.CLM_SK").alias("CLM_SK"),
    col("Lnk_ErrClm.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Lnk_ErrClm.CLM_TYP_CD").alias("CLM_TYP_CD"),
    col("Lnk_ErrClm.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    col("Lnk_ErrClm.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    col("Lnk_ErrClm.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    col("Lnk_ErrClm.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    col("Lnk_ErrClm.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    col("Lnk_ErrClm.SUB_SSN").alias("SUB_SSN"),
    col("Lnk_ErrClm.PATN_LAST_NM").alias("PATN_LAST_NM"),
    col("Lnk_ErrClm.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    col("Lnk_ErrClm.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    col("Lnk_ErrClm.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    col("Lnk_ErrClm.ERR_CD").alias("ERR_CD"),
    col("Lnk_ErrClm.ERR_DESC").alias("ERR_DESC"),
    col("Lnk_ErrClm.FEP_MBR_ID").alias("FEP_MBR_ID"),
    col("Lnk_ErrClm.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    col("Lnk_ErrClm.SUB_LAST_NM").alias("SUB_LAST_NM"),
    col("Lnk_ErrClm.SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    col("Lnk_ErrClm.SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    when(col("GrpBase_lkup.GRP_ID").isNotNull(), col("GrpBase_lkup.GRP_ID"))
     .otherwise(
        when(col("Lnk_Mbr_Lkup.GRP_ID").isNotNull(), col("Lnk_Mbr_Lkup.GRP_ID"))
        .otherwise(
            when(col("Lnk_ErrClm.GRP_ID").isNotNull(), col("Lnk_Mbr_Lkup.GRP_ID"))
            .otherwise(lit("UNK"))
        )
     ).alias("GRP_ID"),
    col("Lnk_ErrClm.FILE_DT_SK").alias("FILE_DT_SK"),
    col("Lnk_ErrClm.PATN_SSN").alias("PATN_SSN")
)

df_Trn_MbrMtch_Lkup_ids_grp_update = df_Trn_MbrMtch_Lkup_enriched.filter(
    (
        ( (col("Lnk_ErrClm.GRP_ID").isNull()) | (trim(col("Lnk_ErrClm.GRP_ID")) == lit("")) )
        & (col("GrpBase_lkup.GRP_ID").isNotNull())
        & (col("IDS_CLM.CLM_SK").isNotNull())
    )
).select(
    col("Lnk_ErrClm.CLM_SK").alias("CLM_SK"),
    col("RunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    # We'll set GRP_SK = GetFkeyGrp(svSrcSysCd, Lnk_ErrClm.CLM_SK, svGrpId, Logging)
    # That is a user-defined function call
    # We'll pass exactly as the expression, because we assume it's available:
    expr("GetFkeyGrp(svSrcSysCd, Lnk_ErrClm.CLM_SK, svGrpId, Logging)").alias("GRP_SK")
)

df_Trn_MbrMtch_Lkup_edw_grp_update = df_Trn_MbrMtch_Lkup_enriched.filter(
    (
        ( (col("Lnk_ErrClm.GRP_ID").isNull()) | (trim(col("Lnk_ErrClm.GRP_ID")) == lit("")) )
        & (col("GrpBase_lkup.GRP_ID").isNotNull())
        & (col("EDW_CLM.CLM_SK").isNotNull())
    )
).select(
    col("Lnk_ErrClm.CLM_SK").alias("CLM_SK"),
    col("RunCycleDate").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("RunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    expr("GetFkeyGrp(svSrcSysCd, Lnk_ErrClm.CLM_SK, svGrpId, Logging)").alias("GRP_SK"),
    col("svGrpId").alias("GRP_ID")
)

# SavRxErrClmLand (CSeqFileStage). We write df_Trn_MbrMtch_Lkup_Lnk_ErrClmLand => #$FilePath#/verified
# Filenames => f"{adls_path}/verified/{SrcSysCd1}_ErrClm_Landing.dat.{RunID}"
write_files(
    df_Trn_MbrMtch_Lkup_Lnk_ErrClmLand,
    f"{adls_path}/verified/{SrcSysCd1}_ErrClm_Landing.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# W_DRUG_ENR (CSeqFileStage). We write df_Trn_MbrMtch_Lkup_Lnk_WDrugEnr => #$FilePath#/load/W_DRUG_ENR.dat
write_files(
    df_Trn_MbrMtch_Lkup_Lnk_WDrugEnr,
    f"{adls_path}/load/W_DRUG_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# IDS_CLM_Update => We do a merge into {IDSOwner}.CLM by CLM_SK
# The link columns: CLM_SK (PK), LAST_UPDT_RUN_CYC_EXCTN_SK, GRP_SK
df_ids_grp_update = df_Trn_MbrMtch_Lkup_ids_grp_update

tmp_tbl_ids_clm_update = "STAGING.SavRxPClmMbrErrExtr_IDS_CLM_Update_temp"
execute_dml(f"DROP TABLE IF EXISTS {tmp_tbl_ids_clm_update}", jdbc_url_ids, jdbc_props_ids)
df_ids_grp_update.write \
    .format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", tmp_tbl_ids_clm_update) \
    .mode("overwrite") \
    .save()

merge_sql_ids_clm = f"""
MERGE INTO {IDSOwner}.CLM AS T
USING {tmp_tbl_ids_clm_update} AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.GRP_SK = S.GRP_SK
WHEN NOT MATCHED THEN
  INSERT (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, GRP_SK)
  VALUES (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_SK, S.GRP_SK);
"""
execute_dml(merge_sql_ids_clm, jdbc_url_ids, jdbc_props_ids)

# EDW_CLM_Update => We do a merge into {EDWOwner}.CLM_F by CLM_SK
# The link columns: CLM_SK (PK), LAST_UPDT_RUN_CYC_EXCTN_DT_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, GRP_SK, GRP_ID
df_edw_grp_update = df_Trn_MbrMtch_Lkup_edw_grp_update

tmp_tbl_edw_clm_update = "STAGING.SavRxPClmMbrErrExtr_EDW_CLM_Update_temp"
execute_dml(f"DROP TABLE IF EXISTS {tmp_tbl_edw_clm_update}", jdbc_url_edw, jdbc_props_edw)
df_edw_grp_update.write \
    .format("jdbc") \
    .option("url", jdbc_url_edw) \
    .options(**jdbc_props_edw) \
    .option("dbtable", tmp_tbl_edw_clm_update) \
    .mode("overwrite") \
    .save()

merge_sql_edw_clm = f"""
MERGE INTO {EDWOwner}.CLM_F AS T
USING {tmp_tbl_edw_clm_update} AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.GRP_SK = S.GRP_SK,
    T.GRP_ID = S.GRP_ID
WHEN NOT MATCHED THEN
  INSERT (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_DT_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, GRP_SK, GRP_ID)
  VALUES (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK, S.LAST_UPDT_RUN_CYC_EXCTN_SK, S.GRP_SK, S.GRP_ID);
"""
execute_dml(merge_sql_edw_clm, jdbc_url_edw, jdbc_props_edw)

# hf_savrx_errmbrclm_errlkup => scenario A => deduplicate on CLM_ID => feed SCErrFile
df_hf_savrx_errmbrclm_errlkup = df_Trn_MbrMtch_Lkup_ErrorClm.dropDuplicates(["CLM_ID"])

# SCErrFile => has 2 inputs: primary "RejectRecs" (df_ErrorFile2) + lookup "Error" => we do a left join on CLM_ID
df_SCErrFile_joined = df_ErrorFile2.alias("RejectRecs").join(
    df_hf_savrx_errmbrclm_errlkup.alias("Error"),
    on=[col("RejectRecs.CLM_ID") == col("Error.CLM_ID")],
    how="left"
)

# Transformer SCErrFile => outputs "ErrFileUpdate" + "ErrFileReport"
df_SCErrFile_ErrFileUpdate = df_SCErrFile_joined.select(
    col("Error.CLM_SK").alias("CLM_SK"),
    col("Error.CLM_ID").alias("CLM_ID"),
    col("Error.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Error.CLM_TYP_CD").alias("CLM_TYP_CD"),
    col("Error.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    col("Error.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    col("Error.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    col("Error.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    col("Error.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    col("Error.SUB_SSN").alias("SUB_SSN"),
    col("Error.PATN_LAST_NM").alias("PATN_LAST_NM"),
    col("Error.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    col("Error.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    col("Error.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    col("Error.ERR_CD").alias("ERR_CD"),
    col("Error.ERR_DESC").alias("ERR_DESC"),
    col("Error.FEP_MBR_ID").alias("FEP_MBR_ID"),
    col("Error.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    col("Error.SUB_LAST_NM").alias("SUB_LAST_NM"),
    col("Error.SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    col("Error.SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    col("Error.GRP_ID").alias("GRP_ID"),
    col("Error.FILE_DT_SK").alias("FILE_DT_SK"),
    col("Error.PATN_SSN").alias("PATN_SSN")
)

df_SCErrFile_ErrFileReport = df_SCErrFile_joined.select(
    col("Error.CLM_ID").alias("CLM_ID"),
    col("Error.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Error.CLM_TYP_CD").alias("CLM_TYP_CD"),
    col("Error.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    col("Error.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    col("Error.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    col("Error.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    col("Error.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    col("Error.SUB_SSN").alias("SUB_SSN"),
    col("Error.PATN_LAST_NM").alias("PATN_LAST_NM"),
    col("Error.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    col("Error.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    col("Error.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    col("Error.ERR_CD").alias("ERR_CD"),
    col("Error.ERR_DESC").alias("ERR_DESC"),
    col("Error.FEP_MBR_ID").alias("FEP_MBR_ID"),
    col("Error.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    col("Error.SUB_LAST_NM").alias("SUB_LAST_NM"),
    col("Error.SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    col("Error.SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    col("Error.GRP_ID").alias("GRP_ID"),
    col("Error.FILE_DT_SK").alias("FILE_DT_SK"),
    col("Error.PATN_SSN").alias("PATN_SSN")
)

# ErrFileUpdate => writes to #$FilePath#/load => P_CLM_MBRSH_ERR_RECYC_#SrcSysCd1#.dat
write_files(
    df_SCErrFile_ErrFileUpdate,
    f"{adls_path}/load/P_CLM_MBRSH_ERR_RECYC_{SrcSysCd1}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ErrFileReport => writes to #$FilePath#/external => {SrcSysCd1}_MbrMatch_MedClm_ErrorFile_Recycle.dat with header=true
write_files(
    df_SCErrFile_ErrFileReport,
    f"{adls_path_publish}/external/{SrcSysCd1}_MbrMatch_MedClm_ErrorFile_Recycle.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)