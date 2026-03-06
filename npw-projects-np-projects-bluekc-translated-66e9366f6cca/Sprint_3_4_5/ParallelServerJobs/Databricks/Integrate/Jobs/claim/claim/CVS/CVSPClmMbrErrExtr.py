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
# MAGIC DESCRIPTION :  CVS Common claims recycle process
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                  Date                 Project/Altiris #          Change Description                                                           Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                            --------------------         ------------------------          -----------------------------------------------------------------------                   --------------------------------         -------------------------------   ----------------------------      
# MAGIC Kaushik Kapoor                    2018-09-19              5828                          Original Programming                                                       IntegrateDev2                   Kalyan Neelam           2018-09-28


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, CharType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
SrcSysCdErr = get_widget_value('SrcSysCdErr','')
RunID = get_widget_value('RunID','')
RunCycle = get_widget_value('RunCycle','')
RunCycleDate = get_widget_value('RunCycleDate','')
Logging = get_widget_value('Logging','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCd1 = get_widget_value('SrcSysCd1','')
RunDate = get_widget_value('RunDate','')

# MAGIC %run ../../../../../shared_containers/PrimaryKey/PBMClaimsStep6MemMatch

schema_ErrorFile2 = StructType([
    StructField("CLM_ID", StringType(), False)
])
df_ErrorFile2 = (
    spark.read.format("csv")
    .option("header", False)
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_ErrorFile2)
    .load(f"{adls_path_publish}/external/{SrcSysCd1}_MbrMatch_RejectRecs.dat")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
qry_GRP_ID = (
    f"SELECT DISTINCT PBM_GRP_ID, GRP_ID "
    f"FROM {IDSOwner}.P_PBM_GRP_XREF XREF "
    f"WHERE UPPER(XREF.SRC_SYS_CD)='CVS' "
    f"AND '{RunDate}' BETWEEN cast(XREF.EFF_DT as date) AND cast(XREF.TERM_DT as date)"
)
df_GRP_ID = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", qry_GRP_ID)
    .load()
)
df_GrpBase_lkup = dedup_sort(df_GRP_ID, ["PBM_GRP_ID"], [])

qry_IDS_CLM = (
    f"SELECT CLM.CLM_SK "
    f"FROM {IDSOwner}.CLM CLM, {IDSOwner}.CD_MPPNG CD "
    f"WHERE CLM.SRC_SYS_CD_SK=CD.CD_MPPNG_SK "
    f"AND CD.TRGT_CD='{SrcSysCd}' "
    f"AND CLM.GRP_SK=0"
)
df_IDS_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", qry_IDS_CLM)
    .load()
)
df_IDS_CLM_lkp = dedup_sort(df_IDS_CLM, ["CLM_SK"], [])

jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
qry_EDW_CLM_F = (
    f"SELECT CLM_SK "
    f"FROM {EDWOwner}.CLM_F "
    f"WHERE SRC_SYS_CD='{SrcSysCd}' "
    f"AND GRP_SK=0"
)
df_EDW_CLM_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", qry_EDW_CLM_F)
    .load()
)
df_EDW_CLM_lkp = dedup_sort(df_EDW_CLM_F, ["CLM_SK"], [])

qry_IDS_MBR = (
    f"SELECT DISTINCT "
    f"MEMBER.MBR_UNIQ_KEY, "
    f"REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MEMBER.FIRST_NM,'-',''),'&',''),'.',''),' ',''),'''',''),'\"',''),',','') AS MBR_FIRST_NM, "
    f"REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MEMBER.LAST_NM,'-',''),'&',''),'.',''),' ',''),'''',''),'\"',''),',','') AS MBR_LAST_NM, "
    f"MEMBER.SSN AS MBR_SSN, "
    f"MEMBER.BRTH_DT_SK AS MBR_BRTH_DT_SK, "
    f"MEMBER.GNDR_CD, "
    f"REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(SUBSCRIBER.FIRST_NM,'-',''),'&',''),'.',''),' ',''),'''',''),'\"',''),',','') AS SUB_FIRST_NM, "
    f"REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(SUBSCRIBER.LAST_NM,'-',''),'&',''),'.',''),' ',''),'''',''),'\"',''),',','') AS SUB_LAST_NM, "
    f"SUBSCRIBER.SSN AS SUB_SSN, "
    f"SUBSCRIBER.BRTH_DT_SK AS SUB_BRTH_DT_SK, "
    f"MEMBER.GRP_ID, "
    f"MEMBER.SUB_ID, "
    f"MEMBER.MBR_SFX_NO "
    f"FROM (SELECT "
    f"MBR.MBR_UNIQ_KEY, "
    f"MBR.FIRST_NM, "
    f"MBR.LAST_NM, "
    f"MBR.SSN, "
    f"MBR.BRTH_DT_SK, "
    f"CD.TRGT_CD GNDR_CD, "
    f"MBR.SUB_SK, "
    f"GRP.GRP_ID, "
    f"SUB.SUB_ID, "
    f"MBR.MBR_SFX_NO "
    f"FROM {IDSOwner}.MBR MBR, "
    f"{IDSOwner}.SUB SUB, "
    f"{IDSOwner}.GRP GRP, "
    f"{IDSOwner}.CD_MPPNG CD, "
    f"{IDSOwner}.MBR_ENR ENR, "
    f"{IDSOwner}.CD_MPPNG CD1, "
    f"{IDSOwner}.P_PBM_GRP_XREF XREF "
    f"WHERE MBR.MBR_SK = ENR.MBR_SK "
    f"AND MBR.SUB_SK = SUB.SUB_SK "
    f"AND SUB.GRP_SK = GRP.GRP_SK "
    f"AND MBR.MBR_GNDR_CD_SK = CD.CD_MPPNG_SK "
    f"AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK "
    f"AND GRP.GRP_ID = XREF.GRP_ID "
    f"AND UPPER(XREF.SRC_SYS_CD)='CVS' "
    f"AND '{RunDate}' BETWEEN cast(XREF.EFF_DT as date) AND cast(XREF.TERM_DT as date) "
    f"AND CD1.TRGT_CD='MED' "
    f"AND UPPER(MBR.LAST_NM) NOT LIKE '%DO NOT USE%') MEMBER "
    f"LEFT OUTER JOIN "
    f"(SELECT "
    f"MBR1.MBR_UNIQ_KEY, "
    f"MBR1.FIRST_NM, "
    f"MBR1.LAST_NM, "
    f"MBR1.SSN, "
    f"MBR1.BRTH_DT_SK, "
    f"MBR1.SUB_SK "
    f"FROM {IDSOwner}.MBR MBR1, "
    f"{IDSOwner}.CD_MPPNG CD1 "
    f"WHERE MBR1.MBR_RELSHP_CD_SK = CD1.CD_MPPNG_SK "
    f"AND CD1.TRGT_CD='SUB' "
    f"AND UPPER(MBR1.LAST_NM) NOT LIKE '%DO NOT USE%') SUBSCRIBER "
    f"ON MEMBER.SUB_SK = SUBSCRIBER.SUB_SK"
)
df_IDS_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", qry_IDS_MBR)
    .load()
)

df_Transformer_265_Step1 = df_IDS_MBR.select(
    F.col("SUB_SSN").alias("SUB_SSN"),
    UpCase(trim(F.col("MBR_FIRST_NM")).substr(F.lit(1), F.lit(4))).alias("MBR_FIRST_NM"),
    UpCase(trim(F.col("MBR_LAST_NM")).substr(F.lit(1), F.lit(4))).alias("MBR_LAST_NM"),
    F.col("MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    UpCase(trim(F.col("GNDR_CD"))).alias("GNDR_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_SSN").alias("MBR_SSN"),
    UpCase(trim(F.col("SUB_FIRST_NM"))).alias("SUB_FIRST_NM"),
    UpCase(trim(F.col("SUB_LAST_NM"))).alias("SUB_LAST_NM"),
    F.col("SUB_BRTH_DT_SK").alias("SUB_BRTH_DT_SK"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO")
)
df_Transformer_265_Step2 = df_IDS_MBR.select(
    UpCase(trim(F.col("MBR_FIRST_NM")).substr(F.lit(1), F.lit(4))).alias("MBR_FIRST_NM"),
    UpCase(trim(F.col("MBR_LAST_NM")).substr(F.lit(1), F.lit(4))).alias("MBR_LAST_NM"),
    F.col("MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    UpCase(trim(F.col("GNDR_CD"))).alias("GNDR_CD"),
    UpCase(trim(F.col("SUB_LAST_NM")).substr(F.lit(1), F.lit(4))).alias("SUB_LAST_NM"),
    UpCase(trim(F.col("SUB_FIRST_NM"))).alias("SUB_FIRST_NM"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_SSN").alias("MBR_SSN"),
    F.col("SUB_SSN").alias("SUB_SSN"),
    F.col("SUB_BRTH_DT_SK").alias("SUB_BRTH_DT_SK"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO")
)
df_Transformer_265_Step3 = df_IDS_MBR.select(
    UpCase(trim(F.col("MBR_FIRST_NM")).substr(F.lit(1), F.lit(4))).alias("MBR_FIRST_NM"),
    UpCase(trim(F.col("MBR_LAST_NM")).substr(F.lit(1), F.lit(4))).alias("MBR_LAST_NM"),
    F.col("MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    UpCase(trim(F.col("SUB_FIRST_NM")).substr(F.lit(1), F.lit(4))).alias("SUB_FIRST_NM"),
    UpCase(trim(F.col("SUB_LAST_NM")).substr(F.lit(1), F.lit(4))).alias("SUB_LAST_NM"),
    F.col("GNDR_CD").alias("GNDR_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_SSN").alias("MBR_SSN"),
    F.col("SUB_SSN").alias("SUB_SSN"),
    F.col("SUB_BRTH_DT_SK").alias("SUB_BRTH_DT_SK"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO")
)
df_Transformer_265_Step4 = df_IDS_MBR.select(
    UpCase(trim(F.col("MBR_FIRST_NM")).substr(F.lit(1), F.lit(4))).alias("MBR_FIRST_NM"),
    UpCase(trim(F.col("MBR_LAST_NM")).substr(F.lit(1), F.lit(4))).alias("MBR_LAST_NM"),
    F.col("SUB_SSN").alias("SUB_SSN"),
    UpCase(trim(F.col("SUB_FIRST_NM")).substr(F.lit(1), F.lit(4))).alias("SUB_FIRST_NM"),
    UpCase(trim(F.col("SUB_LAST_NM")).substr(F.lit(1), F.lit(4))).alias("SUB_LAST_NM"),
    F.col("MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    UpCase(trim(F.col("GNDR_CD"))).alias("GNDR_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_SSN").alias("MBR_SSN"),
    F.col("SUB_BRTH_DT_SK").alias("SUB_BRTH_DT_SK"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO")
)
df_Transformer_265_Step5 = df_IDS_MBR.select(
    F.col("SUB_SSN").alias("SUB_SSN"),
    UpCase(trim(F.col("MBR_FIRST_NM")).substr(F.lit(1), F.lit(4))).alias("MBR_FIRST_NM"),
    F.col("MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    UpCase(trim(F.col("GNDR_CD"))).alias("GNDR_CD"),
    UpCase(trim(F.col("MBR_LAST_NM"))).alias("MBR_LAST_NM"),
    UpCase(trim(F.col("SUB_FIRST_NM"))).alias("SUB_FIRST_NM"),
    UpCase(trim(F.col("SUB_LAST_NM"))).alias("SUB_LAST_NM"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_SSN").alias("MBR_SSN"),
    F.col("SUB_BRTH_DT_SK").alias("SUB_BRTH_DT_SK"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO")
)
df_Transformer_265_Step6 = df_IDS_MBR.select(
    F.col("SUB_SSN").alias("SUB_SSN"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    UpCase(trim(F.col("GNDR_CD"))).alias("GNDR_CD"),
    UpCase(trim(F.col("MBR_FIRST_NM"))).alias("MBR_FIRST_NM"),
    UpCase(trim(F.col("MBR_LAST_NM"))).alias("MBR_LAST_NM"),
    UpCase(trim(F.col("SUB_FIRST_NM"))).alias("SUB_FIRST_NM"),
    UpCase(trim(F.col("SUB_LAST_NM"))).alias("SUB_LAST_NM"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_SSN").alias("MBR_SSN"),
    F.col("SUB_BRTH_DT_SK").alias("SUB_BRTH_DT_SK"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.lit("NA").alias("PBM_GRP_ID")
)
df_Transformer_265_Step1 = dedup_sort(
    df_Transformer_265_Step1,
    ["SUB_SSN","MBR_FIRST_NM","MBR_LAST_NM","MBR_BRTH_DT_SK","GNDR_CD"],
    []
)
df_Transformer_265_Step2 = dedup_sort(
    df_Transformer_265_Step2,
    ["MBR_FIRST_NM","MBR_LAST_NM","MBR_BRTH_DT_SK","GNDR_CD","SUB_LAST_NM"],
    []
)
df_Transformer_265_Step3 = dedup_sort(
    df_Transformer_265_Step3,
    ["MBR_FIRST_NM","MBR_LAST_NM","MBR_BRTH_DT_SK","SUB_FIRST_NM","SUB_LAST_NM"],
    []
)
df_Transformer_265_Step4 = dedup_sort(
    df_Transformer_265_Step4,
    ["MBR_FIRST_NM","MBR_LAST_NM","SUB_SSN","SUB_FIRST_NM","SUB_LAST_NM"],
    []
)
df_Transformer_265_Step5 = dedup_sort(
    df_Transformer_265_Step5,
    ["SUB_SSN","MBR_FIRST_NM","MBR_BRTH_DT_SK","GNDR_CD"],
    []
)
df_Transformer_265_Step6 = dedup_sort(
    df_Transformer_265_Step6,
    ["SUB_SSN","GRP_ID","MBR_BRTH_DT_SK","GNDR_CD"],
    []
)
params = {}
df_Step6_lkp = PBMClaimsStep6MemMatch(df_Transformer_265_Step6, params)

qry_IDS_P_CLM_ERR = (
    f"SELECT CLM_SK, CLM_ID, SRC_SYS_CD, CLM_TYP_CD, CLM_SUBTYP_CD, CLM_SVC_STRT_DT_SK, "
    f"SRC_SYS_GRP_PFX, SRC_SYS_GRP_ID, SRC_SYS_GRP_SFX, SUB_SSN, PATN_LAST_NM, PATN_FIRST_NM, "
    f"PATN_GNDR_CD, PATN_BRTH_DT_SK, ERR_CD, ERR_DESC, FEP_MBR_ID, SUB_FIRST_NM, SUB_LAST_NM, "
    f"SRC_SYS_SUB_ID, SRC_SYS_MBR_SFX_NO, GRP_ID, FILE_DT_SK, PATN_SSN "
    f"FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC "
    f"WHERE SRC_SYS_CD='{SrcSysCdErr}'"
)
df_IDS_P_CLM_ERR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", qry_IDS_P_CLM_ERR)
    .load()
)
df_hf_cvs_ids_p_clm_err = dedup_sort(
    df_IDS_P_CLM_ERR,
    ["CLM_SK"],
    []
)

df_Trn_Mbr_Mtch = df_hf_cvs_ids_p_clm_err

df_Trn_Mbr_Mtch_Primary = df_Trn_Mbr_Mtch.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    trim(F.col("SRC_SYS_GRP_ID")).alias("SRC_SYS_GRP_ID"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    F.col("SUB_SSN").alias("SUB_SSN"),
    F.col("PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.col("CLM_SK").alias("__TMP_CARRY__CLM_SK"),
    F.col("CLM_TYP_CD").alias("__TMP_CARRY__CLM_TYP_CD"),
    F.col("CLM_SUBTYP_CD").alias("__TMP_CARRY__CLM_SUBTYP_CD"),
    F.col("SRC_SYS_GRP_PFX").alias("__TMP_CARRY__SRC_SYS_GRP_PFX"),
    F.col("SRC_SYS_GRP_SFX").alias("__TMP_CARRY__SRC_SYS_GRP_SFX"),
    F.col("ERR_CD").alias("__TMP_CARRY__ERR_CD"),
    F.col("ERR_DESC").alias("__TMP_CARRY__ERR_DESC"),
    F.col("FEP_MBR_ID").alias("__TMP_CARRY__FEP_MBR_ID"),
    F.col("SRC_SYS_SUB_ID").alias("__TMP_CARRY__SRC_SYS_SUB_ID"),
    F.col("SRC_SYS_MBR_SFX_NO").alias("__TMP_CARRY__SRC_SYS_MBR_SFX_NO"),
    F.col("GRP_ID").alias("__TMP_CARRY__GRP_ID"),
    F.col("FILE_DT_SK").alias("__TMP_CARRY__FILE_DT_SK"),
    F.col("PATN_SSN").alias("__TMP_CARRY__PATN_SSN")
)

df_hf_cvs_errclm_mbrmatch_land = df_Trn_Mbr_Mtch.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    trim(F.col("SRC_SYS_GRP_ID")).alias("SRC_SYS_GRP_ID"),
    F.col("SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    F.col("SUB_SSN").alias("SUB_SSN"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.col("PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    F.col("ERR_CD").alias("ERR_CD"),
    F.col("ERR_DESC").alias("ERR_DESC"),
    F.col("FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    F.col("SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("PATN_SSN").alias("PATN_SSN")
)

df_Trn_Mbr_Mtch_Next = df_Trn_Mbr_Mtch_Primary.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    F.col("SUB_SSN").alias("SUB_SSN"),
    F.col("PATN_GNDR_CD").alias("PATN_GNDR_CD")
)

df_Trn_Mbr_Mtch_Err_Clm_To_Hf = df_Trn_Mbr_Mtch.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    trim(F.col("SRC_SYS_GRP_ID")).alias("SRC_SYS_GRP_ID"),
    F.col("SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    F.col("SUB_SSN").alias("SUB_SSN"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.col("PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    F.col("ERR_CD").alias("ERR_CD"),
    F.col("ERR_DESC").alias("ERR_DESC"),
    F.col("FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    F.col("SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("PATN_SSN").alias("PATN_SSN")
)

df_hf_cvs_errclm_mbrmatch_land_dedup = dedup_sort(
    df_hf_cvs_errclm_mbrmatch_land,
    ["CLM_SK"],
    []
)

df_Trn_Mbr_Mtch_Next_Primary = df_Trn_Mbr_Mtch_Next.select(
    F.col("CLM_ID"),
    F.col("SRC_SYS_CD"),
    F.col("CLM_SVC_STRT_DT_SK"),
    F.col("SRC_SYS_GRP_ID"),
    F.col("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM"),
    F.col("SUB_FIRST_NM"),
    F.col("SUB_LAST_NM"),
    F.col("PATN_BRTH_DT_SK"),
    F.col("SUB_SSN"),
    F.col("PATN_GNDR_CD")
)

df_Trn_Mbr_Mtch_Next_Step = df_Trn_Mbr_Mtch_Next_Primary.select(
    UpCase(Convert(F.lit('\r')+F.lit('\n')+F.lit('\t'),'', trim(F.col("PATN_FIRST_NM")))).alias("PATN_FIRST_NM"),
    UpCase(Convert(F.lit('\r')+F.lit('\n')+F.lit('\t'),'', trim(F.col("PATN_LAST_NM")))).alias("PATN_LAST_NM"),
    trim(F.col("PATN_BRTH_DT_SK")).alias("BRTH_DT"),
    trim(F.col("SUB_SSN")).alias("CARDHLDR_ID_NO"),
    trim(F.col("SRC_SYS_GRP_ID")).alias("GRP_NO"),
    UpCase(Convert(F.lit('\r')+F.lit('\n')+F.lit('\t'),'', trim(F.col("SUB_FIRST_NM")))).alias("CARDHLDR_FIRST_NM"),
    UpCase(Convert(F.lit('\r')+F.lit('\n')+F.lit('\t'),'', trim(F.col("SUB_LAST_NM")))).alias("CARDHLDR_LAST_NM"),
    trim(F.col("SUB_SSN")).alias("CARDHLDR_SSN"),
    trim(F.col("PATN_GNDR_CD")).alias("GNDR_CD"),
    trim(F.col("CLM_ID")).alias("CLM_ID"),
    trim(F.col("CLM_SVC_STRT_DT_SK")).alias("CLM_SVC_STRT_DT_SK")
)
df_Transformer_6_SavRx = dedup_sort(df_Trn_Mbr_Mtch_Next_Step, [], [])

df_MemberMatch_Step1_lkp = df_Transformer_265_Step1
df_MemberMatch_Step2_lkp = df_Transformer_265_Step2
df_MemberMatch_Step3_lkp = df_Transformer_265_Step3
df_MemberMatch_Step4_lkp = df_Transformer_265_Step4
df_MemberMatch_Step5_lkp = df_Transformer_265_Step5
df_MemberMatch_Step6_lkp = df_Step6_lkp

df_MemberMatch_SavRx = df_Transformer_6_SavRx
df_MemberMatch = df_MemberMatch_SavRx.alias("SavRx")

df_Lookup_Step1_lkp = df_MemberMatch_Step1_lkp.alias("Step1_lkp")
df_Lookup_Step2_lkp = df_MemberMatch_Step2_lkp.alias("Step2_lkp")
df_Lookup_Step3_lkp = df_MemberMatch_Step3_lkp.alias("Step3_lkp")
df_Lookup_Step4_lkp = df_MemberMatch_Step4_lkp.alias("Step4_lkp")
df_Lookup_Step5_lkp = df_MemberMatch_Step5_lkp.alias("Step5_lkp")
df_Lookup_Step6_lkp = df_MemberMatch_Step6_lkp.alias("Step6_lkp")

cond1 = [
    F.col("SavRx.CARDHLDR_SSN") == F.col("Step1_lkp.SUB_SSN"),
    F.col("SavRx.PATN_FIRST_NM").substr(F.lit(1),F.lit(4)) == F.col("Step1_lkp.MBR_FIRST_NM"),
    F.col("SavRx.PATN_LAST_NM").substr(F.lit(1),F.lit(4)) == F.col("Step1_lkp.MBR_LAST_NM"),
    F.col("SavRx.BRTH_DT") == F.col("Step1_lkp.MBR_BRTH_DT_SK"),
    UpCase(trim(F.col("SavRx.GNDR_CD"))) == F.col("Step1_lkp.GNDR_CD")
]
df_join_1 = df_MemberMatch.join(df_Lookup_Step1_lkp, cond1, "left")

cond2 = [
    F.col("SavRx.PATN_FIRST_NM").substr(F.lit(1),F.lit(4)) == F.col("Step2_lkp.MBR_FIRST_NM"),
    F.col("SavRx.PATN_LAST_NM").substr(F.lit(1),F.lit(4)) == F.col("Step2_lkp.MBR_LAST_NM"),
    F.col("SavRx.BRTH_DT") == F.col("Step2_lkp.MBR_BRTH_DT_SK"),
    UpCase(trim(F.col("SavRx.GNDR_CD"))) == F.col("Step2_lkp.GNDR_CD"),
    F.col("SavRx.CARDHLDR_LAST_NM").substr(F.lit(1),F.lit(4)) == F.col("Step2_lkp.SUB_LAST_NM"),
    F.col("SavRx.CARDHLDR_FIRST_NM").substr(F.lit(1),F.lit(4)) == F.col("Step2_lkp.SUB_FIRST_NM")
]
df_join_2 = df_join_1.join(df_Lookup_Step2_lkp, cond2, "left")

cond3 = [
    F.col("SavRx.PATN_FIRST_NM").substr(F.lit(1),F.lit(4)) == F.col("Step3_lkp.MBR_FIRST_NM"),
    F.col("SavRx.PATN_LAST_NM").substr(F.lit(1),F.lit(4)) == F.col("Step3_lkp.MBR_LAST_NM"),
    F.col("SavRx.BRTH_DT") == F.col("Step3_lkp.MBR_BRTH_DT_SK"),
    F.col("SavRx.CARDHLDR_FIRST_NM").substr(F.lit(1),F.lit(4)) == F.col("Step3_lkp.SUB_FIRST_NM"),
    F.col("SavRx.CARDHLDR_LAST_NM").substr(F.lit(1),F.lit(4)) == F.col("Step3_lkp.SUB_LAST_NM")
]
df_join_3 = df_join_2.join(df_Lookup_Step3_lkp, cond3, "left")

cond4 = [
    F.col("SavRx.PATN_FIRST_NM").substr(F.lit(1),F.lit(4)) == F.col("Step4_lkp.MBR_FIRST_NM"),
    F.col("SavRx.PATN_LAST_NM").substr(F.lit(1),F.lit(4)) == F.col("Step4_lkp.MBR_LAST_NM"),
    F.col("SavRx.CARDHLDR_SSN") == F.col("Step4_lkp.SUB_SSN"),
    F.col("SavRx.CARDHLDR_FIRST_NM").substr(F.lit(1),F.lit(4)) == F.col("Step4_lkp.SUB_FIRST_NM"),
    F.col("SavRx.CARDHLDR_LAST_NM").substr(F.lit(1),F.lit(4)) == F.col("Step4_lkp.SUB_LAST_NM")
]
df_join_4 = df_join_3.join(df_Lookup_Step4_lkp, cond4, "left")

cond5 = [
    F.col("SavRx.CARDHLDR_SSN") == F.col("Step5_lkp.SUB_SSN"),
    F.col("SavRx.PATN_FIRST_NM").substr(F.lit(1),F.lit(4)) == F.col("Step5_lkp.MBR_FIRST_NM"),
    F.col("SavRx.BRTH_DT") == F.col("Step5_lkp.MBR_BRTH_DT_SK"),
    UpCase(trim(F.col("SavRx.GNDR_CD"))) == F.col("Step5_lkp.GNDR_CD")
]
df_join_5 = df_join_4.join(df_Lookup_Step5_lkp, cond5, "left")

cond6 = [
    F.col("SavRx.CARDHLDR_SSN") == F.col("Step6_lkp.SUB_SSN"),
    F.col("SavRx.BRTH_DT") == F.col("Step6_lkp.MBR_BRTH_DT_SK"),
    UpCase(trim(F.col("SavRx.GNDR_CD"))) == F.col("Step6_lkp.GNDR_CD")
]
df_join_6 = df_join_5.join(df_Lookup_Step6_lkp, cond6, "left")

df_MemberMatch_Final = df_join_6.select(
    F.col("SavRx.CLM_ID").alias("CLM_ID"),
    F.col("SavRx.CLM_SVC_STRT_DT_SK").alias("FILL_DT_SK"),
    F.when(
        (F.col("Step1_lkp.MBR_UNIQ_KEY").isNotNull())
        | (F.col("Step2_lkp.MBR_UNIQ_KEY").isNotNull())
        | (F.col("Step3_lkp.MBR_UNIQ_KEY").isNotNull())
        | (F.col("Step4_lkp.MBR_UNIQ_KEY").isNotNull())
        | (F.col("Step5_lkp.MBR_UNIQ_KEY").isNotNull())
        | ((F.col("Step6_lkp.MBR_UNIQ_KEY").isNotNull()) & (F.col("Step6_lkp.CNT") == 1)),
        F.when(F.col("Step1_lkp.MBR_UNIQ_KEY").isNotNull(), F.col("Step1_lkp.MBR_UNIQ_KEY"))
        .when(F.col("Step2_lkp.MBR_UNIQ_KEY").isNotNull(), F.col("Step2_lkp.MBR_UNIQ_KEY"))
        .when(F.col("Step3_lkp.MBR_UNIQ_KEY").isNotNull(), F.col("Step3_lkp.MBR_UNIQ_KEY"))
        .when(F.col("Step4_lkp.MBR_UNIQ_KEY").isNotNull(), F.col("Step4_lkp.MBR_UNIQ_KEY"))
        .when(F.col("Step5_lkp.MBR_UNIQ_KEY").isNotNull(), F.col("Step5_lkp.MBR_UNIQ_KEY"))
        .when((F.col("Step6_lkp.MBR_UNIQ_KEY").isNotNull()) & (F.col("Step6_lkp.CNT") == 1),
              F.col("Step6_lkp.MBR_UNIQ_KEY"))
        .otherwise(F.lit("UNK"))
    ).alias("MBR_UNIQ_KEY")
)

df_MemberMatch_Land = df_MemberMatch_Final.filter(F.col("MBR_UNIQ_KEY") != F.lit("UNK"))
df_MemberMatch_Reject = df_MemberMatch_Final.filter(F.col("MBR_UNIQ_KEY") == F.lit("UNK"))

df_hf_cvs_mbrmatch_drugenrmatch_dedupe = dedup_sort(
    df_MemberMatch_Land,
    ["CLM_ID","FILL_DT_SK","MBR_UNIQ_KEY"],
    []
)

df_W_DRUG_ENR_MATCH = df_hf_cvs_mbrmatch_drugenrmatch_dedupe.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FILL_DT_SK").alias("FILL_DT_SK"),
    F.col("MBR_UNIQ_KEY").cast(IntegerType()).alias("MBR_UNIQ_KEY")
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.CVSPClmMbrErrExtr_W_DRUG_ENR_MATCH_temp", jdbc_url_ids, jdbc_props_ids)
df_W_DRUG_ENR_MATCH.write.format("jdbc").option("url", jdbc_url_ids).options(**jdbc_props_ids)\
    .option("dbtable", "STAGING.CVSPClmMbrErrExtr_W_DRUG_ENR_MATCH_temp").mode("overwrite").save()
execute_dml(
    f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'W_DRUG_ENR_MATCH')",
    jdbc_url_ids,
    jdbc_props_ids
)
merge_sql_w_drug_enr_match = (
    "MERGE INTO {owner}.W_DRUG_ENR_MATCH AS T "
    "USING STAGING.CVSPClmMbrErrExtr_W_DRUG_ENR_MATCH_temp AS S "
    "ON T.CLM_ID=S.CLM_ID AND T.FILL_DT_SK=S.FILL_DT_SK AND T.MBR_UNIQ_KEY=S.MBR_UNIQ_KEY "
    "WHEN MATCHED THEN UPDATE SET "
    "CLM_ID=S.CLM_ID,FILL_DT_SK=S.FILL_DT_SK,MBR_UNIQ_KEY=S.MBR_UNIQ_KEY "
    "WHEN NOT MATCHED THEN INSERT (CLM_ID,FILL_DT_SK,MBR_UNIQ_KEY) "
    "VALUES (S.CLM_ID,S.FILL_DT_SK,S.MBR_UNIQ_KEY);"
).format(owner=IDSOwner)
execute_dml(merge_sql_w_drug_enr_match, jdbc_url_ids, jdbc_props_ids)

df_hf_cvs_ids_p_clm_err_Reject = df_MemberMatch_Reject.select(
    F.col("CLM_ID").alias("CLM_ID")
)
write_files(
    df_hf_cvs_ids_p_clm_err_Reject,
    f"{adls_path_publish}/external/{SrcSysCd1}_MbrMatch_RejectRecs.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_hf_cvs_errclm_mbrmatch_mbrlkup = dedup_sort(
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query",
        f"SELECT CLM_ID, MBR_UNIQ_KEY, GRP_ID, SUB_ID, MBR_SFX_NO, SUB_UNIQ_KEY, EFF_DT_SK "
        f"FROM {IDSOwner}.W_DRUG_ENR_MATCH") 
    .load(),
    ["CLM_ID"],
    []
)

df_Trn_MbrMtch_Lkup_Lnk_ErrClm = df_hf_cvs_errclm_mbrmatch_land_dedup
df_Trn_MbrMtch_Lkup_Lnk_Mbr_Lkup = df_hf_cvs_errclm_mbrmatch_mbrlkup
df_Trn_MbrMtch_Lkup_GrpBase_lkup = df_GrpBase_lkup
df_Trn_MbrMtch_Lkup_IDS_CLM = df_IDS_CLM_lkp
df_Trn_MbrMtch_Lkup_EDW_CLM = df_EDW_CLM_lkp

df_Trn_MbrMtch_Lkup_join_1 = df_Trn_MbrMtch_Lkup_Lnk_ErrClm.alias("Lnk_ErrClm").join(
    df_Trn_MbrMtch_Lkup_Lnk_Mbr_Lkup.alias("Lnk_Mbr_Lkup"),
    F.col("Lnk_ErrClm.CLM_ID") == F.col("Lnk_Mbr_Lkup.CLM_ID"),
    "left"
)
df_Trn_MbrMtch_Lkup_join_2 = df_Trn_MbrMtch_Lkup_join_1.join(
    df_Trn_MbrMtch_Lkup_GrpBase_lkup.alias("GrpBase_lkup"),
    F.col("Lnk_ErrClm.SRC_SYS_GRP_ID") == F.col("GrpBase_lkup.PBM_GRP_ID"),
    "left"
)
df_Trn_MbrMtch_Lkup_join_3 = df_Trn_MbrMtch_Lkup_join_2.join(
    df_Trn_MbrMtch_Lkup_IDS_CLM.alias("IDS_CLM"),
    F.col("Lnk_ErrClm.CLM_SK") == F.col("IDS_CLM.CLM_SK"),
    "left"
)
df_Trn_MbrMtch_Lkup_join_4 = df_Trn_MbrMtch_Lkup_join_3.join(
    df_Trn_MbrMtch_Lkup_EDW_CLM.alias("EDW_CLM"),
    F.col("Lnk_ErrClm.CLM_SK") == F.col("EDW_CLM.CLM_SK"),
    "left"
)

df_Trn_MbrMtch_Lkup = df_Trn_MbrMtch_Lkup_join_4

df_W_DRUG_ENR = df_Trn_MbrMtch_Lkup.filter(F.col("Lnk_Mbr_Lkup.MBR_UNIQ_KEY").isNotNull()).select(
    F.col("Lnk_ErrClm.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_ErrClm.CLM_SVC_STRT_DT_SK").alias("FILL_DT_SK"),
    F.col("Lnk_Mbr_Lkup.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)
df_CVSErrClmLand = df_Trn_MbrMtch_Lkup.filter(F.col("Lnk_Mbr_Lkup.MBR_UNIQ_KEY").isNotNull()).select(
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
    F.when(F.col("Lnk_Mbr_Lkup.MBR_UNIQ_KEY").isNull(), F.lit("0")).otherwise(F.col("Lnk_Mbr_Lkup.MBR_UNIQ_KEY")).alias("MBR_UNIQ_KEY"),
    F.when(
        F.col("GrpBase_lkup.GRP_ID").isNotNull(),
        F.col("GrpBase_lkup.GRP_ID")
    ).otherwise(
        F.when(F.col("Lnk_Mbr_Lkup.GRP_ID").isNotNull(), F.col("Lnk_Mbr_Lkup.GRP_ID"))
         .otherwise(F.lit("UNK"))
    ).alias("GRP_ID"),
    F.when(F.col("Lnk_Mbr_Lkup.SUB_ID").isNull(), F.lit("0")).otherwise(F.col("Lnk_Mbr_Lkup.SUB_ID")).alias("SUB_ID"),
    F.when(F.col("Lnk_Mbr_Lkup.MBR_SFX_NO").isNull(), F.lit("0")).otherwise(F.col("Lnk_Mbr_Lkup.MBR_SFX_NO")).alias("MBR_SFX_NO"),
    F.when(F.col("Lnk_Mbr_Lkup.SUB_UNIQ_KEY").isNotNull(), F.col("Lnk_Mbr_Lkup.SUB_UNIQ_KEY")).otherwise(F.lit("0")).alias("SUB_UNIQ_KEY"),
    F.when(F.col("Lnk_Mbr_Lkup.EFF_DT_SK").isNull(), F.lit("1753-01-01")).otherwise(F.col("Lnk_Mbr_Lkup.EFF_DT_SK")).alias("EFF_DT_SK")
)
df_ErrorClm = df_Trn_MbrMtch_Lkup.select(
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
    F.when(
        F.col("GrpBase_lkup.GRP_ID").isNotNull(),
        F.col("GrpBase_lkup.GRP_ID")
    ).otherwise(
        F.when(F.col("Lnk_Mbr_Lkup.GRP_ID").isNotNull(),F.col("Lnk_Mbr_Lkup.GRP_ID"))
         .otherwise(
             F.when(F.col("Lnk_ErrClm.GRP_ID").isNotNull(),F.col("Lnk_Mbr_Lkup.GRP_ID"))
              .otherwise(F.lit("UNK"))
         )
    ).alias("GRP_ID"),
    F.col("Lnk_ErrClm.FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("Lnk_ErrClm.PATN_SSN").alias("PATN_SSN")
)

df_ids_grp_update = df_Trn_MbrMtch_Lkup.filter(
    ((F.length(F.trim(F.col("Lnk_ErrClm.GRP_ID"))) == 0) | (F.col("Lnk_ErrClm.GRP_ID").isNull()))
    & (F.col("GrpBase_lkup.GRP_ID").isNotNull())
    & (F.col("IDS_CLM.CLM_SK").isNotNull())
).select(
    F.col("Lnk_ErrClm.CLM_SK").alias("CLM_SK"),
    F.col("RunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    GetFkeyGrp(F.col("svSrcSysCd"), F.col("Lnk_ErrClm.CLM_SK"), F.col("svGrpId"), F.col("Logging")).alias("GRP_SK")
)
execute_dml(f"DROP TABLE IF EXISTS STAGING.CVSPClmMbrErrExtr_IDS_CLM_Update_temp", jdbc_url_ids, jdbc_props_ids)
df_ids_grp_update.write.format("jdbc").option("url", jdbc_url_ids).options(**jdbc_props_ids)\
    .option("dbtable", "STAGING.CVSPClmMbrErrExtr_IDS_CLM_Update_temp").mode("overwrite").save()
merge_sql_ids_clm_update = (
    "MERGE INTO {owner}.CLM AS T "
    "USING STAGING.CVSPClmMbrErrExtr_IDS_CLM_Update_temp AS S "
    "ON T.CLM_SK=S.CLM_SK "
    "WHEN MATCHED THEN UPDATE SET T.LAST_UPDT_RUN_CYC_EXCTN_SK=S.LAST_UPDT_RUN_CYC_EXCTN_SK, T.GRP_SK=S.GRP_SK "
    "WHEN NOT MATCHED THEN INSERT (CLM_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,GRP_SK) "
    "VALUES (S.CLM_SK,S.LAST_UPDT_RUN_CYC_EXCTN_SK,S.GRP_SK);"
).format(owner=IDSOwner)
execute_dml(merge_sql_ids_clm_update, jdbc_url_ids, jdbc_props_ids)

df_edw_grp_update = df_Trn_MbrMtch_Lkup.filter(
    ((F.length(F.trim(F.col("Lnk_ErrClm.GRP_ID"))) == 0) | (F.col("Lnk_ErrClm.GRP_ID").isNull()))
    & (F.col("GrpBase_lkup.GRP_ID").isNotNull())
    & (F.col("EDW_CLM.CLM_SK").isNotNull())
).select(
    F.col("Lnk_ErrClm.CLM_SK").alias("CLM_SK"),
    F.col("RunCycleDate").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("RunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    GetFkeyGrp(F.col("svSrcSysCd"), F.col("Lnk_ErrClm.CLM_SK"), F.col("svGrpId"), F.col("Logging")).alias("GRP_SK"),
    F.col("svGrpId").alias("GRP_ID")
)
execute_dml(f"DROP TABLE IF EXISTS STAGING.CVSPClmMbrErrExtr_EDW_CLM_Update_temp", jdbc_url_edw, jdbc_props_edw)
df_edw_grp_update.write.format("jdbc").option("url", jdbc_url_edw).options(**jdbc_props_edw)\
    .option("dbtable", "STAGING.CVSPClmMbrErrExtr_EDW_CLM_Update_temp").mode("overwrite").save()
merge_sql_edw_clm_update = (
    "MERGE INTO {owner}.CLM_F AS T "
    "USING STAGING.CVSPClmMbrErrExtr_EDW_CLM_Update_temp AS S "
    "ON T.CLM_SK=S.CLM_SK "
    "WHEN MATCHED THEN UPDATE SET "
    "T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK=S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK, "
    "T.LAST_UPDT_RUN_CYC_EXCTN_SK=S.LAST_UPDT_RUN_CYC_EXCTN_SK, "
    "T.GRP_SK=S.GRP_SK, "
    "T.GRP_ID=S.GRP_ID "
    "WHEN NOT MATCHED THEN INSERT (CLM_SK,LAST_UPDT_RUN_CYC_EXCTN_DT_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,GRP_SK,GRP_ID) "
    "VALUES (S.CLM_SK,S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,S.LAST_UPDT_RUN_CYC_EXCTN_SK,S.GRP_SK,S.GRP_ID);"
).format(owner=EDWOwner)
execute_dml(merge_sql_edw_clm_update, jdbc_url_edw, jdbc_props_edw)

df_hf_cvs_errmbrclm_errlkup = dedup_sort(df_ErrorClm, ["CLM_ID"], [])

df_SCErrFile_RejectRecs = df_ErrorFile2.alias("RejectRecs")
df_SCErrFile_Error = df_hf_cvs_errmbrclm_errlkup.alias("Error")

cond_err = [F.col("RejectRecs.CLM_ID") == F.col("Error.CLM_ID")]
df_SCErrFile_join = df_SCErrFile_RejectRecs.join(df_SCErrFile_Error, cond_err, "left")

df_SCErrFile_ErrFileUpdate = df_SCErrFile_join.select(
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
df_SCErrFile_ErrFileReport = df_SCErrFile_join.select(
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

write_files(
    df_SCErrFile_ErrFileUpdate,
    f"{adls_path}/load/P_CLM_MBRSH_ERR_RECYC_{SrcSysCd1}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_SCErrFile_ErrFileReport,
    f"{adls_path_publish}/external/{SrcSysCd1}_MbrMatch_MedClm_ErrorFile_Recycle.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_CVSErrClmLand,
    f"{adls_path}/verified/{SrcSysCd1}_ErrClm_Landing.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_W_DRUG_ENR,
    f"{adls_path}/load/W_DRUG_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)