# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION :  SavRx Common claims recycle process
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                  Date                 Project/Altiris #          Change Description                                                           Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                            --------------------         ------------------------          -----------------------------------------------------------------------                   --------------------------------         -------------------------------   ----------------------------      
# MAGIC Kaushik Kapoor                     2018-09-28              5828                          Original Programming                                                       IntegrateDev2


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
EDWOwner = get_widget_value('EDWOwner','')
RunID = get_widget_value('RunID','')
RunCycle = get_widget_value('RunCycle','')
RunCycleDate = get_widget_value('RunCycleDate','')
Logging = get_widget_value('Logging','')
RunDate = get_widget_value('RunDate','')
IncludeSrcSysCd = get_widget_value('IncludeSrcSysCd','')
ids_secret_name = get_widget_value('ids_secret_name','')
edw_secret_name = get_widget_value('edw_secret_name','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

# PBM_GRP_XREF_GRP_REL
extract_query = f"""
SELECT DISTINCT
SRC_SYS_CD,
PBM_GRP_ID,
GRP_ID
FROM {IDSOwner}.P_PBM_GRP_XREF XREF
WHERE UPPER(XREF.SRC_SYS_CD) IN ('{IncludeSrcSysCd}')
AND '{RunCycleDate}' BETWEEN DATE(XREF.EFF_DT) AND DATE(XREF.TERM_DT)
UNION
SELECT DISTINCT
'BCBSSC' AS SRC_SYS_CD,
REL_ENTY_ID AS PBM_GRP_ID,
GRP_ID
FROM {IDSOwner}.GRP_REL_ENTY
WHERE SUBSTR(GRP_REL_ENTY_TYP_CD,1,3) = 'GRE'
"""
df_PBM_GRP_XREF_GRP_REL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)
df_PBM_GRP_XREF_GRP_REL_dedup = df_PBM_GRP_XREF_GRP_REL.dropDuplicates(["SRC_SYS_CD", "PBM_GRP_ID"])

# IDS_CLM
extract_query = f"""
SELECT
CLM.CLM_SK
FROM {IDSOwner}.CLM AS CLM,
     {IDSOwner}.CD_MPPNG AS CD
WHERE CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
AND CLM.GRP_SK = 0
"""
df_IDS_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)
df_IDS_CLM_dedup = df_IDS_CLM.dropDuplicates(["CLM_SK"])

# EDW_CLM_F
extract_query = f"""
SELECT
CLM_SK
FROM {EDWOwner}.CLM_F
WHERE GRP_SK = 0
"""
df_EDW_CLM_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query)
    .load()
)
df_EDW_CLM_F_dedup = df_EDW_CLM_F.dropDuplicates(["CLM_SK"])

# IDS_P_CLM_MBRSH_ERR_RECYC
extract_query = f"""
SELECT
CLM_SK,
CLM_ID,
SRC_SYS_CD,
CASE WHEN LEFT(TRIM(SRC_SYS_CD),6) = 'BCBSSC' THEN 'BCBSSC' ELSE TRIM(SRC_SYS_CD) END AS DER_SRC_SYS_CD,
CLM_TYP_CD,
CLM_SUBTYP_CD,
CLM_SVC_STRT_DT_SK,
SRC_SYS_GRP_PFX,
SRC_SYS_GRP_ID,
SRC_SYS_GRP_SFX,
SUB_SSN,
PATN_LAST_NM,
PATN_FIRST_NM,
PATN_GNDR_CD,
PATN_BRTH_DT_SK,
ERR_CD,
ERR_DESC,
FEP_MBR_ID,
SUB_FIRST_NM,
SUB_LAST_NM,
SRC_SYS_SUB_ID,
SRC_SYS_MBR_SFX_NO,
GRP_ID,
FILE_DT_SK,
PATN_SSN,
MBR_MNL_MATCH_BUNDLE_ID
FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC
WHERE CASE WHEN LEFT(TRIM(SRC_SYS_CD),6) = 'BCBSSC' THEN 'BCBSSC' ELSE TRIM(SRC_SYS_CD) END IN ('{IncludeSrcSysCd}')
"""
df_IDS_P_CLM_MBRSH_ERR_RECYC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)
df_IDS_P_CLM_MBRSH_ERR_RECYC_dedup = df_IDS_P_CLM_MBRSH_ERR_RECYC.dropDuplicates(["CLM_SK"])

# IDS_P_CLM_ERR_BUNDLE
extract_query = """
SELECT DISTINCT MBR_MNL_MATCH_BUNDLE_ID, MBR_UNIQ_KEY
FROM BCBSKC.P_CLM_MBRSH_ERR_MNL_MATCH_BUNDLE with ur
"""
df_IDS_P_CLM_ERR_BUNDLE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)
df_IDS_P_CLM_ERR_BUNDLE_dedup = df_IDS_P_CLM_ERR_BUNDLE.dropDuplicates(["MBR_MNL_MATCH_BUNDLE_ID"])

# Trn_Mbr_Mtch (Primary link => df_IDS_P_CLM_MBRSH_ERR_RECYC_dedup, Lookup => df_IDS_P_CLM_ERR_BUNDLE_dedup)
df_trn_mbr_mtch_join = (
    df_IDS_P_CLM_MBRSH_ERR_RECYC_dedup.alias("p")
    .join(
        df_IDS_P_CLM_ERR_BUNDLE_dedup.alias("lk"),
        F.col("p.MBR_MNL_MATCH_BUNDLE_ID") == F.col("lk.MBR_MNL_MATCH_BUNDLE_ID"),
        how="left"
    )
)

df_trn_mbr_mtch_filtered = df_trn_mbr_mtch_join.filter(F.col("lk.MBR_UNIQ_KEY").isNotNull())

# Output 1 from Trn_Mbr_Mtch => Lnk_Err_Clm_To_Hf
df_Lnk_Err_Clm_To_Hf = df_trn_mbr_mtch_filtered.select(
    F.col("p.CLM_SK").alias("CLM_SK"),
    F.col("p.CLM_ID").alias("CLM_ID"),
    F.col("p.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("p.DER_SRC_SYS_CD").alias("DER_SRC_SYS_CD"),
    F.col("p.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("p.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("p.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("p.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    F.col("p.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("p.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    F.col("p.SUB_SSN").alias("SUB_SSN"),
    F.col("p.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("p.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("p.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.col("p.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    F.col("p.ERR_CD").alias("ERR_CD"),
    F.col("p.ERR_DESC").alias("ERR_DESC"),
    F.col("p.FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("p.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("p.SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("p.SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    F.col("p.SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    F.col("p.GRP_ID").alias("GRP_ID"),
    F.col("p.FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("p.PATN_SSN").alias("PATN_SSN"),
    F.col("lk.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("p.MBR_MNL_MATCH_BUNDLE_ID").alias("MBR_MNL_MATCH_BUNDLE_ID")
)

# Output 2 from Trn_Mbr_Mtch => Next
df_Next = df_trn_mbr_mtch_filtered.select(
    F.col("p.CLM_ID").alias("CLM_ID"),
    F.col("p.CLM_SVC_STRT_DT_SK").alias("FILL_DT_SK"),
    F.col("lk.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

# Deduplicate for hf_mnlmbrmtch_errclm_land
df_Lnk_Err_Clm_To_Hf_dedup = df_Lnk_Err_Clm_To_Hf.dropDuplicates(["CLM_SK"])

# Deduplicate for hf_mnlmbrmtch_w_drug_enr_match
df_Next_dedup = df_Next.dropDuplicates(["CLM_ID", "FILL_DT_SK", "MBR_UNIQ_KEY"])

# W_DRUG_ENR_MATCH (DB2Connector to IDS with Insert => #$IDSOwner#.W_DRUG_ENR_MATCH)
execute_dml(f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'W_DRUG_ENR_MATCH')", jdbc_url_ids, jdbc_props_ids)
(
    df_Next_dedup
    .write
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", f"{IDSOwner}.W_DRUG_ENR_MATCH")
    .mode("append")
    .save()
)

# Read output from W_DRUG_ENR_MATCH => MBR_Out
extract_query = f"""
SELECT 
       CLM_ID,
       MBR_UNIQ_KEY,
       GRP_ID,
       SUB_ID,
       MBR_SFX_NO,
       SUB_UNIQ_KEY,
       EFF_DT_SK
FROM
(
SELECT 
      DRUG.CLM_ID,
      ENR.MBR_UNIQ_KEY,
      MAX(ENR.TERM_DT_SK) TERM_DT_SK,
      MAX(ENR.EFF_DT_SK) EFF_DT_SK,
      GRP.GRP_ID,
      SUB.SUB_ID,
      MBR.MBR_SFX_NO,
      SUB.SUB_UNIQ_KEY,
      1 as Order
FROM
    {IDSOwner}.MBR_ENR ENR,
    {IDSOwner}.CD_MPPNG CD1,
    {IDSOwner}.W_DRUG_ENR_MATCH DRUG,
    {IDSOwner}.PROD PROD,
    {IDSOwner}.MBR MBR,
    {IDSOwner}.SUB SUB,
    {IDSOwner}.GRP GRP
WHERE
     MBR.MBR_SK = ENR.MBR_SK AND
     MBR.SUB_SK = SUB.SUB_SK AND
     SUB.GRP_SK = GRP.GRP_SK AND
     ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND
     CD1.TRGT_CD IN ('MED', 'MED1', 'DNTL') AND
     ENR.ELIG_IN = 'Y' AND
     ENR.MBR_UNIQ_KEY = DRUG.MBR_UNIQ_KEY AND
     ENR.EFF_DT_SK <= DRUG.FILL_DT_SK AND
     ENR.TERM_DT_SK >= DRUG.FILL_DT_SK AND
     ENR.PROD_SK = PROD.PROD_SK
GROUP BY
     DRUG.CLM_ID,
     ENR.MBR_UNIQ_KEY,
     GRP.GRP_ID,
     SUB.SUB_ID,
     MBR.MBR_SFX_NO,
     SUB.SUB_UNIQ_KEY
UNION
SELECT 
      DRUG.CLM_ID,
      ENR.MBR_UNIQ_KEY,
      MAX(ENR.TERM_DT_SK) TERM_DT_SK,
      MAX(ENR.EFF_DT_SK) EFF_DT_SK,
      GRP.GRP_ID,
      SUB.SUB_ID,
      MBR.MBR_SFX_NO,
      SUB.SUB_UNIQ_KEY,
      2 as Order
FROM
    {IDSOwner}.MBR_ENR ENR,
    {IDSOwner}.CD_MPPNG CD1,
    {IDSOwner}.W_DRUG_ENR_MATCH DRUG,
    {IDSOwner}.PROD PROD,
    {IDSOwner}.MBR MBR,
    {IDSOwner}.SUB SUB,
    {IDSOwner}.GRP GRP
WHERE
     MBR.MBR_SK = ENR.MBR_SK AND
     MBR.SUB_SK = SUB.SUB_SK AND
     SUB.GRP_SK = GRP.GRP_SK AND
     ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND
     CD1.TRGT_CD IN ('MED', 'MED1', 'DNTL') AND
     ENR.ELIG_IN = 'Y' AND
     ENR.MBR_UNIQ_KEY = DRUG.MBR_UNIQ_KEY AND
     ENR.PROD_SK = PROD.PROD_SK
GROUP BY
     DRUG.CLM_ID,
     ENR.MBR_UNIQ_KEY,
     GRP.GRP_ID,
     SUB.SUB_ID,
     MBR.MBR_SFX_NO,
     SUB.SUB_UNIQ_KEY
UNION
SELECT 
      DRUG.CLM_ID,
      ENR.MBR_UNIQ_KEY,
      MAX(ENR.TERM_DT_SK) TERM_DT_SK,
      MAX(ENR.EFF_DT_SK) EFF_DT_SK,
      GRP.GRP_ID,
      SUB.SUB_ID,
      MBR.MBR_SFX_NO,
      SUB.SUB_UNIQ_KEY,
      3 as Order
FROM
    {IDSOwner}.MBR_ENR ENR,
    {IDSOwner}.CD_MPPNG CD1,
    {IDSOwner}.W_DRUG_ENR_MATCH DRUG,
    {IDSOwner}.PROD PROD,
    {IDSOwner}.MBR MBR,
    {IDSOwner}.SUB SUB,
    {IDSOwner}.GRP GRP
WHERE
     MBR.MBR_SK = ENR.MBR_SK AND
     MBR.SUB_SK = SUB.SUB_SK AND
     SUB.GRP_SK = GRP.GRP_SK AND
     ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND
     CD1.TRGT_CD IN ('MED', 'MED1', 'DNTL') AND
     ENR.ELIG_IN = 'N' AND
     ENR.MBR_UNIQ_KEY = DRUG.MBR_UNIQ_KEY AND
     ENR.PROD_SK = PROD.PROD_SK
GROUP BY
     DRUG.CLM_ID,
     ENR.MBR_UNIQ_KEY,
     GRP.GRP_ID,
     SUB.SUB_ID,
     MBR.MBR_SFX_NO,
     SUB.SUB_UNIQ_KEY
)
ORDER BY
CLM_ID,
Order desc,
TERM_DT_SK,
EFF_DT_SK,
MBR_UNIQ_KEY
"""
df_MBR_Out = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

# Deduplicate for hf_mnlmbrmtch_mbr_enr_elig
df_mbr_enr_elig_dedup = df_MBR_Out.dropDuplicates(["CLM_ID"])

# Now Trn_MbrMtch_Lkup (Primary link => Lnk_ERR_CLM, multiple lookups => GRP_BASE_LKP, IDS_CLM, EDW_CLM, Lnk_Mbr_Lkup)
# Primary link => df_Lnk_Err_Clm_To_Hf_dedup as "Lnk_ERR_CLM"
# Lookup GRP_BASE_LKP => df_PBM_GRP_XREF_GRP_REL_dedup
# Lookup IDS_CLM => df_IDS_CLM_dedup
# Lookup EDW_CLM => df_EDW_CLM_F_dedup
# Lookup Lnk_Mbr_Lkup => df_mbr_enr_elig_dedup

df_err_clm = df_Lnk_Err_Clm_To_Hf_dedup.alias("Lnk_ERR_CLM")
df_grp_base = df_PBM_GRP_XREF_GRP_REL_dedup.alias("GRP_BASE_LKP")
df_ids_clm = df_IDS_CLM_dedup.alias("IDS_CLM")
df_edw_clm = df_EDW_CLM_F_dedup.alias("EDW_CLM")
df_mbr_lkup = df_mbr_enr_elig_dedup.alias("Lnk_Mbr_Lkup")

df_join_1 = df_err_clm.join(
    df_grp_base,
    (
        F.trim(F.col("Lnk_ERR_CLM.DER_SRC_SYS_CD")) == F.col("GRP_BASE_LKP.SRC_SYS_CD")
    )
    & (
        F.col("Lnk_ERR_CLM.SRC_SYS_GRP_ID") == F.col("GRP_BASE_LKP.PBM_GRP_ID")
    ),
    how="left"
)
df_join_2 = df_join_1.join(
    df_ids_clm,
    F.col("Lnk_ERR_CLM.CLM_SK") == F.col("IDS_CLM.CLM_SK"),
    how="left"
)
df_join_3 = df_join_2.join(
    df_edw_clm,
    F.col("Lnk_ERR_CLM.CLM_SK") == F.col("EDW_CLM.CLM_SK"),
    how="left"
)
df_join_4 = df_join_3.join(
    df_mbr_lkup,
    F.col("Lnk_ERR_CLM.CLM_ID") == F.col("Lnk_Mbr_Lkup.CLM_ID"),
    how="left"
)

# Stage Variables
df_stagevars = df_join_4.withColumn(
    "svGrpId",
    F.when(
        (F.col("GRP_BASE_LKP.GRP_ID").isNotNull()),
        F.col("GRP_BASE_LKP.GRP_ID")
    ).otherwise(
        F.when(
            (F.col("Lnk_Mbr_Lkup.GRP_ID").isNotNull()),
            F.col("Lnk_Mbr_Lkup.GRP_ID")
        ).otherwise(F.lit("UNK"))
    )
).withColumn(
    "svSubId",
    F.when(F.col("Lnk_Mbr_Lkup.SUB_ID").isNull(), F.lit("0")).otherwise(F.col("Lnk_Mbr_Lkup.SUB_ID"))
).withColumn(
    "svMbrSfxNo",
    F.when(F.col("Lnk_Mbr_Lkup.MBR_SFX_NO").isNull(), F.lit(0)).otherwise(F.col("Lnk_Mbr_Lkup.MBR_SFX_NO"))
).withColumn(
    "svMbrEnrEffDt",
    F.when(F.col("Lnk_Mbr_Lkup.EFF_DT_SK").isNull(), F.lit("1753-01-01")).otherwise(F.col("Lnk_Mbr_Lkup.EFF_DT_SK"))
).withColumn(
    "svSubUniqKey",
    F.when(F.col("Lnk_Mbr_Lkup.SUB_UNIQ_KEY").isNotNull(), F.col("Lnk_Mbr_Lkup.SUB_UNIQ_KEY")).otherwise(F.lit(0))
).withColumn(
    "svSrcSysCd",
    F.when(
        F.col("Lnk_ERR_CLM.DER_SRC_SYS_CD").isNull() | (F.length(F.trim(F.col("Lnk_ERR_CLM.DER_SRC_SYS_CD"))) == 0),
        F.lit("UNK")
    ).otherwise(
        F.when(F.trim(F.col("Lnk_ERR_CLM.DER_SRC_SYS_CD")).isin(
            "PCT","WELLDYNERX","PCS","CAREMARK","ARGUS","EDC","OT@2","ADOL","MOHSAIC","CAREADVANCE","ESI","OPTUMRX",
            "MCSOURCE","MCAID","MEDTRAK","BCBSSC","BCA","BCBSA","SAVRX","LDI","EYEMED","CVS","LUMERIS","MEDIMPACT"
        ), F.lit("FACETS")).otherwise(F.col("Lnk_ERR_CLM.DER_SRC_SYS_CD"))
    )
)

# Output dataframes from Trn_MbrMtch_Lkup

# 1) Lnk_WDrugEnr, constraint => IsNull(Lnk_Mbr_Lkup.MBR_UNIQ_KEY) = @FALSE
df_Lnk_WDrugEnr = df_stagevars.filter(F.col("Lnk_Mbr_Lkup.MBR_UNIQ_KEY").isNotNull()).select(
    F.col("Lnk_ERR_CLM.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_ERR_CLM.CLM_SVC_STRT_DT_SK").alias("FILL_DT_SK"),
    F.col("Lnk_ERR_CLM.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

# 2) Lnk_ErrClmLand, constraint => IsNull(Lnk_Mbr_Lkup.MBR_UNIQ_KEY) = @FALSE
df_Lnk_ErrClmLand = df_stagevars.filter(F.col("Lnk_Mbr_Lkup.MBR_UNIQ_KEY").isNotNull()).select(
    F.col("Lnk_ERR_CLM.CLM_SK").alias("CLM_SK"),
    F.col("Lnk_ERR_CLM.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_ERR_CLM.DER_SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_ERR_CLM.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("Lnk_ERR_CLM.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("Lnk_ERR_CLM.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("Lnk_ERR_CLM.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    F.col("Lnk_ERR_CLM.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("Lnk_ERR_CLM.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    F.col("Lnk_ERR_CLM.SUB_SSN").alias("SUB_SSN"),
    F.col("Lnk_ERR_CLM.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("Lnk_ERR_CLM.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("Lnk_ERR_CLM.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.col("Lnk_ERR_CLM.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    F.col("Lnk_ERR_CLM.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("svGrpId").alias("GRP_ID"),
    F.col("svSubId").alias("SUB_ID"),
    F.col("svMbrSfxNo").alias("MBR_SFX_NO"),
    F.col("svSubUniqKey").alias("SUB_UNIQ_KEY"),
    F.col("svMbrEnrEffDt").alias("EFF_DT_SK")
)

# 3) ClmMbrshErrHist => no constraint specified, so it includes all rows
df_ClmMbrshErrHist = df_stagevars.select(
    F.col("Lnk_ERR_CLM.CLM_SK").alias("CLM_SK"),
    F.col("Lnk_ERR_CLM.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_ERR_CLM.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_ERR_CLM.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("Lnk_ERR_CLM.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("Lnk_ERR_CLM.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("Lnk_ERR_CLM.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    F.col("Lnk_ERR_CLM.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("Lnk_ERR_CLM.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    F.col("Lnk_ERR_CLM.SUB_SSN").alias("SUB_SSN"),
    F.col("Lnk_ERR_CLM.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("Lnk_ERR_CLM.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("Lnk_ERR_CLM.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.col("Lnk_ERR_CLM.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    F.col("Lnk_ERR_CLM.ERR_CD").alias("ERR_CD"),
    F.col("Lnk_ERR_CLM.ERR_DESC").alias("ERR_DESC"),
    F.col("Lnk_ERR_CLM.FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("Lnk_ERR_CLM.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("Lnk_ERR_CLM.SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("Lnk_ERR_CLM.SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    F.col("Lnk_ERR_CLM.SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    F.when(
        F.col("GRP_BASE_LKP.GRP_ID").isNotNull(),
        F.col("GRP_BASE_LKP.GRP_ID")
    ).otherwise(
        F.when(
            F.col("Lnk_Mbr_Lkup.GRP_ID").isNotNull(),
            F.col("Lnk_Mbr_Lkup.GRP_ID")
        ).otherwise(
            F.when(
                F.col("Lnk_ERR_CLM.GRP_ID").isNotNull(),
                F.col("Lnk_Mbr_Lkup.GRP_ID")
            ).otherwise(F.lit("UNK"))
        )
    ).alias("GRP_ID"),
    F.col("Lnk_ERR_CLM.FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("Lnk_ERR_CLM.PATN_SSN").alias("PATN_SSN"),
    F.when(F.col("Lnk_ERR_CLM.MBR_MNL_MATCH_BUNDLE_ID").isNull(), F.lit(0)).otherwise(F.col("Lnk_ERR_CLM.MBR_MNL_MATCH_BUNDLE_ID")).alias("MBR_MNL_MATCH_BUNDLE_ID"),
    F.col("Lnk_ERR_CLM.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.expr("FORMAT.DATE(RunCycleDate, 'DATE', 'DATE', 'DB2TIMESTAMP')").alias("PRCS_DTM")
)

# 4) ids_grp_update => constraint => (len(trim(Lnk_ERR_CLM.GRP_ID)) = 0 Or IsNull(Lnk_ERR_CLM.GRP_ID) = @TRUE)
#  and IsNull(GRP_BASE_LKP.GRP_ID) = @FALSE
#  and IsNull(IDS_CLM.CLM_SK) = @FALSE
df_ids_grp_update = df_stagevars.filter(
    (
        (F.length(F.trim(F.col("Lnk_ERR_CLM.GRP_ID"))) == 0)
        | (F.col("Lnk_ERR_CLM.GRP_ID").isNull())
    )
    & (F.col("GRP_BASE_LKP.GRP_ID").isNotNull())
    & (F.col("IDS_CLM.CLM_SK").isNotNull())
).select(
    F.col("Lnk_ERR_CLM.CLM_SK").alias("CLM_SK"),
    F.col("RunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.expr("GetFkeyGrp(svSrcSysCd, Lnk_ERR_CLM.CLM_SK, svGrpId, Logging)").alias("GRP_SK")
)

# 5) edw_grp_update => constraint => (len(trim(Lnk_ERR_CLM.GRP_ID)) = 0 Or IsNull(Lnk_ERR_CLM.GRP_ID) = @TRUE)
# and IsNull(GRP_BASE_LKP.GRP_ID) = @FALSE
# and IsNull(EDW_CLM.CLM_SK) = @FALSE
df_edw_grp_update = df_stagevars.filter(
    (
        (F.length(F.trim(F.col("Lnk_ERR_CLM.GRP_ID"))) == 0)
        | (F.col("Lnk_ERR_CLM.GRP_ID").isNull())
    )
    & (F.col("GRP_BASE_LKP.GRP_ID").isNotNull())
    & (F.col("EDW_CLM.CLM_SK").isNotNull())
).select(
    F.col("Lnk_ERR_CLM.CLM_SK").alias("CLM_SK"),
    F.col("RunCycleDate").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("RunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.expr("GetFkeyGrp(svSrcSysCd, Lnk_ERR_CLM.CLM_SK, svGrpId, Logging)").alias("GRP_SK"),
    F.col("svGrpId").alias("GRP_ID")
)

# MnlMemberMatchErrClmLand => writes a sequence file
# Apply rpad for char/varchar columns and keep column order
df_mnlmembermatcherrclmland_out = df_Lnk_ErrClmLand.withColumn(
    "CLM_ID", F.rpad(F.col("CLM_ID"), <...>, " ")
).withColumn(
    "SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " ")
).withColumn(
    "CLM_TYP_CD", F.rpad(F.col("CLM_TYP_CD"), <...>, " ")
).withColumn(
    "CLM_SUBTYP_CD", F.rpad(F.col("CLM_SUBTYP_CD"), <...>, " ")
).withColumn(
    "CLM_SVC_STRT_DT_SK", F.rpad(F.col("CLM_SVC_STRT_DT_SK"), 10, " ")
).withColumn(
    "SRC_SYS_GRP_PFX", F.rpad(F.col("SRC_SYS_GRP_PFX"), <...>, " ")
).withColumn(
    "SRC_SYS_GRP_ID", F.rpad(F.col("SRC_SYS_GRP_ID"), <...>, " ")
).withColumn(
    "SRC_SYS_GRP_SFX", F.rpad(F.col("SRC_SYS_GRP_SFX"), <...>, " ")
).withColumn(
    "SUB_SSN", F.rpad(F.col("SUB_SSN"), <...>, " ")
).withColumn(
    "PATN_LAST_NM", F.rpad(F.col("PATN_LAST_NM"), <...>, " ")
).withColumn(
    "PATN_FIRST_NM", F.rpad(F.col("PATN_FIRST_NM"), <...>, " ")
).withColumn(
    "PATN_GNDR_CD", F.rpad(F.col("PATN_GNDR_CD"), <...>, " ")
).withColumn(
    "PATN_BRTH_DT_SK", F.rpad(F.col("PATN_BRTH_DT_SK"), 10, " ")
).withColumn(
    "GRP_ID", F.rpad(F.col("GRP_ID"), <...>, " ")
).withColumn(
    "SUB_ID", F.rpad(F.col("SUB_ID"), <...>, " ")
).withColumn(
    "MBR_SFX_NO", F.rpad(F.col("MBR_SFX_NO").cast("string"), 2, " ")
).withColumn(
    "SUB_UNIQ_KEY", F.col("SUB_UNIQ_KEY")  # int => no rpad
).withColumn(
    "EFF_DT_SK", F.rpad(F.col("EFF_DT_SK"), 10, " ")
)

df_mnlmembermatcherrclmland_final = df_mnlmembermatcherrclmland_out.select(
    "CLM_SK",
    "CLM_ID",
    "SRC_SYS_CD",
    "CLM_TYP_CD",
    "CLM_SUBTYP_CD",
    "CLM_SVC_STRT_DT_SK",
    "SRC_SYS_GRP_PFX",
    "SRC_SYS_GRP_ID",
    "SRC_SYS_GRP_SFX",
    "SUB_SSN",
    "PATN_LAST_NM",
    "PATN_FIRST_NM",
    "PATN_GNDR_CD",
    "PATN_BRTH_DT_SK",
    "MBR_UNIQ_KEY",
    "GRP_ID",
    "SUB_ID",
    "MBR_SFX_NO",
    "SUB_UNIQ_KEY",
    "EFF_DT_SK"
)

write_files(
    df_mnlmembermatcherrclmland_final,
    f"{adls_path}/verified/Mnl_Mbr_Match_ErrClm_Landing.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# W_DRUG_ENR => writes a sequence file
df_w_drug_enr_out = df_Lnk_WDrugEnr.withColumn(
    "CLM_ID", F.rpad(F.col("CLM_ID"), <...>, " ")
).withColumn(
    "FILL_DT_SK", F.rpad(F.col("FILL_DT_SK"), 10, " ")
).select(
    "CLM_ID",
    "FILL_DT_SK",
    "MBR_UNIQ_KEY"
)

write_files(
    df_w_drug_enr_out,
    f"{adls_path}/load/W_DRUG_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# IDS_CLM_Update => we do a merge into {IDSOwner}.CLM
# Create a staging table STAGING.IdsClmMnlMbrMtchErrHistExtr_IDS_CLM_Update_temp
execute_dml(
    f"DROP TABLE IF EXISTS STAGING.IdsClmMnlMbrMtchErrHistExtr_IDS_CLM_Update_temp",
    jdbc_url_ids,
    jdbc_props_ids
)

execute_dml(
    f"CREATE TABLE STAGING.IdsClmMnlMbrMtchErrHistExtr_IDS_CLM_Update_temp (CLM_SK INT, LAST_UPDT_RUN_CYC_EXCTN_SK INT, GRP_SK INT)",
    jdbc_url_ids,
    jdbc_props_ids
)

df_ids_grp_update.write.format("jdbc").option("url", jdbc_url_ids).options(**jdbc_props_ids).option("dbtable", "STAGING.IdsClmMnlMbrMtchErrHistExtr_IDS_CLM_Update_temp").mode("append").save()

merge_sql_ids = f"""
MERGE {IDSOwner}.CLM AS T
USING STAGING.IdsClmMnlMbrMtchErrHistExtr_IDS_CLM_Update_temp AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.GRP_SK = S.GRP_SK
WHEN NOT MATCHED THEN
  INSERT (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, GRP_SK)
  VALUES (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_SK, S.GRP_SK);
"""
execute_dml(merge_sql_ids, jdbc_url_ids, jdbc_props_ids)

# EDW_CLM_Update => we do a merge into {EDWOwner}.CLM_F
execute_dml(
    f"DROP TABLE IF EXISTS STAGING.IdsClmMnlMbrMtchErrHistExtr_EDW_CLM_Update_temp",
    jdbc_url_edw,
    jdbc_props_edw
)

execute_dml(
    f"CREATE TABLE STAGING.IdsClmMnlMbrMtchErrHistExtr_EDW_CLM_Update_temp (CLM_SK INT, LAST_UPDT_RUN_CYC_EXCTN_DT_SK VARCHAR(10), LAST_UPDT_RUN_CYC_EXCTN_SK INT, GRP_SK INT, GRP_ID VARCHAR(50))",
    jdbc_url_edw,
    jdbc_props_edw
)

df_edw_grp_update.write.format("jdbc").option("url", jdbc_url_edw).options(**jdbc_props_edw).option("dbtable", "STAGING.IdsClmMnlMbrMtchErrHistExtr_EDW_CLM_Update_temp").mode("append").save()

merge_sql_edw = f"""
MERGE {EDWOwner}.CLM_F AS T
USING STAGING.IdsClmMnlMbrMtchErrHistExtr_EDW_CLM_Update_temp AS S
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
execute_dml(merge_sql_edw, jdbc_url_edw, jdbc_props_edw)

# P_CLM_MBRSH_ERR_MNL_MATCH_HIST => writes a sequence file
df_clm_mbrsh_err_mnl_match_hist_out = (
    df_ClmMbrshErrHist
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), <...>, " "))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("CLM_TYP_CD", F.rpad(F.col("CLM_TYP_CD"), <...>, " "))
    .withColumn("CLM_SUBTYP_CD", F.rpad(F.col("CLM_SUBTYP_CD"), <...>, " "))
    .withColumn("CLM_SVC_STRT_DT_SK", F.rpad(F.col("CLM_SVC_STRT_DT_SK"), 10, " "))
    .withColumn("SRC_SYS_GRP_PFX", F.rpad(F.col("SRC_SYS_GRP_PFX"), <...>, " "))
    .withColumn("SRC_SYS_GRP_ID", F.rpad(F.col("SRC_SYS_GRP_ID"), <...>, " "))
    .withColumn("SRC_SYS_GRP_SFX", F.rpad(F.col("SRC_SYS_GRP_SFX"), <...>, " "))
    .withColumn("SUB_SSN", F.rpad(F.col("SUB_SSN"), <...>, " "))
    .withColumn("PATN_LAST_NM", F.rpad(F.col("PATN_LAST_NM"), <...>, " "))
    .withColumn("PATN_FIRST_NM", F.rpad(F.col("PATN_FIRST_NM"), <...>, " "))
    .withColumn("PATN_GNDR_CD", F.rpad(F.col("PATN_GNDR_CD"), <...>, " "))
    .withColumn("PATN_BRTH_DT_SK", F.rpad(F.col("PATN_BRTH_DT_SK"), 10, " "))
    .withColumn("ERR_CD", F.rpad(F.col("ERR_CD"), <...>, " "))
    .withColumn("ERR_DESC", F.rpad(F.col("ERR_DESC"), <...>, " "))
    .withColumn("FEP_MBR_ID", F.rpad(F.col("FEP_MBR_ID"), <...>, " "))
    .withColumn("SUB_FIRST_NM", F.rpad(F.col("SUB_FIRST_NM"), <...>, " "))
    .withColumn("SUB_LAST_NM", F.rpad(F.col("SUB_LAST_NM"), <...>, " "))
    .withColumn("SRC_SYS_SUB_ID", F.rpad(F.col("SRC_SYS_SUB_ID"), <...>, " "))
    .withColumn("SRC_SYS_MBR_SFX_NO", F.rpad(F.col("SRC_SYS_MBR_SFX_NO"), 3, " "))
    .withColumn("GRP_ID", F.rpad(F.col("GRP_ID"), <...>, " "))
    .withColumn("FILE_DT_SK", F.rpad(F.col("FILE_DT_SK"), 10, " "))
    .withColumn("PATN_SSN", F.rpad(F.col("PATN_SSN"), <...>, " "))
    .withColumn("MBR_MNL_MATCH_BUNDLE_ID", F.col("MBR_MNL_MATCH_BUNDLE_ID"))
    .withColumn("MBR_UNIQ_KEY", F.col("MBR_UNIQ_KEY"))
    .withColumn("PRCS_DTM", F.col("PRCS_DTM"))
    .select(
        "CLM_SK",
        "CLM_ID",
        "SRC_SYS_CD",
        "CLM_TYP_CD",
        "CLM_SUBTYP_CD",
        "CLM_SVC_STRT_DT_SK",
        "SRC_SYS_GRP_PFX",
        "SRC_SYS_GRP_ID",
        "SRC_SYS_GRP_SFX",
        "SUB_SSN",
        "PATN_LAST_NM",
        "PATN_FIRST_NM",
        "PATN_GNDR_CD",
        "PATN_BRTH_DT_SK",
        "ERR_CD",
        "ERR_DESC",
        "FEP_MBR_ID",
        "SUB_FIRST_NM",
        "SUB_LAST_NM",
        "SRC_SYS_SUB_ID",
        "SRC_SYS_MBR_SFX_NO",
        "GRP_ID",
        "FILE_DT_SK",
        "PATN_SSN",
        "MBR_MNL_MATCH_BUNDLE_ID",
        "MBR_UNIQ_KEY",
        "PRCS_DTM"
    )
)

write_files(
    df_clm_mbrsh_err_mnl_match_hist_out,
    f"{adls_path}/load/P_CLM_MBRSH_ERR_MNL_MATCH_HIST.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)