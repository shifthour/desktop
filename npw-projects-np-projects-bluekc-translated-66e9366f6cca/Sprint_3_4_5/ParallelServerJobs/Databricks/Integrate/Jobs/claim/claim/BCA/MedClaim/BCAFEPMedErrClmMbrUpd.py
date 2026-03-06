# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : BCAFEPClmLandSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION :  BCA FEP Med Claims Member Matching Error File Load.
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                  Date                 Project/Altiris #          Change Description                                                           Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                            --------------------         ------------------------          -----------------------------------------------------------------------                   --------------------------------         -------------------------------   ----------------------------      
# MAGIC Sudhir Bomshetty                     2017-10-11              5781                     Original Programming                                                       IntegrateDev2                     Kalyan Neelam          2017-10-20


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# SHARED CONTAINERS (None referenced explicitly to %run, so none placed here)

# PARAMETERS
SrcSysCd = get_widget_value('SrcSysCd','BCA')
RunCycle = get_widget_value('RunCycle','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
Logging = get_widget_value('Logging','')
EDWOwner = get_widget_value('EDWOwner','')
RunDate = get_widget_value('RunDate','')
ids_secret_name = get_widget_value('ids_secret_name','')
edw_secret_name = get_widget_value('edw_secret_name','')

# JDBC CONFIG
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

# 1) IDS_LKP STAGE => 4 DataFrames => drop duplicates on PK
extract_query_exprnc_cat_1 = f"""
SELECT 
 EXPRNC_CAT_SK,
 EXPRNC_CAT_CD,
 TRGT_CD AS FUND_CAT_CD,
 'Y' AS EXPRNC_IND
FROM {IDSOwner}.EXPRNC_CAT,
     {IDSOwner}.CD_MPPNG
WHERE EXPRNC_CAT_FUND_CAT_CD_SK = CD_MPPNG_SK + 0
"""
df_exprnc_cat_1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_exprnc_cat_1)
    .load()
)
df_exprnc_cat_1 = df_exprnc_cat_1.dropDuplicates(["EXPRNC_CAT_SK"])

extract_query_fncl_lob_1 = f"""
SELECT 
 FNCL_LOB_SK,
 FNCL_LOB_CD,
 'Y' AS FNCL_LOB_IND
FROM {IDSOwner}.FNCL_LOB
"""
df_fncl_lob_1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_fncl_lob_1)
    .load()
)
df_fncl_lob_1 = df_fncl_lob_1.dropDuplicates(["FNCL_LOB_SK"])

extract_query_grp_1 = f"""
SELECT 
 GRP_SK,
 GRP_ID
FROM {IDSOwner}.GRP
"""
df_grp_1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_grp_1)
    .load()
)
df_grp_1 = df_grp_1.dropDuplicates(["GRP_SK"])

extract_query_prod_1 = f"""
SELECT 
 PROD.PROD_SK,
 PROD.PROD_ID,
 PROD.PROD_SH_NM_SK,
 CD.TRGT_CD AS PROD_ST_CD,
 PROD_SH_NM
FROM {IDSOwner}.PROD PROD,
     {IDSOwner}.PROD_SH_NM SH,
     {IDSOwner}.CD_MPPNG CD
WHERE PROD.PROD_SH_NM_SK = SH.PROD_SH_NM_SK
  AND PROD.PROD_ST_CD_SK = CD.CD_MPPNG_SK + 0
"""
df_prod_1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_prod_1)
    .load()
)
df_prod_1 = df_prod_1.dropDuplicates(["PROD_SK"])

# 2) EDW_CLM_F => hf_bcafep_med_edw_clm_f => scenario A => direct DF => dropDuplicates
extract_query_edw_clm_f = f"""
SELECT
 CLM_SK
FROM {EDWOwner}.CLM_F
WHERE SRC_SYS_CD = 'BCA'
  AND MBR_SK = 0
"""
df_edw_clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_edw_clm_f)
    .load()
)
df_edw_clm = df_edw_clm.dropDuplicates(["CLM_SK"])

# 3) IDS_CLM => hf_bcafep_med_ids_clm => scenario A => direct DF => dropDuplicates
extract_query_ids_clm = f"""
SELECT
 CLM.CLM_SK
FROM {IDSOwner}.CLM AS CLM,
     {IDSOwner}.CD_MPPNG AS CD
WHERE CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
  AND CD.TRGT_CD = 'BCA'
  AND CLM.MBR_SK = 0
"""
df_ids_clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids_clm)
    .load()
)
df_ids_clm = df_ids_clm.dropDuplicates(["CLM_SK"])

# 4) "ids" STAGE => 3 DataFrames => sub_alpha_pfx, mbr_enroll, mbr
extract_query_sub_alpha_pfx = f"""
SELECT 
 SUB.SUB_UNIQ_KEY,
 ALPHA_PFX_CD
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.ALPHA_PFX PFX,
     {IDSOwner}.W_DRUG_ENR DRUG
WHERE DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  AND MBR.SUB_SK = SUB.SUB_SK
  AND SUB.ALPHA_PFX_SK = PFX.ALPHA_PFX_SK
"""
df_sub_alpha_pfx = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_sub_alpha_pfx)
    .load()
)
df_sub_alpha_pfx = df_sub_alpha_pfx.dropDuplicates(["SUB_UNIQ_KEY"])

extract_query_mbr_enroll = f"""
SELECT 
 DRUG.CLM_ID,
 CLS.CLS_ID,
 PLN.CLS_PLN_ID,
 SUBGRP.SUBGRP_ID,
 CAT.EXPRNC_CAT_CD,
 LOB.FNCL_LOB_CD,
 CMPNT.PROD_ID
FROM {IDSOwner}.W_DRUG_ENR DRUG,
     {IDSOwner}.MBR_ENR MBR,
     {IDSOwner}.CD_MPPNG MAP1,
     {IDSOwner}.CLS CLS,
     {IDSOwner}.SUBGRP SUBGRP,
     {IDSOwner}.CLS_PLN PLN,
     {IDSOwner}.CD_MPPNG MAP2,
     {IDSOwner}.PROD_CMPNT CMPNT,
     {IDSOwner}.PROD_BILL_CMPNT BILL_CMPNT,
     {IDSOwner}.EXPRNC_CAT CAT,
     {IDSOwner}.FNCL_LOB LOB
WHERE DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  AND DRUG.FILL_DT_SK BETWEEN MBR.EFF_DT_SK AND MBR.TERM_DT_SK
  AND MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
  AND MAP1.TRGT_CD IN ('MED')
  AND MBR.CLS_SK = CLS.CLS_SK
  AND MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
  AND MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
  AND MBR.PROD_SK = CMPNT.PROD_SK
  AND CMPNT.PROD_CMPNT_TYP_CD_SK = MAP2.CD_MPPNG_SK
  AND MAP2.TRGT_CD='PDBL'
  AND DRUG.FILL_DT_SK BETWEEN CMPNT.PROD_CMPNT_EFF_DT_SK AND CMPNT.PROD_CMPNT_TERM_DT_SK
  AND CMPNT.PROD_CMPNT_EFF_DT_SK= (
    SELECT MAX (CMPNT2.PROD_CMPNT_EFF_DT_SK)
    FROM {IDSOwner}.PROD_CMPNT CMPNT2
    WHERE CMPNT.PROD_SK = CMPNT2.PROD_SK
      AND CMPNT.PROD_CMPNT_TYP_CD_SK = CMPNT2.PROD_CMPNT_TYP_CD_SK
      AND DRUG.FILL_DT_SK BETWEEN CMPNT2.PROD_CMPNT_EFF_DT_SK AND CMPNT2.PROD_CMPNT_TERM_DT_SK
  )
  AND CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT.PROD_CMPNT_PFX_ID
  AND DRUG.FILL_DT_SK BETWEEN BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK AND BILL_CMPNT.PROD_BILL_CMPNT_TERM_DT_SK
  AND BILL_CMPNT.PROD_BILL_CMPNT_ID IN ('MED', 'MED1')
  AND BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK= (
    SELECT MAX (BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK)
    FROM {IDSOwner}.PROD_BILL_CMPNT BILL_CMPNT2
    WHERE BILL_CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
      AND CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
      AND BILL_CMPNT.PROD_BILL_CMPNT_ID = BILL_CMPNT2.PROD_BILL_CMPNT_ID
      AND BILL_CMPNT2.PROD_BILL_CMPNT_ID IN ('MED', 'MED1', 'DEN', 'DEN1')
      AND DRUG.FILL_DT_SK BETWEEN BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK AND BILL_CMPNT2.PROD_BILL_CMPNT_TERM_DT_SK
  )
  AND BILL_CMPNT.EXPRNC_CAT_SK = CAT.EXPRNC_CAT_SK
  AND BILL_CMPNT.FNCL_LOB_SK = LOB.FNCL_LOB_SK
"""
df_mbr_enroll = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_mbr_enroll)
    .load()
)
df_mbr_enroll = df_mbr_enroll.dropDuplicates(["CLM_ID"])

extract_query_mbr = f"""
SELECT 
 MBR.MBR_UNIQ_KEY AS MBR_UNIQ_KEY,
 SUB.SUB_UNIQ_KEY AS SUB_UNIQ_KEY
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.W_DRUG_ENR DRUG
WHERE DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  AND MBR.SUB_SK = SUB.SUB_SK
"""
df_mbr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_mbr)
    .load()
)
df_mbr = df_mbr.dropDuplicates(["MBR_UNIQ_KEY"])

# 5) BCAFEPClmLanding => read from "verified/BCAFEPMedErrClm_ClaimsLanding.dat.#RunID#"
schema_BCAFEPClmLanding = StructType([
    StructField("CLM_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CLM_TYP_CD", StringType(), False),
    StructField("CLM_SUBTYP_CD", StringType(), False),
    StructField("CLM_SVC_STRT_DT_SK", StringType(), False),
    StructField("SRC_SYS_GRP_PFX", StringType(), False),
    StructField("SRC_SYS_GRP_ID", StringType(), False),
    StructField("SRC_SYS_GRP_SFX", StringType(), False),
    StructField("SUB_SSN", StringType(), False),
    StructField("PATN_LAST_NM", StringType(), False),
    StructField("PATN_FIRST_NM", StringType(), False),
    StructField("PATN_GNDR_CD", StringType(), False),
    StructField("PATN_BRTH_DT_SK", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("SUB_ID", StringType(), False),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), False)
])
df_BCAFEPClmLanding = (
    spark.read.format("csv")
    .option("header","false")
    .option("quote","\"")
    .schema(schema_BCAFEPClmLanding)
    .load(f"{adls_path}/verified/BCAFEPMedErrClm_ClaimsLanding.dat.{RunID}")
)

# 6) BusinessRules => primary link df_BCAFEPClmLanding as BCAFEP, left join df_mbr as mbr_lkp
df_businessrules_joined = df_BCAFEPClmLanding.alias("BCAFEP").join(
    df_mbr.alias("mbr_lkp"),
    F.col("BCAFEP.MBR_UNIQ_KEY") == F.col("mbr_lkp.MBR_UNIQ_KEY"),
    "left"
)
df_BCAFEPClmTrns = df_businessrules_joined.select(
    F.col("BCAFEP.CLM_SK").alias("CLM_SK"),
    F.rpad(F.col("BCAFEP.CLM_ID"),18," ").alias("CLM_ID"),
    F.when(F.col("mbr_lkp.MBR_UNIQ_KEY").isNull(), F.col("BCAFEP.SUB_UNIQ_KEY")).otherwise(F.col("mbr_lkp.SUB_UNIQ_KEY")).alias("SUB_CK"),
    F.col("BCAFEP.SUB_ID").alias("SUB_ID"),
    F.col("BCAFEP.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.rpad(F.col("BCAFEP.GRP_ID"),8," ").alias("GRP"),
    F.col("BCAFEP.MBR_UNIQ_KEY").alias("MBR_CK"),
    F.rpad(F.col("BCAFEP.MBR_SFX_NO"),2," ").alias("MBR_SFX_NO")
)

# 7) alpha_pfx => primary link df_BCAFEPClmTrns as BCAFEPClmTrns, 
#    left join df_sub_alpha_pfx as sub_alpha_pfx_lkup on SUB_CK=SUB_UNIQ_KEY,
#    left join df_mbr_enroll as mbr_enr_lkup on CLM_ID=CLM_ID
alpha_join_1 = df_BCAFEPClmTrns.alias("BCAFEPClmTrns").join(
    df_sub_alpha_pfx.alias("sub_alpha_pfx_lkup"),
    F.col("BCAFEPClmTrns.SUB_CK") == F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY"),
    "left"
)
alpha_join_2 = alpha_join_1.join(
    df_mbr_enroll.alias("mbr_enr_lkup"),
    F.col("BCAFEPClmTrns.CLM_ID") == F.col("mbr_enr_lkup.CLM_ID"),
    "left"
)
df_ClmCrfIn1 = alpha_join_2.select(
    F.col("BCAFEPClmTrns.CLM_SK").alias("CLM_SK"),
    F.rpad(F.col("BCAFEPClmTrns.CLM_ID"),18," ").alias("CLM_ID"),
    F.rpad(
        F.when(F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY").isNull(),"UNK").otherwise(F.col("sub_alpha_pfx_lkup.ALPHA_PFX_CD")),
        3," "
    ).alias("ALPHA_PFX_CD"),
    F.rpad(
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(),"UNK").otherwise(F.col("mbr_enr_lkup.CLS_ID")),
        4," "
    ).alias("CLS"),
    F.rpad(
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(),"UNK").otherwise(F.col("mbr_enr_lkup.CLS_PLN_ID")),
        8," "
    ).alias("CLS_PLN"),
    F.rpad(
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(),"UNK").otherwise(F.col("mbr_enr_lkup.EXPRNC_CAT_CD")),
        4," "
    ).alias("EXPRNC_CAT"),
    F.rpad(
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(),"UNK").otherwise(F.col("mbr_enr_lkup.FNCL_LOB_CD")),
        4," "
    ).alias("FNCL_LOB_NO"),
    F.rpad(F.col("BCAFEPClmTrns.GRP"),8," ").alias("GRP"),
    F.col("BCAFEPClmTrns.MBR_CK").alias("MBR_CK"),
    F.rpad(
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(),"UNK").otherwise(F.trim(F.col("mbr_enr_lkup.PROD_ID"))),
        8," "
    ).alias("PROD"),
    F.rpad(
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(),"UNK").otherwise(F.col("mbr_enr_lkup.SUBGRP_ID")),
        4," "
    ).alias("SUBGRP"),
    F.col("BCAFEPClmTrns.SUB_CK").alias("SUB_CK"),
    F.rpad(F.col("BCAFEPClmTrns.CLM_TYP_CD"),1," ").alias("CLM_TYP_CD"),
    F.rpad(F.col("BCAFEPClmTrns.MBR_SFX_NO"),2," ").alias("MBR_SFX_NO"),
    F.rpad(F.col("BCAFEPClmTrns.SUB_ID"),14," ").alias("SUB_ID")
)

# 8) Trns1 => primary link df_ClmCrfIn1 (as ClmCrfIn1), left join df_ids_clm (as IDS_CLM) on CLM_SK
trns1_join = df_ClmCrfIn1.alias("ClmCrfIn1").join(
    df_ids_clm.alias("IDS_CLM"),
    F.col("ClmCrfIn1.CLM_SK") == F.col("IDS_CLM.CLM_SK"),
    "left"
)
df_Trns1 = trns1_join.select(
    F.col("ClmCrfIn1.*"),
    F.col("IDS_CLM.CLM_SK").alias("IDS_CLM_SK")
)

# Now define the stage variables logic for each row
# We'll produce them with column expressions:
df_Trns1_withVars = df_Trns1.withColumn("SrcSysCdMbr", 
    F.when(F.col("SRC_SYS_CD").isNull() | (F.length(F.trim(F.col("SRC_SYS_CD")))==0), F.lit("UNK"))
     .when(F.trim(F.col("SRC_SYS_CD")).isin("PCT","WELLDYNERX","PCS","CAREMARK","ARGUS","EDC","OT@2","ADOL","MOHSAIC","CAREADVANCE","ESI","OPTUMRX","MCSOURCE","MCAID","MEDTRAK","BCBSSC","BCA","BCBSA","SAVRX","LDI","EYEMED","CVS","LUMERIS","MEDIMPACT"), F.lit("FACETS"))
     .otherwise(F.col("SRC_SYS_CD"))
)
df_Trns1_withVars = df_Trns1_withVars.withColumn("SrcSysCdProd", 
    F.when(F.col("SRC_SYS_CD").isNull() | (F.length(F.trim(F.col("SRC_SYS_CD")))==0), F.lit("UNK"))
     .when(F.trim(F.col("SRC_SYS_CD")).isin("PCT","WELLDYNERX","PCS","CAREMARK","ARGUS","EDC","OT@2","ADOL","ESI","OPTUMRX","MCSOURCE","MCAID","MEDTRAK","BCBSSC","BCA","BCBSA","SAVRX","LDI","EYEMED","CVS","LUMERIS","MEDIMPACT"), F.lit("FACETS"))
     .otherwise(F.col("SRC_SYS_CD"))
)
# StageVar ClsPlnSk => GetFkeyClsPln(SrcSysCdMbr, ClmCrfIn1.CLM_SK, ClmCrfIn1.CLS_PLN, Logging)
# StageVar ClsSk => GetFkeyCls(SrcSysCdMbr, ClmCrfIn1.CLM_SK, ClmCrfIn1.GRP, ClmCrfIn1.CLS, Logging)
# StageVar ExpCatCdSk => GetFkeyExprncCat(SrcSysCdProd, ClmCrfIn1.CLM_SK, ClmCrfIn1.EXPRNC_CAT, Logging)
# StageVar GrpSk => GetFkeyGrp(SrcSysCdMbr, ClmCrfIn1.CLM_SK, ClmCrfIn1.GRP, Logging)
# StageVar MbrSk => GetFkeyMbr(SrcSysCdMbr, ClmCrfIn1.CLM_SK, ClmCrfIn1.MBR_CK, Logging)
# StageVar ProdSk => GetFkeyProd(SrcSysCdProd, ClmCrfIn1.CLM_SK, ClmCrfIn1.PROD, Logging)
# StageVar SubGrpSk => GetFkeySubgrp(SrcSysCdMbr, ClmCrfIn1.CLM_SK, ClmCrfIn1.GRP, ClmCrfIn1.SUBGRP, Logging)
# StageVar SubSk => GetFkeySub(SrcSysCdMbr, ClmCrfIn1.CLM_SK, ClmCrfIn1.SUB_CK, Logging)
# StageVar PlnAlphPfxSk => GetFkeyAlphaPfx('BCA', ClmCrfIn1.CLM_SK, ClmCrfIn1.ALPHA_PFX_CD, Logging)
# StageVar FinancialLOB => GetFkeyFnclLob("PSI", ClmCrfIn1.CLM_SK, ClmCrfIn1.FNCL_LOB_NO, Logging)

df_Trns1_final = df_Trns1_withVars.select(
    F.col("*"),
    F.expr("GetFkeyClsPln(SrcSysCdMbr, CLM_SK, CLS_PLN, Logging)").alias("ClsPlnSk"),
    F.expr("GetFkeyCls(SrcSysCdMbr, CLM_SK, GRP, CLS, Logging)").alias("ClsSk"),
    F.expr("GetFkeyExprncCat(SrcSysCdProd, CLM_SK, EXPRNC_CAT, Logging)").alias("ExpCatCdSk"),
    F.expr("GetFkeyGrp(SrcSysCdMbr, CLM_SK, GRP, Logging)").alias("GrpSk"),
    F.expr("GetFkeyMbr(SrcSysCdMbr, CLM_SK, MBR_CK, Logging)").alias("MbrSk"),
    F.expr("GetFkeyProd(SrcSysCdProd, CLM_SK, PROD, Logging)").alias("ProdSk"),
    F.expr("GetFkeySubgrp(SrcSysCdMbr, CLM_SK, GRP, SUBGRP, Logging)").alias("SubGrpSk"),
    F.expr("GetFkeySub(SrcSysCdMbr, CLM_SK, SUB_CK, Logging)").alias("SubSk"),
    F.expr("GetFkeyAlphaPfx('BCA', CLM_SK, ALPHA_PFX_CD, Logging)").alias("PlnAlphPfxSk"),
    F.expr('GetFkeyFnclLob("PSI", CLM_SK, FNCL_LOB_NO, Logging)').alias("FinancialLOB")
)

# Now produce the three output links from Trns1:
#  (1) ClmFkeyOut1 => constraint "IsNull(IDS_CLM.CLM_SK)=false"
df_ClmFkeyOut1_full = df_Trns1_final.filter(F.col("IDS_CLM_SK").isNotNull())
df_ClmFkeyOut1 = df_ClmFkeyOut1_full.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("RunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PlnAlphPfxSk").alias("ALPHA_PFX_SK"),
    F.col("ClsSk").alias("CLS_SK"),
    F.col("ClsPlnSk").alias("CLS_PLN_SK"),
    F.col("ExpCatCdSk").alias("EXPRNC_CAT_SK"),
    F.col("FinancialLOB").alias("FNCL_LOB_SK"),
    F.col("GrpSk").alias("GRP_SK"),
    F.col("MbrSk").alias("MBR_SK"),
    F.col("ProdSk").alias("PROD_SK"),
    F.col("SubGrpSk").alias("SUBGRP_SK"),
    F.col("SubSk").alias("SUB_SK"),
    F.rpad(F.col("MBR_SFX_NO"),2," ").alias("MBR_SFX_NO"),
    F.rpad(F.col("SUB_ID"),14," ").alias("SUB_ID")
)

#  (2) Lnk_PClmMtch => "CLM_SK" only, from all input (no constraint)
df_Lnk_PClmMtch = df_Trns1_final.select(
    F.col("CLM_SK").alias("CLM_SK")
)

#  (3) Lnk_IDS_CLM => all rows, columns:
df_Lnk_IDS_CLM = df_Trns1_final.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("RunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PlnAlphPfxSk").alias("ALPHA_PFX_SK"),
    F.col("ClsSk").alias("CLS_SK"),
    F.col("ClsPlnSk").alias("CLS_PLN_SK"),
    F.col("ExpCatCdSk").alias("EXPRNC_CAT_SK"),
    F.col("FinancialLOB").alias("FNCL_LOB_SK"),
    F.col("GrpSk").alias("GRP_SK"),
    F.col("MbrSk").alias("MBR_SK"),
    F.col("ProdSk").alias("PROD_SK"),
    F.col("SubGrpSk").alias("SUBGRP_SK"),
    F.col("SubSk").alias("SUB_SK"),
    F.rpad(F.col("MBR_SFX_NO"),2," ").alias("MBR_SFX_NO"),
    F.rpad(F.col("SUB_ID"),14," ").alias("SUB_ID")
)

# IDS_CLM_Update => Merge df_ClmFkeyOut1 into {IDSOwner}.CLM
execute_dml(f"DROP TABLE IF EXISTS STAGING.BCAFEPMedErrClmMbrUpd_IDS_CLM_Update_temp", jdbc_url_ids, jdbc_props_ids)
df_ClmFkeyOut1.write \
    .format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable","STAGING.BCAFEPMedErrClmMbrUpd_IDS_CLM_Update_temp") \
    .mode("append") \
    .save()
merge_sql_ids = f"""
MERGE INTO {IDSOwner}.CLM AS T
USING STAGING.BCAFEPMedErrClmMbrUpd_IDS_CLM_Update_temp AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN UPDATE SET 
 T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
 T.ALPHA_PFX_SK = S.ALPHA_PFX_SK,
 T.CLS_SK = S.CLS_SK,
 T.CLS_PLN_SK = S.CLS_PLN_SK,
 T.EXPRNC_CAT_SK = S.EXPRNC_CAT_SK,
 T.FNCL_LOB_SK = S.FNCL_LOB_SK,
 T.GRP_SK = S.GRP_SK,
 T.MBR_SK = S.MBR_SK,
 T.PROD_SK = S.PROD_SK,
 T.SUBGRP_SK = S.SUBGRP_SK,
 T.SUB_SK = S.SUB_SK,
 T.MBR_SFX_NO = S.MBR_SFX_NO,
 T.SUB_ID = S.SUB_ID
WHEN NOT MATCHED THEN INSERT
 (
  CLM_SK,
  LAST_UPDT_RUN_CYC_EXCTN_SK,
  ALPHA_PFX_SK,
  CLS_SK,
  CLS_PLN_SK,
  EXPRNC_CAT_SK,
  FNCL_LOB_SK,
  GRP_SK,
  MBR_SK,
  PROD_SK,
  SUBGRP_SK,
  SUB_SK,
  MBR_SFX_NO,
  SUB_ID
 )
 VALUES
 (
  S.CLM_SK,
  S.LAST_UPDT_RUN_CYC_EXCTN_SK,
  S.ALPHA_PFX_SK,
  S.CLS_SK,
  S.CLS_PLN_SK,
  S.EXPRNC_CAT_SK,
  S.FNCL_LOB_SK,
  S.GRP_SK,
  S.MBR_SK,
  S.PROD_SK,
  S.SUBGRP_SK,
  S.SUB_SK,
  S.MBR_SFX_NO,
  S.SUB_ID
);
"""
execute_dml(merge_sql_ids, jdbc_url_ids, jdbc_props_ids)

# Seq_PClmMtchSk => write df_Lnk_PClmMtch to verified/BCAFEPMedPClmMbrErrRecyc_Delete.dat.#RunID#
write_files(
  df_Lnk_PClmMtch.select("CLM_SK"),
  f"{adls_path}/verified/BCAFEPMedPClmMbrErrRecyc_Delete.dat.{RunID}",
  delimiter=",",
  mode="overwrite",
  is_pqruet=False,
  header=False,
  quote="\"",
  nullValue=None
)

# Trn_CLM => primary link df_Lnk_IDS_CLM, left join df_prod_1, df_fncl_lob_1, df_exprnc_cat_1, df_grp_1, df_edw_clm
join_trn_clm_1 = df_Lnk_IDS_CLM.alias("Lnk_IDS_CLM").join(
    df_prod_1.alias("PROD"),
    F.col("Lnk_IDS_CLM.PROD_SK") == F.col("PROD.PROD_SK"),
    "left"
)
join_trn_clm_2 = join_trn_clm_1.join(
    df_fncl_lob_1.alias("FNCL_LOB"),
    F.col("Lnk_IDS_CLM.FNCL_LOB_SK") == F.col("FNCL_LOB.FNCL_LOB_SK"),
    "left"
)
join_trn_clm_3 = join_trn_clm_2.join(
    df_exprnc_cat_1.alias("EXPRNC_CAT"),
    F.col("Lnk_IDS_CLM.EXPRNC_CAT_SK") == F.col("EXPRNC_CAT.EXPRNC_CAT_SK"),
    "left"
)
join_trn_clm_4 = join_trn_clm_3.join(
    df_grp_1.alias("GRP"),
    F.col("Lnk_IDS_CLM.GRP_SK") == F.col("GRP.GRP_SK"),
    "left"
)
join_trn_clm_5 = join_trn_clm_4.join(
    df_edw_clm.alias("EDW_CLM"),
    F.col("Lnk_IDS_CLM.CLM_SK") == F.col("EDW_CLM.CLM_SK"),
    "left"
)

df_Trn_CLM = join_trn_clm_5.select(
    F.col("Lnk_IDS_CLM.*"),
    F.col("EDW_CLM.CLM_SK").alias("EDW_CLM_SK"),
    F.col("EXPRNC_CAT.EXPRNC_CAT_CD").alias("EXPRNC_CAT_EXPRNC_CAT_CD"),
    F.col("EXPRNC_CAT.FUND_CAT_CD").alias("EXPRNC_CAT_FUND_CAT_CD"),
    F.col("FNCL_LOB.FNCL_LOB_CD").alias("FNCL_LOB_FNCL_LOB_CD"),
    F.col("GRP.GRP_ID").alias("GRP_GRP_ID"),
    F.col("PROD.PROD_SH_NM_SK").alias("PROD_PROD_SH_NM_SK"),
    F.col("PROD.PROD_SH_NM").alias("PROD_PROD_SH_NM"),
    F.col("PROD.PROD_ST_CD").alias("PROD_PROD_ST_CD")
)

# Output pin "Lnk_EDW_CLM" => constraint IsNull(EDW_CLM.CLM_SK)=false => filter EDW_CLM_SK not null
df_Lnk_EDW_CLM_all = df_Trn_CLM.filter(F.col("EDW_CLM_SK").isNotNull())
df_Lnk_EDW_CLM = df_Lnk_EDW_CLM_all.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.rpad(F.col("RunDate"),10," ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ALPHA_PFX_SK").alias("ALPHA_PFX_SK"),
    F.col("CLS_SK").alias("CLS_SK"),
    F.col("CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
    F.col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.rpad(F.col("MBR_SFX_NO"),2," ").alias("CLM_MBR_SFX_NO"),
    F.when(F.col("EXPRNC_CAT_EXPRNC_CAT_CD").isNull()| (F.trim(F.col("EXPRNC_CAT_EXPRNC_CAT_CD"))==""),F.lit("NA")).otherwise(F.col("EXPRNC_CAT_EXPRNC_CAT_CD")).alias("EXPRNC_CAT_CD"),
    F.when(F.col("EXPRNC_CAT_FUND_CAT_CD").isNull()| (F.trim(F.col("EXPRNC_CAT_FUND_CAT_CD"))==""),F.lit("NA")).otherwise(F.col("EXPRNC_CAT_FUND_CAT_CD")).alias("FUND_CAT_CD"),
    F.when(F.col("FNCL_LOB_FNCL_LOB_CD").isNull()| (F.trim(F.col("FNCL_LOB_FNCL_LOB_CD"))==""),F.lit("0000")).otherwise(F.col("FNCL_LOB_FNCL_LOB_CD")).alias("FNCL_LOB_CD"),
    F.when(F.col("GRP_GRP_ID").isNull()| (F.trim(F.col("GRP_GRP_ID"))==""),F.lit("UNK")).otherwise(F.col("GRP_GRP_ID")).alias("GRP_ID"),
    F.when(F.col("PROD_PROD_SH_NM_SK").isNull(),F.lit(0)).otherwise(F.col("PROD_PROD_SH_NM_SK")).alias("PROD_SH_NM_SK"),
    F.when(F.col("PROD_PROD_SH_NM").isNull()| (F.trim(F.col("PROD_PROD_SH_NM"))==""),F.lit(" ")).otherwise(F.col("PROD_PROD_SH_NM")).alias("PROD_SH_NM"),
    F.when(F.col("PROD_PROD_ST_CD").isNull()| (F.trim(F.col("PROD_PROD_ST_CD"))==""),F.lit("NA")).otherwise(F.col("PROD_PROD_ST_CD")).alias("PROD_ST_CD"),
    F.rpad(F.lit("N"),1," ").alias("CLM_MEDIGAP_IN")
)

# EDW_CLM_Update => MERGE into {EDWOwner}.CLM_F
execute_dml(f"DROP TABLE IF EXISTS STAGING.BCAFEPMedErrClmMbrUpd_EDW_CLM_Update_temp", jdbc_url_edw, jdbc_props_edw)
df_Lnk_EDW_CLM.write \
    .format("jdbc") \
    .option("url", jdbc_url_edw) \
    .options(**jdbc_props_edw) \
    .option("dbtable","STAGING.BCAFEPMedErrClmMbrUpd_EDW_CLM_Update_temp") \
    .mode("append") \
    .save()
merge_sql_edw = f"""
MERGE INTO {EDWOwner}.CLM_F AS T
USING STAGING.BCAFEPMedErrClmMbrUpd_EDW_CLM_Update_temp AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN UPDATE SET
 T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
 T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
 T.ALPHA_PFX_SK = S.ALPHA_PFX_SK,
 T.CLS_SK = S.CLS_SK,
 T.CLS_PLN_SK = S.CLS_PLN_SK,
 T.EXPRNC_CAT_SK = S.EXPRNC_CAT_SK,
 T.FNCL_LOB_SK = S.FNCL_LOB_SK,
 T.GRP_SK = S.GRP_SK,
 T.MBR_SK = S.MBR_SK,
 T.PROD_SK = S.PROD_SK,
 T.SUBGRP_SK = S.SUBGRP_SK,
 T.SUB_SK = S.SUB_SK,
 T.CLM_MBR_SFX_NO = S.CLM_MBR_SFX_NO,
 T.EXPRNC_CAT_CD = S.EXPRNC_CAT_CD,
 T.FUND_CAT_CD = S.FUND_CAT_CD,
 T.FNCL_LOB_CD = S.FNCL_LOB_CD,
 T.GRP_ID = S.GRP_ID,
 T.PROD_SH_NM_SK = S.PROD_SH_NM_SK,
 T.PROD_SH_NM = S.PROD_SH_NM,
 T.PROD_ST_CD = S.PROD_ST_CD,
 T.CLM_MEDIGAP_IN = S.CLM_MEDIGAP_IN
WHEN NOT MATCHED THEN INSERT
(
 CLM_SK,
 LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
 LAST_UPDT_RUN_CYC_EXCTN_SK,
 ALPHA_PFX_SK,
 CLS_SK,
 CLS_PLN_SK,
 EXPRNC_CAT_SK,
 FNCL_LOB_SK,
 GRP_SK,
 MBR_SK,
 PROD_SK,
 SUBGRP_SK,
 SUB_SK,
 CLM_MBR_SFX_NO,
 EXPRNC_CAT_CD,
 FUND_CAT_CD,
 FNCL_LOB_CD,
 GRP_ID,
 PROD_SH_NM_SK,
 PROD_SH_NM,
 PROD_ST_CD,
 CLM_MEDIGAP_IN
)
VALUES
(
 S.CLM_SK,
 S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
 S.LAST_UPDT_RUN_CYC_EXCTN_SK,
 S.ALPHA_PFX_SK,
 S.CLS_SK,
 S.CLS_PLN_SK,
 S.EXPRNC_CAT_SK,
 S.FNCL_LOB_SK,
 S.GRP_SK,
 S.MBR_SK,
 S.PROD_SK,
 S.SUBGRP_SK,
 S.SUB_SK,
 S.CLM_MBR_SFX_NO,
 S.EXPRNC_CAT_CD,
 S.FUND_CAT_CD,
 S.FNCL_LOB_CD,
 S.GRP_ID,
 S.PROD_SH_NM_SK,
 S.PROD_SH_NM,
 S.PROD_ST_CD,
 S.CLM_MEDIGAP_IN
);
"""
execute_dml(merge_sql_edw, jdbc_url_edw, jdbc_props_edw)