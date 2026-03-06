# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : EyeMedClmLandSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION :  EyeMed Claims Member Matching Error File Load.
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                             Date                 Project/Altiris #          Change Description                                                        Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                       --------------------         ------------------------          -----------------------------------------------------------------------               --------------------------------         -------------------------------   ----------------------------      
# MAGIC Madhavan B                     2018-03-14              5781                     Original Programming                                                       IntegrateDev2                   Kalyan Neelam           2018-04-09


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F, types as T
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# 1) Retrieve Job Parameters
SrcSysCd = get_widget_value("SrcSysCd","")
RunCycle = get_widget_value("RunCycle","")
RunID = get_widget_value("RunID","")
IDSOwner = get_widget_value("IDSOwner","")
Logging = get_widget_value("Logging","")
EDWOwner = get_widget_value("EDWOwner","")
RunDate = get_widget_value("RunDate","")
ids_secret_name = get_widget_value("ids_secret_name","")
edw_secret_name = get_widget_value("edw_secret_name","")

# 2) Database Config
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

# 3) Stage: IDS_LKP (DB2Connector) => Four Output Pins => Scenario A HashedFile => Directly Deduplicate
df_IDS_LKP_EXPRNC_CAT_1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
        SELECT 
        EXPRNC_CAT_SK,
        EXPRNC_CAT_CD,
        TRGT_CD FUND_CAT_CD,
        'Y' as EXPRNC_IND
        FROM {IDSOwner}.EXPRNC_CAT,
             {IDSOwner}.CD_MPPNG 
        WHERE EXPRNC_CAT_FUND_CAT_CD_SK = CD_MPPNG_SK+0
    """)
    .load()
)
df_HASH_1_EXPRNC_CAT = df_IDS_LKP_EXPRNC_CAT_1.dropDuplicates(["EXPRNC_CAT_SK"])

df_IDS_LKP_FNCL_LOB_1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
        SELECT 
        FNCL_LOB_SK,
        FNCL_LOB_CD,
        'Y' as FNCL_LOB_IND
        FROM {IDSOwner}.FNCL_LOB
    """)
    .load()
)
df_HASH_1_FNCL_LOB = df_IDS_LKP_FNCL_LOB_1.dropDuplicates(["FNCL_LOB_SK"])

df_IDS_LKP_GRP_1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
        SELECT 
        GRP_SK,
        GRP_ID
        FROM {IDSOwner}.GRP
    """)
    .load()
)
df_HASH_1_GRP = df_IDS_LKP_GRP_1.dropDuplicates(["GRP_SK"])

df_IDS_LKP_PROD_1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
        SELECT 
        PROD_SK,
        PROD_ID,
        PROD.PROD_SH_NM_SK,
        CD.TRGT_CD as PROD_ST_CD,
        PROD_SH_NM
        FROM {IDSOwner}.PROD PROD,
             {IDSOwner}.PROD_SH_NM SH,
             {IDSOwner}.CD_MPPNG CD
        WHERE 
        PROD.PROD_SH_NM_SK = SH.PROD_SH_NM_SK
        AND PROD.PROD_ST_CD_SK = CD.CD_MPPNG_SK+0
    """)
    .load()
)
df_HASH_1_PROD = df_IDS_LKP_PROD_1.dropDuplicates(["PROD_SK"])

# 4) Stage: EDW_CLM_F (DB2Connector) => hf_eyemed_clm_edw_clm_f => Scenario A => Deduplicate then used in Trn_CLM
df_EDW_CLM_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", f"""
        SELECT
        CLM_SK
        FROM {EDWOwner}.CLM_F
        WHERE SRC_SYS_CD = 'EYEMED'
          AND MBR_SK = 0
    """)
    .load()
)
df_hf_eyemed_clm_edw_clm_f = df_EDW_CLM_F.dropDuplicates(["CLM_SK"])

# 5) Stage: IDS_CLM (DB2Connector) => hf_eyemed_clm_ids_clm => Scenario A => Deduplicate -> Goes to Trns1
df_IDS_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
        SELECT
        CLM.CLM_SK
        FROM {IDSOwner}.CLM AS CLM,
             {IDSOwner}.CD_MPPNG AS CD
        WHERE CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
          AND CD.TRGT_CD = 'EYEMED'
          AND CLM.MBR_SK = 0
    """)
    .load()
)
df_hf_eyemed_clm_ids_clm = df_IDS_CLM.dropDuplicates(["CLM_SK"])

# 6) Stage: ids (DB2Connector) => 3 Outputs => Hash => Scenario A => Deduplicate
df_ids_sub_alpha_pfx = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
        SELECT 
        SUB.SUB_UNIQ_KEY,
        ALPHA_PFX_CD 
        FROM {IDSOwner}.MBR MBR,
             {IDSOwner}.SUB SUB,
             {IDSOwner}.ALPHA_PFX PFX,
             {IDSOwner}.W_DRUG_ENR DRUG 
        WHERE 
        DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
        AND MBR.SUB_SK = SUB.SUB_SK 
        AND SUB.ALPHA_PFX_SK = PFX.ALPHA_PFX_SK
    """)
    .load()
)
df_hash_sub_alpha_pfx_lkup = df_ids_sub_alpha_pfx.dropDuplicates(["SUB_UNIQ_KEY"])

df_ids_mbr_enroll = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
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
        WHERE 
        DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
        AND DRUG.FILL_DT_SK BETWEEN MBR.EFF_DT_SK AND MBR.TERM_DT_SK
        AND MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
        AND MAP1.TRGT_CD IN ('VSN')
        AND MBR.CLS_SK = CLS.CLS_SK
        AND MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
        AND MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
        AND MBR.PROD_SK = CMPNT.PROD_SK
        AND CMPNT.PROD_CMPNT_TYP_CD_SK = MAP2.CD_MPPNG_SK
        AND MAP2.TRGT_CD= 'PDBL'
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
        AND BILL_CMPNT.PROD_BILL_CMPNT_ID IN ('VSN')
        AND BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK= (
          SELECT MAX (BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK)
          FROM {IDSOwner}.PROD_BILL_CMPNT BILL_CMPNT2
          WHERE BILL_CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
            AND CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
            AND BILL_CMPNT.PROD_BILL_CMPNT_ID = BILL_CMPNT2.PROD_BILL_CMPNT_ID
            AND BILL_CMPNT2.PROD_BILL_CMPNT_ID IN ('VSN')
            AND DRUG.FILL_DT_SK BETWEEN BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK
                                   AND BILL_CMPNT2.PROD_BILL_CMPNT_TERM_DT_SK
        )
        AND BILL_CMPNT.EXPRNC_CAT_SK = CAT.EXPRNC_CAT_SK
        AND BILL_CMPNT.FNCL_LOB_SK = LOB.FNCL_LOB_SK
    """)
    .load()
)
df_hash_mbr_enr_lkup = df_ids_mbr_enroll.dropDuplicates(["CLM_ID"])

df_ids_mbr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
        SELECT 
        MBR.MBR_UNIQ_KEY as MBR_UNIQ_KEY,
        SUB.SUB_UNIQ_KEY as SUB_UNIQ_KEY
        FROM {IDSOwner}.MBR MBR,
             {IDSOwner}.SUB SUB,
             {IDSOwner}.W_DRUG_ENR DRUG
        WHERE DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
          AND MBR.SUB_SK = SUB.SUB_SK
    """)
    .load()
)
df_hash_mbr_lkp = df_ids_mbr.dropDuplicates(["MBR_UNIQ_KEY"])

# 7) Stage: EyeMedClmLanding (CSeqFileStage) => Read
schema_EyeMedClmLanding = T.StructType([
    T.StructField("CLM_SK", T.IntegerType(), nullable=False),
    T.StructField("CLM_ID", T.StringType(), nullable=False),
    T.StructField("SRC_SYS_CD", T.StringType(), nullable=False),
    T.StructField("CLM_TYP_CD", T.StringType(), nullable=False),
    T.StructField("CLM_SUBTYP_CD", T.StringType(), nullable=False),
    T.StructField("CLM_SVC_STRT_DT_SK", T.StringType(), nullable=False),
    T.StructField("SRC_SYS_GRP_PFX", T.StringType(), nullable=False),
    T.StructField("SRC_SYS_GRP_ID", T.StringType(), nullable=False),
    T.StructField("SRC_SYS_GRP_SFX", T.StringType(), nullable=False),
    T.StructField("SUB_SSN", T.StringType(), nullable=False),
    T.StructField("PATN_LAST_NM", T.StringType(), nullable=False),
    T.StructField("PATN_FIRST_NM", T.StringType(), nullable=False),
    T.StructField("PATN_GNDR_CD", T.StringType(), nullable=False),
    T.StructField("PATN_BRTH_DT_SK", T.StringType(), nullable=False),
    T.StructField("MBR_UNIQ_KEY", T.IntegerType(), nullable=False),
    T.StructField("GRP_ID", T.StringType(), nullable=False),
    T.StructField("SUB_ID", T.StringType(), nullable=False),
    T.StructField("MBR_SFX_NO", T.StringType(), nullable=True),
    T.StructField("SUB_UNIQ_KEY", T.IntegerType(), nullable=False)
])
df_EyeMed = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_EyeMedClmLanding)
    .load(f"{adls_path}/verified/EyeMedErrClm_ClaimsLanding.dat.{RunID}")
)

# 8) Stage: BusinessRules (CTransformerStage) => Left join with mbr_lkp => output EyeMedClmTrns
df_BusinessRules_join = df_EyeMed.alias("EyeMed").join(
    df_hash_mbr_lkp.alias("mbr_lkp"),
    F.col("EyeMed.MBR_UNIQ_KEY") == F.col("mbr_lkp.MBR_UNIQ_KEY"),
    "left"
)
df_BusinessRules = (
    df_BusinessRules_join
    .withColumn(
        "svSubCk",
        F.when(F.col("mbr_lkp.MBR_UNIQ_KEY").isNull(), F.col("EyeMed.SUB_UNIQ_KEY"))
         .otherwise(F.col("mbr_lkp.SUB_UNIQ_KEY"))
    )
    .select(
        F.col("EyeMed.CLM_SK").alias("CLM_SK"),
        F.col("EyeMed.CLM_ID").alias("CLM_ID"),
        F.col("svSubCk").alias("SUB_CK"),
        F.col("EyeMed.SUB_ID").alias("SUB_ID"),
        F.col("EyeMed.CLM_TYP_CD").alias("CLM_TYP_CD"),
        F.col("EyeMed.GRP_ID").alias("GRP"),
        F.col("EyeMed.MBR_UNIQ_KEY").alias("MBR_CK"),
        F.col("EyeMed.MBR_SFX_NO").alias("MBR_SFX_NO")
    )
)

# 9) Stage: alpha_pfx (CTransformerStage) => Left join sub_alpha_pfx_lkup, mbr_enr_lkup => output ClmCrfIn1
df_alpha_pfx_join1 = df_BusinessRules.alias("EyeMedClmTrns").join(
    df_hash_sub_alpha_pfx_lkup.alias("sub_alpha_pfx_lkup"),
    F.col("EyeMedClmTrns.SUB_CK") == F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY"),
    "left"
)
df_alpha_pfx_join2 = df_alpha_pfx_join1.join(
    df_hash_mbr_enr_lkup.alias("mbr_enr_lkup"),
    F.col("EyeMedClmTrns.CLM_ID") == F.col("mbr_enr_lkup.CLM_ID"),
    "left"
)
df_alpha_pfx = (
    df_alpha_pfx_join2
    .select(
        F.col("EyeMedClmTrns.CLM_SK").alias("CLM_SK"),
        F.col("EyeMedClmTrns.CLM_ID").alias("CLM_ID"),
        F.when(F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY").isNull(), F.lit("UNK"))
         .otherwise(F.col("sub_alpha_pfx_lkup.ALPHA_PFX_CD"))
         .alias("ALPHA_PFX_CD"),
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
         .otherwise(F.col("mbr_enr_lkup.CLS_ID"))
         .alias("CLS"),
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
         .otherwise(F.col("mbr_enr_lkup.CLS_PLN_ID"))
         .alias("CLS_PLN"),
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
         .otherwise(F.col("mbr_enr_lkup.EXPRNC_CAT_CD"))
         .alias("EXPRNC_CAT"),
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
         .otherwise(F.col("mbr_enr_lkup.FNCL_LOB_CD"))
         .alias("FNCL_LOB_NO"),
        F.col("EyeMedClmTrns.GRP").alias("GRP"),
        F.col("EyeMedClmTrns.MBR_CK").alias("MBR_CK"),
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
         .otherwise(trim(F.col("mbr_enr_lkup.PROD_ID")))
         .alias("PROD"),
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
         .otherwise(F.col("mbr_enr_lkup.SUBGRP_ID"))
         .alias("SUBGRP"),
        F.col("EyeMedClmTrns.SUB_CK").alias("SUB_CK"),
        F.col("EyeMedClmTrns.CLM_TYP_CD").alias("CLM_TYP_CD"),
        F.col("EyeMedClmTrns.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("EyeMedClmTrns.SUB_ID").alias("SUB_ID")
    )
)

# 10) Stage: Trns1 (CTransformerStage) => PrimaryLink: ClmCrfIn1 => LookupLink: IDS_CLM => Three outputs
df_trns1_join = df_alpha_pfx.alias("ClmCrfIn1").join(
    df_hf_eyemed_clm_ids_clm.alias("IDS_CLM"),
    F.col("ClmCrfIn1.CLM_SK") == F.col("IDS_CLM.CLM_SK"),
    "left"
)

df_trns1_withvars = (
    df_trns1_join
    # Stage Variables:
    .withColumn("SrcSysCdMbr",
        F.when(
            F.col("ClmCrfIn1.CLM_SK").isNull(), F.lit("UNK")
        ).otherwise(
            F.when((F.col("SrcSysCd")=="") | (F.length(trim(F.col("SrcSysCd")))==0), "UNK")
             .otherwise(
                F.when(
                    F.col("SrcSysCd").isin(
                        "PCT","WELLDYNERX","PCS","CAREMARK","ARGUS","EDC","OT@2","ADOL","MOHSAIC","CAREADVANCE","ESI","OPTUMRX",
                        "MCSOURCE","MCAID","MEDTRAK","BCBSSC","BCA","BCBSA","SAVRX","LDI","EYEMED","CVS","LUMERIS","MEDIMPACT"
                    ),
                    "FACETS"
                ).otherwise(F.col("SrcSysCd"))
             )
        )
    )
    .withColumn("SrcSysCdProd",
        F.when(
            F.col("ClmCrfIn1.CLM_SK").isNull(), F.lit("UNK")
        ).otherwise(
            F.when((F.col("SrcSysCd")=="") | (F.length(trim(F.col("SrcSysCd")))==0), "UNK")
             .otherwise(
                F.when(
                    F.col("SrcSysCd").isin(
                        "PCT","WELLDYNERX","PCS","CAREMARK","ARGUS","EDC","OT@2","ADOL","ESI","OPTUMRX","MCSOURCE","MCAID",
                        "MEDTRAK","BCBSSC","BCA","BCBSA","SAVRX","LDI","EYEMED","CVS","LUMERIS","MEDIMPACT"
                    ),
                    "FACETS"
                ).otherwise(F.col("SrcSysCd"))
             )
        )
    )
    .withColumn("ClsPlnSk", GetFkeyClsPln(F.col("SrcSysCdMbr"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.CLS_PLN"), F.col("Logging")))
    .withColumn("ClsSk", GetFkeyCls(F.col("SrcSysCdMbr"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.GRP"), F.col("ClmCrfIn1.CLS"), F.col("Logging")))
    .withColumn("ExpCatCdSk", GetFkeyExprncCat(F.col("SrcSysCdProd"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.EXPRNC_CAT"), F.col("Logging")))
    .withColumn("GrpSk", GetFkeyGrp(F.col("SrcSysCdMbr"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.GRP"), F.col("Logging")))
    .withColumn("MbrSk", GetFkeyMbr(F.col("SrcSysCdMbr"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.MBR_CK"), F.col("Logging")))
    .withColumn("ProdSk", GetFkeyProd(F.col("SrcSysCdProd"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.PROD"), F.col("Logging")))
    .withColumn("SubGrpSk", GetFkeySubgrp(F.col("SrcSysCdMbr"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.GRP"), F.col("ClmCrfIn1.SUBGRP"), F.col("Logging")))
    .withColumn("SubSk", GetFkeySub(F.col("SrcSysCdMbr"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.SUB_CK"), F.col("Logging")))
    .withColumn("PlnAlphPfxSk", GetFkeyAlphaPfx(F.lit("BCA"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.ALPHA_PFX_CD"), F.col("Logging")))
    .withColumn("FinancialLOB", GetFkeyFnclLob(F.lit("PSI"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.FNCL_LOB_NO"), F.col("Logging")))
)

df_ClmFkeyOut1 = (
    df_trns1_withvars
    .filter(F.col("IDS_CLM.CLM_SK").isNotNull())
    .select(
        F.col("ClmCrfIn1.CLM_SK").alias("CLM_SK"),
        F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
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
        F.col("ClmCrfIn1.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("ClmCrfIn1.SUB_ID").alias("SUB_ID")
    )
)

df_Lnk_PClmMtch = (
    df_trns1_withvars
    .select(
        F.col("ClmCrfIn1.CLM_SK").alias("CLM_SK")
    )
)

df_Lnk_IDS_CLM = (
    df_trns1_withvars
    .select(
        F.col("ClmCrfIn1.CLM_SK").alias("CLM_SK"),
        F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
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
        F.col("ClmCrfIn1.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("ClmCrfIn1.SUB_ID").alias("SUB_ID")
    )
)

# 11) Stage: IDS_CLM_Update (DB2Connector) => Merge into #$IDSOwner#.CLM
# Create a physical STAGING table then merge
spark.sql(f"DROP TABLE IF EXISTS STAGING.EyeMedErrClmMbrUpd_IDS_CLM_Update_temp")
(
    df_ClmFkeyOut1
    .write
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", "STAGING.EyeMedErrClmMbrUpd_IDS_CLM_Update_temp")
    .mode("overwrite")
    .save()
)

merge_sql_ids = f"""
MERGE INTO {IDSOwner}.CLM as T
USING STAGING.EyeMedErrClmMbrUpd_IDS_CLM_Update_temp as S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.ALPHA_PFX_SK               = S.ALPHA_PFX_SK,
    T.CLS_SK                     = S.CLS_SK,
    T.CLS_PLN_SK                 = S.CLS_PLN_SK,
    T.EXPRNC_CAT_SK              = S.EXPRNC_CAT_SK,
    T.FNCL_LOB_SK                = S.FNCL_LOB_SK,
    T.GRP_SK                     = S.GRP_SK,
    T.MBR_SK                     = S.MBR_SK,
    T.PROD_SK                    = S.PROD_SK,
    T.SUBGRP_SK                  = S.SUBGRP_SK,
    T.SUB_SK                     = S.SUB_SK,
    T.MBR_SFX_NO                 = S.MBR_SFX_NO,
    T.SUB_ID                     = S.SUB_ID
WHEN NOT MATCHED THEN
  INSERT (
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
  VALUES (
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

# 12) Stage: Seq_PClmMtchSk (CSeqFileStage) => write df_Lnk_PClmMtch
df_Lnk_PClmMtch_out = df_Lnk_PClmMtch.select("CLM_SK")
write_files(
    df_Lnk_PClmMtch_out,
    f"{adls_path}/verified/EyeMedPClmMbrErrRecyc_Delete.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# 13) Stage: Trn_CLM (CTransformerStage) => PrimaryLink: Lnk_IDS_CLM => Lookups: PROD, FNCL_LOB, EXPRNC_CAT, GRP, EDW_CLM => Output Lnk_EDW_CLM
df_trn_clm_join1 = df_Lnk_IDS_CLM.alias("Lnk_IDS_CLM").join(
    df_HASH_1_PROD.alias("PROD"),
    F.col("Lnk_IDS_CLM.PROD_SK") == F.col("PROD.PROD_SK"),
    "left"
)
df_trn_clm_join2 = df_trn_clm_join1.join(
    df_HASH_1_FNCL_LOB.alias("FNCL_LOB"),
    F.col("Lnk_IDS_CLM.FNCL_LOB_SK") == F.col("FNCL_LOB.FNCL_LOB_SK"),
    "left"
)
df_trn_clm_join3 = df_trn_clm_join2.join(
    df_HASH_1_EXPRNC_CAT.alias("EXPRNC_CAT"),
    F.col("Lnk_IDS_CLM.EXPRNC_CAT_SK") == F.col("EXPRNC_CAT.EXPRNC_CAT_SK"),
    "left"
)
df_trn_clm_join4 = df_trn_clm_join3.join(
    df_HASH_1_GRP.alias("GRP"),
    F.col("Lnk_IDS_CLM.GRP_SK") == F.col("GRP.GRP_SK"),
    "left"
)
df_trn_clm_join5 = df_trn_clm_join4.join(
    df_hf_eyemed_clm_edw_clm_f.alias("EDW_CLM"),
    F.col("Lnk_IDS_CLM.CLM_SK") == F.col("EDW_CLM.CLM_SK"),
    "left"
)

df_trn_clm_Lnk_EDW_CLM = (
    df_trn_clm_join5
    .filter(F.col("EDW_CLM.CLM_SK").isNotNull())
    .select(
        F.col("Lnk_IDS_CLM.CLM_SK").alias("CLM_SK"),
        F.lit(RunDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("Lnk_IDS_CLM.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_IDS_CLM.ALPHA_PFX_SK").alias("ALPHA_PFX_SK"),
        F.col("Lnk_IDS_CLM.CLS_SK").alias("CLS_SK"),
        F.col("Lnk_IDS_CLM.CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.col("Lnk_IDS_CLM.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
        F.col("Lnk_IDS_CLM.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
        F.col("Lnk_IDS_CLM.GRP_SK").alias("GRP_SK"),
        F.col("Lnk_IDS_CLM.MBR_SK").alias("MBR_SK"),
        F.col("Lnk_IDS_CLM.PROD_SK").alias("PROD_SK"),
        F.col("Lnk_IDS_CLM.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("Lnk_IDS_CLM.SUB_SK").alias("SUB_SK"),
        F.col("Lnk_IDS_CLM.MBR_SFX_NO").alias("CLM_MBR_SFX_NO"),
        F.when(
            F.col("EXPRNC_CAT.EXPRNC_CAT_CD").isNull() | (F.trim(F.col("EXPRNC_CAT.EXPRNC_CAT_CD")) == ""),
            F.lit("NA")
        ).otherwise(F.col("EXPRNC_CAT.EXPRNC_CAT_CD")).alias("EXPRNC_CAT_CD"),
        F.when(
            F.col("EXPRNC_CAT.FUND_CAT_CD").isNull() | (F.trim(F.col("EXPRNC_CAT.FUND_CAT_CD")) == ""),
            F.lit("NA")
        ).otherwise(F.col("EXPRNC_CAT.FUND_CAT_CD")).alias("FUND_CAT_CD"),
        F.when(
            F.col("FNCL_LOB.FNCL_LOB_CD").isNull() | (F.trim(F.col("FNCL_LOB.FNCL_LOB_CD")) == ""),
            F.lit("0000")
        ).otherwise(F.col("FNCL_LOB.FNCL_LOB_CD")).alias("FNCL_LOB_CD"),
        F.when(
            F.col("GRP.GRP_ID").isNull() | (F.trim(F.col("GRP.GRP_ID"))==""),
            F.lit("UNK")
        ).otherwise(F.col("GRP.GRP_ID")).alias("GRP_ID"),
        F.when(
            F.col("PROD.PROD_SH_NM_SK").isNull(),
            F.lit(0)
        ).otherwise(F.col("PROD.PROD_SH_NM_SK")).alias("PROD_SH_NM_SK"),
        F.when(
            F.col("PROD.PROD_SH_NM").isNull() | (F.trim(F.col("PROD.PROD_SH_NM"))==""),
            F.lit(" ")
        ).otherwise(F.col("PROD.PROD_SH_NM")).alias("PROD_SH_NM"),
        F.when(
            F.col("PROD.PROD_ST_CD").isNull() | (F.trim(F.col("PROD.PROD_ST_CD"))==""),
            F.lit("NA")
        ).otherwise(F.col("PROD.PROD_ST_CD")).alias("PROD_ST_CD"),
        F.lit("N").alias("CLM_MEDIGAP_IN")
    )
)

# 14) Stage: EDW_CLM_Update (DB2Connector) => Merge into #$EDWOwner#.CLM_F
spark.sql(f"DROP TABLE IF EXISTS STAGING.EyeMedErrClmMbrUpd_EDW_CLM_Update_temp")
(
    df_trn_clm_Lnk_EDW_CLM
    .write
    .format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("dbtable", "STAGING.EyeMedErrClmMbrUpd_EDW_CLM_Update_temp")
    .mode("overwrite")
    .save()
)

merge_sql_edw = f"""
MERGE INTO {EDWOwner}.CLM_F AS T
USING STAGING.EyeMedErrClmMbrUpd_EDW_CLM_Update_temp AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
    T.LAST_UPDT_RUN_CYC_EXCTN_SK    = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.ALPHA_PFX_SK                  = S.ALPHA_PFX_SK,
    T.CLS_SK                        = S.CLS_SK,
    T.CLS_PLN_SK                    = S.CLS_PLN_SK,
    T.EXPRNC_CAT_SK                 = S.EXPRNC_CAT_SK,
    T.FNCL_LOB_SK                   = S.FNCL_LOB_SK,
    T.GRP_SK                        = S.GRP_SK,
    T.MBR_SK                        = S.MBR_SK,
    T.PROD_SK                       = S.PROD_SK,
    T.SUBGRP_SK                     = S.SUBGRP_SK,
    T.SUB_SK                        = S.SUB_SK,
    T.CLM_MBR_SFX_NO                = S.CLM_MBR_SFX_NO,
    T.EXPRNC_CAT_CD                 = S.EXPRNC_CAT_CD,
    T.FUND_CAT_CD                   = S.FUND_CAT_CD,
    T.FNCL_LOB_CD                   = S.FNCL_LOB_CD,
    T.GRP_ID                        = S.GRP_ID,
    T.PROD_SH_NM_SK                 = S.PROD_SH_NM_SK,
    T.PROD_SH_NM                    = S.PROD_SH_NM,
    T.PROD_ST_CD                    = S.PROD_ST_CD,
    T.CLM_MEDIGAP_IN                = S.CLM_MEDIGAP_IN
WHEN NOT MATCHED THEN
  INSERT (
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
  VALUES (
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