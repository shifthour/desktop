# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2018 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Reads the Livongo_Monthly_Billing_Claims.*.dat  file and puts the data into the claim common record format and takes it through primary key process using Shared Container ClmPkey
# MAGIC     
# MAGIC     
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                               Date                 Project/Altiris #       Change Description                                                                   Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                             --------------------     ------------------------       ----------------------------------------------------------------                                  --------------------------------       -------------------------------   ----------------------------       
# MAGIC SravyaSree Yarlagadda           2020-12-10      311337             Initial Programming as part of Livongo Encounter Claim Extract        IntegrateDev2               Manasa Andru              2021-03-17   
# MAGIC Lakshmi Devagiri                     2021-05-01        RA                 Updated Member Query to use the correct Join                                 IntegrateDev2	Abhiram Dasarathy	2021-05-03

# MAGIC Writing Sequential File to /key
# MAGIC This container is used in:
# MAGIC ESIClmExtr
# MAGIC FctsClmExtr
# MAGIC MCSourceClmExtr
# MAGIC MedicaidClmExtr
# MAGIC NascoClmExtr
# MAGIC PcsClmExtr
# MAGIC PCTAClmExtr
# MAGIC WellDyneClmExtr
# MAGIC MedtrakClmExtr
# MAGIC BCBSKCCommClmExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Lookup subscriber, product and member information
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmPK
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunCycle = get_widget_value('RunCycle','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
InFile_F = get_widget_value('InFile_F','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Stage: IDS (DB2Connector) - df_IDS_cls_pln
extract_query_cls_pln = """
SELECT 
PFX.ALPHA_PFX_CD||SUB.SUB_ID AS MEMBER_ID,
ME.EFF_DT_SK,
ME.TERM_DT_SK,
CLSPLN.CLS_PLN_ID,
CAT.EXPRNC_CAT_CD,
LOB.FNCL_LOB_CD,
P.PROD_ID
FROM 
#$IDSOwner#.MBR MBR
INNER JOIN #$IDSOwner#.SUB SUB
ON MBR.SUB_SK = SUB.SUB_SK
INNER JOIN #$IDSOwner#.ALPHA_PFX PFX
ON SUB.ALPHA_PFX_SK= PFX.ALPHA_PFX_SK
INNER JOIN #$IDSOwner#.MBR_ENR ME
ON MBR.MBR_SK=ME.MBR_SK
AND ME.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = 1949
LEFT OUTER JOIN  #$IDSOwner#.CLS_PLN CLSPLN
ON ME.CLS_PLN_SK= CLSPLN.CLS_PLN_SK
LEFT OUTER JOIN #$IDSOwner#.PROD P
ON ME.PROD_SK=P.PROD_SK
LEFT OUTER JOIN #$IDSOwner#.EXPRNC_CAT CAT
ON P.EXPRNC_CAT_SK=CAT.EXPRNC_CAT_SK
LEFT OUTER JOIN #$IDSOwner#.FNCL_LOB LOB
ON P.FNCL_LOB_SK = LOB.FNCL_LOB_SK
WHERE ME.term_dt_sk >= ((SELECT CAST(dbo.GetDateCST() AS DATE) FROM sysibm.sysdummy1))
"""
df_IDS_cls_pln = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_cls_pln)
    .load()
)

# Stage: IDS (DB2Connector) - df_IDS_sub_alpha_pfx
extract_query_sub_alpha_pfx = """
SELECT 
PFX.ALPHA_PFX_CD||SUB.SUB_ID AS MEMBER_ID,
PFX.ALPHA_PFX_CD,
CLS.CLS_ID,
GRP.GRP_ID,
MBR.MBR_UNIQ_KEY,
MBR.BRTH_DT_SK,
MBR.MBR_SFX_NO,
SUB.SUB_ID,
SUBGRP.SUBGRP_ID,
SUB.SUB_UNIQ_KEY
FROM 
#$IDSOwner#.MBR MBR
INNER JOIN #$IDSOwner#.SUB SUB
ON   MBR.SUB_SK = SUB.SUB_SK
INNER JOIN #$IDSOwner#.ALPHA_PFX PFX
ON SUB.ALPHA_PFX_SK= PFX.ALPHA_PFX_SK
INNER JOIN #$IDSOwner#.GRP GRP
ON GRP.GRP_SK=SUB.GRP_SK
INNER JOIN #$IDSOwner#.CLS CLS
ON CLS.CLS_SK=MBR.CLS_SK
INNER JOIN #$IDSOwner#.SUBGRP SUBGRP
ON MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
"""
df_IDS_sub_alpha_pfx = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_sub_alpha_pfx)
    .load()
)

# Stage: IDS (DB2Connector) - df_IDS_prov
extract_query_prov = """
SELECT 
 '1' AS FLAG,
 PROV_SPEC.PROV_SPEC_CD AS PROV_SPEC_CD,
 PROV_TYP.PROV_TYP_CD AS PROV_TYP_CD 
FROM #$IDSOwner#.P_SRC_DOMAIN_TRNSLTN SRC_DOMAIN
INNER JOIN #$IDSOwner#.PROV PROV
ON SRC_DOMAIN.TRGT_DOMAIN_TX = PROV.NTNL_PROV_ID
LEFT OUTER JOIN #$IDSOwner#.PROV_SPEC_CD PROV_SPEC
ON PROV.PROV_SPEC_CD_SK = PROV_SPEC.PROV_SPEC_CD_SK
LEFT OUTER JOIN #$IDSOwner#.PROV_TYP_CD PROV_TYP
ON PROV.PROV_TYP_CD_SK = PROV_TYP.PROV_TYP_CD_SK
where SRC_DOMAIN.SRC_SYS_CD = 'LVNGHLTH' 
and SRC_DOMAIN.DOMAIN_ID = 'LIVONGO_NPI'
AND PROV.SRC_SYS_CD_SK IN (select distinct src_cd_sk from #$IDSOwner#.cd_mppng CD
where CD.src_sys_cd = 'FACETS')
"""
df_IDS_prov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_prov)
    .load()
)

# Stage: Hash (CHashedFileStage) - Scenario A for all three links

# MbrID_lkup (dedup on MEMBER_ID)
df_MbrID_lkup = dedup_sort(df_IDS_cls_pln, ["MEMBER_ID"], [("MEMBER_ID","A")])
df_MbrID_lkup = df_MbrID_lkup.select(
    "MEMBER_ID",
    "EFF_DT_SK",
    "TERM_DT_SK",
    "CLS_PLN_ID",
    "EXPRNC_CAT_CD",
    "FNCL_LOB_CD",
    "PROD_ID"
)
# Apply rpad for char/varchar columns that have specified lengths
df_MbrID_lkup = df_MbrID_lkup.withColumn(
    "EFF_DT_SK", F.rpad(F.col("EFF_DT_SK"), 10, " ")
).withColumn(
    "TERM_DT_SK", F.rpad(F.col("TERM_DT_SK"), 10, " ")
).withColumn(
    "PROD_ID", F.rpad(F.col("PROD_ID"), 8, " ")
)

# sub_alpha_pfx_lkup (dedup on MEMBER_ID)
df_sub_alpha_pfx_lkup = dedup_sort(df_IDS_sub_alpha_pfx, ["MEMBER_ID"], [("MEMBER_ID","A")])
df_sub_alpha_pfx_lkup = df_sub_alpha_pfx_lkup.select(
    "MEMBER_ID",
    "ALPHA_PFX_CD",
    "CLS_ID",
    "GRP_ID",
    "MBR_UNIQ_KEY",
    "BRTH_DT_SK",
    "MBR_SFX_NO",
    "SUB_ID",
    "SUBGRP_ID",
    "SUB_UNIQ_KEY"
)
df_sub_alpha_pfx_lkup = df_sub_alpha_pfx_lkup.withColumn(
    "BRTH_DT_SK", F.rpad(F.col("BRTH_DT_SK"), 10, " ")
).withColumn(
    "MBR_SFX_NO", F.rpad(F.col("MBR_SFX_NO"), 2, " ")
)

# prov_lkp (dedup on FLAG)
df_prov_lkp = dedup_sort(df_IDS_prov, ["FLAG"], [("FLAG","A")])
df_prov_lkp = df_prov_lkp.select(
    "FLAG",
    "PROV_SPEC_CD",
    "PROV_TYP_CD"
)
# No lengths are specified for these columns, so no rpad needed

# Stage: LivongoEncClmLanding (CSeqFileStage, reading CSV)
schema_LivongoEncClmLanding = StructType([
    StructField("livongo_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("birth_date", StringType(), True),
    StructField("client_name", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("claim_code", StringType(), True),
    StructField("service_date", StringType(), True),
    StructField("quantity", StringType(), True)
])
df_LivongoEncClmLanding = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_LivongoEncClmLanding)
    .load(f"{adls_path}/verified/{InFile_F}")
)

# Stage: BusinessRules (CTransformerStage)
# Perform left joins on MbrID_lkup, sub_alpha_pfx_lkup
df_BusinessRules = (
    df_LivongoEncClmLanding.alias("Livongo")
    .join(df_MbrID_lkup.alias("MbrID_lkup"), F.col("Livongo.member_id")==F.col("MbrID_lkup.MEMBER_ID"), "left")
    .join(df_sub_alpha_pfx_lkup.alias("sub_alpha_pfx_lkup"), F.col("Livongo.member_id")==F.col("sub_alpha_pfx_lkup.MEMBER_ID"), "left")
)

# Calculate stage variables in columns
df_BusinessRules = df_BusinessRules.withColumn(
    "Svcdt",
    F.when(
        (F.col("Livongo.service_date").isNull()) | (F.length(F.col("Livongo.service_date")) == 0),
        F.lit("1753-01-01")
    ).otherwise(
        # replicate the date format to CCYY-MM-DD
        F.expr("substring(Livongo.service_date,1,4)||'-'||substring(Livongo.service_date,6,2)||'-'||substring(Livongo.service_date,9,2)")
    )
).withColumn(
    "svSubCk",
    F.when(F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY").isNull(), F.lit(1))
    .otherwise(F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY"))
)

# Output link "LivEncClmTrns"
df_LivEncClmTrns = df_BusinessRules.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"),1," ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"),1," ").alias("PASS_THRU_IN"),
    F.lit(CurrentDate).alias("FIRST_RECYC_DT"), 
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("SrcSysCd").alias("SRC_SYS_CD"),
    F.concat(F.lit("SrcSysCd"),F.lit(";"),F.col("Livongo.livongo_id")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_SK"),
    F.lit("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    # CLM_ID => char(18)
    F.rpad(F.col("Livongo.livongo_id"),18," ").alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.lit("NA"),12," ").alias("ADJ_FROM_CLM"),
    F.rpad(F.lit("NA"),12," ").alias("ADJ_TO_CLM"),
    # ALPHA_PFX_CD => char(3)
    F.rpad(
        F.when(
            F.col("sub_alpha_pfx_lkup.ALPHA_PFX_CD").isNotNull(),
            F.col("sub_alpha_pfx_lkup.ALPHA_PFX_CD")
        ).otherwise(F.lit("UNK")),
        3," "
    ).alias("ALPHA_PFX_CD"),
    F.rpad(F.lit("XFE"),3," ").alias("CLM_EOB_EXCD"),
    # CLS => char(4)
    F.rpad(
        F.when(
            F.col("sub_alpha_pfx_lkup.CLS_ID").isNotNull(),
            F.col("sub_alpha_pfx_lkup.CLS_ID")
        ).otherwise(F.lit("UNK")),
        4," "
    ).alias("CLS"),
    # CLS_PLN => char(8)
    F.rpad(
        F.when(
            (F.col("Livongo.service_date") >= F.col("MbrID_lkup.EFF_DT_SK")) &
            (F.col("Livongo.service_date") <= F.col("MbrID_lkup.TERM_DT_SK")),
            F.col("MbrID_lkup.CLS_PLN_ID")
        ).otherwise(F.lit("UNK")),
        8," "
    ).alias("CLS_PLN"),
    # EXPRNC_CAT => char(4)
    F.rpad(
        F.when(
            (F.col("Livongo.service_date") >= F.col("MbrID_lkup.EFF_DT_SK")) &
            (F.col("Livongo.service_date") <= F.col("MbrID_lkup.TERM_DT_SK")),
            F.col("MbrID_lkup.EXPRNC_CAT_CD")
        ).otherwise(F.lit("UNK")),
        4," "
    ).alias("EXPRNC_CAT"),
    # FNCL_LOB_NO => char(4)
    F.rpad(
        F.when(
            (F.col("Livongo.service_date") >= F.col("MbrID_lkup.EFF_DT_SK")) &
            (F.col("Livongo.service_date") <= F.col("MbrID_lkup.TERM_DT_SK")),
            F.col("MbrID_lkup.FNCL_LOB_CD")
        ).otherwise(F.lit("UNK")),
        4," "
    ).alias("FNCL_LOB_NO"),
    # GRP => char(8)
    F.rpad(
        F.when(
            F.col("sub_alpha_pfx_lkup.GRP_ID").isNotNull(),
            F.col("sub_alpha_pfx_lkup.GRP_ID")
        ).otherwise(F.lit("UNK")),
        8," "
    ).alias("GRP"),
    F.when(
        F.col("sub_alpha_pfx_lkup.MBR_UNIQ_KEY").isNotNull(),
        F.col("sub_alpha_pfx_lkup.MBR_UNIQ_KEY")
    ).otherwise(F.lit("NA")).alias("MBR_CK"),
    # PROD => char(8)
    F.rpad(
        F.when(
            (F.col("Livongo.service_date") >= F.col("MbrID_lkup.EFF_DT_SK")) &
            (F.col("Livongo.service_date") <= F.col("MbrID_lkup.TERM_DT_SK")),
            F.col("MbrID_lkup.PROD_ID")
        ).otherwise(F.lit("UNK")),
        8," "
    ).alias("PROD"),
    # SUBGRP => char(4)
    F.rpad(
        F.when(
            F.col("sub_alpha_pfx_lkup.SUBGRP_ID").isNotNull(),
            F.col("sub_alpha_pfx_lkup.SUBGRP_ID")
        ).otherwise(F.lit("UNK")),
        4," "
    ).alias("SUBGRP"),
    F.col("svSubCk").alias("SUB_CK"),
    F.rpad(F.lit("NA"),10," ").alias("CLM_ACDNT_CD"),
    F.rpad(F.lit("NA"),2," ").alias("CLM_ACDNT_ST_CD"),
    F.rpad(F.lit("NA"),10," ").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
    F.rpad(F.lit("NA"),10," ").alias("CLM_AGMNT_SRC_CD"),
    F.rpad(F.lit("NA"),1," ").alias("CLM_BTCH_ACTN_CD"),
    F.rpad(F.lit("N"),10," ").alias("CLM_CAP_CD"),
    F.rpad(F.lit("STD"),10," ").alias("CLM_CAT_CD"),
    F.rpad(F.lit("NA"),1," ").alias("CLM_CHK_CYC_OVRD_CD"),
    F.rpad(F.lit("NA"),2," ").alias("CLM_COB_CD"),
    F.lit("ACPTD").alias("FINL_DISP_CD"),
    F.rpad(F.lit("NA"),1," ").alias("CLM_INPT_METH_CD"),
    F.rpad(F.lit("NA"),10," ").alias("CLM_INPT_SRC_CD"),
    F.rpad(F.lit("NA"),10," ").alias("CLM_IPP_CD"),
    F.rpad(F.lit("NA"),2," ").alias("CLM_NTWK_STTUS_CD"),
    F.rpad(F.lit("NA"),4," ").alias("CLM_NONPAR_PROV_PFX_CD"),
    F.rpad(F.lit("NA"),1," ").alias("CLM_OTHER_BNF_CD"),
    F.rpad(F.lit("PROV"),1," ").alias("CLM_PAYE_CD"),
    F.rpad(F.lit("NA"),4," ").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.rpad(F.lit("NA"),4," ").alias("CLM_SVC_DEFN_PFX_CD"),
    # CLM_SVC_PROV_SPEC_CD => '1'
    F.rpad(F.lit("1"),10," ").alias("CLM_SVC_PROV_SPEC_CD"),
    # CLM_SVC_PROV_TYP_CD => '1'
    F.rpad(F.lit("1"),10," ").alias("CLM_SVC_PROV_TYP_CD"),
    F.rpad(F.lit("A02"),2," ").alias("CLM_STTUS_CD"),
    F.rpad(F.lit("NA"),10," ").alias("CLM_SUBMT_BCBS_PLN_CD"),
    F.rpad(F.lit("NA"),10," ").alias("CLM_SUB_BCBS_PLN_CD"),
    F.rpad(F.lit("PR"),10," ").alias("CLM_SUBTYP_CD"),
    F.rpad(F.lit("MED"),1," ").alias("CLM_TYP_CD"),
    F.rpad(F.lit("N"),1," ").alias("ATCHMT_IN"),
    F.rpad(F.lit("N"),1," ").alias("CLM_CLNCL_EDIT_CD"),
    F.rpad(F.lit("N"),1," ").alias("COBRA_CLM_IN"),
    F.rpad(F.lit("N"),1," ").alias("FIRST_PASS_IN"),
    F.rpad(F.lit("N"),1," ").alias("HOST_IN"),
    F.rpad(F.lit("N"),1," ").alias("LTR_IN"),
    F.rpad(F.lit("N"),1," ").alias("MCARE_ASG_IN"),
    F.rpad(F.lit("N"),1," ").alias("NOTE_IN"),
    F.rpad(F.lit("N"),1," ").alias("PCA_AUDIT_IN"),
    F.rpad(F.lit("N"),1," ").alias("PCP_SUBMT_IN"),
    F.rpad(F.lit("N"),1," ").alias("PROD_OOA_IN"),
    F.rpad(F.lit("1753-01-01"),10," ").alias("ACDNT_DT"),
    F.rpad(F.lit("1753-01-01"),10," ").alias("INPT_DT"),
    F.rpad(F.lit("1753-01-01"),10," ").alias("MBR_PLN_ELIG_DT"),
    F.rpad(F.lit("1753-01-01"),10," ").alias("NEXT_RVW_DT"),
    # PD_DT => Livongo.service_date (char 10)
    F.rpad(F.col("Livongo.service_date"),10," ").alias("PD_DT"),
    F.rpad(F.lit("1753-01-01"),10," ").alias("PAYMT_DRAG_CYC_DT"),
    F.rpad(F.lit("1753-01-01"),10," ").alias("PRCS_DT"),
    F.rpad(F.lit("1753-01-01"),10," ").alias("RCVD_DT"),
    # SVC_STRT_DT => Livongo.service_date (char 10)
    F.rpad(F.col("Livongo.service_date"),10," ").alias("SVC_STRT_DT"),
    # SVC_END_DT => Livongo.service_date (char 10)
    F.rpad(F.col("Livongo.service_date"),10," ").alias("SVC_END_DT"),
    F.rpad(F.lit("1753-01-01"),10," ").alias("SMLR_ILNS_DT"),
    F.rpad(F.lit("1753-01-01"),10," ").alias("STTUS_DT"),
    F.rpad(F.lit("1753-01-01"),10," ").alias("WORK_UNABLE_BEG_DT"),
    F.rpad(F.lit("2199-12-31"),10," ").alias("WORK_UNABLE_END_DT"),
    F.lit(0.00).alias("ACDNT_AMT"),
    F.lit(0.00).alias("ACTL_PD_AMT"),
    F.lit(0.00).alias("ALLOW_AMT"),
    F.lit(0.00).alias("DSALW_AMT"),
    F.lit(0.00).alias("COINS_AMT"),
    F.lit(0.00).alias("CNSD_CHRG_AMT"),
    F.lit(0.00).alias("COPAY_AMT"),
    F.lit(0.00).alias("CHRG_AMT"),
    F.lit(0.00).alias("DEDCT_AMT"),
    F.lit(0.00).alias("PAYBL_AMT"),
    F.lit(1).alias("CLM_CT"),
    # MBR_AGE => DS expression using "AGE" function, assume user-defined. We'll call it directly:
    # "AGE((FORMAT.DATE(Livongo.birth_date[1,4] : ...)), (FORMAT.DATE(Livongo.service_date[1,4]...)))"
    # We can treat "AGE" as a user-defined function if needed. For strictness, just replicate the call:
    F.expr('AGE('
           '(substring(Livongo.birth_date,1,4)||substring(Livongo.birth_date,6,2)||substring(Livongo.birth_date,9,2)),'
           '(substring(Livongo.service_date,1,4)||substring(Livongo.service_date,6,2)||substring(Livongo.service_date,9,2))'
           ')').alias("MBR_AGE"),
    F.lit("NA").alias("ADJ_FROM_CLM_ID"),
    F.lit("NA").alias("ADJ_TO_CLM_ID"),
    F.rpad(F.lit("NA"),18," ").alias("DOC_TX_ID"),
    F.rpad(F.lit("1"),15," ").alias("MCAID_RESUB_NO"),
    F.rpad(F.lit("NA"),12," ").alias("MCARE_ID"),
    # MBR_SFX_NO => char(2)
    F.rpad(
        F.when(
            F.col("sub_alpha_pfx_lkup.MBR_SFX_NO").isNotNull(),
            F.trim(F.col("sub_alpha_pfx_lkup.MBR_SFX_NO"))
        ).otherwise(F.lit("0")),
        2," "
    ).alias("MBR_SFX_NO"),
    F.lit("1").alias("PATN_ACCT_NO"),
    F.rpad(F.lit("NA"),16," ").alias("PAYMT_REF_ID"),
    F.rpad(F.lit("NA"),12," ").alias("PROV_AGMNT_ID"),
    F.lit("NA").alias("RFRNG_PROV_TX"),
    # SUB_ID => char(14)
    F.rpad(
        F.when(
            F.col("sub_alpha_pfx_lkup.SUB_ID").isNotNull(),
            F.trim(F.col("sub_alpha_pfx_lkup.SUB_ID"))
        ).otherwise(F.lit("UNK")),
        14," "
    ).alias("SUB_ID"),
    F.lit("NA").alias("PCA_TYP_CD"),
    F.lit("NA").alias("REL_PCA_CLM_ID"),
    F.lit("NA").alias("REL_BASE_CLM_ID"), 
    F.lit(0.00).alias("REMIT_SUPRSION_AMT"),
    F.lit("NA").alias("MCAID_STTUS_ID"),
    F.lit(0.00).alias("PATN_PD_AMT"),
    F.lit("ICD10").alias("CLM_SUBMT_ICD_VRSN_CD"),
    F.rpad(F.lit("NA"),12," ").alias("NTWK"),
    F.rpad(F.lit("N"),1," ").alias("BILL_PAYMT_EXCL_IN"),
    F.col("Livongo.member_id").alias("member_id")
)

# Stage: alpha_pfx (CTransformerStage)
# Primary link = df_LivEncClmTrns, left lookup = df_prov_lkp on "LivEncClmTrns.CLM_SVC_PROV_SPEC_CD" == "prov_lkp.FLAG"
df_alpha_pfx = (
    df_LivEncClmTrns.alias("LivEncClmTrns")
    .join(
        df_prov_lkp.alias("prov_lkp"),
        F.col("LivEncClmTrns.CLM_SVC_PROV_SPEC_CD") == F.col("prov_lkp.FLAG"), 
        "left"
    )
)

# Output link "Transform"
df_Transform = df_alpha_pfx.select(
    F.col("LivEncClmTrns.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("LivEncClmTrns.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("LivEncClmTrns.DISCARD_IN").alias("DISCARD_IN"),
    F.col("LivEncClmTrns.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("LivEncClmTrns.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("LivEncClmTrns.ERR_CT").alias("ERR_CT"),
    F.col("LivEncClmTrns.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("LivEncClmTrns.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("LivEncClmTrns.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("LivEncClmTrns.CLM_SK").alias("CLM_SK"),
    F.col("LivEncClmTrns.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("LivEncClmTrns.CLM_ID").alias("CLM_ID"),
    F.col("LivEncClmTrns.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LivEncClmTrns.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("LivEncClmTrns.ADJ_FROM_CLM").alias("ADJ_FROM_CLM"),
    F.col("LivEncClmTrns.ADJ_TO_CLM").alias("ADJ_TO_CLM"),
    F.col("LivEncClmTrns.ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
    F.col("LivEncClmTrns.CLM_EOB_EXCD").alias("CLM_EOB_EXCD"),
    F.col("LivEncClmTrns.CLS").alias("CLS"),
    F.col("LivEncClmTrns.CLS_PLN").alias("CLS_PLN"),
    F.col("LivEncClmTrns.EXPRNC_CAT").alias("EXPRNC_CAT"),
    F.col("LivEncClmTrns.FNCL_LOB_NO").alias("FNCL_LOB_NO"),
    F.col("LivEncClmTrns.GRP").alias("GRP"),
    F.col("LivEncClmTrns.MBR_CK").alias("MBR_CK"),
    F.col("LivEncClmTrns.PROD").alias("PROD"),
    F.col("LivEncClmTrns.SUBGRP").alias("SUBGRP"),
    F.col("LivEncClmTrns.SUB_CK").alias("SUB_CK"),
    F.col("LivEncClmTrns.CLM_ACDNT_CD").alias("CLM_ACDNT_CD"),
    F.col("LivEncClmTrns.CLM_ACDNT_ST_CD").alias("CLM_ACDNT_ST_CD"),
    F.col("LivEncClmTrns.CLM_ACTIVATING_BCBS_PLN_CD").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
    F.col("LivEncClmTrns.CLM_AGMNT_SRC_CD").alias("CLM_AGMNT_SRC_CD"),
    F.col("LivEncClmTrns.CLM_BTCH_ACTN_CD").alias("CLM_BTCH_ACTN_CD"),
    F.col("LivEncClmTrns.CLM_CAP_CD").alias("CLM_CAP_CD"),
    F.col("LivEncClmTrns.CLM_CAT_CD").alias("CLM_CAT_CD"),
    F.col("LivEncClmTrns.CLM_CHK_CYC_OVRD_CD").alias("CLM_CHK_CYC_OVRD_CD"),
    F.col("LivEncClmTrns.CLM_COB_CD").alias("CLM_COB_CD"),
    F.col("LivEncClmTrns.FINL_DISP_CD").alias("FINL_DISP_CD"),
    F.col("LivEncClmTrns.CLM_INPT_METH_CD").alias("CLM_INPT_METH_CD"),
    F.col("LivEncClmTrns.CLM_INPT_SRC_CD").alias("CLM_INPT_SRC_CD"),
    F.col("LivEncClmTrns.CLM_IPP_CD").alias("CLM_IPP_CD"),
    F.col("LivEncClmTrns.CLM_NTWK_STTUS_CD").alias("CLM_NTWK_STTUS_CD"),
    F.col("LivEncClmTrns.CLM_NONPAR_PROV_PFX_CD").alias("CLM_NONPAR_PROV_PFX_CD"),
    F.col("LivEncClmTrns.CLM_OTHER_BNF_CD").alias("CLM_OTHER_BNF_CD"),
    F.col("LivEncClmTrns.CLM_PAYE_CD").alias("CLM_PAYE_CD"),
    F.col("LivEncClmTrns.CLM_PRCS_CTL_AGNT_PFX_CD").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.col("LivEncClmTrns.CLM_SVC_DEFN_PFX_CD").alias("CLM_SVC_DEFN_PFX_CD"),
    # CLM_SVC_PROV_SPEC_CD => If prov_lkp.PROV_SPEC_CD is null => 'NA' else that value
    F.rpad(
        F.when(
            F.col("prov_lkp.PROV_SPEC_CD").isNull(), 
            F.lit("NA")
        ).otherwise(F.col("prov_lkp.PROV_SPEC_CD")),
        10," "
    ).alias("CLM_SVC_PROV_SPEC_CD"),
    # CLM_SVC_PROV_TYP_CD => If prov_lkp.PROV_TYP_CD is null => 'NA'
    F.rpad(
        F.when(
            F.col("prov_lkp.PROV_TYP_CD").isNull(),
            F.lit("NA")
        ).otherwise(F.col("prov_lkp.PROV_TYP_CD")),
        10," "
    ).alias("CLM_SVC_PROV_TYP_CD"),
    F.col("LivEncClmTrns.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.col("LivEncClmTrns.CLM_SUBMT_BCBS_PLN_CD").alias("CLM_SUBMT_BCBS_PLN_CD"),
    F.col("LivEncClmTrns.CLM_SUB_BCBS_PLN_CD").alias("CLM_SUB_BCBS_PLN_CD"),
    F.col("LivEncClmTrns.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("LivEncClmTrns.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("LivEncClmTrns.ATCHMT_IN").alias("ATCHMT_IN"),
    F.col("LivEncClmTrns.CLM_CLNCL_EDIT_CD").alias("CLM_CLNCL_EDIT_CD"),
    F.col("LivEncClmTrns.COBRA_CLM_IN").alias("COBRA_CLM_IN"),
    F.col("LivEncClmTrns.FIRST_PASS_IN").alias("FIRST_PASS_IN"),
    F.col("LivEncClmTrns.HOST_IN").alias("HOST_IN"),
    F.col("LivEncClmTrns.LTR_IN").alias("LTR_IN"),
    F.col("LivEncClmTrns.MCARE_ASG_IN").alias("MCARE_ASG_IN"),
    F.col("LivEncClmTrns.NOTE_IN").alias("NOTE_IN"),
    F.col("LivEncClmTrns.PCA_AUDIT_IN").alias("PCA_AUDIT_IN"),
    F.col("LivEncClmTrns.PCP_SUBMT_IN").alias("PCP_SUBMT_IN"),
    F.col("LivEncClmTrns.PROD_OOA_IN").alias("PROD_OOA_IN"),
    F.col("LivEncClmTrns.ACDNT_DT").alias("ACDNT_DT"),
    F.col("LivEncClmTrns.INPT_DT").alias("INPT_DT"),
    F.col("LivEncClmTrns.MBR_PLN_ELIG_DT").alias("MBR_PLN_ELIG_DT"),
    F.col("LivEncClmTrns.NEXT_RVW_DT").alias("NEXT_RVW_DT"),
    F.col("LivEncClmTrns.PD_DT").alias("PD_DT"),
    F.col("LivEncClmTrns.PAYMT_DRAG_CYC_DT").alias("PAYMT_DRAG_CYC_DT"),
    F.col("LivEncClmTrns.PRCS_DT").alias("PRCS_DT"),
    F.col("LivEncClmTrns.RCVD_DT").alias("RCVD_DT"),
    F.col("LivEncClmTrns.SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("LivEncClmTrns.SVC_END_DT").alias("SVC_END_DT"),
    F.col("LivEncClmTrns.SMLR_ILNS_DT").alias("SMLR_ILNS_DT"),
    F.col("LivEncClmTrns.STTUS_DT").alias("STTUS_DT"),
    F.col("LivEncClmTrns.WORK_UNABLE_BEG_DT").alias("WORK_UNABLE_BEG_DT"),
    F.col("LivEncClmTrns.WORK_UNABLE_END_DT").alias("WORK_UNABLE_END_DT"),
    F.col("LivEncClmTrns.ACDNT_AMT").alias("ACDNT_AMT"),
    F.col("LivEncClmTrns.ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    F.col("LivEncClmTrns.ALLOW_AMT").alias("ALLOW_AMT"),
    F.col("LivEncClmTrns.DSALW_AMT").alias("DSALW_AMT"),
    F.col("LivEncClmTrns.COINS_AMT").alias("COINS_AMT"),
    F.col("LivEncClmTrns.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("LivEncClmTrns.COPAY_AMT").alias("COPAY_AMT"),
    F.col("LivEncClmTrns.CHRG_AMT").alias("CHRG_AMT"),
    F.col("LivEncClmTrns.DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("LivEncClmTrns.PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("LivEncClmTrns.CLM_CT").alias("CLM_CT"),
    F.col("LivEncClmTrns.MBR_AGE").alias("MBR_AGE"),
    F.col("LivEncClmTrns.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("LivEncClmTrns.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.col("LivEncClmTrns.DOC_TX_ID").alias("DOC_TX_ID"),
    F.col("LivEncClmTrns.MCAID_RESUB_NO").alias("MCAID_RESUB_NO"),
    F.col("LivEncClmTrns.MCARE_ID").alias("MCARE_ID"),
    F.col("LivEncClmTrns.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("LivEncClmTrns.PATN_ACCT_NO").alias("PATN_ACCT_NO"),
    F.col("LivEncClmTrns.PAYMT_REF_ID").alias("PAYMT_REF_ID"),
    F.col("LivEncClmTrns.PROV_AGMNT_ID").alias("PROV_AGMNT_ID"),
    F.col("LivEncClmTrns.RFRNG_PROV_TX").alias("RFRNG_PROV_TX"),
    F.col("LivEncClmTrns.SUB_ID").alias("SUB_ID"),
    F.lit(None).alias("PRPR_ENTITY"),  # @NULL => we leave it as None
    F.col("LivEncClmTrns.PCA_TYP_CD").alias("PCA_TYP_CD"),
    F.col("LivEncClmTrns.REL_PCA_CLM_ID").alias("REL_PCA_CLM_ID"),
    F.lit("NA").alias("CLCL_MICRO_ID"),
    F.rpad(F.lit("N"),1," ").alias("CLM_UPDT_SW"),
    F.col("LivEncClmTrns.REMIT_SUPRSION_AMT").alias("REMIT_SUPRSION_AMT"),
    F.col("LivEncClmTrns.MCAID_STTUS_ID").alias("MCAID_STTUS_ID"),
    F.col("LivEncClmTrns.PATN_PD_AMT").alias("PATN_PD_AMT"),
    F.col("LivEncClmTrns.CLM_SUBMT_ICD_VRSN_CD").alias("CLM_SUBMT_ICD_VRSN_CD"),
    F.col("LivEncClmTrns.NTWK").alias("NTWK"),
    F.lit("NA").alias("CLM_TXNMY_CD"),
    F.col("LivEncClmTrns.BILL_PAYMT_EXCL_IN").alias("BILL_PAYMT_EXCL_IN")
)

# Stage: Snapshot (CTransformerStage) - Input: df_Transform
df_Snapshot = df_Transform.select(
    F.col("df_Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("df_Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("df_Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("df_Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("df_Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("df_Transform.ERR_CT").alias("ERR_CT"),
    F.col("df_Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("df_Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("df_Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("df_Transform.CLM_SK").alias("CLM_SK"),
    F.col("df_Transform.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("df_Transform.CLM_ID").alias("CLM_ID"),
    F.col("df_Transform.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("df_Transform.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("df_Transform.ADJ_FROM_CLM").alias("ADJ_FROM_CLM"),
    F.col("df_Transform.ADJ_TO_CLM").alias("ADJ_TO_CLM"),
    F.col("df_Transform.ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
    F.col("df_Transform.CLM_EOB_EXCD").alias("CLM_EOB_EXCD"),
    F.col("df_Transform.CLS").alias("CLS"),
    F.col("df_Transform.CLS_PLN").alias("CLS_PLN"),
    F.col("df_Transform.EXPRNC_CAT").alias("EXPRNC_CAT"),
    F.col("df_Transform.FNCL_LOB_NO").alias("FNCL_LOB_NO"),
    F.col("df_Transform.GRP").alias("GRP"),
    F.col("df_Transform.MBR_CK").alias("MBR_CK"),
    F.col("df_Transform.NTWK").alias("NTWK"),
    F.col("df_Transform.PROD").alias("PROD"),
    F.col("df_Transform.SUBGRP").alias("SUBGRP"),
    F.col("df_Transform.SUB_CK").alias("SUB_CK"),
    F.col("df_Transform.CLM_ACDNT_CD").alias("CLM_ACDNT_CD"),
    F.col("df_Transform.CLM_ACDNT_ST_CD").alias("CLM_ACDNT_ST_CD"),
    F.col("df_Transform.CLM_ACTIVATING_BCBS_PLN_CD").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
    F.col("df_Transform.CLM_AGMNT_SRC_CD").alias("CLM_AGMNT_SRC_CD"),
    F.col("df_Transform.CLM_BTCH_ACTN_CD").alias("CLM_BTCH_ACTN_CD"),
    F.col("df_Transform.CLM_CAP_CD").alias("CLM_CAP_CD"),
    F.col("df_Transform.CLM_CAT_CD").alias("CLM_CAT_CD"),
    F.col("df_Transform.CLM_CHK_CYC_OVRD_CD").alias("CLM_CHK_CYC_OVRD_CD"),
    F.col("df_Transform.CLM_COB_CD").alias("CLM_COB_CD"),
    F.col("df_Transform.FINL_DISP_CD").alias("FINL_DISP_CD"),
    F.col("df_Transform.CLM_INPT_METH_CD").alias("CLM_INPT_METH_CD"),
    F.col("df_Transform.CLM_INPT_SRC_CD").alias("CLM_INPT_SRC_CD"),
    F.col("df_Transform.CLM_IPP_CD").alias("CLM_IPP_CD"),
    F.col("df_Transform.CLM_NTWK_STTUS_CD").alias("CLM_NTWK_STTUS_CD"),
    F.col("df_Transform.CLM_NONPAR_PROV_PFX_CD").alias("CLM_NONPAR_PROV_PFX_CD"),
    F.col("df_Transform.CLM_OTHER_BNF_CD").alias("CLM_OTHER_BNF_CD"),
    F.col("df_Transform.CLM_PAYE_CD").alias("CLM_PAYE_CD"),
    F.col("df_Transform.CLM_PRCS_CTL_AGNT_PFX_CD").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.col("df_Transform.CLM_SVC_DEFN_PFX_CD").alias("CLM_SVC_DEFN_PFX_CD"),
    F.col("df_Transform.CLM_SVC_PROV_SPEC_CD").alias("CLM_SVC_PROV_SPEC_CD"),
    F.col("df_Transform.CLM_SVC_PROV_TYP_CD").alias("CLM_SVC_PROV_TYP_CD"),
    F.col("df_Transform.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.col("df_Transform.CLM_SUBMT_BCBS_PLN_CD").alias("CLM_SUBMT_BCBS_PLN_CD"),
    F.col("df_Transform.CLM_SUB_BCBS_PLN_CD").alias("CLM_SUB_BCBS_PLN_CD"),
    F.col("df_Transform.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("df_Transform.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("df_Transform.ATCHMT_IN").alias("ATCHMT_IN"),
    F.col("df_Transform.CLM_CLNCL_EDIT_CD").alias("CLM_CLNCL_EDIT_CD"),
    F.col("df_Transform.COBRA_CLM_IN").alias("COBRA_CLM_IN"),
    F.col("df_Transform.FIRST_PASS_IN").alias("FIRST_PASS_IN"),
    F.col("df_Transform.HOST_IN").alias("HOST_IN"),
    F.col("df_Transform.LTR_IN").alias("LTR_IN"),
    F.col("df_Transform.MCARE_ASG_IN").alias("MCARE_ASG_IN"),
    F.col("df_Transform.NOTE_IN").alias("NOTE_IN"),
    F.col("df_Transform.PCA_AUDIT_IN").alias("PCA_AUDIT_IN"),
    F.col("df_Transform.PCP_SUBMT_IN").alias("PCP_SUBMT_IN"),
    F.col("df_Transform.PROD_OOA_IN").alias("PROD_OOA_IN"),
    F.col("df_Transform.ACDNT_DT").alias("ACDNT_DT"),
    F.col("df_Transform.INPT_DT").alias("INPT_DT"),
    F.col("df_Transform.MBR_PLN_ELIG_DT").alias("MBR_PLN_ELIG_DT"),
    F.col("df_Transform.NEXT_RVW_DT").alias("NEXT_RVW_DT"),
    F.col("df_Transform.PD_DT").alias("PD_DT"),
    F.col("df_Transform.PAYMT_DRAG_CYC_DT").alias("PAYMT_DRAG_CYC_DT"),
    F.col("df_Transform.PRCS_DT").alias("PRCS_DT"),
    F.col("df_Transform.RCVD_DT").alias("RCVD_DT"),
    F.col("df_Transform.SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("df_Transform.SVC_END_DT").alias("SVC_END_DT"),
    F.col("df_Transform.SMLR_ILNS_DT").alias("SMLR_ILNS_DT"),
    F.col("df_Transform.STTUS_DT").alias("STTUS_DT"),
    F.col("df_Transform.WORK_UNABLE_BEG_DT").alias("WORK_UNABLE_BEG_DT"),
    F.col("df_Transform.WORK_UNABLE_END_DT").alias("WORK_UNABLE_END_DT"),
    F.col("df_Transform.ACDNT_AMT").alias("ACDNT_AMT"),
    F.col("df_Transform.ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    F.col("df_Transform.ALLOW_AMT").alias("ALLOW_AMT"),
    F.col("df_Transform.DSALW_AMT").alias("DSALW_AMT"),
    F.col("df_Transform.COINS_AMT").alias("COINS_AMT"),
    F.col("df_Transform.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("df_Transform.COPAY_AMT").alias("COPAY_AMT"),
    F.col("df_Transform.CHRG_AMT").alias("CHRG_AMT"),
    F.col("df_Transform.DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("df_Transform.PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("df_Transform.CLM_CT").alias("CLM_CT"),
    F.col("df_Transform.MBR_AGE").alias("MBR_AGE"),
    F.col("df_Transform.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("df_Transform.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.col("df_Transform.DOC_TX_ID").alias("DOC_TX_ID"),
    F.col("df_Transform.MCAID_RESUB_NO").alias("MCAID_RESUB_NO"),
    F.col("df_Transform.MCARE_ID").alias("MCARE_ID"),
    F.col("df_Transform.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("df_Transform.PATN_ACCT_NO").alias("PATN_ACCT_NO"),
    F.col("df_Transform.PAYMT_REF_ID").alias("PAYMT_REF_ID"),
    F.col("df_Transform.PROV_AGMNT_ID").alias("PROV_AGMNT_ID"),
    F.col("df_Transform.RFRNG_PROV_TX").alias("RFRNG_PROV_TX"),
    F.col("df_Transform.SUB_ID").alias("SUB_ID"),
    F.col("df_Transform.PRPR_ENTITY").alias("PRPR_ENTITY"),
    F.col("df_Transform.PCA_TYP_CD").alias("PCA_TYP_CD"),
    F.col("df_Transform.REL_PCA_CLM_ID").alias("REL_PCA_CLM_ID"),
    F.col("df_Transform.CLCL_MICRO_ID").alias("CLCL_MICRO_ID"),
    F.col("df_Transform.CLM_UPDT_SW").alias("CLM_UPDT_SW"),
    F.col("df_Transform.REMIT_SUPRSION_AMT").alias("REMIT_SUPRSION_AMT"),
    F.col("df_Transform.MCAID_STTUS_ID").alias("MCAID_STTUS_ID"),
    F.col("df_Transform.PATN_PD_AMT").alias("PATN_PD_AMT"),
    F.col("df_Transform.CLM_SUBMT_ICD_VRSN_CD").alias("CLM_SUBMT_ICD_VRSN_CD"),
    F.col("df_Transform.CLM_TXNMY_CD").alias("CLM_TXNMY_CD"),
    F.col("df_Transform.BILL_PAYMT_EXCL_IN").alias("BILL_PAYMT_EXCL_IN")
).alias("Transform")

# Output pin "Pkey"
df_Pkey = df_Snapshot.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Transform.CLM_SK").alias("CLM_SK"),
    F.col("Transform.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("Transform.CLM_ID").alias("CLM_ID"),
    F.col("Transform.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.ADJ_FROM_CLM").alias("ADJ_FROM_CLM"),
    F.col("Transform.ADJ_TO_CLM").alias("ADJ_TO_CLM"),
    F.col("Transform.ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
    F.col("Transform.CLM_EOB_EXCD").alias("CLM_EOB_EXCD"),
    F.col("Transform.CLS").alias("CLS"),
    F.col("Transform.CLS_PLN").alias("CLS_PLN"),
    F.col("Transform.EXPRNC_CAT").alias("EXPRNC_CAT"),
    F.col("Transform.FNCL_LOB_NO").alias("FNCL_LOB_NO"),
    F.col("Transform.GRP").alias("GRP"),
    F.col("Transform.MBR_CK").alias("MBR_CK"),
    F.col("Transform.NTWK").alias("NTWK"),
    F.col("Transform.PROD").alias("PROD"),
    F.col("Transform.SUBGRP").alias("SUBGRP"),
    F.col("Transform.SUB_CK").alias("SUB_CK"),
    F.col("Transform.CLM_ACDNT_CD").alias("CLM_ACDNT_CD"),
    F.col("Transform.CLM_ACDNT_ST_CD").alias("CLM_ACDNT_ST_CD"),
    F.col("Transform.CLM_ACTIVATING_BCBS_PLN_CD").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
    F.col("Transform.CLM_AGMNT_SRC_CD").alias("CLM_AGMNT_SRC_CD"),
    F.col("Transform.CLM_BTCH_ACTN_CD").alias("CLM_BTCH_ACTN_CD"),
    F.col("Transform.CLM_CAP_CD").alias("CLM_CAP_CD"),
    F.col("Transform.CLM_CAT_CD").alias("CLM_CAT_CD"),
    F.col("Transform.CLM_CHK_CYC_OVRD_CD").alias("CLM_CHK_CYC_OVRD_CD"),
    F.col("Transform.CLM_COB_CD").alias("CLM_COB_CD"),
    F.col("Transform.FINL_DISP_CD").alias("FINL_DISP_CD"),
    F.col("Transform.CLM_INPT_METH_CD").alias("CLM_INPT_METH_CD"),
    F.col("Transform.CLM_INPT_SRC_CD").alias("CLM_INPT_SRC_CD"),
    F.col("Transform.CLM_IPP_CD").alias("CLM_IPP_CD"),
    F.col("Transform.CLM_NTWK_STTUS_CD").alias("CLM_NTWK_STTUS_CD"),
    F.col("Transform.CLM_NONPAR_PROV_PFX_CD").alias("CLM_NONPAR_PROV_PFX_CD"),
    F.col("Transform.CLM_OTHER_BNF_CD").alias("CLM_OTHER_BNF_CD"),
    F.col("Transform.CLM_PAYE_CD").alias("CLM_PAYE_CD"),
    F.col("Transform.CLM_PRCS_CTL_AGNT_PFX_CD").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.col("Transform.CLM_SVC_DEFN_PFX_CD").alias("CLM_SVC_DEFN_PFX_CD"),
    F.col("Transform.CLM_SVC_PROV_SPEC_CD").alias("CLM_SVC_PROV_SPEC_CD"),
    F.col("Transform.CLM_SVC_PROV_TYP_CD").alias("CLM_SVC_PROV_TYP_CD"),
    F.col("Transform.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.col("Transform.CLM_SUBMT_BCBS_PLN_CD").alias("CLM_SUBMT_BCBS_PLN_CD"),
    F.col("Transform.CLM_SUB_BCBS_PLN_CD").alias("CLM_SUB_BCBS_PLN_CD"),
    F.col("Transform.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("Transform.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("Transform.ATCHMT_IN").alias("ATCHMT_IN"),
    F.col("Transform.CLM_CLNCL_EDIT_CD").alias("CLM_CLNCL_EDIT_CD"),
    F.col("Transform.COBRA_CLM_IN").alias("COBRA_CLM_IN"),
    F.col("Transform.FIRST_PASS_IN").alias("FIRST_PASS_IN"),
    F.col("Transform.HOST_IN").alias("HOST_IN"),
    F.col("Transform.LTR_IN").alias("LTR_IN"),
    F.col("Transform.MCARE_ASG_IN").alias("MCARE_ASG_IN"),
    F.col("Transform.NOTE_IN").alias("NOTE_IN"),
    F.col("Transform.PCA_AUDIT_IN").alias("PCA_AUDIT_IN"),
    F.col("Transform.PCP_SUBMT_IN").alias("PCP_SUBMT_IN"),
    F.col("Transform.PROD_OOA_IN").alias("PROD_OOA_IN"),
    F.col("Transform.ACDNT_DT").alias("ACDNT_DT"),
    F.col("Transform.INPT_DT").alias("INPT_DT"),
    F.col("Transform.MBR_PLN_ELIG_DT").alias("MBR_PLN_ELIG_DT"),
    F.col("Transform.NEXT_RVW_DT").alias("NEXT_RVW_DT"),
    F.col("Transform.PD_DT").alias("PD_DT"),
    F.col("Transform.PAYMT_DRAG_CYC_DT").alias("PAYMT_DRAG_CYC_DT"),
    F.col("Transform.PRCS_DT").alias("PRCS_DT"),
    F.col("Transform.RCVD_DT").alias("RCVD_DT"),
    F.col("Transform.SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("Transform.SVC_END_DT").alias("SVC_END_DT"),
    F.col("Transform.SMLR_ILNS_DT").alias("SMLR_ILNS_DT"),
    F.col("Transform.STTUS_DT").alias("STTUS_DT"),
    F.col("Transform.WORK_UNABLE_BEG_DT").alias("WORK_UNABLE_BEG_DT"),
    F.col("Transform.WORK_UNABLE_END_DT").alias("WORK_UNABLE_END_DT"),
    F.col("Transform.ACDNT_AMT").alias("ACDNT_AMT"),
    F.col("Transform.ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    F.col("Transform.ALLOW_AMT").alias("ALLOW_AMT"),
    F.col("Transform.DSALW_AMT").alias("DSALW_AMT"),
    F.col("Transform.COINS_AMT").alias("COINS_AMT"),
    F.col("Transform.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("Transform.COPAY_AMT").alias("COPAY_AMT"),
    F.col("Transform.CHRG_AMT").alias("CHRG_AMT"),
    F.col("Transform.DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("Transform.PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("Transform.CLM_CT").alias("CLM_CT"),
    F.col("Transform.MBR_AGE").alias("MBR_AGE"),
    F.col("Transform.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("Transform.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.col("Transform.DOC_TX_ID").alias("DOC_TX_ID"),
    F.col("Transform.MCAID_RESUB_NO").alias("MCAID_RESUB_NO"),
    F.col("Transform.MCARE_ID").alias("MCARE_ID"),
    F.col("Transform.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("Transform.PATN_ACCT_NO").alias("PATN_ACCT_NO"),
    F.col("Transform.PAYMT_REF_ID").alias("PAYMT_REF_ID"),
    F.col("Transform.PROV_AGMNT_ID").alias("PROV_AGMNT_ID"),
    F.col("Transform.RFRNG_PROV_TX").alias("RFRNG_PROV_TX"),
    F.col("Transform.SUB_ID").alias("SUB_ID"),
    F.col("Transform.PRPR_ENTITY").alias("PRPR_ENTITY"),
    F.col("Transform.PCA_TYP_CD").alias("PCA_TYP_CD"),
    F.col("Transform.REL_PCA_CLM_ID").alias("REL_PCA_CLM_ID"),
    F.col("Transform.CLCL_MICRO_ID").alias("CLCL_MICRO_ID"),
    F.col("Transform.CLM_UPDT_SW").alias("CLM_UPDT_SW"),
    F.col("Transform.REMIT_SUPRSION_AMT").alias("REMIT_SUPRSION_AMT"),
    F.col("Transform.MCAID_STTUS_ID").alias("MCAID_STTUS_ID"),
    F.col("Transform.PATN_PD_AMT").alias("PATN_PD_AMT"),
    F.col("Transform.CLM_SUBMT_ICD_VRSN_CD").alias("CLM_SUBMT_ICD_VRSN_CD"),
    F.col("Transform.CLM_TXNMY_CD").alias("CLM_TXNMY_CD"),
    F.col("Transform.BILL_PAYMT_EXCL_IN").alias("BILL_PAYMT_EXCL_IN")
)

# The other output from "Snapshot" => "Snapshot" link
df_Snapshot2 = df_Snapshot.select(
    F.rpad(F.col("Transform.CLM_ID"),12," ").alias("CLM_ID"),
    F.col("Transform.MBR_CK").alias("MBR_CK"),
    F.rpad(F.col("Transform.GRP"),8," ").alias("GRP"),
    F.col("Transform.SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("Transform.CHRG_AMT").alias("CHRG_AMT"),
    F.col("Transform.PAYBL_AMT").alias("PAYBL_AMT"),
    F.rpad(F.col("Transform.EXPRNC_CAT"),4," ").alias("EXPRNC_CAT"),
    F.rpad(F.col("Transform.FNCL_LOB_NO"),4," ").alias("FNCL_LOB_NO"),
    F.col("Transform.CLM_CT").alias("CLM_CT"),
    F.rpad(F.col("Transform.PCA_TYP_CD"),18," ").alias("PCA_TYP_CD"),
    F.rpad(F.col("Transform.CLM_STTUS_CD"),2," ").alias("CLM_STTUS_CD"),
    F.rpad(F.col("Transform.CLM_CAT_CD"),10," ").alias("CLM_CAT_CD")
).alias("Snapshot")

# Stage: Transformer (CTransformerStage)
# InputPins => df_Snapshot2
# StageVariables => ExpCatCdSk, GrpSk, MbrSk, FnclLobSk, PcaTypCdSk, ClmSttusCdSk, ClmCatCdSk => calls to user-defined fkeys
df_Transformer = (
    df_Snapshot2
    .withColumn("ExpCatCdSk", F.expr("GetFkeyExprncCat('FACETS', 0, EXPRNC_CAT, 'N')"))
    .withColumn("GrpSk", F.expr("GetFkeyGrp('FACETS', 0, GRP, 'N')"))
    .withColumn("MbrSk", F.expr("GetFkeyMbr('FACETS', 0, MBR_CK, 'N')"))
    .withColumn("FnclLobSk", F.expr('GetFkeyFnclLob("PSI", 0, FNCL_LOB_NO, "N")'))
    .withColumn("PcaTypCdSk", F.expr('GetFkeyCodes("FACETS", 0, "PERSONAL CARE ACCOUNT PROCESSING", PCA_TYP_CD, "N")'))
    .withColumn("ClmSttusCdSk", F.expr('GetFkeyCodes(SrcSysCd, 0, "CLAIM STATUS", CLM_STTUS_CD, "N")'))
    .withColumn("ClmCatCdSk", F.expr('GetFkeyCodes(SrcSysCd, 0, "CLAIM CATEGORY", CLM_CAT_CD, "X")'))
)

# Output pin "RowCount"
df_B_CLM = df_Transformer.select(
    F.lit("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("ClmSttusCdSk").alias("CLM_STTUS_CD_SK"),
    F.col("ClmCatCdSk").alias("CLM_CAT_CD_SK"),
    F.col("ExpCatCdSk").alias("EXPRNC_CAT_SK"),
    F.col("FnclLobSk").alias("FNCL_LOB_SK"),
    F.col("GrpSk").alias("GRP_SK"),
    F.col("MbrSk").alias("MBR_SK"),
    F.rpad(F.col("SVC_STRT_DT"),10," ").alias("SVC_STRT_DT_SK"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("CLM_CT").alias("CLM_CT"),
    F.col("PcaTypCdSk").alias("PCA_TYP_CD_SK")
)

# Stage: B_CLM (CSeqFileStage) writing:
# path => "load/B_CLM.#SrcSysCd#.dat.#RunID#"
outpath_B_CLM = f"{adls_path}/load/B_CLM.{SrcSysCd}.dat.{RunID}"
write_files(
    df_B_CLM,
    outpath_B_CLM,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage: ClmPK (CContainerStage) 
# Already included the Shared Container reference at top and pass df_Pkey as an input.
# The container outputs "Key" => which is "df_ClmPK_ Key"? We simulate the usage:
params_ClmPK = {
    "CurrRunCycle": RunCycle
}
df_ClmPK_out = ClmPK(df_Pkey, params_ClmPK)  # Container call

# The container has one output => "Key" => mapped to next stage "LivongoEncClmExtr" 
df_Key = df_ClmPK_out  # This is the "Key" link

# Stage: LivongoEncClmExtr (CSeqFileStage) writing:
# path => "key/LivongoEncClmExtr_#SrcSysCd#.dat.#RunID#"
outpath_LivongoEncClmExtr = f"{adls_path}/key/LivongoEncClmExtr_{SrcSysCd}.dat.{RunID}"
write_files(
    df_Key,
    outpath_LivongoEncClmExtr,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)