# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2010, 2015 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     BCBSADrugExtrSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Reads the BCBSADrugClm_Landing.dat file and puts the data into the claim common record format and takes it through 
# MAGIC                     primary key process using Shared Container ClmPkey
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    What needs to happen before a special run can occur?
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                         Project/                                                                                                                              Environment                           Code                   Date
# MAGIC Developer                 Date                Altiris #        Change Description                                                                                                                                     Reviewer            Reviewed
# MAGIC --------------------------------  ---------------------   ----------------   -----------------------------------------------------------------------------------------------------------------     -------------------------                    -------------------------  -------------------
# MAGIC Karthik Chintalapani   2016-02-17      5212           Initial Programming                                                                                                                                      Kalyan Neelam    2016-05-11 
# MAGIC Manasa Andru           2018-03-05      TFS21142   Changes SrcSysCdto 'BCA' in Transformer Stage in the ClmSttusCdSk                                                      Kalyan Neelam    2018-03-12
# MAGIC                                                                                      stage variable as per mapping rule.
# MAGIC 
# MAGIC Mohan Karnati         2019-06-06        ADO-73034             Adding CLM_TXNMY_CD filed in alpha_pfx stage and                     IntegrateDev1\(9)                                 Kalyan Neelam      2019-07-01
# MAGIC                                                                                                    passing it till BCBSARxClmExtr stage
# MAGIC Giri  Mallavaram    2020-04-06         PBM Changes       Added BILL_PAYMT_EXCL_IN to be in sync with changes to CLM_PKey Container        IntegrateDev2         Kalyan Neelam      2020-04-07

# MAGIC Read the BCBSA Rx Claim file created from BCBSADrugClmLand
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
# MAGIC BCBSSCClmExtr
# MAGIC BCBSSCMedClmExtr
# MAGIC BCAClmExtr
# MAGIC BCBSARxClmExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Writing Sequential File to /key
# MAGIC Assign primary surrogate key
# MAGIC Lookup subscriber, product and member information
# MAGIC Apply business logic
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


CurrentDate = get_widget_value("CurrentDate", "")
SrcSysCd = get_widget_value("SrcSysCd", "")
SrcSysCdSk = get_widget_value("SrcSysCdSk", "")
RunCycle = get_widget_value("RunCycle", "")
RunID = get_widget_value("RunID", "")
IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

# MAGIC %run ../../../../../../shared_containers/PrimaryKey/ClmPK
# COMMAND ----------

schema_BCBSARxClmLanding = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("NDW_HOME_PLN_ID", StringType(), True),
    StructField("CLM_LN_NO", StringType(), True),
    StructField("TRACEABILITY_FLD", StringType(), True),
    StructField("ADJ_SEQ_NO", StringType(), True),
    StructField("HOST_PLN_ID", StringType(), True),
    StructField("HOME_PLAN_PROD_ID_CD", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("MBR_ZIP_CD_ON_CLM", StringType(), True),
    StructField("MBR_CTRY_ON_CLM", StringType(), True),
    StructField("NPI_REND_PROV_ID", StringType(), True),
    StructField("PRSCRB_PROV_ID", StringType(), True),
    StructField("NPI_PRSCRB_PROV_ID", StringType(), True),
    StructField("BNF_PAYMT_STTUS_CD", StringType(), True),
    StructField("PDX_CARVE_OUT_SUBMSN_IN", StringType(), True),
    StructField("CAT_OF_SVC", StringType(), True),
    StructField("CLM_PAYMT_STTUS", StringType(), True),
    StructField("DAW_CD", StringType(), True),
    StructField("DAYS_SUPL", StringType(), True),
    StructField("FRMLRY_IN", StringType(), True),
    StructField("PLN_SPEC_DRUG_IN", StringType(), True),
    StructField("PROD_SVC_ID", StringType(), True),
    StructField("QTY_DISPNS", StringType(), True),
    StructField("CLM_PD_DT", StringType(), True),
    StructField("DT_OF_SVC", StringType(), True),
    StructField("AVG_WHLSL_PRICE_SUBMT_AMT", StringType(), True),
    StructField("CONSIS_MBR_ID", StringType(), True),
    StructField("MMI_ID", StringType(), True),
    StructField("SUBGRP_SK", IntegerType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("SUB_SK", IntegerType(), True),
    StructField("SUB_ID", StringType(), True),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("MBR_GNDR_CD_SK", IntegerType(), True),
    StructField("MBR_GNDR_CD", StringType(), True),
    StructField("BRTH_DT_SK", StringType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("GRP_ID", StringType(), True)
])

df_BCBSA = (
    spark.read.format("csv")
    .option("header", False)
    .option("quote", '"')
    .schema(schema_BCBSARxClmLanding)
    .load(f"{adls_path}/verified/BCBSADrugClm_Land.dat.{RunID}")
)

df_MedtrakClmTrns = (
    df_BCBSA.select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        current_date().alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CLM_ID")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("NA").alias("ADJ_FROM_CLM"),
        F.lit("NA").alias("ADJ_TO_CLM"),
        F.lit("NA").alias("ALPHA_PFX_CD"),
        F.lit("NA").alias("CLM_EOB_EXCD"),
        F.lit("NA").alias("CLS"),
        F.lit("NA").alias("CLS_PLN"),
        F.lit("NA").alias("EXPRNC_CAT"),
        F.lit("NA").alias("FNCL_LOB_NO"),
        F.col("GRP_ID").alias("GRP"),
        F.col("MBR_UNIQ_KEY").alias("MBR_CK"),
        F.lit("NA").alias("NTWK"),
        F.lit("NA").alias("PROD"),
        F.lit("NA").alias("SUBGRP"),
        F.col("SUB_UNIQ_KEY").alias("SUB_CK"),
        F.lit("NA").alias("CLM_ACDNT_CD"),
        F.lit("NA").alias("CLM_ACDNT_ST_CD"),
        F.when(
            (F.col("HOST_PLN_ID").isNull()) | (F.length(trim(F.col("HOST_PLN_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("HOST_PLN_ID")).alias("CLM_ACTIVATING_BCBS_PLN_CD"),
        F.lit("NA").alias("CLM_AGMNT_SRC_CD"),
        F.lit("NA").alias("CLM_BTCH_ACTN_CD"),
        F.lit("N").alias("CLM_CAP_CD"),
        F.lit("STD").alias("CLM_CAT_CD"),
        F.lit("NA").alias("CLM_CHK_CYC_OVRD_CD"),
        F.lit("NA").alias("CLM_COB_CD"),
        F.lit("ACPTD").alias("FINL_DISP_CD"),
        F.lit("NA").alias("CLM_INPT_METH_CD"),
        F.lit("NA").alias("CLM_INPT_SRC_CD"),
        F.lit("NA").alias("CLM_IPP_CD"),
        F.when(
            F.col("BNF_PAYMT_STTUS_CD") == "Y", F.lit("I")
        ).when(
            F.col("BNF_PAYMT_STTUS_CD") == "N", F.lit("O")
        ).otherwise(F.lit("NA")).alias("CLM_NTWK_STTUS_CD"),
        F.lit("NA").alias("CLM_NONPAR_PROV_PFX_CD"),
        F.lit("NA").alias("CLM_OTHER_BNF_CD"),
        F.lit("NA").alias("CLM_PAYE_CD"),
        F.lit("NA").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
        F.lit("NA").alias("CLM_SVC_DEFN_PFX_CD"),
        F.lit("NA").alias("CLM_SVC_PROV_SPEC_CD"),
        F.lit("0012").alias("CLM_SVC_PROV_TYP_CD"),
        F.lit("A02").alias("CLM_STTUS_CD"),
        F.when(
            (F.col("HOST_PLN_ID").isNull()) | (F.length(trim(F.col("HOST_PLN_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("HOST_PLN_ID")).alias("CLM_SUBMT_BCBS_PLN_CD"),
        F.when(
            (F.col("NDW_HOME_PLN_ID").isNull()) | (F.length(trim(F.col("NDW_HOME_PLN_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("NDW_HOME_PLN_ID")).alias("CLM_SUB_BCBS_PLN_CD"),
        F.lit("RX").alias("CLM_SUBTYP_CD"),
        F.lit("30").alias("CLM_TYP_CD"),
        F.lit("N").alias("ATCHMT_IN"),
        F.lit("N").alias("CLM_CLNCL_EDIT_CD"),
        F.lit("N").alias("COBRA_CLM_IN"),
        F.lit("N").alias("FIRST_PASS_IN"),
        F.lit("N").alias("HOST_IN"),
        F.lit("N").alias("LTR_IN"),
        F.lit("N").alias("MCARE_ASG_IN"),
        F.lit("N").alias("NOTE_IN"),
        F.lit("N").alias("PCA_AUDIT_IN"),
        F.lit("N").alias("PCP_SUBMT_IN"),
        F.lit("N").alias("PROD_OOA_IN"),
        F.lit("1753-01-01").alias("ACDNT_DT"),
        F.lit("1753-01-01").alias("INPT_DT"),
        F.lit("1753-01-01").alias("MBR_PLN_ELIG_DT"),
        F.lit("2199-12-31").alias("NEXT_RVW_DT"),
        F.col("CLM_PD_DT").alias("PD_DT"),
        F.lit("1753-01-01").alias("PAYMT_DRAG_CYC_DT"),
        current_date().alias("PRCS_DT"),
        F.lit("1753-01-01").alias("RCVD_DT"),
        F.col("DT_OF_SVC").alias("SVC_STRT_DT"),
        F.col("DT_OF_SVC").alias("SVC_END_DT"),
        F.lit("1753-01-01").alias("SMLR_ILNS_DT"),
        current_date().alias("STTUS_DT"),
        F.lit("1753-01-01").alias("WORK_UNABLE_BEG_DT"),
        F.lit("2199-12-31").alias("WORK_UNABLE_END_DT"),
        F.lit(0.00).alias("ACDNT_AMT"),
        F.when(
            (F.col("AVG_WHLSL_PRICE_SUBMT_AMT").isNull()) | (F.length(trim(F.col("AVG_WHLSL_PRICE_SUBMT_AMT"))) == 0),
            F.lit(0.00)
        ).otherwise(F.col("AVG_WHLSL_PRICE_SUBMT_AMT").cast("double")).alias("ACTL_PD_AMT"),
        F.lit(0.00).alias("ALLOW_AMT"),
        F.lit(0.00).alias("DSALW_AMT"),
        F.lit(0.00).alias("COINS_AMT"),
        F.lit(0.00).alias("CNSD_CHRG_AMT"),
        F.lit(0.00).alias("COPAY_AMT"),
        F.lit(0.00).alias("CHRG_AMT"),
        F.lit(0.00).alias("DEDCT_AMT"),
        F.when(
            (F.col("AVG_WHLSL_PRICE_SUBMT_AMT").isNull()) | (F.length(trim(F.col("AVG_WHLSL_PRICE_SUBMT_AMT"))) == 0),
            F.lit(0.00)
        ).otherwise(F.col("AVG_WHLSL_PRICE_SUBMT_AMT").cast("double")).alias("PAYBL_AMT"),
        F.lit(1).alias("CLM_CT"),
        AGE(F.col("BRTH_DT_SK"), F.col("DT_OF_SVC")).alias("MBR_AGE"),
        F.lit("NA").alias("ADJ_FROM_CLM_ID"),
        F.lit("NA").alias("ADJ_TO_CLM_ID"),
        F.lit("NA").alias("DOC_TX_ID"),
        F.lit("NA").alias("MCAID_RESUB_NO"),
        F.lit("NA").alias("MCARE_ID"),
        F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.lit("NA").alias("PATN_ACCT_NO"),
        F.lit("NA").alias("PAYMT_REF_ID"),
        F.lit("NA").alias("PROV_AGMNT_ID"),
        F.lit("NA").alias("RFRNG_PROV_TX"),
        F.col("SUB_ID").alias("SUB_ID"),
        F.lit(0.00).alias("REMIT_SUPRSION_AMT"),
        F.lit("NA").alias("MCAID_STTUS_ID"),
        F.lit(0.00).alias("PATN_PD_AMT"),
        F.lit("NA").alias("CLM_SUBMT_ICD_VRSN_CD")
    )
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_sub_alpha_pfx = """SELECT 
SUB_UNIQ_KEY,
ALPHA_PFX_CD 
FROM 
#$IDSOwner#.MBR MBR,
#$IDSOwner#.SUB SUB,
#$IDSOwner#.ALPHA_PFX PFX,
#$IDSOwner#.W_DRUG_ENR DRUG 

WHERE 
DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
AND MBR.SUB_SK = SUB.SUB_SK 
AND SUB.ALPHA_PFX_SK = PFX.ALPHA_PFX_SK
"""

df_sub_alpha_pfx = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_sub_alpha_pfx.replace("#$IDSOwner#", IDSOwner))
    .load()
)

extract_query_mbr_enroll = """SELECT 
DRUG.CLM_ID,
CLS.CLS_ID,
PLN.CLS_PLN_ID,
SUBGRP.SUBGRP_ID,
CAT.EXPRNC_CAT_CD,
LOB.FNCL_LOB_CD,
CMPNT.PROD_ID,
GRP.GRP_ID  

FROM #$IDSOwner#.W_DRUG_ENR DRUG,
#$IDSOwner#.MBR_ENR                  MBR, 
#$IDSOwner#.CD_MPPNG                MAP1,
#$IDSOwner#.CLS                     CLS, 
#$IDSOwner#.SUBGRP                  SUBGRP,
#$IDSOwner#.CLS_PLN                 PLN, 
#$IDSOwner#.CD_MPPNG               MAP2, 
#$IDSOwner#.PROD_CMPNT             CMPNT,
#$IDSOwner#.PROD_BILL_CMPNT        BILL_CMPNT,
#$IDSOwner#.EXPRNC_CAT             CAT,
#$IDSOwner#.FNCL_LOB               LOB,
#$IDSOwner#.GRP                     GRP 

WHERE 
DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
and DRUG.FILL_DT_SK between MBR.EFF_DT_SK and MBR.TERM_DT_SK
and MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
and MAP1.TRGT_CD = 'MED' 
and MBR.CLS_SK = CLS.CLS_SK
and MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
and MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
and MBR.GRP_SK = GRP.GRP_SK
AND MBR.PROD_SK = CMPNT.PROD_SK
AND  CMPNT.PROD_CMPNT_TYP_CD_SK = MAP2.CD_MPPNG_SK
AND  MAP2.TRGT_CD= 'PDBL'
AND  DRUG.FILL_DT_SK between  CMPNT.PROD_CMPNT_EFF_DT_SK AND CMPNT.PROD_CMPNT_TERM_DT_SK
AND  CMPNT.PROD_CMPNT_EFF_DT_SK= (SELECT MAX ( CMPNT2.PROD_CMPNT_EFF_DT_SK ) FROM  #$IDSOwner#.PROD_CMPNT   CMPNT2
                                  WHERE     CMPNT.PROD_SK = CMPNT2.PROD_SK
                                  AND     CMPNT.PROD_CMPNT_TYP_CD_SK = CMPNT2.PROD_CMPNT_TYP_CD_SK
                                  AND     DRUG.FILL_DT_SK  BETWEEN CMPNT2.PROD_CMPNT_EFF_DT_SK  AND  CMPNT2.PROD_CMPNT_TERM_DT_SK)
AND  CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT.PROD_CMPNT_PFX_ID
AND  DRUG.FILL_DT_SK between BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK  AND BILL_CMPNT.PROD_BILL_CMPNT_TERM_DT_SK
AND  BILL_CMPNT.PROD_BILL_CMPNT_ID IN ('MED', 'MED1')
AND  BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK= (SELECT MAX ( BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK ) FROM  #$IDSOwner#.PROD_BILL_CMPNT   BILL_CMPNT2
                                            WHERE   BILL_CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
                                            AND     CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
                                            AND     BILL_CMPNT.PROD_BILL_CMPNT_ID = BILL_CMPNT2.PROD_BILL_CMPNT_ID
                                            AND     BILL_CMPNT2.PROD_BILL_CMPNT_ID IN ('MED', 'MED1')
                                            AND     DRUG.FILL_DT_SK BETWEEN BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK  AND BILL_CMPNT2.PROD_BILL_CMPNT_TERM_DT_SK)
AND  BILL_CMPNT.EXPRNC_CAT_SK = CAT.EXPRNC_CAT_SK
AND  BILL_CMPNT.FNCL_LOB_SK = LOB.FNCL_LOB_SK
"""

df_mbr_enroll = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_mbr_enroll.replace("#$IDSOwner#", IDSOwner))
    .load()
)

df_sub_alpha_pfx_dedup = dedup_sort(
    df_sub_alpha_pfx,
    ["SUB_UNIQ_KEY"],
    [("SUB_UNIQ_KEY", "A")]
)
df_mbr_enroll_dedup = dedup_sort(
    df_mbr_enroll,
    ["CLM_ID"],
    [("CLM_ID", "A")]
)

df_alpha_pfx_joined = (
    df_MedtrakClmTrns.alias("MedtrakClmTrns")
    .join(
        df_sub_alpha_pfx_dedup.alias("sub_alpha_pfx_lkup"),
        F.col("MedtrakClmTrns.SUB_CK") == F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY"),
        "left"
    )
    .join(
        df_mbr_enroll_dedup.alias("mbr_enr_lkup"),
        F.col("MedtrakClmTrns.CLM_ID") == F.col("mbr_enr_lkup.CLM_ID"),
        "left"
    )
)

df_Transform = df_alpha_pfx_joined.select(
    F.col("MedtrakClmTrns.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("MedtrakClmTrns.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("MedtrakClmTrns.DISCARD_IN").alias("DISCARD_IN"),
    F.col("MedtrakClmTrns.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("MedtrakClmTrns.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("MedtrakClmTrns.ERR_CT").alias("ERR_CT"),
    F.col("MedtrakClmTrns.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("MedtrakClmTrns.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MedtrakClmTrns.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("MedtrakClmTrns.CLM_SK").alias("CLM_SK"),
    F.col("MedtrakClmTrns.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("MedtrakClmTrns.CLM_ID").alias("CLM_ID"),
    F.col("MedtrakClmTrns.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("MedtrakClmTrns.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MedtrakClmTrns.ADJ_FROM_CLM").alias("ADJ_FROM_CLM"),
    F.col("MedtrakClmTrns.ADJ_TO_CLM").alias("ADJ_TO_CLM"),
    F.when(
        F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("sub_alpha_pfx_lkup.ALPHA_PFX_CD")).alias("ALPHA_PFX_CD"),
    F.col("MedtrakClmTrns.CLM_EOB_EXCD").alias("CLM_EOB_EXCD"),
    F.when(
        F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK")
    ).otherwise(F.col("mbr_enr_lkup.CLS_ID")).alias("CLS"),
    F.when(
        F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK")
    ).otherwise(F.col("mbr_enr_lkup.CLS_PLN_ID")).alias("CLS_PLN"),
    F.when(
        F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK")
    ).otherwise(F.col("mbr_enr_lkup.EXPRNC_CAT_CD")).alias("EXPRNC_CAT"),
    F.when(
        F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK")
    ).otherwise(F.col("mbr_enr_lkup.FNCL_LOB_CD")).alias("FNCL_LOB_NO"),
    F.when(
        F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK")
    ).otherwise(trim(F.col("mbr_enr_lkup.GRP_ID"))).alias("GRP"),
    F.col("MedtrakClmTrns.MBR_CK").alias("MBR_CK"),
    F.col("MedtrakClmTrns.NTWK").alias("NTWK"),
    F.when(
        F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK")
    ).otherwise(trim(F.col("mbr_enr_lkup.PROD_ID"))).alias("PROD"),
    F.when(
        F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK")
    ).otherwise(F.col("mbr_enr_lkup.SUBGRP_ID")).alias("SUBGRP"),
    F.col("MedtrakClmTrns.SUB_CK").alias("SUB_CK"),
    F.col("MedtrakClmTrns.CLM_ACDNT_CD").alias("CLM_ACDNT_CD"),
    F.col("MedtrakClmTrns.CLM_ACDNT_ST_CD").alias("CLM_ACDNT_ST_CD"),
    F.col("MedtrakClmTrns.CLM_ACTIVATING_BCBS_PLN_CD").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
    F.col("MedtrakClmTrns.CLM_AGMNT_SRC_CD").alias("CLM_AGMNT_SRC_CD"),
    F.col("MedtrakClmTrns.CLM_BTCH_ACTN_CD").alias("CLM_BTCH_ACTN_CD"),
    F.col("MedtrakClmTrns.CLM_CAP_CD").alias("CLM_CAP_CD"),
    F.col("MedtrakClmTrns.CLM_CAT_CD").alias("CLM_CAT_CD"),
    F.col("MedtrakClmTrns.CLM_CHK_CYC_OVRD_CD").alias("CLM_CHK_CYC_OVRD_CD"),
    F.col("MedtrakClmTrns.CLM_COB_CD").alias("CLM_COB_CD"),
    F.col("MedtrakClmTrns.FINL_DISP_CD").alias("FINL_DISP_CD"),
    F.col("MedtrakClmTrns.CLM_INPT_METH_CD").alias("CLM_INPT_METH_CD"),
    F.col("MedtrakClmTrns.CLM_INPT_SRC_CD").alias("CLM_INPT_SRC_CD"),
    F.col("MedtrakClmTrns.CLM_IPP_CD").alias("CLM_IPP_CD"),
    F.col("MedtrakClmTrns.CLM_NTWK_STTUS_CD").alias("CLM_NTWK_STTUS_CD"),
    F.col("MedtrakClmTrns.CLM_NONPAR_PROV_PFX_CD").alias("CLM_NONPAR_PROV_PFX_CD"),
    F.col("MedtrakClmTrns.CLM_OTHER_BNF_CD").alias("CLM_OTHER_BNF_CD"),
    F.col("MedtrakClmTrns.CLM_PAYE_CD").alias("CLM_PAYE_CD"),
    F.col("MedtrakClmTrns.CLM_PRCS_CTL_AGNT_PFX_CD").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.col("MedtrakClmTrns.CLM_SVC_DEFN_PFX_CD").alias("CLM_SVC_DEFN_PFX_CD"),
    F.col("MedtrakClmTrns.CLM_SVC_PROV_SPEC_CD").alias("CLM_SVC_PROV_SPEC_CD"),
    F.col("MedtrakClmTrns.CLM_SVC_PROV_TYP_CD").alias("CLM_SVC_PROV_TYP_CD"),
    F.col("MedtrakClmTrns.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.col("MedtrakClmTrns.CLM_SUBMT_BCBS_PLN_CD").alias("CLM_SUBMT_BCBS_PLN_CD"),
    F.col("MedtrakClmTrns.CLM_SUB_BCBS_PLN_CD").alias("CLM_SUB_BCBS_PLN_CD"),
    F.col("MedtrakClmTrns.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("MedtrakClmTrns.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("MedtrakClmTrns.ATCHMT_IN").alias("ATCHMT_IN"),
    F.col("MedtrakClmTrns.CLM_CLNCL_EDIT_CD").alias("CLM_CLNCL_EDIT_CD"),
    F.col("MedtrakClmTrns.COBRA_CLM_IN").alias("COBRA_CLM_IN"),
    F.col("MedtrakClmTrns.FIRST_PASS_IN").alias("FIRST_PASS_IN"),
    F.col("MedtrakClmTrns.HOST_IN").alias("HOST_IN"),
    F.col("MedtrakClmTrns.LTR_IN").alias("LTR_IN"),
    F.col("MedtrakClmTrns.MCARE_ASG_IN").alias("MCARE_ASG_IN"),
    F.col("MedtrakClmTrns.NOTE_IN").alias("NOTE_IN"),
    F.col("MedtrakClmTrns.PCA_AUDIT_IN").alias("PCA_AUDIT_IN"),
    F.col("MedtrakClmTrns.PCP_SUBMT_IN").alias("PCP_SUBMT_IN"),
    F.col("MedtrakClmTrns.PROD_OOA_IN").alias("PROD_OOA_IN"),
    F.col("MedtrakClmTrns.ACDNT_DT").alias("ACDNT_DT"),
    F.col("MedtrakClmTrns.INPT_DT").alias("INPT_DT"),
    F.col("MedtrakClmTrns.MBR_PLN_ELIG_DT").alias("MBR_PLN_ELIG_DT"),
    F.col("MedtrakClmTrns.NEXT_RVW_DT").alias("NEXT_RVW_DT"),
    F.col("MedtrakClmTrns.PD_DT").alias("PD_DT"),
    F.col("MedtrakClmTrns.PAYMT_DRAG_CYC_DT").alias("PAYMT_DRAG_CYC_DT"),
    F.col("MedtrakClmTrns.PRCS_DT").alias("PRCS_DT"),
    F.col("MedtrakClmTrns.RCVD_DT").alias("RCVD_DT"),
    F.col("MedtrakClmTrns.SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("MedtrakClmTrns.SVC_END_DT").alias("SVC_END_DT"),
    F.col("MedtrakClmTrns.SMLR_ILNS_DT").alias("SMLR_ILNS_DT"),
    F.col("MedtrakClmTrns.STTUS_DT").alias("STTUS_DT"),
    F.col("MedtrakClmTrns.WORK_UNABLE_BEG_DT").alias("WORK_UNABLE_BEG_DT"),
    F.col("MedtrakClmTrns.WORK_UNABLE_END_DT").alias("WORK_UNABLE_END_DT"),
    F.col("MedtrakClmTrns.ACDNT_AMT").alias("ACDNT_AMT"),
    F.col("MedtrakClmTrns.ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    F.col("MedtrakClmTrns.ALLOW_AMT").alias("ALLOW_AMT"),
    F.col("MedtrakClmTrns.DSALW_AMT").alias("DSALW_AMT"),
    F.col("MedtrakClmTrns.COINS_AMT").alias("COINS_AMT"),
    F.col("MedtrakClmTrns.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("MedtrakClmTrns.COPAY_AMT").alias("COPAY_AMT"),
    F.col("MedtrakClmTrns.CHRG_AMT").alias("CHRG_AMT"),
    F.col("MedtrakClmTrns.DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("MedtrakClmTrns.PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("MedtrakClmTrns.CLM_CT").alias("CLM_CT"),
    F.col("MedtrakClmTrns.MBR_AGE").alias("MBR_AGE"),
    F.col("MedtrakClmTrns.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("MedtrakClmTrns.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.col("MedtrakClmTrns.DOC_TX_ID").alias("DOC_TX_ID"),
    F.col("MedtrakClmTrns.MCAID_RESUB_NO").alias("MCAID_RESUB_NO"),
    F.col("MedtrakClmTrns.MCARE_ID").alias("MCARE_ID"),
    F.col("MedtrakClmTrns.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("MedtrakClmTrns.PATN_ACCT_NO").alias("PATN_ACCT_NO"),
    F.col("MedtrakClmTrns.PAYMT_REF_ID").alias("PAYMT_REF_ID"),
    F.col("MedtrakClmTrns.PROV_AGMNT_ID").alias("PROV_AGMNT_ID"),
    F.col("MedtrakClmTrns.RFRNG_PROV_TX").alias("RFRNG_PROV_TX"),
    F.col("MedtrakClmTrns.SUB_ID").alias("SUB_ID"),
    F.lit(None).alias("PRPR_ENTITY"),
    F.lit("NA").alias("PCA_TYP_CD"),
    F.lit("NA").alias("REL_PCA_CLM_ID"),
    F.lit("NA").alias("CLCL_MICRO_ID"),
    F.lit("N").alias("CLM_UPDT_SW"),
    F.col("MedtrakClmTrns.REMIT_SUPRSION_AMT").alias("REMIT_SUPRSION_AMT"),
    F.col("MedtrakClmTrns.MCAID_STTUS_ID").alias("MCAID_STTUS_ID"),
    F.col("MedtrakClmTrns.PATN_PD_AMT").alias("PATN_PD_AMT"),
    F.col("MedtrakClmTrns.CLM_SUBMT_ICD_VRSN_CD").alias("CLM_SUBMT_ICD_VRSN_CD"),
    F.lit("").alias("CLM_TXNMY_CD")
)

df_Pkey = df_Transform.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ADJ_FROM_CLM").alias("ADJ_FROM_CLM"),
    F.col("ADJ_TO_CLM").alias("ADJ_TO_CLM"),
    F.col("ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
    F.col("CLM_EOB_EXCD").alias("CLM_EOB_EXCD"),
    F.col("CLS").alias("CLS"),
    F.col("CLS_PLN").alias("CLS_PLN"),
    F.col("EXPRNC_CAT").alias("EXPRNC_CAT"),
    F.col("FNCL_LOB_NO").alias("FNCL_LOB_NO"),
    F.col("GRP").alias("GRP"),
    F.col("MBR_CK").alias("MBR_CK"),
    F.col("NTWK").alias("NTWK"),
    F.col("PROD").alias("PROD"),
    F.col("SUBGRP").alias("SUBGRP"),
    F.col("SUB_CK").alias("SUB_CK"),
    F.col("CLM_ACDNT_CD").alias("CLM_ACDNT_CD"),
    F.col("CLM_ACDNT_ST_CD").alias("CLM_ACDNT_ST_CD"),
    F.col("CLM_ACTIVATING_BCBS_PLN_CD").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
    F.col("CLM_AGMNT_SRC_CD").alias("CLM_AGMNT_SRC_CD"),
    F.col("CLM_BTCH_ACTN_CD").alias("CLM_BTCH_ACTN_CD"),
    F.col("CLM_CAP_CD").alias("CLM_CAP_CD"),
    F.col("CLM_CAT_CD").alias("CLM_CAT_CD"),
    F.col("CLM_CHK_CYC_OVRD_CD").alias("CLM_CHK_CYC_OVRD_CD"),
    F.col("CLM_COB_CD").alias("CLM_COB_CD"),
    F.col("FINL_DISP_CD").alias("FINL_DISP_CD"),
    F.col("CLM_INPT_METH_CD").alias("CLM_INPT_METH_CD"),
    F.col("CLM_INPT_SRC_CD").alias("CLM_INPT_SRC_CD"),
    F.col("CLM_IPP_CD").alias("CLM_IPP_CD"),
    F.col("CLM_NTWK_STTUS_CD").alias("CLM_NTWK_STTUS_CD"),
    F.col("CLM_NONPAR_PROV_PFX_CD").alias("CLM_NONPAR_PROV_PFX_CD"),
    F.col("CLM_OTHER_BNF_CD").alias("CLM_OTHER_BNF_CD"),
    F.col("CLM_PAYE_CD").alias("CLM_PAYE_CD"),
    F.col("CLM_PRCS_CTL_AGNT_PFX_CD").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.col("CLM_SVC_DEFN_PFX_CD").alias("CLM_SVC_DEFN_PFX_CD"),
    F.col("CLM_SVC_PROV_SPEC_CD").alias("CLM_SVC_PROV_SPEC_CD"),
    F.col("CLM_SVC_PROV_TYP_CD").alias("CLM_SVC_PROV_TYP_CD"),
    F.col("CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.col("CLM_SUBMT_BCBS_PLN_CD").alias("CLM_SUBMT_BCBS_PLN_CD"),
    F.col("CLM_SUB_BCBS_PLN_CD").alias("CLM_SUB_BCBS_PLN_CD"),
    F.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("ATCHMT_IN").alias("ATCHMT_IN"),
    F.col("CLM_CLNCL_EDIT_CD").alias("CLM_CLNCL_EDIT_CD"),
    F.col("COBRA_CLM_IN").alias("COBRA_CLM_IN"),
    F.col("FIRST_PASS_IN").alias("FIRST_PASS_IN"),
    F.col("HOST_IN").alias("HOST_IN"),
    F.col("LTR_IN").alias("LTR_IN"),
    F.col("MCARE_ASG_IN").alias("MCARE_ASG_IN"),
    F.col("NOTE_IN").alias("NOTE_IN"),
    F.col("PCA_AUDIT_IN").alias("PCA_AUDIT_IN"),
    F.col("PCP_SUBMT_IN").alias("PCP_SUBMT_IN"),
    F.col("PROD_OOA_IN").alias("PROD_OOA_IN"),
    F.col("ACDNT_DT").alias("ACDNT_DT"),
    F.col("INPT_DT").alias("INPT_DT"),
    F.col("MBR_PLN_ELIG_DT").alias("MBR_PLN_ELIG_DT"),
    F.col("NEXT_RVW_DT").alias("NEXT_RVW_DT"),
    F.col("PD_DT").alias("PD_DT"),
    F.col("PAYMT_DRAG_CYC_DT").alias("PAYMT_DRAG_CYC_DT"),
    F.col("PRCS_DT").alias("PRCS_DT"),
    F.col("RCVD_DT").alias("RCVD_DT"),
    F.col("SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("SVC_END_DT").alias("SVC_END_DT"),
    F.col("SMLR_ILNS_DT").alias("SMLR_ILNS_DT"),
    F.col("STTUS_DT").alias("STTUS_DT"),
    F.col("WORK_UNABLE_BEG_DT").alias("WORK_UNABLE_BEG_DT"),
    F.col("WORK_UNABLE_END_DT").alias("WORK_UNABLE_END_DT"),
    F.col("ACDNT_AMT").alias("ACDNT_AMT"),
    F.col("ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    F.col("ALLOW_AMT").alias("ALLOW_AMT"),
    F.col("DSALW_AMT").alias("DSALW_AMT"),
    F.col("COINS_AMT").alias("COINS_AMT"),
    F.col("CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("COPAY_AMT").alias("COPAY_AMT"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("CLM_CT").alias("CLM_CT"),
    F.col("MBR_AGE").alias("MBR_AGE"),
    F.col("ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.col("DOC_TX_ID").alias("DOC_TX_ID"),
    F.col("MCAID_RESUB_NO").alias("MCAID_RESUB_NO"),
    F.col("MCARE_ID").alias("MCARE_ID"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("PATN_ACCT_NO").alias("PATN_ACCT_NO"),
    F.col("PAYMT_REF_ID").alias("PAYMT_REF_ID"),
    F.col("PROV_AGMNT_ID").alias("PROV_AGMNT_ID"),
    F.col("RFRNG_PROV_TX").alias("RFRNG_PROV_TX"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("PRPR_ENTITY").alias("PRPR_ENTITY"),
    F.col("PCA_TYP_CD").alias("PCA_TYP_CD"),
    F.col("REL_PCA_CLM_ID").alias("REL_PCA_CLM_ID"),
    F.col("CLCL_MICRO_ID").alias("CLCL_MICRO_ID"),
    F.col("CLM_UPDT_SW").alias("CLM_UPDT_SW"),
    F.col("REMIT_SUPRSION_AMT").alias("REMIT_SUPRSION_AMT"),
    F.col("MCAID_STTUS_ID").alias("MCAID_STTUS_ID"),
    F.col("PATN_PD_AMT").alias("PATN_PD_AMT"),
    F.col("CLM_SUBMT_ICD_VRSN_CD").alias("CLM_SUBMT_ICD_VRSN_CD"),
    F.col("CLM_TXNMY_CD").alias("CLM_TXNMY_CD"),
    F.lit("N").alias("BILL_PAYMT_EXCL_IN")
)

params_ClmPK = {
    "CurrRunCycle": RunCycle
}
df_key = ClmPK(df_Pkey, params_ClmPK)

df_key_for_file = df_key.select(
    [
        # For each column in the same order, apply rpad if char/varchar
        rpad(F.col("JOB_EXCTN_RCRD_ERR_SK").cast(StringType()), 1, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
        rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ERR_CT"),
        F.col("RECYCLE_CT"),
        F.col("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING"),
        F.col("CLM_SK"),
        F.col("SRC_SYS_CD_SK"),
        rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        rpad(F.col("ADJ_FROM_CLM"), 12, " ").alias("ADJ_FROM_CLM"),
        rpad(F.col("ADJ_TO_CLM"), 12, " ").alias("ADJ_TO_CLM"),
        rpad(F.col("ALPHA_PFX_CD"), 3, " ").alias("ALPHA_PFX_CD"),
        rpad(F.col("CLM_EOB_EXCD"), 3, " ").alias("CLM_EOB_EXCD"),
        rpad(F.col("CLS"), 4, " ").alias("CLS"),
        rpad(F.col("CLS_PLN"), 8, " ").alias("CLS_PLN"),
        rpad(F.col("EXPRNC_CAT"), 4, " ").alias("EXPRNC_CAT"),
        rpad(F.col("FNCL_LOB_NO"), 4, " ").alias("FNCL_LOB_NO"),
        rpad(F.col("GRP"), 8, " ").alias("GRP"),
        F.col("MBR_CK"),
        rpad(F.col("NTWK"), 12, " ").alias("NTWK"),
        rpad(F.col("PROD"), 8, " ").alias("PROD"),
        rpad(F.col("SUBGRP"), 4, " ").alias("SUBGRP"),
        F.col("SUB_CK"),
        rpad(F.col("CLM_ACDNT_CD"), 10, " ").alias("CLM_ACDNT_CD"),
        rpad(F.col("CLM_ACDNT_ST_CD"), 2, " ").alias("CLM_ACDNT_ST_CD"),
        rpad(F.col("CLM_ACTIVATING_BCBS_PLN_CD"), 10, " ").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
        rpad(F.col("CLM_AGMNT_SRC_CD"), 10, " ").alias("CLM_AGMNT_SRC_CD"),
        rpad(F.col("CLM_BTCH_ACTN_CD"), 1, " ").alias("CLM_BTCH_ACTN_CD"),
        rpad(F.col("CLM_CAP_CD"), 10, " ").alias("CLM_CAP_CD"),
        rpad(F.col("CLM_CAT_CD"), 10, " ").alias("CLM_CAT_CD"),
        rpad(F.col("CLM_CHK_CYC_OVRD_CD"), 1, " ").alias("CLM_CHK_CYC_OVRD_CD"),
        rpad(F.col("CLM_COB_CD"), 1, " ").alias("CLM_COB_CD"),
        F.col("FINL_DISP_CD"),
        rpad(F.col("CLM_INPT_METH_CD"), 1, " ").alias("CLM_INPT_METH_CD"),
        rpad(F.col("CLM_INPT_SRC_CD"), 10, " ").alias("CLM_INPT_SRC_CD"),
        rpad(F.col("CLM_IPP_CD"), 10, " ").alias("CLM_IPP_CD"),
        rpad(F.col("CLM_NTWK_STTUS_CD"), 2, " ").alias("CLM_NTWK_STTUS_CD"),
        rpad(F.col("CLM_NONPAR_PROV_PFX_CD"), 4, " ").alias("CLM_NONPAR_PROV_PFX_CD"),
        rpad(F.col("CLM_OTHER_BNF_CD"), 1, " ").alias("CLM_OTHER_BNF_CD"),
        rpad(F.col("CLM_PAYE_CD"), 1, " ").alias("CLM_PAYE_CD"),
        rpad(F.col("CLM_PRCS_CTL_AGNT_PFX_CD"), 4, " ").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
        rpad(F.col("CLM_SVC_DEFN_PFX_CD"), 4, " ").alias("CLM_SVC_DEFN_PFX_CD"),
        rpad(F.col("CLM_SVC_PROV_SPEC_CD"), 10, " ").alias("CLM_SVC_PROV_SPEC_CD"),
        rpad(F.col("CLM_SVC_PROV_TYP_CD"), 10, " ").alias("CLM_SVC_PROV_TYP_CD"),
        rpad(F.col("CLM_STTUS_CD"), 2, " ").alias("CLM_STTUS_CD"),
        rpad(F.col("CLM_SUBMT_BCBS_PLN_CD"), 10, " ").alias("CLM_SUBMT_BCBS_PLN_CD"),
        rpad(F.col("CLM_SUB_BCBS_PLN_CD"), 10, " ").alias("CLM_SUB_BCBS_PLN_CD"),
        rpad(F.col("CLM_SUBTYP_CD"), 10, " ").alias("CLM_SUBTYP_CD"),
        rpad(F.col("CLM_TYP_CD"), 1, " ").alias("CLM_TYP_CD"),
        rpad(F.col("ATCHMT_IN"), 1, " ").alias("ATCHMT_IN"),
        rpad(F.col("CLM_CLNCL_EDIT_CD"), 1, " ").alias("CLM_CLNCL_EDIT_CD"),
        rpad(F.col("COBRA_CLM_IN"), 1, " ").alias("COBRA_CLM_IN"),
        rpad(F.col("FIRST_PASS_IN"), 1, " ").alias("FIRST_PASS_IN"),
        rpad(F.col("HOST_IN"), 1, " ").alias("HOST_IN"),
        rpad(F.col("LTR_IN"), 1, " ").alias("LTR_IN"),
        rpad(F.col("MCARE_ASG_IN"), 1, " ").alias("MCARE_ASG_IN"),
        rpad(F.col("NOTE_IN"), 1, " ").alias("NOTE_IN"),
        rpad(F.col("PCA_AUDIT_IN"), 1, " ").alias("PCA_AUDIT_IN"),
        rpad(F.col("PCP_SUBMT_IN"), 1, " ").alias("PCP_SUBMT_IN"),
        rpad(F.col("PROD_OOA_IN"), 1, " ").alias("PROD_OOA_IN"),
        rpad(F.col("ACDNT_DT"), 10, " ").alias("ACDNT_DT"),
        rpad(F.col("INPT_DT"), 10, " ").alias("INPT_DT"),
        rpad(F.col("MBR_PLN_ELIG_DT"), 10, " ").alias("MBR_PLN_ELIG_DT"),
        rpad(F.col("NEXT_RVW_DT"), 10, " ").alias("NEXT_RVW_DT"),
        rpad(F.col("PD_DT"), 10, " ").alias("PD_DT"),
        rpad(F.col("PAYMT_DRAG_CYC_DT"), 10, " ").alias("PAYMT_DRAG_CYC_DT"),
        rpad(F.col("PRCS_DT"), 10, " ").alias("PRCS_DT"),
        rpad(F.col("RCVD_DT"), 10, " ").alias("RCVD_DT"),
        rpad(F.col("SVC_STRT_DT"), 10, " ").alias("SVC_STRT_DT"),
        rpad(F.col("SVC_END_DT"), 10, " ").alias("SVC_END_DT"),
        rpad(F.col("SMLR_ILNS_DT"), 10, " ").alias("SMLR_ILNS_DT"),
        rpad(F.col("STTUS_DT"), 10, " ").alias("STTUS_DT"),
        rpad(F.col("WORK_UNABLE_BEG_DT"), 10, " ").alias("WORK_UNABLE_BEG_DT"),
        rpad(F.col("WORK_UNABLE_END_DT"), 10, " ").alias("WORK_UNABLE_END_DT"),
        F.col("ACDNT_AMT"),
        F.col("ACTL_PD_AMT"),
        F.col("ALLOW_AMT"),
        F.col("DSALW_AMT"),
        F.col("COINS_AMT"),
        F.col("CNSD_CHRG_AMT"),
        F.col("COPAY_AMT"),
        F.col("CHRG_AMT"),
        F.col("DEDCT_AMT"),
        F.col("PAYBL_AMT"),
        F.col("CLM_CT"),
        F.col("MBR_AGE"),
        F.col("ADJ_FROM_CLM_ID"),
        F.col("ADJ_TO_CLM_ID"),
        rpad(F.col("DOC_TX_ID"), 18, " ").alias("DOC_TX_ID"),
        rpad(F.col("MCAID_RESUB_NO"), 15, " ").alias("MCAID_RESUB_NO"),
        rpad(F.col("MCARE_ID"), 12, " ").alias("MCARE_ID"),
        rpad(F.col("MBR_SFX_NO"), 2, " ").alias("MBR_SFX_NO"),
        F.col("PATN_ACCT_NO"),
        rpad(F.col("PAYMT_REF_ID"), 16, " ").alias("PAYMT_REF_ID"),
        rpad(F.col("PROV_AGMNT_ID"), 12, " ").alias("PROV_AGMNT_ID"),
        F.col("RFRNG_PROV_TX"),
        rpad(F.col("SUB_ID"), 14, " ").alias("SUB_ID"),
        rpad(F.col("PRPR_ENTITY").cast(StringType()), 1, " ").alias("PRPR_ENTITY"),
        rpad(F.col("PCA_TYP_CD"), 18, " ").alias("PCA_TYP_CD"),
        F.col("REL_PCA_CLM_ID"),
        F.col("CLCL_MICRO_ID"),
        rpad(F.col("CLM_UPDT_SW"), 1, " ").alias("CLM_UPDT_SW"),
        F.col("REMIT_SUPRSION_AMT"),
        F.col("MCAID_STTUS_ID"),
        F.col("PATN_PD_AMT"),
        F.col("CLM_SUBMT_ICD_VRSN_CD"),
        F.col("CLM_TXNMY_CD"),
        rpad(F.col("BILL_PAYMT_EXCL_IN"), 1, " ").alias("BILL_PAYMT_EXCL_IN")
    ]
)

write_files(
    df_key_for_file,
    f"{adls_path}/key/BCBSAClmExtr.DrugClm.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_Snapshot = df_Transform.select(
    F.rpad(F.col("CLM_ID"), 12, " ").alias("CLM_ID"),
    F.col("MBR_CK").alias("MBR_CK"),
    F.rpad(F.col("GRP"), 8, " ").alias("GRP"),
    F.col("SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.rpad(F.col("EXPRNC_CAT"), 4, " ").alias("EXPRNC_CAT"),
    F.rpad(F.col("FNCL_LOB_NO"), 4, " ").alias("FNCL_LOB_NO"),
    F.col("CLM_CT").alias("CLM_CT"),
    F.rpad(F.col("PCA_TYP_CD"), 18, " ").alias("PCA_TYP_CD"),
    F.rpad(F.col("CLM_STTUS_CD"), 2, " ").alias("CLM_STTUS_CD"),
    F.rpad(F.col("CLM_CAT_CD"), 10, " ").alias("CLM_CAT_CD")
)

df_Transformer = df_Snapshot.withColumn("ExpCatCdSk", GetFkeyExprncCat("FACETS", F.lit(0), F.col("EXPRNC_CAT"), F.lit("N"))) \
    .withColumn("GrpSk", GetFkeyGrp("FACETS", F.lit(0), F.col("GRP"), F.lit("N"))) \
    .withColumn("MbrSk", GetFkeyMbr("FACETS", F.lit(0), F.col("MBR_CK"), F.lit("N"))) \
    .withColumn("FnclLobSk", GetFkeyFnclLob("PSI", F.lit(0), F.col("FNCL_LOB_NO"), F.lit("N"))) \
    .withColumn("PcaTypCdSk", GetFkeyCodes("FACETS", F.lit(0), F.lit("PERSONAL CARE ACCOUNT PROCESSING"), F.col("PCA_TYP_CD"), F.lit("N"))) \
    .withColumn("ClmSttusCdSk", GetFkeyCodes("BCA", F.lit(0), F.lit("CLAIM STATUS"), F.col("CLM_STTUS_CD"), F.lit("N"))) \
    .withColumn("ClmCatCdSk", GetFkeyCodes(F.col("SrcSysCd"), F.lit(0), F.lit("CLAIM CATEGORY"), F.col("CLM_CAT_CD"), F.lit("X")))

df_RowCount = df_Transformer.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("ClmSttusCdSk").alias("CLM_STTUS_CD_SK"),
    F.col("ClmCatCdSk").alias("CLM_CAT_CD_SK"),
    F.col("ExpCatCdSk").alias("EXPRNC_CAT_SK"),
    F.col("FnclLobSk").alias("FNCL_LOB_SK"),
    F.col("GrpSk").alias("GRP_SK"),
    F.col("MbrSk").alias("MBR_SK"),
    F.rpad(F.col("SVC_STRT_DT"), 10, " ").alias("SVC_STRT_DT_SK"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("CLM_CT").alias("CLM_CT"),
    F.col("PcaTypCdSk").alias("PCA_TYP_CD_SK")
)

df_RowCount_for_file = df_RowCount  # No additional char-type lengths specified, only SVC_STRT_DT gets rpad above.

write_files(
    df_RowCount_for_file,
    f"{adls_path}/load/B_CLM.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)