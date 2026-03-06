# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY:   BCBSADrugExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Reads the BCBSADrugClm_Land.dat created in BCBSADrugClmPreProcExtr job and runs through primary key using shared container ClmLnPK
# MAGIC   
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                              Date               Project/Altiris#                Description                                                                   Project                         Code Reviewer          Date Reviewed
# MAGIC ----------------------------               ------------------       -----------------------               -----------------------------------------------------------------------         ---------------------------------         -------------------------          ---------------------------------
# MAGIC 
# MAGIC 
# MAGIC Karthik Chinalapani              2016-04-29       5212                           Initial programming                                                 	  IntegrateDev 2                     Kalyan Neelam           2016-05-11
# MAGIC 
# MAGIC 
# MAGIC Karthik Chinalapani              2016-09-20       5212         Added logic to remove leading zeroes for CLM_LN_NO          	  IntegrateDev 1                    Kalyan Neelam           2016-10-11  
# MAGIC                                                                                       in Xfm_Data stage.
# MAGIC 
# MAGIC Hari Pinnaka             2017-08-21   5792 Drug Rebate      Added 3 new fields (NDC_SK ,
# MAGIC                                                       Incentive                     NDC_DRUG_FORM_CD_SK, NDC_UNIT_CT) 
# MAGIC                                                                                             at the end of the file                                                            	IntegrateDev2
# MAGIC Jaideep Mankala      2017-11-20      5828 	   Added new field to identify MED / PDX claim                           	IntegrateDev2                     Kalyan Neelam           2017-11-20
# MAGIC                                                       		when passing to Fkey job
# MAGIC Manasa Andru        2018-03-05    TFS 21142          Modified Logic to populate PROV_ID instead of NTNL provid	IntegrateDev2                     Kalyan Neelam           2018-03-12
# MAGIC 					   in stage variable svSvcProvID
# MAGIC 
# MAGIC Madhavan B            2018-02-06      5792 	     Changed the datatype of the column                                                  IntegrateDev1                     Kalyan Neelam           2018-02-08
# MAGIC                                                       		     NDC_UNIT_CT from SMALLINT to DECIMAL (21,10)
# MAGIC 
# MAGIC Sagar S                    2020-08-28 6264-PBM Phase II       Added APC_ID, APC_STTUS_ID                                                   IntegrateDev5                    Kalyan Neelam           2020-12-10
# MAGIC                                                     Government Programs   
# MAGIC 
# MAGIC Deepika C               2023-08-01       US 589700          Added two new fields SNOMED_CT_CD,CVX_VCCN_CD with a       IntegrateDevB		Harsha Ravuri	2023-08-30
# MAGIC                                                                                     default value in BusinessRules stage and mapped it till target

# MAGIC BCBSA Rx Claim Line Extract
# MAGIC Added new field MED_PDX_IND to the common layout file that is sent to IdsClmLnFkey job
# MAGIC which indicates whether the claim is Medical or Pharmacy
# MAGIC Indicator used :
# MAGIC MED - medical
# MAGIC PDX - Pharmacy
# MAGIC The file is created in the BCBSADrugClmPrepProcExtr job
# MAGIC This container is used in:
# MAGIC ArgusClmLnExtr
# MAGIC ESIClmLnExtr
# MAGIC PCSClmLnExtr
# MAGIC PseudoClmLnPkey
# MAGIC FctsClmLnDntlTrns
# MAGIC FctsClmLnExtr
# MAGIC NascoClmLnExtr
# MAGIC WellDyneClmLnExtr
# MAGIC MCSourceClmLnExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Writing Sequential File to /key
# MAGIC Assign primary surrogate key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit, regexp_replace, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunCycle = get_widget_value('RunCycle','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# MAGIC %run ../../../../../../../shared_containers/PrimaryKey/ClmLnPK
# COMMAND ----------

# IDS_PROV (DB2Connector)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_ids_prov = f"""
SELECT
PROV1.NTNL_PROV_ID,
PROV2.PROV_ID
FROM
(
SELECT
PROV.NTNL_PROV_ID,
MIN(PROV.PROV_ID) PROV_ID
FROM
{IDSOwner}.PROV PROV,
{IDSOwner}.CD_MPPNG CDMP,
{IDSOwner}.PROV_ADDR PA
WHERE
PROV.PROV_ADDR_ID=PA.PROV_ADDR_ID
AND PA.PROV_ADDR_TYP_CD_SK=CDMP.CD_MPPNG_SK
AND CDMP.TRGT_CD='P'
AND PROV.NTNL_PROV_ID<>'NA'
AND PA.TERM_DT_SK>'{CurrentDate}'
GROUP BY
PROV.NTNL_PROV_ID
) PROV1,
{IDSOwner}.PROV PROV2
WHERE
PROV1.PROV_ID = PROV2.PROV_ID
"""
df_IDS_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids_prov)
    .load()
)

# hf_bcbsa_clm_ln_provid_6 (CHashedFileStage) - Scenario A (Intermediate)
df_hf_bcbsa_clm_ln_provid_6 = df_IDS_PROV.dropDuplicates(["NTNL_PROV_ID"])

# BCBSARxClmLand (CSeqFileStage) - Read delimited file with defined schema
schema_BCBSARxClmLand = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
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
    StructField("SUBGRP_SK", IntegerType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("SUB_SK", IntegerType(), False),
    StructField("SUB_ID", StringType(), False),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("MBR_GNDR_CD_SK", IntegerType(), False),
    StructField("MBR_GNDR_CD", StringType(), True),
    StructField("BRTH_DT_SK", StringType(), False),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_ID", StringType(), False)
])
df_BCBSARxClmLand = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", '"')
    .schema(schema_BCBSARxClmLand)
    .load(f"{adls_path}/verified/BCBSADrugClm_Land.dat.{RunID}")
)

# Xfm_Data (CTransformerStage)
df_BcbsaData = df_BCBSARxClmLand.select(
    col("CLM_ID").alias("CLM_ID"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("NDW_HOME_PLN_ID").alias("NDW_HOME_PLN_ID"),
    when(
        F.trim(col("CLM_LN_NO")) == '000',
        lit('1')
    ).otherwise(regexp_replace(F.trim(col("CLM_LN_NO")), '^0+', '')).alias("CLM_LN_NO"),
    col("TRACEABILITY_FLD").alias("TRACEABILITY_FLD"),
    col("ADJ_SEQ_NO").alias("ADJ_SEQ_NO"),
    col("HOST_PLN_ID").alias("HOST_PLN_ID"),
    col("MBR_ID").alias("MBR_ID"),
    col("MBR_ZIP_CD_ON_CLM").alias("MBR_ZIP_CD_ON_CLM"),
    col("MBR_CTRY_ON_CLM").alias("MBR_CTRY_ON_CLM"),
    col("NPI_REND_PROV_ID").alias("NPI_REND_PROV_ID"),
    col("PRSCRB_PROV_ID").alias("PRSCRB_PROV_ID"),
    col("NPI_PRSCRB_PROV_ID").alias("NPI_PRSCRB_PROV_ID"),
    col("BNF_PAYMT_STTUS_CD").alias("BNF_PAYMT_STTUS_CD"),
    col("PDX_CARVE_OUT_SUBMSN_IN").alias("PDX_CARVE_OUT_SUBMSN_IN"),
    col("CAT_OF_SVC").alias("CAT_OF_SVC"),
    col("CLM_PAYMT_STTUS").alias("CLM_PAYMT_STTUS"),
    col("DAW_CD").alias("DAW_CD"),
    col("DAYS_SUPL").alias("DAYS_SUPL"),
    col("FRMLRY_IN").alias("FRMLRY_IN"),
    col("PLN_SPEC_DRUG_IN").alias("PLN_SPEC_DRUG_IN"),
    col("PROD_SVC_ID").alias("PROD_SVC_ID"),
    col("QTY_DISPNS").alias("QTY_DISPNS"),
    col("CLM_PD_DT").alias("CLM_PD_DT"),
    col("DT_OF_SVC").alias("DT_OF_SVC"),
    col("AVG_WHLSL_PRICE_SUBMT_AMT").alias("AVG_WHLSL_PRICE_SUBMT_AMT"),
    col("CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    col("MMI_ID").alias("MMI_ID"),
    col("SUBGRP_SK").alias("SUBGRP_SK"),
    col("SUBGRP_ID").alias("SUBGRP_ID"),
    col("SUB_SK").alias("SUB_SK"),
    col("SUB_ID").alias("SUB_ID"),
    col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    col("MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
    col("MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    col("BRTH_DT_SK").alias("BRTH_DT_SK"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("GRP_ID").alias("GRP_ID")
)

# BusinessRules (CTransformerStage) - Left join with provid6_lkup
df_BusinessRules_join = (
    df_BcbsaData.alias("BcbsaData")
    .join(
        df_hf_bcbsa_clm_ln_provid_6.alias("provid6_lkup"),
        on=[F.col("BcbsaData.NPI_REND_PROV_ID") == F.col("provid6_lkup.NTNL_PROV_ID")],
        how="left"
    )
)

df_BusinessRules = df_BusinessRules_join.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    col("SrcSysCd").alias("SRC_SYS_CD"),
    F.concat(F.col("SrcSysCd"), F.lit(";"), F.col("BcbsaData.CLM_ID"), F.lit(";"), F.col("BcbsaData.CLM_LN_NO")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_SK"),
    col("BcbsaData.CLM_ID").alias("CLM_ID"),
    col("BcbsaData.CLM_LN_NO").alias("CLM_LN_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("NA").alias("PROC_CD"),
    when(col("provid6_lkup.PROV_ID").isNotNull(), trim(col("provid6_lkup.PROV_ID"))).otherwise(lit("UNK")).alias("SVC_PROV_ID"),
    F.lit("NA").alias("CLM_LN_DSALW_EXCD"),
    F.lit("NA").alias("CLM_LN_EOB_EXCD"),
    F.rpad(F.lit("ACPTD"), 5, " ").alias("CLM_LN_FINL_DISP_CD"),
    F.lit("NA").alias("CLM_LN_LOB_CD"),
    F.lit("NA").alias("CLM_LN_POS_CD"),
    F.lit("NA").alias("CLM_LN_PREAUTH_CD"),
    F.lit("NA").alias("CLM_LN_PREAUTH_SRC_CD"),
    F.lit("NA").alias("CLM_LN_PRICE_SRC_CD"),
    F.lit("NA").alias("CLM_LN_RFRL_CD"),
    F.lit("NA").alias("CLM_LN_RVNU_CD"),
    F.rpad(F.lit("NA"), 2, " ").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.lit("NA").alias("CLM_LN_ROOM_TYP_CD"),
    F.lit("NA").alias("CLM_LN_TOS_CD"),
    F.lit("NA").alias("CLM_LN_UNIT_TYP_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("CAP_LN_IN"),
    F.rpad(F.lit("N"), 1, " ").alias("PRI_LOB_IN"),
    col("BcbsaData.DT_OF_SVC").alias("SVC_END_DT"),
    col("BcbsaData.DT_OF_SVC").alias("SVC_STRT_DT"),
    F.lit(0.00).alias("AGMNT_PRICE_AMT"),
    when(
        (F.trim(col("BcbsaData.AVG_WHLSL_PRICE_SUBMT_AMT")).isNull())
        | (F.length(F.trim(col("BcbsaData.AVG_WHLSL_PRICE_SUBMT_AMT"))) == 0),
        lit(0.00)
    ).otherwise(col("BcbsaData.AVG_WHLSL_PRICE_SUBMT_AMT").cast("double")).alias("ALW_AMT"),
    when(
        (F.trim(col("BcbsaData.AVG_WHLSL_PRICE_SUBMT_AMT")).isNull())
        | (F.length(F.trim(col("BcbsaData.AVG_WHLSL_PRICE_SUBMT_AMT"))) == 0),
        lit(0.00)
    ).otherwise(col("BcbsaData.AVG_WHLSL_PRICE_SUBMT_AMT").cast("double")).alias("CHRG_AMT"),
    F.lit(0.00).alias("COINS_AMT"),
    when(
        (F.trim(col("BcbsaData.AVG_WHLSL_PRICE_SUBMT_AMT")).isNull())
        | (F.length(F.trim(col("BcbsaData.AVG_WHLSL_PRICE_SUBMT_AMT"))) == 0),
        lit(0.00)
    ).otherwise(col("BcbsaData.AVG_WHLSL_PRICE_SUBMT_AMT").cast("double")).alias("CNSD_CHRG_AMT"),
    F.lit(0.00).alias("COPAY_AMT"),
    F.lit(0.00).alias("DEDCT_AMT"),
    F.lit(0.00).alias("DSALW_AMT"),
    F.lit(0.00).alias("ITS_HOME_DSCNT_AMT"),
    F.lit(0.00).alias("NO_RESP_AMT"),
    F.lit(0.00).alias("MBR_LIAB_BSS_AMT"),
    F.lit(0.00).alias("PATN_RESP_AMT"),
    F.lit(0.00).alias("PAYBL_AMT"),
    F.lit(0.00).alias("PAYBL_TO_PROV_AMT"),
    F.lit(0.00).alias("PAYBL_TO_SUB_AMT"),
    F.lit(0.00).alias("PROC_TBL_PRICE_AMT"),
    F.lit(0.00).alias("PROFL_PRICE_AMT"),
    F.lit(0.00).alias("PROV_WRT_OFF_AMT"),
    F.lit(0.00).alias("RISK_WTHLD_AMT"),
    F.lit(0.00).alias("SVC_PRICE_AMT"),
    F.lit(0.00).alias("SUPLMT_DSCNT_AMT"),
    F.lit(0).alias("ALW_PRICE_UNIT_CT"),
    F.lit(0).alias("UNIT_CT"),
    F.rpad(F.lit("NA"), 4, " ").alias("DEDCT_AMT_ACCUM_ID"),
    F.rpad(F.lit("NA"), 4, " ").alias("PREAUTH_SVC_SEQ_NO"),
    F.rpad(F.lit("NA"), 4, " ").alias("RFRL_SVC_SEQ_NO"),
    F.rpad(F.lit("NA"), 4, " ").alias("LMT_PFX_ID"),
    F.rpad(F.lit("NA"), 9, " ").alias("PREAUTH_ID"),
    F.rpad(F.lit("NA"), 4, " ").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.rpad(F.lit("NA"), 4, " ").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.rpad(F.lit("NA"), 9, " ").alias("RFRL_ID_TX"),
    F.rpad(F.lit("NA"), 4, " ").alias("SVC_ID"),
    F.rpad(F.lit("NA"), 4, " ").alias("SVC_PRICE_RULE_ID"),
    F.rpad(F.lit("NA"), 3, " ").alias("SVC_RULE_TYP_TX"),
    F.rpad(F.lit("NA"), 20, " ").alias("SVC_LOC_TYP_CD"),
    F.lit(0.00).alias("NON_PAR_SAV_AMT"),
    F.lit("NA").alias("PROC_CD_TYP_CD"),
    F.lit("NA").alias("PROC_CD_CAT_CD"),
    F.lit("NA").alias("VBB_RULE_ID"),
    F.lit("NA").alias("VBB_EXCD_ID"),
    F.rpad(F.lit("N"), 1, " ").alias("CLM_LN_VBB_IN"),
    F.lit(0.00).alias("ITS_SUPLMT_DSCNT_AMT"),
    F.lit(0.00).alias("ITS_SRCHRG_AMT"),
    F.lit("NA").alias("NDC"),
    F.lit("NA").alias("NDC_DRUG_FORM_CD"),
    F.lit(None).alias("NDC_UNIT_CT"),
    F.rpad(F.lit("PDX"), 3, " ").alias("MED_PDX_IND"),
    F.lit(None).alias("APC_ID"),
    F.lit(None).alias("APC_STTUS_ID"),
    col("SNOMED_CT_CD").alias("SNOMED_CT_CD"),  # from transform defined above, now referencing the same alias
    col("CVX_VCCN_CD").alias("CVX_VCCN_CD")     # from transform defined above, also referencing same alias
)

# Snapshot (CTransformerStage) - splits output: PKey, Snapshot
df_Snapshot_forPKey = df_BusinessRules.select(
    col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("CLM_LN_SK").alias("CLM_LN_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PROC_CD").alias("PROC_CD"),
    col("SVC_PROV_ID").alias("SVC_PROV_ID"),
    col("CLM_LN_DSALW_EXCD").alias("CLM_LN_DSALW_EXCD"),
    col("CLM_LN_EOB_EXCD").alias("CLM_LN_EOB_EXCD"),
    col("CLM_LN_FINL_DISP_CD").alias("CLM_LN_FINL_DISP_CD"),
    col("CLM_LN_LOB_CD").alias("CLM_LN_LOB_CD"),
    col("CLM_LN_POS_CD").alias("CLM_LN_POS_CD"),
    col("CLM_LN_PREAUTH_CD").alias("CLM_LN_PREAUTH_CD"),
    col("CLM_LN_PREAUTH_SRC_CD").alias("CLM_LN_PREAUTH_SRC_CD"),
    col("CLM_LN_PRICE_SRC_CD").alias("CLM_LN_PRICE_SRC_CD"),
    col("CLM_LN_RFRL_CD").alias("CLM_LN_RFRL_CD"),
    col("CLM_LN_RVNU_CD").alias("CLM_LN_RVNU_CD"),
    col("CLM_LN_ROOM_PRICE_METH_CD").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    col("CLM_LN_ROOM_TYP_CD").alias("CLM_LN_ROOM_TYP_CD"),
    col("CLM_LN_TOS_CD").alias("CLM_LN_TOS_CD"),
    col("CLM_LN_UNIT_TYP_CD").alias("CLM_LN_UNIT_TYP_CD"),
    col("CAP_LN_IN").alias("CAP_LN_IN"),
    col("PRI_LOB_IN").alias("PRI_LOB_IN"),
    col("SVC_END_DT").alias("SVC_END_DT"),
    col("SVC_STRT_DT").alias("SVC_STRT_DT"),
    col("AGMNT_PRICE_AMT").alias("AGMNT_PRICE_AMT"),
    col("ALW_AMT").alias("ALW_AMT"),
    col("CHRG_AMT").alias("CHRG_AMT"),
    col("COINS_AMT").alias("COINS_AMT"),
    col("CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    col("COPAY_AMT").alias("COPAY_AMT"),
    col("DEDCT_AMT").alias("DEDCT_AMT"),
    col("DSALW_AMT").alias("DSALW_AMT"),
    col("ITS_HOME_DSCNT_AMT").alias("ITS_HOME_DSCNT_AMT"),
    col("NO_RESP_AMT").alias("NO_RESP_AMT"),
    col("MBR_LIAB_BSS_AMT").alias("MBR_LIAB_BSS_AMT"),
    col("PATN_RESP_AMT").alias("PATN_RESP_AMT"),
    col("PAYBL_AMT").alias("PAYBL_AMT"),
    col("PAYBL_TO_PROV_AMT").alias("PAYBL_TO_PROV_AMT"),
    col("PAYBL_TO_SUB_AMT").alias("PAYBL_TO_SUB_AMT"),
    col("PROC_TBL_PRICE_AMT").alias("PROC_TBL_PRICE_AMT"),
    col("PROFL_PRICE_AMT").alias("PROFL_PRICE_AMT"),
    col("PROV_WRT_OFF_AMT").alias("PROV_WRT_OFF_AMT"),
    col("RISK_WTHLD_AMT").alias("RISK_WTHLD_AMT"),
    col("SVC_PRICE_AMT").alias("SVC_PRICE_AMT"),
    col("SUPLMT_DSCNT_AMT").alias("SUPLMT_DSCNT_AMT"),
    col("ALW_PRICE_UNIT_CT").alias("ALW_PRICE_UNIT_CT"),
    col("UNIT_CT").alias("UNIT_CT"),
    col("DEDCT_AMT_ACCUM_ID").alias("DEDCT_AMT_ACCUM_ID"),
    col("PREAUTH_SVC_SEQ_NO").alias("PREAUTH_SVC_SEQ_NO"),
    col("RFRL_SVC_SEQ_NO").alias("RFRL_SVC_SEQ_NO"),
    col("LMT_PFX_ID").alias("LMT_PFX_ID"),
    col("PREAUTH_ID").alias("PREAUTH_ID"),
    col("PROD_CMPNT_DEDCT_PFX_ID").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    col("PROD_CMPNT_SVC_PAYMT_ID").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    col("RFRL_ID_TX").alias("RFRL_ID_TX"),
    col("SVC_ID").alias("SVC_ID"),
    col("SVC_PRICE_RULE_ID").alias("SVC_PRICE_RULE_ID"),
    col("SVC_RULE_TYP_TX").alias("SVC_RULE_TYP_TX"),
    col("SVC_LOC_TYP_CD").alias("SVC_LOC_TYP_CD"),
    col("NON_PAR_SAV_AMT").alias("NON_PAR_SAV_AMT"),
    col("PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD"),
    col("PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD"),
    col("VBB_RULE_ID").alias("VBB_RULE_ID"),
    col("VBB_EXCD_ID").alias("VBB_EXCD_ID"),
    col("CLM_LN_VBB_IN").alias("CLM_LN_VBB_IN"),
    col("ITS_SUPLMT_DSCNT_AMT").alias("ITS_SUPLMT_DSCNT_AMT"),
    col("ITS_SRCHRG_AMT").alias("ITS_SRCHRG_AMT"),
    col("NDC").alias("NDC"),
    col("NDC_DRUG_FORM_CD").alias("NDC_DRUG_FORM_CD"),
    col("NDC_UNIT_CT").alias("NDC_UNIT_CT"),
    col("MED_PDX_IND").alias("MED_PDX_IND"),
    col("APC_ID").alias("APC_ID"),
    col("APC_STTUS_ID").alias("APC_STTUS_ID"),
    col("SNOMED_CT_CD").alias("SNOMED_CT_CD"),
    col("CVX_VCCN_CD").alias("CVX_VCCN_CD")
)

df_Snapshot_forSnapshot = df_BusinessRules.select(
    col("CLM_ID").alias("CLCL_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("PROC_CD").alias("PROC_CD"),
    col("CLM_LN_RVNU_CD").alias("CLM_LN_RVNU_CD"),
    col("ALW_AMT").alias("ALW_AMT"),
    col("CHRG_AMT").alias("CHRG_AMT"),
    col("PAYBL_AMT").alias("PAYBL_AMT"),
    col("PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD"),
    col("PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD")
)

# Transformer (CTransformerStage) after Snapshot => B_CLM_LN
# Stage variables: ProcCdSk = GetFkeyProcCd(...), ClmLnRvnuCdSk = GetFkeyRvnu(...)
# We assume these user-defined functions already exist in the namespace.
df_Transformer = df_Snapshot_forSnapshot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLCL_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    GetFkeyProcCd("FACETS", F.lit(0), col("PROC_CD")[0:5], col("PROC_CD_TYP_CD"), col("PROC_CD_CAT_CD"), lit('N')).alias("PROC_CD_SK"),
    GetFkeyRvnu("FACETS", F.lit(0), col("CLM_LN_RVNU_CD"), lit('N')).alias("CLM_LN_RVNU_CD_SK"),
    col("ALW_AMT").alias("ALW_AMT"),
    col("CHRG_AMT").alias("CHRG_AMT"),
    col("PAYBL_AMT").alias("PAYBL_AMT")
).alias("RowCount")

# B_CLM_LN (CSeqFileStage) - write final
df_B_CLM_LN = df_Transformer.select(
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("PROC_CD_SK"),
    col("CLM_LN_RVNU_CD_SK"),
    col("ALW_AMT"),
    col("CHRG_AMT"),
    col("PAYBL_AMT")
)
write_files(
    df_B_CLM_LN,
    f"{adls_path}/load/B_CLM_LN.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)

# ClmLnPK (CContainerStage)
# Input: PKey => df_Snapshot_forPKey
# Output: Key => next stage
params_ClmLnPK = {
    "CurrRunCycle": RunCycle
}
df_Key = ClmLnPK(df_Snapshot_forPKey, params_ClmLnPK)

# BCBSARxClmLnExtr (CSeqFileStage) - final file write
# Must preserve same columns, rpad for char/varchar with known length
# The container’s output columns have lengths in the JSON where applicable.

df_BCBSARxClmLnExtr = df_Key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_LN_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROC_CD"),
    F.col("SVC_PROV_ID"),
    F.col("CLM_LN_DSALW_EXCD"),
    F.col("CLM_LN_EOB_EXCD"),
    F.rpad(F.col("CLM_LN_FINL_DISP_CD"), 5, " ").alias("CLM_LN_FINL_DISP_CD"),
    F.col("CLM_LN_LOB_CD"),
    F.col("CLM_LN_POS_CD"),
    F.col("CLM_LN_PREAUTH_CD"),
    F.col("CLM_LN_PREAUTH_SRC_CD"),
    F.col("CLM_LN_PRICE_SRC_CD"),
    F.col("CLM_LN_RFRL_CD"),
    F.col("CLM_LN_RVNU_CD"),
    F.rpad(F.col("CLM_LN_ROOM_PRICE_METH_CD"), 2, " ").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.col("CLM_LN_ROOM_TYP_CD"),
    F.col("CLM_LN_TOS_CD"),
    F.col("CLM_LN_UNIT_TYP_CD"),
    F.rpad(F.col("CAP_LN_IN"), 1, " ").alias("CAP_LN_IN"),
    F.rpad(F.col("PRI_LOB_IN"), 1, " ").alias("PRI_LOB_IN"),
    F.col("SVC_END_DT"),
    F.col("SVC_STRT_DT"),
    F.col("AGMNT_PRICE_AMT"),
    F.col("ALW_AMT"),
    F.col("CHRG_AMT"),
    F.col("COINS_AMT"),
    F.col("CNSD_CHRG_AMT"),
    F.col("COPAY_AMT"),
    F.col("DEDCT_AMT"),
    F.col("DSALW_AMT"),
    F.col("ITS_HOME_DSCNT_AMT"),
    F.col("NO_RESP_AMT"),
    F.col("MBR_LIAB_BSS_AMT"),
    F.col("PATN_RESP_AMT"),
    F.col("PAYBL_AMT"),
    F.col("PAYBL_TO_PROV_AMT"),
    F.col("PAYBL_TO_SUB_AMT"),
    F.col("PROC_TBL_PRICE_AMT"),
    F.col("PROFL_PRICE_AMT"),
    F.col("PROV_WRT_OFF_AMT"),
    F.col("RISK_WTHLD_AMT"),
    F.col("SVC_PRICE_AMT"),
    F.col("SUPLMT_DSCNT_AMT"),
    F.col("ALW_PRICE_UNIT_CT"),
    F.col("UNIT_CT"),
    F.rpad(F.col("DEDCT_AMT_ACCUM_ID"), 4, " ").alias("DEDCT_AMT_ACCUM_ID"),
    F.rpad(F.col("PREAUTH_SVC_SEQ_NO"), 4, " ").alias("PREAUTH_SVC_SEQ_NO"),
    F.rpad(F.col("RFRL_SVC_SEQ_NO"), 4, " ").alias("RFRL_SVC_SEQ_NO"),
    F.rpad(F.col("LMT_PFX_ID"), 4, " ").alias("LMT_PFX_ID"),
    F.rpad(F.col("PREAUTH_ID"), 9, " ").alias("PREAUTH_ID"),
    F.rpad(F.col("PROD_CMPNT_DEDCT_PFX_ID"), 4, " ").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.rpad(F.col("PROD_CMPNT_SVC_PAYMT_ID"), 4, " ").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.rpad(F.col("RFRL_ID_TX"), 9, " ").alias("RFRL_ID_TX"),
    F.rpad(F.col("SVC_ID"), 4, " ").alias("SVC_ID"),
    F.rpad(F.col("SVC_PRICE_RULE_ID"), 4, " ").alias("SVC_PRICE_RULE_ID"),
    F.rpad(F.col("SVC_RULE_TYP_TX"), 3, " ").alias("SVC_RULE_TYP_TX"),
    F.rpad(F.col("SVC_LOC_TYP_CD"), 20, " ").alias("SVC_LOC_TYP_CD"),
    F.col("NON_PAR_SAV_AMT"),
    F.col("PROC_CD_TYP_CD"),
    F.col("PROC_CD_CAT_CD"),
    F.col("VBB_RULE_ID"),
    F.col("VBB_EXCD_ID"),
    F.rpad(F.col("CLM_LN_VBB_IN"), 1, " ").alias("CLM_LN_VBB_IN"),
    F.col("ITS_SUPLMT_DSCNT_AMT"),
    F.col("ITS_SRCHRG_AMT"),
    F.col("NDC"),
    F.col("NDC_DRUG_FORM_CD"),
    F.col("NDC_UNIT_CT"),
    F.rpad(F.col("MED_PDX_IND"), 3, " ").alias("MED_PDX_IND"),
    F.col("APC_ID"),
    F.col("APC_STTUS_ID"),
    F.col("SNOMED_CT_CD"),
    F.col("CVX_VCCN_CD")
)

write_files(
    df_BCBSARxClmLnExtr,
    f"{adls_path}/key/BCBSAClmLnExtr.DrugClmLn.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)