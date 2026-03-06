# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2018, 2021, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  EmrProcedureClmExtr
# MAGIC CALLED BY:  EmrProcedureClmExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:  Reads the #PROVIDERNAME#.PROCEDURE.#Timestamp#.TXT  file and puts the data into the claim common record format and takes it through primary key process using Shared Container ClmPkey
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                               	Date                 	Project/Altiris # 	Change Description                                                                   			Development Project      	Code Reviewer	Date Reviewed       
# MAGIC =======================================================================================================================================================================================   
# MAGIC Mrudula Kodali        		2021-02-22                       		Initial Programming                                                                    			IntegrateDev2                    	Jaideep Mankala 	03/04/2021
# MAGIC Lakshmi Devagiri       	2021-05-04                                      	Updated Member query to use Upper case for
# MAGIC                                                                                                         	First Name and Last Name                                                        			IntegrateDev2                         Manasa Andru    	2021-05-10
# MAGIC Vikas A       		2021-05-24                                      	Changed ENR table join from inner to Left in Mbr Lkp               			IntegrateDev2             	Jaideep Mankala    	06/02/2021     
# MAGIC Ken Bradmon		2022-11-14	us542805		Changed transformation of column CLM_TXNMY_CD				IntegrateDev2
# MAGIC Ken Bradmon		2022-12-15	us542805		Added new columns: PROC_CD_CPT, 						IntegrateDev2		Harsha Ravuri	06/14/2023			
# MAGIC 							PROC_CD_CPT_MOD_1, PROC_CD_CPT_2,					IntegrateDev1		
# MAGIC 							PROC_CD_CPTII_MOD2, PROC_CD_CPTII_MOD2, 
# MAGIC 							SNOMED, and CVX.

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
# MAGIC Writing Sequential File to /key
# MAGIC Assign primary surrogate key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, DoubleType
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
PROVIDERNAME = get_widget_value('PROVIDERNAME','')
InFile_F = get_widget_value('InFile_F','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

schema_EmrProecClmLanding = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("POL_NO", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_MID_NM", StringType(), True),
    StructField("MBI", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("GNDR", StringType(), True),
    StructField("PROC_TYPE", StringType(), True),
    StructField("DT_OF_SVC", StringType(), True),
    StructField("RSLT_VAL", StringType(), True),
    StructField("RNDR_NTNL_PROV_ID", StringType(), True),
    StructField("RNDR_PROV_TYP", StringType(), True),
    StructField("PROC_CD_CPT", StringType(), True),
    StructField("PROC_CD_CPT_MOD_1", StringType(), True),
    StructField("PROC_CD_CPT_MOD_2", StringType(), True),
    StructField("PROC_CD_CPTII_MOD_1", StringType(), True),
    StructField("PROC_CD_CPTII_MOD_2", StringType(), True),
    StructField("SNOMED", StringType(), True),
    StructField("CVX", StringType(), True),
    StructField("SOURCE_ID", StringType(), True),
    StructField("MBR_UNIQ_KEY", StringType(), True),
    StructField("CLM_LN_SEQ", StringType(), True)
])

df_EmrProc = (
    spark.read.csv(
        path=f"{adls_path}/verified/{InFile_F}",
        schema=schema_EmrProecClmLanding,
        sep=",",
        quote='',
        header=False
    )
)

query_alpha = f"""SELECT 
SUB.SUB_ID,
UPPER(MBR.LAST_NM) AS LAST_NM,
UPPER(MBR.FIRST_NM) AS FIRST_NM,
REPLACE(MBR.BRTH_DT_SK, '-' ,'') AS BRTH_DT_SK,
ALPHA.ALPHA_PFX_CD AS ALPHA_PFX_CD,
CLS.CLS_ID AS CLS_ID,
GRP.GRP_ID AS GRP_ID,
SUB.SUB_UNIQ_KEY as SUB_UNIQ_KEY,
MBR.MBR_UNIQ_KEY AS MBR_ID,
SUBGRP.SUBGRP_ID AS SUBGRP_ID,
MBR.MBR_SFX_NO AS MBR_SFX_NO,
PLN.CLS_PLN_ID AS CLS_PLN_ID,
PROD.PROD_ID AS PROD_ID 
FROM {IDSOwner}.GRP GRP 
INNER JOIN {IDSOwner}.SUB SUB ON GRP.GRP_SK = SUB.GRP_SK
INNER JOIN {IDSOwner}.MBR MBR ON SUB.SUB_SK=MBR.SUB_SK
INNER JOIN {IDSOwner}.SUBGRP SUBGRP ON MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
LEFT OUTER JOIN {IDSOwner}.MBR_ENR ENR ON MBR.MBR_SK= ENR.MBR_SK 
INNER JOIN {IDSOwner}.CD_MPPNG CD ON ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD.CD_MPPNG_SK 
LEFT OUTER JOIN {IDSOwner}.CLS CLS ON ENR.CLS_SK = CLS.CLS_SK 
LEFT OUTER JOIN {IDSOwner}.CLS_PLN PLN ON ENR.CLS_PLN_SK = PLN.CLS_PLN_SK
LEFT OUTER JOIN {IDSOwner}.PROD PROD ON PROD.PROD_SK = ENR.PROD_SK
LEFT OUTER JOIN {IDSOwner}.ALPHA_PFX ALPHA ON SUB.ALPHA_PFX_SK=ALPHA.ALPHA_PFX_SK
WHERE GRP.GRP_SK=ENR.GRP_SK
AND MBR.MBR_UNIQ_KEY = ENR.MBR_UNIQ_KEY
AND MBR.SRC_SYS_CD_SK = ENR.SRC_SYS_CD_SK
AND ENR.ELIG_IN ='Y'
AND CD.SRC_CD = 'M'
AND CD.SRC_DOMAIN_NM = 'CLASS PLAN PRODUCT CATEGORY'
ORDER BY SUB.SUB_ID, ENR.EFF_DT_SK, ENR.TERM_DT_SK
"""

df_IDS_MBR_SUB_alpha = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", query_alpha)
        .load()
)

query_prov = f"""SELECT 
NTNL_PROV_ID,
PROV_SPEC.PROV_SPEC_CD AS PROV_SPEC_CD,
PROV_TYP.PROV_TYP_CD AS PROV_TYP_CD
FROM {IDSOwner}.PROV PROV
LEFT OUTER JOIN {IDSOwner}.PROV_SPEC_CD PROV_SPEC 
ON PROV.PROV_SPEC_CD_SK = PROV_SPEC.PROV_SPEC_CD_SK
LEFT OUTER JOIN {IDSOwner}.PROV_TYP_CD PROV_TYP 
ON PROV.PROV_TYP_CD_SK = PROV_TYP.PROV_TYP_CD_SK
WHERE (NTNL_PROV_ID, ABS(PROV_SK)) IN (
  SELECT NTNL_PROV_ID, PROV_SK FROM 
    (SELECT NTNL_PROV_ID, ABS(PROV_SK) PROV_SK,
            ROW_NUMBER() OVER (PARTITION BY NTNL_PROV_ID ORDER BY ABS(PROV_SK) DESC) AS NUM
     FROM {IDSOwner}.PROV
    ) A
  WHERE A.NUM=1
)
AND NTNL_PROV_ID IS NOT NULL
AND LENGTH(TRIM(NTNL_PROV_ID)) > 1
"""

df_IDS_MBR_SUB_prov = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", query_prov)
        .load()
)

df_lnk_prov = dedup_sort(df_IDS_MBR_SUB_prov, ["NTNL_PROV_ID"], [])
df_lnk_alpha = dedup_sort(df_IDS_MBR_SUB_alpha, ["SUB_ID","LAST_NM","FIRST_NM","BRTH_DT_SK"], [])

df_TransformLogics = (
    df_EmrProc.alias("EmrProc")
    .join(df_lnk_prov.alias("lnk_prov"), F.col("EmrProc.RNDR_NTNL_PROV_ID")==F.col("lnk_prov.NTNL_PROV_ID"), "left")
    .join(
        df_lnk_alpha.alias("lnk_alpha"),
        (
            (F.col("EmrProc.POL_NO")==F.col("lnk_alpha.SUB_ID")) &
            (F.col("EmrProc.PATN_LAST_NM")==F.col("lnk_alpha.LAST_NM")) &
            (F.col("EmrProc.PATN_FIRST_NM")==F.col("lnk_alpha.FIRST_NM")) &
            (F.col("EmrProc.DOB")==F.col("lnk_alpha.BRTH_DT_SK"))
        ),
        "left"
    )
)

df_lnk_trx_out = df_TransformLogics.select(
    F.lit(0).alias("CLM_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("EmrProc.CLM_ID").alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("NA").alias("ADJ_FROM_CLM"),
    F.lit("NA").alias("ADJ_TO_CLM"),
    F.col("lnk_alpha.ALPHA_PFX_CD").alias("ALPHA_PFX_SK"),
    F.lit("NA").alias("CLM_EOB_EXCD"),
    F.col("lnk_alpha.CLS_ID").alias("CLS_SK"),
    F.col("lnk_alpha.CLS_PLN_ID").alias("CLS_PLN_SK"),
    F.lit("NA").alias("EXPRNC_CAT"),
    F.lit("NA").alias("FNCL_LOB"),
    F.col("lnk_alpha.GRP_ID").alias("GRP_SK"),
    F.col("lnk_alpha.MBR_ID").alias("MBR_SK"),
    F.lit("NA").alias("NTWK"),
    F.col("lnk_alpha.PROD_ID").alias("PROD_SK"),
    F.col("lnk_alpha.SUBGRP_ID").alias("SUBGRP_SK"),
    F.col("lnk_alpha.SUB_UNIQ_KEY").alias("SUB_SK"),
    F.lit("NA").alias("CLM_ACDNT_CD"),
    F.lit("NA").alias("CLM_ACDNT_ST_CD"),
    F.lit("NA").alias("CLM_ACTV_BCBS_PLN_CD"),
    F.lit("NA").alias("CLM_AGMNT_SRC_CD"),
    F.lit("NA").alias("CLM_BTCH_ACTN_CD"),
    F.lit("N").alias("CLM_CAP_CD_SK"),
    F.lit("PSEUDO").alias("CLM_CAT_CD_SK"),
    F.lit("NA").alias("CLM_CHK_CYC_OVRD_CD"),
    F.lit("NA").alias("CLM_COB_CD"),
    F.lit("NA").alias("CLM_FINL_DISP_CD"),
    F.lit("NA").alias("CLM_INPT_METH_CD"),
    F.lit("NA").alias("CLM_INPT_SRC_CD"),
    F.lit("NA").alias("CLM_INTER_PLN_PGM_CD"),
    F.lit("NA").alias("CLM_NTWK_STTUS_CD"),
    F.lit("NA").alias("CLM_NONPAR_PROV_PFX_CD"),
    F.lit("NA").alias("CLM_OTHR_BNF_CD"),
    F.lit("NA").alias("CLM_PAYE_CD"),
    F.lit("NA").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.lit("NA").alias("CLM_SVC_DEFN_PFX_CD"),
    F.col("lnk_prov.PROV_SPEC_CD").alias("CLM_SVC_PROV_SPEC_CD_SK"),
    F.col("lnk_prov.PROV_TYP_CD").alias("CLM_SVC_PROV_TYP_CD_SK"),
    F.lit("A02").alias("CLM_STTUS_CD_SK"),
    F.lit("240").alias("CLM_SUBMTTING_BCBS_PLN_CD_SK"),
    F.lit("240").alias("CLM_SUB_BCBS_PLN_CD_SK"),
    F.lit("PR").alias("CLM_SUBTYP_CD_SK"),
    F.lit("MED").alias("CLM_TYP_CD_SK"),
    F.lit("N").alias("ATCHMT_IN"),
    F.lit("N").alias("CLNCL_EDIT_IN"),
    F.lit("N").alias("COBRA_CLM_IN"),
    F.lit("N").alias("FIRST_PASS_IN"),
    F.lit("N").alias("HOST_IN"),
    F.lit("N").alias("LTR_IN"),
    F.lit("N").alias("MCARE_ASG_IN"),
    F.lit("N").alias("NOTE_IN"),
    F.lit("N").alias("PCA_AUDIT_IN"),
    F.lit("N").alias("PCP_SUBMT_IN"),
    F.lit("N").alias("PROD_OOA_IN"),
    F.lit("1753-01-01").alias("ACDNT_DT_SK"),
    F.concat_ws("-", F.col("EmrProc.DT_OF_SVC").substr(1,4), F.col("EmrProc.DT_OF_SVC").substr(5,2), F.col("EmrProc.DT_OF_SVC").substr(7,2)).alias("INPT_DT_SK"),
    F.lit("1753-01-01").alias("MBR_PLN_ELIG_DT_SK"),
    F.lit("1753-01-01").alias("NEXT_RVW_DT_SK"),
    F.lit("1753-01-01").alias("PD_DT_SK"),
    F.lit("1753-01-01").alias("PAYMT_DRAG_CYC_DT_SK"),
    F.lit("1753-01-01").alias("PRCS_DT_SK"),
    F.lit("1753-01-01").alias("RCVD_DT_SK"),
    F.concat_ws("-", F.col("EmrProc.DT_OF_SVC").substr(1,4), F.col("EmrProc.DT_OF_SVC").substr(5,2), F.col("EmrProc.DT_OF_SVC").substr(7,2)).alias("SVC_STRT_DT_SK"),
    F.concat_ws("-", F.col("EmrProc.DT_OF_SVC").substr(1,4), F.col("EmrProc.DT_OF_SVC").substr(5,2), F.col("EmrProc.DT_OF_SVC").substr(7,2)).alias("SVC_END_DT_SK"),
    F.lit("1753-01-01").alias("SMLR_ILNS_DT_SK"),
    F.concat_ws("-", F.col("EmrProc.DT_OF_SVC").substr(1,4), F.col("EmrProc.DT_OF_SVC").substr(5,2), F.col("EmrProc.DT_OF_SVC").substr(7,2)).alias("STTUS_DT_SK"),
    F.lit("1753-01-01").alias("WORK_UNABLE_BEG_DT_SK"),
    F.lit("2199-12-31").alias("WORK_UNABLE_END_DT_SK"),
    F.lit("0.00").alias("ACDNT_AMT"),
    F.lit("0.00").alias("ACTL_PD_AMT"),
    F.lit("0.00").alias("ALW_AMT"),
    F.lit("0.00").alias("DSALW_AMT"),
    F.lit("0.00").alias("COINS_AMT"),
    F.lit("0.00").alias("CNSD_CHRG_AMT"),
    F.lit("0.00").alias("COPAY_AMT"),
    F.lit("0.00").alias("CHRG_AMT"),
    F.lit("0.00").alias("DEDCT_AMT"),
    F.lit("0.00").alias("PAYBL_AMT"),
    F.lit(1).alias("CLM_CT"),
    AGE(
      FORMAT.DATE(F.col("EmrProc.DOB"), "CHAR", "CCYYMMDD", "CCYY-MM-DD"),
      FORMAT.DATE(F.col("EmrProc.DT_OF_SVC"), "CHAR", "CCYYMMDD", "CCYY-MM-DD")
    ).alias("MBR_AGE"),
    F.lit("NA").alias("ADJ_FROM_CLM_ID"),
    F.lit("NA").alias("ADJ_TO_CLM_ID"),
    F.lit("NA").alias("DOC_TX_ID"),
    F.lit("1").alias("MCAID_RESUB_NO"),
    F.when(
        F.col("EmrProc.MBI").isNull() | (trim(F.col("EmrProc.MBI")) == ''),
        'NA'
    ).otherwise(F.col("EmrProc.MBI")).alias("MCARE_ID"),
    F.col("lnk_alpha.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.lit("1").alias("PATN_ACCT_NO"),
    F.lit("NA").alias("PAYMT_REF_ID"),
    F.lit("NA").alias("PROV_AGMNT_ID"),
    F.lit("NA").alias("RFRNG_PROV_TX"),
    F.col("EmrProc.POL_NO").alias("SUB_ID"),
    F.lit("NA").alias("PCA_TYP_CD"),
    F.lit("NA").alias("REL_PCA_CLM"),
    F.lit("NA").alias("REL_BASE_CLM"),
    F.lit("0.00").alias("REMIT_SUPRSION_AMT"),
    F.lit("NA").alias("MCAID_STTUS_ID"),
    F.lit("0.00").alias("PATN_PD_AMT"),
    F.when(
        F.col("EmrProc.DT_OF_SVC") < '20151001',
        'ICD9'
    ).when(
        F.col("EmrProc.DT_OF_SVC") >= '20151001',
        'ICD10'
    ).otherwise('NA').alias("CLM_SUBMT_ICD_VRSN_CD_SK"),
    F.lit("N").alias("BILL_PAYMT_EXCL_IN"),
    F.when(
        F.col("EmrProc.RNDR_PROV_TYP").isNull() | (trim(F.col("EmrProc.RNDR_PROV_TYP"))==''),
        ''
    ).otherwise(
        F.upper(trim(F.col("EmrProc.RNDR_PROV_TYP")))
    ).alias("CLM_TXNMY_CD")
)

df_Snapshot = df_lnk_trx_out.select(
    F.col("CLM_ID").cast("char(12)").alias("CLM_ID"),
    F.col("MBR_SK").alias("MBR_CK"),
    F.col("GRP_SK").cast("char(8)").alias("GRP"),
    F.col("SVC_STRT_DT_SK").alias("SVC_STRT_DT"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("EXPRNC_CAT").cast("char(4)").alias("EXPRNC_CAT"),
    F.col("FNCL_LOB").cast("char(4)").alias("FNCL_LOB_NO"),
    F.col("CLM_CT").alias("CLM_CT"),
    F.col("PCA_TYP_CD").cast("char(18)").alias("PCA_TYP_CD"),
    F.col("CLM_STTUS_CD_SK").cast("char(2)").alias("CLM_STTUS_CD"),
    F.col("CLM_CAT_CD_SK").cast("char(10)").alias("CLM_CAT_CD")
)

df_Pkey = df_lnk_trx_out.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("|").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CLM_ID")).alias("PRI_KEY_STRING"),
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").cast("char(18)").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ADJ_FROM_CLM").cast("char(12)").alias("ADJ_FROM_CLM"),
    F.col("ADJ_TO_CLM").cast("char(12)").alias("ADJ_TO_CLM"),
    F.col("ALPHA_PFX_SK").cast("char(3)").alias("ALPHA_PFX_CD"),
    F.col("CLM_EOB_EXCD").cast("char(3)").alias("CLM_EOB_EXCD"),
    F.col("CLS_SK").cast("char(4)").alias("CLS"),
    F.col("CLS_PLN_SK").cast("char(8)").alias("CLS_PLN"),
    F.col("EXPRNC_CAT").cast("char(4)").alias("EXPRNC_CAT"),
    F.col("FNCL_LOB").cast("char(4)").alias("FNCL_LOB_NO"),
    F.col("GRP_SK").cast("char(8)").alias("GRP"),
    F.col("MBR_SK").alias("MBR_CK"),
    F.col("NTWK").cast("char(12)").alias("NTWK"),
    F.col("PROD_SK").cast("char(8)").alias("PROD"),
    F.col("SUBGRP_SK").cast("char(4)").alias("SUBGRP"),
    F.col("SUB_SK").alias("SUB_CK"),
    F.col("CLM_ACDNT_CD").cast("char(10)").alias("CLM_ACDNT_CD"),
    F.col("CLM_ACDNT_ST_CD").cast("char(2)").alias("CLM_ACDNT_ST_CD"),
    F.col("CLM_ACTV_BCBS_PLN_CD").cast("char(10)").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
    F.col("CLM_AGMNT_SRC_CD").cast("char(10)").alias("CLM_AGMNT_SRC_CD"),
    F.col("CLM_BTCH_ACTN_CD").cast("char(1)").alias("CLM_BTCH_ACTN_CD"),
    F.col("CLM_CAP_CD_SK").cast("char(10)").alias("CLM_CAP_CD"),
    F.col("CLM_CAT_CD_SK").cast("char(10)").alias("CLM_CAT_CD"),
    F.col("CLM_CHK_CYC_OVRD_CD").cast("char(1)").alias("CLM_CHK_CYC_OVRD_CD"),
    F.col("CLM_COB_CD").cast("char(1)").alias("CLM_COB_CD"),
    F.col("CLM_FINL_DISP_CD").alias("FINL_DISP_CD"),
    F.col("CLM_INPT_METH_CD").cast("char(1)").alias("CLM_INPT_METH_CD"),
    F.col("CLM_INPT_SRC_CD").cast("char(10)").alias("CLM_INPT_SRC_CD"),
    F.col("CLM_INTER_PLN_PGM_CD").cast("char(10)").alias("CLM_IPP_CD"),
    F.col("CLM_NTWK_STTUS_CD").cast("char(2)").alias("CLM_NTWK_STTUS_CD"),
    F.col("CLM_NONPAR_PROV_PFX_CD").cast("char(4)").alias("CLM_NONPAR_PROV_PFX_CD"),
    F.col("CLM_OTHR_BNF_CD").cast("char(1)").alias("CLM_OTHER_BNF_CD"),
    F.col("CLM_PAYE_CD").cast("char(1)").alias("CLM_PAYE_CD"),
    F.col("CLM_PRCS_CTL_AGNT_PFX_CD").cast("char(4)").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.col("CLM_SVC_DEFN_PFX_CD").cast("char(4)").alias("CLM_SVC_DEFN_PFX_CD"),
    F.col("CLM_SVC_PROV_SPEC_CD_SK").cast("char(10)").alias("CLM_SVC_PROV_SPEC_CD"),
    F.col("CLM_SVC_PROV_TYP_CD_SK").cast("char(10)").alias("CLM_SVC_PROV_TYP_CD"),
    F.col("CLM_STTUS_CD_SK").cast("char(2)").alias("CLM_STTUS_CD"),
    F.col("CLM_SUBMTTING_BCBS_PLN_CD_SK").cast("char(10)").alias("CLM_SUBMT_BCBS_PLN_CD"),
    F.col("CLM_SUB_BCBS_PLN_CD_SK").cast("char(10)").alias("CLM_SUB_BCBS_PLN_CD"),
    F.col("CLM_SUBTYP_CD_SK").cast("char(10)").alias("CLM_SUBTYP_CD"),
    F.col("CLM_TYP_CD_SK").cast("char(1)").alias("CLM_TYP_CD"),
    F.col("ATCHMT_IN").cast("char(1)").alias("ATCHMT_IN"),
    F.col("CLNCL_EDIT_IN").cast("char(1)").alias("CLM_CLNCL_EDIT_CD"),
    F.col("COBRA_CLM_IN").cast("char(1)").alias("COBRA_CLM_IN"),
    F.col("FIRST_PASS_IN").cast("char(1)").alias("FIRST_PASS_IN"),
    F.col("HOST_IN").cast("char(1)").alias("HOST_IN"),
    F.col("LTR_IN").cast("char(1)").alias("LTR_IN"),
    F.col("MCARE_ASG_IN").cast("char(1)").alias("MCARE_ASG_IN"),
    F.col("NOTE_IN").cast("char(1)").alias("NOTE_IN"),
    F.col("PCA_AUDIT_IN").cast("char(1)").alias("PCA_AUDIT_IN"),
    F.col("PCP_SUBMT_IN").cast("char(1)").alias("PCP_SUBMT_IN"),
    F.col("PROD_OOA_IN").cast("char(1)").alias("PROD_OOA_IN"),
    F.col("ACDNT_DT_SK").cast("char(10)").alias("ACDNT_DT"),
    F.col("INPT_DT_SK").cast("char(10)").alias("INPT_DT"),
    F.col("MBR_PLN_ELIG_DT_SK").cast("char(10)").alias("MBR_PLN_ELIG_DT"),
    F.col("NEXT_RVW_DT_SK").cast("char(10)").alias("NEXT_RVW_DT"),
    F.col("PD_DT_SK").cast("char(10)").alias("PD_DT"),
    F.col("PAYMT_DRAG_CYC_DT_SK").cast("char(10)").alias("PAYMT_DRAG_CYC_DT"),
    F.col("PRCS_DT_SK").cast("char(10)").alias("PRCS_DT"),
    F.col("RCVD_DT_SK").cast("char(10)").alias("RCVD_DT"),
    F.col("SVC_STRT_DT_SK").cast("char(10)").alias("SVC_STRT_DT"),
    F.col("SVC_END_DT_SK").cast("char(10)").alias("SVC_END_DT"),
    F.col("SMLR_ILNS_DT_SK").cast("char(10)").alias("SMLR_ILNS_DT"),
    F.col("STTUS_DT_SK").cast("char(10)").alias("STTUS_DT"),
    F.col("WORK_UNABLE_BEG_DT_SK").cast("char(10)").alias("WORK_UNABLE_BEG_DT"),
    F.col("WORK_UNABLE_END_DT_SK").cast("char(10)").alias("WORK_UNABLE_END_DT"),
    F.col("ACDNT_AMT").alias("ACDNT_AMT"),
    F.col("ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    F.col("ALW_AMT").alias("ALLOW_AMT"),
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
    F.col("DOC_TX_ID").cast("char(18)").alias("DOC_TX_ID"),
    F.col("MCAID_RESUB_NO").cast("char(15)").alias("MCAID_RESUB_NO"),
    F.col("MCARE_ID").cast("char(12)").alias("MCARE_ID"),
    F.col("MBR_SFX_NO").cast("char(2)").alias("MBR_SFX_NO"),
    F.col("PATN_ACCT_NO").alias("PATN_ACCT_NO"),
    F.col("PAYMT_REF_ID").cast("char(16)").alias("PAYMT_REF_ID"),
    F.col("PROV_AGMNT_ID").cast("char(12)").alias("PROV_AGMNT_ID"),
    F.col("RFRNG_PROV_TX").alias("RFRNG_PROV_TX"),
    F.col("SUB_ID").cast("char(14)").alias("SUB_ID"),
    F.lit("").cast("char(1)").alias("PRPR_ENTITY"),
    F.col("PCA_TYP_CD").cast("char(18)").alias("PCA_TYP_CD"),
    F.col("REL_PCA_CLM").alias("REL_PCA_CLM_ID"),
    F.lit("NA").alias("CLCL_MICRO_ID"),
    F.lit("N").cast("char(1)").alias("CLM_UPDT_SW"),
    F.col("REMIT_SUPRSION_AMT").alias("REMIT_SUPRSION_AMT"),
    F.col("MCAID_STTUS_ID").alias("MCAID_STTUS_ID"),
    F.col("PATN_PD_AMT").alias("PATN_PD_AMT"),
    F.col("CLM_SUBMT_ICD_VRSN_CD_SK").alias("CLM_SUBMT_ICD_VRSN_CD"),
    F.col("CLM_TXNMY_CD").alias("CLM_TXNMY_CD"),
    F.col("BILL_PAYMT_EXCL_IN").cast("char(1)").alias("BILL_PAYMT_EXCL_IN")
)

params_ClmPK = {
    "CurrRunCycle": RunCycle
}
df_Key = ClmPK(df_Pkey, params_ClmPK)

# Prepare final output for EmrProcedureClmExtr (same column order as the link from ClmPK)
cols_EmrProcedureClmExtr = [
    ("JOB_EXCTN_RCRD_ERR_SK", None, None),
    ("INSRT_UPDT_CD", "char", 10),
    ("DISCARD_IN", "char", 1),
    ("PASS_THRU_IN", "char", 1),
    ("FIRST_RECYC_DT", None, None),
    ("ERR_CT", None, None),
    ("RECYCLE_CT", None, None),
    ("SRC_SYS_CD", None, None),
    ("PRI_KEY_STRING", None, None),
    ("CLM_SK", None, None),
    ("SRC_SYS_CD_SK", None, None),
    ("CLM_ID", "char", 18),
    ("CRT_RUN_CYC_EXCTN_SK", None, None),
    ("LAST_UPDT_RUN_CYC_EXCTN_SK", None, None),
    ("ADJ_FROM_CLM", "char", 12),
    ("ADJ_TO_CLM", "char", 12),
    ("ALPHA_PFX_CD", "char", 3),
    ("CLM_EOB_EXCD", "char", 3),
    ("CLS", "char", 4),
    ("CLS_PLN", "char", 8),
    ("EXPRNC_CAT", "char", 4),
    ("FNCL_LOB_NO", "char", 4),
    ("GRP", "char", 8),
    ("MBR_CK", None, None),
    ("NTWK", "char", 12),
    ("PROD", "char", 8),
    ("SUBGRP", "char", 4),
    ("SUB_CK", None, None),
    ("CLM_ACDNT_CD", "char", 10),
    ("CLM_ACDNT_ST_CD", "char", 2),
    ("CLM_ACTIVATING_BCBS_PLN_CD", "char", 10),
    ("CLM_AGMNT_SRC_CD", "char", 10),
    ("CLM_BTCH_ACTN_CD", "char", 1),
    ("CLM_CAP_CD", "char", 10),
    ("CLM_CAT_CD", "char", 10),
    ("CLM_CHK_CYC_OVRD_CD", "char", 1),
    ("CLM_COB_CD", "char", 1),
    ("FINL_DISP_CD", None, None),
    ("CLM_INPT_METH_CD", "char", 1),
    ("CLM_INPT_SRC_CD", "char", 10),
    ("CLM_IPP_CD", "char", 10),
    ("CLM_NTWK_STTUS_CD", "char", 2),
    ("CLM_NONPAR_PROV_PFX_CD", "char", 4),
    ("CLM_OTHER_BNF_CD", "char", 1),
    ("CLM_PAYE_CD", "char", 1),
    ("CLM_PRCS_CTL_AGNT_PFX_CD", "char", 4),
    ("CLM_SVC_DEFN_PFX_CD", "char", 4),
    ("CLM_SVC_PROV_SPEC_CD", "char", 10),
    ("CLM_SVC_PROV_TYP_CD", "char", 10),
    ("CLM_STTUS_CD", "char", 2),
    ("CLM_SUBMT_BCBS_PLN_CD", "char", 10),
    ("CLM_SUB_BCBS_PLN_CD", "char", 10),
    ("CLM_SUBTYP_CD", "char", 10),
    ("CLM_TYP_CD", "char", 1),
    ("ATCHMT_IN", "char", 1),
    ("CLM_CLNCL_EDIT_CD", "char", 1),
    ("COBRA_CLM_IN", "char", 1),
    ("FIRST_PASS_IN", "char", 1),
    ("HOST_IN", "char", 1),
    ("LTR_IN", "char", 1),
    ("MCARE_ASG_IN", "char", 1),
    ("NOTE_IN", "char", 1),
    ("PCA_AUDIT_IN", "char", 1),
    ("PCP_SUBMT_IN", "char", 1),
    ("PROD_OOA_IN", "char", 1),
    ("ACDNT_DT", "char", 10),
    ("INPT_DT", "char", 10),
    ("MBR_PLN_ELIG_DT", "char", 10),
    ("NEXT_RVW_DT", "char", 10),
    ("PD_DT", "char", 10),
    ("PAYMT_DRAG_CYC_DT", "char", 10),
    ("PRCS_DT", "char", 10),
    ("RCVD_DT", "char", 10),
    ("SVC_STRT_DT", "char", 10),
    ("SVC_END_DT", "char", 10),
    ("SMLR_ILNS_DT", "char", 10),
    ("STTUS_DT", "char", 10),
    ("WORK_UNABLE_BEG_DT", "char", 10),
    ("WORK_UNABLE_END_DT", "char", 10),
    ("ACDNT_AMT", None, None),
    ("ACTL_PD_AMT", None, None),
    ("ALLOW_AMT", None, None),
    ("DSALW_AMT", None, None),
    ("COINS_AMT", None, None),
    ("CNSD_CHRG_AMT", None, None),
    ("COPAY_AMT", None, None),
    ("CHRG_AMT", None, None),
    ("DEDCT_AMT", None, None),
    ("PAYBL_AMT", None, None),
    ("CLM_CT", None, None),
    ("MBR_AGE", None, None),
    ("ADJ_FROM_CLM_ID", None, None),
    ("ADJ_TO_CLM_ID", None, None),
    ("DOC_TX_ID", "char", 18),
    ("MCAID_RESUB_NO", "char", 15),
    ("MCARE_ID", "char", 12),
    ("MBR_SFX_NO", "char", 2),
    ("PATN_ACCT_NO", None, None),
    ("PAYMT_REF_ID", "char", 16),
    ("PROV_AGMNT_ID", "char", 12),
    ("RFRNG_PROV_TX", None, None),
    ("SUB_ID", "char", 14),
    ("PRPR_ENTITY", "char", 1),
    ("PCA_TYP_CD", "char", 18),
    ("REL_PCA_CLM_ID", None, None),
    ("CLCL_MICRO_ID", None, None),
    ("CLM_UPDT_SW", "char", 1),
    ("REMIT_SUPRSION_AMT", None, None),
    ("MCAID_STTUS_ID", None, None),
    ("PATN_PD_AMT", None, None),
    ("CLM_SUBMT_ICD_VRSN_CD", None, None),
    ("CLM_TXNMY_CD", None, None),
    ("BILL_PAYMT_EXCL_IN", "char", 1)
]

def rpad_columns(df_in, columns_defs):
    df_out = df_in
    for (c, typ, length) in columns_defs:
        if typ in ["char", "varchar"] and length is not None:
            df_out = df_out.withColumn(c, F.rpad(F.col(c), length, " "))
    return df_out.select([c[0] for c in columns_defs])

df_Key_padded = rpad_columns(df_Key, cols_EmrProcedureClmExtr)
write_files(
    df_Key_padded,
    f"{adls_path}/key/EmrProcedureClmExtr_{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

stageVar_ExpCatCdSk = "GetFkeyExprncCat('FACETS', 0, Snapshot.EXPRNC_CAT, 'N')"
stageVar_GrpSk = "GetFkeyGrp('FACETS', 0, Snapshot.GRP, 'N')"
stageVar_MbrSk = "GetFkeyMbr('FACETS', 0, Snapshot.MBR_CK, 'N')"
stageVar_FnclLobSk = "GetFkeyFnclLob(\"PSI\", 0, Snapshot.FNCL_LOB_NO, 'N')"
stageVar_PcaTypCdSk = "GetFkeyCodes('FACETS', 0, \"PERSONAL CARE ACCOUNT PROCESSING\", Snapshot.PCA_TYP_CD, 'N')"
stageVar_ClmSttusCdSk = "GetFkeyCodes(SrcSysCd, 0, \"CLAIM STATUS\", Snapshot.CLM_STTUS_CD, 'N')"
stageVar_ClmCatCdSk = "GetFkeyCodes(SrcSysCd, 0, \"CLAIM CATEGORY\", Snapshot.CLM_CAT_CD, 'X')"

df_xfrn_B_CLM = df_Snapshot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.expr(stageVar_ClmSttusCdSk).alias("CLM_STTUS_CD_SK"),
    F.expr(stageVar_ClmCatCdSk).alias("CLM_CAT_CD_SK"),
    F.expr(stageVar_ExpCatCdSk).alias("EXPRNC_CAT_SK"),
    F.expr(stageVar_FnclLobSk).alias("FNCL_LOB_SK"),
    F.expr(stageVar_GrpSk).alias("GRP_SK"),
    F.expr(stageVar_MbrSk).alias("MBR_SK"),
    F.col("SVC_STRT_DT").cast("char(10)").alias("SVC_STRT_DT_SK"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("CLM_CT").alias("CLM_CT"),
    F.expr(stageVar_PcaTypCdSk).alias("PCA_TYP_CD_SK")
)

cols_B_CLM = [
    ("SRC_SYS_CD_SK", None, None),
    ("CLM_ID", "char", 12),
    ("CLM_STTUS_CD_SK", None, None),
    ("CLM_CAT_CD_SK", None, None),
    ("EXPRNC_CAT_SK", None, None),
    ("FNCL_LOB_SK", None, None),
    ("GRP_SK", None, None),
    ("MBR_SK", None, None),
    ("SVC_STRT_DT_SK", "char", 10),
    ("CHRG_AMT", None, None),
    ("PAYBL_AMT", None, None),
    ("CLM_CT", None, None),
    ("PCA_TYP_CD_SK", None, None)
]

df_B_CLM_padded = rpad_columns(df_xfrn_B_CLM, cols_B_CLM)
write_files(
    df_B_CLM_padded,
    f"{adls_path}/load/B_CLM.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)