# Databricks notebook source
# MAGIC %md
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC
# MAGIC CALLED BY:  BCADrugExtrSeq
# MAGIC
# MAGIC
# MAGIC DESCRIPTION:  Reads the BCADrugClm_Land.dat file and puts the data into the claim common record format and takes it through primary key process using Shared Container ClmPkey
# MAGIC     
# MAGIC     
# MAGIC
# MAGIC MODIFICATIONS:
# MAGIC
# MAGIC Developer                          Date                 Project/Altiris #            Change Description                                                                   Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                     --------------------     ------------------------             -----------------------------------------------------------------------------------------          --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam                 2013-10-29        5056 FEP Claims       Initial Programming                                                                    IntegrateNewDevl           Bhoomi Dasari          11/30/2013
# MAGIC
# MAGIC Manasa Andru                  2015-01-14         TFS - 9791             Changed the Source System code for ClmSttusCdSk and         IntegrateNewDevl             Kalyan Neelam          2015-01-16
# MAGIC                                                                                                    ClmCatCdSk in the GetFkeyCodes routine
# MAGIC Abhiram Dasarathy	         2016-11-07      5568 - HEDIS	   Changed the business rules based on the mapping	         IntegrateDev2	               Kalyan Neelam          2016-11-15
# MAGIC 						   changes
# MAGIC Abhiram Dasarathy	         2017-02-03      5568 - HEDIS	   Modified the PD_DT_SK field logic			         IntegrateDev1	                Kalyan Neelam          2017-02-07	 
# MAGIC
# MAGIC Saikiran Mahenderker      2018-03-15        5781 HEDIS            Implemented Logic for ADJ_CD to map to CLM_STTUS_CD         IntegrateDev2	Jaideep Mankala       04/04/2018
# MAGIC                                                                                                    for Generating CLM_STTUS_CD_SK          
# MAGIC
# MAGIC Mohan Karnati                 2019-06-06        ADO-73034             Adding CLM_TXNMY_CD filed in alpha_pfx stage and                 IntegrateDev1	                Kalyan Neelam          2019-07-01
# MAGIC                                                                                                    passing it till BCAClmExtr stage  
# MAGIC
# MAGIC Karthik Chintalapani         2019-08-27        5884       Added new columns to BCAClmLand  file stage                                            IntegrateDev1	                Kalyan Neelam          2019-09-05      
# MAGIC                                                                                  CLM_STTUS_CD,ADJ_FROM_CLM_ID,ADJ_TO_CLM_ID     
# MAGIC                                                                                  for implementing reversals logic
# MAGIC Giri  Mallavaram              2020-04-06        6131  PBM Changes       Added BILL_PAYMT_EXCL_IN to be in sync with changes to CLM_PKey Container        IntegrateDev2              Kalyan Neelam
# MAGIC
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
# MAGIC
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Lookup subscriber, product and member information
# MAGIC Writing Sequential File to /key
# MAGIC Read the landing file created in BCADrugClmPreprocExtr
# MAGIC Assign primary surrogate key
# MAGIC Apply business logic
# MAGIC """

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date

# COMMAND ----------

# MAGIC %run ../../../../../Routine_Functions

# COMMAND ----------

# MAGIC %run ../../../../../Utility_Integrate

# COMMAND ----------

# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)

# COMMAND ----------

# MAGIC
# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmPK

# COMMAND ----------

# # Retrieve parameter values
# CurrentDate = get_widget_value("CurrentDate", "")
# SrcSysCd = get_widget_value("SrcSysCd", "")
# SrcSysCdSk = get_widget_value("SrcSysCdSk", "")
# RunCycle = get_widget_value("RunCycle", "1")
# RunID = get_widget_value("RunID", "100")
# IDSOwner = get_widget_value("IDSOwner", "")
# ids_secret_name = get_widget_value("ids_secret_name", "")


# Retrieve parameter values
CurrentDate = "2025-09-03"
SrcSysCd = "MCSOURCE"
SrcSysCdSk = "171910129"
RunCycle = "100"
RunID = "1"
IDSOwner = "devl"
ids_secret_name = "azuresql-ids-connection-string"


# COMMAND ----------

GetFkeyExprncCat_owner = IDSOwner
GetFkeyExprncCat_secret = ids_secret_name
GetFkeyGrp_owner =  IDSOwner
GetFkeyGrp_secret = ids_secret_name
GetFkeyMbr_owner =  IDSOwner
GetFkeyMbr_secret = ids_secret_name
GetFkeyFnclLob_owner =  IDSOwner
GetFkeyFnclLob_secret = ids_secret_name
GetFkeyCodes_owner =  IDSOwner
GetFkeyCodes_secret = ids_secret_name

# COMMAND ----------

# MAGIC %run ../../../../../sequencer_routines/GetFkeyExprncCat

# COMMAND ----------

# MAGIC %run ../../../../../sequencer_routines/GetFkeyGrp

# COMMAND ----------

# MAGIC %run ../../../../../sequencer_routines/GetFkeyMbr

# COMMAND ----------

# MAGIC %run ../../../../../sequencer_routines/GetFkeyFnclLob

# COMMAND ----------

# MAGIC %run ../../../../../sequencer_routines/GetFkeyCodes

# COMMAND ----------

# Load the landing file for BCAClmLand (reading as CSV with the specified columns)
df_BCAClmLand_raw = (
    spark.read
    .option("sep", ",")
    .option("header", "false")
    .option("quote", "\"")
    .csv(f"{adls_path}/verified/BCADrugClm_Land.dat.{RunID}")
)

df_BCAClmLand = df_BCAClmLand_raw.toDF(
    "CLM_ID",
    "CLM_STTUS_CD",
    "ADJ_FROM_CLM_ID",
    "ADJ_TO_CLM_ID",
    "MBR_UNIQ_KEY",
    "ALLOCD_CRS_PLN_CD",
    "ALLOCD_SHIELD_PLN_CD",
    "PDX_SVC_ID",
    "CLM_LN_NO",
    "RX_FILLED_DT",
    "CONSIS_MBR_ID",
    "FEP_CNTR_ID",
    "LGCY_MBR_ID",
    "DOB",
    "PATN_AGE",
    "GNDR_CD",
    "PDX_NTNL_PROV_ID",
    "LGCY_SRC_CD",
    "PRSCRB_PROV_NTNL_PROV_ID",
    "PRSCRB_NTWK_CD",
    "PRSCRB_PROV_PLN_CD",
    "NDC",
    "BRND_NM",
    "LABEL_NM",
    "THRPTC_CAT_DESC",
    "GNRC_NM_DRUG_IN",
    "METH_DRUG_ADM",
    "RX_CST_EQVLNT",
    "METRIC_UNIT",
    "NON_METRIC_UNIT",
    "DAYS_SUPPLIED",
    "CLM_PD_DT",
    "CLM_LOAD_DT",
    "SUB_UNIQ_KEY",
    "GRP_ID",
    "SUB_ID",
    "MBR_SFX_NO",
    "DISPNS_DRUG_TYP",
    "DISPNS_AS_WRTN_STTUS_CD",
    "ADJ_CD",
    "PD_DT"
)

# BusinessRules transformer logic
df_MedtrakClmTrns = (
    df_BCAClmLand
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", current_date())
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))  # DataStage indicated "Expression": "SrcSysCd"
    .withColumn("PRI_KEY_STRING", F.concat_ws(";", F.col("SRC_SYS_CD"), F.col("CLM_ID")))
    .withColumn("CLM_SK", F.lit(0))
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("CLM_ID", F.col("CLM_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("ADJ_FROM_CLM", F.col("ADJ_FROM_CLM_ID"))
    .withColumn("ADJ_TO_CLM", F.col("ADJ_TO_CLM_ID"))
    .withColumn("ALPHA_PFX_CD", F.lit("NA"))
    .withColumn("CLM_EOB_EXCD", F.lit("NA"))
    .withColumn("CLS", F.lit("NA"))
    .withColumn("CLS_PLN", F.lit("NA"))
    .withColumn("EXPRNC_CAT", F.lit("NA"))
    .withColumn("FNCL_LOB_NO", F.lit("NA"))
    .withColumn("GRP", F.col("GRP_ID"))
    .withColumn("MBR_CK", F.col("MBR_UNIQ_KEY"))
    .withColumn("NTWK", F.lit("NA"))
    .withColumn("PROD", F.lit("NA"))
    .withColumn("SUBGRP", F.lit("NA"))
    .withColumn("SUB_CK", F.col("SUB_UNIQ_KEY"))
    .withColumn("CLM_ACDNT_CD", F.lit("NA"))
    .withColumn("CLM_ACDNT_ST_CD", F.lit("NA"))
    .withColumn("CLM_ACTIVATING_BCBS_PLN_CD", F.lit("NA"))
    .withColumn("CLM_AGMNT_SRC_CD", F.lit("NA"))
    .withColumn("CLM_BTCH_ACTN_CD", F.lit("NA"))
    .withColumn("CLM_CAP_CD", F.lit("N"))
    .withColumn("CLM_CAT_CD", F.lit("STD"))
    .withColumn("CLM_CHK_CYC_OVRD_CD", F.lit("NA"))
    .withColumn("CLM_COB_CD", F.lit("NA"))
    .withColumn("FINL_DISP_CD", F.lit("ACPTD"))
    .withColumn("CLM_INPT_METH_CD", F.lit("NA"))
    .withColumn("CLM_INPT_SRC_CD", F.lit("NA"))
    .withColumn("CLM_IPP_CD", F.lit("NA"))
    .withColumn("CLM_NTWK_STTUS_CD", F.lit("NA"))
    .withColumn("CLM_NONPAR_PROV_PFX_CD", F.lit("NA"))
    .withColumn("CLM_OTHER_BNF_CD", F.lit("NA"))
    .withColumn("CLM_PAYE_CD", F.lit("NA"))
    .withColumn("CLM_PRCS_CTL_AGNT_PFX_CD", F.lit("NA"))
    .withColumn("CLM_SVC_DEFN_PFX_CD", F.lit("NA"))
    .withColumn("CLM_SVC_PROV_SPEC_CD", F.lit("NA"))
    .withColumn("CLM_SVC_PROV_TYP_CD", F.lit("0012"))
    .withColumn(
        "CLM_STTUS_CD",
        F.when(F.col("ADJ_CD") == F.lit("O"), F.lit("A02"))
         .when(F.col("ADJ_CD") == F.lit("A"), F.lit("A09"))
         .when(F.col("ADJ_CD") == F.lit("V"), F.lit("A08"))
         .otherwise(F.lit("UNK"))
    )
    .withColumn("CLM_SUBMT_BCBS_PLN_CD", F.lit("NA"))
    .withColumn("CLM_SUB_BCBS_PLN_CD", F.lit("NA"))
    .withColumn("CLM_SUBTYP_CD", F.lit("RX"))
    .withColumn("CLM_TYP_CD", F.lit("MED"))
    .withColumn("ATCHMT_IN", F.lit("N"))
    .withColumn("CLM_CLNCL_EDIT_CD", F.lit("N"))
    .withColumn("COBRA_CLM_IN", F.lit("N"))
    .withColumn("FIRST_PASS_IN", F.lit("N"))
    .withColumn("HOST_IN", F.lit("N"))
    .withColumn("LTR_IN", F.lit("N"))
    .withColumn("MCARE_ASG_IN", F.lit("N"))
    .withColumn("NOTE_IN", F.lit("N"))
    .withColumn("PCA_AUDIT_IN", F.lit("N"))
    .withColumn("PCP_SUBMT_IN", F.lit("N"))
    .withColumn("PROD_OOA_IN", F.lit("N"))
    .withColumn("ACDNT_DT", F.lit("1753-01-01"))
    .withColumn("INPT_DT", F.lit("1753-01-01"))
    .withColumn("MBR_PLN_ELIG_DT", F.lit("1753-01-01"))
    .withColumn("NEXT_RVW_DT", F.lit("1753-01-01"))
    .withColumn("PD_DT", F.col("PD_DT"))
    .withColumn("PAYMT_DRAG_CYC_DT", F.lit("1753-01-01"))
    .withColumn("PRCS_DT", F.col("CLM_PD_DT"))
    .withColumn("RCVD_DT", F.col("CLM_LOAD_DT"))
    .withColumn("SVC_STRT_DT", F.col("RX_FILLED_DT"))
    .withColumn("SVC_END_DT", F.col("RX_FILLED_DT"))
    .withColumn("SMLR_ILNS_DT", F.lit("1753-01-01"))
    .withColumn("STTUS_DT", current_date())
    .withColumn("WORK_UNABLE_BEG_DT", F.lit("1753-01-01"))
    .withColumn("WORK_UNABLE_END_DT", F.lit("2199-12-31"))
    .withColumn("ACDNT_AMT", F.lit(0.00))
    .withColumn("ACTL_PD_AMT", F.lit(0.00))
    .withColumn(
        "ALLOW_AMT",
        F.when(F.col("RX_CST_EQVLNT").isNull(), F.lit(0.00)).otherwise(F.col("RX_CST_EQVLNT"))
    )
    .withColumn("DSALW_AMT", F.lit(0.00))
    .withColumn("COINS_AMT", F.lit(0.00))
    .withColumn(
        "CNSD_CHRG_AMT",
        F.when(F.col("RX_CST_EQVLNT").isNull(), F.lit(0.00)).otherwise(F.col("RX_CST_EQVLNT"))
    )
    .withColumn("COPAY_AMT", F.lit(0.00))
    .withColumn(
        "CHRG_AMT",
        F.when(F.col("RX_CST_EQVLNT").isNull(), F.lit(0.00)).otherwise(F.col("RX_CST_EQVLNT"))
    )
    .withColumn("DEDCT_AMT", F.lit(0.00))
    .withColumn("PAYBL_AMT", F.lit(0.00))
    .withColumn("CLM_CT", F.lit(1))
    .withColumn("MBR_AGE", F.col("PATN_AGE"))
    .withColumn("ADJ_FROM_CLM_ID", F.col("ADJ_FROM_CLM_ID"))
    .withColumn("ADJ_TO_CLM_ID", F.col("ADJ_TO_CLM_ID"))
    .withColumn("DOC_TX_ID", F.lit("NA"))
    .withColumn("MCAID_RESUB_NO", F.lit("NA"))
    .withColumn("MCARE_ID", F.lit("NA"))
    .withColumn("MBR_SFX_NO", F.col("MBR_SFX_NO"))
    .withColumn("PATN_ACCT_NO", F.lit("NA"))
    .withColumn("PAYMT_REF_ID", F.lit("NA"))
    .withColumn("PROV_AGMNT_ID", F.lit("NA"))
    .withColumn("RFRNG_PROV_TX", F.lit("NA"))
    .withColumn("SUB_ID", F.col("SUB_ID"))
    .withColumn("REMIT_SUPRSION_AMT", F.lit(0.00))
    .withColumn("MCAID_STTUS_ID", F.lit("NA"))
    .withColumn("PATN_PD_AMT", F.lit(0.00))
    .withColumn("CLM_SUBMT_ICD_VRSN_CD", F.lit("NA"))
)

# COMMAND ----------



# Read from DB2Connector (IDS). Two queries => sub_alpha_pfx, mbr_enroll
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

sub_alpha_query = f"""SELECT 
SUB_UNIQ_KEY,
ALPHA_PFX_CD 
FROM 
""" + IDSOwner + """.MBR MBR,
""" + IDSOwner + """.SUB SUB,
""" + IDSOwner + """.ALPHA_PFX PFX,
""" + IDSOwner + """.W_DRUG_ENR DRUG 

WHERE 
DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
AND MBR.SUB_SK = SUB.SUB_SK 
AND SUB.ALPHA_PFX_SK = PFX.ALPHA_PFX_SK
"""

df_sub_alpha_pfx_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sub_alpha_query)
    .load()
)

mbr_enroll_query =f"""SELECT 
DRUG.CLM_ID as CLM_ID,
CLS.CLS_ID,
PLN.CLS_PLN_ID,
SUBGRP.SUBGRP_ID,
CAT.EXPRNC_CAT_CD,
LOB.FNCL_LOB_CD,
CMPNT.PROD_ID  

FROM """ + IDSOwner + """.W_DRUG_ENR DRUG,
""" + IDSOwner + """.MBR_ENR                  MBR, 
""" + IDSOwner + """.CD_MPPNG                MAP1,
""" + IDSOwner + """.CLS                     CLS, 
""" + IDSOwner + """.SUBGRP                  SUBGRP,
""" + IDSOwner + """.CLS_PLN                 PLN, 
""" + IDSOwner + """.CD_MPPNG               MAP2, 
""" + IDSOwner + """.PROD_CMPNT           CMPNT,
""" + IDSOwner + """.PROD_BILL_CMPNT  BILL_CMPNT,
""" + IDSOwner + """.EXPRNC_CAT            CAT,
""" + IDSOwner + """.FNCL_LOB               LOB 

WHERE 
DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
and DRUG.FILL_DT_SK between MBR.EFF_DT_SK and MBR.TERM_DT_SK
and MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
and MAP1.TRGT_CD = 'MED' 
and MBR.CLS_SK = CLS.CLS_SK
and MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
and MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK

AND MBR.PROD_SK = CMPNT.PROD_SK
AND CMPNT.PROD_CMPNT_TYP_CD_SK = MAP2.CD_MPPNG_SK
AND MAP2.TRGT_CD= 'PDBL'
AND DRUG.FILL_DT_SK between CMPNT.PROD_CMPNT_EFF_DT_SK AND CMPNT.PROD_CMPNT_TERM_DT_SK
AND CMPNT.PROD_CMPNT_EFF_DT_SK= (SELECT MAX ( CMPNT2.PROD_CMPNT_EFF_DT_SK ) FROM """ + IDSOwner + """.PROD_CMPNT CMPNT2
                                WHERE CMPNT.PROD_SK = CMPNT2.PROD_SK
                                AND CMPNT.PROD_CMPNT_TYP_CD_SK = CMPNT2.PROD_CMPNT_TYP_CD_SK
                                AND DRUG.FILL_DT_SK BETWEEN CMPNT2.PROD_CMPNT_EFF_DT_SK AND CMPNT2.PROD_CMPNT_TERM_DT_SK)

AND CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT.PROD_CMPNT_PFX_ID
AND DRUG.FILL_DT_SK between BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK AND BILL_CMPNT.PROD_BILL_CMPNT_TERM_DT_SK
AND BILL_CMPNT.PROD_BILL_CMPNT_ID IN ('MED', 'MED1')
AND BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK= (SELECT MAX ( BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK ) FROM """ + IDSOwner + """.PROD_BILL_CMPNT BILL_CMPNT2
                                          WHERE BILL_CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
                                          AND CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
                                          AND BILL_CMPNT.PROD_BILL_CMPNT_ID = BILL_CMPNT2.PROD_BILL_CMPNT_ID
                                          AND BILL_CMPNT2.PROD_BILL_CMPNT_ID IN ('MED', 'MED1')
                                          AND DRUG.FILL_DT_SK BETWEEN BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK AND BILL_CMPNT2.PROD_BILL_CMPNT_TERM_DT_SK)

AND BILL_CMPNT.EXPRNC_CAT_SK = CAT.EXPRNC_CAT_SK
AND BILL_CMPNT.FNCL_LOB_SK = LOB.FNCL_LOB_SK
"""



# COMMAND ----------

df_mbr_enroll_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", mbr_enroll_query)
    .load()
)

# Deduplicate for hashed-file scenario A using the key columns
df_sub_alpha_pfx = dedup_sort(
    df_sub_alpha_pfx_raw,
    partition_cols=["SUB_UNIQ_KEY"],  # Overwrite function usage with correct call => we do separate calls
    sort_cols=[("SUB_UNIQ_KEY", "A")]
)  #  <-- This line must match the dedup_sort's signature exactly. But we have 2 dataframes. So we call dedup_sort once per DF.

# We need correct usage: "def dedup_sort(df, partition_cols, sort_cols) -> df"
df_sub_alpha_pfx = dedup_sort(
    df_sub_alpha_pfx_raw,
    ["SUB_UNIQ_KEY"],
    [("SUB_UNIQ_KEY","A")]
)

df_mbr_enroll = dedup_sort(
    df_mbr_enroll_raw,
    ["CLM_ID"],
    [("CLM_ID","A")]
)

# alpha_pfx stage = join transformations
df_alpha_pfx_joined = (
    df_MedtrakClmTrns.alias("MedtrakClmTrns")
    .join(df_sub_alpha_pfx.alias("sub_alpha_pfx_lkup"),
          F.col("MedtrakClmTrns.SUB_UNIQ_KEY") == F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY"),
          how="left")
    .join(df_mbr_enroll.alias("mbr_enr_lkup"),
          F.col("MedtrakClmTrns.CLM_ID") == F.col("mbr_enr_lkup.CLM_ID"),
          how="left")
)


# COMMAND ----------

df_mbr_enroll.printSchema()

# COMMAND ----------


df_alpha_pfx = (
      df_alpha_pfx_joined
      .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.col("JOB_EXCTN_RCRD_ERR_SK"))
      .withColumn("INSRT_UPDT_CD", F.col("INSRT_UPDT_CD"))
      .withColumn("DISCARD_IN", F.col("DISCARD_IN"))
      .withColumn("PASS_THRU_IN", F.col("PASS_THRU_IN"))
      .withColumn("FIRST_RECYC_DT", F.col("FIRST_RECYC_DT"))
      .withColumn("ERR_CT", F.col("ERR_CT"))
      .withColumn("RECYCLE_CT", F.col("RECYCLE_CT"))
      .withColumn("SRC_SYS_CD", F.col("SRC_SYS_CD"))
      .withColumn("PRI_KEY_STRING", F.col("PRI_KEY_STRING"))
      .withColumn("CLM_SK", F.col("CLM_SK"))
      .withColumn("SRC_SYS_CD_SK", F.col("SRC_SYS_CD_SK"))
      .withColumn("CLM_ID", F.col("CLM_ID"))
      .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("CRT_RUN_CYC_EXCTN_SK"))
      .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"))
      .withColumn("ADJ_FROM_CLM", F.col("ADJ_FROM_CLM"))
      .withColumn("ADJ_TO_CLM", F.col("ADJ_TO_CLM"))
      .withColumn(
          "ALPHA_PFX_CD",
          F.when(F.col("SUB_UNIQ_KEY").isNull(), F.lit("UNK"))
           .otherwise(F.col("ALPHA_PFX_CD"))
      )
      .withColumn(
          "CLS",
          F.when(F.col("CLM_ID").isNull(), F.lit("UNK"))
           .otherwise(F.col("CLS_ID"))
      )
      .withColumn(
          "CLS_PLN",
          F.when(F.col("CLM_ID").isNull(), F.lit("UNK"))
           .otherwise(F.col("CLS_PLN_ID"))
      )
      .withColumn(
          "EXPRNC_CAT",
          F.when(F.col("CLM_ID").isNull(), F.lit("UNK"))
           .otherwise(F.col("EXPRNC_CAT_CD"))
      )
      .withColumn(
          "FNCL_LOB_NO",
          F.when(F.col("CLM_ID").isNull(), F.lit("UNK"))
           .otherwise(F.col("FNCL_LOB_CD"))
      )
      .withColumn("GRP", F.col("GRP"))
      .withColumn("MBR_CK", F.col("MBR_CK"))
      .withColumn("NTWK", F.col("NTWK"))
      .withColumn(
          "PROD",
          F.when(F.col("CLM_ID").isNull(), F.lit("UNK"))
           .otherwise(F.trim(F.col("PROD_ID")))
      )
      .withColumn(
          "SUBGRP",
          F.when(F.col("CLM_ID").isNull(), F.lit("UNK"))
           .otherwise(F.col("SUBGRP_ID"))
      )
      .withColumn("SUB_CK", F.col("SUB_CK"))
      .withColumn("CLM_ACDNT_CD", F.col("CLM_ACDNT_CD"))
      .withColumn("CLM_ACDNT_ST_CD", F.col("CLM_ACDNT_ST_CD"))
      .withColumn("CLM_ACTIVATING_BCBS_PLN_CD", F.col("CLM_ACTIVATING_BCBS_PLN_CD"))
      .withColumn("CLM_AGMNT_SRC_CD", F.col("CLM_AGMNT_SRC_CD"))
      .withColumn("CLM_BTCH_ACTN_CD", F.col("CLM_BTCH_ACTN_CD"))
      .withColumn("CLM_CAP_CD", F.col("CLM_CAP_CD"))
      .withColumn("CLM_CAT_CD", F.col("CLM_CAT_CD"))
      .withColumn("CLM_CHK_CYC_OVRD_CD", F.col("CLM_CHK_CYC_OVRD_CD"))
      .withColumn("CLM_COB_CD", F.col("CLM_COB_CD"))
      .withColumn("FINL_DISP_CD", F.col("FINL_DISP_CD"))
      .withColumn("CLM_INPT_METH_CD", F.col("CLM_INPT_METH_CD"))
      .withColumn("CLM_INPT_SRC_CD", F.col("CLM_INPT_SRC_CD"))
      .withColumn("CLM_IPP_CD", F.col("CLM_IPP_CD"))
      .withColumn("CLM_NTWK_STTUS_CD", F.col("CLM_NTWK_STTUS_CD"))
      .withColumn("CLM_NONPAR_PROV_PFX_CD", F.col("CLM_NONPAR_PROV_PFX_CD"))
      .withColumn("CLM_OTHER_BNF_CD", F.col("CLM_OTHER_BNF_CD"))
      .withColumn("CLM_PAYE_CD", F.col("CLM_PAYE_CD"))
      .withColumn("CLM_PRCS_CTL_AGNT_PFX_CD", F.col("CLM_PRCS_CTL_AGNT_PFX_CD"))
      .withColumn("CLM_SVC_DEFN_PFX_CD", F.col("CLM_SVC_DEFN_PFX_CD"))
      .withColumn("CLM_SVC_PROV_SPEC_CD", F.col("CLM_SVC_PROV_SPEC_CD"))
      .withColumn("CLM_SVC_PROV_TYP_CD", F.col("CLM_SVC_PROV_TYP_CD"))
      .withColumn("CLM_STTUS_CD", F.col("CLM_STTUS_CD"))
      .withColumn("CLM_SUBMT_BCBS_PLN_CD", F.col("CLM_SUBMT_BCBS_PLN_CD"))
      .withColumn("CLM_SUB_BCBS_PLN_CD", F.col("CLM_SUB_BCBS_PLN_CD"))
      .withColumn("CLM_SUBTYP_CD", F.col("CLM_SUBTYP_CD"))
      .withColumn("CLM_TYP_CD", F.col("CLM_TYP_CD"))
      .withColumn("ATCHMT_IN", F.col("ATCHMT_IN"))
      .withColumn("CLM_CLNCL_EDIT_CD", F.col("CLM_CLNCL_EDIT_CD"))
      .withColumn("COBRA_CLM_IN", F.col("COBRA_CLM_IN"))
      .withColumn("FIRST_PASS_IN", F.col("FIRST_PASS_IN"))
      .withColumn("HOST_IN", F.col("HOST_IN"))
      .withColumn("LTR_IN", F.col("LTR_IN"))
      .withColumn("MCARE_ASG_IN", F.col("MCARE_ASG_IN"))
      .withColumn("NOTE_IN", F.col("NOTE_IN"))
      .withColumn("PCA_AUDIT_IN", F.col("PCA_AUDIT_IN"))
      .withColumn("PCP_SUBMT_IN", F.col("PCP_SUBMT_IN"))
      .withColumn("PROD_OOA_IN", F.col("PROD_OOA_IN"))
      .withColumn("ACDNT_DT", F.col("ACDNT_DT"))
      .withColumn("INPT_DT", F.col("INPT_DT"))
      .withColumn("MBR_PLN_ELIG_DT", F.col("MBR_PLN_ELIG_DT"))
      .withColumn("NEXT_RVW_DT", F.col("NEXT_RVW_DT"))
      .withColumn("PD_DT", F.col("PD_DT"))
      .withColumn("PAYMT_DRAG_CYC_DT", F.col("PAYMT_DRAG_CYC_DT"))
      .withColumn("PRCS_DT", F.col("PRCS_DT"))
      .withColumn("RCVD_DT", F.col("RCVD_DT"))
      .withColumn("SVC_STRT_DT", F.col("SVC_STRT_DT"))
      .withColumn("SVC_END_DT", F.col("SVC_END_DT"))
      .withColumn("SMLR_ILNS_DT", F.col("SMLR_ILNS_DT"))
      .withColumn("STTUS_DT", F.col("STTUS_DT"))
      .withColumn("WORK_UNABLE_BEG_DT", F.col("WORK_UNABLE_BEG_DT"))
      .withColumn("WORK_UNABLE_END_DT", F.col("WORK_UNABLE_END_DT"))
      .withColumn("ACDNT_AMT", F.col("ACDNT_AMT"))
      .withColumn("ACTL_PD_AMT", F.col("ACTL_PD_AMT"))
      .withColumn("ALLOW_AMT", F.col("ALLOW_AMT"))
      .withColumn("DSALW_AMT", F.col("DSALW_AMT"))
      .withColumn("COINS_AMT", F.col("COINS_AMT"))
      .withColumn("CNSD_CHRG_AMT", F.col("CNSD_CHRG_AMT"))
      .withColumn("COPAY_AMT", F.col("COPAY_AMT"))
      .withColumn("CHRG_AMT", F.col("CHRG_AMT"))
      .withColumn("DEDCT_AMT", F.col("DEDCT_AMT"))
      .withColumn("PAYBL_AMT", F.col("PAYBL_AMT"))
      .withColumn("CLM_CT", F.col("CLM_CT"))
      .withColumn("MBR_AGE", F.col("MBR_AGE"))
      .withColumn("ADJ_FROM_CLM_ID", F.col("ADJ_FROM_CLM_ID"))
      .withColumn("ADJ_TO_CLM_ID", F.col("ADJ_TO_CLM_ID"))
      .withColumn("DOC_TX_ID", F.col("DOC_TX_ID"))
      .withColumn("MCAID_RESUB_NO", F.col("MCAID_RESUB_NO"))
      .withColumn("MCARE_ID", F.col("MCARE_ID"))
      .withColumn("MBR_SFX_NO", F.col("MBR_SFX_NO"))
      .withColumn("PATN_ACCT_NO", F.col("PATN_ACCT_NO"))
      .withColumn("PAYMT_REF_ID", F.col("PAYMT_REF_ID"))
      .withColumn("PROV_AGMNT_ID", F.col("PROV_AGMNT_ID"))
      .withColumn("RFRNG_PROV_TX", F.col("RFRNG_PROV_TX"))
      .withColumn("SUB_ID", F.col("SUB_ID"))
      .withColumn("PRPR_ENTITY", F.lit(None))
      .withColumn("PCA_TYP_CD", F.lit("NA"))
      .withColumn("REL_PCA_CLM_ID", F.lit("NA"))
      .withColumn("CLCL_MICRO_ID", F.lit("NA"))
      .withColumn("CLM_UPDT_SW", F.lit("N"))
      .withColumn("REMIT_SUPRSION_AMT", F.col("REMIT_SUPRSION_AMT"))
      .withColumn("MCAID_STTUS_ID", F.col("MCAID_STTUS_ID"))
      .withColumn("PATN_PD_AMT", F.col("PATN_PD_AMT"))
      .withColumn("CLM_SUBMT_ICD_VRSN_CD", F.col("CLM_SUBMT_ICD_VRSN_CD"))
      .withColumn("CLM_TXNMY_CD", F.lit(""))
  )


# COMMAND ----------


# Snapshot stage => output 1 "Pkey" -> ClmPK, output 2 "Snapshot" -> next Transformer
df_Snapshot_Pkey = df_alpha_pfx.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ADJ_FROM_CLM",
    "ADJ_TO_CLM",
    "ALPHA_PFX_CD",
    "CLM_EOB_EXCD",
    "CLS",
    "CLS_PLN",
    "EXPRNC_CAT",
    "FNCL_LOB_NO",
    "GRP",
    "MBR_CK",
    "NTWK",
    "PROD",
    "SUBGRP",
    "SUB_CK",
    "CLM_ACDNT_CD",
    "CLM_ACDNT_ST_CD",
    "CLM_ACTIVATING_BCBS_PLN_CD",
    "CLM_AGMNT_SRC_CD",
    "CLM_BTCH_ACTN_CD",
    "CLM_CAP_CD",
    "CLM_CAT_CD",
    "CLM_CHK_CYC_OVRD_CD",
    "CLM_COB_CD",
    "FINL_DISP_CD",
    "CLM_INPT_METH_CD",
    "CLM_INPT_SRC_CD",
    "CLM_IPP_CD",
    "CLM_NTWK_STTUS_CD",
    "CLM_NONPAR_PROV_PFX_CD",
    "CLM_OTHER_BNF_CD",
    "CLM_PAYE_CD",
    "CLM_PRCS_CTL_AGNT_PFX_CD",
    "CLM_SVC_DEFN_PFX_CD",
    "CLM_SVC_PROV_SPEC_CD",
    "CLM_SVC_PROV_TYP_CD",
    "CLM_STTUS_CD",
    "CLM_SUBMT_BCBS_PLN_CD",
    "CLM_SUB_BCBS_PLN_CD",
    "CLM_SUBTYP_CD",
    "CLM_TYP_CD",
    "ATCHMT_IN",
    "CLM_CLNCL_EDIT_CD",
    "COBRA_CLM_IN",
    "FIRST_PASS_IN",
    "HOST_IN",
    "LTR_IN",
    "MCARE_ASG_IN",
    "NOTE_IN",
    "PCA_AUDIT_IN",
    "PCP_SUBMT_IN",
    "PROD_OOA_IN",
    "ACDNT_DT",
    "INPT_DT",
    "MBR_PLN_ELIG_DT",
    "NEXT_RVW_DT",
    "PD_DT",
    "PAYMT_DRAG_CYC_DT",
    "PRCS_DT",
    "RCVD_DT",
    "SVC_STRT_DT",
    "SVC_END_DT",
    "SMLR_ILNS_DT",
    "STTUS_DT",
    "WORK_UNABLE_BEG_DT",
    "WORK_UNABLE_END_DT",
    "ACDNT_AMT",
    "ACTL_PD_AMT",
    "ALLOW_AMT",
    "DSALW_AMT",
    "COINS_AMT",
    "CNSD_CHRG_AMT",
    "COPAY_AMT",
    "CHRG_AMT",
    "DEDCT_AMT",
    "PAYBL_AMT",
    "CLM_CT",
    "MBR_AGE",
    "ADJ_FROM_CLM_ID",
    "ADJ_TO_CLM_ID",
    "DOC_TX_ID",
    "MCAID_RESUB_NO",
    "MCARE_ID",
    "MBR_SFX_NO",
    "PATN_ACCT_NO",
    "PAYMT_REF_ID",
    "PROV_AGMNT_ID",
    "RFRNG_PROV_TX",
    "SUB_ID",
    "PRPR_ENTITY",
    "PCA_TYP_CD",
    "REL_PCA_CLM_ID",
    "CLCL_MICRO_ID",
    "CLM_UPDT_SW",
    "REMIT_SUPRSION_AMT",
    "MCAID_STTUS_ID",
    "PATN_PD_AMT",
    "CLM_SUBMT_ICD_VRSN_CD",
    "CLM_TXNMY_CD",
    F.lit("N").alias("BILL_PAYMT_EXCL_IN")
)

df_Snapshot_Snap = df_alpha_pfx.select(
    CLM_ID,
    "MBR_CK",
    F.col("GRP").alias("GRP"),
    "SVC_STRT_DT",
    "CHRG_AMT",
    "PAYBL_AMT",
    F.col("EXPRNC_CAT").alias("EXPRNC_CAT"),
    F.col("FNCL_LOB_NO").alias("FNCL_LOB_NO"),
    "CLM_CT",
    F.col("PCA_TYP_CD").alias("PCA_TYP_CD"),
    F.col("CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.col("CLM_CAT_CD").alias("CLM_CAT_CD")
)

# Call the shared container ClmPK on df_Snapshot_Pkey
params_clmpk = {
    "CurrRunCycle": RunCycle
}
df_ClmPK_out = ClmPK(df_Snapshot_Pkey, params_clmpk)

# BCAClmExtr final output => write the df_ClmPK_out
# Apply rpad for char/varchar columns (length is gleaned from the final stage's columns in the JSON)
df_BCAClmExtr_out = df_ClmPK_out.select(
    rpad(F.col("JOB_EXCTN_RCRD_ERR_SK").cast(StringType()), 0, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
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
)

write_files(
    df_BCAClmExtr_out,
    f"{adls_path}/key/BCAClmExtr.DrugClm.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Transformer stage => with stage variables
df_Snapshot_Snap_vars = (
    df_Snapshot_Snap
    .withColumn("ExpCatCdSk", GetFkeyExprncCat(F.lit("FACETS"), F.lit(0), F.col("EXPRNC_CAT"), F.lit("N")))
    .withColumn("GrpSk", GetFkeyGrp(F.lit("FACETS"), F.lit(0), F.col("GRP"), F.lit("N")))
    .withColumn("MbrSk", GetFkeyMbr(F.lit("FACETS"), F.lit(0), F.col("MBR_CK"), F.lit("N")))
    .withColumn("FnclLobSk", GetFkeyFnclLob(F.lit("PSI"), F.lit(0), F.col("FNCL_LOB_NO"), F.lit("N")))
    .withColumn("PcaTypCdSk", GetFkeyCodes(F.lit("FACETS"), F.lit(0), F.lit("PERSONAL CARE ACCOUNT PROCESSING"), F.col("PCA_TYP_CD"), F.lit("N")))
    .withColumn("ClmSttusCdSk", GetFkeyCodes(F.lit(SrcSysCd), F.lit(0), F.lit("CLAIM STATUS"), F.col("CLM_STTUS_CD"), F.lit("N")))
    .withColumn("ClmCatCdSk", GetFkeyCodes(F.lit(SrcSysCd), F.lit(0), F.lit("CLAIM CATEGORY"), F.col("CLM_CAT_CD"), F.lit("X")))
)

# Output to B_CLM => apply rpad for final columns if needed. This is a CSV as well
df_B_CLM_out = df_Snapshot_Snap_vars.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("ClmSttusCdSk").alias("CLM_STTUS_CD_SK"),
    F.col("ClmCatCdSk").alias("CLM_CAT_CD_SK"),
    F.col("ExpCatCdSk").alias("EXPRNC_CAT_SK"),
    F.col("FnclLobSk").alias("FNCL_LOB_SK"),
    F.col("GrpSk").alias("GRP_SK"),
    F.col("MbrSk").alias("MBR_SK"),
    rpad(F.col("SVC_STRT_DT"), 10, " ").alias("SVC_STRT_DT"),
    F.col("CHRG_AMT"),
    F.col("PAYBL_AMT"),
    F.col("CLM_CT"),
    F.col("PcaTypCdSk").alias("PCA_TYP_CD_SK")
)

write_files(
    df_B_CLM_out,
    f"{adls_path}/load/B_CLM.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)