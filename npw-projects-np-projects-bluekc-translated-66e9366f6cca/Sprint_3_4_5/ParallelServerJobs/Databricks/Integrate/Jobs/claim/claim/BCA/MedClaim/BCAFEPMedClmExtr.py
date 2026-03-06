# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  BCAFEPMedExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Reads the BCAFEPMedDrugClm_Land.dat file and puts the data into the claim common record format and takes it through primary key process using Shared Container ClmPkey
# MAGIC     
# MAGIC     
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer               Date                 Project/Altiris #       Change Description                                                                   Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------           --------------------     ------------------------       -----------------------------------------------------------------------------------------          --------------------------------       -------------------------------   ----------------------------       
# MAGIC Sudhir Bomshetty   2017-09-20        5781 HEDIS           Initial Programming                                                                    IntegrateDev2                 Kalyan Neelam          2017-10-18
# MAGIC 
# MAGIC Sudhir Bomshetty   2018-01-05        5781 HEDIS           Removing Date formating for RCVD_DT, STTUS_DT             IntegrateDev2                 Kalyan Neelam          2018-01-16
# MAGIC 
# MAGIC Sudhir Bomshetty   2018-01-05        5781 HEDIS           Changing logic for PROV_TYP_CD, PROV_SPEC_CD           IntegrateDev2                 Jaideep Mankala       04/02/2018
# MAGIC 
# MAGIC Mohan Karnati                 2019-06-06        ADO-73034             Adding CLM_TXNMY_CD filed in alpha_pfx stage and                 IntegrateDev1	         Kalyan Neelam             2019-07-01
# MAGIC                                                                                                    passing it till BCAFepClmExtr stage 
# MAGIC 
# MAGIC Giri  Mallavaram    2020-04-06         6131 PBM Changes       Added BILL_PAYMT_EXCL_IN to be in sync with changes to CLM_PKey Container        IntegrateDev2         Kalyan Neelam     2020-04-07

# MAGIC Lookup subscriber, product and member information
# MAGIC Writing Sequential File to /key
# MAGIC Read the landing file created in BCAFEPMedClmPreprocExtr
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
# MAGIC Assign primary surrogate key
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
    IntegerType,
    DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
CurrentDate = get_widget_value("CurrentDate","")
SrcSysCd = get_widget_value("SrcSysCd","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
RunCycle = get_widget_value("RunCycle","")
RunID = get_widget_value("RunID","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

# MAGIC %run ../../../../../../shared_containers/PrimaryKey/ClmPK
# COMMAND ----------

################################################################################
# Read from BCAFepClmLand (CSeqFileStage)
################################################################################
schema_BCAFepClmLand = (
    StructType()
    .add("CLM_ID", StringType(), False)
    .add("CLM_LN_NO", IntegerType(), False)
    .add("ADJ_FROM_CLM_ID", StringType(), False)
    .add("ADJ_TO_CLM_ID", StringType(), False)
    .add("CLM_STTUS_CD", StringType(), False)
    .add("MBR_UNIQ_KEY", IntegerType(), False)
    .add("SUB_UNIQ_KEY", IntegerType(), False)
    .add("GRP_ID", StringType(), False)
    .add("DOB", StringType(), True)   # char(10)
    .add("GNDR_CD", StringType(), True)   # char(1)
    .add("SUB_ID", StringType(), False)
    .add("MBR_SFX_NO", StringType(), True)   # char(2)
    .add("SRC_SYS", StringType(), False)
    .add("RCRD_ID", StringType(), False)
    .add("ADJ_NO", StringType(), True)
    .add("PERFORMING_PROV_ID", StringType(), True)
    .add("FEP_PROD", StringType(), True)
    .add("MBR_ID", StringType(), True)
    .add("BILL_TYP__CD", StringType(), True)   # char(4)
    .add("FEP_SVC_LOC_CD", StringType(), True)   # char(2)
    .add("IP_CLM_TYP_IN", StringType(), True)   # char(1)
    .add("DRG_VRSN_IN", StringType(), True)   # char(2)
    .add("DRG_CD", StringType(), True)   # char(4)
    .add("PATN_STTUS_CD", StringType(), True)
    .add("CLM_CLS_IN", StringType(), True)   # char(1)
    .add("CLM_DENIED_FLAG", StringType(), True)   # char(1)
    .add("ILNS_DT", StringType(), True)
    .add("IP_CLM_BEG_DT", StringType(), True)   # char(10)
    .add("IP_CLM_DSCHG_DT", StringType(), True)   # char(10)
    .add("CLM_SVC_DT_BEG", StringType(), True)   # char(10)
    .add("CLM_SVC_DT_END", StringType(), True)   # char(10)
    .add("FCLTY_CLM_STMNT_BEG_DT", StringType(), True)   # char(10)
    .add("FCLTY_CLM_STMNT_END_DT", StringType(), True)   # char(10)
    .add("CLM_PRCS_DT", StringType(), True)   # char(10)
    .add("IP_ADMS_CT", StringType(), True)   # char(1)
    .add("NO_COV_DAYS", IntegerType(), True)
    .add("DIAG_CDNG_TYP", StringType(), True)   # char(1)
    .add("PRI_DIAG_CD", StringType(), True)   # char(10)
    .add("PRI_DIAG_POA_IN", StringType(), True)   # char(1)
    .add("ADM_DIAG_CD", StringType(), True)
    .add("ADM_DIAG_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_1", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_1_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_2", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_2_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_3", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_3_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_4", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_4_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_5", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_5_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_6", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_6_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_7", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_7_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_8", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_8_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_9", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_9_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_10", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_10_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_11", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_11_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_12", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_12_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_13", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_13_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_14", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_14_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_15", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_15_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_16", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_16_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_17", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_17_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_18", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_18_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_19", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_19_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_20", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_20_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_21", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_21_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_22", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_22_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_23", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_23_POA_IN", StringType(), True)   # char(1)
    .add("OTHR_DIAG_CD_24", StringType(), True)   # char(10)
    .add("OTHR_DIAG_CD_24_POA_IN", StringType(), True)   # char(1)
    .add("PROC_CDNG_TYP", StringType(), True)   # char(1)
    .add("PRINCIPLE_PROC_CD", StringType(), True)
    .add("PRINCIPLE_PROC_CD_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_1", StringType(), True)
    .add("OTHR_PROC_CD_1_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_2", StringType(), True)
    .add("OTHR_PROC_CD_2_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_3", StringType(), True)
    .add("OTHR_PROC_CD_3_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_4", StringType(), True)
    .add("OTHR_PROC_CD_4_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_5", StringType(), True)
    .add("OTHR_PROC_CD_5_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_6", StringType(), True)
    .add("OTHR_PROC_CD_6_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_7", StringType(), True)
    .add("OTHR_PROC_CD_7_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_8", StringType(), True)
    .add("OTHR_PROC_CD_8_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_9", StringType(), True)
    .add("OTHR_PROC_CD_9_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_10", StringType(), True)
    .add("OTHR_PROC_CD_10_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_11", StringType(), True)
    .add("OTHR_PROC_CD_11_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_12", StringType(), True)
    .add("OTHR_PROC_CD_12_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_13", StringType(), True)
    .add("OTHR_PROC_CD_13_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_14", StringType(), True)
    .add("OTHR_PROC_CD_14_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_15", StringType(), True)
    .add("OTHR_PROC_CD_15_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_16", StringType(), True)
    .add("OTHR_PROC_CD_16_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_17", StringType(), True)
    .add("OTHR_PROC_CD_17_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_18", StringType(), True)
    .add("OTHR_PROC_CD_18_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_19", StringType(), True)
    .add("OTHR_PROC_CD_19_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_20", StringType(), True)
    .add("OTHR_PROC_CD_20_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_21", StringType(), True)
    .add("OTHR_PROC_CD_21_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_22", StringType(), True)
    .add("OTHR_PROC_CD_22_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_23", StringType(), True)
    .add("OTHR_PROC_CD_23_DT", StringType(), True)   # char(10)
    .add("OTHR_PROC_CD_24", StringType(), True)
    .add("OTHR_PROC_CD_24_DT", StringType(), True)   # char(10)
    .add("RVNU_CD", StringType(), True)   # char(4)
    .add("PROC_CD_NON_ICD", StringType(), True)   # char(10)
    .add("PROC_CD_MOD_1", StringType(), True)
    .add("PROC_CD_MOD_2", StringType(), True)
    .add("PROC_CD_MOD_3", StringType(), True)
    .add("PROC_CD_MOD_4", StringType(), True)
    .add("CLM_UNIT", DecimalType(38,10), True)
    .add("CLM_LN_TOT_ALL_SVC_CHRG_AMT", DecimalType(38,10), True)
    .add("CLM_LN_PD_AMT", DecimalType(38,10), True)
    .add("CLM_ALT_AMT_PD_1", DecimalType(38,10), True)
    .add("CLM_ALT_AMT_PD_2", DecimalType(38,10), True)
    .add("LOINC_CD", StringType(), True)   # char(7)
    .add("CLM_TST_RSLT", StringType(), True)
    .add("ALT_CLM_TST_RSLT", StringType(), True)
    .add("DRUG_CD", StringType(), True)
    .add("DRUG_CLM_INCUR_DT", StringType(), True)
    .add("CLM_TREAT_DURATN", StringType(), True)
    .add("DATA_SRC", StringType(), True)   # char(2)
    .add("SUPLMT_DATA_SRC_TYP", StringType(), True)
    .add("ADTR_APRV_STTUS", StringType(), True)
    .add("RUN_DT", StringType(), True)
    .add("PD_DAYS", IntegerType(), True)
    .add("PERFORMING_NTNL_PROV_ID", StringType(), True)
)

df_BCAFepClmLand = (
    spark.read.csv(
        path=f"{adls_path}/verified/BCAFEPCMedClm_ClmLanding.dat.{RunID}",
        schema=schema_BCAFepClmLand,
        sep=",",
        quote='"',
        header=False
    )
)

################################################################################
# Read from "ids" DB2Connector (two queries) -> sub_alpha_pfx, mbr_enroll
################################################################################
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_sub_alpha_pfx = f"""
SELECT 
SUB_UNIQ_KEY,
ALPHA_PFX_CD
FROM 
{IDSOwner}.MBR MBR,
{IDSOwner}.SUB SUB,
{IDSOwner}.ALPHA_PFX PFX,
{IDSOwner}.W_DRUG_ENR DRUG

WHERE 
DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
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
{IDSOwner}.MBR_ENR                  MBR, 
{IDSOwner}.CD_MPPNG                MAP1,
{IDSOwner}.CLS                     CLS,
{IDSOwner}.SUBGRP                  SUBGRP,
{IDSOwner}.CLS_PLN                 PLN,
{IDSOwner}.CD_MPPNG                MAP2,
{IDSOwner}.PROD_CMPNT              CMPNT,
{IDSOwner}.PROD_BILL_CMPNT         BILL_CMPNT,
{IDSOwner}.EXPRNC_CAT              CAT,
{IDSOwner}.FNCL_LOB                LOB

WHERE 
DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
and DRUG.FILL_DT_SK between MBR.EFF_DT_SK and MBR.TERM_DT_SK
and MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
and MAP1.TRGT_CD = 'MED'
and MBR.ELIG_IN = 'Y'
and MBR.CLS_SK = CLS.CLS_SK
and MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
and MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
AND MBR.PROD_SK = CMPNT.PROD_SK
AND CMPNT.PROD_CMPNT_TYP_CD_SK = MAP2.CD_MPPNG_SK
AND MAP2.TRGT_CD= 'PDBL'
AND DRUG.FILL_DT_SK between CMPNT.PROD_CMPNT_EFF_DT_SK AND CMPNT.PROD_CMPNT_TERM_DT_SK
AND CMPNT.PROD_CMPNT_EFF_DT_SK= (
  SELECT MAX ( CMPNT2.PROD_CMPNT_EFF_DT_SK )
  FROM  {IDSOwner}.PROD_CMPNT CMPNT2
  WHERE
    CMPNT.PROD_SK = CMPNT2.PROD_SK
    AND CMPNT.PROD_CMPNT_TYP_CD_SK = CMPNT2.PROD_CMPNT_TYP_CD_SK
    AND DRUG.FILL_DT_SK between CMPNT2.PROD_CMPNT_EFF_DT_SK AND CMPNT2.PROD_CMPNT_TERM_DT_SK
)
AND CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT.PROD_CMPNT_PFX_ID
AND DRUG.FILL_DT_SK between BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK AND BILL_CMPNT.PROD_BILL_CMPNT_TERM_DT_SK
AND BILL_CMPNT.PROD_BILL_CMPNT_ID IN ('MED', 'MED1')
AND BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK= (
  SELECT MAX ( BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK )
  FROM {IDSOwner}.PROD_BILL_CMPNT BILL_CMPNT2
  WHERE
    BILL_CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
    AND CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
    AND BILL_CMPNT.PROD_BILL_CMPNT_ID = BILL_CMPNT2.PROD_BILL_CMPNT_ID
    AND BILL_CMPNT2.PROD_BILL_CMPNT_ID IN ('MED', 'MED1')
    AND DRUG.FILL_DT_SK between BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK AND BILL_CMPNT2.PROD_BILL_CMPNT_TERM_DT_SK
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

# Hash stage (CHashedFileStage) scenario A: deduplicate by primary keys
df_sub_alpha_pfx_lkup = df_sub_alpha_pfx.dropDuplicates(["SUB_UNIQ_KEY"])
df_mbr_enr_lkup = df_mbr_enroll.dropDuplicates(["CLM_ID"])

################################################################################
# Read from IDS_PROV (DB2Connector) -> Prov, Ntnl_ProvId
################################################################################
extract_query_Prov = f"""
SELECT 
PROV.PROV_ID,
PROV_SPEC.PROV_SPEC_CD,
PROV_TYP.PROV_TYP_CD
FROM
{IDSOwner}.CD_MPPNG CD,
{IDSOwner}.PROV PROV
LEFT OUTER JOIN {IDSOwner}.PROV_SPEC_CD PROV_SPEC 
  ON PROV.PROV_SPEC_CD_SK = PROV_SPEC.PROV_SPEC_CD_SK
LEFT OUTER JOIN {IDSOwner}.PROV_TYP_CD PROV_TYP 
  ON PROV.PROV_TYP_CD_SK = PROV_TYP.PROV_TYP_CD_SK
WHERE
PROV.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
AND CD.TRGT_CD= 'BCA'
"""

extract_query_Ntnl_ProvId = f"""
SELECT 
PROV2.NTNL_PROV_ID,
PROV_SPEC.PROV_SPEC_CD,
PROV_TYP.PROV_TYP_CD
FROM
(
  SELECT PROV.NTNL_PROV_ID, MIN(PROV.PROV_ID) PROV_ID
  FROM {IDSOwner}.PROV PROV
  WHERE PROV.NTNL_PROV_ID<>'NA'
    AND PROV.TERM_DT_SK > '{CurrentDate}'
  GROUP BY PROV.NTNL_PROV_ID
) PROV1,
{IDSOwner}.CD_MPPNG CD,
{IDSOwner}.PROV PROV2
LEFT OUTER JOIN {IDSOwner}.PROV_SPEC_CD PROV_SPEC
  ON PROV2.PROV_SPEC_CD_SK = PROV_SPEC.PROV_SPEC_CD_SK
LEFT OUTER JOIN {IDSOwner}.PROV_TYP_CD PROV_TYP
  ON PROV2.PROV_TYP_CD_SK = PROV_TYP.PROV_TYP_CD_SK
WHERE
PROV2.PROV_ID = PROV1.PROV_ID
AND PROV2.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
AND CD.TRGT_CD= 'BCA'
"""

jdbc_url_ids_prov, jdbc_props_ids_prov = get_db_config(ids_secret_name)

df_Prov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids_prov)
    .options(**jdbc_props_ids_prov)
    .option("query", extract_query_Prov)
    .load()
)

df_Ntnl_ProvId = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids_prov)
    .options(**jdbc_props_ids_prov)
    .option("query", extract_query_Ntnl_ProvId)
    .load()
)

# hf_ProvId stage (CHashedFileStage) scenario A: deduplicate by PK
df_ProvId = df_Prov.dropDuplicates(["PROV_ID"])
df_NtnlProvId = df_Ntnl_ProvId.dropDuplicates(["NTNL_PROV_ID"])

################################################################################
# BusinessRules (CTransformerStage)
# Primary Link: df_BCAFepClmLand as BCA
# Lookup Link 1: df_ProvId as ProvId (left join on BCA.PERFORMING_PROV_ID = ProvId.PROV_ID)
# Lookup Link 2: df_NtnlProvId as NtnlProvId (left join on BCA.PERFORMING_NTNL_PROV_ID = NtnlProvId.NTNL_PROV_ID)
################################################################################

df_BCA_alias = df_BCAFepClmLand.alias("BCA")
df_ProvId_alias = df_ProvId.alias("ProvId")
df_NtnlProvId_alias = df_NtnlProvId.alias("NtnlProvId")

df_joined_BusinessRules = (
    df_BCA_alias
    .join(
        df_ProvId_alias,
        on=(F.col("BCA.PERFORMING_PROV_ID") == F.col("ProvId.PROV_ID")),
        how="left"
    )
    .join(
        df_NtnlProvId_alias,
        on=(F.col("BCA.PERFORMING_NTNL_PROV_ID") == F.col("NtnlProvId.NTNL_PROV_ID")),
        how="left"
    )
)

# Build the output columns (MedtrakClmTrns).
# Using expressions or literal values from the JSON instructions.
df_MedtrakClmTrns_temp = df_joined_BusinessRules.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),          # char(10)
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),              # char(1)
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),            # char(1)
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("BCA.SRC_SYS").alias("SRC_SYS_CD"),
    F.concat(F.col("BCA.SRC_SYS"), F.lit(";"), F.col("BCA.CLM_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.rpad(F.col("BCA.CLM_ID"), 18, " ").alias("CLM_ID"),        # char(18)
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("BCA.ADJ_FROM_CLM_ID"), 12, " ").alias("ADJ_FROM_CLM"),   # char(12)
    F.rpad(F.col("BCA.ADJ_TO_CLM_ID"), 12, " ").alias("ADJ_TO_CLM"),       # char(12)
    F.rpad(F.lit(""), 3, " ").alias("ALPHA_PFX_CD"),                       # char(3)
    F.rpad(F.lit("NA"), 3, " ").alias("CLM_EOB_EXCD"),                     # char(3)
    F.rpad(F.lit(""), 4, " ").alias("CLS"),                                # char(4)
    F.rpad(F.lit(""), 8, " ").alias("CLS_PLN"),                            # char(8)
    F.rpad(F.lit(""), 4, " ").alias("EXPRNC_CAT"),                         # char(4)
    F.rpad(F.lit(""), 4, " ").alias("FNCL_LOB_NO"),                        # char(4)
    F.rpad(F.col("BCA.GRP_ID"), 8, " ").alias("GRP"),                      # char(8)
    F.col("BCA.MBR_UNIQ_KEY").alias("MBR_CK"),
    F.rpad(F.lit("NA"), 12, " ").alias("NTWK"),                            # char(12)
    F.rpad(F.lit(""), 8, " ").alias("PROD"),                               # char(8)
    F.rpad(F.lit(""), 4, " ").alias("SUBGRP"),                             # char(4)
    F.col("BCA.SUB_UNIQ_KEY").alias("SUB_CK"),
    F.rpad(F.lit("NA"), 10, " ").alias("CLM_ACDNT_CD"),                    # char(10)
    F.rpad(F.lit("NA"), 2, " ").alias("CLM_ACDNT_ST_CD"),                  # char(2)
    F.rpad(F.lit("NA"), 10, " ").alias("CLM_ACTIVATING_BCBS_PLN_CD"),      # char(10)
    F.rpad(F.lit("NA"), 10, " ").alias("CLM_AGMNT_SRC_CD"),                # char(10)
    F.rpad(F.lit("NA"), 1, " ").alias("CLM_BTCH_ACTN_CD"),                 # char(1)
    F.rpad(F.lit("N"), 10, " ").alias("CLM_CAP_CD"),                       # char(10)
    F.rpad(F.lit("STD"), 10, " ").alias("CLM_CAT_CD"),                     # char(10)
    F.rpad(F.lit("NA"), 1, " ").alias("CLM_CHK_CYC_OVRD_CD"),              # char(1)
    F.rpad(F.lit("NA"), 2, " ").alias("CLM_COB_CD"),                       # char(2)
    F.when(F.col("BCA.CLM_DENIED_FLAG") == "Y", F.lit("DENIEDREJ"))
     .otherwise(F.lit("ACPTD"))
     .alias("FINL_DISP_CD"),
    F.rpad(F.lit("NA"), 1, " ").alias("CLM_INPT_METH_CD"),                 # char(1)
    F.rpad(F.lit("NA"), 10, " ").alias("CLM_INPT_SRC_CD"),                 # char(10)
    F.rpad(F.lit("NA"), 10, " ").alias("CLM_IPP_CD"),                      # char(10)
    F.rpad(F.lit("NA"), 2, " ").alias("CLM_NTWK_STTUS_CD"),                # char(2)
    F.rpad(F.lit("NA"), 4, " ").alias("CLM_NONPAR_PROV_PFX_CD"),           # char(4)
    F.rpad(F.lit("NA"), 1, " ").alias("CLM_OTHER_BNF_CD"),                 # char(1)
    F.rpad(F.lit("NA"), 1, " ").alias("CLM_PAYE_CD"),                      # char(1)
    F.rpad(F.lit("NA"), 4, " ").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),         # char(4)
    F.rpad(F.lit("NA"), 4, " ").alias("CLM_SVC_DEFN_PFX_CD"),              # char(4)
    F.when(
        F.isnull(F.col("BCA.PERFORMING_NTNL_PROV_ID")) | (F.trim(F.col("BCA.PERFORMING_NTNL_PROV_ID")) == ""),
        F.lit("NA")
    ).otherwise(
        F.when(
            (F.col("BCA.PERFORMING_NTNL_PROV_ID").substr(F.lit(1),F.lit(1)).cast("int")==0),
            F.when(F.isnull(F.col("ProvId.PROV_SPEC_CD")), F.lit("UNK")).otherwise(F.col("ProvId.PROV_SPEC_CD"))
        ).otherwise(
            F.when(F.isnull(F.col("NtnlProvId.PROV_SPEC_CD")), F.lit("UNK")).otherwise(F.col("NtnlProvId.PROV_SPEC_CD"))
        )
    ).alias("CLM_SVC_PROV_SPEC_CD"),
    F.when(
        F.isnull(F.col("BCA.PERFORMING_NTNL_PROV_ID")) | (F.trim(F.col("BCA.PERFORMING_NTNL_PROV_ID")) == ""),
        F.lit("NA")
    ).otherwise(
        F.when(
            (F.col("BCA.PERFORMING_NTNL_PROV_ID").substr(F.lit(1),F.lit(1)).cast("int")==0),
            F.when(F.isnull(F.col("ProvId.PROV_TYP_CD")), F.lit("UNK")).otherwise(F.col("ProvId.PROV_TYP_CD"))
        ).otherwise(
            F.when(F.isnull(F.col("NtnlProvId.PROV_TYP_CD")), F.lit("UNK")).otherwise(F.col("NtnlProvId.PROV_TYP_CD"))
        )
    ).alias("CLM_SVC_PROV_TYP_CD"),
    F.rpad(F.col("BCA.CLM_STTUS_CD"), 2, " ").alias("CLM_STTUS_CD"),  # char(2)
    F.rpad(F.col("BCA.RCRD_ID").substr(F.lit(1),F.lit(3)), 10, " ").alias("CLM_SUBMT_BCBS_PLN_CD"), # char(10)
    F.rpad(F.lit("NA"), 10, " ").alias("CLM_SUB_BCBS_PLN_CD"),        # char(10)
    F.when(
        (F.col("BCA.IP_CLM_TYP_IN") == "I"),
        F.lit("11")
    ).otherwise(
        F.when(
            F.isnull(F.col("BCA.IP_CLM_TYP_IN")) | (F.length(F.trim(F.col("BCA.IP_CLM_TYP_IN")))==0),
            F.when(
                F.isnull(F.col("BCA.PATN_STTUS_CD")) & (F.col("BCA.CLM_CLS_IN")=="F"),
                F.lit("12")
            ).otherwise(
                F.when(
                    F.isnull(F.col("BCA.PATN_STTUS_CD")) & (F.col("BCA.CLM_CLS_IN")=="M"),
                    F.lit("20")
                ).otherwise(F.lit("UNK"))
            )
        ).otherwise(F.lit("UNK"))
    ).alias("CLM_SUBTYP_CD"),
    F.rpad(F.lit("MED"), 1, " ").alias("CLM_TYP_CD"),                 # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("ATCHMT_IN"),                    # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("CLM_CLNCL_EDIT_CD"),            # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("COBRA_CLM_IN"),                 # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("FIRST_PASS_IN"),                # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("HOST_IN"),                      # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("LTR_IN"),                       # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("MCARE_ASG_IN"),                 # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("NOTE_IN"),                      # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("PCA_AUDIT_IN"),                 # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("PCP_SUBMT_IN"),                 # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("PROD_OOA_IN"),                  # char(1)
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("ACDNT_DT"),           # char(10)
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("INPT_DT"),            # char(10)
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("MBR_PLN_ELIG_DT"),    # char(10)
    F.rpad(F.lit("2199-12-31"), 10, " ").alias("NEXT_RVW_DT"),        # char(10)
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("PD_DT"),              # char(10)
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("PAYMT_DRAG_CYC_DT"),  # char(10)
    F.rpad(F.col("BCA.CLM_PRCS_DT"), 10, " ").alias("PRCS_DT"),       # char(10)
    F.rpad(F.col("BCA.RUN_DT"), 10, " ").alias("RCVD_DT"),            # char(10)
    F.rpad(F.col("BCA.CLM_SVC_DT_BEG"), 10, " ").alias("SVC_STRT_DT"),# char(10)
    F.rpad(F.col("BCA.CLM_SVC_DT_END"), 10, " ").alias("SVC_END_DT"), # char(10)
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("SMLR_ILNS_DT"),       # char(10)
    F.rpad(F.col("BCA.RUN_DT"), 10, " ").alias("STTUS_DT"),           # char(10)
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("WORK_UNABLE_BEG_DT"), # char(10)
    F.rpad(F.lit("2199-12-31"), 10, " ").alias("WORK_UNABLE_END_DT"), # char(10)
    F.lit(0.00).alias("ACDNT_AMT"),
    F.lit(0.00).alias("ACTL_PD_AMT"),
    F.lit(0.00).alias("ALLOW_AMT"),
    F.lit(0.00).alias("DSALW_AMT"),
    F.lit(0.00).alias("COINS_AMT"),
    F.lit(0.00).alias("CNSD_CHRG_AMT"),
    F.lit(0.00).alias("COPAY_AMT"),
    F.col("BCA.CLM_LN_TOT_ALL_SVC_CHRG_AMT").alias("CHRG_AMT"),
    F.lit(0.00).alias("DEDCT_AMT"),
    F.lit(0.00).alias("PAYBL_AMT"),
    F.lit(1).alias("CLM_CT"),
    # MBR_AGE => AGE(BCA.DOB, BCA.CLM_SVC_DT_BEG) is a user function? We'll assume it's available as-is:
    F.expr("AGE(BCA.DOB, BCA.CLM_SVC_DT_BEG)").alias("MBR_AGE"),
    F.col("BCA.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("BCA.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.rpad(F.lit("NA"), 18, " ").alias("DOC_TX_ID"),             # char(18)
    F.rpad(F.lit("NA"), 15, " ").alias("MCAID_RESUB_NO"),        # char(15)
    F.rpad(F.lit("NA"), 12, " ").alias("MCARE_ID"),              # char(12)
    F.rpad(F.col("BCA.MBR_SFX_NO"), 2, " ").alias("MBR_SFX_NO"), # char(2)
    F.lit("NA").alias("PATN_ACCT_NO"),
    F.rpad(F.lit("NA"), 16, " ").alias("PAYMT_REF_ID"),          # char(16)
    F.rpad(F.lit("NA"), 12, " ").alias("PROV_AGMNT_ID"),         # char(12)
    F.lit("NA").alias("RFRNG_PROV_TX"),
    F.rpad(
        F.when(F.isnull(F.col("BCA.SUB_ID")), F.lit("NA")).otherwise(F.col("BCA.SUB_ID")),
        14, " "
    ).alias("SUB_ID"),                                           # char(14)
    F.lit(0.00).alias("REMIT_SUPRSION_AMT"),
    F.lit("NA").alias("MCAID_STTUS_ID"),
    F.lit(0.00).alias("PATN_PD_AMT"),
    F.when(F.col("BCA.DIAG_CDNG_TYP")=="9", F.lit("ICD9"))
     .when(F.col("BCA.DIAG_CDNG_TYP")=="0", F.lit("ICD10"))
     .otherwise(F.lit("NA"))
     .alias("CLM_SUBMT_ICD_VRSN_CD"),
    F.col("BCA.PERFORMING_PROV_ID").alias("PERFORMING_PROV_ID"),
    F.rpad(F.col("BCA.DOB"),10," ").alias("DOB")  # char(10)
).cache()

df_MedtrakClmTrns = df_MedtrakClmTrns_temp

################################################################################
# alpha_pfx (CTransformerStage)
# Primary Link: df_MedtrakClmTrns as MedtrakClmTrns
# Lookup Link 1: df_sub_alpha_pfx_lkup as sub_alpha_pfx_lkup (left join on SUB_CK = SUB_UNIQ_KEY)
# Lookup Link 2: df_mbr_enr_lkup as mbr_enr_lkup (left join on CLM_ID = CLM_ID)
################################################################################

df_MedtrakClmTrns_alias = df_MedtrakClmTrns.alias("MedtrakClmTrns")
df_sub_alpha_pfx_lkup_alias = df_sub_alpha_pfx_lkup.alias("sub_alpha_pfx_lkup")
df_mbr_enr_lkup_alias = df_mbr_enr_lkup.alias("mbr_enr_lkup")

df_joined_alpha_pfx = (
    df_MedtrakClmTrns_alias
    .join(
        df_sub_alpha_pfx_lkup_alias,
        on=(F.col("MedtrakClmTrns.SUB_CK") == F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY")),
        how="left"
    )
    .join(
        df_mbr_enr_lkup_alias,
        on=(F.col("MedtrakClmTrns.CLM_ID") == F.col("mbr_enr_lkup.CLM_ID")),
        how="left"
    )
)

df_Transform_temp = df_joined_alpha_pfx.select(
    F.col("MedtrakClmTrns.JOB_EXCTN_RCRD_ERR_SK"),
    F.col("MedtrakClmTrns.INSRT_UPDT_CD"),
    F.col("MedtrakClmTrns.DISCARD_IN"),
    F.col("MedtrakClmTrns.PASS_THRU_IN"),
    F.col("MedtrakClmTrns.FIRST_RECYC_DT"),
    F.col("MedtrakClmTrns.ERR_CT"),
    F.col("MedtrakClmTrns.RECYCLE_CT"),
    F.col("MedtrakClmTrns.SRC_SYS_CD"),
    F.col("MedtrakClmTrns.PRI_KEY_STRING"),
    F.col("MedtrakClmTrns.CLM_SK").alias("CLM_SK"),
    F.col("MedtrakClmTrns.SRC_SYS_CD_SK"),
    F.col("MedtrakClmTrns.CLM_ID"),
    F.col("MedtrakClmTrns.CRT_RUN_CYC_EXCTN_SK"),
    F.col("MedtrakClmTrns.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MedtrakClmTrns.ADJ_FROM_CLM"),
    F.col("MedtrakClmTrns.ADJ_TO_CLM"),
    F.when(F.isnull(F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY")), F.lit("UNK"))
     .otherwise(F.col("sub_alpha_pfx_lkup.ALPHA_PFX_CD")).alias("ALPHA_PFX_CD"),
    F.col("MedtrakClmTrns.CLM_EOB_EXCD"),
    F.when(F.isnull(F.col("mbr_enr_lkup.CLM_ID")), F.lit("UNK"))
     .otherwise(F.col("mbr_enr_lkup.CLS_ID")).alias("CLS"),
    F.when(F.isnull(F.col("mbr_enr_lkup.CLM_ID")), F.lit("UNK"))
     .otherwise(F.col("mbr_enr_lkup.CLS_PLN_ID")).alias("CLS_PLN"),
    F.when(F.isnull(F.col("mbr_enr_lkup.CLM_ID")), F.lit("UNK"))
     .otherwise(F.col("mbr_enr_lkup.EXPRNC_CAT_CD")).alias("EXPRNC_CAT"),
    F.when(F.isnull(F.col("mbr_enr_lkup.CLM_ID")), F.lit("UNK"))
     .otherwise(F.col("mbr_enr_lkup.FNCL_LOB_CD")).alias("FNCL_LOB_NO"),
    F.col("MedtrakClmTrns.GRP"),
    F.col("MedtrakClmTrns.MBR_CK"),
    F.col("MedtrakClmTrns.NTWK"),
    F.when(F.isnull(F.col("mbr_enr_lkup.CLM_ID")), F.lit("UNK"))
     .otherwise(F.trim(F.col("mbr_enr_lkup.PROD_ID"))).alias("PROD"),
    F.when(F.isnull(F.col("mbr_enr_lkup.CLM_ID")), F.lit("UNK"))
     .otherwise(F.col("mbr_enr_lkup.SUBGRP_ID")).alias("SUBGRP"),
    F.col("MedtrakClmTrns.SUB_CK"),
    F.col("MedtrakClmTrns.CLM_ACDNT_CD"),
    F.col("MedtrakClmTrns.CLM_ACDNT_ST_CD"),
    F.col("MedtrakClmTrns.CLM_ACTIVATING_BCBS_PLN_CD"),
    F.col("MedtrakClmTrns.CLM_AGMNT_SRC_CD"),
    F.col("MedtrakClmTrns.CLM_BTCH_ACTN_CD"),
    F.col("MedtrakClmTrns.CLM_CAP_CD"),
    F.col("MedtrakClmTrns.CLM_CAT_CD"),
    F.col("MedtrakClmTrns.CLM_CHK_CYC_OVRD_CD"),
    F.col("MedtrakClmTrns.CLM_COB_CD"),
    F.col("MedtrakClmTrns.FINL_DISP_CD"),
    F.col("MedtrakClmTrns.CLM_INPT_METH_CD"),
    F.col("MedtrakClmTrns.CLM_INPT_SRC_CD"),
    F.col("MedtrakClmTrns.CLM_IPP_CD"),
    F.col("MedtrakClmTrns.CLM_NTWK_STTUS_CD"),
    F.col("MedtrakClmTrns.CLM_NONPAR_PROV_PFX_CD"),
    F.col("MedtrakClmTrns.CLM_OTHER_BNF_CD"),
    F.col("MedtrakClmTrns.CLM_PAYE_CD"),
    F.col("MedtrakClmTrns.CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.col("MedtrakClmTrns.CLM_SVC_DEFN_PFX_CD"),
    F.col("MedtrakClmTrns.CLM_SVC_PROV_SPEC_CD"),
    F.col("MedtrakClmTrns.CLM_SVC_PROV_TYP_CD"),
    F.col("MedtrakClmTrns.CLM_STTUS_CD"),
    F.col("MedtrakClmTrns.CLM_SUBMT_BCBS_PLN_CD"),
    F.col("MedtrakClmTrns.CLM_SUB_BCBS_PLN_CD"),
    F.col("MedtrakClmTrns.CLM_SUBTYP_CD"),
    F.col("MedtrakClmTrns.CLM_TYP_CD"),
    F.col("MedtrakClmTrns.ATCHMT_IN"),
    F.col("MedtrakClmTrns.CLM_CLNCL_EDIT_CD"),
    F.col("MedtrakClmTrns.COBRA_CLM_IN"),
    F.col("MedtrakClmTrns.FIRST_PASS_IN"),
    F.col("MedtrakClmTrns.HOST_IN"),
    F.col("MedtrakClmTrns.LTR_IN"),
    F.col("MedtrakClmTrns.MCARE_ASG_IN"),
    F.col("MedtrakClmTrns.NOTE_IN"),
    F.col("MedtrakClmTrns.PCA_AUDIT_IN"),
    F.col("MedtrakClmTrns.PCP_SUBMT_IN"),
    F.col("MedtrakClmTrns.PROD_OOA_IN"),
    F.col("MedtrakClmTrns.ACDNT_DT"),
    F.col("MedtrakClmTrns.INPT_DT"),
    F.col("MedtrakClmTrns.MBR_PLN_ELIG_DT"),
    F.col("MedtrakClmTrns.NEXT_RVW_DT"),
    F.col("MedtrakClmTrns.PD_DT"),
    F.col("MedtrakClmTrns.PAYMT_DRAG_CYC_DT"),
    F.col("MedtrakClmTrns.PRCS_DT"),
    F.col("MedtrakClmTrns.RCVD_DT"),
    F.col("MedtrakClmTrns.SVC_STRT_DT"),
    F.col("MedtrakClmTrns.SVC_END_DT"),
    F.col("MedtrakClmTrns.SMLR_ILNS_DT"),
    F.col("MedtrakClmTrns.STTUS_DT"),
    F.col("MedtrakClmTrns.WORK_UNABLE_BEG_DT"),
    F.col("MedtrakClmTrns.WORK_UNABLE_END_DT"),
    F.col("MedtrakClmTrns.ACDNT_AMT"),
    F.col("MedtrakClmTrns.ACTL_PD_AMT"),
    F.col("MedtrakClmTrns.ALLOW_AMT"),
    F.col("MedtrakClmTrns.DSALW_AMT"),
    F.col("MedtrakClmTrns.COINS_AMT"),
    F.col("MedtrakClmTrns.CNSD_CHRG_AMT"),
    F.col("MedtrakClmTrns.COPAY_AMT"),
    F.col("MedtrakClmTrns.CHRG_AMT"),
    F.col("MedtrakClmTrns.DEDCT_AMT"),
    F.col("MedtrakClmTrns.PAYBL_AMT"),
    F.col("MedtrakClmTrns.CLM_CT"),
    F.col("MedtrakClmTrns.MBR_AGE"),
    F.col("MedtrakClmTrns.ADJ_FROM_CLM_ID"),
    F.col("MedtrakClmTrns.ADJ_TO_CLM_ID"),
    F.col("MedtrakClmTrns.DOC_TX_ID"),
    F.col("MedtrakClmTrns.MCAID_RESUB_NO"),
    F.col("MedtrakClmTrns.MCARE_ID"),
    F.col("MedtrakClmTrns.MBR_SFX_NO"),
    F.col("MedtrakClmTrns.PATN_ACCT_NO"),
    F.col("MedtrakClmTrns.PAYMT_REF_ID"),
    F.col("MedtrakClmTrns.PROV_AGMNT_ID"),
    F.col("MedtrakClmTrns.RFRNG_PROV_TX"),
    F.col("MedtrakClmTrns.SUB_ID"),
    F.rpad(F.lit(""),1," ").alias("PRPR_ENTITY"),             # char(1) = @NULL => leaving as empty
    F.rpad(F.lit("NA"),18," ").alias("PCA_TYP_CD"),           # char(18)
    F.lit("NA").alias("REL_PCA_CLM_ID"),
    F.lit("NA").alias("CLCL_MICRO_ID"),
    F.rpad(F.lit("N"),1," ").alias("CLM_UPDT_SW"),            # char(1)
    F.col("MedtrakClmTrns.REMIT_SUPRSION_AMT"),
    F.col("MedtrakClmTrns.MCAID_STTUS_ID"),
    F.col("MedtrakClmTrns.PATN_PD_AMT"),
    F.col("MedtrakClmTrns.CLM_SUBMT_ICD_VRSN_CD"),
    F.rpad(F.lit(""),0," ").alias("CLM_TXNMY_CD")  # '' => no length given, treat as empty
)

df_Transform = df_Transform_temp

################################################################################
# Snapshot (CTransformerStage)
# Primary Link: df_Transform as Transform
# Output pins: Pkey -> ClmPK, Snapshot -> Transformer
################################################################################

df_Transform_alias = df_Transform.alias("Transform")

# Pkey columns
df_Pkey = df_Transform_alias.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("Transform.INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("Transform.DISCARD_IN"),1," ").alias("DISCARD_IN"),
    F.rpad(F.col("Transform.PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT"),
    F.col("Transform.RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING"),
    F.col("Transform.CLM_SK"),
    F.col("Transform.SRC_SYS_CD_SK"),
    F.rpad(F.col("Transform.CLM_ID"),18," ").alias("CLM_ID"),
    F.col("Transform.CRT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("Transform.ADJ_FROM_CLM"),12," ").alias("ADJ_FROM_CLM"),
    F.rpad(F.col("Transform.ADJ_TO_CLM"),12," ").alias("ADJ_TO_CLM"),
    F.rpad(F.col("Transform.ALPHA_PFX_CD"),3," ").alias("ALPHA_PFX_CD"),
    F.rpad(F.col("Transform.CLM_EOB_EXCD"),3," ").alias("CLM_EOB_EXCD"),
    F.rpad(F.col("Transform.CLS"),4," ").alias("CLS"),
    F.rpad(F.col("Transform.CLS_PLN"),8," ").alias("CLS_PLN"),
    F.rpad(F.col("Transform.EXPRNC_CAT"),4," ").alias("EXPRNC_CAT"),
    F.rpad(F.col("Transform.FNCL_LOB_NO"),4," ").alias("FNCL_LOB_NO"),
    F.rpad(F.col("Transform.GRP"),8," ").alias("GRP"),
    F.col("Transform.MBR_CK"),
    F.rpad(F.col("Transform.NTWK"),12," ").alias("NTWK"),
    F.rpad(F.col("Transform.PROD"),8," ").alias("PROD"),
    F.rpad(F.col("Transform.SUBGRP"),4," ").alias("SUBGRP"),
    F.col("Transform.SUB_CK"),
    F.rpad(F.col("Transform.CLM_ACDNT_CD"),10," ").alias("CLM_ACDNT_CD"),
    F.rpad(F.col("Transform.CLM_ACDNT_ST_CD"),2," ").alias("CLM_ACDNT_ST_CD"),
    F.rpad(F.col("Transform.CLM_ACTIVATING_BCBS_PLN_CD"),10," ").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
    F.rpad(F.col("Transform.CLM_AGMNT_SRC_CD"),10," ").alias("CLM_AGMNT_SRC_CD"),
    F.rpad(F.col("Transform.CLM_BTCH_ACTN_CD"),1," ").alias("CLM_BTCH_ACTN_CD"),
    F.rpad(F.col("Transform.CLM_CAP_CD"),10," ").alias("CLM_CAP_CD"),
    F.rpad(F.col("Transform.CLM_CAT_CD"),10," ").alias("CLM_CAT_CD"),
    F.rpad(F.col("Transform.CLM_CHK_CYC_OVRD_CD"),1," ").alias("CLM_CHK_CYC_OVRD_CD"),
    F.rpad(F.col("Transform.CLM_COB_CD"),1," ").alias("CLM_COB_CD"),
    F.col("Transform.FINL_DISP_CD"),
    F.rpad(F.col("Transform.CLM_INPT_METH_CD"),1," ").alias("CLM_INPT_METH_CD"),
    F.rpad(F.col("Transform.CLM_INPT_SRC_CD"),10," ").alias("CLM_INPT_SRC_CD"),
    F.rpad(F.col("Transform.CLM_IPP_CD"),10," ").alias("CLM_IPP_CD"),
    F.rpad(F.col("Transform.CLM_NTWK_STTUS_CD"),2," ").alias("CLM_NTWK_STTUS_CD"),
    F.rpad(F.col("Transform.CLM_NONPAR_PROV_PFX_CD"),4," ").alias("CLM_NONPAR_PROV_PFX_CD"),
    F.rpad(F.col("Transform.CLM_OTHER_BNF_CD"),1," ").alias("CLM_OTHER_BNF_CD"),
    F.rpad(F.col("Transform.CLM_PAYE_CD"),1," ").alias("CLM_PAYE_CD"),
    F.rpad(F.col("Transform.CLM_PRCS_CTL_AGNT_PFX_CD"),4," ").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.rpad(F.col("Transform.CLM_SVC_DEFN_PFX_CD"),4," ").alias("CLM_SVC_DEFN_PFX_CD"),
    F.rpad(F.col("Transform.CLM_SVC_PROV_SPEC_CD"),10," ").alias("CLM_SVC_PROV_SPEC_CD"),
    F.rpad(F.col("Transform.CLM_SVC_PROV_TYP_CD"),10," ").alias("CLM_SVC_PROV_TYP_CD"),
    F.rpad(F.col("Transform.CLM_STTUS_CD"),2," ").alias("CLM_STTUS_CD"),
    F.rpad(F.col("Transform.CLM_SUBMT_BCBS_PLN_CD"),10," ").alias("CLM_SUBMT_BCBS_PLN_CD"),
    F.rpad(F.col("Transform.CLM_SUB_BCBS_PLN_CD"),10," ").alias("CLM_SUB_BCBS_PLN_CD"),
    F.rpad(F.col("Transform.CLM_SUBTYP_CD"),10," ").alias("CLM_SUBTYP_CD"),
    F.rpad(F.col("Transform.CLM_TYP_CD"),1," ").alias("CLM_TYP_CD"),
    F.rpad(F.col("Transform.ATCHMT_IN"),1," ").alias("ATCHMT_IN"),
    F.rpad(F.col("Transform.CLM_CLNCL_EDIT_CD"),1," ").alias("CLM_CLNCL_EDIT_CD"),
    F.rpad(F.col("Transform.COBRA_CLM_IN"),1," ").alias("COBRA_CLM_IN"),
    F.rpad(F.col("Transform.FIRST_PASS_IN"),1," ").alias("FIRST_PASS_IN"),
    F.rpad(F.col("Transform.HOST_IN"),1," ").alias("HOST_IN"),
    F.rpad(F.col("Transform.LTR_IN"),1," ").alias("LTR_IN"),
    F.rpad(F.col("Transform.MCARE_ASG_IN"),1," ").alias("MCARE_ASG_IN"),
    F.rpad(F.col("Transform.NOTE_IN"),1," ").alias("NOTE_IN"),
    F.rpad(F.col("Transform.PCA_AUDIT_IN"),1," ").alias("PCA_AUDIT_IN"),
    F.rpad(F.col("Transform.PCP_SUBMT_IN"),1," ").alias("PCP_SUBMT_IN"),
    F.rpad(F.col("Transform.PROD_OOA_IN"),1," ").alias("PROD_OOA_IN"),
    F.rpad(F.col("Transform.ACDNT_DT"),10," ").alias("ACDNT_DT"),
    F.rpad(F.col("Transform.INPT_DT"),10," ").alias("INPT_DT"),
    F.rpad(F.col("Transform.MBR_PLN_ELIG_DT"),10," ").alias("MBR_PLN_ELIG_DT"),
    F.rpad(F.col("Transform.NEXT_RVW_DT"),10," ").alias("NEXT_RVW_DT"),
    F.rpad(F.col("Transform.PD_DT"),10," ").alias("PD_DT"),
    F.rpad(F.col("Transform.PAYMT_DRAG_CYC_DT"),10," ").alias("PAYMT_DRAG_CYC_DT"),
    F.rpad(F.col("Transform.PRCS_DT"),10," ").alias("PRCS_DT"),
    F.rpad(F.col("Transform.RCVD_DT"),10," ").alias("RCVD_DT"),
    F.rpad(F.col("Transform.SVC_STRT_DT"),10," ").alias("SVC_STRT_DT"),
    F.rpad(F.col("Transform.SVC_END_DT"),10," ").alias("SVC_END_DT"),
    F.rpad(F.col("Transform.SMLR_ILNS_DT"),10," ").alias("SMLR_ILNS_DT"),
    F.rpad(F.col("Transform.STTUS_DT"),10," ").alias("STTUS_DT"),
    F.rpad(F.col("Transform.WORK_UNABLE_BEG_DT"),10," ").alias("WORK_UNABLE_BEG_DT"),
    F.rpad(F.col("Transform.WORK_UNABLE_END_DT"),10," ").alias("WORK_UNABLE_END_DT"),
    F.col("Transform.ACDNT_AMT"),
    F.col("Transform.ACTL_PD_AMT"),
    F.col("Transform.ALLOW_AMT"),
    F.col("Transform.DSALW_AMT"),
    F.col("Transform.COINS_AMT"),
    F.col("Transform.CNSD_CHRG_AMT"),
    F.col("Transform.COPAY_AMT"),
    F.col("Transform.CHRG_AMT"),
    F.col("Transform.DEDCT_AMT"),
    F.col("Transform.PAYBL_AMT"),
    F.col("Transform.CLM_CT"),
    F.col("Transform.MBR_AGE"),
    F.col("Transform.ADJ_FROM_CLM_ID"),
    F.col("Transform.ADJ_TO_CLM_ID"),
    F.rpad(F.col("Transform.DOC_TX_ID"),18," ").alias("DOC_TX_ID"),
    F.rpad(F.col("Transform.MCAID_RESUB_NO"),15," ").alias("MCAID_RESUB_NO"),
    F.rpad(F.col("Transform.MCARE_ID"),12," ").alias("MCARE_ID"),
    F.rpad(F.col("Transform.MBR_SFX_NO"),2," ").alias("MBR_SFX_NO"),
    F.col("Transform.PATN_ACCT_NO"),
    F.rpad(F.col("Transform.PAYMT_REF_ID"),16," ").alias("PAYMT_REF_ID"),
    F.rpad(F.col("Transform.PROV_AGMNT_ID"),12," ").alias("PROV_AGMNT_ID"),
    F.col("Transform.RFRNG_PROV_TX"),
    F.rpad(F.col("Transform.SUB_ID"),14," ").alias("SUB_ID"),
    F.rpad(F.col("Transform.PRPR_ENTITY"),1," ").alias("PRPR_ENTITY"),
    F.rpad(F.col("Transform.PCA_TYP_CD"),18," ").alias("PCA_TYP_CD"),
    F.col("Transform.REL_PCA_CLM_ID"),
    F.col("Transform.CLCL_MICRO_ID"),
    F.rpad(F.col("Transform.CLM_UPDT_SW"),1," ").alias("CLM_UPDT_SW"),
    F.col("Transform.REMIT_SUPRSION_AMT"),
    F.col("Transform.MCAID_STTUS_ID"),
    F.col("Transform.PATN_PD_AMT"),
    F.col("Transform.CLM_SUBMT_ICD_VRSN_CD"),
    F.col("Transform.CLM_TXNMY_CD"),
    F.rpad(F.lit("N"),1," ").alias("BILL_PAYMT_EXCL_IN")  # char(1)
)

# Snapshot columns
df_Snapshot = df_Transform_alias.select(
    F.rpad(F.col("Transform.CLM_ID"),12," ").alias("CLM_ID"),  # char(12) PK
    F.col("Transform.MBR_CK"),
    F.rpad(F.col("Transform.GRP"),8," ").alias("GRP"),  # char(8)
    F.col("Transform.SVC_STRT_DT"),
    F.col("Transform.CHRG_AMT"),
    F.col("Transform.PAYBL_AMT"),
    F.rpad(F.col("Transform.EXPRNC_CAT"),4," ").alias("EXPRNC_CAT"),  # char(4)
    F.rpad(F.col("Transform.FNCL_LOB_NO"),4," ").alias("FNCL_LOB_NO"),# char(4)
    F.col("Transform.CLM_CT"),
    F.rpad(F.col("Transform.PCA_TYP_CD"),18," ").alias("PCA_TYP_CD"),  # char(18)
    F.rpad(F.col("Transform.CLM_STTUS_CD"),2," ").alias("CLM_STTUS_CD"),  # char(2)
    F.rpad(F.col("Transform.CLM_CAT_CD"),10," ").alias("CLM_CAT_CD")      # char(10)
)

################################################################################
# ClmPK (CContainerStage) - Shared Container
################################################################################
params_ClmPK = {
    "CurrRunCycle": RunCycle
}
df_Key = ClmPK(df_Pkey, params_ClmPK)

################################################################################
# BCAFepClmExtr (CSeqFileStage) - input df_Key
################################################################################
# Write to file in directory "key", name "BCAFEPMedClmExtr.BCAFEPMedClm.dat.#RunID#"
output_file_BCAFepClmExtr = f"{adls_path}/key/BCAFEPMedClmExtr.BCAFEPMedClm.dat.{RunID}"

# Select columns in correct order, applying rpad if needed was already done in df_Key.
df_final_BCAFepClmExtr = df_Key.select(*df_Key.columns)

write_files(
    df_final_BCAFepClmExtr,
    output_file_BCAFepClmExtr,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

################################################################################
# Transformer after Snapshot => stage variables => outputs => B_CLM
################################################################################

# Input: df_Snapshot => "Snapshot"
df_Snapshot_alias = df_Snapshot.alias("Snapshot")

# Stage variables (expressions using user-defined functions):
#   ExpCatCdSk = GetFkeyExprncCat('FACETS', 0, Snapshot.EXPRNC_CAT, 'N')
#   GrpSk = GetFkeyGrp('FACETS', 0, Snapshot.GRP, 'N')
#   MbrSk = GetFkeyMbr('FACETS', 0, Snapshot.MBR_CK, 'N')
#   FnclLobSk = GetFkeyFnclLob("PSI", 0, Snapshot.FNCL_LOB_NO, 'N')
#   PcaTypCdSk = GetFkeyCodes('FACETS', 0, "PERSONAL CARE ACCOUNT PROCESSING", Snapshot.PCA_TYP_CD, 'N')
#   ClmSttusCdSk = GetFkeyCodes(SrcSysCd, 0, "CLAIM STATUS", Snapshot.CLM_STTUS_CD, 'N')
#   ClmCatCdSk = GetFkeyCodes(SrcSysCd, 0, "CLAIM CATEGORY", Snapshot.CLM_CAT_CD, 'X')

df_Transformer_temp = df_Snapshot_alias.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),  # primary key per design
    F.col("Snapshot.CLM_ID").alias("CLM_ID"),  # primary key
    F.expr("GetFkeyCodes(SrcSysCd, 0, 'CLAIM STATUS', Snapshot.CLM_STTUS_CD, 'N')").alias("CLM_STTUS_CD_SK"),
    F.expr("GetFkeyCodes(SrcSysCd, 0, 'CLAIM CATEGORY', Snapshot.CLM_CAT_CD, 'X')").alias("CLM_CAT_CD_SK"),
    F.expr("GetFkeyExprncCat('FACETS', 0, Snapshot.EXPRNC_CAT, 'N')").alias("EXPRNC_CAT_SK"),
    F.expr("GetFkeyFnclLob('PSI', 0, Snapshot.FNCL_LOB_NO, 'N')").alias("FNCL_LOB_SK"),
    F.expr("GetFkeyGrp('FACETS', 0, Snapshot.GRP, 'N')").alias("GRP_SK"),
    F.expr("GetFkeyMbr('FACETS', 0, Snapshot.MBR_CK, 'N')").alias("MBR_SK"),
    F.rpad(F.col("Snapshot.SVC_STRT_DT"),10," ").alias("SVC_STRT_DT_SK"),  # char(10)
    F.col("Snapshot.CHRG_AMT").alias("CHRG_AMT"),
    F.col("Snapshot.PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("Snapshot.CLM_CT").alias("CLM_CT"),
    F.expr("GetFkeyCodes('FACETS', 0, 'PERSONAL CARE ACCOUNT PROCESSING', Snapshot.PCA_TYP_CD, 'N')").alias("PCA_TYP_CD_SK")
)

df_Transformer = df_Transformer_temp

################################################################################
# B_CLM (CSeqFileStage) - write to file
################################################################################
output_file_B_CLM = f"{adls_path}/load/B_CLM.{SrcSysCd}.dat.{RunID}"

df_final_B_CLM = df_Transformer.select(*df_Transformer.columns)

write_files(
    df_final_B_CLM,
    output_file_B_CLM,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)