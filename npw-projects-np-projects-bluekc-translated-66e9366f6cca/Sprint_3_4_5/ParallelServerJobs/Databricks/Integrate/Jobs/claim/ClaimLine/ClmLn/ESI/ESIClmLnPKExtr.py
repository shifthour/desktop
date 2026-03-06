# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  ESIClmExtrSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   
# MAGIC          *  This job must be run after ArgusClmLand
# MAGIC          *  UNIX file K_CLM.dat is removed in Before-job to remove previous keys incase of earlier abend.  File writting is set to append.  
# MAGIC          *  Assigns / creates claim primary key surrogate keys for all records processed in current run.  Adjusted to/from claims don't need to be in hash file. 
# MAGIC          *  The primary key hash file hf_clm is the output of this job and is used by the following tables for keying
# MAGIC              CLM
# MAGIC              DRUG_CLM
# MAGIC              CLM_REMIT_HIST
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	Change Description				Development Project	Code Reviewer	Date Reviewed       
# MAGIC ------------------              --------------------     	------------------------	-----------------------------------------------------------------------	--------------------------------	-------------------------------	----------------------------       
# MAGIC Ralph Tucker	2008-08-25	3567 Primary Key     Original Programming                                               devlIDS                                   Steph Goddard        10/03/2008
# MAGIC                         
# MAGIC Tracy Davis             2009-04-09              TTR 477                  New field CLM_TRANSMITTAL_METH                 devlIDS                                   Steph Goddard         04/14/2009
# MAGIC 
# MAGIC 
# MAGIC SAndrew                 2014-01-30                     added hf_esi_clmln_pkextr_dedup                            devlnew                                                          Bhoomi Dasari         4/16/2014
# MAGIC                                                                      changed the source of the original claim id to be from the field CROSS_REFERENCE_ID.  And only if it does not equal 18 zeros.
# MAGIC Shashank A              2019-05-20          US 97615      Added CLM_LN_VBB_IN source column & adjusting the Filler length.                          Integrate Dev2     Hugh Sisson                2019-05-20

# MAGIC Get SK for primary key on input record
# MAGIC ESI Claim Primary Key Process.
# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code claim ID and seq_no
# MAGIC Output required by container but not used here
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# MAGIC %run ../../../../../../shared_containers/PrimaryKey/ClmLnLoadPK
# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schemaESIClmLand = StructType([
    StructField("RCRD_ID", DoubleType(), nullable=False),
    StructField("CLAIM_ID", StringType(), nullable=False),
    StructField("PRCSR_NO", DoubleType(), nullable=False),
    StructField("MEM_CK_KEY", IntegerType(), nullable=False),
    StructField("BTCH_NO", DoubleType(), nullable=False),
    StructField("PDX_NO", StringType(), nullable=False),
    StructField("RX_NO", DoubleType(), nullable=False),
    StructField("DT_FILLED", StringType(), nullable=False),
    StructField("NDC_NO", DoubleType(), nullable=False),
    StructField("DRUG_DESC", StringType(), nullable=False),
    StructField("NEW_RFL_CD", DoubleType(), nullable=False),
    StructField("METRIC_QTY", DoubleType(), nullable=False),
    StructField("DAYS_SUPL", DoubleType(), nullable=False),
    StructField("BSS_OF_CST_DTRM", StringType(), nullable=False),
    StructField("INGR_CST", DoubleType(), nullable=False),
    StructField("DISPNS_FEE", DoubleType(), nullable=False),
    StructField("COPAY_AMT", DoubleType(), nullable=False),
    StructField("SLS_TAX", DoubleType(), nullable=False),
    StructField("AMT_BILL", DoubleType(), nullable=False),
    StructField("PATN_FIRST_NM", StringType(), nullable=False),
    StructField("PATN_LAST_NM", StringType(), nullable=False),
    StructField("DOB", StringType(), nullable=False),
    StructField("SEX_CD", DoubleType(), nullable=False),
    StructField("CARDHLDR_ID_NO", StringType(), nullable=False),
    StructField("RELSHP_CD", DoubleType(), nullable=False),
    StructField("GRP_NO", StringType(), nullable=False),
    StructField("HOME_PLN", StringType(), nullable=False),
    StructField("HOST_PLN", DoubleType(), nullable=False),
    StructField("PRESCRIBER_ID", StringType(), nullable=False),
    StructField("DIAG_CD", StringType(), nullable=False),
    StructField("CARDHLDR_FIRST_NM", StringType(), nullable=False),
    StructField("CARDHLDR_LAST_NM", StringType(), nullable=False),
    StructField("PRAUTH_NO", DoubleType(), nullable=False),
    StructField("PA_MC_SC_NO", StringType(), nullable=False),
    StructField("CUST_LOC", DoubleType(), nullable=False),
    StructField("RESUB_CYC_CT", DoubleType(), nullable=False),
    StructField("DT_RX_WRTN", StringType(), nullable=False),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), nullable=False),
    StructField("PRSN_CD", StringType(), nullable=False),
    StructField("OTHR_COV_CD", DoubleType(), nullable=False),
    StructField("ELIG_CLRFCTN_CD", DoubleType(), nullable=False),
    StructField("CMPND_CD", DoubleType(), nullable=False),
    StructField("NO_OF_RFLS_AUTH", DoubleType(), nullable=False),
    StructField("LVL_OF_SVC", DoubleType(), nullable=False),
    StructField("RX_ORIG_CD", DoubleType(), nullable=False),
    StructField("RX_DENIAL_CLRFCTN", DoubleType(), nullable=False),
    StructField("PRI_PRESCRIBER", StringType(), nullable=False),
    StructField("CLNC_ID_NO", DoubleType(), nullable=False),
    StructField("DRUG_TYP", DoubleType(), nullable=False),
    StructField("PRESCRIBER_LAST_NM", StringType(), nullable=False),
    StructField("POSTAGE_AMT_CLMED", DoubleType(), nullable=False),
    StructField("UNIT_DOSE_IN", DoubleType(), nullable=False),
    StructField("OTHR_PAYOR_AMT", DoubleType(), nullable=False),
    StructField("BSS_OF_DAYS_SUPL_DTRM", DoubleType(), nullable=False),
    StructField("FULL_AWP", DoubleType(), nullable=False),
    StructField("EXPNSN_AREA", StringType(), nullable=False),
    StructField("MSTR_CAR", StringType(), nullable=False),
    StructField("SUB_CAR", StringType(), nullable=False),
    StructField("CLM_TYP", StringType(), nullable=False),
    StructField("ESI_SUB_GRP", StringType(), nullable=False),
    StructField("PLN_DSGNR", StringType(), nullable=False),
    StructField("ADJDCT_DT", StringType(), nullable=False),
    StructField("ADMIN_FEE", DoubleType(), nullable=False),
    StructField("CAP_AMT", DoubleType(), nullable=False),
    StructField("INGR_CST_SUB", DoubleType(), nullable=False),
    StructField("MBR_NON_COPAY_AMT", DoubleType(), nullable=False),
    StructField("MBR_PAY_CD", StringType(), nullable=False),
    StructField("INCNTV_FEE", DoubleType(), nullable=False),
    StructField("CLM_ADJ_AMT", DoubleType(), nullable=False),
    StructField("CLM_ADJ_CD", StringType(), nullable=False),
    StructField("FRMLRY_FLAG", StringType(), nullable=False),
    StructField("GNRC_CLS_NO", StringType(), nullable=False),
    StructField("THRPTC_CLS_AHFS", StringType(), nullable=False),
    StructField("PDX_TYP", StringType(), nullable=False),
    StructField("BILL_BSS_CD", StringType(), nullable=False),
    StructField("USL_AND_CUST_CHRG", DoubleType(), nullable=False),
    StructField("PD_DT", StringType(), nullable=False),
    StructField("BNF_CD", StringType(), nullable=False),
    StructField("DRUG_STRG", StringType(), nullable=False),
    StructField("ORIG_MBR", StringType(), nullable=False),
    StructField("DT_OF_INJURY", StringType(), nullable=False),
    StructField("FEE_AMT", DoubleType(), nullable=False),
    StructField("ESI_REF_NO", StringType(), nullable=False),
    StructField("CLNT_CUST_ID", StringType(), nullable=False),
    StructField("PLN_TYP", StringType(), nullable=False),
    StructField("ESI_ADJDCT_REF_NO", DoubleType(), nullable=False),
    StructField("ESI_ANCLRY_AMT", DoubleType(), nullable=False),
    StructField("GRP_ID", StringType(), nullable=False),
    StructField("SUBGRP_ID", StringType(), nullable=False),
    StructField("CLS_PLN_ID", StringType(), nullable=False),
    StructField("PAID_DATE", StringType(), nullable=False),
    StructField("PRTL_FILL_STTUS_CD", StringType(), nullable=False),
    StructField("ESI_BILL_DT", StringType(), nullable=False),
    StructField("FSA_VNDR_CD", StringType(), nullable=False),
    StructField("PICA_DRUG_CD", StringType(), nullable=False),
    StructField("AMT_CLMED", DoubleType(), nullable=False),
    StructField("AMT_DSALW", DoubleType(), nullable=False),
    StructField("FED_DRUG_CLS_CD", StringType(), nullable=False),
    StructField("DEDCT_AMT", DoubleType(), nullable=False),
    StructField("BNF_COPAY_100", StringType(), nullable=False),
    StructField("CLM_PRCS_TYP", StringType(), nullable=False),
    StructField("INDEM_HIER_TIER_NO", DoubleType(), nullable=False),
    StructField("FLR", StringType(), nullable=False),
    StructField("MCARE_D_COV_DRUG", StringType(), nullable=False),
    StructField("RETRO_LICS_CD", StringType(), nullable=False),
    StructField("RETRO_LICS_AMT", DoubleType(), nullable=False),
    StructField("LICS_SBSDY_AMT", DoubleType(), nullable=False),
    StructField("MED_B_DRUG", StringType(), nullable=False),
    StructField("MED_B_CLM", StringType(), nullable=False),
    StructField("PRESCRIBER_QLFR", StringType(), nullable=False),
    StructField("PRESCRIBER_ID_NPI", StringType(), nullable=False),
    StructField("PDX_QLFR", StringType(), nullable=False),
    StructField("PDX_ID_NPI", StringType(), nullable=False),
    StructField("HRA_APLD_AMT", DoubleType(), nullable=False),
    StructField("ESI_THER_CLS", DoubleType(), nullable=False),
    StructField("HIC_NO", StringType(), nullable=False),
    StructField("HRA_FLAG", StringType(), nullable=False),
    StructField("DOSE_CD", DoubleType(), nullable=False),
    StructField("LOW_INCM", StringType(), nullable=False),
    StructField("RTE_OF_ADMIN", StringType(), nullable=False),
    StructField("DEA_SCHD", DoubleType(), nullable=False),
    StructField("COPAY_BNF_OPT", DoubleType(), nullable=False),
    StructField("GNRC_PROD_IN_GPI", DoubleType(), nullable=False),
    StructField("PRESCRIBER_SPEC", StringType(), nullable=False),
    StructField("VAL_CD", StringType(), nullable=False),
    StructField("PRI_CARE_PDX", StringType(), nullable=False),
    StructField("OFC_OF_INSPECTOR_GNRL_OIG", StringType(), nullable=False),
    StructField("FLR3", StringType(), nullable=False),
    StructField("PSL_FMLY_MET_AMT", DoubleType(), nullable=False),
    StructField("PSL_MBR_MET_AMT", DoubleType(), nullable=False),
    StructField("PSL_FMLY_AMT", DoubleType(), nullable=False),
    StructField("DED_FMLY_MET_AMT", DoubleType(), nullable=False),
    StructField("DED_FMLY_AMT", DoubleType(), nullable=False),
    StructField("MOPS_FMLY_AMT", DoubleType(), nullable=False),
    StructField("MOPS_FMLY_MET_AMT", DoubleType(), nullable=False),
    StructField("MOPS_MBR_MET_AMT", DoubleType(), nullable=False),
    StructField("DED_MBR_MET_AMT", DoubleType(), nullable=False),
    StructField("PSL_APLD_AMT", DoubleType(), nullable=False),
    StructField("MOPS_APLD_AMT", DoubleType(), nullable=False),
    StructField("PAR_PDX_IND", StringType(), nullable=False),
    StructField("COPAY_PCT_AMT", DoubleType(), nullable=False),
    StructField("COPAY_FLAT_AMT", DoubleType(), nullable=False),
    StructField("CLM_TRANSMITTAL_METH", StringType(), nullable=False),
    StructField("PRESCRIPTION_NBR_2", StringType(), nullable=False),
    StructField("TRANSACTION_ID", StringType(), nullable=False),
    StructField("CROSS_REF_ID", StringType(), nullable=False),
    StructField("ADJDCT_TIMESTAMP", StringType(), nullable=False),
    StructField("CLM_LN_VBB_IN", StringType(), nullable=False),
    StructField("FLR4", StringType(), nullable=False)
])

df_ESIClmLand = (
    spark.read
    .option("header", "false")
    .option("quote", "\"")
    .schema(schemaESIClmLand)
    .csv(f"{adls_path}/verified/ESIDrugClmDaily_Land.dat.{RunID}")
)

df_Trans1_Regular = df_ESIClmLand.select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.col("CLAIM_ID").alias("CLM_ID"),
    F.lit(1).alias("CLM_LN_SEQ_NO")
)

df_Trans1_Adjustments = (
    df_ESIClmLand
    .filter(F.col("CROSS_REF_ID") != "000000000000000000")
    .select(
        F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
        F.col("CROSS_REF_ID").alias("CLM_ID"),
        F.lit(1).alias("CLM_LN_SEQ_NO")
    )
)

df_Collector = df_Trans1_Adjustments.union(df_Trans1_Regular)

df_hf_esi_clmln_pkey_extr_dedup = df_Collector.dropDuplicates(
    ["SRC_SYS_CD_SK","CLM_ID","CLM_LN_SEQ_NO"]
)

params_ClmLnLoadPK = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner,
    "ids_secret_name": ids_secret_name
}
df_ClmLnLoadPK = ClmLnLoadPK(df_hf_esi_clmln_pkey_extr_dedup, params_ClmLnLoadPK)

df_clm_ln_pk_lkup = df_ClmLnLoadPK.select(
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "CLM_LN_SK"
)

write_files(
    df_clm_ln_pk_lkup,
    "hf_clm_ln_pk_lkup.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)