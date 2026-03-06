# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2018 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  BCBSKCCommDrugExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Reads the PDX_CLM_STD_INPT_Land file and runs through primary key using shared container ClmLnPk
# MAGIC     
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                       Date                 Project/Altiris #      Change Description                                                                                              Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                     --------------------     ------------------------      -------------------------------------------------------------------------------------------------------------------------    --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kaushik Kapoor              2018-02-26      5828                       Original Programming: Reads data from PDX_CLM_STD_INPT_Land.dat file     IntegrateDev2                 Kalyan Neelam           2018-02-26
# MAGIC                                                                                               and creates an extract file for CLM_LN which will be read 
# MAGIC                                                                                                by IdsClmLnFKey job to create CLM_LN load file
# MAGIC Kaushik Kapoor              2018-03-16      5828                       Changed SVC_PRICE_RULE_ID from ' ' to 'NA'                                                  IntegrateDev2	   Jaideep Mankala       03/20/2018
# MAGIC Madhavan B                   2018-02-06      5792 	              Changed the datatype of the column                                                                   IntegrateDev1                  Kalyan Neelam          2018-02-08
# MAGIC                                                              		              NDC_UNIT_CT from SMALLINT to DECIMAL (21,10)
# MAGIC Rekha Radhakrishna     2020-02-27       6131                      Changed ALW_AMT calculation for OPTUMRX                                                  IntegrateDev2                  Hugh Sisson              2020-03-06
# MAGIC 
# MAGIC Goutham Kalidindi        10/22/2020       US-283560          Added MEDIMPACT  derivation for CLM_LN_POS_CD                                         IntegrateDev2                Reddy Sanam             2020-11-09
# MAGIC                                                                                              and PAYBL_AMT
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi        10/22/2020       US-283560          Added MEDIMPACT derivation for   ALLWD_AMT                                                IntegrateDev2                  Kalyan Neelam        2020-11-10
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi        11/11/2020       US-283560                           Changed MEDIMPACT derivation for   ALLWD_AMT                 IntegrateDev2                
# MAGIC                                                                                                            Added MEDIMPACT derivation for CNSD_CHRG_AMT
# MAGIC                                                                                                            CHRG_AMT  
# MAGIC 
# MAGIC                                                                                             
# MAGIC 
# MAGIC 
# MAGIC Rekha Radhakrishna     2020-07-27       6131                     Modified common file layout to include157 columns                                              IntegrateDev2                 Kalyan Neelam         2020-11-12
# MAGIC                                                                                              and updated CLM_LN_POS_CD mapping for MedD and ACA
# MAGIC 
# MAGIC Rekha Radhakrishna     2020-08-13       6131                    Modified source file common layout to include LOB_IN                                         IntegrateDev2       
# MAGIC Rekha Radhakrishna     2020-08-17       6131                    Mapped APC_ID and APC_STTUS_ID to Null for 'OPTUMRX'                            IntegrateDev2 
# MAGIC                     
# MAGIC Rekha Radhakrishna     2020-09-25       6131                    UpdatedCLM_LN_POS_CD for  'OPTUMRX'                                                         IntegrateDev2             Sravya Gorla              2020-09-12       
# MAGIC 
# MAGIC Amritha A J                      2023-07-31   US 589700           Added two new fields SNOMED_CT_CD,CVX_VCCN_CD with a default                IntegrateDevB	Harsha Ravuri	2023-08-31
# MAGIC                                                                                             value in BusinessRules stage and mapped it till target

# MAGIC Read the BCBSKCComm file created
# MAGIC Writing Sequential File to /key
# MAGIC Added new field MED_PDX_IND to the common layout file that is sent to IdsClmLnFkey job
# MAGIC which indicates whether the claim is Medical or Pharmacy
# MAGIC Indicator used :
# MAGIC MED - medical
# MAGIC PDX - Pharmacy
# MAGIC This container is used in:
# MAGIC ESIClmLnExtr
# MAGIC FctsClmLnDntlTrns
# MAGIC FctsClmLnMedTrns
# MAGIC MCSourceClmLnExtr
# MAGIC MedicaidClmLnExtr
# MAGIC NascoClmLnExtr
# MAGIC PcsClmLnExtr
# MAGIC PCTAClmLnExtr
# MAGIC WellDyneClmLnExtr
# MAGIC MedtrakClmLnExtr
# MAGIC BCBSKCCommClmLnExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# MAGIC %run ../../../../../../shared_containers/PrimaryKey/ClmLnPK
# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunID = get_widget_value('RunID','')
RunCycle = get_widget_value('RunCycle','')

# -----------------------
# Stage: BCBSKCCommClmLand (CSeqFileStage) - Read delimited file
# -----------------------
schema_bcbskccommclmland = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("FILE_RCVD_DT", StringType(), True),
    StructField("RCRD_ID", DoubleType(), True),
    StructField("PRCSR_NO", StringType(), True),
    StructField("BTCH_NO", DoubleType(), True),
    StructField("PDX_NO", DoubleType(), True),
    StructField("RX_NO", DoubleType(), True),
    StructField("FILL_DT", StringType(), True),
    StructField("NDC", DoubleType(), True),
    StructField("DRUG_DESC", StringType(), True),
    StructField("NEW_OR_RFL_CD", IntegerType(), True),
    StructField("METRIC_QTY", DoubleType(), True),
    StructField("DAYS_SUPL", DoubleType(), True),
    StructField("BSS_OF_CST_DTRM", StringType(), True),
    StructField("INGR_CST_AMT", DoubleType(), True),
    StructField("DISPNS_FEE_AMT", DoubleType(), True),
    StructField("COPAY_AMT", DoubleType(), True),
    StructField("SLS_TAX_AMT", DoubleType(), True),
    StructField("BILL_AMT", DoubleType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("BRTH_DT", StringType(), True),
    StructField("SEX_CD", DoubleType(), True),
    StructField("CARDHLDR_ID_NO", StringType(), True),
    StructField("RELSHP_CD", DoubleType(), True),
    StructField("GRP_NO", StringType(), True),
    StructField("HOME_PLN", StringType(), True),
    StructField("HOST_PLN", StringType(), True),
    StructField("PRSCRBR_ID", StringType(), True),
    StructField("DIAG_CD", StringType(), True),
    StructField("CARDHLDR_FIRST_NM", StringType(), True),
    StructField("CARDHLDR_LAST_NM", StringType(), True),
    StructField("PRAUTH_NO", StringType(), True),
    StructField("PA_MC_SC_NO", StringType(), True),
    StructField("CUST_LOC", DoubleType(), True),
    StructField("RESUB_CYC_CT", DoubleType(), True),
    StructField("RX_DT", StringType(), True),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), True),
    StructField("PRSN_CD", StringType(), True),
    StructField("OTHR_COV_CD", DoubleType(), True),
    StructField("ELIG_CLRFCTN_CD", DoubleType(), True),
    StructField("CMPND_CD", DoubleType(), True),
    StructField("NO_OF_RFLS_AUTH", DoubleType(), True),
    StructField("LVL_OF_SVC", DoubleType(), True),
    StructField("RX_ORIG_CD", DoubleType(), True),
    StructField("RX_DENIAL_CLRFCTN", DoubleType(), True),
    StructField("PRI_PRSCRBR", StringType(), True),
    StructField("CLNC_ID_NO", DoubleType(), True),
    StructField("DRUG_TYP", DoubleType(), True),
    StructField("PRSCRBR_LAST_NM", StringType(), True),
    StructField("POSTAGE_AMT", DoubleType(), True),
    StructField("UNIT_DOSE_IN", DoubleType(), True),
    StructField("OTHR_PAYOR_AMT", DoubleType(), True),
    StructField("BSS_OF_DAYS_SUPL_DTRM", DoubleType(), True),
    StructField("FULL_AVG_WHLSL_PRICE", DoubleType(), True),
    StructField("EXPNSN_AREA", StringType(), True),
    StructField("MSTR_CAR", StringType(), True),
    StructField("SUBCAR", StringType(), True),
    StructField("CLM_TYP", StringType(), True),
    StructField("SUBGRP", StringType(), True),
    StructField("PLN_DSGNR", StringType(), True),
    StructField("ADJDCT_DT", StringType(), True),
    StructField("ADMIN_FEE_AMT", DoubleType(), True),
    StructField("CAP_AMT", DoubleType(), True),
    StructField("INGR_CST_SUB_AMT", DoubleType(), True),
    StructField("MBR_NON_COPAY_AMT", DoubleType(), True),
    StructField("MBR_PAY_CD", StringType(), True),
    StructField("INCNTV_FEE_AMT", DoubleType(), True),
    StructField("CLM_ADJ_AMT", DoubleType(), True),
    StructField("CLM_ADJ_CD", StringType(), True),
    StructField("FRMLRY_FLAG", StringType(), True),
    StructField("GNRC_CLS_NO", StringType(), True),
    StructField("THRPTC_CLS_AHFS", StringType(), True),
    StructField("PDX_TYP", StringType(), True),
    StructField("BILL_BSS_CD", StringType(), True),
    StructField("USL_AND_CUST_CHRG_AMT", DoubleType(), True),
    StructField("PD_DT", StringType(), True),
    StructField("BNF_CD", StringType(), True),
    StructField("DRUG_STRG", StringType(), True),
    StructField("ORIG_MBR", StringType(), True),
    StructField("INJRY_DT", StringType(), True),
    StructField("FEE_AMT", DoubleType(), True),
    StructField("REF_NO", StringType(), True),
    StructField("CLNT_CUST_ID", StringType(), True),
    StructField("PLN_TYP", StringType(), True),
    StructField("ADJDCT_REF_NO", DoubleType(), True),
    StructField("ANCLRY_AMT", DoubleType(), True),
    StructField("CLNT_GNRL_PRPS_AREA", StringType(), True),
    StructField("PRTL_FILL_STTUS_CD", StringType(), True),
    StructField("BILL_DT", StringType(), True),
    StructField("FSA_VNDR_CD", StringType(), True),
    StructField("PICA_DRUG_CD", StringType(), True),
    StructField("CLM_AMT", DoubleType(), True),
    StructField("DSALW_AMT", DoubleType(), True),
    StructField("FED_DRUG_CLS_CD", StringType(), True),
    StructField("DEDCT_AMT", DoubleType(), True),
    StructField("BNF_COPAY_100", StringType(), True),
    StructField("CLM_PRCS_TYP", StringType(), True),
    StructField("INDEM_HIER_TIER_NO", DoubleType(), True),
    StructField("MCARE_D_COV_DRUG", StringType(), True),
    StructField("RETRO_LICS_CD", StringType(), True),
    StructField("RETRO_LICS_AMT", DoubleType(), True),
    StructField("LICS_SBSDY_AMT", DoubleType(), True),
    StructField("MCARE_B_DRUG", StringType(), True),
    StructField("MCARE_B_CLM", StringType(), True),
    StructField("PRSCRBR_QLFR", StringType(), True),
    StructField("PRSCRBR_NTNL_PROV_ID", StringType(), True),
    StructField("PDX_QLFR", StringType(), True),
    StructField("PDX_NTNL_PROV_ID", StringType(), True),
    StructField("HLTH_RMBRMT_ARGMT_APLD_AMT", DoubleType(), True),
    StructField("THER_CLS", DoubleType(), True),
    StructField("HIC_NO", StringType(), True),
    StructField("HLTH_RMBRMT_ARGMT_FLAG", StringType(), True),
    StructField("DOSE_CD", DoubleType(), True),
    StructField("LOW_INCM", StringType(), True),
    StructField("RTE_OF_ADMIN", StringType(), True),
    StructField("DEA_SCHD", DoubleType(), True),
    StructField("COPAY_BNF_OPT", DoubleType(), True),
    StructField("GNRC_PROD_IN", DoubleType(), True),
    StructField("PRSCRBR_SPEC", StringType(), True),
    StructField("VAL_CD", StringType(), True),
    StructField("PRI_CARE_PDX", StringType(), True),
    StructField("OFC_OF_INSPECTOR_GNRL", StringType(), True),
    StructField("PATN_SSN", StringType(), True),
    StructField("CARDHLDR_SSN", StringType(), True),
    StructField("CARDHLDR_BRTH_DT", StringType(), True),
    StructField("CARDHLDR_ADDR", StringType(), True),
    StructField("CARDHLDR_CITY", StringType(), True),
    StructField("CHADHLDR_ST", StringType(), True),
    StructField("CARDHLDR_ZIP_CD", StringType(), True),
    StructField("PSL_FMLY_MET_AMT", DoubleType(), True),
    StructField("PSL_MBR_MET_AMT", DoubleType(), True),
    StructField("PSL_FMLY_AMT", DoubleType(), True),
    StructField("DEDCT_FMLY_MET_AMT", StringType(), True),
    StructField("DEDCT_FMLY_AMT", DoubleType(), True),
    StructField("MOPS_FMLY_AMT", DoubleType(), True),
    StructField("MOPS_FMLY_MET_AMT", DoubleType(), True),
    StructField("MOPS_MBR_MET_AMT", DoubleType(), True),
    StructField("DEDCT_MBR_MET_AMT", DoubleType(), True),
    StructField("PSL_APLD_AMT", DoubleType(), True),
    StructField("MOPS_APLD_AMT", DoubleType(), True),
    StructField("PAR_PDX_IN", StringType(), True),
    StructField("COPAY_PCT_AMT", DoubleType(), True),
    StructField("COPAY_FLAT_AMT", DoubleType(), True),
    StructField("CLM_TRNSMSN_METH", StringType(), True),
    StructField("RX_NO_2012", DoubleType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_STTUS_CD", StringType(), True),
    StructField("ADJ_FROM_CLM_ID", StringType(), True),
    StructField("ADJ_TO_CLM_ID", StringType(), True),
    StructField("SUBMT_PROD_ID_QLFR", StringType(), True),
    StructField("CNTNGNT_THER_FLAG", StringType(), True),
    StructField("CNTNGNT_THER_SCHD", StringType(), True),
    StructField("CLNT_PATN_PAY_ATRBD_PROD_AMT", DoubleType(), True),
    StructField("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT", DoubleType(), True),
    StructField("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT", DoubleType(), True),
    StructField("CLNT_PATN_PAY_ATRBD_NTWK_AMT", DoubleType(), True),
    StructField("LOB_IN", StringType(), True)
])

df_bcbskccommclmland = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("escape", "\"")
    .option("header", "false")
    .schema(schema_bcbskccommclmland)
    .load(f"{adls_path}/verified/PDX_CLM_STD_INPT_Land_{SrcSysCd}.dat.{RunID}")
)

# -----------------------
# Stage: IDS_NPI (DB2Connector) - Read from IDS database
# -----------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_IDS_NPI = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT PROV.NTNL_PROV_ID, PROV.PROV_ID FROM {IDSOwner}.PROV PROV")
    .load()
)

# -----------------------
# Stage: hf_bcbskccomm_prov (CHashedFileStage) - Scenario A (intermediate hashed file)
#   Key column is NTNL_PROV_ID (PrimaryKey).
# -----------------------
# Deduplicate by NTNL_PROV_ID
df_natln_prov = dedup_sort(df_IDS_NPI, ["NTNL_PROV_ID"], [("NTNL_PROV_ID", "A")])

# -----------------------
# Stage: BusinessRules (CTransformerStage)
#   Primary link: df_bcbskccommclmland AS Extract
#   Lookup link: df_natln_prov AS natln_prov (left join on Extract.PDX_NTNL_PROV_ID = natln_prov.NTNL_PROV_ID)
# -----------------------
df_businessRules_joined = (
    df_bcbskccommclmland.alias("Extract")
    .join(
        df_natln_prov.alias("natln_prov"),
        on=[F.col("Extract.PDX_NTNL_PROV_ID") == F.col("natln_prov.NTNL_PROV_ID")],
        how="left"
    )
)

# Define stage variables as new columns
df_businessRules_stageVars = (
    df_businessRules_joined
    .withColumn(
        "svServicingProvID",
        F.when(F.col("Extract.PDX_NO").isNotNull(), F.col("Extract.PDX_NO"))
         .otherwise(
            F.when(F.col("natln_prov.NTNL_PROV_ID").isNotNull(), F.col("natln_prov.PROV_ID"))
             .otherwise(F.lit("UNK"))
         )
    )
    .withColumn(
        "svAllowedAmt",
        F.when(
            trim(F.substring("Extract.CLNT_GNRL_PRPS_AREA", 61, 2)) != F.lit("01"),
            (F.col("Extract.SLS_TAX_AMT") + F.col("Extract.INGR_CST_AMT") + F.col("Extract.DISPNS_FEE_AMT")) - F.col("Extract.OTHR_PAYOR_AMT")
        ).otherwise(
            F.col("Extract.INGR_CST_AMT") + F.col("Extract.DISPNS_FEE_AMT") + F.col("Extract.SLS_TAX_AMT")
        )
    )
    .withColumn(
        "svChrgAmt",
        F.when(F.col("Extract.SRC_SYS_CD") == F.lit("OPTUMRX"), F.col("Extract.FULL_AVG_WHLSL_PRICE"))
         .when(F.col("Extract.SRC_SYS_CD") == F.lit("MEDIMPACT"), F.col("Extract.CLM_AMT"))
         .otherwise(F.lit(0.00))
    )
    .withColumn(
        "svCnsdChrgAmt",
        F.when(F.col("Extract.SRC_SYS_CD") == F.lit("OPTUMRX"), F.col("Extract.FULL_AVG_WHLSL_PRICE"))
         .when(F.col("Extract.SRC_SYS_CD") == F.lit("MEDIMPACT"), F.col("Extract.CLM_AMT"))
         .otherwise(F.lit(0.00))
    )
    .withColumn(
        "svNdc",
        F.when(F.trim(F.col("Extract.NDC")).isNull(), F.lit("UNK"))
         .otherwise(F.col("Extract.NDC"))
    )
    .withColumn(
        "svCoinsAmt",
        F.when(F.col("Extract.SRC_SYS_CD") == F.lit("OPTUMRX"), F.col("Extract.COPAY_PCT_AMT"))
         .otherwise(F.lit(0.00))
    )
    .withColumn(
        "svMIAllowedAmt",
        F.when(
            F.col("Extract.OTHR_COV_CD") == F.lit("2"),
            (F.col("Extract.INGR_CST_AMT") + F.col("Extract.DISPNS_FEE_AMT") + F.col("Extract.SLS_TAX_AMT")) - F.col("Extract.OTHR_PAYOR_AMT")
        ).otherwise(
            F.col("Extract.INGR_CST_AMT") + F.col("Extract.DISPNS_FEE_AMT") + F.col("Extract.SLS_TAX_AMT")
        )
    )
)

# Now produce the output pin "Transform" with the exact column order and expressions:

df_BusinessRules_Transform_pre = df_businessRules_stageVars

# For each column, we apply the DataStage expression or WhereExpression. Also apply rpad if char/varchar with known length.
# Below we inline-literal the transformations for each output column in the specified order:

df_BusinessRules_Transform = df_BusinessRules_Transform_pre.select(
    # 1 JOB_EXCTN_RCRD_ERR_SK
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    # 2 INSRT_UPDT_CD char(10)
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    # 3 DISCARD_IN char(1)
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    # 4 PASS_THRU_IN char(1)
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    # 5 FIRST_RECYC_DT
    current_date().alias("FIRST_RECYC_DT"),
    # 6 ERR_CT
    F.lit(0).alias("ERR_CT"),
    # 7 RECYCLE_CT
    F.lit(0).alias("RECYCLE_CT"),
    # 8 SRC_SYS_CD
    F.col("Extract.SRC_SYS_CD").alias("SRC_SYS_CD"),
    # 9 PRI_KEY_STRING
    F.concat(F.col("Extract.SRC_SYS_CD"), F.lit(";"), F.col("Extract.CLM_ID"), F.lit(";"), F.lit("1")).alias("PRI_KEY_STRING"),
    # 10 CLM_LN_SK
    F.lit(0).alias("CLM_LN_SK"),
    # 11 CLM_ID
    F.col("Extract.CLM_ID").alias("CLM_ID"),
    # 12 CLM_LN_SEQ_NO
    F.lit(1).alias("CLM_LN_SEQ_NO"),
    # 13 CRT_RUN_CYC_EXCTN_SK
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    # 14 LAST_UPDT_RUN_CYC_EXCTN_SK
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    # 15 PROC_CD
    F.lit("NA").alias("PROC_CD"),
    # 16 SVC_PROV_ID
    F.col("svServicingProvID").alias("SVC_PROV_ID"),
    # 17 CLM_LN_DSALW_EXCD
    F.lit("NA").alias("CLM_LN_DSALW_EXCD"),
    # 18 CLM_LN_EOB_EXCD
    F.lit("NA").alias("CLM_LN_EOB_EXCD"),
    # 19 CLM_LN_FINL_DISP_CD
    F.lit("ACPTD").alias("CLM_LN_FINL_DISP_CD"),
    # 20 CLM_LN_LOB_CD
    F.lit("NA").alias("CLM_LN_LOB_CD"),
    # 21 CLM_LN_POS_CD
    F.when(
        F.col("Extract.SRC_SYS_CD") == F.lit("MEDIMPACT"),
        trim(F.col("Extract.CLNT_GNRL_PRPS_AREA"))
    ).otherwise(
        F.when(
            F.length(trim(F.substring("Extract.CLNT_GNRL_PRPS_AREA", 65, 2))) == 0,
            F.lit("NA")
        ).otherwise(
            F.when(
                F.col("Extract.SRC_SYS_CD") == F.lit("OPTUMRX"),
                trim(F.substring("Extract.CLNT_GNRL_PRPS_AREA", 65, 2))
            ).otherwise(F.lit("NA"))
        )
    ).alias("CLM_LN_POS_CD"),
    # 22 CLM_LN_PREAUTH_CD
    F.lit("NA").alias("CLM_LN_PREAUTH_CD"),
    # 23 CLM_LN_PREAUTH_SRC_CD
    F.lit("NA").alias("CLM_LN_PREAUTH_SRC_CD"),
    # 24 CLM_LN_PRICE_SRC_CD
    F.lit("NA").alias("CLM_LN_PRICE_SRC_CD"),
    # 25 CLM_LN_RFRL_CD
    F.lit("NA").alias("CLM_LN_RFRL_CD"),
    # 26 CLM_LN_RVNU_CD
    F.lit("NA").alias("CLM_LN_RVNU_CD"),
    # 27 CLM_LN_ROOM_PRICE_METH_CD char(2)
    F.rpad(F.lit("NA"), 2, " ").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    # 28 CLM_LN_ROOM_TYP_CD
    F.lit("NA").alias("CLM_LN_ROOM_TYP_CD"),
    # 29 CLM_LN_TOS_CD
    F.lit("NA").alias("CLM_LN_TOS_CD"),
    # 30 CLM_LN_UNIT_TYP_CD
    F.lit("NA").alias("CLM_LN_UNIT_TYP_CD"),
    # 31 CAP_LN_IN char(1)
    F.rpad(F.lit("N"), 1, " ").alias("CAP_LN_IN"),
    # 32 PRI_LOB_IN char(1)
    F.rpad(F.lit("N"), 1, " ").alias("PRI_LOB_IN"),
    # 33 SVC_END_DT
    F.col("Extract.FILL_DT").alias("SVC_END_DT"),
    # 34 SVC_STRT_DT
    F.col("Extract.FILL_DT").alias("SVC_STRT_DT"),
    # 35 AGMNT_PRICE_AMT
    F.lit(0.00).alias("AGMNT_PRICE_AMT"),
    # 36 ALW_AMT
    F.when(
        F.col("Extract.SRC_SYS_CD") == F.lit("OPTUMRX"),
        F.col("svAllowedAmt")
    ).otherwise(
        F.when(
            F.col("Extract.SRC_SYS_CD") == F.lit("MEDIMPACT"),
            F.col("svMIAllowedAmt")
        ).otherwise(F.lit(0.00))
    ).alias("ALW_AMT"),
    # 37 CHRG_AMT
    F.col("svChrgAmt").alias("CHRG_AMT"),
    # 38 COINS_AMT
    F.col("svCoinsAmt").alias("COINS_AMT"),
    # 39 CNSD_CHRG_AMT
    F.col("svCnsdChrgAmt").alias("CNSD_CHRG_AMT"),
    # 40 COPAY_AMT
    F.when(
        F.col("Extract.COPAY_AMT").isNull() | (F.length(F.trim(F.col("Extract.COPAY_AMT"))) == 0),
        F.lit("0.00")
    ).otherwise(trim(F.col("Extract.COPAY_AMT"))).alias("COPAY_AMT"),
    # 41 DEDCT_AMT
    F.when(
        F.col("Extract.DEDCT_AMT").isNull() | (F.length(F.trim(F.col("Extract.DEDCT_AMT"))) == 0),
        F.lit("0.00")
    ).otherwise(trim(F.col("Extract.DEDCT_AMT"))).alias("DEDCT_AMT"),
    # 42 DSALW_AMT
    F.lit(0.00).alias("DSALW_AMT"),
    # 43 ITS_HOME_DSCNT_AMT
    F.lit(0.00).alias("ITS_HOME_DSCNT_AMT"),
    # 44 NO_RESP_AMT
    F.lit(0.00).alias("NO_RESP_AMT"),
    # 45 MBR_LIAB_BSS_AMT
    F.lit(0.00).alias("MBR_LIAB_BSS_AMT"),
    # 46 PATN_RESP_AMT
    F.when(
        F.col("Extract.SRC_SYS_CD") == F.lit("OPTUMRX"),
        trim(F.col("Extract.CLM_AMT"))
    ).otherwise(
        F.when(
            F.col("Extract.COPAY_AMT").isNull() | (F.length(F.trim(F.col("Extract.COPAY_AMT"))) == 0),
            F.lit("0.00")
        ).otherwise(trim(F.col("Extract.COPAY_AMT")))
    ).alias("PATN_RESP_AMT"),
    # 47 PAYBL_AMT
    F.when(
        F.col("Extract.BILL_AMT").isNull() | (F.length(F.trim(F.col("Extract.BILL_AMT"))) == 0),
        F.lit("0.00")
    ).otherwise(
        F.when(
            (F.col("Extract.SRC_SYS_CD") == F.lit("OPTUMRX")) | (F.col("Extract.SRC_SYS_CD") == F.lit("MEDIMPACT")),
            trim(F.col("Extract.BILL_AMT"))
        ).otherwise(F.lit("0.00"))
    ).alias("PAYBL_AMT"),
    # 48 PAYBL_TO_PROV_AMT
    F.lit(0.00).alias("PAYBL_TO_PROV_AMT"),
    # 49 PAYBL_TO_SUB_AMT
    F.lit(0.00).alias("PAYBL_TO_SUB_AMT"),
    # 50 PROC_TBL_PRICE_AMT
    F.lit(0.00).alias("PROC_TBL_PRICE_AMT"),
    # 51 PROFL_PRICE_AMT
    F.lit(0.00).alias("PROFL_PRICE_AMT"),
    # 52 PROV_WRT_OFF_AMT
    F.lit(0.00).alias("PROV_WRT_OFF_AMT"),
    # 53 RISK_WTHLD_AMT
    F.lit(0.00).alias("RISK_WTHLD_AMT"),
    # 54 SVC_PRICE_AMT
    F.lit(0.00).alias("SVC_PRICE_AMT"),
    # 55 SUPLMT_DSCNT_AMT
    F.lit(0.00).alias("SUPLMT_DSCNT_AMT"),
    # 56 ALW_PRICE_UNIT_CT
    F.lit(0).alias("ALW_PRICE_UNIT_CT"),
    # 57 UNIT_CT
    F.lit(0).alias("UNIT_CT"),
    # 58 DEDCT_AMT_ACCUM_ID char(4)
    F.rpad(F.lit("NA"), 4, " ").alias("DEDCT_AMT_ACCUM_ID"),
    # 59 PREAUTH_SVC_SEQ_NO char(4)
    F.rpad(F.lit("NA"), 4, " ").alias("PREAUTH_SVC_SEQ_NO"),
    # 60 RFRL_SVC_SEQ_NO char(4)
    F.rpad(F.lit("NA"), 4, " ").alias("RFRL_SVC_SEQ_NO"),
    # 61 LMT_PFX_ID char(4)
    F.rpad(F.lit("NA"), 4, " ").alias("LMT_PFX_ID"),
    # 62 PREAUTH_ID char(9)
    F.rpad(F.lit("NA"), 9, " ").alias("PREAUTH_ID"),
    # 63 PROD_CMPNT_DEDCT_PFX_ID char(4)
    F.rpad(F.lit("NA"), 4, " ").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    # 64 PROD_CMPNT_SVC_PAYMT_ID char(4)
    F.rpad(F.lit("NA"), 4, " ").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    # 65 RFRL_ID_TX char(9)
    F.rpad(
        F.when(F.col("Extract.SRC_SYS_CD") == F.lit("OPTUMRX"), F.lit(None))
         .otherwise(F.lit("NA"))
        , 9, " "
    ).alias("RFRL_ID_TX"),
    # 66 SVC_ID char(4)
    F.rpad(F.lit("NA"), 4, " ").alias("SVC_ID"),
    # 67 SVC_PRICE_RULE_ID char(4)
    F.rpad(F.lit("NA"), 4, " ").alias("SVC_PRICE_RULE_ID"),
    # 68 SVC_RULE_TYP_TX char(3)
    F.rpad(
        F.when(F.col("Extract.SRC_SYS_CD") == F.lit("OPTUMRX"), F.lit(None))
         .otherwise(F.lit("NA"))
        , 3, " "
    ).alias("SVC_RULE_TYP_TX"),
    # Extra column from the logic: CLM_TYPE => 'M'
    F.rpad(F.lit("M"), 1, " ").alias("CLM_TYPE"),
    # SVC_LOC_TYP_CD char(20)
    F.rpad(F.lit("NA"), 20, " ").alias("SVC_LOC_TYP_CD"),
    # NON_PAR_SAV_AMT
    F.lit(0.00).alias("NON_PAR_SAV_AMT"),
    # PROC_CD_TYP_CD
    F.lit("NA").alias("PROC_CD_TYP_CD"),
    # PROC_CD_CAT_CD
    F.lit("NA").alias("PROC_CD_CAT_CD"),
    # NDC
    F.when(
        F.col("Extract.SRC_SYS_CD") == F.lit("MEDIMPACT"),
        F.col("Extract.NDC").cast(StringType())
    ).otherwise(
        F.when(
            F.col("Extract.SRC_SYS_CD") != F.lit("OPTUMRX"),
            F.lit("NA")
        ).otherwise(
            F.when(
                F.trim(F.col("Extract.NDC")).isNull(),
                F.lit("UNK")
            ).otherwise(
                F.when(F.col("Extract.CMPND_CD") == F.lit(2), F.lit("99999999999")).otherwise(F.col("Extract.NDC").cast(StringType()))
            )
        )
    ).alias("NDC"),
    # APC_ID => If Extract.SRC_SYS_CD <> 'OPTUMRX' => @Null else @Null
    F.lit(None).alias("APC_ID"),
    # APC_STTUS_ID => same logic
    F.lit(None).alias("APC_STTUS_ID"),
    # SNOMED_CT_CD => 'NA'
    F.lit("NA").alias("SNOMED_CT_CD"),
    # CVX_VCCN_CD => 'NA'
    F.lit("NA").alias("CVX_VCCN_CD")
)

# -----------------------
# Stage: Snapshot (CTransformerStage)
#   Input: df_BusinessRules_Transform
#   Output pins: PKey -> ClmLnPK, Snapshot -> Transformer
# -----------------------
df_SnapshotInput = df_BusinessRules_Transform

# Output pin "PKey" (C106) with specified columns
df_PKey = df_SnapshotInput.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROC_CD").alias("PROC_CD"),
    F.col("SVC_PROV_ID").alias("SVC_PROV_ID"),
    F.col("CLM_LN_DSALW_EXCD").alias("CLM_LN_DSALW_EXCD"),
    F.col("CLM_LN_EOB_EXCD").alias("CLM_LN_EOB_EXCD"),
    F.col("CLM_LN_FINL_DISP_CD").alias("CLM_LN_FINL_DISP_CD"),
    F.col("CLM_LN_LOB_CD").alias("CLM_LN_LOB_CD"),
    F.col("CLM_LN_POS_CD").alias("CLM_LN_POS_CD"),
    F.col("CLM_LN_PREAUTH_CD").alias("CLM_LN_PREAUTH_CD"),
    F.col("CLM_LN_PREAUTH_SRC_CD").alias("CLM_LN_PREAUTH_SRC_CD"),
    F.col("CLM_LN_PRICE_SRC_CD").alias("CLM_LN_PRICE_SRC_CD"),
    F.col("CLM_LN_RFRL_CD").alias("CLM_LN_RFRL_CD"),
    F.col("CLM_LN_RVNU_CD").alias("CLM_LN_RVNU_CD"),
    F.col("CLM_LN_ROOM_PRICE_METH_CD").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.col("CLM_LN_ROOM_TYP_CD").alias("CLM_LN_ROOM_TYP_CD"),
    F.col("CLM_LN_TOS_CD").alias("CLM_LN_TOS_CD"),
    F.col("CLM_LN_UNIT_TYP_CD").alias("CLM_LN_UNIT_TYP_CD"),
    F.col("CAP_LN_IN").alias("CAP_LN_IN"),
    F.col("PRI_LOB_IN").alias("PRI_LOB_IN"),
    F.col("SVC_END_DT").alias("SVC_END_DT"),
    F.col("SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("AGMNT_PRICE_AMT").alias("AGMNT_PRICE_AMT"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("COINS_AMT").alias("COINS_AMT"),
    F.col("CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("COPAY_AMT").alias("COPAY_AMT"),
    F.col("DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("DSALW_AMT").alias("DSALW_AMT"),
    F.col("ITS_HOME_DSCNT_AMT").alias("ITS_HOME_DSCNT_AMT"),
    F.col("NO_RESP_AMT").alias("NO_RESP_AMT"),
    F.col("MBR_LIAB_BSS_AMT").alias("MBR_LIAB_BSS_AMT"),
    F.col("PATN_RESP_AMT").alias("PATN_RESP_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("PAYBL_TO_PROV_AMT").alias("PAYBL_TO_PROV_AMT"),
    F.col("PAYBL_TO_SUB_AMT").alias("PAYBL_TO_SUB_AMT"),
    F.col("PROC_TBL_PRICE_AMT").alias("PROC_TBL_PRICE_AMT"),
    F.col("PROFL_PRICE_AMT").alias("PROFL_PRICE_AMT"),
    F.col("PROV_WRT_OFF_AMT").alias("PROV_WRT_OFF_AMT"),
    F.col("RISK_WTHLD_AMT").alias("RISK_WTHLD_AMT"),
    F.col("SVC_PRICE_AMT").alias("SVC_PRICE_AMT"),
    F.col("SUPLMT_DSCNT_AMT").alias("SUPLMT_DSCNT_AMT"),
    F.col("ALW_PRICE_UNIT_CT").alias("ALW_PRICE_UNIT_CT"),
    F.col("UNIT_CT").alias("UNIT_CT"),
    F.col("DEDCT_AMT_ACCUM_ID").alias("DEDCT_AMT_ACCUM_ID"),
    F.col("PREAUTH_SVC_SEQ_NO").alias("PREAUTH_SVC_SEQ_NO"),
    F.col("RFRL_SVC_SEQ_NO").alias("RFRL_SVC_SEQ_NO"),
    F.col("LMT_PFX_ID").alias("LMT_PFX_ID"),
    F.col("PREAUTH_ID").alias("PREAUTH_ID"),
    F.col("PROD_CMPNT_DEDCT_PFX_ID").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.col("PROD_CMPNT_SVC_PAYMT_ID").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.col("RFRL_ID_TX").alias("RFRL_ID_TX"),
    F.col("SVC_ID").alias("SVC_ID"),
    F.col("SVC_PRICE_RULE_ID").alias("SVC_PRICE_RULE_ID"),
    F.col("SVC_RULE_TYP_TX").alias("SVC_RULE_TYP_TX"),
    F.col("SVC_LOC_TYP_CD").alias("SVC_LOC_TYP_CD"),
    F.col("NON_PAR_SAV_AMT").alias("NON_PAR_SAV_AMT"),
    F.col("PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD"),
    F.col("PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD"),
    F.lit("NA").alias("VBB_RULE_ID"),
    F.lit("NA").alias("VBB_EXCD_ID"),
    F.rpad(F.lit("N"), 1, " ").alias("CLM_LN_VBB_IN"),
    F.lit(0.00).alias("ITS_SUPLMT_DSCNT_AMT"),
    F.lit(0.00).alias("ITS_SRCHRG_AMT"),
    F.col("NDC").alias("NDC"),
    F.lit("NA").alias("NDC_DRUG_FORM_CD"),
    F.lit(None).alias("NDC_UNIT_CT"),
    F.rpad(F.lit("PDX"), 3, " ").alias("MED_PDX_IND"),
    F.col("APC_ID").alias("APC_ID"),
    F.col("APC_STTUS_ID").alias("APC_STTUS_ID"),
    F.col("SNOMED_CT_CD").alias("SNOMED_CT_CD"),
    F.col("CVX_VCCN_CD").alias("CVX_VCCN_CD")
)

# Output pin "Snapshot" (V99S0) with specified columns
df_Snapshot = df_SnapshotInput.select(
    F.col("CLM_ID").alias("CLCL_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("PROC_CD").alias("PROC_CD"),
    F.col("CLM_LN_RVNU_CD").alias("CLM_LN_RVNU_CD"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.rpad(F.col("CLM_TYPE"), 1, " ").alias("CLM_TYPE"),
    F.col("PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD"),
    F.col("PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD")
)

# -----------------------
# Stage: Transformer (CTransformerStage)
#   Input: df_Snapshot => (link name "Snapshot")
#   Stage variables: ProcCdSk, ClmLnRvnuCdSk (both from user-defined functions)
#   Output pin: RowCount -> B_CLM_LN
# -----------------------
df_Transformer_stageVars = (
    df_Snapshot
    .withColumn(
        "ProcCdSk",
        GetFkeyProcCd(
            F.lit("FACETS"),
            F.lit(0),
            F.col("PROC_CD").substr(F.lit(1), F.lit(5)),
            F.col("PROC_CD_TYP_CD"),
            F.col("PROC_CD_CAT_CD"),
            F.lit("X")
        )
    )
    .withColumn(
        "ClmLnRvnuCdSk",
        GetFkeyRvnu(
            F.lit("FACETS"),
            F.lit(0),
            F.col("CLM_LN_RVNU_CD"),
            F.lit("X")
        )
    )
)

df_Transformer_RowCount = df_Transformer_stageVars.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("CLCL_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("ProcCdSk").alias("PROC_CD_SK"),
    F.col("ClmLnRvnuCdSk").alias("CLM_LN_RVNU_CD_SK"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT")
)

# -----------------------
# Stage: B_CLM_LN (CSeqFileStage) - Write delimited file
# -----------------------
write_files(
    df_Transformer_RowCount,
    f"{adls_path}/load/B_CLM_LN.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# -----------------------
# Stage: ClmLnPK (CContainerStage) - SharedContainer "ClmLnPK"
#   Input: df_PKey
#   Output: single DataFrame
# -----------------------
params = {
    "CurrRunCycle": RunCycle
}
df_ClmLnPK = ClmLnPK(df_PKey, params)

# -----------------------
# Stage: BCBSKCCommClmLnExtr (CSeqFileStage) - Write final file
# -----------------------
write_files(
    df_ClmLnPK,
    f"{adls_path}/key/BCBSKCCommClmLnExtr_{SrcSysCd}.DrugClmLn.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)