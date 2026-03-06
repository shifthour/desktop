# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ********************************************************************************************
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    ESIDrugInvoiceUpdateSeq 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from ESI Invoice file to a landing file for the IDS. Amount fields are unpacked. Member information is applied to records.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS
# MAGIC Developer                Date                 Prjoect / TTR        Change Description                                                                                        Development Project                    Code Reviewer          Date Reviewed          
# MAGIC ------------------              --------------------    -----------------------       -----------------------------------------------------------------------------------------------------                   --------------------------------                   -----------------------------     ----------------------------   
# MAGIC SANdrew                2008-10-10                                    Originally Programmed                                                                                      devlIDSnew                                 Steph Goddard           11/07/2008      
# MAGIC Steph Goddard       11/11/2008    3784 PBM              changed calculation for punch fields                                                               devlIDSnew                                  Steph Goddard           11/11/2008           
# MAGIC 
# MAGIC SAnderw                2014-04-01        5082 ESI              1.  changed how Claim ID is derived from ESI_ADJ_REF_NO to TRANSACTION_ID                                        Bhoomi Dasari               4/16/2014
# MAGIC                                                                                       2.  Added 4 new fields within the Filler4 char (82) field that was at the end of the record.    Filler4 is now char (34).   now after CLM_TRANSMITTAL_METH are fields PRESCRIPTION_NBR2, TRANSMISSION_ID, CROSS_REFERENCE_ID, ADJUSTMNT_DTM
# MAGIC                                                                                       3.  Changed testing Member Uniq Key to ensure it is integer in unpunched_amt_fields 
# MAGIC                                                                                      If Len (Trim ( Rec4.ESI_CLNT_GNRL_PRPS_AREA ))= 0 then 0 else  If Num ( Int (  trim (Rec4.ESI_CLNT_GNRL_PRPS_AREA [21,20]))   )=@TRUE then  Int (  trim(Rec4.ESI_CLNT_GNRL_PRPS_AREA [21,20] ) )   else 0
# MAGIC                                                                                     4.  For Other Payor Amount changed from  svOthrPayorAmt/100
# MAGIC                                                                                                                                                to      if trim ( Rec4.OTHR_PAYOR_AMT )= "000000000" then 0.00 else svOthrPayorAmt/100

# MAGIC Do not rename link.  Count used in sequencer logic
# MAGIC Used in IDS, EDW, and Web DM update jobs
# MAGIC ESI Invoice Landing
# MAGIC This ESI Claim Invoice Landing job is for the Invoice process only.    The daily and the monthly (ie the Invoice file) are not similarily structured.   Daily file has more fields than Invoice.
# MAGIC The ESI Drug Claim File is fixed width ascii created in ESIDrugClmInvoicePrep
# MAGIC Very Important - this landing program will assign the claim id and the amount fields based on the claim status.  if an adjustment claim, the claimR will be given as the ClaimID and the amount fields are negated.   Some fields are Absolute(Esi.Field) in order to stay true to Paid / Adjust claim calulations.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','100')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_ESI_InvoiceRec4 = StructType([
    StructField("RCRD_ID", StringType(), True),
    StructField("PRCSR_NO", StringType(), True),
    StructField("BTCH_NO", StringType(), True),
    StructField("PDX_NO", StringType(), True),
    StructField("RX_NO", StringType(), True),
    StructField("DT_FILLED", StringType(), True),
    StructField("NDC_NO", StringType(), True),
    StructField("DRUG_DESC", StringType(), True),
    StructField("NEW_RFL_CD", StringType(), True),
    StructField("METRIC_QTY", StringType(), True),
    StructField("DAYS_SUPL", StringType(), True),
    StructField("BSS_OF_CST_DTRM", StringType(), True),
    StructField("INGR_CST", StringType(), True),
    StructField("DISPNS_FEE", StringType(), True),
    StructField("COPAY_AMT", StringType(), True),
    StructField("SLS_TAX", StringType(), True),
    StructField("AMT_BILL", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("SEX_CD", StringType(), True),
    StructField("CARDHLDR_ID_NO", StringType(), True),
    StructField("RELSHP_CD", StringType(), True),
    StructField("GRP_NO", StringType(), True),
    StructField("HOME_PLN", StringType(), True),
    StructField("HOST_PLN", StringType(), True),
    StructField("PRSCRBR_ID", StringType(), True),
    StructField("DIAG_CD", StringType(), True),
    StructField("CARDHLDR_FIRST_NM", StringType(), True),
    StructField("CARDHLDR_LAST_NM", StringType(), True),
    StructField("PRAUTH_NO", StringType(), True),
    StructField("PA_MC_SC_NO", StringType(), True),
    StructField("CUST_LOC", StringType(), True),
    StructField("RESUB_CYC_CT", StringType(), True),
    StructField("DT_RX_WRTN", StringType(), True),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), True),
    StructField("PRSN_CD", StringType(), True),
    StructField("OTHR_COV_CD", StringType(), True),
    StructField("ELIG_CLRFCTN_CD", StringType(), True),
    StructField("CMPND_CD", StringType(), True),
    StructField("NO_OF_RFLS_AUTH", StringType(), True),
    StructField("LVL_OF_SVC", StringType(), True),
    StructField("RX_ORIG_CD", StringType(), True),
    StructField("RX_DENIAL_CLRFCTN", StringType(), True),
    StructField("PRI_PRSCRBR", StringType(), True),
    StructField("CLNC_ID_NO", StringType(), True),
    StructField("DRUG_TYP", StringType(), True),
    StructField("PRSCRBR_LAST_NM", StringType(), True),
    StructField("POSTAGE_AMT_CLMED", StringType(), True),
    StructField("UNIT_DOSE_IN", StringType(), True),
    StructField("OTHR_PAYOR_AMT", StringType(), True),
    StructField("BSS_OF_DAYS_SUPL_DTRM", StringType(), True),
    StructField("FULL_AWP", StringType(), True),
    StructField("EXPNSN_AREA", StringType(), True),
    StructField("MSTR_CAR", StringType(), True),
    StructField("SUB_CAR", StringType(), True),
    StructField("CLM_TYP", StringType(), True),
    StructField("ESI_SUB_GRP", StringType(), True),
    StructField("PLN_DSGNR", StringType(), True),
    StructField("ADJDCT_DT", StringType(), True),
    StructField("ADMIN_FEE", StringType(), True),
    StructField("CAP_AMT", StringType(), True),
    StructField("INGR_CST_SUB", StringType(), True),
    StructField("MBR_NON_COPAY_AMT", StringType(), True),
    StructField("MBR_PAY_CD", StringType(), True),
    StructField("INCNTV_FEE", StringType(), True),
    StructField("CLM_ADJ_AMT", StringType(), True),
    StructField("CLM_ADJ_CD", StringType(), True),
    StructField("FRMLRY_FLAG", StringType(), True),
    StructField("GNRC_CLS_NO", StringType(), True),
    StructField("THRPTC_CLS_AHFS", StringType(), True),
    StructField("PDX_TYP", StringType(), True),
    StructField("BILL_BSS_CD", StringType(), True),
    StructField("USL_AND_CUST_CHRG", StringType(), True),
    StructField("PD_DT", StringType(), True),
    StructField("BNF_CD", StringType(), True),
    StructField("DRUG_STRG", StringType(), True),
    StructField("ORIG_MBR", StringType(), True),
    StructField("DT_OF_INJURY", StringType(), True),
    StructField("FEE_AMT", StringType(), True),
    StructField("ESI_REF_NO", StringType(), True),
    StructField("CLNT_CUST_ID", StringType(), True),
    StructField("PLN_TYP", StringType(), True),
    StructField("ESI_ADJDCT_REF_NO", StringType(), True),
    StructField("ESI_ANCLRY_AMT", StringType(), True),
    StructField("ESI_CLNT_GNRL_PRPS_AREA", StringType(), True),
    StructField("PRTL_FILL_STTUS_CD", StringType(), True),
    StructField("ESI_BILL_DT", StringType(), True),
    StructField("FSA_VNDR_CD", StringType(), True),
    StructField("PICA_DRUG_CD", StringType(), True),
    StructField("AMT_CLMED", StringType(), True),
    StructField("AMT_DSALW", StringType(), True),
    StructField("FED_DRUG_CLS_CD", StringType(), True),
    StructField("DEDCT_AMT", StringType(), True),
    StructField("BNF_COPAY_100", StringType(), True),
    StructField("CLM_PRCS_TYP", StringType(), True),
    StructField("INDEM_HIER_TIER_NO", StringType(), True),
    StructField("FLR", StringType(), True),
    StructField("MCARE_D_COV_DRUG", StringType(), True),
    StructField("RETRO_LICS_CD", StringType(), True),
    StructField("RETRO_LICS_AMT", StringType(), True),
    StructField("LICS_SBSDY_AMT", StringType(), True),
    StructField("MED_B_DRUG", StringType(), True),
    StructField("MED_B_CLM", StringType(), True),
    StructField("PRSCRBR_QLFR", StringType(), True),
    StructField("PRSCRBR_ID_NPI", StringType(), True),
    StructField("PDX_QLFR", StringType(), True),
    StructField("PDX_ID_NPI", StringType(), True),
    StructField("HRA_APLD_AMT", StringType(), True),
    StructField("ESI_THER_CLS", StringType(), True),
    StructField("HIC_NO", StringType(), True),
    StructField("HRA_FLAG", StringType(), True),
    StructField("DOSE_CD", StringType(), True),
    StructField("LOW_INCM", StringType(), True),
    StructField("RTE_OF_ADMIN", StringType(), True),
    StructField("DEA_SCHD", StringType(), True),
    StructField("COPAY_BNF_OPT", StringType(), True),
    StructField("GNRC_PROD_IN_GPI", StringType(), True),
    StructField("PRSCRBR_SPEC", StringType(), True),
    StructField("VAL_CD", StringType(), True),
    StructField("PRI_CARE_PDX", StringType(), True),
    StructField("OFC_OF_INSPECTOR_GNRL_OIG", StringType(), True),
    StructField("FLR3", StringType(), True),
    StructField("PSL_FMLY_MET_AMT", StringType(), True),
    StructField("PSL_MBR_MET_AMT", StringType(), True),
    StructField("PSL_FMLY_AMT", StringType(), True),
    StructField("DED_FMLY_MET_AMT", StringType(), True),
    StructField("DED_FMLY_AMT", StringType(), True),
    StructField("MOPS_FMLY_AMT", StringType(), True),
    StructField("MOPS_FMLY_MET_AMT", StringType(), True),
    StructField("MOPS_MBR_MET_AMT", StringType(), True),
    StructField("DED_MBR_MET_AMT", StringType(), True),
    StructField("PSL_APLD_AMT", StringType(), True),
    StructField("MOPS_APLD_AMT", StringType(), True),
    StructField("PAR_PDX_IND", StringType(), True),
    StructField("COPAY_PCT_AMT", StringType(), True),
    StructField("COPAY_FLAT_AMT", StringType(), True),
    StructField("CLM_TRANSMITTAL_METH", StringType(), True),
    StructField("PRESCRIPTION_NBR_2", StringType(), True),
    StructField("TRANSACTION_ID", StringType(), True),
    StructField("CROSS_REF_ID", StringType(), True),
    StructField("FILLER4", StringType(), True)
])

df_Rec4 = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", '"')
    .option("escape", '"')
    .option("delimiter", "|")  # Not specified in the JSON, but must set something. If unknown, use a safe delimiter assumption.
    .schema(schema_ESI_InvoiceRec4)
    .load(f"{adls_path}/verified/ESI_InvoiceRec4.dat.{RunID}")
)

df_unpunch = (
    df_Rec4
    .withColumn("PersonCode", F.expr(
        """
        CASE 
          WHEN Len(trim(PRSN_CD)) = 3 AND trim(substring(PRSN_CD,1,1)) = '0' THEN substring(PRSN_CD,2,2)
          WHEN Len(trim(PRSN_CD)) = 2 THEN PRSN_CD
          WHEN Len(trim(PRSN_CD)) = 1 THEN concat('0',trim(PRSN_CD))
          ELSE trim(PRSN_CD)
        END
        """
    ))
    .withColumn("svClmId", F.col("TRANSACTION_ID"))
    .withColumn("svSignCalcDisp", GetRvrsPunchSign(F.expr("substring(DISPNS_FEE, length(DISPNS_FEE), 1)")))
    .withColumn("svSignCalcIngr", GetRvrsPunchSign(F.expr("substring(INGR_CST, length(INGR_CST), 1)")))
    .withColumn("svSignCalcAwp", GetRvrsPunchSign(F.expr("substring(FULL_AWP, length(FULL_AWP), 1)")))
    .withColumn("svSignCalcAnclry", GetRvrsPunchSign(F.expr("substring(ESI_ANCLRY_AMT, length(ESI_ANCLRY_AMT), 1)")))
    .withColumn("svSignCalcSales", GetRvrsPunchSign(F.expr("substring(SLS_TAX, length(SLS_TAX), 1)")))
    .withColumn("svSignCalcAdmin", GetRvrsPunchSign(F.expr("substring(ADMIN_FEE, length(ADMIN_FEE), 1)")))
    .withColumn("svSignCalcCapAmt", GetRvrsPunchSign(F.expr("substring(CAP_AMT, length(CAP_AMT), 1)")))
    .withColumn("svSignCalcBill", GetRvrsPunchSign(F.expr("substring(AMT_BILL, length(AMT_BILL), 1)")))
    .withColumn("svSignCalcDedct", GetRvrsPunchSign(F.expr("substring(DEDCT_AMT, length(DEDCT_AMT), 1)")))
    .withColumn("svSignCalcCopay", GetRvrsPunchSign(F.expr("substring(COPAY_AMT, length(COPAY_AMT), 1)")))
    .withColumn("svSignCalcOthrPayor", GetRvrsPunchSign(F.expr("substring(OTHR_PAYOR_AMT, length(OTHR_PAYOR_AMT), 1)")))
    .withColumn("svDispFee", F.expr("""
        CASE 
         WHEN length(svSignCalcDisp) > 1 
          THEN concat('-', Ereplace(DISPNS_FEE, substring(DISPNS_FEE,length(DISPNS_FEE),1), substring(svSignCalcDisp,2,1))) * 1
         ELSE Ereplace(DISPNS_FEE, substring(DISPNS_FEE,length(DISPNS_FEE),1), substring(svSignCalcDisp,1,1)) * 1
        END
    """))
    .withColumn("svIngrCst", F.expr("""
        CASE 
         WHEN length(svSignCalcIngr) > 1 
          THEN concat('-', Ereplace(INGR_CST, substring(INGR_CST,length(INGR_CST),1), substring(svSignCalcIngr,2,1))) * 1
         ELSE Ereplace(INGR_CST, substring(INGR_CST,length(INGR_CST),1), substring(svSignCalcIngr,1,1)) * 1
        END
    """))
    .withColumn("svFullAwp", F.expr("""
        CASE 
         WHEN length(svSignCalcAwp) > 1 
          THEN concat('-', Ereplace(FULL_AWP, substring(FULL_AWP,length(FULL_AWP),1), substring(svSignCalcAwp,2,1))) * 1
         ELSE Ereplace(FULL_AWP, substring(FULL_AWP,length(FULL_AWP),1), substring(svSignCalcAwp,1,1)) * 1
        END
    """))
    .withColumn("svAnclryAmt", F.expr("""
        CASE 
         WHEN length(svSignCalcAnclry) > 1 
          THEN concat('-', Ereplace(ESI_ANCLRY_AMT, substring(ESI_ANCLRY_AMT,length(ESI_ANCLRY_AMT),1), substring(svSignCalcAnclry,2,1))) * 1
         ELSE Ereplace(ESI_ANCLRY_AMT, substring(ESI_ANCLRY_AMT,length(ESI_ANCLRY_AMT),1), substring(svSignCalcAnclry,1,1)) * 1
        END
    """))
    .withColumn("svSlsTax", F.expr("""
        CASE 
         WHEN length(svSignCalcSales) > 1 
          THEN concat('-', Ereplace(SLS_TAX, substring(SLS_TAX,length(SLS_TAX),1), substring(svSignCalcSales,2,1))) * 1
         ELSE Ereplace(SLS_TAX, substring(SLS_TAX,length(SLS_TAX),1), substring(svSignCalcSales,1,1)) * 1
        END
    """))
    .withColumn("svAdminFee", F.expr("""
        CASE 
         WHEN length(svSignCalcAdmin) > 1 
          THEN concat('-', Ereplace(ADMIN_FEE, substring(ADMIN_FEE,length(ADMIN_FEE),1), substring(svSignCalcAdmin,2,1))) * 1
         ELSE Ereplace(ADMIN_FEE, substring(ADMIN_FEE,length(ADMIN_FEE),1), substring(svSignCalcAdmin,1,1)) * 1
        END
    """))
    .withColumn("svAmtBill", F.expr("""
        CASE 
         WHEN length(svSignCalcBill) > 1 
          THEN concat('-', Ereplace(AMT_BILL, substring(AMT_BILL,length(AMT_BILL),1), substring(svSignCalcBill,2,1))) * 1
         ELSE Ereplace(AMT_BILL, substring(AMT_BILL,length(AMT_BILL),1), substring(svSignCalcBill,1,1)) * 1
        END
    """))
    .withColumn("svDedctAmt", F.expr("""
        CASE 
         WHEN length(svSignCalcDedct) > 1 
          THEN concat('-', Ereplace(DEDCT_AMT, substring(DEDCT_AMT,length(DEDCT_AMT),1), substring(svSignCalcDedct,2,1))) * 1
         ELSE Ereplace(DEDCT_AMT, substring(DEDCT_AMT,length(DEDCT_AMT),1), substring(svSignCalcDedct,1,1)) * 1
        END
    """))
    .withColumn("svCopayAmt", F.expr("""
        CASE 
         WHEN length(svSignCalcCopay) > 1 
          THEN concat('-', Ereplace(COPAY_AMT, substring(COPAY_AMT,length(COPAY_AMT),1), substring(svSignCalcCopay,2,1))) * 1
         ELSE Ereplace(COPAY_AMT, substring(COPAY_AMT,length(COPAY_AMT),1), substring(svSignCalcCopay,1,1)) * 1
        END
    """))
    .withColumn("svOthrPayorAmt", F.expr("""
        CASE 
         WHEN length(svSignCalcOthrPayor) > 1 
          THEN concat('-', Ereplace(OTHR_PAYOR_AMT, substring(OTHR_PAYOR_AMT,length(OTHR_PAYOR_AMT),1), substring(svSignCalcOthrPayor,2,1))) * 1
         ELSE Ereplace(OTHR_PAYOR_AMT, substring(OTHR_PAYOR_AMT,length(OTHR_PAYOR_AMT),1), substring(svSignCalcOthrPayor,1,1)) * 1
        END
    """))
    .withColumn("svSignCalcIngrCstSub", GetRvrsPunchSign(F.expr("substring(INGR_CST_SUB, length(INGR_CST_SUB), 1)")))
    .withColumn("svIngrCstSub", F.expr("""
        CASE 
         WHEN length(svSignCalcIngrCstSub) > 1 
          THEN concat('-', Ereplace(INGR_CST_SUB, substring(INGR_CST_SUB,length(INGR_CST_SUB),1), substring(svSignCalcIngrCstSub,2,1))) * 1
         ELSE Ereplace(INGR_CST_SUB, substring(INGR_CST_SUB,length(INGR_CST_SUB),1), substring(svSignCalcIngrCstSub,1,1)) * 1
        END
    """))
    .withColumn("svSignCalcMbrNonCopay", GetRvrsPunchSign(F.expr("substring(MBR_NON_COPAY_AMT, length(MBR_NON_COPAY_AMT), 1)")))
    .withColumn("svMbrNonCopyaAmt", F.expr("""
        CASE 
         WHEN length(svSignCalcMbrNonCopay) > 1 
          THEN concat('-', Ereplace(MBR_NON_COPAY_AMT, substring(MBR_NON_COPAY_AMT,length(MBR_NON_COPAY_AMT),1), substring(svSignCalcMbrNonCopay,2,1))) * 1
         ELSE Ereplace(MBR_NON_COPAY_AMT, substring(MBR_NON_COPAY_AMT,length(MBR_NON_COPAY_AMT),1), substring(svSignCalcMbrNonCopay,1,1)) * 1
        END
    """))
    .withColumn("svSignCalcIncntvFee", GetRvrsPunchSign(F.expr("substring(INCNTV_FEE, length(INCNTV_FEE), 1)")))
    .withColumn("svIncntvFee", F.expr("""
        CASE 
         WHEN length(svSignCalcIncntvFee) > 1 
          THEN concat('-', Ereplace(INCNTV_FEE, substring(INCNTV_FEE,length(INCNTV_FEE),1), substring(svSignCalcIncntvFee,2,1))) * 1
         ELSE Ereplace(INCNTV_FEE, substring(INCNTV_FEE,length(INCNTV_FEE),1), substring(svSignCalcIncntvFee,1,1)) * 1
        END
    """))
    .withColumn("svSignCalcClmAdjAmt", GetRvrsPunchSign(F.expr("substring(CLM_ADJ_AMT, length(CLM_ADJ_AMT), 1)")))
    .withColumn("svClmAdjAmt", F.expr("""
        CASE 
         WHEN length(svSignCalcClmAdjAmt) > 1 
          THEN concat('-', Ereplace(CLM_ADJ_AMT, substring(CLM_ADJ_AMT,length(CLM_ADJ_AMT),1), substring(svSignCalcClmAdjAmt,2,1))) * 1
         ELSE Ereplace(CLM_ADJ_AMT, substring(CLM_ADJ_AMT,length(CLM_ADJ_AMT),1), substring(svSignCalcClmAdjAmt,1,1)) * 1
        END
    """))
    .withColumn("svSignCalcFeeAmt", GetRvrsPunchSign(F.expr("substring(FEE_AMT, length(FEE_AMT), 1)")))
    .withColumn("svFeeAmt", F.expr("""
        CASE 
         WHEN length(svSignCalcFeeAmt) > 1 
          THEN concat('-', Ereplace(FEE_AMT, substring(FEE_AMT,length(FEE_AMT),1), substring(svSignCalcFeeAmt,2,1))) * 1
         ELSE Ereplace(FEE_AMT, substring(FEE_AMT,length(FEE_AMT),1), substring(svSignCalcFeeAmt,1,1)) * 1
        END
    """))
    .withColumn("svSignCalcUslAndCustChg", GetRvrsPunchSign(F.expr("substring(USL_AND_CUST_CHRG, length(USL_AND_CUST_CHRG), 1)")))
    .withColumn("svUslAndCustChg", F.expr("""
        CASE 
         WHEN length(svSignCalcUslAndCustChg) > 1 
          THEN concat('-', Ereplace(USL_AND_CUST_CHRG, substring(USL_AND_CUST_CHRG,length(USL_AND_CUST_CHRG),1), substring(svSignCalcUslAndCustChg,2,1))) * 1
         ELSE Ereplace(USL_AND_CUST_CHRG, substring(USL_AND_CUST_CHRG,length(USL_AND_CUST_CHRG),1), substring(svSignCalcUslAndCustChg,1,1)) * 1
        END
    """))
    .withColumn("svSignCalcAmtCtMed", GetRvrsPunchSign(F.expr("substring(AMT_CLMED, length(AMT_CLMED), 1)")))
    .withColumn("svAmtClMed", F.expr("""
        CASE 
         WHEN length(svSignCalcAmtCtMed) > 1 
          THEN concat('-', Ereplace(AMT_CLMED, substring(AMT_CLMED,length(AMT_CLMED),1), substring(svSignCalcAmtCtMed,2,1))) * 1
         ELSE Ereplace(AMT_CLMED, substring(AMT_CLMED,length(AMT_CLMED),1), substring(svSignCalcAmtCtMed,1,1)) * 1
        END
    """))
    .withColumn("svSignCalcAmtDsalw", GetRvrsPunchSign(F.expr("substring(AMT_DSALW, length(AMT_DSALW), 1)")))
    .withColumn("svAmtDsalw", F.expr("""
        CASE 
         WHEN length(svSignCalcAmtDsalw) > 1 
          THEN concat('-', Ereplace(AMT_DSALW, substring(AMT_DSALW,length(AMT_DSALW),1), substring(svSignCalcAmtDsalw,2,1))) * 1
         ELSE Ereplace(AMT_DSALW, substring(AMT_DSALW,length(AMT_DSALW),1), substring(svSignCalcAmtDsalw,1,1)) * 1
        END
    """))
    .withColumn("svSignCalcRetroLicsAmt", GetRvrsPunchSign(F.expr("substring(RETRO_LICS_AMT, length(RETRO_LICS_AMT), 1)")))
    .withColumn("svRetroLicsAmt", F.expr("""
        CASE 
         WHEN length(svSignCalcRetroLicsAmt) > 1 
          THEN concat('-', Ereplace(RETRO_LICS_AMT, substring(RETRO_LICS_AMT,length(RETRO_LICS_AMT),1), substring(svSignCalcRetroLicsAmt,2,1))) * 1
         ELSE Ereplace(RETRO_LICS_AMT, substring(RETRO_LICS_AMT,length(RETRO_LICS_AMT),1), substring(svSignCalcRetroLicsAmt,1,1)) * 1
        END
    """))
    .withColumn("svSignCalcLicsSbsdAmt", GetRvrsPunchSign(F.expr("substring(LICS_SBSDY_AMT, length(LICS_SBSDY_AMT), 1)")))
    .withColumn("svLicsSbsdyAmt", F.expr("""
        CASE 
         WHEN length(svSignCalcLicsSbsdAmt) > 1 
          THEN concat('-', Ereplace(LICS_SBSDY_AMT, substring(LICS_SBSDY_AMT,length(LICS_SBSDY_AMT),1), substring(svSignCalcLicsSbsdAmt,2,1))) * 1
         ELSE Ereplace(LICS_SBSDY_AMT, substring(LICS_SBSDY_AMT,length(LICS_SBSDY_AMT),1), substring(svSignCalcLicsSbsdAmt,1,1)) * 1
        END
    """))
    .withColumn("svSignCalcHRAApldAmt", GetRvrsPunchSign(F.expr("substring(HRA_APLD_AMT, length(HRA_APLD_AMT), 1)")))
    .withColumn("svHRAApldAmt", F.expr("""
        CASE 
         WHEN length(svSignCalcHRAApldAmt) > 1 
          THEN concat('-', Ereplace(HRA_APLD_AMT, substring(HRA_APLD_AMT,length(HRA_APLD_AMT),1), substring(svSignCalcHRAApldAmt,2,1))) * 1
         ELSE Ereplace(HRA_APLD_AMT, substring(HRA_APLD_AMT,length(HRA_APLD_AMT),1), substring(svSignCalcHRAApldAmt,1,1)) * 1
        END
    """))
)

df_esi = df_unpunch.select(
    F.col("RCRD_ID").alias("RCRD_ID"),
    F.col("svClmId").alias("CLM_ID"),
    F.col("PRCSR_NO").alias("PRCSR_NO"),
    F.col("BTCH_NO").alias("BTCH_NO"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("PDX_NO"))).alias("PDX_NO"),
    F.col("RX_NO").alias("RX_NO"),
    F.col("DT_FILLED").alias("DT_FILLED"),
    F.col("NDC_NO").alias("NDC_NO"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("DRUG_DESC"))).alias("DRUG_DESC"),
    F.col("NEW_RFL_CD").alias("NEW_RFL_CD"),
    (
        (
            F.concat(
                F.col("METRIC_QTY").substr(F.lit(1), F.length("METRIC_QTY")-3),
                F.lit("."),
                F.col("METRIC_QTY").substr(F.length("METRIC_QTY")-2, 3)
            )
        ).cast("double")
    ).alias("METRIC_QTY"),
    F.col("DAYS_SUPL").alias("DAYS_SUPL"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("BSS_OF_CST_DTRM"))).alias("BSS_OF_CST_DTRM"),
    (
        F.concat(
            F.col("svIngrCst").cast("string").substr(F.lit(1), F.length("svIngrCst")-1),
            F.lit("."),
            F.col("svIngrCst").cast("string").substr(F.length("svIngrCst"), 1)
        )
    ).alias("INGR_CST"),
    (
        F.concat(
            F.col("svDispFee").cast("string").substr(F.lit(1), F.length("svDispFee")-1),
            F.lit("."),
            F.col("svDispFee").cast("string").substr(F.length("svDispFee"), 1)
        )
    ).alias("DISPNS_FEE"),
    (
        F.concat(
            F.col("svCopayAmt").cast("string").substr(F.lit(1), F.length("svCopayAmt")-1),
            F.lit("."),
            F.col("svCopayAmt").cast("string").substr(F.length("svCopayAmt"), 1)
        )
    ).alias("COPAY_AMT"),
    (F.col("svSlsTax")/100).alias("SLS_TAX"),
    (F.col("svAmtBill")/100).alias("AMT_BILL"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("PATN_FIRST_NM"))).alias("PATN_FIRST_NM"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("PATN_LAST_NM"))).alias("PATN_LAST_NM"),
    F.col("DOB").alias("DOB"),
    F.col("SEX_CD").alias("SEX_CD"),
    # The DataStage expression for CARDHLDR_ID_NO was Trim(Substrings(Trim(Rec4.CARDHLDR_ID_NO),1,Len(Trim(Rec4.CARDHLDR_ID_NO))-2))
    # but we will replicate that as a direct expression call. Assume Substrings is available:
    F.trim(Substrings(F.trim(F.col("CARDHLDR_ID_NO")), F.lit(1), F.length(F.trim(F.col("CARDHLDR_ID_NO")))-2)).alias("CARDHLDR_ID_NO"),
    F.col("RELSHP_CD").alias("RELSHP_CD"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("GRP_NO"))).alias("GRP_NO"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("HOME_PLN"))).alias("HOME_PLN"),
    F.col("HOST_PLN").alias("HOST_PLN"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("PRSCRBR_ID"))).alias("PRESCRIBER_ID"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("DIAG_CD"))).alias("DIAG_CD"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("CARDHLDR_FIRST_NM"))).alias("CARDHLDR_FIRST_NM"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("CARDHLDR_LAST_NM"))).alias("CARDHLDR_LAST_NM"),
    F.col("PRAUTH_NO").alias("PRAUTH_NO"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("PA_MC_SC_NO"))).alias("PA_MC_SC_NO"),
    F.col("CUST_LOC").alias("CUST_LOC"),
    F.col("RESUB_CYC_CT").alias("RESUB_CYC_CT"),
    F.col("DT_RX_WRTN").alias("DT_RX_WRTN"),
    Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("DISPENSE_AS_WRTN_PROD_SEL_CD")).alias("DISPENSE_AS_WRTN_PROD_SEL_CD"),
    F.col("PersonCode").alias("PRSN_CD"),
    F.col("OTHR_COV_CD").cast("int").alias("OTHR_COV_CD"),
    F.col("ELIG_CLRFCTN_CD").alias("ELIG_CLRFCTN_CD"),
    F.col("CMPND_CD").alias("CMPND_CD"),
    F.col("NO_OF_RFLS_AUTH").alias("NO_OF_RFLS_AUTH"),
    F.col("LVL_OF_SVC").alias("LVL_OF_SVC"),
    F.col("RX_ORIG_CD").alias("RX_ORIG_CD"),
    F.col("RX_DENIAL_CLRFCTN").alias("RX_DENIAL_CLRFCTN"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("PRI_PRSCRBR"))).alias("PRI_PRESCRIBER"),
    F.col("CLNC_ID_NO").alias("CLNC_ID_NO"),
    F.col("DRUG_TYP").alias("DRUG_TYP"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("PRSCRBR_LAST_NM"))).alias("PRESCRIBER_LAST_NM"),
    F.col("POSTAGE_AMT_CLMED").alias("POSTAGE_AMT_CLMED"),
    F.col("UNIT_DOSE_IN").alias("UNIT_DOSE_IN"),
    F.expr("CASE WHEN trim(OTHR_PAYOR_AMT) = '000000000' THEN 0.00 ELSE svOthrPayorAmt/100 END").alias("OTHR_PAYOR_AMT"),
    F.col("BSS_OF_DAYS_SUPL_DTRM").alias("BSS_OF_DAYS_SUPL_DTRM"),
    (
        F.concat(
            F.col("svFullAwp").cast("string").substr(F.lit(1), F.length("svFullAwp")-1),
            F.lit("."),
            F.col("svFullAwp").cast("string").substr(F.length("svFullAwp"), 1)
        )
    ).alias("FULL_AWP"),
    Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("EXPNSN_AREA")).alias("EXPNSN_AREA"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("MSTR_CAR"))).alias("MSTR_CAR"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("SUB_CAR"))).alias("SUB_CAR"),
    Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("CLM_TYP")).alias("CLM_TYP"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("ESI_SUB_GRP"))).alias("ESI_SUB_GRP"),
    Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("PLN_DSGNR")).alias("PLN_DSGNR"),
    FORMAT_DATE(F.col("ADJDCT_DT"), F.lit("CHAR"), F.lit("CCYYMMDD"), F.lit("CCYY-MM-DD")).alias("ADJDCT_DT"),
    (F.col("svAdminFee")/100).alias("ADMIN_FEE"),
    F.col("svCapAmt").alias("CAP_AMT"),
    F.col("svIngrCstSub").alias("INGR_CST_SUB"),
    F.col("svMbrNonCopyaAmt").alias("MBR_NON_COPAY_AMT"),
    F.col("MBR_PAY_CD").alias("MBR_PAY_CD"),
    F.col("svIncntvFee").alias("INCNTV_FEE"),
    F.col("svClmAdjAmt").alias("CLM_ADJ_AMT"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("CLM_ADJ_CD"))).alias("CLM_ADJ_CD"),
    Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("FRMLRY_FLAG")).alias("FRMLRY_FLAG"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("GNRC_CLS_NO"))).alias("GNRC_CLS_NO"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("THRPTC_CLS_AHFS"))).alias("THRPTC_CLS_AHFS"),
    Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("PDX_TYP")).alias("PDX_TYP"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("BILL_BSS_CD"))).alias("BILL_BSS_CD"),
    F.col("svUslAndCustChg").alias("USL_AND_CUST_CHRG"),
    F.col("PD_DT").alias("PD_DT"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("BNF_CD"))).alias("BNF_CD"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("DRUG_STRG"))).alias("DRUG_STRG"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("ORIG_MBR"))).alias("ORIG_MBR"),
    F.col("DT_OF_INJURY").alias("DT_OF_INJURY"),
    F.col("svFeeAmt").alias("FEE_AMT"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("ESI_REF_NO"))).alias("ESI_REF_NO"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("CLNT_CUST_ID"))).alias("CLNT_CUST_ID"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("PLN_TYP"))).alias("PLN_TYP"),
    F.col("ESI_ADJDCT_REF_NO").alias("ESI_ADJDCT_REF_NO"),
    (
        F.concat(
            F.col("svAnclryAmt").cast("string").substr(F.lit(1), F.length("svAnclryAmt")-1),
            F.lit("."),
            F.col("svAnclryAmt").cast("string").substr(F.length("svAnclryAmt"), 1)
        )
    ).alias("ESI_ANCLRY_AMT"),
    F.trim(F.col("ESI_CLNT_GNRL_PRPS_AREA").substr(F.lit(1), F.lit(8))).alias("GRP_ID"),
    F.trim(F.col("ESI_CLNT_GNRL_PRPS_AREA").substr(F.lit(9), F.lit(4))).alias("SUBGRP_ID"),
    F.trim(F.col("ESI_CLNT_GNRL_PRPS_AREA").substr(F.lit(13), F.lit(4))).alias("CLS_PLN_ID"),
    F.expr("""
        CASE 
         WHEN length(trim(ESI_CLNT_GNRL_PRPS_AREA))=0 THEN 0
         WHEN Num(Int(trim(substring(ESI_CLNT_GNRL_PRPS_AREA,21,20))))=1 THEN Int(trim(substring(ESI_CLNT_GNRL_PRPS_AREA,21,20)))
         ELSE 0
        END
    """).alias("MBR_UNIQ_KEY"),
    Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("PRTL_FILL_STTUS_CD")).alias("PRTL_FILL_STTUS_CD"),
    F.col("ESI_BILL_DT").alias("ESI_BILL_DT"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("FSA_VNDR_CD"))).alias("FSA_VNDR_CD"),
    Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("PICA_DRUG_CD")).alias("PICA_DRUG_CD"),
    F.col("svAmtClMed").alias("AMT_CLMED"),
    F.col("svAmtDsalw").alias("AMT_DSALW"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("FED_DRUG_CLS_CD"))).alias("FED_DRUG_CLS_CD"),
    (
        F.concat(
            F.col("svDedctAmt").cast("string").substr(F.lit(1), F.length("svDedctAmt")-1),
            F.lit("."),
            F.col("svDedctAmt").cast("string").substr(F.length("svDedctAmt"), 1)
        )
    ).alias("DEDCT_AMT"),
    Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("BNF_COPAY_100")).alias("BNF_COPAY_100"),
    Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("CLM_PRCS_TYP")).alias("CLM_PRCS_TYP"),
    F.col("INDEM_HIER_TIER_NO").alias("INDEM_HIER_TIER_NO"),
    F.col("FLR").alias("FLR"),
    Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("MCARE_D_COV_DRUG")).alias("MCARE_D_COV_DRUG"),
    Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("RETRO_LICS_CD")).alias("RETRO_LICS_CD"),
    F.col("svRetroLicsAmt").alias("RETRO_LICS_AMT"),
    F.col("svLicsSbsdyAmt").alias("LICS_SBSDY_AMT"),
    Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("MED_B_DRUG")).alias("MED_B_DRUG"),
    Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("MED_B_CLM")).alias("MED_B_CLM"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("PRSCRBR_QLFR"))).alias("PRESCRIBER_QLFR"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("PRSCRBR_ID_NPI"))).alias("PRESCRIBER_ID_NPI"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("PDX_QLFR"))).alias("PDX_QLFR"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("PDX_ID_NPI"))).alias("PDX_ID_NPI"),
    F.col("svHRAApldAmt").alias("HRA_APLD_AMT"),
    F.col("ESI_THER_CLS").alias("ESI_THER_CLS"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("HIC_NO"))).alias("HIC_NO"),
    Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("HRA_FLAG")).alias("HRA_FLAG"),
    F.col("DOSE_CD").alias("DOSE_CD"),
    Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("LOW_INCM")).alias("LOW_INCM"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("RTE_OF_ADMIN"))).alias("RTE_OF_ADMIN"),
    F.col("DEA_SCHD").alias("DEA_SCHD"),
    F.col("COPAY_BNF_OPT").alias("COPAY_BNF_OPT"),
    F.col("GNRC_PROD_IN_GPI").alias("GNRC_PROD_IN_GPI"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("PRSCRBR_SPEC"))).alias("PRESCRIBER_SPEC"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("VAL_CD"))).alias("VAL_CD"),
    F.trim(Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("PRI_CARE_PDX"))).alias("PRI_CARE_PDX"),
    Convert(F.lit("\u000A") + F.lit("\u000D") + F.lit("\u0009"), F.lit(""), F.col("OFC_OF_INSPECTOR_GNRL_OIG")).alias("OFC_OF_INSPECTOR_GNRL_OIG")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_ids = f"""
SELECT 
 MBR_UNIQ_KEY,
 MBR_SFX_NO,
 SUB_ID,
 GRP_ID,
 SUB_UNIQ_KEY
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.GRP GRP
WHERE MBR.SUB_SK = SUB.SUB_SK
  AND SUB.GRP_SK = GRP.GRP_SK
"""
df_ids = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids)
    .load()
)

df_mbr_uniq_key = df_ids.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY")
)

df_sub_id_sfx = df_ids.select(
    F.trim(F.col("SUB_ID")).alias("SUB_ID"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY")
)

df_mbr_uniq_key_dedup = dedup_sort(
    df_mbr_uniq_key,
    ["MBR_UNIQ_KEY"],
    []
)

df_sub_id_sfx_dedup = dedup_sort(
    df_sub_id_sfx,
    ["SUB_ID", "MBR_SFX_NO"],
    []
)

df_bl_joined = (
    df_esi.alias("Esi")
    .join(
        df_mbr_uniq_key_dedup.alias("find_mbr_ck_lkup"),
        (
            (F.col("Esi.MBR_UNIQ_KEY") == F.col("find_mbr_ck_lkup.MBR_UNIQ_KEY")) &
            (F.col("Esi.PRSN_CD") == F.col("find_mbr_ck_lkup.MBR_SFX_NO")) &
            (F.col("Esi.CARDHLDR_ID_NO") == F.col("find_mbr_ck_lkup.SUB_ID")) &
            (F.col("Esi.GRP_ID") == F.col("find_mbr_ck_lkup.GRP_ID"))
        ),
        "left"
    )
    .join(
        df_sub_id_sfx_dedup.alias("sub_id_mbr_sfx"),
        (
            (F.trim(F.col("Esi.CARDHLDR_ID_NO")) == F.col("sub_id_mbr_sfx.SUB_ID")) &
            (F.expr("substring(Esi.PRSN_CD,1,2)") == F.col("sub_id_mbr_sfx.MBR_SFX_NO"))
        ),
        "left"
    )
)

df_bl_vars = (
    df_bl_joined
    .withColumn("svAdjustedClaim", F.expr("CASE WHEN trim(Esi.CLM_TYP).substring(1,1)='R' THEN true ELSE false END"))
    .withColumn("svClaimID", F.expr("""
        CASE 
         WHEN svAdjustedClaim THEN concat(Esi.CLM_ID,'R')
         ELSE Esi.CLM_ID 
        END
    """))
    .withColumn("svCardHolderID", F.expr("""
        CASE 
         WHEN ISNULL(Esi.CARDHLDR_ID_NO) OR length(Esi.CARDHLDR_ID_NO)=0 THEN 'UNK'
         ELSE Esi.CARDHLDR_ID_NO
        END
    """))
    .withColumn("svMbrCk", F.expr("""
        CASE
         WHEN NOT ISNULL(find_mbr_ck_lkup.MBR_UNIQ_KEY) THEN find_mbr_ck_lkup.MBR_UNIQ_KEY
         WHEN NOT ISNULL(sub_id_mbr_sfx.MBR_UNIQ_KEY) THEN sub_id_mbr_sfx.MBR_UNIQ_KEY
         ELSE Esi.MBR_UNIQ_KEY
        END
    """))
    .withColumn("svGrpID", F.expr("""
        CASE
         WHEN NOT ISNULL(find_mbr_ck_lkup.MBR_UNIQ_KEY) THEN find_mbr_ck_lkup.GRP_ID
         WHEN NOT ISNULL(sub_id_mbr_sfx.MBR_UNIQ_KEY) THEN sub_id_mbr_sfx.GRP_ID
         ELSE Esi.GRP_ID
        END
    """))
    .withColumn("svFillDate", F.expr("""
        CASE 
         WHEN length(Esi.DT_FILLED)=0 THEN '1753-01-01'
         ELSE FORMAT.DATE(Esi.DT_FILLED,'CHAR','CCYYMMDD','CCYY-MM-DD')
        END
    """))
    .withColumn("svDOB", F.expr("""
        CASE 
         WHEN length(Esi.DOB)=0 THEN '1753-01-01'
         ELSE Esi.DOB
        END
    """))
    .withColumn("svMetricQty", F.expr("""
        CASE 
         WHEN svAdjustedClaim THEN -1*(Esi.METRIC_QTY) 
         ELSE Esi.METRIC_QTY
        END
    """))
    .withColumn("svDaysSuppl", F.expr("""
        CASE 
         WHEN svAdjustedClaim THEN -1*(Esi.DAYS_SUPL)
         ELSE Esi.DAYS_SUPL
        END
    """))
    .withColumn("svEsiAnclryAmt", F.expr("""
        CASE 
         WHEN svAdjustedClaim THEN -1*(Esi.ESI_ANCLRY_AMT)
         ELSE Esi.ESI_ANCLRY_AMT
        END
    """))
    .withColumn("svUslAndCustChrg", F.expr("""
        CASE 
         WHEN svAdjustedClaim THEN -1*(Esi.USL_AND_CUST_CHRG)
         ELSE Esi.USL_AND_CUST_CHRG
        END
    """))
    .withColumn("svMbrNonCopayAmt", F.expr("""
        CASE 
         WHEN svAdjustedClaim THEN -1*(Esi.MBR_NON_COPAY_AMT)
         ELSE Esi.MBR_NON_COPAY_AMT
        END
    """))
    .withColumn("svFullAWP", F.expr("""
        CASE 
         WHEN svAdjustedClaim THEN -1*(Esi.FULL_AWP)
         ELSE Esi.FULL_AWP
        END
    """))
    .withColumn("svESIBillDate", F.expr("""
        CASE 
         WHEN length(Esi.DT_FILLED)=0 THEN '1753-01-01'
         ELSE FORMAT.DATE(Esi.ESI_BILL_DT,'CHAR','CCYYMMDD','CCYY-MM-DD')
        END
    """))
)

df_MbrVerified = df_bl_vars.select(
    F.col("Esi.RCRD_ID").alias("RCRD_ID"),
    F.col("svClaimID").alias("CLAIM_ID"),
    F.col("Esi.PRCSR_NO").alias("PRCSR_NO"),
    F.col("svMbrCk").alias("MEM_CK_KEY"),
    F.col("Esi.BTCH_NO").alias("BTCH_NO"),
    F.col("Esi.PDX_NO").alias("PDX_NO"),
    F.col("Esi.RX_NO").alias("RX_NO"),
    F.col("svFillDate").alias("DT_FILLED"),
    F.col("Esi.NDC_NO").alias("NDC_NO"),
    F.col("Esi.DRUG_DESC").alias("DRUG_DESC"),
    F.col("Esi.NEW_RFL_CD").alias("NEW_RFL_CD"),
    F.col("svMetricQty").alias("METRIC_QTY"),
    F.col("svDaysSuppl").alias("DAYS_SUPL"),
    F.col("Esi.BSS_OF_CST_DTRM").alias("BSS_OF_CST_DTRM"),
    F.col("Esi.INGR_CST").alias("INGR_CST"),
    F.col("Esi.DISPNS_FEE").alias("DISPNS_FEE"),
    F.col("Esi.COPAY_AMT").alias("COPAY_AMT"),
    F.col("Esi.SLS_TAX").alias("SLS_TAX"),
    F.col("Esi.AMT_BILL").alias("AMT_BILL"),
    F.col("Esi.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("Esi.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("svDOB").alias("DOB"),
    F.col("Esi.SEX_CD").alias("SEX_CD"),
    F.col("svCardHolderID").alias("CARDHLDR_ID_NO"),
    F.col("Esi.RELSHP_CD").alias("RELSHP_CD"),
    F.col("Esi.GRP_NO").alias("GRP_NO"),
    F.col("Esi.HOME_PLN").alias("HOME_PLN"),
    F.col("Esi.HOST_PLN").alias("HOST_PLN"),
    F.col("Esi.PRESCRIBER_ID").alias("PRESCRIBER_ID"),
    F.col("Esi.DIAG_CD").alias("DIAG_CD"),
    F.col("Esi.CARDHLDR_FIRST_NM").alias("CARDHLDR_FIRST_NM"),
    F.col("Esi.CARDHLDR_LAST_NM").alias("CARDHLDR_LAST_NM"),
    F.col("Esi.PRAUTH_NO").alias("PRAUTH_NO"),
    F.col("Esi.PA_MC_SC_NO").alias("PA_MC_SC_NO"),
    F.col("Esi.CUST_LOC").alias("CUST_LOC"),
    F.col("Esi.RESUB_CYC_CT").alias("RESUB_CYC_CT"),
    F.col("Esi.DT_RX_WRTN").alias("DT_RX_WRTN"),
    F.col("Esi.DISPENSE_AS_WRTN_PROD_SEL_CD").alias("DISPENSE_AS_WRTN_PROD_SEL_CD"),
    F.col("Esi.PRSN_CD").alias("PRSN_CD"),
    F.col("Esi.OTHR_COV_CD").alias("OTHR_COV_CD"),
    F.col("Esi.ELIG_CLRFCTN_CD").alias("ELIG_CLRFCTN_CD"),
    F.col("Esi.CMPND_CD").alias("CMPND_CD"),
    F.col("Esi.NO_OF_RFLS_AUTH").alias("NO_OF_RFLS_AUTH"),
    F.col("Esi.LVL_OF_SVC").alias("LVL_OF_SVC"),
    F.col("Esi.RX_ORIG_CD").alias("RX_ORIG_CD"),
    F.col("Esi.RX_DENIAL_CLRFCTN").alias("RX_DENIAL_CLRFCTN"),
    F.col("Esi.PRI_PRESCRIBER").alias("PRI_PRESCRIBER"),
    F.col("Esi.CLNC_ID_NO").alias("CLNC_ID_NO"),
    F.col("Esi.DRUG_TYP").alias("DRUG_TYP"),
    F.col("Esi.PRESCRIBER_LAST_NM").alias("PRESCRIBER_LAST_NM"),
    F.col("Esi.POSTAGE_AMT_CLMED").alias("POSTAGE_AMT_CLMED"),
    F.col("Esi.UNIT_DOSE_IN").alias("UNIT_DOSE_IN"),
    F.col("Esi.OTHR_PAYOR_AMT").alias("OTHR_PAYOR_AMT"),
    F.col("Esi.BSS_OF_DAYS_SUPL_DTRM").alias("BSS_OF_DAYS_SUPL_DTRM"),
    F.col("svFullAWP").alias("FULL_AWP"),
    F.col("Esi.EXPNSN_AREA").alias("EXPNSN_AREA"),
    F.col("Esi.MSTR_CAR").alias("MSTR_CAR"),
    F.col("Esi.SUB_CAR").alias("SUB_CAR"),
    F.col("Esi.CLM_TYP").alias("CLM_TYP"),
    F.col("Esi.ESI_SUB_GRP").alias("ESI_SUB_GRP"),
    F.col("Esi.PLN_DSGNR").alias("PLN_DSGNR"),
    F.col("Esi.ADJDCT_DT").alias("ADJDCT_DT"),
    F.col("Esi.ADMIN_FEE").alias("ADMIN_FEE"),
    F.col("Esi.CAP_AMT").alias("CAP_AMT"),
    F.col("Esi.INGR_CST_SUB").alias("INGR_CST_SUB"),
    F.col("svMbrNonCopayAmt").alias("MBR_NON_COPAY_AMT"),
    F.col("Esi.MBR_PAY_CD").alias("MBR_PAY_CD"),
    F.col("Esi.INCNTV_FEE").alias("INCNTV_FEE"),
    F.col("Esi.CLM_ADJ_AMT").alias("CLM_ADJ_AMT"),
    F.col("Esi.CLM_ADJ_CD").alias("CLM_ADJ_CD"),
    F.col("Esi.FRMLRY_FLAG").alias("FRMLRY_FLAG"),
    F.col("Esi.GNRC_CLS_NO").alias("GNRC_CLS_NO"),
    F.col("Esi.THRPTC_CLS_AHFS").alias("THRPTC_CLS_AHFS"),
    F.col("Esi.PDX_TYP").alias("PDX_TYP"),
    F.col("Esi.BILL_BSS_CD").alias("BILL_BSS_CD"),
    F.col("svUslAndCustChrg").alias("USL_AND_CUST_CHRG"),
    F.col("Esi.PD_DT").alias("PD_DT"),
    F.col("Esi.BNF_CD").alias("BNF_CD"),
    F.col("Esi.DRUG_STRG").alias("DRUG_STRG"),
    F.col("Esi.ORIG_MBR").alias("ORIG_MBR"),
    F.col("Esi.DT_OF_INJURY").alias("DT_OF_INJURY"),
    F.col("Esi.FEE_AMT").alias("FEE_AMT"),
    F.col("Esi.ESI_REF_NO").alias("ESI_REF_NO"),
    F.col("Esi.CLNT_CUST_ID").alias("CLNT_CUST_ID"),
    F.col("Esi.PLN_TYP").alias("PLN_TYP"),
    F.col("Esi.ESI_ADJDCT_REF_NO").alias("ESI_ADJDCT_REF_NO"),
    F.col("svEsiAnclryAmt").alias("ESI_ANCLRY_AMT"),
    F.col("svGrpID").alias("GRP_ID"),
    F.col("Esi.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("Esi.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("Esi.PD_DT").alias("PAID_DATE"),
    F.col("Esi.PRTL_FILL_STTUS_CD").alias("PRTL_FILL_STTUS_CD"),
    F.col("svESIBillDate").alias("ESI_BILL_DT"),
    F.col("Esi.FSA_VNDR_CD").alias("FSA_VNDR_CD"),
    F.col("Esi.PICA_DRUG_CD").alias("PICA_DRUG_CD"),
    F.col("Esi.AMT_CLMED").alias("AMT_CLMED"),
    F.col("Esi.AMT_DSALW").alias("AMT_DSALW"),
    F.col("Esi.FED_DRUG_CLS_CD").alias("FED_DRUG_CLS_CD"),
    F.col("Esi.DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("Esi.BNF_COPAY_100").alias("BNF_COPAY_100"),
    F.col("Esi.CLM_PRCS_TYP").alias("CLM_PRCS_TYP"),
    F.col("Esi.INDEM_HIER_TIER_NO").alias("INDEM_HIER_TIER_NO"),
    F.col("Esi.FLR").alias("FLR"),
    F.col("Esi.MCARE_D_COV_DRUG").alias("MCARE_D_COV_DRUG"),
    F.col("Esi.RETRO_LICS_CD").alias("RETRO_LICS_CD"),
    F.col("Esi.RETRO_LICS_AMT").alias("RETRO_LICS_AMT"),
    F.col("Esi.LICS_SBSDY_AMT").alias("LICS_SBSDY_AMT"),
    F.col("Esi.MED_B_DRUG").alias("MED_B_DRUG"),
    F.col("Esi.MED_B_CLM").alias("MED_B_CLM"),
    F.col("Esi.PRESCRIBER_QLFR").alias("PRESCRIBER_QLFR"),
    F.col("Esi.PRESCRIBER_ID_NPI").alias("PRESCRIBER_ID_NPI"),
    F.col("Esi.PDX_QLFR").alias("PDX_QLFR"),
    F.col("Esi.PDX_ID_NPI").alias("PDX_ID_NPI"),
    F.col("Esi.HRA_APLD_AMT").alias("HRA_APLD_AMT"),
    F.col("Esi.ESI_THER_CLS").alias("ESI_THER_CLS"),
    F.col("Esi.HIC_NO").alias("HIC_NO"),
    F.col("Esi.HRA_FLAG").alias("HRA_FLAG"),
    F.col("Esi.DOSE_CD").alias("DOSE_CD"),
    F.col("Esi.LOW_INCM").alias("LOW_INCM"),
    F.col("Esi.RTE_OF_ADMIN").alias("RTE_OF_ADMIN"),
    F.col("Esi.DEA_SCHD").alias("DEA_SCHD"),
    F.col("Esi.COPAY_BNF_OPT").alias("COPAY_BNF_OPT"),
    F.col("Esi.GNRC_PROD_IN_GPI").alias("GNRC_PROD_IN_GPI"),
    F.col("Esi.PRESCRIBER_SPEC").alias("PRESCRIBER_SPEC"),
    F.col("Esi.VAL_CD").alias("VAL_CD"),
    F.col("Esi.PRI_CARE_PDX").alias("PRI_CARE_PDX"),
    F.col("Esi.OFC_OF_INSPECTOR_GNRL_OIG").alias("OFC_OF_INSPECTOR_GNRL_OIG")
)

# Applying rpad for char/varchar columns with their lengths exactly as declared in the final pin's metadata:
df_final = (
    df_MbrVerified
    .withColumn("PDX_NO", F.rpad(F.col("PDX_NO"), 15, " "))
    .withColumn("DRUG_DESC", F.rpad(F.col("DRUG_DESC"), 30, " "))
    .withColumn("BSS_OF_CST_DTRM", F.rpad(F.col("BSS_OF_CST_DTRM"), 2, " "))
    .withColumn("PATN_FIRST_NM", F.rpad(F.col("PATN_FIRST_NM"), 12, " "))
    .withColumn("PATN_LAST_NM", F.rpad(F.col("PATN_LAST_NM"), 15, " "))
    .withColumn("CARDHLDR_ID_NO", F.rpad(F.col("CARDHLDR_ID_NO"), 18, " "))
    .withColumn("GRP_NO", F.rpad(F.col("GRP_NO"), 18, " "))
    .withColumn("HOME_PLN", F.rpad(F.col("HOME_PLN"), 3, " "))
    .withColumn("PRESCRIBER_ID", F.rpad(F.col("PRESCRIBER_ID"), 15, " "))
    .withColumn("DIAG_CD", F.rpad(F.col("DIAG_CD"), 6, " "))
    .withColumn("CARDHLDR_FIRST_NM", F.rpad(F.col("CARDHLDR_FIRST_NM"), 12, " "))
    .withColumn("CARDHLDR_LAST_NM", F.rpad(F.col("CARDHLDR_LAST_NM"), 15, " "))
    .withColumn("PA_MC_SC_NO", F.rpad(F.col("PA_MC_SC_NO"), 7, " "))
    .withColumn("DISPENSE_AS_WRTN_PROD_SEL_CD", F.rpad(F.col("DISPENSE_AS_WRTN_PROD_SEL_CD"), 1, " "))
    .withColumn("PRSN_CD", F.rpad(F.col("PRSN_CD"), 2, " "))
    .withColumn("PRI_PRESCRIBER", F.rpad(F.col("PRI_PRESCRIBER"), 10, " "))
    .withColumn("PRESCRIBER_LAST_NM", F.rpad(F.col("PRESCRIBER_LAST_NM"), 15, " "))
    .withColumn("EXPNSN_AREA", F.rpad(F.col("EXPNSN_AREA"), 1, " "))
    .withColumn("MSTR_CAR", F.rpad(F.col("MSTR_CAR"), 4, " "))
    .withColumn("SUB_CAR", F.rpad(F.col("SUB_CAR"), 4, " "))
    .withColumn("CLM_TYP", F.rpad(F.col("CLM_TYP"), 1, " "))
    .withColumn("ESI_SUB_GRP", F.rpad(F.col("ESI_SUB_GRP"), 20, " "))
    .withColumn("PLN_DSGNR", F.rpad(F.col("PLN_DSGNR"), 1, " "))
    .withColumn("ADJDCT_DT", F.rpad(F.col("ADJDCT_DT"), 10, " "))
    .withColumn("CLM_ADJ_CD", F.rpad(F.col("CLM_ADJ_CD"), 2, " "))
    .withColumn("FRMLRY_FLAG", F.rpad(F.col("FRMLRY_FLAG"), 1, " "))
    .withColumn("GNRC_CLS_NO", F.rpad(F.col("GNRC_CLS_NO"), 14, " "))
    .withColumn("THRPTC_CLS_AHFS", F.rpad(F.col("THRPTC_CLS_AHFS"), 6, " "))
    .withColumn("PDX_TYP", F.rpad(F.col("PDX_TYP"), 1, " "))
    .withColumn("BILL_BSS_CD", F.rpad(F.col("BILL_BSS_CD"), 2, " "))
    .withColumn("BNF_CD", F.rpad(F.col("BNF_CD"), 10, " "))
    .withColumn("DRUG_STRG", F.rpad(F.col("DRUG_STRG"), 10, " "))
    .withColumn("ORIG_MBR", F.rpad(F.col("ORIG_MBR"), 2, " "))
    .withColumn("ESI_REF_NO", F.rpad(F.col("ESI_REF_NO"), 14, " "))
    .withColumn("CLNT_CUST_ID", F.rpad(F.col("CLNT_CUST_ID"), 20, " "))
    .withColumn("PLN_TYP", F.rpad(F.col("PLN_TYP"), 10, " "))
    .withColumn("GRP_ID", F.rpad(F.col("GRP_ID"), 8, " "))
    .withColumn("SUBGRP_ID", F.rpad(F.col("SUBGRP_ID"), 4, " "))
    .withColumn("CLS_PLN_ID", F.rpad(F.col("CLS_PLN_ID"), 4, " "))
    .withColumn("PAID_DATE", F.rpad(F.col("PAID_DATE"), 10, " "))
    .withColumn("PRTL_FILL_STTUS_CD", F.rpad(F.col("PRTL_FILL_STTUS_CD"), 1, " "))
    .withColumn("ESI_BILL_DT", F.rpad(F.col("ESI_BILL_DT"), 10, " "))
    .withColumn("FSA_VNDR_CD", F.rpad(F.col("FSA_VNDR_CD"), 2, " "))
    .withColumn("PICA_DRUG_CD", F.rpad(F.col("PICA_DRUG_CD"), 1, " "))
    .withColumn("FED_DRUG_CLS_CD", F.rpad(F.col("FED_DRUG_CLS_CD"), 1, " "))
    .withColumn("BNF_COPAY_100", F.rpad(F.col("BNF_COPAY_100"), 1, " "))
    .withColumn("CLM_PRCS_TYP", F.rpad(F.col("CLM_PRCS_TYP"), 1, " "))
    .withColumn("MCARE_D_COV_DRUG", F.rpad(F.col("MCARE_D_COV_DRUG"), 1, " "))
    .withColumn("RETRO_LICS_CD", F.rpad(F.col("RETRO_LICS_CD"), 1, " "))
    .withColumn("MED_B_DRUG", F.rpad(F.col("MED_B_DRUG"), 1, " "))
    .withColumn("MED_B_CLM", F.rpad(F.col("MED_B_CLM"), 1, " "))
    .withColumn("PRESCRIBER_QLFR", F.rpad(F.col("PRESCRIBER_QLFR"), 2, " "))
    .withColumn("PRESCRIBER_ID_NPI", F.rpad(F.col("PRESCRIBER_ID_NPI"), 10, " "))
    .withColumn("PDX_QLFR", F.rpad(F.col("PDX_QLFR"), 2, " "))
    .withColumn("PDX_ID_NPI", F.rpad(F.col("PDX_ID_NPI"), 10, " "))
    .withColumn("HIC_NO", F.rpad(F.col("HIC_NO"), 12, " "))
    .withColumn("HRA_FLAG", F.rpad(F.col("HRA_FLAG"), 1, " "))
    .withColumn("LOW_INCM", F.rpad(F.col("LOW_INCM"), 1, " "))
    .withColumn("RTE_OF_ADMIN", F.rpad(F.col("RTE_OF_ADMIN"), 2, " "))
    .withColumn("PRESCRIBER_SPEC", F.rpad(F.col("PRESCRIBER_SPEC"), 10, " "))
    .withColumn("VAL_CD", F.rpad(F.col("VAL_CD"), 18, " "))
    .withColumn("PRI_CARE_PDX", F.rpad(F.col("PRI_CARE_PDX"), 18, " "))
    .withColumn("OFC_OF_INSPECTOR_GNRL_OIG", F.rpad(F.col("OFC_OF_INSPECTOR_GNRL_OIG"), 1, " "))
)

write_files(
    df_final,
    f"{adls_path}/verified/ESIDrugClmInvoicePaidUpdt.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)