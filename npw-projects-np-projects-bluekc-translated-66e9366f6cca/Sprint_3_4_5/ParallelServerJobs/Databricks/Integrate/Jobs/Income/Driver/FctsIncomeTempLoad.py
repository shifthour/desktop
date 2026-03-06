# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC NEW hf: hf_fcts_incm_drvr_future_invc  
# MAGIC 
# MAGIC Â© Copyright 2006, 2007, 2010, 2011 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  FctsIncomeTempLoad
# MAGIC Called by:     FctsIncomePrereqSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Extract invoices from Facets and load into TMP_IDS_INCOME.
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Create temp table, if necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC                            
# MAGIC Hashed Files:
# MAGIC                     14;hf_fcts_incm_drvr_invc_chngd;hf_fcts_incm_drvr_dscrtny_chngd;hf_fcts_incm_drvr_subprem_chng;hf_fcts_incm_drvr_invc_due;hf_fcts_incm_drvr_subprem_due;hf_fcts_incm_drvr_rebilled_invc;hf_fcts_incm_drvr_fee_chngd;hf_fcts_incm_drvr_discrtny_due;hf_fcts_incm_drvr_fee_due;hf_fcts_incm_drvr_invc_cmpnt_ct;hf_fcts_incm_drvr_future_invc;hf_invc_drvr_dup_rem;hf_fcts_incm_drvr_subsidy_subprem_chng;hf_fcts_incm_drvr_ChangedInvc_CurrentMonth
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                                                                                                                        Code                   Date
# MAGIC Developer           Date              Altiris #     Change Description                                                                                                                          Environment                                       Reviewer            Reviewed
# MAGIC -------------------------  -------------------   -------------   -----------------------------------------------------------------------------------------------------------------------------                            --------------------                                       -------------------------  -------------------
# MAGIC Sharon Andrew   05/01/2006                  Originally Programmed
# MAGIC Sharon Andrew   03/21/2007                  Re-Org'd to not pull in future due dated' invoices, to pull in invoices if subscriber 
# MAGIC                                                                  premiums have been added, to pull in invoices that have been rebilled, 
# MAGIC Naren Garapaty  05/18/2007                   Re-Org'd the due_now,changed and no longer last extract in the table
# MAGIC SAndrew             2009-10-08    Altiris#     Change the amount going into the FEE_DSNCT_AMT field  from                                                                                                                Steph Goddard  10/09/2009
# MAGIC                                                  223143    ( INFD.BLFD_FEE_AMT +  INFD.BLFD_DISC_AMT + INFD.BLFD_FEE_AMT)
# MAGIC                                                  TTR 613  To  - new stage varialble 
# MAGIC                                                                   iF  Extract.PMFA_FEE_DISC_IND = 'D' THEN (-1 *  Extract.BLFD_DISC_AMT)
# MAGIC                                                                   ELSE  
# MAGIC                                                                   iF Extract.PMFA_FEE_DISC_IND = 'F' THEN  Extract.BLFD_FEE_AMT
# MAGIC                                                                   and no longer add them together - just move appropriate field
# MAGIC Hugh Sisson       2010-09-17    3346        Added SQL to pull rows for INVC_CMPNT_CT.                                                                                                                                            Steph Goddard    09/21/2010               
# MAGIC                                                                  Add new hashed file - hf_fcts_incm_drvr_invc_cmpnt_ct and added the                  
# MAGIC                                                                  hashed file's name to the After-job subroutine HASH.CLEAR Input Value list.           
# MAGIC                                                                  Upgraded Full job description entry to meet current standards.                                 
# MAGIC                                                                  Gave Link_Collector stage and Transformer stage meaningful names.          
# MAGIC 
# MAGIC Terri O'Bryan       2011-04-22   3346      1. Added new extract \(201C)future_invc\(201D). Added NEW hash file                                                                                                                              SAndrew             2011-04-25
# MAGIC                                                                   (hf_fcts_incm_drvr_future_invc) for this extract and added it to HASH.CLEAR.  
# MAGIC                                                               2. made following [existing]DEFECT corrections:  (a)In the changed_fee section it should 
# MAGIC                                                                  say \(2026)INVC.BLBL_DUE_DT < '#DueBeginDate#'.  Not <=.  [Changed SQL for
# MAGIC                                                                  extract changed_fee]  (b) In the due_now_fee section it should say \(2026)
# MAGIC                                                                  INVC.BLBL_DUE_DT <= '#DueEndDate#'.  Not the DueBeginDate.   
# MAGIC                                                                  [Changed SQL for extract due_now_fee]  (c) All the records in the 
# MAGIC                                                                  Cmsn_subprem_chngd file have a blank Due Date   [Changed input  
# MAGIC                                                                  hf \(2018)hf_fcts_incm_drvr_subprem_chng\(2019) to indicate BLBL_DUE_DT as key field.]
# MAGIC                                                              3. Removed "distinct" and "group by" in all extract SQL     
# MAGIC                                                              4. Added "RUNSTATS" to After-SQL tab in 'Driver Table' stage to improve 
# MAGIC                                                                  performance on downstream joins.        
# MAGIC Karen made me.
# MAGIC 12;hf_fcts_incm_drvr_invc_chngd;hf_fcts_incm_drvr_dscrtny_chngd;hf_fcts_incm_drvr_subprem_chng;hf_fcts_incm_drvr_invc_due;hf_fcts_incm_drvr_subprem_due;hf_fcts_incm_drvr_rebilled_invc;hf_fcts_incm_drvr_fee_chngd;hf_fcts_incm_drvr_discrtny_due;hf_fcts_incm_drvr_fee_due;hf_fcts_incm_drvr_invc_cmpnt_ct;hf_fcts_incm_drvr_future_invc;hf_invc_drvr_dup_rem
# MAGIC 
# MAGIC Dan Long       2013-10-24 TFS-1429   Removed the following code from the no_longer_last_bliv_id extract:                                          IntegrateNewDevl                                         Kalyan Neelam     2013-11-06
# MAGIC                                                             AND INVC.BLIV_CREATE_DTM   >=   '#CreateBeginDate#'
# MAGIC                                                             AND INVC.BLIV_CREATE_DTM   <=   '#CreateEndDate#'         
# MAGIC                                                              
# MAGIC                                                             Changed After-job subroutine to HASH.CLEAR and add the following to the Input Value:
# MAGIC                                                             12;hf_fcts_incm_drvr_invc_chngd;hf_fcts_incm_drvr_dscrtny_chngd;hf_fcts_incm_drvr_subprem_chng;hf_fcts_incm_drvr_invc_due;
# MAGIC                                                                  hf_fcts_incm_drvr_subprem_due;hf_fcts_incm_drvr_rebilled_invc;hf_fcts_incm_drvr_fee_chngd;hf_fcts_incm_drvr_discrtny_due;
# MAGIC                                                                  hf_fcts_incm_drvr_fee_due;hf_fcts_incm_drvr_invc_cmpnt_ct;hf_fcts_incm_drvr_future_invc;hf_invc_drvr_dup_rem    hf_invc_drvr_dup_rem
# MAGIC 
# MAGIC SAndrew        2013-12-02  ProdAbend   Added back in the criteria that TTR-1429 requested to take out.        The reason was the criteria will only allow those Billing Invoices that were created in the past month AND that since they were created are now no longer the last bill.   IE A new invoice has been created for that bill.   There should now be another new BLIV created in the same month or later month that is now the last bill.   When it was taken out, every single invoice that had been changed met the criteria and was extracted causing the job to abended.  Hash file sizes were exceeded.
# MAGIC                                                             AND INVC.BLIV_CREATE_DTM   >=   '#CreateBeginDate#'
# MAGIC                                                             AND INVC.BLIV_CREATE_DTM   <=   '#CreateEndDate#'      
# MAGIC 
# MAGIC Kalyan NEelam  2014-06-20   5235        Added new output link from sourrce "changed_subsidiary_subprem" to extract data for SUB_SBSDY   IntegrateNewDevl                            Bhoomi Dasari       6/21/2014 
# MAGIC 
# MAGIC Reddy Sanam   2020-11-20  317542     Added new output link from source "changed_invc_current_month" to extract changed invoices in
# MAGIC                                                                Current Month         
# MAGIC                                                                Added hash file and output link for the new source link                                                                                                                                 IntegrateDev2             Goutham K             2020-11-24
# MAGIC 
# MAGIC Goutham Kalidindi  2021-03-24 358186    Changed Datatype length for field BLBL_BLIV_ID_LAST                                                                                                                           IntegrateDev2
# MAGIC                                                                   char(12) to Varchar(15)
# MAGIC Prabhu ES             2022-03-02                 S2S Remediation-MSSQL ODBC conn added and other param changes                                                                                                    IntegrateDev5\(9)Ken Bradmon\(9)2022-06-13

# MAGIC Remove Duplicate records
# MAGIC Extract changed invoices or invoice supporting data
# MAGIC Insert records into driver table into BILL_INVC_ID, the only field on the table TMP_IDS_INCOME #RunId#
# MAGIC End Load of invoices into TMP_IDS_INCOME#### field = .BILL_INVC_ID
# MAGIC **The driver for the IDS Income will be based on any changed/new invoices on the CDS_INID_INVOICE, .CDS_INDI_DISCRETN  
# MAGIC    INDI , and CMC_INSB_SB_DETAIL. 
# MAGIC ** All invoices must be for a special billing type of   IN ('N','F','P','C') .    
# MAGIC ** If there is a create date within the CharngedDateBegin and ChangedDateEndDt on any of the three table  the invoice is extracted.    
# MAGIC **If there is an invoice due or a subpremium due with the DueDateBeginDate and DueDateEndDate, then the invoice is extracted and loaded into the tmp table.   
# MAGIC ** The temp table is created by script /ids/###/script/tmp_income.ksh              
# MAGIC ** In all following extraction jobs, the temp tables field BILL_INVC_ID field will be match to every BLIV_ID field on facets income table. If match, the BLIV_ID record will be extracted.
# MAGIC Same Invoices Different Due Dates
# MAGIC NOTE:  Added the following code to SQL 'After' tab to improve downstream join performance (2011-04-25):  
# MAGIC UPDATE ALL STATISTICS tempdb..#DriverTable#;
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CreateBeginDate = get_widget_value('CreateBeginDate','')
CreateEndDate = get_widget_value('CreateEndDate','')
DueBeginDate = get_widget_value('DueBeginDate','')
DueEndDate = get_widget_value('DueEndDate','')
DriverTable = get_widget_value('DriverTable','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
InvcSubSbsdyBegDt = get_widget_value('InvcSubSbsdyBegDt','')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

extract_query_due_now_invc = f"""
SELECT 
     INVC.BLIV_ID BILL_INVC_ID,
     INVC.BLBL_DUE_DT
FROM  
     {FacetsOwner}.CDS_INID_INVOICE INVC,
     {FacetsOwner}.CMC_BLBL_BILL_SUMM SUMM
WHERE 
     INVC.BLIV_ID=SUMM.BLBL_BLIV_ID_LAST
 AND INVC.BLBL_DUE_DT >= '{DueBeginDate}'
 AND INVC.BLBL_DUE_DT <= '{DueEndDate}'
 AND INVC.BLBL_SPCL_BL_IND IN ('N','F','P','C') 
 AND INVC.AFAI_CK = 0
"""
df_due_now_invc = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_due_now_invc)
    .load()
)

extract_query_changed_discrtny = f"""
SELECT
     INVC.BLIV_ID BILL_INVC_ID,
     INVC.BLBL_DUE_DT
FROM  
     {FacetsOwner}.CDS_INID_INVOICE INVC,
     {FacetsOwner}.CDS_INDI_DISCRETN INDI,
     {FacetsOwner}.CMC_BLBL_BILL_SUMM SUMM
WHERE
     INDI.BLDI_UPDATE_DTM >= '{CreateBeginDate}'
 AND INDI.BLDI_UPDATE_DTM <= '{CreateEndDate}'
 AND INDI.BLIV_ID = INVC.BLIV_ID
 AND INVC.BLIV_ID = SUMM.BLBL_BLIV_ID_LAST
 AND INVC.BLBL_DUE_DT < '{DueBeginDate}'
 AND INVC.BLBL_DUE_DT >= '2000-01-01 00:00:00.000'
 AND INVC.BLBL_SPCL_BL_IND IN ('N','F','P','C')
 AND INVC.AFAI_CK = 0
"""
df_changed_discrtny = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_changed_discrtny)
    .load()
)

extract_query_changed_subPrem = f"""
SELECT
     INVC.BLIV_ID BILL_INVC_ID,
     INVC.BLBL_DUE_DT
FROM  
     {FacetsOwner}.CDS_INID_INVOICE INVC,
     {FacetsOwner}.CDS_INSB_SB_DETAIL BLSB,
     {FacetsOwner}.CMC_BLBL_BILL_SUMM SUMM
WHERE
     BLSB.BLSB_CREATE_DTM >= '{CreateBeginDate}'
 AND BLSB.BLSB_CREATE_DTM <= '{CreateEndDate}'
 AND BLSB.BLIV_ID = INVC.BLIV_ID
 AND INVC.BLIV_ID = SUMM.BLBL_BLIV_ID_LAST
 AND INVC.BLBL_DUE_DT < '{DueBeginDate}'
 AND INVC.BLBL_DUE_DT >= '2000-01-01 00:00:00.000'
 AND INVC.BLBL_SPCL_BL_IND IN ('N','F','P','C')
 AND INVC.AFAI_CK = 0
 AND ( BLSB.BLSB_PREM_SB <> 0.00 OR BLSB.BLSB_PREM_DEP <> 0.00 )
"""
df_changed_subPrem = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_changed_subPrem)
    .load()
)

extract_query_changed_invc = f"""
SELECT
     INVC.BLIV_ID BILL_INVC_ID,
     INVC.BLBL_DUE_DT
FROM  
     {FacetsOwner}.CDS_INID_INVOICE INVC,
     {FacetsOwner}.CMC_BLBL_BILL_SUMM SUMM
WHERE
     INVC.BLIV_ID=SUMM.BLBL_BLIV_ID_LAST
 AND INVC.BLBL_CREATE_DTM >= '{CreateBeginDate}'
 AND INVC.BLBL_CREATE_DTM <= '{CreateEndDate}'
 AND INVC.BLBL_DUE_DT < '{DueBeginDate}'
 AND INVC.BLBL_DUE_DT >= '2000-01-01 00:00:00.000'
 AND INVC.BLBL_SPCL_BL_IND IN ('N','F','P','C')
 AND INVC.AFAI_CK = 0
"""
df_changed_invc = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_changed_invc)
    .load()
)

extract_query_due_now_subPrem = f"""
SELECT
 INVC.BLIV_ID BILL_INVC_ID,
 INVC.BLBL_DUE_DT
FROM
 {FacetsOwner}.CDS_INSB_SB_DETAIL BLSB,
 {FacetsOwner}.CDS_INID_INVOICE INVC,
 {FacetsOwner}.CMC_BLBL_BILL_SUMM SUMM
WHERE
 BLSB.BLSB_CREATE_DTM <= '{CreateEndDate}'
 AND BLSB.BLIV_ID = INVC.BLIV_ID
 AND INVC.BLIV_ID = SUMM.BLBL_BLIV_ID_LAST
 AND INVC.BLBL_DUE_DT >= '{DueBeginDate}'
 AND INVC.BLBL_DUE_DT <= '{DueEndDate}'
 AND INVC.BLBL_SPCL_BL_IND IN ('N','F','P','C')
 AND INVC.AFAI_CK = 0
 AND (BLSB.BLSB_PREM_SB<>0.00 or BLSB.BLSB_PREM_DEP<>0.00)
"""
df_due_now_subPrem = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_due_now_subPrem)
    .load()
)

extract_query_no_longer_last_bliv_id = f"""
SELECT 
 INVC.BLIV_ID BILL_INVC_ID,
 BILL_INVC.BLBL_DUE_DT
FROM  {FacetsOwner}.CDS_INID_INVOICE INVC,
      {FacetsOwner}.CMC_BLIV_INVOICE BILL_INVC,
      {FacetsOwner}.CMC_BLBL_BILL_SUMM SUMM
WHERE
 BILL_INVC.BLBL_DUE_DT < '{DueBeginDate}'
 AND BILL_INVC.BLBL_DUE_DT >= '2000-01-01 00:00:00.000'
 AND BILL_INVC.BLIV_ID = INVC.BLIV_ID
 AND BILL_INVC.BLEI_CK = SUMM.BLEI_CK
 AND BILL_INVC.BLBL_DUE_DT = SUMM.BLBL_DUE_DT
 AND INVC.BLIV_ID <> SUMM.BLBL_BLIV_ID_LAST
 AND INVC.BLBL_CREATE_DTM <> INVC.BLIV_CREATE_DTM
 AND INVC.AFAI_CK = 0
 AND INVC.BLBL_SPCL_BL_IND In ('N','F','P','C')
 AND INVC.BLIV_CREATE_DTM >= '{CreateBeginDate}'
 AND INVC.BLIV_CREATE_DTM <= '{CreateEndDate}'
"""
df_no_longer_last_bliv_id = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_no_longer_last_bliv_id)
    .load()
)

extract_query_changed_fee = f"""
SELECT
 INVC.BLIV_ID,
 INVC.BLBL_DUE_DT
FROM  {FacetsOwner}.CDS_INID_INVOICE INVC,
      {FacetsOwner}.CDS_INFD_FEES_DISC FEE,
      {FacetsOwner}.CMC_BLBL_BILL_SUMM SUMM
WHERE
 FEE.BLIV_ID = INVC.BLIV_ID
 AND INVC.BLIV_ID = SUMM.BLBL_BLIV_ID_LAST
 AND INVC.BLBL_DUE_DT < '{DueBeginDate}'
 AND INVC.BLBL_DUE_DT >= '2000-01-01 00:00:00.000'
 AND INVC.BLBL_SPCL_BL_IND IN ('N','F','P','C')
 AND INVC.AFAI_CK = 0
 AND ( FEE.PMFA_FEE_DISC_IND = 'D' OR FEE.PMFA_FEE_DISC_IND = 'F')
"""
df_changed_fee = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_changed_fee)
    .load()
)

extract_query_due_now_discrtny = f"""
SELECT
 INVC.BLIV_ID,
 INVC.BLBL_DUE_DT
FROM  {FacetsOwner}.CDS_INID_INVOICE INVC,
      {FacetsOwner}.CDS_INDI_DISCRETN INDI,
      {FacetsOwner}.CMC_BLBL_BILL_SUMM SUMM
WHERE
 INDI.BLDI_UPDATE_DTM >= '{CreateBeginDate}'
 AND INDI.BLDI_UPDATE_DTM <= '{CreateEndDate}'
 AND INDI.BLIV_ID = INVC.BLIV_ID
 AND INVC.BLIV_ID = SUMM.BLBL_BLIV_ID_LAST
 AND INVC.BLBL_DUE_DT >= '{DueBeginDate}'
 AND INVC.BLBL_DUE_DT <= '{DueEndDate}'
 AND INVC.BLBL_SPCL_BL_IND IN ('N','F','P','C')
 AND INVC.AFAI_CK = 0
"""
df_due_now_discrtny = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_due_now_discrtny)
    .load()
)

extract_query_due_now_fee = f"""
SELECT
 INVC.BLIV_ID,
 INVC.BLBL_DUE_DT
FROM  {FacetsOwner}.CDS_INID_INVOICE INVC,
      {FacetsOwner}.CDS_INFD_FEES_DISC FEE,
      {FacetsOwner}.CMC_BLBL_BILL_SUMM SUMM
WHERE
 FEE.BLIV_ID = INVC.BLIV_ID
 AND INVC.BLIV_ID = SUMM.BLBL_BLIV_ID_LAST
 AND INVC.BLBL_DUE_DT >= '{DueBeginDate}'
 AND INVC.BLBL_DUE_DT <= '{DueEndDate}'
 AND INVC.BLBL_SPCL_BL_IND IN ('N','F','P','C')
 AND INVC.AFAI_CK = 0
 AND ( FEE.PMFA_FEE_DISC_IND = 'D' OR FEE.PMFA_FEE_DISC_IND = 'F')
"""
df_due_now_fee = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_due_now_fee)
    .load()
)

extract_query_cmpnt_ct_invc = f"""
SELECT
 INVC.BLIV_ID,
 INVC.BLBL_DUE_DT
FROM  
 {FacetsOwner}.CDS_INID_INVOICE INVC,
 {FacetsOwner}.CDS_INCT_COMPONENT INCT
WHERE
 INVC.BLIV_ID=INCT.BLIV_ID
 AND INVC.BLBL_DUE_DT >= '{DueBeginDate}'
 AND INVC.BLBL_DUE_DT <= '{DueEndDate}'
 AND INVC.BLBL_SPCL_BL_IND IN ('N','F','P','C')
 AND INVC.AFAI_CK = 0
"""
df_cmpnt_ct_invc = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_cmpnt_ct_invc)
    .load()
)

extract_query_future_invc = f"""
SELECT 
 INVC.BLIV_ID,
 INVC.BLBL_DUE_DT
FROM  
 {FacetsOwner}.CDS_INID_INVOICE INVC,
 {FacetsOwner}.CMC_BLBL_BILL_SUMM SUMM,
 {FacetsOwner}.CMC_BLRC_BILL_RCPT RCPT
WHERE
 INVC.BLIV_ID = SUMM.BLBL_BLIV_ID_LAST
 AND SUMM.BLEI_CK = RCPT.BLEI_CK
 AND SUMM.BLBL_DUE_DT = RCPT.BLBL_DUE_DT
 AND INVC.BLBL_SPCL_BL_IND IN ('N','F','P','C')
 AND INVC.AFAI_CK = 0
 AND RCPT.BLRC_CREATE_DTM >= '{CreateBeginDate}'
 AND RCPT.BLRC_CREATE_DTM <= '{CreateEndDate}'
"""
df_future_invc = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_future_invc)
    .load()
)

extract_query_changed_subsidiary_subPrem = f"""
SELECT
 INVC.BLIV_ID BILL_INVC_ID,
 INVC.BLBL_DUE_DT
FROM  
 {FacetsOwner}.CDS_INID_INVOICE INVC,
 {FacetsOwner}.CDS_INSS_SB_SUB SUB,
 {FacetsOwner}.CMC_BLBL_BILL_SUMM SUMM
WHERE
 SUB.BLSB_CREATE_DTM >= '{InvcSubSbsdyBegDt}'
 AND SUB.BLSB_CREATE_DTM <= '{CreateEndDate}'
 AND SUB.BLIV_ID = INVC.BLIV_ID
 AND INVC.BLIV_ID = SUMM.BLBL_BLIV_ID_LAST
 AND INVC.BLBL_DUE_DT < '{DueBeginDate}'
 AND INVC.BLBL_DUE_DT >= '2014-01-01 00:00:00.000'
 AND INVC.BLBL_SPCL_BL_IND IN ( 'N', 'F', 'P', 'C' )
 AND INVC.AFAI_CK = 0
 AND (SUB.BLSS_PREM_SB <> 0.00  OR  SUB.BLSS_PREM_DEP <> 0.00 )
"""
df_changed_subsidiary_subPrem = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_changed_subsidiary_subPrem)
    .load()
)

extract_query_changed_invc_current_month = f"""
SELECT
 INVC.BLIV_ID,
 BILL_INVC.BLBL_DUE_DT
FROM  {FacetsOwner}.CDS_INID_INVOICE INVC,
      {FacetsOwner}.CMC_BLIV_INVOICE BILL_INVC,
      {FacetsOwner}.CMC_BLBL_BILL_SUMM SUMM,
      {FacetsOwner}.CDS_INID_INVOICE CurrentInvc
WHERE
 BILL_INVC.BLBL_DUE_DT <= '{DueBeginDate}'
 AND BILL_INVC.BLBL_DUE_DT >= '2000-01-01 00:00:00.000'
 AND BILL_INVC.BLIV_ID = INVC.BLIV_ID
 AND BILL_INVC.BLEI_CK = SUMM.BLEI_CK
 AND BILL_INVC.BLBL_DUE_DT = SUMM.BLBL_DUE_DT
 AND BILL_INVC.BLEI_CK = CurrentInvc.BLEI_CK
 AND BILL_INVC.BLBL_DUE_DT = CurrentInvc.BLBL_DUE_DT
 AND INVC.BLIV_ID <> CurrentInvc.BLIV_ID
 AND INVC.BLIV_ID <> SUMM.BLBL_BLIV_ID_LAST
 AND CurrentInvc.BLIV_ID = SUMM.BLBL_BLIV_ID_LAST
 AND INVC.AFAI_CK = 0
 AND INVC.BLBL_SPCL_BL_IND In ('N','F','P','C')
 AND CurrentInvc.BLIV_CREATE_DTM >= '{CreateBeginDate}'
 AND CurrentInvc.BLIV_CREATE_DTM <= '{CreateEndDate}'
"""
df_changed_invc_current_month = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_changed_invc_current_month)
    .load()
)

df_invc_due = (
    df_due_now_invc
    .dropDuplicates(["BILL_INVC_ID", "BLBL_DUE_DT"])
    .select("BILL_INVC_ID", "BLBL_DUE_DT")
)

df_dscrtny_chngd = (
    df_changed_discrtny
    .dropDuplicates(["BILL_INVC_ID", "BLBL_DUE_DT"])
    .select("BILL_INVC_ID", "BLBL_DUE_DT")
)

df_subprem_chngd = (
    df_changed_subPrem
    .dropDuplicates(["BILL_INVC_ID", "BLBL_DUE_DT"])
    .select("BILL_INVC_ID", "BLBL_DUE_DT")
)

df_invoices_chngd = (
    df_changed_invc
    .dropDuplicates(["BILL_INVC_ID", "BLBL_DUE_DT"])
    .select("BILL_INVC_ID", "BLBL_DUE_DT")
)

df_subprem_due = (
    df_due_now_subPrem
    .dropDuplicates(["BILL_INVC_ID", "BLBL_DUE_DT"])
    .select("BILL_INVC_ID", "BLBL_DUE_DT")
)

df_rebilled_invc = (
    df_no_longer_last_bliv_id
    .dropDuplicates(["BILL_INVC_ID", "BLBL_DUE_DT"])
    .select("BILL_INVC_ID", "BLBL_DUE_DT")
)

df_fee_changed = (
    df_changed_fee
    .withColumnRenamed("BLIV_ID", "BILL_INVC_ID")
    .dropDuplicates(["BILL_INVC_ID", "BLBL_DUE_DT"])
    .select("BILL_INVC_ID", "BLBL_DUE_DT")
)

df_discrtny_due = (
    df_due_now_discrtny
    .withColumnRenamed("BLIV_ID", "BILL_INVC_ID")
    .dropDuplicates(["BILL_INVC_ID", "BLBL_DUE_DT"])
    .select("BILL_INVC_ID", "BLBL_DUE_DT")
)

df_fee_due = (
    df_due_now_fee
    .withColumnRenamed("BLIV_ID", "BILL_INVC_ID")
    .dropDuplicates(["BILL_INVC_ID", "BLBL_DUE_DT"])
    .select("BILL_INVC_ID", "BLBL_DUE_DT")
)

df_invc_cmpnt_ct = (
    df_cmpnt_ct_invc
    .withColumnRenamed("BLIV_ID", "BILL_INVC_ID")
    .dropDuplicates(["BILL_INVC_ID", "BLBL_DUE_DT"])
    .select("BILL_INVC_ID", "BLBL_DUE_DT")
)

df_invc_future = (
    df_future_invc
    .withColumnRenamed("BLIV_ID", "BILL_INVC_ID")
    .dropDuplicates(["BILL_INVC_ID", "BLBL_DUE_DT"])
    .select("BILL_INVC_ID", "BLBL_DUE_DT")
)

df_subsidy_subprem_chng = (
    df_changed_subsidiary_subPrem
    .dropDuplicates(["BILL_INVC_ID", "BLBL_DUE_DT"])
    .select("BILL_INVC_ID", "BLBL_DUE_DT")
)

df_CurrMon_Changed_Invc = (
    df_changed_invc_current_month
    .withColumnRenamed("BLIV_ID", "BILL_INVC_ID")
    .dropDuplicates(["BILL_INVC_ID", "BLBL_DUE_DT"])
    .select("BILL_INVC_ID", "BLBL_DUE_DT")
)

df_merged_invc = (
    df_invoices_chngd
    .unionByName(df_dscrtny_chngd, allowMissingColumns=True)
    .unionByName(df_subprem_chngd, allowMissingColumns=True)
    .unionByName(df_invc_due, allowMissingColumns=True)
    .unionByName(df_subprem_due, allowMissingColumns=True)
    .unionByName(df_rebilled_invc, allowMissingColumns=True)
    .unionByName(df_fee_changed, allowMissingColumns=True)
    .unionByName(df_discrtny_due, allowMissingColumns=True)
    .unionByName(df_fee_due, allowMissingColumns=True)
    .unionByName(df_invc_cmpnt_ct, allowMissingColumns=True)
    .unionByName(df_invc_future, allowMissingColumns=True)
    .unionByName(df_subsidy_subprem_chng, allowMissingColumns=True)
    .unionByName(df_CurrMon_Changed_Invc, allowMissingColumns=True)
)

df_no_blank_dates = (
    df_merged_invc
    .filter(col("BLBL_DUE_DT") >= '2000-01-01 00:00:00.000')
    .select("BILL_INVC_ID")
)

df_invoices = df_no_blank_dates.dropDuplicates(["BILL_INVC_ID"]).select("BILL_INVC_ID")

df_invoices = df_invoices.withColumn("BILL_INVC_ID", rpad(col("BILL_INVC_ID"), <...>, " "))

execute_dml(f"TRUNCATE TABLE tempdb..{DriverTable}", jdbc_url_facets, jdbc_props_facets)

df_invoices.write.format("jdbc") \
    .option("url", jdbc_url_facets) \
    .options(**jdbc_props_facets) \
    .option("dbtable", f"tempdb..{DriverTable}") \
    .mode("append") \
    .save()