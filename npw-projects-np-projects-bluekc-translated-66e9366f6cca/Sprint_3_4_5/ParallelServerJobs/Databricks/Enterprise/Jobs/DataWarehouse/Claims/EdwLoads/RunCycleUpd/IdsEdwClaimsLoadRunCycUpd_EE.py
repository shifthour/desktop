# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2006 - 2023 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name:  IdsEdwClaimsLoadRuncycUpd_EE
# MAGIC Called by: IdsClaimCntl
# MAGIC 
# MAGIC DESCRIPTION:    Update the P_RUN_CYC table EDW load indicator fields to show those records have been copied to the EDW.
# MAGIC 
# MAGIC PROCESSING:  Each IDS Claim source has a seperate SQL to extract the maximum run cycle value.  These run cycle values are used to update the IDS P_RUN_CYCLE, load indicator field for the particular source and subject.  All run cycles less than the maximum where the indicator is "N" are updated to "Y".
# MAGIC                  
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date		Developer		Project/Altiris #		Change Description							Development Project	Code Reviewer	Date Reviewed       
# MAGIC ===========================================================================================================================================================================================================      
# MAGIC 2006-03-21         	Brent Leland                                                   		Original Programming.
# MAGIC 2006-07-26         	Steph Goddard                                                		Changed OT@2, ADOL, EDC to PSEUDOCLAIM source
# MAGIC 2008-12-09         	Brent Leland                                                   		Added ESI
# MAGIC 2009-03-30         	Sharon Andrew           	Labor Accnts               	Added get run cycle for WELLDYNERX.  Removed Argus run cycle lookup      	devlEDW                		Steph Goddard   	03/31/2009
# MAGIC 2010-01-05         	Kalyan Neelam          	4110                              	Added MEDICAID, MCSOURCE                                                                         	EnterpriseCurDevl
# MAGIC 2010-03-09         	Judy Reynolds           	4250 - Alineo                 	Modified to correct Pseudo Claim extract to include all pseudo claim types   	EnterpriseCurDevl          	Steph Goddard  	03/11/2010
# MAGIC 2010-09-27         	Kalyan Neelam          	TTR-729/4297             	Added CAREMARK. Added 'PCT' in the extract SQL for Pseudo_Claim        	EnterpriseNewDevl        	SAndrew         	2010-09-28
# MAGIC 2010-12-20         	Kalyan Neelam          	4616 - Medtrak             	Added MEDTRAK source                                                                              	EnterpriseNewDevl         	Steph Goddard    	12/28/2010
# MAGIC 2012-06-28         	Karthik Chintpni         	4784 - BCBSSC            	Added BCBSSC source 
# MAGIC 2013-11-15        	Pooja Sunkara             	5114                           	Rewrite in Parallel                                                                                        	EnterpriseWrhsDevl         	Peter Marshall   	12/24/2013 
# MAGIC 2014-06-24         	Bhoomi Dasari            	5345                            	Added BCA process line                                                                                	EnterpriseNewDevl           	Kalyan Neelam     	2014-06-24
# MAGIC 2015-01-20    	Karthik Chintalapani     	5212                            	Added BCBSA source                                                                                 	EnterpriseCurDevl            	Bhoomi Dasari   	02/04/2015
# MAGIC 2015-03-24        	Raja Gummadi            	5125                            	Added Examone process line                                                                         	EnterpriseNewDevl            	Kalyan Neelam   	2015-03-25
# MAGIC 2015-10-09        	Raja Gummadi            	5382                         	Added ESI CLM LN SHADOW runcycle                                                           	EnterpriseDev2                	Bhoomi Dasari    	10/27/2015
# MAGIC 2018-03-05        	Jaideep Mankala        	5828                        	Added SAVRX and LDI source                                                                         	EnterpriseDev2                	Kalyan Neelam    	2018-03-07
# MAGIC 2018-04-02        	Sethuraman R            	5744                        	Added EyeMed source                                                                                       	EnterpriseDev2
# MAGIC 2018-09-23        	Kaushik Kapoor          	5828                 		Added CVS Source                                                                                  	EnterpriseDev2        	Kalyan Neelam  	2018-10-01
# MAGIC 2019-10-08        	Sagar  sayam              	6131- PBM Replacement	Added OPTUMRX value in optumrx db stage                                         		EnterpriseDev5         	Kalyan Neelam    	2019-11-22
# MAGIC 2020-09-11        	Reddy Sanam            	US281072              		Added db2  source stage for Lumeris source                                                                                     		Kalyan Neelam  	2020-09-15
# MAGIC 2020-11-16        	SravyaSree Yarlagadda 	RA                      		Added Db2 stage for 13 sources                                                  		EnterpriseDev2 		Harsha Ravuri	2020-11-17
# MAGIC 2020-11-23        	Jeyaprasanna                  	RA                      		Added Db2 stage for 2 sources (DENTAQUEST,ASH)                          		EnterpriseDev2 		Jaideep Mankala 	11/23/2020
# MAGIC 2021-03-22        	Vikas Abbu          		RA                        		Added Db2 stage for 2 sources (LVNGHLTH) and SOLUTRAN     		EnterpriseDev2 		Kalyan Neelam	2021-03-22
# MAGIC 2021-03-24        	Vikas Abbu          		RA                        		Added Db2 stage for 6 sources BARRYPOINTE / LEAWOODFMLYCARE
# MAGIC                                                                                                			MOSAICLIFECARE / PRIMEMO / SPIRA / TRUMAN                      		EnterpriseDev2 	 	Kalyan Neelam 	2021-03-25
# MAGIC 2021-12-05     	Lokesh                        	US 459610                   	Added NationsBenefits Claims sources to Variables Stage             		EnterpriseDev2      		Jeyaprasanna  	2022-01-10
# MAGIC 2022-01-12      	Venkata Y          		480876                    	Added Db2 stage for 3 sourcesHCA,OLATHEMED,NONSTANDARDSUPP	EnterpriseDev2
# MAGIC 2023-08-08	Ken Bradmon		us559895			Added 8 new Source IDs to the DB stage "db2_EMRProcedureClm_In"	EnterpriseDev2	                Reddy Sanam          2023-10-27
# MAGIC 2023-12-22              Saranya                                   US 599810                              Added DB2 stage for MARC, Dominion                                                                   EnterpriseDev1                        Jeyaprasanna          2023-12-26
# MAGIC 2024-03-21	Arpitha V  	                US 614486		Updated query in db2_NATIONS_Clm_In stage to include NATIONBFTSFLEX	EnterpriseDev2                        Jeyaprasanna          2024-03-28
# MAGIC 2023-12-22              Harsha Ravuri		US#			Added new Source 'ASCENTIST' to the DB stage "db2_EMRProcedureClm_In"	EnterpriseDev2                        Jeyaprasanna          2025-05-20

# MAGIC CLAYPLATTE,ENCOMPASS,
# MAGIC HEDIS,JAYHAWK,KCINTRNLMEDS,
# MAGIC LIBERTYHOSP,MERITAS,NORTHLAND,
# MAGIC OLATHEMED,PROVIDENCE,PROVSTLUKES,SUNFLOWER,
# MAGIC UNTDMEDGRP
# MAGIC JobName: IdsEdwClaimsLoadRunCycUpd
# MAGIC Update the P_RUN_CYC table to indicate source system data has been loaded to the EDW for each of the run cycles processed.
# MAGIC Run Cycle Extract Update
# MAGIC Get list of source systems and the maximum run cycles for the source system from the driver table.
# MAGIC CLM_LN_SHADOW_ADJDCT
# MAGIC DENTAQUEST, ASH
# MAGIC SOLUTRAN
# MAGIC LVNGHLTH
# MAGIC BARRYPOINTE
# MAGIC LEAWOODFMLYCARE
# MAGIC MOSAICLIFECARE 
# MAGIC PRIMEMO 
# MAGIC SPIRA 
# MAGIC TRUMAN
# MAGIC CENTRUSHEALTH
# MAGIC MOHEALTH
# MAGIC GOLDENVALLEY
# MAGIC JEFFERSON
# MAGIC WESTMOMEDCNTR
# MAGIC BLUESPRINGS
# MAGIC EXCELSIOR
# MAGIC HARRISONVILLE
# MAGIC ASCENTIST
# MAGIC HCA
# MAGIC CHILDRENMERCY
# MAGIC NONSTDSUPLMTDATA
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameter retrieval
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
JPCurrentPBM = get_widget_value('JPCurrentPBM','')
JPPreviousPBM = get_widget_value('JPPreviousPBM','')

# Database config
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# db2_Facets_In
extract_query_db2_Facets_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'FACETS'
GROUP BY SRC_SYS_CD
"""
df_db2_Facets_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Facets_In)
    .load()
)

# db2_ESI_Clm_In
extract_query_db2_ESI_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'ESI'
GROUP BY SRC_SYS_CD
"""
df_db2_ESI_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_ESI_Clm_In)
    .load()
)

# db2_WellDyne_Clm_In
extract_query_db2_WellDyne_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'WELLDYNERX'
GROUP BY SRC_SYS_CD
"""
df_db2_WellDyne_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_WellDyne_Clm_In)
    .load()
)

# db2_Nasco_Clm_In
extract_query_db2_Nasco_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'NPS'
GROUP BY SRC_SYS_CD
"""
df_db2_Nasco_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Nasco_Clm_In)
    .load()
)

# db2_PHP_Clm_In
extract_query_db2_PHP_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'PHP'
GROUP BY SRC_SYS_CD
"""
df_db2_PHP_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PHP_Clm_In)
    .load()
)

# db2_MCSource_Clm_In
extract_query_db2_MCSource_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'MCSOURCE'
GROUP BY SRC_SYS_CD
"""
df_db2_MCSource_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_MCSource_Clm_In)
    .load()
)

# db2_PSEUDO_claim_In
extract_query_db2_PSEUDO_claim_In = f"""
SELECT 'PCT' as SRC_SYS_CD,
       max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD in ('OT@2', 'EDC', 'ADOL', 'MOHSAIC', 'CAREADVANCE','PCT')
GROUP BY SRC_SYS_CD
"""
df_db2_PSEUDO_claim_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PSEUDO_claim_In)
    .load()
)

# db2_PCS_Clm_In
extract_query_db2_PCS_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'PCS'
GROUP BY SRC_SYS_CD
"""
df_db2_PCS_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PCS_Clm_In)
    .load()
)

# db2_Medtrak_Clm_In
extract_query_db2_Medtrak_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'MEDTRAK'
GROUP BY SRC_SYS_CD
"""
df_db2_Medtrak_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Medtrak_Clm_In)
    .load()
)

# db2_BCBSSC_Clm_In
extract_query_db2_BCBSSC_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'BCBSSC'
GROUP BY SRC_SYS_CD
"""
df_db2_BCBSSC_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_BCBSSC_Clm_In)
    .load()
)

# db2_Medicaid_Clm_In
extract_query_db2_Medicaid_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'MCAID'
GROUP BY SRC_SYS_CD
"""
df_db2_Medicaid_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Medicaid_Clm_In)
    .load()
)

# db2_Caremark_Clm_In
extract_query_db2_Caremark_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'CAREMARK'
GROUP BY SRC_SYS_CD
"""
df_db2_Caremark_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Caremark_Clm_In)
    .load()
)

# db2_BCA_Clm_In
extract_query_db2_BCA_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'BCA'
GROUP BY SRC_SYS_CD
"""
df_db2_BCA_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_BCA_Clm_In)
    .load()
)

# db2_BCBSA_Clm_In
extract_query_db2_BCBSA_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'BCBSA'
GROUP BY SRC_SYS_CD
"""
df_db2_BCBSA_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_BCBSA_Clm_In)
    .load()
)

# db2_Examone_Clm_In
extract_query_db2_Examone_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'EXAMONE'
GROUP BY SRC_SYS_CD
"""
df_db2_Examone_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Examone_Clm_In)
    .load()
)

# db2_SAVRX_Clm_In
extract_query_db2_SAVRX_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'SAVRX'
GROUP BY SRC_SYS_CD
"""
df_db2_SAVRX_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_SAVRX_Clm_In)
    .load()
)

# db2_LDI_Clm_In
extract_query_db2_LDI_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'LDI'
GROUP BY SRC_SYS_CD
"""
df_db2_LDI_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_LDI_Clm_In)
    .load()
)

# db2_EyeMed_Clm_In
extract_query_db2_EyeMed_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'EYEMED'
GROUP BY SRC_SYS_CD
"""
df_db2_EyeMed_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_EyeMed_Clm_In)
    .load()
)

# db2_CVS_Clm_In
extract_query_db2_CVS_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'CVS'
GROUP BY SRC_SYS_CD
"""
df_db2_CVS_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CVS_Clm_In)
    .load()
)

# db2_OPTUMRX_Clm_In
extract_query_db2_OPTUMRX_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD ='OPTUMRX'
GROUP BY SRC_SYS_CD
"""
df_db2_OPTUMRX_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_OPTUMRX_Clm_In)
    .load()
)

# db2_Lumeris_In
extract_query_db2_Lumeris_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'LUMERIS'
GROUP BY SRC_SYS_CD
"""
df_db2_Lumeris_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Lumeris_In)
    .load()
)

# db2_MedImpact_In
extract_query_db2_MedImpact_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'MEDIMPACT'
GROUP BY SRC_SYS_CD
"""
df_db2_MedImpact_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_MedImpact_In)
    .load()
)

# db2_LHOHistProc_Clm_In
extract_query_db2_LHOHistProc_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD IN ('CLAYPLATTE'
,'ENCOMPASS'
,'HEDIS'
,'JAYHAWK'
,'KCINTRNLMEDS'
,'LIBERTYHOSP'
,'MERITAS'
,'NORTHLAND'
,'OLATHEMED'
,'PROVIDENCE'
,'PROVSTLUKES'
,'SUNFLOWER'
,'UNTDMEDGRP')
GROUP BY SRC_SYS_CD
"""
df_db2_LHOHistProc_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_LHOHistProc_Clm_In)
    .load()
)

# db2_LHOHistEnc_Clm_In
extract_query_db2_LHOHistEnc_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD IN ('DENTAQUEST'
,'ASH')
GROUP BY SRC_SYS_CD
"""
df_db2_LHOHistEnc_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_LHOHistEnc_Clm_In)
    .load()
)

# db2_LivongoEncClm_In
extract_query_db2_LivongoEncClm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'LVNGHLTH'
GROUP BY SRC_SYS_CD
"""
df_db2_LivongoEncClm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_LivongoEncClm_In)
    .load()
)

# db2_SolutranClm_In
extract_query_db2_SolutranClm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD = 'SOLUTRAN'
GROUP BY SRC_SYS_CD
"""
df_db2_SolutranClm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_SolutranClm_In)
    .load()
)

# db2_EMRProcedureClm_In
extract_query_db2_EMRProcedureClm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD IN ('BARRYPOINTE',
'LEAWOODFMLYCARE',
'MOSAICLIFECARE',
'PRIMEMO',
'SPIRA',
'TRUMAN',
'CENTRUSHEALTH',
'MOHEALTH',
'GOLDENVALLEY',
'JEFFERSON',
'WESTMOMEDCNTR',
'BLUESPRINGS',
'EXCELSIOR',
'HARRISONVILLE',
'ASCENTIST')
GROUP BY SRC_SYS_CD
"""
df_db2_EMRProcedureClm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_EMRProcedureClm_In)
    .load()
)

# db2_EMRPROC_Clm_In
extract_query_db2_EMRPROC_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD IN ('CHILDRENSMERCY','NONSTDSUPLMTDATA','HCA')
GROUP BY SRC_SYS_CD
"""
df_db2_EMRPROC_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_EMRPROC_Clm_In)
    .load()
)

# db2_NATIONS_Clm_In
extract_query_db2_NATIONS_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD IN ('NATIONBFTSBBB','NATIONBFTSOTC','NATIONBFTSRWD','NATIONBFTSHRNG','NATIONBFTSFLEX')
GROUP BY SRC_SYS_CD
"""
df_db2_NATIONS_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_NATIONS_Clm_In)
    .load()
)

# db2_MARC_Clm_In
extract_query_db2_MARC_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD IN ('MARC')
GROUP BY SRC_SYS_CD
"""
df_db2_MARC_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_MARC_Clm_In)
    .load()
)

# db2_DOMINION_Clm_In
extract_query_db2_DOMINION_Clm_In = f"""
SELECT  SRC_SYS_CD,
        max(LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_EDW_ETL_DRVR
WHERE SRC_SYS_CD IN ('DOMINION')
GROUP BY SRC_SYS_CD
"""
df_db2_DOMINION_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_DOMINION_Clm_In)
    .load()
)

# Funnel (PxFunnel) -> fnl_Data (union of all)
df_db2_Facets_In_sub = df_db2_Facets_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_ESI_Clm_In_sub = df_db2_ESI_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_WellDyne_Clm_In_sub = df_db2_WellDyne_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_Nasco_Clm_In_sub = df_db2_Nasco_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_PSEUDO_claim_In_sub = df_db2_PSEUDO_claim_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_PHP_Clm_In_sub = df_db2_PHP_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_MCSource_Clm_In_sub = df_db2_MCSource_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_PCS_Clm_In_sub = df_db2_PCS_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_Medtrak_Clm_In_sub = df_db2_Medtrak_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_Medicaid_Clm_In_sub = df_db2_Medicaid_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_Caremark_Clm_In_sub = df_db2_Caremark_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_BCBSSC_Clm_In_sub = df_db2_BCBSSC_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_BCA_Clm_In_sub = df_db2_BCA_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_BCBSA_Clm_In_sub = df_db2_BCBSA_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_Examone_Clm_In_sub = df_db2_Examone_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_SAVRX_Clm_In_sub = df_db2_SAVRX_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_LDI_Clm_In_sub = df_db2_LDI_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_EyeMed_Clm_In_sub = df_db2_EyeMed_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_CVS_Clm_In_sub = df_db2_CVS_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_OPTUMRX_Clm_In_sub = df_db2_OPTUMRX_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_Lumeris_In_sub = df_db2_Lumeris_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_MedImpact_In_sub = df_db2_MedImpact_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_LHOHistProc_Clm_In_sub = df_db2_LHOHistProc_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_LHOHistEnc_Clm_In_sub = df_db2_LHOHistEnc_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_LivongoEncClm_In_sub = df_db2_LivongoEncClm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_SolutranClm_In_sub = df_db2_SolutranClm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_EMRProcedureClm_In_sub = df_db2_EMRProcedureClm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_EMRPROC_Clm_In_sub = df_db2_EMRPROC_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_NATIONS_Clm_In_sub = df_db2_NATIONS_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_MARC_Clm_In_sub = df_db2_MARC_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
df_db2_DOMINION_Clm_In_sub = df_db2_DOMINION_Clm_In.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")

df_fnl_Data = df_db2_Facets_In_sub.unionByName(df_db2_ESI_Clm_In_sub) \
    .unionByName(df_db2_WellDyne_Clm_In_sub) \
    .unionByName(df_db2_Nasco_Clm_In_sub) \
    .unionByName(df_db2_PSEUDO_claim_In_sub) \
    .unionByName(df_db2_PHP_Clm_In_sub) \
    .unionByName(df_db2_MCSource_Clm_In_sub) \
    .unionByName(df_db2_PCS_Clm_In_sub) \
    .unionByName(df_db2_Medtrak_Clm_In_sub) \
    .unionByName(df_db2_Medicaid_Clm_In_sub) \
    .unionByName(df_db2_Caremark_Clm_In_sub) \
    .unionByName(df_db2_BCBSSC_Clm_In_sub) \
    .unionByName(df_db2_BCA_Clm_In_sub) \
    .unionByName(df_db2_BCBSA_Clm_In_sub) \
    .unionByName(df_db2_Examone_Clm_In_sub) \
    .unionByName(df_db2_SAVRX_Clm_In_sub) \
    .unionByName(df_db2_LDI_Clm_In_sub) \
    .unionByName(df_db2_EyeMed_Clm_In_sub) \
    .unionByName(df_db2_CVS_Clm_In_sub) \
    .unionByName(df_db2_OPTUMRX_Clm_In_sub) \
    .unionByName(df_db2_Lumeris_In_sub) \
    .unionByName(df_db2_MedImpact_In_sub) \
    .unionByName(df_db2_LHOHistProc_Clm_In_sub) \
    .unionByName(df_db2_LHOHistEnc_Clm_In_sub) \
    .unionByName(df_db2_LivongoEncClm_In_sub) \
    .unionByName(df_db2_SolutranClm_In_sub) \
    .unionByName(df_db2_EMRProcedureClm_In_sub) \
    .unionByName(df_db2_EMRPROC_Clm_In_sub) \
    .unionByName(df_db2_NATIONS_Clm_In_sub) \
    .unionByName(df_db2_MARC_Clm_In_sub) \
    .unionByName(df_db2_DOMINION_Clm_In_sub)

# xfm_BusinessLogic (CTransformerStage)
df_enriched_xfm_BusinessLogic = df_fnl_Data.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    lit("CLAIM").alias("SUBJ_CD"),
    lit("IDS").alias("TRGT_SYS_CD"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("RUN_CYC_NO"),
    rpad(lit("Y"), 1, " ").alias("EDW_LOAD_IN")
)

# db2_P_RUN_CYC_Out (DB2ConnectorPX) -> Merge logic
# 1) Write to staging table: STAGING.IdsEdwClaimsLoadRunCycUpd_EE_db2_P_RUN_CYC_Out_temp
temp_table_1 = "STAGING.IdsEdwClaimsLoadRunCycUpd_EE_db2_P_RUN_CYC_Out_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_1}", jdbc_url, jdbc_props)
df_enriched_xfm_BusinessLogic.write.jdbc(
    url=jdbc_url,
    table=temp_table_1,
    mode="overwrite",
    properties=jdbc_props
)

merge_sql_1 = f"""
MERGE INTO {IDSOwner}.P_RUN_CYC AS T
USING {temp_table_1} AS S
ON T.SRC_SYS_CD=S.SRC_SYS_CD
   AND T.SUBJ_CD=S.SUBJ_CD
   AND T.TRGT_SYS_CD=S.TRGT_SYS_CD
   AND T.RUN_CYC_NO <= S.RUN_CYC_NO
   AND T.EDW_LOAD_IN='N'
WHEN MATCHED THEN
  UPDATE SET T.EDW_LOAD_IN='Y';
"""
execute_dml(merge_sql_1, jdbc_url, jdbc_props)

# db2_ESI_ShdwClm_In
extract_query_db2_ESI_ShdwClm_In = f"""
SELECT  CD.SRC_CD as SRC_SYS_CD,
        max(CLM.LAST_UPDT_RUN_CYC_EXCTN_SK) as LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.CLM_LN_SHADOW_ADJDCT CLM, {IDSOwner}.CD_MPPNG CD
WHERE CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
  AND CD.SRC_CD in ('{JPCurrentPBM}', '{JPPreviousPBM}')
GROUP BY CD.SRC_CD
"""
df_db2_ESI_ShdwClm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_ESI_ShdwClm_In)
    .load()
)

# xfm (CTransformerStage)
df_enriched_xfm = df_db2_ESI_ShdwClm_In.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    lit("CLAIM_SHADOW").alias("SUBJ_CD"),
    lit("IDS").alias("TRGT_SYS_CD"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("RUN_CYC_NO"),
    rpad(lit("Y"), 1, " ").alias("EDW_LOAD_IN")
)

# db2_P_RUN_CYC (DB2ConnectorPX) -> Merge logic
# 1) Write to staging table: STAGING.IdsEdwClaimsLoadRunCycUpd_EE_db2_P_RUN_CYC_temp
temp_table_2 = "STAGING.IdsEdwClaimsLoadRunCycUpd_EE_db2_P_RUN_CYC_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_2}", jdbc_url, jdbc_props)
df_enriched_xfm.write.jdbc(
    url=jdbc_url,
    table=temp_table_2,
    mode="overwrite",
    properties=jdbc_props
)

merge_sql_2 = f"""
MERGE INTO {IDSOwner}.P_RUN_CYC AS T
USING {temp_table_2} AS S
ON T.SRC_SYS_CD=S.SRC_SYS_CD
   AND T.SUBJ_CD=S.SUBJ_CD
   AND T.TRGT_SYS_CD=S.TRGT_SYS_CD
   AND T.RUN_CYC_NO <= S.RUN_CYC_NO
   AND T.EDW_LOAD_IN='N'
WHEN MATCHED THEN
  UPDATE SET T.EDW_LOAD_IN='Y';
"""
execute_dml(merge_sql_2, jdbc_url, jdbc_props)