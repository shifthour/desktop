# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 11/12/08 10:10:08 Batch  14927_36617 PROMOTE bckcetl ids20 dsadm rc for brent 
# MAGIC ^1_2 11/12/08 10:04:14 Batch  14927_36261 INIT bckcett testIDS dsadm rc for brent 
# MAGIC ^1_1 11/06/08 08:57:33 Batch  14921_32258 PROMOTE bckcett testIDS u03651 steph for Brent
# MAGIC ^1_1 11/06/08 08:52:23 Batch  14921_31946 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 10/24/07 09:59:24 Batch  14542_35967 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/24/07 09:54:15 Batch  14542_35658 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_2 10/17/07 13:59:14 Batch  14535_50357 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_2 10/17/07 13:54:06 Batch  14535_50077 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 10/16/07 09:44:13 Batch  14534_35065 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_8 09/28/05 09:15:12 Batch  13786_33322 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_7 09/28/05 08:42:15 Batch  13786_31339 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_2 07/29/05 14:39:17 Batch  13725_52764 INIT bckcetl ids20 dsadm Brent
# MAGIC ^1_1 07/29/05 13:02:36 Batch  13725_46962 INIT bckcetl ids20 dsadm Brent
# MAGIC ^1_1 07/20/05 07:35:13 Batch  13716_27318 PROMOTE bckcetl ids20 dsadm Gina Parr
# MAGIC ^1_1 07/20/05 07:29:40 Batch  13716_26986 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_6 06/22/05 09:10:14 Batch  13688_33020 PROMOTE bckcett VERSION u06640 Ralph
# MAGIC ^1_6 06/22/05 09:08:48 Batch  13688_32937 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_5 06/20/05 13:59:07 Batch  13686_50351 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_4 06/17/05 10:29:55 Batch  13683_37806 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_3 06/17/05 10:28:29 Batch  13683_37715 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_2 06/15/05 08:54:19 Batch  13681_32063 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_1 06/09/05 13:18:55 Batch  13675_47941 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005, 2022  BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsCapFundExtr
# MAGIC CALLED BY:  FctsCapExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_CRFD_FUND_DEFN for loading into IDS.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:     Facets -  CMC_CRFD_FUND_DEFN
# MAGIC 
# MAGIC 
# MAGIC HASH FILES: none
# MAGIC 
# MAGIC TRANSFORMS:   STRIP.FIELD
# MAGIC                              FORMAT.DATE
# MAGIC                            
# MAGIC PROCESSING:  Output file is created with a temp. name.  File renamed in job control
# MAGIC   
# MAGIC OUTPUTS:   Sequential file
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Ralph Tucker    04/22/2005  -  Originally Programmed
# MAGIC               BJ Luce             05/25/2005 - changed to use standard facets parameters
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer 	Date   		Project/Altiris #		Change Description					Development Project		Code Reviewer	Date Reviewed       
# MAGIC =========================================================================================================================================================================================
# MAGIC Parik                	4/19/2007         	 3264                              		Added Balancing process to the overall                 		devlIDS30                		Steph Goddard       	09/19/2007
# MAGIC                                                                                                       			job that takes a snapshot of the source data                
# MAGIC 
# MAGIC Parik               	2008-09-05   	3567(Primary Key)        	Added Primary Key process to the job                     		devlIDS                       		Steph Goddard   	09/10/2008
# MAGIC Prabhu ES          	2022-02-25       	S2S Remediation   		MSSQL connection parameters added                    		IntegrateDev5		Ken Bradmon	2022-06-07

# MAGIC Remove Carriage Return, Line Feed, and  Tab in fields
# MAGIC Extract Cap Fund Data
# MAGIC Writing Sequential File to /key
# MAGIC Apply Business Logic to common record format
# MAGIC This container is used in:
# MAGIC FctsCapFundExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Hash file (hf_cap_fund_allcol) cleared in calling program
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/CapFundPK
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_cmc_crfd_fund_defn = f"SELECT CRFD_FUND_ID,CRFD_DESC,CRFD_FUND_RATE_IND,CRFD_PRORATE_RULE,CRFD_PYMT_METH_IND,CRFD_ACCT_CAT,CRFD_KEY_MOD_IND,CRFD_MIN_AMT,CRFD_MAX_AMT FROM {FacetsOwner}.CMC_CRFD_FUND_DEFN"
df_CMC_CRFD_FUND_DEFN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_cmc_crfd_fund_defn)
    .load()
)

df_TrnsStripField = df_CMC_CRFD_FUND_DEFN.select(
    Convert("\n\r\t","",F.col("CRFD_FUND_ID")).alias("CRFD_FUND_ID"),
    Convert("\n\r\t","",F.col("CRFD_DESC")).alias("CRFD_DESC"),
    Convert("\n\r\t","",F.col("CRFD_FUND_RATE_IND")).alias("CRFD_FUND_RATE_IND"),
    Convert("\n\r\t","",F.col("CRFD_PRORATE_RULE")).alias("CRFD_PRORATE_RULE"),
    Convert("\n\r\t","",F.col("CRFD_PYMT_METH_IND")).alias("CRFD_PYMT_METH_IND"),
    Convert("\n\r\t","",F.col("CRFD_ACCT_CAT")).alias("CRFD_ACCT_CAT"),
    Convert("\n\r\t","",F.col("CRFD_KEY_MOD_IND")).alias("CRFD_KEY_MOD_IND"),
    F.col("CRFD_MIN_AMT").alias("CRFD_MIN_AMT"),
    F.col("CRFD_MAX_AMT").alias("CRFD_MAX_AMT")
)

df_TrnsCommRec_AllCol = df_TrnsStripField.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.trim(F.col("CRFD_FUND_ID")).alias("CAP_FUND_ID"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.trim(F.col("CRFD_FUND_ID"))).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CAP_FUND_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("CRFD_ACCT_CAT").alias("CAP_FUND_ACCTG_CAT_CD"),
    F.col("CRFD_PYMT_METH_IND").alias("CAP_FUND_PAYMT_METH_CD"),
    F.col("CRFD_PRORATE_RULE").alias("CAP_FUND_PRORT_RULE_CD"),
    F.col("CRFD_FUND_RATE_IND").alias("CAP_FUND_RATE_CD"),
    F.when(F.length(F.trim(F.col("CRFD_KEY_MOD_IND"))) == 0, "X").otherwise(F.col("CRFD_KEY_MOD_IND")).alias("GRP_CAP_MOD_APLD_IN"),
    F.col("CRFD_MAX_AMT").alias("MAX_AMT"),
    F.col("CRFD_MIN_AMT").alias("MIN_AMT"),
    F.when(F.length(F.trim(F.col("CRFD_DESC"))) == 0, "NA").otherwise(F.trim(F.col("CRFD_DESC"))).alias("FUND_DESC")
)

df_TrnsCommRec_Transform = df_TrnsStripField.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.trim(F.col("CRFD_FUND_ID")).alias("CAP_FUND_ID")
)

params_CapFundPK = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd
}
df_IdsCapFundPkey, = CapFundPK(df_TrnsCommRec_Transform, df_TrnsCommRec_AllCol, params_CapFundPK)

df_IdsCapFundPkey_final = df_IdsCapFundPkey.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CAP_FUND_SK"),
    F.col("CAP_FUND_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("CAP_FUND_ACCTG_CAT_CD"), 4, " ").alias("CAP_FUND_ACCTG_CAT_CD"),
    F.rpad(F.col("CAP_FUND_PAYMT_METH_CD"), 1, " ").alias("CAP_FUND_PAYMT_METH_CD"),
    F.rpad(F.col("CAP_FUND_PRORT_RULE_CD"), 1, " ").alias("CAP_FUND_PRORT_RULE_CD"),
    F.rpad(F.col("CAP_FUND_RATE_CD"), 1, " ").alias("CAP_FUND_RATE_CD"),
    F.rpad(F.col("GRP_CAP_MOD_APLD_IN"), 1, " ").alias("GRP_CAP_MOD_APLD_IN"),
    F.col("MAX_AMT"),
    F.col("MIN_AMT"),
    F.rpad(F.col("FUND_DESC"), 70, " ").alias("FUND_DESC")
)

write_files(
    df_IdsCapFundPkey_final,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

extract_query_facets_source = f"SELECT CRFD_FUND_ID FROM {FacetsOwner}.CMC_CRFD_FUND_DEFN"
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_facets_source)
    .load()
)

df_Transform = df_Facets_Source.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.trim(Convert("\n\r\t","",F.col("CRFD_FUND_ID"))).alias("CAP_FUND_ID")
)

df_Snapshot_File_final = df_Transform.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CAP_FUND_ID")
)

write_files(
    df_Snapshot_File_final,
    f"{adls_path}/load/B_CAP_FUND.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)