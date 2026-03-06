# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 02/20/09 11:05:46 Batch  15027_39950 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_3 02/20/09 10:48:45 Batch  15027_38927 INIT bckcett testIDS dsadm bls for sa
# MAGIC ^1_2 02/19/09 15:48:36 Batch  15026_56922 PROMOTE bckcett testIDS u03651 steph for Sharon
# MAGIC ^1_2 02/19/09 15:43:11 Batch  15026_56617 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 10/31/07 14:11:20 Batch  14549_51083 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_2 10/31/07 12:55:15 Batch  14549_46521 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_2 10/31/07 12:32:28 Batch  14549_45152 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/26/07 08:07:18 Batch  14544_29241 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/26/07 07:46:17 Batch  14544_27982 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_6 05/12/06 15:03:19 Batch  14012_54207 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 05/11/06 16:20:33 Batch  14011_58841 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 05/11/06 12:36:55 Batch  14011_45419 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 05/11/06 12:31:29 Batch  14011_45092 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 05/02/06 13:04:24 Batch  14002_47066 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_2 01/26/06 07:54:36 Batch  13906_28483 INIT bckcetl ids20 dsadm Gina
# MAGIC ^1_1 01/24/06 14:49:28 Batch  13904_53375 PROMOTE bckcetl ids20 dsadm Gina Parr
# MAGIC ^1_1 01/24/06 14:47:17 Batch  13904_53242 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_2 01/24/06 14:36:07 Batch  13904_52574 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_1 01/24/06 14:21:30 Batch  13904_51695 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_2 12/23/05 11:39:15 Batch  13872_41979 PROMOTE bckcett testIDS30 u10913 Move Income Commission to test
# MAGIC ^1_2 12/23/05 11:12:01 Batch  13872_40337 INIT bckcett devlIDS30 u10913 Ollie Move Income/Commisions to test
# MAGIC ^1_1 12/23/05 10:39:46 Batch  13872_38400 INIT bckcett devlIDS30 u10913 Ollie Moving Income / Commission to Test
# MAGIC 
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsComsnCalcExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from COC_CALC_COMM to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	CMC_COCC_CALC_COMM
# MAGIC   
# MAGIC 
# MAGIC HASH FILES:  hf_cmsn_calc
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  STRIP.FIELD
# MAGIC                             FORMAT.DATE
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Output file is created with a temp. name.  File renamed in job control
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:    Sequential file 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2005-10-19      Suzanne Saylor         Original Programming.
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              5/30/2007          3264                              Added Balancing process to the overall                 devlIDS30               Steph Goddard             9/17/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data                        
# MAGIC 
# MAGIC Bhoomi Dasari                 09/09/2008       3567                             Added new primay key contianer and SrcSysCdsk  devlIDS                     Steph Goddard             09/22/2008
# MAGIC                                                                                                       and SrcSysCd  
# MAGIC Prabhu ES                       2022-03-01        S2S Remediation         MSSQL connection parameters added                     IntegrateDev5

# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC 
# MAGIC Use STRIP.FIELD function on each character column coming in
# MAGIC Extract Facets Member Eligibility Data
# MAGIC Hash file hf_comsn_calc_allcol cleared
# MAGIC Apply business logic
# MAGIC Writing Sequential File to ../key
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


facets_secret_name = get_widget_value('facets_secret_name','')
tmpOutFile = get_widget_value('TmpOutFile','')
currRunCycle = get_widget_value('CurrRunCycle','')
runID = get_widget_value('RunID','')
currentDate = get_widget_value('CurrDate','')
srcSysCdSk = get_widget_value('SrcSysCdSk','')
srcSysCd = get_widget_value('SrcSysCd','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = "SELECT COBL_CK,LOBD_ID,COST_CK,COCC_BASIS,COCC_PER_BILL_CTR,COCC_SOURCE_AMT,COCC_OVRD_AMT,COCC_COMM_AMT_CALC,COCC_ADV_AMT_CALC,COCC_CREATE_DTM,COCC_FNCO_PCS_IND FROM #$FacetsOwner#.CMC_COCC_CALC_COMM CALC WHERE CALC.COCC_CREATE_DTM  >= dateadd(dd,-7,GetDate())"
df_CMC_COCC_CALC_COMM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Strip = df_CMC_COCC_CALC_COMM.select(
    col("COBL_CK").alias("COBL_CK"),
    trim(col("LOBD_ID")).alias("LOBD_ID"),
    col("COST_CK").alias("COST_CK"),
    col("COCC_BASIS").alias("COCC_BASIS"),
    col("COCC_PER_BILL_CTR").alias("COCC_PER_BILL_CTR"),
    col("COCC_SOURCE_AMT").alias("COCC_SOURCE_AMT"),
    col("COCC_OVRD_AMT").alias("COCC_OVRD_AMT"),
    col("COCC_COMM_AMT_CALC").alias("COCC_COMM_AMT_CALC"),
    col("COCC_ADV_AMT_CALC").alias("COCC_ADV_AMT_CALC"),
    col("COCC_CREATE_DTM").alias("COCC_CREATE_DTM"),
    trim(col("COCC_FNCO_PCS_IND")).alias("COCC_FNCO_PCS_IND")
).withColumn("LOBD_ID", rpad(col("LOBD_ID"), 4, " ")).withColumn("COCC_FNCO_PCS_IND", rpad(col("COCC_FNCO_PCS_IND"), 1, " "))

df_BusinessRules_AllCol = df_Strip.select(
    lit(srcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("COBL_CK").alias("COMSN_BILL_REL_UNIQ_KEY"),
    col("LOBD_ID").alias("COMSN_CALC_LOB_CD_SK"),
    col("COST_CK").alias("COMSN_SCHD_TIER_UNIQ_KEY"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    lit(currentDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit("FACETS").alias("SRC_SYS_CD"),
    (lit("FACETS") + lit(";") + col("COBL_CK") + lit(";") + col("LOBD_ID") + lit(";") + col("COST_CK")).alias("PRI_KEY_STRING"),
    col("COCC_FNCO_PCS_IND").alias("FNCL_COMSN_RPTNG_PRCS_IN"),
    col("COCC_COMM_AMT_CALC").alias("CALC_COMSN_AMT"),
    col("COCC_ADV_AMT_CALC").alias("CALC_COMSN_ADV_AMT"),
    col("COCC_BASIS").alias("COMSN_BSS_AMT"),
    col("COCC_OVRD_AMT").alias("OVRD_AMT"),
    col("COCC_SOURCE_AMT").alias("INCM_AMT"),
    col("COCC_PER_BILL_CTR").alias("BILL_CT")
).withColumn(
    "CRT_DT_SK", FORMAT_DATE(col("COCC_CREATE_DTM"), 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD')
).select(
    "SRC_SYS_CD_SK",
    "COMSN_BILL_REL_UNIQ_KEY",
    "COMSN_CALC_LOB_CD_SK",
    "COMSN_SCHD_TIER_UNIQ_KEY",
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "FNCL_COMSN_RPTNG_PRCS_IN",
    "CRT_DT_SK",
    "CALC_COMSN_AMT",
    "CALC_COMSN_ADV_AMT",
    "COMSN_BSS_AMT",
    "OVRD_AMT",
    "INCM_AMT",
    "BILL_CT"
).withColumn("COMSN_CALC_LOB_CD_SK", rpad(col("COMSN_CALC_LOB_CD_SK"), 4, " ")) \
 .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " ")) \
 .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " ")) \
 .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " ")) \
 .withColumn("FNCL_COMSN_RPTNG_PRCS_IN", rpad(col("FNCL_COMSN_RPTNG_PRCS_IN"), 1, " ")) \
 .withColumn("CRT_DT_SK", rpad(col("CRT_DT_SK"), 10, " "))

df_BusinessRules_Transform = df_Strip.select(
    lit(srcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("COBL_CK").alias("COMSN_BILL_REL_UNIQ_KEY"),
    col("LOBD_ID").alias("COMSN_CALC_LOB_CD"),
    col("COST_CK").alias("COMSN_SCHD_TIER_UNIQ_KEY")
).select(
    "SRC_SYS_CD_SK",
    "COMSN_BILL_REL_UNIQ_KEY",
    "COMSN_CALC_LOB_CD",
    "COMSN_SCHD_TIER_UNIQ_KEY"
)

# MAGIC %run ../../../../shared_containers/PrimaryKey/ComsnCalcPK
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
params = {
    "CurrRunCycle": currRunCycle,
    "SrcSysCd": srcSysCd,
    "RunID": runID,
    "CurrentDate": currentDate,
    "IDSOwner": IDSOwner
}

df_ComsnCalcPK_Output = ComsnCalcPK(df_BusinessRules_AllCol, df_BusinessRules_Transform, params)

df_IdsComsnCalcExtr = df_ComsnCalcPK_Output.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "FIRST_RECYC_DT",
    "PASS_THRU_IN",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "COMSN_CALC_SK",
    "COMSN_BILL_REL_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "LOBD_ID",
    "COMSN_SCHD_TIER_UNIQ_KEY",
    "FNCL_COMSN_RPTNG_PRCS_IN",
    "CRT_DT_SK",
    "CALC_COMSN_AMT",
    "CALC_COMSN_ADV_AMT",
    "COMSN_BSS_AMT",
    "OVRD_AMT",
    "INCM_AMT",
    "BILL_CT"
).withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " ")) \
 .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " ")) \
 .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " ")) \
 .withColumn("LOBD_ID", rpad(col("LOBD_ID"), 4, " ")) \
 .withColumn("FNCL_COMSN_RPTNG_PRCS_IN", rpad(col("FNCL_COMSN_RPTNG_PRCS_IN"), 1, " ")) \
 .withColumn("CRT_DT_SK", rpad(col("CRT_DT_SK"), 10, " "))

write_files(
    df_IdsComsnCalcExtr,
    f"{adls_path}/key/{tmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

extract_query_2 = "SELECT COBL_CK,LOBD_ID,COST_CK FROM #$FacetsOwner#.CMC_COCC_CALC_COMM CALC WHERE CALC.COCC_CREATE_DTM  >= dateadd(dd,-7,GetDate())"
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

df_Transform_Output = df_Facets_Source.withColumn(
    "COMSN_CALC_LOB_CD_SK",
    GetFkeyCodes("FACETS", 100, "CLAIM LINE LOB", trim(col("LOBD_ID")), "X")
).select(
    lit(srcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("COBL_CK").alias("COMSN_BILL_REL_UNIQ_KEY"),
    col("COMSN_CALC_LOB_CD_SK"),
    col("COST_CK").alias("COMSN_SCHD_TIER_UNIQ_KEY")
).select(
    "SRC_SYS_CD_SK",
    "COMSN_BILL_REL_UNIQ_KEY",
    "COMSN_CALC_LOB_CD_SK",
    "COMSN_SCHD_TIER_UNIQ_KEY"
)

write_files(
    df_Transform_Output,
    f"{adls_path}/load/B_COMSN_CALC.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)