# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_4 02/20/09 11:05:46 Batch  15027_39950 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_4 02/20/09 10:48:45 Batch  15027_38927 INIT bckcett testIDS dsadm bls for sa
# MAGIC ^1_2 02/19/09 15:48:36 Batch  15026_56922 PROMOTE bckcett testIDS u03651 steph for Sharon
# MAGIC ^1_2 02/19/09 15:43:11 Batch  15026_56617 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 01/11/08 09:22:31 Batch  14621_33760 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_3 01/11/08 09:17:53 Batch  14621_33476 INIT bckcett testIDS dsadm bls for sa
# MAGIC ^1_1 01/10/08 10:22:18 Batch  14620_37342 PROMOTE bckcett testIDS dsadm steph for Sharon
# MAGIC ^1_1 01/10/08 10:20:07 Batch  14620_37220 INIT bckcett devlIDS dsadm steffy
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
# MAGIC JOB NAME:     FctsComsnErnExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from FACETS TABLE NAME(S) to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	CMC_COEC_EARN_COMM
# MAGIC   
# MAGIC 
# MAGIC HASH FILES:  hf_cmsn_ern
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
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                                                Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                                           ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              5/30/2007          3264                          Added Balancing process to the overall job that takes a snapshot             devlIDS30                   Steph Goddard             09/17/2007
# MAGIC                                                                                                         of the source data                             
# MAGIC SAndrew                           01/03/2008     ProductnSupport          Renamed hf_cmsn_ern  to hf_comsn_ern.                                                 devlIDS                       Steph Goddard             01/10/2008
# MAGIC                                                                                                         Conversion program needed for production.                        
# MAGIC 
# MAGIC Bhoomi Dasari                 09/11/2008       3567                             Added new primay key contianer and SrcSysCdsk                                     devlIDS                      Steph Goddard             09/22/2008    
# MAGIC                                                                                                       and SrcSysCd  
# MAGIC Prabhu ES                       2022-03-01        S2S Remediation         MSSQL connection parameters added                                                        IntegrateDev5

# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC 
# MAGIC Use STRIP.FIELD function on each character column coming in
# MAGIC Extract Facets Member Eligibility Data
# MAGIC Hash file hf_comsn_ern_allcol cleared
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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
BeginDate = get_widget_value('BeginDate','')
EndDate = get_widget_value('EndDate','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')

# Read from CMC_COEC_EARN_COMM (ODBCConnector) - Stage: CMC_COEC_EARN_COMM
jdbc_url_CMC_COEC_EARN_COMM, jdbc_props_CMC_COEC_EARN_COMM = get_db_config(facets_secret_name)
extract_query_CMC_COEC_EARN_COMM = (
    f"SELECT COBL_CK,LOBD_ID,COST_CK,COEC_SEQ_NO,COEC_PAID_DT,COEC_SOURCE_AMT,COEC_OVRD_AMT,COEC_COMM_AMT_EARN,"
    f"COEC_COMM_AMT_PAID,COEC_ADV_AMT_EARN,COEC_RETN_AMT_EARN,COEC_HLD_FOR_RECON,COEC_FORGO_AMT,COEC_FNCO_PCS_IND,"
    f"COCE_ID_PAYEE FROM {FacetsOwner}.CMC_COEC_EARN_COMM EARN "
    f"WHERE COEC_PAID_DT >= '{BeginDate}' and COEC_PAID_DT <= '{EndDate}'"
)
df_CMC_COEC_EARN_COMM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CMC_COEC_EARN_COMM)
    .options(**jdbc_props_CMC_COEC_EARN_COMM)
    .option("query", extract_query_CMC_COEC_EARN_COMM)
    .load()
)

# Transformer - Stage: StripField
df_StripField = df_CMC_COEC_EARN_COMM.select(
    F.col("COBL_CK").alias("COBL_CK"),
    trim(F.col("LOBD_ID")).alias("LOBD_ID"),
    F.col("COST_CK").alias("COST_CK"),
    F.col("COEC_SEQ_NO").alias("COEC_SEQ_NO"),
    F.col("COEC_PAID_DT").alias("COEC_PAID_DT"),
    F.col("COEC_SOURCE_AMT").alias("COEC_SOURCE_AMT"),
    F.col("COEC_OVRD_AMT").alias("COEC_OVRD_AMT"),
    F.col("COEC_COMM_AMT_EARN").alias("COEC_COMM_AMT_EARN"),
    F.col("COEC_COMM_AMT_PAID").alias("COEC_COMM_AMT_PAID"),
    F.col("COEC_ADV_AMT_EARN").alias("COEC_ADV_AMT_EARN"),
    F.col("COEC_RETN_AMT_EARN").alias("COEC_RETN_AMT_EARN"),
    F.col("COEC_HLD_FOR_RECON").alias("COEC_HLD_FOR_RECON"),
    F.col("COEC_FORGO_AMT").alias("COEC_FORGO_AMT"),
    trim(F.col("COEC_FNCO_PCS_IND")).alias("COEC_FNCO_PCS_IND"),
    trim(F.col("COCE_ID_PAYEE")).alias("COCE_ID_PAYEE")
)

# Transformer - Stage: BusinessRules (produces two output links: AllCol, Transform)
df_BusinessRules_AllCol = df_StripField.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("COBL_CK").alias("COMSN_BILL_REL_UNIQ_KEY"),
    F.col("LOBD_ID").alias("COMSN_CALC_LOB_CD_SK"),
    F.col("COST_CK").alias("COMSN_SCHD_TIER_UNIQ_KEY"),
    F.col("COEC_SEQ_NO").alias("SEQ_NO"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(
        F.lit("FACETS"), F.lit(";"), F.col("COBL_CK"), F.lit(";"),
        F.col("LOBD_ID"), F.lit(";"), F.col("COST_CK"), F.lit(";"), F.col("COEC_SEQ_NO")
    ).alias("PRI_KEY_STRING"),
    F.when(F.col("COEC_FNCO_PCS_IND") == "Y", F.lit("Y"))
     .when(F.col("COEC_FNCO_PCS_IND") == "N", F.lit("N"))
     .otherwise(F.lit("X")).alias("FNCL_COMSN_RPTNG_RCS_IN"),
    format_date(F.col("COEC_PAID_DT"), "SYBASE", "TIMESTAMP", "CCYY-MM-DD").alias("PD_DT_SK"),
    F.col("COEC_ADV_AMT_EARN").alias("ADV_ERN_AMT"),
    F.col("COEC_COMM_AMT_EARN").alias("ERN_COMSN_AMT"),
    F.col("COEC_FORGO_AMT").alias("FORGO_AMT"),
    F.col("COEC_RETN_AMT_EARN").alias("NET_COMSN_ERN_AMT"),
    F.col("COEC_OVRD_AMT").alias("OVRD_AMT"),
    F.col("COEC_COMM_AMT_PAID").alias("PD_COMSN_AMT"),
    F.col("COEC_HLD_FOR_RECON").alias("RECON_HOLD_AMT"),
    F.col("COEC_SOURCE_AMT").alias("SRC_INCM_AMT"),
    F.col("COCE_ID_PAYEE").alias("COCE_ID_PAYEE")
)

df_BusinessRules_Transform = df_StripField.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("COBL_CK").alias("COMSN_BILL_REL_UNIQ_KEY"),
    F.col("LOBD_ID").alias("COMSN_CALC_LOB_CD"),
    F.col("COST_CK").alias("COMSN_SCHD_TIER_UNIQ_KEY"),
    F.col("COEC_SEQ_NO").alias("SEQ_NO")
)

# MAGIC %run ../../../../shared_containers/PrimaryKey/ComsnErnPK
# COMMAND ----------

params_ComsnErnPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrDate,
    "IDSOwner": IDSOwner
}
df_ComsnErnPK = ComsnErnPK(df_BusinessRules_AllCol, df_BusinessRules_Transform, params_ComsnErnPK)

# CSeqFileStage - Stage: IdsComsnErnExtr
df_IdsComsnErnExtr = (
    df_ComsnErnPK
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("LOBD_ID", F.rpad(F.col("LOBD_ID"), 4, " "))
    .withColumn("FNCL_COMSN_RPTNG_RCS_IN", F.rpad(F.col("FNCL_COMSN_RPTNG_RCS_IN"), 1, " "))
    .withColumn("PD_DT_SK", F.rpad(F.col("PD_DT_SK"), 10, " "))
    .withColumn("COCE_ID_PAYEE", F.rpad(F.col("COCE_ID_PAYEE"), 12, " "))
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD"),
        F.col("DISCARD_IN"),
        F.col("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ERR_CT"),
        F.col("RECYCLE_CT"),
        F.col("PRI_KEY_STRING"),
        F.col("COMSN_ERN_SK"),
        F.col("SRC_SYS_CD"),
        F.col("COMSN_BILL_REL_UNIQ_KEY"),
        F.col("LOBD_ID"),
        F.col("COMSN_SCHD_TIER_UNIQ_KEY"),
        F.col("SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("FNCL_COMSN_RPTNG_RCS_IN"),
        F.col("PD_DT_SK"),
        F.col("ADV_ERN_AMT"),
        F.col("ERN_COMSN_AMT"),
        F.col("FORGO_AMT"),
        F.col("NET_COMSN_ERN_AMT"),
        F.col("OVRD_AMT"),
        F.col("PD_COMSN_AMT"),
        F.col("RECON_HOLD_AMT"),
        F.col("SRC_INCM_AMT"),
        F.col("COCE_ID_PAYEE")
    )
)
write_files(
    df_IdsComsnErnExtr,
    f"{adls_path}/key/{TmpOutFile}",
    ",",
    "overwrite",
    False,
    header=False,
    quote="\"",
    nullValue=None
)

# Read from CMC_COEC_EARN_COMM (ODBCConnector) - Stage: Facets_Source
jdbc_url_Facets_Source, jdbc_props_Facets_Source = get_db_config(facets_secret_name)
extract_query_Facets_Source = (
    f"SELECT COBL_CK,LOBD_ID,COST_CK,COEC_SEQ_NO FROM {FacetsOwner}.CMC_COEC_EARN_COMM "
    f"WHERE COEC_PAID_DT >= '{BeginDate}' and COEC_PAID_DT <= '{EndDate}'"
)
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_Facets_Source)
    .options(**jdbc_props_Facets_Source)
    .option("query", extract_query_Facets_Source)
    .load()
)

# Transformer - Stage: Transform (Facets_Source -> Transform)
df_Transform_Stage = df_Facets_Source.withColumn(
    "svComsnCalcLobCdSk",
    GetFkeyCodes("FACETS", F.lit(100), "CLAIM LINE LOB", trim(F.col("LOBD_ID")), "X")
)

df_Transform_Output = df_Transform_Stage.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("COBL_CK").alias("COMSN_BILL_REL_UNIQ_KEY"),
    F.col("svComsnCalcLobCdSk").alias("COMSN_CALC_LOB_CD_SK"),
    F.col("COST_CK").alias("COMSN_SCHD_TIER_UNIQ_KEY"),
    F.col("COEC_SEQ_NO").alias("SEQ_NO")
)

# CSeqFileStage - Stage: Snapshot_File
df_Snapshot_File = df_Transform_Output.select(
    "SRC_SYS_CD_SK",
    "COMSN_BILL_REL_UNIQ_KEY",
    "COMSN_CALC_LOB_CD_SK",
    "COMSN_SCHD_TIER_UNIQ_KEY",
    "SEQ_NO"
)
write_files(
    df_Snapshot_File,
    f"{adls_path}/load/B_COMSN_ERN.dat",
    ",",
    "overwrite",
    False,
    header=False,
    quote="\"",
    nullValue=None
)