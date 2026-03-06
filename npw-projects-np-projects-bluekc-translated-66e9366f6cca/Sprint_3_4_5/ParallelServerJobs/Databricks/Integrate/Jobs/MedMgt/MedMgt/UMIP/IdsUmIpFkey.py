# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsUmIpFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the primary key file and applies the foreign keys.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  Primary Key output file
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes
# MAGIC                             GetFkeyDiagCd
# MAGIC                             GetFkeyProv
# MAGIC                             GetFkeyApplUsr
# MAGIC                             GetFkeyExcd
# MAGIC                             GetFkeyProcCd
# MAGIC                             GetFkeyDate
# MAGIC                             GetFkeyErrorCnt
# MAGIC 
# MAGIC PROCESSING:  Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:   UM_IP table load file
# MAGIC                       hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Hugh Sisson - 03/01/2006 - Original program
# MAGIC             Hugh Sisson - 05/05/2006 - Corrected GetFkeyCodes lookup for source system code by changing
# MAGIC                                                           parameter value from "IDS" to Key.SRC_SYS_CD
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC       
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                  Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                            ----------------------------------              ---------------------------------               -------------------------
# MAGIC Bhoomi Dasari                 03/25/2009              3808                                Added SrcSysCdSk                                                           devlIDS                             Steph Goddard                      03/30/2009
# MAGIC   
# MAGIC Rick Henry                      2012-05-13         4896                                     Modify SK for Diag_Cd and Proc_Cd                            NewDevl                                  SAndrew                                 2012-05-20
# MAGIC Raja Gummadi                 2012-10-04             TTR-1461                         Fkey Lookup Updated for                                            IntegrateNewDevl                   Bhoomi Dasari                         10/9/2012
# MAGIC                                                                                                                  UM_IP_AUTH_POS_TYP_CD_SK 
# MAGIC Raja Gummadi                 2013-01-10             TTR-526                           Added new column DSCHG_DIAG_CD_SK                IntegrateNewDevl                   Bhoomi Dasari                         2/25/2013

# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','IdsUmIpExtr.tmp')
OutFile = get_widget_value('OutFile','UM_IP.dat')
Logging = get_widget_value('Logging','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_IdsUmIpExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("UM_SK", IntegerType(), nullable=False),
    StructField("UM_REF_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("ADMS_PRI_DIAG_CD", StringType(), nullable=False),
    StructField("ADMS_PROV", StringType(), nullable=False),
    StructField("ATND_PROV", StringType(), nullable=False),
    StructField("DENIED_USER", StringType(), nullable=False),
    StructField("DSALW_EXCD", StringType(), nullable=False),
    StructField("FCLTY_PROV", StringType(), nullable=False),
    StructField("INPT_USER", StringType(), nullable=False),
    StructField("PCP_PROV", StringType(), nullable=False),
    StructField("PRI_DIAG_CD", StringType(), nullable=False),
    StructField("PRI_SURG_PROC_CD", StringType(), nullable=False),
    StructField("PROV_AGMNT_SK", StringType(), nullable=False),
    StructField("RQST_PROV", StringType(), nullable=False),
    StructField("SURGEON_PROV", StringType(), nullable=False),
    StructField("UM_IP_ADMS_TREAT_CAT_CD", StringType(), nullable=False),
    StructField("UM_IP_AUTH_POS_CAT_CD", StringType(), nullable=False),
    StructField("UM_IP_AUTH_POS_TYP_CD", StringType(), nullable=False),
    StructField("UM_IP_CARE_TYP_CD", StringType(), nullable=False),
    StructField("UM_IP_CUR_TREAT_CAT_CD", StringType(), nullable=False),
    StructField("UM_IP_DENIAL_RSN_CD", StringType(), nullable=False),
    StructField("UM_IP_DSCHG_STTUS_CD", StringType(), nullable=False),
    StructField("UM_IP_FCLTY_NTWK_STTUS_CD", StringType(), nullable=False),
    StructField("UM_IP_RQST_POS_CAT_CD", StringType(), nullable=False),
    StructField("UM_IP_RQST_POS_TYP_CD", StringType(), nullable=False),
    StructField("UM_IP_STTUS_CD", StringType(), nullable=False),
    StructField("PREAUTH_IN", StringType(), nullable=False),
    StructField("RFRL_IN", StringType(), nullable=False),
    StructField("RQST_PROV_PCP_IN", StringType(), nullable=False),
    StructField("ACTL_ADMS_DT", StringType(), nullable=False),
    StructField("ATCHMT_SRC_DTM", TimestampType(), nullable=False),
    StructField("AUTH_ADMS_DT", StringType(), nullable=False),
    StructField("AUTH_DT", StringType(), nullable=False),
    StructField("DENIAL_DT", StringType(), nullable=False),
    StructField("DSCHG_DTM", TimestampType(), nullable=False),
    StructField("XPCT_DSCHG_DT", StringType(), nullable=False),
    StructField("INPT_DT", StringType(), nullable=False),
    StructField("RCVD_DT", StringType(), nullable=False),
    StructField("RQST_ADMS_DT", StringType(), nullable=False),
    StructField("NEXT_RVW_DT", StringType(), nullable=False),
    StructField("STTUS_DT", StringType(), nullable=False),
    StructField("UM_IP_RQST_SURG_DT", StringType(), nullable=False),
    StructField("UM_IP_AUTH_SURG_DT", StringType(), nullable=False),
    StructField("ACTL_LOS_DAYS_QTY", IntegerType(), nullable=False),
    StructField("ALW_TOT_LOS_DAYS_QTY", IntegerType(), nullable=False),
    StructField("AUTH_TOT_LOS_DAYS_QTY", IntegerType(), nullable=False),
    StructField("BRTH_WT_QTY", IntegerType(), nullable=False),
    StructField("MBR_AGE", IntegerType(), nullable=False),
    StructField("PRI_GOAL_END_LOS_DAYS_QTY", IntegerType(), nullable=False),
    StructField("PRI_GOAL_STRT_LOS_DAYS_QTY", IntegerType(), nullable=False),
    StructField("RQST_PREOP_DAYS_QTY", IntegerType(), nullable=False),
    StructField("RQST_TOT_LOS_DAYS_QTY", IntegerType(), nullable=False),
    StructField("PRI_SURG_PROC_CD_MOD_TX", StringType(), nullable=False),
    StructField("DIAG_CD_TYP_CD", StringType(), nullable=False),
    StructField("PROC_CD_TYP_CD", StringType(), nullable=False),
    StructField("PROC_CD_CAT_CD", StringType(), nullable=False),
    StructField("DSCHG_DIAG_CD", StringType(), nullable=False)
])

df_IdsUmIpExtr = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_IdsUmIpExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_ForeignKey = (
    df_IdsUmIpExtr
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svDefaultDate", lit("1753-01-01-00.00.00.000000"))
    .withColumn("svAdmsPriDiagCdSk", GetFkeyDiagCd(col("SRC_SYS_CD"), col("UM_SK"), col("ADMS_PRI_DIAG_CD"), col("DIAG_CD_TYP_CD"), lit(Logging)))
    .withColumn("svAdmsProvSk", GetFkeyProv(col("SRC_SYS_CD"), col("UM_SK"), col("ADMS_PROV"), lit(Logging)))
    .withColumn("svAtndProvSk", GetFkeyProv(col("SRC_SYS_CD"), col("UM_SK"), col("ATND_PROV"), lit(Logging)))
    .withColumn("svDeniedUser", GetFkeyAppUsr(col("SRC_SYS_CD"), col("UM_SK"), col("DENIED_USER"), lit(Logging)))
    .withColumn("svDsalwExcd", GetFkeyExcd(col("SRC_SYS_CD"), col("UM_SK"), col("DSALW_EXCD"), lit(Logging)))
    .withColumn("svFcltyProvSk", GetFkeyProv(col("SRC_SYS_CD"), col("UM_SK"), col("FCLTY_PROV"), lit(Logging)))
    .withColumn("svInputUserSk", GetFkeyAppUsr(col("SRC_SYS_CD"), col("UM_SK"), col("INPT_USER"), lit(Logging)))
    .withColumn("svPcpProvSk", GetFkeyProv(col("SRC_SYS_CD"), col("UM_SK"), col("PCP_PROV"), lit(Logging)))
    .withColumn("svPriDiagCdSk", GetFkeyDiagCd(col("SRC_SYS_CD"), col("UM_SK"), col("PRI_DIAG_CD"), col("DIAG_CD_TYP_CD"), lit(Logging)))
    .withColumn("svPriSurgProcCdSk", GetFkeyProcCd(col("SRC_SYS_CD"), col("UM_SK"), col("PRI_SURG_PROC_CD"), col("PROC_CD_TYP_CD"), col("PROC_CD_CAT_CD"), lit(Logging)))
    .withColumn("svRqstProvSk", GetFkeyProv(col("SRC_SYS_CD"), col("UM_SK"), col("RQST_PROV"), lit(Logging)))
    .withColumn("svSurgeonProvSk", GetFkeyProv(col("SRC_SYS_CD"), col("UM_SK"), col("SURGEON_PROV"), lit(Logging)))
    .withColumn("svUmIpAdmsTreatCatCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("UM_SK"), lit("UTILIZATION MANAGEMENT TREATMENT"), col("UM_IP_ADMS_TREAT_CAT_CD"), lit(Logging)))
    .withColumn("svUmIpAuthPosCatCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("UM_SK"), lit("SERVICE LOCATION TYPE"), col("UM_IP_AUTH_POS_CAT_CD"), lit(Logging)))
    .withColumn("svUmIpAuthPosTypCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("UM_SK"), lit("PLACE OF SERVICE"), col("UM_IP_AUTH_POS_TYP_CD"), lit(Logging)))
    .withColumn("svUmIpCareTypCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("UM_SK"), lit("FACILITY CLAIM ADMISSION TYPE"), col("UM_IP_CARE_TYP_CD"), lit(Logging)))
    .withColumn("svUmIpCurTreatCatCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("UM_SK"), lit("UTILIZATION MANAGEMENT TREATMENT"), col("UM_IP_CUR_TREAT_CAT_CD"), lit(Logging)))
    .withColumn("svUmIpDenialRsnCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("UM_SK"), lit("UTILIZATION MANAGEMENT INPATIENT DENIAL REASON"), col("UM_IP_DENIAL_RSN_CD"), lit(Logging)))
    .withColumn("svUmIpDschgSttusCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("UM_SK"), lit("FACILITY CLAIM DISCHARGE STATUS"), col("UM_IP_DSCHG_STTUS_CD"), lit(Logging)))
    .withColumn("svUmIpFcltyNtwkSttusCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("UM_SK"), lit("CLAIM IN NETWORK"), col("UM_IP_FCLTY_NTWK_STTUS_CD"), lit(Logging)))
    .withColumn("svUmIpRqstPosCatCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("UM_SK"), lit("SERVICE LOCATION TYPE"), col("UM_IP_RQST_POS_CAT_CD"), lit(Logging)))
    .withColumn("svUmIpRqstPosTypCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("UM_SK"), lit("PLACE OF SERVICE"), col("UM_IP_RQST_POS_TYP_CD"), lit(Logging)))
    .withColumn("svUmIpSttusCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("UM_SK"), lit("UTILIZATION MANAGEMENT STATUS"), col("UM_IP_STTUS_CD"), lit(Logging)))
    .withColumn("svActlAdmsDtSk", GetFkeyDate(lit("IDS"), col("UM_SK"), col("ACTL_ADMS_DT"), lit(Logging)))
    .withColumn("svAuthAdmsDtSk", GetFkeyDate(lit("IDS"), col("UM_SK"), col("AUTH_ADMS_DT"), lit(Logging)))
    .withColumn("svAuthDtSk", GetFkeyDate(lit("IDS"), col("UM_SK"), col("AUTH_DT"), lit(Logging)))
    .withColumn("svDenialDtSk", GetFkeyDate(lit("IDS"), col("UM_SK"), col("DENIAL_DT"), lit(Logging)))
    .withColumn("svXpctDschgDtSk", GetFkeyDate(lit("IDS"), col("UM_SK"), col("XPCT_DSCHG_DT"), lit(Logging)))
    .withColumn("svInptDtSk", GetFkeyDate(lit("IDS"), col("UM_SK"), col("INPT_DT"), lit(Logging)))
    .withColumn("svRcvdDtSk", GetFkeyDate(lit("IDS"), col("UM_SK"), col("RCVD_DT"), lit(Logging)))
    .withColumn("svRqstAdmsDtSk", GetFkeyDate(lit("IDS"), col("UM_SK"), col("RQST_ADMS_DT"), lit(Logging)))
    .withColumn("svNextRvwDtSd", GetFkeyDate(lit("IDS"), col("UM_SK"), col("NEXT_RVW_DT"), lit(Logging)))
    .withColumn("svSttusDtSk", GetFkeyDate(lit("IDS"), col("UM_SK"), col("STTUS_DT"), lit(Logging)))
    .withColumn("svUmIpRqstSurgDtSk", GetFkeyDate(lit("IDS"), col("UM_SK"), col("UM_IP_RQST_SURG_DT"), lit(Logging)))
    .withColumn("svDschgDiagCdSk", GetFkeyDiagCd(col("SRC_SYS_CD"), col("UM_SK"), col("DSCHG_DIAG_CD"), col("DIAG_CD_TYP_CD"), lit(Logging)))
    .withColumn("svUmIpAuthSurgDtSk", GetFkeyDate(lit("IDS"), col("UM_SK"), col("UM_IP_AUTH_SURG_DT"), lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("UM_SK")))
)

w = Window.orderBy(lit(1))
df_ForeignKeyWithRN = df_ForeignKey.withColumn("rowNumber", row_number().over(w))

df_ForeignKey_Fkey = df_ForeignKeyWithRN.filter((col("ErrCount") == lit(0)) | (col("PassThru") == lit("Y")))
df_ForeignKey_Recycle = df_ForeignKeyWithRN.filter(col("ErrCount") > lit(0))
df_ForeignKey_DefaultUNK = df_ForeignKeyWithRN.filter(col("rowNumber") == lit(1))
df_ForeignKey_DefaultNA = df_ForeignKeyWithRN.filter(col("rowNumber") == lit(1))

df_Fkey_select = df_ForeignKey_Fkey.select(
    col("UM_SK").alias("UM_SK"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("UM_REF_ID").alias("UM_REF_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svAdmsPriDiagCdSk").alias("ADMS_PRI_DIAG_CD_SK"),
    col("svAdmsProvSk").alias("ADMS_PROV_SK"),
    col("svAtndProvSk").alias("ATND_PROV_SK"),
    col("svDeniedUser").alias("DENIED_USER_SK"),
    col("svDsalwExcd").alias("DSALW_EXCD_SK"),
    col("svFcltyProvSk").alias("FCLTY_PROV_SK"),
    col("svInputUserSk").alias("INPT_USER_SK"),
    col("svPcpProvSk").alias("PCP_PROV_SK"),
    col("svPriDiagCdSk").alias("PRI_DIAG_CD_SK"),
    col("svPriSurgProcCdSk").alias("PRI_SURG_PROC_CD_SK"),
    col("PROV_AGMNT_SK").alias("PROV_AGMNT_SK"),
    col("svRqstProvSk").alias("RQST_PROV_SK"),
    col("svSurgeonProvSk").alias("SRGN_PROV_SK"),
    col("svUmIpAdmsTreatCatCdSk").alias("UM_IP_ADMS_TREAT_CAT_CD_SK"),
    col("svUmIpAuthPosCatCdSk").alias("UM_IP_AUTH_POS_CAT_CD_SK"),
    col("svUmIpAuthPosTypCdSk").alias("UM_IP_AUTH_POS_TYP_CD_SK"),
    col("svUmIpCareTypCdSk").alias("UM_IP_CARE_TYP_CD_SK"),
    col("svUmIpCurTreatCatCdSk").alias("UM_IP_CUR_TREAT_CAT_CD_SK"),
    col("svUmIpDenialRsnCdSk").alias("UM_IP_DENIAL_RSN_CD_SK"),
    col("svUmIpDschgSttusCdSk").alias("UM_IP_DSCHG_STTUS_CD_SK"),
    col("svUmIpFcltyNtwkSttusCdSk").alias("UM_IP_FCLTY_NTWK_STTUS_CD_SK"),
    col("svUmIpRqstPosCatCdSk").alias("UM_IP_RQST_POS_CAT_CD_SK"),
    col("svUmIpRqstPosTypCdSk").alias("UM_IP_RQST_POS_TYP_CD_SK"),
    col("svUmIpSttusCdSk").alias("UM_IP_STTUS_CD_SK"),
    col("PREAUTH_IN").alias("PREAUTH_IN"),
    col("RFRL_IN").alias("RFRL_IN"),
    col("RQST_PROV_PCP_IN").alias("RQST_PROV_PCP_IN"),
    col("svActlAdmsDtSk").alias("ACTL_ADMS_DT_SK"),
    col("ATCHMT_SRC_DTM").alias("ATCHMT_SRC_DTM"),
    col("svAuthAdmsDtSk").alias("AUTH_ADMS_DT_SK"),
    col("svAuthDtSk").alias("AUTH_DT_SK"),
    col("svDenialDtSk").alias("DENIAL_DT_SK"),
    col("DSCHG_DTM").alias("DSCHG_DTM"),
    col("svXpctDschgDtSk").alias("XPCT_DSCHG_DT_SK"),
    col("svInptDtSk").alias("INPT_DT_SK"),
    col("svRcvdDtSk").alias("RCVD_DT_SK"),
    col("svRqstAdmsDtSk").alias("RQST_ADMS_DT_SK"),
    col("svNextRvwDtSd").alias("NEXT_RVW_DT_SK"),
    col("svSttusDtSk").alias("STTUS_DT_SK"),
    col("svUmIpRqstSurgDtSk").alias("UM_IP_RQST_SURG_DT_SK"),
    col("svUmIpAuthSurgDtSk").alias("UM_IP_AUTH_SURG_DT_SK"),
    col("ACTL_LOS_DAYS_QTY").alias("ACTL_LOS_DAYS_QTY"),
    col("ALW_TOT_LOS_DAYS_QTY").alias("ALW_TOT_LOS_DAYS_QTY"),
    col("AUTH_TOT_LOS_DAYS_QTY").alias("AUTH_TOT_LOS_DAYS_QTY"),
    col("BRTH_WT_QTY").alias("BRTH_WT_QTY"),
    col("MBR_AGE").alias("MBR_AGE"),
    col("PRI_GOAL_END_LOS_DAYS_QTY").alias("PRI_GOAL_END_LOS_DAYS_QTY"),
    col("PRI_GOAL_STRT_LOS_DAYS_QTY").alias("PRI_GOAL_STRT_LOS_DAYS_QTY"),
    col("RQST_PREOP_DAYS_QTY").alias("RQST_PREOP_DAYS_QTY"),
    col("RQST_TOT_LOS_DAYS_QTY").alias("RQST_TOT_LOS_DAYS_QTY"),
    col("PRI_SURG_PROC_CD_MOD_TX").alias("PRI_SURG_PROC_CD_MOD_TX"),
    col("svDschgDiagCdSk").alias("DSCHG_DIAG_CD_SK")
)

df_Recycle_select = df_ForeignKey_Recycle.select(
    GetRecycleKey(col("UM_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ErrCount").alias("ERR_CT"),
    (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("UM_SK").alias("UM_SK"),
    col("UM_REF_ID").alias("UM_REF_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ADMS_PRI_DIAG_CD").alias("ADMS_PRI_DIAG_CD"),
    col("ADMS_PROV").alias("ADMS_PROV"),
    col("ATND_PROV").alias("ATND_PROV"),
    col("DENIED_USER").alias("DENIED_USER"),
    col("DSALW_EXCD").alias("DSALW_EXCD"),
    col("FCLTY_PROV").alias("FCLTY_PROV"),
    col("INPT_USER").alias("INPT_USER"),
    col("PCP_PROV").alias("PCP_PROV"),
    col("PRI_DIAG_CD").alias("PRI_DIAG_CD"),
    col("PRI_SURG_PROC_CD").alias("PRI_SURG_PROC_CD"),
    col("PROV_AGMNT_SK").alias("PROV_AGMNT_SK"),
    col("RQST_PROV").alias("RQST_PROV"),
    col("SURGEON_PROV").alias("SURGEON_PROV"),
    col("UM_IP_ADMS_TREAT_CAT_CD").alias("UM_IP_ADMS_TREAT_CAT_CD"),
    col("UM_IP_AUTH_POS_CAT_CD").alias("UM_IP_AUTH_POS_CAT_CD"),
    col("UM_IP_AUTH_POS_TYP_CD").alias("UM_IP_AUTH_POS_TYP_CD"),
    col("UM_IP_CARE_TYP_CD").alias("UM_IP_CARE_TYP_CD"),
    col("UM_IP_CUR_TREAT_CAT_CD").alias("UM_IP_CUR_TREAT_CAT_CD"),
    col("UM_IP_DENIAL_RSN_CD").alias("UM_IP_DENIAL_RSN_CD"),
    col("UM_IP_DSCHG_STTUS_CD").alias("UM_IP_DSCHG_STTUS_CD"),
    col("UM_IP_FCLTY_NTWK_STTUS_CD").alias("UM_IP_FCLTY_NTWK_STTUS_CD"),
    col("UM_IP_RQST_POS_CAT_CD").alias("UM_IP_RQST_POS_CAT_CD"),
    col("UM_IP_RQST_POS_TYP_CD").alias("UM_IP_RQST_POS_TYP_CD"),
    col("UM_IP_STTUS_CD").alias("UM_IP_STTUS_CD"),
    col("PREAUTH_IN").alias("PREAUTH_IN"),
    col("RFRL_IN").alias("RFRL_IN"),
    col("RQST_PROV_PCP_IN").alias("RQST_PROV_PCP_IN"),
    col("ACTL_ADMS_DT").alias("ACTL_ADMS_DT"),
    col("ATCHMT_SRC_DTM").alias("ATCHMT_SRC_DTM"),
    col("AUTH_ADMS_DT").alias("AUTH_ADMS_DT"),
    col("AUTH_DT").alias("AUTH_DT"),
    col("DENIAL_DT").alias("DENIAL_DT"),
    col("DSCHG_DTM").alias("DSCHG_DTM"),
    col("XPCT_DSCHG_DT").alias("XPCT_DSCHG_DT"),
    col("INPT_DT").alias("INPT_DT"),
    col("RCVD_DT").alias("RCVD_DT"),
    col("RQST_ADMS_DT").alias("RQST_ADMS_DT"),
    col("NEXT_RVW_DT").alias("NEXT_RVW_DT"),
    col("STTUS_DT").alias("STTUS_DT"),
    col("UM_IP_RQST_SURG_DT").alias("UM_IP_RQST_SURG_DT"),
    col("UM_IP_AUTH_SURG_DT").alias("UM_IP_AUTH_SURG_DT"),
    col("ACTL_LOS_DAYS_QTY").alias("ACTL_LOS_DAYS_QTY"),
    col("ALW_TOT_LOS_DAYS_QTY").alias("ALW_TOT_LOS_DAYS_QTY"),
    col("AUTH_TOT_LOS_DAYS_QTY").alias("AUTH_TOT_LOS_DAYS_QTY"),
    col("BRTH_WT_QTY").alias("BRTH_WT_QTY"),
    col("MBR_AGE").alias("MBR_AGE"),
    col("PRI_GOAL_END_LOS_DAYS_QTY").alias("PRI_GOAL_END_LOS_DAYS_QTY"),
    col("PRI_GOAL_STRT_LOS_DAYS_QTY").alias("PRI_GOAL_STRT_LOS_DAYS_QTY"),
    col("RQST_PREOP_DAYS_QTY").alias("RQST_PREOP_DAYS_QTY"),
    col("RQST_TOT_LOS_DAYS_QTY").alias("RQST_TOT_LOS_DAYS_QTY"),
    col("PRI_SURG_PROC_CD_MOD_TX").alias("PRI_SURG_PROC_CD_MOD_TX")
)

write_files(
    df_Recycle_select,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_DefaultUNK_select = df_ForeignKey_DefaultUNK.select(
    lit(0).alias("UM_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit("UNK").alias("UM_REF_ID"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("ADMS_PRI_DIAG_CD_SK"),
    lit(0).alias("ADMS_PROV_SK"),
    lit(0).alias("ATND_PROV_SK"),
    lit(0).alias("DENIED_USER_SK"),
    lit(0).alias("DSALW_EXCD_SK"),
    lit(0).alias("FCLTY_PROV_SK"),
    lit(0).alias("INPT_USER_SK"),
    lit(0).alias("PCP_PROV_SK"),
    lit(0).alias("PRI_DIAG_CD_SK"),
    lit(0).alias("PRI_SURG_PROC_CD_SK"),
    lit(0).alias("PROV_AGMNT_SK"),
    lit(0).alias("RQST_PROV_SK"),
    lit(0).alias("SRGN_PROV_SK"),
    lit(0).alias("UM_IP_ADMS_TREAT_CAT_CD_SK"),
    lit(0).alias("UM_IP_AUTH_POS_CAT_CD_SK"),
    lit(0).alias("UM_IP_AUTH_POS_TYP_CD_SK"),
    lit(0).alias("UM_IP_CARE_TYP_CD_SK"),
    lit(0).alias("UM_IP_CUR_TREAT_CAT_CD_SK"),
    lit(0).alias("UM_IP_DENIAL_RSN_CD_SK"),
    lit(0).alias("UM_IP_DSCHG_STTUS_CD_SK"),
    lit(0).alias("UM_IP_FCLTY_NTWK_STTUS_CD_SK"),
    lit(0).alias("UM_IP_RQST_POS_CAT_CD_SK"),
    lit(0).alias("UM_IP_RQST_POS_TYP_CD_SK"),
    lit(0).alias("UM_IP_STTUS_CD_SK"),
    lit("U").alias("PREAUTH_IN"),
    lit("U").alias("RFRL_IN"),
    lit("U").alias("RQST_PROV_PCP_IN"),
    lit("1753-01-01").alias("ACTL_ADMS_DT_SK"),
    col("svDefaultDate").alias("ATCHMT_SRC_DTM"),
    lit("1753-01-01").alias("AUTH_ADMS_DT_SK"),
    lit("1753-01-01").alias("AUTH_DT_SK"),
    lit("1753-01-01").alias("DENIAL_DT_SK"),
    col("svDefaultDate").alias("DSCHG_DTM"),
    lit("1753-01-01").alias("XPCT_DSCHG_DT_SK"),
    lit("1753-01-01").alias("INPT_DT_SK"),
    lit("1753-01-01").alias("RCVD_DT_SK"),
    lit("1753-01-01").alias("RQST_ADMS_DT_SK"),
    lit("1753-01-01").alias("NEXT_RVW_DT_SK"),
    lit("1753-01-01").alias("STTUS_DT_SK"),
    lit("1753-01-01").alias("UM_IP_RQST_SURG_DT_SK"),
    lit("1753-01-01").alias("UM_IP_AUTH_SURG_DT_SK"),
    lit(0).alias("ACTL_LOS_DAYS_QTY"),
    lit(0).alias("ALW_TOT_LOS_DAYS_QTY"),
    lit(0).alias("AUTH_TOT_LOS_DAYS_QTY"),
    lit(0).alias("BRTH_WT_QTY"),
    lit(0).alias("MBR_AGE"),
    lit(0).alias("PRI_GOAL_END_LOS_DAYS_QTY"),
    lit(0).alias("PRI_GOAL_STRT_LOS_DAYS_QTY"),
    lit(0).alias("RQST_PREOP_DAYS_QTY"),
    lit(0).alias("RQST_TOT_LOS_DAYS_QTY"),
    lit("  ").alias("PRI_SURG_PROC_CD_MOD_TX"),
    lit(0).alias("DSCHG_DIAG_CD_SK")
)

df_DefaultNA_select = df_ForeignKey_DefaultNA.select(
    lit(1).alias("UM_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit("NA").alias("UM_REF_ID"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("ADMS_PRI_DIAG_CD_SK"),
    lit(1).alias("ADMS_PROV_SK"),
    lit(1).alias("ATND_PROV_SK"),
    lit(1).alias("DENIED_USER_SK"),
    lit(1).alias("DSALW_EXCD_SK"),
    lit(1).alias("FCLTY_PROV_SK"),
    lit(1).alias("INPT_USER_SK"),
    lit(1).alias("PCP_PROV_SK"),
    lit(1).alias("PRI_DIAG_CD_SK"),
    lit(1).alias("PRI_SURG_PROC_CD_SK"),
    lit(1).alias("PROV_AGMNT_SK"),
    lit(1).alias("RQST_PROV_SK"),
    lit(1).alias("SRGN_PROV_SK"),
    lit(1).alias("UM_IP_ADMS_TREAT_CAT_CD_SK"),
    lit(1).alias("UM_IP_AUTH_POS_CAT_CD_SK"),
    lit(1).alias("UM_IP_AUTH_POS_TYP_CD_SK"),
    lit(1).alias("UM_IP_CARE_TYP_CD_SK"),
    lit(1).alias("UM_IP_CUR_TREAT_CAT_CD_SK"),
    lit(1).alias("UM_IP_DENIAL_RSN_CD_SK"),
    lit(1).alias("UM_IP_DSCHG_STTUS_CD_SK"),
    lit(1).alias("UM_IP_FCLTY_NTWK_STTUS_CD_SK"),
    lit(1).alias("UM_IP_RQST_POS_CAT_CD_SK"),
    lit(1).alias("UM_IP_RQST_POS_TYP_CD_SK"),
    lit(1).alias("UM_IP_STTUS_CD_SK"),
    lit("X").alias("PREAUTH_IN"),
    lit("X").alias("RFRL_IN"),
    lit("X").alias("RQST_PROV_PCP_IN"),
    lit("1753-01-01").alias("ACTL_ADMS_DT_SK"),
    col("svDefaultDate").alias("ATCHMT_SRC_DTM"),
    lit("1753-01-01").alias("AUTH_ADMS_DT_SK"),
    lit("1753-01-01").alias("AUTH_DT_SK"),
    lit("1753-01-01").alias("DENIAL_DT_SK"),
    col("svDefaultDate").alias("DSCHG_DTM"),
    lit("1753-01-01").alias("XPCT_DSCHG_DT_SK"),
    lit("1753-01-01").alias("INPT_DT_SK"),
    lit("1753-01-01").alias("RCVD_DT_SK"),
    lit("1753-01-01").alias("RQST_ADMS_DT_SK"),
    lit("1753-01-01").alias("NEXT_RVW_DT_SK"),
    lit("1753-01-01").alias("STTUS_DT_SK"),
    lit("1753-01-01").alias("UM_IP_RQST_SURG_DT_SK"),
    lit("1753-01-01").alias("UM_IP_AUTH_SURG_DT_SK"),
    lit(1).alias("ACTL_LOS_DAYS_QTY"),
    lit(1).alias("ALW_TOT_LOS_DAYS_QTY"),
    lit(1).alias("AUTH_TOT_LOS_DAYS_QTY"),
    lit(0).alias("BRTH_WT_QTY"),
    lit(0).alias("MBR_AGE"),
    lit(1).alias("PRI_GOAL_END_LOS_DAYS_QTY"),
    lit(1).alias("PRI_GOAL_STRT_LOS_DAYS_QTY"),
    lit(1).alias("RQST_PREOP_DAYS_QTY"),
    lit(1).alias("RQST_TOT_LOS_DAYS_QTY"),
    lit("  ").alias("PRI_SURG_PROC_CD_MOD_TX"),
    lit(1).alias("DSCHG_DIAG_CD_SK")
)

df_Collector = (
    df_Fkey_select.union(df_DefaultUNK_select)
    .union(df_DefaultNA_select)
)

# Now apply rpad for final columns that are char or varchar, matching the lengths given in the final stage:
# The final order is exactly as they appear in the Collector -> "LoadFile" -> "UM_IP"
df_Collector_padded = (
    df_Collector
    .withColumn("PREAUTH_IN", rpad(col("PREAUTH_IN"), 1, " "))
    .withColumn("RFRL_IN", rpad(col("RFRL_IN"), 1, " "))
    .withColumn("RQST_PROV_PCP_IN", rpad(col("RQST_PROV_PCP_IN"), 1, " "))
    .withColumn("ACTL_ADMS_DT_SK", rpad(col("ACTL_ADMS_DT_SK"), 10, " "))
    .withColumn("AUTH_ADMS_DT_SK", rpad(col("AUTH_ADMS_DT_SK"), 10, " "))
    .withColumn("AUTH_DT_SK", rpad(col("AUTH_DT_SK"), 10, " "))
    .withColumn("DENIAL_DT_SK", rpad(col("DENIAL_DT_SK"), 10, " "))
    .withColumn("XPCT_DSCHG_DT_SK", rpad(col("XPCT_DSCHG_DT_SK"), 10, " "))
    .withColumn("INPT_DT_SK", rpad(col("INPT_DT_SK"), 10, " "))
    .withColumn("RCVD_DT_SK", rpad(col("RCVD_DT_SK"), 10, " "))
    .withColumn("RQST_ADMS_DT_SK", rpad(col("RQST_ADMS_DT_SK"), 10, " "))
    .withColumn("NEXT_RVW_DT_SK", rpad(col("NEXT_RVW_DT_SK"), 10, " "))
    .withColumn("STTUS_DT_SK", rpad(col("STTUS_DT_SK"), 10, " "))
    .withColumn("UM_IP_RQST_SURG_DT_SK", rpad(col("UM_IP_RQST_SURG_DT_SK"), 10, " "))
    .withColumn("UM_IP_AUTH_SURG_DT_SK", rpad(col("UM_IP_AUTH_SURG_DT_SK"), 10, " "))
    .withColumn("PRI_SURG_PROC_CD_MOD_TX", rpad(col("PRI_SURG_PROC_CD_MOD_TX"), 2, " "))
)

df_UM_IP_final = df_Collector_padded.select(
    "UM_SK",
    "SRC_SYS_CD_SK",
    "UM_REF_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ADMS_PRI_DIAG_CD_SK",
    "ADMS_PROV_SK",
    "ATND_PROV_SK",
    "DENIED_USER_SK",
    "DSALW_EXCD_SK",
    "FCLTY_PROV_SK",
    "INPT_USER_SK",
    "PCP_PROV_SK",
    "PRI_DIAG_CD_SK",
    "PRI_SURG_PROC_CD_SK",
    "PROV_AGMNT_SK",
    "RQST_PROV_SK",
    "SRGN_PROV_SK",
    "UM_IP_ADMS_TREAT_CAT_CD_SK",
    "UM_IP_AUTH_POS_CAT_CD_SK",
    "UM_IP_AUTH_POS_TYP_CD_SK",
    "UM_IP_CARE_TYP_CD_SK",
    "UM_IP_CUR_TREAT_CAT_CD_SK",
    "UM_IP_DENIAL_RSN_CD_SK",
    "UM_IP_DSCHG_STTUS_CD_SK",
    "UM_IP_FCLTY_NTWK_STTUS_CD_SK",
    "UM_IP_RQST_POS_CAT_CD_SK",
    "UM_IP_RQST_POS_TYP_CD_SK",
    "UM_IP_STTUS_CD_SK",
    "PREAUTH_IN",
    "RFRL_IN",
    "RQST_PROV_PCP_IN",
    "ACTL_ADMS_DT_SK",
    "ATCHMT_SRC_DTM",
    "AUTH_ADMS_DT_SK",
    "AUTH_DT_SK",
    "DENIAL_DT_SK",
    "DSCHG_DTM",
    "XPCT_DSCHG_DT_SK",
    "INPT_DT_SK",
    "RCVD_DT_SK",
    "RQST_ADMS_DT_SK",
    "NEXT_RVW_DT_SK",
    "STTUS_DT_SK",
    "UM_IP_RQST_SURG_DT_SK",
    "UM_IP_AUTH_SURG_DT_SK",
    "ACTL_LOS_DAYS_QTY",
    "ALW_TOT_LOS_DAYS_QTY",
    "AUTH_TOT_LOS_DAYS_QTY",
    "BRTH_WT_QTY",
    "MBR_AGE",
    "PRI_GOAL_END_LOS_DAYS_QTY",
    "PRI_GOAL_STRT_LOS_DAYS_QTY",
    "RQST_PREOP_DAYS_QTY",
    "RQST_TOT_LOS_DAYS_QTY",
    "PRI_SURG_PROC_CD_MOD_TX",
    "DSCHG_DIAG_CD_SK"
)

write_files(
    df_UM_IP_final,
    f"{adls_path}/load/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)