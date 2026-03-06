# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsUMSvcFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  Primary Key output file
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes
# MAGIC                             
# MAGIC PROCESSING:  Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:   table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Ralph Tucker  -  02/28/2006  -  Originally programmed
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------              ------------------------------               -------------------------
# MAGIC    
# MAGIC O. Nielsen                        07/29/2008           Facets 4.5.1                 Changed UMSV_IDCD_ID_PRID to VarChar(10)                    devlIDSnew                           Steph Goddard                      08/20/2008
# MAGIC                                                                                                             throughout all transforms                    
# MAGIC 
# MAGIC Bhoomi D                        03/19/2009           3808                              Made change to the Recycle Lookup as the                          devlIDS                                  Steph Goddard                       03/30/2009
# MAGIC                                                                                                             JOB_EXCTN_RCRD_ERR_SK was directed
# MAGIC                                                                                                             to UM_SK instead of UM_SVC_SK 
# MAGIC 
# MAGIC Bhoomi Dasari                 03/25/2009              3808                                Added SrcSysCdSk                                                           devlIDS                                   Steph Goddard                       04/10/2009
# MAGIC 
# MAGIC Rick Henry                      2012-05-13             4896                              Modify SK for DIAG_CD and PROC_CD                                NewDevl                                 Sandrew                               2012-05-20
# MAGIC 
# MAGIC Raja Gummadi                2012-10-01             TTR-1402                      Changed logic in Diag CD SK Fkey stage variable               InegrateNewDevl                       Bhoomi Dasari                      2/25/2013 
# MAGIC 
# MAGIC Harikrishnarao Yadav     2024-01-30             US 610136                   Added "svRfrltypcd" in ForeignKey stage and Mapped         InegrateDev2                             Jeyaprasanna                       2024-02-09
# MAGIC                                                                                                            to RFRL_TYP_CD_SK field

# MAGIC Set all foreign surragote keys
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
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    TimestampType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
InFile = get_widget_value('InFile','IdsUmSvcExtr.dat.pkey')
Logging = get_widget_value('Logging','X')
OutFile = get_widget_value('OutFile','UM_SVC.dat')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# Define schema for IdsUMSvcExtr input
schema_IdsUMSvcExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("UM_SVC_SK", IntegerType(), False),
    StructField("UM_REF_ID", StringType(), False),
    StructField("SEQ_NO", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("UM_SK", IntegerType(), False),
    StructField("MEME_CK", IntegerType(), False),
    StructField("UMSV_AUTH_IND", StringType(), False),
    StructField("UMSV_REF_IND", StringType(), False),
    StructField("UMSV_TYPE", StringType(), False),
    StructField("UMSV_CAT", StringType(), False),
    StructField("UMSV_INPUT_USID", StringType(), False),
    StructField("UMSV_INPUT_DT", TimestampType(), False),
    StructField("UMSV_RECD_DT", TimestampType(), False),
    StructField("UMSV_NEXT_REV_DT", TimestampType(), False),
    StructField("UMSV_AUTH_DT", TimestampType(), False),
    StructField("UMVT_STS", StringType(), False),
    StructField("UMVT_SEQ_NO", IntegerType(), False),
    StructField("UMSV_FROM_DT", TimestampType(), False),
    StructField("UMSV_TO_DT", TimestampType(), False),
    StructField("UMSV_PRPR_ID_REQ", StringType(), False),
    StructField("UMSV_PRPR_ID_SVC", StringType(), False),
    StructField("UMSV_PRPR_ID_FAC", StringType(), False),
    StructField("UMSV_PRPR_ID_PCP", StringType(), False),
    StructField("UMSV_IDCD_ID_PRI", StringType(), False),
    StructField("UMSV_PSCD_ID_REQ", StringType(), False),
    StructField("UMSV_PSCD_ID_AUTH", StringType(), False),
    StructField("UMSV_PSCD_POS_AUTH", StringType(), False),
    StructField("SESE_ID", StringType(), False),
    StructField("SESE_RULE", StringType(), False),
    StructField("SEGR_ID", StringType(), False),
    StructField("IPCD_ID", StringType(), False),
    StructField("PROC_CD_MOD", StringType(), False),
    StructField("UMSV_UNITS_REQ", IntegerType(), False),
    StructField("UMSV_UNITS_AUTH", IntegerType(), False),
    StructField("UMSV_DISALL_EXCD", StringType(), False),
    StructField("UMSV_MCTR_LDNY", StringType(), False),
    StructField("UMSV_ME_AGE", IntegerType(), False),
    StructField("UMSV_AMT_ALLOW", DecimalType(38,10), False),
    StructField("UMSV_UNITS_ALLOW", IntegerType(), False),
    StructField("UMSV_AMT_PAID", DecimalType(38,10), False),
    StructField("UMSV_UNITS_PAID", IntegerType(), False),
    StructField("UMSV_DENY_DT", TimestampType(), False),
    StructField("ATXR_SOURCE_ID", TimestampType(), False),
    StructField("DIAG_CD_TYP_CD", StringType(), False),
    StructField("PROC_CD_TYP_CD", StringType(), False),
    StructField("PROC_CD_CAT_CD", StringType(), False),
    StructField("UMSV_MCTR_RIND", StringType(), False)
])

# Build file path for IdsUMSvcExtr (not "landing" or "external", so default to adls_path)
file_path_IdsUMSvcExtr = f"{adls_path}/key/{InFile}"

# Read input file into dataframe
df_IdsUMSvcExtr = (
    spark.read
    .format("csv")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_IdsUMSvcExtr)
    .load(file_path_IdsUMSvcExtr)
)

# Add stage variables as columns
df_ForeignKey_vars = (
    df_IdsUMSvcExtr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svDisallExcd", GetFkeyExcd(F.col("SRC_SYS_CD"), F.col("UM_SVC_SK"), F.col("UMSV_DISALL_EXCD"), F.lit(Logging)))
    .withColumn("svFacProv", GetFkeyProv(F.col("SRC_SYS_CD"), F.col("UM_SVC_SK"), F.col("UMSV_PRPR_ID_FAC"), F.lit(Logging)))
    .withColumn("svDiagCdSk", GetFkeyDiagCd(F.col("SRC_SYS_CD"), F.col("UM_SVC_SK"), F.col("UMSV_IDCD_ID_PRI"), F.col("DIAG_CD_TYP_CD"), F.lit(Logging)))
    .withColumn("svInptUserSk", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("UM_SVC_SK"), F.col("UMSV_INPUT_USID"), F.lit(Logging)))
    .withColumn("svMbr", GetFkeyMbr(F.col("SRC_SYS_CD"), F.col("UM_SVC_SK"), F.col("MEME_CK"), F.lit(Logging)))
    .withColumn("svPcpProv", GetFkeyProv(F.col("SRC_SYS_CD"), F.col("UM_SVC_SK"), F.col("UMSV_PRPR_ID_PCP"), F.lit(Logging)))
    .withColumn("svRqstProv", GetFkeyProv(F.col("SRC_SYS_CD"), F.col("UM_SVC_SK"), F.col("UMSV_PRPR_ID_REQ"), F.lit(Logging)))
    .withColumn("svSvcProv", GetFkeyProv(F.col("SRC_SYS_CD"), F.col("UM_SVC_SK"), F.col("UMSV_PRPR_ID_SVC"), F.lit(Logging)))
    .withColumn("svProcCd", GetFkeyProcCd(F.col("SRC_SYS_CD"), F.col("UM_SVC_SK"), F.col("IPCD_ID"), F.col("PROC_CD_TYP_CD"), F.col("PROC_CD_CAT_CD"), F.lit(Logging)))
    .withColumn("svAuthPosCatCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("UM_SVC_SK"), F.lit("SERVICE LOCATION TYPE"), F.col("UMSV_PSCD_POS_AUTH"), F.lit(Logging)))
    .withColumn("svAuthPosTypCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("UM_SVC_SK"), F.lit("PLACE OF SERVICE"), F.col("UMSV_PSCD_ID_AUTH"), F.lit(Logging)))
    .withColumn("svDenialRsnCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("UM_SVC_SK"), F.lit("UTILIZATION MANAGEMENT SERVICE DENIAL REASON"), F.col("UMSV_MCTR_LDNY"), F.lit(Logging)))
    .withColumn("svRqstPosTypCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("UM_SVC_SK"), F.lit("PLACE OF SERVICE"), F.col("UMSV_PSCD_ID_REQ"), F.lit(Logging)))
    .withColumn("svSvcSttusCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("UM_SVC_SK"), F.lit("UTILIZATION MANAGEMENT STATUS"), F.col("UMVT_STS"), F.lit(Logging)))
    .withColumn("svSvcTreatCatCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("UM_SVC_SK"), F.lit("UTILIZATION MANAGEMENT TREATMENT"), F.col("UMSV_CAT"), F.lit(Logging)))
    .withColumn("svSvcTypCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("UM_SVC_SK"), F.lit("FACILITY CLAIM ADMISSION TYPE"), F.col("UMSV_TYPE"), F.lit(Logging)))
    .withColumn("svSvcTosCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("UM_SVC_SK"), F.lit("TYPE OF SERVICE"), F.col("SESE_ID"), F.lit(Logging)))
    .withColumn("svAuthDt", GetFkeyDate(F.lit("IDS"), F.col("UM_SVC_SK"), F.col("UMSV_AUTH_DT"), F.lit(Logging)))
    .withColumn("svDenialDt", GetFkeyDate(F.lit("IDS"), F.col("UM_SVC_SK"), F.col("UMSV_DENY_DT"), F.lit(Logging)))
    .withColumn("svInptDt", GetFkeyDate(F.lit("IDS"), F.col("UM_SVC_SK"), F.col("UMSV_INPUT_DT"), F.lit(Logging)))
    .withColumn("svNextRvwDt", GetFkeyDate(F.lit("IDS"), F.col("UM_SVC_SK"), F.col("UMSV_NEXT_REV_DT"), F.lit(Logging)))
    .withColumn("svRcvdDt", GetFkeyDate(F.lit("IDS"), F.col("UM_SVC_SK"), F.col("UMSV_RECD_DT"), F.lit(Logging)))
    .withColumn("svSvcEndDt", GetFkeyDate(F.lit("IDS"), F.col("UM_SVC_SK"), F.col("UMSV_TO_DT"), F.lit(Logging)))
    .withColumn("svSvcStrtDt", GetFkeyDate(F.lit("IDS"), F.col("UM_SVC_SK"), F.col("UMSV_FROM_DT"), F.lit(Logging)))
    .withColumn("svUm", GetFkeyUm(F.col("SRC_SYS_CD"), F.col("UM_SVC_SK"), F.col("UM_REF_ID"), F.lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("UM_SVC_SK")))
    .withColumn("svRfrlTypCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("UM_SVC_SK"), F.lit("UTILIZATION MANAGEMENT SERVICE REFERRAL TYPE"), F.col("UMSV_MCTR_RIND"), F.lit(Logging)))
)

# Split out the data for each output link of the Transformer

# 1) Fkey link: constraint => ErrCount = 0 OR PassThru = 'Y'
df_Fkey = (
    df_ForeignKey_vars
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("UM_SVC_SK").alias("UM_SVC_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("UM_REF_ID").alias("UM_REF_ID"),
        F.col("SEQ_NO").alias("UM_SVC_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svDisallExcd").alias("DSALW_EXCD_SK"),
        F.col("svFacProv").alias("FCLTY_PROV_SK"),
        F.col("svInptUserSk").alias("INPT_USER_SK"),
        F.col("svMbr").alias("MBR_SK"),
        F.col("svPcpProv").alias("PCP_PROV_SK"),
        F.col("svDiagCdSk").alias("PRI_DIAG_CD_SK"),
        F.col("svProcCd").alias("PROC_CD_SK"),
        F.col("svRqstProv").alias("RQST_PROV_SK"),
        F.col("svSvcProv").alias("SVC_PROV_SK"),
        F.col("svUm").alias("UM_SK"),
        F.col("svAuthPosCatCd").alias("UM_SVC_AUTH_POS_CAT_CD_SK"),
        F.col("svAuthPosTypCd").alias("UM_SVC_AUTH_POS_TYP_CD_SK"),
        F.col("svDenialRsnCd").alias("UM_SVC_DENIAL_RSN_CD_SK"),
        F.col("svRqstPosTypCd").alias("UM_SVC_RQST_POS_TYP_CD_SK"),
        F.col("svSvcSttusCd").alias("UM_SVC_STTUS_CD_SK"),
        F.col("svSvcTreatCatCd").alias("UM_SVC_TREAT_CAT_CD_SK"),
        F.col("svSvcTypCd").alias("UM_SVC_TYP_CD_SK"),
        F.col("svSvcTosCd").alias("UM_SVC_TOS_CD_SK"),
        F.col("UMSV_AUTH_IND").alias("PREAUTH_IN"),
        F.col("UMSV_REF_IND").alias("RFRL_IN"),
        F.col("ATXR_SOURCE_ID").alias("ATCHMT_SRC_DTM"),
        F.col("svAuthDt").alias("AUTH_DT_SK"),
        F.col("svDenialDt").alias("DENIAL_DT_SK"),
        F.col("svInptDt").alias("INPT_DT_SK"),
        F.col("svNextRvwDt").alias("NEXT_RVW_DT_SK"),
        F.col("svRcvdDt").alias("RCVD_DT_SK"),
        F.col("svSvcEndDt").alias("SVC_END_DT_SK"),
        F.col("svSvcStrtDt").alias("SVC_STRT_DT_SK"),
        F.col("UMSV_AMT_ALLOW").alias("ALW_AMT"),
        F.col("UMSV_AMT_PAID").alias("PD_AMT"),
        F.col("UMSV_UNITS_ALLOW").alias("ALW_UNIT_CT"),
        F.col("UMSV_UNITS_AUTH").alias("AUTH_UNIT_CT"),
        F.col("UMSV_ME_AGE").alias("MBR_AGE"),
        F.col("UMSV_UNITS_PAID").alias("PD_UNIT_CT"),
        F.col("UMSV_UNITS_REQ").alias("RQST_UNIT_CT"),
        F.col("UMVT_SEQ_NO").alias("STTUS_SEQ_NO"),
        F.col("PROC_CD_MOD").alias("PROC_CD_MOD_TX"),
        F.col("SEGR_ID").alias("SVC_GRP_ID"),
        F.col("SESE_RULE").alias("SVC_RULE_TYP_TX"),
        F.col("svRfrlTypCd").alias("RFRL_TYP_CD_SK")
    )
)

# 2) Recycle link: constraint => ErrCount > 0
df_Recycle = (
    df_ForeignKey_vars
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("UM_SVC_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ERR_CT").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("UM_SVC_SK").alias("UM_SVC_SK"),
        F.col("UM_REF_ID").alias("UM_REF_ID"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("UM_SK").alias("UM_SK"),
        F.col("MEME_CK").alias("MEME_CK"),
        F.col("UMSV_AUTH_IND").alias("UMSV_AUTH_IND"),
        F.col("UMSV_REF_IND").alias("UMSV_REF_IND"),
        F.col("UMSV_TYPE").alias("UMSV_TYPE"),
        F.col("UMSV_CAT").alias("UMSV_CAT"),
        F.col("UMSV_INPUT_USID").alias("UMSV_INPUT_USID"),
        F.col("UMSV_INPUT_DT").alias("UMSV_INPUT_DT"),
        F.col("UMSV_RECD_DT").alias("UMSV_RECD_DT"),
        F.col("UMSV_NEXT_REV_DT").alias("UMSV_NEXT_REV_DT"),
        F.col("UMSV_AUTH_DT").alias("UMSV_AUTH_DT"),
        F.col("UMVT_STS").alias("UMVT_STS"),
        F.col("UMVT_SEQ_NO").alias("UMVT_SEQ_NO"),
        F.col("UMSV_FROM_DT").alias("UMSV_FROM_DT"),
        F.col("UMSV_TO_DT").alias("UMSV_TO_DT"),
        F.col("UMSV_PRPR_ID_REQ").alias("UMSV_PRPR_ID_REQ"),
        F.col("UMSV_PRPR_ID_SVC").alias("UMSV_PRPR_ID_SVC"),
        F.col("UMSV_PRPR_ID_FAC").alias("UMSV_PRPR_ID_FAC"),
        F.col("UMSV_PRPR_ID_PCP").alias("UMSV_PRPR_ID_PCP"),
        F.col("UMSV_IDCD_ID_PRI").alias("UMSV_IDCD_ID_PRI"),
        F.col("UMSV_PSCD_ID_REQ").alias("UMSV_PSCD_ID_REQ"),
        F.col("UMSV_PSCD_ID_AUTH").alias("UMSV_PSCD_ID_AUTH"),
        F.col("UMSV_PSCD_POS_AUTH").alias("UMSV_PSCD_POS_AUTH"),
        F.col("SESE_ID").alias("SESE_ID"),
        F.col("SESE_RULE").alias("SESE_RULE"),
        F.col("SEGR_ID").alias("SEGR_ID"),
        F.col("IPCD_ID").alias("IPCD_ID"),
        F.col("UMSV_UNITS_REQ").alias("UMSV_UNITS_REQ"),
        F.col("UMSV_UNITS_AUTH").alias("UMSV_UNITS_AUTH"),
        F.col("UMSV_DISALL_EXCD").alias("UMSV_DISALL_EXCD"),
        F.col("UMSV_MCTR_LDNY").alias("UMSV_MCTR_LDNY"),
        F.col("UMSV_ME_AGE").alias("UMSV_ME_AGE"),
        F.col("UMSV_AMT_ALLOW").alias("UMSV_AMT_ALLOW"),
        F.col("UMSV_UNITS_ALLOW").alias("UMSV_UNITS_ALLOW"),
        F.col("UMSV_AMT_PAID").alias("UMSV_AMT_PAID"),
        F.col("UMSV_UNITS_PAID").alias("UMSV_UNITS_PAID"),
        F.col("UMSV_DENY_DT").alias("UMSV_DENY_DT"),
        F.col("ATXR_SOURCE_ID").alias("ATXR_SOURCE_ID"),
        F.col("UMSV_MCTR_RIND").alias("UMSV_MCTR_RIND")
    )
)

# Write recycle hashed file to parquet (Scenario C)
file_path_recycle = f"{adls_path}/hf_recycle.parquet"
write_files(
    df_Recycle,
    file_path_recycle,
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# 3) DefaultUNK link => a single row with specified values
# Matching the 46-column structure for the Collector output
df_DefaultUNK = spark.createDataFrame(
    [
        (
            0,  # UM_SVC_SK
            0,  # SRC_SYS_CD_SK
            "UNK",  # UM_REF_ID
            0,  # UM_SVC_SEQ_NO
            0,  # CRT_RUN_CYC_EXCTN_SK
            0,  # LAST_UPDT_RUN_CYC_EXCTN_SK
            0,  # DSALW_EXCD_SK
            0,  # FCLTY_PROV_SK
            0,  # INPT_USER_SK
            0,  # MBR_SK
            0,  # PCP_PROV_SK
            0,  # PRI_DIAG_CD_SK
            0,  # PROC_CD_SK
            0,  # RQST_PROV_SK
            0,  # SVC_PROV_SK
            0,  # UM_SK
            0,  # UM_SVC_AUTH_POS_CAT_CD_SK
            0,  # UM_SVC_AUTH_POS_TYP_CD_SK
            0,  # UM_SVC_DENIAL_RSN_CD_SK
            0,  # UM_SVC_RQST_POS_TYP_CD_SK
            0,  # UM_SVC_STTUS_CD_SK
            0,  # UM_SVC_TREAT_CAT_CD_SK
            0,  # UM_SVC_TYP_CD_SK
            0,  # UM_SVC_TOS_CD_SK
            "U",  # PREAUTH_IN (char(1))
            "U",  # RFRL_IN (char(1))
            "1753-01-01 00:00:00.000",  # ATCHMT_SRC_DTM
            "1753-01-01",  # AUTH_DT_SK (char(10))
            "1753-01-01",  # DENIAL_DT_SK (char(10))
            "1753-01-01",  # INPT_DT_SK (char(10))
            "1753-01-01",  # NEXT_RVW_DT_SK (char(10))
            "1753-01-01",  # RCVD_DT_SK (char(10))
            "1753-01-01",  # SVC_END_DT_SK (char(10))
            "1753-01-01",  # SVC_STRT_DT_SK (char(10))
            0,  # ALW_AMT
            0,  # PD_AMT
            0,  # ALW_UNIT_CT
            0,  # AUTH_UNIT_CT
            0,  # MBR_AGE
            0,  # PD_UNIT_CT
            0,  # RQST_UNIT_CT
            0,  # STTUS_SEQ_NO
            "  ",  # PROC_CD_MOD_TX (char(2))
            "UNK",  # SVC_GRP_ID
            "UNK",  # SVC_RULE_TYP_TX
            0   # RFRL_TYP_CD_SK
        )
    ],
    [
        "UM_SVC_SK",
        "SRC_SYS_CD_SK",
        "UM_REF_ID",
        "UM_SVC_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "DSALW_EXCD_SK",
        "FCLTY_PROV_SK",
        "INPT_USER_SK",
        "MBR_SK",
        "PCP_PROV_SK",
        "PRI_DIAG_CD_SK",
        "PROC_CD_SK",
        "RQST_PROV_SK",
        "SVC_PROV_SK",
        "UM_SK",
        "UM_SVC_AUTH_POS_CAT_CD_SK",
        "UM_SVC_AUTH_POS_TYP_CD_SK",
        "UM_SVC_DENIAL_RSN_CD_SK",
        "UM_SVC_RQST_POS_TYP_CD_SK",
        "UM_SVC_STTUS_CD_SK",
        "UM_SVC_TREAT_CAT_CD_SK",
        "UM_SVC_TYP_CD_SK",
        "UM_SVC_TOS_CD_SK",
        "PREAUTH_IN",
        "RFRL_IN",
        "ATCHMT_SRC_DTM",
        "AUTH_DT_SK",
        "DENIAL_DT_SK",
        "INPT_DT_SK",
        "NEXT_RVW_DT_SK",
        "RCVD_DT_SK",
        "SVC_END_DT_SK",
        "SVC_STRT_DT_SK",
        "ALW_AMT",
        "PD_AMT",
        "ALW_UNIT_CT",
        "AUTH_UNIT_CT",
        "MBR_AGE",
        "PD_UNIT_CT",
        "RQST_UNIT_CT",
        "STTUS_SEQ_NO",
        "PROC_CD_MOD_TX",
        "SVC_GRP_ID",
        "SVC_RULE_TYP_TX",
        "RFRL_TYP_CD_SK"
    ]
)

# 4) DefaultNA link => a single row with specified values
df_DefaultNA = spark.createDataFrame(
    [
        (
            1,  # UM_SVC_SK
            1,  # SRC_SYS_CD_SK
            "NA",  # UM_REF_ID
            1,  # UM_SVC_SEQ_NO
            1,  # CRT_RUN_CYC_EXCTN_SK
            1,  # LAST_UPDT_RUN_CYC_EXCTN_SK
            1,  # DSALW_EXCD_SK
            1,  # FCLTY_PROV_SK
            1,  # INPT_USER_SK
            1,  # MBR_SK
            1,  # PCP_PROV_SK
            1,  # PRI_DIAG_CD_SK
            1,  # PROC_CD_SK
            1,  # RQST_PROV_SK
            1,  # SVC_PROV_SK
            1,  # UM_SK
            1,  # UM_SVC_AUTH_POS_CAT_CD_SK
            1,  # UM_SVC_AUTH_POS_TYP_CD_SK
            1,  # UM_SVC_DENIAL_RSN_CD_SK
            1,  # UM_SVC_RQST_POS_TYP_CD_SK
            1,  # UM_SVC_STTUS_CD_SK
            1,  # UM_SVC_TREAT_CAT_CD_SK
            1,  # UM_SVC_TYP_CD_SK
            1,  # UM_SVC_TOS_CD_SK
            "X",  # PREAUTH_IN (char(1))
            "X",  # RFRL_IN (char(1))
            "1753-01-01 00:00:00.000",  # ATCHMT_SRC_DTM
            "1753-01-01",  # AUTH_DT_SK (char(10))
            "1753-01-01",  # DENIAL_DT_SK (char(10))
            "1753-01-01",  # INPT_DT_SK (char(10))
            "1753-01-01",  # NEXT_RVW_DT_SK (char(10))
            "1753-01-01",  # RCVD_DT_SK (char(10))
            "1753-01-01",  # SVC_END_DT_SK (char(10))
            "1753-01-01",  # SVC_STRT_DT_SK (char(10))
            0,   # ALW_AMT
            0,   # PD_AMT
            0,   # ALW_UNIT_CT
            0,   # AUTH_UNIT_CT
            0,   # MBR_AGE
            0,   # PD_UNIT_CT
            0,   # RQST_UNIT_CT
            0,   # STTUS_SEQ_NO
            "  ",  # PROC_CD_MOD_TX (char(2))
            "NA",  # SVC_GRP_ID
            "NA",  # SVC_RULE_TYP_TX
            1     # RFRL_TYP_CD_SK
        )
    ],
    [
        "UM_SVC_SK",
        "SRC_SYS_CD_SK",
        "UM_REF_ID",
        "UM_SVC_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "DSALW_EXCD_SK",
        "FCLTY_PROV_SK",
        "INPT_USER_SK",
        "MBR_SK",
        "PCP_PROV_SK",
        "PRI_DIAG_CD_SK",
        "PROC_CD_SK",
        "RQST_PROV_SK",
        "SVC_PROV_SK",
        "UM_SK",
        "UM_SVC_AUTH_POS_CAT_CD_SK",
        "UM_SVC_AUTH_POS_TYP_CD_SK",
        "UM_SVC_DENIAL_RSN_CD_SK",
        "UM_SVC_RQST_POS_TYP_CD_SK",
        "UM_SVC_STTUS_CD_SK",
        "UM_SVC_TREAT_CAT_CD_SK",
        "UM_SVC_TYP_CD_SK",
        "UM_SVC_TOS_CD_SK",
        "PREAUTH_IN",
        "RFRL_IN",
        "ATCHMT_SRC_DTM",
        "AUTH_DT_SK",
        "DENIAL_DT_SK",
        "INPT_DT_SK",
        "NEXT_RVW_DT_SK",
        "RCVD_DT_SK",
        "SVC_END_DT_SK",
        "SVC_STRT_DT_SK",
        "ALW_AMT",
        "PD_AMT",
        "ALW_UNIT_CT",
        "AUTH_UNIT_CT",
        "MBR_AGE",
        "PD_UNIT_CT",
        "RQST_UNIT_CT",
        "STTUS_SEQ_NO",
        "PROC_CD_MOD_TX",
        "SVC_GRP_ID",
        "SVC_RULE_TYP_TX",
        "RFRL_TYP_CD_SK"
    ]
)

# 5) Collector merges Fkey, DefaultUNK, DefaultNA
df_Collector = df_Fkey.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

# Apply rpad for final char columns
df_final = (
    df_Collector
    .withColumn("PREAUTH_IN", F.rpad(F.col("PREAUTH_IN"), 1, " "))
    .withColumn("RFRL_IN", F.rpad(F.col("RFRL_IN"), 1, " "))
    .withColumn("AUTH_DT_SK", F.rpad(F.col("AUTH_DT_SK"), 10, " "))
    .withColumn("DENIAL_DT_SK", F.rpad(F.col("DENIAL_DT_SK"), 10, " "))
    .withColumn("INPT_DT_SK", F.rpad(F.col("INPT_DT_SK"), 10, " "))
    .withColumn("NEXT_RVW_DT_SK", F.rpad(F.col("NEXT_RVW_DT_SK"), 10, " "))
    .withColumn("RCVD_DT_SK", F.rpad(F.col("RCVD_DT_SK"), 10, " "))
    .withColumn("SVC_END_DT_SK", F.rpad(F.col("SVC_END_DT_SK"), 10, " "))
    .withColumn("SVC_STRT_DT_SK", F.rpad(F.col("SVC_STRT_DT_SK"), 10, " "))
    .withColumn("PROC_CD_MOD_TX", F.rpad(F.col("PROC_CD_MOD_TX"), 2, " "))
)

# Select columns in the correct order before writing
df_final_ordered = df_final.select(
    "UM_SVC_SK",
    "SRC_SYS_CD_SK",
    "UM_REF_ID",
    "UM_SVC_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "DSALW_EXCD_SK",
    "FCLTY_PROV_SK",
    "INPT_USER_SK",
    "MBR_SK",
    "PCP_PROV_SK",
    "PRI_DIAG_CD_SK",
    "PROC_CD_SK",
    "RQST_PROV_SK",
    "SVC_PROV_SK",
    "UM_SK",
    "UM_SVC_AUTH_POS_CAT_CD_SK",
    "UM_SVC_AUTH_POS_TYP_CD_SK",
    "UM_SVC_DENIAL_RSN_CD_SK",
    "UM_SVC_RQST_POS_TYP_CD_SK",
    "UM_SVC_STTUS_CD_SK",
    "UM_SVC_TREAT_CAT_CD_SK",
    "UM_SVC_TYP_CD_SK",
    "UM_SVC_TOS_CD_SK",
    "PREAUTH_IN",
    "RFRL_IN",
    "ATCHMT_SRC_DTM",
    "AUTH_DT_SK",
    "DENIAL_DT_SK",
    "INPT_DT_SK",
    "NEXT_RVW_DT_SK",
    "RCVD_DT_SK",
    "SVC_END_DT_SK",
    "SVC_STRT_DT_SK",
    "ALW_AMT",
    "PD_AMT",
    "ALW_UNIT_CT",
    "AUTH_UNIT_CT",
    "MBR_AGE",
    "PD_UNIT_CT",
    "RQST_UNIT_CT",
    "STTUS_SEQ_NO",
    "PROC_CD_MOD_TX",
    "SVC_GRP_ID",
    "SVC_RULE_TYP_TX",
    "RFRL_TYP_CD_SK"
)

# Write final file to OutFile (not "landing" or "external", so default to adls_path)
output_path = f"{adls_path}/load/{OutFile}"
write_files(
    df_final_ordered,
    output_path,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)