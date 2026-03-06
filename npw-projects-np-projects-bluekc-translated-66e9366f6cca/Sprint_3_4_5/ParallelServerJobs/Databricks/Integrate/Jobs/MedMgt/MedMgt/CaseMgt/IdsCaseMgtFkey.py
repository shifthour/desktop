# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsCaseMgtFkey
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
# MAGIC             Suzanne Saylor  -  02/14/2006  -  Originally programmed
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------              ---------------------------------               -------------------------
# MAGIC O. Nielsen                        07/29/2008            Facets 4.5.1                     Changed IDCD_ID from char(6) to VarChar(10)                 devlIDSnew                             Steph Goddard                     08/20/2008
# MAGIC Ralph Tucker                  03/27/2009      3808 - BICC                           Initial development                                                            devlIDS  
# MAGIC Ralph Tucker                  04/27/2012      4714 - Facets 5.0                  Added DIAG_CD_TYP_CD to PROC_CD lookup              IntegrateNewDevl                    Sandrew                               20120-05-23
# MAGIC 
# MAGIC Manasa Andru                2014-03-12           TFS - 2295                           Updated the svProcCd Stage Variable.                           IntegrateCurDevl                      Kalyan Neelam                       2014-03-20
# MAGIC 
# MAGIC Jaideep Mankala            2019-12-05           US164999                  Replaced GetFkeyCodes with GetFkeySrcTrgtClctnCodes         IntegrateDev2   	        Abhiram Dasarathy	       2019-12-05         
# MAGIC 							to find accurate Code Sk value

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','')
Logging = get_widget_value('Logging','')
OutFile = get_widget_value('OutFile','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schemaKey = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(10,0), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CASE_MGT_SK", IntegerType(), False),
    StructField("CMCM_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("IDCD_ID", StringType(), False),
    StructField("GRGR_CK", IntegerType(), False),
    StructField("CMCM_INPUT_USID", StringType(), False),
    StructField("MED_MGT_NOTE_ID", TimestampType(), False),
    StructField("MEME_CK", IntegerType(), False),
    StructField("CMCM_USID_PRI", StringType(), False),
    StructField("IPCD_ID", StringType(), False),
    StructField("SGSG_CK", IntegerType(), False),
    StructField("SBSB_CK", IntegerType(), False),
    StructField("CSPD_CAT", StringType(), False),
    StructField("CMCM_MCTR_CPLX", StringType(), False),
    StructField("CMCM_MCTR_ORIG", StringType(), False),
    StructField("CMST_STS", StringType(), False),
    StructField("CMCM_MCTR_TYPE", StringType(), False),
    StructField("CMCM_END_DT", StringType(), False),
    StructField("CMCM_INPUT_DT", TimestampType(), False),
    StructField("CMCM_NEXT_REV_DT", StringType(), False),
    StructField("CMCM_BEG_DT", StringType(), False),
    StructField("CMST_STS_DTM", StringType(), False),
    StructField("CMCM_ME_AGE", IntegerType(), False),
    StructField("CMCT_SEQ_NO", IntegerType(), False),
    StructField("CMST_SEQ_NO", IntegerType(), False),
    StructField("CMCM_SUMMARY", StringType(), False),
    StructField("PDPD_ID", StringType(), False),
    StructField("MED_DENT_ID", StringType(), False),
    StructField("SGSG_ID", StringType(), False),
    StructField("GRGR_ID", StringType(), False),
    StructField("DIAG_CD_TYP_CD", StringType(), False),
    StructField("PROC_CD_CAT_CD", StringType(), False),
    StructField("PROC_CD_TYP_CD", StringType(), False),
])

dfKey = (
    spark.read
    .option("header", False)
    .option("quote", '"')
    .schema(schemaKey)
    .csv(f"{adls_path}/key/{InFile}")
)

dfForeignKey = (
    dfKey
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svDiagCdSk", GetFkeyDiagCd(F.col("SRC_SYS_CD"), F.col("CASE_MGT_SK"), F.col("IDCD_ID"), F.col("DIAG_CD_TYP_CD"), Logging))
    .withColumn("svGrp", GetFkeyGrp(F.col("SRC_SYS_CD"), F.col("CASE_MGT_SK"), F.col("GRGR_ID"), Logging))
    .withColumn("svInptUserSk", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("CASE_MGT_SK"), F.col("CMCM_INPUT_USID"), Logging))
    .withColumn("svMbr", GetFkeyMbr(F.col("SRC_SYS_CD"), F.col("CASE_MGT_SK"), F.col("MEME_CK"), Logging))
    .withColumn("svUsid", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("CASE_MGT_SK"), F.col("CMCM_USID_PRI"), Logging))
    .withColumn("svProcCd", GetFkeyProcCd(F.col("SRC_SYS_CD"), F.col("CASE_MGT_SK"), F.col("IPCD_ID"), trim(F.col("PROC_CD_TYP_CD")), F.col("PROC_CD_CAT_CD"), Logging))
    .withColumn("svProd", GetFkeyProd(F.col("SRC_SYS_CD"), F.col("CASE_MGT_SK"), F.col("PDPD_ID"), Logging))
    .withColumn("svSubGrp", GetFkeySubgrp(F.col("SRC_SYS_CD"), F.col("CASE_MGT_SK"), F.col("GRGR_ID"), F.col("SGSG_ID"), Logging))
    .withColumn("svSub", GetFkeySub(F.col("SRC_SYS_CD"), F.col("CASE_MGT_SK"), F.col("SBSB_CK"), Logging))
    .withColumn("svClsPlnProdcat", GetFkeySrcTrgtClctnCodes(F.col("SRC_SYS_CD"), F.col("CASE_MGT_SK"), F.lit("CLASS PLAN PRODUCT CATEGORY"), F.col("CSPD_CAT"), F.lit("FACETS DBO"), F.lit("IDS"), Logging))
    .withColumn("svCmplxtyLv", GetFkeySrcTrgtClctnCodes(F.col("SRC_SYS_CD"), F.col("CASE_MGT_SK"), F.lit("CASE MANAGEMENT COMPLEXITY LEVEL"), F.col("CMCM_MCTR_CPLX"), F.lit("FACETS DBO"), F.lit("IDS"), Logging))
    .withColumn("svOrigTyp", GetFkeySrcTrgtClctnCodes(F.col("SRC_SYS_CD"), F.col("CASE_MGT_SK"), F.lit("CASE MANAGEMENT ORIGIN"), F.col("CMCM_MCTR_ORIG"), F.lit("FACETS DBO"), F.lit("IDS"), Logging))
    .withColumn("svSttus", GetFkeySrcTrgtClctnCodes(F.col("SRC_SYS_CD"), F.col("CASE_MGT_SK"), F.lit("CASE MANAGEMENT STATUS"), F.col("CMST_STS"), F.lit("FACETS DBO"), F.lit("IDS"), Logging))
    .withColumn("svTyp", GetFkeySrcTrgtClctnCodes(F.col("SRC_SYS_CD"), F.col("CASE_MGT_SK"), F.lit("CASE MANAGEMENT TYPE"), F.col("CMCM_MCTR_TYPE"), F.lit("FACETS DBO"), F.lit("IDS"), Logging))
    .withColumn("svEndDt", GetFkeyDate(F.lit("IDS"), F.col("CASE_MGT_SK"), F.col("CMCM_END_DT"), Logging))
    .withColumn("svInputDt", GetFkeyDate(F.lit("IDS"), F.col("CASE_MGT_SK"), F.col("CMCM_INPUT_DT"), Logging))
    .withColumn("svRevDt", GetFkeyDate(F.lit("IDS"), F.col("CASE_MGT_SK"), F.col("CMCM_NEXT_REV_DT"), Logging))
    .withColumn("svBegDt", GetFkeyDate(F.lit("IDS"), F.col("CASE_MGT_SK"), F.col("CMCM_BEG_DT"), Logging))
    .withColumn("svStsDt", GetFkeyDate(F.lit("IDS"), F.col("CASE_MGT_SK"), F.col("CMST_STS_DTM"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("CASE_MGT_SK")))
)

dfFkey = (
    dfForeignKey
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("CASE_MGT_SK").alias("CASE_MGT_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CMCM_ID").alias("CASE_MGT_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svDiagCdSk").alias("DIAG_CD_SK"),
        F.col("svGrp").alias("GRP_SK"),
        F.col("svInptUserSk").alias("INPT_USER_SK"),
        F.col("svMbr").alias("MBR_SK"),
        F.col("svUsid").alias("PRI_CASE_MGR_USER_SK"),
        F.col("svProcCd").alias("PROC_CD_SK"),
        F.col("svProd").alias("PROD_SK"),
        F.col("svSubGrp").alias("SUBGRP_SK"),
        F.col("svSub").alias("SUB_SK"),
        F.col("svClsPlnProdcat").alias("CASE_MGT_CLS_PLN_PRODCAT_CD_SK"),
        F.col("svCmplxtyLv").alias("CASE_MGT_CMPLXTY_LVL_CD_SK"),
        F.col("svOrigTyp").alias("CASE_MGT_ORIG_TYP_CD_SK"),
        F.col("svSttus").alias("CASE_MGT_STTUS_CD_SK"),
        F.col("svTyp").alias("CASE_MGT_TYP_CD_SK"),
        F.col("svEndDt").alias("END_DT_SK"),
        F.col("svInputDt").alias("INPT_DT_SK"),
        F.col("svRevDt").alias("PRI_CASE_MGR_NEXT_RVW_DT_SK"),
        FORMAT.DATE(F.col("MED_MGT_NOTE_ID"), F.lit("SYBASE"), F.lit("TIMESTAMP"), F.lit("DB2TIMESTAMP")).alias("MED_MGT_NOTE_DTM"),
        F.col("svBegDt").alias("STRT_DT_SK"),
        F.col("svStsDt").alias("STTUS_DT_SK"),
        F.col("CMCM_ME_AGE").alias("MBR_AGE"),
        F.col("CMCT_SEQ_NO").alias("PRI_CNTCT_SEQ_NO"),
        F.col("CMST_SEQ_NO").alias("STTUS_SEQ_NO"),
        F.col("CMCM_SUMMARY").alias("SUM_DESC")
    )
)

dfRecycle = (
    dfForeignKey
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("CASE_MGT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("CASE_MGT_SK").alias("CASE_MGT_SK"),
        F.col("CMCM_ID").alias("CMCM_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("IDCD_ID").alias("IDCD_ID"),
        F.col("GRGR_CK").alias("GRGR_CK"),
        F.col("CMCM_INPUT_USID").alias("CMCM_INPUT_USID"),
        F.col("MED_MGT_NOTE_ID").alias("MED_MGT_NOTE_ID"),
        F.col("MEME_CK").alias("MEME_CK"),
        F.col("CMCM_USID_PRI").alias("CMCM_USID_PRI"),
        F.col("IPCD_ID").alias("IPCD_ID"),
        F.col("SGSG_CK").alias("SGSG_CK"),
        F.col("SBSB_CK").alias("SBSB_CK"),
        F.col("CSPD_CAT").alias("CSPD_CAT"),
        F.col("CMCM_MCTR_CPLX").alias("CMCM_MCTR_CPLX"),
        F.col("CMCM_MCTR_ORIG").alias("CMCM_MCTR_ORIG"),
        F.col("CMST_STS").alias("CMST_STS"),
        F.col("CMCM_MCTR_TYPE").alias("CMCM_MCTR_TYPE"),
        F.col("CMCM_END_DT").alias("CMCM_END_DT"),
        F.col("CMCM_INPUT_DT").alias("CMCM_INPUT_DT"),
        F.col("CMCM_NEXT_REV_DT").alias("CMCM_NEXT_REV_DT"),
        F.col("CMCM_BEG_DT").alias("CMCM_BEG_DT"),
        F.col("CMST_STS_DTM").alias("CMST_STS_DTM"),
        F.col("CMCM_ME_AGE").alias("CMCM_ME_AGE"),
        F.col("CMCT_SEQ_NO").alias("CMCT_SEQ_NO"),
        F.col("CMST_SEQ_NO").alias("CMST_SEQ_NO"),
        F.col("CMCM_SUMMARY").alias("CMCM_SUMMARY"),
        F.col("PDPD_ID").alias("PDPD_ID"),
        F.col("MED_DENT_ID").alias("MED_DENT_ID"),
        F.col("SGSG_ID").alias("SGSG_ID"),
        F.col("GRGR_ID").alias("GRGR_ID"),
        F.col("DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD")
    )
)

write_files(
    dfRecycle,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    '"',
    None
)

dfForeignKeyWithRowNum = dfForeignKey.withColumn(
    "rownum",
    F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
)

dfDefaultUNKpre = dfForeignKeyWithRowNum.filter(F.col("rownum") == 1)
dfDefaultUNK = dfDefaultUNKpre.select(
    F.lit(0).alias("CASE_MGT_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("CASE_MGT_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("DIAG_CD_SK"),
    F.lit(0).alias("GRP_SK"),
    F.lit(0).alias("INPT_USER_SK"),
    F.lit(0).alias("MBR_SK"),
    F.lit(0).alias("PRI_CASE_MGR_USER_SK"),
    F.lit(0).alias("PROC_CD_SK"),
    F.lit(0).alias("PROD_SK"),
    F.lit(0).alias("SUBGRP_SK"),
    F.lit(0).alias("SUB_SK"),
    F.lit(0).alias("CASE_MGT_CLS_PLN_PRODCAT_CD_SK"),
    F.lit(0).alias("CASE_MGT_CMPLXTY_LVL_CD_SK"),
    F.lit(0).alias("CASE_MGT_ORIG_TYP_CD_SK"),
    F.lit(0).alias("CASE_MGT_STTUS_CD_SK"),
    F.lit(0).alias("CASE_MGT_TYP_CD_SK"),
    F.lit("2199-12-31").alias("END_DT_SK"),
    F.lit("1753-01-01").alias("INPT_DT_SK"),
    F.lit("1753-01-01").alias("PRI_CASE_MGR_NEXT_RVW_DT_SK"),
    FORMAT.DATE(F.lit("1753-01-01 00:00:00.000"), F.lit("SYBASE"), F.lit("TIMESTAMP"), F.lit("DB2TIMESTAMP")).alias("MED_MGT_NOTE_DTM"),
    F.lit("1753-01-01").alias("STRT_DT_SK"),
    F.lit("1753-01-01").alias("STTUS_DT_SK"),
    F.lit(0).alias("MBR_AGE"),
    F.lit(0).alias("PRI_CNTCT_SEQ_NO"),
    F.lit(0).alias("STTUS_SEQ_NO"),
    F.lit("").alias("SUM_DESC")
)

dfDefaultNApre = dfForeignKeyWithRowNum.filter(F.col("rownum") == 1)
dfDefaultNA = dfDefaultNApre.select(
    F.lit(1).alias("CASE_MGT_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("CASE_MGT_ID"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("DIAG_CD_SK"),
    F.lit(1).alias("GRP_SK"),
    F.lit(1).alias("INPT_USER_SK"),
    F.lit(1).alias("MBR_SK"),
    F.lit(1).alias("PRI_CASE_MGR_USER_SK"),
    F.lit(1).alias("PROC_CD_SK"),
    F.lit(1).alias("PROD_SK"),
    F.lit(1).alias("SUBGRP_SK"),
    F.lit(1).alias("SUB_SK"),
    F.lit(1).alias("CASE_MGT_CLS_PLN_PRODCAT_CD_SK"),
    F.lit(1).alias("CASE_MGT_CMPLXTY_LVL_CD_SK"),
    F.lit(1).alias("CASE_MGT_ORIG_TYP_CD_SK"),
    F.lit(1).alias("CASE_MGT_STTUS_CD_SK"),
    F.lit(1).alias("CASE_MGT_TYP_CD_SK"),
    F.lit("2199-12-31").alias("END_DT_SK"),
    F.lit("1753-01-01").alias("INPT_DT_SK"),
    F.lit("1753-01-01").alias("PRI_CASE_MGR_NEXT_RVW_DT_SK"),
    FORMAT.DATE(F.lit("1753-01-01 00:00:00.001"), F.lit("SYBASE"), F.lit("TIMESTAMP"), F.lit("DB2TIMESTAMP")).alias("MED_MGT_NOTE_DTM"),
    F.lit("1753-01-01").alias("STRT_DT_SK"),
    F.lit("1753-01-01").alias("STTUS_DT_SK"),
    F.lit(0).alias("MBR_AGE"),
    F.lit(0).alias("PRI_CNTCT_SEQ_NO"),
    F.lit(0).alias("STTUS_SEQ_NO"),
    F.lit("").alias("SUM_DESC")
)

dfCollectorCols = [
    "CASE_MGT_SK",
    "SRC_SYS_CD_SK",
    "CASE_MGT_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "DIAG_CD_SK",
    "GRP_SK",
    "INPT_USER_SK",
    "MBR_SK",
    "PRI_CASE_MGR_USER_SK",
    "PROC_CD_SK",
    "PROD_SK",
    "SUBGRP_SK",
    "SUB_SK",
    "CASE_MGT_CLS_PLN_PRODCAT_CD_SK",
    "CASE_MGT_CMPLXTY_LVL_CD_SK",
    "CASE_MGT_ORIG_TYP_CD_SK",
    "CASE_MGT_STTUS_CD_SK",
    "CASE_MGT_TYP_CD_SK",
    "END_DT_SK",
    "INPT_DT_SK",
    "PRI_CASE_MGR_NEXT_RVW_DT_SK",
    "MED_MGT_NOTE_DTM",
    "STRT_DT_SK",
    "STTUS_DT_SK",
    "MBR_AGE",
    "PRI_CNTCT_SEQ_NO",
    "STTUS_SEQ_NO",
    "SUM_DESC"
]

dfCollector = (
    dfFkey.select(dfCollectorCols)
    .unionByName(dfDefaultUNK.select(dfCollectorCols))
    .unionByName(dfDefaultNA.select(dfCollectorCols))
)

dfFinal = (
    dfCollector
    .withColumn("END_DT_SK", F.rpad(F.col("END_DT_SK"), 10, " "))
    .withColumn("INPT_DT_SK", F.rpad(F.col("INPT_DT_SK"), 10, " "))
    .withColumn("PRI_CASE_MGR_NEXT_RVW_DT_SK", F.rpad(F.col("PRI_CASE_MGR_NEXT_RVW_DT_SK"), 10, " "))
    .withColumn("STRT_DT_SK", F.rpad(F.col("STRT_DT_SK"), 10, " "))
    .withColumn("STTUS_DT_SK", F.rpad(F.col("STTUS_DT_SK"), 10, " "))
)

write_files(
    dfFinal.select(dfCollectorCols),
    f"{adls_path}/load/{OutFile}",
    ",",
    "overwrite",
    False,
    False,
    '"',
    None
)