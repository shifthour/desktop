# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004, 2005, 2006, 2007, 2008, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  LhoFctsClmPayRductnActvtyExtr
# MAGIC CALLED BY:LhoFctsClmExtr1Seq
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING: Pulls data from CMC_ACRH_RED_HIST to a landing file for the IDS
# MAGIC   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------    
# MAGIC SAndrew                  08/04/2004-                                   Originally Programmed
# MAGIC SAndrew                  10/22/2004-                                   Changed NullCode ( ClmPayRedActIN.ACRH_EVENT_TYPE ) 
# MAGIC                                                                                         to NullOptCode ( ClmPayRedActIN.ACRH_EVENT_TYPE ) 
# MAGIC                                                                                        Changed NullOptCode ( ClmPayRedActIN.ACRH_MCTR_RSN )   
# MAGIC                                                                                         to NullOptCode (ClmPayRedActIN.ACRH_MCTR_RSN )
# MAGIC SAndrew                  08/08/2005                                    Facets 4.2 changes.   Added key field ACPR_SUB_TYP.  impacts extract, CRF, primary and load.  
# MAGIC Steph Goddard        02/16/2006                                     Added transform and primary key for sequencer
# MAGIC Oliver Nielsen           08/21/2007    Balancing                 Added Snapshot for Balancing Table                                          devlIDS30                      Steph Goddard             8/30/07
# MAGIC Bhoomi Dasari         2008-07-07      3657(Primary Key)    Changed primary key process from hash file to DB2 table            devlIDS                          Brent Leland               07-14-2008
# MAGIC                                                                                         Removed balancing snapshot since it was not used.
# MAGIC Krishnakanth           2018-04-25       60037                     Added logic to extract both initial and Delta.                                EnterpriseDev2                Kalyan Neelam           2018-04-27
# MAGIC   Manivannan
# MAGIC 
# MAGIC 
# MAGIC Reddy Sanam        2018-08-13     263449                      Removed Facets db Env variables and added LhoFacetsStg
# MAGIC                                                                                        Changed source query to reflect LhoFacetsStg DB
# MAGIC                                                                                        Changed the Substring FACETS to SrcSysCd in the Business
# MAGIC                                                                                        Rules Transformer
# MAGIC                                                                                        Changed the Shared contrainer name
# MAGIC                                                                                        Changed the Target file name to reflect LhoFacets
# MAGIC Prabhu ES               2022-03-29         S2S                     MSSQL ODBC conn params added                                               IntegrateDev5	Ken Bradmon	2022-06-11

# MAGIC Run STRIP.FIELD transform on fields that might have Carriage Return, Line Feed, and  Tab
# MAGIC Pulling Facets Claim Override Data
# MAGIC Hash file (hf_paymt_reductn_actvty_allcol) cleared from the container - PaymtRductnActvtyPK
# MAGIC No reversals created for this table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, length, upper, substring, concat, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../shared_containers/PrimaryKey/PaymtRductnActvtyPK

DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrentDateParam = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
LastRunDateTime = get_widget_value('LastRunDateTime','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')
LhoFacetsStgAcct = get_widget_value('LhoFacetsStgAcct','')
LhoFacetsStgPW = get_widget_value('LhoFacetsStgPW','')
LhoFacetsStgDSN = get_widget_value('LhoFacetsStgDSN','')

jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)

extract_query = (
    f"SELECT \n"
    f"actvty.ACPR_REF_ID,\n"
    f"actvty.ACPR_SUB_TYPE,\n"
    f"actvty.ACRH_SEQ_NO,\n"
    f"actvty.ACRH_CREATE_DT,\n"
    f"actvty.ACRH_EVENT_TYPE,\n"
    f"actvty.CKPY_REF_ID,\n"
    f"actvty.ACRH_RECD_DT,\n"
    f"actvty.ACRH_PER_END_DT,\n"
    f"actvty.ACRH_AMT,\n"
    f"actvty.USUS_ID,\n"
    f"actvty.ACRH_MCTR_RSN,\n"
    f"actvty.ACRH_RCVD_CKNO \n"
    f"FROM {LhoFacetsStgOwner}.CMC_CLOV_OVERPAY opay, \n"
    f"{LhoFacetsStgOwner}.CMC_ACRH_RED_HIST actvty,\n"
    f"tempdb..{DriverTable} tmp\n"
    f"where tmp.CLM_ID = opay.CLCL_ID\n"
    f"AND opay.ACPR_REF_ID = actvty.ACPR_REF_ID\n"
    f"UNION\n"
    f"SELECT \n"
    f"actvty.ACPR_REF_ID,\n"
    f"actvty.ACPR_SUB_TYPE,\n"
    f"actvty.ACRH_SEQ_NO,\n"
    f"actvty.ACRH_CREATE_DT,\n"
    f"actvty.ACRH_EVENT_TYPE,\n"
    f"actvty.CKPY_REF_ID,\n"
    f"actvty.ACRH_RECD_DT,\n"
    f"actvty.ACRH_PER_END_DT,\n"
    f"actvty.ACRH_AMT,\n"
    f"actvty.USUS_ID,\n"
    f"actvty.ACRH_MCTR_RSN,\n"
    f"actvty.ACRH_RCVD_CKNO \n"
    f"FROM {LhoFacetsStgOwner}.CMC_ACPR_PYMT_RED payred,\n"
    f"{LhoFacetsStgOwner}.CMC_ACRH_RED_HIST actvty\n"
    f"WHERE payred.ACPR_REF_ID = actvty.ACPR_REF_ID\n"
    f"AND payred.ACPR_PAYEE_PR_ID LIKE 'MH%' AND actvty.ACRH_CREATE_DT > '{LastRunDateTime}'"
)

df_CMC_ARCH_RED_HIST = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_StripField = df_CMC_ARCH_RED_HIST.select(
    strip_field(col("ACPR_REF_ID")).alias("ACPR_REF_ID"),
    strip_field(col("ACPR_SUB_TYPE")).alias("ACPR_SUB_TYPE"),
    col("ACRH_SEQ_NO").alias("ACRH_SEQ_NO"),
    col("ACRH_CREATE_DT").alias("ACRH_CREATE_DT"),
    strip_field(col("ACRH_EVENT_TYPE")).alias("ACRH_EVENT_TYPE"),
    strip_field(col("CKPY_REF_ID")).alias("CKPY_REF_ID"),
    col("ACRH_RECD_DT").alias("ACRH_RECD_DT"),
    col("ACRH_PER_END_DT").alias("ACRH_PER_END_DT"),
    col("ACRH_AMT").alias("ACRH_AMT"),
    strip_field(col("USUS_ID")).alias("USUS_ID"),
    strip_field(col("ACRH_MCTR_RSN")).alias("ACRH_MCTR_RSN"),
    strip_field(col("ACRH_RCVD_CKNO")).alias("ACRH_RCVD_CKNO")
)

df_BusinessRules = df_StripField.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(lit("N"), 1, " ").alias("DISCARD_IN"),
    rpad(lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    concat(
        lit(SrcSysCd), lit(";"), trim(col("ACPR_REF_ID")),
        lit(";"), trim(col("ACPR_SUB_TYPE")), lit(";"),
        col("ACRH_SEQ_NO")
    ).alias("PRI_KEY_STRING"),
    lit(0).alias("PAYMT_RDUCTN_ACTVTY_SK"),
    when(
        (col("ACPR_REF_ID").isNull()) | (length(trim(col("ACPR_REF_ID"))) == 0),
        lit("UNK")
    ).otherwise(upper(trim(col("ACPR_REF_ID")))).alias("PAYMT_RDUCTN_REF_ID"),
    when(
        (col("ACPR_SUB_TYPE").isNull()) | (length(trim(col("ACPR_SUB_TYPE"))) == 0),
        lit("UNK")
    ).otherwise(upper(trim(col("ACPR_SUB_TYPE")))).alias("PAYMT_RDUCTN_SUB_TYPE"),
    when(
        (col("ACRH_SEQ_NO").isNull()) | (length(trim(col("ACRH_SEQ_NO"))) == 0),
        lit("UNK")
    ).otherwise(upper(trim(col("ACRH_SEQ_NO")))).alias("PAYMT_RDUCTN_ACTVTY_SEQ_NO"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(
        (col("ACPR_REF_ID").isNull()) | (length(trim(col("ACPR_REF_ID"))) == 0),
        lit("UNK")
    ).otherwise(upper(trim(col("ACPR_REF_ID")))).alias("PAYMT_RDUCTN_ID"),
    when(
        (col("USUS_ID").isNull()) | (length(trim(col("USUS_ID"))) == 0),
        lit("NA")
    ).otherwise(trim(col("USUS_ID"))).alias("USER_ID"),
    when(
        (col("ACRH_EVENT_TYPE").isNull()) | (length(trim(col("ACRH_EVENT_TYPE"))) == 0),
        lit("NA")
    ).otherwise(upper(trim(col("ACRH_EVENT_TYPE")))).alias("PAYMT_RDUCTN_ACT_EVT_TYP_CD"),
    when(
        (col("ACRH_MCTR_RSN").isNull()) | (length(trim(col("ACRH_MCTR_RSN"))) == 0),
        lit("NA")
    ).otherwise(upper(trim(col("ACRH_MCTR_RSN")))).alias("PAYMT_RDUCTN_ACTVTY_RSN_CD"),
    when(
        (col("ACRH_PER_END_DT").isNull()) | (length(trim(col("ACRH_PER_END_DT"))) == 0),
        lit("UNK")
    ).otherwise(substring(trim(col("ACRH_PER_END_DT")), 1, 10)).alias("ACCTG_PERD_END_DT"),
    when(
        (col("ACRH_CREATE_DT").isNull()) | (length(trim(col("ACRH_CREATE_DT"))) == 0),
        lit("UNK")
    ).otherwise(substring(trim(col("ACRH_CREATE_DT")), 1, 10)).alias("CRT_DT"),
    when(
        (col("ACRH_RECD_DT").isNull()) | (length(trim(col("ACRH_RECD_DT"))) == 0),
        lit("UNK")
    ).otherwise(substring(trim(col("ACRH_RECD_DT")), 1, 10)).alias("RFND_RCVD_DT"),
    when(
        (col("ACRH_AMT").isNull()) | (length(trim(col("ACRH_AMT"))) == 0),
        lit(0)
    ).when(
        ~(col("ACRH_AMT").rlike("^-?\\d*\\.?\\d+$")),
        lit(0)
    ).otherwise(col("ACRH_AMT")).alias("PAYMT_RDUCTN_ACTVTY_AMT"),
    lit(0.00).alias("PCA_RCVR_AMT"),
    when(
        (col("CKPY_REF_ID").isNull()) | (length(trim(col("CKPY_REF_ID"))) == 0),
        lit("UNK")
    ).otherwise(upper(trim(col("CKPY_REF_ID")))).alias("PAYMT_REF_ID"),
    when(
        (col("ACRH_RCVD_CKNO").isNull()) | (length(trim(col("ACRH_RCVD_CKNO"))) == 0),
        lit("UNK")
    ).otherwise(upper(trim(col("ACRH_RCVD_CKNO")))).alias("RCVD_CHK_NO")
)

df_Snapshot_AllColl = df_BusinessRules.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("PAYMT_RDUCTN_REF_ID").alias("PAYMT_RDUCTN_REF_ID"),
    rpad(col("PAYMT_RDUCTN_SUB_TYPE"), 1, " ").alias("PAYMT_RDUCTN_SUBTYP_CD"),
    col("PAYMT_RDUCTN_ACTVTY_SEQ_NO").alias("PAYMT_RDUCTN_ACTVTY_SEQ_NO"),
    col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("PAYMT_RDUCTN_ID"), 10, " ").alias("PAYMT_RDUCTN_ID"),
    rpad(col("USER_ID"), 10, " ").alias("USER_ID"),
    rpad(col("PAYMT_RDUCTN_ACT_EVT_TYP_CD"), 10, " ").alias("PAYMT_RDUCTN_ACT_EVT_TYP_CD"),
    rpad(col("PAYMT_RDUCTN_ACTVTY_RSN_CD"), 10, " ").alias("PAYMT_RDUCTN_ACTVTY_RSN_CD"),
    rpad(col("ACCTG_PERD_END_DT"), 10, " ").alias("ACCTG_PERD_END_DT"),
    rpad(col("CRT_DT"), 10, " ").alias("CRT_DT"),
    rpad(col("RFND_RCVD_DT"), 10, " ").alias("RFND_RCVD_DT"),
    col("PAYMT_RDUCTN_ACTVTY_AMT").alias("PAYMT_RDUCTN_ACTVTY_AMT"),
    col("PCA_RCVR_AMT").alias("PCA_RCVR_AMT"),
    col("PAYMT_REF_ID").alias("PAYMT_REF_ID"),
    rpad(col("RCVD_CHK_NO"), 10, " ").alias("RCVD_CHK_NO")
)

df_Snapshot_Transform = df_BusinessRules.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("PAYMT_RDUCTN_REF_ID").alias("PAYMT_RDUCTN_REF_ID"),
    col("PAYMT_RDUCTN_SUB_TYPE").alias("PAYMT_RDUCTN_SUBTYP_CD"),
    col("PAYMT_RDUCTN_ACTVTY_SEQ_NO").alias("PAYMT_RDUCTN_ACTVTY_SEQ_NO")
)

sc_params = {
    "CurrRunCycle": CurrRunCycle,
    "CurrDate": CurrentDateParam,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "IDSOwner": IDSOwner
}

df_PaymtRductnActvty_Key = PaymtRductnActvtyPK(df_Snapshot_Transform, df_Snapshot_AllColl, sc_params)

df_FctsClmPayRductnActvtyExtr = df_PaymtRductnActvty_Key.select(
    col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("PAYMT_RDUCTN_ACTVTY_SK").alias("PAYMT_RDUCTN_ACTVTY_SK"),
    col("PAYMT_RDUCTN_REF_ID").alias("PAYMT_RDUCTN_REF_ID"),
    rpad(col("PAYMT_RDUCTN_SUBTYP_CD"), 1, " ").alias("PAYMT_RDUCTN_SUBTYP_CD"),
    col("PAYMT_RDUCTN_ACTVTY_SEQ_NO").alias("PAYMT_RDUCTN_ACTVTY_SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("PAYMT_RDUCTN_ID"), 10, " ").alias("PAYMT_RDUCTN_ID"),
    rpad(col("USER_ID"), 10, " ").alias("USER_ID"),
    rpad(col("PAYMT_RDUCTN_ACT_EVT_TYP_CD"), 10, " ").alias("PAYMT_RDUCTN_ACT_EVT_TYP_CD"),
    rpad(col("PAYMT_RDUCTN_ACTVTY_RSN_CD"), 10, " ").alias("PAYMT_RDUCTN_ACTVTY_RSN_CD"),
    rpad(col("ACCTG_PERD_END_DT"), 10, " ").alias("ACCTG_PERD_END_DT"),
    rpad(col("CRT_DT"), 10, " ").alias("CRT_DT"),
    rpad(col("RFND_RCVD_DT"), 10, " ").alias("RFND_RCVD_DT"),
    col("PAYMT_RDUCTN_ACTVTY_AMT").alias("PAYMT_RDUCTN_ACTVTY_AMT"),
    col("PCA_RCVR_AMT").alias("PCA_RCVR_AMT"),
    col("PAYMT_REF_ID").alias("PAYMT_REF_ID"),
    rpad(col("RCVD_CHK_NO"), 10, " ").alias("RCVD_CHK_NO")
)

write_files(
    df_FctsClmPayRductnActvtyExtr,
    f"{adls_path}/key/LhoFctsClmPayRductnActvtyExtr.LhoFctsClmPayRductnActvty.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)