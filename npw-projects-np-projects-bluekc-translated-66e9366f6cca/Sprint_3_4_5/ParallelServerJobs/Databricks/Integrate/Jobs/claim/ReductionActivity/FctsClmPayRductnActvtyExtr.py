# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004, 2005, 2006, 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
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
# MAGIC Prabhu ES               2022-02-28      S2S Remediation     MSSQL connection parameters added                                        IntegrateDev5                 Kalyan Neelam           2022-06-10

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
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/PaymtRductnActvtyPK
# COMMAND ----------

DriverTable = get_widget_value('DriverTable','TMP_IDS_CLAIM')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','20080708')
CurrentDate = get_widget_value('CurrentDate','2008-07-08')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105859')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
LastRunDateTime = get_widget_value('LastRunDateTime','')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)

extract_query = f"""
SELECT 
 actvty.ACPR_REF_ID,
 actvty.ACPR_SUB_TYPE,
 actvty.ACRH_SEQ_NO,
 actvty.ACRH_CREATE_DT,
 actvty.ACRH_EVENT_TYPE,
 actvty.CKPY_REF_ID,
 actvty.ACRH_RECD_DT,
 actvty.ACRH_PER_END_DT,
 actvty.ACRH_AMT,
 actvty.USUS_ID,
 actvty.ACRH_MCTR_RSN,
 actvty.ACRH_RCVD_CKNO
FROM  {FacetsOwner}.CMC_CLOV_OVERPAY opay, 
      {FacetsOwner}.CMC_ACRH_RED_HIST actvty,
      tempdb..{DriverTable} tmp
WHERE tmp.CLM_ID = opay.CLCL_ID
  AND opay.ACPR_REF_ID = actvty.ACPR_REF_ID

UNION

SELECT 
 actvty.ACPR_REF_ID,
 actvty.ACPR_SUB_TYPE,
 actvty.ACRH_SEQ_NO,
 actvty.ACRH_CREATE_DT,
 actvty.ACRH_EVENT_TYPE,
 actvty.CKPY_REF_ID,
 actvty.ACRH_RECD_DT,
 actvty.ACRH_PER_END_DT,
 actvty.ACRH_AMT,
 actvty.USUS_ID,
 actvty.ACRH_MCTR_RSN,
 actvty.ACRH_RCVD_CKNO
FROM {FacetsOwner}.CMC_ACPR_PYMT_RED payred,
     {FacetsOwner}.CMC_ACRH_RED_HIST actvty
WHERE payred.ACPR_REF_ID = actvty.ACPR_REF_ID
  AND payred.ACPR_PAYEE_PR_ID LIKE 'MH%' 
  AND actvty.ACRH_CREATE_DT > '{LastRunDateTime}'
"""

df_CMC_ARCH_RED_HIST = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query)
        .load()
)

df_StripField = df_CMC_ARCH_RED_HIST.select(
    strip_field(F.col("ACPR_REF_ID")).alias("ACPR_REF_ID"),
    strip_field(F.col("ACPR_SUB_TYPE")).alias("ACPR_SUB_TYPE"),
    F.col("ACRH_SEQ_NO").alias("ACRH_SEQ_NO"),
    F.col("ACRH_CREATE_DT").alias("ACRH_CREATE_DT"),
    strip_field(F.col("ACRH_EVENT_TYPE")).alias("ACRH_EVENT_TYPE"),
    strip_field(F.col("CKPY_REF_ID")).alias("CKPY_REF_ID"),
    F.col("ACRH_RECD_DT").alias("ACRH_RECD_DT"),
    F.col("ACRH_PER_END_DT").alias("ACRH_PER_END_DT"),
    F.col("ACRH_AMT").alias("ACRH_AMT"),
    strip_field(F.col("USUS_ID")).alias("USUS_ID"),
    strip_field(F.col("ACRH_MCTR_RSN")).alias("ACRH_MCTR_RSN"),
    strip_field(F.col("ACRH_RCVD_CKNO")).alias("ACRH_RCVD_CKNO")
)

df_Transform1 = (
    df_StripField
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", current_date())
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(
            F.lit("FACETS"),
            F.lit(";"),
            trim(F.col("ACPR_REF_ID")),
            F.lit(";"),
            trim(F.col("ACPR_SUB_TYPE")),
            F.lit(";"),
            F.col("ACRH_SEQ_NO")
        )
    )
    .withColumn("PAYMT_RDUCTN_ACTVTY_SK", F.lit(0))
    .withColumn(
        "PAYMT_RDUCTN_REF_ID",
        F.when(
            F.col("ACPR_REF_ID").isNull() | (F.trim(F.col("ACPR_REF_ID")) == ""),
            F.lit("UNK")
        ).otherwise(F.upper(trim(F.col("ACPR_REF_ID"))))
    )
    .withColumn(
        "PAYMT_RDUCTN_SUB_TYPE",
        F.when(
            F.col("ACPR_SUB_TYPE").isNull() | (F.trim(F.col("ACPR_SUB_TYPE")) == ""),
            F.lit("UNK")
        ).otherwise(F.upper(trim(F.col("ACPR_SUB_TYPE"))))
    )
    .withColumn(
        "PAYMT_RDUCTN_ACTVTY_SEQ_NO",
        F.when(
            F.col("ACRH_SEQ_NO").isNull() | (F.trim(F.col("ACRH_SEQ_NO")) == ""),
            F.lit("UNK")
        ).otherwise(F.upper(trim(F.col("ACRH_SEQ_NO"))))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn(
        "PAYMT_RDUCTN_ID",
        F.when(
            F.col("ACPR_REF_ID").isNull() | (F.trim(F.col("ACPR_REF_ID")) == ""),
            F.lit("UNK")
        ).otherwise(F.upper(trim(F.col("ACPR_REF_ID"))))
    )
    .withColumn(
        "USER_ID",
        F.when(
            F.col("USUS_ID").isNull() | (F.trim(F.col("USUS_ID")) == ""),
            F.lit("NA")
        ).otherwise(trim(F.col("USUS_ID")))
    )
    .withColumn(
        "PAYMT_RDUCTN_ACT_EVT_TYP_CD",
        F.when(
            F.col("ACRH_EVENT_TYPE").isNull() | (F.trim(F.col("ACRH_EVENT_TYPE")) == ""),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("ACRH_EVENT_TYPE"))))
    )
    .withColumn(
        "PAYMT_RDUCTN_ACTVTY_RSN_CD",
        F.when(
            F.col("ACRH_MCTR_RSN").isNull() | (F.trim(F.col("ACRH_MCTR_RSN")) == ""),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("ACRH_MCTR_RSN"))))
    )
    .withColumn(
        "ACCTG_PERD_END_DT",
        F.when(
            F.col("ACRH_PER_END_DT").isNull() | (F.trim(F.col("ACRH_PER_END_DT")) == ""),
            F.lit("UNK")
        ).otherwise(F.col("ACRH_PER_END_DT").substr(F.lit(1), F.lit(10)))
    )
    .withColumn(
        "CRT_DT",
        F.when(
            F.col("ACRH_CREATE_DT").isNull() | (F.trim(F.col("ACRH_CREATE_DT")) == ""),
            F.lit("UNK")
        ).otherwise(F.col("ACRH_CREATE_DT").substr(F.lit(1), F.lit(10)))
    )
    .withColumn(
        "RFND_RCVD_DT",
        F.when(
            F.col("ACRH_RECD_DT").isNull() | (F.trim(F.col("ACRH_RECD_DT")) == ""),
            F.lit("UNK")
        ).otherwise(F.col("ACRH_RECD_DT").substr(F.lit(1), F.lit(10)))
    )
    .withColumn(
        "PAYMT_RDUCTN_ACTVTY_AMT",
        F.when(
            F.col("ACRH_AMT").isNull() | (F.trim(F.col("ACRH_AMT")) == ""),
            F.lit(0)
        )
        .when(
            F.col("ACRH_AMT").cast(DoubleType()).isNull(),
            F.lit(0)
        )
        .otherwise(F.col("ACRH_AMT").cast(DoubleType()))
    )
    .withColumn("PCA_RCVR_AMT", F.lit(0.00))
    .withColumn(
        "PAYMT_REF_ID",
        F.when(
            F.col("CKPY_REF_ID").isNull() | (F.trim(F.col("CKPY_REF_ID")) == ""),
            F.lit("UNK")
        ).otherwise(F.upper(trim(F.col("CKPY_REF_ID"))))
    )
    .withColumn(
        "RCVD_CHK_NO",
        F.when(
            F.col("ACRH_RCVD_CKNO").isNull() | (F.trim(F.col("ACRH_RCVD_CKNO")) == ""),
            F.lit("UNK")
        ).otherwise(F.upper(trim(F.col("ACRH_RCVD_CKNO"))))
    )
)

df_AllColl = df_Transform1.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("PAYMT_RDUCTN_REF_ID").alias("PAYMT_RDUCTN_REF_ID"),
    F.col("PAYMT_RDUCTN_SUB_TYPE").alias("PAYMT_RDUCTN_SUBTYP_CD"),
    F.col("PAYMT_RDUCTN_ACTVTY_SEQ_NO").alias("PAYMT_RDUCTN_ACTVTY_SEQ_NO"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PAYMT_RDUCTN_ID").alias("PAYMT_RDUCTN_ID"),
    F.col("USER_ID").alias("USER_ID"),
    F.col("PAYMT_RDUCTN_ACT_EVT_TYP_CD").alias("PAYMT_RDUCTN_ACT_EVT_TYP_CD"),
    F.col("PAYMT_RDUCTN_ACTVTY_RSN_CD").alias("PAYMT_RDUCTN_ACTVTY_RSN_CD"),
    F.col("ACCTG_PERD_END_DT").alias("ACCTG_PERD_END_DT"),
    F.col("CRT_DT").alias("CRT_DT"),
    F.col("RFND_RCVD_DT").alias("RFND_RCVD_DT"),
    F.col("PAYMT_RDUCTN_ACTVTY_AMT").alias("PAYMT_RDUCTN_ACTVTY_AMT"),
    F.col("PCA_RCVR_AMT").alias("PCA_RCVR_AMT"),
    F.col("PAYMT_REF_ID").alias("PAYMT_REF_ID"),
    F.col("RCVD_CHK_NO").alias("RCVD_CHK_NO")
)

df_Transform = df_Transform1.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("PAYMT_RDUCTN_REF_ID").alias("PAYMT_RDUCTN_REF_ID"),
    F.col("PAYMT_RDUCTN_SUB_TYPE").alias("PAYMT_RDUCTN_SUBTYP_CD"),
    F.col("PAYMT_RDUCTN_ACTVTY_SEQ_NO").alias("PAYMT_RDUCTN_ACTVTY_SEQ_NO")
)

params = {
    "CurrRunCycle": CurrRunCycle,
    "CurrDate": CurrentDate,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "IDSOwner": IDSOwner
}

df_FctsClmPayRductnActvtyExtr = PaymtRductnActvtyPK(df_AllColl, df_Transform, params)

df_final = df_FctsClmPayRductnActvtyExtr.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("PAYMT_RDUCTN_ACTVTY_SK").alias("PAYMT_RDUCTN_ACTVTY_SK"),
    F.col("PAYMT_RDUCTN_REF_ID").alias("PAYMT_RDUCTN_REF_ID"),
    F.rpad(F.col("PAYMT_RDUCTN_SUBTYP_CD"), 1, " ").alias("PAYMT_RDUCTN_SUBTYP_CD"),
    F.col("PAYMT_RDUCTN_ACTVTY_SEQ_NO").alias("PAYMT_RDUCTN_ACTVTY_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("PAYMT_RDUCTN_ID"), 10, " ").alias("PAYMT_RDUCTN_ID"),
    F.rpad(F.col("USER_ID"), 10, " ").alias("USER_ID"),
    F.rpad(F.col("PAYMT_RDUCTN_ACT_EVT_TYP_CD"), 10, " ").alias("PAYMT_RDUCTN_ACT_EVT_TYP_CD"),
    F.rpad(F.col("PAYMT_RDUCTN_ACTVTY_RSN_CD"), 10, " ").alias("PAYMT_RDUCTN_ACTVTY_RSN_CD"),
    F.rpad(F.col("ACCTG_PERD_END_DT"), 10, " ").alias("ACCTG_PERD_END_DT"),
    F.rpad(F.col("CRT_DT"), 10, " ").alias("CRT_DT"),
    F.rpad(F.col("RFND_RCVD_DT"), 10, " ").alias("RFND_RCVD_DT"),
    F.col("PAYMT_RDUCTN_ACTVTY_AMT").alias("PAYMT_RDUCTN_ACTVTY_AMT"),
    F.col("PCA_RCVR_AMT").alias("PCA_RCVR_AMT"),
    F.col("PAYMT_REF_ID").alias("PAYMT_REF_ID"),
    F.rpad(F.col("RCVD_CHK_NO"), 10, " ").alias("RCVD_CHK_NO")
)

file_path = f"{adls_path}/key/FctsClmPayRductnActvtyExtr.FctsClmPayRductnActvty.dat.{RunID}"

write_files(
    df_final,
    file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)