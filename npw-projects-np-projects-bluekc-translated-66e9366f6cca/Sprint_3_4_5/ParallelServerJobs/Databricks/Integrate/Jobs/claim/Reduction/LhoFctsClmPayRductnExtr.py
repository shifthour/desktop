# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004, 2005, 2006, 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  LhoFctsClmPayRductnExtr
# MAGIC CALLED BY: LhoFctsClmExtr1Seq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC   Pulls data from CMC_ACPR_PYMT_RED  to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------    
# MAGIC               SAndrew   08/04/2004-   Originally Programmed
# MAGIC               SAndrew   08/08/2005    Facets 4.2 changes.   Added key field ACPR_SUB_TYP.  impacts extract, CRF, primary and load.
# MAGIC                                                         changed PCA_OVERPD_NET_AMT to have default of 0.00
# MAGIC               Steph Goddard  02/16/2006  Combine extract, transform, primary key for sequencer
# MAGIC 
# MAGIC Oliver Nielsen          08/21/2007        Balancing            Added Balancing Snapshot                                                           devlIDS30                     Steph Goddard           8/30/07 
# MAGIC 
# MAGIC Bhoomi Dasari         2008-07-08      3657(Primary Key)  Changed primary key process from hash file to DB2 table               devlIDS                        Brent Leland               7-11-2008           
# MAGIC                                                                                       Removed balancing snapshot since it was not used.
# MAGIC 
# MAGIC Manasa Andru       2015-04-22        TFS - 12493         Updated the field - PAYMT_RDUCTN_EXCD_ID                      IntegrateDev2                    Jag Yelavarthi             2016-05-03
# MAGIC                                                                                       to No Upcase so that the field would find
# MAGIC                                                                                          a match on the EXCD table.
# MAGIC 
# MAGIC Krishnakanth          2018-04-25       60037                    Added logic to extract both initial and Delta.                                EnterpriseDev2                 Kalyan Neelam           2018-04-27
# MAGIC   Manivannan
# MAGIC 
# MAGIC Reddy Sanam       2020-08-12     263448                  Added LhoFacetsStg Env Variables and removed                         IntegrateDev2
# MAGIC                                                                                   Facets Env variales
# MAGIC                                                                                   Replaced FACETS with SrcSycCd in the BusinessRules
# MAGIC                                                                                   Transformer
# MAGIC                                                                                    Changed Target file name to relfect LhoFcts
# MAGIC Prabhu ES            2022-03-29         S2S                    MSSQL ODBC conn params added                                               IntegrateDev5	Ken Bradmon	2022-06-11

# MAGIC Run STRIP.FIELD transform on fields that might have Carriage Return, Line Feed, and  Tab
# MAGIC Pulling Facets Claim Override Data
# MAGIC Hash file (hf_paymt_reductn_allcol) cleared from the container - PaymtRductnPK
# MAGIC Writing Sequential File to /key
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
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/PaymtRductnPK
# COMMAND ----------

DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
LastRunDateTime = get_widget_value('LastRunDateTime','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')

jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)

extract_query = f"""
SELECT
payred.ACPR_REF_ID,
payred.ACPR_SUB_TYPE,
payred.ACPR_TX_YR,
payred.ACPR_TYPE,
payred.ACPR_CREATE_DT,
payred.LOBD_ID,
payred.ACPR_PAYEE_PR_ID,
payred.ACPR_PAYEE_CK,
payred.ACPR_PAYEE_TYPE,
payred.ACPR_AUTO_REDUC,
payred.ACPR_ORIG_AMT,
payred.ACPR_RECOV_AMT,
payred.ACPR_RECD_AMT,
payred.ACPR_WOFF_AMT,
payred.ACPR_NET_AMT,
payred.ACPR_STS,
payred.EXCD_ID,
payred.USUS_ID,
payred.PDDS_PREM_IND,
payred.ACPR_VARCHAR_MSG,
payred.SBFS_PLAN_YEAR_DT
FROM {LhoFacetsStgOwner}.CMC_CLOV_OVERPAY opay,
     {LhoFacetsStgOwner}.CMC_ACPR_PYMT_RED payred,
     tempdb..{DriverTable} tmp
WHERE tmp.CLM_ID = opay.CLCL_ID
  AND opay.ACPR_REF_ID = payred.ACPR_REF_ID
UNION
SELECT
payred.ACPR_REF_ID,
payred.ACPR_SUB_TYPE,
payred.ACPR_TX_YR,
payred.ACPR_TYPE,
payred.ACPR_CREATE_DT,
payred.LOBD_ID,
payred.ACPR_PAYEE_PR_ID,
payred.ACPR_PAYEE_CK,
payred.ACPR_PAYEE_TYPE,
payred.ACPR_AUTO_REDUC,
payred.ACPR_ORIG_AMT,
payred.ACPR_RECOV_AMT,
payred.ACPR_RECD_AMT,
payred.ACPR_WOFF_AMT,
payred.ACPR_NET_AMT,
payred.ACPR_STS,
payred.EXCD_ID,
payred.USUS_ID,
payred.PDDS_PREM_IND,
payred.ACPR_VARCHAR_MSG,
payred.SBFS_PLAN_YEAR_DT
FROM {LhoFacetsStgOwner}.CMC_ACPR_PYMT_RED payred
WHERE payred.ACPR_PAYEE_PR_ID LIKE 'MH%'
  AND payred.ACPR_CREATE_DT > '{LastRunDateTime}'
"""

df_CMC_ACPR_PYMT_RED = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_StripField = df_CMC_ACPR_PYMT_RED.select(
    strip_field(F.col("ACPR_REF_ID")).alias("ACPR_REF_ID"),
    strip_field(F.col("ACPR_SUB_TYPE")).alias("ACPR_SUB_TYPE"),
    strip_field(F.col("ACPR_TX_YR")).alias("ACPR_TX_YR"),
    strip_field(F.col("ACPR_TYPE")).alias("ACPR_TYPE"),
    current_date().alias("EXTRACTION_TIMESTATMP"),
    F.col("ACPR_CREATE_DT").alias("ACPR_CREATE_DT"),
    strip_field(F.col("LOBD_ID")).alias("LOBD_ID"),
    strip_field(F.col("ACPR_PAYEE_PR_ID")).alias("ACPR_PAYEE_PR_ID"),
    F.col("ACPR_PAYEE_CK").alias("ACPR_PAYEE_CK"),
    strip_field(F.col("ACPR_PAYEE_TYPE")).alias("ACPR_PAYEE_TYPE"),
    strip_field(F.col("ACPR_AUTO_REDUC")).alias("ACPR_AUTO_REDUC"),
    F.col("ACPR_ORIG_AMT").alias("ACPR_ORIG_AMT"),
    F.col("ACPR_RECOV_AMT").alias("ACPR_RECOV_AMT"),
    F.col("ACPR_RECD_AMT").alias("ACPR_RECD_AMT"),
    F.col("ACPR_WOFF_AMT").alias("ACPR_WOFF_AMT"),
    F.col("ACPR_NET_AMT").alias("ACPR_NET_AMT"),
    strip_field(F.col("ACPR_STS")).alias("ACPR_STS"),
    strip_field(F.col("EXCD_ID")).alias("EXCD_ID"),
    strip_field(F.col("USUS_ID")).alias("USUS_ID"),
    strip_field(F.col("PDDS_PREM_IND")).alias("PDDS_PREM_IND"),
    F.when(
        (F.length(strip_field(F.col("ACPR_VARCHAR_MSG"))) == 0)
        | (strip_field(F.col("ACPR_VARCHAR_MSG")) == ""),
        F.lit(" ")
    ).otherwise(strip_field(F.col("ACPR_VARCHAR_MSG"))).alias("ACPR_VARCHAR_MSG"),
    F.col("SBFS_PLAN_YEAR_DT").alias("SBFS_PLAN_YEAR_DT")
)

df_BusinessRules = (
    df_StripField
    .withColumn(
        "ReductionPayeeType",
        F.when(
            F.col("ACPR_PAYEE_TYPE").isNull() | (F.length(trim(F.col("ACPR_PAYEE_TYPE"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(trim(F.col("ACPR_PAYEE_TYPE"))))
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("EXTRACTION_TIMESTATMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(
            F.lit(SrcSysCd), F.lit(";"),
            trim(F.col("ACPR_REF_ID")), F.lit(";"),
            trim(F.col("ACPR_SUB_TYPE")), F.lit(";"),
            trim(F.col("ACPR_TX_YR")), F.lit(";"),
            trim(F.col("ACPR_TYPE"))
        ).alias("PRI_KEY_STRING"),
        F.lit(0).alias("PAYMT_RDUCTN_SK"),
        F.when(
            F.col("ACPR_REF_ID").isNull() | (F.length(trim(F.col("ACPR_REF_ID"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(trim(F.col("ACPR_REF_ID")))).alias("PAYMT_RDUCTN_REF_ID"),
        F.when(
            F.col("ACPR_SUB_TYPE").isNull() | (F.length(trim(F.col("ACPR_SUB_TYPE"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(trim(F.col("ACPR_SUB_TYPE")))).alias("PAYMT_RDUCTN_SUBTYP_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when(
            (F.col("ReductionPayeeType") == F.lit("P")),
            F.when(
                F.col("ACPR_PAYEE_PR_ID").isNull() | (F.length(trim(F.col("ACPR_PAYEE_PR_ID"))) == 0),
                F.lit("UNK")
            ).otherwise(F.upper(trim(F.col("ACPR_PAYEE_PR_ID"))))
        ).otherwise(
            F.when(
                F.col("ACPR_PAYEE_PR_ID").isNull() | (F.length(trim(F.col("ACPR_PAYEE_PR_ID"))) == 0),
                F.lit("NA")
            ).otherwise(F.upper(trim(F.col("ACPR_PAYEE_PR_ID"))))
        ).alias("PAYEE_PROVDER_ID"),
        F.when(
            trim(F.col("ACPR_TYPE")) == F.lit("MR"),
            F.when(
                F.col("USUS_ID").isNull() | (F.length(trim(F.col("USUS_ID"))) == 0),
                F.lit("UNK")
            ).otherwise(F.upper(trim(F.col("USUS_ID"))))
        ).otherwise(
            F.when(
                F.col("USUS_ID").isNull() | (F.length(trim(F.col("USUS_ID"))) == 0),
                F.lit("NA")
            ).otherwise(F.upper(trim(F.col("USUS_ID"))))
        ).alias("USER_ID"),
        F.when(
            trim(F.col("ACPR_TYPE")) == F.lit("MR"),
            F.when(
                trim(F.col("EXCD_ID")).isNull(),
                F.lit("UNK")
            ).otherwise(trim(F.col("EXCD_ID")))
        ).otherwise(
            F.when(
                F.col("EXCD_ID").isNull() | (F.length(trim(F.col("EXCD_ID"))) == 0),
                F.lit("NA")
            ).otherwise(trim(F.col("EXCD_ID")))
        ).alias("PAYMT_RDUCTN_EXCD_ID"),
        F.col("ReductionPayeeType").alias("PAYMT_RDUCTN_PAYE_TYP_CD"),
        F.when(
            F.col("PDDS_PREM_IND").isNull() | (F.length(trim(F.col("PDDS_PREM_IND"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(trim(F.col("PDDS_PREM_IND")))).alias("PAYMT_RDUCTN_PRM_TYP_CD"),
        F.when(
            F.col("ACPR_STS").isNull() | (F.length(trim(F.col("ACPR_STS"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(trim(F.col("ACPR_STS")))).alias("PAYMT_RDUCTN_STTUS_CD"),
        F.when(
            F.col("ACPR_TYPE").isNull() | (F.length(trim(F.col("ACPR_TYPE"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(trim(F.col("ACPR_TYPE")))).alias("PAYMT_RDUCTN_TYP_CD"),
        F.when(
            F.col("ACPR_AUTO_REDUC").isNull() | (F.length(trim(F.col("ACPR_AUTO_REDUC"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(trim(F.col("ACPR_AUTO_REDUC")))).alias("AUTO_PAYMT_RDUCTN_IN"),
        F.expr("FORMAT.DATE(ACPR_CREATE_DT, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD')").alias("CRT_DT"),
        trim(F.substring(F.col("SBFS_PLAN_YEAR_DT"), 1, 4)).alias("PLN_YR_DT"),
        F.col("ACPR_ORIG_AMT").alias("ORIG_RDUCTN_AMT"),
        F.lit(0.0).alias("PCA_OVERPD_NET_AMT"),
        F.col("ACPR_RECD_AMT").alias("RCVD_AMT"),
        F.col("ACPR_RECOV_AMT").alias("RCVRED_AMT"),
        F.col("ACPR_NET_AMT").alias("REMN_NET_AMT"),
        F.col("ACPR_WOFF_AMT").alias("WRT_OFF_AMT"),
        F.col("ACPR_VARCHAR_MSG").alias("RDUCTN_DESC"),
        F.when(
            F.col("ACPR_TX_YR").isNull() | (F.length(trim(F.col("ACPR_TX_YR"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(trim(F.col("ACPR_TX_YR")))).alias("TAX_YR")
    )
)

df_SnapshotAllColl = df_BusinessRules.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("PAYMT_RDUCTN_REF_ID").alias("PAYMT_RDUCTN_REF_ID"),
    F.col("PAYMT_RDUCTN_SUBTYP_CD").alias("PAYMT_RDUCTN_SUBTYP_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PAYEE_PROVDER_ID").alias("PAYEE_PROVDER_ID"),
    F.col("USER_ID").alias("USER_ID"),
    F.col("PAYMT_RDUCTN_EXCD_ID").alias("PAYMT_RDUCTN_EXCD_ID"),
    F.col("PAYMT_RDUCTN_PAYE_TYP_CD").alias("PAYMT_RDUCTN_PAYE_TYP_CD"),
    F.col("PAYMT_RDUCTN_PRM_TYP_CD").alias("PAYMT_RDUCTN_PRM_TYP_CD"),
    F.col("PAYMT_RDUCTN_STTUS_CD").alias("PAYMT_RDUCTN_STTUS_CD"),
    F.col("PAYMT_RDUCTN_TYP_CD").alias("PAYMT_RDUCTN_TYP_CD"),
    F.col("AUTO_PAYMT_RDUCTN_IN").alias("AUTO_PAYMT_RDUCTN_IN"),
    F.col("CRT_DT").alias("CRT_DT"),
    F.col("PLN_YR_DT").alias("PLN_YR_DT"),
    F.col("ORIG_RDUCTN_AMT").alias("ORIG_RDUCTN_AMT"),
    F.col("PCA_OVERPD_NET_AMT").alias("PCA_OVERPD_NET_AMT"),
    F.col("RCVD_AMT").alias("RCVD_AMT"),
    F.col("RCVRED_AMT").alias("RCVRED_AMT"),
    F.col("REMN_NET_AMT").alias("REMN_NET_AMT"),
    F.col("WRT_OFF_AMT").alias("WRT_OFF_AMT"),
    F.when(
        (F.col("RDUCTN_DESC") == F.lit("")),
        F.lit("NA")
    ).otherwise(F.col("RDUCTN_DESC")).alias("RDUCTN_DESC"),
    F.col("TAX_YR").alias("TAX_YR")
)

df_SnapshotTransform = df_BusinessRules.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("PAYMT_RDUCTN_REF_ID").alias("PAYMT_RDUCTN_REF_ID"),
    F.col("PAYMT_RDUCTN_SUBTYP_CD").alias("PAYMT_RDUCTN_SUBTYP_CD")
)

params = {
    "CurrRunCycle": CurrRunCycle,
    "CurrDate": CurrentDate,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "IDSOwner": IDSOwner
}

df_IdsClmPayReductnPkey = PaymtRductnPK(df_SnapshotAllColl, df_SnapshotTransform, params)

df_IdsClmPayReductnPkey_final = df_IdsClmPayReductnPkey.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("PAYMT_RDUCTN_SK"),
    F.col("PAYMT_RDUCTN_REF_ID"),
    rpad(F.col("PAYMT_RDUCTN_SUBTYP_CD"), 1, " ").alias("PAYMT_RDUCTN_SUBTYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(F.col("PAYEE_PROVDER_ID"), 12, " ").alias("PAYEE_PROVDER_ID"),
    F.col("USER_ID"),
    F.col("PAYMT_RDUCTN_EXCD_ID"),
    F.col("PAYMT_RDUCTN_PAYE_TYP_CD"),
    F.col("PAYMT_RDUCTN_PRM_TYP_CD"),
    F.col("PAYMT_RDUCTN_STTUS_CD"),
    F.col("PAYMT_RDUCTN_TYP_CD"),
    rpad(F.col("AUTO_PAYMT_RDUCTN_IN"), 1, " ").alias("AUTO_PAYMT_RDUCTN_IN"),
    rpad(F.col("CRT_DT"), 10, " ").alias("CRT_DT"),
    rpad(F.col("PLN_YR_DT"), 10, " ").alias("PLN_YR_DT"),
    F.col("ORIG_RDUCTN_AMT"),
    F.col("PCA_OVERPD_NET_AMT"),
    F.col("RCVD_AMT"),
    F.col("RCVRED_AMT"),
    F.col("REMN_NET_AMT"),
    F.col("WRT_OFF_AMT"),
    F.col("RDUCTN_DESC"),
    rpad(F.col("TAX_YR"), 4, " ").alias("TAX_YR")
)

write_files(
    df_IdsClmPayReductnPkey_final,
    f"{adls_path}/key/LhoFctsClmPayRductnExtr.LhoFctsClmPayRductn.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)