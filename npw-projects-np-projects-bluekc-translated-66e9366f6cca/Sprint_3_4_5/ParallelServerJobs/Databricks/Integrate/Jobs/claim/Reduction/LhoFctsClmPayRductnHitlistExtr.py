# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004, 2005, 2006, 2007, 2008, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  LhoFctsClmPayRductnHitlistExtr
# MAGIC CALLED BY:  LhoFctsClmExtr1Seq 
# MAGIC 
# MAGIC PROCESSING:
# MAGIC   Pulls data from CMC_ACPR_PYMT_RED  and looks up against the Hitlist file
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                       Date                 Project/Altiris #                  Change Description                                                              Development Project      Code Reviewer          Date Reviewed      
# MAGIC ==================================================================================================================================================================
# MAGIC Manasa Andru                            2020-07-06          US - 248795           Original Programming to histlist the missing data                     IntegrateDev2                 Jaideep Mankala       07/08/2020
# MAGIC 
# MAGIC Reddy Sanam                       2020-08-17          263448                        Copied from original job named with LhoFcts prefix                IntegrateDev2
# MAGIC                                                                                                            Removed Facets Env variables and added LhoFctsStg
# MAGIC                                                                                                            Env variables
# MAGIC                                                                                                            modified source and target sequential file name to reflect
# MAGIC                                                                                                           LhoFcts
# MAGIC                                                                                                           Modified source query to reflect LhoFacetsStg
# MAGIC                                                                                                           Changed Hard Coded value "FACETS" to SrcSysCd
# MAGIC                                                                                                           in BusinessRules transformer
# MAGIC Prabhu ES                              2022-03-29           S2S                      MSSQL ODBC conn params added                                           IntegrateDev5		Ken Bradmon	2022-06-11

# MAGIC Run STRIP.FIELD transform on fields that might have Carriage Return, Line Feed, and  Tab
# MAGIC Hash file (hf_paymt_reductn_allcol) cleared from the container - PaymtRductnPK
# MAGIC Hitlist with ACPR_REF_ID and ACPR_SUB_TYPE 
# MAGIC fields
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
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/PaymtRductnPK
# COMMAND ----------

# Retrieve parameters
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
LastRunDateTime = get_widget_value('LastRunDateTime','')
LhoFacetsStgDSN = get_widget_value('LhoFacetsStgDSN','')

#--------------------------------------------------------------------------------
# PaymtRedHL (CSeqFileStage) - Read from update/LhoFctsClmPayRdctnHitlist.dat
schema_PaymtRedHL = StructType([
    StructField("ACPR_REF_ID", StringType(), True),
    StructField("ACPR_SUB_TYPE", StringType(), True)
])
df_PaymtRedHL = (
    spark.read
    .option("header", "false")
    .option("quote", '"')
    .schema(schema_PaymtRedHL)
    .csv(f"{adls_path}/update/LhoFctsClmPayRdctnHitlist.dat")
)

#--------------------------------------------------------------------------------
# CMC_ACPR_PYMT_RED (ODBCConnector) - Read via JDBC
jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)
extract_query = (
    f"SELECT payred.ACPR_REF_ID, payred.ACPR_SUB_TYPE, payred.ACPR_TX_YR, "
    f"payred.ACPR_TYPE, payred.ACPR_CREATE_DT, payred.LOBD_ID, ACPR_PAYEE_PR_ID, "
    f"payred.ACPR_PAYEE_CK, payred.ACPR_PAYEE_TYPE, payred.ACPR_AUTO_REDUC, "
    f"payred.ACPR_ORIG_AMT, payred.ACPR_RECOV_AMT, payred.ACPR_RECD_AMT, "
    f"payred.ACPR_WOFF_AMT, payred.ACPR_NET_AMT, ACPR_STS, payred.EXCD_ID, USUS_ID, "
    f"payred.PDDS_PREM_IND, payred.ACPR_VARCHAR_MSG, payred.SBFS_PLAN_YEAR_DT "
    f"FROM {LhoFacetsStgOwner}.CMC_ACPR_PYMT_RED payred"
)
df_CMC_ACPR_PYMT_RED = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

#--------------------------------------------------------------------------------
# Transformer_31 (CTransformerStage) - builds df_Transformer_31
df_Transformer_31 = df_CMC_ACPR_PYMT_RED.select(
    F.trim(F.col("ACPR_REF_ID")).alias("ACPR_REF_ID_LKUP"),
    F.trim(F.col("ACPR_SUB_TYPE")).alias("ACPR_SUB_TYPE_LKUP"),
    F.col("ACPR_REF_ID").alias("ACPR_REF_ID"),
    F.col("ACPR_SUB_TYPE").alias("ACPR_SUB_TYPE"),
    F.col("ACPR_TX_YR").alias("ACPR_TX_YR"),
    F.col("ACPR_TYPE").alias("ACPR_TYPE"),
    F.col("ACPR_CREATE_DT").alias("ACPR_CREATE_DT"),
    F.col("LOBD_ID").alias("LOBD_ID"),
    F.col("ACPR_PAYEE_PR_ID").alias("ACPR_PAYEE_PR_ID"),
    F.col("ACPR_PAYEE_CK").alias("ACPR_PAYEE_CK"),
    F.col("ACPR_PAYEE_TYPE").alias("ACPR_PAYEE_TYPE"),
    F.col("ACPR_AUTO_REDUC").alias("ACPR_AUTO_REDUC"),
    F.col("ACPR_ORIG_AMT").alias("ACPR_ORIG_AMT"),
    F.col("ACPR_RECOV_AMT").alias("ACPR_RECOV_AMT"),
    F.col("ACPR_RECD_AMT").alias("ACPR_RECD_AMT"),
    F.col("ACPR_WOFF_AMT").alias("ACPR_WOFF_AMT"),
    F.col("ACPR_NET_AMT").alias("ACPR_NET_AMT"),
    F.col("ACPR_STS").alias("ACPR_STS"),
    F.col("EXCD_ID").alias("EXCD_ID"),
    F.col("USUS_ID").alias("USUS_ID"),
    F.col("PDDS_PREM_IND").alias("PDDS_PREM_IND"),
    F.col("ACPR_VARCHAR_MSG").alias("ACPR_VARCHAR_MSG"),
    F.col("SBFS_PLAN_YEAR_DT").alias("SBFS_PLAN_YEAR_DT")
)

#--------------------------------------------------------------------------------
# hf_paymt_red (CHashedFileStage) - Scenario A (intermediate hashed file)
# Deduplicate df_Transformer_31 on key columns ACPR_REF_ID_LKUP, ACPR_SUB_TYPE_LKUP
df_hf_paymt_red = dedup_sort(
    df_Transformer_31,
    ["ACPR_REF_ID_LKUP", "ACPR_SUB_TYPE_LKUP"],
    [("ACPR_REF_ID_LKUP", "A"), ("ACPR_SUB_TYPE_LKUP", "A")]
)

#--------------------------------------------------------------------------------
# StripField (CTransformerStage) - reference link from df_hf_paymt_red, primary link from df_PaymtRedHL
df_StripField_pre = (
    df_PaymtRedHL.alias("Hitlist")
    .join(
        df_hf_paymt_red.alias("Extract"),
        (
            (F.trim(F.col("Hitlist.ACPR_REF_ID")) == F.col("Extract.ACPR_REF_ID_LKUP")) &
            (F.trim(F.col("Hitlist.ACPR_SUB_TYPE")) == F.col("Extract.ACPR_SUB_TYPE_LKUP")) &
            (F.col("Hitlist.ACPR_REF_ID") == F.col("Extract.ACPR_REF_ID")) &
            (F.col("Hitlist.ACPR_SUB_TYPE") == F.col("Extract.ACPR_SUB_TYPE"))
        ),
        "left"
    )
    .filter(
        F.col("Extract.ACPR_REF_ID_LKUP").isNotNull() &
        F.col("Extract.ACPR_SUB_TYPE_LKUP").isNotNull()
    )
)

df_StripField = df_StripField_pre.select(
    strip_field(F.col("Extract.ACPR_REF_ID")).alias("ACPR_REF_ID"),
    strip_field(F.col("Extract.ACPR_SUB_TYPE")).alias("ACPR_SUB_TYPE"),
    strip_field(F.col("Extract.ACPR_TX_YR")).alias("ACPR_TX_YR"),
    strip_field(F.col("Extract.ACPR_TYPE")).alias("ACPR_TYPE"),
    F.expr("current_date()").alias("EXTRACTION_TIMESTATMP"),
    F.col("Extract.ACPR_CREATE_DT").alias("ACPR_CREATE_DT"),
    strip_field(F.col("Extract.LOBD_ID")).alias("LOBD_ID"),
    strip_field(F.col("Extract.ACPR_PAYEE_PR_ID")).alias("ACPR_PAYEE_PR_ID"),
    F.col("Extract.ACPR_PAYEE_CK").alias("ACPR_PAYEE_CK"),
    strip_field(F.col("Extract.ACPR_PAYEE_TYPE")).alias("ACPR_PAYEE_TYPE"),
    strip_field(F.col("Extract.ACPR_AUTO_REDUC")).alias("ACPR_AUTO_REDUC"),
    F.col("Extract.ACPR_ORIG_AMT").alias("ACPR_ORIG_AMT"),
    F.col("Extract.ACPR_RECOV_AMT").alias("ACPR_RECOV_AMT"),
    F.col("Extract.ACPR_RECD_AMT").alias("ACPR_RECD_AMT"),
    F.col("Extract.ACPR_WOFF_AMT").alias("ACPR_WOFF_AMT"),
    F.col("Extract.ACPR_NET_AMT").alias("ACPR_NET_AMT"),
    strip_field(F.col("Extract.ACPR_STS")).alias("ACPR_STS"),
    strip_field(F.col("Extract.EXCD_ID")).alias("EXCD_ID"),
    strip_field(F.col("Extract.USUS_ID")).alias("USUS_ID"),
    strip_field(F.col("Extract.PDDS_PREM_IND")).alias("PDDS_PREM_IND"),
    F.when(
        (F.length(strip_field(F.col("Extract.ACPR_VARCHAR_MSG"))) == 0) |
        (strip_field(F.col("Extract.ACPR_VARCHAR_MSG")) == ''),
        F.lit(' ')
    ).otherwise(strip_field(F.col("Extract.ACPR_VARCHAR_MSG"))).alias("ACPR_VARCHAR_MSG"),
    F.col("Extract.SBFS_PLAN_YEAR_DT").alias("SBFS_PLAN_YEAR_DT")
)

#--------------------------------------------------------------------------------
# BusinessRules (CTransformerStage)
df_BusinessRules_withvars = df_StripField.withColumn(
    "ReductionPayeeType",
    F.when(
        (F.col("ACPR_PAYEE_TYPE").isNull()) | (F.length(F.trim(F.col("ACPR_PAYEE_TYPE"))) == 0),
        F.lit("UNK")
    ).otherwise(F.upper(F.trim(F.col("ACPR_PAYEE_TYPE"))))
)

df_BusinessRules_out = df_BusinessRules_withvars.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("EXTRACTION_TIMESTATMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),  # from job param at runtime
    F.concat(
        F.col("SrcSysCd"), F.lit(";"),
        F.trim(F.col("ACPR_REF_ID")), F.lit(";"),
        F.trim(F.col("ACPR_SUB_TYPE")), F.lit(";"),
        F.trim(F.col("ACPR_TX_YR")), F.lit(";"),
        F.trim(F.col("ACPR_TYPE"))
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("PAYMT_RDUCTN_SK"),
    F.when(
        (F.col("ACPR_REF_ID").isNull()) | (F.length(F.trim(F.col("ACPR_REF_ID"))) == 0),
        F.lit("UNK")
    ).otherwise(F.upper(F.trim(F.col("ACPR_REF_ID")))).alias("PAYMT_RDUCTN_REF_ID"),
    F.when(
        (F.col("ACPR_SUB_TYPE").isNull()) | (F.length(F.trim(F.col("ACPR_SUB_TYPE"))) == 0),
        F.lit("UNK")
    ).otherwise(F.upper(F.trim(F.col("ACPR_SUB_TYPE")))).alias("PAYMT_RDUCTN_SUBTYP_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        F.trim(F.col("ReductionPayeeType")) == F.lit("P"),
        F.when(
            (F.col("ACPR_PAYEE_PR_ID").isNull()) | (F.length(F.trim(F.col("ACPR_PAYEE_PR_ID"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(F.trim(F.col("ACPR_PAYEE_PR_ID"))))
    ).otherwise(
        F.when(
            (F.col("ACPR_PAYEE_PR_ID").isNull()) | (F.length(F.trim(F.col("ACPR_PAYEE_PR_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("ACPR_PAYEE_PR_ID"))))
    ).alias("PAYEE_PROVDER_ID"),
    F.when(
        F.trim(F.col("ACPR_TYPE")) == F.lit("MR"),
        F.when(
            (F.col("USUS_ID").isNull()) | (F.length(F.trim(F.col("USUS_ID"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(F.trim(F.col("USUS_ID"))))
    ).otherwise(
        F.when(
            (F.col("USUS_ID").isNull()) | (F.length(F.trim(F.col("USUS_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("USUS_ID"))))
    ).alias("USER_ID"),
    F.when(
        F.trim(F.col("ACPR_TYPE")) == F.lit("MR"),
        F.when(
            F.trim(F.col("EXCD_ID")).isNull(),
            F.lit("UNK")
        ).otherwise(F.trim(F.col("EXCD_ID")))
    ).otherwise(
        F.when(
            (F.col("EXCD_ID").isNull()) | (F.length(F.trim(F.col("EXCD_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.trim(F.col("EXCD_ID")))
    ).alias("PAYMT_RDUCTN_EXCD_ID"),
    F.col("ReductionPayeeType").alias("PAYMT_RDUCTN_PAYE_TYP_CD"),
    F.when(
        (F.col("PDDS_PREM_IND").isNull()) | (F.length(F.trim(F.col("PDDS_PREM_IND"))) == 0),
        F.lit("UNK")
    ).otherwise(F.upper(F.trim(F.col("PDDS_PREM_IND")))).alias("PAYMT_RDUCTN_PRM_TYP_CD"),
    F.when(
        (F.col("ACPR_STS").isNull()) | (F.length(F.trim(F.col("ACPR_STS"))) == 0),
        F.lit("UNK")
    ).otherwise(F.upper(F.trim(F.col("ACPR_STS")))).alias("PAYMT_RDUCTN_STTUS_CD"),
    F.when(
        (F.col("ACPR_TYPE").isNull()) | (F.length(F.trim(F.col("ACPR_TYPE"))) == 0),
        F.lit("UNK")
    ).otherwise(F.upper(F.trim(F.col("ACPR_TYPE")))).alias("PAYMT_RDUCTN_TYP_CD"),
    F.when(
        (F.col("ACPR_AUTO_REDUC").isNull()) | (F.length(F.trim(F.col("ACPR_AUTO_REDUC"))) == 0),
        F.lit("UNK")
    ).otherwise(F.upper(F.trim(F.col("ACPR_AUTO_REDUC")))).alias("AUTO_PAYMT_RDUCTN_IN"),
    F.date_format(F.col("ACPR_CREATE_DT"), "yyyy-MM-dd").alias("CRT_DT"),
    F.substring(F.col("SBFS_PLAN_YEAR_DT"), 1, 4).alias("PLN_YR_DT"),
    F.col("ACPR_ORIG_AMT").alias("ORIG_RDUCTN_AMT"),
    F.lit(0.0).alias("PCA_OVERPD_NET_AMT"),
    F.col("ACPR_RECD_AMT").alias("RCVD_AMT"),
    F.col("ACPR_RECOV_AMT").alias("RCVRED_AMT"),
    F.col("ACPR_NET_AMT").alias("REMN_NET_AMT"),
    F.col("ACPR_WOFF_AMT").alias("WRT_OFF_AMT"),
    F.col("ACPR_VARCHAR_MSG").alias("RDUCTN_DESC"),
    F.when(
        (F.col("ACPR_TX_YR").isNull()) | (F.length(F.trim(F.col("ACPR_TX_YR"))) == 0),
        F.lit("UNK")
    ).otherwise(F.upper(F.trim(F.col("ACPR_TX_YR")))).alias("TAX_YR")
)

#--------------------------------------------------------------------------------
# Snapshot (CTransformerStage) with two output links: AllColl and Transform
df_Snapshot_AllColl = df_BusinessRules_out.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),                # from job param
    F.col("PAYMT_RDUCTN_REF_ID").alias("PAYMT_RDUCTN_REF_ID"),
    F.col("PAYMT_RDUCTN_SUBTYP_CD").alias("PAYMT_RDUCTN_SUBTYP_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PAYEE_PROVDER_ID"),
    F.col("USER_ID"),
    F.col("PAYMT_RDUCTN_EXCD_ID"),
    F.col("PAYMT_RDUCTN_PAYE_TYP_CD"),
    F.col("PAYMT_RDUCTN_PRM_TYP_CD"),
    F.col("PAYMT_RDUCTN_STTUS_CD"),
    F.col("PAYMT_RDUCTN_TYP_CD"),
    F.col("AUTO_PAYMT_RDUCTN_IN"),
    F.col("CRT_DT"),
    F.col("PLN_YR_DT"),
    F.col("ORIG_RDUCTN_AMT"),
    F.col("PCA_OVERPD_NET_AMT"),
    F.col("RCVD_AMT"),
    F.col("RCVRED_AMT"),
    F.col("REMN_NET_AMT"),
    F.col("WRT_OFF_AMT"),
    F.col("RDUCTN_DESC"),
    F.col("TAX_YR")
)

df_Snapshot_Transform = df_BusinessRules_out.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("PAYMT_RDUCTN_REF_ID").alias("PAYMT_RDUCTN_REF_ID"),
    F.col("PAYMT_RDUCTN_SUBTYP_CD").alias("PAYMT_RDUCTN_SUBTYP_CD")
)

#--------------------------------------------------------------------------------
# PaymtRductnPK (CContainerStage) - Shared Container
container_params = {
    "CurrRunCycle": CurrRunCycle,
    "CurrDate": CurrentDate,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID
}
df_IdsClmPayReductnPkey = PaymtRductnPK(df_Snapshot_AllColl, df_Snapshot_Transform, container_params)

#--------------------------------------------------------------------------------
# IdsClmPayReductnPkey (CSeqFileStage) - write final file
# Preserve column order and apply rpad for char/varchar in final output
df_final = df_IdsClmPayReductnPkey.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("PAYMT_RDUCTN_SK"),
    F.col("PAYMT_RDUCTN_REF_ID"),
    F.rpad(F.col("PAYMT_RDUCTN_SUBTYP_CD"), 1, " ").alias("PAYMT_RDUCTN_SUBTYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("PAYEE_PROVDER_ID"), 12, " ").alias("PAYEE_PROVDER_ID"),
    F.col("USER_ID"),
    F.col("PAYMT_RDUCTN_EXCD_ID"),
    F.col("PAYMT_RDUCTN_PAYE_TYP_CD"),
    F.col("PAYMT_RDUCTN_PRM_TYP_CD"),
    F.col("PAYMT_RDUCTN_STTUS_CD"),
    F.col("PAYMT_RDUCTN_TYP_CD"),
    F.rpad(F.col("AUTO_PAYMT_RDUCTN_IN"), 1, " ").alias("AUTO_PAYMT_RDUCTN_IN"),
    F.rpad(F.col("CRT_DT"), 10, " ").alias("CRT_DT"),
    F.rpad(F.col("PLN_YR_DT"), 10, " ").alias("PLN_YR_DT"),
    F.col("ORIG_RDUCTN_AMT"),
    F.col("PCA_OVERPD_NET_AMT"),
    F.col("RCVD_AMT"),
    F.col("RCVRED_AMT"),
    F.col("REMN_NET_AMT"),
    F.col("WRT_OFF_AMT"),
    F.col("RDUCTN_DESC"),
    F.rpad(F.col("TAX_YR"), 4, " ").alias("TAX_YR")
)

write_files(
    df_final,
    f"{adls_path}/key/LhoFctsClmPayRductnExtr.LhoFctsClmPayRductn.dat.{RunID}",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)