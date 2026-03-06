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
# MAGIC PROCESSING:
# MAGIC   Pulls data from CMC_ACPR_PYMT_RED  and looks up against the Hitlist file
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                       Date                 Project/Altiris #                  Change Description                                                              Development Project      Code Reviewer          Date Reviewed      
# MAGIC ==================================================================================================================================================================
# MAGIC Manasa Andru                            2020-07-06          US - 248795           Original Programming to histlist the missing data                     IntegrateDev2                 Jaideep Mankala       07/08/2020
# MAGIC Prabhu ES                                  2022-02-28          S2S Remediation    MSSQL connection parameters added                                  IntegrateDev5                  Kalyan Neelam           2022-06-10

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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/PaymtRductnPK
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"SELECT payred.ACPR_REF_ID, payred.ACPR_SUB_TYPE, payred.ACPR_TX_YR, payred.ACPR_TYPE, payred.ACPR_CREATE_DT, payred.LOBD_ID, payred.ACPR_PAYEE_PR_ID, payred.ACPR_PAYEE_CK, payred.ACPR_PAYEE_TYPE, payred.ACPR_AUTO_REDUC, payred.ACPR_ORIG_AMT, payred.ACPR_RECOV_AMT, payred.ACPR_RECD_AMT, payred.ACPR_WOFF_AMT, payred.ACPR_NET_AMT, payred.ACPR_STS, payred.EXCD_ID, payred.USUS_ID, payred.PDDS_PREM_IND, payred.ACPR_VARCHAR_MSG, payred.SBFS_PLAN_YEAR_DT FROM {FacetsOwner}.CMC_ACPR_PYMT_RED payred"
df_CMC_ACPR_PYMT_RED = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

schema_paymtRedHL = StructType([
    StructField("ACPR_REF_ID", StringType(), nullable=False),
    StructField("ACPR_SUB_TYPE", StringType(), nullable=False)
])
dfPaymtRedHL = (
    spark.read.format("csv")
    .schema(schema_paymtRedHL)
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", False)
    .load(f"{adls_path}/update/FctsClmPayRdctnHitlist.dat")
)

df_Transformer_31 = (
    df_CMC_ACPR_PYMT_RED
    .withColumn("ACPR_REF_ID_LKUP", trim(F.col("ACPR_REF_ID")))
    .withColumn("ACPR_SUB_TYPE_LKUP", trim(F.col("ACPR_SUB_TYPE")))
    .withColumn("ACPR_REF_ID", F.col("ACPR_REF_ID"))
    .withColumn("ACPR_SUB_TYPE", F.col("ACPR_SUB_TYPE"))
    .withColumn("ACPR_TX_YR", F.col("ACPR_TX_YR"))
    .withColumn("ACPR_TYPE", F.col("ACPR_TYPE"))
    .withColumn("ACPR_CREATE_DT", F.col("ACPR_CREATE_DT"))
    .withColumn("LOBD_ID", F.col("LOBD_ID"))
    .withColumn("ACPR_PAYEE_PR_ID", F.col("ACPR_PAYEE_PR_ID"))
    .withColumn("ACPR_PAYEE_CK", F.col("ACPR_PAYEE_CK"))
    .withColumn("ACPR_PAYEE_TYPE", F.col("ACPR_PAYEE_TYPE"))
    .withColumn("ACPR_AUTO_REDUC", F.col("ACPR_AUTO_REDUC"))
    .withColumn("ACPR_ORIG_AMT", F.col("ACPR_ORIG_AMT"))
    .withColumn("ACPR_RECOV_AMT", F.col("ACPR_RECOV_AMT"))
    .withColumn("ACPR_RECD_AMT", F.col("ACPR_RECD_AMT"))
    .withColumn("ACPR_WOFF_AMT", F.col("ACPR_WOFF_AMT"))
    .withColumn("ACPR_NET_AMT", F.col("ACPR_NET_AMT"))
    .withColumn("ACPR_STS", F.col("ACPR_STS"))
    .withColumn("EXCD_ID", F.col("EXCD_ID"))
    .withColumn("USUS_ID", F.col("USUS_ID"))
    .withColumn("PDDS_PREM_IND", F.col("PDDS_PREM_IND"))
    .withColumn("ACPR_VARCHAR_MSG", F.col("ACPR_VARCHAR_MSG"))
    .withColumn("SBFS_PLAN_YEAR_DT", F.col("SBFS_PLAN_YEAR_DT"))
    .select(
        "ACPR_REF_ID_LKUP",
        "ACPR_SUB_TYPE_LKUP",
        "ACPR_REF_ID",
        "ACPR_SUB_TYPE",
        "ACPR_TX_YR",
        "ACPR_TYPE",
        "ACPR_CREATE_DT",
        "LOBD_ID",
        "ACPR_PAYEE_PR_ID",
        "ACPR_PAYEE_CK",
        "ACPR_PAYEE_TYPE",
        "ACPR_AUTO_REDUC",
        "ACPR_ORIG_AMT",
        "ACPR_RECOV_AMT",
        "ACPR_RECD_AMT",
        "ACPR_WOFF_AMT",
        "ACPR_NET_AMT",
        "ACPR_STS",
        "EXCD_ID",
        "USUS_ID",
        "PDDS_PREM_IND",
        "ACPR_VARCHAR_MSG",
        "SBFS_PLAN_YEAR_DT"
    )
)

write_files(
    df_Transformer_31,
    f"{adls_path}/hf_paymt_red_hitlist_lkup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_hf_paymt_red = spark.read.parquet(f"{adls_path}/hf_paymt_red_hitlist_lkup.parquet")

df_StripField_Joined = (
    dfPaymtRedHL.alias("Hitlist")
    .join(
        df_hf_paymt_red.alias("Extract"),
        (
            (trim(F.col("Hitlist.ACPR_REF_ID")) == F.col("Extract.ACPR_REF_ID_LKUP")) &
            (trim(F.col("Hitlist.ACPR_SUB_TYPE")) == F.col("Extract.ACPR_SUB_TYPE_LKUP")) &
            (F.col("Hitlist.ACPR_REF_ID") == F.col("Extract.ACPR_REF_ID")) &
            (F.col("Hitlist.ACPR_SUB_TYPE") == F.col("Extract.ACPR_SUB_TYPE"))
        ),
        "left"
    )
)

df_StripField_Filtered = df_StripField_Joined.filter(
    (F.col("Extract.ACPR_REF_ID_LKUP").isNotNull()) &
    (F.col("Extract.ACPR_SUB_TYPE_LKUP").isNotNull())
)

df_Strip = (
    df_StripField_Filtered
    .withColumn("ACPR_REF_ID", F.regexp_replace(F.col("Extract.ACPR_REF_ID"), "[\r\n\t]", ""))
    .withColumn("ACPR_SUB_TYPE", F.regexp_replace(F.col("Extract.ACPR_SUB_TYPE"), "[\r\n\t]", ""))
    .withColumn("ACPR_TX_YR", F.regexp_replace(F.col("Extract.ACPR_TX_YR"), "[\r\n\t]", ""))
    .withColumn("ACPR_TYPE", F.regexp_replace(F.col("Extract.ACPR_TYPE"), "[\r\n\t]", ""))
    .withColumn("EXTRACTION_TIMESTATMP", current_date())
    .withColumn("ACPR_CREATE_DT", F.col("Extract.ACPR_CREATE_DT"))
    .withColumn("LOBD_ID", F.regexp_replace(F.col("Extract.LOBD_ID"), "[\r\n\t]", ""))
    .withColumn("ACPR_PAYEE_PR_ID", F.regexp_replace(F.col("Extract.ACPR_PAYEE_PR_ID"), "[\r\n\t]", ""))
    .withColumn("ACPR_PAYEE_CK", F.col("Extract.ACPR_PAYEE_CK"))
    .withColumn("ACPR_PAYEE_TYPE", F.regexp_replace(F.col("Extract.ACPR_PAYEE_TYPE"), "[\r\n\t]", ""))
    .withColumn("ACPR_AUTO_REDUC", F.regexp_replace(F.col("Extract.ACPR_AUTO_REDUC"), "[\r\n\t]", ""))
    .withColumn("ACPR_ORIG_AMT", F.col("Extract.ACPR_ORIG_AMT"))
    .withColumn("ACPR_RECOV_AMT", F.col("Extract.ACPR_RECOV_AMT"))
    .withColumn("ACPR_RECD_AMT", F.col("Extract.ACPR_RECD_AMT"))
    .withColumn("ACPR_WOFF_AMT", F.col("Extract.ACPR_WOFF_AMT"))
    .withColumn("ACPR_NET_AMT", F.col("Extract.ACPR_NET_AMT"))
    .withColumn("ACPR_STS", F.regexp_replace(F.col("Extract.ACPR_STS"), "[\r\n\t]", ""))
    .withColumn("EXCD_ID", F.regexp_replace(F.col("Extract.EXCD_ID"), "[\r\n\t]", ""))
    .withColumn("USUS_ID", F.regexp_replace(F.col("Extract.USUS_ID"), "[\r\n\t]", ""))
    .withColumn("PDDS_PREM_IND", F.regexp_replace(F.col("Extract.PDDS_PREM_IND"), "[\r\n\t]", ""))
    .withColumn(
        "ACPR_VARCHAR_MSG",
        F.when(
            (F.length(F.regexp_replace(F.col("Extract.ACPR_VARCHAR_MSG"), "[\r\n\t]", "")) == 0) |
            (F.regexp_replace(F.col("Extract.ACPR_VARCHAR_MSG"), "[\r\n\t]", "") == ""),
            F.lit(" ")
        ).otherwise(F.regexp_replace(F.col("Extract.ACPR_VARCHAR_MSG"), "[\r\n\t]", ""))
    )
    .withColumn("SBFS_PLAN_YEAR_DT", F.col("Extract.SBFS_PLAN_YEAR_DT"))
    .select(
        "ACPR_REF_ID",
        "ACPR_SUB_TYPE",
        "ACPR_TX_YR",
        "ACPR_TYPE",
        "EXTRACTION_TIMESTATMP",
        "ACPR_CREATE_DT",
        "LOBD_ID",
        "ACPR_PAYEE_PR_ID",
        "ACPR_PAYEE_CK",
        "ACPR_PAYEE_TYPE",
        "ACPR_AUTO_REDUC",
        "ACPR_ORIG_AMT",
        "ACPR_RECOV_AMT",
        "ACPR_RECD_AMT",
        "ACPR_WOFF_AMT",
        "ACPR_NET_AMT",
        "ACPR_STS",
        "EXCD_ID",
        "USUS_ID",
        "PDDS_PREM_IND",
        "ACPR_VARCHAR_MSG",
        "SBFS_PLAN_YEAR_DT"
    )
)

df_BusinessRulesTemp = (
    df_Strip
    .withColumn(
        "ReductionPayeeType",
        F.when(
            F.col("ACPR_PAYEE_TYPE").isNull() | (F.length(F.trim(F.col("ACPR_PAYEE_TYPE"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(F.trim(F.col("ACPR_PAYEE_TYPE"))))
    )
)

df_BusinessRules = (
    df_BusinessRulesTemp
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("EXTRACTION_TIMESTATMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit("FACETS").alias("SRC_SYS_CD"),
        F.concat(
            F.lit("FACETS"), F.lit(";"),
            F.upper(F.trim(F.col("ACPR_REF_ID"))), F.lit(";"),
            F.upper(F.trim(F.col("ACPR_SUB_TYPE"))), F.lit(";"),
            F.upper(F.trim(F.col("ACPR_TX_YR"))), F.lit(";"),
            F.upper(F.trim(F.col("ACPR_TYPE")))
        ).alias("PRI_KEY_STRING"),
        F.lit(0).alias("PAYMT_RDUCTN_SK"),
        F.when(
            F.col("ACPR_REF_ID").isNull() | (F.length(F.trim(F.col("ACPR_REF_ID"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(F.trim(F.col("ACPR_REF_ID")))).alias("PAYMT_RDUCTN_REF_ID"),
        F.when(
            F.col("ACPR_SUB_TYPE").isNull() | (F.length(F.trim(F.col("ACPR_SUB_TYPE"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(F.trim(F.col("ACPR_SUB_TYPE")))).alias("PAYMT_RDUCTN_SUBTYP_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when(
            F.trim(F.col("ReductionPayeeType")) == F.lit("P"),
            F.when(
                F.col("ACPR_PAYEE_PR_ID").isNull() | (F.length(F.trim(F.col("ACPR_PAYEE_PR_ID"))) == 0),
                F.lit("UNK")
            ).otherwise(F.upper(F.trim(F.col("ACPR_PAYEE_PR_ID"))))
        ).otherwise(
            F.when(
                F.col("ACPR_PAYEE_PR_ID").isNull() | (F.length(F.trim(F.col("ACPR_PAYEE_PR_ID"))) == 0),
                F.lit("NA")
            ).otherwise(F.upper(F.trim(F.col("ACPR_PAYEE_PR_ID"))))
        ).alias("PAYEE_PROVDER_ID"),
        F.when(
            F.trim(F.col("ACPR_TYPE")) == F.lit("MR"),
            F.when(
                F.col("USUS_ID").isNull() | (F.length(F.trim(F.col("USUS_ID"))) == 0),
                F.lit("UNK")
            ).otherwise(F.upper(F.trim(F.col("USUS_ID"))))
        ).otherwise(
            F.when(
                F.col("USUS_ID").isNull() | (F.length(F.trim(F.col("USUS_ID"))) == 0),
                F.lit("NA")
            ).otherwise(F.upper(F.trim(F.col("USUS_ID"))))
        ).alias("USER_ID"),
        F.when(
            F.trim(F.col("ACPR_TYPE")) == F.lit("MR"),
            F.when(
                F.col("EXCD_ID").isNull() | (F.trim(F.col("EXCD_ID")) == ""),
                F.lit("UNK")
            ).otherwise(F.trim(F.col("EXCD_ID")))
        ).otherwise(
            F.when(
                F.col("EXCD_ID").isNull() | (F.length(F.trim(F.col("EXCD_ID"))) == 0),
                F.lit("NA")
            ).otherwise(F.trim(F.col("EXCD_ID")))
        ).alias("PAYMT_RDUCTN_EXCD_ID"),
        F.col("ReductionPayeeType").alias("PAYMT_RDUCTN_PAYE_TYP_CD"),
        F.when(
            F.col("PDDS_PREM_IND").isNull() | (F.length(F.trim(F.col("PDDS_PREM_IND"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(F.trim(F.col("PDDS_PREM_IND")))).alias("PAYMT_RDUCTN_PRM_TYP_CD"),
        F.when(
            F.col("ACPR_STS").isNull() | (F.length(F.trim(F.col("ACPR_STS"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(F.trim(F.col("ACPR_STS")))).alias("PAYMT_RDUCTN_STTUS_CD"),
        F.when(
            F.col("ACPR_TYPE").isNull() | (F.length(F.trim(F.col("ACPR_TYPE"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(F.trim(F.col("ACPR_TYPE")))).alias("PAYMT_RDUCTN_TYP_CD"),
        F.when(
            F.col("ACPR_AUTO_REDUC").isNull() | (F.length(F.trim(F.col("ACPR_AUTO_REDUC"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(F.trim(F.col("ACPR_AUTO_REDUC")))).alias("AUTO_PAYMT_RDUCTN_IN"),
        F.expr("FORMAT.DATE(ACPR_CREATE_DT, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD')").alias("CRT_DT"),
        F.substring(F.col("SBFS_PLAN_YEAR_DT"), 1, 4).alias("PLN_YR_DT"),
        F.col("ACPR_ORIG_AMT").alias("ORIG_RDUCTN_AMT"),
        F.lit(0.0).alias("PCA_OVERPD_NET_AMT"),
        F.col("ACPR_RECD_AMT").alias("RCVD_AMT"),
        F.col("ACPR_RECOV_AMT").alias("RCVRED_AMT"),
        F.col("ACPR_NET_AMT").alias("REMN_NET_AMT"),
        F.col("ACPR_WOFF_AMT").alias("WRT_OFF_AMT"),
        F.col("ACPR_VARCHAR_MSG").alias("RDUCTN_DESC"),
        F.when(
            F.col("ACPR_TX_YR").isNull() | (F.length(F.trim(F.col("ACPR_TX_YR"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(F.trim(F.col("ACPR_TX_YR")))).alias("TAX_YR")
    )
)

dfSnapshotAllColl = (
    df_BusinessRules
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .select(
        F.col("SRC_SYS_CD_SK"),
        F.col("PAYMT_RDUCTN_REF_ID"),
        F.col("PAYMT_RDUCTN_SUBTYP_CD"),
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "PAYMT_RDUCTN_SK",
        "PAYMT_RDUCTN_REF_ID",
        "PAYMT_RDUCTN_SUBTYP_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PAYEE_PROVDER_ID",
        "USER_ID",
        "PAYMT_RDUCTN_EXCD_ID",
        "PAYMT_RDUCTN_PAYE_TYP_CD",
        "PAYMT_RDUCTN_PRM_TYP_CD",
        "PAYMT_RDUCTN_STTUS_CD",
        "PAYMT_RDUCTN_TYP_CD",
        "AUTO_PAYMT_RDUCTN_IN",
        "CRT_DT",
        "PLN_YR_DT",
        "ORIG_RDUCTN_AMT",
        "PCA_OVERPD_NET_AMT",
        "RCVD_AMT",
        "RCVRED_AMT",
        "REMN_NET_AMT",
        "WRT_OFF_AMT",
        "RDUCTN_DESC",
        "TAX_YR"
    )
)

dfSnapshotTransform = (
    df_BusinessRules
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .select(
        "SRC_SYS_CD_SK",
        "PAYMT_RDUCTN_REF_ID",
        "PAYMT_RDUCTN_SUBTYP_CD"
    )
)

params = {
    "CurrRunCycle": CurrRunCycle,
    "CurrDate": CurrentDate,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "IDSOwner": IDSOwner
}
dfContainerOutput = PaymtRductnPK(dfSnapshotAllColl, dfSnapshotTransform, params)

final_df = (
    dfContainerOutput
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "PAYMT_RDUCTN_SK",
        "PAYMT_RDUCTN_REF_ID",
        "PAYMT_RDUCTN_SUBTYP_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PAYEE_PROVDER_ID",
        "USER_ID",
        "PAYMT_RDUCTN_EXCD_ID",
        "PAYMT_RDUCTN_PAYE_TYP_CD",
        "PAYMT_RDUCTN_PRM_TYP_CD",
        "PAYMT_RDUCTN_STTUS_CD",
        "PAYMT_RDUCTN_TYP_CD",
        "AUTO_PAYMT_RDUCTN_IN",
        "CRT_DT",
        "PLN_YR_DT",
        "ORIG_RDUCTN_AMT",
        "PCA_OVERPD_NET_AMT",
        "RCVD_AMT",
        "RCVRED_AMT",
        "REMN_NET_AMT",
        "WRT_OFF_AMT",
        "RDUCTN_DESC",
        "TAX_YR"
    )
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("PAYMT_RDUCTN_SUBTYP_CD", F.rpad(F.col("PAYMT_RDUCTN_SUBTYP_CD"), 1, " "))
    .withColumn("PAYEE_PROVDER_ID", F.rpad(F.col("PAYEE_PROVDER_ID"), 12, " "))
    .withColumn("AUTO_PAYMT_RDUCTN_IN", F.rpad(F.col("AUTO_PAYMT_RDUCTN_IN"), 1, " "))
    .withColumn("CRT_DT", F.rpad(F.col("CRT_DT"), 10, " "))
    .withColumn("PLN_YR_DT", F.rpad(F.col("PLN_YR_DT"), 10, " "))
    .withColumn("TAX_YR", F.rpad(F.col("TAX_YR"), 4, " "))
)

write_files(
    final_df,
    f"{adls_path}/key/FctsClmPayRductnExtr.FctsClmPayRductn.dat.{RunID}",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)