# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004, 2005, 2006, 2007, 2008, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  LhoFctsClmPayRductnActvtyHitlistExtr
# MAGIC CALLED BY:  LhoFctsClmExtr1Seq
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING: Pulls data from CMC_ACRH_RED to hitlist missing data
# MAGIC   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC =====================================================================================================================================================
# MAGIC Developer                               Date                 Project/Altiris #                                         Change Description                                         Development Project      Code Reviewer          Date Reviewed       
# MAGIC =====================================================================================================================================================
# MAGIC Manasa Andru                    2020-07-06          US - 248795               Original Programming to histlist the missing data                     IntegrateDev2              Jaideep Mankala       07/08/2020
# MAGIC 
# MAGIC 
# MAGIC Reddy Sanam                    2020-08-17          263449                        Copied from original job named with LhoFcts prefix                IntegrateDev2
# MAGIC                                                                                                            Removed Facets Env variables and added LhoFctsStg
# MAGIC                                                                                                            Env variables
# MAGIC                                                                                                            modified source and target sequential file name to reflect
# MAGIC                                                                                                           LhoFcts
# MAGIC                                                                                                           Modified source query to reflect LhoFacetsStg
# MAGIC                                                                                                           Changed Hard Coded value "FACETS" to SrcSysCd
# MAGIC                                                                                                           in BusinessRules transformer
# MAGIC 
# MAGIC Prabhu ES                         2022-03-29            S2S                           MSSQL ODBC conn params added                                       IntegrateDev5	Ken Bradmon	2022-06-11

# MAGIC Hitlist with ACPR_REF_ID, ACPR_SUB_TYPE and ARCH_SEQ_NO fields
# MAGIC Run STRIP.FIELD transform on fields that might have Carriage Return, Line Feed, and  Tab
# MAGIC Hash file (hf_paymt_reductn_actvty_allcol) cleared from the container - PaymtRductnActvtyPK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit, when, length, upper, concat, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../shared_containers/PrimaryKey/PaymtRductnActvtyPK

CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
LastRunDateTime = get_widget_value('LastRunDateTime','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
lhofacetsstgowner_secret_name = get_widget_value('lhofacetsstgowner_secret_name','')
LhoFacetsStgAcct = get_widget_value('LhoFacetsStgAcct','')
LhoFacetsStgPW = get_widget_value('LhoFacetsStgPW','')
LhoFacetsStgDSN = get_widget_value('LhoFacetsStgDSN','')

schema_PaymtRedHL = StructType([
    StructField("ACPR_REF_ID", StringType(), True),
    StructField("ACPR_SUB_TYPE", StringType(), True),
    StructField("ACRH_SEQ_NO", IntegerType(), True)
])

df_PaymtRedHL = (
    spark.read
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_PaymtRedHL)
    .csv(f"{adls_path}/update/LhoFctsClmPayRdctnActvtyHitlist.dat")
)

jdbc_url_CMC_ARCH_RED_HIST, jdbc_props_CMC_ARCH_RED_HIST = get_db_config(lhofacetsstgowner_secret_name)
extract_query_CMC_ARCH_RED_HIST = (
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
    f"FROM {LhoFacetsStgOwner}.CMC_ACRH_RED_HIST actvty"
)

df_CMC_ARCH_RED_HIST = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CMC_ARCH_RED_HIST)
    .options(**jdbc_props_CMC_ARCH_RED_HIST)
    .option("query", extract_query_CMC_ARCH_RED_HIST)
    .load()
)

df_Trim = (
    df_CMC_ARCH_RED_HIST
    .select(
        trim(col("ACPR_REF_ID")).alias("ACPR_REF_ID_LKUP"),
        trim(col("ACPR_SUB_TYPE")).alias("ACPR_SUB_TYPE_LKUP"),
        col("ACRH_SEQ_NO").alias("ACRH_SEQ_NO_LKUP"),
        col("ACPR_REF_ID").alias("ACPR_REF_ID"),
        col("ACPR_SUB_TYPE").alias("ACPR_SUB_TYPE"),
        col("ACRH_SEQ_NO").alias("ACRH_SEQ_NO"),
        col("ACRH_CREATE_DT").alias("ACRH_CREATE_DT"),
        col("ACRH_EVENT_TYPE").alias("ACRH_EVENT_TYPE"),
        col("CKPY_REF_ID").alias("CKPY_REF_ID"),
        col("ACRH_RECD_DT").alias("ACRH_RECD_DT"),
        col("ACRH_PER_END_DT").alias("ACRH_PER_END_DT"),
        col("ACRH_AMT").alias("ACRH_AMT"),
        col("USUS_ID").alias("USUS_ID"),
        col("ACRH_MCTR_RSN").alias("ACRH_MCTR_RSN"),
        col("ACRH_RCVD_CKNO").alias("ACRH_RCVD_CKNO")
    )
)

# Scenario A for hashed file hf_actvty_payred_hl_lkup
df_hf_actvty_payred_hl_lkup = dedup_sort(
    df_Trim,
    ["ACPR_REF_ID_LKUP", "ACPR_SUB_TYPE_LKUP", "ACRH_SEQ_NO_LKUP"],
    []
)

df_StripField_joined = (
    df_PaymtRedHL.alias("Hitlist")
    .join(
        df_hf_actvty_payred_hl_lkup.alias("Extract"),
        (
            (trim(col("Hitlist.ACPR_REF_ID")) == col("Extract.ACPR_REF_ID_LKUP")) &
            (trim(col("Hitlist.ACPR_SUB_TYPE")) == col("Extract.ACPR_SUB_TYPE_LKUP")) &
            (trim(col("Hitlist.ACRH_SEQ_NO")) == col("Extract.ACRH_SEQ_NO_LKUP")) &
            (trim(col("Hitlist.ACPR_REF_ID")) == col("Extract.ACPR_REF_ID")) &
            (trim(col("Hitlist.ACPR_SUB_TYPE")) == col("Extract.ACPR_SUB_TYPE")) &
            (trim(col("Hitlist.ACRH_SEQ_NO")) == col("Extract.ACRH_SEQ_NO"))
        ),
        "left"
    )
)

df_StripField_filtered = df_StripField_joined.filter(
    col("Extract.ACPR_REF_ID").isNotNull() &
    col("Extract.ACPR_SUB_TYPE").isNotNull() &
    col("Extract.ACRH_SEQ_NO").isNotNull()
)

df_StripField = (
    df_StripField_filtered
    .select(
        strip_field(col("Extract.ACPR_REF_ID")).alias("ACPR_REF_ID"),
        strip_field(col("Extract.ACPR_SUB_TYPE")).alias("ACPR_SUB_TYPE"),
        col("Extract.ACRH_SEQ_NO").alias("ACRH_SEQ_NO"),
        col("Extract.ACRH_CREATE_DT").alias("ACRH_CREATE_DT"),
        strip_field(col("Extract.ACRH_EVENT_TYPE")).alias("ACRH_EVENT_TYPE"),
        strip_field(col("Extract.CKPY_REF_ID")).alias("CKPY_REF_ID"),
        col("Extract.ACRH_RECD_DT").alias("ACRH_RECD_DT"),
        col("Extract.ACRH_PER_END_DT").alias("ACRH_PER_END_DT"),
        col("Extract.ACRH_AMT").alias("ACRH_AMT"),
        strip_field(col("Extract.USUS_ID")).alias("USUS_ID"),
        strip_field(col("Extract.ACRH_MCTR_RSN")).alias("ACRH_MCTR_RSN"),
        strip_field(col("Extract.ACRH_RCVD_CKNO")).alias("ACRH_RCVD_CKNO")
    )
)

df_BusinessRules = (
    df_StripField
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", lit("I"))
    .withColumn("DISCARD_IN", lit("N"))
    .withColumn("PASS_THRU_IN", lit("Y"))
    .withColumn("FIRST_RECYC_DT", current_date())
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", lit(SrcSysCd))
    .withColumn(
        "PRI_KEY_STRING",
        concat(
            lit(SrcSysCd),
            lit(";"),
            trim(col("ACPR_REF_ID")),
            lit(";"),
            trim(col("ACPR_SUB_TYPE")),
            lit(";"),
            col("ACRH_SEQ_NO")
        )
    )
    .withColumn("PAYMT_RDUCTN_ACTVTY_SK", lit(0))
    .withColumn(
        "PAYMT_RDUCTN_REF_ID",
        when(
            col("ACPR_REF_ID").isNull() | (length(trim(col("ACPR_REF_ID"))) == 0),
            lit("UNK")
        ).otherwise(upper(trim(col("ACPR_REF_ID"))))
    )
    .withColumn(
        "PAYMT_RDUCTN_SUB_TYPE",
        when(
            col("ACPR_SUB_TYPE").isNull() | (length(trim(col("ACPR_SUB_TYPE"))) == 0),
            lit("UNK")
        ).otherwise(upper(trim(col("ACPR_SUB_TYPE"))))
    )
    .withColumn(
        "PAYMT_RDUCTN_ACTVTY_SEQ_NO",
        when(
            col("ACRH_SEQ_NO").isNull() | (length(trim(col("ACRH_SEQ_NO"))) == 0),
            lit("UNK")
        ).otherwise(upper(trim(col("ACRH_SEQ_NO"))))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn(
        "PAYMT_RDUCTN_ID",
        when(
            col("ACPR_REF_ID").isNull() | (length(trim(col("ACPR_REF_ID"))) == 0),
            lit("UNK")
        ).otherwise(upper(trim(col("ACPR_REF_ID"))))
    )
    .withColumn(
        "USER_ID",
        when(
            col("USUS_ID").isNull() | (length(trim(col("USUS_ID"))) == 0),
            lit("NA")
        ).otherwise(trim(col("USUS_ID")))
    )
    .withColumn(
        "PAYMT_RDUCTN_ACT_EVT_TYP_CD",
        when(
            col("ACRH_EVENT_TYPE").isNull() | (length(trim(col("ACRH_EVENT_TYPE"))) == 0),
            lit("NA")
        ).otherwise(upper(trim(col("ACRH_EVENT_TYPE"))))
    )
    .withColumn(
        "PAYMT_RDUCTN_ACTVTY_RSN_CD",
        when(
            col("ACRH_MCTR_RSN").isNull() | (length(trim(col("ACRH_MCTR_RSN"))) == 0),
            lit("NA")
        ).otherwise(upper(trim(col("ACRH_MCTR_RSN"))))
    )
    .withColumn(
        "ACCTG_PERD_END_DT",
        when(
            col("ACRH_PER_END_DT").isNull() | (length(trim(col("ACRH_PER_END_DT"))) == 0),
            lit("UNK")
        ).otherwise(col("ACRH_PER_END_DT").substr(lit(1), lit(10)))
    )
    .withColumn(
        "CRT_DT",
        when(
            col("ACRH_CREATE_DT").isNull() | (length(trim(col("ACRH_CREATE_DT"))) == 0),
            lit("UNK")
        ).otherwise(col("ACRH_CREATE_DT").substr(lit(1), lit(10)))
    )
    .withColumn(
        "RFND_RCVD_DT",
        when(
            col("ACRH_RECD_DT").isNull() | (length(trim(col("ACRH_RECD_DT"))) == 0),
            lit("UNK")
        ).otherwise(col("ACRH_RECD_DT").substr(lit(1), lit(10)))
    )
    .withColumn(
        "PAYMT_RDUCTN_ACTVTY_AMT",
        when(
            col("ACRH_AMT").isNull() | (length(trim(col("ACRH_AMT"))) == 0) | (~col("ACRH_AMT").cast("string").rlike("^[0-9.+-]*$")),
            lit(0)
        ).otherwise(col("ACRH_AMT"))
    )
    .withColumn("PCA_RCVR_AMT", lit("0.00"))
    .withColumn(
        "PAYMT_REF_ID",
        when(
            col("CKPY_REF_ID").isNull() | (length(trim(col("CKPY_REF_ID"))) == 0),
            lit("UNK")
        ).otherwise(upper(trim(col("CKPY_REF_ID"))))
    )
    .withColumn(
        "RCVD_CHK_NO",
        when(
            col("ACRH_RCVD_CKNO").isNull() | (length(trim(col("ACRH_RCVD_CKNO"))) == 0),
            lit("UNK")
        ).otherwise(upper(trim(col("ACRH_RCVD_CKNO"))))
    )
)

df_Snapshot_AllColl = (
    df_BusinessRules
    .select(
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("PAYMT_RDUCTN_REF_ID").alias("PAYMT_RDUCTN_REF_ID"),
        col("PAYMT_RDUCTN_SUB_TYPE").alias("PAYMT_RDUCTN_SUBTYP_CD"),
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
        col("PAYMT_RDUCTN_ID").alias("PAYMT_RDUCTN_ID"),
        col("USER_ID").alias("USER_ID"),
        col("PAYMT_RDUCTN_ACT_EVT_TYP_CD").alias("PAYMT_RDUCTN_ACT_EVT_TYP_CD"),
        col("PAYMT_RDUCTN_ACTVTY_RSN_CD").alias("PAYMT_RDUCTN_ACTVTY_RSN_CD"),
        col("ACCTG_PERD_END_DT").alias("ACCTG_PERD_END_DT"),
        col("CRT_DT").alias("CRT_DT"),
        col("RFND_RCVD_DT").alias("RFND_RCVD_DT"),
        col("PAYMT_RDUCTN_ACTVTY_AMT").alias("PAYMT_RDUCTN_ACTVTY_AMT"),
        col("PCA_RCVR_AMT").alias("PCA_RCVR_AMT"),
        col("PAYMT_REF_ID").alias("PAYMT_REF_ID"),
        col("RCVD_CHK_NO").alias("RCVD_CHK_NO")
    )
)

df_Snapshot_Transform = (
    df_BusinessRules
    .select(
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("PAYMT_RDUCTN_REF_ID").alias("PAYMT_RDUCTN_REF_ID"),
        col("PAYMT_RDUCTN_SUB_TYPE").alias("PAYMT_RDUCTN_SUBTYP_CD"),
        col("PAYMT_RDUCTN_ACTVTY_SEQ_NO").alias("PAYMT_RDUCTN_ACTVTY_SEQ_NO")
    )
)

params_container = {
    "CurrRunCycle": CurrRunCycle,
    "CurrDate": CurrentDate,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "IDSOwner": IDSOwner
}

df_PaymtRductnActvtyPK_out = PaymtRductnActvtyPK(df_Snapshot_AllColl, df_Snapshot_Transform, params_container)

df_FctsClmPayRductnActvtyExtr = (
    df_PaymtRductnActvtyPK_out
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("PAYMT_RDUCTN_SUBTYP_CD", rpad(col("PAYMT_RDUCTN_SUBTYP_CD"), 1, " "))
    .withColumn("PAYMT_RDUCTN_ID", rpad(col("PAYMT_RDUCTN_ID"), 10, " "))
    .withColumn("USER_ID", rpad(col("USER_ID"), 10, " "))
    .withColumn("PAYMT_RDUCTN_ACT_EVT_TYP_CD", rpad(col("PAYMT_RDUCTN_ACT_EVT_TYP_CD"), 10, " "))
    .withColumn("PAYMT_RDUCTN_ACTVTY_RSN_CD", rpad(col("PAYMT_RDUCTN_ACTVTY_RSN_CD"), 10, " "))
    .withColumn("ACCTG_PERD_END_DT", rpad(col("ACCTG_PERD_END_DT"), 10, " "))
    .withColumn("CRT_DT", rpad(col("CRT_DT"), 10, " "))
    .withColumn("RFND_RCVD_DT", rpad(col("RFND_RCVD_DT"), 10, " "))
    .withColumn("RCVD_CHK_NO", rpad(col("RCVD_CHK_NO"), 10, " "))
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
        "PAYMT_RDUCTN_ACTVTY_SK",
        "PAYMT_RDUCTN_REF_ID",
        "PAYMT_RDUCTN_SUBTYP_CD",
        "PAYMT_RDUCTN_ACTVTY_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PAYMT_RDUCTN_ID",
        "USER_ID",
        "PAYMT_RDUCTN_ACT_EVT_TYP_CD",
        "PAYMT_RDUCTN_ACTVTY_RSN_CD",
        "ACCTG_PERD_END_DT",
        "CRT_DT",
        "RFND_RCVD_DT",
        "PAYMT_RDUCTN_ACTVTY_AMT",
        "PCA_RCVR_AMT",
        "PAYMT_REF_ID",
        "RCVD_CHK_NO"
    )
)

write_files(
    df_FctsClmPayRductnActvtyExtr,
    f"{adls_path}/key/LhoFctsClmPayRductnActvtyExtr.LhoFctsClmPayRductnActvty.dat.{RunID}",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)