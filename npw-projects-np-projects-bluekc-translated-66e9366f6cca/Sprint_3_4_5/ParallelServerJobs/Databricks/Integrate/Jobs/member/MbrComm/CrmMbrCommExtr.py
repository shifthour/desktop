# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   CRM MBR_COMM Extract job
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Kalyan Neelam        2013-05-07                Initial programming                                                   4841 - Digital Communications   IntegrateNewDevl             Bhoomi Dasari           5/12/2013
# MAGIC Kalyan Neelam       2013-06-17              Update the extract SQL to use the Contact view        4841 Digital Communications      IntegrateNewDevl           Bhoomi Dasari           6/21/2013
# MAGIC                                                                and not the Contact base tables to extract data
# MAGIC 
# MAGIC Michael Harmon     2016-11-111             Added 3 new email address fields, OTHR_EMAIL_ADDR         5541 Shop To Enroll     IDS\\Dev1                      Kalyan Neelam          2016-11-17
# MAGIC                                                                FCTS_EMAIL_ADDR and WEB_RGSTRN_EMAIL_ADDR
# MAGIC Raja Gummadi        2018-02-26              Changed source from CRM 2011 to CRM 2016                         5683 CRM Migration      IntegrateDev2               Kalyan Neelam          2018-03-04
# MAGIC 
# MAGIC HariKrishnaRao Yadav  2021-08-24       Changed source Database CRM to MBR_PREFNC File and    US 420239                     IntegrateSITF                Jeyaprasanna            2022-01-31
# MAGIC                                                                applied Tranformation in Stage Business logic
# MAGIC Praneeth K          2022-01-25                    Added 5 new Member fields                                                      469607                  EnterpriseDev2                    Jeyaprasanna            2022-01-31
# MAGIC 
# MAGIC Vamsi Aripaka     2023-12-21                   Added new field MBR_OPTNL_PHN_CALL_IN                         607496                   IntegrateDev1                     Jeyaprasanna           2024-01-17
# MAGIC                                                                  from source to target.
# MAGIC 
# MAGIC Ediga Maruthi  2024-06-18        Removed default 'N' value for the field MBR_OPTNL_PHN_CALL_IN in     621348                  IntegrateDev2                  Jeyaprasanna           2024-06-24
# MAGIC                                                  xfm_Business_Logic stage and Allowed nulls for the fileds MBR_EDUC_EMAIL_IN,
# MAGIC                                                  MBR_EDUC_POSTAL_IN,MBR_RQRD_EMAIL_IN,MBR_RQRD_POSTAL_IN,
# MAGIC                                                  MBR_GNRC_SMS_IN,MBR_OPTNL_PHN_CALL_IN from source to target.

# MAGIC Writing Sequential File to /key
# MAGIC Primary Key Shared Container used in 
# MAGIC 
# MAGIC CrmMbrCommExtr
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC Extract  MBR_COMM Data from MBR_PREFNC File
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Obtain parameter values
CurrRunCycle = get_widget_value("CurrRunCycle","")
RunID = get_widget_value("RunID","")
SrcSysCd = get_widget_value("SrcSysCd","")
RunDate = get_widget_value("RunDate","")
SourceSk = get_widget_value("SourceSk","")
FileName = get_widget_value("FileName","")
LastUpdtRunTime = get_widget_value("LastUpdtRunTime","")

# Read the hashed file "hf_etrnl_mbr_uniq_key" as parquet (Scenario C)
df_hf_etrnl_mbr_uniq_key = spark.read.parquet(f"{adls_path}/hf_etrnl_mbr_uniq_key.parquet")

# Read the CSeqFileStage "CrmMbrCommExtr" from landing
schema_CrmMbrCommExtr = StructType([
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("MYCLM_EOB_PRFRNC_CD", StringType(), True),
    StructField("RCRD_STTUS_CD", StringType(), True),
    StructField("AUTO_PAY_NTFCTN_IN", StringType(), True),
    StructField("BLUELOOP_PARTCPN_IN", StringType(), True),
    StructField("MBR_PRFRNCS_SET_IN", StringType(), True),
    StructField("MYBLUEKC_ELTRNC_BILL_EMAIL_IN", StringType(), True),
    StructField("MYBLUEKC_ELTRNC_BILL_SMS_IN", StringType(), True),
    StructField("MYBLUEKC_PRT_BILL_EMAIL_IN", StringType(), True),
    StructField("MYBLUEKC_PRT_BILL_POSTAL_IN", StringType(), True),
    StructField("MYBLUEKC_PRT_BILL_SMS_IN", StringType(), True),
    StructField("MYCLM_EMAIL_IN", StringType(), True),
    StructField("MYCLM_POSTAL_IN", StringType(), True),
    StructField("MYCLM_SMS_IN", StringType(), True),
    StructField("SRC_SYS_UPDT_DTM", StringType(), True),
    StructField("CELL_PHN_NO", StringType(), True),
    StructField("EMAIL_ADDR", StringType(), True),
    StructField("OTHR_EMAIL_ADDR", StringType(), True),
    StructField("FCTS_EMAIL_ADDR", StringType(), True),
    StructField("WEB_RGSTRN_EMAIL_ADDR", StringType(), True),
    StructField("RELAY_CELL_PHN_NO", StringType(), True),
    StructField("RELAY_OPT_IN_IN", StringType(), True),
    StructField("MEMBER_EDUCATIONAL_EMAIL_INDICATOR", StringType(), True),
    StructField("MEMBER_EDUCATIONAL_POSTAL_INDICATOR", StringType(), True),
    StructField("MEMBER_REQUIRED_EMAIL_INDICATOR", StringType(), True),
    StructField("MEMBER_REQUIRED_POSTAL_INDICATOR", StringType(), True),
    StructField("MEMBER_GENERIC_SMS_INDICATOR", StringType(), True),
    StructField("MEMBER_OPTIONAL_PHONE_CALL_INDICATOR", StringType(), True)
])

df_CrmMbrCommExtr = (
    spark.read.format("csv")
    .option("header", "true")
    .option("quote", "\"")
    .schema(schema_CrmMbrCommExtr)
    .load(f"{adls_path_raw}/landing/{FileName}")
)

# Xfm_Strip (CTransformerStage)
df_Xfm_Strip = df_CrmMbrCommExtr.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    strip_field(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    strip_field(F.col("MYCLM_EOB_PRFRNC_CD")).alias("MYCLM_EOB_PRFRNC_CD"),
    strip_field(F.col("RCRD_STTUS_CD")).alias("RCRD_STTUS_CD"),
    strip_field(F.col("AUTO_PAY_NTFCTN_IN")).alias("AUTO_PAY_NTFCTN_IN"),
    strip_field(F.col("BLUELOOP_PARTCPN_IN")).alias("BLUELOOP_PARTCPN_IN"),
    strip_field(F.col("MBR_PRFRNCS_SET_IN")).alias("MBR_PRFRNCS_SET_IN"),
    strip_field(F.col("MYBLUEKC_ELTRNC_BILL_EMAIL_IN")).alias("MYBLUEKC_ELTRNC_BILL_EMAIL_IN"),
    strip_field(F.col("MYBLUEKC_ELTRNC_BILL_SMS_IN")).alias("MYBLUEKC_ELTRNC_BILL_SMS_IN"),
    strip_field(F.col("MYBLUEKC_PRT_BILL_EMAIL_IN")).alias("MYBLUEKC_PRT_BILL_EMAIL_IN"),
    strip_field(F.col("MYBLUEKC_PRT_BILL_POSTAL_IN")).alias("MYBLUEKC_PRT_BILL_POSTAL_IN"),
    strip_field(F.col("MYBLUEKC_PRT_BILL_SMS_IN")).alias("MYBLUEKC_PRT_BILL_SMS_IN"),
    strip_field(F.col("MYCLM_EMAIL_IN")).alias("MYCLM_EMAIL_IN"),
    strip_field(F.col("MYCLM_POSTAL_IN")).alias("MYCLM_POSTAL_IN"),
    strip_field(F.col("MYCLM_SMS_IN")).alias("MYCLM_SMS_IN"),
    strip_field(F.col("SRC_SYS_UPDT_DTM")).alias("SRC_SYS_UPDT_DTM"),
    strip_field(F.col("CELL_PHN_NO")).alias("CELL_PHN_NO"),
    strip_field(F.col("EMAIL_ADDR")).alias("EMAIL_ADDR"),
    strip_field(F.col("OTHR_EMAIL_ADDR")).alias("OTHR_EMAIL_ADDR"),
    strip_field(F.col("FCTS_EMAIL_ADDR")).alias("FCTS_EMAIL_ADDR"),
    strip_field(F.col("WEB_RGSTRN_EMAIL_ADDR")).alias("WEB_RGSTRN_EMAIL_ADDR"),
    strip_field(F.col("RELAY_CELL_PHN_NO")).alias("RELAY_CELL_PHN_NO"),
    strip_field(F.col("RELAY_OPT_IN_IN")).alias("RELAY_OPT_IN_IN"),
    strip_field(F.col("MEMBER_EDUCATIONAL_EMAIL_INDICATOR")).alias("MEMBER_EDUCATIONAL_EMAIL_INDICATOR"),
    strip_field(F.col("MEMBER_EDUCATIONAL_POSTAL_INDICATOR")).alias("MEMBER_EDUCATIONAL_POSTAL_INDICATOR"),
    strip_field(F.col("MEMBER_REQUIRED_EMAIL_INDICATOR")).alias("MEMBER_REQUIRED_EMAIL_INDICATOR"),
    strip_field(F.col("MEMBER_REQUIRED_POSTAL_INDICATOR")).alias("MEMBER_REQUIRED_POSTAL_INDICATOR"),
    strip_field(F.col("MEMBER_GENERIC_SMS_INDICATOR")).alias("MEMBER_GENERIC_SMS_INDICATOR"),
    strip_field(F.col("MEMBER_OPTIONAL_PHONE_CALL_INDICATOR")).alias("MEMBER_OPTIONAL_PHONE_CALL_INDICATOR")
)

# xfm_Lkp (CTransformerStage) - left join with df_hf_etrnl_mbr_uniq_key
df_lkp_joined = (
    df_Xfm_Strip.alias("lnk_Striplkp")
    .join(
        df_hf_etrnl_mbr_uniq_key.alias("mbr_uniq_key_lkup"),
        (
            trim(F.col("lnk_Striplkp.MBR_UNIQ_KEY")) 
            == F.col("mbr_uniq_key_lkup.MBR_UNIQ_KEY")
        ),
        "left"
    )
)

df_lnk_Extract = df_lkp_joined.filter(
    (F.col("lnk_Striplkp.MBR_UNIQ_KEY").isNotNull())
    & (F.length(trim(F.col("lnk_Striplkp.MBR_UNIQ_KEY"))) > 0)
    & (F.col("mbr_uniq_key_lkup.MBR_UNIQ_KEY").isNotNull())
).select(
    F.col("lnk_Striplkp.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lnk_Striplkp.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_Striplkp.MYCLM_EOB_PRFRNC_CD").alias("MYCLM_EOB_PRFRNC_CD"),
    F.col("lnk_Striplkp.RCRD_STTUS_CD").alias("RCRD_STTUS_CD"),
    F.col("lnk_Striplkp.AUTO_PAY_NTFCTN_IN").alias("AUTO_PAY_NTFCTN_IN"),
    F.col("lnk_Striplkp.BLUELOOP_PARTCPN_IN").alias("BLUELOOP_PARTCPN_IN"),
    F.col("lnk_Striplkp.MBR_PRFRNCS_SET_IN").alias("MBR_PRFRNCS_SET_IN"),
    F.col("lnk_Striplkp.MYBLUEKC_ELTRNC_BILL_EMAIL_IN").alias("MYBLUEKC_ELTRNC_BILL_EMAIL_IN"),
    F.col("lnk_Striplkp.MYBLUEKC_ELTRNC_BILL_SMS_IN").alias("MYBLUEKC_ELTRNC_BILL_SMS_IN"),
    F.col("lnk_Striplkp.MYBLUEKC_PRT_BILL_EMAIL_IN").alias("MYBLUEKC_PRT_BILL_EMAIL_IN"),
    F.col("lnk_Striplkp.MYBLUEKC_PRT_BILL_POSTAL_IN").alias("MYBLUEKC_PRT_BILL_POSTAL_IN"),
    F.col("lnk_Striplkp.MYBLUEKC_PRT_BILL_SMS_IN").alias("MYBLUEKC_PRT_BILL_SMS_IN"),
    F.col("lnk_Striplkp.MYCLM_EMAIL_IN").alias("MYCLM_EMAIL_IN"),
    F.col("lnk_Striplkp.MYCLM_POSTAL_IN").alias("MYCLM_POSTAL_IN"),
    F.col("lnk_Striplkp.MYCLM_SMS_IN").alias("MYCLM_SMS_IN"),
    F.col("lnk_Striplkp.SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
    F.col("lnk_Striplkp.CELL_PHN_NO").alias("CELL_PHN_NO"),
    F.col("lnk_Striplkp.EMAIL_ADDR").alias("EMAIL_ADDR"),
    F.col("lnk_Striplkp.OTHR_EMAIL_ADDR").alias("OTHR_EMAIL_ADDR"),
    F.col("lnk_Striplkp.FCTS_EMAIL_ADDR").alias("FCTS_EMAIL_ADDR"),
    F.col("lnk_Striplkp.WEB_RGSTRN_EMAIL_ADDR").alias("WEB_RGSTRN_EMAIL_ADDR"),
    F.col("lnk_Striplkp.RELAY_CELL_PHN_NO").alias("RELAY_CELL_PHN_NO"),
    F.col("lnk_Striplkp.RELAY_OPT_IN_IN").alias("RELAY_OPT_IN_IN"),
    F.col("lnk_Striplkp.MEMBER_EDUCATIONAL_EMAIL_INDICATOR").alias("MEMBER_EDUCATIONAL_EMAIL_INDICATOR"),
    F.col("lnk_Striplkp.MEMBER_EDUCATIONAL_POSTAL_INDICATOR").alias("MEMBER_EDUCATIONAL_POSTAL_INDICATOR"),
    F.col("lnk_Striplkp.MEMBER_REQUIRED_EMAIL_INDICATOR").alias("MEMBER_REQUIRED_EMAIL_INDICATOR"),
    F.col("lnk_Striplkp.MEMBER_REQUIRED_POSTAL_INDICATOR").alias("MEMBER_REQUIRED_POSTAL_INDICATOR"),
    F.col("lnk_Striplkp.MEMBER_GENERIC_SMS_INDICATOR").alias("MEMBER_GENERIC_SMS_INDICATOR"),
    F.col("lnk_Striplkp.MEMBER_OPTIONAL_PHONE_CALL_INDICATOR").alias("MEMBER_OPTIONAL_PHONE_CALL_INDICATOR")
)

# xfm_Business_Logic (CTransformerStage)
# Stage Variables: svMbrUniqKey = lnk_Extract.MBR_UNIQ_KEY, RowPassThru = "Y"
# Output columns follow the expressions given
df_xfm_Business_Logic_prep = (
    df_lnk_Extract
    .withColumn("svMbrUniqKey", F.col("MBR_UNIQ_KEY"))
    .withColumn("RowPassThru", F.lit("Y"))
)

df_xfm_Business_Logic = (
    df_xfm_Business_Logic_prep
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.col("RowPassThru"))
    .withColumn("FIRST_RECYC_DT", F.lit(RunDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn("PRI_KEY_STRING", F.concat(F.col("svMbrUniqKey"), F.lit(";"), F.lit(SrcSysCd)))
    .withColumn("MBR_UNIQ_KEY", F.col("svMbrUniqKey"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn("MBR_COMM_SK", F.lit(0))
    .withColumn("MYCLM_EOB_PRFRNC_CD", F.col("MYCLM_EOB_PRFRNC_CD"))
    .withColumn("RCRD_STTUS_CD", F.col("RCRD_STTUS_CD"))
    .withColumn("STTUS_RSN_CD", F.col("RCRD_STTUS_CD"))
    .withColumn(
        "AUTO_PAY_NTFCTN_IN",
        F.when(
            F.col("AUTO_PAY_NTFCTN_IN").isNull() | (F.length(trim(F.col("AUTO_PAY_NTFCTN_IN"))) == 0),
            F.lit("N")
        ).otherwise(F.col("AUTO_PAY_NTFCTN_IN"))
    )
    .withColumn(
        "BLUELOOP_PARTCPN_IN",
        F.when(
            F.col("BLUELOOP_PARTCPN_IN").isNull() | (F.length(trim(F.col("BLUELOOP_PARTCPN_IN"))) == 0),
            F.lit("N")
        ).otherwise(F.col("BLUELOOP_PARTCPN_IN"))
    )
    .withColumn("HLTH_WELNS_DO_NOT_SEND_IN", F.lit("N"))
    .withColumn("HLTH_WELNS_EMAIL_IN", F.lit("N"))
    .withColumn("HLTH_WELNS_POSTAL_IN", F.lit("N"))
    .withColumn("HLTH_WELNS_SMS_IN", F.lit("N"))
    .withColumn(
        "MBR_PRFRNCS_SET_IN",
        F.when(
            F.col("MBR_PRFRNCS_SET_IN").isNull() | (F.length(trim(F.col("MBR_PRFRNCS_SET_IN"))) == 0),
            F.lit("N")
        ).otherwise(F.col("MBR_PRFRNCS_SET_IN"))
    )
    .withColumn(
        "MYBLUEKC_ELTRNC_BILL_EMAIL_IN",
        F.when(
            F.col("MYBLUEKC_ELTRNC_BILL_EMAIL_IN").isNull() | (F.length(trim(F.col("MYBLUEKC_ELTRNC_BILL_EMAIL_IN"))) == 0),
            F.lit("N")
        ).otherwise(F.col("MYBLUEKC_ELTRNC_BILL_EMAIL_IN"))
    )
    .withColumn(
        "MYBLUEKC_ELTRNC_BILL_SMS_IN",
        F.when(
            F.col("MYBLUEKC_ELTRNC_BILL_SMS_IN").isNull() | (F.length(trim(F.col("MYBLUEKC_ELTRNC_BILL_SMS_IN"))) == 0),
            F.lit("N")
        ).otherwise(F.col("MYBLUEKC_ELTRNC_BILL_SMS_IN"))
    )
    .withColumn(
        "MYBLUEKC_PRT_BILL_EMAIL_IN",
        F.when(
            F.col("MYBLUEKC_PRT_BILL_EMAIL_IN").isNull() | (F.length(trim(F.col("MYBLUEKC_PRT_BILL_EMAIL_IN"))) == 0),
            F.lit("N")
        ).otherwise(F.col("MYBLUEKC_PRT_BILL_EMAIL_IN"))
    )
    .withColumn(
        "MYBLUEKC_PRT_BILL_POSTAL_IN",
        F.when(
            F.col("MYBLUEKC_PRT_BILL_POSTAL_IN").isNull() | (F.length(trim(F.col("MYBLUEKC_PRT_BILL_POSTAL_IN"))) == 0),
            F.lit("N")
        ).otherwise(F.col("MYBLUEKC_PRT_BILL_POSTAL_IN"))
    )
    .withColumn(
        "MYBLUEKC_PRT_BILL_SMS_IN",
        F.when(
            F.col("MYBLUEKC_PRT_BILL_SMS_IN").isNull() | (F.length(trim(F.col("MYBLUEKC_PRT_BILL_SMS_IN"))) == 0),
            F.lit("N")
        ).otherwise(F.col("MYBLUEKC_PRT_BILL_SMS_IN"))
    )
    .withColumn(
        "MYCLM_EMAIL_IN",
        F.when(
            F.col("MYCLM_EMAIL_IN").isNull() | (F.length(trim(F.col("MYCLM_EMAIL_IN"))) == 0),
            F.lit("N")
        ).otherwise(F.col("MYCLM_EMAIL_IN"))
    )
    .withColumn(
        "MYCLM_POSTAL_IN",
        F.when(
            F.col("MYCLM_POSTAL_IN").isNull() | (F.length(trim(F.col("MYCLM_POSTAL_IN"))) == 0),
            F.lit("N")
        ).otherwise(F.col("MYCLM_POSTAL_IN"))
    )
    .withColumn(
        "MYCLM_SMS_IN",
        F.when(
            F.col("MYCLM_SMS_IN").isNull() | (F.length(trim(F.col("MYCLM_SMS_IN"))) == 0),
            F.lit("N")
        ).otherwise(F.col("MYCLM_SMS_IN"))
    )
    .withColumn("MYPLN_INFO_EMAIL_IN", F.lit("N"))
    .withColumn("MYPLN_INFO_POSTAL_IN", F.lit("N"))
    .withColumn("MYPLN_INFO_SMS_IN", F.lit("N"))
    .withColumn("PROD_SVC_DO_NOT_SEND_IN", F.lit("N"))
    .withColumn("PROD_SVC_EMAIL_IN", F.lit("N"))
    .withColumn("PROD_SVC_POSTAL_IN", F.lit("N"))
    .withColumn("PROD_SVC_SMS_IN", F.lit("N"))
    .withColumn(
        "SRC_SYS_UPDT_DTM",
        F.when(
            F.col("SRC_SYS_UPDT_DTM").isNull()
            | (F.upper(trim(F.col("SRC_SYS_UPDT_DTM"))) == F.lit("NA"))
            | (F.length(trim(F.col("SRC_SYS_UPDT_DTM"))) == 0),
            current_timestamp()
        ).otherwise(
            current_timestamp()
        )
    )
    .withColumn(
        "CELL_PHN_NO",
        F.when(
            F.col("CELL_PHN_NO").isNull() | (F.length(trim(F.col("CELL_PHN_NO"))) == 0),
            F.lit("UNK")
        ).otherwise(F.col("CELL_PHN_NO"))
    )
    .withColumn(
        "EMAIL_ADDR",
        F.when(
            F.col("EMAIL_ADDR").isNull() | (F.length(trim(F.col("EMAIL_ADDR"))) == 0),
            F.lit("UNK")
        ).otherwise(F.col("EMAIL_ADDR"))
    )
    .withColumn(
        "OTHR_EMAIL_ADDR",
        F.when(
            F.col("OTHR_EMAIL_ADDR").isNull() | (F.length(trim(F.col("OTHR_EMAIL_ADDR"))) == 0),
            F.lit("UNK")
        ).otherwise(F.col("OTHR_EMAIL_ADDR"))
    )
    .withColumn(
        "FCTS_EMAIL_ADDR",
        F.when(
            F.col("FCTS_EMAIL_ADDR").isNull() | (F.length(trim(F.col("FCTS_EMAIL_ADDR"))) == 0),
            F.lit("UNK")
        ).otherwise(F.col("FCTS_EMAIL_ADDR"))
    )
    .withColumn(
        "WEB_RGSTRN_EMAIL_ADDR",
        F.when(
            F.col("WEB_RGSTRN_EMAIL_ADDR").isNull() | (F.length(trim(F.col("WEB_RGSTRN_EMAIL_ADDR"))) == 0),
            F.lit("UNK")
        ).otherwise(F.col("WEB_RGSTRN_EMAIL_ADDR"))
    )
    .withColumn("MEMBER_EDUCATIONAL_EMAIL_INDICATOR", F.col("MEMBER_EDUCATIONAL_EMAIL_INDICATOR"))
    .withColumn("MEMBER_EDUCATIONAL_POSTAL_INDICATOR", F.col("MEMBER_EDUCATIONAL_POSTAL_INDICATOR"))
    .withColumn("MEMBER_REQUIRED_EMAIL_INDICATOR", F.col("MEMBER_REQUIRED_EMAIL_INDICATOR"))
    .withColumn("MEMBER_REQUIRED_POSTAL_INDICATOR", F.col("MEMBER_REQUIRED_POSTAL_INDICATOR"))
    .withColumn("MEMBER_GENERIC_SMS_INDICATOR", F.col("MEMBER_GENERIC_SMS_INDICATOR"))
    .withColumn("MEMBER_OPTIONAL_PHONE_CALL_INDICATOR", F.col("MEMBER_OPTIONAL_PHONE_CALL_INDICATOR"))
)

# MAGIC %run ../../../../shared_containers/PrimaryKey/MbrCommPK
# COMMAND ----------

# Pass data to shared container
container_params = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunDate": RunDate,
    "SourceSk": SourceSk
}
df_seq_IdsMbrCommExtr = MbrCommPK(df_xfm_Business_Logic, container_params)

# Final select in the same column order as "Seq_IdsMbrCommExtr" stage
df_final = df_seq_IdsMbrCommExtr.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "MBR_COMM_SK",
    "MBR_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MYCLM_EOB_PRFRNC_CD",
    "RCRD_STTUS_CD",
    "STTUS_RSN_CD",
    "AUTO_PAY_NTFCTN_IN",
    "BLUELOOP_PARTCPN_IN",
    "HLTH_WELNS_DO_NOT_SEND_IN",
    "HLTH_WELNS_EMAIL_IN",
    "HLTH_WELNS_POSTAL_IN",
    "HLTH_WELNS_SMS_IN",
    "MBR_PRFRNCS_SET_IN",
    "MYBLUEKC_ELTRNC_BILL_EMAIL_IN",
    "MYBLUEKC_ELTRNC_BILL_SMS_IN",
    "MYBLUEKC_PRT_BILL_EMAIL_IN",
    "MYBLUEKC_PRT_BILL_POSTAL_IN",
    "MYBLUEKC_PRT_BILL_SMS_IN",
    "MYCLM_EMAIL_IN",
    "MYCLM_POSTAL_IN",
    "MYCLM_SMS_IN",
    "MYPLN_INFO_EMAIL_IN",
    "MYPLN_INFO_POSTAL_IN",
    "MYPLN_INFO_SMS_IN",
    "PROD_SVC_DO_NOT_SEND_IN",
    "PROD_SVC_EMAIL_IN",
    "PROD_SVC_POSTAL_IN",
    "PROD_SVC_SMS_IN",
    "SRC_SYS_UPDT_DTM",
    "CELL_PHN_NO",
    "EMAIL_ADDR",
    "OTHR_EMAIL_ADDR",
    "FCTS_EMAIL_ADDR",
    "WEB_RGSTRN_EMAIL_ADDR",
    "MEMBER_EDUCATIONAL_EMAIL_INDICATOR",
    "MEMBER_EDUCATIONAL_POSTAL_INDICATOR",
    "MEMBER_REQUIRED_EMAIL_INDICATOR",
    "MEMBER_REQUIRED_POSTAL_INDICATOR",
    "MEMBER_GENERIC_SMS_INDICATOR",
    "MEMBER_OPTIONAL_PHONE_CALL_INDICATOR"
)

# rpad for columns with defined char lengths in the final file
df_final = (
    df_final
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("AUTO_PAY_NTFCTN_IN", F.rpad(F.col("AUTO_PAY_NTFCTN_IN"), 1, " "))
    .withColumn("BLUELOOP_PARTCPN_IN", F.rpad(F.col("BLUELOOP_PARTCPN_IN"), 1, " "))
    .withColumn("HLTH_WELNS_DO_NOT_SEND_IN", F.rpad(F.col("HLTH_WELNS_DO_NOT_SEND_IN"), 1, " "))
    .withColumn("HLTH_WELNS_EMAIL_IN", F.rpad(F.col("HLTH_WELNS_EMAIL_IN"), 1, " "))
    .withColumn("HLTH_WELNS_POSTAL_IN", F.rpad(F.col("HLTH_WELNS_POSTAL_IN"), 1, " "))
    .withColumn("HLTH_WELNS_SMS_IN", F.rpad(F.col("HLTH_WELNS_SMS_IN"), 1, " "))
    .withColumn("MBR_PRFRNCS_SET_IN", F.rpad(F.col("MBR_PRFRNCS_SET_IN"), 1, " "))
    .withColumn("MYBLUEKC_ELTRNC_BILL_EMAIL_IN", F.rpad(F.col("MYBLUEKC_ELTRNC_BILL_EMAIL_IN"), 1, " "))
    .withColumn("MYBLUEKC_ELTRNC_BILL_SMS_IN", F.rpad(F.col("MYBLUEKC_ELTRNC_BILL_SMS_IN"), 1, " "))
    .withColumn("MYBLUEKC_PRT_BILL_EMAIL_IN", F.rpad(F.col("MYBLUEKC_PRT_BILL_EMAIL_IN"), 1, " "))
    .withColumn("MYBLUEKC_PRT_BILL_POSTAL_IN", F.rpad(F.col("MYBLUEKC_PRT_BILL_POSTAL_IN"), 1, " "))
    .withColumn("MYBLUEKC_PRT_BILL_SMS_IN", F.rpad(F.col("MYBLUEKC_PRT_BILL_SMS_IN"), 1, " "))
    .withColumn("MYCLM_EMAIL_IN", F.rpad(F.col("MYCLM_EMAIL_IN"), 1, " "))
    .withColumn("MYCLM_POSTAL_IN", F.rpad(F.col("MYCLM_POSTAL_IN"), 1, " "))
    .withColumn("MYCLM_SMS_IN", F.rpad(F.col("MYCLM_SMS_IN"), 1, " "))
    .withColumn("MYPLN_INFO_EMAIL_IN", F.rpad(F.col("MYPLN_INFO_EMAIL_IN"), 1, " "))
    .withColumn("MYPLN_INFO_POSTAL_IN", F.rpad(F.col("MYPLN_INFO_POSTAL_IN"), 1, " "))
    .withColumn("MYPLN_INFO_SMS_IN", F.rpad(F.col("MYPLN_INFO_SMS_IN"), 1, " "))
    .withColumn("PROD_SVC_DO_NOT_SEND_IN", F.rpad(F.col("PROD_SVC_DO_NOT_SEND_IN"), 1, " "))
    .withColumn("PROD_SVC_EMAIL_IN", F.rpad(F.col("PROD_SVC_EMAIL_IN"), 1, " "))
    .withColumn("PROD_SVC_POSTAL_IN", F.rpad(F.col("PROD_SVC_POSTAL_IN"), 1, " "))
    .withColumn("PROD_SVC_SMS_IN", F.rpad(F.col("PROD_SVC_SMS_IN"), 1, " "))
    .withColumn("MEMBER_EDUCATIONAL_EMAIL_INDICATOR", F.rpad(F.col("MEMBER_EDUCATIONAL_EMAIL_INDICATOR"), 1, " "))
    .withColumn("MEMBER_EDUCATIONAL_POSTAL_INDICATOR", F.rpad(F.col("MEMBER_EDUCATIONAL_POSTAL_INDICATOR"), 1, " "))
    .withColumn("MEMBER_REQUIRED_EMAIL_INDICATOR", F.rpad(F.col("MEMBER_REQUIRED_EMAIL_INDICATOR"), 1, " "))
    .withColumn("MEMBER_REQUIRED_POSTAL_INDICATOR", F.rpad(F.col("MEMBER_REQUIRED_POSTAL_INDICATOR"), 1, " "))
    .withColumn("MEMBER_GENERIC_SMS_INDICATOR", F.rpad(F.col("MEMBER_GENERIC_SMS_INDICATOR"), 1, " "))
    .withColumn("MEMBER_OPTIONAL_PHONE_CALL_INDICATOR", F.rpad(F.col("MEMBER_OPTIONAL_PHONE_CALL_INDICATOR"), 1, " "))
)

# Write the final file (CSeqFileStage) "Seq_IdsMbrCommExtr" to key directory
write_files(
    df_final,
    f"{adls_path}/key/CrmMbrCommExtr.MbrComm.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)