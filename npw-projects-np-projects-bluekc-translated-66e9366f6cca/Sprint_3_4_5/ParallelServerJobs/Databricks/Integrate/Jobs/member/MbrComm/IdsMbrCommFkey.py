# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Bhoomi Dasari         2010-09-23       3035 - DigiComm    New ETL Development                                                               IntegrateCurDevl            Steph Goddard              09/27/2010
# MAGIC Kalyan Neelam      2013-05-07          4841 -                         Updated the table layout, added some new columns              IntegrateNewDevl        Bhoomi Dasari                5/12/2013
# MAGIC                                                   Digital Communications          and removed exisitng
# MAGIC                                                
# MAGIC 
# MAGIC Michael Harmon     2016-11-111      5541 Shop To Enroll       Added 3 new email address fields, OTHR_EMAIL_ADDR              IDS\\Dev1          Kalyan Neelam          2016-11-17
# MAGIC                                                                                                 FCTS_EMAIL_ADDR and WEB_RGSTRN_EMAIL_ADDR
# MAGIC 
# MAGIC HariKrishnaRao Yadav  2021-08-27 US 420239                    Applied Tranformation logic for Columns MBR_SK,                     IntegrateSITF         Jeyaprasanna           2022-01-31
# MAGIC                                                                                                MYCLM_EOB_PRFRNC_CD_SK, RCRD_STTUS_CD_SK 
# MAGIC                                                                                                and STTUS_RSN_CD_SK in Stage Business logic
# MAGIC 
# MAGIC Praneeth K                2022-01-25             469607                     Added 5 new Member fields                                                    EnterpriseDev2       Jeyaprasanna           2022-01-31
# MAGIC 
# MAGIC Vamsi Aripaka           2023-12-21             607496                    Added new field MBR_OPTNL_PHN_CALL_IN                         IntegrateDev1       Jeyaprasanna           2024-01-17
# MAGIC                                                                                                  from source to target.
# MAGIC 
# MAGIC Ediga Maruthi          2024-06-18               621348          Removed default 'N' values in PurgeTrn stage and allowed nulls for the fileds      IntegrateDev2       Jeyaprasanna    2024-06-24
# MAGIC                                                                                        MBR_EDUC_EMAIL_IN,MBR_EDUC_POSTAL_IN,MBR_RQRD_EMAIL_IN,
# MAGIC                                                                                        MBR_RQRD_POSTAL_IN,MBR_GNRC_SMS_IN,MBR_OPTNL_PHN_CALL_IN
# MAGIC                                                                                        from source to target.

# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','')
Logging = get_widget_value('Logging','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_MbrComm = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("MBR_COMM_SK", IntegerType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("MYCLM_EOB_PRFRNC_CD", StringType(), nullable=False),
    StructField("RCRD_STTUS_CD", StringType(), nullable=False),
    StructField("STTUS_RSN_CD", StringType(), nullable=False),
    StructField("AUTO_PAY_NTFCTN_IN", StringType(), nullable=False),
    StructField("BLUELOOP_PARTCPN_IN", StringType(), nullable=False),
    StructField("HLTH_WELNS_DO_NOT_SEND_IN", StringType(), nullable=False),
    StructField("HLTH_WELNS_EMAIL_IN", StringType(), nullable=False),
    StructField("HLTH_WELNS_POSTAL_IN", StringType(), nullable=False),
    StructField("HLTH_WELNS_SMS_IN", StringType(), nullable=False),
    StructField("MBR_PRFRNCS_SET_IN", StringType(), nullable=False),
    StructField("MYBLUEKC_ELTRNC_BILL_EMAIL_IN", StringType(), nullable=False),
    StructField("MYBLUEKC_ELTRNC_BILL_SMS_IN", StringType(), nullable=False),
    StructField("MYBLUEKC_PRT_BILL_EMAIL_IN", StringType(), nullable=False),
    StructField("MYBLUEKC_PRT_BILL_POSTAL_IN", StringType(), nullable=False),
    StructField("MYBLUEKC_PRT_BILL_SMS_IN", StringType(), nullable=False),
    StructField("MYCLM_EMAIL_IN", StringType(), nullable=False),
    StructField("MYCLM_POSTAL_IN", StringType(), nullable=False),
    StructField("MYCLM_SMS_IN", StringType(), nullable=False),
    StructField("MYPLN_INFO_EMAIL_IN", StringType(), nullable=False),
    StructField("MYPLN_INFO_POSTAL_IN", StringType(), nullable=False),
    StructField("MYPLN_INFO_SMS_IN", StringType(), nullable=False),
    StructField("PROD_SVC_DO_NOT_SEND_IN", StringType(), nullable=False),
    StructField("PROD_SVC_EMAIL_IN", StringType(), nullable=False),
    StructField("PROD_SVC_POSTAL_IN", StringType(), nullable=False),
    StructField("PROD_SVC_SMS_IN", StringType(), nullable=False),
    StructField("SRC_SYS_UPDT_DTM", TimestampType(), nullable=False),
    StructField("CELL_PHN_NO", StringType(), nullable=False),
    StructField("EMAIL_ADDR", StringType(), nullable=False),
    StructField("OTHR_EMAIL_ADDR", StringType(), nullable=False),
    StructField("FCTS_EMAIL_ADDR", StringType(), nullable=False),
    StructField("WEB_RGSTRN_EMAIL_ADDR", StringType(), nullable=False),
    StructField("MEMBER_EDUCATIONAL_EMAIL_INDICATOR", StringType(), nullable=True),
    StructField("MEMBER_EDUCATIONAL_POSTAL_INDICATOR", StringType(), nullable=True),
    StructField("MEMBER_REQUIRED_EMAIL_INDICATOR", StringType(), nullable=True),
    StructField("MEMBER_REQUIRED_POSTAL_INDICATOR", StringType(), nullable=True),
    StructField("MEMBER_GENERIC_SMS_INDICATOR", StringType(), nullable=True),
    StructField("MEMBER_OPTIONAL_PHONE_CALL_INDICATOR", StringType(), nullable=True)
])

df_MbrComm = (
    spark.read
    .option("header", False)
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_MbrComm)
    .csv(f"{adls_path}/key/{InFile}")
)

df_MbrComm = (
    df_MbrComm
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svMbrSk", GetFkeyMbr('FACETS', F.col("MBR_COMM_SK"), F.col("MBR_UNIQ_KEY"), Logging))
    .withColumn("svMyClmEobPrfrncCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("MBR_COMM_SK"), F.lit("EOB CONTACT PREFERENCE"), F.col("MYCLM_EOB_PRFRNC_CD"), Logging))
    .withColumn("svRcrdSttusCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("MBR_COMM_SK"), F.lit("CONTACT RECORD STATUS"), F.col("RCRD_STTUS_CD"), Logging))
    .withColumn("svSttusRsnCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("MBR_COMM_SK"), F.lit("CONTACT STATUS REASON"), F.col("STTUS_RSN_CD"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("MBR_COMM_SK")))
)

df_MbrCommFkeyOut = (
    df_MbrComm
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("MBR_COMM_SK").alias("MBR_COMM_SK"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when(
            (F.col("svMbrSk").isNull()) |
            (F.length(F.col("svMbrSk")) == 0) |
            (F.col("svMbrSk") == 0),
            F.lit(1)
        ).otherwise(F.col("svMbrSk")).alias("MBR_SK"),
        F.when(
            (F.col("svMyClmEobPrfrncCdSk").isNull()) |
            (F.length(F.col("svMyClmEobPrfrncCdSk")) == 0) |
            (F.col("svMyClmEobPrfrncCdSk") == 0),
            F.lit(1)
        ).otherwise(F.col("svMyClmEobPrfrncCdSk")).alias("MYCLM_EOB_PRFRNC_CD_SK"),
        F.when(
            (F.col("svRcrdSttusCdSk").isNull()) |
            (F.length(F.col("svRcrdSttusCdSk")) == 0) |
            (F.col("svRcrdSttusCdSk") == 0),
            F.lit(1)
        ).otherwise(F.col("svRcrdSttusCdSk")).alias("RCRD_STTUS_CD_SK"),
        F.when(
            (F.col("svSttusRsnCdSk").isNull()) |
            (F.length(F.col("svSttusRsnCdSk")) == 0) |
            (F.col("svSttusRsnCdSk") == 0),
            F.lit(1)
        ).otherwise(F.col("svSttusRsnCdSk")).alias("STTUS_RSN_CD_SK"),
        F.col("AUTO_PAY_NTFCTN_IN").alias("AUTO_PAY_NTFCTN_IN"),
        F.col("BLUELOOP_PARTCPN_IN").alias("BLUELOOP_PARTCPN_IN"),
        F.col("HLTH_WELNS_DO_NOT_SEND_IN").alias("HLTH_WELNS_DO_NOT_SEND_IN"),
        F.col("HLTH_WELNS_EMAIL_IN").alias("HLTH_WELNS_EMAIL_IN"),
        F.col("HLTH_WELNS_POSTAL_IN").alias("HLTH_WELNS_POSTAL_IN"),
        F.col("HLTH_WELNS_SMS_IN").alias("HLTH_WELNS_SMS_IN"),
        F.col("MBR_PRFRNCS_SET_IN").alias("MBR_PRFRNCS_SET_IN"),
        F.col("MYBLUEKC_ELTRNC_BILL_EMAIL_IN").alias("MYBLUEKC_ELTRNC_BILL_EMAIL_IN"),
        F.col("MYBLUEKC_ELTRNC_BILL_SMS_IN").alias("MYBLUEKC_ELTRNC_BILL_SMS_IN"),
        F.col("MYBLUEKC_PRT_BILL_EMAIL_IN").alias("MYBLUEKC_PRT_BILL_EMAIL_IN"),
        F.col("MYBLUEKC_PRT_BILL_POSTAL_IN").alias("MYBLUEKC_PRT_BILL_POSTAL_IN"),
        F.col("MYBLUEKC_PRT_BILL_SMS_IN").alias("MYBLUEKC_PRT_BILL_SMS_IN"),
        F.col("MYCLM_EMAIL_IN").alias("MYCLM_EMAIL_IN"),
        F.col("MYCLM_POSTAL_IN").alias("MYCLM_POSTAL_IN"),
        F.col("MYCLM_SMS_IN").alias("MYCLM_SMS_IN"),
        F.col("MYPLN_INFO_EMAIL_IN").alias("MYPLN_INFO_EMAIL_IN"),
        F.col("MYPLN_INFO_POSTAL_IN").alias("MYPLN_INFO_POSTAL_IN"),
        F.col("MYPLN_INFO_SMS_IN").alias("MYPLN_INFO_SMS_IN"),
        F.col("PROD_SVC_DO_NOT_SEND_IN").alias("PROD_SVC_DO_NOT_SEND_IN"),
        F.col("PROD_SVC_EMAIL_IN").alias("PROD_SVC_EMAIL_IN"),
        F.col("PROD_SVC_POSTAL_IN").alias("PROD_SVC_POSTAL_IN"),
        F.col("PROD_SVC_SMS_IN").alias("PROD_SVC_SMS_IN"),
        F.col("SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
        F.col("CELL_PHN_NO").alias("CELL_PHN_NO"),
        F.col("EMAIL_ADDR").alias("EMAIL_ADDR"),
        F.col("OTHR_EMAIL_ADDR").alias("OTHR_EMAIL_ADDR"),
        F.col("FCTS_EMAIL_ADDR").alias("FCTS_EMAIL_ADDR"),
        F.col("WEB_RGSTRN_EMAIL_ADDR").alias("WEB_RGSTRN_EMAIL_ADDR"),
        F.col("MEMBER_EDUCATIONAL_EMAIL_INDICATOR").alias("MBR_EDUC_EMAIL_IN"),
        F.col("MEMBER_EDUCATIONAL_POSTAL_INDICATOR").alias("MBR_EDUC_POSTAL_IN"),
        F.col("MEMBER_REQUIRED_EMAIL_INDICATOR").alias("MBR_RQRD_EMAIL_IN"),
        F.col("MEMBER_REQUIRED_POSTAL_INDICATOR").alias("MBR_RQRD_POSTAL_IN"),
        F.col("MEMBER_GENERIC_SMS_INDICATOR").alias("MBR_GNRC_SMS_IN"),
        F.col("MEMBER_OPTIONAL_PHONE_CALL_INDICATOR").alias("MBR_OPTNL_PHN_CALL_IN")
    )
)

df_lnkRecycle = (
    df_MbrComm
    .filter(F.col("ErrCount") > 0)
    .select(
        F.expr("GetRecycleKey(MBR_COMM_SK)").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ERR_CT").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("MBR_COMM_SK").alias("MBR_COMM_SK"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("MYCLM_EOB_PRFRNC_CD").alias("MYCLM_EOB_PRFRNC_CD"),
        F.col("RCRD_STTUS_CD").alias("RCRD_STTUS_CD"),
        F.col("STTUS_RSN_CD").alias("STTUS_RSN_CD"),
        F.col("AUTO_PAY_NTFCTN_IN").alias("AUTO_PAY_NTFCTN_IN"),
        F.col("BLUELOOP_PARTCPN_IN").alias("BLUELOOP_PARTCPN_IN"),
        F.col("HLTH_WELNS_DO_NOT_SEND_IN").alias("HLTH_WELNS_DO_NOT_SEND_IN"),
        F.col("HLTH_WELNS_EMAIL_IN").alias("HLTH_WELNS_EMAIL_IN"),
        F.col("HLTH_WELNS_POSTAL_IN").alias("HLTH_WELNS_POSTAL_IN"),
        F.col("HLTH_WELNS_SMS_IN").alias("HLTH_WELNS_SMS_IN"),
        F.col("MBR_PRFRNCS_SET_IN").alias("MBR_PRFRNCS_SET_IN"),
        F.col("MYBLUEKC_ELTRNC_BILL_EMAIL_IN").alias("MYBLUEKC_ELTRNC_BILL_EMAIL_IN"),
        F.col("MYBLUEKC_ELTRNC_BILL_SMS_IN").alias("MYBLUEKC_ELTRNC_BILL_SMS_IN"),
        F.col("MYBLUEKC_PRT_BILL_EMAIL_IN").alias("MYBLUEKC_PRT_BILL_EMAIL_IN"),
        F.col("MYBLUEKC_PRT_BILL_POSTAL_IN").alias("MYBLUEKC_PRT_BILL_POSTAL_IN"),
        F.col("MYBLUEKC_PRT_BILL_SMS_IN").alias("MYBLUEKC_PRT_BILL_SMS_IN"),
        F.col("MYCLM_EMAIL_IN").alias("MYCLM_EMAIL_IN"),
        F.col("MYCLM_POSTAL_IN").alias("MYCLM_POSTAL_IN"),
        F.col("MYCLM_SMS_IN").alias("MYCLM_SMS_IN"),
        F.col("MYPLN_INFO_EMAIL_IN").alias("MYPLN_INFO_EMAIL_IN"),
        F.col("MYPLN_INFO_POSTAL_IN").alias("MYPLN_INFO_POSTAL_IN"),
        F.col("MYPLN_INFO_SMS_IN").alias("MYPLN_INFO_SMS_IN"),
        F.col("PROD_SVC_DO_NOT_SEND_IN").alias("PROD_SVC_DO_NOT_SEND_IN"),
        F.col("PROD_SVC_EMAIL_IN").alias("PROD_SVC_EMAIL_IN"),
        F.col("PROD_SVC_POSTAL_IN").alias("PROD_SVC_POSTAL_IN"),
        F.col("PROD_SVC_SMS_IN").alias("PROD_SVC_SMS_IN"),
        F.col("SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
        F.col("CELL_PHN_NO").alias("CELL_PHN_NO"),
        F.col("EMAIL_ADDR").alias("EMAIL_ADDR"),
        F.col("OTHR_EMAIL_ADDR").alias("OTHR_EMAIL_ADDR"),
        F.col("FCTS_EMAIL_ADDR").alias("FCTS_EMAIL_ADDR"),
        F.col("WEB_RGSTRN_EMAIL_ADDR").alias("WEB_RGSTRN_EMAIL_ADDR"),
        F.col("MEMBER_EDUCATIONAL_EMAIL_INDICATOR").alias("MEMBER_EDUCATIONAL_EMAIL_INDICATOR"),
        F.col("MEMBER_EDUCATIONAL_POSTAL_INDICATOR").alias("MEMBER_EDUCATIONAL_POSTAL_INDICATOR"),
        F.col("MEMBER_REQUIRED_EMAIL_INDICATOR").alias("MEMBER_REQUIRED_EMAIL_INDICATOR"),
        F.col("MEMBER_REQUIRED_POSTAL_INDICATOR").alias("MEMBER_REQUIRED_POSTAL_INDICATOR"),
        F.col("MEMBER_GENERIC_SMS_INDICATOR").alias("MEMBER_GENERIC_SMS_INDICATOR"),
        F.col("MEMBER_OPTIONAL_PHONE_CALL_INDICATOR").alias("MEMBER_OPTIONAL_PHONE_CALL_INDICATOR")
    )
)

write_files(
    df_lnkRecycle,
    f"{adls_path}/hf_recycle.parquet",
    ',',
    'overwrite',
    True,
    True,
    '\"',
    None
)

columns_Collector = [
    "MBR_COMM_SK",
    "MBR_UNIQ_KEY",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_SK",
    "MYCLM_EOB_PRFRNC_CD_SK",
    "RCRD_STTUS_CD_SK",
    "STTUS_RSN_CD_SK",
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
    "MBR_EDUC_EMAIL_IN",
    "MBR_EDUC_POSTAL_IN",
    "MBR_RQRD_EMAIL_IN",
    "MBR_RQRD_POSTAL_IN",
    "MBR_GNRC_SMS_IN",
    "MBR_OPTNL_PHN_CALL_IN"
]

# DefaultUNK (one row)
df_DefaultUNK = spark.createDataFrame(
    [
        (
            0, 0, 0, 0, 0, 0, 0, 0, 0,
            'N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N',
            '1753-01-01 00:00:00','UNK','UNK','UNK','UNK','UNK',
            '', '', '', '', '', ''
        )
    ],
    columns_Collector
)

# DefaultNA (one row)
df_DefaultNA = spark.createDataFrame(
    [
        (
            1, 1, 1, 1, 1, 1, 1, 1, 1,
            'N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N',
            '1753-01-01 00:00:00','NA','NA','NA','NA','NA',
            '', '', '', '', '', ''
        )
    ],
    columns_Collector
)

df_Collector = df_MbrCommFkeyOut.select(columns_Collector).union(df_DefaultUNK.select(columns_Collector)).union(df_DefaultNA.select(columns_Collector))

df_final = df_Collector
for c in df_final.schema.fields:
    if c.dataType == StringType():
        col_name = c.name
        # check if it is char(1) or char(10) from the final stage metadata
        # length=1 for columns with "SqlType": "char" and "Length": "1"
        # length=10 if "INSRT_UPDT_CD"
        # here in final stage, all char columns have length 1 except none is length=10 in the final output
        if col_name in [
            "AUTO_PAY_NTFCTN_IN", "BLUELOOP_PARTCPN_IN", "HLTH_WELNS_DO_NOT_SEND_IN", "HLTH_WELNS_EMAIL_IN",
            "HLTH_WELNS_POSTAL_IN", "HLTH_WELNS_SMS_IN", "MBR_PRFRNCS_SET_IN",
            "MYBLUEKC_ELTRNC_BILL_EMAIL_IN", "MYBLUEKC_ELTRNC_BILL_SMS_IN",
            "MYBLUEKC_PRT_BILL_EMAIL_IN", "MYBLUEKC_PRT_BILL_POSTAL_IN", "MYBLUEKC_PRT_BILL_SMS_IN",
            "MYCLM_EMAIL_IN", "MYCLM_POSTAL_IN", "MYCLM_SMS_IN", "MYPLN_INFO_EMAIL_IN",
            "MYPLN_INFO_POSTAL_IN", "MYPLN_INFO_SMS_IN", "PROD_SVC_DO_NOT_SEND_IN",
            "PROD_SVC_EMAIL_IN", "PROD_SVC_POSTAL_IN", "PROD_SVC_SMS_IN",
            "MBR_EDUC_EMAIL_IN", "MBR_EDUC_POSTAL_IN", "MBR_RQRD_EMAIL_IN",
            "MBR_RQRD_POSTAL_IN", "MBR_GNRC_SMS_IN", "MBR_OPTNL_PHN_CALL_IN"
        ]:
            df_final = df_final.withColumn(col_name, F.rpad(F.col(col_name), 1, ' '))

write_files(
    df_final.select(columns_Collector),
    f"{adls_path}/load/MBR_COMM.dat",
    ',',
    'overwrite',
    False,
    False,
    '\"',
    None
)