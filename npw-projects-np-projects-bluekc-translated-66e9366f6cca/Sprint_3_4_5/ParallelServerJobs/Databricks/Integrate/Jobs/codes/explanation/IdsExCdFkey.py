# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 08:28:47 Batch  14638_30532 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 11:16:35 Batch  14605_40601 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/21/07 14:41:36 Batch  14570_52900 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/21/07 12:23:43 Batch  14478_44630 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/21/07 15:26:34 Batch  14325_55600 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/26/06 07:54:36 Batch  13906_28483 INIT bckcetl ids20 dsadm Gina
# MAGIC ^1_11 09/26/05 09:38:06 Batch  13784_34693 PROMOTE bckcetl ids20 dsadm Gina Parr
# MAGIC ^1_11 09/26/05 09:33:27 Batch  13784_34412 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_11 09/23/05 09:42:33 Batch  13781_34962 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_11 09/23/05 09:39:19 Batch  13781_34764 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_10 09/23/05 08:27:20 Batch  13781_30444 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_9 09/23/05 08:22:17 Batch  13781_30141 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_8 09/20/05 15:13:43 Batch  13778_54827 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_7 09/20/05 15:06:38 Batch  13778_54403 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_6 09/13/05 13:35:31 Batch  13771_48935 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 09/12/05 16:01:51 Batch  13770_57714 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 09/12/05 15:45:16 Batch  13770_56719 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_3 09/12/05 15:38:28 Batch  13770_56313 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 09/09/05 16:34:40 Batch  13767_59682 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 09/09/05 16:10:39 Batch  13767_58243 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_6 03/21/05 19:59:41 Batch  13595_72003 PROMOTE bckcett VERSION u08717 Brent
# MAGIC ^1_6 03/21/05 19:43:38 Batch  13595_71024 INIT bckcett testIDS30 u08717 Brent
# MAGIC ^1_5 03/11/05 09:03:23 Batch  13585_32608 PROMOTE 191.168.1.20 VERSION u10157 sa
# MAGIC ^1_5 03/10/05 16:59:22 Batch  13584_61164 INIT 191.168.1.20 devlIDS30 u10157 sa
# MAGIC ^1_4 03/10/05 16:26:27 Batch  13584_59190 INIT 191.168.1.20 devlIDS30 u10157 sa
# MAGIC ^1_3 03/09/05 13:03:12 Batch  13583_46995 INIT 191.168.1.20 devlIDS30 u10157 sa
# MAGIC ^1_2 03/04/05 10:22:57 Batch  13578_37382 INIT bckccdt devlIDS30 u10157 sa
# MAGIC ^1_1 03/04/05 08:55:33 Batch  13578_32139 INIT bckccdt devlIDS30 u08717 Brent
# MAGIC ^1_7 03/03/05 16:03:41 Batch  13577_57825 INIT bckccdt devlIDS30 u10157 sa
# MAGIC ^1_6 03/03/05 11:25:12 Batch  13577_41125 INIT bckccdt devlIDS30 u10157 SA - move to testIDS30
# MAGIC ^1_4 12/28/04 14:40:47 Batch  13512_52857 PROMOTE bckccdt VERSION u08717 Brent
# MAGIC ^1_4 12/28/04 14:00:02 Batch  13512_50409 INIT bckcetl ids20 dsadm Brent
# MAGIC ^1_1 11/08/04 09:23:42 Batch  13462_33826 PROMOTE bckcetl VERSION dcg01 Ollie
# MAGIC ^1_1 11/08/04 09:16:05 Batch  13462_33423 INIT bckccdt testIDS21 u10913 Ollie
# MAGIC ^1_5 11/01/04 08:37:58 Batch  13455_31082 INIT bckccdt testIDS21 dsadm Gina Parr
# MAGIC ^1_3 10/20/04 11:42:29 Batch  13443_42156 PROMOTE bckccdt VERSION u08717 Brent
# MAGIC ^1_3 10/20/04 11:33:58 Batch  13443_41640 INIT bckccdt devIDS21 u08717 Brent
# MAGIC ^1_2 10/12/04 14:05:43 Batch  13435_50797 PROMOTE bckccdt VERSION u10913 Ollie move from production
# MAGIC ^1_3 09/28/04 14:22:43 Batch  13421_51769 PROMOTE bckcetl VERSIONIDS dsadm Gina Parr
# MAGIC ^1_3 09/28/04 14:14:54 Batch  13421_51299 PROMOTE bckcetl VERSIONIDS dsadm Gina Parr
# MAGIC ^1_3 09/28/04 14:11:04 Batch  13421_51069 INIT bckccdt testIDS20 dsadm Gina Parr
# MAGIC ^1_1 09/20/04 07:47:55 Batch  13413_28080 PROMOTE bckccdt VERSION u08717 Brent
# MAGIC ^1_1 09/20/04 07:45:44 Batch  13413_27949 INIT bckccdt devIDS20 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC ****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsExCdFkey
# MAGIC 
# MAGIC DESCRIPTION:     IDS Explanation Code  Data foreign key job.
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_excd    - this is both read from and written to.   
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  Trim 
# MAGIC 
# MAGIC BCBSKC TRANSFORMS:     GetFKey
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING: Takes the file  from primary key job and does foreign key lookups.
# MAGIC 
# MAGIC OUTPUTS:  Load file for table EXCD
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset just rerun. 
# MAGIC              PREVIOUS RUN ABORTED:         Normally nothing has to be done before restarting.  In some cases files in the working directories may have to be removed or recreated.                                                                           
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC            Judy Brammer 6/2004  -   Originally Programmed.
# MAGIC            Oliver Nielsen 10/2004 -  411 Conversion / IDS2.1
# MAGIC            Ralph Tucker - 01/13/2004 - Field name changes (EXCD_HIPAA_PROV_ADJ_CD_SK, EXCD_HIPAA_REMIT_REMARK_CD_SK);  Same location different name.

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, when, length, rpad, trim
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FilePath = get_widget_value("FilePath","")
OutFile = get_widget_value("OutFile","")
InFile = get_widget_value("InFile","")
Logging = get_widget_value("Logging","")

schema_ExCcCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("EXCD_SK", IntegerType(), False),
    StructField("EXCD_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("EXCD_HC_ADJ_CD", StringType(), False),
    StructField("EXCD_PT_LIAB_IND", StringType(), False),
    StructField("EXCD_PROV_ADJ_CD", StringType(), False),
    StructField("EXCD_REMIT_REMARK_CD", StringType(), False),
    StructField("EXCD_STS", StringType(), False),
    StructField("EXCD_TYPE", StringType(), False),
    StructField("EXCD_LONG_TX1", StringType(), True),
    StructField("EXCD_LONG_TX2", StringType(), True),
    StructField("EXCD_SH_TX", StringType(), True)
])

df_ExCcCrf = (
    spark.read
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_ExCcCrf)
    .csv(f"{adls_path}/key/{InFile}")
)

df_trn = (
    df_ExCcCrf
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("EXCD_SK")))
    .withColumn("SrcSysCdSk",
        GetFkeyCodes(
            trim(lit("IDS")),
            trim(col("EXCD_SK")),
            trim(lit("SOURCE SYSTEM")),
            trim(col("SRC_SYS_CD")),
            lit(Logging)
        )
    )
    .withColumn("EXCDHlthCareAdjRsnCdSk",
        when(length(trim(col("EXCD_HC_ADJ_CD"))) == 0,
            GetFkeyCodes(
                trim(lit("FACETS")),
                col("EXCD_SK"),
                trim(lit("EXPLANATION CODE HEALTHCARE ADJUSTMENT REASON")),
                trim(lit(" ")),
                lit(Logging)
            )
        ).otherwise(
            GetFkeyCodes(
                trim(lit("FACETS")),
                col("EXCD_SK"),
                trim(lit("EXPLANATION CODE HEALTHCARE ADJUSTMENT REASON")),
                trim(col("EXCD_HC_ADJ_CD")),
                lit(Logging)
            )
        )
    )
    .withColumn("EXCDLiabCdSk",
        when(length(trim(col("EXCD_PT_LIAB_IND"))) == 0,
            GetFkeyCodes(
                trim(lit("FACETS")),
                col("EXCD_SK"),
                trim(lit("EXPLANATION CODE LIABILITY")),
                trim(lit(" ")),
                lit(Logging)
            )
        ).otherwise(
            GetFkeyCodes(
                trim(lit("FACETS")),
                col("EXCD_SK"),
                trim(lit("EXPLANATION CODE LIABILITY")),
                trim(col("EXCD_PT_LIAB_IND")),
                lit(Logging)
            )
        )
    )
    .withColumn("EXCDProvAdjCdSk",
        GetFkeyCodes(
            trim(lit("FACETS")),
            col("EXCD_SK"),
            trim(lit("EXPLANATION CODE PROVIDER ADJUSTMENT")),
            trim(col("EXCD_PROV_ADJ_CD")),
            lit(Logging)
        )
    )
    .withColumn("EXCDRemitRemarkCdSk",
        GetFkeyCodes(
            trim(lit("FACETS")),
            col("EXCD_SK"),
            trim(lit("EXPLANATION CODE REMITTANCE REMARK")),
            trim(col("EXCD_REMIT_REMARK_CD")),
            lit(Logging)
        )
    )
    .withColumn("EXCDSttusCdSk",
        when(length(trim(col("EXCD_STS"))) == 0,
            GetFkeyCodes(
                trim(lit("FACETS")),
                col("EXCD_SK"),
                trim(lit("EXPLANATION CODE STATUS")),
                trim(lit(" ")),
                lit(Logging)
            )
        ).otherwise(
            GetFkeyCodes(
                trim(lit("FACETS")),
                col("EXCD_SK"),
                trim(lit("EXPLANATION CODE STATUS")),
                trim(col("EXCD_STS")),
                lit(Logging)
            )
        )
    )
    .withColumn("EXCDTypCdSk",
        when(length(trim(col("EXCD_TYPE"))) == 0,
            GetFkeyCodes(
                trim(lit("FACETS")),
                col("EXCD_SK"),
                trim(lit("EXPLANATION CODE TYPE")),
                trim(lit(" ")),
                lit(Logging)
            )
        ).otherwise(
            GetFkeyCodes(
                trim(lit("FACETS")),
                col("EXCD_SK"),
                trim(lit("EXPLANATION CODE TYPE")),
                trim(col("EXCD_TYPE")),
                lit(Logging)
            )
        )
    )
)

df_lnkRows = (
    df_trn
    .filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
    .select(
        col("EXCD_SK").alias("EXCD_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        when(col("EXCD_ID").isNull(), lit("    ")).otherwise(col("EXCD_ID")).alias("EXCD_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("EXCDHlthCareAdjRsnCdSk").alias("EXCD_HLTHCARE_ADJ_RSN_CD_SK"),
        col("EXCDLiabCdSk").alias("EXCD_LIAB_CD_SK"),
        col("EXCDProvAdjCdSk").alias("EXCD_HIPPA_PROV_ADJ_CD_SK"),
        col("EXCDRemitRemarkCdSk").alias("EXCD_HIPPA_REMIT_REMARK_CD_SK"),
        col("EXCDSttusCdSk").alias("EXCD_STTUS_CD_SK"),
        col("EXCDTypCdSk").alias("EXCD_TYP_CD_SK"),
        col("EXCD_LONG_TX1").alias("EXCD_LONG_TX1"),
        col("EXCD_LONG_TX2").alias("EXCD_LONG_TX2"),
        col("EXCD_SH_TX").alias("EXCD_SH_TX")
    )
)

df_lnkRecycle = (
    df_trn
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("EXCD_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("EXCD_SK").alias("EXCD_SK"),
        col("EXCD_ID").alias("EXCD_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("EXCD_HC_ADJ_CD").alias("EXCD_HC_ADJ_CD"),
        col("EXCD_PT_LIAB_IND").alias("EXCD_PT_LIAB_IND"),
        col("EXCD_PROV_ADJ_CD").alias("EXCD_PROV_ADJ_CD"),
        col("EXCD_REMIT_REMARK_CD").alias("EXCD_REMIT_REMARK_CD"),
        col("EXCD_STS").alias("EXCD_STS"),
        col("EXCD_TYPE").alias("EXCD_TYPE"),
        col("EXCD_LONG_TX1").alias("EXCD_LONG_TX1"),
        col("EXCD_LONG_TX2").alias("EXCD_LONG_TX2"),
        col("EXCD_SH_TX").alias("EXCD_SH_TX")
    )
)

df_lnkRecycle_write = (
    df_lnkRecycle
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("EXCD_ID", rpad(col("EXCD_ID"), 3, " "))
    .withColumn("EXCD_HC_ADJ_CD", rpad(col("EXCD_HC_ADJ_CD"), 5, " "))
    .withColumn("EXCD_PT_LIAB_IND", rpad(col("EXCD_PT_LIAB_IND"), 1, " "))
    .withColumn("EXCD_PROV_ADJ_CD", rpad(col("EXCD_PROV_ADJ_CD"), 2, " "))
    .withColumn("EXCD_REMIT_REMARK_CD", rpad(col("EXCD_REMIT_REMARK_CD"), 2, " "))
    .withColumn("EXCD_STS", rpad(col("EXCD_STS"), 1, " "))
    .withColumn("EXCD_TYPE", rpad(col("EXCD_TYPE"), 2, " "))
)

write_files(
    df_lnkRecycle_write.select(
        "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN","FIRST_RECYC_DT","ERR_CT","RECYCLE_CT","SRC_SYS_CD","PRI_KEY_STRING","EXCD_SK","EXCD_ID","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","EXCD_HC_ADJ_CD","EXCD_PT_LIAB_IND","EXCD_PROV_ADJ_CD","EXCD_REMIT_REMARK_CD","EXCD_STS","EXCD_TYPE","EXCD_LONG_TX1","EXCD_LONG_TX2","EXCD_SH_TX"
    ),
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

schema_collRows = StructType([
    StructField("EXCD_SK", IntegerType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("EXCD_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("EXCD_HLTHCARE_ADJ_RSN_CD_SK", IntegerType(), True),
    StructField("EXCD_LIAB_CD_SK", IntegerType(), True),
    StructField("EXCD_HIPPA_PROV_ADJ_CD_SK", IntegerType(), True),
    StructField("EXCD_HIPPA_REMIT_REMARK_CD_SK", IntegerType(), True),
    StructField("EXCD_STTUS_CD_SK", IntegerType(), True),
    StructField("EXCD_TYP_CD_SK", IntegerType(), True),
    StructField("EXCD_LONG_TX1", StringType(), True),
    StructField("EXCD_LONG_TX2", StringType(), True),
    StructField("EXCD_SH_TX", StringType(), True)
])

df_DefaultUNK = spark.createDataFrame([
    (0, 0, "UNK", 0, 0, 0, 0, 0, 0, 0, 0, "UNK", "UNK", "UNK")
], schema_collRows)

df_DefaultNA = spark.createDataFrame([
    (1, 1, "NA", 1, 1, 1, 1, 1, 1, 1, 1, "NA", "NA", "NA")
], schema_collRows)

df_collRows = df_lnkRows.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

df_ExCdFkeyOut = (
    df_collRows
    .withColumn("EXCD_ID", rpad(col("EXCD_ID"), 4, " "))
    .select(
        "EXCD_SK",
        "SRC_SYS_CD_SK",
        "EXCD_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "EXCD_HLTHCARE_ADJ_RSN_CD_SK",
        "EXCD_LIAB_CD_SK",
        "EXCD_HIPPA_PROV_ADJ_CD_SK",
        "EXCD_HIPPA_REMIT_REMARK_CD_SK",
        "EXCD_STTUS_CD_SK",
        "EXCD_TYP_CD_SK",
        "EXCD_LONG_TX1",
        "EXCD_LONG_TX2",
        "EXCD_SH_TX"
    )
)

write_files(
    df_ExCdFkeyOut,
    f"{adls_path}/load/{OutFile}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)