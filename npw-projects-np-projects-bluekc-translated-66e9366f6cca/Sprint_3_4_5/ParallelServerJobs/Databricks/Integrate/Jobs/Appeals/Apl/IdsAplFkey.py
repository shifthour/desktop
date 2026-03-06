# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 20018 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Takes the primary key file and applies the foreign keys.   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date               Change Description                                                                           Project #           Development Project     Code Reviewer          Date Reviewed  
# MAGIC --------------------------   -------------------    -------------------------------------------------------------------------------------------------------   ----------------------   ----------------------------------    ----------------------------      -------------------------
# MAGIC Bhoomi Dasari      07/11/2007    Initial program                                                                                    3028                 devlIDS30                      Steph Goddard          8/23/07
# MAGIC Ravi Singh           09/07/2018    Updated stage variables for fetch FACETS data                               MCAS- 5796     Intergrate Dev1              Hugh Sisson              2018-09-13
# MAGIC Ravi Singh           09/30/2018    Update the stage variable logic for Fkey fetching records for  
# MAGIC                                                     CLS_PLN_PROD_CAT_CD_SK  column from Facets                      MCAS- 5796     Intergrate Dev1               Kalyan Neelam           2018-10-01

# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
Logging = get_widget_value("Logging", "X")
InFile = get_widget_value("InFile", "")

# Read "IdsAplExtr" (CSeqFileStage)
schema_IdsAplExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("APL_SK", IntegerType(), False),
    StructField("APL_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CRT_USER_SK", IntegerType(), False),
    StructField("CUR_PRI_USER_SK", IntegerType(), False),
    StructField("CUR_SEC_USER_SK", IntegerType(), False),
    StructField("CUR_TRTY_USER_SK", IntegerType(), False),
    StructField("GRP_SK", IntegerType(), False),
    StructField("LAST_UPDT_USER_SK", IntegerType(), False),
    StructField("MBR_SK", IntegerType(), False),
    StructField("PROD_SK", IntegerType(), False),
    StructField("PROD_SH_NM_SK", IntegerType(), False),
    StructField("SUBGRP_SK", IntegerType(), False),
    StructField("SUB_SK", IntegerType(), False),
    StructField("APL_CAT_CD_SK", IntegerType(), False),
    StructField("APL_CUR_DCSN_CD_SK", IntegerType(), False),
    StructField("APL_CUR_STTUS_CD_SK", IntegerType(), False),
    StructField("APL_INITN_METH_CD_SK", IntegerType(), False),
    StructField("APL_MBR_HOME_ADDR_ST_CD_SK", IntegerType(), False),
    StructField("APL_SUBTYP_CD_SK", IntegerType(), False),
    StructField("APL_TYP_CD_SK", IntegerType(), False),
    StructField("CLS_PLN_PROD_CAT_CD_SK", IntegerType(), False),
    StructField("CUR_APL_LVL_CD_SK", IntegerType(), False),
    StructField("CUR_APL_LVL_EXPDTD_IN", StringType(), False),
    StructField("CRT_DTM", TimestampType(), False),
    StructField("CUR_STTUS_DTM", TimestampType(), False),
    StructField("END_DT_SK", StringType(), False),
    StructField("INITN_DT_SK", StringType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("NEXT_RVW_DT_SK", StringType(), False),
    StructField("CUR_APL_LVL_SEQ_NO", IntegerType(), False),
    StructField("CUR_STTUS_SEQ_NO", IntegerType(), False),
    StructField("NEXT_RVW_INTRVL_NO", IntegerType(), False),
    StructField("APL_DESC", StringType(), True),
    StructField("APL_SUM_DESC", StringType(), True),
])

df_IdsAplExtr = (
    spark.read
    .option("header", "false")
    .option("quote", '"')
    .option("delimiter", ",")
    .schema(schema_IdsAplExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

# Add stage-variable columns in "ForeignKey" (CTransformerStage)
df_transform = (
    df_IdsAplExtr
    .withColumn(
        "ErrCount",
        GetFkeyErrorCnt(col("APL_SK"))
    )
    .withColumn(
        "PassThru",
        col("PASS_THRU_IN")
    )
    .withColumn(
        "svCrtUsrSk",
        when(col("SRC_SYS_CD") == lit("FACETS"),
             GetFkeyAppUsr(lit("FACETS"), col("APL_SK"), col("CRT_USER_SK"), lit(Logging)))
        .otherwise(col("CRT_USER_SK"))
    )
    .withColumn(
        "svCurPriUsrSk",
        when(col("SRC_SYS_CD") == lit("FACETS"),
             GetFkeyAppUsr(lit("FACETS"), col("APL_SK"), col("CUR_PRI_USER_SK"), lit(Logging)))
        .otherwise(col("CUR_PRI_USER_SK"))
    )
    .withColumn(
        "svCurSecUsrSk",
        GetFkeyAppUsr(lit("FACETS"), col("APL_SK"), col("CUR_SEC_USER_SK"), lit(Logging))
    )
    .withColumn(
        "svCurTrtyUsrSk",
        GetFkeyAppUsr(lit("FACETS"), col("APL_SK"), col("CUR_TRTY_USER_SK"), lit(Logging))
    )
    .withColumn(
        "svGrpSk",
        GetFkeyGrp(lit("FACETS"), col("APL_SK"), col("GRP_SK"), lit(Logging))
    )
    .withColumn(
        "svLstUpdtUsrSk",
        when(col("SRC_SYS_CD") == lit("FACETS"),
             GetFkeyAppUsr(lit("FACETS"), col("APL_SK"), col("LAST_UPDT_USER_SK"), lit(Logging)))
        .otherwise(col("CRT_USER_SK"))
    )
    .withColumn(
        "svMbrSk",
        GetFkeyMbr(lit("FACETS"), col("APL_SK"), col("MBR_SK"), lit(Logging))
    )
    .withColumn(
        "svSubGrpSk",
        GetFkeySubgrp(lit("FACETS"), col("APL_SK"), col("GRP_SK"), col("SUBGRP_SK"), lit(Logging))
    )
    .withColumn(
        "svSubSk",
        GetFkeySub(lit("FACETS"), col("APL_SK"), col("SUB_SK"), lit(Logging))
    )
    .withColumn(
        "svEndDtSk",
        GetFkeyDate(lit("IDS"), col("APL_SK"), col("END_DT_SK"), lit(Logging))
    )
    .withColumn(
        "svInitDtSk",
        GetFkeyDate(lit("IDS"), col("APL_SK"), col("INITN_DT_SK"), lit(Logging))
    )
    .withColumn(
        "svNextRvwDtSk",
        GetFkeyDate(lit("IDS"), col("APL_SK"), col("NEXT_RVW_DT_SK"), lit(Logging))
    )
    .withColumn(
        "svAplCatCdSk",
        GetFkeyAplCatCode(col("SRC_SYS_CD"), col("APL_SK"), lit("APPEAL CATEGORY"), col("APL_CAT_CD_SK"), lit(Logging))
    )
    .withColumn(
        "svAplCurDcsnCdSk",
        GetFkeyCodes(col("SRC_SYS_CD"), col("APL_SK"), lit("APPEAL LEVEL DECISION"), col("APL_CUR_DCSN_CD_SK"), lit(Logging))
    )
    .withColumn(
        "svAplCurStusCdSk",
        GetFkeyCodes(col("SRC_SYS_CD"), col("APL_SK"), lit("APPEAL STATUS"), col("APL_CUR_STTUS_CD_SK"), lit(Logging))
    )
    .withColumn(
        "svAplInitMethCdSk",
        GetFkeyCodes(col("SRC_SYS_CD"), col("APL_SK"), lit("APPEAL INITIATION METHOD"), col("APL_INITN_METH_CD_SK"), lit(Logging))
    )
    .withColumn(
        "svAplMbrHmAddStCdSk",
        when(col("SRC_SYS_CD") == lit("MEDSLTNS"),
             GetFkeyCodes(lit("IDS"), col("APL_SK"), lit("STATE"), col("APL_MBR_HOME_ADDR_ST_CD_SK"), lit(Logging)))
        .when((col("SRC_SYS_CD") == lit("NDBH")) | (col("SRC_SYS_CD") == lit("TELLIGEN")),
              col("APL_MBR_HOME_ADDR_ST_CD_SK"))
        .otherwise(
            GetFkeyCodes(col("SRC_SYS_CD"), col("APL_SK"), lit("STATE"), col("APL_MBR_HOME_ADDR_ST_CD_SK"), lit(Logging))
        )
    )
    .withColumn(
        "svAplSubtypCdSk",
        GetFkeyCodes(col("SRC_SYS_CD"), col("APL_SK"), lit("APPEAL SUBTYPE"), col("APL_SUBTYP_CD_SK"), lit(Logging))
    )
    .withColumn(
        "svAplTypCdSk",
        GetFkeyCodes(col("SRC_SYS_CD"), col("APL_SK"), lit("APPEAL TYPE"), col("APL_TYP_CD_SK"), lit(Logging))
    )
    .withColumn(
        "svClsPlnProdCdSk",
        when(
            (col("SRC_SYS_CD") == lit("MEDSLTNS")) | (col("SRC_SYS_CD") == lit("NDBH")) | (col("SRC_SYS_CD") == lit("TELLIGEN")),
            col("CLS_PLN_PROD_CAT_CD_SK")
        ).otherwise(
            GetFkeyCodes(col("SRC_SYS_CD"), col("APL_SK"), lit("CLASS PLAN PRODUCT CATEGORY"), col("CLS_PLN_PROD_CAT_CD_SK"), lit(Logging))
        )
    )
    .withColumn(
        "svCurAplLvlCdSk",
        GetFkeyCodes(col("SRC_SYS_CD"), col("APL_SK"), lit("APPEAL LEVEL CODE"), col("CUR_APL_LVL_CD_SK"), lit(Logging))
    )
)

# "Fkey" link => Constraint: (ErrCount = 0 OR PassThru = 'Y')
df_Fkey = (
    df_transform
    .filter((col("ErrCount") == lit(0)) | (col("PassThru") == lit("Y")))
    .select(
        col("APL_SK").alias("APL_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("APL_ID").alias("APL_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svCrtUsrSk").alias("CRT_USER_SK"),
        col("svCurPriUsrSk").alias("CUR_PRI_USER_SK"),
        col("svCurSecUsrSk").alias("CUR_SEC_USER_SK"),
        col("svCurTrtyUsrSk").alias("CUR_TRTY_USER_SK"),
        col("svGrpSk").alias("GRP_SK"),
        col("svLstUpdtUsrSk").alias("LAST_UPDT_USER_SK"),
        col("svMbrSk").alias("MBR_SK"),
        col("PROD_SK").alias("PROD_SK"),
        col("PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
        col("svSubGrpSk").alias("SUBGRP_SK"),
        col("svSubSk").alias("SUB_SK"),
        col("svAplCatCdSk").alias("APL_CAT_CD_SK"),
        col("svAplCurDcsnCdSk").alias("APL_CUR_DCSN_CD_SK"),
        col("svAplCurStusCdSk").alias("APL_CUR_STTUS_CD_SK"),
        col("svAplInitMethCdSk").alias("APL_INITN_METH_CD_SK"),
        col("svAplMbrHmAddStCdSk").alias("APL_MBR_HOME_ADDR_ST_CD_SK"),
        col("svAplSubtypCdSk").alias("APL_SUBTYP_CD_SK"),
        col("svAplTypCdSk").alias("APL_TYP_CD_SK"),
        col("svClsPlnProdCdSk").alias("CLS_PLN_PROD_CAT_CD_SK"),
        col("svCurAplLvlCdSk").alias("CUR_APL_LVL_CD_SK"),
        col("CUR_APL_LVL_EXPDTD_IN").alias("CUR_APL_LVL_EXPDTD_IN"),
        col("CRT_DTM").alias("CRT_DTM"),
        col("CUR_STTUS_DTM").alias("CUR_STTUS_DTM"),
        col("svEndDtSk").alias("END_DT_SK"),
        col("svInitDtSk").alias("INITN_DT_SK"),
        col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        col("svNextRvwDtSk").alias("NEXT_RVW_DT_SK"),
        col("CUR_APL_LVL_SEQ_NO").alias("CUR_APL_LVL_SEQ_NO"),
        col("CUR_STTUS_SEQ_NO").alias("CUR_STTUS_SEQ_NO"),
        col("NEXT_RVW_INTRVL_NO").alias("NEXT_RVW_INTRVL_NO"),
        col("APL_DESC").alias("APL_DESC"),
        col("APL_SUM_DESC").alias("APL_SUM_DESC"),
    ]
)

# "Recycle" link => Constraint: ErrCount > 0 => goes to "hf_recycle" (CHashedFileStage)
df_Recycle = (
    df_transform
    .filter(col("ErrCount") > lit(0))
    .select(
        GetRecycleKey(col("APL_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("APL_SK").alias("APL_SK"),
        col("APL_ID").alias("APL_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("CRT_USER_SK").alias("CRT_USER_SK"),
        col("CUR_PRI_USER_SK").alias("CUR_PRI_USER_SK"),
        col("CUR_SEC_USER_SK").alias("CUR_SEC_USER_SK"),
        col("CUR_TRTY_USER_SK").alias("CUR_TRTY_USER_SK"),
        col("GRP_SK").alias("GRP_SK"),
        col("LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
        col("MBR_SK").alias("MBR_SK"),
        col("PROD_SK").alias("PROD_SK"),
        col("PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
        col("SUBGRP_SK").alias("SUBGRP_SK"),
        col("SUB_SK").alias("SUB_SK"),
        col("APL_CAT_CD_SK").alias("APL_CAT_CD_SK"),
        col("APL_CUR_DCSN_CD_SK").alias("APL_CUR_DCSN_CD_SK"),
        col("APL_CUR_STTUS_CD_SK").alias("APL_CUR_STTUS_CD_SK"),
        col("APL_INITN_METH_CD_SK").alias("APL_INITN_METH_CD_SK"),
        col("APL_MBR_HOME_ADDR_ST_CD_SK").alias("APL_MBR_HOME_ADDR_ST_CD_SK"),
        col("APL_SUBTYP_CD_SK").alias("APL_SUBTYP_CD_SK"),
        col("APL_TYP_CD_SK").alias("APL_TYP_CD_SK"),
        col("CLS_PLN_PROD_CAT_CD_SK").alias("CLS_PLN_PROD_CAT_CD_SK"),
        col("CUR_APL_LVL_CD_SK").alias("CUR_APL_LVL_CD_SK"),
        col("CUR_APL_LVL_EXPDTD_IN").alias("CUR_APL_LVL_EXPDTD_IN"),
        col("CRT_DTM").alias("CRT_DTM"),
        col("CUR_STTUS_DTM").alias("CUR_STTUS_DTM"),
        col("END_DT_SK").alias("END_DT_SK"),
        col("INITN_DT_SK").alias("INITN_DT_SK"),
        col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        col("NEXT_RVW_DT_SK").alias("NEXT_RVW_DT_SK"),
        col("CUR_APL_LVL_SEQ_NO").alias("CUR_APL_LVL_SEQ_NO"),
        col("CUR_STTUS_SEQ_NO").alias("CUR_STTUS_SEQ_NO"),
        col("NEXT_RVW_INTRVL_NO").alias("NEXT_RVW_INTRVL_NO"),
        col("APL_DESC").alias("APL_DESC"),
        col("APL_SUM_DESC").alias("APL_SUM_DESC"),
    ]
)

# Write "Recycle" to hashed file => Scenario C => translate to .parquet
write_files(
    df_Recycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

# "DefaultUNK" link => Constraint: @INROWNUM = 1 => single row
# "DefaultNA" link => Constraint: @INROWNUM = 1 => single row
# Both feed into "Collector" => union with df_Fkey

collectorSchema = StructType([
    StructField("APL_SK", IntegerType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("APL_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("CRT_USER_SK", IntegerType(), True),
    StructField("CUR_PRI_USER_SK", IntegerType(), True),
    StructField("CUR_SEC_USER_SK", IntegerType(), True),
    StructField("CUR_TRTY_USER_SK", IntegerType(), True),
    StructField("GRP_SK", IntegerType(), True),
    StructField("LAST_UPDT_USER_SK", IntegerType(), True),
    StructField("MBR_SK", IntegerType(), True),
    StructField("PROD_SK", IntegerType(), True),
    StructField("PROD_SH_NM_SK", IntegerType(), True),
    StructField("SUBGRP_SK", IntegerType(), True),
    StructField("SUB_SK", IntegerType(), True),
    StructField("APL_CAT_CD_SK", IntegerType(), True),
    StructField("APL_CUR_DCSN_CD_SK", IntegerType(), True),
    StructField("APL_CUR_STTUS_CD_SK", IntegerType(), True),
    StructField("APL_INITN_METH_CD_SK", IntegerType(), True),
    StructField("APL_MBR_HOME_ADDR_ST_CD_SK", IntegerType(), True),
    StructField("APL_SUBTYP_CD_SK", IntegerType(), True),
    StructField("APL_TYP_CD_SK", IntegerType(), True),
    StructField("CLS_PLN_PROD_CAT_CD_SK", IntegerType(), True),
    StructField("CUR_APL_LVL_CD_SK", IntegerType(), True),
    StructField("CUR_APL_LVL_EXPDTD_IN", StringType(), True),
    StructField("CRT_DTM", TimestampType(), True),
    StructField("CUR_STTUS_DTM", TimestampType(), True),
    StructField("END_DT_SK", StringType(), True),
    StructField("INITN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_DTM", TimestampType(), True),
    StructField("NEXT_RVW_DT_SK", StringType(), True),
    StructField("CUR_APL_LVL_SEQ_NO", IntegerType(), True),
    StructField("CUR_STTUS_SEQ_NO", IntegerType(), True),
    StructField("NEXT_RVW_INTRVL_NO", IntegerType(), True),
    StructField("APL_DESC", StringType(), True),
    StructField("APL_SUM_DESC", StringType(), True),
])

df_DefaultUNK = spark.createDataFrame(
    [
        (
            0,  # APL_SK
            0,  # SRC_SYS_CD_SK
            "UNK",  # APL_ID
            0,  # CRT_RUN_CYC_EXCTN_SK
            0,  # LAST_UPDT_RUN_CYC_EXCTN_SK
            0,  # CRT_USER_SK
            0,  # CUR_PRI_USER_SK
            0,  # CUR_SEC_USER_SK
            0,  # CUR_TRTY_USER_SK
            0,  # GRP_SK
            0,  # LAST_UPDT_USER_SK
            0,  # MBR_SK
            0,  # PROD_SK
            0,  # PROD_SH_NM_SK
            0,  # SUBGRP_SK
            0,  # SUB_SK
            0,  # APL_CAT_CD_SK
            0,  # APL_CUR_DCSN_CD_SK
            0,  # APL_CUR_STTUS_CD_SK
            0,  # APL_INITN_METH_CD_SK
            0,  # APL_MBR_HOME_ADDR_ST_CD_SK
            0,  # APL_SUBTYP_CD_SK
            0,  # APL_TYP_CD_SK
            0,  # CLS_PLN_PROD_CAT_CD_SK
            0,  # CUR_APL_LVL_CD_SK
            "U",  # CUR_APL_LVL_EXPDTD_IN
            # Timestamps as 1753-01-01 00:00:00
            # Will be parsed into a valid timestamp by Spark
            "1753-01-01 00:00:00",  
            "1753-01-01 00:00:00",
            "UNK",  # END_DT_SK
            "UNK",  # INITN_DT_SK
            "1753-01-01 00:00:00",
            "UNK",  # NEXT_RVW_DT_SK
            0,  # CUR_APL_LVL_SEQ_NO
            0,  # CUR_STTUS_SEQ_NO
            0,  # NEXT_RVW_INTRVL_NO
            "UNK",  # APL_DESC
            "UNK",  # APL_SUM_DESC
        )
    ],
    collectorSchema
)

df_DefaultNA = spark.createDataFrame(
    [
        (
            1,  # APL_SK
            1,  # SRC_SYS_CD_SK
            "NA",  # APL_ID
            1,  # CRT_RUN_CYC_EXCTN_SK
            1,  # LAST_UPDT_RUN_CYC_EXCTN_SK
            1,  # CRT_USER_SK
            1,  # CUR_PRI_USER_SK
            1,  # CUR_SEC_USER_SK
            1,  # CUR_TRTY_USER_SK
            1,  # GRP_SK
            1,  # LAST_UPDT_USER_SK
            1,  # MBR_SK
            1,  # PROD_SK
            1,  # PROD_SH_NM_SK
            1,  # SUBGRP_SK
            1,  # SUB_SK
            1,  # APL_CAT_CD_SK
            1,  # APL_CUR_DCSN_CD_SK
            1,  # APL_CUR_STTUS_CD_SK
            1,  # APL_INITN_METH_CD_SK
            1,  # APL_MBR_HOME_ADDR_ST_CD_SK
            1,  # APL_SUBTYP_CD_SK
            1,  # APL_TYP_CD_SK
            1,  # CLS_PLN_PROD_CAT_CD_SK
            1,  # CUR_APL_LVL_CD_SK
            "X",  # CUR_APL_LVL_EXPDTD_IN
            "1753-01-01 00:00:00",
            "1753-01-01 00:00:00",
            "NA",  # END_DT_SK
            "NA",  # INITN_DT_SK
            "1753-01-01 00:00:00",
            "NA",  # NEXT_RVW_DT_SK
            1,  # CUR_APL_LVL_SEQ_NO
            1,  # CUR_STTUS_SEQ_NO
            1,  # NEXT_RVW_INTRVL_NO
            "NA",  # APL_DESC
            "NA",  # APL_SUM_DESC
        )
    ],
    collectorSchema
)

# "Collector" => Union of df_Fkey, df_DefaultUNK, df_DefaultNA
df_Collector = df_Fkey.union(df_DefaultUNK).union(df_DefaultNA)

# Prepare final select with rpad for char columns before writing "APL" (CSeqFileStage)
df_Final = (
    df_Collector
    .withColumn("CUR_APL_LVL_EXPDTD_IN", rpad(col("CUR_APL_LVL_EXPDTD_IN"), 1, " "))
    .withColumn("END_DT_SK", rpad(col("END_DT_SK"), 10, " "))
    .withColumn("INITN_DT_SK", rpad(col("INITN_DT_SK"), 10, " "))
    .withColumn("NEXT_RVW_DT_SK", rpad(col("NEXT_RVW_DT_SK"), 10, " "))
    .select(
        "APL_SK",
        "SRC_SYS_CD_SK",
        "APL_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CRT_USER_SK",
        "CUR_PRI_USER_SK",
        "CUR_SEC_USER_SK",
        "CUR_TRTY_USER_SK",
        "GRP_SK",
        "LAST_UPDT_USER_SK",
        "MBR_SK",
        "PROD_SK",
        "PROD_SH_NM_SK",
        "SUBGRP_SK",
        "SUB_SK",
        "APL_CAT_CD_SK",
        "APL_CUR_DCSN_CD_SK",
        "APL_CUR_STTUS_CD_SK",
        "APL_INITN_METH_CD_SK",
        "APL_MBR_HOME_ADDR_ST_CD_SK",
        "APL_SUBTYP_CD_SK",
        "APL_TYP_CD_SK",
        "CLS_PLN_PROD_CAT_CD_SK",
        "CUR_APL_LVL_CD_SK",
        "CUR_APL_LVL_EXPDTD_IN",
        "CRT_DTM",
        "CUR_STTUS_DTM",
        "END_DT_SK",
        "INITN_DT_SK",
        "LAST_UPDT_DTM",
        "NEXT_RVW_DT_SK",
        "CUR_APL_LVL_SEQ_NO",
        "CUR_STTUS_SEQ_NO",
        "NEXT_RVW_INTRVL_NO",
        "APL_DESC",
        "APL_SUM_DESC"
    )
)

# Write final file "APL.dat"
write_files(
    df_Final,
    f"{adls_path}/load/APL.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)