# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2007, 2015, 2023 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Job Name:  LabRsltFKey
# MAGIC Called by: LabRsltLoadSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Assigns foreign keys to result record
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restore key file, if necessary,then rerun
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer\(9)\(9)Date\(9)\(9)Project/Altius #\(9)\(9)Change Description\(9)\(9)\(9)\(9)\(9)\(9)\(9)Development Project\(9)\(9)Code Reviewer\(9)\(9)Date Reviewed  
# MAGIC =================================================================================================================================================================================================================
# MAGIC Laurel Kindley     \(9)\(9)2007-10-08\(9)3429           \(9)\(9)Original Programming.            \(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)Brent Leland       12/27/2007  
# MAGIC Laurel Kindley     \(9)\(9)2008-12-01\(9)3567          \(9)\(9)added src sys sk parameter \(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)Steph Goddard   12/19/2008
# MAGIC Kalyan Neelam   \(9)\(9)2010-05-12\(9)4428           \(9)\(9)Changed SRC_SYS_CD_SK logic                                                
# MAGIC                                                                       \(9)\(9)\(9)\(9)Health Screen can have multiple sources
# MAGIC Terri O'Bryan      \(9)\(9)2010-05-25      \(9)4547           \(9)\(9)Set LAB_RSLT_CLS_CD_SK to 1 for Hooper Holmes\(9)\(9)\(9)\(9)\(9)\(9)\(9)Steph Goddard   05/28/2010
# MAGIC Raja Gummadi    \(9)\(9)2012-05-10      \(9)4896          \(9)\(9)Added new columns PROC_CD_TYP_CD, 
# MAGIC                                                                        \(9)\(9)\(9)\(9)PROC_CD_TYP_CD_SK and changed PROC_CD_SK and\(9)\(9)\(9)\(9)\(9)\(9)\(9)SAndrew             2012-05-17
# MAGIC                                                                        \(9)\(9)\(9)\(9)DIAG_CD_SK lookups
# MAGIC Raja Gummadi   \(9)\(9)2012-11-16    \(9)TTR-1401   \(9)\(9)NA and UNK rows updated with correct Run Cycle Numbers\(9)\(9)\(9)\(9)\(9)\(9)\(9)Bhoomi Dasari     11/30/2012
# MAGIC Hugh Sisson       \(9)\(9)2015-01-14\(9)TFS8292      \(9)\(9)Add 6 fields to the natural key\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)Kalyan Neelam     2015-03-09
# MAGIC 
# MAGIC Akhila M            \(9)\(9)2016-12-20     \(9)TFS13948      \(9)\(9)Added RSLT_SK value from RSLT table for St.Lukes\(9)\(9)\(9)\(9)\(9)\(9)\(9)Kalyan Neelam     2017-01-31
# MAGIC Shanmugam A   \(9)\(9)2016-12-20     \(9)TFS21033      \(9)\(9)Null Handling added for SK value of Diag Code 1,2 and 3\(9)\(9)\(9)\(9)\(9)\(9)\(9)Jaideep Mankala  03/02/2018   
# MAGIC 
# MAGIC Amarendra Po   \(9)\(9)2018-07-24     \(9)TFS5854        \(9)\(9)Prov_sk is derived from Fkey lookup\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)Kalyan Neelam     2018-08-03
# MAGIC 
# MAGIC Pokaa               \(9)\(9)2018-09-21     \(9)TFS5854       \(9)\(9)Added ORDER_PROV_SRC_SYS_CD field for prov_sk lookup\(9)\(9)\(9)\(9)\(9)\(9)Kalyan Neelam     2018-09-26
# MAGIC 
# MAGIC Lakshmi Devagiri\(9)\(9)2021-02-21  \(9)US278143     \(9)\(9)Updated Stage Variable svLabRsltClsCdSk                  \(9)\(9)\(9)\(9)\(9)\(9)\(9)Kalyan Neelam     2021-02-23
# MAGIC Lakshmi Devagiri\(9)\(9)2021-02-21  \(9)US364108     \(9)\(9)Updated Stage Variable svLabRsltClsCdSk to add 'MOSAICLIFECARE'\(9)\(9)\(9)\(9)\(9)\(9)Kalyan Neelam     2021-03-31     
# MAGIC Venkata Yama\(9)\(9)2022-01-12  \(9)us480876       \(9)\(9)Added src_sys_cd='HCA'\(9)\(9)\(9)\(9)\(9)\(9)IntegrateDev2\(9)\(9)\(9)Harsha Ravuri\(9)2022-01-13                 
# MAGIC Ken Bradmon\(9)\(9)2023-08-07\(9)us559895\(9)\(9)\(9)Added 8 new Providers to the svLabRsltClsCdSk variable of the ForeignKey\(9)IntegrateDev2                                         Reddy Sanam       2023-10-27\(9)\(9)
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)Stage.  CENTRUSHEALTH, MOHEALTH, GOLDENVALLEY, JEFFERSON, 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)WESTMOMEDCNTR, BLUESPRINGS, EXCELSIOR, and HARRISONVILLE.
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)This is the only change. 
# MAGIC Harsha Ravuri\(9)\(9)2025-05-12\(9)US#646625\(9)\(9)Added new Provider 'ASCENTIST' to the svLabRsltClsCdSk variable of the \(9)IntegrateDev2                                         Jeyaprasanna       2025-05-20
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)ForeignKey

# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC Prov_sk is derived from lookup
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
    IntegerType,
    TimestampType,
    DecimalType,
    FloatType,
    LongType,
    BooleanType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameters
RowPassThru = get_widget_value('RowPassThru','')
Logging = get_widget_value('Logging','')
RunId = get_widget_value('RunId','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Read "LabRslt_uniq" file as df_Key
schema_LabRslt_uniq = (
    StructType()
    .add("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False)
    .add("INSRT_UPDT_CD", StringType(), False)
    .add("DISCARD_IN", StringType(), False)
    .add("PASS_THRU_IN", StringType(), False)
    .add("FIRST_RECYC_DT", TimestampType(), False)
    .add("ERR_CT", IntegerType(), False)
    .add("RECYCLE_CT", DecimalType(38,10), False)
    .add("SRC_SYS_CD", StringType(), False)
    .add("PRI_KEY_STRING", StringType(), False)
    .add("LAB_RSLT_SK", IntegerType(), False)
    .add("MBR_UNIQ_KEY", IntegerType(), False)
    .add("SVC_DT_SK", StringType(), False)
    .add("PROC_CD", StringType(), False)
    .add("PROC_CD_TYP_CD", StringType(), False)
    .add("LOINC_CD", StringType(), True)
    .add("DIAG_CD_1", StringType(), True)
    .add("DIAG_CD_TYP_CD_1", StringType(), False)
    .add("RSLT_ID", StringType(), False)
    .add("ORDER_TST_ID", StringType(), False)
    .add("PATN_ENCNTR_ID", StringType(), True)
    .add("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False)
    .add("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False)
    .add("DIAG_CD_2", StringType(), True)
    .add("DIAG_CD_3", StringType(), True)
    .add("SRVC_PROV_ID", StringType(), True)
    .add("NORM_RSLT_IN", StringType(), False)
    .add("ORDER_DT_SK", StringType(), False)
    .add("SRC_SYS_EXTR_DT_SK", StringType(), False)
    .add("NUM_RSLT_VAL", DecimalType(38,10), False)
    .add("RSLT_NORM_HI_VAL", DecimalType(38,10), True)
    .add("RSLT_NORM_LOW_VAL", DecimalType(38,10), True)
    .add("ORDER_TST_NM", StringType(), True)
    .add("RSLT_DESC", StringType(), True)
    .add("RSLT_LONG_DESC_1", StringType(), True)
    .add("RSLT_LONG_DESC_2", StringType(), True)
    .add("RSLT_MESR_UNIT_DESC", StringType(), False)
    .add("RSLT_RNG_DESC", StringType(), True)
    .add("SPCMN_ID", StringType(), False)
    .add("SRC_SYS_ORDER_PROV_ID", StringType(), False)
    .add("SRC_SYS_PROC_CD_TX", StringType(), True)
    .add("TX_RSLT_VAL", StringType(), True)
    .add("ORDER_PROV_SRC_SYS_CD", StringType(), True)
)
df_Key = (
    spark.read
    .format("csv")
    .option("header", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_LabRslt_uniq)
    .load(f"{adls_path}/key/LabRsltExtr.LabRslt.uniq")
)

# Read from IDS database for lookup (where SRC_SYS_CD_SK = SrcSysCdSk).
jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)
extract_query_IDS = f"""
SELECT RSLT.RSLT_ID AS RSLT_ID,
       RSLT.SRC_SYS_CD_SK AS SRC_SYS_CD_SK,
       RSLT.RSLT_SK AS RSLT_SK
  FROM {IDSOwner}.RSLT RSLT
 WHERE RSLT.SRC_SYS_CD_SK={SrcSysCdSk}
"""
df_Rslt_Lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query_IDS)
    .load()
    .withColumnRenamed("RSLT_ID","RslT_Lkup_RSLT_ID")
    .withColumnRenamed("SRC_SYS_CD_SK","RslT_Lkup_SRC_SYS_CD_SK")
    .withColumnRenamed("RSLT_SK","RslT_Lkup_RSLT_SK")
)

# Join "Key" and "Rslt_Lkup" (left join) per ForeignKey stage
df_joined = df_Key.alias("Key").join(
    df_Rslt_Lkup.alias("Rslt_Lkup"),
    F.col("Key.RSLT_ID") == F.col("Rslt_Lkup.RslT_Lkup_RSLT_ID"),
    "left"
)

# Add stage variables
df_transform = (
    df_joined
    .withColumn(
        "svPassThru",
        F.col("Key.PASS_THRU_IN")
    )
    .withColumn(
        "svMbrSk",
        GetFkeyMbr(
            F.lit("FACETS"),
            F.col("Key.LAB_RSLT_SK"),
            F.col("Key.MBR_UNIQ_KEY"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svProcCdSk",
        F.when(
            F.col("Key.PROC_CD") == F.lit("UNK"),
            F.lit(0)
        ).otherwise(
            GetFkeyProcCd(
                F.lit("FACETS"),
                F.col("Key.LAB_RSLT_SK"),
                F.col("Key.PROC_CD"),
                F.col("Key.PROC_CD_TYP_CD"),
                F.lit("MED"),
                F.lit(Logging)
            )
        )
    )
    .withColumn(
        "svLoincSk",
        GetFkeyLoincCd(
            F.col("Key.SRC_SYS_CD"),
            F.col("Key.LAB_RSLT_SK"),
            F.col("Key.LOINC_CD"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svLabRsltClsCdSk",
        F.when(
            F.col("Key.SRC_SYS_CD").isin(
                "BARRYPOINTE","CLAYPLATTE","ENCOMPASS","JAYHAWK","LEAWOODFMLYCARE","LIBERTYHOSP",
                "MERITAS","MOSAIC","NORTHLAND","OLATHEMED","PRIMEMO","PROVIDENCE","PROVSTLUKES",
                "SPIRA","SUNFLOWER","TRUMAN","UNTDMEDGRP","MOSAICLIFECARE","HCA","NONSTDSUPLMTDATA",
                "CHILDRENSMERCY","CENTRUSHEALTH","MOHEALTH","GOLDENVALLEY","JEFFERSON","WESTMOMEDCNTR",
                "BLUESPRINGS","EXCELSIOR","HARRISONVILLE","ASCENTIST"
            ),
            GetFkeyCodes(
                F.col("Key.SRC_SYS_CD"),
                F.col("Key.LAB_RSLT_SK"),
                F.lit("LAB RESULT CLASS"),
                F.upper(F.col("Key.NORM_RSLT_IN")),
                F.lit(Logging)
            )
        ).otherwise(
            GetFkeyCodes(
                F.lit("LABVNDR"),
                F.col("Key.LAB_RSLT_SK"),
                F.lit("LAB RESULT CLASS"),
                F.upper(F.col("Key.NORM_RSLT_IN")),
                F.lit(Logging)
            )
        )
    )
    .withColumn(
        "svDiagCdSk",
        F.when(
            F.isnull(F.col("Key.DIAG_CD_1")) | (F.length(F.trim(F.col("Key.DIAG_CD_1"))) == 0),
            F.lit(1)
        ).otherwise(
            GetFkeyDiagCd(
                F.lit("FACETS"),
                F.col("Key.LAB_RSLT_SK"),
                F.col("Key.DIAG_CD_1"),
                F.col("Key.DIAG_CD_TYP_CD_1"),
                F.lit(Logging)
            )
        )
    )
    .withColumn(
        "svDiagCd2Sk",
        F.when(
            F.isnull(F.col("Key.DIAG_CD_2")) | (F.length(F.trim(F.col("Key.DIAG_CD_2"))) == 0),
            F.lit(1)
        ).otherwise(
            GetFkeyDiagCd(
                F.lit("FACETS"),
                F.col("Key.LAB_RSLT_SK"),
                F.col("Key.DIAG_CD_2"),
                F.col("Key.DIAG_CD_TYP_CD_1"),
                F.lit(Logging)
            )
        )
    )
    .withColumn(
        "svDiagCd3Sk",
        F.when(
            F.isnull(F.col("Key.DIAG_CD_3")) | (F.length(F.trim(F.col("Key.DIAG_CD_3"))) == 0),
            F.lit(1)
        ).otherwise(
            GetFkeyDiagCd(
                F.lit("FACETS"),
                F.col("Key.LAB_RSLT_SK"),
                F.col("Key.DIAG_CD_3"),
                F.col("Key.DIAG_CD_TYP_CD_1"),
                F.lit(Logging)
            )
        )
    )
    .withColumn(
        "svProcCdTypCdSk",
        GetFkeySrcTrgtClctnCodes(
            F.lit("FACETS"),
            F.col("Key.LAB_RSLT_SK"),
            F.lit("PROCEDURE CODE TYPE"),
            F.col("Key.PROC_CD_TYP_CD"),
            F.lit("FACETS DBO"),
            F.lit("IDS"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svDiagCodeTypCdSrcVal",
        F.when(
            F.col("Key.SRC_SYS_CD") == F.lit("PROVSTLUKES"),
            F.lit("NA")
        ).otherwise(
            F.when(
                F.col("Key.DIAG_CD_TYP_CD_1") == F.lit("ICD9"),
                F.lit("9")
            ).otherwise(
                F.when(
                    F.col("Key.DIAG_CD_TYP_CD_1") == F.lit("ICD10"),
                    F.lit("0")
                ).otherwise(F.lit("UNK"))
            )
        )
    )
    .withColumn(
        "svDiagCdTypCdSk",
        GetFkeyCodes(
            F.lit("FACETS"),
            F.col("Key.LAB_RSLT_SK"),
            F.lit("DIAGNOSIS CODE TYPE"),
            F.col("svDiagCodeTypCdSrcVal"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svSvcDtSk",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("Key.LAB_RSLT_SK"),
            F.col("Key.SVC_DT_SK"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svErrCount",
        GetFkeyErrorCnt(
            F.col("Key.LAB_RSLT_SK")
        )
    )
    .withColumn(
        "svProvSk",
        GetFkeyProv(
            F.trim(F.col("Key.ORDER_PROV_SRC_SYS_CD")),
            F.col("Key.LAB_RSLT_SK"),
            F.trim(F.col("Key.SRVC_PROV_ID")),
            F.lit("X")
        )
    )
)

# The transform stage outputs:
# 1) FKey link (no constraint specified, so all rows) - columns are listed
df_FKey_all = df_transform.select(
    F.col("Key.LAB_RSLT_SK").alias("LAB_RSLT_SK"),
    F.col("Key.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("svSvcDtSk").alias("SVC_DT_SK"),
    F.col("Key.PROC_CD").alias("PROC_CD"),
    F.col("Key.PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD"),
    F.col("Key.LOINC_CD").alias("LOINC_CD"),
    F.col("Key.DIAG_CD_1").alias("DIAG_CD_1"),
    F.col("Key.DIAG_CD_TYP_CD_1").alias("DIAG_CD_TYP_CD_1"),
    F.col("Key.RSLT_ID").alias("RSLT_ID"),
    F.col("Key.ORDER_TST_ID").alias("ORDER_TST_ID"),
    F.col("Key.PATN_ENCNTR_ID").alias("PATN_ENCNTR_ID"),
    # Expression "SrcSysCdSk" is the parameter
    F.lit(SrcSysCdSk).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
    F.col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svDiagCdSk").alias("DIAG_CD_1_SK"),
    F.col("svDiagCd2Sk").alias("DIAG_CD_2_SK"),
    F.col("svDiagCd3Sk").alias("DIAG_CD_3_SK"),
    F.col("svLoincSk").alias("LOINC_CD_SK"),
    F.col("svMbrSk").alias("MBR_SK"),
    F.col("svProcCdSk").alias("PROC_CD_SK"),
    F.col("svProvSk").alias("PROV_SK"),
    F.when(
        F.col("Key.SRC_SYS_CD") == F.lit("PROVSTLUKES"),
        F.col("Rslt_Lkup.RslT_Lkup_RSLT_SK")
    ).otherwise(F.lit(1)).alias("RSLT_SK"),
    F.col("svDiagCdTypCdSk").alias("DIAG_CD_TYP_CD_1_SK"),
    F.col("svLabRsltClsCdSk").alias("LAB_RSLT_CLS_CD_SK"),
    F.col("svProcCdTypCdSk").alias("PROC_CD_TYP_CD_SK"),
    F.col("Key.ORDER_DT_SK").alias("ORDER_DT_SK"),
    F.col("Key.SRC_SYS_EXTR_DT_SK").alias("SRC_SYS_EXTR_DT_SK"),
    F.col("Key.NUM_RSLT_VAL").alias("NUM_RSLT_VAL"),
    F.col("Key.RSLT_NORM_HI_VAL").alias("RSLT_NORM_HI_VAL"),
    F.col("Key.RSLT_NORM_LOW_VAL").alias("RSLT_NORM_LOW_VAL"),
    F.col("Key.ORDER_TST_NM").alias("ORDER_TST_NM"),
    F.col("Key.RSLT_DESC").alias("RSLT_DESC"),
    F.col("Key.RSLT_LONG_DESC_1").alias("RSLT_LONG_DESC_1"),
    F.col("Key.RSLT_LONG_DESC_2").alias("RSLT_LONG_DESC_2"),
    F.col("Key.RSLT_MESR_UNIT_DESC").alias("RSLT_MESR_UNIT_DESC"),
    F.col("Key.RSLT_RNG_DESC").alias("RSLT_RNG_DESC"),
    F.col("Key.SPCMN_ID").alias("SPCMN_ID"),
    F.col("Key.SRC_SYS_ORDER_PROV_ID").alias("SRC_SYS_ORDER_PROV_ID"),
    F.col("Key.SRC_SYS_PROC_CD_TX").alias("SRC_SYS_PROC_CD_TX"),
    F.col("Key.TX_RSLT_VAL").alias("TX_RSLT_VAL")
)

# 2) DefaultUNK link: single row of literal values
df_DefaultUNK = spark.createDataFrame(
    [(
       0,     # LAB_RSLT_SK
       0,     # MBR_UNIQ_KEY
       "UNK", # SVC_DT_SK
       "UNK", # PROC_CD
       "UNK", # PROC_CD_TYP_CD
       "UNK", # LOINC_CD
       "UNK", # DIAG_CD_1
       "UNK", # DIAG_CD_TYP_CD_1
       "UNK", # RSLT_ID
       "UNK", # ORDER_TST_ID
       "UNK", # PATN_ENCNTR_ID
       0,     # SRC_SYS_CD_SK
       0,     # CRT_RUN_CYC_EXCTN_SK
       0,     # LAST_UPDT_RUN_CYC_EXCTN_SK
       0,     # DIAG_CD_1_SK
       0,     # DIAG_CD_2_SK
       0,     # DIAG_CD_3_SK
       0,     # LOINC_CD_SK
       0,     # MBR_SK
       0,     # PROC_CD_SK
       0,     # PROV_SK
       0,     # RSLT_SK
       0,     # DIAG_CD_TYP_CD_1_SK
       0,     # LAB_RSLT_CLS_CD_SK
       0,     # PROC_CD_TYP_CD_SK
       "UNK", # ORDER_DT_SK
       "UNK", # SRC_SYS_EXTR_DT_SK
       0,     # NUM_RSLT_VAL
       0,     # RSLT_NORM_HI_VAL
       0,     # RSLT_NORM_LOW_VAL
       "UNK", # ORDER_TST_NM
       "UNK", # RSLT_DESC
       "UNK", # RSLT_LONG_DESC_1
       "UNK", # RSLT_LONG_DESC_2
       "UNK", # RSLT_MESR_UNIT_DESC
       "UNK", # RSLT_RNG_DESC
       "UNK", # SPCMN_ID
       "UNK", # SRC_SYS_ORDER_PROV_ID
       "UNK", # SRC_SYS_PROC_CD_TX
       "UNK"  # TX_RSLT_VAL
    )],
    df_FKey_all.schema
)

# 3) DefaultNA link: single row of literal values
df_DefaultNA = spark.createDataFrame(
    [(
       1,      # LAB_RSLT_SK
       1,      # MBR_UNIQ_KEY
       "NA",   # SVC_DT_SK
       "NA",   # PROC_CD
       "NA",   # PROC_CD_TYP_CD
       "NA",   # LOINC_CD
       "NA",   # DIAG_CD_1
       "NA",   # DIAG_CD_TYP_CD_1
       "NA",   # RSLT_ID
       "NA",   # ORDER_TST_ID
       "NA",   # PATN_ENCNTR_ID
       1,      # SRC_SYS_CD_SK
       1,      # CRT_RUN_CYC_EXCTN_SK
       1,      # LAST_UPDT_RUN_CYC_EXCTN_SK
       1,      # DIAG_CD_1_SK
       1,      # DIAG_CD_2_SK
       1,      # DIAG_CD_3_SK
       1,      # LOINC_CD_SK
       1,      # MBR_SK
       1,      # PROC_CD_SK
       1,      # PROV_SK
       1,      # RSLT_SK
       1,      # DIAG_CD_TYP_CD_1_SK
       1,      # LAB_RSLT_CLS_CD_SK
       1,      # PROC_CD_TYP_CD_SK
       "NA",   # ORDER_DT_SK
       "NA",   # SRC_SYS_EXTR_DT_SK
       1,      # NUM_RSLT_VAL
       1,      # RSLT_NORM_HI_VAL
       1,      # RSLT_NORM_LOW_VAL
       "NA",   # ORDER_TST_NM
       "NA",   # RSLT_DESC
       "NA",   # RSLT_LONG_DESC_1
       "NA",   # RSLT_LONG_DESC_2
       "NA",   # RSLT_MESR_UNIT_DESC
       "NA",   # RSLT_RNG_DESC
       "NA",   # SPCMN_ID
       "NA",   # SRC_SYS_ORDER_PROV_ID
       "NA",   # SRC_SYS_PROC_CD_TX
       "NA"    # TX_RSLT_VAL
    )],
    df_FKey_all.schema
)

# 4) Recycle link: constraint "svErrCount > 0", columns mapped
df_Recycle = (
    df_transform.filter(F.col("svErrCount") > 0)
    .select(
        GetRecycleKey(F.col("Key.LAB_RSLT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("Key.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("Key.DISCARD_IN").alias("DISCARD_IN"),
        F.col("Key.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("Key.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("svErrCount").alias("ERR_CT"),
        F.col("Key.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("Key.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Key.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("Key.LAB_RSLT_SK").alias("LAB_RSLT_SK"),
        F.lit(SrcSysCdSk).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
        F.col("Key.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("svSvcDtSk").alias("SVC_DT_SK"),
        F.col("Key.PROC_CD").alias("PROC_CD"),
        F.col("Key.PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD"),
        F.col("Key.RSLT_ID").alias("RSLT_ID"),
        F.col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svDiagCdSk").alias("DIAG_CD_1_SK"),
        F.col("svDiagCd2Sk").alias("DIAG_CD_2_SK"),
        F.col("svDiagCd3Sk").alias("DIAG_CD_3_SK"),
        F.col("svLoincSk").alias("LOINC_CD_SK"),
        F.col("svMbrSk").alias("MBR_SK"),
        F.col("svProcCdSk").alias("PROC_CD_SK"),
        F.lit(1).alias("PROV_SK"),
        F.lit(1).alias("RSLT_SK"),
        F.when(
            F.col("Key.SRC_SYS_CD") == F.lit("HOOPERHOLMES"),
            F.lit(1)
        ).otherwise(F.col("svLabRsltClsCdSk")).alias("LAB_RSLT_CLS_CD_SK"),
        F.col("Key.ORDER_DT_SK").alias("ORDER_DT_SK"),
        F.col("Key.SRC_SYS_EXTR_DT_SK").alias("SRC_SYS_EXTR_DT_SK"),
        F.col("Key.NUM_RSLT_VAL").alias("NUM_RSLT_VAL"),
        F.col("Key.RSLT_NORM_HI_VAL").alias("RSLT_NORM_HI_VAL"),
        F.col("Key.RSLT_NORM_LOW_VAL").alias("RSLT_NORM_LOW_VAL"),
        F.col("Key.RSLT_DESC").alias("RSLT_DESC"),
        F.col("Key.RSLT_LONG_DESC_1").alias("RSLT_LONG_DESC_1"),
        F.col("Key.RSLT_LONG_DESC_2").alias("RSLT_LONG_DESC_2"),
        F.col("Key.RSLT_MESR_UNIT_DESC").alias("RSLT_MESR_UNIT_DESC"),
        F.col("Key.RSLT_RNG_DESC").alias("RSLT_RNG_DESC"),
        F.col("Key.SPCMN_ID").alias("SPCMN_ID"),
        F.col("Key.SRC_SYS_ORDER_PROV_ID").alias("SRC_SYS_ORDER_PROV_ID"),
        F.col("Key.SRC_SYS_PROC_CD_TX").alias("SRC_SYS_PROC_CD_TX"),
        F.col("Key.TX_RSLT_VAL").alias("TX_RSLT_VAL"),
        F.col("Key.LOINC_CD").alias("LOINC_CD"),
        F.col("Key.DIAG_CD_TYP_CD_1").alias("DIAG_CD_TYP_CD_1")
    )
)

# Write the hashed file "hf_recycle" (Scenario C => convert to parquet)
write_files(
    df_Recycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Collector: union of FKey, DefaultUNK, DefaultNA
df_Collector = df_DefaultUNK.unionByName(df_DefaultNA).unionByName(df_FKey_all)

# For the final output to "LAB_RSLT.dat", we apply rpad for columns that are char(...) in the final schema:
df_Collector_final = (
    df_Collector
    .withColumn("SVC_DT_SK", F.rpad(F.col("SVC_DT_SK"), 10, " "))
    .withColumn("ORDER_DT_SK", F.rpad(F.col("ORDER_DT_SK"), 10, " "))
    .withColumn("SRC_SYS_EXTR_DT_SK", F.rpad(F.col("SRC_SYS_EXTR_DT_SK"), 10, " "))
)

# Write final file LAB_RSLT.dat
write_files(
    df_Collector_final.select(
        "LAB_RSLT_SK","MBR_UNIQ_KEY","SVC_DT_SK","PROC_CD","PROC_CD_TYP_CD","LOINC_CD",
        "DIAG_CD_1","DIAG_CD_TYP_CD_1","RSLT_ID","ORDER_TST_ID","PATN_ENCNTR_ID",
        "SRC_SYS_CD_SK","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","DIAG_CD_1_SK",
        "DIAG_CD_2_SK","DIAG_CD_3_SK","LOINC_CD_SK","MBR_SK","PROC_CD_SK","PROV_SK",
        "RSLT_SK","DIAG_CD_TYP_CD_1_SK","LAB_RSLT_CLS_CD_SK","PROC_CD_TYP_CD_SK",
        "ORDER_DT_SK","SRC_SYS_EXTR_DT_SK","NUM_RSLT_VAL","RSLT_NORM_HI_VAL","RSLT_NORM_LOW_VAL",
        "ORDER_TST_NM","RSLT_DESC","RSLT_LONG_DESC_1","RSLT_LONG_DESC_2","RSLT_MESR_UNIT_DESC",
        "RSLT_RNG_DESC","SPCMN_ID","SRC_SYS_ORDER_PROV_ID","SRC_SYS_PROC_CD_TX","TX_RSLT_VAL"
    ),
    f"{adls_path}/load/LAB_RSLT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)