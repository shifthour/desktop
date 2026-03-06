# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsFcltyOccurFkey
# MAGIC Called By: IdsFctsClmLoad1Seq, IdsLhoFctsClmLoad1Seq
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     CDMA - Read only for codes lookup
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Tom Harrocks  08/2004        -   Originally Programmed
# MAGIC              Ralph Tucker  8/17/2004    - Added FKey on From & To Dates
# MAGIC              Brent Leland    09/09/2004  - Corrected default values for UNK and NA
# MAGIC             Steph Goddard 02/14/2006    Minor changes for sequencer implementation
# MAGIC             Brent Leland     03/24/2006   Changed key lookup from GetFkeyFcltyClm to GetFkeyClm to match the Fclty_clm table.
# MAGIC             Steph Goddard 06/26/2006   Changed lookup for recycle key - production support issue
# MAGIC                                                             primary key string was blank on recycle reports 
# MAGIC              Brent Leland     07/18/2006   Changed Fkey() to use Pkey.FCLTY_CLM_OCCUR_SK instead of Pkey.CLM_LN_SK
# MAGIC 
# MAGIC Steph Goddard  07/21/2008   walkthru - changes required
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                               Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                                       --------------------------------       -------------------------------   ----------------------------       
# MAGIC Bhoomi Dasari         2008-07-28      3567(Primary Key)    Added new parameter SrcSysCdSk and passing it directly from source                                   devlIDS                          Steph Goddard          07/29/2008
# MAGIC Shanmugam A         2018-03-08      TFS21148               validate FROM_DT_SK and TO_DT_SK dates before passing it through GetFkeyDate()        Integrate Dev1	Jaideep Mankala      03/13/2018
# MAGIC 
# MAGIC Reddy Sanam          2020-09-28      263445                   changed Derivation for this stage variable to use 'FACETS' as the source
# MAGIC                                                                                         system code when attaching Clm Occurance Code SK                                                            IntegrateDev2
# MAGIC 
# MAGIC Sunitha Ganta         10-12-2020                                Brought up to standards

# MAGIC Lookup all the codes and ID references. If any fail send the record to the recycle bin.
# MAGIC Read common record format file.
# MAGIC Remove recycle records when a new source record with the same natural key from the  is found.
# MAGIC Writing Sequential File to /load
# MAGIC Add default rows for UNK and NA
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Source = get_widget_value('Source','')
InFile = get_widget_value('InFile','')
Logging = get_widget_value('Logging','')
SrcSysCdSkVal = get_widget_value('SrcSysCdSk','')

if "#$FilePath#/landing/" in InFile:
    file_part = InFile.replace("#$FilePath#/landing/","")
    full_inFile_path = f"{adls_path_raw}/landing/{file_part}"
elif "#$FilePath#/external/" in InFile:
    file_part = InFile.replace("#$FilePath#/external/","")
    full_inFile_path = f"{adls_path_publish}/external/{file_part}"
else:
    file_part = InFile.replace("#$FilePath#/","")
    full_inFile_path = f"{adls_path}/{file_part}"

schema = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("FCLTY_CLM_OCCUR_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("FCLTY_CLM_OCCUR_SEQ_NO", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("FCLTY_CLM_SK", IntegerType(), nullable=False),
    StructField("FCLTY_CLM_OCCUR_CD", StringType(), nullable=False),
    StructField("FROM_DT_SK", StringType(), nullable=False),
    StructField("TO_DT_SK", StringType(), nullable=False)
])

df_pkey = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema)
    .load(full_inFile_path)
)

df_withStageVars = (
    df_pkey
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("DiscardInd", F.col("DISCARD_IN"))
    .withColumn("FcltyClmOccrCdSk", GetFkeyCodes(
        F.when(F.col("SRC_SYS_CD")=="LUMERIS", F.lit("FACETS")).otherwise(F.col("SRC_SYS_CD")),
        F.col("FCLTY_CLM_OCCUR_SK"),
        F.lit("FACILITY CLAIM OCCURRENCE"),
        F.col("FCLTY_CLM_OCCUR_CD"),
        F.lit(Logging)
    ))
    .withColumn("FcltyClmSk", GetFkeyClm(
        F.col("SRC_SYS_CD"),
        F.col("FCLTY_CLM_OCCUR_SK"),
        F.col("CLM_ID"),
        F.lit(Logging)
    ))
    .withColumn("FromDtSK", F.when(
        F.col("FROM_DT_SK").rlike("^[0-9]{8}$"),
        GetFkeyDate("IDS", F.col("FCLTY_CLM_OCCUR_SK"), F.col("FROM_DT_SK"), F.lit("Y"))
    ).otherwise(F.lit("UNK")))
    .withColumn("ToDtSK", F.when(
        F.col("TO_DT_SK").rlike("^[0-9]{8}$"),
        GetFkeyDate("IDS", F.col("FCLTY_CLM_OCCUR_SK"), F.col("TO_DT_SK"), F.lit("Y"))
    ).otherwise(F.lit("UNK")))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("FCLTY_CLM_OCCUR_SK")))
    .withColumn("rownum", F.row_number().over(Window.orderBy(F.lit(1))))
    .withColumn("SrcSysCdSk", F.lit(SrcSysCdSkVal))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(F.col("FCLTY_CLM_OCCUR_SK")))
    .withColumn("RECYCLE_CT", (F.col("RECYCLE_CT") + F.lit(1)))
)

fkeyDF = (
    df_withStageVars
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("FCLTY_CLM_OCCUR_SK").alias("FCLTY_CLM_OCCUR_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("FCLTY_CLM_OCCUR_SEQ_NO").alias("FCLTY_CLM_OCCUR_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("FcltyClmSk").alias("FCLTY_CLM_SK"),
        F.col("FcltyClmOccrCdSk").alias("FCLTY_CLM_OCCUR_CD_SK"),
        F.col("FromDtSK").alias("FROM_DT_SK"),
        F.col("ToDtSK").alias("TO_DT_SK")
    )
)

recycleDF = (
    df_withStageVars
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DiscardInd").alias("DISCARD_IN"),
        F.col("PassThru").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        F.col("RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("FCLTY_CLM_OCCUR_SK").alias("FCLTY_CLM_OCCUR_SK"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("FCLTY_CLM_OCCUR_SEQ_NO").alias("FCLTY_CLM_OCCUR_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("FCLTY_CLM_SK").alias("FCLTY_CLM_SK"),
        F.col("FCLTY_CLM_OCCUR_CD").alias("FCLTY_CLM_OCCUR_CD"),
        F.col("FROM_DT_SK").alias("FROM_DT_SK"),
        F.col("TO_DT_SK").alias("TO_DT_SK")
    )
)

write_files(
    recycleDF,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

recycleClmsDF = (
    df_withStageVars
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLM_ID").alias("CLM_ID")
    )
)

write_files(
    recycleClmsDF,
    "hf_claim_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

defaultUNKdf = (
    df_withStageVars
    .filter(F.col("rownum") == 1)
    .select(
        F.lit(0).alias("FCLTY_CLM_OCCUR_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("CLM_ID"),
        F.lit(0).alias("FCLTY_CLM_OCCUR_SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("FCLTY_CLM_SK"),
        F.lit(0).alias("FCLTY_CLM_OCCUR_CD_SK"),
        F.lit("NA").alias("FROM_DT_SK"),
        F.lit("NA").alias("TO_DT_SK")
    )
)

defaultNAdf = (
    df_withStageVars
    .filter(F.col("rownum") == 1)
    .select(
        F.lit(1).alias("FCLTY_CLM_OCCUR_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("CLM_ID"),
        F.lit(1).alias("FCLTY_CLM_OCCUR_SEQ_NO"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("FCLTY_CLM_SK"),
        F.lit(1).alias("FCLTY_CLM_OCCUR_CD_SK"),
        F.lit("NA").alias("FROM_DT_SK"),
        F.lit("NA").alias("TO_DT_SK")
    )
)

df_collector = (
    fkeyDF
    .unionByName(defaultUNKdf)
    .unionByName(defaultNAdf)
)

df_final = df_collector.select(
    "FCLTY_CLM_OCCUR_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "FCLTY_CLM_OCCUR_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "FCLTY_CLM_SK",
    "FCLTY_CLM_OCCUR_CD_SK",
    "FROM_DT_SK",
    "TO_DT_SK"
).withColumn(
    "FROM_DT_SK", F.rpad(F.col("FROM_DT_SK"), 10, " ")
).withColumn(
    "TO_DT_SK", F.rpad(F.col("TO_DT_SK"), 10, " ")
)

output_path = f"{adls_path}/load/FCLTY_CLM_OCCUR.{Source}.dat"
write_files(
    df_final,
    output_path,
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)