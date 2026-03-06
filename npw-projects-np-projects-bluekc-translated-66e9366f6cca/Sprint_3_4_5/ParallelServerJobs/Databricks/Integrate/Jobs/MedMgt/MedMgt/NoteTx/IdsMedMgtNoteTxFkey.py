# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 05/12/09 13:30:39 Batch  15108_48642 PROMOTE bckcetl ids20 dsadm bls for rt
# MAGIC ^1_3 05/12/09 13:07:49 Batch  15108_47273 INIT bckcett:31540 testIDS dsadm BLS FOR RT
# MAGIC ^1_3 04/29/09 14:19:50 Batch  15095_51598 PROMOTE bckcett testIDS u03651 steph for Ralph/Bhoomi
# MAGIC ^1_3 04/29/09 14:18:57 Batch  15095_51540 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_2 04/29/09 14:14:40 Batch  15095_51289 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 04/28/09 15:26:41 Batch  15094_55604 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_2 07/31/08 13:47:05 Batch  14823_49643 PROMOTE bckcetl ids20 dsadm rc for brent 
# MAGIC ^1_2 07/31/08 13:32:03 Batch  14823_48732 INIT bckcett testIDS dsadm rc for brent 
# MAGIC ^1_2 07/31/08 07:45:39 Batch  14823_27944 PROMOTE bckcett testIDS u08717 Brent
# MAGIC ^1_2 07/31/08 07:38:12 Batch  14823_27497 INIT bckcett devlIDS u08717 Brent
# MAGIC ^1_1 07/17/08 14:25:56 Batch  14809_51969 INIT bckcett devlIDS u06640 Ralph deploying for Brent
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 08:55:46 Batch  14484_32151 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/27/07 13:53:11 Batch  14331_49995 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 07/13/06 09:01:21 Batch  14074_32499 PROMOTE bckcetl ids20 dsadm J. Mahaffey for O. Nielsen
# MAGIC ^1_2 07/13/06 08:56:37 Batch  14074_32207 INIT bckcett testIDS30 dsadm J. Mahaffey for O. Nielsen
# MAGIC ^1_2 06/28/06 09:07:27 Batch  14059_32851 INIT bckcett testIDS30 u50871 By Tao Luo
# MAGIC ^1_1 06/28/06 09:02:32 Batch  14059_32558 INIT bckcett testIDS30 u50871 By Tao Luo
# MAGIC ^1_1 04/11/06 15:49:38 Batch  13981_56990 INIT bckcett testIDS30 dsadm J. Mahaffey for Hugh Sisson
# MAGIC ^1_1 03/22/06 11:59:36 Batch  13961_43182 PROMOTE bckcett testIDS30 u11141 Hugh Sisson
# MAGIC ^1_1 03/22/06 11:56:45 Batch  13961_43028 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsMedMgtNoteTxFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  Primary Key output file
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes
# MAGIC                             
# MAGIC PROCESSING:  Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:   table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Suzanne Saylor  -  02/15/2006  -  Originally programmed
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                            Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                      ----------------------------------              ---------------------------------               -------------------------
# MAGIC Bhoomi Dasari                04/24/2009           3808 - BICC                          Added SrcSysCdSk & Added new logic                         devlIDS                         Steph Goddard                      04/29/2009
# MAGIC                                                                                                                  for MED_MGT_NOTE_SK

# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging','X')
RunID = get_widget_value('RunID','20080626')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105838')

# 1) Read from IdsMedMgtNoteTxExtr (CSeqFileStage)
schema_IdsMedMgtNoteTxExtr = T.StructType([
    T.StructField("JOB_EXCTN_RCRD_ERR_SK", T.IntegerType(), nullable=False),
    T.StructField("INSRT_UPDT_CD", T.StringType(), nullable=False),
    T.StructField("DISCARD_IN", T.StringType(), nullable=False),
    T.StructField("PASS_THRU_IN", T.StringType(), nullable=False),
    T.StructField("FIRST_RECYC_DT", T.TimestampType(), nullable=False),
    T.StructField("ERR_CT", T.IntegerType(), nullable=False),
    T.StructField("RECYCLE_CT", T.DecimalType(38,10), nullable=False),
    T.StructField("SRC_SYS_CD", T.StringType(), nullable=False),
    T.StructField("PRI_KEY_STRING", T.StringType(), nullable=False),
    T.StructField("MED_MGT_NOTE_TX_SK", T.IntegerType(), nullable=False),
    T.StructField("MED_MGT_NOTE_DTM", T.TimestampType(), nullable=False),
    T.StructField("MED_MGT_NOTE_INPT_DTM", T.TimestampType(), nullable=False),
    T.StructField("NOTE_TX_SEQ_NO", T.IntegerType(), nullable=False),
    T.StructField("CRT_RUN_CYC_EXCTN_SK", T.IntegerType(), nullable=False),
    T.StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", T.IntegerType(), nullable=False),
    T.StructField("MED_MGT_NOTE_SK", T.IntegerType(), nullable=False),
    T.StructField("NOTE_TX", T.StringType(), nullable=True)
])
file_path_IdsMedMgtNoteTxExtr = f"{adls_path}/key/FctsMedMgtNoteTxExtr.MedMgtNoteTx.dat.{RunID}"
df_IdsMedMgtNoteTxExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_IdsMedMgtNoteTxExtr)
    .load(file_path_IdsMedMgtNoteTxExtr)
)

# 2) Read from K_MED_MGT_NOTE_TX_TEMP (DB2Connector)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_K_MED_MGT_NOTE_TX_TEMP = (
    f"SELECT k.SRC_SYS_CD_SK, k.MED_MGT_NOTE_DTM, k.MED_MGT_NOTE_INPT_DTM, k.MED_MGT_NOTE_SK "
    f"FROM {IDSOwner}.K_MED_MGT_NOTE_TX_TEMP w, {IDSOwner}.K_MED_MGT_NOTE k "
    f"WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK "
    f"AND w.MED_MGT_NOTE_DTM = k.MED_MGT_NOTE_DTM "
    f"AND w.MED_MGT_NOTE_INPT_DTM = k.MED_MGT_NOTE_INPT_DTM "
    f"{IDSOwner}.W_PROV_CAP"
)
df_K_MED_MGT_NOTE_TX_TEMP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_K_MED_MGT_NOTE_TX_TEMP)
    .load()
)

# 3) hf_medmgt_notetx (CHashedFileStage) - Scenario A (intermediate hashed file used as a lookup, no write back)
# Deduplicate on key columns [SRC_SYS_CD_SK, MED_MGT_NOTE_DTM, MED_MGT_NOTE_INPT_DTM]
df_hf_medmgt_notetx = dedup_sort(
    df_K_MED_MGT_NOTE_TX_TEMP,
    partition_cols=["SRC_SYS_CD_SK","MED_MGT_NOTE_DTM","MED_MGT_NOTE_INPT_DTM"],
    sort_cols=[]
)

# 4) ForeignKey (CTransformerStage)
# Primary link = df_IdsMedMgtNoteTxExtr, Lookup link = df_hf_medmgt_notetx
df_Key = df_IdsMedMgtNoteTxExtr.withColumn("SrcSysCdSk", F.lit(SrcSysCdSk).cast(T.IntegerType()))

# Join left
df_joined = df_Key.alias("Key").join(
    df_hf_medmgt_notetx.alias("NoteTxKeyLkup"),
    on=[
        F.col("Key.SrcSysCdSk") == F.col("NoteTxKeyLkup.SRC_SYS_CD_SK"),
        F.col("Key.MED_MGT_NOTE_DTM") == F.col("NoteTxKeyLkup.MED_MGT_NOTE_DTM"),
        F.col("Key.MED_MGT_NOTE_INPT_DTM") == F.col("NoteTxKeyLkup.MED_MGT_NOTE_INPT_DTM")
    ],
    how="left"
)

# Add stage variables
df_joined = (
    df_joined
    .withColumn("PassThru", F.col("Key.PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("Key.MED_MGT_NOTE_SK")))
    .withColumn("svMedMgtNoteSk", GetFkeyMedMgtNote(
        F.col("Key.SRC_SYS_CD"),
        F.col("Key.MED_MGT_NOTE_TX_SK"),
        F.col("Key.MED_MGT_NOTE_DTM"),
        F.col("Key.MED_MGT_NOTE_INPT_DTM"),
        Logging
    ))
)

# Fkey constraint = ErrCount = 0 or PassThru = 'Y'
# We also apply the expressions for each output column
df_Fkey = (
    df_joined
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .withColumn("OUT_MED_MGT_NOTE_TX_SK", F.col("Key.MED_MGT_NOTE_TX_SK"))
    .withColumn("OUT_SRC_SYS_CD_SK", F.col("SrcSysCdSk"))
    .withColumn("OUT_MED_MGT_NOTE_DTM", F.col("Key.MED_MGT_NOTE_DTM"))
    .withColumn("OUT_MED_MGT_NOTE_INPT_DTM", F.col("Key.MED_MGT_NOTE_INPT_DTM"))
    .withColumn("OUT_NOTE_TX_SEQ_NO", F.col("Key.NOTE_TX_SEQ_NO"))
    .withColumn("OUT_CRT_RUN_CYC_EXCTN_SK", F.col("Key.CRT_RUN_CYC_EXCTN_SK"))
    .withColumn("OUT_LAST_UPDT_RUN_CYC_EXCTN_SK", F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn(
        "OUT_MED_MGT_NOTE_SK",
        F.when(F.col("NoteTxKeyLkup.MED_MGT_NOTE_SK").isNull(), F.lit(0)).otherwise(F.col("NoteTxKeyLkup.MED_MGT_NOTE_SK"))
    )
    # Perform the chained Ereplace / trim / Left(...,70)
    # We assume Ereplace, trim, Left are all available in the namespace as user-defined functions
    .withColumn(
        "OUT_NOTE_TX",
        Left(
            Ereplace(
                Ereplace(
                    Ereplace(
                        Ereplace(
                            Ereplace(
                                Ereplace(
                                    Ereplace(
                                        Ereplace(
                                            Ereplace(
                                                Ereplace(
                                                    Ereplace(
                                                        Ereplace(
                                                            Ereplace(
                                                                Ereplace(
                                                                    Ereplace(
                                                                        Ereplace(
                                                                            Ereplace(
                                                                                Ereplace(
                                                                                    Ereplace(
                                                                                        Ereplace(
                                                                                            Ereplace(
                                                                                                Ereplace(
                                                                                                    trim(F.col("Key.NOTE_TX")),
                                                                                                    " ",
                                                                                                    " "
                                                                                                ),
                                                                                                "\\(2013) ",
                                                                                                " "
                                                                                            ),
                                                                                            "\\(2019)",
                                                                                            " "
                                                                                        ),
                                                                                        "\\(2022)",
                                                                                        ""
                                                                                    ),
                                                                                    "\\(201D)",
                                                                                    " "
                                                                                ),
                                                                                "\\(201C)",
                                                                                " "
                                                                            ),
                                                                            "\\(2014)",
                                                                            " "
                                                                        ),
                                                                        "\\(2026)",
                                                                        " "
                                                                    ),
                                                                    "",
                                                                    " "
                                                                ),
                                                                "",
                                                                ""
                                                            ),
                                                            "",
                                                            ""
                                                        ),
                                                        "",
                                                        ""
                                                    ),
                                                    "\\(2122)",
                                                    ""
                                                ),
                                                "",
                                                ""
                                            ),
                                            "",
                                            ""
                                        ),
                                        "   ",
                                        " "
                                    ),
                                    "  ",
                                    " "
                                ),
                                F.lit("\u0080"),  # CHAR(128) as a unicode literal
                                ""
                            ),
                            "",
                            ""
                        ),
                        "",
                        ""
                    ),
                    "&#x9c",
                    ""
                ),
                "",
                ""
            ),
            70
        )
    )
)

# Recycle constraint = ErrCount > 0
# Output column expressions
df_Recycle = (
    df_joined
    .filter(F.col("ErrCount") > 0)
    .withColumn("OUT_JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(F.col("Key.MED_MGT_NOTE_TX_SK")))
    .withColumn("OUT_INSRT_UPDT_CD", F.col("Key.INSRT_UPDT_CD"))
    .withColumn("OUT_DISCARD_IN", F.col("Key.DISCARD_IN"))
    .withColumn("OUT_PASS_THRU_IN", F.col("Key.PASS_THRU_IN"))
    .withColumn("OUT_FIRST_RECYC_DT", F.col("Key.FIRST_RECYC_DT"))
    .withColumn("OUT_ERR_CT", F.col("ErrCount"))
    .withColumn("OUT_RECYCLE_CT", F.col("Key.RECYCLE_CT") + F.lit(1))
    .withColumn("OUT_SRC_SYS_CD", F.col("Key.SRC_SYS_CD"))
    .withColumn("OUT_PRI_KEY_STRING", F.col("Key.PRI_KEY_STRING"))
    .withColumn("OUT_MED_MGT_NOTE_TX_SK", F.col("Key.MED_MGT_NOTE_TX_SK"))
    .withColumn("OUT_MED_MGT_NOTE_DTM", F.col("Key.MED_MGT_NOTE_DTM"))
    .withColumn("OUT_MED_MGT_NOTE_INPT_DTM", F.col("Key.MED_MGT_NOTE_INPT_DTM"))
    .withColumn("OUT_NOTE_TX_SEQ_NO", F.col("Key.NOTE_TX_SEQ_NO"))
    .withColumn("OUT_CRT_RUN_CYC_EXCTN_SK", F.col("Key.CRT_RUN_CYC_EXCTN_SK"))
    .withColumn("OUT_LAST_UPDT_RUN_CYC_EXCTN_SK", F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn("OUT_MED_MGT_NOTE_SK", F.col("Key.MED_MGT_NOTE_SK"))
    .withColumn("OUT_NOTE_TX", F.col("Key.NOTE_TX"))
)

# DefaultUNK constraint = @INROWNUM = 1
# We create a single-row DataFrame with the specified columns
defaultUNK_row = Row(
    OUT_MED_MGT_NOTE_TX_SK=0,
    OUT_SRC_SYS_CD_SK=0,
    OUT_MED_MGT_NOTE_DTM="1753-01-01 00:00:00.000000",
    OUT_MED_MGT_NOTE_INPT_DTM="1753-01-01 00:00:00.000000",
    OUT_NOTE_TX_SEQ_NO=0,
    OUT_CRT_RUN_CYC_EXCTN_SK=0,
    OUT_LAST_UPDT_RUN_CYC_EXCTN_SK=0,
    OUT_MED_MGT_NOTE_SK=0,
    OUT_NOTE_TX="UNK"
)
df_DefaultUNK = spark.createDataFrame([defaultUNK_row])

# DefaultNA constraint = @INROWNUM = 1
# We create a single-row DataFrame with the specified columns
defaultNA_row = Row(
    OUT_MED_MGT_NOTE_TX_SK=1,
    OUT_SRC_SYS_CD_SK=1,
    OUT_MED_MGT_NOTE_DTM="1753-01-01 00:00:00.000000",
    OUT_MED_MGT_NOTE_INPT_DTM="1753-01-01 00:00:00.000000",
    OUT_NOTE_TX_SEQ_NO=1,
    OUT_CRT_RUN_CYC_EXCTN_SK=1,
    OUT_LAST_UPDT_RUN_CYC_EXCTN_SK=1,
    OUT_MED_MGT_NOTE_SK=1,
    OUT_NOTE_TX="NA"
)
df_DefaultNA = spark.createDataFrame([defaultNA_row])

# 5) hf_recycle (CHashedFileStage) - Scenario C => Write to parquet
df_recycle_selected = df_Recycle.select(
    F.col("OUT_JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("OUT_INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("OUT_DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("OUT_PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("OUT_FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("OUT_ERR_CT").alias("ERR_CT"),
    F.col("OUT_RECYCLE_CT").alias("RECYCLE_CT"),
    rpad(F.col("OUT_SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    rpad(F.col("OUT_PRI_KEY_STRING"), 255, " ").alias("PRI_KEY_STRING"),
    F.col("OUT_MED_MGT_NOTE_TX_SK").alias("MED_MGT_NOTE_TX_SK"),
    F.col("OUT_MED_MGT_NOTE_DTM").alias("MED_MGT_NOTE_DTM"),
    F.col("OUT_MED_MGT_NOTE_INPT_DTM").alias("MED_MGT_NOTE_INPT_DTM"),
    F.col("OUT_NOTE_TX_SEQ_NO").alias("NOTE_TX_SEQ_NO"),
    F.col("OUT_CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("OUT_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("OUT_MED_MGT_NOTE_SK").alias("MED_MGT_NOTE_SK"),
    rpad(F.col("OUT_NOTE_TX"), 255, " ").alias("NOTE_TX")
)
write_files(
    df_recycle_selected,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_parqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# 6) Collector (CCollector)
# Union the Fkey, DefaultUNK, DefaultNA
df_Fkey_for_union = df_Fkey.select(
    F.col("OUT_MED_MGT_NOTE_TX_SK").alias("MED_MGT_NOTE_TX_SK"),
    F.col("OUT_SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("OUT_MED_MGT_NOTE_DTM").alias("MED_MGT_NOTE_DTM"),
    F.col("OUT_MED_MGT_NOTE_INPT_DTM").alias("MED_MGT_NOTE_INPT_DTM"),
    F.col("OUT_NOTE_TX_SEQ_NO").alias("NOTE_TX_SEQ_NO"),
    F.col("OUT_CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("OUT_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("OUT_MED_MGT_NOTE_SK").alias("MED_MGT_NOTE_SK"),
    F.col("OUT_NOTE_TX").alias("NOTE_TX")
)

df_DefaultUNK_for_union = df_DefaultUNK.select(
    F.col("OUT_MED_MGT_NOTE_TX_SK").alias("MED_MGT_NOTE_TX_SK"),
    F.col("OUT_SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("OUT_MED_MGT_NOTE_DTM").alias("MED_MGT_NOTE_DTM"),
    F.col("OUT_MED_MGT_NOTE_INPT_DTM").alias("MED_MGT_NOTE_INPT_DTM"),
    F.col("OUT_NOTE_TX_SEQ_NO").alias("NOTE_TX_SEQ_NO"),
    F.col("OUT_CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("OUT_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("OUT_MED_MGT_NOTE_SK").alias("MED_MGT_NOTE_SK"),
    F.col("OUT_NOTE_TX").alias("NOTE_TX")
)

df_DefaultNA_for_union = df_DefaultNA.select(
    F.col("OUT_MED_MGT_NOTE_TX_SK").alias("MED_MGT_NOTE_TX_SK"),
    F.col("OUT_SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("OUT_MED_MGT_NOTE_DTM").alias("MED_MGT_NOTE_DTM"),
    F.col("OUT_MED_MGT_NOTE_INPT_DTM").alias("MED_MGT_NOTE_INPT_DTM"),
    F.col("OUT_NOTE_TX_SEQ_NO").alias("NOTE_TX_SEQ_NO"),
    F.col("OUT_CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("OUT_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("OUT_MED_MGT_NOTE_SK").alias("MED_MGT_NOTE_SK"),
    F.col("OUT_NOTE_TX").alias("NOTE_TX")
)

df_Collector = df_Fkey_for_union.unionByName(df_DefaultUNK_for_union).unionByName(df_DefaultNA_for_union)

# 7) MED_MGT_NOTE_TX (CSeqFileStage) => Writing to "load/MED_MGT_NOTE_TX.dat"
df_Final = df_Collector.select(
    F.col("MED_MGT_NOTE_TX_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("MED_MGT_NOTE_DTM"),
    F.col("MED_MGT_NOTE_INPT_DTM"),
    F.col("NOTE_TX_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MED_MGT_NOTE_SK"),
    rpad(F.col("NOTE_TX"), 255, " ").alias("NOTE_TX")
)

write_files(
    df_Final,
    f"{adls_path}/load/MED_MGT_NOTE_TX.dat",
    delimiter=",",
    mode="overwrite",
    is_parqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)