# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsJrnlEntryTransFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys; essentially converting the common record format file to a table load file.   
# MAGIC                             If any of the foreign key lookups fail, then it is written to the recycle hash file.
# MAGIC      
# MAGIC                            Not date specific.  
# MAGIC 
# MAGIC                          No databases are used.  Just files.
# MAGIC                                   
# MAGIC   
# MAGIC INPUTS:             Input from primary key job IdsJrnlEntryTransPkey
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle - written to only.
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                             GetFkeyFnclTrans
# MAGIC                             GetFkeyFnclLOB
# MAGIC                              GetFkeyDate
# MAGIC                             GetFkeyCodes
# MAGIC                             GetFkeyErrorCnt
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC                   All records from the input file are processed; no records are filtered out.  
# MAGIC                   Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC                   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                              File to load to tables for table JRNL_ENTRY_TRANS
# MAGIC                              A second file is created for table JRNL_ENTRY_PRCS.  This table is a work table for I/S personnel therefore does not have the
# MAGIC                                      run_cyc_exctn and other information.  It is tied to the JRNL_ENTRY_TRANS table by the same surrogate key.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Steph Goddard     11/03/2004     -   Originally Programmed
# MAGIC             Steph Goddard     07/18/2005         Changes for sequencer
# MAGIC 
# MAGIC             
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parik                               2008-08-19        3567(Primary Key)        Added Source System Code SK as parameter          devlIDS                     Steph Goddard            08/22/2008
# MAGIC 
# MAGIC Sudheer Champati           2015-07-17        5128                            Modified the TransTbkSk and SrcCkClmID stage variables.  IntegrateDev1    Kalyan Neelam        2015-07-20
# MAGIC 
# MAGIC 
# MAGIC Sudheer Champati           2015-10-12        5212                            Modified the TransTbkSk and SrcCkClmID stage variables.  IntegrateDev1    Kalyan Neelam       2016-01-04
# MAGIC 
# MAGIC Manasa Andru                  2017-06-26            TFS - 19354               Updated the performance parameters with         IntegrateDev1                   Jag Yelavarthi          2017-06-30
# MAGIC                                                                                                      the right values to avoid job abend due to mutex error.     
# MAGIC 
# MAGIC Sudheer Champati            2017-09-11       5599                       Added 4 new columns for workday at the end to         IntegrateDev2                  Jag Yelavarthi          2017-09-12
# MAGIC                                                                                                  load the JRNL_ENTRY_TRANS table.
# MAGIC 
# MAGIC Tim Sieg                           2017-10-06       5599                       Added JRNL_ENTRY_TRANS_CRSWALK table extract       IntegrateDev2        Kalyan Neelam      2017-10-10
# MAGIC                                                                                                  hf_jrnl_entry_trans_crswalk and IDS parameters
# MAGIC                                                                                                  to load JRNL_ENTRY_TRANS_CRSWALK_SK to JRNL_ENTRY_TRANS 
# MAGIC 
# MAGIC Tim Sieg                         2017-11-06         5599                      Changed FNCL_LOB extract and  hash file to only use the   IntegrateDev2        Jag Yelavarthi         2017-11-29
# MAGIC                                                                                                  required fields for the GL_FNCL_LOB_CD lookup in the 
# MAGIC                                                                                                 ForeignKey trans
# MAGIC 
# MAGIC Tim Sieg                         2018-02-13         5599                      	 Modify SQL for last updated row in                                      IntegrateDev2          Kalyan Neelam       2018-02-20
# MAGIC                                                                                                  JRNL_ENTRY_TRANS_CRSWALK           
# MAGIC                                                                                                  extract in FNCL_LOB stage

# MAGIC If primary key found, assign surrogate key, otherwise get next key and update hash file.
# MAGIC Extract CRSWALK_SK for FNCL_LOB historical lookup. Using LGCY_FNCL_LOB_CD from source
# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load - one for JRNL_ENTRY_TRANS (the main table) and one for JRNL_ENTRY_PRCS (extra information for I/S personnel - tied to TRANS by surrogate key)
# MAGIC Assign foreign keys and recycle keys not found.
# MAGIC Create default rows for UNK and NA
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType,
    DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Add shared container references (none found in this job JSON, so none included).

# ----------------------------------------------------------------
# Retrieve job parameters
# ----------------------------------------------------------------
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
InFile = get_widget_value('InFile', '')
TmpOutFile = get_widget_value('TmpOutFile', '')
TmpOutFilePRCS = get_widget_value('TmpOutFilePRCS', '')
Logging = get_widget_value('Logging', '')
SrcSysCdSk = get_widget_value('SrcSysCdSk', '')

# ----------------------------------------------------------------
# Stage: JrnlEntryExtr (CSeqFileStage) - Read a .dat file
# ----------------------------------------------------------------
schema_JrnlEntryExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("JRNL_ENTRY_TRANS_SK", IntegerType(), nullable=False),
    StructField("SRC_TRANS_CK", DecimalType(38,10), nullable=False),
    StructField("SRC_TRANS_TYP_CD", StringType(), nullable=False),
    StructField("ACCTG_DT_SK", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("TRANS_TBL_SK", IntegerType(), nullable=False),
    StructField("FNCL_LOB", StringType(), nullable=False),
    StructField("JRNL_ENTRY_TRANS_DR_CR_CD", StringType(), nullable=False),
    StructField("DIST_GL_IN", StringType(), nullable=False),
    StructField("JRNL_ENTRY_TRANS_AMT", DecimalType(38,10), nullable=False),
    StructField("TRANS_LN_NO", IntegerType(), nullable=False),
    StructField("ACCT_NO", StringType(), nullable=False),
    StructField("AFFILIAT_NO", StringType(), nullable=False),
    StructField("APPL_JRNL_ID", StringType(), nullable=False),
    StructField("BUS_UNIT_GL_NO", StringType(), nullable=False),
    StructField("BUS_UNIT_NO", StringType(), nullable=False),
    StructField("CC_ID", StringType(), nullable=False),
    StructField("JRNL_LN_DESC", StringType(), nullable=True),
    StructField("JRNL_LN_REF_NO", StringType(), nullable=False),
    StructField("OPR_UNIT_NO", StringType(), nullable=False),
    StructField("SUB_ACCT_NO", StringType(), nullable=False),
    StructField("TRANS_ID", StringType(), nullable=False),
    StructField("PRCS_MAP_1_TX", StringType(), nullable=False),
    StructField("PRCS_MAP_2_TX", StringType(), nullable=False),
    StructField("PRCS_MAP_3_TX", StringType(), nullable=False),
    StructField("PRCS_MAP_4_TX", StringType(), nullable=False),
    StructField("PRCS_SUB_SRC_CD", StringType(), nullable=False),
    StructField("CLCL_ID", StringType(), nullable=False),
    StructField("WD_LOB_CD", StringType(), nullable=True),
    StructField("WD_BANK_ACCT_NM", StringType(), nullable=True),
    StructField("WD_RVNU_CAT_ID", StringType(), nullable=True),
    StructField("WD_SPEND_CAT_ID", StringType(), nullable=True),
])

df_JrnlEntryExtr = (
    spark.read
    .option("quote", '"')
    .schema(schema_JrnlEntryExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

# ----------------------------------------------------------------
# Stage: FNCL_LOB (DB2Connector) - Read from IDS database
# ----------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT
TRIM(CRSWALK.LGCY_FNCL_LOB_CD) AS LGCY_FNCL_LOB_CD,
CRSWALK.JRNL_ENTRY_TRANS_CRSWALK_SK,
CRSWALK.GL_FNCL_LOB_CD
FROM {IDSOwner}.JRNL_ENTRY_TRANS_CRSWALK CRSWALK,
(
  SELECT SRC_SYS_CD,
         LGCY_FNCL_LOB_CD,
         MAX(JRNL_ENTRY_TRANS_CRSWALK_SK) AS MXSK,
         MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS MXUPDTSK
  FROM {IDSOwner}.JRNL_ENTRY_TRANS_CRSWALK
  GROUP BY SRC_SYS_CD,
           LGCY_FNCL_LOB_CD
) MX
WHERE CRSWALK.JRNL_ENTRY_TRANS_CRSWALK_SK = MX.MXSK
  AND CRSWALK.LAST_UPDT_RUN_CYC_EXCTN_SK = MX.MXUPDTSK
  AND CRSWALK.LGCY_FNCL_LOB_CD = MX.LGCY_FNCL_LOB_CD
"""
df_FNCL_LOB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# ----------------------------------------------------------------
# Hashed File: hf_jrnl_entry_trans_crswalk (Scenario A: Intermediate hashed file)
# We deduplicate on primary key = LGCY_FNCL_LOB_CD, remove the hashed file altogether.
# ----------------------------------------------------------------
df_FNCL_LOB_dedup = dedup_sort(df_FNCL_LOB, ["LGCY_FNCL_LOB_CD"], [])

# ----------------------------------------------------------------
# Stage: ForeignKey (CTransformerStage)
# Join: Primary link = JrnlEntryExtr (df_JrnlEntryExtr), Lookup link = df_FNCL_LOB_dedup
# with conditions:
#   trim(FNCL_LOB) == LGCY_FNCL_LOB_CD
#   WD_LOB_CD == GL_FNCL_LOB_CD
# Stage variables set as new columns, then output links.
# ----------------------------------------------------------------
df_transform = df_JrnlEntryExtr.alias("Key").join(
    df_FNCL_LOB_dedup.alias("DSLink47"),
    (
        trim(F.col("Key.FNCL_LOB")) == F.col("DSLink47.LGCY_FNCL_LOB_CD")
    )
    & (
        F.col("Key.WD_LOB_CD") == F.col("DSLink47.GL_FNCL_LOB_CD")
    ),
    "left"
)

df_transform = (
    df_transform
    # Stage variables:
    .withColumn(
        "SrcCKClmId",
        F.when(
            (F.col("SRC_TRANS_TYP_CD") == 'CLM') | 
            (F.col("SRC_TRANS_TYP_CD") == '820') | 
            (F.col("SRC_TRANS_TYP_CD") == 'MVP'),
            F.col("CLCL_ID")
        ).otherwise(F.col("SRC_TRANS_CK"))
    )
    .withColumn(
        "SrcTransTypCdSk",
        F.when(
            F.col("SRC_TRANS_TYP_CD") == 'MVP',
            GetFkeyCodes(
                'BCBSFINANCE',
                F.col("JRNL_ENTRY_TRANS_SK"),
                "SOURCE TRANSACTION TYPE CODE",
                F.col("SRC_TRANS_TYP_CD"),
                Logging
            )
        ).otherwise(
            GetFkeyCodes(
                F.col("SRC_SYS_CD"),
                F.col("JRNL_ENTRY_TRANS_SK"),
                "SOURCE TRANSACTION TYPE CODE",
                F.col("SRC_TRANS_TYP_CD"),
                Logging
            )
        )
    )
    .withColumn(
        "TransTblSk",
        F.when(
            (F.col("SRC_TRANS_TYP_CD") == 'CLM') |
            (F.col("SRC_TRANS_TYP_CD") == '820') |
            (F.col("SRC_TRANS_TYP_CD") == 'MVP'),
            F.col("TRANS_TBL_SK")
        ).otherwise(
            GetFkeyFnclTrans(
                F.col("JRNL_ENTRY_TRANS_SK"),
                F.col("SRC_TRANS_TYP_CD"),
                F.col("SRC_SYS_CD"),
                F.col("SrcCKClmId"),
                F.col("ACCTG_DT_SK"),
                Logging
            )
        )
    )
    .withColumn(
        "FnclLob",
        GetFkeyFnclLob(
            "PSI",
            F.col("JRNL_ENTRY_TRANS_SK"),
            F.col("FNCL_LOB"),
            Logging
        )
    )
    .withColumn(
        "JrnlEntryDbCr",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("JRNL_ENTRY_TRANS_SK"),
            "JOURNAL ENTRY TRANSACTION DEBIT CREDIT",
            F.col("JRNL_ENTRY_TRANS_DR_CR_CD"),
            Logging
        )
    )
    .withColumn(
        "AcctgDt",
        GetFkeyDate(
            "IDS",
            F.col("JRNL_ENTRY_TRANS_SK"),
            F.col("ACCTG_DT_SK"),
            Logging
        )
    )
    .withColumn(
        "PassThru",
        F.col("PASS_THRU_IN")
    )
    .withColumn(
        "ErrCount",
        GetFkeyErrorCnt(
            F.col("JRNL_ENTRY_TRANS_SK")
        )
    )
    .withColumn(
        "svCrswlkSK",
        F.when(
            F.isnull(F.col("JRNL_ENTRY_TRANS_CRSWALK_SK")) |
            (trim(F.col("JRNL_ENTRY_TRANS_CRSWALK_SK")) == '') |
            (trim(F.col("JRNL_ENTRY_TRANS_CRSWALK_SK")) == '0') |
            (trim(F.col("JRNL_ENTRY_TRANS_CRSWALK_SK")) == '1'),
            F.lit(0)
        ).otherwise(
            F.col("JRNL_ENTRY_TRANS_CRSWALK_SK")
        )
    )
    .withColumn(
        "svGlLob",
        F.when(
            F.isnull(F.col("GL_FNCL_LOB_CD")) |
            (trim(F.col("GL_FNCL_LOB_CD")) == '') |
            (trim(F.col("LGCY_FNCL_LOB_CD")) == 'NA') |
            (trim(F.col("LGCY_FNCL_LOB_CD")) == 'UNK'),
            trim(F.col("WD_LOB_CD"))
        ).otherwise(
            F.col("GL_FNCL_LOB_CD")
        )
    )
)

# ----------------------------------------------------------------
# OutputLink "Fkey": Constraint => (ErrCount = 0 Or PassThru = 'Y')
# ----------------------------------------------------------------
df_fkey = df_transform.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y')
).select(
    F.col("JRNL_ENTRY_TRANS_SK").alias("JRNL_ENTRY_TRANS_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("SRC_TRANS_CK").alias("SRC_TRANS_CK"),
    F.col("SrcTransTypCdSk").alias("SRC_TRANS_TYP_CD_SK"),
    F.col("AcctgDt").alias("ACCTG_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("TransTblSk").alias("TRANS_TBL_SK"),
    F.col("FnclLob").alias("FNCL_LOB_SK"),
    F.col("JrnlEntryDbCr").alias("JRNL_ENTRY_TRANS_DR_CR_CD_SK"),
    F.col("DIST_GL_IN").alias("DIST_GL_IN"),
    F.col("JRNL_ENTRY_TRANS_AMT").alias("JRNL_ENTRY_TRANS_AMT"),
    F.col("TRANS_LN_NO").alias("TRANS_LN_NO"),
    F.col("ACCT_NO").alias("ACCT_NO"),
    F.col("AFFILIAT_NO").alias("AFFILIATE_NO"),
    F.col("APPL_JRNL_ID").alias("APP_JRNL_ID"),
    F.col("BUS_UNIT_GL_NO").alias("BUS_UNIT_GL_NO"),
    F.col("BUS_UNIT_NO").alias("BUS_UNIT_NO"),
    F.col("CC_ID").alias("CC_ID"),
    F.col("JRNL_LN_DESC").alias("JRNL_LN_DESC"),
    F.col("JRNL_LN_REF_NO").alias("JRNL_LN_REF_NO"),
    F.col("OPR_UNIT_NO").alias("OPR_UNIT_NO"),
    F.col("SUB_ACCT_NO").alias("SUB_ACCT_NO"),
    F.col("TRANS_ID").alias("TRANS_ID"),
    F.col("svCrswlkSK").alias("JRNL_ENTRY_TRANS_CRSWALK_SK"),
    F.col("svGlLob").alias("GL_FNCL_LOB_CD"),
    trim(F.col("WD_BANK_ACCT_NM")).alias("BANK_ACCT_NM"),
    trim(F.col("WD_RVNU_CAT_ID")).alias("RVNU_CAT_ID"),
    trim(F.col("WD_SPEND_CAT_ID")).alias("SPEND_CAT_ID")
)

# ----------------------------------------------------------------
# OutputLink "Recycle": Constraint => (ErrCount > 0)
# ----------------------------------------------------------------
df_recycle = df_transform.filter(
    F.col("ErrCount") > 0
).select(
    GetRecycleKey(F.col("JRNL_ENTRY_TRANS_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("JRNL_ENTRY_TRANS_SK").alias("JRNL_ENTRY_TRANS_SK"),
    F.col("SRC_TRANS_CK").alias("SRC_TRANS_CK"),
    F.col("SRC_TRANS_TYP_CD").alias("SRC_TRANS_TYP_CD"),
    F.col("ACCTG_DT_SK").alias("ACCTG_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("TRANS_TBL_SK").alias("TRANS_TBL_SK"),
    F.col("FNCL_LOB").alias("FNCL_LOB"),
    F.col("JRNL_ENTRY_TRANS_DR_CR_CD").alias("JRNL_ENTRY_TRANS_DR_CR_CD"),
    F.col("DIST_GL_IN").alias("DIST_GL_IN"),
    F.col("JRNL_ENTRY_TRANS_AMT").alias("JRNL_ENTRY_TRANS_AMT"),
    F.col("TRANS_LN_NO").alias("TRANS_LN_NO"),
    F.col("ACCT_NO").alias("ACCT_NO"),
    F.col("AFFILIAT_NO").alias("AFFILIAT_NO"),
    F.col("APPL_JRNL_ID").alias("APPL_JRNL_ID"),
    F.col("BUS_UNIT_GL_NO").alias("BUS_UNIT_GL_NO"),
    F.col("BUS_UNIT_NO").alias("BUS_UNIT_NO"),
    F.col("CC_ID").alias("CC_ID"),
    F.col("JRNL_LN_DESC").alias("JRNL_LN_DESC"),
    F.col("JRNL_LN_REF_NO").alias("JRNL_LN_REF_NO"),
    F.col("OPR_UNIT_NO").alias("OPR_UNIT_NO"),
    F.col("SUB_ACCT_NO").alias("SUB_ACCT_NO"),
    F.col("TRANS_ID").alias("TRANS_ID"),
    F.col("PRCS_MAP_1_TX").alias("PRCS_MAP_1_TX"),
    F.col("PRCS_MAP_2_TX").alias("PRCS_MAP_2_TX"),
    F.col("PRCS_MAP_3_TX").alias("PRCS_MAP_3_TX"),
    F.col("PRCS_MAP_4_TX").alias("PRCS_MAP_4_TX"),
    F.col("PRCS_SUB_SRC_CD").alias("PRCS_SUB_SRC_CD"),
    F.col("CLCL_ID").alias("CLCL_ID")
)

# ----------------------------------------------------------------
# OutputLink "DefaultUNK": Constraint => (@INROWNUM = 1) in DataStage
# Create exactly one row with the specified defaults.
# ----------------------------------------------------------------
df_defaultUNK = spark.createDataFrame(
    [
        (
            0, 0, 0, 0, '1753-01-01', 0, 0, 0, 0, 0,
            'U',
            0.00,
            0,
            '0',
            '0',
            ' ',  # APP_JRNL_ID => Space(1)
            ' ',  # BUS_UNIT_GL_NO => ' '
            ' ',  # BUS_UNIT_NO => ' '
            ' ',  # CC_ID => ' '
            ' ',  # JRNL_LN_DESC => ' '
            ' ',  # JRNL_LN_REF_NO => ' '
            ' ',  # OPR_UNIT_NO => ' '
            ' ',  # SUB_ACCT_NO => ' '
            ' ',  # TRANS_ID => ' '
            0,
            ' ',  # GL_FNCL_LOB_CD => Space(1)
            ' ',  # BANK_ACCT_NM => Space(1)
            ' ',  # RVNU_CAT_ID => Space(1)
            ' '   # SPEND_CAT_ID => Space(1)
        )
    ],
    [
        "JRNL_ENTRY_TRANS_SK","SRC_SYS_CD_SK","SRC_TRANS_CK","SRC_TRANS_TYP_CD_SK","ACCTG_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","TRANS_TBL_SK","FNCL_LOB_SK","JRNL_ENTRY_TRANS_DR_CR_CD_SK",
        "DIST_GL_IN","JRNL_ENTRY_TRANS_AMT","TRANS_LN_NO","ACCT_NO","AFFILIATE_NO","APP_JRNL_ID",
        "BUS_UNIT_GL_NO","BUS_UNIT_NO","CC_ID","JRNL_LN_DESC","JRNL_LN_REF_NO","OPR_UNIT_NO",
        "SUB_ACCT_NO","TRANS_ID","JRNL_ENTRY_TRANS_CRSWALK_SK","GL_FNCL_LOB_CD","BANK_ACCT_NM","RVNU_CAT_ID","SPEND_CAT_ID"
    ]
)

# ----------------------------------------------------------------
# OutputLink "DefaultNA": Constraint => (@INROWNUM = 1) in DataStage
# Create exactly one row with the specified defaults.
# ----------------------------------------------------------------
df_defaultNA = spark.createDataFrame(
    [
        (
            1, 1, 1, 1, '1753-01-01',
            1, 1, 1, 1, 1,
            'N',
            0.00,
            1,
            ' ',
            ' ',
            ' ',
            ' ',
            ' ',
            ' ',
            ' ',
            ' ',
            ' ',
            ' ',
            ' ',
            1,
            ' ',
            ' ',
            ' ',
            ' '
        )
    ],
    [
        "JRNL_ENTRY_TRANS_SK","SRC_SYS_CD_SK","SRC_TRANS_CK","SRC_TRANS_TYP_CD_SK","ACCTG_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","TRANS_TBL_SK","FNCL_LOB_SK","JRNL_ENTRY_TRANS_DR_CR_CD_SK",
        "DIST_GL_IN","JRNL_ENTRY_TRANS_AMT","TRANS_LN_NO","ACCT_NO","AFFILIATE_NO","APP_JRNL_ID",
        "BUS_UNIT_GL_NO","BUS_UNIT_NO","CC_ID","JRNL_LN_DESC","JRNL_LN_REF_NO","OPR_UNIT_NO",
        "SUB_ACCT_NO","TRANS_ID","JRNL_ENTRY_TRANS_CRSWALK_SK","GL_FNCL_LOB_CD","BANK_ACCT_NM","RVNU_CAT_ID","SPEND_CAT_ID"
    ]
)

# ----------------------------------------------------------------
# OutputLink "JrnlEntryPrcsOut": Constraint => (ErrCount = 0 Or PassThru = 'Y')
# ----------------------------------------------------------------
df_JrnlEntryPrcs = df_transform.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y')
).select(
    F.col("JRNL_ENTRY_TRANS_SK").alias("JRNL_ENTRY_TRANS_SK"),
    F.col("PRCS_MAP_1_TX").alias("PRCS_MAP_1_TX"),
    F.col("PRCS_MAP_2_TX").alias("PRCS_MAP_2_TX"),
    F.col("PRCS_MAP_3_TX").alias("PRCS_MAP_3_TX"),
    F.col("PRCS_MAP_4_TX").alias("PRCS_MAP_4_TX"),
    F.col("PRCS_SUB_SRC_CD").alias("PRCS_SUB_SRC_CD")
)

# ----------------------------------------------------------------
# Stage: hf_recycle (CHashedFileStage) - Scenario C => write to parquet
# ----------------------------------------------------------------
# Before writing, apply rpad to char/varchar columns that have known lengths:
df_recycle_rpad = df_recycle.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),     # varchar, no length found => no rpad
    F.col("PRI_KEY_STRING"), # varchar, no length found => no rpad
    F.col("JRNL_ENTRY_TRANS_SK"),
    F.col("SRC_TRANS_CK"),
    F.col("SRC_TRANS_TYP_CD"), # varchar, no length found => no rpad
    F.rpad(F.col("ACCTG_DT_SK"), 10, " ").alias("ACCTG_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("TRANS_TBL_SK"),
    F.rpad(F.col("FNCL_LOB"), 10, " ").alias("FNCL_LOB"),
    F.rpad(F.col("JRNL_ENTRY_TRANS_DR_CR_CD"), 2, " ").alias("JRNL_ENTRY_TRANS_DR_CR_CD"),
    F.rpad(F.col("DIST_GL_IN"), 1, " ").alias("DIST_GL_IN"),
    F.col("JRNL_ENTRY_TRANS_AMT"),
    F.col("TRANS_LN_NO"),
    F.rpad(F.col("ACCT_NO"), 10, " ").alias("ACCT_NO"),
    F.rpad(F.col("AFFILIAT_NO"), 5, " ").alias("AFFILIAT_NO"),
    F.rpad(F.col("APPL_JRNL_ID"), 10, " ").alias("APPL_JRNL_ID"),
    F.rpad(F.col("BUS_UNIT_GL_NO"), 5, " ").alias("BUS_UNIT_GL_NO"),
    F.rpad(F.col("BUS_UNIT_NO"), 5, " ").alias("BUS_UNIT_NO"),
    F.rpad(F.col("CC_ID"), 10, " ").alias("CC_ID"),
    F.rpad(F.col("JRNL_LN_DESC"), 30, " ").alias("JRNL_LN_DESC"),
    F.rpad(F.col("JRNL_LN_REF_NO"), 10, " ").alias("JRNL_LN_REF_NO"),
    F.rpad(F.col("OPR_UNIT_NO"), 8, " ").alias("OPR_UNIT_NO"),
    F.rpad(F.col("SUB_ACCT_NO"), 10, " ").alias("SUB_ACCT_NO"),
    F.rpad(F.col("TRANS_ID"), 10, " ").alias("TRANS_ID"),
    F.rpad(F.col("PRCS_MAP_1_TX"), 4, " ").alias("PRCS_MAP_1_TX"),
    F.rpad(F.col("PRCS_MAP_2_TX"), 4, " ").alias("PRCS_MAP_2_TX"),
    F.rpad(F.col("PRCS_MAP_3_TX"), 4, " ").alias("PRCS_MAP_3_TX"),
    F.rpad(F.col("PRCS_MAP_4_TX"), 4, " ").alias("PRCS_MAP_4_TX"),
    F.rpad(F.col("PRCS_SUB_SRC_CD"), 5, " ").alias("PRCS_SUB_SRC_CD"),
    F.rpad(F.col("CLCL_ID"), 12, " ").alias("CLCL_ID")
)

write_files(
    df_recycle_rpad,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------
# Stage: JrnlEntryPrcs (CSeqFileStage) - Writing the df_JrnlEntryPrcsOut
# ----------------------------------------------------------------
# Apply rpad to char columns:
df_JrnlEntryPrcs_rpad = df_JrnlEntryPrcs.select(
    F.col("JRNL_ENTRY_TRANS_SK"),
    F.rpad(F.col("PRCS_MAP_1_TX"), 4, " ").alias("PRCS_MAP_1_TX"),
    F.rpad(F.col("PRCS_MAP_2_TX"), 4, " ").alias("PRCS_MAP_2_TX"),
    F.rpad(F.col("PRCS_MAP_3_TX"), 4, " ").alias("PRCS_MAP_3_TX"),
    F.rpad(F.col("PRCS_MAP_4_TX"), 4, " ").alias("PRCS_MAP_4_TX"),
    F.rpad(F.col("PRCS_SUB_SRC_CD"), 5, " ").alias("PRCS_SUB_SRC_CD")
)

write_files(
    df_JrnlEntryPrcs_rpad,
    f"{adls_path}/load/{TmpOutFilePRCS}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------
# Stage: Collector (CCollector) - inputs Fkey, DefaultUNK, DefaultNA
# Output link => LoadFile => next stage
# We union these three dataframes. They have the same 29 columns in the same order.
# ----------------------------------------------------------------
df_fkey2 = df_fkey.select(
    "JRNL_ENTRY_TRANS_SK","SRC_SYS_CD_SK","SRC_TRANS_CK","SRC_TRANS_TYP_CD_SK",
    "ACCTG_DT_SK","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","TRANS_TBL_SK",
    "FNCL_LOB_SK","JRNL_ENTRY_TRANS_DR_CR_CD_SK","DIST_GL_IN","JRNL_ENTRY_TRANS_AMT",
    "TRANS_LN_NO","ACCT_NO","AFFILIATE_NO","APP_JRNL_ID","BUS_UNIT_GL_NO",
    "BUS_UNIT_NO","CC_ID","JRNL_LN_DESC","JRNL_LN_REF_NO","OPR_UNIT_NO",
    "SUB_ACCT_NO","TRANS_ID","JRNL_ENTRY_TRANS_CRSWALK_SK","GL_FNCL_LOB_CD",
    "BANK_ACCT_NM","RVNU_CAT_ID","SPEND_CAT_ID"
)
df_defaultUNK2 = df_defaultUNK.select(
    "JRNL_ENTRY_TRANS_SK","SRC_SYS_CD_SK","SRC_TRANS_CK","SRC_TRANS_TYP_CD_SK",
    "ACCTG_DT_SK","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","TRANS_TBL_SK",
    "FNCL_LOB_SK","JRNL_ENTRY_TRANS_DR_CR_CD_SK","DIST_GL_IN","JRNL_ENTRY_TRANS_AMT",
    "TRANS_LN_NO","ACCT_NO","AFFILIATE_NO","APP_JRNL_ID","BUS_UNIT_GL_NO",
    "BUS_UNIT_NO","CC_ID","JRNL_LN_DESC","JRNL_LN_REF_NO","OPR_UNIT_NO",
    "SUB_ACCT_NO","TRANS_ID","JRNL_ENTRY_TRANS_CRSWALK_SK","GL_FNCL_LOB_CD",
    "BANK_ACCT_NM","RVNU_CAT_ID","SPEND_CAT_ID"
)
df_defaultNA2 = df_defaultNA.select(
    "JRNL_ENTRY_TRANS_SK","SRC_SYS_CD_SK","SRC_TRANS_CK","SRC_TRANS_TYP_CD_SK",
    "ACCTG_DT_SK","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","TRANS_TBL_SK",
    "FNCL_LOB_SK","JRNL_ENTRY_TRANS_DR_CR_CD_SK","DIST_GL_IN","JRNL_ENTRY_TRANS_AMT",
    "TRANS_LN_NO","ACCT_NO","AFFILIATE_NO","APP_JRNL_ID","BUS_UNIT_GL_NO",
    "BUS_UNIT_NO","CC_ID","JRNL_LN_DESC","JRNL_LN_REF_NO","OPR_UNIT_NO",
    "SUB_ACCT_NO","TRANS_ID","JRNL_ENTRY_TRANS_CRSWALK_SK","GL_FNCL_LOB_CD",
    "BANK_ACCT_NM","RVNU_CAT_ID","SPEND_CAT_ID"
)

df_Collector = df_fkey2.union(df_defaultUNK2).union(df_defaultNA2)

# ----------------------------------------------------------------
# Stage: JrnlEntryTrans (CSeqFileStage) - Write to #TmpOutFile
# ----------------------------------------------------------------
# Apply rpad for char/varchar columns that have lengths:
df_JrnlEntryTrans_rpad = df_Collector.select(
    F.col("JRNL_ENTRY_TRANS_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("SRC_TRANS_CK"),
    F.col("SRC_TRANS_TYP_CD_SK"),
    F.rpad(F.col("ACCTG_DT_SK"), 10, " ").alias("ACCTG_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("TRANS_TBL_SK"),
    F.col("FNCL_LOB_SK"),
    F.col("JRNL_ENTRY_TRANS_DR_CR_CD_SK"),
    F.rpad(F.col("DIST_GL_IN"), 1, " ").alias("DIST_GL_IN"),
    F.col("JRNL_ENTRY_TRANS_AMT"),
    F.col("TRANS_LN_NO"),
    F.rpad(F.col("ACCT_NO"), 10, " ").alias("ACCT_NO"),
    F.rpad(F.col("AFFILIATE_NO"), 5, " ").alias("AFFILIATE_NO"),
    F.rpad(F.col("APP_JRNL_ID"), 10, " ").alias("APP_JRNL_ID"),
    F.rpad(F.col("BUS_UNIT_GL_NO"), 5, " ").alias("BUS_UNIT_GL_NO"),
    F.rpad(F.col("BUS_UNIT_NO"), 5, " ").alias("BUS_UNIT_NO"),
    F.rpad(F.col("CC_ID"), 10, " ").alias("CC_ID"),
    F.rpad(F.col("JRNL_LN_DESC"), 30, " ").alias("JRNL_LN_DESC"),
    F.rpad(F.col("JRNL_LN_REF_NO"), 10, " ").alias("JRNL_LN_REF_NO"),
    F.rpad(F.col("OPR_UNIT_NO"), 8, " ").alias("OPR_UNIT_NO"),
    F.rpad(F.col("SUB_ACCT_NO"), 10, " ").alias("SUB_ACCT_NO"),
    F.rpad(F.col("TRANS_ID"), 10, " ").alias("TRANS_ID"),
    F.col("JRNL_ENTRY_TRANS_CRSWALK_SK"),
    F.col("GL_FNCL_LOB_CD"),  # no length given => skip rpad
    F.col("BANK_ACCT_NM"),    # no length
    F.col("RVNU_CAT_ID"),     # no length
    F.col("SPEND_CAT_ID")     # no length
)

write_files(
    df_JrnlEntryTrans_rpad,
    f"{adls_path}/load/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)