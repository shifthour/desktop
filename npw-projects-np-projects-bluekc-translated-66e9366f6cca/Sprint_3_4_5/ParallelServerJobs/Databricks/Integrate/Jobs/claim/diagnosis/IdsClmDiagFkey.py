# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2021 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2023 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  IdsClmDiagFkey
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC Calling Job(s):IdsBCAFEPMedClmLoadSeq,IdsBCBSAClmLoadSeq,IdsBCBSSCClmLoad1Seq,IdsEyeMedClmLoadSeq,IdsFctsClmLoad2Seq,IdsLhoFctsClmLoad2Seq
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer		Date		Project/Altius #		Change Description							Development Project		Code Reviewer		Date Reviewed  
# MAGIC =================================================================================================================================================================================================================
# MAGIC Steph Goddard		04/2004					Originally Programmed
# MAGIC SharonAndrew		06/21/2004				put GetRecycleKey( ClmDiagCrfIn.CLM_DIAG_SK) into recycled derivation
# MAGIC Brent Leland		09/07/2004				Added link partitioner
# MAGIC Ralph Tucker		2008-7-25	365			Primary Key   Added SrcSysCdSk parameter                                     		devlIDS				Steph Goddard		07/27/2008
# MAGIC Parik			2008-08-13	3567			Primary Key    Removed the source system code sk since PCTA       		devlIDS				Steph Goddard		08/19/2008
# MAGIC 								uses many sources and lookup needs to be done
# MAGIC 								within job
# MAGIC SAndrew			2008-12-22	DRG			Added new field CLM_DIAG_POA_CD_SK to load file			devlIDS				Brent Leland		12-29-2008
# MAGIC 								Added new field CLM_DIAG_POA_CD to input file
# MAGIC Rick Henry		2012-04-24	4896			Added Diag_Cd_Typ_cd to SK lookup					NewDevl				SAndrew			2012-05-17
# MAGIC Kalyan Neelam		2014-12-17	5212			Added If SRC_SYS_CD = 'BCBSA' Then 'BCA' Else 			IntegrateCurDevl 			Bhoomi Dasari		02/04/2015
# MAGIC 								SRC_SYS_CD in the stage variables and pass it to GetFkeyCodes 
# MAGIC 								because code sets are created under BCA for BCBSA							
# MAGIC Sudhir Bomshetty		2017-11-03	5781			Added SRC_SYS_CD = 'BCA' for ClmDiagPOACdSk			IntegrateDev2			Jag Yelavarthi		2017-11-10
# MAGIC 								stage variable
# MAGIC Reddy Sanam		2020-08-17				In Stage variable DiagSk's Derivation "FACETS"				IntegrateDev2
# MAGIC 								is replaced with #Source#
# MAGIC 								Mapped "FACETS" when source is LUMERIS
# MAGIC 								for this stage variable "svCdMpngSrcSysCd"
# MAGIC Sunitha Ganta		10-12-2020				Brought up to standards
# MAGIC Vikas Abbu		2021-03-01	RA			Added Src Sys Cd values for EMR Procedure Data			IntegrateDev2			Abhiram Dasarathy		2021-03-03
# MAGIC 								in stage variable                                                                                     
# MAGIC Mrudula Kodali		2021-03-15	311337			Updated DiagCdSk for Livongo and					IntegrateDev2			Jaideep Mankala		03/18/2021
# MAGIC 								Solutran Encounters data                  
# MAGIC Mrudula Kodali		2021-03-24	311337			Added'LVNGHLTH'and 'SOLUTRAN' in DiagCdSk Stage			IntegrateDev2			Hugh Sisson		2021-03-25
# MAGIC 
# MAGIC Rajasekhar Kamma		10/15/2021	us447658			In the stage "ForeignKey" I added a new row				IntegrateDev1			Ken Bradmon		2021-10-15	 
# MAGIC 								"MOSAICLIFECARE" for the stage variable "svEMRSrcSysCd."
# MAGIC 								That is the only change.
# MAGIC 
# MAGIC Venkata Yama		2022-01-12            us480876                                   Added SRC_SYS_CD='HCA'                                                			IntegrateDev2			Harsha Ravuri		2022-01-13
# MAGIC 
# MAGIC Manisha Gandra    		2021-01-15               US 459610                        	Added NATIONS to StageVar DiagCdSK                                         		IntegrateDev2                    		Jeyaprasanna                          2022-01-20
# MAGIC 
# MAGIC Reddy Sanam        		2023-06-15                US586933                       	Added SRC_SYS_CD='BCA' to pass FACETS so the
# MAGIC                                                                                                         		diag codes come through without any issues                                      		IntegrateDev2(11.7)            		Goutham Kalidindi                     2023-06-15
# MAGIC 
# MAGIC Ken Bradmon		2023-08-07	us559895			Added 8 new Providers to the svEMRSrcSysCd variable of the ForeignKey	IntegrateDev2                                        Reddy Sanam                            2023-10-27		
# MAGIC 								stage.  CENTRUSHEALTH, MOHEALTH, GOLDENVALLEY, JEFFERSON, 
# MAGIC 								WESTMOMEDCNTR, BLUESPRINGS, EXCELSIOR, and HARRISONVILLE.
# MAGIC 								This is the only change.
# MAGIC 
# MAGIC Sham Shankaranarayana       2023-12-21               US 599810                        	Added new Stage variable svSBVSrcSysCd and updated existing StageVar DiagCdSk 
# MAGIC                                                                                                                                 in Transformer ForeignKey                                                                                        IntegrateDev1                                       Jeyaprasanna                            2023-12-21     
# MAGIC Harsha Ravuri		2025-05-06	US#649651		Added new provider 'ASCENTIST' to the  svEMRSrcSysCd variable of the	Integrate Dev1		               Jeyaprasanna                            2025-05-20
# MAGIC 								ForeignKey

# MAGIC Writing Sequential File to /load
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DecimalType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Source = get_widget_value("Source","")
InFile = get_widget_value("InFile","")
Logging = get_widget_value("Logging","")

# Read from IdsClmDiagExtr (CSeqFileStage)
schema_IdsClmDiagExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("CLM_DIAG_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CLM_DIAG_ORDNL_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXTCN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXTCN_SK", IntegerType(), nullable=False),
    StructField("DIAG_CD", StringType(), nullable=False),
    StructField("CLM_DIAG_POA_CD", StringType(), nullable=False),
    StructField("DIAG_CD_TYP_CD", StringType(), nullable=False)
])

df_IdsClmDiagExtr = (
    spark.read
    .option("header", False)
    .option("inferSchema", False)
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_IdsClmDiagExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

# Prepare columns needed for the Transformer (ForeignKey)
df_fk = (
    df_IdsClmDiagExtr
    # svEMRSrcSysCd
    .withColumn(
        "svEMRSrcSysCd",
        F.when(
            F.trim(F.col("SRC_SYS_CD")).isin(
                "CLAYPLATTE","JAYHAWK","ENCOMPASS","LIBERTYHOSP","MERITAS",
                "NORTHLAND","OLATHEMED","PROVIDENCE","PROVSTLUKES","SUNFLOWER",
                "TRUMAN","UNTDMEDGRP","MOSAIC","MOSAICLIFECARE","BARRYPOINTE",
                "PRIMEMO","SPIRA","HCA","NONSTDSUPLMTDATA","LEAWOODFMLYCARE",
                "CHILDRENSMERCY","CENTRUSHEALTH","MOHEALTH","GOLDENVALLEY",
                "JEFFERSON","WESTMOMEDCNTR","BLUESPRINGS","EXCELSIOR","HARRISONVILLE","ASCENTIST"
            ),
            F.lit("EMR")
        ).otherwise(F.lit(""))
    )
    # svSBVSrcSysCd
    .withColumn(
        "svSBVSrcSysCd",
        F.when(
            F.trim(F.col("SRC_SYS_CD")).isin("DOMINION","MARC"),
            F.lit("SBV")
        ).otherwise(F.lit(""))
    )
    # svCdMpngSrcSysCd
    .withColumn(
        "svCdMpngSrcSysCd",
        F.when(F.col("SRC_SYS_CD")==F.lit("BCBSA"), F.lit("BCA"))
        .when(F.col("SRC_SYS_CD").isin("LUMERIS","DOMINION","MARC"), F.lit("FACETS"))
        .otherwise(F.col("SRC_SYS_CD"))
    )
    # DiagOrdnlCdSk
    .withColumn(
        "DiagOrdnlCdSk",
        GetFkeyCodes(
            F.col("svCdMpngSrcSysCd"),
            F.col("CLM_DIAG_SK"),
            "DIAGNOSIS ORDINAL",
            F.col("CLM_DIAG_ORDNL_CD"),
            Logging
        )
    )
    # ClmSk
    .withColumn(
        "ClmSk",
        GetFkeyClm(
            F.col("SRC_SYS_CD"),
            F.col("CLM_DIAG_SK"),
            F.col("CLM_ID"),
            Logging
        )
    )
    # DiagCdSk
    .withColumn(
        "DiagCdSk",
        GetFkeyDiagCd(
            F.when(
                (F.col("SRC_SYS_CD")==F.lit("LUMERIS")) |
                (F.col("SRC_SYS_CD")==F.lit("EYEMED")) |
                (F.col("svEMRSrcSysCd")==F.lit("EMR")) |
                (F.col("SRC_SYS_CD")==F.lit("LVNGHLTH")) |
                (F.col("SRC_SYS_CD")==F.lit("SOLUTRAN")) |
                (F.substring(F.col("SRC_SYS_CD"),1,6)==F.lit("NATION")) |
                (F.col("SRC_SYS_CD")==F.lit("BCA")) |
                (F.col("svSBVSrcSysCd")==F.lit("SBV")),
                F.lit("FACETS")
            ).otherwise(F.col("SRC_SYS_CD")),
            F.col("CLM_DIAG_SK"),
            F.col("DIAG_CD"),
            F.col("DIAG_CD_TYP_CD"),
            Logging
        )
    )
    # ClmDiagPOACdSk
    .withColumn(
        "ClmDiagPOACdSk",
        F.when(
            (F.col("SRC_SYS_CD")==F.lit("BCA")) | (F.col("svCdMpngSrcSysCd")==F.lit("BCA")),
            GetFkeyCodes(
                "FACETS",
                F.col("CLM_DIAG_SK"),
                "CLAIM DIAGNOSIS PRESENT ON ADMISSION",
                F.col("CLM_DIAG_POA_CD"),
                Logging
            )
        ).otherwise(
            GetFkeyCodes(
                F.col("svCdMpngSrcSysCd"),
                F.col("CLM_DIAG_SK"),
                "CLAIM DIAGNOSIS PRESENT ON ADMISSION",
                F.col("CLM_DIAG_POA_CD"),
                Logging
            )
        )
    )
    # PassThru
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    # ErrCount
    .withColumn(
        "ErrCount",
        GetFkeyErrorCnt(
            F.col("CLM_DIAG_SK")
        )
    )
)

# Generate outputs from the Transformer constraints:
df_Fkey1 = df_fk.filter("(ErrCount = 0) or (PassThru = 'Y')")

# For Recycle1, add columns that differ from the original input expressions
df_Recycle1 = (
    df_fk
    .filter("ErrCount > 0")
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(F.col("CLM_DIAG_SK")))
    .withColumn("RECYCLE_CT", F.col("RECYCLE_CT") + F.lit(1))
)

# Recycle_Clms
df_RecycleClms = (
    df_fk
    .filter("ErrCount > 0")
    .select(
        F.col("SRC_SYS_CD"),
        F.col("CLM_ID")
    )
)

# DefaultUNK (Constraint @OUTROWNUM = 1 with the given WhereExpressions)
df_DefaultUNK = spark.createDataFrame(
    [(0, 0, "UNK", 0, 0, 0, 0, 0, 0)],
    ["CLM_DIAG_SK","SRC_SYS_CD_SK","CLM_ID","CLM_DIAG_ORDNL_CD_SK","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","CLM_SK","DIAG_CD_SK","CLM_DIAG_POA_CD_SK"]
).limit(1)

# DefaultNA (Constraint @OUTROWNUM = 1 with the given WhereExpressions)
df_DefaultNA = spark.createDataFrame(
    [(1, 1, "NA", 1, 1, 1, 1, 1, 1)],
    ["CLM_DIAG_SK","SRC_SYS_CD_SK","CLM_ID","CLM_DIAG_ORDNL_CD_SK","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","CLM_SK","DIAG_CD_SK","CLM_DIAG_POA_CD_SK"]
).limit(1)

# ----- Write hashed files (Scenario C) -----
# hf_recycle
df_Recycle1_write = (
    df_Recycle1.select(
        F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        # char(10)
        F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        # char(1)
        F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
        # char(1)
        F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        F.col("RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("CLM_DIAG_SK").alias("CLM_DIAG_SK"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        # char(2)
        F.rpad(F.col("CLM_DIAG_ORDNL_CD"), 2, " ").alias("DIAG_ORDNL_CD"),
        F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
        F.col("DIAG_CD").alias("DIAG_CD"),
        # char(1)
        F.rpad(F.col("CLM_DIAG_POA_CD"), 1, " ").alias("CLM_DIAG_POA_CD"),
        F.col("DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD")
    )
)

write_files(
    df_Recycle1_write,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# hf_claim_recycle_keys
df_RecycleClms_write = df_RecycleClms.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID")
)

write_files(
    df_RecycleClms_write,
    "hf_claim_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# ----- Collector -----
# Union Fkey1, DefaultUNK, DefaultNA
df_Fkey1_sel = df_Fkey1.select(
    F.col("CLM_DIAG_SK").alias("CLM_DIAG_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("DiagOrdnlCdSk").alias("CLM_DIAG_ORDNL_CD_SK"),
    F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ClmSk").alias("CLM_SK"),
    F.col("DiagCdSk").alias("DIAG_CD_SK"),
    F.col("ClmDiagPOACdSk").alias("CLM_DIAG_POA_CD_SK")
)

df_Collector = (
    df_Fkey1_sel
    .unionByName(df_DefaultUNK)
    .unionByName(df_DefaultNA)
)

# ----- Write final output (CLM_DIAG CSeqFileStage) -----
df_Collector_write = df_Collector.select(
    "CLM_DIAG_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_DIAG_ORDNL_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "DIAG_CD_SK",
    "CLM_DIAG_POA_CD_SK"
)

write_files(
    df_Collector_write,
    f"CLM_DIAG.{Source}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)