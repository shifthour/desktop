# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ********************************************************************************
# MAGIC  Copyright 2004 Blue Cross/Blue Shield of Kansas City
# MAGIC  Copyright 2005 Blue Cross/Blue Shield of Kansas City
# MAGIC  Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC DESCRIPTION:
# MAGIC \(9)Transform extracted Claim Line dental data from Facets into a common format to 
# MAGIC \(9)be potentially merged with claim line records from other systems for the 
# MAGIC \(9)foreign keying process and ultimate load into IDS
# MAGIC 
# MAGIC PRIMARY SOURCE:
# MAGIC \(9)InFile param\(9)- Sequential file containing extracted claim line 
# MAGIC \(9)\(9)\(9)  data to be transformed (../verified)
# MAGIC 
# MAGIC HASH FILES
# MAGIC          The following hash files are created in other processes and are used in other processes besides this job
# MAGIC                 DO NOT CLEAR
# MAGIC                 hf_clm_fcts_reversal - do not clear
# MAGIC                hf_clm_nasco_dup_bypass - do not clear
# MAGIC 
# MAGIC  OUTTPUTS:
# MAGIC 
# MAGIC \(9)TmpOutFile param\(9)- Sequential file (parameterized name) containing 
# MAGIC \(9)\(9)\(9)  transformed claim line data in common record format 
# MAGIC \(9)\(9)\(9)/key
# MAGIC JOB PARAMETERS:
# MAGIC 
# MAGIC \(9)FilePath\(9)\(9)- Directory path for sequential data files
# MAGIC \(9)InFile\(9)\(9)- Name of sequential input file
# MAGIC \(9)TmpOutFile\(9)- Name of sequential output file
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC =============================================================================================================================================================
# MAGIC Developer                    Date                 \(9)Project                                                     Change Description                                         Development Project             Code Reviewer          Date Reviewed    
# MAGIC =============================================================================================================================================================
# MAGIC Manasa Andru          2020-10-14                        US -  263702                                         Original Programming                                              IntegrateDev2                     
# MAGIC \(9)
# MAGIC Goutham Kalidindi    2020-01-15                      US-318408                                 Added 2 new fields APC_ID and APC_STTUS_ID               IntegrateDev2         Reddy S                   1/15/2021
# MAGIC 
# MAGIC Kshema H K             2023-08-01              US-589700                   Added two new fields SNOMED_CT_CD,CVX_VCCN_CD with a default  IntegrateDevB\(9)\(9)Harsha Ravuri\(9)2023-08-31
# MAGIC                                                                                                        value in BusinessRules stage and mapped it till target

# MAGIC Sequential file (parameterized name) containing extracted claim line data to be transformed (../verified)
# MAGIC 
# MAGIC Created in FctsDnltClmLineExtr
# MAGIC Apply transformations to extracted claim line data based on business rules and format output into a common record format to be able for merging with data from other systems for foreign keying and ultimate load into IDS
# MAGIC Extract disallow and override amounts
# MAGIC This container is used in:
# MAGIC ArgusClmLnExtr
# MAGIC PCSClmLnExtr
# MAGIC           FctsClmLnDntlTrns          FctsClmLnMedTrns
# MAGIC FctsClmLnExtr
# MAGIC NascoClmLnExtr
# MAGIC PCTAClmLnExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Added new field MED_PDX_IND to the common layout file that is sent to IdsClmLnFkey job
# MAGIC which indicates whether the claim is Medical or Pharmacy
# MAGIC Indicator used :
# MAGIC MED - medical
# MAGIC PDX - Pharmacy
# MAGIC Hash files created in the FctsClmLnHashFileExtr are also used in FctsClmLnMedTrns, FctsClmLnMedExtr and FctsDntlClmLineExtr
# MAGIC 
# MAGIC Do Not Clear in this process
# MAGIC hf_clm_ln_dsalw_ln_amts
# MAGIC File is written out as \"FctsClmLnDntlExtr.FctsClmLn.dat.#RunID#\" - and will be picked up in Load4 sequencer with the Claim Line sort
# MAGIC Hash files created in FctsClmDriverBuild
# MAGIC hash files created in FctsClmLnMedExtr and used in both FctsClmLnMedTrns and FctsClmLnDntlTrns, 
# MAGIC 
# MAGIC hf_clm_ln_cdmd_provcntrct
# MAGIC hf_clm_ln_chlp
# MAGIC hf_clm_clor
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
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
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnPK
# COMMAND ----------

# Parameters
RunID = get_widget_value('RunID','20120425')
CurrRunCycle = get_widget_value('CurrRunCycle','1')
CurrentDate = get_widget_value('CurrentDate','20120425')
SrcSysCdSk = get_widget_value('SrcSysCdSk','FCTS')

# Read hashed file stages (Scenario C => read parquet)
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
df_hf_clm_ln_dsalw_ln_amts = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_ln_amts.parquet")
df_hf_clm_ln_cdmd_provcntrct = spark.read.parquet(f"{adls_path}/hf_clm_ln_cdmd_provcntrct.parquet")
df_hf_clm_ln_clhp = spark.read.parquet(f"{adls_path}/hf_clm_ln_clhp.parquet")
df_hf_clm_ln_clor = spark.read.parquet(f"{adls_path}/hf_clm_ln_clor.parquet")

# Read seqVerified (CSeqFileStage) => .dat file (define schema, then read)
schema_seqVerified = T.StructType([
    T.StructField("CLCL_ID", T.StringType(), nullable=False),
    T.StructField("CDDL_SEQ_NO", T.IntegerType(), nullable=False),
    T.StructField("EXT_TIMESTAMP", T.TimestampType(), nullable=False),
    T.StructField("PRPR_ID", T.StringType(), nullable=False),
    T.StructField("LOBD_ID", T.StringType(), nullable=False),
    T.StructField("PDVC_LOBD_PTR", T.StringType(), nullable=False),
    T.StructField("DPPY_PFX", T.StringType(), nullable=False),
    T.StructField("LTLT_PFX", T.StringType(), nullable=False),
    T.StructField("CGPY_PFX", T.StringType(), nullable=False),
    T.StructField("DPCG_DP_ID_ALT", T.StringType(), nullable=False),
    T.StructField("CDDL_ALTDP_EXCD_ID", T.StringType(), nullable=False),
    T.StructField("CGCG_ID", T.StringType(), nullable=False),
    T.StructField("CDDL_TOOTH_NO", T.StringType(), nullable=False),
    T.StructField("CDDL_TOOTH_BEG", T.StringType(), nullable=False),
    T.StructField("CDDL_TOOTH_END", T.StringType(), nullable=False),
    T.StructField("CDDL_SURF", T.StringType(), nullable=False),
    T.StructField("UTUT_CD", T.StringType(), nullable=False),
    T.StructField("DEDE_PFX", T.StringType(), nullable=False),
    T.StructField("DPPC_PRICE_ID", T.StringType(), nullable=False),
    T.StructField("DPDP_ID", T.StringType(), nullable=False),
    T.StructField("CGCG_RULE", T.StringType(), nullable=False),
    T.StructField("CDDL_FROM_DT", T.TimestampType(), nullable=False),
    T.StructField("CDDL_CHG_AMT", T.DecimalType(), nullable=False),
    T.StructField("CDDL_CONSIDER_CHG", T.DecimalType(), nullable=False),
    T.StructField("CDDL_ALLOW", T.DecimalType(), nullable=False),
    T.StructField("CDDL_UNITS", T.IntegerType(), nullable=False),
    T.StructField("CDDL_UNITS_ALLOW", T.IntegerType(), nullable=False),
    T.StructField("CDDL_DED_AMT", T.DecimalType(), nullable=False),
    T.StructField("CDDL_DED_AC_NO", T.IntegerType(), nullable=False),
    T.StructField("CDDL_COPAY_AMT", T.DecimalType(), nullable=False),
    T.StructField("CDDL_COINS_AMT", T.DecimalType(), nullable=False),
    T.StructField("CDDL_RISK_WH_AMT", T.DecimalType(), nullable=False),
    T.StructField("CDDL_PAID_AMT", T.DecimalType(), nullable=False),
    T.StructField("CDDL_DISALL_AMT", T.DecimalType(), nullable=False),
    T.StructField("CDDL_DISALL_EXCD", T.StringType(), nullable=False),
    T.StructField("CDDL_AG_PRICE", T.DecimalType(), nullable=False),
    T.StructField("CDDL_PF_PRICE", T.DecimalType(), nullable=False),
    T.StructField("CDDL_DP_PRICE", T.DecimalType(), nullable=False),
    T.StructField("CDDL_IP_PRICE", T.DecimalType(), nullable=False),
    T.StructField("CDDL_PRICE_IND", T.StringType(), nullable=False),
    T.StructField("CDDL_OOP_CALC_BASE", T.DecimalType(), nullable=False),
    T.StructField("CDDL_CL_NTWK_IND", T.StringType(), nullable=False),
    T.StructField("CDDL_REF_IND", T.StringType(), nullable=False),
    T.StructField("CDDL_CAP_IND", T.StringType(), nullable=False),
    T.StructField("CDDL_SB_PYMT_AMT", T.DecimalType(), nullable=False),
    T.StructField("CDDL_PR_PYMT_AMT", T.DecimalType(), nullable=False),
    T.StructField("CDDL_UMREF_ID", T.StringType(), nullable=False),
    T.StructField("CDDL_REFSV_SEQ_NO", T.IntegerType(), nullable=False),
    T.StructField("CLM_LN_FINL_DISP_CD", T.StringType(), nullable=False),
    T.StructField("PSCD_ID", T.StringType(), nullable=False),
    T.StructField("CLCL_NTWK_IND", T.StringType(), nullable=False),
    T.StructField("CDDL_RESP_CD", T.StringType(), nullable=True),
    T.StructField("EXCD_FOUND", T.StringType(), nullable=False),
    T.StructField("CDDL_EOB_EXCD", T.StringType(), nullable=False),
])
df_FctsClmLn = (
    spark.read
    .format("csv")
    .schema(schema_seqVerified)
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .load(f"{adls_path}/verified/LhoFctsClmLnDntlExtr.ClmLnDntl.dat.{RunID}")
)

# Prepare reference data frames for the hashed-file lookups (Lookups stage).
# For the first 8 references, the same parquet (df_hf_clm_ln_dsalw_ln_amts) but filtered differently.

df_RefMY = df_hf_clm_ln_dsalw_ln_amts.filter((F.col("EXCD_RESP_CD") == 'M') & (F.col("BYPS_IN") == 'Y'))
df_RefMN = df_hf_clm_ln_dsalw_ln_amts.filter((F.col("EXCD_RESP_CD") == 'M') & (F.col("BYPS_IN") == 'N'))
df_RefNY = df_hf_clm_ln_dsalw_ln_amts.filter((F.col("EXCD_RESP_CD") == 'N') & (F.col("BYPS_IN") == 'Y'))
df_RefNN = df_hf_clm_ln_dsalw_ln_amts.filter((F.col("EXCD_RESP_CD") == 'N') & (F.col("BYPS_IN") == 'N'))
df_RefOY = df_hf_clm_ln_dsalw_ln_amts.filter((F.col("EXCD_RESP_CD") == 'O') & (F.col("BYPS_IN") == 'Y'))
df_RefON = df_hf_clm_ln_dsalw_ln_amts.filter((F.col("EXCD_RESP_CD") == 'O') & (F.col("BYPS_IN") == 'N'))
df_RefPY = df_hf_clm_ln_dsalw_ln_amts.filter((F.col("EXCD_RESP_CD") == 'P') & (F.col("BYPS_IN") == 'Y'))
df_RefPN = df_hf_clm_ln_dsalw_ln_amts.filter((F.col("EXCD_RESP_CD") == 'P') & (F.col("BYPS_IN") == 'N'))

df_RefPvdCntrct = df_hf_clm_ln_cdmd_provcntrct
df_RefClhp = df_hf_clm_ln_clhp
df_clor_lkup = df_hf_clm_ln_clor

# Now perform the multiple left joins to replicate "Lookups" stage usage in "tBusinessRules".
df_tBusinessRules_input = df_FctsClmLn.alias("FctsClmLn")

df_tBusinessRules_input = (
    df_tBusinessRules_input
    .join(df_RefMY.alias("RefMY"),
          (F.col("FctsClmLn.CLCL_ID") == F.col("RefMY.CLCL_ID")) &
          (F.col("FctsClmLn.CDDL_SEQ_NO") == F.col("RefMY.CDML_SEQ_NO")),
          "left")
    .join(df_RefMN.alias("RefMN"),
          (F.col("FctsClmLn.CLCL_ID") == F.col("RefMN.CLCL_ID")) &
          (F.col("FctsClmLn.CDDL_SEQ_NO") == F.col("RefMN.CDML_SEQ_NO")),
          "left")
    .join(df_RefNY.alias("RefNY"),
          (F.col("FctsClmLn.CLCL_ID") == F.col("RefNY.CLCL_ID")) &
          (F.col("FctsClmLn.CDDL_SEQ_NO") == F.col("RefNY.CDML_SEQ_NO")),
          "left")
    .join(df_RefNN.alias("RefNN"),
          (F.col("FctsClmLn.CLCL_ID") == F.col("RefNN.CLCL_ID")) &
          (F.col("FctsClmLn.CDDL_SEQ_NO") == F.col("RefNN.CDML_SEQ_NO")),
          "left")
    .join(df_RefOY.alias("RefOY"),
          (F.col("FctsClmLn.CLCL_ID") == F.col("RefOY.CLCL_ID")) &
          (F.col("FctsClmLn.CDDL_SEQ_NO") == F.col("RefOY.CDML_SEQ_NO")),
          "left")
    .join(df_RefON.alias("RefON"),
          (F.col("FctsClmLn.CLCL_ID") == F.col("RefON.CLCL_ID")) &
          (F.col("FctsClmLn.CDDL_SEQ_NO") == F.col("RefON.CDML_SEQ_NO")),
          "left")
    .join(df_RefPY.alias("RefPY"),
          (F.col("FctsClmLn.CLCL_ID") == F.col("RefPY.CLCL_ID")) &
          (F.col("FctsClmLn.CDDL_SEQ_NO") == F.col("RefPY.CDML_SEQ_NO")),
          "left")
    .join(df_RefPN.alias("RefPN"),
          (F.col("FctsClmLn.CLCL_ID") == F.col("RefPN.CLCL_ID")) &
          (F.col("FctsClmLn.CDDL_SEQ_NO") == F.col("RefPN.CDML_SEQ_NO")),
          "left")
    .join(df_RefPvdCntrct.alias("RefPvdCntrct"),
          (F.col("FctsClmLn.CLCL_ID") == F.col("RefPvdCntrct.CLCL_ID")) &
          (F.col("FctsClmLn.CDDL_SEQ_NO") == F.col("RefPvdCntrct.CDML_SEQ_NO")),
          "left")
    .join(df_RefClhp.alias("RefClhp"),
          F.col("FctsClmLn.CLCL_ID") == F.col("RefClhp.CLCL_ID"),
          "left")
    .join(df_clor_lkup.alias("clor_lkup"),
          F.col("FctsClmLn.CLCL_ID") == F.col("clor_lkup.CLM_ID"),
          "left")
)

# Now replicate the logic of tBusinessRules (CTransformerStage).
df_enriched = df_tBusinessRules_input

# Stage Variables in tBusinessRules
# We replicate them as columns:

df_enriched = df_enriched.withColumn(
    "svNtwkO",
    F.when(F.trim(F.col("FctsClmLn.CDDL_CL_NTWK_IND")) == F.lit("O"), F.lit("Y"))
     .otherwise(F.lit("N"))
)

df_enriched = df_enriched.withColumn(
    "svNtwkIP",
    F.when(F.trim(F.col("FctsClmLn.CDDL_CL_NTWK_IND")) == F.lit("I"), F.lit("Y"))
     .when(F.trim(F.col("FctsClmLn.CDDL_CL_NTWK_IND")) == F.lit("P"), F.lit("Y"))
     .otherwise(F.lit("N"))
)

df_enriched = df_enriched.withColumn(
    "svN",
    F.when(F.col("RefNY.CLCL_ID").isNotNull(), F.lit("Y"))
     .otherwise(
        F.when(
          (F.col("RefMY.CLCL_ID").isNotNull()) | (F.col("RefOY.CLCL_ID").isNotNull()) | (F.col("RefPY.CLCL_ID").isNotNull()),
          F.lit("O")
        ).otherwise(
          F.when(F.col("RefNN.CLCL_ID").isNotNull(), F.lit("N"))
           .otherwise(F.lit("X"))
        )
     )
)

df_enriched = df_enriched.withColumn(
    "svM",
    F.when(F.col("RefMY.CLCL_ID").isNotNull(), F.lit("Y"))
     .otherwise(
        F.when(
          (F.col("RefNY.CLCL_ID").isNotNull()) | (F.col("RefOY.CLCL_ID").isNotNull()) | (F.col("RefPY.CLCL_ID").isNotNull()),
          F.lit("O")
        ).otherwise(
          F.when(F.col("RefMN.CLCL_ID").isNotNull(), F.lit("N"))
           .otherwise(F.lit("X"))
        )
     )
)

df_enriched = df_enriched.withColumn(
    "svO",
    F.when(F.col("RefOY.CLCL_ID").isNotNull(), F.lit("Y"))
     .otherwise(
        F.when(
          (F.col("RefNY.CLCL_ID").isNotNull()) | (F.col("RefMY.CLCL_ID").isNotNull()) | (F.col("RefPY.CLCL_ID").isNotNull()),
          F.lit("O")
        ).otherwise(
          F.when(F.col("RefON.CLCL_ID").isNotNull(), F.lit("N"))
           .otherwise(F.lit("X"))
        )
     )
)

df_enriched = df_enriched.withColumn(
    "svP",
    F.when(F.col("RefPY.CLCL_ID").isNotNull(), F.lit("Y"))
     .otherwise(
        F.when(
          (F.col("RefNY.CLCL_ID").isNotNull()) | (F.col("RefOY.CLCL_ID").isNotNull()) | (F.col("RefMY.CLCL_ID").isNotNull()),
          F.lit("O")
        ).otherwise(
          F.when(F.col("RefPN.CLCL_ID").isNotNull(), F.lit("N"))
           .otherwise(F.lit("X"))
        )
     )
)

df_enriched = df_enriched.withColumn(
    "svMaster",
    F.when(F.col("svN") == "Y", F.lit("N"))
     .when(F.col("svP") == "Y", F.lit("P"))
     .when(F.col("svM") == "Y", F.lit("M"))
     .when(F.col("svO") == "Y", F.lit("O"))
     .otherwise(F.lit("X"))
)

df_enriched = df_enriched.withColumn(
    "svTotalofAll",
    F.col("FctsClmLn.CDDL_DISALL_AMT")
)

df_enriched = df_enriched.withColumn(
    "svDefaultToProvWriteOff",
    F.when(F.col("clor_lkup.CLM_ID").isNull(), F.lit("N"))
     .otherwise(
       F.when(F.col("FctsClmLn.EXCD_FOUND") == "Y", F.lit("N")).otherwise(F.lit("Y"))
     )
)

df_enriched = df_enriched.withColumn(
    "svNoRespAmt",
    F.when(F.col("svDefaultToProvWriteOff") == "Y", F.lit(0.00))
     .otherwise(
       F.when(F.col("svMaster") == "N", F.col("svTotalofAll"))
        .otherwise(
          F.when(F.col("svMaster") == "X",
                 F.when(F.col("RefNN.DSALW_AMT").isNull() | (F.length(F.trim(F.col("RefNN.DSALW_AMT"))) == 0) |
                        ~F.col("RefNN.DSALW_AMT").cast("string").rlike("^[0-9.+-]*$"),
                        F.lit(0.0))
                  .otherwise(F.col("RefNN.DSALW_AMT")))
           .otherwise(F.lit(0.00))
        )
     )
)

df_enriched = df_enriched.withColumn(
    "svPatRespAmt",
    F.when(F.col("svDefaultToProvWriteOff") == "Y", F.lit(0.00))
     .otherwise(
       F.when((F.col("svMaster") == "M") | ((F.col("svMaster") == "O") & (F.col("svNtwkO") == "Y")), F.col("svTotalofAll"))
        .otherwise(
          (
            F.when(F.col("svMaster") == "X",
                   F.when(F.col("RefMN.DSALW_AMT").isNull() | (F.length(F.trim(F.col("RefMN.DSALW_AMT"))) == 0) |
                          ~F.col("RefMN.DSALW_AMT").cast("string").rlike("^[0-9.+-]*$"),
                          F.lit(0.0))
                    .otherwise(F.col("RefMN.DSALW_AMT")))
          ) + (
            F.when((F.col("svNtwkO") == "Y") & (F.col("svMaster") == "X"),
                   F.when(F.col("RefON.DSALW_AMT").isNull() | (F.length(F.trim(F.col("RefON.DSALW_AMT"))) == 0) |
                          ~F.col("RefON.DSALW_AMT").cast("string").rlike("^[0-9.+-]*$"),
                          F.lit(0.0))
                    .otherwise(F.col("RefON.DSALW_AMT")))
            .otherwise(F.lit(0.00))
          )
        )
     )
)

df_enriched = df_enriched.withColumn(
    "svProvWriteOff",
    F.when(F.col("svDefaultToProvWriteOff") == "Y", F.col("svTotalofAll"))
     .otherwise(
       F.when((F.col("svMaster") == "P") | ((F.col("svMaster") == "O") & (F.col("svNtwkIP") == "Y")), F.col("svTotalofAll"))
        .otherwise(
          (
            F.when(F.col("svMaster") == "X",
                   F.when(F.col("RefPN.DSALW_AMT").isNull() | (F.length(F.trim(F.col("RefPN.DSALW_AMT"))) == 0) |
                          ~F.col("RefPN.DSALW_AMT").cast("string").rlike("^[0-9.+-]*$"),
                          F.lit(0.0))
                    .otherwise(F.col("RefPN.DSALW_AMT")))
          ) + (
            F.when((F.col("svNtwkIP") == "Y") & (F.col("svMaster") == "X"),
                   F.when(F.col("RefON.DSALW_AMT").isNull() | (F.length(F.trim(F.col("RefON.DSALW_AMT"))) == 0) |
                          ~F.col("RefON.DSALW_AMT").cast("string").rlike("^[0-9.+-]*$"),
                          F.lit(0.0))
                    .otherwise(F.col("RefON.DSALW_AMT")))
            .otherwise(F.lit(0.00))
          )
        )
     )
)

df_enriched = df_enriched.withColumn(
    "svSvcPrice",
    F.when(
       F.col("FctsClmLn.DPPC_PRICE_ID").isNull() | (F.length(F.trim(F.col("FctsClmLn.DPPC_PRICE_ID"))) == 0),
       F.lit("")
    ).otherwise(F.trim(F.col("FctsClmLn.DPPC_PRICE_ID")))
)

# Now produce the output link: "ClmLnDntRecs"
# In DataStage, the transformer output columns are enumerated with expressions.

df_ClmLnDntRecs = df_enriched.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("LUMERIS").alias("SRC_SYS_CD"),
    F.concat(F.lit("LUMERIS"), F.lit(";"), 
             F.when(F.col("FctsClmLn.CLCL_ID").isNull() | (F.length(F.trim(F.col("FctsClmLn.CLCL_ID"))) == 0), F.lit("UNK"))
              .otherwise(F.trim(F.col("FctsClmLn.CLCL_ID"))),
             F.lit(";"),
             F.when(F.col("FctsClmLn.CDDL_SEQ_NO").isNull(), F.lit("UNK"))
              .otherwise(F.col("FctsClmLn.CDDL_SEQ_NO").cast("string")))
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_SK"),
    F.when(F.col("FctsClmLn.CLCL_ID").isNull() | (F.length(F.trim(F.col("FctsClmLn.CLCL_ID"))) == 0), F.lit("UNK"))
     .otherwise(F.trim(F.col("FctsClmLn.CLCL_ID"))).alias("CLM_ID"),
    F.when(F.col("FctsClmLn.CDDL_SEQ_NO").isNull() | (F.length(F.col("FctsClmLn.CDDL_SEQ_NO").cast("string")) == 0), F.lit("UNK"))
     .otherwise(F.col("FctsClmLn.CDDL_SEQ_NO").cast("string")).alias("CLM_LN_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("FctsClmLn.DPDP_ID").isNull() | (F.length(F.trim(F.col("FctsClmLn.DPDP_ID"))) == 0), F.lit("NA"))
     .otherwise(F.upper(F.trim(F.col("FctsClmLn.DPDP_ID")))).alias("PROC_CD"),
    F.when(F.col("FctsClmLn.PRPR_ID").isNull() | (F.length(F.trim(F.col("FctsClmLn.PRPR_ID"))) == 0), F.lit("NA"))
     .otherwise(F.upper(F.trim(F.col("FctsClmLn.PRPR_ID")))).alias("SVC_PROV_ID"),
    F.when(F.col("FctsClmLn.CDDL_DISALL_EXCD").isNull() | (F.length(F.trim(F.col("FctsClmLn.CDDL_DISALL_EXCD"))) == 0), F.lit("NA"))
     .otherwise(F.trim(F.col("FctsClmLn.CDDL_DISALL_EXCD"))).alias("CLM_LN_DSALW_EXCD"),
    F.when(F.col("FctsClmLn.CDDL_EOB_EXCD").isNull() | (F.length(F.trim(F.col("FctsClmLn.CDDL_EOB_EXCD"))) == 0), F.lit("NA"))
     .otherwise(F.trim(F.col("FctsClmLn.CDDL_EOB_EXCD"))).alias("CLM_LN_EOB_EXCD"),
    F.when(F.col("FctsClmLn.CLM_LN_FINL_DISP_CD").isNull() | (F.length(F.trim(F.col("FctsClmLn.CLM_LN_FINL_DISP_CD"))) == 0), F.lit("NA"))
     .otherwise(F.upper(F.trim(F.col("FctsClmLn.CLM_LN_FINL_DISP_CD")))).alias("CLM_LN_FINL_DISP_CD"),
    F.when(F.col("FctsClmLn.LOBD_ID").isNull() | (F.length(F.trim(F.col("FctsClmLn.LOBD_ID"))) == 0), F.lit("NA"))
     .otherwise(F.upper(F.trim(F.col("FctsClmLn.LOBD_ID")))).alias("CLM_LN_LOB_CD"),
    F.when(F.col("FctsClmLn.PSCD_ID").isNull() | (F.length(F.trim(F.col("FctsClmLn.PSCD_ID"))) == 0), F.lit("NA"))
     .otherwise(F.upper(F.trim(F.col("FctsClmLn.PSCD_ID")))).alias("CLM_LN_POS_CD"),
    F.lit("NA").alias("CLM_LN_PREAUTH_CD"),
    F.lit("NA").alias("CLM_LN_PREAUTH_SRC_CD"),
    F.when(F.col("FctsClmLn.CDDL_PRICE_IND").isNull() | (F.length(F.trim(F.col("FctsClmLn.CDDL_PRICE_IND"))) == 0), F.lit("NA"))
     .otherwise(F.upper(F.trim(F.col("FctsClmLn.CDDL_PRICE_IND")))).alias("CLM_LN_PRICE_SRC_CD"),
    F.when(F.col("FctsClmLn.CDDL_REF_IND").isNull() | (F.length(F.trim(F.col("FctsClmLn.CDDL_REF_IND"))) == 0), F.lit("NA"))
     .otherwise(F.upper(F.trim(F.col("FctsClmLn.CDDL_REF_IND")))).alias("CLM_LN_RFRL_CD"),
    F.lit("NA").alias("CLM_LN_RVNU_CD"),
    F.lit("NA").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.lit("NA").alias("CLM_LN_ROOM_TYP_CD"),
    F.lit("NA").alias("CLM_LN_TOS_CD"),
    F.lit("UN").alias("CLM_LN_UNIT_TYP_CD"),  # 'UN' with quotes in DataStage
    F.when(F.trim(F.col("FctsClmLn.CDDL_CAP_IND")) == F.lit("Y"), F.lit("Y")).otherwise(F.lit("N")).alias("CAP_LN_IN"),
    F.when(F.trim(F.col("FctsClmLn.PDVC_LOBD_PTR")) == F.lit("1"), F.lit("Y")).otherwise(F.lit("N")).alias("PRI_LOB_IN"),
    F.when(F.col("FctsClmLn.CDDL_FROM_DT").isNull(), F.lit("UNK"))
     .otherwise(F.substring(F.trim(F.col("FctsClmLn.CDDL_FROM_DT").cast("string")),1,10)).alias("SVC_END_DT"),
    F.when(F.col("FctsClmLn.CDDL_FROM_DT").isNull(), F.lit("UNK"))
     .otherwise(F.substring(F.trim(F.col("FctsClmLn.CDDL_FROM_DT").cast("string")),1,10)).alias("SVC_STRT_DT"),
    F.when(
      (F.col("FctsClmLn.CDDL_AG_PRICE").isNull()) | (F.length(F.trim(F.col("FctsClmLn.CDDL_AG_PRICE"))) == 0) |
      ~F.col("FctsClmLn.CDDL_AG_PRICE").cast("string").rlike("^[0-9.+-]*$")
      , F.lit(0)
    ).otherwise(F.col("FctsClmLn.CDDL_AG_PRICE")).alias("AGMNT_PRICE_AMT"),
    F.when(
      (F.col("FctsClmLn.CDDL_ALLOW").isNull()) | (F.length(F.trim(F.col("FctsClmLn.CDDL_ALLOW"))) == 0) |
      ~F.col("FctsClmLn.CDDL_ALLOW").cast("string").rlike("^[0-9.+-]*$")
      , F.lit(0)
    ).otherwise(F.col("FctsClmLn.CDDL_ALLOW")).alias("ALW_AMT"),
    F.when(
      (F.col("FctsClmLn.CDDL_CHG_AMT").isNull()) | (F.length(F.trim(F.col("FctsClmLn.CDDL_CHG_AMT"))) == 0) |
      ~F.col("FctsClmLn.CDDL_CHG_AMT").cast("string").rlike("^[0-9.+-]*$")
      , F.lit(0)
    ).otherwise(F.col("FctsClmLn.CDDL_CHG_AMT")).alias("CHRG_AMT"),
    F.when(
      (F.col("FctsClmLn.CDDL_COINS_AMT").isNull()) | (F.length(F.trim(F.col("FctsClmLn.CDDL_COINS_AMT"))) == 0) |
      ~F.col("FctsClmLn.CDDL_COINS_AMT").cast("string").rlike("^[0-9.+-]*$")
      , F.lit(0)
    ).otherwise(F.col("FctsClmLn.CDDL_COINS_AMT")).alias("COINS_AMT"),
    F.when(
      (F.col("FctsClmLn.CDDL_CONSIDER_CHG").isNull()) | (F.length(F.trim(F.col("FctsClmLn.CDDL_CONSIDER_CHG"))) == 0) |
      ~F.col("FctsClmLn.CDDL_CONSIDER_CHG").cast("string").rlike("^[0-9.+-]*$")
      , F.lit(0)
    ).otherwise(F.col("FctsClmLn.CDDL_CONSIDER_CHG")).alias("CNSD_CHRG_AMT"),
    F.when(
      (F.col("FctsClmLn.CDDL_COPAY_AMT").isNull()) | (F.length(F.trim(F.col("FctsClmLn.CDDL_COPAY_AMT"))) == 0) |
      ~F.col("FctsClmLn.CDDL_COPAY_AMT").cast("string").rlike("^[0-9.+-]*$")
      , F.lit(0)
    ).otherwise(F.col("FctsClmLn.CDDL_COPAY_AMT")).alias("COPAY_AMT"),
    F.when(
      (F.col("FctsClmLn.CDDL_DED_AMT").isNull()) | (F.length(F.trim(F.col("FctsClmLn.CDDL_DED_AMT"))) == 0) |
      ~F.col("FctsClmLn.CDDL_DED_AMT").cast("string").rlike("^[0-9.+-]*$")
      , F.lit(0)
    ).otherwise(F.col("FctsClmLn.CDDL_DED_AMT")).alias("DEDCT_AMT"),
    F.col("FctsClmLn.CDDL_DISALL_AMT").alias("DSALW_AMT"),
    F.lit(0).alias("ITS_HOME_DSCNT_AMT"),
    F.col("svNoRespAmt").alias("NO_RESP_AMT"),
    F.when(
      (F.col("FctsClmLn.CDDL_OOP_CALC_BASE").isNull()) | (F.length(F.trim(F.col("FctsClmLn.CDDL_OOP_CALC_BASE"))) == 0) |
      ~F.col("FctsClmLn.CDDL_OOP_CALC_BASE").cast("string").rlike("^[0-9.+-]*$")
      , F.lit(0)
    ).otherwise(F.col("FctsClmLn.CDDL_OOP_CALC_BASE")).alias("MBR_LIAB_BSS_AMT"),
    F.col("svPatRespAmt").alias("PATN_RESP_AMT"),
    F.when(
      (F.col("FctsClmLn.CDDL_PAID_AMT").isNull()) | (F.length(F.trim(F.col("FctsClmLn.CDDL_PAID_AMT"))) == 0) |
      ~F.col("FctsClmLn.CDDL_PAID_AMT").cast("string").rlike("^[0-9.+-]*$")
      , F.lit(0)
    ).otherwise(F.col("FctsClmLn.CDDL_PAID_AMT")).alias("PAYBL_AMT"),
    F.when(
      (F.col("FctsClmLn.CDDL_PR_PYMT_AMT").isNull()) | (F.length(F.trim(F.col("FctsClmLn.CDDL_PR_PYMT_AMT"))) == 0) |
      ~F.col("FctsClmLn.CDDL_PR_PYMT_AMT").cast("string").rlike("^[0-9.+-]*$")
      , F.lit(0)
    ).otherwise(F.col("FctsClmLn.CDDL_PR_PYMT_AMT")).alias("PAYBL_TO_PROV_AMT"),
    F.when(
      (F.col("FctsClmLn.CDDL_SB_PYMT_AMT").isNull()) | (F.length(F.trim(F.col("FctsClmLn.CDDL_SB_PYMT_AMT"))) == 0) |
      ~F.col("FctsClmLn.CDDL_SB_PYMT_AMT").cast("string").rlike("^[0-9.+-]*$")
      , F.lit(0)
    ).otherwise(F.col("FctsClmLn.CDDL_SB_PYMT_AMT")).alias("PAYBL_TO_SUB_AMT"),
    F.when(
      (F.col("FctsClmLn.CDDL_IP_PRICE").isNull()) | (F.length(F.trim(F.col("FctsClmLn.CDDL_IP_PRICE"))) == 0) |
      ~F.col("FctsClmLn.CDDL_IP_PRICE").cast("string").rlike("^[0-9.+-]*$")
      , F.lit(0)
    ).otherwise(F.col("FctsClmLn.CDDL_IP_PRICE")).alias("PROC_TBL_PRICE_AMT"),
    F.when(
      (F.col("FctsClmLn.CDDL_PF_PRICE").isNull()) | (F.length(F.trim(F.col("FctsClmLn.CDDL_PF_PRICE"))) == 0) |
      ~F.col("FctsClmLn.CDDL_PF_PRICE").cast("string").rlike("^[0-9.+-]*$")
      , F.lit(0)
    ).otherwise(F.col("FctsClmLn.CDDL_PF_PRICE")).alias("PROFL_PRICE_AMT"),
    F.col("svProvWriteOff").alias("PROV_WRT_OFF_AMT"),
    F.when(
      (F.col("FctsClmLn.CDDL_RISK_WH_AMT").isNull()) | (F.length(F.trim(F.col("FctsClmLn.CDDL_RISK_WH_AMT"))) == 0) |
      ~F.col("FctsClmLn.CDDL_RISK_WH_AMT").cast("string").rlike("^[0-9.+-]*$")
      , F.lit(0)
    ).otherwise(F.col("FctsClmLn.CDDL_RISK_WH_AMT")).alias("RISK_WTHLD_AMT"),
    F.when(
      (F.col("FctsClmLn.CDDL_DP_PRICE").isNull()) | (F.length(F.trim(F.col("FctsClmLn.CDDL_DP_PRICE"))) == 0) |
      ~F.col("FctsClmLn.CDDL_DP_PRICE").cast("string").rlike("^[0-9.+-]*$")
      , F.lit(0)
    ).otherwise(F.col("FctsClmLn.CDDL_DP_PRICE")).alias("SVC_PRICE_AMT"),
    F.lit(0).alias("SUPLMT_DSCNT_AMT"),
    F.when(
      (F.col("FctsClmLn.CDDL_UNITS_ALLOW").isNull()) | (F.length(F.trim(F.col("FctsClmLn.CDDL_UNITS_ALLOW").cast("string"))) == 0) |
      ~F.col("FctsClmLn.CDDL_UNITS_ALLOW").cast("string").rlike("^[0-9.+-]*$")
      , F.lit(0)
    ).otherwise(F.col("FctsClmLn.CDDL_UNITS_ALLOW")).alias("ALW_PRICE_UNIT_CT"),
    F.when(
      (F.col("FctsClmLn.CDDL_UNITS").isNull()) | (F.length(F.trim(F.col("FctsClmLn.CDDL_UNITS").cast("string"))) == 0) |
      ~F.col("FctsClmLn.CDDL_UNITS").cast("string").rlike("^[0-9.+-]*$")
      , F.lit(0)
    ).otherwise(F.col("FctsClmLn.CDDL_UNITS")).alias("UNIT_CT"),
    F.when(F.col("FctsClmLn.CDDL_DED_AC_NO").isNull() | (F.length(F.trim(F.col("FctsClmLn.CDDL_DED_AC_NO").cast("string"))) == 0),
           F.lit(" ")).otherwise(F.trim(F.col("FctsClmLn.CDDL_DED_AC_NO").cast("string"))).alias("DEDCT_AMT_ACCUM_ID"),
    F.lit("NA").alias("PREAUTH_SVC_SEQ_NO"),
    F.when(F.col("FctsClmLn.CDDL_REFSV_SEQ_NO").isNull() | (F.length(F.trim(F.col("FctsClmLn.CDDL_REFSV_SEQ_NO").cast("string"))) == 0),
           F.lit(" ")).otherwise(F.trim(F.col("FctsClmLn.CDDL_REFSV_SEQ_NO").cast("string"))).alias("RFRL_SVC_SEQ_NO"),
    F.when(F.length(
      F.when(F.col("FctsClmLn.LTLT_PFX").isNull() | (F.length(F.trim(F.col("FctsClmLn.LTLT_PFX"))) == 0),
             F.lit(""))
    ) == 0, F.lit("NA"))
     .otherwise(
       F.when(F.col("FctsClmLn.LTLT_PFX").isNull() | (F.length(F.trim(F.col("FctsClmLn.LTLT_PFX"))) == 0), F.lit(""))
        .otherwise(F.trim(F.col("FctsClmLn.LTLT_PFX")))
     ).alias("LMT_PFX_ID"),
    F.lit("NA").alias("PREAUTH_ID"),
    F.when(F.length(
      F.when(F.col("FctsClmLn.DEDE_PFX").isNull() | (F.length(F.trim(F.col("FctsClmLn.DEDE_PFX"))) == 0), F.lit(""))
    ) == 0, F.lit("NA"))
     .otherwise(
       F.when(F.col("FctsClmLn.DEDE_PFX").isNull() | (F.length(F.trim(F.col("FctsClmLn.DEDE_PFX"))) == 0), F.lit(""))
        .otherwise(F.trim(F.col("FctsClmLn.DEDE_PFX")))
     ).alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.when(F.length(
      F.when(F.col("FctsClmLn.DPPY_PFX").isNull() | (F.length(F.trim(F.col("FctsClmLn.DPPY_PFX"))) == 0), F.lit(""))
    ) == 0, F.lit("NA"))
     .otherwise(
       F.when(F.col("FctsClmLn.DPPY_PFX").isNull() | (F.length(F.trim(F.col("FctsClmLn.DPPY_PFX"))) == 0), F.lit(""))
        .otherwise(F.trim(F.col("FctsClmLn.DPPY_PFX")))
     ).alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.when(F.length(
      F.when(F.col("FctsClmLn.CDDL_UMREF_ID").isNull() | (F.length(F.trim(F.col("FctsClmLn.CDDL_UMREF_ID"))) == 0), F.lit(""))
    ) == 0, F.lit("NA"))
     .otherwise(
       F.when(F.col("FctsClmLn.CDDL_UMREF_ID").isNull() | (F.length(F.trim(F.col("FctsClmLn.CDDL_UMREF_ID"))) == 0), F.lit(""))
        .otherwise(F.trim(F.col("FctsClmLn.CDDL_UMREF_ID")))
     ).alias("RFRL_ID_TX"),
    F.lit("NA").alias("SVC_ID"),
    F.when(F.length(F.col("svSvcPrice")) == 0, F.lit("NA"))
     .otherwise(
       F.when(F.col("svSvcPrice") == "CPC1", F.lit("DENTCPC1"))
        .when(F.col("svSvcPrice") == "TRD4", F.lit("DENTTRD4"))
        .otherwise(F.col("svSvcPrice"))
     ).alias("SVC_PRICE_RULE_ID"),
    F.when(F.col("FctsClmLn.CGCG_RULE").isNull() | (F.length(F.trim(F.col("FctsClmLn.CGCG_RULE"))) == 0), F.lit(None))
     .otherwise(F.trim(F.col("FctsClmLn.CGCG_RULE"))).alias("SVC_RULE_TYP_TX"),
    F.when(F.length(F.trim(F.col("FctsClmLn.PSCD_ID"))) == 0, F.lit("OV"))
     .otherwise(F.col("FctsClmLn.PSCD_ID")).alias("SVC_LOC_TYP_CD"),
    F.when(F.col("RefPvdCntrct.CDML_SEQ_NO").isNull(), F.lit(0.00))
     .otherwise(
       F.when(F.col("FctsClmLn.CLCL_NTWK_IND") == "O",
              F.col("FctsClmLn.CDDL_CONSIDER_CHG") - F.col("FctsClmLn.CDDL_ALLOW"))
       .otherwise(F.lit(0.00))
     ).alias("NON_PAR_SAV_AMT"),
    F.lit("NA").alias("SNOMED_CT_CD"),
    F.lit("NA").alias("CVX_VCCN_CD")
)

# Next stage: Build_reversals (CTransformerStage).
# Two lookups: fcts_reversals (df_hf_clm_fcts_reversals) and nasco_dup_lkup (df_clm_nasco_dup_bypass).
df_build_reversals_input = (
    df_ClmLnDntRecs.alias("ClmLnDntRecs")
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        F.col("ClmLnDntRecs.CLM_ID") == F.col("fcts_reversals.CLCL_ID"),
        "left"
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        F.col("ClmLnDntRecs.CLM_ID") == F.col("nasco_dup_lkup.CLM_ID"),
        "left"
    )
)

# "ClmLnDentalRecs" link => IsNull(nasco_dup_lkup.CLM_ID) == TRUE
df_ClmLnDentalRecs = df_build_reversals_input.filter(F.col("nasco_dup_lkup.CLM_ID").isNull())

# "reversals" link => IsNull(fcts_reversals.CLCL_ID) == FALSE AND fcts_reversals.CLCL_CUR_STS in ('89','91','99')
df_reversals = df_build_reversals_input.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
    ((F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
     (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
     (F.col("fcts_reversals.CLCL_CUR_STS") == "99"))
)

# Collector stage => union of ClmLnDentalRecs and reversals
df_Collector = df_ClmLnDentalRecs.unionByName(df_reversals)

# Snapshot stage => produces two output links: "Pkey" and "Snapshot"
# The output columns (in the "Transform" link) are enumerated with their metadata.
df_Snapshot = df_Collector.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_LN_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROC_CD",
    "SVC_PROV_ID",
    "CLM_LN_DSALW_EXCD",
    "CLM_LN_EOB_EXCD",
    "CLM_LN_FINL_DISP_CD",
    "CLM_LN_LOB_CD",
    "CLM_LN_POS_CD",
    "CLM_LN_PREAUTH_CD",
    "CLM_LN_PREAUTH_SRC_CD",
    "CLM_LN_PRICE_SRC_CD",
    "CLM_LN_RFRL_CD",
    "CLM_LN_RVNU_CD",
    "CLM_LN_ROOM_PRICE_METH_CD",
    "CLM_LN_ROOM_TYP_CD",
    "CLM_LN_TOS_CD",
    "CLM_LN_UNIT_TYP_CD",
    "CAP_LN_IN",
    "PRI_LOB_IN",
    "SVC_END_DT",
    "SVC_STRT_DT",
    "AGMNT_PRICE_AMT",
    "ALW_AMT",
    "CHRG_AMT",
    "COINS_AMT",
    "CNSD_CHRG_AMT",
    "COPAY_AMT",
    "DEDCT_AMT",
    "DSALW_AMT",
    "ITS_HOME_DSCNT_AMT",
    "NO_RESP_AMT",
    "MBR_LIAB_BSS_AMT",
    "PATN_RESP_AMT",
    "PAYBL_AMT",
    "PAYBL_TO_PROV_AMT",
    "PAYBL_TO_SUB_AMT",
    "PROC_TBL_PRICE_AMT",
    "PROFL_PRICE_AMT",
    "PROV_WRT_OFF_AMT",
    "RISK_WTHLD_AMT",
    "SVC_PRICE_AMT",
    "SUPLMT_DSCNT_AMT",
    "ALW_PRICE_UNIT_CT",
    "UNIT_CT",
    "DEDCT_AMT_ACCUM_ID",
    "PREAUTH_SVC_SEQ_NO",
    "RFRL_SVC_SEQ_NO",
    "LMT_PFX_ID",
    "PREAUTH_ID",
    "PROD_CMPNT_DEDCT_PFX_ID",
    "PROD_CMPNT_SVC_PAYMT_ID",
    "RFRL_ID_TX",
    "SVC_ID",
    "SVC_PRICE_RULE_ID",
    "SVC_RULE_TYP_TX",
    "SVC_LOC_TYP_CD",
    "NON_PAR_SAV_AMT",
    F.lit("DNTL").alias("PROC_CD_TYP_CD"),
    F.lit("DNTL").alias("PROC_CD_CAT_CD"),
    # The snapshot layout calls it "SNOMED_CD_CT" in JSON, but the stage named it "SNOMED_CD_CT" vs "SNOMED_CT_CD" mismatch?
    # The JSON final column is "SNOMED_CD_CT", so we rename here.
    F.col("SNOMED_CT_CD").alias("SNOMED_CD_CT"),
    "CVX_VCCN_CD"
)

df_Pkey = df_Snapshot.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_LN_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROC_CD"),
    F.col("SVC_PROV_ID"),
    F.col("CLM_LN_DSALW_EXCD"),
    F.col("CLM_LN_EOB_EXCD"),
    F.col("CLM_LN_FINL_DISP_CD"),
    F.col("CLM_LN_LOB_CD"),
    F.col("CLM_LN_POS_CD"),
    F.col("CLM_LN_PREAUTH_CD"),
    F.col("CLM_LN_PREAUTH_SRC_CD"),
    F.col("CLM_LN_PRICE_SRC_CD"),
    F.col("CLM_LN_RFRL_CD"),
    F.col("CLM_LN_RVNU_CD"),
    F.col("CLM_LN_ROOM_PRICE_METH_CD"),
    F.col("CLM_LN_ROOM_TYP_CD"),
    F.col("CLM_LN_TOS_CD"),
    F.col("CLM_LN_UNIT_TYP_CD"),
    F.col("CAP_LN_IN"),
    F.col("PRI_LOB_IN"),
    F.col("SVC_END_DT"),
    F.col("SVC_STRT_DT"),
    F.col("AGMNT_PRICE_AMT"),
    F.col("ALW_AMT"),
    F.col("CHRG_AMT"),
    F.col("COINS_AMT"),
    F.col("CNSD_CHRG_AMT"),
    F.col("COPAY_AMT"),
    F.col("DEDCT_AMT"),
    F.col("DSALW_AMT"),
    F.col("ITS_HOME_DSCNT_AMT"),
    F.col("NO_RESP_AMT"),
    F.col("MBR_LIAB_BSS_AMT"),
    F.col("PATN_RESP_AMT"),
    F.col("PAYBL_AMT"),
    F.col("PAYBL_TO_PROV_AMT"),
    F.col("PAYBL_TO_SUB_AMT"),
    F.col("PROC_TBL_PRICE_AMT"),
    F.col("PROFL_PRICE_AMT"),
    F.col("PROV_WRT_OFF_AMT"),
    F.col("RISK_WTHLD_AMT"),
    F.col("SVC_PRICE_AMT"),
    F.col("SUPLMT_DSCNT_AMT"),
    F.col("ALW_PRICE_UNIT_CT"),
    F.col("UNIT_CT"),
    F.col("DEDCT_AMT_ACCUM_ID"),
    F.col("PREAUTH_SVC_SEQ_NO"),
    F.col("RFRL_SVC_SEQ_NO"),
    F.col("LMT_PFX_ID"),
    F.col("PREAUTH_ID"),
    F.col("PROD_CMPNT_DEDCT_PFX_ID"),
    F.col("PROD_CMPNT_SVC_PAYMT_ID"),
    F.col("RFRL_ID_TX"),
    F.col("SVC_ID"),
    F.col("SVC_PRICE_RULE_ID"),
    F.col("SVC_RULE_TYP_TX"),
    F.col("SVC_LOC_TYP_CD"),
    F.col("NON_PAR_SAV_AMT"),
    F.col("PROC_CD_TYP_CD"),
    F.col("PROC_CD_CAT_CD"),
    F.lit("NA").alias("VBB_RULE_ID"),
    F.lit("NA").alias("VBB_EXCD_ID"),
    F.lit("N").alias("CLM_LN_VBB_IN"),
    F.lit(0).alias("ITS_SUPLMT_DSCNT_AMT"),
    F.lit(0).alias("ITS_SRCHRG_AMT"),
    F.lit("NA").alias("NDC"),
    F.lit("NA").alias("NDC_DRUG_FORM_CD"),
    F.lit(None).alias("NDC_UNIT_CT"),
    F.lit("MED").alias("MED_PDX_IND"),
    F.lit(" ").alias("APC_ID"),
    F.lit(" ").alias("APC_STTUS_ID"),
    F.col("SNOMED_CD_CT").alias("SNOMED_CT_CD"),
    F.col("CVX_VCCN_CD")
)

df_Snapshot2 = df_Snapshot.select(
    F.col("CLM_ID").alias("CLCL_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("PROC_CD"),
    F.col("CLM_LN_RVNU_CD"),
    F.col("ALW_AMT"),
    F.col("CHRG_AMT"),
    F.col("PAYBL_AMT"),
    F.col("PROC_CD_TYP_CD"),
    F.col("PROC_CD_CAT_CD")
)

# Next stage: Transformer => input is df_Snapshot2
# Stage variables: "ProcCdSk"=GetFkeyProcCd(…), "ClmLnRvnuCdSk"=GetFkeyRvnu(…)
# We assume user-defined function calls are direct:

df_Transformer_in = df_Snapshot2.alias("Snapshot")

df_Transformer_out = df_Transformer_in.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("Snapshot.CLCL_ID").alias("CLM_ID"),
    F.col("Snapshot.CLM_LN_SEQ_NO"),
    F.expr("GetFkeyProcCd('FACETS', 0, Snapshot.PROC_CD[1,5], Snapshot.PROC_CD_TYP_CD, Snapshot.PROC_CD_CAT_CD, 'N')").alias("PROC_CD_SK"),
    F.expr("GetFkeyRvnu('FACETS', 0, Snapshot.CLM_LN_RVNU_CD, 'N')").alias("CLM_LN_RVNU_CD_SK"),
    F.col("Snapshot.ALW_AMT").alias("ALW_AMT"),
    F.col("Snapshot.CHRG_AMT").alias("CHRG_AMT"),
    F.col("Snapshot.PAYBL_AMT").alias("PAYBL_AMT")
)

# Next stage: B_CLM_LN (CSeqFileStage) => write df_Transformer_out
# We must apply column order and rpad for any char or varchar columns as final. 
# Checking those columns: "SRC_SYS_CD_SK" (no length?), "CLM_ID" is not declared char in the B_CLM_LN stage schema snippet.
# The JSON shows no special char length for these 8 columns. We'll just select in order.

df_B_CLM_LN_final = df_Transformer_out.select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "PROC_CD_SK",
    "CLM_LN_RVNU_CD_SK",
    "ALW_AMT",
    "CHRG_AMT",
    "PAYBL_AMT"
)

# Write to the .dat file
write_files(
    df_B_CLM_LN_final,
    f"{adls_path}/load/B_CLM_LN.DENTAL.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Next stage: ClmLnPK (CContainerStage)
clmLnPk_params = {
    "CurrRunCycle": CurrRunCycle
}
df_ClmLnPK_out = ClmLnPK(df_Pkey, clmLnPk_params)

# Finally, stage "ClmLnDntlExtr" (CSeqFileStage) => write
# The columns are in the same order as "ClmLnPK" output pins. We'll do a final select with rpad where needed.
# Checking the JSON for the final file columns -> the same columns as in the container output, in the same order.

df_ClmLnDntlExtr_final = df_ClmLnPK_out.select(
    # Apply rpad for char/varchar columns with known lengths
    F.rpad(F.col("JOB_EXCTN_RCRD_ERR_SK").cast("string"), 1, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_LN_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROC_CD",
    "SVC_PROV_ID",
    "CLM_LN_DSALW_EXCD",
    "CLM_LN_EOB_EXCD",
    "CLM_LN_FINL_DISP_CD",
    "CLM_LN_LOB_CD",
    "CLM_LN_POS_CD",
    "CLM_LN_PREAUTH_CD",
    "CLM_LN_PREAUTH_SRC_CD",
    "CLM_LN_PRICE_SRC_CD",
    "CLM_LN_RFRL_CD",
    "CLM_LN_RVNU_CD",
    F.rpad(F.col("CLM_LN_ROOM_PRICE_METH_CD"), 2, " ").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    "CLM_LN_ROOM_TYP_CD",
    "CLM_LN_TOS_CD",
    "CLM_LN_UNIT_TYP_CD",
    F.rpad(F.col("CAP_LN_IN"), 1, " ").alias("CAP_LN_IN"),
    F.rpad(F.col("PRI_LOB_IN"), 1, " ").alias("PRI_LOB_IN"),
    "SVC_END_DT",
    "SVC_STRT_DT",
    "AGMNT_PRICE_AMT",
    "ALW_AMT",
    "CHRG_AMT",
    "COINS_AMT",
    "CNSD_CHRG_AMT",
    "COPAY_AMT",
    "DEDCT_AMT",
    "DSALW_AMT",
    "ITS_HOME_DSCNT_AMT",
    "NO_RESP_AMT",
    "MBR_LIAB_BSS_AMT",
    "PATN_RESP_AMT",
    "PAYBL_AMT",
    "PAYBL_TO_PROV_AMT",
    "PAYBL_TO_SUB_AMT",
    "PROC_TBL_PRICE_AMT",
    "PROFL_PRICE_AMT",
    "PROV_WRT_OFF_AMT",
    "RISK_WTHLD_AMT",
    "SVC_PRICE_AMT",
    "SUPLMT_DSCNT_AMT",
    "ALW_PRICE_UNIT_CT",
    "UNIT_CT",
    F.rpad(F.col("DEDCT_AMT_ACCUM_ID"), 4, " ").alias("DEDCT_AMT_ACCUM_ID"),
    F.rpad(F.col("PREAUTH_SVC_SEQ_NO"), 4, " ").alias("PREAUTH_SVC_SEQ_NO"),
    F.rpad(F.col("RFRL_SVC_SEQ_NO"), 4, " ").alias("RFRL_SVC_SEQ_NO"),
    F.rpad(F.col("LMT_PFX_ID"), 4, " ").alias("LMT_PFX_ID"),
    F.rpad(F.col("PREAUTH_ID"), 9, " ").alias("PREAUTH_ID"),
    F.rpad(F.col("PROD_CMPNT_DEDCT_PFX_ID"), 4, " ").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.rpad(F.col("PROD_CMPNT_SVC_PAYMT_ID"), 4, " ").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.rpad(F.col("RFRL_ID_TX"), 9, " ").alias("RFRL_ID_TX"),
    F.rpad(F.col("SVC_ID"), 4, " ").alias("SVC_ID"),
    F.rpad(F.col("SVC_PRICE_RULE_ID"), 4, " ").alias("SVC_PRICE_RULE_ID"),
    F.rpad(F.col("SVC_RULE_TYP_TX"), 3, " ").alias("SVC_RULE_TYP_TX"),
    F.rpad(F.col("SVC_LOC_TYP_CD"), 20, " ").alias("SVC_LOC_TYP_CD"),
    "NON_PAR_SAV_AMT",
    "PROC_CD_TYP_CD",
    "PROC_CD_CAT_CD",
    "VBB_RULE_ID",
    "VBB_EXCD_ID",
    F.rpad(F.col("CLM_LN_VBB_IN"), 1, " ").alias("CLM_LN_VBB_IN"),
    "ITS_SUPLMT_DSCNT_AMT",
    "ITS_SRCHRG_AMT",
    "NDC",
    "NDC_DRUG_FORM_CD",
    "NDC_UNIT_CT",
    F.rpad(F.col("MED_PDX_IND"), 3, " ").alias("MED_PDX_IND"),
    "APC_ID",
    "APC_STTUS_ID",
    "SNOMED_CT_CD",
    "CVX_VCCN_CD"
)

write_files(
    df_ClmLnDntlExtr_final,
    f"{adls_path}/key/LhoFctsClmLnDntlExtr.LhoFctsClmLn.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)