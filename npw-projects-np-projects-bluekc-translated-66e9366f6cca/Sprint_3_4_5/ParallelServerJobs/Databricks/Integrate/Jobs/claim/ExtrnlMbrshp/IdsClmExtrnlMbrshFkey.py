# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClmExtrnlMbrshFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys; essentially converting the common record format file to a table load file.   
# MAGIC                             If any of the foreign key lookups fail, then it is written to the recycle hash file.
# MAGIC      
# MAGIC                            Not date specific.   Just drives off what is on the Claim header file.
# MAGIC 
# MAGIC                          No databases are used.  Just files.
# MAGIC                                   
# MAGIC   
# MAGIC INPUTS:             #FilePath#  NpsClmExtrnlMbrshPkey.NPSExtrnMbr.RUNID
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle - written to only.
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                              GetFkeyClm ()
# MAGIC                              GetFkeyDate()
# MAGIC                             GetFkeyCodes()
# MAGIC                             GetFkeyErrorCnt(  )
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC                   All records from the input file are processed; no records are filtered out.  
# MAGIC                   Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC                   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                               #FilePath#/ load / NpsClmExtrnlMbrshTrns.NPSExtrnMbrTmp.RUNID
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Brent Leland     03/22/2004     -   Originally Programmed
# MAGIC             SharonAndrew  06/22/2004    - Documented and standardized for version 1.1
# MAGIC             Brent Leland     09/09/2004    - Added default rows for UNK and NA.
# MAGIC             Brent Leland     10/11/2004    - Changed to multi-row processing
# MAGIC             Brent Leland      11/02/2004   - Corrected Error recycle common record format
# MAGIC             Steph Goddard  03/06/2006     Changes for sequencer
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer           Date                        Project/Altius #             Change Description                                                                    Development Project          Code Reviewer          Date Reviewed  
# MAGIC -----------------------    ----------------------------    ---------------------------          -------------------------------------------------------------------------------------------        ------------------------------------       ----------------------------      -------------------------
# MAGIC Ralph Tucker      2008-7-25              3657 Primary Key           Added SrcSysCdSk parameter                                                   devlIDS                              Steph Goddard           07/27/2008
# MAGIC Ralph Tucker     10/02/2008            Blue Exchange - 3223   Added two new fields (ACTL_SUB_ID, SUBMT_SUB_ID)         devlIDScur                         Steph Goddard          12/04/2008
# MAGIC 
# MAGIC Jag Yelavarthi     2012-02-23            TTR#1309                    Changed the GetFkey Logging to "X" to ignore 
# MAGIC                                                                                                the code lookup failures to trigger sending a claim ID to hit list     IntegrateCurDevl              SAndrew                    2012-03-02
# MAGIC Nikhil Sinha            02/28/2017     Data Catalyst - 30001     TREO - Add Mbr_SK to CLM_EXTRNL_MBRSH                          IntegrateDev2                  Kalyan Neelam          2017-03-07                                                                   
# MAGIC 
# MAGIC Ravi Abburi            04/11/2017     Data Catalyst - 30001     Fixed the recycle key issue when MBR_UNIQ_KEY is 1 or 0         IntegrateDev2                  Jag Yelavarthi             2017-04-11
# MAGIC                                                                                               the recycle key log file is growing in size due to these errors;
# MAGIC                                                                                             Changes done in pMbrSK stage variable in ForeignKey transformer. 
# MAGIC 
# MAGIC Reddy Sanam       10/09/2020                                           Changed derivation for stage var -RelshpCdSk to pass 'FACETS'
# MAGIC                                                                                             when the source is 'LUMERIS'
# MAGIC 
# MAGIC Sunitha Ganta         10-12-2020                                           Brought up to standards

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC This hash file is written to by all claim foriegn key jobs.
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
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DecimalType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','IDSClmExtrnlMbrshPkey.ClmExtrnlMbrshTmp.RUNID')
Source = get_widget_value('Source','')
Logging = get_widget_value('Logging','Y')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105838')

schema_ClmExtrnlMbrshCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(10, 0), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("CLM_EXTRNL_MBRSH_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("MBR_GNDR_CD", IntegerType(), nullable=False),
    StructField("MBR_RELSHP_CD", IntegerType(), nullable=False),
    StructField("MBR_BRTH_DT", TimestampType(), nullable=False),
    StructField("GRP_NM", StringType(), nullable=False),
    StructField("MBR_FIRST_NM", StringType(), nullable=False),
    StructField("MBR_MIDINIT", StringType(), nullable=False),
    StructField("MBR_LAST_NM", StringType(), nullable=False),
    StructField("PCKG_CD_ID", StringType(), nullable=False),
    StructField("PATN_NTWK_SH_NM", StringType(), nullable=False),
    StructField("SUB_GRP_BASE_NO", StringType(), nullable=False),
    StructField("SUB_GRP_SECT_NO", StringType(), nullable=False),
    StructField("SUB_ID", StringType(), nullable=False),
    StructField("SUB_FIRST_NM", StringType(), nullable=False),
    StructField("SUB_MIDINIT", StringType(), nullable=False),
    StructField("SUB_LAST_NM", StringType(), nullable=False),
    StructField("SUB_ADDR_LN_1", StringType(), nullable=False),
    StructField("SUB_ADDR_LN_2", StringType(), nullable=False),
    StructField("SUB_CITY_NM", StringType(), nullable=False),
    StructField("CLM_EXTRNL_MBRSH_SUB_ST_CD", StringType(), nullable=False),
    StructField("SUB_CNTY_FIPS_ID", StringType(), nullable=False),
    StructField("SUB_POSTAL_CD", StringType(), nullable=False),
    StructField("SUBMT_SUB_ID", StringType(), nullable=False),
    StructField("ACTL_SUB_ID", StringType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False)
])

df_ClmExtrnlMbrshCrf = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_ClmExtrnlMbrshCrf)
    .csv(f"{adls_path}/key/{InFile}")
)

df_ForeignKey = df_ClmExtrnlMbrshCrf \
    .withColumn(
        "ClmSK",
        GetFkeyClm(
            F.col("SRC_SYS_CD"),
            F.col("CLM_EXTRNL_MBRSH_SK"),
            F.col("CLM_ID"),
            Logging
        )
    ) \
    .withColumn(
        "MbrGndrCdSk",
        GetFkeyCodes(
            F.lit("FACETS"),
            F.col("CLM_EXTRNL_MBRSH_SK"),
            F.lit("GENDER"),
            F.col("MBR_GNDR_CD"),
            F.lit("X")
        )
    ) \
    .withColumn(
        "RelshpCdSk",
        GetFkeyCodes(
            F.when(F.col("SRC_SYS_CD") == "LUMERIS", "FACETS").otherwise(F.col("SRC_SYS_CD")),
            F.col("CLM_EXTRNL_MBRSH_SK"),
            F.lit("MEMBER RELATIONSHIP"),
            F.col("MBR_RELSHP_CD"),
            F.lit("X")
        )
    ) \
    .withColumn(
        "MbrBrthDtSk",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("CLM_EXTRNL_MBRSH_SK"),
            F.col("MBR_BRTH_DT"),
            F.lit("X")
        )
    ) \
    .withColumn(
        "ExtrnlMbrshSubSt",
        GetFkeyCodes(
            F.lit("FACETS"),
            F.col("CLM_EXTRNL_MBRSH_SK"),
            F.lit("STATE"),
            F.col("CLM_EXTRNL_MBRSH_SUB_ST_CD"),
            F.lit("X")
        )
    ) \
    .withColumn(
        "pMbrSK",
        F.when(F.col("MBR_UNIQ_KEY") == 1, F.lit(1))
         .when(F.col("MBR_UNIQ_KEY") == 0, F.lit(0))
         .otherwise(
             GetFkeyMbr(
                 trim(F.col("SRC_SYS_CD")),
                 F.col("CLM_EXTRNL_MBRSH_SK"),
                 F.col("MBR_UNIQ_KEY"),
                 F.lit("Y")
             )
         )
    ) \
    .withColumn("PassThru", F.col("PASS_THRU_IN")) \
    .withColumn(
        "ErrCount",
        GetFkeyErrorCnt(F.col("CLM_EXTRNL_MBRSH_SK"))
    )

df_Fkey1 = df_ForeignKey.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y')
).select(
    F.col("CLM_EXTRNL_MBRSH_SK").alias("CLM_EXTRNL_MBRSH_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ClmSK").alias("CLM_SK"),
    F.col("MbrGndrCdSk").alias("MBR_GNDR_CD_SK"),
    F.col("RelshpCdSk").alias("MBR_RELSHP_CD_SK"),
    F.col("MbrBrthDtSk").alias("MBR_BRTH_DT_SK"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("MBR_MIDINIT").alias("MBR_MIDINIT"),
    F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("PCKG_CD_ID").alias("PCKG_CD_ID"),
    F.col("PATN_NTWK_SH_NM").alias("PATN_NTWK_SH_NM"),
    F.col("SUB_GRP_BASE_NO").alias("SUB_GRP_BASE_NO"),
    F.col("SUB_GRP_SECT_NO").alias("SUB_GRP_SECT_NO"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("SUB_MIDINIT").alias("SUB_MIDINIT"),
    F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("SUB_ADDR_LN_1").alias("SUB_ADDR_LN_1"),
    F.col("SUB_ADDR_LN_2").alias("SUB_ADDR_LN_2"),
    F.col("SUB_CITY_NM").alias("SUB_CITY_NM"),
    F.col("ExtrnlMbrshSubSt").alias("CLM_EXTRNL_MBRSH_SUB_ST_CD_SK"),
    F.col("SUB_CNTY_FIPS_ID").alias("SUB_CNTY_FIPS_ID"),
    F.col("SUB_POSTAL_CD").alias("SUB_POSTAL_CD"),
    F.col("SUBMT_SUB_ID").alias("SUBMT_SUB_ID"),
    F.col("ACTL_SUB_ID").alias("ACTL_SUB_ID"),
    F.expr("CASE WHEN pMbrSK=0 THEN 1 ELSE pMbrSK END").alias("MBR_SK")
)

df_Recycle1 = df_ForeignKey.filter(
    F.col("ErrCount") > 0
).select(
    GetRecycleKey(F.col("CLM_EXTRNL_MBRSH_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("RECYCLE_CT") + 1).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_EXTRNL_MBRSH_SK").alias("CLM_EXTRNL_MBRSH_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_GNDR_CD").alias("MBR_GNDR_CD_CD"),
    F.col("MBR_RELSHP_CD").alias("MBR_RELSHP_CD_CD"),
    F.col("MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("MBR_MIDINIT").alias("MBR_MIDINIT"),
    F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("PCKG_CD_ID").alias("PCKG_CD_ID"),
    F.col("PATN_NTWK_SH_NM").alias("PATN_NTWK_SH_NM"),
    F.col("SUB_GRP_BASE_NO").alias("SUB_GRP_BASE_NO"),
    F.col("SUB_GRP_SECT_NO").alias("SUB_GRP_SECT_NO"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("SUB_MIDINIT").alias("SUB_MIDINIT"),
    F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("SUB_ADDR_LN_1").alias("SUB_ADDR_LN_1"),
    F.col("SUB_ADDR_LN_2").alias("SUB_ADDR_LN_2"),
    F.col("SUB_CITY_NM").alias("SUB_CITY_NM"),
    F.col("CLM_EXTRNL_MBRSH_SUB_ST_CD").alias("CLM_EXTRNL_MBRSH_SUB_ST_CD"),
    F.col("SUB_CNTY_FIPS_ID").alias("SUB_CNTY_FIPS_ID"),
    F.col("SUB_POSTAL_CD").alias("SUB_POSTAL_CD"),
    F.col("SUBMT_SUB_ID").alias("SUBMT_SUB_ID"),
    F.col("ACTL_SUB_ID").alias("ACTL_SUB_ID"),
    F.expr("CASE WHEN pMbrSK=0 THEN 1 ELSE pMbrSK END").alias("MBR_SK")
)

df_DefaultUNK_base = df_ForeignKey.limit(1)
df_DefaultUNK = df_DefaultUNK_base.select(
    F.lit(0).alias("CLM_EXTRNL_MBRSH_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.lit(0).alias("MBR_GNDR_CD_SK"),
    F.lit(0).alias("MBR_RELSHP_CD_SK"),
    F.lit("NA").alias("MBR_BRTH_DT_SK"),
    F.lit("UNK").alias("GRP_NM"),
    F.lit("UNK").alias("MBR_FIRST_NM"),
    F.lit("U").alias("MBR_MIDINIT"),
    F.lit("UNK").alias("MBR_LAST_NM"),
    F.lit("UNK").alias("PCKG_CD_ID"),
    F.lit("UNK").alias("PATN_NTWK_SH_NM"),
    F.lit("UNK").alias("SUB_GRP_BASE_NO"),
    F.lit("UNK").alias("SUB_GRP_SECT_NO"),
    F.lit("UNK").alias("SUB_ID"),
    F.lit("UNK").alias("SUB_FIRST_NM"),
    F.lit("U").alias("SUB_MIDINIT"),
    F.lit("UNK").alias("SUB_LAST_NM"),
    F.lit("UNK").alias("SUB_ADDR_LN_1"),
    F.lit("UNK").alias("SUB_ADDR_LN_2"),
    F.lit("UNK").alias("SUB_CITY_NM"),
    F.lit(0).alias("CLM_EXTRNL_MBRSH_SUB_ST_CD_SK"),
    F.lit("UNK").alias("SUB_CNTY_FIPS_ID"),
    F.lit("UNK").alias("SUB_POSTAL_CD"),
    F.lit("UNK").alias("SUBMT_SUB_ID"),
    F.lit("UNK").alias("ACTL_SUB_ID"),
    F.lit(0).alias("MBR_SK")
)

df_DefaultNA_base = df_ForeignKey.limit(1)
df_DefaultNA = df_DefaultNA_base.select(
    F.lit(1).alias("CLM_EXTRNL_MBRSH_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("CLM_ID"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CLM_SK"),
    F.lit(1).alias("MBR_GNDR_CD_SK"),
    F.lit(1).alias("MBR_RELSHP_CD_SK"),
    F.lit("NA").alias("MBR_BRTH_DT_SK"),
    F.lit("NA").alias("GRP_NM"),
    F.lit("NA").alias("MBR_FIRST_NM"),
    Space(1).alias("MBR_MIDINIT"),
    F.lit("NA").alias("MBR_LAST_NM"),
    F.lit("NA").alias("PCKG_CD_ID"),
    F.lit("NA").alias("PATN_NTWK_SH_NM"),
    F.lit("NA").alias("SUB_GRP_BASE_NO"),
    F.lit("NA").alias("SUB_GRP_SECT_NO"),
    F.lit("NA").alias("SUB_ID"),
    F.lit("NA").alias("SUB_FIRST_NM"),
    Space(1).alias("SUB_MIDINIT"),
    F.lit("NA").alias("SUB_LAST_NM"),
    F.lit("NA").alias("SUB_ADDR_LN_1"),
    F.lit("NA").alias("SUB_ADDR_LN_2"),
    F.lit("NA").alias("SUB_CITY_NM"),
    F.lit(1).alias("CLM_EXTRNL_MBRSH_SUB_ST_CD_SK"),
    F.lit("NA").alias("SUB_CNTY_FIPS_ID"),
    F.lit("NA").alias("SUB_POSTAL_CD"),
    F.lit("NA").alias("SUBMT_SUB_ID"),
    F.lit("NA").alias("ACTL_SUB_ID"),
    F.lit(1).alias("MBR_SK")
)

df_Recycle_Clms = df_ForeignKey.filter(
    F.col("ErrCount") > 0
).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID")
)

df_Collector = df_Fkey1.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

df_Collector_final = df_Collector.select(
    "CLM_EXTRNL_MBRSH_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "MBR_GNDR_CD_SK",
    "MBR_RELSHP_CD_SK",
    "MBR_BRTH_DT_SK",
    "GRP_NM",
    "MBR_FIRST_NM",
    "MBR_MIDINIT",
    "MBR_LAST_NM",
    "PCKG_CD_ID",
    "PATN_NTWK_SH_NM",
    "SUB_GRP_BASE_NO",
    "SUB_GRP_SECT_NO",
    "SUB_ID",
    "SUB_FIRST_NM",
    "SUB_MIDINIT",
    "SUB_LAST_NM",
    "SUB_ADDR_LN_1",
    "SUB_ADDR_LN_2",
    "SUB_CITY_NM",
    "CLM_EXTRNL_MBRSH_SUB_ST_CD_SK",
    "SUB_CNTY_FIPS_ID",
    "SUB_POSTAL_CD",
    "SUBMT_SUB_ID",
    "ACTL_SUB_ID",
    "MBR_SK"
)

df_hf_recycle_rpad = df_Recycle1 \
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " ")) \
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " ")) \
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " ")) \
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " ")) \
    .withColumn("MBR_MIDINIT", F.rpad(F.col("MBR_MIDINIT"), 1, " ")) \
    .withColumn("PATN_NTWK_SH_NM", F.rpad(F.col("PATN_NTWK_SH_NM"), 6, " ")) \
    .withColumn("SUB_GRP_BASE_NO", F.rpad(F.col("SUB_GRP_BASE_NO"), 9, " ")) \
    .withColumn("SUB_GRP_SECT_NO", F.rpad(F.col("SUB_GRP_SECT_NO"), 4, " ")) \
    .withColumn("SUB_ID", F.rpad(F.col("SUB_ID"), 14, " ")) \
    .withColumn("SUB_MIDINIT", F.rpad(F.col("SUB_MIDINIT"), 1, " ")) \
    .withColumn("SUB_CNTY_FIPS_ID", F.rpad(F.col("SUB_CNTY_FIPS_ID"), 3, " ")) \
    .withColumn("SUB_POSTAL_CD", F.rpad(F.col("SUB_POSTAL_CD"), 11, " "))

write_files(
    df_hf_recycle_rpad,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=False,
    quote="\"",
    nullValue=None
)

df_hf_claim_recycle_keys_rpad = df_Recycle_Clms  # "SRC_SYS_CD" is varchar (length unknown), "CLM_ID" is char(18) in the source reference
df_hf_claim_recycle_keys_rpad = df_hf_claim_recycle_keys_rpad.withColumn(
    "CLM_ID", F.rpad(F.col("CLM_ID"), 18, " ")
)

write_files(
    df_hf_claim_recycle_keys_rpad,
    "hf_claim_recycle_keys.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=False,
    quote="\"",
    nullValue=None
)

df_CLM_EXTRNL_MBRSH_rpad = df_Collector_final \
    .withColumn("MBR_BRTH_DT_SK", F.rpad(F.col("MBR_BRTH_DT_SK"), 10, " ")) \
    .withColumn("MBR_MIDINIT", F.rpad(F.col("MBR_MIDINIT"), 1, " ")) \
    .withColumn("PATN_NTWK_SH_NM", F.rpad(F.col("PATN_NTWK_SH_NM"), 6, " ")) \
    .withColumn("SUB_GRP_BASE_NO", F.rpad(F.col("SUB_GRP_BASE_NO"), 9, " ")) \
    .withColumn("SUB_GRP_SECT_NO", F.rpad(F.col("SUB_GRP_SECT_NO"), 4, " ")) \
    .withColumn("SUB_ID", F.rpad(F.col("SUB_ID"), 14, " ")) \
    .withColumn("SUB_MIDINIT", F.rpad(F.col("SUB_MIDINIT"), 1, " ")) \
    .withColumn("SUB_CNTY_FIPS_ID", F.rpad(F.col("SUB_CNTY_FIPS_ID"), 3, " ")) \
    .withColumn("SUB_POSTAL_CD", F.rpad(F.col("SUB_POSTAL_CD"), 11, " "))

write_files(
    df_CLM_EXTRNL_MBRSH_rpad,
    f"CLM_EXTRNL_MBRSH.{Source}.dat" if Source else "CLM_EXTRNL_MBRSH.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)