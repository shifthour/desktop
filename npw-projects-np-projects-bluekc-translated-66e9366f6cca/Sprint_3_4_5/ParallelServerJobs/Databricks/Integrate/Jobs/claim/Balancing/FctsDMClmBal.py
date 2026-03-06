# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_3 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 08/30/07 07:19:35 Batch  14487_26382 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 10:06:05 Batch  14484_36371 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/26/07 12:34:37 Batch  14330_45286 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_5 12/18/06 15:24:00 Batch  14232_55459 PROMOTE bckcetl ids20 dsadm Keith for Ralph install 12/18/2006
# MAGIC ^1_5 12/18/06 14:39:27 Batch  14232_52797 INIT bckcett testIDS30 dsadm Keith for Ralph CDHP Install 12182006
# MAGIC ^1_1 11/22/06 12:44:08 Batch  14206_45860 PROMOTE bckcett testIDS30 u06640 Ralph
# MAGIC ^1_1 11/22/06 12:42:17 Batch  14206_45746 PROMOTE bckcett testIDS30 u06640 Ralph
# MAGIC ^1_1 11/22/06 12:41:14 Batch  14206_45680 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_1 08/07/06 07:51:34 Batch  14099_28298 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_3 04/13/06 07:36:31 Batch  13983_27397 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_2 04/02/06 15:43:48 Batch  13972_56632 INIT bckcett devlIDS30 dcg01 steffy
# MAGIC ^1_2 03/29/06 15:32:39 Batch  13968_55966 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_1 03/29/06 14:54:04 Batch  13968_53650 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_1 03/29/06 05:18:35 Batch  13968_19121 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 02/24/06 07:36:27 Batch  13935_27392 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 4;hf_clm_remit_clm_bal;hf_clm_bal_prov;hf_clm_bal_facility;hf_clm_bal_status
# MAGIC 
# MAGIC JOB NAME:     FctsDMClmBal
# MAGIC CALLED BY:  FctsClmPrereqSeq
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from Facets tables to create a balancing table used to compare Facets to  Claim Data Mart
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	CMC_CLCL_CLAIM
# MAGIC                 tempdb..TMP_IDS_CLAIMxxxxx
# MAGIC                 CMC_CLST_STATUS
# MAGIC                 CMC_CLHP_HOSP
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                Created in BCBSClmRemitHistExt - cleared here
# MAGIC                   hf_clm_remit_clm_bal
# MAGIC                Created here - cleared here
# MAGIC                   hf_clm_bal_prov
# MAGIC                   hf_clm_bal_facility
# MAGIC                   hf_clm_bal_status
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   STRIP.FIELD
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Most fields are pulled from Facets exactly as they are for the CLM table.  Fields pulled from CLM_REMIT are obtained from a hash file created in BCBSClmRemitHistExtr - therefore, this job cannot run before the prereq step.  Later, these fields are compared to the DataMart fields for balancing purposes.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     W_CLM_BAL.dat to be loaded to IDS
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Steph Goddard   02/06/2006-   Originally Programmed
# MAGIC               
# MAGIC 
# MAGIC 4;hf_clm_remit_clm_bal; hf_clm_bal_prov; hf_clm_bal_facility; hf_clm_bal_status
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Prabhu ES               2022-02-26       S2S Remediation   MSSQL connection parameters added                                        IntegrateDev5	Ken Bradmon	2022-06-10

# MAGIC Facets - Claim Mart
# MAGIC Claim Balancing
# MAGIC This program will extract claim information from Facets using the driver table - and create a file to load to W_CLM_BAL (used by Claim Mart processing to balance against)
# MAGIC Hash file from BCBSClmRemitHistExtr with remit history fields (cleared here) - too much logic to re-create in this program.  
# MAGIC 
# MAGIC BCBSClmRemitHistExtr MUST run before this program!
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# ----------------------------------------------------------------------------
# Parameters
# ----------------------------------------------------------------------------
DriverTable = get_widget_value('DriverTable', '')
FacetsOwner = get_widget_value('FacetsOwner', '')
facets_secret_name = get_widget_value('facets_secret_name', '')
RunID = get_widget_value('RunID', '')

# ----------------------------------------------------------------------------
# Read Config for DB Connection
# ----------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(facets_secret_name)

# ----------------------------------------------------------------------------
# Read Hashed File: hf_clm_remit_clm_bal (Scenario C -> read parquet)
# ----------------------------------------------------------------------------
schema_hf_clm_remit_clm_bal = StructType([
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("NET_PAY_AMT", DecimalType(38, 10), nullable=False),
    StructField("ACTL_PD_AMT", DecimalType(38, 10), nullable=False),
    StructField("CHK_PD_DT_SK", StringType(), nullable=False),
    StructField("CHK_NO", IntegerType(), nullable=False)
])

df_clm_remit_clm_bal = (
    spark.read.schema(schema_hf_clm_remit_clm_bal)
    .parquet(f"{adls_path}/hf_clm_remit_clm_bal.parquet")
)

# ----------------------------------------------------------------------------
# ODBCConnector: Facets_Claim -> lnkBaseClms
# ----------------------------------------------------------------------------
extract_query_lnkBaseClms = f"""
SELECT A.CLCL_ID,
       A.MEME_CK,
       A.CLCL_CL_TYPE,
       A.CLCL_CL_SUB_TYPE,
       A.CLCL_CUR_STS,
       A.CLCL_LAST_ACT_DTM,
       A.CLCL_PAID_DT,
       A.CLCL_PAY_PR_IND,
       A.CLCL_TOT_CHG,
       A.CLCL_TOT_PAYABLE
FROM {FacetsOwner}.CMC_CLCL_CLAIM A,
     tempdb..{DriverTable} B
WHERE A.CLCL_ID = B.CLM_ID
"""

df_lnkBaseClms = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_lnkBaseClms)
    .load()
)

# ----------------------------------------------------------------------------
# ODBCConnector: Facets_Claim -> Facility_fields
# ----------------------------------------------------------------------------
extract_query_facility_fields = f"""
SELECT A.CLCL_ID,
       A.CLHP_FAC_TYPE,
       A.CLHP_BILL_CLASS
FROM {FacetsOwner}.CMC_CLHP_HOSP A,
     tempdb..{DriverTable} B
WHERE A.CLCL_ID = B.CLM_ID
"""

df_facility_fields = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_facility_fields)
    .load()
)

# ----------------------------------------------------------------------------
# ODBCConnector: Facets_Claim -> Pd_prov
# ----------------------------------------------------------------------------
extract_query_pd_prov = f"""
SELECT A.CLCL_ID,
       A.CLCL_PAYEE_PR_ID,
       A.CLCL_CUR_STS
FROM {FacetsOwner}.CMC_CLCL_CLAIM A,
     tempdb..{DriverTable} TMP
WHERE TMP.CLM_ID = A.CLCL_ID
  AND A.CLCL_PAY_PR_IND = "P"
"""

df_pd_prov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_pd_prov)
    .load()
)

# ----------------------------------------------------------------------------
# ODBCConnector: Facets_Claim -> Status_field
# ----------------------------------------------------------------------------
extract_query_status_field = f"""
SELECT stat.CLCL_ID,
       stat.CLST_STS,
       stat.CLST_STS_DTM
FROM tempdb..{DriverTable} TMP,
     {FacetsOwner}.CMC_CLST_STATUS stat
WHERE TMP.CLM_ID = stat.CLCL_ID
  AND stat.CLST_STS_DTM = (
       SELECT MAX(stat2.CLST_STS_DTM)
       FROM {FacetsOwner}.CMC_CLST_STATUS stat2
       WHERE stat2.CLCL_ID = stat.CLCL_ID
  )
"""

df_status_field = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_status_field)
    .load()
)

# ----------------------------------------------------------------------------
# CHashedFileStage: hf_clm_bal_hash_files (Scenario C)
# Write the three incoming links to three parquet files
# ----------------------------------------------------------------------------

# Facility_fields -> write to hf_clm_bal_facility.parquet
write_files(
    df_facility_fields,
    "hf_clm_bal_facility.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Pd_prov -> write to Pd_prov.parquet
write_files(
    df_pd_prov,
    "Pd_prov.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Status_field -> write to Status_field.parquet
write_files(
    df_status_field,
    "Status_field.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------
# CHashedFileStage: hf_clm_bal_hash_files (Scenario C)
# Read from the three parquet files to produce ref_facility, ref_prov, ref_status
# ----------------------------------------------------------------------------

schema_ref_facility = StructType([
    StructField("CLCL_ID", StringType(), nullable=False),
    StructField("CLHP_FAC_TYPE", StringType(), nullable=False),
    StructField("CLHP_BILL_CLASS", StringType(), nullable=False)
])

df_ref_facility = (
    spark.read.schema(schema_ref_facility)
    .parquet("hf_clm_bal_facility.parquet")
)

schema_ref_prov = StructType([
    StructField("CLCL_ID", StringType(), nullable=False),
    StructField("CLCL_PAYEE_PR_ID", StringType(), nullable=False),
    StructField("CLCL_CUR_STS", StringType(), nullable=False)
])

df_ref_prov = (
    spark.read.schema(schema_ref_prov)
    .parquet("hf_clm_bal_prov.parquet")
)

schema_ref_status = StructType([
    StructField("CLCL_ID", StringType(), nullable=False),
    StructField("CLST_STS", StringType(), nullable=False),
    StructField("CLST_STS_DTM", TimestampType(), nullable=False)
])

df_ref_status = (
    spark.read.schema(schema_ref_status)
    .parquet("hf_clm_bal_status.parquet")
)

# ----------------------------------------------------------------------------
# TransBal (CTransformerStage)
# ----------------------------------------------------------------------------

# 1) Join the primary link (lnkBaseClms) with all lookups

df_join_1 = df_lnkBaseClms.alias("lnkBaseClms").join(
    df_clm_remit_clm_bal.alias("clm_remit_fields"),
    F.col("lnkBaseClms.CLCL_ID") == F.col("clm_remit_fields.CLM_ID"),
    how="left"
)

df_join_2 = df_join_1.alias("df1").join(
    df_ref_facility.alias("ref_facility"),
    F.col("df1.lnkBaseClms.CLCL_ID") == F.col("ref_facility.CLCL_ID"),
    how="left"
)

df_join_3 = df_join_2.alias("df2").join(
    df_ref_prov.alias("ref_prov"),
    F.col("df2.lnkBaseClms.CLCL_ID") == F.col("ref_prov.CLCL_ID"),
    how="left"
)

df_join_4 = df_join_3.alias("df3").join(
    df_ref_status.alias("ref_status"),
    F.col("df3.lnkBaseClms.CLCL_ID") == F.col("ref_status.CLCL_ID"),
    how="left"
)

# 2) Create stage-variable columns

# bIsClosed: True if CLCL_CUR_STS is in 93,97,99
df_with_bIsClosed = df_join_4.withColumn(
    "bIsClosed",
    F.when(
        F.col("lnkBaseClms.CLCL_CUR_STS").isin("93","97","99"),
        F.lit(True)
    ).otherwise(False)
)

# BillProv: complicated conditional with user-defined function GetFkeyProv
# We nest the logic exactly as stated:
df_with_BillProv = df_with_bIsClosed.withColumn(
    "BillProv",
    F.when(
        F.col("bIsClosed"),
        F.when(
            GetFkeyProv(
                F.lit("FACETS"), 
                F.lit(1), 
                F.col("ref_prov.CLCL_PAYEE_PR_ID"), 
                F.lit("X")
            ) == 0,
            F.lit("NA")
        ).otherwise(
            F.when(
                F.col("ref_prov.CLCL_PAYEE_PR_ID").isNull() | (F.length(trim(F.col("ref_prov.CLCL_PAYEE_PR_ID"))) == 0),
                F.lit("NA")
            ).otherwise(trim(F.col("ref_prov.CLCL_PAYEE_PR_ID")))
        )
    ).otherwise(
        F.when(
            F.col("ref_prov.CLCL_PAYEE_PR_ID").isNull() | (F.length(trim(F.col("ref_prov.CLCL_PAYEE_PR_ID"))) == 0),
            F.lit("NA")
        ).otherwise(trim(F.col("ref_prov.CLCL_PAYEE_PR_ID")))
    )
)

# StatusDt: If IsNull(ref_status.CLCL_ID) then lnkBaseClms.CLCL_LAST_ACT_DTM[1,10]
#           else if ref_status.CLST_STS_DTM >= lnkBaseClms.CLCL_LAST_ACT_DTM => ref_status.CLST_STS_DTM[1,10]
#           else lnkBaseClms.CLCL_LAST_ACT_DTM[1,10]
df_with_StatusDt = df_with_BillProv.withColumn(
    "StatusDt",
    F.when(
        F.col("ref_status.CLCL_ID").isNull(),
        F.substring(F.col("lnkBaseClms.CLCL_LAST_ACT_DTM"), 1, 10)
    ).otherwise(
        F.when(
            F.col("ref_status.CLST_STS_DTM") >= F.col("lnkBaseClms.CLCL_LAST_ACT_DTM"),
            F.substring(F.col("ref_status.CLST_STS_DTM").cast(StringType()), 1, 10)
        ).otherwise(
            F.substring(F.col("lnkBaseClms.CLCL_LAST_ACT_DTM"), 1, 10)
        )
    )
)

# FacetsSK: user-defined function GetFkeyCodes("FACETS","1","SOURCE SYSTEM","FACETS","N")
df_TransBal = df_with_StatusDt.withColumn(
    "FacetsSK",
    GetFkeyCodes(
        F.lit("FACETS"),
        F.lit("1"),
        F.lit("SOURCE SYSTEM"),
        F.lit("FACETS"),
        F.lit("N")
    )
)

# 3) Build the final columns in the same order as in the Output Pin "balancing"

df_balancing = (
    df_TransBal
    .select(
        F.col("FacetsSK").alias("SRC_SYS_CD_SK"),
        strip_field(trim(F.col("lnkBaseClms.CLCL_ID"))).alias("CLM_ID"),
        strip_field(trim(F.col("lnkBaseClms.CLCL_CUR_STS"))).alias("CLM_STTUS_CD"),
        strip_field(trim(F.col("lnkBaseClms.CLCL_CL_TYPE"))).alias("CLM_TYP_CD"),
        strip_field(trim(F.col("lnkBaseClms.CLCL_CL_SUB_TYPE"))).alias("CLM_SUBTYP_CD"),
        F.substring(F.col("lnkBaseClms.CLCL_PAID_DT").cast(StringType()),1,10).alias("CLM_PD_DT_SK"),
        F.when(
            F.col("clm_remit_fields.CLM_ID").isNull() , F.lit("UNK")
        ).otherwise(
            F.when(
                F.length(trim(F.col("clm_remit_fields.CHK_PD_DT_SK"))) == 0,
                F.lit("UNK")
            ).otherwise(F.col("clm_remit_fields.CHK_PD_DT_SK"))
        ).alias("CLM_REMIT_HIST_CHK_PD_DT_SK"),
        F.col("StatusDt").alias("CLM_STTUS_DT_SK"),
        F.col("lnkBaseClms.CLCL_TOT_PAYABLE").alias("CLM_PAYBL_AMT"),
        F.when(
            F.col("clm_remit_fields.CLM_ID").isNull(),
            F.lit(0)
        ).otherwise(F.col("clm_remit_fields.ACTL_PD_AMT")).alias("CLM_ACTL_PD_AMT"),
        F.col("lnkBaseClms.CLCL_TOT_CHG").alias("CLM_CHRG_AMT"),
        F.when(
            F.col("clm_remit_fields.CLM_ID").isNull(),
            F.lit(0)
        ).otherwise(F.col("clm_remit_fields.NET_PAY_AMT")).alias("CLM_REMIT_HIST_CHK_NET_PAY_AMT"),
        F.when(
            F.col("clm_remit_fields.CLM_ID").isNull(),
            F.lit(0)
        ).otherwise(
            F.when(
                F.col("clm_remit_fields.CHK_NO").isNull(),
                F.lit(0)
            ).otherwise(
                F.when(F.col("clm_remit_fields.CHK_NO") >= 0, F.col("clm_remit_fields.CHK_NO")).otherwise(F.lit(0))
            )
        ).alias("CLM_REMIT_HIST_CHK_NO"),
        F.col("lnkBaseClms.MEME_CK").alias("MBR_UNIQ_KEY"),
        F.col("BillProv").alias("PD_PROV_ID"),
        strip_field(trim(F.col("lnkBaseClms.CLCL_PAY_PR_IND"))).alias("CLM_PAYE_CD"),
        F.when(
            F.col("ref_facility.CLCL_ID").isNull(),
            F.lit("NA")
        ).otherwise(
            F.col("ref_facility.CLHP_BILL_CLASS")
        ).alias("FCLTY_CLM_BILL_CLS_CD"),
        F.when(
            F.col("ref_facility.CLCL_ID").isNull(),
            F.lit("NA")
        ).otherwise(
            F.col("ref_facility.CLHP_FAC_TYPE")
        ).alias("FCLTY_CLM_BILL_TYP_CD")
    )
)

# 4) Apply rpad for known char columns in the final DataFrame
#    (where length is explicitly known from the job)
df_balancing_padded = (
    df_balancing
    .withColumn("CLM_PD_DT_SK", F.rpad(F.col("CLM_PD_DT_SK"), 10, " "))
    .withColumn("CLM_REMIT_HIST_CHK_PD_DT_SK", F.rpad(F.col("CLM_REMIT_HIST_CHK_PD_DT_SK"), 10, " "))
    .withColumn("CLM_STTUS_DT_SK", F.rpad(F.col("CLM_STTUS_DT_SK"), 10, " "))
)

# ----------------------------------------------------------------------------
# W_CLM_BAL (CSeqFileStage) -> Write to load/W_CLM_BAL.dat.#RunID#
# ----------------------------------------------------------------------------
final_file_path = f"{adls_path}/load/W_CLM_BAL.dat.{RunID}"
schema_out = StructType([])  # We define an empty schema placeholder here; the columns are already set in df.

write_files(
    df_balancing_padded,
    final_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)