# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 4;hf_clm_remit_clm_bal;hf_clm_bal_prov;hf_clm_bal_facility;hf_clm_bal_status
# MAGIC 
# MAGIC JOB NAME:     FctsDMClmBal
# MAGIC CALLED BY:  LhoFctsClmOnDmdPrereqSeq
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
# MAGIC             4;hf_clm_remit_clm_bal; hf_clm_bal_prov; hf_clm_bal_facility; hf_clm_bal_status
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:                                                                                                                                                                       Development                                Date 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                              Project              Code Reviewer    Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                      ----------------------   -------------------------   ------------------   
# MAGIC   Harikanth Reddy              10/12/2020                          brought up to Standards                                   IntegrateDev2
# MAGIC   Kotha Venkat        
# MAGIC Prabhu ES                        2022-03-17    S2S                 MSSQL ODBC conn params added                                                      IntegrateDev5	Ken Bradmon	2022-06-10

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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


DriverTable = get_widget_value('DriverTable','')
FacetsOwner = get_widget_value('$FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)

# 1) Read from hashed file hf_clm_remit_clm_bal (Scenario C: source hashed file => read parquet):
df_hf_clm_remit_clm_bal = spark.read.parquet("hf_clm_remit_clm_bal.parquet")

# 2) Facets_Claim (ODBCConnector) => Four output links => each query as its own DataFrame:
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
df_Facets_Claim_lnkBaseClms = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_lnkBaseClms)
    .load()
)

extract_query_Facility_fields = f"""
SELECT A.CLCL_ID,
       A.CLHP_FAC_TYPE,
       A.CLHP_BILL_CLASS
FROM {FacetsOwner}.CMC_CLHP_HOSP A,
     tempdb..{DriverTable} B
WHERE A.CLCL_ID = B.CLM_ID
"""
df_Facets_Claim_Facility_fields = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Facility_fields)
    .load()
)

extract_query_Pd_prov = f"""
SELECT A.CLCL_ID,
       A.CLCL_PAYEE_PR_ID,
       A.CLCL_CUR_STS
FROM {FacetsOwner}.CMC_CLCL_CLAIM A,
     tempdb..{DriverTable} TMP
WHERE TMP.CLM_ID = A.CLCL_ID
  AND A.CLCL_PAY_PR_IND = 'P'
"""
df_Facets_Claim_Pd_prov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Pd_prov)
    .load()
)

extract_query_Status_field = f"""
SELECT stat.CLCL_ID,
       stat.CLST_STS,
       stat.CLST_STS_DTM
FROM tempdb..{DriverTable} TMP,
     {FacetsOwner}.CMC_CLST_STATUS stat,
     (
       SELECT MAX(CLST_STS_DTM) as CLST_STS_DTM
       FROM {FacetsOwner}.CMC_CLST_STATUS
     ) stat2
WHERE TMP.CLM_ID = stat.CLCL_ID
  AND stat.CLST_STS_DTM = stat2.CLST_STS_DTM
"""
df_Facets_Claim_Status_field = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Status_field)
    .load()
)

# 3) hf_clm_bal_hash_files (Scenario A: intermediate hashed file) => remove it and deduplicate each link on key columns:
df_ref_facility = df_Facets_Claim_Facility_fields.dropDuplicates(["CLCL_ID"])
df_ref_prov = df_Facets_Claim_Pd_prov.dropDuplicates(["CLCL_ID"])
df_ref_status = df_Facets_Claim_Status_field.dropDuplicates(["CLCL_ID"])

# 4) TransBal (CTransformerStage):
df_enriched = (
    df_Facets_Claim_lnkBaseClms.alias("lnkBaseClms")
    .join(df_ref_facility.alias("ref_facility"), F.col("lnkBaseClms.CLCL_ID") == F.col("ref_facility.CLCL_ID"), how="left")
    .join(df_ref_prov.alias("ref_prov"), F.col("lnkBaseClms.CLCL_ID") == F.col("ref_prov.CLCL_ID"), how="left")
    .join(df_ref_status.alias("ref_status"), F.col("lnkBaseClms.CLCL_ID") == F.col("ref_status.CLCL_ID"), how="left")
    .join(df_hf_clm_remit_clm_bal.alias("clm_remit_fields"), F.col("lnkBaseClms.CLCL_ID") == F.col("clm_remit_fields.CLM_ID"), how="left")
)

# Stage variable: bIsClosed
df_enriched = df_enriched.withColumn(
    "bIsClosed",
    F.when(F.col("lnkBaseClms.CLCL_CUR_STS").isin("93","97","99"), F.lit(True)).otherwise(F.lit(False))
)

# Stage variable: BillProv
df_enriched = df_enriched.withColumn(
    "BillProv",
    F.when(
        F.col("bIsClosed"), 
        F.when(
            GetFkeyProv(F.lit("FACETS"), F.lit(1), F.col("ref_prov.CLCL_PAYEE_PR_ID"), F.lit("X")).eqNullSafe(F.lit(0)),
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

# Stage variable: StatusDt
df_enriched = df_enriched.withColumn(
    "StatusDt",
    F.when(
        F.col("ref_status.CLCL_ID").isNull(),
        F.col("lnkBaseClms.CLCL_LAST_ACT_DTM").substr(F.lit(1), F.lit(10))
    ).otherwise(
        F.when(
            F.col("ref_status.CLST_STS_DTM") >= F.col("lnkBaseClms.CLCL_LAST_ACT_DTM"),
            F.col("ref_status.CLST_STS_DTM").substr(F.lit(1), F.lit(10))
        ).otherwise(F.col("lnkBaseClms.CLCL_LAST_ACT_DTM").substr(F.lit(1), F.lit(10)))
    )
)

# Stage variable: FacetsSK
df_enriched = df_enriched.withColumn(
    "FacetsSK",
    GetFkeyCodes(F.lit("FACETS"), F.lit("1"), F.lit("SOURCE SYSTEM"), F.lit("FACETS"), F.lit("N"))
)

# 5) Output link columns (balancing):
df_balancing = df_enriched.select(
    F.col("FacetsSK").alias("SRC_SYS_CD_SK"),
    strip_field(trim(F.col("lnkBaseClms.CLCL_ID"))).alias("CLM_ID"),
    strip_field(trim(F.col("lnkBaseClms.CLCL_CUR_STS"))).alias("CLM_STTUS_CD"),
    strip_field(trim(F.col("lnkBaseClms.CLCL_CL_TYPE"))).alias("CLM_TYP_CD"),
    strip_field(trim(F.col("lnkBaseClms.CLCL_CL_SUB_TYPE"))).alias("CLM_SUBTYP_CD"),
    F.col("lnkBaseClms.CLCL_PAID_DT").substr(F.lit(1), F.lit(10)).alias("CLM_PD_DT_SK"),
    F.when(
        F.col("clm_remit_fields.CLM_ID").isNull(), 
        F.lit("UNK")
    ).otherwise(
        F.when(F.length(trim(F.col("clm_remit_fields.CHK_PD_DT_SK"))) == 0, "UNK")
        .otherwise(F.col("clm_remit_fields.CHK_PD_DT_SK"))
    ).alias("CLM_REMIT_HIST_CHK_PD_DT_SK"),
    F.col("StatusDt").alias("CLM_STTUS_DT_SK"),
    F.col("lnkBaseClms.CLCL_TOT_PAYABLE").alias("CLM_PAYBL_AMT"),
    F.when(
        F.col("clm_remit_fields.CLM_ID").isNull(), 
        F.lit(0)
    ).otherwise(
        F.col("clm_remit_fields.ACTL_PD_AMT")
    ).alias("CLM_ACTL_PD_AMT"),
    F.col("lnkBaseClms.CLCL_TOT_CHG").alias("CLM_CHRG_AMT"),
    F.when(
        F.col("clm_remit_fields.CLM_ID").isNull(),
        F.lit(0)
    ).otherwise(
        F.col("clm_remit_fields.NET_PAY_AMT")
    ).alias("CLM_REMIT_HIST_CHK_NET_PAY_AMT"),
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

# Apply rpad for char/varchar columns where known lengths are provided or inferred
df_balancing = (
    df_balancing
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 12, " "))
    .withColumn("CLM_STTUS_CD", F.rpad(F.col("CLM_STTUS_CD"), 2, " "))
    .withColumn("CLM_TYP_CD", F.rpad(F.col("CLM_TYP_CD"), 2, " "))
    .withColumn("CLM_SUBTYP_CD", F.rpad(F.col("CLM_SUBTYP_CD"), 2, " "))
    .withColumn("CLM_PD_DT_SK", F.rpad(F.col("CLM_PD_DT_SK"), 10, " "))
    .withColumn("CLM_REMIT_HIST_CHK_PD_DT_SK", F.rpad(F.col("CLM_REMIT_HIST_CHK_PD_DT_SK"), 10, " "))
    .withColumn("CLM_STTUS_DT_SK", F.rpad(F.col("CLM_STTUS_DT_SK"), 10, " "))
    .withColumn("PD_PROV_ID", F.rpad(F.col("PD_PROV_ID"), 12, " "))
    .withColumn("CLM_PAYE_CD", F.rpad(F.col("CLM_PAYE_CD"), 1, " "))
    .withColumn("FCLTY_CLM_BILL_CLS_CD", F.rpad(F.col("FCLTY_CLM_BILL_CLS_CD"), 1, " "))
    .withColumn("FCLTY_CLM_BILL_TYP_CD", F.rpad(F.col("FCLTY_CLM_BILL_TYP_CD"), 2, " "))
)

# 6) W_CLM_BAL (CSeqFileStage) => write to .dat.#RunID# file
write_files(
    df_balancing,
    f"{adls_path}/load/W_CLM_BAL.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)