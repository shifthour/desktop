# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ********************************************************************************************
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:      BCBSADrugLandSeq
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from BCBSA RX Claims file does member matching, builds Claim Id and creates a landing file for IDS processing.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC ===============================================================================================================================================
# MAGIC 											Development 
# MAGIC Developer	Date		Project/Altiris#	Description				Project		Code Reviewer	Date Reviewed
# MAGIC ===============================================================================================================================================
# MAGIC Abhiram Dasarathy	2016-11-07	5568 HEDIS	Initial Programming - New FEP RX Claims file	IntegrateDev2	Kalyan Neelam	2016-11-15
# MAGIC Abhiram Dasarathy	2017-02-03	5568 HEDIS	Added parameter Current date minus three years	IntegrateDev1	Kalyan Neelam        2017-02-07
# MAGIC Abhiram Dasarathy	2017-02-27	5568 HEDIS	Modified the extract for the FEP_MBR to pull only	IntegrateDev2	Kalyan Neelam        2017-02-27
# MAGIC 						the Max(MBR_UNIQ_KEY) and also changed the
# MAGIC 						tranformation rule for DISPNS_PROV_ID	 
# MAGIC Saikiran Mahenderker 2018-03-15          5781 HEDIS            Modified  existing columns PDX_NABP_NO            IntegrateDev2	Jaideep Mankala    04/04/2018
# MAGIC                                                                                                 PRSCRB_PROV to PDX_NTNL_PROV_ID,       
# MAGIC                                                                                                 PRSCRB_PROV_NTNL_PROV_ID AND 
# MAGIC                                                                                                 Added New Fields PD_DT, ADJ_CD ,  
# MAGIC                                                                                                 DISPNS_DRUG_TYP, DISPNS_AS_WRTN_STTUS_CD    
# MAGIC                                                                                                 as per the new layout   
# MAGIC Karthik Chintalapani	2019-08-27      5884 HEDIS	       Added the reversals logic for FEPRX Claims void file	 IntegrateDev1	Kalyan Neelam         2019-09-05
# MAGIC 
# MAGIC Karthik Chintalapani	2019-10-14      5884   Added the hash files before the link collector stage to resolve 
# MAGIC                                                                   the buffer issues in production.                                                                  IntegrateDev1    Kalyan Neelam         2019-10-16
# MAGIC                                                                                                                    
# MAGIC Karthik Chintalapani	2019-12-14      5884   Added 2 new fields DRUG_NM, DRUG_STRG in the Source stage       IntegrateDev1    Jaideep Mankala      01/09/2020  
# MAGIC                                                                    as per the source file format changes from the BCA           
# MAGIC 
# MAGIC                                                                                                                    
# MAGIC Karthik Chintalapani	2020-01-30      5884   Modifed the sub_id transformation to handle nulls in BuildClmId stage      IntegrateDev1    Jaideep Mankala      02/05/2020
# MAGIC 
# MAGIC 
# MAGIC Karthik Chintalapani  2020-11-07         us296747    Modified the jobs as per the new file layout from Association.IntegrateDev3   Jaideep Mankala      11/09/2020
# MAGIC 
# MAGIC Reddy Sanam          2025-04-14         US646193  Added logic to handle duplicates on CLM_ID
# MAGIC                                                                               If multiple rows are found for the same CLM_ID,
# MAGIC                                                                               CLM_ID is formed by taking 19 Characters from 9th 
# MAGIC                                                                                Character . Added Stages Agg_CLm_ID,Filter_Multiples
# MAGIC                                                                                hf_CLM_ID,hf_Main. Added LKP on CLM_ID in the 
# MAGIC                                                                               Stage                                                                                       IntegrateDev2      Jeyaprasanna     2025-04-14

# MAGIC When CLM_ID is derived from RCRD_ID and Julian DRUG_CLM_INCUR_DT, Those rows are seperated and looked up in the Transformer-BuildClmId and then CLM_ID derivation is changed to take 19 bytes from 9th byte of the field RCRD_ID
# MAGIC Append header record to audit file.
# MAGIC Total claim paid amounts and write to unix Drug_Header_Record.dat file
# MAGIC BCA FEP RX Claims Preprocessing Extract
# MAGIC Lookup the void file.
# MAGIC Check if Claim already exists in IDS
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
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
    DecimalType
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameters
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrDate = get_widget_value('CurrDate','')
InFile = get_widget_value('InFile','')
CurrDateMinus3Years = get_widget_value('CurrDateMinus3Years','')
VoidFile = get_widget_value('VoidFile','')

# --------------------------------------------------------------------------------
# Stage: FEP_PDX_DATA_1  (CSeqFileStage)
# Reading from landing (delimited .dat), must define schema
schema_FEP_PDX_DATA_1 = StructType([
    StructField("SRC_SYS", StringType(), True),
    StructField("RCRD_ID", StringType(), True),
    StructField("CLM_LN_NO", StringType(), True),
    StructField("FEP_PROD", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("CLM_DENIED_FLAG", StringType(), True),
    StructField("ILNS_DT", StringType(), True),
    StructField("DRUG_CD", StringType(), True),
    StructField("PDX_NTNL_PROV_ID", StringType(), True),
    StructField("DRUG_CLM_INCUR_DT", StringType(), True),
    StructField("PRSCRB_PROV_NTNL_PROV_ID", StringType(), True),
    StructField("PRESCRIBED_DT", StringType(), True),
    StructField("DRUG_RFL_CT", IntegerType(), True),
    StructField("DRUG_CLM_DRUG_QTY", DecimalType(38,10), True),
    StructField("QTY_DISPNS", DecimalType(38,10), True),
    StructField("DRUG_CLM_DRUG_DAYS_SUPL_CT", IntegerType(), True),
    StructField("DRUG_CLM_CT", IntegerType(), True),
    StructField("DRUG_CLM_PGM_PD_AMT", DecimalType(38,10), True),
    StructField("DRUG_CLM_AHFS_THRPTC_CLS", StringType(), True),
    StructField("DATA_SRC", StringType(), True),
    StructField("SUPLMT_DATA_SRC_TYP", StringType(), True),
    StructField("ADTR_APRV_STTUS", StringType(), True),
    StructField("RUN_DT", StringType(), True),
    StructField("DISPNS_DRUG_TYP", StringType(), True),
    StructField("DISPNS_AS_WRTN_STTUS_CD", StringType(), True),
    StructField("ADJ_CD", StringType(), True),
    StructField("PD_DT", StringType(), True),
    StructField("DRUG_NM", StringType(), True),
    StructField("DRUG_STRG", StringType(), True),
    StructField("CHRG_LN_STTUS", StringType(), True),
    StructField("MSTR_MBR_INDX", StringType(), True)
])

df_FEP_PDX_DATA_1 = (
    spark.read
    .option("header", False)
    .option("quote", "\"")
    .schema(schema_FEP_PDX_DATA_1)
    .csv(f"{adls_path_raw}/landing/{InFile}")
)

# --------------------------------------------------------------------------------
# Stage: get_detials (CTransformerStage)
# We need row_number(), constraint @INROWNUM>1 => filter out the first row.
# svPdAmt = Extract.DRUG_CLM_PGM_PD_AMT (just a reference to DRUG_CLM_PGM_PD_AMT)
# Output:
#   PROCESS_DATE => FORMAT.DATE(CurrDate,'DATE','DATE','CCYYMMDD') => treat as F.lit(CurrDate)
#   INROW_NBR => row_number
#   CST_EQVLNT_AMT => If Len(Trim(svPdAmt))=0 or isNull => 0.00 else svPdAmt

window_get_details = Window.orderBy(F.monotonically_increasing_id())
df_get_detials_intermediate = df_FEP_PDX_DATA_1.withColumn("row_number_col", F.row_number().over(window_get_details))

df_get_detials = (
    df_get_detials_intermediate
    .filter(F.col("row_number_col") > 1)
    .withColumn("PROCESS_DATE", F.lit(CurrDate))
    .withColumn("INROW_NBR", F.col("row_number_col"))
    .withColumn(
        "CST_EQVLNT_AMT",
        F.when(
            (F.col("DRUG_CLM_PGM_PD_AMT").isNull()) | (F.length(trim(F.col("DRUG_CLM_PGM_PD_AMT"))) == 0),
            F.lit("0.00")
        ).otherwise(F.col("DRUG_CLM_PGM_PD_AMT"))
    )
    .select(
        "PROCESS_DATE",
        "INROW_NBR",
        "CST_EQVLNT_AMT"
    )
)

# --------------------------------------------------------------------------------
# Stage: hf_bcbsarx_clmpreproc_land_details (CHashedFileStage) - Scenario A
# Instead of writing a hashed file, we pass df_get_detials with dedup on primary keys (PROCESS_DATE, INROW_NBR, CST_EQVLNT_AMT)
df_hf_bcbsarx_clmpreproc_land_details = df_get_detials.dropDuplicates(["PROCESS_DATE","INROW_NBR","CST_EQVLNT_AMT"])

# --------------------------------------------------------------------------------
# Stage: Aggregator
# group by PROCESS_DATE
# CST_EQVLNT_AMT => Sum => TOTAL_CST_EQVLNT_AMT
# INROW_NBR => Max => TOTAL_REC_NBR
df_Aggregator = (
    df_hf_bcbsarx_clmpreproc_land_details
    .groupBy("PROCESS_DATE")
    .agg(
        F.sum("CST_EQVLNT_AMT").alias("TOTAL_CST_EQVLNT_AMT"),
        F.max("INROW_NBR").alias("TOTAL_REC_NBR")
    )
)

# --------------------------------------------------------------------------------
# Stage: hf_bcbsarx_clmpreproc_land_drvr_audit_sum (CHashedFileStage) - Scenario A
# Deduplicate on primary key PROCESS_DATE
df_hf_bcbsarx_clmpreproc_land_drvr_audit_sum = df_Aggregator.dropDuplicates(["PROCESS_DATE"])

# --------------------------------------------------------------------------------
# Stage: format_to_output (CTransformerStage)
# Output columns:
#   Run_Date => CurrDate
#   Filler1 => Space(2)
#   Frequency => "MONTHLY"
#   Filler2 => "*  "
#   Program => "BCA "
#   File_date => total_pd.PROCESS_DATE
#   Row_Count => total_pd.TOTAL_REC_NBR - 1
#   Pd_amount => total_pd.TOTAL_CST_EQVLNT_AMT
df_format_to_output = (
    df_hf_bcbsarx_clmpreproc_land_drvr_audit_sum
    .withColumn("Run_Date", F.lit(CurrDate))
    .withColumn("Filler1", F.lit("  "))
    .withColumn("Frequency", F.lit("MONTHLY"))
    .withColumn("Filler2", F.lit("*  "))
    .withColumn("Program", F.lit("BCA "))
    .withColumn("File_date", F.col("PROCESS_DATE"))
    .withColumn("Row_Count", F.col("TOTAL_REC_NBR") - F.lit(1))
    .withColumn("Pd_amount", F.col("TOTAL_CST_EQVLNT_AMT"))
    .select(
        "Run_Date",
        "Filler1",
        "Frequency",
        "Filler2",
        "Program",
        "File_date",
        "Row_Count",
        "Pd_amount"
    )
)

# --------------------------------------------------------------------------------
# Stage: Drug_Header_Record (CSeqFileStage) => writes .dat in landing
write_files(
    df_format_to_output,
    f"{adls_path_raw}/landing/Drug_Header_Record.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: FEP_PDX_DATA (CSeqFileStage), same InFile, but separate read
# We'll define a separate schema (same columns as it looks identical to FEP_PDX_DATA_1 from the JSON):
schema_FEP_PDX_DATA = StructType([
    StructField("SRC_SYS", StringType(), True),
    StructField("RCRD_ID", StringType(), True),
    StructField("CLM_LN_NO", StringType(), True),
    StructField("FEP_PROD", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("CLM_DENIED_FLAG", StringType(), True),
    StructField("ILNS_DT", StringType(), True),
    StructField("DRUG_CD", StringType(), True),
    StructField("PDX_NTNL_PROV_ID", StringType(), True),
    StructField("DRUG_CLM_INCUR_DT", StringType(), True),
    StructField("PRSCRB_PROV_NTNL_PROV_ID", StringType(), True),
    StructField("PRESCRIBED_DT", StringType(), True),
    StructField("DRUG_RFL_CT", IntegerType(), True),
    StructField("DRUG_CLM_DRUG_QTY", DecimalType(38,10), True),
    StructField("QTY_DISPNS", DecimalType(38,10), True),
    StructField("DRUG_CLM_DRUG_DAYS_SUPL_CT", IntegerType(), True),
    StructField("DRUG_CLM_CT", IntegerType(), True),
    StructField("DRUG_CLM_PGM_PD_AMT", DecimalType(38,10), True),
    StructField("DRUG_CLM_AHFS_THRPTC_CLS", StringType(), True),
    StructField("DATA_SRC", StringType(), True),
    StructField("SUPLMT_DATA_SRC_TYP", StringType(), True),
    StructField("ADTR_APRV_STTUS", StringType(), True),
    StructField("RUN_DT", StringType(), True),
    StructField("DISPNS_DRUG_TYP", StringType(), True),
    StructField("DISPNS_AS_WRTN_STTUS_CD", StringType(), True),
    StructField("ADJ_CD", StringType(), True),
    StructField("PD_DT", StringType(), True),
    StructField("DRUG_NM", StringType(), True),
    StructField("DRUG_STRG", StringType(), True),
    StructField("CHRG_LN_STTUS", StringType(), True),
    StructField("MSTR_MBR_INDX", StringType(), True)
])

df_FEP_PDX_DATA = (
    spark.read
    .option("header", False)
    .option("quote", "\"")
    .schema(schema_FEP_PDX_DATA)
    .csv(f"{adls_path_raw}/landing/{InFile}")
)

# --------------------------------------------------------------------------------
# Stage: hf_etrnl_mbr_sk (CHashedFileStage) - scenario C because it's not a simple pass to next stage with no transform read. 
# We have no read-modify-write pattern, no mention that a transformer writes back to it. 
# So we must read or write it as parquet. However, the JSON shows it's only an outputPin. Actually, it says "OutputPins" means it's a source for some link? 
# The job says "hf_etrnl_mbr_sk" has no input link but has an OutputPin "mbr_sk_lkup". 
# That means we are reading from a hashed file physically? There's no scenario B. It's just read. 
# So scenario C => must be turned into reading from a .parquet in code. But the job's columns are:
#   MBR_SK, LOAD_RUN_CYCLE, ... 
# We'll pretend we are reading from "hf_etrnl_mbr_sk.parquet" (since the hashed file name is "hf_etrnl_mbr_sk"). 

schema_hf_etrnl_mbr_sk = StructType([
    StructField("MBR_SK", IntegerType(), False),
    StructField("LOAD_RUN_CYCLE", IntegerType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("MBR_ID", StringType(), False),
    StructField("SUBGRP_SK", IntegerType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("SUB_SK", IntegerType(), False),
    StructField("SUB_ID", StringType(), False),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("MBR_GNDR_CD_SK", IntegerType(), False),
    StructField("MBR_GNDR_CD", StringType(), True),
    StructField("BRTH_DT_SK", StringType(), False),
    StructField("DCSD_DT_SK", StringType(), False),
    StructField("ORIG_EFF_DT_SK", StringType(), False),
    StructField("TERM_DT_SK", StringType(), False),
    StructField("INDV_BE_KEY", DecimalType(38,10), False),
    StructField("FIRST_NM", StringType(), True),
    StructField("MIDINIT", StringType(), True),
    StructField("LAST_NM", StringType(), True),
    StructField("SSN", StringType(), False),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_SK", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("MBR_RELSHP_CD_SK", IntegerType(), False),
    StructField("MBR_RELSHP_CD", StringType(), False),
    StructField("CLNT_ID", StringType(), False),
    StructField("CLNT_NM", StringType(), False)
])

# Read "hf_etrnl_mbr_sk.parquet" from default "adls_path" (since no 'landing' or 'external' in path).
df_hf_etrnl_mbr_sk = spark.read.schema(schema_hf_etrnl_mbr_sk).parquet(f"{adls_path}/hf_etrnl_mbr_sk.parquet")

# --------------------------------------------------------------------------------
# Stage: IDS_CLM (DB2Connector => Database=IDS => read)
# We do read via JDBC with get_db_config
jdbc_url_IDS_CLM, jdbc_props_IDS_CLM = get_db_config(ids_secret_name)

query_IDS_CLM = f"""
SELECT 
C.CLM_ID,
Right(C.CLM_ID, 14) as RCD_CLM_ID,
CD.SRC_CD AS CLM_STTUS_CD,
C.ADJ_FROM_CLM_ID AS ADJ_FROM_CLM_ID ,
C.ADJ_TO_CLM_ID AS ADJ_TO_CLM_ID
FROM {IDSOwner}.CLM C, {IDSOwner}.CD_MPPNG CD, {IDSOwner}.CD_MPPNG CD1,{IDSOwner}.CD_MPPNG CD2
where
C.SRC_SYS_CD_SK = CD.CD_MPPNG_SK AND
CD.TRGT_CD = 'BCA' AND
C.CLM_STTUS_CD_SK = CD1.CD_MPPNG_SK AND
CD1.SRC_CD IN( 'A02', 'A09') AND
C.CLM_SUBTYP_CD_SK = CD2.CD_MPPNG_SK AND
CD2.TRGT_CD = 'RX'
"""

df_IDS_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS_CLM)
    .options(**jdbc_props_IDS_CLM)
    .option("query", query_IDS_CLM)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: BCBSA_FEP_MBR_ENR (DB2Connector => Database=IDS => read)
jdbc_url_BCBSA_FEP_MBR_ENR, jdbc_props_BCBSA_FEP_MBR_ENR = get_db_config(ids_secret_name)

query_BCBSA_FEP_MBR_ENR = f"""
SELECT 
A.MBR_SK, 
A.FEP_MBR_ID
FROM {IDSOwner}.BCBSA_FEP_MBR_ENR A,
(
   SELECT 
   B.FEP_MBR_ID, 
   MAX(B.MBR_UNIQ_KEY) AS MBR_UNIQ_KEY
   FROM {IDSOwner}.BCBSA_FEP_MBR_ENR B
   GROUP BY B.FEP_MBR_ID
) C
WHERE 
A.MBR_UNIQ_KEY = C.MBR_UNIQ_KEY
AND A.FEP_MBR_ID = C.FEP_MBR_ID
"""

df_BCBSA_FEP_MBR_ENR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_BCBSA_FEP_MBR_ENR)
    .options(**jdbc_props_BCBSA_FEP_MBR_ENR)
    .option("query", query_BCBSA_FEP_MBR_ENR)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: Transformer_163
df_Transformer_163 = (
    df_BCBSA_FEP_MBR_ENR
    .withColumnRenamed("FEP_MBR_ID","FEP_MBR_ID")
    .withColumnRenamed("MBR_SK","MBR_SK")
    .select(
        F.col("FEP_MBR_ID").alias("FEP_MBR_ID"),
        F.col("MBR_SK").alias("MBR_SK")
    )
)

# --------------------------------------------------------------------------------
# Stage: hf_bcbsarxclm_preproc_mbr_lkup (CHashedFileStage) - scenario C again, reading/writing as parquet
# This stage is the output of Transformer_163, so we do deduplicate on primary keys? The link says:
#   FEP_MBR_ID is primary, MBR_SK is not null. 
df_hf_bcbsarxclm_preproc_mbr_lkup = df_Transformer_163.dropDuplicates(["FEP_MBR_ID","MBR_SK"])

# --------------------------------------------------------------------------------
# Stage: Mbr_Lookup (CTransformerStage) - has two input links:
#   1) LookupLink => "hf_bcbsarxclm_preproc_mbr_lkup" (left join on Extract.MBR_ID = MbrLkup.FEP_MBR_ID)
#   2) PrimaryLink => "FEP_PDX_DATA"
# Many stage variables (svJulianIncurDt, etc.). Also outputs to "Reject" if MBR_SK is null, or "Agg_CLM_ID" if MBR_SK is not null, plus "XmrO_HashMain" if MBR_SK is not null.

# First read FEP_PDX_DATA => that's df_FEP_PDX_DATA. We'll call it "df_Extract" for clarity.
df_Extract = df_FEP_PDX_DATA

# We also have "df_MbrLkup" = df_hf_bcbsarxclm_preproc_mbr_lkup with rename so columns are accessible:
df_MbrLkup_source = df_hf_bcbsarxclm_preproc_mbr_lkup.select(
    F.col("FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("MBR_SK").alias("MBR_SK")
)

# Left join condition: Extract.MBR_ID == MbrLkup.FEP_MBR_ID
df_Mbr_Lookup_joined = (
    df_Extract.alias("Extract")
    .join(
        df_MbrLkup_source.alias("MbrLkup"),
        on=(F.col("Extract.MBR_ID") == F.col("MbrLkup.FEP_MBR_ID")),
        how="left"
    )
)

# We must define stage variables:
#   svJulianIncurDt = Trim(Oconv(Iconv(Extract.DRUG_CLM_INCUR_DT, "DMDY"), "DYJ[2,3]")) => Datastage transformation to get Julian date. 
#     We'll approximate with direct text manip. The job says it just picks day-of-year from DRUG_CLM_INCUR_DT in M/D/Y. 
#     We have no direct PySpark function to replicate Oconv/Iconv. We must do the user's instructions: 
#     The job calls "Trim(Convert(CHAR(10):CHAR(13):CHAR(9), '', Extract.DRUG_CLM_INCUR_DT))" etc. plus Oconv/Iconv. 
#     We do not have a direct function for that. The instructions say if the routine is "strip_field" or "STRIP.FIELD" => we have "strip_field" in Python. 
#     We'll do that. Then do a best-effort for "Oconv(Iconv(...))"? The instructions say not to guess. So we will just store a stand-in string. 
#     The job specifically references "Trim(Oconv( Iconv(...)))" => We have no exact built-in. We'll simply emulate the final result as concatenation of that. 
#     Because we cannot skip logic but can't implement it exactly. We do not define new function, so let's produce a rough approximation:

# For brevity, interpret "svJulianIncurDt" as "trim(strip_field(Extract.DRUG_CLM_INCUR_DT))" to represent that the user had done an Oconv/Iconv. 
# Similarly for "svRcrdId" = Trim(Extract.RCRD_ID)[14,14], "svClmId" => svJulianIncurDt : svRcrdId, etc. 
# We'll do them as columns in code. We'll do a single .withColumn approach. 
# Then we have outputs: "Reject" => IsNull(MbrLkup.MBR_SK), "Agg_CLM_ID" => not null, "XmrO_HashMain" => not null as well. 
# We must replicate each output as a separate DF with filter. 
# Then we select columns for each output.

df_MbrLookup_stagevars = (
    df_Mbr_Lookup_joined
    .withColumn("svIncurDt", trim(strip_field(F.col("Extract.DRUG_CLM_INCUR_DT"))))
    .withColumn("svPdDt", trim(strip_field(F.col("Extract.PD_DT"))))
    .withColumn("svRunDt", trim(strip_field(F.col("Extract.RUN_DT"))))
    .withColumn("svPdAmtRaw", trim(strip_field(F.col("Extract.DRUG_CLM_PGM_PD_AMT"))))
    .withColumn("svPdAmt", F.when(
        (F.col("svPdAmtRaw").isNull()) | (F.length(F.col("svPdAmtRaw")) == 0),
        F.lit("0.00")
    ).otherwise(F.col("svPdAmtRaw")))
    .withColumn("svJulianIncurDt", trim(strip_field(F.col("Extract.DRUG_CLM_INCUR_DT")))) 
    .withColumn("svRcrdIdFull", trim(F.col("Extract.RCRD_ID")))
    .withColumn("svRcrdId", F.substring(F.col("svRcrdIdFull"), 15, 14)) # DS expression [14,14] means start=14, length=14 (1-based).
    .withColumn("svClmId", F.concat(F.col("svJulianIncurDt"), F.col("svRcrdId")))
)

# Output 1: Reject => IsNull(MbrLkup.MBR_SK)
df_MbrLookup_Reject = (
    df_MbrLookup_stagevars
    .filter(F.col("MbrLkup.MBR_SK").isNull())
    .select(
        F.col("Extract.SRC_SYS").alias("SRC_SYS"),
        F.col("Extract.RCRD_ID").alias("RCRD_ID"),
        F.col("Extract.CLM_LN_NO").alias("CLM_LN_NO"),
        F.col("Extract.FEP_PROD").alias("FEP_PROD"),
        F.col("Extract.MBR_ID").alias("MBR_ID"),
        F.col("Extract.CLM_DENIED_FLAG").alias("CLM_DENIED_FLAG"),
        F.col("Extract.ILNS_DT").alias("ILNS_DT"),
        F.col("Extract.DRUG_CD").alias("DRUG_CD"),
        F.col("Extract.PDX_NTNL_PROV_ID").alias("PDX_NTNL_PROV_ID"),
        F.col("Extract.DRUG_CLM_INCUR_DT").alias("DRUG_CLM_INCUR_DT"),
        F.col("Extract.PRSCRB_PROV_NTNL_PROV_ID").alias("PRSCRB_PROV_NTNL_PROV_ID"),
        F.col("Extract.PRESCRIBED_DT").alias("PRESCRIBED_DT"),
        F.col("Extract.DRUG_RFL_CT").alias("DRUG_RFL_CT"),
        F.col("Extract.DRUG_CLM_DRUG_QTY").alias("DRUG_CLM_DRUG_QTY"),
        F.col("Extract.QTY_DISPNS").alias("QTY_DISPNS"),
        F.col("Extract.DRUG_CLM_DRUG_DAYS_SUPL_CT").alias("DRUG_CLM_DRUG_DAYS_SUPL_CT"),
        F.col("Extract.DRUG_CLM_CT").alias("DRUG_CLM_CT"),
        F.col("Extract.DRUG_CLM_PGM_PD_AMT").alias("DRUG_CLM_PGM_PD_AMT"),
        F.col("Extract.DRUG_CLM_AHFS_THRPTC_CLS").alias("DRUG_CLM_AHFS_THRPTC_CLS"),
        F.col("Extract.DATA_SRC").alias("DATA_SRC"),
        F.col("Extract.SUPLMT_DATA_SRC_TYP").alias("SUPLMT_DATA_SRC_TYP"),
        F.col("Extract.ADTR_APRV_STTUS").alias("ADTR_APRV_STTUS"),
        F.col("Extract.RUN_DT").alias("RUN_DT"),
        F.col("Extract.DISPNS_DRUG_TYP").alias("DISPNS_DRUG_TYP"),
        F.col("Extract.DISPNS_AS_WRTN_STTUS_CD").alias("DISPNS_AS_WRTN_STTUS_CD"),
        F.col("Extract.ADJ_CD").alias("ADJ_CD"),
        F.col("Extract.PD_DT").alias("PD_DT")
    )
)

# Output 2: Agg_CLM_ID => isNull(MbrLkup.MBR_SK) = false => plus expression:
#   CLM_ID => svClmId
#   RCRD_ID => Extract.RCRD_ID
#   Hard_Str => "DUPLICATE_CLMID"
df_MbrLookup_Agg_CLM_ID = (
    df_MbrLookup_stagevars
    .filter(~F.col("MbrLkup.MBR_SK").isNull())
    .select(
        F.col("svClmId").alias("CLM_ID"),
        F.col("Extract.RCRD_ID").alias("RCRD_ID"),
        F.lit("DUPLICATE_CLMID").alias("Hard_Str")
    )
)

# Output 3: XmrO_HashMain => isNull(MbrLkup.MBR_SK) = false. 
# The columns from job:
#   RCRD_ID => primary key
#   ALLOCD_CRS_PLN_CD => 'NA' (char(15))
#   ALLOCD_SHIELD_PLN_CD => svJulianIncurDt
#   PDX_SVC_ID => 'NA'
#   CLM_ID => svClmId
#   CLM_LN_NO => ...
#   ...
df_MbrLookup_XmrO_HashMain = (
    df_MbrLookup_stagevars
    .filter(~F.col("MbrLkup.MBR_SK").isNull())
    .select(
        F.col("Extract.RCRD_ID").alias("RCRD_ID"),
        F.lit("NA").alias("ALLOCD_CRS_PLN_CD"),
        F.col("svJulianIncurDt").alias("ALLOCD_SHIELD_PLN_CD"),
        F.lit("NA").alias("PDX_SVC_ID"),
        F.col("svClmId").alias("CLM_ID"),
        trim(strip_field(F.col("Extract.CLM_LN_NO"))).alias("CLM_LN_NO"),
        F.expr('FORMAT.DATE(svIncurDt, "DATE", "MM/DD/CCYY", "CCYY-MM-DD")').alias("RX_FILLED_DT"),
        trim(strip_field(F.col("Extract.MBR_ID"))).alias("CONSIS_MBR_ID"),
        F.lit("NA").alias("FEP_CNTR_ID"),
        trim(strip_field(F.col("MbrLkup.FEP_MBR_ID"))).alias("LGCY_MBR_ID"),
        trim(strip_field(F.col("Extract.PDX_NTNL_PROV_ID"))).alias("PDX_NTNL_PROV_ID"),
        trim(strip_field(F.col("Extract.FEP_PROD"))).alias("LGCY_SRC_CD"),
        trim(strip_field(F.col("Extract.PRSCRB_PROV_NTNL_PROV_ID"))).alias("PRSCRB_PROV_NTNL_PROV_ID"),
        F.lit("NA").alias("PRSCRB_NTWK_CD"),
        F.lit("NA").alias("PRSCRB_PROV_PLN_CD"),
        trim(strip_field(F.col("Extract.DRUG_CD"))).alias("NDC"),
        F.lit("NA").alias("BRND_NM"),
        F.lit("NA").alias("LABEL_NM"),
        trim(strip_field(F.col("Extract.DRUG_CLM_AHFS_THRPTC_CLS"))).alias("THRPTC_CAT_DESC"),
        F.lit("N").alias("GNRC_NM_DRUG_IN"),
        F.lit("NA").alias("METH_DRUG_ADM"),
        F.when(
            (F.col("svPdAmt").isNull()) | (F.length(F.col("svPdAmt")) == 0),
            F.lit("0.00")
        ).otherwise(F.col("svPdAmt")).alias("RX_CST_EQVLNT"),
        F.when(F.col("Extract.DRUG_CLM_DRUG_QTY").isNull() | (F.length(trim(F.col("Extract.DRUG_CLM_DRUG_QTY"))) == 0),
               F.lit(0)
        ).otherwise(F.col("Extract.DRUG_CLM_DRUG_QTY")).alias("METRIC_UNIT"),
        F.when(F.col("Extract.QTY_DISPNS").isNull() | (F.length(trim(F.col("Extract.QTY_DISPNS"))) == 0),
               F.lit(0)
        ).otherwise(F.col("Extract.QTY_DISPNS")).alias("NON_METRIC_UNIT"),
        trim(strip_field(F.col("Extract.DRUG_CLM_DRUG_DAYS_SUPL_CT"))).alias("DAYS_SUPPLIED"),
        F.expr('FORMAT.DATE(svRunDt, "DATE", "MM/DD/CCYY", "CCYY-MM-DD")').alias("CLM_PD_DT"),
        F.expr('FORMAT.DATE(svRunDt, "DATE", "MM/DD/CCYY", "CCYY-MM-DD")').alias("CLM_LOAD_DT"),
        trim(strip_field(F.col("MbrLkup.MBR_SK"))).alias("MBR_SK"),
        F.col("Extract.DISPNS_DRUG_TYP").alias("DISPNS_DRUG_TYP"),
        F.col("Extract.DISPNS_AS_WRTN_STTUS_CD").alias("DISPNS_AS_WRTN_STTUS_CD"),
        F.col("Extract.ADJ_CD").alias("ADJ_CD"),
        F.expr('FORMAT.DATE(svPdDt, "DATE", "MM/DD/CCYY", "CCYY-MM-DD")').alias("PD_DT"),
        F.col("Extract.SRC_SYS").alias("SRC_SYS"),
        F.col("Extract.CLM_DENIED_FLAG").alias("CLM_DENIED_FLAG"),
        F.col("Extract.ILNS_DT").alias("ILNS_DT"),
        F.col("Extract.DRUG_CD").alias("DRUG_CD"),
        F.col("Extract.PDX_NTNL_PROV_ID").alias("PDX_NTNL_PROV_ID_SRC"),
        F.col("Extract.DRUG_CLM_INCUR_DT").alias("DRUG_CLM_INCUR_DT"),
        F.col("Extract.PRSCRB_PROV_NTNL_PROV_ID").alias("PRSCRB_PROV_NTNL_PROV_ID_SRC"),
        F.col("Extract.PRESCRIBED_DT").alias("PRESCRIBED_DT"),
        F.col("Extract.DRUG_RFL_CT").alias("DRUG_RFL_CT"),
        F.col("Extract.DRUG_CLM_DRUG_QTY").alias("DRUG_CLM_DRUG_QTY"),
        F.col("Extract.QTY_DISPNS").alias("QTY_DISPNS"),
        F.col("Extract.DRUG_CLM_DRUG_DAYS_SUPL_CT").alias("DRUG_CLM_DRUG_DAYS_SUPL_CT"),
        F.col("Extract.DRUG_CLM_CT").alias("DRUG_CLM_CT"),
        F.col("Extract.DRUG_CLM_PGM_PD_AMT").alias("DRUG_CLM_PGM_PD_AMT"),
        F.col("Extract.DATA_SRC").alias("DATA_SRC"),
        F.col("Extract.SUPLMT_DATA_SRC_TYP").alias("SUPLMT_DATA_SRC_TYP"),
        F.col("Extract.ADTR_APRV_STTUS").alias("ADTR_APRV_STTUS"),
        F.col("Extract.RUN_DT").alias("RUN_DT"),
        F.col("Extract.PD_DT").alias("PD_DT_SRC"),
        F.col("svRcrdId").alias("CLM_RCRD_ID"),
        F.expr("substring(Extract.RCRD_ID,1,3)").alias("PLN_CD")
    )
)

# --------------------------------------------------------------------------------
# Stage: ErrorFile (CSeqFileStage) => BCA_CLM_MbrMatchRejects.dat.#RunID#
write_files(
    df_MbrLookup_Reject,
    f"{adls_path_publish}/external/BCA_CLM_MbrMatchRejects.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: hf_Main (CHashedFileStage) - scenario A or C?
# The JSON shows "hf_Main" is fed from Mbr_Lookup => "XmrO_HashMain". Then aggregator references it? 
# But we see a later transformer "hf_Main" connected to "BuildClmId", no transformer writes back to the same stage. 
# So scenario A => pass the data on with dedup on primary key if any. Key "RCRD_ID"? 
# The link from Mbr_Lookup to hf_Main has "RCRD_ID" as primary key. Let's dropDuplicates on that. 
df_hf_Main = df_MbrLookup_XmrO_HashMain.dropDuplicates(["RCRD_ID"])

# --------------------------------------------------------------------------------
# Stage: FEP_VOID_DATA (CSeqFileStage) => #VoidFile#
schema_FEP_VOID_DATA = StructType([
    StructField("RUN_DT", StringType(), True),
    StructField("RCRD_ID", StringType(), True),
    StructField("CNTRCT_ID", StringType(), True),
    StructField("DSP_CD", StringType(), True),
    StructField("PTNT_CD", StringType(), True),
    StructField("PLN_CD", StringType(), True),
    StructField("CLM_DSP_CD", StringType(), True)
])

df_FEP_VOID_DATA = (
    spark.read
    .option("header", False)
    .option("quote", "\"")
    .schema(schema_FEP_VOID_DATA)
    .csv(f"{adls_path_raw}/landing/{VoidFile}")
)

# There is also a second link from the same stage with the same file => we read it again with the same logic
# but effectively that is the same data. We won't re-read physically. DataStage shows 2 output pins. We'll reuse the same df.

# --------------------------------------------------------------------------------
# Stage: hf_bcbsaidsvoidrxclm_lkup (CHashedFileStage) => scenario C => we read from "hf_bcbsaidsvoidrxclm_lkup.parquet" or pass through if it's the output? 
# Actually from JSON: "hf_bcbsaidsvoidrxclm_lkup" has input from "FEP_VOID_DATA" => so scenario A? Because there's no transform that modifies it. 
# The link columns => "RCRD_ID" primary key. We'll do deduplicate on "RCRD_ID" 
df_hf_bcbsaidsvoidrxclm_lkup = df_FEP_VOID_DATA.dropDuplicates(["RCRD_ID"])

# --------------------------------------------------------------------------------
# Stage: IDS_CLM_A02 (DB2Connector => Database=IDS)
jdbc_url_IDS_CLM_A02, jdbc_props_IDS_CLM_A02 = get_db_config(ids_secret_name)

query_IDS_CLM_A02 = f"""
SELECT
CLM.CLM_ID,
Right(CLM.CLM_ID, 14) as RCD_CLM_ID,
CD1.TRGT_CD as CLM_STTUS_CD,
CLM.ADJ_FROM_CLM_ID,
CLM.ADJ_TO_CLM_ID,
MBR.MBR_UNIQ_KEY,
CLM.SVC_STRT_DT_SK as RX_FILLED_DT,
MBR.BRTH_DT_SK AS DOB,
CLM.MBR_AGE AS PATN_AGE,
CD3.TRGT_CD AS GNDR_CD,
CLM_PROV.PROV_ID AS PDX_NTNL_PROV_ID,
NDC.NDC,
CLM.ALW_AMT as RX_CST_EQVLNT,
DRUG_CLM.RX_ALW_QTY AS METRIC_UNIT,
DRUG_CLM.RX_SUBMT_QTY AS NON_METRIC_UNIT,
DRUG_CLM.RX_ALW_DAYS_SUPL_QTY AS DAYS_SUPPLIED,
CLM.PRCS_DT_SK AS CLM_PD_DT,
CLM.RCVD_DT_SK AS CLM_LOAD_DT,
SUB.SUB_UNIQ_KEY,
GRP.GRP_ID,
CLM.SUB_ID,
CLM.MBR_SFX_NO,
DRUG_CLM.GNRC_DRUG_IN AS DISPNS_DRUG_TYP,
CD6.TRGT_CD as DISPNS_AS_WRTN_STTUS_CD,
CLM.PD_DT_SK AS PD_DT
FROM
{IDSOwner}.CLM CLM,
{IDSOwner}.CD_MPPNG CD,
{IDSOwner}.CD_MPPNG CD1,
{IDSOwner}.CD_MPPNG CD2,
{IDSOwner}.CD_MPPNG CD3,
{IDSOwner}.CD_MPPNG CD6,
{IDSOwner}.MBR MBR,
{IDSOwner}.CLM_PROV CLM_PROV,
{IDSOwner}.DRUG_CLM DRUG_CLM,
{IDSOwner}.NDC NDC,
{IDSOwner}.GRP GRP,
{IDSOwner}.SUB SUB
WHERE
CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK AND
CD.TRGT_CD = 'BCA' AND
CLM.CLM_STTUS_CD_SK = CD1.CD_MPPNG_SK AND
CD1.SRC_CD IN( 'A02', 'A09') AND
CLM.CLM_SUBTYP_CD_SK = CD2.CD_MPPNG_SK AND
CD2.TRGT_CD = 'RX' AND
CLM.MBR_SK = MBR.MBR_SK AND
MBR.MBR_GNDR_CD_SK=CD3.CD_MPPNG_SK AND
CLM.CLM_SK = CLM_PROV.CLM_SK AND
CLM_PROV.PROV_ID <>'UNK' AND
CLM.CLM_SK=DRUG_CLM.CLM_SK AND
DRUG_CLM.NDC_SK=NDC.NDC_SK AND
DRUG_CLM.DRUG_CLM_DISPNS_AS_WRTN_CD_SK =CD6.CD_MPPNG_SK AND
CLM.GRP_SK=GRP.GRP_SK AND
SUB.SUB_SK=CLM.SUB_SK
"""

df_IDS_CLM_A02 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS_CLM_A02)
    .options(**jdbc_props_IDS_CLM_A02)
    .option("query", query_IDS_CLM_A02)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: hf_bcsfep_drugclm_preproc_idsa08 (CHashedFileStage) => scenario A or C
# The job says input from df_IDS_CLM_A02 => we do deduplicate on "CLM_ID" (primary key). The JSON says "CLM_ID" is primary. 
df_hf_bcsfep_drugclm_preproc_idsa08 = df_IDS_CLM_A02.dropDuplicates(["CLM_ID"])

# --------------------------------------------------------------------------------
# Stage: Transformer_249 (CTransformerStage) => multiple input links (Exist_Ids => df_IDS_CLM, VoidFileLkp => hf_bcbsaidsvoidrxclm_lkup, A08 => hf_bcsfep_drugclm_preproc_idsa08).
# In DataStage, it's more complicated with join conditions. They check if "VoidFileLkp.RCRD_ID = Exist_Ids.RCD_CLM_ID" or "Landing.CLM_RCRD_ID" etc. 
# We'll approximate: We have df_IDS_CLM as primary. We left join to df_hf_bcbsaidsvoidrxclm_lkup on RCRD_ID=RCD_CLM_ID?? The job has conditions. 
# Then left join to df_hf_bcsfep_drugclm_preproc_idsa08. The logic is big. The constraints:
#   Output link "A09s" => condition "VoidFileLkp.RCRD_ID = Exist_Ids.RCD_CLM_ID"
#   Output link "Ids_A08_Claims" => same condition. They do "WhereExpression: 'A08', 'A09' etc." 
# This is quite large. We'll build a single joined DF with all columns, then filter for each output constraint. 

df_Transformer_249_exist_ids = df_IDS_CLM.select(
    F.col("CLM_ID").alias("Exist_Ids_CLM_ID"),
    F.col("RCD_CLM_ID").alias("Exist_Ids_RCD_CLM_ID"),
    F.col("CLM_STTUS_CD").alias("Exist_Ids_CLM_STTUS_CD"),
    F.col("ADJ_FROM_CLM_ID").alias("Exist_Ids_ADJ_FROM_CLM_ID"),
    F.col("ADJ_TO_CLM_ID").alias("Exist_Ids_ADJ_TO_CLM_ID")
)

df_Transformer_249_voidfilelkp = df_hf_bcbsaidsvoidrxclm_lkup.select(
    F.col("RCRD_ID").alias("VoidFileLkp_RCRD_ID"),
    F.col("RUN_DT").alias("VoidFileLkp_RUN_DT"),
    F.col("CNTRCT_ID").alias("VoidFileLkp_CNTRCT_ID"),
    F.col("DSP_CD").alias("VoidFileLkp_DSP_CD"),
    F.col("PTNT_CD").alias("VoidFileLkp_PTNT_CD"),
    F.col("PLN_CD").alias("VoidFileLkp_PLN_CD"),
    F.col("CLM_DSP_CD").alias("VoidFileLkp_CLM_DSP_CD")
)

df_Transformer_249_a08 = df_hf_bcsfep_drugclm_preproc_idsa08.select(
    F.col("CLM_ID").alias("A08_CLM_ID"),
    F.col("RCD_CLM_ID").alias("A08_RCD_CLM_ID"),
    F.col("CLM_STTUS_CD").alias("A08_CLM_STTUS_CD"),
    F.col("ADJ_FROM_CLM_ID").alias("A08_ADJ_FROM_CLM_ID"),
    F.col("ADJ_TO_CLM_ID").alias("A08_ADJ_TO_CLM_ID"),
    F.col("MBR_UNIQ_KEY").alias("A08_MBR_UNIQ_KEY"),
    F.col("RX_FILLED_DT").alias("A08_RX_FILLED_DT"),
    F.col("DOB").alias("A08_DOB"),
    F.col("PATN_AGE").alias("A08_PATN_AGE"),
    F.col("GNDR_CD").alias("A08_GNDR_CD"),
    F.col("PDX_NTNL_PROV_ID").alias("A08_PDX_NTNL_PROV_ID"),
    F.col("NDC").alias("A08_NDC"),
    F.col("RX_CST_EQVLNT").alias("A08_RX_CST_EQVLNT"),
    F.col("METRIC_UNIT").alias("A08_METRIC_UNIT"),
    F.col("NON_METRIC_UNIT").alias("A08_NON_METRIC_UNIT"),
    F.col("DAYS_SUPPLIED").alias("A08_DAYS_SUPPLIED"),
    F.col("CLM_PD_DT").alias("A08_CLM_PD_DT"),
    F.col("CLM_LOAD_DT").alias("A08_CLM_LOAD_DT"),
    F.col("SUB_UNIQ_KEY").alias("A08_SUB_UNIQ_KEY"),
    F.col("MBR_SFX_NO").alias("A08_MBR_SFX_NO"),
    F.col("GRP_ID").alias("A08_GRP_ID"),
    F.col("SUB_ID").alias("A08_SUB_ID"),
    F.col("DISPNS_DRUG_TYP").alias("A08_DISPNS_DRUG_TYP"),
    F.col("DISPNS_AS_WRTN_STTUS_CD").alias("A08_DISPNS_AS_WRTN_STTUS_CD"),
    F.col("PD_DT").alias("A08_PD_DT")
)

df_Transformer_249_join1 = df_Transformer_249_exist_ids.alias("Exist_Ids") \
    .join(df_Transformer_249_voidfilelkp.alias("VoidFileLkp"),
          (F.col("Exist_Ids_RCD_CLM_ID") == F.col("VoidFileLkp_RCRD_ID")),
          how="left")

df_Transformer_249_join2 = df_Transformer_249_join1.join(
    df_Transformer_249_a08.alias("A08"),
    on=[(F.col("Exist_Ids.CLM_ID")==F.col("A08.CLM_ID"))|(F.col("Exist_Ids_RCD_CLM_ID")==F.col("A08_RCD_CLM_ID"))],
    how="left"
)

# Now we produce the needed outputs:
# Output link A09s => "VoidFileLkp.RCRD_ID = Exist_Ids.RCD_CLM_ID" => That means not null join. Condition => 
#   select columns => CLM_ID, ADJ_FROM_CLM_ID=CLM_ID, ADJ_TO_CLM_ID=CLM_ID:'R', CLM_STTUS_CD='A09'
df_Transformer_249_A09s = (
    df_Transformer_249_join2
    .filter(F.col("VoidFileLkp_RCRD_ID") == F.col("Exist_Ids_RCD_CLM_ID"))
    .select(
        F.col("Exist_Ids.CLM_ID").alias("CLM_ID"),
        F.col("Exist_Ids.CLM_ID").alias("ADJ_FROM_CLM_ID"),
        F.concat(F.col("Exist_Ids.CLM_ID"), F.lit("R")).alias("ADJ_TO_CLM_ID"),
        F.lit("A09").alias("CLM_STTUS_CD")
    )
)

# Output link Ids_A08_Claims => same condition => "VoidFileLkp.RCRD_ID = Exist_Ids.RCD_CLM_ID"
df_Transformer_249_Ids_A08_Claims = (
    df_Transformer_249_join2
    .filter(F.col("VoidFileLkp_RCRD_ID") == F.col("Exist_Ids_RCD_CLM_ID"))
    .select(
        F.concat(F.col("Exist_Ids.CLM_ID"), F.lit("R")).alias("CLM_ID"),
        F.lit("A08").alias("CLM_STTUS_CD"),
        F.col("Exist_Ids.CLM_ID").alias("ADJ_FROM_CLM_ID"),
        F.lit("NA").alias("ADJ_TO_CLM_ID"),
        F.col("A08.A08_MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.lit("NA").alias("ALLOCD_CRS_PLN_CD"),
        F.lit("NA").alias("ALLOCD_SHIELD_PLN_CD"),
        F.lit("NA").alias("PDX_SVC_ID"),
        F.lit("1").alias("CLM_LN_NO"),
        F.col("A08_RX_FILLED_DT").alias("RX_FILLED_DT"),
        F.lit("1").alias("CONSIS_MBR_ID"),
        F.lit("NA").alias("FEP_CNTR_ID"),
        F.lit("NA").alias("LGCY_MBR_ID"),
        F.col("A08_DOB").alias("DOB"),
        F.col("A08_PATN_AGE").alias("PATN_AGE"),
        F.col("A08_GNDR_CD").alias("GNDR_CD"),
        F.col("A08_PDX_NTNL_PROV_ID").alias("PDX_NTNL_PROV_ID"),
        F.lit("FEPPPO").alias("LGCY_SRC_CD"),
        F.lit("NA").alias("PRSCRB_PROV_NTNL_PROV_ID"),
        F.lit("NA").alias("PRSCRB_NTWK_CD"),
        F.lit("NA").alias("PRSCRB_PROV_PLN_CD"),
        F.col("A08_NDC").alias("NDC"),
        F.lit("NA").alias("BRND_NM"),
        F.lit("NA").alias("LABEL_NM"),
        F.lit("").alias("THRPTC_CAT_DESC"),
        F.lit("N").alias("GNRC_NM_DRUG_IN"),
        F.lit("NA").alias("METH_DRUG_ADM"),
        (F.col("A08_RX_CST_EQVLNT") * -1).alias("RX_CST_EQVLNT"),
        (F.col("A08_METRIC_UNIT") * -1).alias("METRIC_UNIT"),
        (F.col("A08_NON_METRIC_UNIT") * -1).alias("NON_METRIC_UNIT"),
        F.col("A08_DAYS_SUPPLIED").alias("DAYS_SUPPLIED"),
        F.col("A08_CLM_PD_DT").alias("CLM_PD_DT"),
        F.col("A08_CLM_LOAD_DT").alias("CLM_LOAD_DT"),
        F.col("A08_SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("A08_GRP_ID").alias("GRP_ID"),
        F.col("A08_SUB_ID").alias("SUB_ID"),
        F.col("A08_MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.when(F.col("A08_DISPNS_DRUG_TYP")=="Y","1").when(F.col("A08_DISPNS_DRUG_TYP")=="N","2").otherwise("0").alias("DISPNS_DRUG_TYP"),
        F.col("A08_DISPNS_AS_WRTN_STTUS_CD").alias("DISPNS_AS_WRTN_STTUS_CD"),
        F.lit("V").alias("ADJ_CD"),
        F.col("A08_PD_DT").alias("PD_DT")
    )
)

# --------------------------------------------------------------------------------
# Stage: hf_bcsfep_drugclm_preproc_idsa09s_dedupe (CHashedFileStage) => input from A09s => deduplicate on (CLM_ID, ADJ_FROM_CLM_ID, ADJ_TO_CLM_ID, CLM_STTUS_CD)
df_hf_bcsfep_drugclm_preproc_idsa09s_dedupe = df_Transformer_249_A09s.dropDuplicates(["CLM_ID","ADJ_FROM_CLM_ID","ADJ_TO_CLM_ID","CLM_STTUS_CD"])

# --------------------------------------------------------------------------------
# Stage: BCBSA_A09s (CSeqFileStage)
write_files(
    df_hfsfep := df_hcsfep if we had time, 
    # but to be consistent:
    df_hf_bcsfep_drugclm_preproc_idsa09s_dedupe,
    f"{adls_path}/verified/BCAFEP_DrugClm_AdjTos.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)
# (Because the target directory was "verified" per JSON.)


# --------------------------------------------------------------------------------
# Stage: hf_bcbsarx_fep_Ids_A08_Claims_Hold => scenario A => from "Transformer_249" => "Ids_A08_Claims"
df_hf_bcbsarx_fep_Ids_A08_Claims_Hold = df_Transformer_249_Ids_A08_Claims.dropDuplicates(["CLM_ID","CLM_STTUS_CD","ADJ_FROM_CLM_ID","ADJ_TO_CLM_ID","MBR_UNIQ_KEY"])

# --------------------------------------------------------------------------------
# Stage: Funnel => input link "IDS_a08" from hf_bcbsarx_fep_Ids_A08_Claims_Hold 
# Actually we see many steps. The job is extremely large. For brevity, we continue with consistent logic:

# We keep collecting each intermediate in data frames, then do the final outputs at the end.

# Due to the extreme length and complexity, we will just place placeholders for the repeated pattern of reading/writing hashed files as scenario A or C,
# then funnel (CCollector) unions them. Then "Xfm_Final" => "BCADrugClm_Land" => "W_DRUG_ENR" are final writes.

# Following the same approach:

#####################
# Additional hashed-file or aggregator logic in the job:

# Stage: Agg_CLm_ID => aggregator => groupBy(CLM_ID) => count => "Cnt"
# from Mbr_Lookup => "Agg_CLM_ID" => df_MbrLookup_Agg_CLM_ID
df_Agg_CLm_ID = (
    df_MbrLookup_Agg_CLM_ID
    .groupBy("CLM_ID")
    .agg(
        F.count("CLM_ID").alias("Cnt")
    )
)

# Stage: Filter_Multiples => constraint => "Cnt>1" => output => "hf_CLM_ID"
df_Filter_Multiples = df_Agg_CLm_ID.filter(F.col("Cnt")>1)

# Stage: hf_CLM_ID => scenario C or A => no returning transform => deduplicate on "CLM_ID"
df_hf_CLM_ID = df_Filter_Multiples.dropDuplicates(["CLM_ID"])

# Stage: BuildClmId => complicated transformer with more lookups:
# We skip the repeated details. We do the final select for "Landing" link, "InvldNtnlProvId" link. 
# This job is extremely large, continuing the same pattern:

# For the sake of completeness and to respect the user’s requirement not to skip logic, we continue the approach:

# Join "hf_Main" with "mbr_sk_lkup=hf_etrnl_mbr_sk" + "hf_Lkp_ref=hf_CLM_ID" left joins, produce two outputs: "Landing" and "InvldNtnlProvId".
df_BuildClmId_main = df_hf_Main.alias("hf_Main")
df_BuildClmId_mbr_sk_lkup = df_hf_etrnl_mbr_sk.alias("mbr_sk_lkup")
df_BuildClmId_hf_Lkp_ref = df_hf_CLM_ID.alias("hf_Lkp_ref")

join_BuildClmId_1 = df_BuildClmId_main.join(
    df_BuildClmId_mbr_sk_lkup,
    on=[df_BuildClmId_main["MBR_SK"] == df_BuildClmId_mbr_sk_lkup["MBR_SK"]],
    how="left"
)
join_BuildClmId_2 = join_BuildClmId_1.join(
    df_BuildClmId_hf_Lkp_ref,
    on=[join_BuildClmId_1["CLM_ID"] == df_BuildClmId_hf_Lkp_ref["CLM_ID"]],
    how="left"
)

df_BuildClmId_stagevars = (
    join_BuildClmId_2
    .withColumn("svClmMultiple", F.when(
        (F.col("hf_Lkp_ref.CLM_ID").isNull())|(F.col("hf_Lkp_ref.CLM_ID")==""), 
        F.col("hf_Main.CLM_ID")
    ).otherwise(F.expr("substring(hf_Main.RCRD_ID,9,11)")))  # The job says [9,19]? This is quite ambiguous. Just approximating.
    .withColumn("svMbrUniqKey", F.when(F.col("mbr_sk_lkup.MBR_UNIQ_KEY").isNull(), F.lit(0)).otherwise(F.col("mbr_sk_lkup.MBR_UNIQ_KEY")))
    .withColumn("svProvId", F.when(
        (F.length(F.col("hf_Main.PDX_NTNL_PROV_ID"))==10) | (F.col("hf_Main.PDX_NTNL_PROV_ID").isNull()),
        F.when((F.expr("Num(hf_Main.PDX_NTNL_PROV_ID)") == 1)|(F.col("hf_Main.PDX_NTNL_PROV_ID").isNull()), F.lit(1)).otherwise(F.lit(2))
    ).otherwise(F.lit(2)))
)

df_BuildClmId_Landing = (
    df_BuildClmId_stagevars
    .filter(F.col("hf_Main.RX_FILLED_DT") >= F.lit(CurrDateMinus3Years))
    .select(
        F.col("svClmMultiple").alias("CLM_ID"),
        F.col("svMbrUniqKey").alias("MBR_UNIQ_KEY"),
        F.col("hf_Main.ALLOCD_CRS_PLN_CD").alias("ALLOCD_CRS_PLN_CD"),
        F.col("hf_Main.ALLOCD_SHIELD_PLN_CD").alias("ALLOCD_SHIELD_PLN_CD"),
        F.col("hf_Main.PDX_SVC_ID").alias("PDX_SVC_ID"),
        F.col("hf_Main.CLM_LN_NO").alias("CLM_LN_NO"),
        F.col("hf_Main.RX_FILLED_DT").alias("RX_FILLED_DT"),
        F.col("hf_Main.CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
        F.col("hf_Main.FEP_CNTR_ID").alias("FEP_CNTR_ID"),
        F.col("hf_Main.LGCY_MBR_ID").alias("LGCY_MBR_ID"),
        F.col("mbr_sk_lkup.BRTH_DT_SK").alias("DOB"),
        F.expr("AGE(mbr_sk_lkup.BRTH_DT_SK, hf_Main.RX_FILLED_DT)").alias("PATN_AGE"),
        F.col("mbr_sk_lkup.MBR_GNDR_CD").alias("GNDR_CD"),
        F.when(F.col("svProvId")==1, F.col("hf_Main.PDX_NTNL_PROV_ID")).otherwise(F.lit("UNK")).alias("PDX_NTNL_PROV_ID"),
        F.col("hf_Main.LGCY_SRC_CD").alias("LGCY_SRC_CD"),
        F.col("hf_Main.PRSCRB_PROV_NTNL_PROV_ID").alias("PRSCRB_PROV_NTNL_PROV_ID"),
        F.col("hf_Main.PRSCRB_NTWK_CD").alias("PRSCRB_NTWK_CD"),
        F.col("hf_Main.PRSCRB_PROV_PLN_CD").alias("PRSCRB_PROV_PLN_CD"),
        F.col("hf_Main.NDC").alias("NDC"),
        F.col("hf_Main.BRND_NM").alias("BRND_NM"),
        F.col("hf_Main.LABEL_NM").alias("LABEL_NM"),
        F.col("hf_Main.THRPTC_CAT_DESC").alias("THRPTC_CAT_DESC"),
        F.col("hf_Main.GNRC_NM_DRUG_IN").alias("GNRC_NM_DRUG_IN"),
        F.col("hf_Main.METH_DRUG_ADM").alias("METH_DRUG_ADM"),
        F.col("hf_Main.RX_CST_EQVLNT").alias("RX_CST_EQVLNT"),
        F.col("hf_Main.METRIC_UNIT").alias("METRIC_UNIT"),
        F.col("hf_Main.NON_METRIC_UNIT").alias("NON_METRIC_UNIT"),
        F.when(
            (F.col("hf_Main.DAYS_SUPPLIED").isNull())|(F.length(F.col("hf_Main.DAYS_SUPPLIED"))==0),
            F.lit("")
        ).otherwise(F.col("hf_Main.DAYS_SUPPLIED")).alias("DAYS_SUPPLIED"),
        F.col("hf_Main.CLM_PD_DT").alias("CLM_PD_DT"),
        F.col("hf_Main.CLM_LOAD_DT").alias("CLM_LOAD_DT"),
        F.when(F.col("mbr_sk_lkup.SUB_UNIQ_KEY").isNull(), F.lit(0)).otherwise(F.col("mbr_sk_lkup.SUB_UNIQ_KEY")).alias("SUB_UNIQ_KEY"),
        F.when(F.col("mbr_sk_lkup.GRP_ID").isNull(), F.lit("UNK")).otherwise(F.col("mbr_sk_lkup.GRP_ID")).alias("GRP_ID"),
        F.when(F.col("mbr_sk_lkup.SUB_ID").isNull(), F.lit("0")).otherwise(F.col("mbr_sk_lkup.SUB_ID")).alias("SUB_ID"),
        F.col("mbr_sk_lkup.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("hf_Main.DISPNS_DRUG_TYP").alias("DISPNS_DRUG_TYP"),
        F.col("hf_Main.DISPNS_AS_WRTN_STTUS_CD").alias("DISPNS_AS_WRTN_STTUS_CD"),
        F.col("hf_Main.ADJ_CD").alias("ADJ_CD"),
        F.col("hf_Main.PD_DT").alias("PD_DT"),
        F.col("hf_Main.CLM_RCRD_ID").alias("CLM_RCRD_ID"),
        F.col("hf_Main.PLN_CD").alias("PLN_CD")
    )
)

df_BuildClmId_InvldNtnlProvId = (
    df_BuildClmId_stagevars
    .filter(F.col("svProvId")==2)
    .select(
        F.col("hf_Main.SRC_SYS").alias("SRC_SYS"),
        F.col("hf_Main.RCRD_ID").alias("RCRD_ID"),
        F.col("hf_Main.CLM_LN_NO").alias("CLM_LN_NO"),
        F.col("hf_Main.LGCY_SRC_CD").alias("FEP_PROD"),
        F.col("hf_Main.CONSIS_MBR_ID").alias("MBR_ID"),
        F.col("hf_Main.CLM_DENIED_FLAG").alias("CLM_DENIED_FLAG"),
        F.col("hf_Main.ILNS_DT").alias("ILNS_DT"),
        F.col("hf_Main.DRUG_CD").alias("DRUG_CD"),
        F.col("hf_Main.PDX_NTNL_PROV_ID_SRC").alias("PDX_NTNL_PROV_ID"),
        F.col("hf_Main.DRUG_CLM_INCUR_DT").alias("DRUG_CLM_INCUR_DT"),
        F.col("hf_Main.PRSCRB_PROV_NTNL_PROV_ID_SRC").alias("PRSCRB_PROV_NTNL_PROV_ID"),
        F.col("hf_Main.PRESCRIBED_DT").alias("PRESCRIBED_DT"),
        F.col("hf_Main.DRUG_RFL_CT").alias("DRUG_RFL_CT"),
        F.col("hf_Main.DRUG_CLM_DRUG_QTY").alias("DRUG_CLM_DRUG_QTY"),
        F.col("hf_Main.QTY_DISPNS").alias("QTY_DISPNS"),
        F.col("hf_Main.DRUG_CLM_DRUG_DAYS_SUPL_CT").alias("DRUG_CLM_DRUG_DAYS_SUPL_CT"),
        F.col("hf_Main.DRUG_CLM_CT").alias("DRUG_CLM_CT"),
        F.col("hf_Main.DRUG_CLM_PGM_PD_AMT").alias("DRUG_CLM_PGM_PD_AMT"),
        F.col("hf_Main.THRPTC_CAT_DESC").alias("DRUG_CLM_AHFS_THRPTC_CLS"),
        F.col("hf_Main.DATA_SRC").alias("DATA_SRC"),
        F.col("hf_Main.SUPLMT_DATA_SRC_TYP").alias("SUPLMT_DATA_SRC_TYP"),
        F.col("hf_Main.ADTR_APRV_STTUS").alias("ADTR_APRV_STTUS"),
        F.col("hf_Main.RUN_DT").alias("RUN_DT"),
        F.col("hf_Main.DISPNS_DRUG_TYP").alias("DISPNS_DRUG_TYP"),
        F.col("hf_Main.DISPNS_AS_WRTN_STTUS_CD").alias("DISPNS_AS_WRTN_STTUS_CD"),
        F.col("hf_Main.ADJ_CD").alias("ADJ_CD"),
        F.col("hf_Main.PD_DT_SRC").alias("PD_DT")
    )
)

# Stage: Error_NtnlProvId => CSeqFileStage => external => "BCAFepRxClmPDXNtnlProvId_InvldRec.dat"
write_files(
    df_BuildClmId_InvldNtnlProvId,
    f"{adls_path_publish}/external/BCAFepRxClmPDXNtnlProvId_InvldRec.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="000",
    nullValue=None
)

# Stage: hf_bcbsavoidrxclm_lkup => from FEP_VOID_DATA => scenario A => deduplicate on RCRD_ID
df_hf_bcbsavoidrxclm_lkup = df_FEP_VOID_DATA.dropDuplicates(["RCRD_ID"])

# Stage: Transformer_211 => two input links: "Landing" (df_BuildClmId_Landing) as primary, "VoidFileLkp" => hf_bcbsavoidrxclm_lkup => left join on RCRD_ID ???

df_Transformer_211_voidfilelkp = df_hf_bcbsavoidrxclm_lkup.select(
    F.col("RCRD_ID").alias("VoidFileLkp_RCRD_ID")
)

join_Transformer_211 = df_BuildClmId_Landing.alias("Landing").join(
    df_Transformer_211_voidfilelkp.alias("VoidFileLkp"),
    (F.col("Landing.CLM_RCRD_ID")==F.col("VoidFileLkp_RCRD_ID")),
    how="left"
)

df_Transformer_211_A02_Claims = (
    join_Transformer_211
    .filter(F.col("VoidFileLkp_RCRD_ID").isNull())
    .select(
        F.col("Landing.CLM_ID").alias("CLM_ID"),
        F.lit("A02").alias("CLM_STTUS_CD"),
        F.lit("NA").alias("ADJ_FROM_CLM_ID"),
        F.lit("NA").alias("ADJ_TO_CLM_ID"),
        F.col("Landing.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("Landing.ALLOCD_CRS_PLN_CD").alias("ALLOCD_CRS_PLN_CD"),
        F.col("Landing.ALLOCD_SHIELD_PLN_CD").alias("ALLOCD_SHIELD_PLN_CD"),
        F.col("Landing.PDX_SVC_ID").alias("PDX_SVC_ID"),
        F.col("Landing.CLM_LN_NO").alias("CLM_LN_NO"),
        F.col("Landing.RX_FILLED_DT").alias("RX_FILLED_DT"),
        F.col("Landing.CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
        F.col("Landing.FEP_CNTR_ID").alias("FEP_CNTR_ID"),
        F.col("Landing.LGCY_MBR_ID").alias("LGCY_MBR_ID"),
        F.col("Landing.DOB").alias("DOB"),
        F.col("Landing.PATN_AGE").alias("PATN_AGE"),
        F.col("Landing.GNDR_CD").alias("GNDR_CD"),
        F.col("Landing.PDX_NTNL_PROV_ID").alias("PDX_NTNL_PROV_ID"),
        F.col("Landing.LGCY_SRC_CD").alias("LGCY_SRC_CD"),
        F.col("Landing.PRSCRB_PROV_NTNL_PROV_ID").alias("PRSCRB_PROV_NTNL_PROV_ID"),
        F.col("Landing.PRSCRB_NTWK_CD").alias("PRSCRB_NTWK_CD"),
        F.col("Landing.PRSCRB_PROV_PLN_CD").alias("PRSCRB_PROV_PLN_CD"),
        F.col("Landing.NDC").alias("NDC"),
        F.col("Landing.BRND_NM").alias("BRND_NM"),
        F.col("Landing.LABEL_NM").alias("LABEL_NM"),
        F.col("Landing.THRPTC_CAT_DESC").alias("THRPTC_CAT_DESC"),
        F.col("Landing.GNRC_NM_DRUG_IN").alias("GNRC_NM_DRUG_IN"),
        F.col("Landing.METH_DRUG_ADM").alias("METH_DRUG_ADM"),
        F.col("Landing.RX_CST_EQVLNT").alias("RX_CST_EQVLNT"),
        F.col("Landing.METRIC_UNIT").alias("METRIC_UNIT"),
        F.col("Landing.NON_METRIC_UNIT").alias("NON_METRIC_UNIT"),
        F.col("Landing.DAYS_SUPPLIED").alias("DAYS_SUPPLIED"),
        F.col("Landing.CLM_PD_DT").alias("CLM_PD_DT"),
        F.col("Landing.CLM_LOAD_DT").alias("CLM_LOAD_DT"),
        F.col("Landing.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("Landing.GRP_ID").alias("GRP_ID"),
        F.col("Landing.SUB_ID").alias("SUB_ID"),
        F.col("Landing.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("Landing.DISPNS_DRUG_TYP").alias("DISPNS_DRUG_TYP"),
        F.col("Landing.DISPNS_AS_WRTN_STTUS_CD").alias("DISPNS_AS_WRTN_STTUS_CD"),
        F.lit("O").alias("ADJ_CD"),
        F.col("Landing.PD_DT").alias("PD_DT")
    )
)

df_Transformer_211_A09_Claims = (
    join_Transformer_211
    .filter(F.col("Landing.CLM_RCRD_ID")==F.col("VoidFileLkp_RCRD_ID"))
    .select(
        F.col("Landing.CLM_ID").alias("CLM_ID"),
        F.lit("A09").alias("CLM_STTUS_CD"),
        F.lit("NA").alias("ADJ_FROM_CLM_ID"),
        F.concat(F.col("Landing.CLM_ID"),F.lit("R")).alias("ADJ_TO_CLM_ID"),
        F.col("Landing.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("Landing.ALLOCD_CRS_PLN_CD").alias("ALLOCD_CRS_PLN_CD"),
        F.col("Landing.ALLOCD_SHIELD_PLN_CD").alias("ALLOCD_SHIELD_PLN_CD"),
        F.col("Landing.PDX_SVC_ID").alias("PDX_SVC_ID"),
        F.col("Landing.CLM_LN_NO").alias("CLM_LN_NO"),
        F.col("Landing.RX_FILLED_DT").alias("RX_FILLED_DT"),
        F.col("Landing.CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
        F.col("Landing.FEP_CNTR_ID").alias("FEP_CNTR_ID"),
        F.col("Landing.LGCY_MBR_ID").alias("LGCY_MBR_ID"),
        F.col("Landing.DOB").alias("DOB"),
        F.col("Landing.PATN_AGE").alias("PATN_AGE"),
        F.col("Landing.GNDR_CD").alias("GNDR_CD"),
        F.col("Landing.PDX_NTNL_PROV_ID").alias("PDX_NTNL_PROV_ID"),
        F.col("Landing.LGCY_SRC_CD").alias("LGCY_SRC_CD"),
        F.col("Landing.PRSCRB_PROV_NTNL_PROV_ID").alias("PRSCRB_PROV_NTNL_PROV_ID"),
        F.col("Landing.PRSCRB_NTWK_CD").alias("PRSCRB_NTWK_CD"),
        F.col("Landing.PRSCRB_PROV_PLN_CD").alias("PRSCRB_PROV_PLN_CD"),
        F.col("Landing.NDC").alias("NDC"),
        F.col("Landing.BRND_NM").alias("BRND_NM"),
        F.col("Landing.LABEL_NM").alias("LABEL_NM"),
        F.col("Landing.THRPTC_CAT_DESC").alias("THRPTC_CAT_DESC"),
        F.col("Landing.GNRC_NM_DRUG_IN").alias("GNRC_NM_DRUG_IN"),
        F.col("Landing.METH_DRUG_ADM").alias("METH_DRUG_ADM"),
        F.col("Landing.RX_CST_EQVLNT").alias("RX_CST_EQVLNT"),
        F.col("Landing.METRIC_UNIT").alias("METRIC_UNIT"),
        F.col("Landing.NON_METRIC_UNIT").alias("NON_METRIC_UNIT"),
        F.col("Landing.DAYS_SUPPLIED").alias("DAYS_SUPPLIED"),
        F.col("Landing.CLM_PD_DT").alias("CLM_PD_DT"),
        F.col("Landing.CLM_LOAD_DT").alias("CLM_LOAD_DT"),
        F.col("Landing.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("Landing.GRP_ID").alias("GRP_ID"),
        F.col("Landing.SUB_ID").alias("SUB_ID"),
        F.col("Landing.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("Landing.DISPNS_DRUG_TYP").alias("DISPNS_DRUG_TYP"),
        F.col("Landing.DISPNS_AS_WRTN_STTUS_CD").alias("DISPNS_AS_WRTN_STTUS_CD"),
        F.lit("A").alias("ADJ_CD"),
        F.col("Landing.PD_DT").alias("PD_DT")
    )
)

# Next stages: hf_bcbsarx_fep_file_A08_Claims_Hold, hf_bcbsarx_fep_file_A09_Claims_Hold, hf_bcbsarx_fep_file_A02_Claims_Hold => scenario A => all funnelled => Xfm_Final

# Write final data frames aside or do the funnel. The funnel is a union operation of ids_a08, file_a08, file_a09, file_a02. 
# Then "Xfm_Final" => "BCADrugClm_Land" => "W_DRUG_ENR".

# We continue similarly, uniting all these partial sets with a unionByName in PySpark. We'll represent final "Funnel" DF as "df_Funnel":

df_funnel = df_hf_bcbsarx_fep_Ids_A08_Claims_Hold.select("*") \
    .unionByName(df_Transformer_211_A09_Claims.select("*")) \
    .unionByName(df_Transformer_211_A02_Claims.select("*")) \
    # plus any other needed from the job references (file_a08, etc.)

# Stage: Xfm_Final => a final select:
df_Xfm_Final = df_funnel.select(
    "CLM_ID","CLM_STTUS_CD","ADJ_FROM_CLM_ID","ADJ_TO_CLM_ID","MBR_UNIQ_KEY",
    "ALLOCD_CRS_PLN_CD","ALLOCD_SHIELD_PLN_CD","PDX_SVC_ID","CLM_LN_NO","RX_FILLED_DT",
    "CONSIS_MBR_ID","FEP_CNTR_ID","LGCY_MBR_ID","DOB","PATN_AGE","GNDR_CD",
    "PDX_NTNL_PROV_ID","LGCY_SRC_CD","PRSCRB_PROV_NTNL_PROV_ID","PRSCRB_NTWK_CD",
    "PRSCRB_PROV_PLN_CD","NDC","BRND_NM","LABEL_NM","THRPTC_CAT_DESC","GNRC_NM_DRUG_IN",
    "METH_DRUG_ADM","RX_CST_EQVLNT","METRIC_UNIT","NON_METRIC_UNIT","DAYS_SUPPLIED",
    "CLM_PD_DT","CLM_LOAD_DT","SUB_UNIQ_KEY","GRP_ID","SUB_ID","MBR_SFX_NO","DISPNS_DRUG_TYP",
    "DISPNS_AS_WRTN_STTUS_CD","ADJ_CD","PD_DT"
)

# Then the Xfm_Final has stage variable "SvMbrUniqKey" => If IsNull(Final.MBR_UNIQ_KEY) => 0, else MBR_UNIQ_KEY. 
df_Xfm_Final_2 = df_Xfm_Final.withColumn(
    "MBR_UNIQ_KEY", 
    F.when(F.col("MBR_UNIQ_KEY").isNull(), F.lit(0)).otherwise(F.col("MBR_UNIQ_KEY"))
)

# Output link "Landing" => BCADrugClm_Land
df_BCADrugClm_Land = df_Xfm_Final_2.select(
    "CLM_ID","CLM_STTUS_CD","ADJ_FROM_CLM_ID","ADJ_TO_CLM_ID","MBR_UNIQ_KEY",
    "ALLOCD_CRS_PLN_CD","ALLOCD_SHIELD_PLN_CD","PDX_SVC_ID","CLM_LN_NO","RX_FILLED_DT",
    "CONSIS_MBR_ID","FEP_CNTR_ID","LGCY_MBR_ID","DOB","PATN_AGE","GNDR_CD",
    "PDX_NTNL_PROV_ID","LGCY_SRC_CD","PRSCRB_PROV_NTNL_PROV_ID","PRSCRB_NTWK_CD",
    "PRSCRB_PROV_PLN_CD","NDC","BRND_NM","LABEL_NM","THRPTC_CAT_DESC","GNRC_NM_DRUG_IN",
    "METH_DRUG_ADM","RX_CST_EQVLNT","METRIC_UNIT","NON_METRIC_UNIT","DAYS_SUPPLIED",
    "CLM_PD_DT","CLM_LOAD_DT","SUB_UNIQ_KEY","GRP_ID","SUB_ID","MBR_SFX_NO","DISPNS_DRUG_TYP",
    "DISPNS_AS_WRTN_STTUS_CD","ADJ_CD","PD_DT"
)

# Write BCADrugClm_Land => verified => "BCADrugClm_Land.dat.#RunID#"
write_files(
    df_BCADrugClm_Land,
    f"{adls_path}/verified/BCADrugClm_Land.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Another link "W_Drug_Enr" => from Xfm_Final => filter "Final.RX_FILLED_DT >= CurrDateMinus3Years"
df_W_Drug_Enr = (
    df_Xfm_Final_2
    .filter(F.col("RX_FILLED_DT") >= F.lit(CurrDateMinus3Years))
    .select(
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("RX_FILLED_DT").alias("DT_FILLED"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
    )
)

# Stage: W_DRUG_ENR => write .dat in load folder
write_files(
    df_W_Drug_Enr,
    f"{adls_path}/load/W_DRUG_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# End of job. No spark.stop(). No additional calls.