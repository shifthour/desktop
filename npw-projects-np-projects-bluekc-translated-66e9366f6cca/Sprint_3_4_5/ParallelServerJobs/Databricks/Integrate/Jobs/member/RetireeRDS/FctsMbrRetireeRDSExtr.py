# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 13:39:53 Batch  14638_49197 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/10/08 10:01:22 Batch  14620_36086 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 15:33:58 Batch  14605_56043 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/23/07 15:42:37 Batch  14572_56566 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 09/25/07 16:07:19 Batch  14513_58043 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 09/18/07 09:05:03 Batch  14506_32892 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/23/07 12:38:45 Batch  14480_45530 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/23/07 14:23:20 Batch  14327_51805 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_11 11/03/06 15:53:42 Batch  14187_57225 PROMOTE bckcett ids20 u10157 sa
# MAGIC ^1_11 11/03/06 15:52:10 Batch  14187_57132 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_11 11/03/06 15:50:55 Batch  14187_57057 INIT bckcett testIDS30 u10157 sa
# MAGIC ^1_4 10/09/06 14:14:37 Batch  14162_51294 INIT bckcett testIDS30 dsadm Keith for Sharon
# MAGIC ^1_10 09/28/06 18:22:34 Batch  14151_66266 PROMOTE bckcett testIDS30 u10157 SA
# MAGIC ^1_10 09/28/06 18:10:23 Batch  14151_65432 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_10 09/28/06 18:00:51 Batch  14151_64855 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_9 09/27/06 11:08:05 Batch  14150_40087 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_9 09/27/06 10:56:54 Batch  14150_39416 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_8 09/26/06 10:33:56 Batch  14149_38040 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_7 09/22/06 12:01:15 Batch  14145_43279 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_6 07/25/06 13:54:32 Batch  14086_50074 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_6 07/25/06 13:53:45 Batch  14086_50027 INIT bckcett testIDS30 u10157 sa
# MAGIC ^1_5 07/24/06 14:04:43 Batch  14085_50687 INIT bckcett testIDS30 u10157 sa
# MAGIC ^1_4 07/21/06 17:58:45 Batch  14082_64734 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_4 07/21/06 17:57:35 Batch  14082_64657 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_3 07/21/06 14:52:31 Batch  14082_53554 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_3 07/21/06 14:51:47 Batch  14082_53509 INIT bckcett testIDS30 u10157 sa
# MAGIC 
# MAGIC 1;hf_mbr_rds_mbr_uniq_key
# MAGIC 
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:    FctsMbrMcareRetireeExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   This job extracts data from sequential file /ids/***/external/ directory
# MAGIC                              Files will be loaded to the  /ids/***/external/ directory on a monthly, quaterly and annaul basis.
# MAGIC                              Will require Foreign key lookup for MBR_SK and GRP_SK.
# MAGIC                              Loaded in IdsMbrLoadSeq4
# MAGIC 
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	    /ids/***/external /Mbr_McareD_Ford_Retiree.dat
# MAGIC                     /ids/***/external /Mbr_McareD_GM_Retiree.dat
# MAGIC                     /ids/***/external /Mbr_McareD_NonMotor_Retiree.dat
# MAGIC   
# MAGIC HASH FILES:    hf_mbr_rds
# MAGIC                           hf_mbr_rds_mbr_uniq_key - used to get the member unique key knowing the EDW Mbr_SK
# MAGIC 
# MAGIC TRANSFORMS:  STRIP.FIELD
# MAGIC                         
# MAGIC    
# MAGIC PROCESSING:   Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC 
# MAGIC                              Extract.  Reads in input file placed in the directory /ids/**/external by any of three (3) EDW /MedicareD jobs:  EDWMedDGMMotorsRetireeListExtr, EDWMedDFordRetireeListExtr or EDWMedDDetermineRetireeListExt.
# MAGIC                              Ford and GM will run monthly 
# MAGIC                              Non-Motor will run quaterly
# MAGIC                              There may be annual jobs for any or all of these sources.   Same datastage job, different ZEKE jobs.
# MAGIC                             The extract record contains six (6) seperate subsidy efftective and term dates.   For every field that is populated with a valid date, a seperate record needs to be written out.   Therefore, 1 extract record can create up to 6 records.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Sharon Andrew    03/04/2006-        Originally Programmed
# MAGIC     
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------      
# MAGIC Bhoomi D                2/8/2011          TTR-884                Changed "IDS_SK" to "Table_Name_SK"                                  IntegrateNewDevl          Steph Goddard          02/10/2011

# MAGIC Facets Member Retiree Drug Subsidary Extract
# MAGIC Assign primary surrogate key
# MAGIC Apply business logic
# MAGIC Input file for the FctsMbrRetireeRDSFkey job. 
# MAGIC Writing Sequential File to ../key
# MAGIC Pull member x-ref from CDS
# MAGIC Reads in EDW generated Retiree Listing file and preps for load to MBR_RDS. 
# MAGIC Job will run quarterly or monthly.
# MAGIC Quaterly sometime on the 10th of the first month following the end of a quarter.  
# MAGIC Since from EDW, already has MBR_SK and GRP_SK and foreign lookups not needed in Fkey job.
# MAGIC Loaded in IdsMbrLoadSeq4
# MAGIC Extract.  Reads in input file placed in the directory /ids/**/external by any of three (3) EDW /MedicareD jobs:  EDWMedDGMMotorsRetireeListExtr, EDWMedDFordRetireeListExtr or EDWMedDDetermineRetireeListExt.
# MAGIC The extract record contains six (6) seperate subsidy efftective and term dates.   For every field that is populated with a valid date, NOT spaces or NA, a seperate record needs to be written out.   Therefore, 1 extract record can create up to 6 records.
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC End of Facets Member Retiree Drug Subsidary Extract
# MAGIC Reads an input sequential file, finds the member's sk on IDS, primary keys, and then plucks the group, run cycle and the submit date into a file for table deletion.   
# MAGIC 
# MAGIC Table deletion is job IdsMbrRDSDeleteFromIdsNotOnFile.  Takes any members off the IDS MBR_RDS table for this submission date that are not on the finalized input file.
# MAGIC hf_mbr_rds_extr_delete_keys is input and cleared in IdsMbrRDSDeleteFromIdsNotOnFile
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


RunCycle = get_widget_value('RunCycle','100')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','20060701')

# 1) Read dummy_hf_mbr_rds (Scenario B read step):
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_hf_mbr_rds_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT SRC_SYS_CD, MBR_UNIQ_KEY, SUBMT_DT, PLN_YR_STRT_DT, SUBSIDY_PERD_EFF_DT, CRT_RUN_CYC_EXCTN_SK, MBR_RDS_SK FROM IDS.dummy_hf_mbr_rds"
    )
    .load()
)

# 2) Read the CSeqFileStage "ford_retiree"
schema_ford_retiree = StructType([
    StructField("MBR_ID", StringType(), False),
    StructField("MBR_SFX", StringType(), False),
    StructField("SSN", StringType(), False),
    StructField("HICN", StringType(), False),
    StructField("FIRST_NAME", StringType(), False),
    StructField("MIDDLE_INIT", StringType(), False),
    StructField("LAST_NAME", StringType(), False),
    StructField("DATE_OF_BIRTH", StringType(), False),
    StructField("GENDER", StringType(), False),
    StructField("RELATIONSHIP_TO_RETIREE", StringType(), False),
    StructField("SUBSIDY_EFF_DT_1", StringType(), False),
    StructField("SUBSIDY_TERM_DT_1", StringType(), False),
    StructField("SUBSIDY_EFF_DT_2", StringType(), False),
    StructField("SUBSIDY_TERM_DT_2", StringType(), False),
    StructField("SUBSIDY_EFF_DT_3", StringType(), False),
    StructField("SUBSIDY_TERM_DT_3", StringType(), False),
    StructField("SUBSIDY_EFF_DT_4", StringType(), False),
    StructField("SUBSIDY_TERM_DT_4", StringType(), False),
    StructField("SUBSIDY_EFF_DT_5", StringType(), False),
    StructField("SUBSIDY_TERM_DT_5", StringType(), False),
    StructField("SUBSIDY_EFF_DT_6", StringType(), False),
    StructField("SUBSIDY_TERM_DT_6", StringType(), False),
    StructField("DATE_SUBMITTED", StringType(), False),
    StructField("PLAN_YR_START_DT", StringType(), False),
    StructField("PLAN_YR_END_DT", StringType(), False),
    StructField("GRP_SK", IntegerType(), False),
    StructField("MBR_SK", IntegerType(), False)
])
df_ford_retiree = (
    spark.read
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_ford_retiree)
    .csv(f"{adls_path_publish}/external/Mbr_MedicareD_Retiree.dat")
)

# 3) "StripField" transformer
df_stripped = (
    df_ford_retiree
    .withColumn("MBR_ID", strip_field("MBR_ID"))
    .withColumn("MBR_SFX", strip_field("MBR_SFX"))
    .withColumn("SSN", strip_field("SSN"))
    .withColumn("HICN", strip_field("HICN"))
    .withColumn("FIRST_NAME", strip_field("FIRST_NAME"))
    .withColumn("MIDDLE_INIT", strip_field("MIDDLE_INIT"))
    .withColumn("LAST_NAME", strip_field("LAST_NAME"))
    .withColumn("DATE_OF_BIRTH", strip_field("DATE_OF_BIRTH"))
    .withColumn("GENDER", strip_field("GENDER"))
    .withColumn("RELATIONSHIP_TO_RETIREE", strip_field("RELATIONSHIP_TO_RETIREE"))
    .withColumn("SUBSIDY_EFF_DT_1", strip_field("SUBSIDY_EFF_DT_1"))
    .withColumn("SUBSIDY_TERM_DT_1", strip_field("SUBSIDY_TERM_DT_1"))
    .withColumn("SUBSIDY_EFF_DT_2", strip_field("SUBSIDY_EFF_DT_2"))
    .withColumn("SUBSIDY_TERM_DT_2", strip_field("SUBSIDY_TERM_DT_2"))
    .withColumn("SUBSIDY_EFF_DT_3", strip_field("SUBSIDY_EFF_DT_3"))
    .withColumn("SUBSIDY_TERM_DT_3", strip_field("SUBSIDY_TERM_DT_3"))
    .withColumn("SUBSIDY_EFF_DT_4", strip_field("SUBSIDY_EFF_DT_4"))
    .withColumn("SUBSIDY_TERM_DT_4", strip_field("SUBSIDY_TERM_DT_4"))
    .withColumn("SUBSIDY_EFF_DT_5", strip_field("SUBSIDY_EFF_DT_5"))
    .withColumn("SUBSIDY_TERM_DT_5", strip_field("SUBSIDY_TERM_DT_5"))
    .withColumn("SUBSIDY_EFF_DT_6", strip_field("SUBSIDY_EFF_DT_6"))
    .withColumn("SUBSIDY_TERM_DT_6", strip_field("SUBSIDY_TERM_DT_6"))
    .withColumn("DATE_SUBMITTED", strip_field("DATE_SUBMITTED"))
    .withColumn("PLAN_YR_START_DT", strip_field("PLAN_YR_START_DT"))
    .withColumn("PLAN_YR_END_DT", strip_field("PLAN_YR_END_DT"))
    .withColumn("GRP_SK", F.col("GRP_SK"))
    .withColumn("MBR_SK", F.col("MBR_SK"))
)

# 4) "IDS_Mbr" DB2Connector
extract_query = f"SELECT MBR_SK, MBR_UNIQ_KEY FROM {IDSOwner}.MBR MBR"
df_IDS_Mbr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# 5) "hf_mbr_rds_mbr_uniq_key" is Scenario A -> direct link with dedup
df_ids_mbr_for_businessRules = dedup_sort(
    df_IDS_Mbr,
    ["MBR_SK"],
    []
)

# 6) "businessRules" - primary link is df_stripped, lookup link is df_ids_mbr_for_businessRules
df_businessRules = (
    df_stripped.alias("Stripped")
    .join(
        df_ids_mbr_for_businessRules.alias("ids_mbr"),
        on=[F.col("Stripped.MBR_SK") == F.col("ids_mbr.MBR_SK")],
        how="left"
    )
)

df_businessRules_enriched = df_businessRules.withColumn(
    "MbrUniqKey",
    F.when(F.col("ids_mbr.MBR_SK").isNull(), F.lit(0)).otherwise(F.col("ids_mbr.MBR_UNIQ_KEY"))
)

# Create each subsidy output link filter and select
df_subsid_dt_1 = df_businessRules_enriched.filter(
    (F.length(F.trim(F.col("SUBSIDY_EFF_DT_1"))) > 0) &
    (F.trim(F.col("SUBSIDY_EFF_DT_1")) != F.lit("NA"))
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    (
        F.lit("FACETS") 
        + F.lit(";") 
        + F.col("MbrUniqKey").cast(StringType()) 
        + F.lit(";") 
        + trim(F.col("DATE_SUBMITTED")) 
        + F.lit(";") 
        + trim(F.col("PLAN_YR_START_DT")) 
        + F.lit(";") 
        + trim(F.col("SUBSIDY_EFF_DT_1"))
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("MBR_RDS_SK"),
    F.col("MbrUniqKey").alias("MBR_UNIQ_KEY"),
    F.col("DATE_SUBMITTED").alias("SUBMT_DT"),
    F.col("PLAN_YR_START_DT").alias("PLN_YR_STRT_DT"),
    F.col("SUBSIDY_EFF_DT_1").alias("SUBSIDY_PERD_EFF_DT"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("PLAN_YR_END_DT").alias("PLN_YR_END_DT"),
    F.col("SUBSIDY_TERM_DT_1").alias("SUBSIDY_PERD_TERM_DT")
)

df_subsid_dt_6 = df_businessRules_enriched.filter(
    (F.length(F.trim(F.col("SUBSIDY_EFF_DT_6"))) > 0) &
    (F.trim(F.col("SUBSIDY_EFF_DT_6")) != F.lit("NA"))
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_timestamp().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    (
        trim(F.lit("FACETS"))
        + F.lit(";")
        + F.col("MbrUniqKey").cast(StringType())
        + F.lit(";")
        + trim(F.col("DATE_SUBMITTED"))
        + F.lit(";")
        + trim(F.col("PLAN_YR_START_DT"))
        + F.lit(";")
        + trim(F.col("SUBSIDY_EFF_DT_6"))
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("MBR_RDS_SK"),
    F.col("MbrUniqKey").alias("MBR_UNIQ_KEY"),
    current_timestamp().alias("SUBMT_DT"),
    trim(F.col("PLAN_YR_START_DT")).alias("PLN_YR_STRT_DT"),
    trim(F.col("SUBSIDY_EFF_DT_6")).alias("SUBSIDY_PERD_EFF_DT"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.expr("Substrings(trim(SUBSIDY_EFF_DT_6), 1, 4)").alias("PLN_YR_END_DT"),
    trim(F.col("SUBSIDY_TERM_DT_6")).alias("SUBSIDY_PERD_TERM_DT")
)

df_subsid_dt_2 = df_businessRules_enriched.filter(
    (F.length(F.trim(F.col("SUBSIDY_EFF_DT_2"))) > 0) &
    (F.trim(F.col("SUBSIDY_EFF_DT_2")) != F.lit("NA"))
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_timestamp().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    (
        trim(F.lit("FACETS"))
        + F.lit(";")
        + F.col("MbrUniqKey").cast(StringType())
        + F.lit(";")
        + trim(F.col("DATE_SUBMITTED"))
        + F.lit(";")
        + trim(F.col("PLAN_YR_START_DT"))
        + F.lit(";")
        + trim(F.col("SUBSIDY_EFF_DT_2"))
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("MBR_RDS_SK"),
    F.col("MbrUniqKey").alias("MBR_UNIQ_KEY"),
    current_timestamp().alias("SUBMT_DT"),
    trim(F.col("PLAN_YR_START_DT")).alias("PLN_YR_STRT_DT"),
    trim(F.col("SUBSIDY_EFF_DT_2")).alias("SUBSIDY_PERD_EFF_DT"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.expr("Substrings(trim(SUBSIDY_EFF_DT_2), 1, 4)").alias("PLN_YR_END_DT"),
    trim(F.col("SUBSIDY_TERM_DT_2")).alias("SUBSIDY_PERD_TERM_DT")
)

df_subsid_dt_3 = df_businessRules_enriched.filter(
    (F.length(F.trim(F.col("SUBSIDY_EFF_DT_3"))) > 0) &
    (F.trim(F.col("SUBSIDY_EFF_DT_3")) != F.lit("NA"))
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_timestamp().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    (
        trim(F.lit("FACETS"))
        + F.lit(";")
        + F.col("MbrUniqKey").cast(StringType())
        + F.lit(";")
        + trim(F.col("DATE_SUBMITTED"))
        + F.lit(";")
        + trim(F.col("PLAN_YR_START_DT"))
        + F.lit(";")
        + trim(F.col("SUBSIDY_EFF_DT_3"))
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("MBR_RDS_SK"),
    F.col("MbrUniqKey").alias("MBR_UNIQ_KEY"),
    current_timestamp().alias("SUBMT_DT"),
    trim(F.col("PLAN_YR_START_DT")).alias("PLN_YR_STRT_DT"),
    trim(F.col("SUBSIDY_EFF_DT_3")).alias("SUBSIDY_PERD_EFF_DT"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.expr("Substrings(trim(SUBSIDY_EFF_DT_3), 1, 4)").alias("PLN_YR_END_DT"),
    trim(F.col("SUBSIDY_TERM_DT_3")).alias("SUBSIDY_PERD_TERM_DT")
)

df_subsid_dt_4 = df_businessRules_enriched.filter(
    (F.length(F.trim(F.col("SUBSIDY_EFF_DT_4"))) > 0) &
    (F.trim(F.col("SUBSIDY_EFF_DT_4")) != F.lit("NA"))
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_timestamp().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    (
        trim(F.lit("FACETS"))
        + F.lit(";")
        + F.col("MbrUniqKey").cast(StringType())
        + F.lit(";")
        + trim(F.col("DATE_SUBMITTED"))
        + F.lit(";")
        + trim(F.col("PLAN_YR_START_DT"))
        + F.lit(";")
        + trim(F.col("SUBSIDY_EFF_DT_4"))
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("MBR_RDS_SK"),
    F.col("MbrUniqKey").alias("MBR_UNIQ_KEY"),
    current_timestamp().alias("SUBMT_DT"),
    trim(F.col("PLAN_YR_START_DT")).alias("PLN_YR_STRT_DT"),
    trim(F.col("SUBSIDY_EFF_DT_4")).alias("SUBSIDY_PERD_EFF_DT"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.expr("Substrings(trim(SUBSIDY_EFF_DT_4), 1, 4)").alias("PLN_YR_END_DT"),
    trim(F.col("SUBSIDY_TERM_DT_4")).alias("SUBSIDY_PERD_TERM_DT")
)

df_subsid_dt_5 = df_businessRules_enriched.filter(
    (F.length(F.trim(F.col("SUBSIDY_EFF_DT_5"))) > 0) &
    (F.trim(F.col("SUBSIDY_EFF_DT_5")) != F.lit("NA"))
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_timestamp().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    (
        trim(F.lit("FACETS"))
        + F.lit(";")
        + F.col("MbrUniqKey").cast(StringType())
        + F.lit(";")
        + trim(F.col("DATE_SUBMITTED"))
        + F.lit(";")
        + trim(F.col("PLAN_YR_START_DT"))
        + F.lit(";")
        + trim(F.col("SUBSIDY_EFF_DT_5"))
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("MBR_RDS_SK"),
    F.col("MbrUniqKey").alias("MBR_UNIQ_KEY"),
    current_timestamp().alias("SUBMT_DT"),
    trim(F.col("PLAN_YR_START_DT")).alias("PLN_YR_STRT_DT"),
    trim(F.col("SUBSIDY_EFF_DT_5")).alias("SUBSIDY_PERD_EFF_DT"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.expr("Substrings(trim(SUBSIDY_EFF_DT_5), 1, 4)").alias("PLN_YR_END_DT"),
    trim(F.col("SUBSIDY_TERM_DT_5")).alias("SUBSIDY_PERD_TERM_DT")
)

# Union them for the "hf_mbr_rds_subsid_dates" scenario A, then deduplicate
df_subsid_union = df_subsid_dt_1.union(df_subsid_dt_2).union(df_subsid_dt_3).union(df_subsid_dt_4).union(df_subsid_dt_5).union(df_subsid_dt_6)

df_mbr_retirees = dedup_sort(
    df_subsid_union,
    [
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING"
    ],
    []
)

# 7) "all_records_for_audit" -> "FctsMbrRetireeRdsExtrAllRecs"
df_all_records_for_audit = df_businessRules_enriched.select(
    F.when(F.col("ids_mbr.MBR_UNIQ_KEY").isNull(), F.lit(0)).otherwise(F.col("ids_mbr.MBR_UNIQ_KEY")).alias("MBR_UNIQ_KEY"),
    F.col("Stripped.MBR_SK").alias("MBR_SK"),
    F.when(F.length(F.trim(F.col("Stripped.MBR_ID")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.MBR_ID"))).alias("MBR_ID"),
    F.when(F.length(F.trim(F.col("Stripped.MBR_SFX")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.MBR_SFX"))).alias("MBR_SFX"),
    F.when(F.length(F.trim(F.col("Stripped.SSN")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.SSN"))).alias("SSN"),
    F.when(F.length(F.trim(F.col("Stripped.HICN")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.HICN"))).alias("HICN"),
    F.when(F.length(F.trim(F.col("Stripped.FIRST_NAME")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.FIRST_NAME"))).alias("FIRST_NAME"),
    F.when(F.length(F.trim(F.col("Stripped.MIDDLE_INIT")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.MIDDLE_INIT"))).alias("MIDDLE_INIT"),
    F.when(F.length(F.trim(F.col("Stripped.LAST_NAME")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.LAST_NAME"))).alias("LAST_NAME"),
    F.when(F.length(F.trim(F.col("Stripped.DATE_OF_BIRTH")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.DATE_OF_BIRTH"))).alias("DATE_OF_BIRTH"),
    F.when(F.length(F.trim(F.col("Stripped.GENDER")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.GENDER"))).alias("GENDER"),
    F.when(F.length(F.trim(F.col("Stripped.RELATIONSHIP_TO_RETIREE")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.RELATIONSHIP_TO_RETIREE"))).alias("RELATIONSHIP_TO_RETIREE"),
    F.when(F.length(F.trim(F.col("Stripped.SUBSIDY_EFF_DT_1")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.SUBSIDY_EFF_DT_1"))).alias("SUBSIDY_EFF_DT_1"),
    F.when(F.length(F.trim(F.col("Stripped.SUBSIDY_TERM_DT_1")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.SUBSIDY_TERM_DT_1"))).alias("SUBSIDY_TERM_DT_1"),
    F.when(F.length(F.trim(F.col("Stripped.SUBSIDY_EFF_DT_2")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.SUBSIDY_EFF_DT_2"))).alias("SUBSIDY_EFF_DT_2"),
    F.when(F.length(F.trim(F.col("Stripped.SUBSIDY_TERM_DT_2")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.SUBSIDY_TERM_DT_2"))).alias("SUBSIDY_TERM_DT_2"),
    F.when(F.length(F.trim(F.col("Stripped.SUBSIDY_EFF_DT_3")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.SUBSIDY_EFF_DT_3"))).alias("SUBSIDY_EFF_DT_3"),
    F.when(F.length(F.trim(F.col("Stripped.SUBSIDY_TERM_DT_3")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.SUBSIDY_TERM_DT_3"))).alias("SUBSIDY_TERM_DT_3"),
    F.when(F.length(F.trim(F.col("Stripped.SUBSIDY_EFF_DT_4")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.SUBSIDY_EFF_DT_4"))).alias("SUBSIDY_EFF_DT_4"),
    F.when(F.length(F.trim(F.col("Stripped.SUBSIDY_TERM_DT_4")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.SUBSIDY_TERM_DT_4"))).alias("SUBSIDY_TERM_DT_4"),
    F.when(F.length(F.trim(F.col("Stripped.SUBSIDY_EFF_DT_5")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.SUBSIDY_EFF_DT_5"))).alias("SUBSIDY_EFF_DT_5"),
    F.when(F.length(F.trim(F.col("Stripped.SUBSIDY_TERM_DT_5")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.SUBSIDY_TERM_DT_5"))).alias("SUBSIDY_TERM_DT_5"),
    F.when(F.length(F.trim(F.col("Stripped.SUBSIDY_EFF_DT_6")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.SUBSIDY_EFF_DT_6"))).alias("SUBSIDY_EFF_DT_6"),
    F.when(F.length(F.trim(F.col("Stripped.SUBSIDY_TERM_DT_6")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.SUBSIDY_TERM_DT_6"))).alias("SUBSIDY_TERM_DT_6"),
    F.when(F.length(F.trim(F.col("Stripped.DATE_SUBMITTED")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.DATE_SUBMITTED"))).alias("DATE_SUBMITTED"),
    F.when(F.length(F.trim(F.col("Stripped.PLAN_YR_START_DT")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.PLAN_YR_START_DT"))).alias("PLAN_YR_START_DT"),
    F.when(F.length(F.trim(F.col("Stripped.PLAN_YR_END_DT")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.PLAN_YR_END_DT"))).alias("PLAN_YR_END_DT"),
    F.when(F.length(F.trim(F.col("Stripped.GRP_SK")))==0, F.lit(" ")).otherwise(F.trim(F.col("Stripped.GRP_SK"))).alias("GRP_SK")
)

# Write "FctsMbrRetireeRdsExtrAllRecs"
df_all_records_for_audit_write = df_all_records_for_audit.select(
    rpad(F.col("MBR_UNIQ_KEY").cast(StringType()), 10, " ").alias("MBR_UNIQ_KEY"),
    rpad(F.col("MBR_SK").cast(StringType()), 10, " ").alias("MBR_SK"),
    rpad(F.col("MBR_ID"), 9, " ").alias("MBR_ID"),
    rpad(F.col("MBR_SFX"), 2, " ").alias("MBR_SFX"),
    rpad(F.col("SSN"), 9, " ").alias("SSN"),
    rpad(F.col("HICN"), 12, " ").alias("HICN"),
    rpad(F.col("FIRST_NAME"), 30, " ").alias("FIRST_NAME"),
    rpad(F.col("MIDDLE_INIT"), 1, " ").alias("MIDDLE_INIT"),
    rpad(F.col("LAST_NAME"), 40, " ").alias("LAST_NAME"),
    rpad(F.col("DATE_OF_BIRTH"), 8, " ").alias("DATE_OF_BIRTH"),
    rpad(F.col("GENDER"), 1, " ").alias("GENDER"),
    rpad(F.col("RELATIONSHIP_TO_RETIREE"), 2, " ").alias("RELATIONSHIP_TO_RETIREE"),
    rpad(F.col("SUBSIDY_EFF_DT_1"), 8, " ").alias("SUBSIDY_EFF_DT_1"),
    rpad(F.col("SUBSIDY_TERM_DT_1"), 8, " ").alias("SUBSIDY_TERM_DT_1"),
    rpad(F.col("SUBSIDY_EFF_DT_2"), 8, " ").alias("SUBSIDY_EFF_DT_2"),
    rpad(F.col("SUBSIDY_TERM_DT_2"), 8, " ").alias("SUBSIDY_TERM_DT_2"),
    rpad(F.col("SUBSIDY_EFF_DT_3"), 8, " ").alias("SUBSIDY_EFF_DT_3"),
    rpad(F.col("SUBSIDY_TERM_DT_3"), 8, " ").alias("SUBSIDY_TERM_DT_3"),
    rpad(F.col("SUBSIDY_EFF_DT_4"), 8, " ").alias("SUBSIDY_EFF_DT_4"),
    rpad(F.col("SUBSIDY_TERM_DT_4"), 8, " ").alias("SUBSIDY_TERM_DT_4"),
    rpad(F.col("SUBSIDY_EFF_DT_5"), 8, " ").alias("SUBSIDY_EFF_DT_5"),
    rpad(F.col("SUBSIDY_TERM_DT_5"), 8, " ").alias("SUBSIDY_TERM_DT_5"),
    rpad(F.col("SUBSIDY_EFF_DT_6"), 8, " ").alias("SUBSIDY_EFF_DT_6"),
    rpad(F.col("SUBSIDY_TERM_DT_6"), 8, " ").alias("SUBSIDY_TERM_DT_6"),
    rpad(F.col("DATE_SUBMITTED"), 8, " ").alias("DATE_SUBMITTED"),
    rpad(F.col("PLAN_YR_START_DT"), 8, " ").alias("PLAN_YR_START_DT"),
    rpad(F.col("PLAN_YR_END_DT"), 8, " ").alias("PLAN_YR_END_DT"),
    rpad(F.col("GRP_SK").cast(StringType()), 10, " ").alias("GRP_SK")
)
write_files(
    df_all_records_for_audit_write,
    f"{adls_path}/verified/FctsMbrRetireeRdsExtrAllRecs.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# 8) "hf_mbr_rds_subsid_dates" scenario A output -> direct link to "PrimaryKey"
df_mbr_retirees_for_primarykey = df_mbr_retirees

# 9) "PrimaryKey" stage: has "mbr_retirees" (df_mbr_retirees_for_primarykey) as primary link,
#    and "lkup" (df_hf_mbr_rds_lookup) as lookup on 5 columns.
df_primaryKey_join = (
    df_mbr_retirees_for_primarykey.alias("mbr_retirees")
    .join(
        df_hf_mbr_rds_lookup.alias("lkup"),
        on=[
            F.col("mbr_retirees.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
            F.col("mbr_retirees.MBR_UNIQ_KEY") == F.col("lkup.MBR_UNIQ_KEY"),
            F.col("mbr_retirees.SUBMT_DT") == F.col("lkup.SUBMT_DT"),
            F.col("mbr_retirees.PLN_YR_STRT_DT") == F.col("lkup.PLN_YR_STRT_DT"),
            F.col("mbr_retirees.SUBSIDY_PERD_EFF_DT") == F.col("lkup.SUBSIDY_PERD_EFF_DT")
        ],
        how="left"
    )
)

df_primaryKey_stage_1 = df_primaryKey_join.withColumn(
    "NewCrtRunCycExtcnSk",
    F.when(F.col("lkup.MBR_RDS_SK").isNull(), F.lit(RunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)

# SurrogateKeyGen for "MBR_RDS_SK"
df_enriched = SurrogateKeyGen(
    df_primaryKey_stage_1,
    <DB sequence name>,
    'MBR_RDS_SK',
    <schema>,
    <secret_name>
)

# Output "tmp_file" -> "IdsMbrRetireeExtr"
df_tmp_file = df_enriched.select(
    F.col("mbr_retirees.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("mbr_retirees.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("mbr_retirees.DISCARD_IN").alias("DISCARD_IN"),
    F.col("mbr_retirees.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("mbr_retirees.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("mbr_retirees.ERR_CT").alias("ERR_CT"),
    F.col("mbr_retirees.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("mbr_retirees.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("mbr_retirees.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("MBR_RDS_SK").alias("MBR_RDS_SK"),
    F.col("mbr_retirees.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("mbr_retirees.SUBMT_DT").alias("SUBMT_DT"),
    F.col("mbr_retirees.PLN_YR_STRT_DT").alias("PLN_YR_STRT_DT"),
    F.col("mbr_retirees.SUBSIDY_PERD_EFF_DT").alias("SUBSIDY_PERD_EFF_DT"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("mbr_retirees.GRP_SK").alias("GRP_SK"),
    F.col("mbr_retirees.MBR_SK").alias("MBR_SK"),
    F.col("mbr_retirees.PLN_YR_END_DT").alias("PLN_YR_END_DT"),
    F.col("mbr_retirees.SUBSIDY_PERD_TERM_DT").alias("SUBSIDY_PERD_TERM_DT")
)

df_tmp_file_write = df_tmp_file.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("MBR_RDS_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("SUBMT_DT"),
    F.col("PLN_YR_STRT_DT"),
    F.col("SUBSIDY_PERD_EFF_DT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_SK"),
    F.col("MBR_SK"),
    F.col("PLN_YR_END_DT"),
    F.col("SUBSIDY_PERD_TERM_DT")
)
write_files(
    df_tmp_file_write,
    f"{adls_path}/key/IdsMbrRetireeExtr.MbrRetiree.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Output "updt" -> "hf_mbr_rds" (Scenario B write step):
df_updt = df_enriched.filter(F.col("lkup.MBR_RDS_SK").isNull()).select(
    F.trim(F.col("mbr_retirees.SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("mbr_retirees.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.trim(F.col("mbr_retirees.SUBMT_DT")).alias("SUBMT_DT"),
    F.trim(F.col("mbr_retirees.PLN_YR_STRT_DT")).alias("PLN_YR_STRT_DT"),
    F.trim(F.col("mbr_retirees.SUBSIDY_PERD_EFF_DT")).alias("SUBSIDY_PERD_EFF_DT"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_RDS_SK").alias("MBR_RDS_SK")
)

# Create a temporary physical table for merge
merge_temp_table = "STAGING.FctsMbrRetireeRDSExtr_hf_mbr_rds_temp"
execute_dml(f"DROP TABLE IF EXISTS {merge_temp_table}", jdbc_url, jdbc_props)

df_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", merge_temp_table) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO IDS.dummy_hf_mbr_rds AS T
USING {merge_temp_table} AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD AND
    T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY AND
    T.SUBMT_DT = S.SUBMT_DT AND
    T.PLN_YR_STRT_DT = S.PLN_YR_STRT_DT AND
    T.SUBSIDY_PERD_EFF_DT = S.SUBSIDY_PERD_EFF_DT
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.MBR_RDS_SK = S.MBR_RDS_SK
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    MBR_UNIQ_KEY,
    SUBMT_DT,
    PLN_YR_STRT_DT,
    SUBSIDY_PERD_EFF_DT,
    CRT_RUN_CYC_EXCTN_SK,
    MBR_RDS_SK
  )
  VALUES (
    S.SRC_SYS_CD,
    S.MBR_UNIQ_KEY,
    S.SUBMT_DT,
    S.PLN_YR_STRT_DT,
    S.SUBSIDY_PERD_EFF_DT,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.MBR_RDS_SK
  );
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# 10) "build_delete_keys" -> "hf_mbr_rds_extr_delete_keys" (Scenario C)
df_pull_delete_key = df_tmp_file_write.select(
    F.col("SUBMT_DT"),
    F.col("PLN_YR_STRT_DT"),
    F.col("GRP_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_pull_delete_key,
    f"{adls_path}/hf_mbr_rds_extr_delete_keys.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)