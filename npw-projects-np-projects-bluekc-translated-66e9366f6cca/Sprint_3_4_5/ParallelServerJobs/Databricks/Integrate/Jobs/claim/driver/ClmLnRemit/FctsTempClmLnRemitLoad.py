# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_4 09/22/09 15:32:09 Batch  15241_56279 PROMOTE bckcetl:31540 ids20 dsadm bls for sa
# MAGIC ^1_4 09/22/09 15:22:02 Batch  15241_55325 INIT bckcett:31540 testIDSnew dsadm bls for sa
# MAGIC ^1_2 09/22/09 09:19:44 Batch  15241_33586 INIT bckcett:31540 testIDSnew dsadm bls for sa
# MAGIC ^1_2 07/06/09 11:52:09 Batch  15163_42760 PROMOTE bckcett:31540 testIDSnew u150906 3833-RemitAlternateCharge_Sharon_testIDSnew             Maddy
# MAGIC ^1_2 07/06/09 11:37:10 Batch  15163_41931 INIT bckcett:31540 devlIDSnew u150906 3833-RemitAlternateCharge_Sharon_devlIDSnew                  Maddy
# MAGIC ^1_1 06/29/09 16:32:48 Batch  15156_59635 INIT bckcett:31540 devlIDSnew u150906 3833-RemitAlternateCharge_Sharon_devlIDSnew        Maddy
# MAGIC ^1_1 04/24/09 09:20:30 Batch  15090_33635 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_1 04/24/09 09:14:25 Batch  15090_33296 INIT bckcett testIDS dsadm BLS FOR SA
# MAGIC ^1_1 08/22/08 10:09:03 Batch  14845_36590 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_1 08/22/08 09:55:32 Batch  14845_35734 INIT bckcett testIDS dsadm bls for sa
# MAGIC ^1_1 08/19/08 10:43:19 Batch  14842_38609 PROMOTE bckcett testIDS u03651 steph for Sharon 3057
# MAGIC ^1_1 08/19/08 10:38:02 Batch  14842_38285 INIT bckcett devlIDSnew u03651 steffy
# MAGIC 
# MAGIC **************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2008, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsTempClmLnRemitLoad
# MAGIC CALLED BY:    BCBSClmRemitPrereqSeq
# MAGIC 
# MAGIC DESCRIPTION:  Load tempdb driver table with Facets claims for remit processing
# MAGIC       
# MAGIC MODIFICATIONS:                                                                                                              
# MAGIC                                                                                                                                                                                                                              development                code                     date
# MAGIC Developer                Date                 Project/Altiris #                Change Description                                                                                                 Project                  Reviewer                  Reviewed       
# MAGIC ------------------              --------------------     ------------------------                -----------------------------------------------------------------------                                                    ----------------------      ---------------------------       ----------------------------   
# MAGIC Sharon Andrew        2008-08-01     3057 Web Redesign       Originally Programmed                                                                                           devlIDSnew          Steph Goddrad          08/11/2008
# MAGIC SAndrew                 2009-03-10      ProdSupport                     Renamed FctsTempClmRemitLoad to FctsTempClmLnRemitLoad                       devlIDS                Steph Goddard          03/24/2009
# MAGIC                                                                                                      Added criteria for building a working table that is later used to delete from 
# MAGIC                                                                                                      IDS tables those claim line remit records loaded during ABF conversion with different natural keys
# MAGIC                                                                                                      Builds W_CLM_LN_REMIT_DRVR.dat file
# MAGIC                                                                                                      Moved from /claim/driver/remit   to   /claim/driver/ClmLnRemit
# MAGIC 
# MAGIC SAndrew                 2009-06-23      #3833 Remit Alt Chrg      Added two new BCBS tables which contains Alternate Remit amounts  to            devlIDSnew         Steph Goddard           07/01/2009
# MAGIC                                                                                                extract data from  PD_MED_CLM_DTL_ALT_CHRG and PD_MED_CLN_LN_ALT_CHRG
# MAGIC Prabhu ES              2022-02-26       S2S Remediation               MSSQL connection parameters added                                                               IntegrateDev5	Ken Bradmon	2022-06-11

# MAGIC Remove Duplicate records
# MAGIC Get all BCBS Claim Line Remit data from the BCBS Remit tables and some claim data from CLCL.
# MAGIC Insert records into driver table  tmp_ids_claim_remit
# MAGIC w_clm_ln_remit_drvr is used to delete old conversion records that are no longer applicable becuase a valid remit type is now coming thru from facets.   this remit type field is a natrural key to tables.  if conversion record is not deleted, then amounts are overstated.
# MAGIC All BCBS tables within these extractions get overwritten on every facets payment process.  
# MAGIC Data is lost if this process is not completed
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Job Parameters
DriverTable = get_widget_value("DriverTable","")
SourcsSytemSK = get_widget_value("SourcsSytemSK","")
FacetsOwner = get_widget_value("FacetsOwner","")
facets_secret_name = get_widget_value("facets_secret_name","")
BCBSOwner = get_widget_value("BCBSOwner","")
bcbsowner_secret_name = get_widget_value("bcbsowner_secret_name","")

# Stage: BCBS_PD_MED_CLM_LN (ODBCConnector)
jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbsowner_secret_name)
query_BCBS_PD_MED_CLM_LN = f"""
SELECT
  CLM.CLCL_ID AS CLM_ID,
  CLM.CLCL_CUR_STS AS CLM_STS,
  CLM.CLCL_PAID_DT,
  CLM.CLCL_ID_ADJ_TO,
  CLM.CLCL_ID_ADJ_FROM,
  CLM.CLCL_LAST_ACT_DTM
FROM
  {FacetsOwner}.CMC_CLCL_CLAIM CLM,
  {BCBSOwner}.PD_MED_CLM_LN BCBS_MED
WHERE
  BCBS_MED.CLCL_ID = CLM.CLCL_ID
  AND NOT EXISTS (
    SELECT 'Y'
    FROM {FacetsOwner}.CMC_CLCL_CLAIM clm2,
         {FacetsOwner}.CMC_CLMI_MISC clmi,
         {FacetsOwner}.CMC_GRGR_GROUP grgr
    WHERE CLM.CLCL_ID = clm2.CLCL_ID
      AND clm2.CLCL_ID = clmi.CLCL_ID
      AND clm2.GRGR_CK = grgr.GRGR_CK
      AND (
        clmi.CLMI_LOCAL_PLAN_CD = 'PAR'
        OR grgr.GRGR_ID = 'NASCOPAR'
      )
  )
"""
df_BCBS_PD_MED_CLM_LN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", query_BCBS_PD_MED_CLM_LN)
    .load()
)

# Stage: PD_MED_CLM_DTL_ALT_CHRG (ODBCConnector)
query_PD_MED_CLM_DTL_ALT_CHRG = f"""
SELECT
  CLM.CLCL_ID AS CLM_ID,
  CLM.CLCL_CUR_STS AS CLM_STS,
  CLM.CLCL_PAID_DT,
  CLM.CLCL_ID_ADJ_TO,
  CLM.CLCL_ID_ADJ_FROM,
  CLM.CLCL_LAST_ACT_DTM
FROM
  {FacetsOwner}.CMC_CLCL_CLAIM CLM,
  {BCBSOwner}.PD_MED_CLM_DTL_ALT_CHRG CLM_DTL_ALT
WHERE
  CLM_DTL_ALT.CLCL_ID = CLM.CLCL_ID
  AND NOT EXISTS (
    SELECT 'Y'
    FROM {FacetsOwner}.CMC_CLCL_CLAIM clm2,
         {FacetsOwner}.CMC_CLMI_MISC clmi,
         {FacetsOwner}.CMC_GRGR_GROUP grgr
    WHERE CLM.CLCL_ID = clm2.CLCL_ID
      AND clm2.CLCL_ID = clmi.CLCL_ID
      AND clm2.GRGR_CK = grgr.GRGR_CK
      AND (
        clmi.CLMI_LOCAL_PLAN_CD = 'PAR'
        OR grgr.GRGR_ID = 'NASCOPAR'
      )
  )
"""
df_PD_MED_CLM_DTL_ALT_CHRG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", query_PD_MED_CLM_DTL_ALT_CHRG)
    .load()
)

# Stage: PD_DNTL_CLM_DTL (ODBCConnector)
query_PD_DNTL_CLM_DTL = f"""
SELECT
  CLM.CLCL_ID AS CLM_ID,
  CLM.CLCL_CUR_STS AS CLM_STS,
  CLM.CLCL_PAID_DT,
  CLM.CLCL_ID_ADJ_TO,
  CLM.CLCL_ID_ADJ_FROM,
  CLM.CLCL_LAST_ACT_DTM
FROM
  {FacetsOwner}.CMC_CLCL_CLAIM CLM,
  {BCBSOwner}.PD_DNTL_CLM_DTL DNTL_CLM_DTL
WHERE
  DNTL_CLM_DTL.CLCL_ID = CLM.CLCL_ID
  AND NOT EXISTS (
    SELECT 'Y'
    FROM {FacetsOwner}.CMC_CLCL_CLAIM clm2,
         {FacetsOwner}.CMC_CLMI_MISC clmi,
         {FacetsOwner}.CMC_GRGR_GROUP grgr
    WHERE CLM.CLCL_ID = clm2.CLCL_ID
      AND clm2.CLCL_ID = clmi.CLCL_ID
      AND clm2.GRGR_CK = grgr.GRGR_CK
      AND (
        clmi.CLMI_LOCAL_PLAN_CD = 'PAR'
        OR grgr.GRGR_ID = 'NASCOPAR'
      )
  )
"""
df_PD_DNTL_CLM_DTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", query_PD_DNTL_CLM_DTL)
    .load()
)

# Stage: BCBS_PD_DNTL_CLM_LN (ODBCConnector)
query_BCBS_PD_DNTL_CLM_LN = f"""
SELECT
  CLM.CLCL_ID AS CLM_ID,
  CLM.CLCL_CUR_STS AS CLM_STS,
  CLM.CLCL_PAID_DT,
  CLM.CLCL_ID_ADJ_TO,
  CLM.CLCL_ID_ADJ_FROM,
  CLM.CLCL_LAST_ACT_DTM
FROM
  {FacetsOwner}.CMC_CLCL_CLAIM CLM,
  {BCBSOwner}.PD_DNTL_CLM_LN BCBS_DNTL
WHERE
  BCBS_DNTL.CLCL_ID = CLM.CLCL_ID
  AND NOT EXISTS (
    SELECT 'Y'
    FROM {FacetsOwner}.CMC_CLCL_CLAIM clm2,
         {FacetsOwner}.CMC_CLMI_MISC clmi,
         {FacetsOwner}.CMC_GRGR_GROUP grgr
    WHERE CLM.CLCL_ID = clm2.CLCL_ID
      AND clm2.CLCL_ID = clmi.CLCL_ID
      AND clm2.GRGR_CK = grgr.GRGR_CK
      AND (
        clmi.CLMI_LOCAL_PLAN_CD = 'PAR'
        OR grgr.GRGR_ID = 'NASCOPAR'
      )
  )
"""
df_BCBS_PD_DNTL_CLM_LN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", query_BCBS_PD_DNTL_CLM_LN)
    .load()
)

# Stage: PD_MED_CLM_DTL (ODBCConnector)
query_PD_MED_CLM_DTL = f"""
SELECT
  CLM.CLCL_ID AS CLM_ID,
  CLM.CLCL_CUR_STS AS CLM_STS,
  CLM.CLCL_PAID_DT,
  CLM.CLCL_ID_ADJ_TO,
  CLM.CLCL_ID_ADJ_FROM,
  CLM.CLCL_LAST_ACT_DTM
FROM
  {FacetsOwner}.CMC_CLCL_CLAIM CLM,
  {BCBSOwner}.PD_MED_CLM_DTL MED_CLM_DTL
WHERE
  MED_CLM_DTL.CLCL_ID = CLM.CLCL_ID
  AND NOT EXISTS (
    SELECT 'Y'
    FROM {FacetsOwner}.CMC_CLCL_CLAIM clm2,
         {FacetsOwner}.CMC_CLMI_MISC clmi,
         {FacetsOwner}.CMC_GRGR_GROUP grgr
    WHERE CLM.CLCL_ID = clm2.CLCL_ID
      AND clm2.CLCL_ID = clmi.CLCL_ID
      AND clm2.GRGR_CK = grgr.GRGR_CK
      AND (
        clmi.CLMI_LOCAL_PLAN_CD = 'PAR'
        OR grgr.GRGR_ID = 'NASCOPAR'
      )
  )
"""
df_PD_MED_CLM_DTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", query_PD_MED_CLM_DTL)
    .load()
)

# Stage: PD_MED_CLN_LN_ALT_CHRG (ODBCConnector)
query_PD_MED_CLN_LN_ALT_CHRG = f"""
SELECT
  CLM.CLCL_ID AS CLM_ID,
  CLM.CLCL_CUR_STS AS CLM_STS,
  CLM.CLCL_PAID_DT,
  CLM.CLCL_ID_ADJ_TO,
  CLM.CLCL_ID_ADJ_FROM,
  CLM.CLCL_LAST_ACT_DTM
FROM
  {FacetsOwner}.CMC_CLCL_CLAIM CLM,
  {BCBSOwner}.PD_MED_CLM_LN_ALT_CHRG CLM_LN_ALT
WHERE
  CLM_LN_ALT.CLCL_ID = CLM.CLCL_ID
  AND NOT EXISTS (
    SELECT 'Y'
    FROM {FacetsOwner}.CMC_CLCL_CLAIM clm2,
         {FacetsOwner}.CMC_CLMI_MISC clmi,
         {FacetsOwner}.CMC_GRGR_GROUP grgr
    WHERE CLM.CLCL_ID = clm2.CLCL_ID
      AND clm2.CLCL_ID = clmi.CLCL_ID
      AND clm2.GRGR_CK = grgr.GRGR_CK
      AND (
        clmi.CLMI_LOCAL_PLAN_CD = 'PAR'
        OR grgr.GRGR_ID = 'NASCOPAR'
      )
  )
"""
df_PD_MED_CLN_LN_ALT_CHRG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", query_PD_MED_CLN_LN_ALT_CHRG)
    .load()
)

# Stage: Collector (CCollector) - Union all input DataFrames
df_Collector = (
    df_BCBS_PD_MED_CLM_LN.select(
        "CLM_ID", "CLM_STS", "CLCL_PAID_DT", "CLCL_ID_ADJ_TO", "CLCL_ID_ADJ_FROM", "CLCL_LAST_ACT_DTM"
    )
    .unionByName(
        df_BCBS_PD_DNTL_CLM_LN.select(
            "CLM_ID", "CLM_STS", "CLCL_PAID_DT", "CLCL_ID_ADJ_TO", "CLCL_ID_ADJ_FROM", "CLCL_LAST_ACT_DTM"
        )
    )
    .unionByName(
        df_PD_MED_CLM_DTL.select(
            "CLM_ID", "CLM_STS", "CLCL_PAID_DT", "CLCL_ID_ADJ_TO", "CLCL_ID_ADJ_FROM", "CLCL_LAST_ACT_DTM"
        )
    )
    .unionByName(
        df_PD_DNTL_CLM_DTL.select(
            "CLM_ID", "CLM_STS", "CLCL_PAID_DT", "CLCL_ID_ADJ_TO", "CLCL_ID_ADJ_FROM", "CLCL_LAST_ACT_DTM"
        )
    )
    .unionByName(
        df_PD_MED_CLM_DTL_ALT_CHRG.select(
            "CLM_ID", "CLM_STS", "CLCL_PAID_DT", "CLCL_ID_ADJ_TO", "CLCL_ID_ADJ_FROM", "CLCL_LAST_ACT_DTM"
        )
    )
    .unionByName(
        df_PD_MED_CLN_LN_ALT_CHRG.select(
            "CLM_ID", "CLM_STS", "CLCL_PAID_DT", "CLCL_ID_ADJ_TO", "CLCL_ID_ADJ_FROM", "CLCL_LAST_ACT_DTM"
        )
    )
)

# Stage: hf_tmp_clm_remit_dedup (CHashedFileStage) - Scenario A
df_hf_tmp_clm_remit_dedup = df_Collector.dropDuplicates(["CLM_ID"])

# Two output pins from the hashed file stage:
df_clmID = df_hf_tmp_clm_remit_dedup
df_send_to_working_tbl = df_hf_tmp_clm_remit_dedup

# Stage: DriverTable (ODBCConnector) - writing to tempdb..#DriverTable#
df_DriverTable = df_clmID.select(
    "CLM_ID",
    "CLM_STS",
    "CLCL_PAID_DT",
    "CLCL_ID_ADJ_TO",
    "CLCL_ID_ADJ_FROM",
    "CLCL_LAST_ACT_DTM"
).withColumn("CLM_ID", rpad(col("CLM_ID"), 12, " ")) \
 .withColumn("CLM_STS", rpad(col("CLM_STS"), 2, " ")) \
 .withColumn("CLCL_ID_ADJ_TO", rpad(col("CLCL_ID_ADJ_TO"), 12, " ")) \
 .withColumn("CLCL_ID_ADJ_FROM", rpad(col("CLCL_ID_ADJ_FROM"), 12, " "))

# Write to temporary table, then MERGE
tempdb_secret_name = get_widget_value("tempdb_secret_name","")
jdbc_url_tempdb, jdbc_props_tempdb = get_db_config(tempdb_secret_name)

execute_dml(f"DROP TABLE IF EXISTS tempdb.FctsTempClmLnRemitLoad_DriverTable_temp", jdbc_url_tempdb, jdbc_props_tempdb)

df_DriverTable.write.format("jdbc") \
    .option("url", jdbc_url_tempdb) \
    .options(**jdbc_props_tempdb) \
    .option("dbtable", "tempdb.FctsTempClmLnRemitLoad_DriverTable_temp") \
    .mode("overwrite") \
    .save()

merge_sql = """
MERGE tempdb..#DriverTable# AS T
USING tempdb.FctsTempClmLnRemitLoad_DriverTable_temp AS S
  ON T.CLM_ID = S.CLM_ID
WHEN MATCHED THEN
  UPDATE SET
    T.CLM_STS = S.CLM_STS,
    T.CLCL_PAID_DT = S.CLCL_PAID_DT,
    T.CLCL_ID_ADJ_TO = S.CLCL_ID_ADJ_TO,
    T.CLCL_ID_ADJ_FROM = S.CLCL_ID_ADJ_FROM,
    T.CLCL_LAST_ACT_DTM = S.CLCL_LAST_ACT_DTM
WHEN NOT MATCHED THEN
  INSERT (
    CLM_ID,
    CLM_STS,
    CLCL_PAID_DT,
    CLCL_ID_ADJ_TO,
    CLCL_ID_ADJ_FROM,
    CLCL_LAST_ACT_DTM
  )
  VALUES (
    S.CLM_ID,
    S.CLM_STS,
    S.CLCL_PAID_DT,
    S.CLCL_ID_ADJ_TO,
    S.CLCL_ID_ADJ_FROM,
    S.CLCL_LAST_ACT_DTM
  );
"""
execute_dml(merge_sql, jdbc_url_tempdb, jdbc_props_tempdb)

# Stage: format_delete_drv_tbl (CTransformerStage)
df_format_delete_drv_tbl = (
    df_send_to_working_tbl
    .select("CLM_ID")  # keep the needed column
    .withColumn("SRC_SYS_CD_SK", lit(SourcsSytemSK))
    .withColumn("CLM_ID", rpad(col("CLM_ID"), 12, " "))
    .select("SRC_SYS_CD_SK", "CLM_ID")
)

# Stage: w_clm_ln_remit_drvt (CSeqFileStage)
write_files(
    df_format_delete_drv_tbl,
    f"{adls_path}/load/W_CLM_LN_REMIT_DRVR.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)