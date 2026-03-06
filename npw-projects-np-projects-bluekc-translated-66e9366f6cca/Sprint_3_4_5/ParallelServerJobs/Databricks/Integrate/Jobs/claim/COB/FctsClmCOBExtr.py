# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsClmCOBExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Reads the FctsClmCOBExtr file created in FctsClmCOBExtr  job and adds the Recycle fields and values for the SK if possible.
# MAGIC 
# MAGIC     
# MAGIC 
# MAGIC INPUTS:
# MAGIC    #FilePath#/verified #InFile#.Prod #InFile#;
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC HASH FILES:     hf_clm_nasco_dup_bypass
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Output file is created with a temp. name.  File is renamed in job control to final file name before sorting.
# MAGIC                   
# MAGIC                  This is a pulls all records for procedure code table - table is replaced each day - there is no way to determine new records - no audit file
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------      
# MAGIC O. Nielsen                 02/01/2005-   Originally Programmed 
# MAGIC SAndrew                   02/22/2005-   Renamed hf_clsm_cob  to hf_clm_cob_med_sup
# MAGIC                                                         renamed from hf_clcb_cob to hf_clm_cob
# MAGIC Landon H.                03/2005         Added logic to integrate ITS COB from the CLIO table.
# MAGIC Sharon Andrew        05/2005-   added logic to support claims that were on the hit list and therefore their data still existed on the old Facets tables.  
# MAGIC                                                               if claim last activity date was after Nov 6 2004 then all its COB data is on two different facets tables
# MAGIC                                                               if claim last activity date was before  Nov 6 2004 then all its COB data is on one facets tables and it is not product specific
# MAGIC Steph Goddard          02/10/2006  Changed to combine extract, transform, primary key for sequencer
# MAGIC BJ Luce                     03/20/2006     add hf_clm_nasco_dup_bypass, identifies claims that are nasco dups. If the claim is on the file, a row is not generated for it in IDS. However, an R row will be build for it if the status if '91'
# MAGIC Steph Goddard          07/31/2006  Added a 'TRIM' to claim number for old claims in transform
# MAGIC Brent Leland              08/23/3006   Changed CLM_COB_TYP field definition from integer/unknown to varchar(20).
# MAGIC                                                            Added delete process to remove historical records with different cob_typ_sk.
# MAGIC SAndrew                   12/08/2006   Project 1576  - Reversal logix added for new status codes 89 and  and 99
# MAGIC Oliver Nielsen            08/15/2007   Added Balancing Snapshot File                                                                                     devlIDS30                   Steph Goddard          8/30/07
# MAGIC Ralph Tucker           2008-7-01        3657 Primary Key  Changed primary key from hash file to DB2 table                         devlIDS                           Steph Goddard          changes required
# MAGIC 
# MAGIC Manasa Andru          2013-09-23     Changed the hashed file name from hf_prod_list to hf_clm_cob_prod_list                 IntegrateNewdevl           Kalyan Neelam             2013-09-27
# MAGIC                                                                     and added documentation for the file.
# MAGIC 
# MAGIC Manasa Andru        2015-05-08           Changed the metadata for CLM_COB_TYP_CD to                                              IntegrateNewDevl           Kalyan Neelam             2015-05-11 
# MAGIC                                                                    Varchar 20 in the Transform output link.
# MAGIC 
# MAGIC Manasa Andru        2016-10-14           Changed the datatype of the field CLM_ID from Char18 to Varchar20                 IntegrateDev1                 Kalyan Neelam            2016-10-21
# MAGIC                                                                   in the Snapshot stage and Transform link.
# MAGIC 
# MAGIC Goutham Kalidnidi   2021-03-17          Facets Upgrade added JOIN to get CLCB_COB_REAS_CD                                IntegrateDev1                Kalyan Neelam           2021-03-18 
# MAGIC Prabhu ES              2022-02-28          S2S Remediation     MSSQL connection parameters added                                 IntegrateDev5                Kalyan Neelam           2022-06-10

# MAGIC This hashed file is loaded by FctsClmPrereqSeq. The source data for this hashed file is found in /landing/CLM_COB_PROD_LIST.dat
# MAGIC Delete process is done before loading to remove historical data that no longer exists in the source system.
# MAGIC Run STRIP.FIELD transform on fields that might have Carriage Return, Line Feed, and  Tab
# MAGIC Pulling Facets Claim COB 
# MAGIC Data for a specific Time Period
# MAGIC COB info is in two different places depending on the claims date of entry into Facets.  Claims entered < Nov 2004 have all its COB info on the clcb_cl_cob table.  Clms entered > November 2004 have COB info on CLCB and med_sup.  If claim with service < Nov 2004 is edited, its COB info is moved into the >Nov 2004 tables.   If clm with service < Nov 2004 is just added to hit list, then COB info is still in old tables.
# MAGIC This container is used in:
# MAGIC FctsClmCOBExtr
# MAGIC NascoClmCOBTrns
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
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
    StructType, StructField, StringType, IntegerType, TimestampType,
    DecimalType, NumericType, BooleanType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmCobPK
# COMMAND ----------

# --------------------------------------------------------------------------------
# Retrieve Job Parameters
# --------------------------------------------------------------------------------
DriverTable = get_widget_value('DriverTable','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')

# --------------------------------------------------------------------------------
# 1) Read from hashed file "hf_clm_fcts_reversals" (Scenario C)
# --------------------------------------------------------------------------------
df_hf_clm_fcts_reversals_read = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_hf_clm_fcts_reversals = (
    df_hf_clm_fcts_reversals_read
    .withColumn("CLCL_ID", trim(F.col("CLCL_ID")))    # Expression "trim(CLMCOB.CLM_ID)" approximated as trim of itself
    .select(
        F.col("CLCL_ID"),
        F.col("CLCL_CUR_STS"),
        F.col("CLCL_PAID_DT"),
        F.col("CLCL_ID_ADJ_TO"),
        F.col("CLCL_ID_ADJ_FROM")
    )
)

# --------------------------------------------------------------------------------
# 2) Read from hashed file "clm_nasco_dup_bypass" (Scenario C)
# --------------------------------------------------------------------------------
df_clm_nasco_dup_bypass_read = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
df_clm_nasco_dup_bypass = (
    df_clm_nasco_dup_bypass_read
    .select(
        F.col("CLM_ID")
    )
)

# --------------------------------------------------------------------------------
# 3) DB2Connector Stage "CD_MPPNG" (Read)
#    Database = IDS => use ids_secret_name
#    We then produce df_CD_MPPNG_QUERY
# --------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_cd_mppng = f"""
SELECT TRGT_DOMAIN_NM, SRC_CD, TRGT_CD_NM 
FROM {IDSOwner}.CD_MPPNG 
WHERE TRGT_DOMAIN_NM='EXPLANATION CODE PROVIDER ADJUSTMENT'
"""
df_CD_MPPNG_QUERY = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_cd_mppng)
    .load()
)

# --------------------------------------------------------------------------------
# 4) "Hashed_File_76" (Scenario A: intermediate hashed file)
#    Pattern: DB => Hashed_File_76 => Next
#    Key columns: TRGT_DOMAIN_NM, SRC_CD (both flagged PrimaryKey=true)
#    We apply the expressions from that stage:
#      TRGT_DOMAIN_NM => literal 'EXPLANATION CODE PROVIDER ADJUSTMENT'
#      SRC_CD => trim(...)  (but it references "FctsClmCOB.COB_CAR_RSN_CD_TX" which is not physically present.
#      Instead, we treat it as trimming the existing SRC_CD from the DB.)
#    Then deduplicate by (TRGT_DOMAIN_NM, SRC_CD).
# --------------------------------------------------------------------------------
df_Hashed_File_76_pre = (
    df_CD_MPPNG_QUERY
    .withColumn("TRGT_DOMAIN_NM", F.lit("EXPLANATION CODE PROVIDER ADJUSTMENT"))
    .withColumn("SRC_CD", trim(F.col("SRC_CD")))
    .select("TRGT_DOMAIN_NM", "SRC_CD", "TRGT_CD_NM")
)

df_Hashed_File_76 = dedup_sort(
    df_Hashed_File_76_pre,
    partition_cols=["TRGT_DOMAIN_NM", "SRC_CD"],
    sort_cols=[]
)

# --------------------------------------------------------------------------------
# 5) Read from hashed file "hf_clm_cob_prod_list" (Scenario C)
# --------------------------------------------------------------------------------
df_hf_clm_cob_prod_list_read = spark.read.parquet(f"{adls_path}/hf_clm_cob_prod_list.parquet")
df_hf_clm_cob_prod_list = df_hf_clm_cob_prod_list_read.select("prod_prefix")

# --------------------------------------------------------------------------------
# 6) ODBCConnector "FACETS_clm"
#    Read from facets_secret_name
# --------------------------------------------------------------------------------
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
query_facets_clm = f"""
SELECT clm.CLCL_ID, clm.PDPD_ID, clm.CLCL_LAST_ACT_DTM
FROM tempdb..{DriverTable} TMP,
     {FacetsOwner}.CMC_CLCL_CLAIM clm
WHERE TMP.CLM_ID = clm.CLCL_ID
"""
df_FACETS_clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_facets_clm)
    .load()
)
# --------------------------------------------------------------------------------
# 7) Transformer Stage "TrnsStripField"
#    - CLCL_ID => remove ctrl chars 10,13,9 => use strip_field()
#    - PDPD_ID => remove ctrl chars 10,13,9 => use strip_field()
#    - CLCL_LAST_ACT_DT => remove ctrl chars then substring(1,10)
# --------------------------------------------------------------------------------
df_TrnsStripField_temp = df_FACETS_clm.select(
    F.col("CLCL_ID").alias("CLCL_ID_raw"),
    F.col("PDPD_ID").alias("PDPD_ID_raw"),
    F.col("CLCL_LAST_ACT_DTM").alias("CLCL_LAST_ACT_DTM_raw")
)
df_TrnsStripField = (
    df_TrnsStripField_temp
    .withColumn("CLCL_ID", strip_field(F.col("CLCL_ID_raw")))
    .withColumn("PDPD_ID", strip_field(F.col("PDPD_ID_raw")))
    .withColumn("CLCL_LAST_ACT_DT", F.substring(strip_field(F.col("CLCL_LAST_ACT_DTM_raw")), 1, 10))
    .select(
        F.col("CLCL_ID").alias("CLCL_ID"),
        F.col("PDPD_ID").alias("PDPD_ID"),
        F.col("CLCL_LAST_ACT_DT").alias("CLCL_LAST_ACT_DT")
    )
)

# --------------------------------------------------------------------------------
# 8) ODBCConnector "Facets_COB" => 2 output queries
#    (facets_secret_name again)
# --------------------------------------------------------------------------------
jdbc_url_facets2, jdbc_props_facets2 = get_db_config(facets_secret_name)

query_CMC_CLCB_CL_COB = f"""
SELECT 
COB.CLCL_ID,
COB.MEME_CK,
COB.CLCB_COB_TYPE,
COB.CLCB_COB_AMT,
COB.CLCB_COB_DISALLOW,
COB.CLCB_COB_ALLOW,
COB.CLCB_COB_SANCTION,
COB.CLCB_COB_DED_AMT,
COB.CLCB_COB_COPAY_AMT,
COB.CLCB_COB_COINS_AMT,
C.CLCC_OC_REAS_CODE as CLCB_COB_REAS_CD
FROM 
tempdb..{DriverTable} TMP
INNER JOIN {FacetsOwner}.CMC_CLCB_CL_COB COB
     ON TMP.CLM_ID = COB.CLCL_ID
LEFT OUTER JOIN (
    select A.CLCL_ID,A.CLCC_OC_REAS_CODE 
    from {FacetsOwner}.CMC_CLCC_CARC_CODE A,
         (
           select CLCL_ID, MIN(CLCC_SEQ_NO) as CLCC_SEQ_NO , CLCC_OC_REAS_CODE 
           from {FacetsOwner}.CMC_CLCC_CARC_CODE
           group by CLCL_ID, CLCC_OC_REAS_CODE
         ) as B
    where A.CLCL_ID = B.CLCL_ID 
      and A.CLCC_SEQ_NO = B.CLCC_SEQ_NO 
      and A.CLCC_OC_REAS_CODE = B.CLCC_OC_REAS_CODE
) C
ON COB.CLCL_ID = C.CLCL_ID
"""

df_CMC_CLCB_CL_COB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets2)
    .options(**jdbc_props_facets2)
    .option("query", query_CMC_CLCB_CL_COB)
    .load()
)

query_CMC_CLSM_MED_SUPP = f"""
SELECT CLCL_ID,
       MEME_CK,
       CLSM_OC_ALLOW,
       CLSM_OC_DED_AMT,
       CLSM_OC_COINS_AMTM,
       CLSM_OC_COINS_AMTP,
       CLSM_OC_PAID_AMT,
       CLSM_OC_REAS_CD,
       CLSM_OC_GROUP_CD 
FROM {FacetsOwner}.CMC_CLSM_MED_SUPP A,
     tempdb..{DriverTable}  TMP 
WHERE TMP.CLM_ID = A.CLCL_ID
;
"""
df_CMC_CLSM_MED_SUPP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets2)
    .options(**jdbc_props_facets2)
    .option("query", query_CMC_CLSM_MED_SUPP)
    .load()
)

# --------------------------------------------------------------------------------
# 9) "from_FctClmExtr" (CHashedFileStage) with 2 input pins:
#    - "CMC_CLCB_CL_COB" => we write to "hf_clm_cob_info" 
#    - "CMC_CLSM_MED_SUPP" => we write to "hf_clm_cob_med_sup"
#    According to Scenario C, we create 2 parquet outputs.
# --------------------------------------------------------------------------------
df_from_FctClmExtr_clm_cob = df_CMC_CLCB_CL_COB.select(
    F.col("CLCL_ID").cast(StringType()).alias("CLCL_ID"),
    F.col("MEME_CK").cast(IntegerType()).alias("MEME_CK"),
    F.col("CLCB_COB_TYPE").cast(StringType()).alias("CLCB_COB_TYPE"),
    F.col("CLCB_COB_AMT").cast(DecimalType(38,10)).alias("CLCB_COB_AMT"),
    F.col("CLCB_COB_DISALLOW").cast(DecimalType(38,10)).alias("CLCB_COB_DISALLOW"),
    F.col("CLCB_COB_ALLOW").cast(DecimalType(38,10)).alias("CLCB_COB_ALLOW"),
    F.col("CLCB_COB_SANCTION").cast(DecimalType(38,10)).alias("CLCB_COB_SANCTION"),
    F.col("CLCB_COB_DED_AMT").cast(DecimalType(38,10)).alias("CLCB_COB_DED_AMT"),
    F.col("CLCB_COB_COPAY_AMT").cast(DecimalType(38,10)).alias("CLCB_COB_COPAY_AMT"),
    F.col("CLCB_COB_COINS_AMT").cast(DecimalType(38,10)).alias("CLCB_COB_COINS_AMT"),
    F.col("CLCB_COB_REAS_CD").cast(StringType()).alias("CLCB_COB_REAS_CD")
)

write_files(
    df_from_FctClmExtr_clm_cob,
    f"hf_clm_cob_info.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_from_FctClmExtr_med_sup = df_CMC_CLSM_MED_SUPP.select(
    F.col("CLCL_ID").cast(StringType()).alias("CLCL_ID"),
    F.col("MEME_CK").cast(IntegerType()).alias("MEME_CK"),
    F.col("CLSM_OC_ALLOW").cast(DecimalType(38,10)).alias("CLSM_OC_ALLOW"),
    F.col("CLSM_OC_DED_AMT").cast(DecimalType(38,10)).alias("CLSM_OC_DED_AMT"),
    F.col("CLSM_OC_COINS_AMTM").cast(DecimalType(38,10)).alias("CLSM_OC_COINS_AMTM"),
    F.col("CLSM_OC_COINS_AMTP").cast(DecimalType(38,10)).alias("CLSM_OC_COINS_AMTP"),
    F.col("CLSM_OC_PAID_AMT").cast(DecimalType(38,10)).alias("CLSM_OC_PAID_AMT"),
    F.col("CLSM_OC_REAS_CD").cast(StringType()).alias("CLSM_OC_REAS_CD"),
    F.col("CLSM_OC_GROUP_CD").cast(StringType()).alias("CLSM_OC_GROUP_CD")
)

write_files(
    df_from_FctClmExtr_med_sup,
    f"hf_clm_cob_med_sup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Next stages read "hf_clm_cob_med_sup" => "med_sup" link,
# and "hf_clm_cob_info" => "clm_cob" link, for the "BusinessRules" transformer.
# --------------------------------------------------------------------------------
df_hf_clm_cob_med_sup_read = spark.read.parquet(f"{adls_path}/hf_clm_cob_med_sup.parquet")
df_from_FctClmExtr_med_sup_out = df_hf_clm_cob_med_sup_read.select(
    "CLCL_ID",
    "MEME_CK",
    "CLSM_OC_ALLOW",
    "CLSM_OC_DED_AMT",
    "CLSM_OC_COINS_AMTM",
    "CLSM_OC_COINS_AMTP",
    "CLSM_OC_PAID_AMT",
    "CLSM_OC_REAS_CD",
    "CLSM_OC_GROUP_CD"
)

df_hf_clm_cob_info_read = spark.read.parquet(f"{adls_path}/hf_clm_cob_info.parquet")
df_from_FctClmExtr_clm_cob_out = df_hf_clm_cob_info_read.select(
    "CLCL_ID",
    "MEME_CK",
    "CLCB_COB_TYPE",
    "CLCB_COB_AMT",
    "CLCB_COB_DISALLOW",
    "CLCB_COB_ALLOW",
    "CLCB_COB_SANCTION",
    "CLCB_COB_DED_AMT",
    "CLCB_COB_COPAY_AMT",
    "CLCB_COB_COINS_AMT",
    "CLCB_COB_REAS_CD"
)

# --------------------------------------------------------------------------------
# 10) ODBCConnector "FACETS2" with 2 outputs => "CMC_CDDC_DNLI_COB" & "CMC_CDCB_LI_COB"
# --------------------------------------------------------------------------------
jdbc_url_facets3, jdbc_props_facets3 = get_db_config(facets_secret_name)

query_CMC_CDDC_DNLI_COB = f"""
SELECT distinct
A.CLCL_ID,
A.CDDC_COB_TYPE
FROM tempdb..{DriverTable} TMP
INNER JOIN {FacetsOwner}.CMC_CDDC_DNLI_COB A
   ON TMP.CLM_ID = A.CLCL_ID
group by A.CLCL_ID, A.CDDC_COB_TYPE
"""

df_CMC_CDDC_DNLI_COB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets3)
    .options(**jdbc_props_facets3)
    .option("query", query_CMC_CDDC_DNLI_COB)
    .load()
)

query_CMC_CDCB_LI_COB = f"""
SELECT CLCL_ID,CDCB_COB_TYPE
FROM tempdb..{DriverTable} TMP,
     {FacetsOwner}.CMC_CDCB_LI_COB A
WHERE TMP.CLM_ID = A.CLCL_ID
"""
df_CMC_CDCB_LI_COB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets3)
    .options(**jdbc_props_facets3)
    .option("query", query_CMC_CDCB_LI_COB)
    .load()
)

# --------------------------------------------------------------------------------
# 11) HashedFile "hf_clm_cob_cmc_cdcb_li_cob" => input from "CMC_CDCB_LI_COB" => Scenario C => write parquet
# --------------------------------------------------------------------------------
df_hf_clm_cob_cmc_cdcb_li_cob_in = df_CMC_CDCB_LI_COB.select(
    F.col("CLCL_ID").cast(StringType()).alias("CLCL_ID"),
    F.col("CDCB_COB_TYPE").cast(StringType()).alias("CDCB_COB_TYPE")
)
write_files(
    df_hf_clm_cob_cmc_cdcb_li_cob_in,
    f"hf_clm_cob_cmc_cdcb_li_cob.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Then read it for next
df_hf_clm_cob_cmc_cdcb_li_cob_read = spark.read.parquet(f"{adls_path}/hf_clm_cob_cmc_cdcb_li_cob.parquet")
df_hf_clm_cob_cmc_cdcb_li_cob = df_hf_clm_cob_cmc_cdcb_li_cob_read.select("CLCL_ID","CDCB_COB_TYPE")

# --------------------------------------------------------------------------------
# 12) HashedFile "hf_clm_cob_cmc_cddc_dnli_cob" => input from "CMC_CDDC_DNLI_COB" => Scenario C => write parquet
# --------------------------------------------------------------------------------
df_hf_clm_cob_cmc_cddc_dnli_cob_in = df_CMC_CDDC_DNLI_COB.select(
    F.col("CLCL_ID").cast(StringType()).alias("CLCL_ID"),
    F.col("CDDC_COB_TYPE").cast(StringType()).alias("CDDC_COB_TYPE")
)

write_files(
    df_hf_clm_cob_cmc_cddc_dnli_cob_in,
    f"hf_clm_cob_cmc_cddc_dnli_cob.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_hf_clm_cob_cmc_cddc_dnli_cob_read = spark.read.parquet(f"{adls_path}/hf_clm_cob_cmc_cddc_dnli_cob.parquet")
df_hf_clm_cob_cmc_cddc_dnli_cob = df_hf_clm_cob_cmc_cddc_dnli_cob_read.select("CLCL_ID","CDDC_COB_TYPE")

# --------------------------------------------------------------------------------
# 13) Transformer "BusinessRules"
#    This stage has a bunch of input links (all "LookupLink" except "Extract" primary),
#    with complex rule to produce 2 outputs: "current" => Nov2004PresentCRF, "old" => PriorNov2004.
# --------------------------------------------------------------------------------
df_Extract = df_TrnsStripField.select(
    "CLCL_ID",
    "PDPD_ID",
    "CLCL_LAST_ACT_DT"
).alias("Extract")

df_product_list_in = df_hf_clm_cob_prod_list.alias("product_list_in")
df_clm_cob = df_from_FctClmExtr_clm_cob_out.alias("clm_cob")
df_med_sup = df_from_FctClmExtr_med_sup_out.alias("med_sup")
df_cdcb_li = df_hf_clm_cob_cmc_cdcb_li_cob.alias("cdcb_li")
df_cddc_dnli = df_hf_clm_cob_cmc_cddc_dnli_cob.alias("cddc_dnli")

# Perform left joins in Spark properly:
df_BusinessRules_joined = (
    df_Extract
    .join(df_product_list_in, F.col("Extract.PDPD_ID").substr(F.lit(1),F.lit(2)) == F.col("product_list_in.prod_prefix"), how="left")
    .join(df_clm_cob, F.col("Extract.CLCL_ID") == F.col("clm_cob.CLCL_ID"), how="left")
    .join(df_med_sup, F.col("Extract.CLCL_ID") == F.col("med_sup.CLCL_ID"), how="left")
    .join(df_cdcb_li, F.col("Extract.CLCL_ID") == F.col("cdcb_li.CLCL_ID"), how="left")
    .join(df_cddc_dnli, F.col("Extract.CLCL_ID") == F.col("cddc_dnli.CLCL_ID"), how="left")
)

# Define stage variables with Spark logic:
#   SetPrefix, COBType, ClmId
# We interpret them in a single expression column by column.
def business_rules_transform(row):
    # We'll do our best to replicate the logic inline.
    # "SetPrefix" = If IsNull(product_list_in.prod_prefix) then "CLCB" else "CLSM"
    # "COBType" follows the big condition
    # "ClmId" = trim(Extract.CLCL_ID)
    # We'll do them in a UDF or a row-based approach to avoid partial misses.
    Extract_CLCL_ID = row["Extract.CLCL_ID"]
    product_list_in_prod_prefix = row["product_list_in.prod_prefix"]
    clm_cob_CLCL_ID = row["clm_cob.CLCL_ID"]
    clm_cob_CLCB_COB_TYPE = row["clm_cob.CLCB_COB_TYPE"]
    med_sup_CLCL_ID = row["med_sup.CLCL_ID"]
    cdcb_li_CLCL_ID = row["cdcb_li.CLCL_ID"]
    cdcb_li_CDCB_COB_TYPE = row["cdcb_li.CDCB_COB_TYPE"]
    cddc_dnli_CLCL_ID = row["cddc_dnli.CLCL_ID"]
    cddc_dnli_CDDC_COB_TYPE = row["cddc_dnli.CDDC_COB_TYPE"]
    Extract_CLCL_LAST_ACT_DT = row["Extract.CLCL_LAST_ACT_DT"]

    # ClmId
    ClmId = trim(Extract_CLCL_ID) if Extract_CLCL_ID is not None else None

    # SetPrefix
    SetPrefix = "CLCB" if product_list_in_prod_prefix is None else "CLSM"

    # COBType logic
    def trim_nonnull(x):
        return trim(x) if x is not None else ""

    dt_str = Extract_CLCL_LAST_ACT_DT if Extract_CLCL_LAST_ACT_DT else ""
    # big if logic:
    # IF (dt_str <= "2004-11-05") THEN
    #    if len(trim(clm_cob_CLCB_COB_TYPE))>0 => trim(...) else "NA"
    # ELSE
    #   if SetPrefix=='CLCB' then ...
    #   else if SetPrefix=='CLSM' then ...
    #   etc.
    # Detailed as in the expression:
    # IF (Extract.CLCL_LAST_ACT_DT[1, 10] <= "2004-11-05") THEN
    #     If Len(trim(clm_cob.CLCB_COB_TYPE))>0 then trim(...) Else "NA"
    # ELSE
    #  IF SetPrefix = 'CLCB' then
    #   IF clm_cob.CLCL_ID not null => if len(trim(clm_cob.CLCB_COB_TYPE))>0 => that trim => else if cdcb_li not null => trim(...) ...
    #  ELSE => if med_sup not null => "M" else "NA"

    # Compare date string
    COBType = None
    if dt_str <= "2004-11-05":
        # clm_cob must not be null to attempt the length
        if clm_cob_CLCL_ID is not None and len(trim_nonnull(clm_cob_CLCB_COB_TYPE)) > 0:
            COBType = trim_nonnull(clm_cob_CLCB_COB_TYPE)
        else:
            COBType = "NA"
    else:
        if SetPrefix == "CLCB":
            if clm_cob_CLCL_ID is not None:
                if len(trim_nonnull(clm_cob_CLCB_COB_TYPE)) > 0:
                    COBType = trim_nonnull(clm_cob_CLCB_COB_TYPE)
                else:
                    if cdcb_li_CLCL_ID is not None and len(trim_nonnull(cdcb_li_CDCB_COB_TYPE)) > 0:
                        COBType = trim_nonnull(cdcb_li_CDCB_COB_TYPE)
                    else:
                        if cddc_dnli_CLCL_ID is not None and len(trim_nonnull(cddc_dnli_CDDC_COB_TYPE)) > 0:
                            COBType = trim_nonnull(cddc_dnli_CDDC_COB_TYPE)
                        else:
                            COBType = "UNK"
            else:
                COBType = "NA"
        else:
            # 'CLSM'
            if med_sup_CLCL_ID is not None:
                COBType = "M"
            else:
                COBType = "NA"

    return (SetPrefix, COBType, ClmId)

schema_business_rules = StructType([
    StructField("SetPrefix", StringType()),
    StructField("COBType", StringType()),
    StructField("ClmId", StringType())
])

udf_business_rules = F.udf(business_rules_transform, schema_business_rules)

df_BusinessRules_vars = df_BusinessRules_joined.withColumn("sv", udf_business_rules(F.struct([df_BusinessRules_joined[x] for x in df_BusinessRules_joined.columns])))

df_BusinessRules_result = df_BusinessRules_vars.select(
    *[F.col(c) for c in df_BusinessRules_joined.columns],
    F.col("sv.SetPrefix").alias("SetPrefix"),
    F.col("sv.COBType").alias("COBType"),
    F.col("sv.ClmId").alias("ClmId")
)

# We now produce 2 output DataFrames "current" and "old" with the given constraints.
# "current" constraint:
#   (UPCASE(PDPD_ID[1,2]) in ('TP','TC','MV','MG','MS') and med_sup not null) OR
#   ((not in that set) and clm_cob not null)
# We'll interpret in Spark:
def is_current(row):
    pdprefix = (row["Extract.PDPD_ID"] or "")[:2].upper()
    med_sup_id = row["med_sup.CLCL_ID"]
    clm_cob_id = row["clm_cob.CLCL_ID"]
    if (
        ((pdprefix in ['TP','TC','MV','MG','MS']) and med_sup_id is not None)
        or ((pdprefix not in ['TP','TC','MV','MG','MS']) and clm_cob_id is not None)
    ):
        return True
    return False

udf_is_current = F.udf(lambda r: is_current(r), BooleanType())

df_BusinessRules_flagged = df_BusinessRules_result.withColumn("is_current", udf_is_current(F.struct([df_BusinessRules_result[x] for x in df_BusinessRules_result.columns])))

# "old" constraint:
#  (ISNULL(clm_cob.MEME_CK)=false) and Extract.CLCL_LAST_ACT_DT <= '2004-11-05'
def is_old(row):
    clm_cob_meme_ck = row["clm_cob.MEME_CK"]
    dt_str = row["Extract.CLCL_LAST_ACT_DT"] or ""
    if clm_cob_meme_ck is not None and dt_str <= "2004-11-05":
        return True
    return False

udf_is_old = F.udf(lambda r: is_old(r), BooleanType())

df_BusinessRules_flagged2 = df_BusinessRules_flagged.withColumn("is_old", udf_is_old(F.struct([df_BusinessRules_flagged[x] for x in df_BusinessRules_flagged.columns])))

# Then we produce columns for each link. We'll do them in separate dataframes:
# Fields for "current" link
df_current = df_BusinessRules_flagged2.filter("is_current").select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),  # "CurrentDate" => current_date()
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS;"), F.col("ClmId"), F.lit(";"), F.col("COBType")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_COB_SK"),
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),      # "SrcSysCdSk" job param usage?
    F.col("ClmId").alias("CLM_ID"),
    F.col("COBType").alias("CLM_COB_TYP_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.when(
        F.col("SetPrefix")=="CLCB",
        F.lit("NA")
    ).otherwise(
        F.when(
            (F.col("med_sup.CLSM_OC_GROUP_CD").isNull()) | (F.length(F.trim(F.col("med_sup.CLSM_OC_GROUP_CD")))<1),
            F.lit("NA")
        ).otherwise(F.col("med_sup.CLSM_OC_GROUP_CD"))
    ).alias("CLM_COB_LIAB_TYP_CD"),
    F.when(
        F.col("SetPrefix")=="CLCB",
        F.when(F.col("clm_cob.CLCB_COB_ALLOW").isNull(), F.lit(0.00)).otherwise(F.abs(F.col("clm_cob.CLCB_COB_ALLOW")))
    ).otherwise(
        F.when(F.col("med_sup.CLSM_OC_ALLOW").isNull(), F.lit(0.00)).otherwise(F.abs(F.col("med_sup.CLSM_OC_ALLOW")))
    ).alias("ALW_AMT"),
    F.when(
        F.col("SetPrefix")=="CLCB",
        F.when(F.col("clm_cob.CLCB_COB_COPAY_AMT").isNull(), F.lit(0.00)).otherwise(F.abs(F.col("clm_cob.CLCB_COB_COPAY_AMT")))
    ).otherwise(F.lit(0.00)).alias("COPAY_AMT"),
    F.when(
        F.col("SetPrefix")=="CLCB",
        F.when(F.col("clm_cob.CLCB_COB_DED_AMT").isNull(), F.lit(0.00)).otherwise(F.abs(F.col("clm_cob.CLCB_COB_DED_AMT")))
    ).otherwise(
        F.when(F.col("med_sup.CLSM_OC_DED_AMT").isNull(), F.lit(0.00)).otherwise(F.abs(F.col("med_sup.CLSM_OC_DED_AMT")))
    ).alias("DEDCT_AMT"),
    F.when(
        F.col("SetPrefix")=="CLCB",
        F.when(F.col("clm_cob.CLCB_COB_DISALLOW").isNull(), F.lit(0.00)).otherwise(F.abs(F.col("clm_cob.CLCB_COB_DISALLOW")))
    ).otherwise(F.lit(0.00)).alias("DSALW_AMT"),
    F.when(
        F.col("SetPrefix")=="CLCB",
        F.when(F.col("clm_cob.CLCB_COB_COINS_AMT").isNull(), F.lit(0.00)).otherwise(F.abs(F.col("clm_cob.CLCB_COB_COINS_AMT")))
    ).otherwise(
        F.when(F.col("med_sup.CLSM_OC_COINS_AMTM").isNull(), F.lit(0.00)).otherwise(F.abs(F.col("med_sup.CLSM_OC_COINS_AMTM")))
    ).alias("MED_COINS_AMT"),
    F.when(
        F.col("SetPrefix")=="CLCB",
        F.lit(0.00)
    ).otherwise(
        F.when(F.col("med_sup.CLSM_OC_COINS_AMTP").isNull(), F.lit(0.00)).otherwise(F.abs(F.col("med_sup.CLSM_OC_COINS_AMTP")))
    ).alias("MNTL_HLTH_COINS_AMT"),
    F.when(
        F.col("SetPrefix")=="CLCB",
        F.when(F.col("clm_cob.CLCB_COB_AMT").isNull(), F.lit(0.00)).otherwise(F.abs(F.col("clm_cob.CLCB_COB_AMT")))
    ).otherwise(
        F.when(F.col("med_sup.CLSM_OC_PAID_AMT").isNull(), F.lit(0.00)).otherwise(F.abs(F.col("med_sup.CLSM_OC_PAID_AMT")))
    ).alias("PD_AMT"),
    F.when(
        F.col("SetPrefix")=="CLCB",
        F.when(F.col("clm_cob.CLCB_COB_SANCTION").isNull(), F.lit(0.00)).otherwise(F.abs(F.col("clm_cob.CLCB_COB_SANCTION")))
    ).otherwise(F.lit(0.00)).alias("SANC_AMT"),
    F.when(
        F.col("SetPrefix")=="CLCB",
        F.col("clm_cob.CLCB_COB_REAS_CD")
    ).otherwise(F.col("med_sup.CLSM_OC_REAS_CD")).alias("COB_CAR_RSN_CD_TX"),
    F.when(
        F.col("SetPrefix")=="CLCB",
        F.col("clm_cob.CLCB_COB_REAS_CD")
    ).otherwise(F.col("med_sup.CLSM_OC_REAS_CD")).alias("COB_CAR_RSN_TX")
)

df_current_ordered = df_current.select(
    "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN","FIRST_RECYC_DT","ERR_CT","RECYCLE_CT","SRC_SYS_CD","PRI_KEY_STRING","CLM_COB_SK","SRC_SYS_CD_SK","CLM_ID","CLM_COB_TYP_CD","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","CLM_SK","CLM_COB_LIAB_TYP_CD","ALW_AMT","COPAY_AMT","DEDCT_AMT","DSALW_AMT","MED_COINS_AMT","MNTL_HLTH_COINS_AMT","PD_AMT","SANC_AMT","COB_CAR_RSN_CD_TX","COB_CAR_RSN_TX"
)

df_old = df_BusinessRules_flagged2.filter("is_old").select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS;"), F.col("ClmId"), F.lit(";"), F.col("COBType")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_COB_SK"),
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("ClmId").alias("CLM_ID"),
    F.col("COBType").alias("CLM_COB_TYP_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.lit("NA").alias("CLM_COB_LIAB_TYP_CD"),
    F.abs(F.col("clm_cob.CLCB_COB_ALLOW")).alias("ALW_AMT"),
    F.abs(F.col("clm_cob.CLCB_COB_COPAY_AMT")).alias("COPAY_AMT"),
    F.abs(F.col("clm_cob.CLCB_COB_DED_AMT")).alias("DEDCT_AMT"),
    F.abs(F.col("clm_cob.CLCB_COB_DISALLOW")).alias("DSALW_AMT"),
    F.abs(F.col("clm_cob.CLCB_COB_COINS_AMT")).alias("MED_COINS_AMT"),
    F.lit(0.00).alias("MNTL_HLTH_COINS_AMT"),
    F.when(F.col("clm_cob.CLCB_COB_AMT").isNull(), F.lit(0.00)).otherwise(F.abs(F.col("clm_cob.CLCB_COB_AMT"))).alias("PD_AMT"),
    F.abs(F.col("clm_cob.CLCB_COB_SANCTION")).alias("SANC_AMT"),
    F.col("clm_cob.CLCB_COB_REAS_CD").alias("COB_CAR_RSN_CD_TX"),
    F.lit(None).alias("COB_CAR_RSN_TX")
)

df_old_ordered = df_old.select(
    "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN","FIRST_RECYC_DT","ERR_CT","RECYCLE_CT","SRC_SYS_CD","PRI_KEY_STRING","CLM_COB_SK","SRC_SYS_CD_SK","CLM_ID","CLM_COB_TYP_CD","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","CLM_SK","CLM_COB_LIAB_TYP_CD","ALW_AMT","COPAY_AMT","DEDCT_AMT","DSALW_AMT","MED_COINS_AMT","MNTL_HLTH_COINS_AMT","PD_AMT","SANC_AMT","COB_CAR_RSN_CD_TX","COB_CAR_RSN_TX"
)

# --------------------------------------------------------------------------------
# 14) "PriorNov2004" hashed file => scenario C => we write df_old_ordered => then read => next stage "Collector"
# --------------------------------------------------------------------------------
write_files(
    df_old_ordered,
    f"hf_facets_cob_PriorNov2004CRF.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_PriorNov2004_read = df_old_ordered  # We can directly keep it in memory as if "read" (no actual read needed).

# --------------------------------------------------------------------------------
# 15) "Nov2004PresentCRF" hashed file => scenario C => write df_current_ordered => then read for next stage
# --------------------------------------------------------------------------------
write_files(
    df_current_ordered,
    f"hf_facets_cob_Nov2004CRF.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_Nov2004PresentCRF_read = df_current_ordered

# --------------------------------------------------------------------------------
# 16) "Collector" merges CRF1 (Nov2004PresentCRF) and CRF2 (PriorNov2004)
# --------------------------------------------------------------------------------
df_collector_merged = df_Nov2004PresentCRF_read.unionByName(df_PriorNov2004_read)

# --------------------------------------------------------------------------------
# 17) "hf_fcts_cob_All" hashed file => scenario C => write the collector => read for next stage
# --------------------------------------------------------------------------------
write_files(
    df_collector_merged,
    f"hf_fcts_cob_All.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_fcts_cob_All_read = df_collector_merged

# --------------------------------------------------------------------------------
# 18) "Transformer_74"
#     InputPins: 
#       - "CD_MPPNG" (left join with df_Hashed_File_76) 
#       - "FctsClmCOB" => the data from "hf_fcts_cob_All"
#     We'll replicate the left join on TRGT_DOMAIN_NM and SRC_CD, plus transform columns.
# --------------------------------------------------------------------------------

# We have df_Hashed_File_76 from earlier step (deduplicated). We'll call it df_CD_MPPNG
df_CD_MPPNG = df_Hashed_File_76.alias("CD_MPPNG")

df_FctsClmCOB = df_hf_fcts_cob_All_read.alias("FctsClmCOB")
join_cond_74 = [
    (F.lit("EXPLANATION CODE PROVIDER ADJUSTMENT") == F.col("CD_MPPNG.TRGT_DOMAIN_NM")),
    (F.trim(F.col("FctsClmCOB.COB_CAR_RSN_CD_TX")) == F.col("CD_MPPNG.SRC_CD"))
]

df_Transformer_74_joined = df_FctsClmCOB.join(df_CD_MPPNG, join_cond_74, how="left")

# Then apply the column expressions => "CLMCOB" output link
df_Transformer_74 = df_Transformer_74_joined.select(
    F.col("FctsClmCOB.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("FctsClmCOB.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("FctsClmCOB.DISCARD_IN").alias("DISCARD_IN"),
    F.col("FctsClmCOB.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FctsClmCOB.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("FctsClmCOB.ERR_CT").alias("ERR_CT"),
    F.col("FctsClmCOB.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("FctsClmCOB.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("FctsClmCOB.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("FctsClmCOB.CLM_COB_SK").alias("CLM_COB_SK"),
    F.col("FctsClmCOB.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.trim(F.col("FctsClmCOB.CLM_ID")).alias("CLM_ID"),
    F.col("FctsClmCOB.CLM_COB_TYP_CD").alias("CLM_COB_TYP_CD"),
    F.col("FctsClmCOB.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("FctsClmCOB.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("FctsClmCOB.CLM_SK").alias("CLM_SK"),
    F.col("FctsClmCOB.CLM_COB_LIAB_TYP_CD").alias("CLM_COB_LIAB_TYP_CD_SK"),
    F.col("FctsClmCOB.ALW_AMT").alias("ALW_AMT"),
    F.col("FctsClmCOB.COPAY_AMT").alias("COPAY_AMT"),
    F.col("FctsClmCOB.DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("FctsClmCOB.DSALW_AMT").alias("DSALW_AMT"),
    F.col("FctsClmCOB.MED_COINS_AMT").alias("MED_COINS_AMT"),
    F.col("FctsClmCOB.MNTL_HLTH_COINS_AMT").alias("MNTL_HLTH_COINS_AMT"),
    F.col("FctsClmCOB.PD_AMT").alias("PD_AMT"),
    F.col("FctsClmCOB.SANC_AMT").alias("SANC_AMT"),
    F.col("FctsClmCOB.COB_CAR_RSN_CD_TX").alias("COB_CAR_RSN_CD_TX"),
    F.when(F.col("CD_MPPNG.TRGT_CD_NM").isNull(), F.lit(None)).otherwise(F.trim(F.col("CD_MPPNG.TRGT_CD_NM"))).alias("COB_CAR_RSN_TX")
)

# --------------------------------------------------------------------------------
# 19) "Apply_reversals"
#     PrimaryLink => "CLMCOB"
#     Lookup => "fcts_reversals" on CLCL_ID
#     Lookup => "nasco_dup_lkup" on CLM_ID
#     2 outputs => "COB" (isNull(nasco_dup_lkup.CLM_ID)) => Link_Collector
#                => "Reversals" (not null fcts_reversals.CLCL_ID and CLCL_CUR_STS in ["89","91","99"])
# --------------------------------------------------------------------------------
df_CLMCOB_in = df_Transformer_74.alias("CLMCOB")
df_fcts_reversals_in = df_hf_clm_fcts_reversals.alias("fcts_reversals")
df_nasco_dup_lkup_in = df_clm_nasco_dup_bypass.alias("nasco_dup_lkup")

df_Apply_reversals_join = (
    df_CLMCOB_in
    .join(df_fcts_reversals_in, F.trim(F.col("CLMCOB.CLM_ID")) == F.col("fcts_reversals.CLCL_ID"), how="left")
    .join(df_nasco_dup_lkup_in, F.col("CLMCOB.CLM_ID") == F.col("nasco_dup_lkup.CLM_ID"), how="left")
)

# "COB" => isNull(nasco_dup_lkup.CLM_ID)
df_COB = df_Apply_reversals_join.filter("nasco_dup_lkup.CLM_ID is null").select(
    F.col("CLMCOB.JOB_EXCTN_RCRD_ERR_SK"),
    F.col("CLMCOB.INSRT_UPDT_CD"),
    F.col("CLMCOB.DISCARD_IN"),
    F.col("CLMCOB.PASS_THRU_IN"),
    F.col("CLMCOB.FIRST_RECYC_DT"),
    F.col("CLMCOB.ERR_CT"),
    F.col("CLMCOB.RECYCLE_CT"),
    F.col("CLMCOB.SRC_SYS_CD"),
    F.col("CLMCOB.PRI_KEY_STRING"),
    F.col("CLMCOB.CLM_COB_SK"),
    F.col("CLMCOB.SRC_SYS_CD_SK"),
    F.col("CLMCOB.CLM_ID"),
    F.col("CLMCOB.CLM_COB_TYP_CD"),
    F.col("CLMCOB.CRT_RUN_CYC_EXCTN_SK"),
    F.col("CLMCOB.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLMCOB.CLM_SK"),
    F.col("CLMCOB.CLM_COB_LIAB_TYP_CD_SK"),
    F.col("CLMCOB.ALW_AMT"),
    F.col("CLMCOB.COPAY_AMT"),
    F.col("CLMCOB.DEDCT_AMT"),
    F.col("CLMCOB.DSALW_AMT"),
    F.col("CLMCOB.MED_COINS_AMT"),
    F.col("CLMCOB.MNTL_HLTH_COINS_AMT"),
    F.col("CLMCOB.PD_AMT"),
    F.col("CLMCOB.SANC_AMT"),
    F.col("CLMCOB.COB_CAR_RSN_CD_TX"),
    F.col("CLMCOB.COB_CAR_RSN_TX")
)

# "Reversals" => isNull(fcts_reversals.CLCL_ID)=false and CLCL_CUR_STS in ["89","91","99"]
df_Reversals = df_Apply_reversals_join.filter(
    "(fcts_reversals.CLCL_ID is not null) AND (fcts_reversals.CLCL_CUR_STS in ('89','91','99'))"
).select(
    F.col("CLMCOB.JOB_EXCTN_RCRD_ERR_SK"),
    F.col("CLMCOB.INSRT_UPDT_CD"),
    F.col("CLMCOB.DISCARD_IN"),
    F.col("CLMCOB.PASS_THRU_IN"),
    F.col("CLMCOB.FIRST_RECYC_DT"),
    F.col("CLMCOB.ERR_CT"),
    F.col("CLMCOB.RECYCLE_CT"),
    F.col("CLMCOB.SRC_SYS_CD"),
    F.col("CLMCOB.PRI_KEY_STRING"),
    F.col("CLMCOB.CLM_COB_SK"),
    F.col("CLMCOB.SRC_SYS_CD_SK"),
    F.expr("CLMCOB.CLM_ID || 'R'").alias("CLM_ID"),
    F.col("CLMCOB.CLM_COB_TYP_CD"),
    F.col("CLMCOB.CRT_RUN_CYC_EXCTN_SK"),
    F.col("CLMCOB.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.col("CLMCOB.CLM_COB_LIAB_TYP_CD_SK"),
    F.expr("-1 * ALW_AMT").alias("ALW_AMT"),
    F.expr("-1 * COPAY_AMT").alias("COPAY_AMT"),
    F.expr("-1 * DEDCT_AMT").alias("DEDCT_AMT"),
    F.expr("-1 * DSALW_AMT").alias("DSALW_AMT"),
    F.expr("-1 * MED_COINS_AMT").alias("MED_COINS_AMT"),
    F.expr("-1 * MNTL_HLTH_COINS_AMT").alias("MNTL_HLTH_COINS_AMT"),
    F.expr("-1 * PD_AMT").alias("PD_AMT"),
    F.expr("-1 * SANC_AMT").alias("SANC_AMT"),
    F.col("CLMCOB.COB_CAR_RSN_CD_TX"),
    F.col("CLMCOB.COB_CAR_RSN_TX")
)

df_link_collector_merged = df_COB.unionByName(df_Reversals)

# --------------------------------------------------------------------------------
# 20) "Link_Collector" => merges "COB" and "Reversals" => output => "TransformIt"
# --------------------------------------------------------------------------------
df_TransformIt = df_link_collector_merged

# --------------------------------------------------------------------------------
# 21) "Snapshot" => receives "TransformIt" primary => outputs "AllCol" (C126P2) and "RowCount" (V0S115P3) and "Transform" (C126P1)
# --------------------------------------------------------------------------------
df_Snapshot_in = df_TransformIt.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_COB_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_COB_TYP_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    F.col("CLM_COB_LIAB_TYP_CD_SK").alias("CLM_COB_LIAB_TYP_CD"),  # per output pin: char(20)
    "ALW_AMT",
    "COPAY_AMT",
    "DEDCT_AMT",
    "DSALW_AMT",
    "MED_COINS_AMT",
    "MNTL_HLTH_COINS_AMT",
    "PD_AMT",
    "SANC_AMT",
    "COB_CAR_RSN_CD_TX",
    "COB_CAR_RSN_TX"
)

# Outputs:
#  A) AllCol => (C126P2)
df_AllCol = df_Snapshot_in.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_COB_TYP_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_COB_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK"),
    F.col("CLM_COB_LIAB_TYP_CD"),
    F.col("ALW_AMT"),
    F.col("COPAY_AMT"),
    F.col("DEDCT_AMT"),
    F.col("DSALW_AMT"),
    F.col("MED_COINS_AMT"),
    F.col("MNTL_HLTH_COINS_AMT"),
    F.col("PD_AMT"),
    F.col("SANC_AMT"),
    F.col("COB_CAR_RSN_CD_TX"),
    F.col("COB_CAR_RSN_TX")
)

#  B) RowCount => (V0S115P3)
df_RowCount = df_Snapshot_in.select(
    F.col("CLM_ID"),
    F.col("CLM_COB_TYP_CD"),
    F.col("ALW_AMT"),
    F.col("PD_AMT")
)

#  C) "Transform" => (C126P1)
df_Transform = df_Snapshot_in.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.when(F.trim(F.col("CLM_COB_TYP_CD"))=="UNK", F.lit("UN")).otherwise(F.col("CLM_COB_TYP_CD")).alias("CLM_COB_TYP_CD")
)

# --------------------------------------------------------------------------------
# 22) Next "Transformer" => input "RowCount"
#     Stage variables => "ITSHost", "ITSCOBType", "svClmCobTypCdSk" => calls GetFkeyCodes(…)
# --------------------------------------------------------------------------------
# We have no direct reference to that Python UDF besides "GetFkeyCodes". We assume it's predefined.
def transform_ITS(row):
    # ITSHost = If row.CLM_COB_TYP_CD[0]=='*' => 'Y' else 'N'
    # Then ITSCOBType => if ITSHost=='Y' => row.CLM_COB_TYP_CD[1] else 'NA'
    # Then svClmCobTypCdSk => if ITSHost=='N' => GetFkeyCodes('FACETS',0,'CLAIM COB', row.CLM_COB_TYP_CD,'X') else GetFkeyCodes('FIT',0,'CLAIM COB', ITSCOBType,'X')
    ctype = row["CLM_COB_TYP_CD"] or ""
    host = 'Y' if (len(ctype)>0 and ctype[0]=='*') else 'N'
    if host=='Y':
        itstype = ctype[1] if len(ctype)>1 else ''
    else:
        itstype = 'NA'
    if host=='N':
        val = GetFkeyCodes('FACETS', 0, 'CLAIM COB', ctype, 'X')
    else:
        val = GetFkeyCodes('FIT', 0, 'CLAIM COB', itstype, 'X')
    return (host, itstype, val)

schema_ITS = StructType([
    StructField("ITSHost", StringType()),
    StructField("ITSCOBType", StringType()),
    StructField("svClmCobTypCdSk", StringType()),
])

udf_ITS = F.udf(transform_ITS, schema_ITS)

df_Transformer_in = df_RowCount.alias("RowCount")

df_Transformer_vars = df_Transformer_in.withColumn("sv", udf_ITS(F.struct([df_Transformer_in[x] for x in df_Transformer_in.columns])))

df_Transformer_out = df_Transformer_vars.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("RowCount.CLM_ID"),
    F.col("sv.svClmCobTypCdSk").alias("CLM_COB_TYP_CD_SK"),
    F.col("RowCount.ALW_AMT"),
    F.col("RowCount.PD_AMT")
)

# --------------------------------------------------------------------------------
# Output => "Load" => B_CLM_COB
# --------------------------------------------------------------------------------
df_B_CLM_COB = df_Transformer_out

# --------------------------------------------------------------------------------
# 23) "B_CLM_COB" => CSeqFileStage => writes a delimited file to path: "load/B_CLM_COB.FACETS.dat.#RunID#"
# --------------------------------------------------------------------------------
# Define writing with the original columns in the same order
write_files(
    df_B_CLM_COB.select("SRC_SYS_CD_SK","CLM_ID","CLM_COB_TYP_CD_SK","ALW_AMT","PD_AMT"),
    f"{adls_path}/load/B_CLM_COB.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# 24) "ClmCobPK" Shared Container => input pins "Transform" (C126P1) and "AllCol" (C126P2), output => "Key"
#    We call the shared container as a function:
# --------------------------------------------------------------------------------
params_ClmCobPK = {}
# The container expects two inputs plus parameters. We'll mimic the call:
df_input_transform = df_Transform
df_input_allcol = df_AllCol
output_df_key = None

# Call the shared container function (as per instructions):
# The container has 2 inputs, 1 output
# Example usage:
# params = {"mod_value":"2","greater_than":<envVar>}
# output_df_1 = ClmCobPK(input_df_1, input_df_2, params)
# But we have it returning 1 DF "Key" for this particular container.
# We do not know the exact signature, so let's replicate:
output_df_key = ClmCobPK(df_input_transform, df_input_allcol, params_ClmCobPK)

# The JSON indicates the container transforms columns:
# We'll assume the "Key" link is the final DataFrame from the container.

df_ClmCobPK_Key = output_df_key

# --------------------------------------------------------------------------------
# 25) "IdsClmCOB" => CSeqFileStage => writes to "key/FctsClmCOBExtr.FctsClmCOB.dat.#RunID#"
# --------------------------------------------------------------------------------
write_files(
    df_ClmCobPK_Key,
    f"{adls_path}/key/FctsClmCOBExtr.FctsClmCOB.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# 26) "Select_CLM_COB" => CODBCStage => reads 
#     We do a query "SELECT CLM_COB_SK FROM #$IDSOwner#.CLM_COB c, #$IDSOwner#.W_FCTS_RCRD_DEL d ... AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> #CurrRunCycle#"
#     We'll store results in df_select_CLM_COB
# --------------------------------------------------------------------------------
jdbc_url_ids2, jdbc_props_ids2 = get_db_config(ids_secret_name)
query_select_clm_cob = f"""
SELECT c.CLM_COB_SK
FROM {IDSOwner}.CLM_COB c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SrcSysCdSk}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {CurrRunCycle}
"""
df_select_CLM_COB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids2)
    .options(**jdbc_props_ids2)
    .option("query", query_select_clm_cob)
    .load()
)

# --------------------------------------------------------------------------------
# 27) "CLM_COB" => CODBCStage => input => "ClmCOB"
#     A row-by-row "DELETE FROM #$IDSOwner#.CLM_COB c WHERE CLM_COB_SK=?"
#     We'll implement as a set-delete approach in Spark:
# --------------------------------------------------------------------------------
df_delete_keys = df_select_CLM_COB.select(F.col("CLM_COB_SK").cast("string"))
if df_delete_keys.count() > 0:
    # We create a temporary table in STAGING or tempdb as per instructions.
    # Then we do a "DELETE FROM main WHERE CLM_COB_SK IN (SELECT...)".
    tmp_table_name = f"STAGING.FctsClmCOBExtr_CLM_COB_temp"
    execute_dml(f"DROP TABLE IF EXISTS {tmp_table_name}", jdbc_url_ids2, jdbc_props_ids2)

    # Create the table via spark write
    # Note: Must be a physical table, so we use spark to .write.jdbc to create it:
    df_delete_keys.write \
        .format("jdbc") \
        .option("url", jdbc_url_ids2) \
        .options(**jdbc_props_ids2) \
        .option("dbtable", tmp_table_name) \
        .mode("overwrite") \
        .save()

    # Now do the delete
    delete_stmt = f"""
    DELETE FROM {IDSOwner}.CLM_COB
    WHERE CLM_COB_SK in (
        SELECT CLM_COB_SK FROM {tmp_table_name}
    )
    """
    execute_dml(delete_stmt, jdbc_url_ids2, jdbc_props_ids2)

# End of job.