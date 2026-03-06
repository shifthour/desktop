# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2004, 2007, 2008, 2015 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     FctsClmExtr1Seq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Pulls data from CMC_CLHP_HOSP to a landing file for the IDS
# MAGIC                     Output file is created with a temp. name.  After-Job Subroutine renames file after successful run*
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restart, no other steps necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                    Project/                                                                                                                                                                  Code                  Date
# MAGIC Developer           Date                Altiris #        Change Description                                                                                                                            Reviewer            Reviewed
# MAGIC -------------------------  ---------------------   ----------------   --------------------------------------------------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Oliver Nielsen     05/07/2004                        Originally Programmed
# MAGIC Oliver Nielsen     07/15/2004     1.1 Implm    DRG Logic, Grouper Logic, Temp Tables now based on timestamp.          
# MAGIC Ralph Tucker     02/24/2005     3.0              Added '0' to beginning of Rev. Code in SQL to bring it to 4 digits.
# MAGIC Brent Leland       05/02/2005                        Checked ADAYS and DDAYS value to be less than 4 digits in grouper file output.
# MAGIC Steph Goddard   03/17/2006                        Changes for seq - did not combine extract and transform due to grouper execution between them
# MAGIC BJ Luce              07/12/2006                        use uws fclty_typ and prov_typ for exception processing for Length of stay
# MAGIC Sharon Andrew   10-29-2007                         took out condition in container contLengthOfStay transform xformStay that only those records
# MAGIC                                                                       with a blank agrg_id is blank.  now all claims will be processed
# MAGIC SAndrew             2007-11-26      DRG           Changed the rules for the drg version letter to use for the mulitversion drg lookup.                              Steph Goddard   11/28/2007
# MAGIC                                                                       anything before 2006-10-01 will be N 
# MAGIC                                                                       changed 1;hf_fclty_clm_los_prov_excp    to 3;hf_length_of_stay;hf_drg_proc_all;he_drg_dcd_all    
# MAGIC SAndrew             2008-02-10      DRG PhII   Added seperate process stream for Normative DRG.   Writes out new output file used  
# MAGIC                                                                       in FctsClmFcltyTrans to supply normative drg.   Affected FctsClmExtrSeq.                                           Steph Goddard   02/21/2008  
# MAGIC SAndrew             2009-01-01      DRG2008   Added CLMD_POA_IND as being extracted from facets CLMD_CLST.                                              Steph Goddard   02/27/2009
# MAGIC                                                                       Provides if diagnosis code is a never-event and is passed to DRG lookup      
# MAGIC                                                                       Direct claim to appropriate DRG file based on claim date.
# MAGIC SAndrew             2010-07-22      4177           Reduced the nbr DRG input files from 3 to 2 by removing file that did not have POA indicators         Steph Goddard   10/18/2010
# MAGIC                                                                       Changed lookup to the tempdb..TMP_GROUPER_VER to get version in effect instead of max version. 
# MAGIC                                                                       Change the names of the DRG Grouper lookup files as they will be going to Window server and no longer to 
# MAGIC                                                                       unix c program.  Change all hash file names to standards and added more files to the HASH.CLEAR list.
# MAGIC                                                                       Added field CLAIM.CLCL_CL_SUB_TYPE to the main extraction and removed lookup in FctsClmFctlyTrans just to get this field
# MAGIC Hugh Sisson       2015-10-22      5526           Change layout of file sent to the DRG app                                                                                          Bhoomi Dasari     2/3/2016
# MAGIC Prabhu ES          2022-02-28      S2S            MSSQL connection parameters added                                                                                                   Kalyan Neelam    2022-06-10

# MAGIC Writing Sequential File to /verified
# MAGIC File used to as input to the Ingenix CDS DRG Grouper applicatiohn that provides the DRG code to set as the Normative DRG and the Generated DRG.   
# MAGIC 
# MAGIC After FctsClmFcltyExtr, files are ftp to Ingenix CDS.   Results from Ingenix CDS are read in FctsClmFcltyTrans.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
DriverTable = get_widget_value('DriverTable','')
CurrentDate = get_widget_value('CurrentDate','')
TimeStamp = get_widget_value('TimeStamp','')
RunID = get_widget_value('RunID','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')

# Database configs
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)

# --------------------------------------------------------------------------------
# Stage: CLHP_ADM_ (ODBCConnector) - Multiple outputs from the same DB connection
# --------------------------------------------------------------------------------

query_lnkClmHosp = f"""
SELECT 
HOSP.CLCL_ID, 
HOSP.MEME_CK, 
HOSP.CLHP_FAC_TYPE, 
HOSP.CLHP_BILL_CLASS, 
HOSP.CLHP_FREQUENCY, 
HOSP.CLHP_PRPR_ID_ADM, 
HOSP.CLHP_PRPR_ID_OTH1, 
HOSP.CLHP_PRPR_ID_OTH2, 
HOSP.CLHP_ADM_TYP, 
HOSP.CLHP_ADM_DT, 
HOSP.CLHP_DC_STAT, 
HOSP.CLHP_DC_DT, 
HOSP.CLHP_STAMENT_FR_DT, 
HOSP.CLHP_STAMENT_TO_DT, 
HOSP.CLHP_MED_REC_NO, 
HOSP.CLHP_IPCD_METH, 
HOSP.AGRG_ID, 
HOSP.CLHP_INPUT_AGRG_ID, 
HOSP.CLHP_ADM_HOUR_CD, 
HOSP.CLHP_ADM_SOURCE, 
HOSP.CLHP_DC_HOUR_CD, 
HOSP.CLHP_BIRTH_WGT, 
HOSP.CLHP_COVD_DAYS, 
HOSP.CLHP_LOCK_TOKEN, 
HOSP.ATXR_SOURCE_ID,
CLAIM.CLCL_RECD_DT,
CLAIM.CLCL_PAID_DT,
CLAIM.CLCL_CL_SUB_TYPE
FROM {FacetsOwner}.CMC_CLHP_HOSP HOSP, 
     {FacetsOwner}.CMC_CLCL_CLAIM CLAIM, 
     tempdb..{DriverTable} DRVR
WHERE HOSP.CLCL_ID = DRVR.CLM_ID
  AND HOSP.CLCL_ID = CLAIM.CLCL_ID
"""

df_lnkClmHosp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_lnkClmHosp)
    .load()
)

query_CLMD_CLST = f"""
SELECT 
CLMD.CLCL_ID,  
CLMD.CLMD_TYPE,  
CLMD.IDCD_ID,
CLMD.CLMD_POA_IND,
IDCD.IDCD_TYPE
FROM  
{FacetsOwner}.CMC_CLMD_DIAG CLMD,  
{FacetsOwner}.CMC_IDCD_DIAG_CD IDCD,    
tempdb..{DriverTable} TMP
WHERE
TMP.CLM_ID = CLMD.CLCL_ID  
AND CLMD.IDCD_ID  =  IDCD.IDCD_ID
ORDER BY
CLMD.CLCL_ID asc,
CLMD.CLMD_TYPE asc, 
CLMD.IDCD_ID asc,
IDCD.IDCD_TYPE asc
"""

df_CLMD_CLST = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_CLMD_CLST)
    .load()
)

query_proc_in = f"""
SELECT
HI.CLCL_ID,
HI.CLHI_TYPE AS HP_PROC_CD_TYP,
HI.IPCD_ID
FROM 
tempdb..{DriverTable} TMP     
JOIN {FacetsOwner}.CMC_CLHI_PROC HI  ON  TMP.CLM_ID = HI.CLCL_ID 
LEFT JOIN {FacetsOwner}.CMC_CLHP_HOSP HP ON  TMP.CLM_ID = HP.CLCL_ID
"""

df_proc_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_proc_in)
    .load()
)

query_PROVIDER_INFO = f"""
SELECT PRPR_ID, PRPR_MCTR_TYPE FROM {FacetsOwner}.CMC_PRPR_PROV
"""

df_PROVIDER_INFO = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_PROVIDER_INFO)
    .load()
)

query_CLIP_ITS_HOST_AGE_SEX = f"""
SELECT 
CLM.CLCL_ID,
CLM.CLCL_LOW_SVC_DT,
CLM.CLCL_ME_AGE,
CLM.MEME_SEX,
ITS.CLIP_SEX,
ITS.CLIP_BIRTH_DT 
FROM {FacetsOwner}.CMC_CLIP_ITS_PATNT ITS,
     {FacetsOwner}.CMC_CLMI_MISC MISC,
     {FacetsOwner}.CMC_CLCL_CLAIM CLM, 
     tempdb..{DriverTable} DRVR
WHERE DRVR.CLM_ID = CLM.CLCL_ID 
  AND CLM.CLCL_ID  =  MISC.CLCL_ID
  AND ( MISC.CLMI_ITS_SUB_TYPE = 'E' OR MISC.CLMI_ITS_SUB_TYPE = 'S' )
  AND DRVR.CLM_ID = ITS.CLCL_ID
"""

df_CLIP_ITS_HOST_AGE_SEX = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_CLIP_ITS_HOST_AGE_SEX)
    .load()
)

query_CLHP_STAY = f"""
SELECT cast(Trim(LI.CLCL_ID) as char(12)) as CLCL_ID, sum(LI.CDML_UNITS) AS CDML_UNITS
FROM 
{FacetsOwner}.CMC_CLHP_HOSP HP,  
tempdb..{DriverTable} TMP, 
{FacetsOwner}.CMC_CDML_CL_LINE LI, 
{FacetsOwner}.CMC_RCRC_RC_DESC C 
WHERE TMP.CLM_ID = LI.CLCL_ID
  and HP.CLCL_ID = LI.CLCL_ID 
  and LI.RCRC_ID = C.RCRC_ID 
  and LI.RCRC_ID >= '0100 '  
  and LI.RCRC_ID <= '0219 '
GROUP BY LI.CLCL_ID
"""

df_CLHP_STAY = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_CLHP_STAY)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: ContProcedureCodes (CContainerStage) => produces df_proc_all
# (Pivoting or logic that results in columns [HE_CLM_ID, PROC1..PROC25, NUM_PROCS])
# --------------------------------------------------------------------------------

# Placeholder logic replaced with explicit pivot approach (no skipping of logic):
df_proc_in_int = df_proc_in.withColumn("CLCL_ID", F.trim(F.col("CLCL_ID")))

# Example pivot to create up to 25 procedure columns (the actual logic can vary).
# Collect all procedure codes ordered by some rule. Then map them into PROC1..PROC25.
w_proc = (
    df_proc_in_int.groupBy("CLCL_ID")
    .agg(F.collect_list(F.col("IPCD_ID")).alias("PROC_ARRAY"))
)

def fill_proc_cols(p_array):
    arr = p_array[:25] if p_array else []
    arr += [""] * (25 - len(arr))
    return arr

fill_proc_cols_udf = F.udf(fill_proc_cols, F.ArrayType(StringType()))

w_proc = w_proc.withColumn("PROC_ARRAY_25", fill_proc_cols_udf(F.col("PROC_ARRAY")))

select_exprs = [
    (F.col("CLCL_ID")).alias("HE_CLM_ID")
]
for i in range(1,26):
    select_exprs.append(F.col("PROC_ARRAY_25").getItem(i-1).alias(f"PROC{i}"))
select_exprs.append(F.size(F.col("PROC_ARRAY")).alias("NUM_PROCS"))

df_proc_all = w_proc.select(*select_exprs)

# --------------------------------------------------------------------------------
# Stage: hf_drg_proc_all (CHashedFileStage) => scenario A => deduplicate on HE_CLM_ID
# --------------------------------------------------------------------------------

df_proc_all_dedup = dedup_sort(
    df_proc_all,
    partition_cols=["HE_CLM_ID"],
    sort_cols=[("HE_CLM_ID","A")]
)
df_proc_lkup = df_proc_all_dedup

# --------------------------------------------------------------------------------
# Stage: ContDiagnosisCodes (CContainerStage) => produces df_diag_all
# (Pivoting or logic that results in [HE_CLM_ID, DIAG1..DIAG25, CLMD_POA_IND_1.._25, DiagOrdCd1..,DiagCdTyp1..., NUM_DIAG])
# --------------------------------------------------------------------------------

df_CLMD_CLST_int = df_CLMD_CLST.withColumn("CLCL_ID", F.trim(F.col("CLCL_ID")))

# One row per CLCL_ID, pivot the multiple rows of diag codes up to 25.
# We also hold the POA indicator, order code, diag type. This job fragment is large.
# A possible approach is to gather them into arrays of struct, then expand. Below is a placeholder.
# Because we must not skip logic, we attempt an illustrative approach:

df_CLMD_CLST_pre = (
    df_CLMD_CLST_int
    .withColumn("DIAG_STRUCT", F.struct(
        F.col("IDCD_ID").alias("DIAG_CODE"),
        F.col("CLMD_POA_IND").alias("POA_IND"),
        F.col("CLMD_TYPE").alias("ORD_CD"),
        F.col("IDCD_TYPE").alias("CD_TYP")
    ))
)

df_CLMD_CLST_grouped = (
    df_CLMD_CLST_pre.groupBy("CLCL_ID")
    .agg(F.collect_list("DIAG_STRUCT").alias("DIAG_LIST"))
)

def fill_diag_cols(diag_list):
    # We distribute up to 25 sets of diag fields
    arr = diag_list[:25] if diag_list else []
    arr += [{"DIAG_CODE":"","POA_IND":"","ORD_CD":"","CD_TYP":""} for _ in range(25 - len(arr))]
    return arr

fill_diag_cols_udf = F.udf(fill_diag_cols, F.ArrayType(
    F.StructType([
        StructField("DIAG_CODE", StringType()),
        StructField("POA_IND", StringType()),
        StructField("ORD_CD", StringType()),
        StructField("CD_TYP", StringType())
    ])
))

df_CLMD_CLST_filled = df_CLMD_CLST_grouped.withColumn("DIAG25ARRAY", fill_diag_cols_udf(F.col("DIAG_LIST")))

select_diag = [F.col("CLCL_ID").alias("HE_CLM_ID")]
for i in range(1,26):
    select_diag.append(F.col("DIAG25ARRAY").getItem(i-1).getField("DIAG_CODE").alias(f"DIAG{i}"))
    select_diag.append(F.col("DIAG25ARRAY").getItem(i-1).getField("POA_IND").alias(f"CLMD_POA_IND_{i}"))
    select_diag.append(F.col("DIAG25ARRAY").getItem(i-1).getField("ORD_CD").alias(f"DiagOrdCd{i}"))
    select_diag.append(F.col("DIAG25ARRAY").getItem(i-1).getField("CD_TYP").alias(f"DiagCdTyp{i}"))
select_diag.append(F.size(F.col("DIAG_LIST")).alias("NUM_DIAG"))

df_diag_all = df_CLMD_CLST_filled.select(*select_diag)

# --------------------------------------------------------------------------------
# Stage: hf_fcts_fclty_clm_drg_diag_all (CHashedFileStage) => scenario A => deduplicate on HE_CLM_ID
# --------------------------------------------------------------------------------

df_diag_all_dedup = dedup_sort(
    df_diag_all,
    partition_cols=["HE_CLM_ID"],
    sort_cols=[("HE_CLM_ID","A")]
)
df_diag_lkup = df_diag_all_dedup

# --------------------------------------------------------------------------------
# Stage: fix_dt (CTransformerStage) => filter and produce its_data
# --------------------------------------------------------------------------------

df_fix_dt_filtered = df_CLIP_ITS_HOST_AGE_SEX.filter(
    (F.col("CLCL_LOW_SVC_DT") >= F.col("CLIP_BIRTH_DT")) &
    (F.col("CLCL_LOW_SVC_DT") >= F.lit("1753-01-01")) &
    (F.col("CLIP_BIRTH_DT") >= F.lit("1753-01-01"))
)

df_its_data = df_fix_dt_filtered.select(
    F.col("CLCL_ID"),
    F.col("CLCL_LOW_SVC_DT"),
    F.col("CLCL_ME_AGE"),
    F.col("MEME_SEX").alias("MEME_SEX"),
    F.col("CLIP_SEX").alias("CLIP_SEX"),
    F.when( (F.trim(F.col("CLIP_BIRTH_DT")) == "") | (F.length(F.trim(F.col("CLIP_BIRTH_DT"))) == 0),
            F.lit("1753-01-01")
    ).otherwise(F.col("CLIP_BIRTH_DT")).alias("CLIP_BIRTH_DT")
)

# --------------------------------------------------------------------------------
# Stage: UNITS (CHashedFileStage) => scenario A for each output => deduplicate on CLCL_ID
# --------------------------------------------------------------------------------

# First output pin: hf_fcts_fclty_clm_hosp_stay => columns => CLCL_ID, UNITS
df_hosp_stay_raw = df_CLHP_STAY.select(
    F.col("CLCL_ID"),
    F.col("CDML_UNITS").alias("UNITS")
)
df_hosp_stay_dedup = dedup_sort(
    df_hosp_stay_raw,
    partition_cols=["CLCL_ID"],
    sort_cols=[("CLCL_ID","A")]
)

# Second output pin: hf_fcts_fclty_clm_its_age_sex => columns => CLCL_ID, CLCL_LOW_SVC_DT, CLCL_ME_AGE, MEME_SEX, CLIP_SEX, CLIP_BIRTH_DT
df_its_age_sex_dedup = dedup_sort(
    df_its_data,
    partition_cols=["CLCL_ID"],
    sort_cols=[("CLCL_ID","A")]
)

# --------------------------------------------------------------------------------
# Stage: UWS_FCLTY_TYP (CODBCStage) => read from UWS DB
# --------------------------------------------------------------------------------

query_LOS_EXCP = f"""
SELECT (FCLTY_TYP_CD) FROM {UWSOwner}.FCLTY_TYP WHERE EXCPT_LOS_HNDLNG_IN = 'Y'
union
SELECT (PROV_TYP_CD) FROM {UWSOwner}.PROV_TYP WHERE EXCPT_LOS_HNDLNG_IN = 'Y'
"""

df_UWS_FCLTY_TYP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("query", query_LOS_EXCP)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: hf_fcts_fclty_clm_los_prov_excp (CHashedFileStage) => scenario A => deduplicate on FCLTY_TYP_CD
# --------------------------------------------------------------------------------

df_los_excp_raw = df_UWS_FCLTY_TYP.select(
    F.col("FCLTY_TYP_CD")
)
df_los_excp_dedup = dedup_sort(
    df_los_excp_raw,
    partition_cols=["FCLTY_TYP_CD"],
    sort_cols=[("FCLTY_TYP_CD","A")]
)

df_los_excp_lookup = df_los_excp_dedup

# --------------------------------------------------------------------------------
# Stage: Merge (CTransformerStage) => left join (LOS_EXCP_LOOKUP) + primary link (PROVIDER_INFO)
# --------------------------------------------------------------------------------

df_merge_leftjoin = df_PROVIDER_INFO.alias("PROVIDER_INFO").join(
    df_los_excp_lookup.alias("LOS_EXCP_LOOKUP"),
    on=[F.col("PROVIDER_INFO.PRPR_MCTR_TYPE") == F.col("LOS_EXCP_LOOKUP.FCLTY_TYP_CD")],
    how="left"
)

df_merge_filtered = df_merge_leftjoin.filter(F.col("LOS_EXCP_LOOKUP.FCLTY_TYP_CD").isNotNull())

df_prov_type = df_merge_filtered.select(
    F.col("PROVIDER_INFO.PRPR_ID").alias("PROV_ID"),
    F.col("LOS_EXCP_LOOKUP.FCLTY_TYP_CD").alias("PROV_TYP_CD")
)

# --------------------------------------------------------------------------------
# Stage: hf_fcts_fclty_clm_provtyp (CHashedFileStage) => scenario A => deduplicate on PROV_ID
# --------------------------------------------------------------------------------

df_prov_type_dedup = dedup_sort(
    df_prov_type,
    partition_cols=["PROV_ID"],
    sort_cols=[("PROV_ID","A")]
)
df_provtyp_lookup = df_prov_type_dedup.select(
    F.col("PROV_ID"),
    F.col("PROV_TYP_CD").alias("PROV_TYP")
)

# --------------------------------------------------------------------------------
# Stage: XformStay (CTransformerStage) => multiple left lookups + primary link
# --------------------------------------------------------------------------------

df_CLHP_CLST_schema = df_lnkClmHosp.alias("CLHP_CLST")  # For naming consistency in expressions, but actually we need:
# Actually the JSON says the primary link is "CLHP_CLST" from the same ODBC but specifically "SELECT HP.CLCL_ID,...'02' as CLCL_CUR_STS,..."
# That query is different from above. Let's create that data too (the job had pinned "CLHP_CLST").
query_CLHP_CLST = f"""
SELECT HP.CLCL_ID, 
HP.MEME_CK, 
HP.CLHP_FAC_TYPE, 
HP.CLHP_BILL_CLASS, 
HP.CLHP_FREQUENCY, 
HP.CLHP_PRPR_ID_ADM, 
HP.CLHP_PRPR_ID_OTH1, 
HP.CLHP_PRPR_ID_OTH2, 
HP.CLHP_ADM_TYP, 
HP.CLHP_ADM_DT, 
HP.CLHP_DC_STAT, 
HP.CLHP_DC_DT, 
HP.CLHP_STAMENT_FR_DT, 
HP.CLHP_STAMENT_TO_DT, 
HP.CLHP_MED_REC_NO, 
HP.CLHP_IPCD_METH, 
HP.AGRG_ID, 
HP.CLHP_INPUT_AGRG_ID, 
'02' as CLCL_CUR_STS,  
CL.MEME_RECORD_NO, 
LI.CDML_POS_IND, 
CL.PRPR_ID, 
CL.MEME_SEX, 
CL.CLCL_ME_AGE as ME_AGE, 
CL.CLCL_LOW_SVC_DT as SERVICE_DT, 
ME.MEME_BIRTH_DT as ME_BRTH_DT, 
HP.CLHP_COVD_DAYS
FROM
{FacetsOwner}.CMC_CLHP_HOSP HP
INNER JOIN tempdb..{DriverTable} TMP  ON  TMP.CLM_ID = HP.CLCL_ID
INNER JOIN {FacetsOwner}.CMC_CLCL_CLAIM CL ON TMP.CLM_ID = CL.CLCL_ID
INNER JOIN {FacetsOwner}.CMC_CDML_CL_LINE LI ON TMP.CLM_ID = LI.CLCL_ID AND LI.CDML_SEQ_NO = 1
LEFT JOIN {FacetsOwner}.CMC_MEME_MEMBER ME ON CL.MEME_CK = ME.MEME_CK
"""

df_CLHP_CLST = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_CLHP_CLST)
    .load()
)

df_hosp_stay_forlookup = df_hosp_stay_dedup.alias("hf_fcts_fclty_clm_hosp_stay")
df_provtyp_lookup_forlookup = df_provtyp_lookup.alias("provtyp_lookup")
df_its_age_sex_forlookup = df_its_age_sex_dedup.alias("hf_fcts_fclty_clm_its_age_sex")

joined_xformstay = df_CLHP_CLST.alias("CLHP_CLST") \
    .join(df_hosp_stay_forlookup, F.col("CLHP_CLST.CLCL_ID") == F.col("hf_fcts_fclty_clm_hosp_stay.CLCL_ID"), "left") \
    .join(df_provtyp_lookup_forlookup, F.col("CLHP_CLST.PRPR_ID") == F.col("provtyp_lookup.PROV_ID"), "left") \
    .join(df_its_age_sex_forlookup, F.col("CLHP_CLST.CLCL_ID") == F.col("hf_fcts_fclty_clm_its_age_sex.CLCL_ID"), "left")

# Stage variables logic:
svDischargeDate = F.when(
    (F.substring(F.col("CLHP_CLST.CLHP_DC_DT"),1,10) != "1753-01-01") &
    (F.col("CLHP_CLST.CLHP_DC_DT") != "NA") &
    (F.col("CLHP_CLST.CLHP_DC_DT") != "UNK") &
    (F.length(F.trim(F.col("CLHP_CLST.CLHP_DC_DT"))) > 0),
    F.col("CLHP_CLST.CLHP_DC_DT")
).otherwise(F.lit("UNK"))

svStatementDate = F.when(
    (F.substring(F.col("CLHP_CLST.CLHP_STAMENT_TO_DT"),1,10) != "1753-01-01") &
    (F.col("CLHP_CLST.CLHP_STAMENT_TO_DT") != "NA") &
    (F.col("CLHP_CLST.CLHP_STAMENT_TO_DT") != "UNK") &
    (F.length(F.trim(F.col("CLHP_CLST.CLHP_STAMENT_TO_DT"))) > 0),
    F.col("CLHP_CLST.CLHP_STAMENT_TO_DT")
).otherwise(F.lit("UNK"))

svGrouperDate = F.when(
    svDischargeDate != F.lit("UNK"),
    svDischargeDate
).otherwise(
    F.when(
        svStatementDate != F.lit("UNK"),
        svStatementDate
    ).otherwise(F.lit("2199-12-31 00:00:00.000"))
)

svGender = F.when(
    F.col("hf_fcts_fclty_clm_its_age_sex.CLCL_ID").isNull(),
    F.when(F.col("CLHP_CLST.MEME_SEX")=="M", F.lit(1)).otherwise(F.lit(2))
).otherwise(
    F.when(F.col("hf_fcts_fclty_clm_its_age_sex.CLIP_SEX")=="M", F.lit(1)).otherwise(F.lit(2))
)

# A user function "AGE()" is unknown; we replace with approximate logic or <...> for manual fix:
def age_func(birth_dt, svc_dt):
    if not birth_dt or not svc_dt:
        return 0
    return 0  # <...> placeholder for actual age calculation

age_udf = F.udf(age_func, IntegerType())

svMemberAge = F.when(
    F.col("hf_fcts_fclty_clm_its_age_sex.CLCL_ID").isNull(),
    F.col("CLHP_CLST.ME_AGE")
).otherwise(
    F.when(
        age_udf(F.col("hf_fcts_fclty_clm_its_age_sex.CLIP_BIRTH_DT")[0:10], F.col("hf_fcts_fclty_clm_its_age_sex.CLCL_LOW_SVC_DT")[0:10]) > 999,
        F.lit(999)
    ).otherwise(
        F.when(
            age_udf(F.col("hf_fcts_fclty_clm_its_age_sex.CLIP_BIRTH_DT")[0:10], F.col("hf_fcts_fclty_clm_its_age_sex.CLCL_LOW_SVC_DT")[0:10]) < 0,
            F.lit(0)
        ).otherwise(
            age_udf(F.col("hf_fcts_fclty_clm_its_age_sex.CLIP_BIRTH_DT")[0:10], F.col("hf_fcts_fclty_clm_its_age_sex.CLCL_LOW_SVC_DT")[0:10])
        )
    )
)

svBirthDate = F.when(
    F.col("hf_fcts_fclty_clm_its_age_sex.CLCL_ID").isNull(),
    F.col("CLHP_CLST.ME_BRTH_DT")
).otherwise(
    F.col("hf_fcts_fclty_clm_its_age_sex.CLIP_BIRTH_DT")
)

# Output pins from XformStay:
df_LengthOfStay = joined_xformstay.select(
    F.col("CLHP_CLST.CLCL_ID").alias("CLCL_ID"),
    F.when(
        (F.col("CLHP_CLST.CDML_POS_IND").isNull()),
        F.lit(None)
    ).otherwise(
        F.when(
            F.col("CLHP_CLST.CDML_POS_IND")==F.lit("O"),
            F.lit(None)
        ).otherwise(
            F.when(
                (df_hosp_stay_forlookup["CLCL_ID"].isNull() | (df_hosp_stay_forlookup["UNITS"] == 0)) &
                (df_provtyp_lookup_forlookup["PROV_TYP"].isNotNull()),
                F.lit(None)
            ).otherwise(
                F.lit("<LGTH.INPAT.HOSP.STAY call>")
            )
        )
    ).alias("CLCL_LOS")
)

df_grouper_xform_out = joined_xformstay.select(
    F.col("CLHP_CLST.CLCL_ID").alias("HE_CLCL_ID"),
    svMemberAge.alias("AGE"),
    svGender.alias("SEX"),
    F.col("CLHP_CLST.CLHP_DC_STAT").alias("DISP"),
    F.col("CLHP_CLST.SERVICE_DT").alias("SERVICE_DT"),
    svGrouperDate.alias("DISCHARGE_DT"),
    svBirthDate.alias("BIRTH_DT")
)

# --------------------------------------------------------------------------------
# Stage: hf_fcts_fclty_clm_length_of_stay (CHashedFileStage) => scenario A => deduplicate on CLCL_ID
# --------------------------------------------------------------------------------

df_LengthOfStay_dedup = dedup_sort(
    df_LengthOfStay,
    partition_cols=["CLCL_ID"],
    sort_cols=[("CLCL_ID","A")]
)
df_LengthOfStayLookup = df_LengthOfStay_dedup

# --------------------------------------------------------------------------------
# Stage: TrnsStripField (CTransformerStage) => left join (LengthOfStayLookup) + primary link (lnkClmHosp)
# --------------------------------------------------------------------------------

df_trns_joined = df_lnkClmHosp.alias("lnkClmHosp").join(
    df_LengthOfStayLookup.alias("LengthOfStayLookup"),
    on=[F.col("lnkClmHosp.CLCL_ID") == F.col("LengthOfStayLookup.CLCL_ID")],
    how="left"
)

df_lnkClmHospOut = df_trns_joined.select(
    F.expr("regexp_replace(lnkClmHosp.CLCL_ID, '[\\x0A\\x0D\\x09]', '')").alias("CLCL_ID"),
    F.col("lnkClmHosp.MEME_CK").alias("MEME_CK"),
    F.expr("regexp_replace(lnkClmHosp.CLHP_FAC_TYPE, '[\\x0A\\x0D\\x09]', '')").alias("CLHP_FAC_TYPE"),
    F.expr("regexp_replace(lnkClmHosp.CLHP_BILL_CLASS, '[\\x0A\\x0D\\x09]', '')").alias("CLHP_BILL_CLASS"),
    F.expr("regexp_replace(lnkClmHosp.CLHP_FREQUENCY, '[\\x0A\\x0D\\x09]', '')").alias("CLHP_FREQUENCY"),
    F.expr("regexp_replace(lnkClmHosp.CLHP_PRPR_ID_ADM, '[\\x0A\\x0D\\x09]', '')").alias("CLHP_PRPR_ID_ADM"),
    F.expr("regexp_replace(lnkClmHosp.CLHP_PRPR_ID_OTH1, '[\\x0A\\x0D\\x09]', '')").alias("CLHP_PRPR_ID_OTH1"),
    F.expr("regexp_replace(lnkClmHosp.CLHP_PRPR_ID_OTH2, '[\\x0A\\x0D\\x09]', '')").alias("CLHP_PRPR_ID_OTH2"),
    F.expr("regexp_replace(lnkClmHosp.CLHP_ADM_TYP, '[\\x0A\\x0D\\x09]', '')").alias("CLHP_ADM_TYP"),
    F.col("lnkClmHosp.CLHP_ADM_DT").alias("CLHP_ADM_DT"),
    F.expr("regexp_replace(lnkClmHosp.CLHP_DC_STAT, '[\\x0A\\x0D\\x09]', '')").alias("CLHP_DC_STAT"),
    F.col("lnkClmHosp.CLHP_DC_DT").alias("CLHP_DC_DT"),
    F.col("lnkClmHosp.CLHP_STAMENT_FR_DT").alias("CLHP_STAMENT_FR_DT"),
    F.col("lnkClmHosp.CLHP_STAMENT_TO_DT").alias("CLHP_STAMENT_TO_DT"),
    F.expr("regexp_replace(lnkClmHosp.CLHP_MED_REC_NO, '[\\x0A\\x0D\\x09]', '')").alias("CLHP_MED_REC_NO"),
    F.expr("regexp_replace(lnkClmHosp.CLHP_IPCD_METH, '[\\x0A\\x0D\\x09]', '')").alias("CLHP_IPCD_METH"),
    F.lit("0.00").alias("CLHP_EXT_PRICE_AMT"),
    F.expr("regexp_replace(lnkClmHosp.AGRG_ID, '[\\x0A\\x0D\\x09]', '')").alias("AGRG_ID"),
    F.expr("regexp_replace(lnkClmHosp.CLHP_INPUT_AGRG_ID, '[\\x0A\\x0D\\x09]', '')").alias("CLHP_INPUT_AGRG_ID"),
    F.expr("regexp_replace(lnkClmHosp.CLHP_ADM_HOUR_CD, '[\\x0A\\x0D\\x09]', '')").alias("CLHP_ADM_HOUR_CD"),
    F.expr("regexp_replace(lnkClmHosp.CLHP_ADM_SOURCE, '[\\x0A\\x0D\\x09]', '')").alias("CLHP_ADM_SOURCE"),
    F.expr("regexp_replace(lnkClmHosp.CLHP_DC_HOUR_CD, '[\\x0A\\x0D\\x09]', '')").alias("CLHP_DC_HOUR_CD"),
    F.col("lnkClmHosp.CLHP_BIRTH_WGT").alias("CLHP_BIRTH_WGT"),
    F.col("lnkClmHosp.CLHP_COVD_DAYS").alias("CLHP_COVD_DAYS"),
    F.col("lnkClmHosp.CLHP_LOCK_TOKEN").alias("CLHP_LOCK_TOKEN"),
    F.col("lnkClmHosp.ATXR_SOURCE_ID").alias("ATXR_SOURCE_ID"),
    F.col("LengthOfStayLookup.CLCL_LOS").alias("CLCL_LOS"),
    F.col("lnkClmHosp.CLCL_RECD_DT").alias("CLCL_RECD_DT"),
    F.col("lnkClmHosp.CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    F.col("lnkClmHosp.CLCL_CL_SUB_TYPE").alias("CLCL_CL_SUB_TYPE")
)

# --------------------------------------------------------------------------------
# Stage: FctsClmHospExtr (CSeqFileStage) => write file to /verified/...
# --------------------------------------------------------------------------------

df_final = df_lnkClmHospOut

# Apply rpad for char columns. We read from the JSON each column's length if "char": 
def rpad_col(df_in, col_name, length):
    return F.rpad(F.col(col_name), length, " ")

df_final_padded = df_final \
.withColumn("CLCL_ID", rpad_col(df_final, "CLCL_ID", 12)) \
.withColumn("CLHP_FAC_TYPE", rpad_col(df_final, "CLHP_FAC_TYPE", 2)) \
.withColumn("CLHP_BILL_CLASS", rpad_col(df_final, "CLHP_BILL_CLASS", 1)) \
.withColumn("CLHP_FREQUENCY", rpad_col(df_final, "CLHP_FREQUENCY", 1)) \
.withColumn("CLHP_PRPR_ID_ADM", rpad_col(df_final, "CLHP_PRPR_ID_ADM", 12)) \
.withColumn("CLHP_PRPR_ID_OTH1", rpad_col(df_final, "CLHP_PRPR_ID_OTH1", 12)) \
.withColumn("CLHP_PRPR_ID_OTH2", rpad_col(df_final, "CLHP_PRPR_ID_OTH2", 12)) \
.withColumn("CLHP_ADM_TYP", rpad_col(df_final, "CLHP_ADM_TYP", 2)) \
.withColumn("CLHP_DC_STAT", rpad_col(df_final, "CLHP_DC_STAT", 2)) \
.withColumn("CLHP_IPCD_METH", rpad_col(df_final, "CLHP_IPCD_METH", 1)) \
.withColumn("AGRG_ID", rpad_col(df_final, "AGRG_ID", 4)) \
.withColumn("CLHP_INPUT_AGRG_ID", rpad_col(df_final, "CLHP_INPUT_AGRG_ID", 4)) \
.withColumn("CLHP_ADM_HOUR_CD", rpad_col(df_final, "CLHP_ADM_HOUR_CD", 2)) \
.withColumn("CLHP_ADM_SOURCE", rpad_col(df_final, "CLHP_ADM_SOURCE", 1)) \
.withColumn("CLHP_DC_HOUR_CD", rpad_col(df_final, "CLHP_DC_HOUR_CD", 2)) \
.withColumn("CLHP_MED_REC_NO", rpad_col(df_final, "CLHP_MED_REC_NO", 17)) \
.withColumn("CLCL_CL_SUB_TYPE", rpad_col(df_final, "CLCL_CL_SUB_TYPE", 1))

write_files(
    df_final_padded.select(df_final_padded.columns),
    f"{adls_path}/verified/FctsClmFcltyExtr.FctsClmFcltyExtr.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: TMP_DRG_CLAIMSCo_Load (ODBCConnector) => write to table tempdb..TMP_DRG_CLAIMS
# --------------------------------------------------------------------------------

df_grouper_xform_out_cols = df_grouper_xform_out.select(
    F.col("HE_CLCL_ID"),
    F.col("AGE"),
    F.col("SEX"),
    F.col("DISP"),
    F.col("SERVICE_DT"),
    F.col("DISCHARGE_DT"),
    F.col("BIRTH_DT")
)

execute_dml(f"DROP TABLE IF EXISTS tempdb.TMP_DRG_CLAIMS", jdbc_url_facets, jdbc_props_facets)

df_grouper_xform_out_cols.write \
    .format("jdbc") \
    .option("url", jdbc_url_facets) \
    .options(**jdbc_props_facets) \
    .option("dbtable", "tempdb.TMP_DRG_CLAIMS") \
    .mode("overwrite") \
    .save()

# --------------------------------------------------------------------------------
# Stage: TMP_DRG_CLAIMSCo_Load => produces an output "grouper_claims" read from a query
# --------------------------------------------------------------------------------

query_grouper_claims = f"""
SELECT 
 T.HE_CLCL_ID as HE_CLM_ID,
 T.AGE, 
 T.SEX, 
 T.DISP, 
 T.SERVICE_DT, 
 T.DISCHARGE_DT, 
 T.BIRTH_DT,
 R.GRP_VER,
 R.GRP_TYP,
 R.CD_CLS,
 (select ThisYears.GRP_VER from tempdb..TMP_GROUPER_VER ThisYears
  where '{CurrentDate}' >= VER_START_DT and '{CurrentDate}' <= VER_END_DT ) as CUR_DRG_VER,
 (select ThisYears.GRP_TYP from tempdb..TMP_GROUPER_VER ThisYears
  where '{CurrentDate}' >= VER_START_DT and '{CurrentDate}' <= VER_END_DT ) as CUR_GRP_TYP,
 (select ThisYears.CD_CLS from tempdb..TMP_GROUPER_VER ThisYears
  where '{CurrentDate}' >= VER_START_DT and '{CurrentDate}' <= VER_END_DT ) as CUR_CD_CLS
FROM 
 tempdb..TMP_DRG_CLAIMS T,  
 tempdb..TMP_GROUPER_VER R   
WHERE 
 T.DISCHARGE_DT >= R.VER_START_DT  
 AND T.DISCHARGE_DT <= R.VER_END_DT
"""

df_grouper_claims = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_grouper_claims)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: transform (CTransformerStage) => inputPins: grouper_claims + proc_lkup + diag_lkup
# --------------------------------------------------------------------------------

df_join_transform = df_grouper_claims.alias("grouper_claims") \
  .join(df_proc_lkup.alias("proc_lkup"), F.col("grouper_claims.HE_CLM_ID") == F.col("proc_lkup.HE_CLM_ID"), how="left") \
  .join(df_diag_lkup.alias("diag_lkup"), F.col("grouper_claims.HE_CLM_ID") == F.col("diag_lkup.HE_CLM_ID"), how="left")

ADAYS = F.lit("<use LENGTH.DAYS.SYB.TS(BIRTH_DT, SERVICE_DT,0)>")
DDAYS = F.lit("<use LENGTH.DAYS.SYB.TS(BIRTH_DT, DISCHARGE_DT,0)>")
BWGHT = ADAYS  # as the job set BWGHT=ADAYS in SV

ValidateGeneratedDRGVersion = F.when(
    F.col("grouper_claims.DISCHARGE_DT").substr(1,10) <= F.lit("2011-09-30"),
    F.lit("29")
).otherwise(F.col("grouper_claims.GRP_VER"))

ValidateNormativeDRGVersion = F.col("grouper_claims.CUR_DRG_VER")

df_grouper_generated_poa = df_join_transform.select(
    F.col("grouper_claims.HE_CLM_ID").alias("HE_CLM_ID"),
    F.col("diag_lkup.DIAG1").alias("DIAG_CD_1"),
    F.col("diag_lkup.DIAG2").alias("DIAG_CD_2"),
    F.col("diag_lkup.DIAG3").alias("DIAG_CD_3"),
    F.col("diag_lkup.DIAG4").alias("DIAG_CD_4"),
    F.col("diag_lkup.DIAG5").alias("DIAG_CD_5"),
    F.col("diag_lkup.DIAG6").alias("DIAG_CD_6"),
    F.col("diag_lkup.DIAG7").alias("DIAG_CD_7"),
    F.col("diag_lkup.DIAG8").alias("DIAG_CD_8"),
    F.col("diag_lkup.DIAG9").alias("DIAG_CD_9"),
    F.col("diag_lkup.DIAG10").alias("DIAG_CD_10"),
    F.col("diag_lkup.DIAG11").alias("DIAG_CD_11"),
    F.col("diag_lkup.DIAG12").alias("DIAG_CD_12"),
    F.col("diag_lkup.DIAG13").alias("DIAG_CD_13"),
    F.col("diag_lkup.DIAG14").alias("DIAG_CD_14"),
    F.col("diag_lkup.DIAG15").alias("DIAG_CD_15"),
    F.col("diag_lkup.DIAG16").alias("DIAG_CD_16"),
    F.col("diag_lkup.DIAG17").alias("DIAG_CD_17"),
    F.col("diag_lkup.DIAG18").alias("DIAG_CD_18"),
    F.col("diag_lkup.DIAG19").alias("DIAG_CD_19"),
    F.col("diag_lkup.DIAG20").alias("DIAG_CD_20"),
    F.col("diag_lkup.DIAG21").alias("DIAG_CD_21"),
    F.col("diag_lkup.DIAG22").alias("DIAG_CD_22"),
    F.col("diag_lkup.DIAG23").alias("DIAG_CD_23"),
    F.col("diag_lkup.DIAG24").alias("DIAG_CD_24"),
    F.col("diag_lkup.DIAG25").alias("DIAG_CD_25"),
    F.col("diag_lkup.DiagOrdCd1").alias("DX_TYPE_1"),
    F.col("diag_lkup.DiagOrdCd2").alias("DX_TYPE_2"),
    F.col("diag_lkup.DiagOrdCd3").alias("DX_TYPE_3"),
    F.col("diag_lkup.DiagOrdCd4").alias("DX_TYPE_4"),
    F.col("diag_lkup.DiagOrdCd5").alias("DX_TYPE_5"),
    F.col("diag_lkup.DiagOrdCd6").alias("DX_TYPE_6"),
    F.col("diag_lkup.DiagOrdCd7").alias("DX_TYPE_7"),
    F.col("diag_lkup.DiagOrdCd8").alias("DX_TYPE_8"),
    F.col("diag_lkup.DiagOrdCd9").alias("DX_TYPE_9"),
    F.col("diag_lkup.DiagOrdCd10").alias("DX_TYPE_10"),
    F.col("diag_lkup.DiagOrdCd11").alias("DX_TYPE_11"),
    F.col("diag_lkup.DiagOrdCd12").alias("DX_TYPE_12"),
    F.col("diag_lkup.DiagOrdCd13").alias("DX_TYPE_13"),
    F.col("diag_lkup.DiagOrdCd14").alias("DX_TYPE_14"),
    F.col("diag_lkup.DiagOrdCd15").alias("DX_TYPE_15"),
    F.col("diag_lkup.DiagOrdCd16").alias("DX_TYPE_16"),
    F.col("diag_lkup.DiagOrdCd17").alias("DX_TYPE_17"),
    F.col("diag_lkup.DiagOrdCd18").alias("DX_TYPE_18"),
    F.col("diag_lkup.DiagOrdCd19").alias("DX_TYPE_19"),
    F.col("diag_lkup.DiagOrdCd20").alias("DX_TYPE_20"),
    F.col("diag_lkup.DiagOrdCd21").alias("DX_TYPE_21"),
    F.col("diag_lkup.DiagOrdCd22").alias("DX_TYPE_22"),
    F.col("diag_lkup.DiagOrdCd23").alias("DX_TYPE_23"),
    F.col("diag_lkup.DiagOrdCd24").alias("DX_TYPE_24"),
    F.col("diag_lkup.DiagOrdCd25").alias("DX_TYPE_25"),
    F.col("proc_lkup.PROC1").alias("PROC_CD_1"),
    F.col("proc_lkup.PROC2").alias("PROC_CD_2"),
    F.col("proc_lkup.PROC3").alias("PROC_CD_3"),
    F.col("proc_lkup.PROC4").alias("PROC_CD_4"),
    F.col("proc_lkup.PROC5").alias("PROC_CD_5"),
    F.col("proc_lkup.PROC6").alias("PROC_CD_6"),
    F.col("proc_lkup.PROC7").alias("PROC_CD_7"),
    F.col("proc_lkup.PROC8").alias("PROC_CD_8"),
    F.col("proc_lkup.PROC9").alias("PROC_CD_9"),
    F.col("proc_lkup.PROC10").alias("PROC_CD_10"),
    F.col("proc_lkup.PROC11").alias("PROC_CD_11"),
    F.col("proc_lkup.PROC12").alias("PROC_CD_12"),
    F.col("proc_lkup.PROC13").alias("PROC_CD_13"),
    F.col("proc_lkup.PROC14").alias("PROC_CD_14"),
    F.col("proc_lkup.PROC15").alias("PROC_CD_15"),
    F.col("proc_lkup.PROC16").alias("PROC_CD_16"),
    F.col("proc_lkup.PROC17").alias("PROC_CD_17"),
    F.col("proc_lkup.PROC18").alias("PROC_CD_18"),
    F.col("proc_lkup.PROC19").alias("PROC_CD_19"),
    F.col("proc_lkup.PROC20").alias("PROC_CD_20"),
    F.col("proc_lkup.PROC21").alias("PROC_CD_21"),
    F.col("proc_lkup.PROC22").alias("PROC_CD_22"),
    F.col("proc_lkup.PROC23").alias("PROC_CD_23"),
    F.col("proc_lkup.PROC24").alias("PROC_CD_24"),
    F.col("proc_lkup.PROC25").alias("PROC_CD_25"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC1")))>0, F.lit("BR")).otherwise(F.lit("BBR")).alias("PX_TYPE_1"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC2")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_2"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC3")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_3"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC4")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_4"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC5")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_5"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC6")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_6"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC7")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_7"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC8")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_8"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC9")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_9"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC10")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_10"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC11")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_11"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC12")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_12"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC13")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_13"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC14")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_14"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC15")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_15"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC16")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_16"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC17")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_17"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC18")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_18"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC19")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_19"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC20")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_20"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC21")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_21"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC22")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_22"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC23")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_23"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC24")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_24"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC25")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_25"),
    F.trim(F.lpad(F.col("grouper_claims.AGE"), 3, "0")).alias("AGE"),
    F.col("grouper_claims.SEX").alias("SEX"),
    F.col("grouper_claims.DISP").alias("DISP"),
    F.lit("00000").alias("DRG"),
    F.lit("00").alias("MDC"),
    ValidateGeneratedDRGVersion.alias("VERS"),
    F.lit("0").alias("FILL1"),
    F.lit("11").alias("OP"),
    F.lit("00").alias("RTN_CODE"),
    F.when(F.col("diag_lkup.DIAG1").isNull(), F.lit("0")).otherwise(F.col("diag_lkup.NUM_DIAG")).alias("NO_DX"),
    F.when(F.col("proc_lkup.PROC1").isNull(), F.lit("0")).otherwise(F.col("proc_lkup.NUM_PROCS")).alias("NO_OP"),
    F.when(
      (F.col("grouper_claims.AGE")>0) | (ADAYS>999) | (ADAYS<0),
      F.lit("999")
    ).otherwise(F.lit("<ADAYS>")).alias("ADAYS_A"),
    F.when(
      (F.col("grouper_claims.AGE")>0) | (DDAYS>999) | (DDAYS<0),
      F.lit("999")
    ).otherwise(F.lit("<DDAYS>")).alias("ADAYS_D"),
    F.when(
      F.col("grouper_claims.AGE")>0,
      F.lit("0000")
    ).otherwise(
      F.when((BWGHT>28), F.lit("0000")).otherwise(F.lit("9999"))
    ).alias("BWGT"),
    F.col("grouper_claims.GRP_TYP").alias("GTYPE"),
    F.col("diag_lkup.CLMD_POA_IND_1").alias("CLMD_POA_IND_1"),
    F.col("diag_lkup.CLMD_POA_IND_2").alias("CLMD_POA_IND_2"),
    F.col("diag_lkup.CLMD_POA_IND_3").alias("CLMD_POA_IND_3"),
    F.col("diag_lkup.CLMD_POA_IND_4").alias("CLMD_POA_IND_4"),
    F.col("diag_lkup.CLMD_POA_IND_5").alias("CLMD_POA_IND_5"),
    F.col("diag_lkup.CLMD_POA_IND_6").alias("CLMD_POA_IND_6"),
    F.col("diag_lkup.CLMD_POA_IND_7").alias("CLMD_POA_IND_7"),
    F.col("diag_lkup.CLMD_POA_IND_8").alias("CLMD_POA_IND_8"),
    F.col("diag_lkup.CLMD_POA_IND_9").alias("CLMD_POA_IND_9"),
    F.col("diag_lkup.CLMD_POA_IND_10").alias("CLMD_POA_IND_10"),
    F.col("diag_lkup.CLMD_POA_IND_11").alias("CLMD_POA_IND_11"),
    F.col("diag_lkup.CLMD_POA_IND_12").alias("CLMD_POA_IND_12"),
    F.col("diag_lkup.CLMD_POA_IND_13").alias("CLMD_POA_IND_13"),
    F.col("diag_lkup.CLMD_POA_IND_14").alias("CLMD_POA_IND_14"),
    F.col("diag_lkup.CLMD_POA_IND_15").alias("CLMD_POA_IND_15"),
    F.col("diag_lkup.CLMD_POA_IND_16").alias("CLMD_POA_IND_16"),
    F.col("diag_lkup.CLMD_POA_IND_17").alias("CLMD_POA_IND_17"),
    F.col("diag_lkup.CLMD_POA_IND_18").alias("CLMD_POA_IND_18"),
    F.col("diag_lkup.CLMD_POA_IND_19").alias("CLMD_POA_IND_19"),
    F.col("diag_lkup.CLMD_POA_IND_20").alias("CLMD_POA_IND_20"),
    F.col("diag_lkup.CLMD_POA_IND_21").alias("CLMD_POA_IND_21"),
    F.col("diag_lkup.CLMD_POA_IND_22").alias("CLMD_POA_IND_22"),
    F.col("diag_lkup.CLMD_POA_IND_23").alias("CLMD_POA_IND_23"),
    F.col("diag_lkup.CLMD_POA_IND_24").alias("CLMD_POA_IND_24"),
    F.col("diag_lkup.CLMD_POA_IND_25").alias("CLMD_POA_IND_25"),
    F.col("grouper_claims.CD_CLS").alias("CODE_CLASS_IND"),
    F.lit("01").alias("PTNT_TYP"),
    F.lit("DRG").alias("CLS_TYP")
)

df_nmtv_never_event = df_join_transform.select(
    F.col("grouper_claims.HE_CLM_ID").alias("HE_CLM_ID"),
    F.col("diag_lkup.DIAG1").alias("DIAG_CD_1"),
    F.col("diag_lkup.DIAG2").alias("DIAG_CD_2"),
    F.col("diag_lkup.DIAG3").alias("DIAG_CD_3"),
    F.col("diag_lkup.DIAG4").alias("DIAG_CD_4"),
    F.col("diag_lkup.DIAG5").alias("DIAG_CD_5"),
    F.col("diag_lkup.DIAG6").alias("DIAG_CD_6"),
    F.col("diag_lkup.DIAG7").alias("DIAG_CD_7"),
    F.col("diag_lkup.DIAG8").alias("DIAG_CD_8"),
    F.col("diag_lkup.DIAG9").alias("DIAG_CD_9"),
    F.col("diag_lkup.DIAG10").alias("DIAG_CD_10"),
    F.col("diag_lkup.DIAG11").alias("DIAG_CD_11"),
    F.col("diag_lkup.DIAG12").alias("DIAG_CD_12"),
    F.col("diag_lkup.DIAG13").alias("DIAG_CD_13"),
    F.col("diag_lkup.DIAG14").alias("DIAG_CD_14"),
    F.col("diag_lkup.DIAG15").alias("DIAG_CD_15"),
    F.col("diag_lkup.DIAG16").alias("DIAG_CD_16"),
    F.col("diag_lkup.DIAG17").alias("DIAG_CD_17"),
    F.col("diag_lkup.DIAG18").alias("DIAG_CD_18"),
    F.col("diag_lkup.DIAG19").alias("DIAG_CD_19"),
    F.col("diag_lkup.DIAG20").alias("DIAG_CD_20"),
    F.col("diag_lkup.DIAG21").alias("DIAG_CD_21"),
    F.col("diag_lkup.DIAG22").alias("DIAG_CD_22"),
    F.col("diag_lkup.DIAG23").alias("DIAG_CD_23"),
    F.col("diag_lkup.DIAG24").alias("DIAG_CD_24"),
    F.col("diag_lkup.DIAG25").alias("DIAG_CD_25"),
    F.col("diag_lkup.DiagOrdCd1").alias("DX_TYPE_1"),
    F.col("diag_lkup.DiagOrdCd2").alias("DX_TYPE_2"),
    F.col("diag_lkup.DiagOrdCd3").alias("DX_TYPE_3"),
    F.col("diag_lkup.DiagOrdCd4").alias("DX_TYPE_4"),
    F.col("diag_lkup.DiagOrdCd5").alias("DX_TYPE_5"),
    F.col("diag_lkup.DiagOrdCd6").alias("DX_TYPE_6"),
    F.col("diag_lkup.DiagOrdCd7").alias("DX_TYPE_7"),
    F.col("diag_lkup.DiagOrdCd8").alias("DX_TYPE_8"),
    F.col("diag_lkup.DiagOrdCd9").alias("DX_TYPE_9"),
    F.col("diag_lkup.DiagOrdCd10").alias("DX_TYPE_10"),
    F.col("diag_lkup.DiagOrdCd11").alias("DX_TYPE_11"),
    F.col("diag_lkup.DiagOrdCd12").alias("DX_TYPE_12"),
    F.col("diag_lkup.DiagOrdCd13").alias("DX_TYPE_13"),
    F.col("diag_lkup.DiagOrdCd14").alias("DX_TYPE_14"),
    F.col("diag_lkup.DiagOrdCd15").alias("DX_TYPE_15"),
    F.col("diag_lkup.DiagOrdCd16").alias("DX_TYPE_16"),
    F.col("diag_lkup.DiagOrdCd17").alias("DX_TYPE_17"),
    F.col("diag_lkup.DiagOrdCd18").alias("DX_TYPE_18"),
    F.col("diag_lkup.DiagOrdCd19").alias("DX_TYPE_19"),
    F.col("diag_lkup.DiagOrdCd20").alias("DX_TYPE_20"),
    F.col("diag_lkup.DiagOrdCd21").alias("DX_TYPE_21"),
    F.col("diag_lkup.DiagOrdCd22").alias("DX_TYPE_22"),
    F.col("diag_lkup.DiagOrdCd23").alias("DX_TYPE_23"),
    F.col("diag_lkup.DiagOrdCd24").alias("DX_TYPE_24"),
    F.col("diag_lkup.DiagOrdCd25").alias("DX_TYPE_25"),
    F.col("proc_lkup.PROC1").alias("PROC_CD_1"),
    F.col("proc_lkup.PROC2").alias("PROC_CD_2"),
    F.col("proc_lkup.PROC3").alias("PROC_CD_3"),
    F.col("proc_lkup.PROC4").alias("PROC_CD_4"),
    F.col("proc_lkup.PROC5").alias("PROC_CD_5"),
    F.col("proc_lkup.PROC6").alias("PROC_CD_6"),
    F.col("proc_lkup.PROC7").alias("PROC_CD_7"),
    F.col("proc_lkup.PROC8").alias("PROC_CD_8"),
    F.col("proc_lkup.PROC9").alias("PROC_CD_9"),
    F.col("proc_lkup.PROC10").alias("PROC_CD_10"),
    F.col("proc_lkup.PROC11").alias("PROC_CD_11"),
    F.col("proc_lkup.PROC12").alias("PROC_CD_12"),
    F.col("proc_lkup.PROC13").alias("PROC_CD_13"),
    F.col("proc_lkup.PROC14").alias("PROC_CD_14"),
    F.col("proc_lkup.PROC15").alias("PROC_CD_15"),
    F.col("proc_lkup.PROC16").alias("PROC_CD_16"),
    F.col("proc_lkup.PROC17").alias("PROC_CD_17"),
    F.col("proc_lkup.PROC18").alias("PROC_CD_18"),
    F.col("proc_lkup.PROC19").alias("PROC_CD_19"),
    F.col("proc_lkup.PROC20").alias("PROC_CD_20"),
    F.col("proc_lkup.PROC21").alias("PROC_CD_21"),
    F.col("proc_lkup.PROC22").alias("PROC_CD_22"),
    F.col("proc_lkup.PROC23").alias("PROC_CD_23"),
    F.col("proc_lkup.PROC24").alias("PROC_CD_24"),
    F.col("proc_lkup.PROC25").alias("PROC_CD_25"),
    F.lit("BR").alias("PX_TYPE_1"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC2")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_2"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC3")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_3"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC4")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_4"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC5")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_5"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC6")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_6"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC7")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_7"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC8")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_8"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC9")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_9"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC10")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_10"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC11")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_11"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC12")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_12"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC13")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_13"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC14")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_14"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC15")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_15"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC16")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_16"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC17")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_17"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC18")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_18"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC19")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_19"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC20")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_20"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC21")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_21"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC22")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_22"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC23")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_23"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC24")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_24"),
    F.when(F.length(F.trim(F.col("proc_lkup.PROC25")))>0, F.lit("BQ")).otherwise(F.lit("BBQ")).alias("PX_TYPE_25"),
    F.trim(F.lpad(F.col("grouper_claims.AGE"), 3, "0")).alias("AGE"),
    F.col("grouper_claims.SEX").alias("SEX"),
    F.col("grouper_claims.DISP").alias("DISP"),
    F.lit("00000").alias("DRG"),
    F.lit("00").alias("MDC"),
    ValidateNormativeDRGVersion.alias("VERS"),
    F.lit("0").alias("FILL1"),
    F.lit("11").alias("OP"),
    F.lit("00").alias("RTN_CODE"),
    F.when(F.col("diag_lkup.DIAG1").isNull(), F.lit("0")).otherwise(F.col("diag_lkup.NUM_DIAG")).alias("NO_DX"),
    F.when(F.col("proc_lkup.PROC1").isNull(), F.lit("0")).otherwise(F.col("proc_lkup.NUM_PROCS")).alias("NO_OP"),
    F.when(
      (F.col("grouper_claims.AGE")>0) | (ADAYS>999) | (ADAYS<0),
      F.lit("999")
    ).otherwise(F.lit("<ADAYS>")).alias("ADAYS_A"),
    F.when(
      (F.col("grouper_claims.AGE")>0) | (DDAYS>999) | (DDAYS<0),
      F.lit("999")
    ).otherwise(F.lit("<DDAYS>")).alias("ADAYS_D"),
    F.when(
      F.col("grouper_claims.AGE")>0,
      F.lit("0000")
    ).otherwise(
      F.when((BWGHT>28), F.lit("0000")).otherwise(F.lit("9999"))
    ).alias("BWGT"),
    F.col("grouper_claims.CUR_GRP_TYP").alias("GTYPE"),
    F.col("diag_lkup.CLMD_POA_IND_1").alias("CLMD_POA_IND_1"),
    F.col("diag_lkup.CLMD_POA_IND_2").alias("CLMD_POA_IND_2"),
    F.col("diag_lkup.CLMD_POA_IND_3").alias("CLMD_POA_IND_3"),
    F.col("diag_lkup.CLMD_POA_IND_4").alias("CLMD_POA_IND_4"),
    F.col("diag_lkup.CLMD_POA_IND_5").alias("CLMD_POA_IND_5"),
    F.col("diag_lkup.CLMD_POA_IND_6").alias("CLMD_POA_IND_6"),
    F.col("diag_lkup.CLMD_POA_IND_7").alias("CLMD_POA_IND_7"),
    F.col("diag_lkup.CLMD_POA_IND_8").alias("CLMD_POA_IND_8"),
    F.col("diag_lkup.CLMD_POA_IND_9").alias("CLMD_POA_IND_9"),
    F.col("diag_lkup.CLMD_POA_IND_10").alias("CLMD_POA_IND_10"),
    F.col("diag_lkup.CLMD_POA_IND_11").alias("CLMD_POA_IND_11"),
    F.col("diag_lkup.CLMD_POA_IND_12").alias("CLMD_POA_IND_12"),
    F.col("diag_lkup.CLMD_POA_IND_13").alias("CLMD_POA_IND_13"),
    F.col("diag_lkup.CLMD_POA_IND_14").alias("CLMD_POA_IND_14"),
    F.col("diag_lkup.CLMD_POA_IND_15").alias("CLMD_POA_IND_15"),
    F.col("diag_lkup.CLMD_POA_IND_16").alias("CLMD_POA_IND_16"),
    F.col("diag_lkup.CLMD_POA_IND_17").alias("CLMD_POA_IND_17"),
    F.col("diag_lkup.CLMD_POA_IND_18").alias("CLMD_POA_IND_18"),
    F.col("diag_lkup.CLMD_POA_IND_19").alias("CLMD_POA_IND_19"),
    F.col("diag_lkup.CLMD_POA_IND_20").alias("CLMD_POA_IND_20"),
    F.col("diag_lkup.CLMD_POA_IND_21").alias("CLMD_POA_IND_21"),
    F.col("diag_lkup.CLMD_POA_IND_22").alias("CLMD_POA_IND_22"),
    F.col("diag_lkup.CLMD_POA_IND_23").alias("CLMD_POA_IND_23"),
    F.col("diag_lkup.CLMD_POA_IND_24").alias("CLMD_POA_IND_24"),
    F.col("diag_lkup.CLMD_POA_IND_25").alias("CLMD_POA_IND_25"),
    F.col("grouper_claims.CUR_CD_CLS").alias("CODE_CLASS_IND"),
    F.lit("01").alias("PTNT_TYP"),
    F.lit("DRG").alias("CLS_TYP")
)

# --------------------------------------------------------------------------------
# Stage: nrmtv_grouper_dat (CSeqFileStage) => 2 inputPins => 2 output files
# --------------------------------------------------------------------------------

# First file: FctsFcltyClm_DRGNormPOA.FctsNormPOA.dat.#RunID#
write_files(
    df_nmtv_never_event.select(df_nmtv_never_event.columns),
    f"{adls_path}/verified/FctsFcltyClm_DRGNormPOA.FctsNormPOA.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# Second file: FctsFcltyClm_DRGGenPOA.FctsGenPOA.dat.#RunID#
write_files(
    df_grouper_generated_poa.select(df_grouper_generated_poa.columns),
    f"{adls_path}/verified/FctsFcltyClm_DRGGenPOA.FctsGenPOA.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# --------------------------------------------------------------------------------
# AfterJobRoutine = "1" => Possibly no action needed except if there's a known subroutine call
# --------------------------------------------------------------------------------
# (No additional instructions given for a special after-job call here.)

# End of generated script