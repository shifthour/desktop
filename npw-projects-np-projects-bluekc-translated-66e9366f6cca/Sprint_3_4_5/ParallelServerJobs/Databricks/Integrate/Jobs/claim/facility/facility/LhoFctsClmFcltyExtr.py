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
# MAGIC 
# MAGIC Reddy Sanam    2020-08-10       US263442  Facets connection parameters replaced with FacetsStage connection parameters for LHO
# MAGIC                                                                        Reflected new parameters in the source Queries
# MAGIC                                                                        Stage "FctsClmHospExtr" renamed to LhoClmHospExtr
# MAGIC                                                                        Changed file name in the stage LhoClmHospExtr
# MAGIC                                                                        Changed file names in the stage "nrmtv_grouper_dat"
# MAGIC Venkatesh Munnangi 2020-10-12                  Fixed Data Elements Populated in the job
# MAGIC Prabhu ES                 2022-03-29  S2S          MSSQL ODBC conn params added -  IntegrateDev5                                Manasa Andru             2022-06-09

# MAGIC Writing Sequential File to /verified
# MAGIC File used to as input to the Ingenix CDS DRG Grouper applicatiohn that provides the DRG code to set as the Normative DRG and the Generated DRG.   
# MAGIC 
# MAGIC After LhoFctsClmFcltyExtr, files are ftp to Ingenix CDS.   Results from Ingenix CDS are read in LhoFctsClmFcltyTrans.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F, types as T
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ContProcedureCodes
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/ContDiagnosisCodes
# COMMAND ----------

DriverTable = get_widget_value('DriverTable','')
CurrentDate = get_widget_value('CurrentDate','')
TimeStamp = get_widget_value('TimeStamp','')
RunID = get_widget_value('RunID','')
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')
LhoFacetsStgPW = get_widget_value('LhoFacetsStgPW','')
LhoFacetsStgAcct = get_widget_value('LhoFacetsStgAcct','')
LhoFacetsStgDSN = get_widget_value('LhoFacetsStgDSN','')

jdbc_url_lhofacetsstg, jdbc_props_lhofacetsstg = get_db_config(lhofacetsstg_secret_name)

extract_query_CLHP_CLST = f"""
SELECT
  HP.CLCL_ID,
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
  '02' CLCL_CUR_STS,
  CL.MEME_RECORD_NO,
  LI.CDML_POS_IND,
  CL.PRPR_ID,
  CL.MEME_SEX,
  CL.CLCL_ME_AGE ME_AGE,
  CL.CLCL_LOW_SVC_DT SERVICE_DT,
  ME.MEME_BIRTH_DT ME_BRTH_DT,
  HP.CLHP_COVD_DAYS
FROM {LhoFacetsStgOwner}.CMC_CLHP_HOSP HP
INNER JOIN tempdb..{DriverTable} TMP ON TMP.CLM_ID = HP.CLCL_ID
INNER JOIN {LhoFacetsStgOwner}.CMC_CLCL_CLAIM CL ON TMP.CLM_ID = CL.CLCL_ID
INNER JOIN {LhoFacetsStgOwner}.CMC_CDML_CL_LINE LI ON TMP.CLM_ID = LI.CLCL_ID AND LI.CDML_SEQ_NO = 1
LEFT JOIN {LhoFacetsStgOwner}.CMC_MEME_MEMBER ME ON CL.MEME_CK = ME.MEME_CK
"""
df_CLHP_CLST = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_CLHP_CLST)
    .load()
)

extract_query_CLHP_STAY = f"""
SELECT
  LI.CLCL_ID,
  sum(LI.CDML_UNITS) CDML_UNITS
FROM
  {LhoFacetsStgOwner}.CMC_CLHP_HOSP HP,
  tempdb..{DriverTable} TMP,
  {LhoFacetsStgOwner}.CMC_CDML_CL_LINE LI,
  {LhoFacetsStgOwner}.CMC_RCRC_RC_DESC C
WHERE
  TMP.CLM_ID = LI.CLCL_ID
  AND HP.CLCL_ID = LI.CLCL_ID
  AND LI.RCRC_ID = C.RCRC_ID
  AND LI.RCRC_ID >= '0100'
  AND LI.RCRC_ID <= '0219'
GROUP BY
  LI.CLCL_ID
"""
df_CLHP_STAY = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_CLHP_STAY)
    .load()
)

extract_query_proc_in = f"""
SELECT
  HI.CLCL_ID,
  HI.CLHI_TYPE AS HP_PROC_CD_TYP,
  HI.IPCD_ID
FROM
  tempdb..{DriverTable} TMP
  JOIN {LhoFacetsStgOwner}.CMC_CLHI_PROC HI ON TMP.CLM_ID = HI.CLCL_ID
  LEFT JOIN {LhoFacetsStgOwner}.CMC_CLHP_HOSP HP ON TMP.CLM_ID = HP.CLCL_ID
"""
df_proc_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_proc_in)
    .load()
)

extract_query_CLIP_ITS_HOST_AGE_SEX = f"""
SELECT
  CLM.CLCL_ID,
  CLM.CLCL_LOW_SVC_DT,
  CLM.CLCL_ME_AGE,
  CLM.MEME_SEX,
  ITS.CLIP_SEX,
  ITS.CLIP_BIRTH_DT
FROM {LhoFacetsStgOwner}.CMC_CLIP_ITS_PATNT ITS,
     {LhoFacetsStgOwner}.CMC_CLMI_MISC MISC,
     {LhoFacetsStgOwner}.CMC_CLCL_CLAIM CLM,
     tempdb..{DriverTable} DRVR
WHERE
  DRVR.CLM_ID = CLM.CLCL_ID
  AND CLM.CLCL_ID = MISC.CLCL_ID
  AND (MISC.CLMI_ITS_SUB_TYPE = 'E' OR MISC.CLMI_ITS_SUB_TYPE = 'S')
  AND DRVR.CLM_ID = ITS.CLCL_ID
"""
df_CLIP_ITS_HOST_AGE_SEX = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_CLIP_ITS_HOST_AGE_SEX)
    .load()
)

extract_query_lnkClmHosp = f"""
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
FROM {LhoFacetsStgOwner}.CMC_CLHP_HOSP HOSP,
     {LhoFacetsStgOwner}.CMC_CLCL_CLAIM CLAIM,
     tempdb..{DriverTable} DRVR
WHERE
  HOSP.CLCL_ID = DRVR.CLM_ID
  AND HOSP.CLCL_ID = CLAIM.CLCL_ID
"""
df_lnkClmHosp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_lnkClmHosp)
    .load()
)

extract_query_PROVIDER_INFO = f"""
SELECT
  PRPR_ID,
  PRPR_MCTR_TYPE
FROM {LhoFacetsStgOwner}.CMC_PRPR_PROV
"""
df_PROVIDER_INFO = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_PROVIDER_INFO)
    .load()
)

extract_query_CLMD_CLST = f"""
SELECT
  CLMD.CLCL_ID,
  CLMD.CLMD_TYPE,
  CLMD.IDCD_ID,
  CLMD.CLMD_POA_IND,
  IDCD.IDCD_TYPE
FROM {LhoFacetsStgOwner}.CMC_CLMD_DIAG CLMD,
     {LhoFacetsStgOwner}.CMC_IDCD_DIAG_CD IDCD,
     tempdb..{DriverTable} TMP
WHERE
  TMP.CLM_ID = CLMD.CLCL_ID
  AND CLMD.IDCD_ID = IDCD.IDCD_ID
ORDER BY
  CLMD.CLCL_ID ASC,
  CLMD.CLMD_TYPE ASC,
  CLMD.IDCD_ID ASC,
  IDCD.IDCD_TYPE ASC
"""
df_CLMD_CLST = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_CLMD_CLST)
    .load()
)

params_ContProcedureCodes = {}
df_proc_all = ContProcedureCodes(df_proc_in, params_ContProcedureCodes)

df_hf_drg_proc_all = df_proc_all.dropDuplicates(["HE_CLM_ID"])
df_proc_lkup = df_hf_drg_proc_all

params_ContDiagnosisCodes = {}
df_clmd_diag = ContDiagnosisCodes(df_CLMD_CLST, params_ContDiagnosisCodes)

df_hf_fclty_clm_drg_diag_all = df_clmd_diag.dropDuplicates(["HE_CLM_ID"])
df_diag_lkup = df_hf_fclty_clm_drg_diag_all

df_its_data_filtered = df_CLIP_ITS_HOST_AGE_SEX.filter(
    (F.col("CLCL_LOW_SVC_DT") >= F.col("CLIP_BIRTH_DT"))
    & (F.col("CLCL_LOW_SVC_DT") >= F.lit("1753-01-01"))
    & (F.col("CLIP_BIRTH_DT") >= F.lit("1753-01-01"))
)

df_its_data = (
    df_its_data_filtered.withColumn(
        "CLIP_BIRTH_DT",
        F.when(
            (F.trim(F.col("CLIP_BIRTH_DT")) == "") | (F.length(F.trim(F.col("CLIP_BIRTH_DT"))) == 0),
            F.lit("1753-01-01")
        ).otherwise(F.col("CLIP_BIRTH_DT"))
    )
    .select(
        F.col("CLCL_ID"),
        F.col("CLCL_LOW_SVC_DT"),
        F.col("CLCL_ME_AGE"),
        F.col("MEME_SEX"),
        F.col("CLIP_SEX"),
        F.col("CLIP_BIRTH_DT")
    )
)

df_hosp_stay = (
    df_CLHP_STAY
    .select(
        F.col("CLCL_ID"),
        F.col("CDML_UNITS").alias("UNITS")
    )
    .dropDuplicates(["CLCL_ID"])
)

df_hfts_its_age_sex = (
    df_its_data
    .dropDuplicates(["CLCL_ID"])
    .select(
        F.col("CLCL_ID"),
        F.col("CLCL_LOW_SVC_DT"),
        F.col("CLCL_ME_AGE"),
        F.col("MEME_SEX"),
        F.col("CLIP_SEX"),
        F.col("CLIP_BIRTH_DT")
    )
)

jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)

extract_query_UWS_FCLTY_TYP = f"""
SELECT (FCLTY_TYP_CD)
FROM {UWSOwner}.FCLTY_TYP
WHERE EXCPT_LOS_HNDLNG_IN = 'Y'
UNION
SELECT (PROV_TYP_CD)
FROM {UWSOwner}.PROV_TYP
WHERE EXCPT_LOS_HNDLNG_IN = 'Y'
"""
df_LOS_EXCP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("query", extract_query_UWS_FCLTY_TYP)
    .load()
)

df_los_excp_lookup = df_LOS_EXCP.dropDuplicates(["FCLTY_TYP_CD"])

df_merge_left = df_PROVIDER_INFO.alias("PROVIDER_INFO")
df_merge_lookup = df_los_excp_lookup.alias("LOS_EXCP_LOOKUP")
df_merge_joined = (
    df_merge_left.join(
        df_merge_lookup,
        on=[df_merge_left["PRPR_MCTR_TYPE"] == df_merge_lookup["FCLTY_TYP_CD"]],
        how="left"
    )
)
df_merge_filtered = df_merge_joined.filter(F.col("FCLTY_TYP_CD").isNotNull())
df_prov_type = df_merge_filtered.select(
    F.col("PROVIDER_INFO.PRPR_ID").alias("PROV_ID"),
    F.col("LOS_EXCP_LOOKUP.FCLTY_TYP_CD").alias("PROV_TYP_CD")
)

df_hf_fclty_clm_provtyp = df_prov_type.dropDuplicates(["PROV_ID"])
df_provtyp_lookup = df_hf_fclty_clm_provtyp.select(
    F.col("PROV_ID"),
    F.col("PROV_TYP_CD").alias("PROV_TYP")
)

xformstay_clhp_clst = df_CLHP_CLST.alias("CLHP_CLST")
xformstay_hosp_stay = df_hosp_stay.alias("hf_fcts_fclty_clm_hosp_stay")
xformstay_provtyp = df_provtyp_lookup.alias("provtyp_lookup")
xformstay_its_age = df_hfts_its_age_sex.alias("hf_fcts_fclty_clm_its_age_sex")

joined_xformstay = xformstay_clhp_clst.join(
    xformstay_hosp_stay,
    on=[xformstay_clhp_clst["CLCL_ID"] == xformstay_hosp_stay["CLCL_ID"]],
    how="left"
).join(
    xformstay_provtyp,
    on=[xformstay_clhp_clst["PRPR_ID"] == xformstay_provtyp["PROV_ID"]],
    how="left"
).join(
    xformstay_its_age,
    on=[xformstay_clhp_clst["CLCL_ID"] == xformstay_its_age["CLCL_ID"]],
    how="left"
)

joined_xformstay = joined_xformstay.select(
    F.col("CLHP_CLST.*"),
    F.col("hf_fcts_fclty_clm_hosp_stay.UNITS").alias("LOOKUP_UNITS"),
    F.col("provtyp_lookup.PROV_TYP").alias("LOOKUP_PROV_TYP"),
    F.col("hf_fcts_fclty_clm_its_age_sex.CLCL_LOW_SVC_DT").alias("ITS_CLCL_LOW_SVC_DT"),
    F.col("hf_fcts_fclty_clm_its_age_sex.CLCL_ME_AGE").alias("ITS_CLCL_ME_AGE"),
    F.col("hf_fcts_fclty_clm_its_age_sex.MEME_SEX").alias("ITS_MEME_SEX"),
    F.col("hf_fcts_fclty_clm_its_age_sex.CLIP_SEX").alias("ITS_CLIP_SEX"),
    F.col("hf_fcts_fclty_clm_its_age_sex.CLIP_BIRTH_DT").alias("ITS_CLIP_BIRTH_DT")
)

df_XformStay = joined_xformstay.withColumn(
    "svDischargeDate",
    F.when(
        (F.col("CLHP_DC_DT").substr(F.lit(1), F.lit(10)) != F.lit("1753-01-01"))
        & (F.col("CLHP_DC_DT") != F.lit("NA"))
        & (F.col("CLHP_DC_DT") != F.lit("UNK"))
        & (F.trim(F.col("CLHP_DC_DT")).length() > 0),
        F.col("CLHP_DC_DT")
    ).otherwise(F.lit("UNK"))
).withColumn(
    "svStatementDate",
    F.when(
        (F.col("CLHP_STAMENT_TO_DT").substr(F.lit(1), F.lit(10)) != F.lit("1753-01-01"))
        & (F.col("CLHP_STAMENT_TO_DT") != F.lit("NA"))
        & (F.col("CLHP_STAMENT_TO_DT") != F.lit("UNK"))
        & (F.trim(F.col("CLHP_STAMENT_TO_DT")).length() > 0),
        F.col("CLHP_STAMENT_TO_DT")
    ).otherwise(F.lit("UNK"))
).withColumn(
    "svGrouperDate",
    F.when(
        F.col("svDischargeDate") != F.lit("UNK"),
        F.col("svDischargeDate")
    ).when(
        F.col("svStatementDate") != F.lit("UNK"),
        F.col("svStatementDate")
    ).otherwise(F.lit("2199-12-31 00:00:00.000"))
).withColumn(
    "svGender",
    F.when(
        F.col("ITS_CLCL_LOW_SVC_DT").isNull(),
        F.when(
            F.col("MEME_SEX") == F.lit("M"), F.lit(1)
        ).otherwise(F.lit(2))
    ).otherwise(
        F.when(
            F.col("ITS_CLIP_SEX") == F.lit("M"), F.lit(1)
        ).otherwise(F.lit(2))
    )
).withColumn(
    "svMemberAge",
    F.when(
        F.col("ITS_CLCL_LOW_SVC_DT").isNull(),
        F.col("ME_AGE")
    ).otherwise(
        F.when(
            (F.datediff(F.col("ITS_CLCL_LOW_SVC_DT"), F.col("ITS_CLIP_BIRTH_DT")) / 365.25 > 999) |
            (F.datediff(F.col("ITS_CLCL_LOW_SVC_DT"), F.col("ITS_CLIP_BIRTH_DT")) < 0),
            F.lit(999)
        ).otherwise(
            (F.datediff(F.col("ITS_CLCL_LOW_SVC_DT"), F.col("ITS_CLIP_BIRTH_DT")) / 365.25).cast(T.IntegerType())
        )
    )
).withColumn(
    "svBirthDate",
    F.when(
        F.col("ITS_CLCL_LOW_SVC_DT").isNull(),
        F.col("ME_BRTH_DT")
    ).otherwise(
        F.col("ITS_CLIP_BIRTH_DT")
    )
)

df_LengthOfStay = df_XformStay.withColumn(
    "CLCL_LOS",
    F.when(
        F.col("CDML_POS_IND").isNull(),
        F.lit(None).cast(T.IntegerType())
    ).otherwise(
        F.when(
            F.col("CDML_POS_IND") == F.lit("O"),
            F.lit(None).cast(T.IntegerType())
        ).otherwise(
            F.when(
                (F.col("LOOKUP_UNITS").isNull() | (F.col("LOOKUP_UNITS") == 0))
                & (F.col("LOOKUP_PROV_TYP").isNotNull()),
                F.lit(None).cast(T.IntegerType())
            ).otherwise(
                F.call_udf("LGTH.INPAT.HOSP.STAY",
                           F.col("CLCL_CUR_STS"),
                           F.col("CLHP_DC_STAT"),
                           F.when(F.col("LOOKUP_UNITS").isNull(), F.lit(0)).otherwise(F.col("LOOKUP_UNITS")),
                           F.col("CLHP_STAMENT_FR_DT"),
                           F.col("CLHP_STAMENT_TO_DT"),
                           F.col("CLHP_ADM_DT"),
                           F.col("CLHP_DC_DT"))
            )
        )
    )
)

df_LengthOfStay_out = df_LengthOfStay.select(
    F.col("CLCL_ID").alias("CLCL_ID"),
    F.col("CLCL_LOS").alias("CLCL_LOS")
)

df_hf_fcts_fclty_clm_length_of_stay = df_LengthOfStay_out.dropDuplicates(["CLCL_ID"])

df_DSLink85 = df_LengthOfStay.select(
    F.col("CLCL_ID").alias("HE_CLCL_ID"),
    F.col("svMemberAge").alias("AGE"),
    F.col("svGender").alias("SEX"),
    F.col("CLHP_DC_STAT").alias("DISP"),
    F.col("SERVICE_DT").alias("SERVICE_DT"),
    F.col("svGrouperDate").alias("DISCHARGE_DT"),
    F.col("svBirthDate").alias("BIRTH_DT")
)

df_hf_lhofcts_fcltyclm_grouper_dedupe = df_DSLink85.dropDuplicates(["HE_CLCL_ID"])

length_of_stay_lookup_join = df_lnkClmHosp.alias("lnkClmHosp").join(
    df_hf_fcts_fclty_clm_length_of_stay.alias("LengthOfStayLookup"),
    on=[F.col("lnkClmHosp.CLCL_ID") == F.col("LengthOfStayLookup.CLCL_ID")],
    how="left"
).select(
    F.col("lnkClmHosp.CLCL_ID").alias("CLCL_ID"),
    F.col("LengthOfStayLookup.CLCL_LOS").alias("CLCL_LOS"),
    F.col("lnkClmHosp.*")
)

df_TrnsStripField = length_of_stay_lookup_join.select(
    F.expr('REGEXP_REPLACE(CLCL_ID,"[\\x0A\\x0D\\x09]","")').alias("CLCL_ID"),
    F.col("MEME_CK").alias("MEME_CK"),
    F.expr('REGEXP_REPLACE(CLHP_FAC_TYPE,"[\\x0A\\x0D\\x09]","")').alias("CLHP_FAC_TYPE"),
    F.expr('REGEXP_REPLACE(CLHP_BILL_CLASS,"[\\x0A\\x0D\\x09]","")').alias("CLHP_BILL_CLASS"),
    F.expr('REGEXP_REPLACE(CLHP_FREQUENCY,"[\\x0A\\x0D\\x09]","")').alias("CLHP_FREQUENCY"),
    F.expr('REGEXP_REPLACE(CLHP_PRPR_ID_ADM,"[\\x0A\\x0D\\x09]","")').alias("CLHP_PRPR_ID_ADM"),
    F.expr('REGEXP_REPLACE(CLHP_PRPR_ID_OTH1,"[\\x0A\\x0D\\x09]","")').alias("CLHP_PRPR_ID_OTH1"),
    F.expr('REGEXP_REPLACE(CLHP_PRPR_ID_OTH2,"[\\x0A\\x0D\\x09]","")').alias("CLHP_PRPR_ID_OTH2"),
    F.expr('REGEXP_REPLACE(CLHP_ADM_TYP,"[\\x0A\\x0D\\x09]","")').alias("CLHP_ADM_TYP"),
    F.col("CLHP_ADM_DT").alias("CLHP_ADM_DT"),
    F.expr('REGEXP_REPLACE(CLHP_DC_STAT,"[\\x0A\\x0D\\x09]","")').alias("CLHP_DC_STAT"),
    F.col("CLHP_DC_DT").alias("CLHP_DC_DT"),
    F.col("CLHP_STAMENT_FR_DT").alias("CLHP_STAMENT_FR_DT"),
    F.col("CLHP_STAMENT_TO_DT").alias("CLHP_STAMENT_TO_DT"),
    F.expr('REGEXP_REPLACE(CLHP_MED_REC_NO,"[\\x0A\\x0D\\x09]","")').alias("CLHP_MED_REC_NO"),
    F.expr('REGEXP_REPLACE(CLHP_IPCD_METH,"[\\x0A\\x0D\\x09]","")').alias("CLHP_IPCD_METH"),
    F.lit("0.00").alias("CLHP_EXT_PRICE_AMT"),
    F.expr('REGEXP_REPLACE(AGRG_ID,"[\\x0A\\x0D\\x09]","")').alias("AGRG_ID"),
    F.expr('REGEXP_REPLACE(CLHP_INPUT_AGRG_ID,"[\\x0A\\x0D\\x09]","")').alias("CLHP_INPUT_AGRG_ID"),
    F.expr('REGEXP_REPLACE(CLHP_ADM_HOUR_CD,"[\\x0A\\x0D\\x09]","")').alias("CLHP_ADM_HOUR_CD"),
    F.expr('REGEXP_REPLACE(CLHP_ADM_SOURCE,"[\\x0A\\x0D\\x09]","")').alias("CLHP_ADM_SOURCE"),
    F.expr('REGEXP_REPLACE(CLHP_DC_HOUR_CD,"[\\x0A\\x0D\\x09]","")').alias("CLHP_DC_HOUR_CD"),
    F.col("CLHP_BIRTH_WGT").alias("CLHP_BIRTH_WGT"),
    F.col("CLHP_COVD_DAYS").alias("CLHP_COVD_DAYS"),
    F.col("CLHP_LOCK_TOKEN").alias("CLHP_LOCK_TOKEN"),
    F.col("ATXR_SOURCE_ID").alias("ATXR_SOURCE_ID"),
    F.col("CLCL_LOS").alias("CLCL_LOS"),
    F.col("CLCL_RECD_DT").alias("CLCL_RECD_DT"),
    F.col("CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    F.col("CLCL_CL_SUB_TYPE").alias("CLCL_CL_SUB_TYPE")
)

df_LhoClmHospExtr = df_TrnsStripField

df_LhoClmHospExtr_final = df_LhoClmHospExtr.select(
    F.rpad(F.col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    F.col("MEME_CK"),
    F.rpad(F.col("CLHP_FAC_TYPE"), 2, " ").alias("CLHP_FAC_TYPE"),
    F.rpad(F.col("CLHP_BILL_CLASS"), 1, " ").alias("CLHP_BILL_CLASS"),
    F.rpad(F.col("CLHP_FREQUENCY"), 1, " ").alias("CLHP_FREQUENCY"),
    F.rpad(F.col("CLHP_PRPR_ID_ADM"), 12, " ").alias("CLHP_PRPR_ID_ADM"),
    F.rpad(F.col("CLHP_PRPR_ID_OTH1"), 12, " ").alias("CLHP_PRPR_ID_OTH1"),
    F.rpad(F.col("CLHP_PRPR_ID_OTH2"), 12, " ").alias("CLHP_PRPR_ID_OTH2"),
    F.rpad(F.col("CLHP_ADM_TYP"), 2, " ").alias("CLHP_ADM_TYP"),
    F.col("CLHP_ADM_DT").alias("CLHP_ADM_DT"),
    F.rpad(F.col("CLHP_DC_STAT"), 2, " ").alias("CLHP_DC_STAT"),
    F.col("CLHP_DC_DT").alias("CLHP_DC_DT"),
    F.col("CLHP_STAMENT_FR_DT").alias("CLHP_STAMENT_FR_DT"),
    F.col("CLHP_STAMENT_TO_DT").alias("CLHP_STAMENT_TO_DT"),
    F.rpad(F.col("CLHP_MED_REC_NO"), 17, " ").alias("CLHP_MED_REC_NO"),
    F.rpad(F.col("CLHP_IPCD_METH"), 1, " ").alias("CLHP_IPCD_METH"),
    F.col("CLHP_EXT_PRICE_AMT").alias("CLHP_EXT_PRICE_AMT"),
    F.rpad(F.col("AGRG_ID"), 4, " ").alias("AGRG_ID"),
    F.rpad(F.col("CLHP_INPUT_AGRG_ID"), 4, " ").alias("CLHP_INPUT_AGRG_ID"),
    F.rpad(F.col("CLHP_ADM_HOUR_CD"), 2, " ").alias("CLHP_ADM_HOUR_CD"),
    F.rpad(F.col("CLHP_ADM_SOURCE"), 1, " ").alias("CLHP_ADM_SOURCE"),
    F.rpad(F.col("CLHP_DC_HOUR_CD"), 2, " ").alias("CLHP_DC_HOUR_CD"),
    F.col("CLHP_BIRTH_WGT"),
    F.col("CLHP_COVD_DAYS"),
    F.col("CLHP_LOCK_TOKEN"),
    F.col("ATXR_SOURCE_ID"),
    F.col("CLCL_LOS"),
    F.col("CLCL_RECD_DT"),
    F.col("CLCL_PAID_DT"),
    F.rpad(F.col("CLCL_CL_SUB_TYPE"), 1, " ").alias("CLCL_CL_SUB_TYPE")
)

write_files(
    df_LhoClmHospExtr_final,
    f"{adls_path}/verified/LhoFctsClmFcltyExtr.LhoFctsClmFcltyExtr.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_grouper_xform_out = df_hf_lhofcts_fcltyclm_grouper_dedupe.dropDuplicates(["HE_CLCL_ID"])

jdbc_url_tmp, jdbc_props_tmp = get_db_config(lhofacetsstg_secret_name)
(
    df_grouper_xform_out.write
    .format("jdbc")
    .option("url", jdbc_url_tmp)
    .options(**jdbc_props_tmp)
    .option("dbtable", "tempdb.TMP_DRG_CLAIMS")
    .mode("overwrite")
    .save()
)

merge_query = None  # Not a merge but an insert/truncate pattern. So we do direct table load above.

extract_query_TMP_DRG_CLAIMS = f"""
SELECT
  T.HE_CLCL_ID HE_CLM_ID,
  T.AGE,
  T.SEX,
  T.DISP,
  T.SERVICE_DT,
  T.DISCHARGE_DT,
  T.BIRTH_DT,
  R.GRP_VER,
  R.GRP_TYP,
  R.CD_CLS,
  (SELECT ThisYears.GRP_VER
   FROM tempdb..TMP_GROUPER_VER ThisYears
   WHERE '{CurrentDate}' >= VER_START_DT
     AND '{CurrentDate}' <= VER_END_DT ) CUR_DRG_VER,
  (SELECT ThisYears.GRP_TYP
   FROM tempdb..TMP_GROUPER_VER ThisYears
   WHERE '{CurrentDate}' >= VER_START_DT
     AND '{CurrentDate}' <= VER_END_DT ) CUR_GRP_TYP,
  (SELECT ThisYears.CD_CLS
   FROM tempdb..TMP_GROUPER_VER ThisYears
   WHERE '{CurrentDate}' >= VER_START_DT
     AND '{CurrentDate}' <= VER_END_DT ) CUR_CD_CLS
FROM tempdb..TMP_DRG_CLAIMS T,
     tempdb..TMP_GROUPER_VER R
WHERE T.DISCHARGE_DT >= R.VER_START_DT
  AND T.DISCHARGE_DT <= R.VER_END_DT
"""
df_grouper_claims = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_tmp)
    .options(**jdbc_props_tmp)
    .option("query", extract_query_TMP_DRG_CLAIMS)
    .load()
)

transform_proc_lkup = df_proc_lkup.alias("proc_lkup")
transform_diag_lkup = df_diag_lkup.alias("diag_lkup")
transform_grouper_claims = df_grouper_claims.alias("grouper_claims")

joined_transform = transform_grouper_claims.join(
    transform_proc_lkup,
    on=[transform_grouper_claims["HE_CLM_ID"] == transform_proc_lkup["HE_CLM_ID"]],
    how="left"
).join(
    transform_diag_lkup,
    on=[transform_grouper_claims["HE_CLM_ID"] == transform_diag_lkup["HE_CLM_ID"]],
    how="left"
)

df_transform = joined_transform.select(
    F.col("grouper_claims.*"),
    F.col("diag_lkup.*"),
    F.col("proc_lkup.*")
)

df_transform_enriched = df_transform.withColumn(
    "ADAYS",
    F.call_udf("LENGTH.DAYS.SYB.TS", F.col("grouper_claims.BIRTH_DT"), F.col("grouper_claims.SERVICE_DT"), F.lit(0))
).withColumn(
    "DDAYS",
    F.call_udf("LENGTH.DAYS.SYB.TS", F.col("grouper_claims.BIRTH_DT"), F.col("grouper_claims.DISCHARGE_DT"), F.lit(0))
).withColumn(
    "BWGHT",
    F.call_udf("LENGTH.DAYS.SYB.TS", F.col("grouper_claims.BIRTH_DT"), F.col("grouper_claims.SERVICE_DT"), F.lit(0))
).withColumn(
    "ValidateGeneratedDRGVersion",
    F.when(
        F.col("grouper_claims.DISCHARGE_DT").substr(F.lit(1), F.lit(10)) <= F.lit("2011-09-30"),
        F.lit("29")
    ).otherwise(F.col("grouper_claims.GRP_VER"))
).withColumn(
    "ValidateNormativeDRGVersion",
    F.col("grouper_claims.CUR_DRG_VER")
).withColumn(
    "svProcCdTyp1",
    F.when(
        F.col("diag_lkup.DiagCdTyp1") == F.lit("ICD9"),
        F.lit("BR")
    ).otherwise(F.lit("BBR"))
).withColumn(
    "svProcCdTypOther",
    F.when(
        F.col("diag_lkup.DiagCdTyp1") == F.lit("ICD9"),
        F.lit("BQ")
    ).otherwise(F.lit("BBQ"))
)

df_grouper_generated_poa = df_transform_enriched.select(
    F.rpad(F.col("HE_CLM_ID"), 12, " ").alias("HE_CLM_ID"),
    F.rpad(F.col("diag_lkup.DIAG1"), 10, " ").alias("DIAG_CD_1"),
    F.rpad(F.col("diag_lkup.DIAG2"), 10, " ").alias("DIAG_CD_2"),
    F.rpad(F.col("diag_lkup.DIAG3"), 10, " ").alias("DIAG_CD_3"),
    F.rpad(F.col("diag_lkup.DIAG4"), 10, " ").alias("DIAG_CD_4"),
    F.rpad(F.col("diag_lkup.DIAG5"), 10, " ").alias("DIAG_CD_5"),
    F.rpad(F.col("diag_lkup.DIAG6"), 10, " ").alias("DIAG_CD_6"),
    F.rpad(F.col("diag_lkup.DIAG7"), 10, " ").alias("DIAG_CD_7"),
    F.rpad(F.col("diag_lkup.DIAG8"), 10, " ").alias("DIAG_CD_8"),
    F.rpad(F.col("diag_lkup.DIAG9"), 10, " ").alias("DIAG_CD_9"),
    F.rpad(F.col("diag_lkup.DIAG10"), 10, " ").alias("DIAG_CD_10"),
    F.rpad(F.col("diag_lkup.DIAG11"), 10, " ").alias("DIAG_CD_11"),
    F.rpad(F.col("diag_lkup.DIAG12"), 10, " ").alias("DIAG_CD_12"),
    F.rpad(F.col("diag_lkup.DIAG13"), 10, " ").alias("DIAG_CD_13"),
    F.rpad(F.col("diag_lkup.DIAG14"), 10, " ").alias("DIAG_CD_14"),
    F.rpad(F.col("diag_lkup.DIAG15"), 10, " ").alias("DIAG_CD_15"),
    F.rpad(F.col("diag_lkup.DIAG16"), 10, " ").alias("DIAG_CD_16"),
    F.rpad(F.col("diag_lkup.DIAG17"), 10, " ").alias("DIAG_CD_17"),
    F.rpad(F.col("diag_lkup.DIAG18"), 10, " ").alias("DIAG_CD_18"),
    F.rpad(F.col("diag_lkup.DIAG19"), 10, " ").alias("DIAG_CD_19"),
    F.rpad(F.col("diag_lkup.DIAG20"), 10, " ").alias("DIAG_CD_20"),
    F.rpad(F.col("diag_lkup.DIAG21"), 10, " ").alias("DIAG_CD_21"),
    F.rpad(F.col("diag_lkup.DIAG22"), 10, " ").alias("DIAG_CD_22"),
    F.rpad(F.col("diag_lkup.DIAG23"), 10, " ").alias("DIAG_CD_23"),
    F.rpad(F.col("diag_lkup.DIAG24"), 10, " ").alias("DIAG_CD_24"),
    F.rpad(F.col("diag_lkup.DIAG25"), 10, " ").alias("DIAG_CD_25"),
    F.rpad(F.col("diag_lkup.DiagOrdCd1"), 3, " ").alias("DX_TYPE_1"),
    F.rpad(F.col("diag_lkup.DiagOrdCd2"), 3, " ").alias("DX_TYPE_2"),
    F.rpad(F.col("diag_lkup.DiagOrdCd3"), 3, " ").alias("DX_TYPE_3"),
    F.rpad(F.col("diag_lkup.DiagOrdCd4"), 3, " ").alias("DX_TYPE_4"),
    F.rpad(F.col("diag_lkup.DiagOrdCd5"), 3, " ").alias("DX_TYPE_5"),
    F.rpad(F.col("diag_lkup.DiagOrdCd6"), 3, " ").alias("DX_TYPE_6"),
    F.rpad(F.col("diag_lkup.DiagOrdCd7"), 3, " ").alias("DX_TYPE_7"),
    F.rpad(F.col("diag_lkup.DiagOrdCd8"), 3, " ").alias("DX_TYPE_8"),
    F.rpad(F.col("diag_lkup.DiagOrdCd9"), 3, " ").alias("DX_TYPE_9"),
    F.rpad(F.col("diag_lkup.DiagOrdCd10"), 3, " ").alias("DX_TYPE_10"),
    F.rpad(F.col("diag_lkup.DiagOrdCd11"), 3, " ").alias("DX_TYPE_11"),
    F.rpad(F.col("diag_lkup.DiagOrdCd12"), 3, " ").alias("DX_TYPE_12"),
    F.rpad(F.col("diag_lkup.DiagOrdCd13"), 3, " ").alias("DX_TYPE_13"),
    F.rpad(F.col("diag_lkup.DiagOrdCd14"), 3, " ").alias("DX_TYPE_14"),
    F.rpad(F.col("diag_lkup.DiagOrdCd15"), 3, " ").alias("DX_TYPE_15"),
    F.rpad(F.col("diag_lkup.DiagOrdCd16"), 3, " ").alias("DX_TYPE_16"),
    F.rpad(F.col("diag_lkup.DiagOrdCd17"), 3, " ").alias("DX_TYPE_17"),
    F.rpad(F.col("diag_lkup.DiagOrdCd18"), 3, " ").alias("DX_TYPE_18"),
    F.rpad(F.col("diag_lkup.DiagOrdCd19"), 3, " ").alias("DX_TYPE_19"),
    F.rpad(F.col("diag_lkup.DiagOrdCd20"), 3, " ").alias("DX_TYPE_20"),
    F.rpad(F.col("diag_lkup.DiagOrdCd21"), 3, " ").alias("DX_TYPE_21"),
    F.rpad(F.col("diag_lkup.DiagOrdCd22"), 3, " ").alias("DX_TYPE_22"),
    F.rpad(F.col("diag_lkup.DiagOrdCd23"), 3, " ").alias("DX_TYPE_23"),
    F.rpad(F.col("diag_lkup.DiagOrdCd24"), 3, " ").alias("DX_TYPE_24"),
    F.rpad(F.col("diag_lkup.DiagOrdCd25"), 3, " ").alias("DX_TYPE_25"),
    F.rpad(F.col("proc_lkup.PROC1"), 10, " ").alias("PROC_CD_1"),
    F.rpad(F.col("proc_lkup.PROC2"), 10, " ").alias("PROC_CD_2"),
    F.rpad(F.col("proc_lkup.PROC3"), 10, " ").alias("PROC_CD_3"),
    F.rpad(F.col("proc_lkup.PROC4"), 10, " ").alias("PROC_CD_4"),
    F.rpad(F.col("proc_lkup.PROC5"), 10, " ").alias("PROC_CD_5"),
    F.rpad(F.col("proc_lkup.PROC6"), 10, " ").alias("PROC_CD_6"),
    F.rpad(F.col("proc_lkup.PROC7"), 10, " ").alias("PROC_CD_7"),
    F.rpad(F.col("proc_lkup.PROC8"), 10, " ").alias("PROC_CD_8"),
    F.rpad(F.col("proc_lkup.PROC9"), 10, " ").alias("PROC_CD_9"),
    F.rpad(F.col("proc_lkup.PROC10"), 10, " ").alias("PROC_CD_10"),
    F.rpad(F.col("proc_lkup.PROC11"), 10, " ").alias("PROC_CD_11"),
    F.rpad(F.col("proc_lkup.PROC12"), 10, " ").alias("PROC_CD_12"),
    F.rpad(F.col("proc_lkup.PROC13"), 10, " ").alias("PROC_CD_13"),
    F.rpad(F.col("proc_lkup.PROC14"), 10, " ").alias("PROC_CD_14"),
    F.rpad(F.col("proc_lkup.PROC15"), 10, " ").alias("PROC_CD_15"),
    F.rpad(F.col("proc_lkup.PROC16"), 10, " ").alias("PROC_CD_16"),
    F.rpad(F.col("proc_lkup.PROC17"), 10, " ").alias("PROC_CD_17"),
    F.rpad(F.col("proc_lkup.PROC18"), 10, " ").alias("PROC_CD_18"),
    F.rpad(F.col("proc_lkup.PROC19"), 10, " ").alias("PROC_CD_19"),
    F.rpad(F.col("proc_lkup.PROC20"), 10, " ").alias("PROC_CD_20"),
    F.rpad(F.col("proc_lkup.PROC21"), 10, " ").alias("PROC_CD_21"),
    F.rpad(F.col("proc_lkup.PROC22"), 10, " ").alias("PROC_CD_22"),
    F.rpad(F.col("proc_lkup.PROC23"), 10, " ").alias("PROC_CD_23"),
    F.rpad(F.col("proc_lkup.PROC24"), 10, " ").alias("PROC_CD_24"),
    F.rpad(F.col("proc_lkup.PROC25"), 10, " ").alias("PROC_CD_25"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC1"))) > 0, F.col("svProcCdTyp1")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_1"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC2"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_2"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC3"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_3"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC4"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_4"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC5"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_5"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC6"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_6"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC7"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_7"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC8"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_8"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC9"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_9"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC10"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_10"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC11"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_11"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC12"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_12"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC13"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_13"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC14"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_14"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC15"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_15"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC16"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_16"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC17"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_17"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC18"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_18"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC19"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_19"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC20"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_20"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC21"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_21"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC22"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_22"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC23"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_23"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC24"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_24"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC25"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_25"),
    F.rpad(
        F.trim(F.right(F.lit("000") + F.expr("Oconv(trim(grouper_claims.AGE),'MDO')"), 3)),
        3, " "
    ).alias("AGE"),
    F.rpad(F.col("grouper_claims.SEX").cast(T.StringType()), 1, " ").alias("SEX"),
    F.rpad(F.col("grouper_claims.DISP"), 2, " ").alias("DISP"),
    F.lit("00000").alias("DRG"),
    F.lit("00").alias("MDC"),
    F.rpad(F.col("ValidateGeneratedDRGVersion"), 2, " ").alias("VERS"),
    F.lit("0").alias("FILL1"),
    F.lit("11").alias("OP"),
    F.lit("00").alias("RTN_CODE"),
    F.rpad(
        F.when(F.col("diag_lkup.DIAG1").isNull(), F.lit("0"))
         .otherwise(F.col("diag_lkup.NUM_DIAG").cast(T.StringType())),
        2, " "
    ).alias("NO_DX"),
    F.rpad(
        F.when(F.col("proc_lkup.PROC1").isNull(), F.lit("0"))
         .otherwise(F.col("proc_lkup.NUM_PROCS").cast(T.StringType())),
        2, " "
    ).alias("NO_OP"),
    F.rpad(
        F.when(
            (F.col("grouper_claims.AGE") > 0)
            | (F.col("ADAYS") > 999)
            | (F.col("ADAYS") < 0),
            F.lit("999")
        ).otherwise(
            F.trim(F.right(F.lit("000") + F.expr("Oconv(trim(ADAYS),'MDO')"), 3))
        ),
        3, " "
    ).alias("ADAYS_A"),
    F.rpad(
        F.when(
            (F.col("grouper_claims.AGE") > 0)
            | (F.col("DDAYS") > 999)
            | (F.col("DDAYS") < 0),
            F.lit("999")
        ).otherwise(
            F.trim(F.right(F.lit("000") + F.expr("Oconv(trim(DDAYS),'MDO')"), 3))
        ),
        3, " "
    ).alias("ADAYS_D"),
    F.rpad(
        F.when(
            F.col("grouper_claims.AGE") > 0,
            F.lit("0000")
        ).otherwise(
            F.when(F.col("BWGHT") > 28, F.lit("0000")).otherwise(F.lit("9999"))
        ),
        4, " "
    ).alias("BWGT"),
    F.rpad(F.col("grouper_claims.GRP_TYP"), 2, " ").alias("GTYPE"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_1"), 1, " ").alias("CLMD_POA_IND_1"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_2"), 1, " ").alias("CLMD_POA_IND_2"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_3"), 1, " ").alias("CLMD_POA_IND_3"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_4"), 1, " ").alias("CLMD_POA_IND_4"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_5"), 1, " ").alias("CLMD_POA_IND_5"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_6"), 1, " ").alias("CLMD_POA_IND_6"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_7"), 1, " ").alias("CLMD_POA_IND_7"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_8"), 1, " ").alias("CLMD_POA_IND_8"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_9"), 1, " ").alias("CLMD_POA_IND_9"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_10"), 1, " ").alias("CLMD_POA_IND_10"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_11"), 1, " ").alias("CLMD_POA_IND_11"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_12"), 1, " ").alias("CLMD_POA_IND_12"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_13"), 1, " ").alias("CLMD_POA_IND_13"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_14"), 1, " ").alias("CLMD_POA_IND_14"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_15"), 1, " ").alias("CLMD_POA_IND_15"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_16"), 1, " ").alias("CLMD_POA_IND_16"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_17"), 1, " ").alias("CLMD_POA_IND_17"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_18"), 1, " ").alias("CLMD_POA_IND_18"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_19"), 1, " ").alias("CLMD_POA_IND_19"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_20"), 1, " ").alias("CLMD_POA_IND_20"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_21"), 1, " ").alias("CLMD_POA_IND_21"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_22"), 1, " ").alias("CLMD_POA_IND_22"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_23"), 1, " ").alias("CLMD_POA_IND_23"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_24"), 1, " ").alias("CLMD_POA_IND_24"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_25"), 1, " ").alias("CLMD_POA_IND_25"),
    F.rpad(F.col("grouper_claims.CD_CLS"), 2, " ").alias("CODE_CLASS_IND"),
    F.lit("01").alias("PTNT_TYP"),
    F.lit("DRG").alias("CLS_TYP")
)

df_nmtv_never_event = df_transform_enriched.select(
    F.rpad(F.col("HE_CLM_ID"), 12, " ").alias("HE_CLM_ID"),
    F.rpad(F.col("diag_lkup.DIAG1"), 10, " ").alias("DIAG_CD_1"),
    F.rpad(F.col("diag_lkup.DIAG2"), 10, " ").alias("DIAG_CD_2"),
    F.rpad(F.col("diag_lkup.DIAG3"), 10, " ").alias("DIAG_CD_3"),
    F.rpad(F.col("diag_lkup.DIAG4"), 10, " ").alias("DIAG_CD_4"),
    F.rpad(F.col("diag_lkup.DIAG5"), 10, " ").alias("DIAG_CD_5"),
    F.rpad(F.col("diag_lkup.DIAG6"), 10, " ").alias("DIAG_CD_6"),
    F.rpad(F.col("diag_lkup.DIAG7"), 10, " ").alias("DIAG_CD_7"),
    F.rpad(F.col("diag_lkup.DIAG8"), 10, " ").alias("DIAG_CD_8"),
    F.rpad(F.col("diag_lkup.DIAG9"), 10, " ").alias("DIAG_CD_9"),
    F.rpad(F.col("diag_lkup.DIAG10"), 10, " ").alias("DIAG_CD_10"),
    F.rpad(F.col("diag_lkup.DIAG11"), 10, " ").alias("DIAG_CD_11"),
    F.rpad(F.col("diag_lkup.DIAG12"), 10, " ").alias("DIAG_CD_12"),
    F.rpad(F.col("diag_lkup.DIAG13"), 10, " ").alias("DIAG_CD_13"),
    F.rpad(F.col("diag_lkup.DIAG14"), 10, " ").alias("DIAG_CD_14"),
    F.rpad(F.col("diag_lkup.DIAG15"), 10, " ").alias("DIAG_CD_15"),
    F.rpad(F.col("diag_lkup.DIAG16"), 10, " ").alias("DIAG_CD_16"),
    F.rpad(F.col("diag_lkup.DIAG17"), 10, " ").alias("DIAG_CD_17"),
    F.rpad(F.col("diag_lkup.DIAG18"), 10, " ").alias("DIAG_CD_18"),
    F.rpad(F.col("diag_lkup.DIAG19"), 10, " ").alias("DIAG_CD_19"),
    F.rpad(F.col("diag_lkup.DIAG20"), 10, " ").alias("DIAG_CD_20"),
    F.rpad(F.col("diag_lkup.DIAG21"), 10, " ").alias("DIAG_CD_21"),
    F.rpad(F.col("diag_lkup.DIAG22"), 10, " ").alias("DIAG_CD_22"),
    F.rpad(F.col("diag_lkup.DIAG23"), 10, " ").alias("DIAG_CD_23"),
    F.rpad(F.col("diag_lkup.DIAG24"), 10, " ").alias("DIAG_CD_24"),
    F.rpad(F.col("diag_lkup.DIAG25"), 10, " ").alias("DIAG_CD_25"),
    F.rpad(F.col("diag_lkup.DiagOrdCd1"), 3, " ").alias("DX_TYPE_1"),
    F.rpad(F.col("diag_lkup.DiagOrdCd2"), 3, " ").alias("DX_TYPE_2"),
    F.rpad(F.col("diag_lkup.DiagOrdCd3"), 3, " ").alias("DX_TYPE_3"),
    F.rpad(F.col("diag_lkup.DiagOrdCd4"), 3, " ").alias("DX_TYPE_4"),
    F.rpad(F.col("diag_lkup.DiagOrdCd5"), 3, " ").alias("DX_TYPE_5"),
    F.rpad(F.col("diag_lkup.DiagOrdCd6"), 3, " ").alias("DX_TYPE_6"),
    F.rpad(F.col("diag_lkup.DiagOrdCd7"), 3, " ").alias("DX_TYPE_7"),
    F.rpad(F.col("diag_lkup.DiagOrdCd8"), 3, " ").alias("DX_TYPE_8"),
    F.rpad(F.col("diag_lkup.DiagOrdCd9"), 3, " ").alias("DX_TYPE_9"),
    F.rpad(F.col("diag_lkup.DiagOrdCd10"), 3, " ").alias("DX_TYPE_10"),
    F.rpad(F.col("diag_lkup.DiagOrdCd11"), 3, " ").alias("DX_TYPE_11"),
    F.rpad(F.col("diag_lkup.DiagOrdCd12"), 3, " ").alias("DX_TYPE_12"),
    F.rpad(F.col("diag_lkup.DiagOrdCd13"), 3, " ").alias("DX_TYPE_13"),
    F.rpad(F.col("diag_lkup.DiagOrdCd14"), 3, " ").alias("DX_TYPE_14"),
    F.rpad(F.col("diag_lkup.DiagOrdCd15"), 3, " ").alias("DX_TYPE_15"),
    F.rpad(F.col("diag_lkup.DiagOrdCd16"), 3, " ").alias("DX_TYPE_16"),
    F.rpad(F.col("diag_lkup.DiagOrdCd17"), 3, " ").alias("DX_TYPE_17"),
    F.rpad(F.col("diag_lkup.DiagOrdCd18"), 3, " ").alias("DX_TYPE_18"),
    F.rpad(F.col("diag_lkup.DiagOrdCd19"), 3, " ").alias("DX_TYPE_19"),
    F.rpad(F.col("diag_lkup.DiagOrdCd20"), 3, " ").alias("DX_TYPE_20"),
    F.rpad(F.col("diag_lkup.DiagOrdCd21"), 3, " ").alias("DX_TYPE_21"),
    F.rpad(F.col("diag_lkup.DiagOrdCd22"), 3, " ").alias("DX_TYPE_22"),
    F.rpad(F.col("diag_lkup.DiagOrdCd23"), 3, " ").alias("DX_TYPE_23"),
    F.rpad(F.col("diag_lkup.DiagOrdCd24"), 3, " ").alias("DX_TYPE_24"),
    F.rpad(F.col("diag_lkup.DiagOrdCd25"), 3, " ").alias("DX_TYPE_25"),
    F.rpad(F.col("proc_lkup.PROC1"), 10, " ").alias("PROC_CD_1"),
    F.rpad(F.col("proc_lkup.PROC2"), 10, " ").alias("PROC_CD_2"),
    F.rpad(F.col("proc_lkup.PROC3"), 10, " ").alias("PROC_CD_3"),
    F.rpad(F.col("proc_lkup.PROC4"), 10, " ").alias("PROC_CD_4"),
    F.rpad(F.col("proc_lkup.PROC5"), 10, " ").alias("PROC_CD_5"),
    F.rpad(F.col("proc_lkup.PROC6"), 10, " ").alias("PROC_CD_6"),
    F.rpad(F.col("proc_lkup.PROC7"), 10, " ").alias("PROC_CD_7"),
    F.rpad(F.col("proc_lkup.PROC8"), 10, " ").alias("PROC_CD_8"),
    F.rpad(F.col("proc_lkup.PROC9"), 10, " ").alias("PROC_CD_9"),
    F.rpad(F.col("proc_lkup.PROC10"), 10, " ").alias("PROC_CD_10"),
    F.rpad(F.col("proc_lkup.PROC11"), 10, " ").alias("PROC_CD_11"),
    F.rpad(F.col("proc_lkup.PROC12"), 10, " ").alias("PROC_CD_12"),
    F.rpad(F.col("proc_lkup.PROC13"), 10, " ").alias("PROC_CD_13"),
    F.rpad(F.col("proc_lkup.PROC14"), 10, " ").alias("PROC_CD_14"),
    F.rpad(F.col("proc_lkup.PROC15"), 10, " ").alias("PROC_CD_15"),
    F.rpad(F.col("proc_lkup.PROC16"), 10, " ").alias("PROC_CD_16"),
    F.rpad(F.col("proc_lkup.PROC17"), 10, " ").alias("PROC_CD_17"),
    F.rpad(F.col("proc_lkup.PROC18"), 10, " ").alias("PROC_CD_18"),
    F.rpad(F.col("proc_lkup.PROC19"), 10, " ").alias("PROC_CD_19"),
    F.rpad(F.col("proc_lkup.PROC20"), 10, " ").alias("PROC_CD_20"),
    F.rpad(F.col("proc_lkup.PROC21"), 10, " ").alias("PROC_CD_21"),
    F.rpad(F.col("proc_lkup.PROC22"), 10, " ").alias("PROC_CD_22"),
    F.rpad(F.col("proc_lkup.PROC23"), 10, " ").alias("PROC_CD_23"),
    F.rpad(F.col("proc_lkup.PROC24"), 10, " ").alias("PROC_CD_24"),
    F.rpad(F.col("proc_lkup.PROC25"), 10, " ").alias("PROC_CD_25"),
    F.rpad(F.col("svProcCdTyp1"), 3, " ").alias("PX_TYPE_1"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC2"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_2"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC3"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_3"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC4"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_4"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC5"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_5"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC6"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_6"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC7"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_7"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC8"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_8"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC9"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_9"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC10"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_10"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC11"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_11"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC12"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_12"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC13"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_13"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC14"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_14"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC15"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_15"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC16"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_16"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC17"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_17"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC18"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_18"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC19"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_19"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC20"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_20"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC21"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_21"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC22"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_22"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC23"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_23"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC24"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_24"),
    F.rpad(
        F.when(F.length(F.trim(F.col("proc_lkup.PROC25"))) > 0, F.col("svProcCdTypOther")).otherwise(F.lit("  ")),
        3, " "
    ).alias("PX_TYPE_25"),
    F.rpad(
        F.trim(F.right(F.lit("000") + F.expr("Oconv(trim(grouper_claims.AGE),'MDO')"), 3)),
        3, " "
    ).alias("AGE"),
    F.rpad(F.col("grouper_claims.SEX").cast(T.StringType()), 1, " ").alias("SEX"),
    F.rpad(F.col("grouper_claims.DISP"), 2, " ").alias("DISP"),
    F.lit("00000").alias("DRG"),
    F.lit("00").alias("MDC"),
    F.rpad(F.col("ValidateNormativeDRGVersion"), 2, " ").alias("VERS"),
    F.lit("0").alias("FILL1"),
    F.lit("11").alias("OP"),
    F.lit("00").alias("RTN_CODE"),
    F.rpad(
        F.when(F.col("diag_lkup.DIAG1").isNull(), F.lit("0"))
         .otherwise(F.col("diag_lkup.NUM_DIAG").cast(T.StringType())),
        2, " "
    ).alias("NO_DX"),
    F.rpad(
        F.when(F.col("proc_lkup.PROC1").isNull(), F.lit("0"))
         .otherwise(F.col("proc_lkup.NUM_PROCS").cast(T.StringType())),
        2, " "
    ).alias("NO_OP"),
    F.rpad(
        F.when(
            (F.col("grouper_claims.AGE") > 0)
            or (F.col("ADAYS") > 999)
            or (F.col("ADAYS") < 0),
            F.lit("999")
        ).otherwise(
            F.trim(F.right(F.lit("000") + F.expr("Oconv(trim(ADAYS),'MDO')"), 3))
        ),
        3, " "
    ).alias("ADAYS_A"),
    F.rpad(
        F.when(
            (F.col("grouper_claims.AGE") > 0)
            or (F.col("DDAYS") > 999)
            or (F.col("DDAYS") < 0),
            F.lit("999")
        ).otherwise(
            F.trim(F.right(F.lit("000") + F.expr("Oconv(trim(DDAYS),'MDO')"), 3))
        ),
        3, " "
    ).alias("ADAYS_D"),
    F.rpad(
        F.when(
            F.col("grouper_claims.AGE") > 0,
            F.lit("0000")
        ).otherwise(
            F.when(F.col("BWGHT") > 28, F.lit("0000")).otherwise(F.lit("9999"))
        ),
        4, " "
    ).alias("BWGT"),
    F.rpad(F.col("grouper_claims.CUR_GRP_TYP"), 2, " ").alias("GTYPE"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_1"), 1, " ").alias("CLMD_POA_IND_1"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_2"), 1, " ").alias("CLMD_POA_IND_2"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_3"), 1, " ").alias("CLMD_POA_IND_3"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_4"), 1, " ").alias("CLMD_POA_IND_4"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_5"), 1, " ").alias("CLMD_POA_IND_5"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_6"), 1, " ").alias("CLMD_POA_IND_6"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_7"), 1, " ").alias("CLMD_POA_IND_7"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_8"), 1, " ").alias("CLMD_POA_IND_8"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_9"), 1, " ").alias("CLMD_POA_IND_9"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_10"), 1, " ").alias("CLMD_POA_IND_10"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_11"), 1, " ").alias("CLMD_POA_IND_11"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_12"), 1, " ").alias("CLMD_POA_IND_12"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_13"), 1, " ").alias("CLMD_POA_IND_13"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_14"), 1, " ").alias("CLMD_POA_IND_14"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_15"), 1, " ").alias("CLMD_POA_IND_15"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_16"), 1, " ").alias("CLMD_POA_IND_16"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_17"), 1, " ").alias("CLMD_POA_IND_17"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_18"), 1, " ").alias("CLMD_POA_IND_18"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_19"), 1, " ").alias("CLMD_POA_IND_19"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_20"), 1, " ").alias("CLMD_POA_IND_20"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_21"), 1, " ").alias("CLMD_POA_IND_21"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_22"), 1, " ").alias("CLMD_POA_IND_22"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_23"), 1, " ").alias("CLMD_POA_IND_23"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_24"), 1, " ").alias("CLMD_POA_IND_24"),
    F.rpad(F.col("diag_lkup.CLMD_POA_IND_25"), 1, " ").alias("CLMD_POA_IND_25"),
    F.rpad(F.col("grouper_claims.CUR_CD_CLS"), 2, " ").alias("CODE_CLASS_IND"),
    F.lit("01").alias("PTNT_TYP"),
    F.lit("DRG").alias("CLS_TYP")
)

write_files(
    df_grouper_generated_poa,
    f"{adls_path}/verified/LhoFctsFcltyClm_DRGGenPOA.LhoFctsGenPOA.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_nmtv_never_event,
    f"{adls_path}/verified/LhoFctsFcltyClm_DRGNormPOA.LhoFctsNormPOA.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

env_path = "dap/" + "<...>" + "/"  # Placeholder for environment if needed in after-job routine
params = {
  "EnvProjectPath": f"dap/<...>/Integrate",
  "File_Path": "Jobs/claim/facility/facility",
  "File_Name": "LhoFctsClmFcltyExtr"
}
dbutils.notebook.run("../../../../../sequencer_routines/Move_File", timeout_seconds=3600, arguments=params)