# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     FctsClmExtrnlMbrshExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from CMC_CLMI_MISC to a landing file for the IDS to build external member rows in FctsClmExtrnlMbrTrns
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	CMC_CLIS_ITS_SUBSC
# MAGIC                 CMC_CLIP_ITS_PATNT
# MAGIC                 CMC_CLMI_MISC
# MAGIC                 ALPHA_PFX
# MAGIC                
# MAGIC   
# MAGIC HASH FILES:  hf_clis_its_subsc
# MAGIC                         hf_clip_its_patnt
# MAGIC                         hf_extrnl_mbr_ntwk_sh_nm
# MAGIC                         hf_alpha_pfx_nm
# MAGIC 
# MAGIC                        hf_clm_nasco_dup_bypass - do not clear
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   STRIP.FIELD
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                        Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                --------------------------------       -------------------------------   ----------------------------       
# MAGIC Landon Hall             03/22/2005 -                                  Originally Programmed
# MAGIC Hugh Sisson -          06/21/2005 -                                  Modified to pull (for ITS claims) the group name from ALPHA_PFX table claims 
# MAGIC                                                                                        using the three-letter prefix for the subscriber's ID number listed on the claim.
# MAGIC Hugh Sisson -          06/21/2005 -                                  Modifications to the various Sub address-related fields and Group Name
# MAGIC Steph Goddard        03/2006                                          Sequencer changes -combined extract, transform, primary key, changed parameters
# MAGIC Ralph Tucker          03/24/2006                                    Changed  ETL to check CLMI_MISC_INFO.CLMI_ITS_SB_ID_ACT)[4,17] and 
# MAGIC                                                                                         CLMI_MISC_INFO.CLMI_ITS_SBSB_ID)[4,17] for a zero length replacing with a 'NA'  in SBSB_ID
# MAGIC BJ Luce                  03/24/2006                                     add hf_clm_nasco_dup_bypass built in ClmDriverBuild, identifies claims that are nasco dups. 
# MAGIC                                                                                       If the claim is on the file, a row is not generated for it in IDS. However, an R row will be build for it if the status if '91'
# MAGIC Sanderw                12/08/2006   Project 1756  -           Reversal logix added for new status codes 89 and  and 99
# MAGIC Oliver Nielsen         08/13/2007                                    Added Balancing Snapshot File                                                       devIDS30                       Steph Goddard            8/30/07
# MAGIC 
# MAGIC Bhoomi Dasari         2008-08-06      3567(Primary Key)    Changed primary key process from hash file to DB2 table            devlIDS                            Steph Goddard            08/20/2008
# MAGIC Ralph Tucker         10/02/2008     Blue Exchange - 3223   Added two new fields (ACTL_SUB_ID, SUBMT_SUB_ID)     devlIDScur                     Steph Goddard            12/04/2008
# MAGIC Nikhil Sinha            02/28/2017     Data Catalyst - 30001   Add Mbr_SK to CLM_EXTRNL_MBRSH                                                                          Kalyan Neelam               2017-03-07       
# MAGIC                                                                                              for hosted Members
# MAGIC Prabhu ES              2022-02-28      S2S Remediation     MSSQL connection parameters added                                        IntegrateDev5                       Manasa Andru         2022-06-08
# MAGIC 
# MAGIC Goutham K            2024-03-19       US-613735            Added function in the transformer for field GRP_NM to remove      IntegrateDev2                      Reddy Sanam           2024-03-19
# MAGIC                                                                                       | (pipeline charecters)

# MAGIC Run STRIP.FIELD transform on fields that might have Carriage Return, Line Feed, and  Tab
# MAGIC Pulling Facets Claim CLMI Trigger Data for a specific Time Period
# MAGIC Lookup returns only ITS claims with a Mbr_SK, rest of the claims are assigned a value of Mbr_SK
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
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmExtrnlMbrshPK
# COMMAND ----------

RunID = get_widget_value('RunID','')
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
FacetsOwner = get_widget_value('FacetsOwner','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
ids_secret_name = get_widget_value('ids_secret_name','')
facets_secret_name = get_widget_value('facets_secret_name','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

# Hashed file "hf_clm_fcts_reversals" (Scenario C - read parquet)
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

# Hashed file "clm_nasco_dup_bypass" (Scenario C - read parquet)
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# DB2Connector "MBR_DRVR" (IDS)
extract_query_mbr_drvr = f"""SELECT distinct drvr.CLM_ID,drvr.FIRST_NM, drvr.MBR_BRTH_DT_SK, BCBSAdrvr.MBR_UNIQ_KEY 
  FROM {IDSOwner}.W_CLM_EXTRNL_MBR_DRVR drvr
 INNER
  JOIN {IDSOwner}.W_CLM_EXTRNL_BCBSA_MBR_DRVR BCBSAdrvr
    ON drvr.SUB_ID = BCBSAdrvr.BCBSA_ITS_SUB_ID
   AND drvr.FIRST_NM = BCBSAdrvr.BCBSA_MBR_FIRST_NM 
   AND ((drvr.MBR_BRTH_DT_SK   = BCBSAdrvr.BCBSA_MBR_BRTH_DT_SK 
                 AND drvr.MBR_BRTH_DT_SK <> 'UNK'
                 AND drvr.MBR_BRTH_DT_SK <> '1800-01-01')
       OR (drvr.MBR_BRTH_DT_SK   <> BCBSAdrvr.BCBSA_MBR_BRTH_DT_SK 
                 AND drvr.MBR_BRTH_DT_SK = 'UNK'
                 AND drvr.MBR_BRTH_DT_SK = '1800-01-01'))"""
df_MBR_DRVR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_mbr_drvr)
    .load()
)

# Hashed file "hf_clm_extrnl_mbr_sk" (Scenario A - deduplicate on key columns: CLM_ID, FIRST_NM, MBR_BRTH_DT_SK)
df_hf_clm_extrnl_mbr_sk = dedup_sort(
    df_MBR_DRVR,
    partition_cols=["CLM_ID","FIRST_NM","MBR_BRTH_DT_SK"],
    sort_cols=[]
)

# DB2Connector "ALPHA_PFX" (IDS)
extract_query_alpha_pfx = f"SELECT ALPHA_PFX_CD, ALPHA_PFX_NM FROM {IDSOwner}.ALPHA_PFX"
df_ALPHA_PFX = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_alpha_pfx)
    .load()
)

# Hashed file "hf_alpha_pfx_nm" (Scenario A - deduplicate on key columns: ALPHA_PFX_CD)
df_hf_alpha_pfx_nm = dedup_sort(
    df_ALPHA_PFX,
    partition_cols=["ALPHA_PFX_CD"],
    sort_cols=[]
)

# ODBCConnector "FACETS" (3 output pins => separate queries)

# 1) lnkClmMISCin
extract_query_facets_1 = f"""SELECT M.CLCL_ID,CLMI_ITS_SUB_TYPE,CLMI_ITS_SB_ID_ACT,CLMI_ITS_SBSB_ID,NWNW_ID 
FROM {FacetsOwner}.CMC_CLMI_MISC M, {FacetsOwner}.CMC_CLCL_CLAIM C, tempdb..{DriverTable} TMP 
WHERE TMP.CLM_ID = M.CLCL_ID 
  AND TMP.CLM_ID = C.CLCL_ID
  AND ( CLMI_ITS_SUB_TYPE = 'S'  OR CLMI_ITS_SUB_TYPE = 'E') 
ORDER BY M.CLCL_ID,CLMI_ITS_SUB_TYPE,CLMI_ITS_SB_ID_ACT,CLMI_ITS_SBSB_ID,NWNW_ID"""
df_FACETS_lnkClmMISCin = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_facets_1)
    .load()
)

# 2) lnkclis
extract_query_facets_2 = f"""SELECT S.CLCL_ID,S.MEME_CK,CLIS_LAST_NAME,CLIS_FIRST_NAME,CLIS_MID_INIT,CLIS_ADDR1,CLIS_ADDR2,CLIS_CITY,CLIS_STATE,CLIS_ZIP,CLIS_SEX,CLIS_BIRTH_DT,CLIS_GRGR_NAME,CLIS_ADDL_DATA 
FROM {FacetsOwner}.CMC_CLIS_ITS_SUBSC S, {FacetsOwner}.CMC_CLCL_CLAIM C, tempdb..{DriverTable} TMP 
WHERE TMP.CLM_ID = S.CLCL_ID 
  AND TMP.CLM_ID = C.CLCL_ID"""
df_FACETS_lnkclis = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_facets_2)
    .load()
)

# 3) lnkclip
extract_query_facets_3 = f"""SELECT P.CLCL_ID,P.MEME_CK,CLIP_LAST_NAME,CLIP_FIRST_NAME,CLIP_MID_INIT,CLIP_ADDR1,CLIP_ADDR2,CLIP_CITY,CLIP_STATE,CLIP_ZIP,CLIP_SEX,CLIP_BIRTH_DT,CLIP_MARITAL_IND,CLIP_REL,CLIP_EMPLOY_STS_CD,CLIP_SB_GROUP_NO,CLIP_SB_EMP_SCHL,CLIP_SB_EMP_ADDR,CLIP_SB_EMP_CITY,CLIP_SB_EMP_STATE,CLIP_SB_EMP_ZIP,CLIP_STUDENT_STS
FROM {FacetsOwner}.CMC_CLIP_ITS_PATNT P, {FacetsOwner}.CMC_CLCL_CLAIM C, tempdb..{DriverTable} TMP
WHERE TMP.CLM_ID = P.CLCL_ID 
  AND TMP.CLM_ID = C.CLCL_ID"""
df_FACETS_lnkclip = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_facets_3)
    .load()
)

# Hashed file "hf_clis_its_subsc" (Scenario A - deduplicate on key cols: CLCL_ID)
df_hf_clis_its_subsc = dedup_sort(
    df_FACETS_lnkclis,
    partition_cols=["CLCL_ID"],
    sort_cols=[]
)

# Hashed file "hf_clip_its_patnt" (Scenario A - deduplicate on key cols: CLCL_ID)
df_hf_clip_its_patnt = dedup_sort(
    df_FACETS_lnkclip,
    partition_cols=["CLCL_ID"],
    sort_cols=[]
)

# Transformer "StripField" => filter on CLMI_ITS_SUB_TYPE in ('S','E') already in query, 
# plus convert(CHAR(10):CHAR(13):CHAR(9)) => remove \x0A,\x0D,\x09 from columns
df_StripField = df_FACETS_lnkClmMISCin.select(
    F.regexp_replace(F.col("CLCL_ID"), "[\\x0A\\x0D\\x09]", "").alias("CLCL_ID"),
    F.regexp_replace(F.col("CLMI_ITS_SUB_TYPE"), "[\\x0A\\x0D\\x09]", "").alias("ITS_SUB_TYPE"),
    F.regexp_replace(F.col("CLMI_ITS_SB_ID_ACT"), "[\\x0A\\x0D\\x09]", "").alias("CLMI_ITS_SB_ID_ACT"),
    F.regexp_replace(F.col("CLMI_ITS_SBSB_ID"), "[\\x0A\\x0D\\x09]", "").alias("CLMI_ITS_SBSB_ID"),
    F.regexp_replace(F.col("NWNW_ID"), "[\\x0A\\x0D\\x09]", "").alias("NWNW_ID")
)

# DB2Connector "IdsNtwk" => read from #$IDSOwner#.NTWK
extract_query_ntwk = f"SELECT RTRIM(NTWK_ID) as NTWK_ID, RTRIM(NTWK_SH_NM) as NTWK_SH_NM FROM {IDSOwner}.NTWK"
df_IdsNtwk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ntwk)
    .load()
)

# Hashed file "hf_extrnl_mbr_ntwk_sh_nm" (Scenario A - deduplicate on key col: NTWK_ID)
df_hf_extrnl_mbr_ntwk_sh_nm = dedup_sort(
    df_IdsNtwk,
    partition_cols=["NTWK_ID"],
    sort_cols=[]
)

# Transformer "Get_Ntwk_Sh_Nm" => primary link: df_StripField, lookup link left join: df_hf_extrnl_mbr_ntwk_sh_nm
df_Get_Ntwk_Sh_Nm = (
    df_StripField.alias("CLMI_ITS_Misc")
    .join(
        df_hf_extrnl_mbr_ntwk_sh_nm.alias("ndxNtwkShNm"),
        F.trim(F.col("CLMI_ITS_Misc.NWNW_ID")) == F.col("ndxNtwkShNm.NTWK_ID"),
        how="left"
    )
    .select(
        F.col("CLMI_ITS_Misc.CLCL_ID").alias("CLCL_ID"),
        F.col("CLMI_ITS_Misc.ITS_SUB_TYPE").alias("CLMI_ITS_SUB_TYPE"),
        F.col("CLMI_ITS_Misc.CLMI_ITS_SB_ID_ACT").alias("CLMI_ITS_SB_ID_ACT"),
        F.col("CLMI_ITS_Misc.CLMI_ITS_SBSB_ID").alias("CLMI_ITS_SBSB_ID"),
        F.col("CLMI_ITS_Misc.NWNW_ID").alias("NWNW_ID"),
        F.when(F.isnull(F.col("ndxNtwkShNm.NTWK_ID")), F.lit("UNK")).otherwise(F.col("ndxNtwkShNm.NTWK_SH_NM")).alias("NTWK_SH_NM")
    )
)

# Transformer "BusinessRules" => primary: df_Get_Ntwk_Sh_Nm alias "CLMI_w_Ntwk"
# left lookups with: df_hf_clis_its_subsc => "clis_subsc", df_hf_clip_its_patnt => "clip_patnt", df_hf_alpha_pfx_nm => "lnkAlphaPfx"
# alpha prefix join expression:
join_alpha_expr = F.when(
    F.length(F.trim(F.col("CLMI_w_Ntwk.CLMI_ITS_SB_ID_ACT")))==0,
    F.when(
        F.length(F.trim(F.col("CLMI_w_Ntwk.CLMI_ITS_SBSB_ID")))==0,
        F.lit("UNK")
    ).otherwise(F.expr("substring(trim(CLMI_w_Ntwk.CLMI_ITS_SBSB_ID),1,3)"))
).otherwise(F.expr("substring(trim(CLMI_w_Ntwk.CLMI_ITS_SB_ID_ACT),1,3)"))

df_BusinessRules_temp = (
    df_Get_Ntwk_Sh_Nm.alias("CLMI_w_Ntwk")
    .join(
        df_hf_clis_its_subsc.alias("clis_subsc"),
        F.col("CLMI_w_Ntwk.CLCL_ID") == F.col("clis_subsc.CLCL_ID"),
        how="left"
    )
    .join(
        df_hf_clip_its_patnt.alias("clip_patnt"),
        F.col("CLMI_w_Ntwk.CLCL_ID") == F.col("clip_patnt.CLCL_ID"),
        how="left"
    )
    .join(
        df_hf_alpha_pfx_nm.alias("lnkAlphaPfx"),
        join_alpha_expr == F.col("lnkAlphaPfx.ALPHA_PFX_CD"),
        how="left"
    )
)

# Now select all final columns from "BusinessRules" output => name it df_BusinessRules
df_BusinessRules = df_BusinessRules_temp.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("current_date")                .alias("FIRST_RECYC_DT"),               # from parameter: references "CurrentDate" or we can do current_date()
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS"),F.lit(";"),F.trim(F.col("CLMI_w_Ntwk.CLCL_ID"))).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_EXTRNL_MBRSH_SK"),
    F.rpad(F.trim(F.col("CLMI_w_Ntwk.CLCL_ID")),18," ").alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(
        F.when(
            (F.length(F.trim(F.col("clip_patnt.CLIP_SEX")))==0) | F.col("clip_patnt.CLIP_SEX").isNull(),
            F.lit("NA")
        ).otherwise(F.trim(F.col("clip_patnt.CLIP_SEX"))),
        3," "
    ).alias("MBR_GNDR_CD_CD"),
    F.rpad(
        F.when(
            (F.length(F.trim(F.col("clip_patnt.CLIP_REL")))==0) | F.col("clip_patnt.CLIP_REL").isNull(),
            F.lit("NA")
        ).otherwise(F.trim(F.col("clip_patnt.CLIP_REL"))),
        10," "
    ).alias("MBR_RELSHP_CD_CD"),
    F.rpad(
        F.date_format(F.col("clip_patnt.CLIP_BIRTH_DT"),"yyyy-MM-dd"),
        23," "
    ).alias("MBR_BRTH_DT"),
    # AlphaPfxName:
    F.regexp_replace(
        F.when(F.col("lnkAlphaPfx.ALPHA_PFX_NM")=="UNK", F.lit("UNKNOWN"))
         .otherwise(F.col("lnkAlphaPfx.ALPHA_PFX_NM")),
        "\|",""
    ).alias("GRP_NM"),
    F.when(
        (F.length(F.trim(F.col("clip_patnt.CLIP_FIRST_NAME")))==0) | F.col("clip_patnt.CLIP_FIRST_NAME").isNull(),
        F.lit("NA")
    ).otherwise(F.trim(F.col("clip_patnt.CLIP_FIRST_NAME"))).alias("MBR_FIRST_NM"),
    F.rpad(
        F.when(
            (F.length(F.trim(F.col("clip_patnt.CLIP_MID_INIT")))==0) | F.col("clip_patnt.CLIP_MID_INIT").isNull(),
            F.lit(" ")
        ).otherwise(F.trim(F.col("clip_patnt.CLIP_MID_INIT"))),
        1," "
    ).alias("MBR_MIDINIT"),
    F.when(
        (F.length(F.trim(F.col("clip_patnt.CLIP_LAST_NAME")))==0) | F.col("clip_patnt.CLIP_LAST_NAME").isNull(),
        F.lit("NA")
    ).otherwise(F.trim(F.col("clip_patnt.CLIP_LAST_NAME"))).alias("MBR_LAST_NM"),
    F.lit("NA").alias("PCKG_CD_ID"),
    F.rpad(F.trim(F.col("CLMI_w_Ntwk.NTWK_SH_NM")),6," ").alias("PATN_NTWK_SH_NM"),
    F.rpad(F.lit("NA"),9," ").alias("SUB_GRP_BASE_NO"),
    F.rpad(F.lit("NA"),4," ").alias("SUB_GRP_SECT_NO"),
    F.rpad(
        F.when(
            F.length(F.trim(F.col("CLMI_w_Ntwk.CLMI_ITS_SB_ID_ACT")))>0,
            F.when(
                F.length(F.expr("substring(trim(CLMI_w_Ntwk.CLMI_ITS_SB_ID_ACT),4,17)"))==0,
                F.lit("NA")
            ).otherwise(F.expr("substring(trim(CLMI_w_Ntwk.CLMI_ITS_SB_ID_ACT),4,17)"))
        ).otherwise(
            F.when(
                F.length(F.trim(F.col("CLMI_w_Ntwk.CLMI_ITS_SBSB_ID")))>0,
                F.when(
                    F.length(F.expr("substring(trim(CLMI_w_Ntwk.CLMI_ITS_SBSB_ID),4,17)"))==0,
                    F.lit("NA")
                ).otherwise(F.expr("substring(trim(CLMI_w_Ntwk.CLMI_ITS_SBSB_ID),4,17)"))
            ).otherwise(F.lit("NA"))
        ),
        14," "
    ).alias("SUB_ID"),
    F.when(
        (F.length(F.trim(F.col("clis_subsc.CLIS_FIRST_NAME")))==0) | F.col("clis_subsc.CLIS_FIRST_NAME").isNull(),
        F.lit("NA")
    ).otherwise(F.trim(F.col("clis_subsc.CLIS_FIRST_NAME"))).alias("SUB_FIRST_NM"),
    F.rpad(
        F.when(
            (F.length(F.trim(F.col("clis_subsc.CLIS_MID_INIT")))==0) | F.col("clis_subsc.CLIS_MID_INIT").isNull(),
            F.lit(" ")
        ).otherwise(F.trim(F.col("clis_subsc.CLIS_MID_INIT"))),
        1," "
    ).alias("SUB_MIDINIT"),
    F.when(
        (F.length(F.trim(F.col("clis_subsc.CLIS_LAST_NAME")))==0) | F.col("clis_subsc.CLIS_LAST_NAME").isNull(),
        F.lit("NA")
    ).otherwise(F.trim(F.col("clis_subsc.CLIS_LAST_NAME"))).alias("SUB_LAST_NM"),
    (
        F.when(
            (F.col("clis_subsc.CLIS_ADDR1")=="SAME") | (F.length(F.trim(F.col("clis_subsc.CLIS_ADDR1")))==0),
            F.when(F.col("clip_patnt.CLIP_ADDR1")=="SAME",F.lit("UNKNOWN")).otherwise(F.trim(F.col("clip_patnt.CLIP_ADDR1")))
        ).otherwise(F.trim(F.col("clis_subsc.CLIS_ADDR1")))
    ).alias("SUB_ADDR_LN_1"),
    (
        F.when(
            (F.col("clis_subsc.CLIS_ADDR2")=="SAME") | (F.length(F.trim(F.col("clis_subsc.CLIS_ADDR2")))==0),
            F.when(F.col("clip_patnt.CLIP_ADDR2")=="SAME",F.lit("UNKNOWN")).otherwise(F.trim(F.col("clip_patnt.CLIP_ADDR2")))
        ).otherwise(F.trim(F.col("clis_subsc.CLIS_ADDR2")))
    ).alias("SUB_ADDR_LN_2"),
    (
        F.when(
            (F.col("clis_subsc.CLIS_CITY")=="SAME") | (F.length(F.trim(F.col("clis_subsc.CLIS_CITY")))==0),
            F.when(F.col("clip_patnt.CLIP_CITY")=="SAME",F.lit("UNKNOWN")).otherwise(F.trim(F.col("clip_patnt.CLIP_CITY")))
        ).otherwise(F.trim(F.col("clis_subsc.CLIS_CITY")))
    ).alias("SUB_CITY_NM"),
    (
        F.when(
            F.length(F.trim(F.col("clis_subsc.CLIS_STATE")))==0,
            F.when(
                F.length(F.trim(F.col("clip_patnt.CLIP_STATE")))==0,
                F.lit("UNK")
            ).otherwise(F.trim(F.col("clip_patnt.CLIP_STATE")))
        ).otherwise(F.trim(F.col("clis_subsc.CLIS_STATE")))
    ).alias("CLM_EXTRNL_MBRSH_SUB_ST_CD"),
    F.rpad(F.lit("NA"),3," ").alias("SUB_CNTY_FIPS_ID"),
    F.rpad(
        F.when(
            F.length(F.trim(F.col("clis_subsc.CLIS_ZIP")))==0,
            F.when(
                F.length(F.trim(F.col("clip_patnt.CLIP_ZIP")))==0,
                F.lit("UNKNOWN")
            ).otherwise(F.trim(F.col("clip_patnt.CLIP_ZIP")))
        ).otherwise(F.trim(F.col("clis_subsc.CLIS_ZIP"))),
        11," "
    ).alias("SUB_POSTAL_CD"),
    F.when(
        F.length(F.trim(F.col("CLMI_w_Ntwk.CLMI_ITS_SB_ID_ACT")))==0,
        F.col("CLMI_w_Ntwk.CLMI_ITS_SB_ID_ACT").cast("null")
    ).otherwise(F.col("CLMI_w_Ntwk.CLMI_ITS_SB_ID_ACT")).alias("SUBMT_SUB_ID"),
    F.when(
        F.length(F.trim(F.col("CLMI_w_Ntwk.CLMI_ITS_SBSB_ID")))==0,
        F.col("CLMI_w_Ntwk.CLMI_ITS_SBSB_ID").cast("null")
    ).otherwise(F.col("CLMI_w_Ntwk.CLMI_ITS_SBSB_ID")).alias("ACTL_SUB_ID"),
    F.rpad(F.trim(F.col("clip_patnt.CLIP_FIRST_NAME")),10," ").alias("CLIP_FIRST_NAME")
)

# Transformer "Lkp_Mbr_Sk" => primary link: df_BusinessRules, lookup link: df_hf_clm_extrnl_mbr_sk on (CLM_ID=CLM_ID, CLIP_FIRST_NAME=FIRST_NM, MBR_BRTH_DT=MBR_BRTH_DT_SK)
df_Lkp_Mbr_Sk = (
    df_BusinessRules.alias("Strip")
    .join(
        df_hf_clm_extrnl_mbr_sk.alias("Mbr_Sk"),
        [
            F.col("Strip.CLM_ID")==F.col("Mbr_Sk.CLM_ID"),
            F.col("Strip.CLIP_FIRST_NAME")==F.col("Mbr_Sk.FIRST_NM"),
            F.col("Strip.MBR_BRTH_DT")==F.col("Mbr_Sk.MBR_BRTH_DT_SK")
        ],
        how="left"
    )
    .select(
        F.col("Strip.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("Strip.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("Strip.DISCARD_IN").alias("DISCARD_IN"),
        F.col("Strip.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("Strip.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("Strip.ERR_CT").alias("ERR_CT"),
        F.col("Strip.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("Strip.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Strip.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("Strip.CLM_EXTRNL_MBRSH_SK").alias("CLM_EXTRNL_MBRSH_SK"),
        F.col("Strip.CLM_ID").alias("CLM_ID"),
        F.col("Strip.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Strip.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Strip.MBR_GNDR_CD_CD").alias("MBR_GNDR_CD_CD"),
        F.col("Strip.MBR_RELSHP_CD_CD").alias("MBR_RELSHP_CD_CD"),
        F.col("Strip.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
        F.col("Strip.GRP_NM").alias("GRP_NM"),
        F.col("Strip.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
        F.col("Strip.MBR_MIDINIT").alias("MBR_MIDINIT"),
        F.col("Strip.MBR_LAST_NM").alias("MBR_LAST_NM"),
        F.col("Strip.PCKG_CD_ID").alias("PCKG_CD_ID"),
        F.col("Strip.PATN_NTWK_SH_NM").alias("PATN_NTWK_SH_NM"),
        F.col("Strip.SUB_GRP_BASE_NO").alias("SUB_GRP_BASE_NO"),
        F.col("Strip.SUB_GRP_SECT_NO").alias("SUB_GRP_SECT_NO"),
        F.col("Strip.SUB_ID").alias("SUB_ID"),
        F.col("Strip.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
        F.col("Strip.SUB_MIDINIT").alias("SUB_MIDINIT"),
        F.col("Strip.SUB_LAST_NM").alias("SUB_LAST_NM"),
        F.col("Strip.SUB_ADDR_LN_1").alias("SUB_ADDR_LN_1"),
        F.col("Strip.SUB_ADDR_LN_2").alias("SUB_ADDR_LN_2"),
        F.col("Strip.SUB_CITY_NM").alias("SUB_CITY_NM"),
        F.col("Strip.CLM_EXTRNL_MBRSH_SUB_ST_CD").alias("CLM_EXTRNL_MBRSH_SUB_ST_CD"),
        F.col("Strip.SUB_CNTY_FIPS_ID").alias("SUB_CNTY_FIPS_ID"),
        F.col("Strip.SUB_POSTAL_CD").alias("SUB_POSTAL_CD"),
        F.col("Strip.SUBMT_SUB_ID").alias("SUBMT_SUB_ID"),
        F.col("Strip.ACTL_SUB_ID").alias("ACTL_SUB_ID"),
        F.when(F.col("Mbr_Sk.MBR_UNIQ_KEY").isNull(),F.lit(1)).otherwise(F.col("Mbr_Sk.MBR_UNIQ_KEY")).alias("MBR_UNIQ_KEY")
    )
)

# Transformer "reversal_logix" => primary: df_Lkp_Mbr_Sk as "CLM_Mbr_Sk"
# Lookups: df_hf_clm_fcts_reversals as "fcts_reversals" (left), df_clm_nasco_dup_bypass as "nasco_dup_lkup" (left)
# Output 1 (DSLink113) => constraint: IsNull(nasco_dup_lkup.CLM_ID)=@TRUE
df_rev_nasco_cond = (
    df_Lkp_Mbr_Sk.alias("CLM_Mbr_Sk")
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        F.col("CLM_Mbr_Sk.CLM_ID")==F.col("nasco_dup_lkup.CLM_ID"),
        how="left"
    )
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        F.trim(F.col("CLM_Mbr_Sk.CLM_ID"))==F.col("fcts_reversals.CLCL_ID"),
        how="left"
    )
)

df_reversal_logix_DSLink113 = df_rev_nasco_cond.filter(
    F.col("nasco_dup_lkup.CLM_ID").isNull()
).select(
    F.col("CLM_Mbr_Sk.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("CLM_Mbr_Sk.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("CLM_Mbr_Sk.DISCARD_IN").alias("DISCARD_IN"),
    F.col("CLM_Mbr_Sk.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("CLM_Mbr_Sk.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("CLM_Mbr_Sk.ERR_CT").alias("ERR_CT"),
    F.col("CLM_Mbr_Sk.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("CLM_Mbr_Sk.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_Mbr_Sk.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_Mbr_Sk.CLM_EXTRNL_MBRSH_SK").alias("CLM_EXTRNL_MBRSH_SK"),
    F.col("CLM_Mbr_Sk.CLM_ID").alias("CLM_ID"),
    F.col("CLM_Mbr_Sk.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_Mbr_Sk.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_Mbr_Sk.MBR_GNDR_CD_CD").alias("MBR_GNDR_CD_CD"),
    F.col("CLM_Mbr_Sk.MBR_RELSHP_CD_CD").alias("MBR_RELSHP_CD_CD"),
    F.col("CLM_Mbr_Sk.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("CLM_Mbr_Sk.GRP_NM").alias("GRP_NM"),
    F.col("CLM_Mbr_Sk.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("CLM_Mbr_Sk.MBR_MIDINIT").alias("MBR_MIDINIT"),
    F.col("CLM_Mbr_Sk.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("CLM_Mbr_Sk.PCKG_CD_ID").alias("PCKG_CD_ID"),
    F.col("CLM_Mbr_Sk.PATN_NTWK_SH_NM").alias("PATN_NTWK_SH_NM"),
    F.col("CLM_Mbr_Sk.SUB_GRP_BASE_NO").alias("SUB_GRP_BASE_NO"),
    F.col("CLM_Mbr_Sk.SUB_GRP_SECT_NO").alias("SUB_GRP_SECT_NO"),
    F.col("CLM_Mbr_Sk.SUB_ID").alias("SUB_ID"),
    F.col("CLM_Mbr_Sk.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("CLM_Mbr_Sk.SUB_MIDINIT").alias("SUB_MIDINIT"),
    F.col("CLM_Mbr_Sk.SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("CLM_Mbr_Sk.SUB_ADDR_LN_1").alias("SUB_ADDR_LN_1"),
    F.col("CLM_Mbr_Sk.SUB_ADDR_LN_2").alias("SUB_ADDR_LN_2"),
    F.col("CLM_Mbr_Sk.SUB_CITY_NM").alias("SUB_CITY_NM"),
    F.col("CLM_Mbr_Sk.CLM_EXTRNL_MBRSH_SUB_ST_CD").alias("CLM_EXTRNL_MBRSH_SUB_ST_CD"),
    F.col("CLM_Mbr_Sk.SUB_CNTY_FIPS_ID").alias("SUB_CNTY_FIPS_ID"),
    F.col("CLM_Mbr_Sk.SUB_POSTAL_CD").alias("SUB_POSTAL_CD"),
    F.col("CLM_Mbr_Sk.SUBMT_SUB_ID").alias("SUBMT_SUB_ID"),
    F.col("CLM_Mbr_Sk.ACTL_SUB_ID").alias("ACTL_SUB_ID"),
    F.col("CLM_Mbr_Sk.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

df_reversal_logix_Reversals = df_rev_nasco_cond.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull())
    & (
        (F.col("fcts_reversals.CLCL_CUR_STS")==F.lit("89"))
        | (F.col("fcts_reversals.CLCL_CUR_STS")==F.lit("91"))
        | (F.col("fcts_reversals.CLCL_CUR_STS")==F.lit("99"))
    )
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"),1," ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"),1," ").alias("PASS_THRU_IN"),
    F.col("current_date").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS"),F.lit(";"),F.trim(F.col("CLM_Mbr_Sk.CLM_ID")),F.lit("R")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_EXTRNL_MBRSH_SK"),
    F.rpad(F.concat(F.trim(F.col("CLM_Mbr_Sk.CLM_ID")),F.lit("R")),18," ").alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("CLM_Mbr_Sk.MBR_GNDR_CD_CD"),3," ").alias("MBR_GNDR_CD_CD"),
    F.rpad(F.col("CLM_Mbr_Sk.MBR_RELSHP_CD_CD"),10," ").alias("MBR_RELSHP_CD_CD"),
    F.rpad(F.col("CLM_Mbr_Sk.MBR_BRTH_DT"),23," ").alias("MBR_BRTH_DT"),
    F.col("CLM_Mbr_Sk.GRP_NM").alias("GRP_NM"),
    F.col("CLM_Mbr_Sk.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.rpad(F.col("CLM_Mbr_Sk.MBR_MIDINIT"),1," ").alias("MBR_MIDINIT"),
    F.col("CLM_Mbr_Sk.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("CLM_Mbr_Sk.PCKG_CD_ID").alias("PCKG_CD_ID"),
    F.rpad(F.col("CLM_Mbr_Sk.PATN_NTWK_SH_NM"),6," ").alias("PATN_NTWK_SH_NM"),
    F.rpad(F.col("CLM_Mbr_Sk.SUB_GRP_BASE_NO"),9," ").alias("SUB_GRP_BASE_NO"),
    F.rpad(F.col("CLM_Mbr_Sk.SUB_GRP_SECT_NO"),4," ").alias("SUB_GRP_SECT_NO"),
    F.rpad(F.col("CLM_Mbr_Sk.SUB_ID"),14," ").alias("SUB_ID"),
    F.col("CLM_Mbr_Sk.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.rpad(F.col("CLM_Mbr_Sk.SUB_MIDINIT"),1," ").alias("SUB_MIDINIT"),
    F.col("CLM_Mbr_Sk.SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("CLM_Mbr_Sk.SUB_ADDR_LN_1").alias("SUB_ADDR_LN_1"),
    F.col("CLM_Mbr_Sk.SUB_ADDR_LN_2").alias("SUB_ADDR_LN_2"),
    F.col("CLM_Mbr_Sk.SUB_CITY_NM").alias("SUB_CITY_NM"),
    F.col("CLM_Mbr_Sk.CLM_EXTRNL_MBRSH_SUB_ST_CD").alias("CLM_EXTRNL_MBRSH_SUB_ST_CD"),
    F.rpad(F.col("CLM_Mbr_Sk.SUB_CNTY_FIPS_ID"),3," ").alias("SUB_CNTY_FIPS_ID"),
    F.rpad(F.col("CLM_Mbr_Sk.SUB_POSTAL_CD"),11," ").alias("SUB_POSTAL_CD"),
    F.col("CLM_Mbr_Sk.SUBMT_SUB_ID").alias("SUBMT_SUB_ID"),
    F.col("CLM_Mbr_Sk.ACTL_SUB_ID").alias("ACTL_SUB_ID"),
    F.col("CLM_Mbr_Sk.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

# Collector "lnkCollector" => union the two outputs
df_lnkCollector = df_reversal_logix_Reversals.unionByName(df_reversal_logix_DSLink113)

# Transformer "Snapshot"
# Just pass columns along to two outputs. We'll call the main output "df_Snapshot_Transform".
df_Snapshot_Transform = df_lnkCollector.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_EXTRNL_MBRSH_SK"),
    F.rpad(F.col("CLM_ID"),18," ").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_GNDR_CD_CD"),
    F.col("MBR_RELSHP_CD_CD"),
    F.col("MBR_BRTH_DT"),
    F.col("GRP_NM"),
    F.col("MBR_FIRST_NM"),
    F.rpad(F.col("MBR_MIDINIT"),1," ").alias("MBR_MIDINIT"),
    F.col("MBR_LAST_NM"),
    F.col("PCKG_CD_ID"),
    F.rpad(F.col("PATN_NTWK_SH_NM"),6," ").alias("PATN_NTWK_SH_NM"),
    F.rpad(F.col("SUB_GRP_BASE_NO"),9," ").alias("SUB_GRP_BASE_NO"),
    F.rpad(F.col("SUB_GRP_SECT_NO"),4," ").alias("SUB_GRP_SECT_NO"),
    F.rpad(F.col("SUB_ID"),14," ").alias("SUB_ID"),
    F.col("SUB_FIRST_NM"),
    F.rpad(F.col("SUB_MIDINIT"),1," ").alias("SUB_MIDINIT"),
    F.col("SUB_LAST_NM"),
    F.col("SUB_ADDR_LN_1"),
    F.col("SUB_ADDR_LN_2"),
    F.col("SUB_CITY_NM"),
    F.col("CLM_EXTRNL_MBRSH_SUB_ST_CD"),
    F.rpad(F.col("SUB_CNTY_FIPS_ID"),3," ").alias("SUB_CNTY_FIPS_ID"),
    F.rpad(F.col("SUB_POSTAL_CD"),11," ").alias("SUB_POSTAL_CD"),
    F.col("SUBMT_SUB_ID"),
    F.col("ACTL_SUB_ID"),
    F.col("MBR_UNIQ_KEY")
)

# Output Pin "Pkey" from "Snapshot"
df_Snapshot_Pkey = df_Snapshot_Transform.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_EXTRNL_MBRSH_SK"),
    F.col("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_GNDR_CD_CD"),
    F.col("MBR_RELSHP_CD_CD"),
    F.col("MBR_BRTH_DT"),
    F.col("GRP_NM"),
    F.col("MBR_FIRST_NM"),
    F.col("MBR_MIDINIT"),
    F.col("MBR_LAST_NM"),
    F.col("PCKG_CD_ID"),
    F.col("PATN_NTWK_SH_NM"),
    F.col("SUB_GRP_BASE_NO"),
    F.col("SUB_GRP_SECT_NO"),
    F.col("SUB_ID"),
    F.col("SUB_FIRST_NM"),
    F.col("SUB_MIDINIT"),
    F.col("SUB_LAST_NM"),
    F.col("SUB_ADDR_LN_1"),
    F.col("SUB_ADDR_LN_2"),
    F.col("SUB_CITY_NM"),
    F.col("CLM_EXTRNL_MBRSH_SUB_ST_CD"),
    F.col("SUB_CNTY_FIPS_ID"),
    F.col("SUB_POSTAL_CD"),
    F.col("SUBMT_SUB_ID"),
    F.col("ACTL_SUB_ID"),
    F.col("MBR_UNIQ_KEY")
)

# Output Pin "SnapShot" from "Snapshot"
df_Snapshot_SnapShot = df_Snapshot_Transform.select( F.rpad(F.col("CLM_ID"),18," ").alias("CLM_ID") )

# Transformer => "Transformer" => input is df_Snapshot_SnapShot, output "RowCount" => 
# The stage shows columns: SRC_SYS_CD_SK, CLM_ID => with "SrcSysCdSk" for SRC_SYS_CD_SK and input CLM_ID from SnapShot
df_Transformer = df_Snapshot_SnapShot
df_Transformer_out = df_Transformer.select(
    F.col("CLM_ID").alias("CLM_ID"),
    # The stage expression says "SRC_SYS_CD_SK" => "SrcSysCdSk", we must use the param
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

# "RowCount" => goes to "B_CLM_EXTRNL_MBRSH"
# We match the stage's column order: SRC_SYS_CD_SK, CLM_ID
df_RowCount = df_Transformer_out.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID")
)

# Write the sequential file "B_CLM_EXTRNL_MBRSH" => load/B_CLM_EXTRNL_MBRSH.FACETS.dat.#RunID#
write_files(
    df_RowCount,
    f"{adls_path}/load/B_CLM_EXTRNL_MBRSH.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Container "ClmExtrnlMbrshPK" => input "df_Snapshot_Pkey", output "Key"
params_ClmExtrnlMbrshPK = {
    "CurrRunCycle": CurrRunCycle
}
df_ClmExtrnlMbrshPK = ClmExtrnlMbrshPK(df_Snapshot_Pkey, params_ClmExtrnlMbrshPK)

# This output -> "FctsClmExtrnMbrTrns" => sequential file
# The stage "FctsClmExtrnMbrTrns" columns => same as container output
df_FctsClmExtrnMbrTrns = df_ClmExtrnlMbrshPK.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_EXTRNL_MBRSH_SK"),
    F.rpad(F.col("CLM_ID"),18," ").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_GNDR_CD_CD"),
    F.col("MBR_RELSHP_CD_CD"),
    F.col("MBR_BRTH_DT"),
    F.col("GRP_NM"),
    F.col("MBR_FIRST_NM"),
    F.rpad(F.col("MBR_MIDINIT"),1," ").alias("MBR_MIDINIT"),
    F.col("MBR_LAST_NM"),
    F.col("PCKG_CD_ID"),
    F.rpad(F.col("PATN_NTWK_SH_NM"),6," ").alias("PATN_NTWK_SH_NM"),
    F.rpad(F.col("SUB_GRP_BASE_NO"),9," ").alias("SUB_GRP_BASE_NO"),
    F.rpad(F.col("SUB_GRP_SECT_NO"),4," ").alias("SUB_GRP_SECT_NO"),
    F.rpad(F.col("SUB_ID"),14," ").alias("SUB_ID"),
    F.col("SUB_FIRST_NM"),
    F.rpad(F.col("SUB_MIDINIT"),1," ").alias("SUB_MIDINIT"),
    F.col("SUB_LAST_NM"),
    F.col("SUB_ADDR_LN_1"),
    F.col("SUB_ADDR_LN_2"),
    F.col("SUB_CITY_NM"),
    F.col("CLM_EXTRNL_MBRSH_SUB_ST_CD"),
    F.rpad(F.col("SUB_CNTY_FIPS_ID"),3," ").alias("SUB_CNTY_FIPS_ID"),
    F.rpad(F.col("SUB_POSTAL_CD"),11," ").alias("SUB_POSTAL_CD"),
    F.col("SUBMT_SUB_ID"),
    F.col("ACTL_SUB_ID"),
    F.col("MBR_UNIQ_KEY")
)

# Write the sequential file "FctsClmExtrnlMbrshExtr.FctsClmExtrnlMbrsh.dat.#RunID#"
write_files(
    df_FctsClmExtrnMbrTrns,
    f"{adls_path}/key/FctsClmExtrnlMbrshExtr.FctsClmExtrnlMbrsh.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)