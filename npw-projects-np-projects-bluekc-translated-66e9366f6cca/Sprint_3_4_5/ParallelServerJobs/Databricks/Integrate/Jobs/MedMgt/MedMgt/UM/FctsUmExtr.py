# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006, 2018 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsUmExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_UMUM_UTIL_MGT and CMC_GRGR_GROUP for loading into IDS.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:     Facets - CMC_UMUM_UTIL_MGT 
# MAGIC                    Facets - CMC_GRGR_GROUP
# MAGIC                    Facets - CMC_UMIT_STATUS
# MAGIC                    Facets - CMC_UMVT_STATUS
# MAGIC 
# MAGIC 
# MAGIC HASH FILES: none
# MAGIC 
# MAGIC TRANSFORMS:   STRIP.FIELD
# MAGIC                              FORMAT.DATE
# MAGIC                              Trim
# MAGIC                              Left
# MAGIC                              NullOptCode
# MAGIC                              IsNull
# MAGIC 
# MAGIC                           
# MAGIC PROCESSING:  Extract, transform, and primary keying for UM subject area.
# MAGIC 
# MAGIC OUTPUTS:   Sequential file
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               
# MAGIC 
# MAGIC 
# MAGIC Developer                Date               Project/Altiris #              Change Description                                                             Development Project     Code Reviewer           Date Reviewed
# MAGIC -----------------------------   -------------------    ----------------------------------    ----------------------------------------------------------------------------------------    ----------------------------------    -------------------------------    -------------------------
# MAGIC Hugh Sisson            02/2006         Originally Program                   
# MAGIC Parik                        04/10/2007   3264                               Added Balancing process to the overall job that takes        devlIDS30                     Steph Goddard           9/14/07          
# MAGIC                                                                                              a snapshot of the source data         
# MAGIC O. Nielsen                07/29/2008  Facets 4.5.1                    Chaned IDCD_ID to varchar(10) from  char(6)                     devlIDSnew                  Steph Goddard           08/20/2008
# MAGIC Bhoomi Dasari          04/09/2009  3808                               Added SrcSysCdSk to balancing snapshot                          devlIDS                         Steph Goddard           04/10/2009
# MAGIC Rick Henry               2012-05-10   4896                               Get Diag_Cd_Typ_Cd from Cd_Mppg                                  IntegrateNewDevl         Sandrew                     2012-05-01
# MAGIC Raja Gummadi          2013-03-28   TTR-1481                      Changed Run Cycle for Hash File Update stage to              IntegrateNewDevl          Bhoomi Dasari            4/1/2013
# MAGIC                                                                                             NewCrtRunCycExtcnSk from '0'           
# MAGIC  Akhila M                  10/21/2016  5628-WorkersComp       Exclusion Criteria- P_SEL_PRCS_CRITR                             IntegrateDev2                Kalyan Neelam          2016-11-08   
# MAGIC                                                                                             to remove wokers comp GRGR_ID's    
# MAGIC Sethuraman R         10/03/2018  5569-Oncology UM       To add "UM Alternate Ref ID" as part of Oncology UM        IntegrateDev1                Hugh Sisson              2018-10-03
# MAGIC Prabhu ES               2022-03-07   S2S Remediation           MSSQL ODBC conn params added                                      IntegrateDev5		Harsha Ravuri	06-14-2022

# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Trim all string variables
# MAGIC Extract Facets UM Data
# MAGIC Assign primary surrogate key
# MAGIC Apply business logic
# MAGIC Balancing snapshot of source table
# MAGIC Writing Sequential File to /pkey
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
BCBSOwner = get_widget_value('BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
DriverTable = get_widget_value('DriverTable','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# --------------------------------------------------------------------------------
# Read from dummy_hf_um, replacing the hashed file "hf_um" used by "hf_um_lkup".
# --------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_um_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT SRC_SYS_CD, UM_REF_ID, CRT_RUN_CYC_EXCTN_SK, UM_SK FROM dummy_hf_um")
    .load()
)

# --------------------------------------------------------------------------------
# Read from table CD_MPPNG (Stage: CD_MPPG).
# --------------------------------------------------------------------------------
df_CD_MPPG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT SRC_CD, TRGT_CD "
        f"FROM {IDSOwner}.CD_MPPNG "
        f"WHERE SRC_DOMAIN_NM = 'DIAGNOSIS CODE TYPE' "
        f"AND TRGT_DOMAIN_NM = 'DIAGNOSIS CODE TYPE' "
        f"AND SRC_CLCTN_CD = 'FACETS DBO'"
    )
    .load()
)

# --------------------------------------------------------------------------------
# Scenario A intermediate hashed file replacement for "hf_um_cd_mppng_trgt_cd".
# Deduplicate on key columns ("SRC_CD").
# --------------------------------------------------------------------------------
df_hf_um_cd_mppng_trgt_cd = dedup_sort(df_CD_MPPG, ["SRC_CD"], [])

# --------------------------------------------------------------------------------
# Read from FACETS ODBC (Stage: FACETS).
# --------------------------------------------------------------------------------
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
df_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"SELECT "
        f"    U.UMUM_REF_ID, "
        f"    U.MEME_CK, "
        f"    G.GRGR_ID, "
        f"    U.SBSB_CK, "
        f"    U.SGSG_CK, "
        f"    U.CMCM_ID, "
        f"    U.UMUM_CREATE_USID, "
        f"    U.UMUM_CREATE_DT, "
        f"    U.IDCD_ID, "
        f"    U.UMUM_USID_PRI, "
        f"    U.UMUM_MCTR_RISK, "
        f"    U.UMUM_MCTR_QUAL, "
        f"    U.UMUM_FINAL_CARE_DT, "
        f"    U.UMAC_SEQ_NO, "
        f"    U.CSPD_CAT, "
        f"    U.PDPD_ID, "
        f"    U.NTNB_ID, "
        f"    U.UMUM_CONF_IND, "
        f"    U.ATXR_SOURCE_ID, "
        f"    U.UMUM_ICD_IND_PROC, "
        f"    U.UMUM_ALT_REF_ID "
        f"FROM "
        f"    {FacetsOwner}.CMC_UMUM_UTIL_MGT AS U, "
        f"    {FacetsOwner}.CMC_GRGR_GROUP AS G, "
        f"    tempdb..{DriverTable} AS T "
        f"WHERE "
        f"    U.GRGR_CK = G.GRGR_CK AND "
        f"    U.UMUM_REF_ID = T.UM_REF_ID "
        f"    AND NOT EXISTS (SELECT DISTINCT "
        f"        CMC_GRGR.GRGR_CK "
        f"     FROM "
        f"        {BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR, "
        f"        {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR "
        f"     WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL' "
        f"       AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP' "
        f"       AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC' "
        f"       AND CMC_GRGR.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX "
        f"       AND CMC_GRGR.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX "
        f"       AND CMC_GRGR.GRGR_CK = G.GRGR_CK)"
    )
    .load()
)

# --------------------------------------------------------------------------------
# StripFields Transformer (Stage: StripFields).
# --------------------------------------------------------------------------------
df_Strip = (
    df_FACETS
    .withColumn("UMUM_REF_ID", trim(strip_field("UMUM_REF_ID")))
    .withColumn("MEME_CK", F.col("MEME_CK"))
    .withColumn("GRGR_ID", trim(strip_field("GRGR_ID")))
    .withColumn("SBSB_CK", F.col("SBSB_CK"))
    .withColumn("SGSG_CK", F.col("SGSG_CK"))
    .withColumn("CMCM_ID", trim(strip_field("CMCM_ID")))
    .withColumn("UMUM_CREATE_USID", trim(strip_field("UMUM_CREATE_USID")))
    .withColumn("UMUM_CREATE_DT", F.substring(F.col("UMUM_CREATE_DT"), 1, 10))
    .withColumn("IDCD_ID", trim(strip_field("IDCD_ID")))
    .withColumn("UMUM_USID_PRI", trim(strip_field("UMUM_USID_PRI")))
    .withColumn("UMUM_MCTR_RISK", trim(strip_field("UMUM_MCTR_RISK")))
    .withColumn("UMUM_MCTR_QUAL", trim(strip_field("UMUM_MCTR_QUAL")))
    .withColumn("UMUM_FINAL_CARE_DT", F.substring(F.col("UMUM_FINAL_CARE_DT"), 1, 10))
    .withColumn("UMAC_SEQ_NO", F.col("UMAC_SEQ_NO"))
    .withColumn("CSPD_CAT", trim(strip_field("CSPD_CAT")))
    .withColumn("PDPD_ID", trim(strip_field("PDPD_ID")))
    .withColumn("NTNB_ID", F.col("NTNB_ID"))
    .withColumn("UMUM_CONF_IND", trim(strip_field("UMUM_CONF_IND")))
    .withColumn("ATXR_SOURCE_ID", F.col("ATXR_SOURCE_ID"))
    .withColumn(
        "UMUM_ICD_IND_PROC",
        F.when(
            F.length(trim(strip_field("UMUM_ICD_IND_PROC"))) == 0,
            F.lit("9")
        ).otherwise(trim(strip_field("UMUM_ICD_IND_PROC")))
    )
    .withColumn("UMUM_ALT_REF_ID", F.col("UMUM_ALT_REF_ID"))
)

# --------------------------------------------------------------------------------
# BusinessRules Transformer (Stage: BusinessRules).
# Left join with df_hf_um_cd_mppng_trgt_cd on UMUM_ICD_IND_PROC = SRC_CD.
# --------------------------------------------------------------------------------
df_BusinessRules_join = df_Strip.alias("Strip").join(
    df_hf_um_cd_mppng_trgt_cd.alias("diag_cd_typ_cd"),
    F.col("Strip.UMUM_ICD_IND_PROC") == F.col("diag_cd_typ_cd.SRC_CD"),
    "left"
)

df_BusinessRules = (
    df_BusinessRules_join
    .withColumn("RowPassThru", F.lit("Y"))
    .withColumn("svSrcSysCd", F.lit("FACETS"))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.col("RowPassThru"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.col("svSrcSysCd"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(F.col("svSrcSysCd"), F.lit(";"), F.col("Strip.UMUM_REF_ID"))
    )
    .withColumn("UM_SK", F.lit(0))
    .withColumn("UM_REF_ID", F.col("Strip.UMUM_REF_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn(
        "CASE_MGT",
        F.when(
            F.col("Strip.CMCM_ID").isNull() |
            (F.length(trim("Strip.CMCM_ID")) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim("Strip.CMCM_ID")))
    )
    .withColumn("CRT_USER", F.col("Strip.UMUM_CREATE_USID"))
    .withColumn("GRP", F.col("Strip.GRGR_ID"))
    .withColumn("MBR", F.col("Strip.MEME_CK"))
    .withColumn(
        "PRI_DIAG_CD",
        F.when(
            F.col("Strip.IDCD_ID").isNull() |
            (F.length(trim("Strip.IDCD_ID")) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim("Strip.IDCD_ID")))
    )
    .withColumn("PRI_RESP_USER", F.col("Strip.UMUM_USID_PRI"))
    .withColumn("PROD", F.col("Strip.PDPD_ID"))
    .withColumn("SUBGRP", F.col("Strip.SGSG_CK"))
    .withColumn("SUB", F.col("Strip.SBSB_CK"))
    .withColumn(
        "QLTY_OF_SVC_LVL_CD",
        F.when(
            F.col("Strip.UMUM_MCTR_QUAL").isNull() |
            (F.length(trim("Strip.UMUM_MCTR_QUAL")) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim("Strip.UMUM_MCTR_QUAL")))
    )
    .withColumn(
        "RISK_LVL_CD",
        F.when(
            F.col("Strip.UMUM_MCTR_RISK").isNull() |
            (F.length(trim("Strip.UMUM_MCTR_RISK")) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim("Strip.UMUM_MCTR_RISK")))
    )
    .withColumn(
        "UM_CLS_PLN_PROD_CAT",
        F.when(
            F.col("Strip.CSPD_CAT").isNull() |
            (F.length(trim("Strip.CSPD_CAT")) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim("Strip.CSPD_CAT")))
    )
    .withColumn(
        "IP_RCRD_IN",
        F.when(
            (F.length(F.col("Strip.UMUM_CONF_IND")) == 0) |
            (F.col("Strip.UMUM_CONF_IND") == F.lit("N")),
            F.lit("N")
        ).otherwise(F.lit("Y"))
    )
    .withColumn(
        "ATCHMT_SRC_DTM",
        FORMAT.DATE(
            F.col("Strip.ATXR_SOURCE_ID"), 
            "SYBASE",
            "TIMESTAMP",
            "DB2TIMESTAMP"
        )
    )
    .withColumn("CRT_DT", F.col("Strip.UMUM_CREATE_DT"))
    .withColumn(
        "MED_MGT_NOTE",
        FORMAT.DATE(
            F.col("Strip.NTNB_ID"),
            "SYBASE",
            "TIMESTAMP",
            "DB2TIMESTAMP"
        )
    )
    .withColumn("FINL_CARE_DT", F.col("Strip.UMUM_FINAL_CARE_DT"))
    .withColumn("ACTVTY_SEQ_NO", F.col("Strip.UMAC_SEQ_NO"))
    .withColumn(
        "DIAG_CD_TYP_CD",
        F.when(
            F.col("diag_cd_typ_cd.SRC_CD").isNotNull(),
            F.col("diag_cd_typ_cd.TRGT_CD")
        ).otherwise(F.lit("UNK"))
    )
    .withColumn(
        "ALT_REF_ID",
        F.when(
            F.trim(
                F.when(
                    F.col("Strip.UMUM_ALT_REF_ID").isNull(),
                    F.lit("")
                ).otherwise(F.col("Strip.UMUM_ALT_REF_ID"))
            ) == F.lit(""),
            F.lit("NA")
        ).otherwise(F.col("Strip.UMUM_ALT_REF_ID"))
    )
)

# --------------------------------------------------------------------------------
# PrimaryKey Transformer (Stage: PrimaryKey).
# Left join with df_hf_um_lkup on (SRC_SYS_CD, UM_REF_ID). Scenario B read-modify-write.
# --------------------------------------------------------------------------------
df_primarykey_join = (
    df_BusinessRules.alias("Transform")
    .join(
        df_hf_um_lkup.alias("lkup"),
        [
            F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
            F.col("Transform.UM_REF_ID") == F.col("lkup.UM_REF_ID")
        ],
        "left"
    )
    .withColumn(
        "NewCrtRunCycExtcnSk",
        F.when(F.col("lkup.UM_SK").isNull(), F.col("CurrRunCycle")).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn("lkup_UM_SK", F.col("lkup.UM_SK"))
    .withColumn("CurrRunCycle", F.lit(CurrRunCycle))
)

df_enriched = df_primarykey_join.withColumn("UM_SK", F.col("lkup_UM_SK"))
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"UM_SK",<schema>,<secret_name>)

# --------------------------------------------------------------------------------
# Create final output (Key) link to "IdsUmExtr".
# --------------------------------------------------------------------------------
df_key = df_enriched.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "UM_SK",
    "UM_REF_ID",
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CurrRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    "CASE_MGT",
    "CRT_USER",
    "GRP",
    "MBR",
    "PRI_DIAG_CD",
    "PRI_RESP_USER",
    "PROD",
    "SUBGRP",
    "SUB",
    "QLTY_OF_SVC_LVL_CD",
    "RISK_LVL_CD",
    "UM_CLS_PLN_PROD_CAT",
    "IP_RCRD_IN",
    "ATCHMT_SRC_DTM",
    "CRT_DT",
    F.col("MED_MGT_NOTE").alias("MED_MGT_NOTE_DTM"),
    "FINL_CARE_DT",
    "ACTVTY_SEQ_NO",
    "DIAG_CD_TYP_CD",
    "ALT_REF_ID"
)

# --------------------------------------------------------------------------------
# Create the "updt" link to insert into dummy_hf_um if lkup.UM_SK is null.
# --------------------------------------------------------------------------------
df_updt = df_enriched.filter(F.col("lkup_UM_SK").isNull()).select(
    F.col("SRC_SYS_CD"),
    F.col("UM_REF_ID"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("UM_SK")
)

# --------------------------------------------------------------------------------
# Merge df_updt into dummy_hf_um (Scenario B) as inserts only.
# --------------------------------------------------------------------------------
drop_sql = "DROP TABLE IF EXISTS STAGING.FctsUmExtr_hf_um_temp"
execute_dml(drop_sql, jdbc_url_ids, jdbc_props_ids)

df_updt.write.format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", "STAGING.FctsUmExtr_hf_um_temp") \
    .mode("overwrite") \
    .save()

merge_sql = """
MERGE dummy_hf_um AS T
USING STAGING.FctsUmExtr_hf_um_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.UM_REF_ID = S.UM_REF_ID
WHEN NOT MATCHED THEN
INSERT (SRC_SYS_CD, UM_REF_ID, CRT_RUN_CYC_EXCTN_SK, UM_SK)
VALUES (S.SRC_SYS_CD, S.UM_REF_ID, S.CRT_RUN_CYC_EXCTN_SK, S.UM_SK);
"""
execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)

# --------------------------------------------------------------------------------
# Write out IdsUmExtr file (Stage: IdsUmExtr, a CSeqFileStage).
# Some columns are char(...) => apply rpad(...).
# --------------------------------------------------------------------------------
df_key_padded = (
    df_key
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("UM_REF_ID", F.rpad(F.col("UM_REF_ID"), 9, " "))
    .withColumn("CASE_MGT", F.rpad(F.col("CASE_MGT"), 10, " "))
    .withColumn("CRT_USER", F.rpad(F.col("CRT_USER"), 10, " "))
    .withColumn("PRI_DIAG_CD", F.rpad(F.col("PRI_DIAG_CD"), 10, " "))
    .withColumn("PRI_RESP_USER", F.rpad(F.col("PRI_RESP_USER"), 10, " "))
    .withColumn("PROD", F.rpad(F.col("PROD"), 10, " "))
    .withColumn("QLTY_OF_SVC_LVL_CD", F.rpad(F.col("QLTY_OF_SVC_LVL_CD"), 4, " "))
    .withColumn("RISK_LVL_CD", F.rpad(F.col("RISK_LVL_CD"), 4, " "))
    .withColumn("UM_CLS_PLN_PROD_CAT", F.rpad(F.col("UM_CLS_PLN_PROD_CAT"), 10, " "))
    .withColumn("IP_RCRD_IN", F.rpad(F.col("IP_RCRD_IN"), 1, " "))
    .withColumn("CRT_DT", F.rpad(F.col("CRT_DT"), 10, " "))
    .withColumn("FINL_CARE_DT", F.rpad(F.col("FINL_CARE_DT"), 10, " "))
)

write_files(
    df_key_padded.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "UM_SK",
        "UM_REF_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CASE_MGT",
        "CRT_USER",
        "GRP",
        "MBR",
        "PRI_DIAG_CD",
        "PRI_RESP_USER",
        "PROD",
        "SUBGRP",
        "SUB",
        "QLTY_OF_SVC_LVL_CD",
        "RISK_LVL_CD",
        "UM_CLS_PLN_PROD_CAT",
        "IP_RCRD_IN",
        "ATCHMT_SRC_DTM",
        "CRT_DT",
        "MED_MGT_NOTE_DTM",
        "FINL_CARE_DT",
        "ACTVTY_SEQ_NO",
        "DIAG_CD_TYP_CD",
        "ALT_REF_ID"
    ),
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# --------------------------------------------------------------------------------
# Facets_Source (Stage: Facets_Source).
# --------------------------------------------------------------------------------
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"SELECT "
        f"    U.UMUM_REF_ID "
        f"FROM "
        f"    {FacetsOwner}.CMC_UMUM_UTIL_MGT AS U, "
        f"    tempdb..{DriverTable} AS T "
        f"WHERE "
        f"    U.UMUM_REF_ID = T.UM_REF_ID "
        f"    AND NOT EXISTS (SELECT DISTINCT "
        f"        CMC_GRGR.GRGR_CK "
        f"     FROM "
        f"        {BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR, "
        f"        {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR "
        f"     WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL' "
        f"       AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP' "
        f"       AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC' "
        f"       AND CMC_GRGR.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX "
        f"       AND CMC_GRGR.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX "
        f"       AND CMC_GRGR.GRGR_CK = U.GRGR_CK)"
    )
    .load()
)

# --------------------------------------------------------------------------------
# Transform (Stage: Transform) for Facets_Source => Snapshot_File
# --------------------------------------------------------------------------------
df_Transform = (
    df_Facets_Source
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("UM_REF_ID", trim(strip_field("UMUM_REF_ID")))
    .select("SRC_SYS_CD_SK", "UM_REF_ID")
)

# --------------------------------------------------------------------------------
# Write to Snapshot_File (Stage: Snapshot_File, a CSeqFileStage).
# --------------------------------------------------------------------------------
write_files(
    df_Transform,
    f"{adls_path}/load/B_UM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)