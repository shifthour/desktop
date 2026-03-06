# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsPrvcyAuthExtr
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC                    There is a Lookup done for checking a match on PMAX_ENENE_CKE.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                                                                       Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------                                               ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               2/25/2007                                              Originally Programmed                                                                      devlIDS30                        Steph Goddard            
# MAGIC Bhoomi Dasari                   2/17/2009      Prod Supp/15                 Made logic changes to                                                                      devlIDS
# MAGIC                                                                                                         'PRVCY_MBR_SRC_CD_SK'
# MAGIC Bhoomi Dasari                   2/25/2009      Prod Supp/15                 Added 'RunDate' paramter instead                                                   devlIDS                        Steph Goddard              02/28/2009
# MAGIC                                                                                                         of Format.Date routine
# MAGIC Bhoomi Dasari                  3/16/2009       ProdSupp/15                  Updated Mbr_sk &                                                                          devlIDS                         Steph Goddard             04/01/2009
# MAGIC                                                                                                         Prvcy_Mbr_Src_Cd_Sk 
# MAGIC Steph Goddard                 7/14/2010      TTR-689                         changed primary key counter                                                          RebuildIntNewDevl        SAndrew                       2010-09-30
# MAGIC                                                                                                         from IDS_SK; standards updated
# MAGIC Manasa Andru                  7/2/2013       TTR - 901                   Changed the field name PRVCY_MBR_SRC_CD_SK to                     IntegrateCurDevl                Kalyan Neelam             2013-07-05
# MAGIC                                                                                                     PRVCY_MBR_SRC_CD and datatype from Integer to Char
# MAGIC                                                                                                  in Primary Key transformer, hash files and key file.                                                                                                                                                                                          
# MAGIC Manasa Andru                  8/5/2013       TTR - 901                  Mapped the PRVCY_MBR_SRC_CD from Transform link                    IntegrateCurDevl               Bhoomi Dasari           8/5/2013 
# MAGIC                                                                                                        rather than lkup link in the Updt link in the  
# MAGIC                                                                                                                  Primary Key transformer.
# MAGIC Anoop Nair                       2022-03-07         S2S Remediation      Added FACETS DSN Connection parameters                                     IntegrateDev5	Ken Bradmon	2022-06-03
# MAGIC 
# MAGIC Arpitha V                          2023-02-27         US 577999                Added logic to check null values for AUTH_DESC                             IntegrateDevB	 Goutham Kalidindi       2023-03-02

# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Extrnl Enty Data
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to /pkey
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
FacetsOwner = get_widget_value("FacetsOwner", "")
facets_secret_name = get_widget_value("facets_secret_name", "")
CurrRunCycle = get_widget_value("CurrRunCycle", "")
RunID = get_widget_value("RunID", "")
RunDate = get_widget_value("RunDate", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

# Database connections
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# --------------------------------------------------------------------------------
# Stage: hf_prvcy_auth (CHashedFileStage) -> Scenario B (read-modify-write)
# Replacement with a dummy table in IDS: "IDS.dummy_hf_prvcy_auth"
# Reading from dummy table
# --------------------------------------------------------------------------------
df_hf_prvcy_auth = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT SRC_SYS_CD, PRVCY_MBR_UNIQ_KEY, SEQ_NO, PRVCY_MBR_SRC_CD, CRT_RUN_CYC_EXCTN_SK, PRVCY_AUTH_SK "
        "FROM IDS.dummy_hf_prvcy_auth"
    )
    .load()
)

# --------------------------------------------------------------------------------
# Stage: FacetsPrvcyAuth (ODBCConnector)
# Multiple output pins with distinct queries
# --------------------------------------------------------------------------------
df_FacetsPrvcyAuth_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        "SELECT auth.PMED_CKE, auth.PMAX_SEQ_NO, auth.PMAX_PZCD_RQR_RTYP, auth.PMAX_EFF_DT, "
        "auth.PMAX_ORIG_END_DT, auth.PMAX_TERM_DTM, auth.PMAX_ENEN_CKE, auth.PMAX_ENEN_CTYP, auth.PMAX_REVOKE_INDB, "
        "auth.PMAX_PZCD_TRSN, auth.PMAX_CREATE_DTM, auth.PMAX_LAST_USUS_ID, auth.PMAX_LAST_UPD_DTM, auth.PMAT_ID, "
        "auth.ATXR_SOURCE_ID, member.PMED_ID "
        f"FROM {FacetsOwner}.FHP_PMAX_AUTH_X auth, {FacetsOwner}.FHP_PMED_MEMBER_D member "
        "WHERE auth.PMED_CKE=member.PMED_CKE"
    )
    .load()
)

df_FacetsPrvcyAuth_PbedExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", f"SELECT PBED_CKE, PBED_ID FROM {FacetsOwner}.FHP_PBED_BUSINES_D")
    .load()
)

df_FacetsPrvcyAuth_PcedExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", f"SELECT PCED_CKE, PCED_ID FROM {FacetsOwner}.FHP_PCED_COV_ENT_D")
    .load()
)

df_FacetsPrvcyAuth_AttachExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        "SELECT attach.ATXR_SOURCE_ID, attach.ATXR_DESC "
        f"FROM {FacetsOwner}.CER_ATXR_ATTACH_U attach, {FacetsOwner}.FHP_PMAX_AUTH_X auth "
        "WHERE attach.ATXR_SOURCE_ID = auth.ATXR_SOURCE_ID"
    )
    .load()
)

df_FacetsPrvcyAuth_EntityExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"SELECT ENEN_CKE, EXEN_REC FROM {FacetsOwner}.FHD_ENEN_ENTITY_D WHERE EXEN_REC = 0"
    )
    .load()
)

df_FacetsPrvcyAuth_MbrSk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        "SELECT AUTH.PMED_CKE, MEMB.MEME_CK "
        f"FROM {FacetsOwner}.FHP_PMAX_AUTH_X AUTH, {FacetsOwner}.FHD_EXEN_BASE_D BASE, {FacetsOwner}.FHD_EXFM_FA_MEMB_D MEMB "
        "WHERE AUTH.PMED_CKE = BASE.ENEN_CKE AND BASE.EXEN_REC = MEMB.EXEN_REC"
    )
    .load()
)

# --------------------------------------------------------------------------------
# Stage: StripFields (CTransformerStage)
# --------------------------------------------------------------------------------
df_Strip = df_FacetsPrvcyAuth_Extract.select(
    F.col("PMED_CKE").alias("PMED_CKE"),
    F.col("PMAX_SEQ_NO").alias("PMAX_SEQ_NO"),
    trim(strip_field(F.col("PMAX_PZCD_RQR_RTYP"))).alias("PMAX_PZCD_RQR_RTYP"),
    F.date_format(F.col("PMAX_EFF_DT"), "yyyy-MM-dd").alias("PMAX_EFF_DT"),
    F.date_format(F.col("PMAX_ORIG_END_DT"), "yyyy-MM-dd").alias("PMAX_ORIG_END_DT"),
    F.date_format(F.col("PMAX_TERM_DTM"), "yyyy-MM-dd").alias("PMAX_TERM_DTM"),
    trim(strip_field(F.col("PMAX_ENEN_CKE"))).alias("PMAX_ENEN_CKE"),
    trim(strip_field(F.col("PMAX_ENEN_CTYP"))).alias("PMAX_ENEN_CTYP"),
    trim(strip_field(F.col("PMAX_REVOKE_INDB"))).alias("PMAX_REVOKE_INDB"),
    trim(strip_field(F.col("PMAX_PZCD_TRSN"))).alias("PMAX_PZCD_TRSN"),
    F.date_format(F.col("PMAX_CREATE_DTM"), "yyyy-MM-dd").alias("PMAX_CREATE_DTM"),
    trim(strip_field(F.col("PMAX_LAST_USUS_ID"))).alias("PMAX_LAST_USUS_ID"),
    F.date_format(F.col("PMAX_LAST_UPD_DTM"), "yyyy-MM-dd").alias("PMAX_LAST_UPD_DTM"),
    trim(strip_field(F.col("PMAT_ID"))).alias("PMAT_ID"),
    trim(strip_field(F.col("ATXR_SOURCE_ID"))).alias("ATXR_SOURCE_ID"),
    trim(strip_field(F.col("PMED_ID"))).alias("PMED_ID"),
)

# --------------------------------------------------------------------------------
# Stage: hf_prvcy_auth_enen_lkup (CHashedFileStage) -> Scenario A for all pinned inputs
# Deduplicate each input on its key columns
# --------------------------------------------------------------------------------
df_PbedExtr_dedup = dedup_sort(df_FacetsPrvcyAuth_PbedExtr, ["PBED_CKE"], [])
df_PcedExtr_dedup = dedup_sort(df_FacetsPrvcyAuth_PcedExtr, ["PCED_CKE"], [])
df_AttachExtr_dedup = dedup_sort(df_FacetsPrvcyAuth_AttachExtr, ["ATXR_SOURCE_ID"], [])
df_EntityExtr_dedup = dedup_sort(df_FacetsPrvcyAuth_EntityExtr, ["ENEN_CKE"], [])
df_MbrSk_dedup = dedup_sort(df_FacetsPrvcyAuth_MbrSk, ["PMED_CKE"], [])

# --------------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage)
# Perform 5 left lookups
# --------------------------------------------------------------------------------
df_BusinessRules = (
    df_Strip.alias("Strip")
    .join(df_PbedExtr_dedup.alias("PbedLkup"), F.col("Strip.PMAX_ENEN_CKE") == F.col("PbedLkup.PBED_CKE"), "left")
    .join(df_PcedExtr_dedup.alias("PcedLkup"), F.col("Strip.PMAX_ENEN_CKE") == F.col("PcedLkup.PCED_CKE"), "left")
    .join(df_AttachExtr_dedup.alias("AttachLkup"), F.col("Strip.ATXR_SOURCE_ID") == F.col("AttachLkup.ATXR_SOURCE_ID"), "left")
    .join(df_EntityExtr_dedup.alias("EntityLkup"), F.col("Strip.PMED_CKE") == F.col("EntityLkup.ENEN_CKE"), "left")
    .join(df_MbrSk_dedup.alias("MbrSk_lkup"), F.col("Strip.PMED_CKE") == F.col("MbrSk_lkup.PMED_CKE"), "left")
)

df_Transform = df_BusinessRules.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    # "RowPassThru" is always 'Y'
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(RunDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS"), F.lit(";"), F.col("Strip.PMED_CKE"), F.lit(";"), F.col("Strip.PMAX_SEQ_NO"), F.lit(";"), F.col("Strip.PMED_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("PRVCY_AUTH_SK"),
    F.col("Strip.PMED_CKE").alias("PRVCY_MBR_UNIQ_KEY"),
    F.col("Strip.PMAX_SEQ_NO").alias("SEQ_NO"),
    F.when(F.col("EntityLkup.ENEN_CKE").isNotNull(), F.lit("N")).otherwise(F.lit("F")).alias("PRVCY_MBR_SRC_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    trim(F.col("Strip.PMAT_ID")).alias("PMAT_ID"),
    F.col("Strip.PMED_CKE").alias("PRVCY_EXTRNL_MBR_SK"),
    F.col("Strip.PMAX_ENEN_CKE").alias("RECPNT_EXTRNL_ENTY_SK"),
    F.when(
        (F.length(F.trim(F.col("Strip.PMAX_PZCD_RQR_RTYP"))) == 0) | F.col("Strip.PMAX_PZCD_RQR_RTYP").isNull(),
        F.lit("NA"),
    ).otherwise(F.trim(F.col("Strip.PMAX_PZCD_RQR_RTYP"))).alias("PMAX_PZCD_RQR_RTYP"),
    F.when(
        (F.length(F.trim(F.col("Strip.PMAX_ENEN_CTYP"))) == 0) | F.col("Strip.PMAX_ENEN_CTYP").isNull(),
        F.lit("NA"),
    ).otherwise(F.trim(F.col("Strip.PMAX_ENEN_CTYP"))).alias("PMAX_ENEN_CTYP"),
    F.when(
        (F.length(F.trim(F.col("Strip.PMAX_PZCD_TRSN"))) == 0) | F.col("Strip.PMAX_PZCD_TRSN").isNull(),
        F.lit("NA"),
    ).otherwise(F.trim(F.col("Strip.PMAX_PZCD_TRSN"))).alias("PMAX_PZCD_TRSN"),
    F.col("Strip.PMAX_REVOKE_INDB").alias("AUTH_RVKD_IN"),
    F.col("Strip.PMAX_CREATE_DTM").alias("PMAX_CREATE_DTM"),
    F.col("Strip.PMAX_EFF_DT").alias("PMAX_EFF_DT"),
    F.col("Strip.PMAX_ORIG_END_DT").alias("PMAX_ORIG_END_DT"),
    F.col("Strip.PMAX_TERM_DTM").alias("PMAX_TERM_DTM"),
    F.when(
        F.col("AttachLkup.ATXR_SOURCE_ID").isNull() | (F.length(F.trim(F.col("AttachLkup.ATXR_SOURCE_ID"))) == 0),
        F.lit("NA"),
    )
    .otherwise(
        F.when(
            (F.col("AttachLkup.ATXR_DESC").isNotNull())
            & (F.length(F.trim(F.col("AttachLkup.ATXR_DESC"))) != 0),
            F.upper(F.trim(F.col("AttachLkup.ATXR_DESC"))),
        ).otherwise(F.lit(" "))
    )
    .alias("AUTH_DESC"),
    F.when(
        (F.col("PbedLkup.PBED_CKE").isNotNull()) & (F.length(F.trim(F.col("PbedLkup.PBED_CKE"))) != 0),
        F.trim(F.col("PbedLkup.PBED_ID")),
    ).otherwise(
        F.when(
            (F.col("PcedLkup.PCED_CKE").isNotNull()) & (F.length(F.trim(F.col("PcedLkup.PCED_CKE"))) != 0),
            F.trim(F.col("PcedLkup.PCED_ID")),
        ).otherwise(F.lit("UNK"))
    ).alias("RECPNT_ID"),
    F.col("Strip.PMAX_LAST_UPD_DTM").alias("PMAX_LAST_UPD_DTM"),
    F.col("Strip.PMAX_LAST_USUS_ID").alias("PMAX_LAST_USUS_ID"),
    F.when(
        (F.col("MbrSk_lkup.PMED_CKE").isNull()) | (F.length(F.trim(F.col("MbrSk_lkup.PMED_CKE"))) == 0),
        F.lit("NA"),
    ).otherwise(F.col("MbrSk_lkup.MEME_CK")).alias("MBR_SK"),
)

# --------------------------------------------------------------------------------
# Stage: PrimaryKey (CTransformerStage), scenario B lookup with df_hf_prvcy_auth
# --------------------------------------------------------------------------------
df_PrimaryKey_input = (
    df_Transform.alias("Transform")
    .join(
        df_hf_prvcy_auth.alias("lkup"),
        (
            (F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"))
            & (F.col("Transform.PRVCY_MBR_UNIQ_KEY") == F.col("lkup.PRVCY_MBR_UNIQ_KEY"))
            & (F.col("Transform.SEQ_NO") == F.col("lkup.SEQ_NO"))
            & (F.col("Transform.PRVCY_MBR_SRC_CD") == F.col("lkup.PRVCY_MBR_SRC_CD"))
        ),
        "left",
    )
)

df_PrimaryKey_vars = df_PrimaryKey_input.withColumn(
    "NewCurrRunCycExtcnSk",
    F.when(F.col("lkup.PRVCY_AUTH_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK")),
).withColumn(
    "Sk",
    F.when(F.col("lkup.PRVCY_AUTH_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.PRVCY_AUTH_SK")),
)

df_enriched = df_PrimaryKey_vars
# SurrogateKeyGen for 'Sk' because the original transform uses KeyMgtGetNextValueConcurrent
df_enriched = SurrogateKeyGen(df_enriched, <DB sequence name>, 'Sk', <schema>, <secret_name>)

df_Key = df_enriched.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Sk").alias("PRVCY_AUTH_SK"),
    F.col("Transform.PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
    F.col("Transform.SEQ_NO").alias("SEQ_NO"),
    F.col("Transform.PRVCY_MBR_SRC_CD").alias("PRVCY_MBR_SRC_CD"),
    F.col("NewCurrRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.MBR_SK").alias("MBR_SK"),
    F.col("Transform.PMAT_ID").alias("PMAT_ID"),
    F.col("Transform.PMAX_PZCD_RQR_RTYP").alias("PMAX_PZCD_RQR_RTYP"),
    F.col("Transform.PMAX_ENEN_CTYP").alias("PMAX_ENEN_CTYP"),
    F.col("Transform.PMAX_PZCD_TRSN").alias("PMAX_PZCD_TRSN"),
    F.col("Transform.PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
    F.col("Transform.RECPNT_EXTRNL_ENTY_SK").alias("RECPNT_EXTRNL_ENTY_SK"),
    F.col("Transform.AUTH_RVKD_IN").alias("AUTH_RVKD_IN"),
    F.col("Transform.PMAX_CREATE_DTM").alias("PMAX_CREATE_DTM"),
    F.col("Transform.PMAX_EFF_DT").alias("PMAX_EFF_DT"),
    F.col("Transform.PMAX_ORIG_END_DT").alias("PMAX_ORIG_END_DT"),
    F.col("Transform.PMAX_TERM_DTM").alias("PMAX_TERM_DTM"),
    F.col("Transform.AUTH_DESC").alias("AUTH_DESC"),
    F.col("Transform.RECPNT_ID").alias("RECPNT_ID"),
    F.col("Transform.PMAX_LAST_UPD_DTM").alias("PMAX_LAST_UPD_DTM"),
    F.col("Transform.PMAX_LAST_USUS_ID").alias("PMAX_LAST_USUS_ID"),
)

df_updt = df_enriched.filter(F.col("lkup.PRVCY_AUTH_SK").isNull()).select(
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
    F.col("Transform.SEQ_NO").alias("SEQ_NO"),
    F.col("Transform.PRVCY_MBR_SRC_CD").alias("PRVCY_MBR_SRC_CD"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Sk").alias("PRVCY_AUTH_SK"),
)

# --------------------------------------------------------------------------------
# Stage: IdsPrvcyAuthExtr (CSeqFileStage)
# Write the df_Key data to a .dat file
# Apply rpad for columns declared as char or varchar in final link
# --------------------------------------------------------------------------------
df_Key_final = df_Key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("PRVCY_AUTH_SK"),
    F.col("PRVCY_MBR_UNIQ_KEY"),
    F.col("SEQ_NO"),
    F.rpad(F.col("PRVCY_MBR_SRC_CD"), 10, " ").alias("PRVCY_MBR_SRC_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SK"),
    F.rpad(F.col("PMAT_ID"), 8, " ").alias("PMAT_ID"),
    F.rpad(F.col("PMAX_PZCD_RQR_RTYP"), 4, " ").alias("PMAX_PZCD_RQR_RTYP"),
    F.rpad(F.col("PMAX_ENEN_CTYP"), 4, " ").alias("PMAX_ENEN_CTYP"),
    F.rpad(F.col("PMAX_PZCD_TRSN"), 4, " ").alias("PMAX_PZCD_TRSN"),
    F.col("PRVCY_EXTRNL_MBR_SK"),
    F.col("RECPNT_EXTRNL_ENTY_SK"),
    F.rpad(F.col("AUTH_RVKD_IN"), 1, " ").alias("AUTH_RVKD_IN"),
    F.rpad(F.col("PMAX_CREATE_DTM"), 10, " ").alias("PMAX_CREATE_DTM"),
    F.rpad(F.col("PMAX_EFF_DT"), 10, " ").alias("PMAX_EFF_DT"),
    F.rpad(F.col("PMAX_ORIG_END_DT"), 10, " ").alias("PMAX_ORIG_END_DT"),
    F.rpad(F.col("PMAX_TERM_DTM"), 10, " ").alias("PMAX_TERM_DTM"),
    F.col("AUTH_DESC"),
    F.col("RECPNT_ID"),
    F.rpad(F.col("PMAX_LAST_UPD_DTM"), 10, " ").alias("PMAX_LAST_UPD_DTM"),
    F.rpad(F.col("PMAX_LAST_USUS_ID"), 10, " ").alias("PMAX_LAST_USUS_ID"),
)

write_files(
    df_Key_final,
    f"{adls_path}/key/FctsPrvcyAuthExtr.PrvcyAuth.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: hf_prvcy_auth_updt (CHashedFileStage)
# Still scenario B same file: "hf_prvcy_auth" => merges into "IDS.dummy_hf_prvcy_auth"
# Insert if not matched, do nothing if matched
# --------------------------------------------------------------------------------
if df_updt.head(1):
    temp_table = "STAGING.FctsPrvcyAuthExtr_hf_prvcy_auth_updt_temp"
    execute_dml(f"DROP TABLE IF EXISTS {temp_table}", jdbc_url_ids, jdbc_props_ids)
    (
        df_updt.write.format("jdbc")
        .option("url", jdbc_url_ids)
        .options(**jdbc_props_ids)
        .option("dbtable", temp_table)
        .mode("overwrite")
        .save()
    )
    merge_sql = (
        f"MERGE INTO IDS.dummy_hf_prvcy_auth AS T "
        f"USING {temp_table} AS S "
        f"ON "
        f"(T.SRC_SYS_CD = S.SRC_SYS_CD) AND "
        f"(T.PRVCY_MBR_UNIQ_KEY = S.PRVCY_MBR_UNIQ_KEY) AND "
        f"(T.SEQ_NO = S.SEQ_NO) AND "
        f"(T.PRVCY_MBR_SRC_CD = S.PRVCY_MBR_SRC_CD) "
        f"WHEN MATCHED THEN "
        f"  UPDATE SET T.SRC_SYS_CD = T.SRC_SYS_CD "
        f"WHEN NOT MATCHED THEN "
        f"  INSERT (SRC_SYS_CD, PRVCY_MBR_UNIQ_KEY, SEQ_NO, PRVCY_MBR_SRC_CD, CRT_RUN_CYC_EXCTN_SK, PRVCY_AUTH_SK) "
        f"  VALUES (S.SRC_SYS_CD, S.PRVCY_MBR_UNIQ_KEY, S.SEQ_NO, S.PRVCY_MBR_SRC_CD, S.CRT_RUN_CYC_EXCTN_SK, S.PRVCY_AUTH_SK);"
    )
    execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)