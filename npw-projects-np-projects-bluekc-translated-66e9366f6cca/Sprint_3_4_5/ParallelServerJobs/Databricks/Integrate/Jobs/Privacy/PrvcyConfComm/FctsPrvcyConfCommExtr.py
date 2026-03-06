# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME: FctsPrvcyConfCommExtr
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty               2/25/2007           FHP/3028                      Originally Programmed                              devlIDS30                  Steph Goddard            
# MAGIC              
# MAGIC Bhoomi Dasari                2/17/2009         Prod Supp/15                 Made logic changes to                               devlIDS
# MAGIC                                                                                                         'PRVCY_MBR_SRC_CD_SK'
# MAGIC 
# MAGIC 
# MAGIC Bhoomi Dasari                2/25/2009         Prod Supp/15                 Added 'RunDate' paramter instead             devlIDS                     Steph Goddard            02/28/2009
# MAGIC                                                                                                         of Format.Date routine
# MAGIC 
# MAGIC Bhoomi Dasari               3/17/2009         Prod Supp/15                  Added Mbr_sk new logic and                      devlIDS                     Steph Goddard           04/01/2009
# MAGIC                                                                                                         Prvcy_Mbr_Src_Cd_Sk
# MAGIC 
# MAGIC Steph Goddard              7/14/10             TTR-689                         Changed prmary key counter from                                                SAndrew                      2010-09-30
# MAGIC                                                                                                         IDS_SK to PRVCY_CONF_COMM_SK; 
# MAGIC                                                                                                         brought to current standards
# MAGIC 
# MAGIC Manasa Andru              7/8/2013           TTR - 778                 Added the business rule in PrimaryKey transformer     devlCurIDS           Kalyan Neelam            2013-07-10
# MAGIC                                                                                                      for MBR_SK and PRVCY_EXTRNL_MBR_SK
# MAGIC 
# MAGIC Anoop Nair                2022-03-07         S2S Remediation       Added  FACETS DSN Connection parameters      IntegrateDev5	Ken Bradmon	2022-06-03

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
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, length, upper, regexp_replace, date_format, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')

# --------------------------------------------------------------------------------
# Scenario B Hashed File: hf_prvcy_conf_comm as a dummy table
# Read dummy table
# --------------------------------------------------------------------------------
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
query_hf_prvcy_conf_comm = (
    "SELECT SRC_SYS_CD, PRVCY_MBR_UNIQ_KEY, SEQ_NO, PRVCY_MBR_SRC_CD_SK, "
    "PRVCY_CONF_COMM_TYP_CD_SK, CRT_RUN_CYC_EXCTN_SK, PRVCY_CONF_COMM_SK "
    "FROM dummy_hf_prvcy_conf_comm"
)
df_hf_prvcy_conf_comm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_hf_prvcy_conf_comm)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: FctsPrvcyConfCommExtr (ODBCConnector)
# OutputLink: Extract
# --------------------------------------------------------------------------------
query_Extract = (
    "SELECT \n"
    "     PMCC.PMED_CKE,\n"
    "     PMCC.PMCC_REQ_NO,\n"
    "     PMCC.PMCC_PZCD_ED_DTYP,\n"
    "     PMCC.PMCC_PZCD_STS,\n"
    "     PMCC.PMCC_RECEIVED_DT,\n"
    "     PMCC.PMCC_EFF_DT,\n"
    "     PMCC.PMCC_TERM_DTM,\n"
    "     PMCC.PMCC_DESC,\n"
    "     PMCC.PMCC_ADDRESS_LNAME,\n"
    "     PMCC.PMCC_ADDRESS_FNAME,\n"
    "     PMCC.PMCC_PZCD_RRSN,\n"
    "     PMED.PMED_ID,\n"
    "     PMCC.PMCC_ED_SEQ_NO\n"
    "FROM  {0}.FHP_PMCC_COMM_X PMCC ,\n"
    "      {0}.FHP_PMED_MEMBER_D PMED\n"
    "WHERE PMCC.PMED_CKE = PMED.PMED_CKE"
).format(FacetsOwner)

df_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_Extract)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: FctsPrvcyConfCommExtr (ODBCConnector)
# OutputLink: Extract1
# --------------------------------------------------------------------------------
query_Extract1 = (
    "SELECT\n"
    "{0}.FHD_ENEM_EMAIL_D.ENEN_CKE,\n"
    "{0}.FHD_ENEM_EMAIL_D.ENEM_PZCD_TYPE,\n"
    "{0}.FHD_ENEM_EMAIL_D.ENEM_SEQ_NO,\n"
    "{0}.FHD_ENEM_EMAIL_D.ENEM_EMAIL,\n"
    "{0}.FHP_PMCC_COMM_X.PMED_CKE,\n"
    "{0}.FHP_PMCC_COMM_X.PMCC_PZCD_ED_DTYP,\n"
    "{0}.FHP_PMCC_COMM_X.PMCC_ED_SEQ_NO\n"
    "FROM \n"
    "{0}.FHD_ENEM_EMAIL_D,\n"
    "{0}.FHP_PMCC_COMM_X\n"
    "WHERE\n"
    "{0}.FHD_ENEM_EMAIL_D.ENEN_CKE={0}.FHP_PMCC_COMM_X.PMED_CKE AND\n"
    "{0}.FHD_ENEM_EMAIL_D.ENEM_PZCD_TYPE={0}.FHP_PMCC_COMM_X.PMCC_PZCD_ED_DTYP AND\n"
    "{0}.FHD_ENEM_EMAIL_D.ENEM_SEQ_NO={0}.FHP_PMCC_COMM_X.PMCC_ED_SEQ_NO"
).format(FacetsOwner)

df_Extract1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_Extract1)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: FctsPrvcyConfCommExtr (ODBCConnector)
# OutputLink: Entity
# --------------------------------------------------------------------------------
query_Entity = (
    "SELECT ENEN_CKE,EXEN_REC FROM {0}.FHD_ENEN_ENTITY_D WHERE EXEN_REC = 0"
).format(FacetsOwner)

df_Entity = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_Entity)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: FctsPrvcyConfCommExtr (ODBCConnector)
# OutputLink: MbrSk
# --------------------------------------------------------------------------------
query_MbrSk = (
    "SELECT\n"
    "COMM.PMED_CKE,\n"
    "MEMB.MEME_CK\n"
    "FROM \n"
    "{0}.FHP_PMCC_COMM_X COMM,\n"
    "{0}.FHD_EXEN_BASE_D BASE,\n"
    "{0}.FHD_EXFM_FA_MEMB_D MEMB\n"
    "WHERE\n"
    "COMM.PMED_CKE = BASE.ENEN_CKE AND\n"
    "BASE.EXEN_REC = MEMB.EXEN_REC"
).format(FacetsOwner)

df_MbrSk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_MbrSk)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: StripFields (CTransformerStage)
# Input: df_Extract => Output: df_Strip
# --------------------------------------------------------------------------------
df_Strip = df_Extract
df_Strip = df_Strip.withColumn(
    "PMCC_PZCD_ED_DTYP",
    regexp_replace(upper(col("PMCC_PZCD_ED_DTYP")), "[\n\r\t]", "")
).withColumn(
    "PMCC_PZCD_STS",
    regexp_replace(upper(col("PMCC_PZCD_STS")), "[\n\r\t]", "")
).withColumn(
    "PMCC_RECEIVED_DT",
    date_format(col("PMCC_RECEIVED_DT"), "yyyy-MM-dd")
).withColumn(
    "PMCC_EFF_DT",
    date_format(col("PMCC_EFF_DT"), "yyyy-MM-dd")
).withColumn(
    "PMCC_TERM_DTM",
    date_format(col("PMCC_TERM_DTM"), "yyyy-MM-dd")
).withColumn(
    "PMCC_DESC",
    regexp_replace(upper(col("PMCC_DESC")), "[\n\r\t]", "")
).withColumn(
    "PMCC_ADDRESS_LNAME",
    regexp_replace(upper(col("PMCC_ADDRESS_LNAME")), "[\n\r\t]", "")
).withColumn(
    "PMCC_ADDRESS_FNAME",
    regexp_replace(upper(col("PMCC_ADDRESS_FNAME")), "[\n\r\t]", "")
).withColumn(
    "PMED_ID",
    regexp_replace(col("PMED_ID"), "[\n\r\t]", "")
).withColumn(
    "PMCC_PZCD_RRSN",
    regexp_replace(upper(col("PMCC_PZCD_RRSN")), "[\n\r\t]", "")
).withColumn(
    "PMCC_ED_SEQ_NO",
    col("PMCC_ED_SEQ_NO")
)

df_Strip = df_Strip.select(
    "PMED_CKE",
    "PMCC_REQ_NO",
    "PMCC_PZCD_ED_DTYP",
    "PMCC_PZCD_STS",
    "PMCC_RECEIVED_DT",
    "PMCC_EFF_DT",
    "PMCC_TERM_DTM",
    "PMCC_DESC",
    "PMCC_ADDRESS_LNAME",
    "PMCC_ADDRESS_FNAME",
    "PMED_ID",
    "PMCC_PZCD_RRSN",
    "PMCC_ED_SEQ_NO"
)

# --------------------------------------------------------------------------------
# Stage: StripEmail (CTransformerStage)
# Input: df_Extract1 => Output: df_StripEmail
# --------------------------------------------------------------------------------
df_StripEmail = df_Extract1.withColumn(
    "ENEM_EMAIL",
    regexp_replace(col("ENEM_EMAIL"), "[\n\r\t]", "")
).select(
    "PMED_CKE",
    "ENEM_EMAIL"
)

# --------------------------------------------------------------------------------
# Stage: hf_prvcy_conf_comm_email (CHashedFileStage) - Scenario C for 3 separate hashed files
# Reading them as Parquet
# OutputPins: EmailAdd, EntityLkup, MbrSk_lkup
# --------------------------------------------------------------------------------
df_hf_prvcy_conf_comm_email = spark.read.parquet("hf_prvcy_conf_comm_email.parquet")
df_hf_prvcy_conf_comm_entity = spark.read.parquet("hf_prvcy_conf_comm_entity.parquet")
df_hf_prvcyconfcomm_mbrsk_lkup = spark.read.parquet("hf_prvcyconfcomm_mbrsk_lkup.parquet")

df_EmailAdd = df_hf_prvcy_conf_comm_email.select("PMED_CKE", "ENEM_EMAIL")
df_EntityLkup = df_hf_prvcy_conf_comm_entity.select("ENEN_CKE", "EXEN_REC")
df_MbrSk_lkup = df_hf_prvcyconfcomm_mbrsk_lkup.select("PMED_CKE", "MEME_CK")

# --------------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage)
# PrimaryLink: df_Strip
# LookupLink: df_EmailAdd (left join), df_EntityLkup (left join), df_MbrSk_lkup (left join)
# Output: df_Transform
# --------------------------------------------------------------------------------
df_BusinessRules = (
    df_Strip.alias("Strip")
    .join(df_EmailAdd.alias("EmailAdd"),
          trim(col("Strip.PMED_CKE")) == col("EmailAdd.PMED_CKE"),
          how="left")
    .join(df_EntityLkup.alias("EntityLkup"),
          trim(col("Strip.PMED_CKE")) == col("EntityLkup.ENEN_CKE"),
          how="left")
    .join(df_MbrSk_lkup.alias("MbrSk_lkup"),
          trim(col("Strip.PMED_CKE")) == col("MbrSk_lkup.PMED_CKE"),
          how="left")
)

df_Transform = df_BusinessRules.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"),10," ").alias("INSRT_UPDT_CD"),
    rpad(lit("N"),1," ").alias("DISCARD_IN"),
    rpad(lit("Y"),1," ").alias("PASS_THRU_IN"),
    col("RunDate").alias("FIRST_RECYC_DT"),  # Will overwrite after we attach the column
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    rpad(lit("FACETS"),<...>," ").alias("SRC_SYS_CD"),  # length unknown
    F.concat_ws(";", 
        lit("FACETS"),
        col("Strip.PMED_CKE"),
        col("Strip.PMCC_REQ_NO"),
        col("Strip.PMED_ID"),
        col("Strip.PMCC_PZCD_ED_DTYP")
    ).alias("PRI_KEY_STRING"),
    lit(0).alias("PRVCY_CONF_COMM_SK"),
    col("Strip.PMED_CKE").alias("PRVCY_MBR_UNIQ_KEY"),
    col("Strip.PMCC_REQ_NO").alias("SEQ_NO"),
    when(col("EntityLkup.ENEN_CKE").isNull(), lit("F")).otherwise(lit("N")).alias("PRVCY_MBR_SRC_CD_SK"),
    col("Strip.PMCC_PZCD_ED_DTYP").alias("PRVCY_CONF_COMM_TYP_CD_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(
        (col("MbrSk_lkup.PMED_CKE").isNull()) | (length(trim(col("MbrSk_lkup.PMED_CKE"))) == 0),
        lit("NA")
    ).otherwise(trim(col("MbrSk_lkup.MEME_CK"))).alias("MBR_SK"),
    col("Strip.PMED_CKE").alias("PRVCY_EXTRNL_MBR_SK"),
    col("Strip.PMCC_PZCD_STS").alias("PRVCY_CONF_COMM_STTUS_CD_SK"),
    rpad(col("Strip.PMCC_EFF_DT"),10," ").alias("EFF_DT_SK"),
    rpad(col("Strip.PMCC_RECEIVED_DT"),10," ").alias("RCVD_DT_SK"),
    rpad(col("Strip.PMCC_TERM_DTM"),10," ").alias("TERM_DT_SK"),
    when(
        (col("Strip.PMCC_ADDRESS_FNAME").isNull()) | (length(trim(col("Strip.PMCC_ADDRESS_FNAME"))) == 0),
        lit("  ")
    ).otherwise(trim(col("Strip.PMCC_ADDRESS_FNAME"))).alias("ADDREE_FIRST_NM"),
    when(
        (col("Strip.PMCC_ADDRESS_LNAME").isNull()) | (length(trim(col("Strip.PMCC_ADDRESS_LNAME"))) == 0),
        lit("  ")
    ).otherwise(trim(col("Strip.PMCC_ADDRESS_LNAME"))).alias("ADDREE_LAST_NM"),
    trim(col("Strip.PMCC_DESC")).alias("CONF_COMM_DESC"),
    col("Strip.PMCC_PZCD_RRSN").alias("PRVCY_CONF_COMM_RQST_RSN_CD_SK"),
    when(
        (col("EmailAdd.PMED_CKE").isNull()) | (length(trim(col("EmailAdd.PMED_CKE"))) == 0),
        lit("NA")
    ).otherwise(col("EmailAdd.ENEM_EMAIL")).alias("EMAIL_ADDR_TX"),
    col("Strip.PMCC_ED_SEQ_NO").alias("ADDR_SEQ_NO"),
    lit(RunDate).alias("FIRST_RECYC_DT")  # Overwrite explicitly with the job parameter
)

# --------------------------------------------------------------------------------
# Stage: PrimaryKey (CTransformerStage)
# LookupLink: df_hf_prvcy_conf_comm => scenario B
# We do the left join
# --------------------------------------------------------------------------------
df_PrimaryKeyJoin = (
    df_Transform.alias("Transform")
    .join(
        df_hf_prvcy_conf_comm.alias("lkup"),
        on=[
            col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD"),
            col("Transform.PRVCY_MBR_UNIQ_KEY") == col("lkup.PRVCY_MBR_UNIQ_KEY"),
            col("Transform.SEQ_NO") == col("lkup.SEQ_NO"),
            col("Transform.PRVCY_MBR_SRC_CD_SK") == col("lkup.PRVCY_MBR_SRC_CD_SK"),
            col("Transform.PRVCY_CONF_COMM_TYP_CD_SK") == col("lkup.PRVCY_CONF_COMM_TYP_CD_SK")
        ],
        how="left"
    )
)

df_PrimaryKey = df_PrimaryKeyJoin.select(
    col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("Transform.ERR_CT").alias("ERR_CT"),
    col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    # Surrogate Key placeholder for new vs existing
    when(col("lkup.PRVCY_CONF_COMM_SK").isNull(), lit(None)).otherwise(col("lkup.PRVCY_CONF_COMM_SK")).alias("Sk_condition"),
    col("Transform.PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
    col("Transform.SEQ_NO").alias("SEQ_NO"),
    col("Transform.PRVCY_MBR_SRC_CD_SK").alias("PRVCY_MBR_SRC_CD_SK"),
    col("Transform.PRVCY_CONF_COMM_TYP_CD_SK").alias("PRVCY_CONF_COMM_TYP_CD_SK"),
    when(col("lkup.PRVCY_CONF_COMM_SK").isNull(), lit(CurrRunCycle)).otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK")).alias("NewCurrRunCycExtcnSk"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(col("Transform.PRVCY_MBR_SRC_CD_SK") == lit("F"), col("Transform.MBR_SK")).otherwise(lit("NA")).alias("MBR_SK"),
    when(col("Transform.PRVCY_MBR_SRC_CD_SK") == lit("N"), col("Transform.PRVCY_EXTRNL_MBR_SK")).otherwise(lit("NA")).alias("PRVCY_EXTRNL_MBR_SK"),
    col("Transform.PRVCY_CONF_COMM_RQST_RSN_CD_SK").alias("PRVCY_CONF_COMM_RQST_RSN_CD_SK"),
    col("Transform.PRVCY_CONF_COMM_STTUS_CD_SK").alias("PRVCY_CONF_COMM_STTUS_CD_SK"),
    col("Transform.EFF_DT_SK").alias("EFF_DT_SK"),
    col("Transform.RCVD_DT_SK").alias("RCVD_DT_SK"),
    col("Transform.TERM_DT_SK").alias("TERM_DT_SK"),
    col("Transform.ADDR_SEQ_NO").alias("ADDR_SEQ_NO"),
    col("Transform.ADDREE_FIRST_NM").alias("ADDREE_FIRST_NM"),
    col("Transform.ADDREE_LAST_NM").alias("ADDREE_LAST_NM"),
    col("Transform.CONF_COMM_DESC").alias("CONF_COMM_DESC"),
    col("Transform.EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX")
)

# We now apply SurrogateKeyGen for "Sk_condition"
df_enriched = SurrogateKeyGen(df_PrimaryKey,<DB sequence name>,"Sk_condition",<schema>,<secret_name>)

# Now rename or select final columns in the correct order, substituting "Sk_condition" -> "PRVCY_CONF_COMM_SK" and "NewCurrRunCycExtcnSk" -> "CRT_RUN_CYC_EXCTN_SK"
df_enriched = df_enriched.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    col("Sk_condition").alias("PRVCY_CONF_COMM_SK"),
    "PRVCY_MBR_UNIQ_KEY",
    "SEQ_NO",
    "PRVCY_MBR_SRC_CD_SK",
    "PRVCY_CONF_COMM_TYP_CD_SK",
    col("NewCurrRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_SK",
    "PRVCY_EXTRNL_MBR_SK",
    "PRVCY_CONF_COMM_RQST_RSN_CD_SK",
    "PRVCY_CONF_COMM_STTUS_CD_SK",
    "EFF_DT_SK",
    "RCVD_DT_SK",
    "TERM_DT_SK",
    "ADDR_SEQ_NO",
    "ADDREE_FIRST_NM",
    "ADDREE_LAST_NM",
    "CONF_COMM_DESC",
    "EMAIL_ADDR_TX"
)

# --------------------------------------------------------------------------------
# OutputLink 1 from PrimaryKey: Key -> IdsPrvcyConfComm (CSeqFileStage)
# Write to file
# --------------------------------------------------------------------------------
df_IdsPrvcyConfComm = df_enriched.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "PRVCY_CONF_COMM_SK",
    "PRVCY_MBR_UNIQ_KEY",
    "SEQ_NO",
    "PRVCY_MBR_SRC_CD_SK",
    "PRVCY_CONF_COMM_TYP_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_SK",
    "PRVCY_EXTRNL_MBR_SK",
    "PRVCY_CONF_COMM_RQST_RSN_CD_SK",
    "PRVCY_CONF_COMM_STTUS_CD_SK",
    "EFF_DT_SK",
    "RCVD_DT_SK",
    "TERM_DT_SK",
    "ADDR_SEQ_NO",
    "ADDREE_FIRST_NM",
    "ADDREE_LAST_NM",
    "CONF_COMM_DESC",
    "EMAIL_ADDR_TX"
)

# Apply final rpad for known char/varchar columns:
df_IdsPrvcyConfComm = df_IdsPrvcyConfComm.withColumn(
    "INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"),10," ")
).withColumn(
    "DISCARD_IN", rpad(col("DISCARD_IN"),1," ")
).withColumn(
    "PASS_THRU_IN", rpad(col("PASS_THRU_IN"),1," ")
).withColumn(
    "SRC_SYS_CD", rpad(col("SRC_SYS_CD"),<...>," ")
).withColumn(
    "PRVCY_CONF_COMM_TYP_CD_SK", rpad(col("PRVCY_CONF_COMM_TYP_CD_SK"),4," ")
).withColumn(
    "PRVCY_CONF_COMM_STTUS_CD_SK", rpad(col("PRVCY_CONF_COMM_STTUS_CD_SK"),4," ")
).withColumn(
    "PRVCY_CONF_COMM_RQST_RRSN_CD_SK", rpad(col("PRVCY_CONF_COMM_RQST_RSN_CD_SK"),4," ")
).withColumn(
    "EFF_DT_SK", rpad(col("EFF_DT_SK"),10," ")
).withColumn(
    "RCVD_DT_SK", rpad(col("RCVD_DT_SK"),10," ")
).withColumn(
    "TERM_DT_SK", rpad(col("TERM_DT_SK"),10," ")
).withColumn(
    "ADDREE_FIRST_NM", rpad(col("ADDREE_FIRST_NM"),20," ")
).withColumn(
    "ADDREE_LAST_NM", rpad(col("ADDREE_LAST_NM"),35," ")
).withColumn(
    "CONF_COMM_DESC", rpad(col("CONF_COMM_DESC"),<...>," ")
).withColumn(
    "EMAIL_ADDR_TX", rpad(col("EMAIL_ADDR_TX"),<...>," ")
)

write_files(
    df_IdsPrvcyConfComm,
    f"{adls_path}/key/FctsPrvcyConfCommExtr.PrvcyConfComm.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# OutputLink 2 from PrimaryKey: updt -> hf_prvcy_conf_comm_updt (CHashedFileStage) -> Scenario B
# Insert or update into dummy table
# --------------------------------------------------------------------------------
df_updt = df_enriched.filter(col("PRVCY_CONF_COMM_SK").isNotNull())  # All rows have Sk now, but DS constraint was "IsNull(lkup.PRVCY_CONF_COMM_SK)=@TRUE". We will merge (upsert).

df_updt = df_updt.select(
    "SRC_SYS_CD",
    "PRVCY_MBR_UNIQ_KEY",
    "SEQ_NO",
    "PRVCY_MBR_SRC_CD_SK",
    "PRVCY_CONF_COMM_TYP_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "PRVCY_CONF_COMM_SK"
)

temp_table = "STAGING.FctsPrvcyConfCommExtr_hf_prvcy_conf_comm_updt_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table}", jdbc_url_facets, jdbc_props_facets)

# Write df_updt to the staging table
(
    df_updt.write.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("dbtable", temp_table)
    .mode("overwrite")
    .save()
)

merge_sql = (
    f"MERGE INTO dummy_hf_prvcy_conf_comm AS T "
    f"USING {temp_table} AS S "
    f"ON (T.SRC_SYS_CD = S.SRC_SYS_CD "
    f"AND T.PRVCY_MBR_UNIQ_KEY = S.PRVCY_MBR_UNIQ_KEY "
    f"AND T.SEQ_NO = S.SEQ_NO "
    f"AND T.PRVCY_MBR_SRC_CD_SK = S.PRVCY_MBR_SRC_CD_SK "
    f"AND T.PRVCY_CONF_COMM_TYP_CD_SK = S.PRVCY_CONF_COMM_TYP_CD_SK) "
    f"WHEN MATCHED THEN UPDATE SET "
    f"T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK, "
    f"T.PRVCY_CONF_COMM_SK = S.PRVCY_CONF_COMM_SK "
    f"WHEN NOT MATCHED THEN INSERT (SRC_SYS_CD, PRVCY_MBR_UNIQ_KEY, SEQ_NO, PRVCY_MBR_SRC_CD_SK, PRVCY_CONF_COMM_TYP_CD_SK, CRT_RUN_CYC_EXCTN_SK, PRVCY_CONF_COMM_SK) "
    f"VALUES (S.SRC_SYS_CD, S.PRVCY_MBR_UNIQ_KEY, S.SEQ_NO, S.PRVCY_MBR_SRC_CD_SK, S.PRVCY_CONF_COMM_TYP_CD_SK, S.CRT_RUN_CYC_EXCTN_SK, S.PRVCY_CONF_COMM_SK);"
)

execute_dml(merge_sql, jdbc_url_facets, jdbc_props_facets)