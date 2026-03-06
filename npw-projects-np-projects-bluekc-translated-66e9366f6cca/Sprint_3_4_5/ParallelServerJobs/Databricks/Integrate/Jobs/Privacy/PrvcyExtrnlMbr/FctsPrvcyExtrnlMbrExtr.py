# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 06/19/09 14:44:48 Batch  15146_53151 PROMOTE bckcetl:31540 ids20 dsadm rc for steph 
# MAGIC ^1_2 06/19/09 14:29:48 Batch  15146_52211 INIT bckcett:31540 testIDS dsadm rc for steph 
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/19/07 16:25:25 Batch  14568_59149 PROMOTE bckcetl ids20 dsadm rc for oliver 
# MAGIC ^1_1 11/19/07 16:20:39 Batch  14568_58862 INIT bckcett testIDS30 dsadm rc for oliver 
# MAGIC ^1_2 11/13/07 13:18:55 Batch  14562_47947 PROMOTE bckcett testIDS30 u03651 steph for Ollie
# MAGIC ^1_2 11/13/07 12:57:35 Batch  14562_46657 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 11/06/07 11:53:28 Batch  14555_42843 INIT bckcett devlIDS30 u10913 Ollie move Privacy from devlIDS30 to testIDS30
# MAGIC ^1_1 05/17/07 09:38:32 Batch  14382_34718 INIT bckcett devlIDS30 u10913 Ollie Move from devl to test
# MAGIC ^1_1 04/27/07 12:54:12 Batch  14362_46467 INIT bckcett devlIDS30 u10913 O. Nielsen move from devl to 4.3 Environment
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007-2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsPrvcyExtrnlMbrExtr
# MAGIC CALLED BY: IdsPrvcyExtrSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty               2/25/2007            FHP/3028                     Originally Programmed                              devlIDS30                        Steph Goddard       3/29/2007     
# MAGIC Naren Gerapaty               6/2007              Production Support        Add  P_PRVCY_GRP_ALPHA_PFX           devlIDS30
# MAGIC 
# MAGIC Bhoomi Dasari                 2/17/2009           Prod Supp/15              Added new filter criteria and made              devlIDS
# MAGIC                                                                                                         changes to 
# MAGIC                                                                                                         'PRVCY_EXTRNL_MBR_RELSHP_CD_SK'
# MAGIC                                                                                                         'ALPHA_PFX_TX', 'CNTR_NO', 'GRP_NO',
# MAGIC                                                                                                         'MBR_EXT_NO'  
# MAGIC Bhoomi Dasari                2/25/2009         Prod Supp/15                 Added 'RunDate' paramter instead             devlIDS                      Steph Goddard          02/28/2009
# MAGIC                                                                                                         of Format.Date routine
# MAGIC 
# MAGIC Bhoomi Dasari               3/20/2009          Prod Supp/15                 Updated conditions in the extract SQL       devlIDS                      Steph Goddard           04/01/2009
# MAGIC 
# MAGIC Steph Goddard             7/14/10              TTR-689                         changed primary key counter from           RebuildIntNewDevl        SAndrew                   2010-09-30
# MAGIC                                                                                                         IDS_SK to PRVCY_EXTRNL_MBR_SK; updated to current standards
# MAGIC 
# MAGIC Anoop Nair                2022-03-08         S2S Remediation      Added BCBS and FACETS DSN Connection parameters      IntegrateDev5	Ken Bradmon	2022-06-03

# MAGIC Manually maintained DB2 table
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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
TableTimestamp = get_widget_value('TableTimestamp','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

# --------------------------------------------------------------------------------
# P_PRVCY_GRP_ALPHA_PFX (DB2Connector, reading from IDS)
# --------------------------------------------------------------------------------
extract_query_alpha = f"SELECT GRP_NO as GRP_NO, ALPHA_PFX_TX as ALPHA_PFX_TX FROM {IDSOwner}.P_PRVCY_GRP_ALPHA_PFX PFX"
df_P_PRVCY_GRP_ALPHA_PFX = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_alpha)
    .load()
)

# Deduplicate on GRP_NO for hf_prvcy_mbr_alpha_lkup (Scenario A)
df_hf_prvcy_mbr_alpha_lkup = dedup_sort(df_P_PRVCY_GRP_ALPHA_PFX, ["GRP_NO"], [("GRP_NO","A")])

# --------------------------------------------------------------------------------
# FacetsPrvcyExtrnMbr (ODBCConnector)
# --------------------------------------------------------------------------------
extract_query_facets = f"""
SELECT 
  PMED.PMED_CKE,      
  PMED.PMED_ID,  
  PMED.PMED_LAST_USUS_ID,
  PMED.PMED_LAST_UPD_DTM,
  ENEN.ENEN_CKE
FROM
  {FacetsOwner}.FHP_PMED_MEMBER_D PMED,
  {FacetsOwner}.FHD_ENEN_ENTITY_D ENEN,
  tempdb..TMP_PRVCY_CDC_EM_DRVR TMP
WHERE
  TMP.PMED_CKE = ENEN.ENEN_CKE
  AND PMED.PMED_CKE = ENEN.ENEN_CKE
  AND ENEN.EXEN_REC = 0
"""
df_FacetsPrvcyExtrnMbr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_facets)
    .load()
)

# --------------------------------------------------------------------------------
# StripFields (Transformer)
# --------------------------------------------------------------------------------
df_StripFields = (
    df_FacetsPrvcyExtrnMbr
    .withColumn("PMED_CKE", F.col("PMED_CKE"))
    .withColumn("PMED_ID", trim(Convert( F.lit("CHAR(10) : CHAR(13) : CHAR(9)"), F.lit(""), F.col("PMED_ID"))))
    .withColumn("PMED_LAST_USUS_ID", Convert( F.lit("CHAR(10) : CHAR(13) : CHAR(9)"), F.lit(""), F.col("PMED_LAST_USUS_ID")))
    .withColumn("PMED_LAST_UPD_DTM", FORMAT_DATE(F.col("PMED_LAST_UPD_DTM"), "SYBASE", "TIMESTAMP", "CCYY-MM-DD"))
)

# --------------------------------------------------------------------------------
# BusinessRules (Transformer)
# --------------------------------------------------------------------------------
df_BusinessRules_stage = (
    df_StripFields
    .withColumn("RowPassThru", F.lit("Y"))
    .withColumn("svSrcSysCd", F.lit("FACETS"))
    .withColumn("svPmedCke", F.col("PMED_CKE"))
    .withColumn("svPmedId", trim(F.col("PMED_ID")))
)

df_BusinessRules_stage = df_BusinessRules_stage.withColumn("temp_num_for_relshp", F.col("svPmedId").substr(F.lit(2),F.lit(19)).cast("long"))
df_BusinessRules = (
    df_BusinessRules_stage
    .withColumn(
        "PRVCY_EXTRNL_MBR_RELSHP_CD",
        F.when(F.col("svPmedId").substr(F.lit(1),F.lit(1)) != F.lit("N"), F.lit("09"))
         .otherwise(
            F.when(
                (F.length(F.col("svPmedId").substr(F.lit(2),F.lit(19)))==19)
                & (F.col("temp_num_for_relshp").isNotNull()),
                F.col("svPmedId").substr(F.lit(21),F.lit(2))
            ).otherwise(F.lit("09"))
         )
    )
    .withColumn("ALPHA_PFX_TX", F.col("svPmedId"))
    .withColumn(
        "CNTR_NO",
        F.when(
            F.col("svPmedId").substr(F.lit(1),F.lit(1)) != F.lit("N"),
            F.lit(" ")
        ).otherwise(
            F.when(
                (F.length(F.col("svPmedId").substr(F.lit(2),F.lit(19)))==19)
                & (F.col("temp_num_for_relshp").isNotNull()),
                F.col("svPmedId").substr(F.lit(11),F.lit(9))
            ).otherwise(F.col("svPmedId").substr(F.lit(2),F.lit(17)))
        )
    )
    .withColumn(
        "GRP_NO",
        F.when(
            F.col("svPmedId").substr(F.lit(1),F.lit(1)) != F.lit("N"),
            F.lit(" ")
        ).otherwise(
            F.when(
                (F.length(F.col("svPmedId").substr(F.lit(2),F.lit(19)))==19)
                & (F.col("temp_num_for_relshp").isNotNull()),
                F.col("svPmedId").substr(F.lit(2),F.lit(9))
            ).otherwise(F.lit(" "))
        )
    )
    .withColumn(
        "MBR_EXT_NO",
        F.when(
            F.col("svPmedId").substr(F.lit(1),F.lit(1)) != F.lit("N"),
            F.lit(" ")
        ).otherwise(
            F.when(
                (F.length(F.col("svPmedId").substr(F.lit(2),F.lit(19)))==19)
                & (F.col("temp_num_for_relshp").isNotNull()),
                F.col("svPmedId").substr(F.lit(20),F.lit(1))
            ).otherwise(F.lit(" "))
        )
    )
    .withColumn("MBR_ID", F.col("svPmedId"))
    .withColumn("SRC_SYS_LAST_UPDT_DT", F.col("PMED_LAST_UPD_DTM"))
    .withColumn("SRC_SYS_LAST_UPDT_USER", trim(F.col("PMED_LAST_USUS_ID")))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit("0"))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.col("RowPassThru"))
    .withColumn("FIRST_RECYC_DT", F.lit(RunDate))
    .withColumn("ERR_CT", F.lit("0"))
    .withColumn("RECYCLE_CT", F.lit("0"))
    .withColumn("SRC_SYS_CD", F.col("svSrcSysCd"))
    .withColumn("PRI_KEY_STRING", F.concat_ws(";", F.col("svSrcSysCd"), F.col("svPmedCke")))
    .withColumn("PRVCY_EXTRNL_MBR_SK", F.lit("0"))
    .withColumn("PRVCY_EXTRNL_MBR_UNIQ_KEY", F.col("svPmedCke"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit("0"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit("0"))
)

# --------------------------------------------------------------------------------
# AlphaLookup (Transformer with left join to df_hf_prvcy_mbr_alpha_lkup)
# --------------------------------------------------------------------------------
df_AlphaLookup_joined = (
    df_BusinessRules.alias("AlphaTx")
    .join(
        df_hf_prvcy_mbr_alpha_lkup.alias("AlphaLkup"),
        (F.col("AlphaTx.GRP_NO") == F.col("AlphaLkup.GRP_NO")),
        how="left"
    )
)

df_AlphaLookup_stage = df_AlphaLookup_joined.withColumn("temp_num_for_pfx", F.col("AlphaTx.ALPHA_PFX_TX").substr(F.lit(2),F.lit(19)).cast("long"))
df_AlphaLookup = (
    df_AlphaLookup_stage
    .select(
        F.col("AlphaTx.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("AlphaTx.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("AlphaTx.DISCARD_IN").alias("DISCARD_IN"),
        F.col("AlphaTx.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("AlphaTx.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("AlphaTx.ERR_CT").alias("ERR_CT"),
        F.col("AlphaTx.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("AlphaTx.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("AlphaTx.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("AlphaTx.PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
        F.col("AlphaTx.PRVCY_EXTRNL_MBR_UNIQ_KEY").alias("PRVCY_EXTRNL_MBR_UNIQ_KEY"),
        F.col("AlphaTx.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("AlphaTx.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("AlphaTx.PRVCY_EXTRNL_MBR_RELSHP_CD").alias("ORIG_RELSHP_CD"),
        F.col("AlphaTx.ALPHA_PFX_TX").alias("ALPHA_TX_RAW"),
        F.col("AlphaTx.CNTR_NO").alias("CNTR_NO"),
        F.col("AlphaTx.GRP_NO").alias("GRP_NO"),
        F.col("AlphaTx.MBR_EXT_NO").alias("MBR_EXT_NO"),
        F.col("AlphaTx.MBR_ID").alias("MBR_ID"),
        F.col("AlphaTx.SRC_SYS_LAST_UPDT_DT").alias("SRC_SYS_LAST_UPDT_DT"),
        F.col("AlphaTx.SRC_SYS_LAST_UPDT_USER").alias("SRC_SYS_LAST_UPDT_USER"),
        F.col("AlphaLkup.GRP_NO").alias("lkup_GRP_NO"),
        F.col("AlphaLkup.ALPHA_PFX_TX").alias("lkup_ALPHA_PFX_TX"),
        F.col("temp_num_for_pfx")
    )
    .withColumn(
        "ALPHA_PFX_TX",
        F.when(
            F.col("ALPHA_TX_RAW").substr(F.lit(1),F.lit(1)) != F.lit("N"),
            F.lit(" ")
        ).otherwise(
            F.when(
                (F.length(F.col("ALPHA_TX_RAW").substr(F.lit(2),F.lit(19)))==19)
                & (F.col("temp_num_for_pfx").isNull()),
                F.col("ALPHA_TX_RAW").substr(F.lit(2),F.lit(3))
            ).otherwise(
                F.when(
                    F.col("lkup_GRP_NO").isNotNull(),
                    F.col("lkup_ALPHA_PFX_TX")
                ).otherwise(F.lit("UNK"))
            )
        )
    )
    .drop("temp_num_for_pfx","lkup_GRP_NO","lkup_ALPHA_PFX_TX","ALPHA_TX_RAW")
    .withColumn("PRVCY_EXTRNL_MBR_RELSHP_CD", F.col("ORIG_RELSHP_CD"))
    .drop("ORIG_RELSHP_CD")
)

# --------------------------------------------------------------------------------
# hf_extrnl_mbr read from dummy table (Scenario B)
# --------------------------------------------------------------------------------
df_hf_extrnl_mbr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT SRC_SYS_CD, PRVCY_EXTRNL_MBR_UNIQ_KEY, CRT_RUN_CYC_EXCTN_SK, PRVCY_EXTRNL_MBR_SK FROM IDS.dummy_hf_extrnl_mbr")
    .load()
)

# --------------------------------------------------------------------------------
# PrimaryKey (Transformer) with left join to df_hf_extrnl_mbr
# --------------------------------------------------------------------------------
df_PrimaryKey_Stg = (
    df_AlphaLookup.alias("Transform")
    .join(
        df_hf_extrnl_mbr.alias("lkup"),
        (F.col("Transform.SRC_SYS_CD")==F.col("lkup.SRC_SYS_CD")) & (F.col("Transform.PRVCY_EXTRNL_MBR_UNIQ_KEY")==F.col("lkup.PRVCY_EXTRNL_MBR_UNIQ_KEY")),
        how="left"
    )
    .select(
        F.col("Transform.*"),
        F.col("lkup.PRVCY_EXTRNL_MBR_SK").alias("lkup_PRVCY_EXTRNL_MBR_SK"),
        F.col("lkup.CRT_RUN_CYC_EXCTN_SK").alias("lkup_CRT_RUN_CYC_EXCTN_SK")
    )
    .withColumn(
        "NewCurrRunCycExtcnSk",
        F.when(F.col("lkup_PRVCY_EXTRNL_MBR_SK").isNull(), F.lit(CurrRunCycle))
         .otherwise(F.col("lkup_CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn(
        "Sk",
        F.when(F.col("lkup_PRVCY_EXTRNL_MBR_SK").isNull(), F.lit(None).cast("long"))
         .otherwise(F.col("lkup_PRVCY_EXTRNL_MBR_SK").cast("long"))
    )
)

df_enriched = df_PrimaryKey_Stg
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"Sk",<schema>,<secret_name>)

df_enriched = (
    df_enriched
    .withColumn("PRVCY_EXTRNL_MBR_SK", F.col("Sk"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("NewCurrRunCycExtcnSk"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn(
        "temp_relshp",
        F.when(
            F.col("PRVCY_EXTRNL_MBR_RELSHP_CD").isin("01","02","03","04","05","06","07","08","09"),
            F.col("PRVCY_EXTRNL_MBR_RELSHP_CD")
        ).otherwise(F.lit("09"))
    )
    .drop("PRVCY_EXTRNL_MBR_RELSHP_CD")
    .withColumnRenamed("temp_relshp","PRVCY_EXTRNL_MBR_RELSHP_CD")
)

df_final_Key = df_enriched.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "PRVCY_EXTRNL_MBR_SK",
    "PRVCY_EXTRNL_MBR_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRVCY_EXTRNL_MBR_RELSHP_CD",
    "ALPHA_PFX_TX",
    "CNTR_NO",
    "GRP_NO",
    "MBR_EXT_NO",
    "MBR_ID",
    "SRC_SYS_LAST_UPDT_DT",
    "SRC_SYS_LAST_UPDT_USER"
)

df_final_Key = (
    df_final_Key
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("PRI_KEY_STRING", F.rpad(F.col("PRI_KEY_STRING"), <...>, " "))
    .withColumn("ALPHA_PFX_TX", F.rpad(F.col("ALPHA_PFX_TX"), <...>, " "))
    .withColumn("CNTR_NO", F.rpad(F.col("CNTR_NO"), <...>, " "))
    .withColumn("GRP_NO", F.rpad(F.col("GRP_NO"), <...>, " "))
    .withColumn("MBR_EXT_NO", F.rpad(F.col("MBR_EXT_NO"), 2, " "))
    .withColumn("MBR_ID", F.rpad(F.col("MBR_ID"), <...>, " "))
    .withColumn("SRC_SYS_LAST_UPDT_DT", F.rpad(F.col("SRC_SYS_LAST_UPDT_DT"), 10, " "))
    .withColumn("SRC_SYS_LAST_UPDT_USER", F.rpad(F.col("SRC_SYS_LAST_UPDT_USER"), 10, " "))
)

out_file_path = f"{adls_path}/key/FctsPrvcyExtrnlMbrExtr.PrvcyExtrnlMbr.dat.{RunID}"
write_files(
    df_final_Key,
    out_file_path,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_updt = (
    df_enriched
    .filter(F.col("lkup_PRVCY_EXTRNL_MBR_SK").isNull())
    .select(
        F.col("SRC_SYS_CD").cast("int"),
        F.col("PRVCY_EXTRNL_MBR_UNIQ_KEY").cast("int"),
        F.lit(CurrRunCycle).cast("int").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Sk").cast("int").alias("PRVCY_EXTRNL_MBR_SK")
    )
)

temp_table_name = "STAGING.FctsPrvcyExtrnlMbrExtr_hf_extrnl_mbr_updt_temp"
drop_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
execute_dml(drop_sql, jdbc_url_ids, jdbc_props_ids)

df_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", temp_table_name) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO IDS.dummy_hf_extrnl_mbr AS T
USING {temp_table_name} AS S
ON (T.SRC_SYS_CD=S.SRC_SYS_CD AND T.PRVCY_EXTRNL_MBR_UNIQ_KEY=S.PRVCY_EXTRNL_MBR_UNIQ_KEY)
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK=S.CRT_RUN_CYC_EXCTN_SK,
    T.PRVCY_EXTRNL_MBR_SK=S.PRVCY_EXTRNL_MBR_SK
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, PRVCY_EXTRNL_MBR_UNIQ_KEY, CRT_RUN_CYC_EXCTN_SK, PRVCY_EXTRNL_MBR_SK)
  VALUES (S.SRC_SYS_CD, S.PRVCY_EXTRNL_MBR_UNIQ_KEY, S.CRT_RUN_CYC_EXCTN_SK, S.PRVCY_EXTRNL_MBR_SK);
"""
execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)