# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from PMDX_DISC_X FHP_PMED_MEMBER_D,FHP_PBED_BUSINES_D,FHP_PCED_COV_ENT_D for loading into IDS.
# MAGIC 
# MAGIC PROCESSING:   PRVCY_EXTRNL_ENTY.EXTRNL_ENTY_SK is used as foriegn key.
# MAGIC       
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Naren Garapaty     02/20/2007                Initial program                                                                                                             devlIDS30                          
# MAGIC 
# MAGIC Bhoomi Dasari       03/26/2007               Modification made to PRVCY_EXTRNL_ENTY_SK                                                   CDS Sunset/3279 devlIDS30
# MAGIC                                                                 to PRVCY_ENTY_SK and removed DSCLSUR_ID                     
# MAGIC Naren Garapaty     06/01/2007               Added FHD_EXFM lookup from Facets                                           Production Support   devlIDS30
# MAGIC 
# MAGIC Bhoomi Dasari       02/18/2009               Updated logic for PRVCY_MBR_SRC_CD_SK                                Prod Supp/15      devlIDS
# MAGIC 
# MAGIC Bhoomi Dasari       2/25/2009                Added 'RunDate' paramter instead                                                    Prod Supp/15      devlIDS                             Steph Goddard        02/28/2009
# MAGIC                                                                of Format.Date routine
# MAGIC 
# MAGIC 
# MAGIC Bhoomi Dasari      3/16/2009                Updated Mbr_sk &                                                                              ProdSupp/15        devlIDS                            Steph Goddard          04/01/2009
# MAGIC                                                               Prvcy_Mbr_Src_Cd_Sk ProdSupp/15          
# MAGIC 
# MAGIC Steph Goddard      7/14/10                  Changed primary key counter IDS_SK to PRVCY_DSCLSR_SK        TTR-689             RebuildIntNewDevl           SAndrew                     2010-09-30
# MAGIC                                                              updated to current standards         
# MAGIC 
# MAGIC Manasa Andru              7/8/2013          Added the business rule in PrimaryKey transformer                            TTR - 778             devlCurIDS                          Kalyan Neelam            2013-07-10
# MAGIC                                                                for MBR_SK and PRVCY_EXTRNL_MBR_SK
# MAGIC 
# MAGIC Manasa Andru             7/23/2013         Trimmed the fields in the updt link in Primarykey transformer             TTR - 902           IntegrateCurDevl                      Kalyan Neelam         2013-07-29
# MAGIC                                                                   and updated Metadata of PRVCY_MBR_SRC_CD.
# MAGIC 
# MAGIC Anoop Nair                2022-03-07             Added  FACETS DSN Connection parameters                           S2S Remediation            IntegrateDev5

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


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','2013-07-23')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)

extract_query = """SELECT 
    PMDX.PMED_CKE,
    PMDX.PMDX_SEQ_NO,
    PMDX.PMDX_DSC_ENEN_CKE,
    PMDX.PMDX_RCP_ENEN_CKE,
    PMDX.PMDX_PZCD_DRSN,
    PMDX.PMDX_PZCD_MTYP,
    PMDX.PMDX_DISC_DTM,
    PMDX.PMDX_PZCD_CAT1,
    PMDX.PMDX_PZCD_CAT2,
    PMDX.PMDX_PZCD_CAT3,
    PMDX.PMDX_PZCD_CAT4,
    PMDX.PMDX_DESC,
    PMDX.PMDX_CREATE_DTM,
    PMDX.PMDX_LAST_USUS_ID,
    PMDX.PMDX_LAST_UPD_DTM,
    PMED.PMED_ID
FROM {}.FHP_PMDX_DISC_X PMDX,
     {}.FHP_PMED_MEMBER_D PMED
WHERE PMDX.PMED_CKE = PMED.PMED_CKE
""".format(FacetsOwner, FacetsOwner)

df_FctsPrvcyDsclsur_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

fhdexfm_query = """SELECT
    DISC.PMED_CKE,
    MEMB.MEME_CK
FROM {}.FHP_PMDX_DISC_X DISC,
     {}.FHD_EXEN_BASE_D BASE,
     {}.FHD_EXFM_FA_MEMB_D MEMB
WHERE DISC.PMED_CKE=BASE.ENEN_CKE
  AND BASE.EXEN_REC=MEMB.EXEN_REC
""".format(FacetsOwner, FacetsOwner, FacetsOwner)

df_FctsPrvcyDsclsur_FHDEXFM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", fhdexfm_query)
    .load()
)

entity_query = """SELECT 
    ENEN_CKE,
    EXEN_REC
FROM {}.FHD_ENEN_ENTITY_D
WHERE EXEN_REC = 0
""".format(FacetsOwner)

df_FctsPrvcyDsclsur_Entity = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", entity_query)
    .load()
)

df_Strip = (
    df_FctsPrvcyDsclsur_Extract
    .withColumn("PMED_CKE", F.col("PMED_CKE"))
    .withColumn("PMDX_SEQ_NO", F.col("PMDX_SEQ_NO"))
    .withColumn("PMDX_DSC_ENEN_CKE", F.col("PMDX_DSC_ENEN_CKE"))
    .withColumn("PMDX_RCP_ENEN_CKE", F.col("PMDX_RCP_ENEN_CKE"))
    .withColumn("PMDX_PZCD_DRSN", F.regexp_replace(F.upper(F.col("PMDX_PZCD_DRSN")), "[\\r\\n\\t]", ""))
    .withColumn("PMDX_PZCD_MTYP", F.regexp_replace(F.upper(F.col("PMDX_PZCD_MTYP")), "[\\r\\n\\t]", ""))
    .withColumn("PMDX_DISC_DTM", F.date_format(F.col("PMDX_DISC_DTM"), "yyyy-MM-dd"))
    .withColumn(
        "PMDX_PZCD_CAT1",
        trim(F.regexp_replace(F.upper(F.col("PMDX_PZCD_CAT1")), "[\\r\\n\\t]", "")),
    )
    .withColumn(
        "PMDX_PZCD_CAT2",
        trim(F.regexp_replace(F.upper(F.col("PMDX_PZCD_CAT2")), "[\\r\\n\\t]", "")),
    )
    .withColumn(
        "PMDX_PZCD_CAT3",
        trim(F.regexp_replace(F.upper(F.col("PMDX_PZCD_CAT3")), "[\\r\\n\\t]", "")),
    )
    .withColumn(
        "PMDX_PZCD_CAT4",
        trim(F.regexp_replace(F.upper(F.col("PMDX_PZCD_CAT4")), "[\\r\\n\\t]", "")),
    )
    .withColumn(
        "PMDX_DESC",
        F.regexp_replace(F.upper(F.col("PMDX_DESC")), "[\\r\\n\\t]", ""),
    )
    .withColumn("PMDX_CREATE_DTM", F.date_format(F.col("PMDX_CREATE_DTM"), "yyyy-MM-dd"))
    .withColumn("PMDX_LAST_USUS_ID", trim(F.col("PMDX_LAST_USUS_ID")))
    .withColumn("PMDX_LAST_UPD_DTM", F.date_format(F.col("PMDX_LAST_UPD_DTM"), "yyyy-MM-dd"))
    .withColumn("PMED_ID", trim(F.col("PMED_ID")))
)

df_meme_lookup = df_FctsPrvcyDsclsur_FHDEXFM.dropDuplicates(["PMED_CKE"])
df_EntityLkup = df_FctsPrvcyDsclsur_Entity.dropDuplicates(["ENEN_CKE"])

df_BusinessRules = (
    df_Strip.alias("Strip")
    .join(
        df_meme_lookup.alias("meme_lookup"),
        F.col("Strip.PMED_CKE") == F.col("meme_lookup.PMED_CKE"),
        "left",
    )
    .join(
        df_EntityLkup.alias("EntityLkup"),
        F.col("Strip.PMED_CKE") == F.col("EntityLkup.ENEN_CKE"),
        "left",
    )
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(RunDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(
            F.lit("FACETS"), F.lit(";"),
            F.col("Strip.PMED_CKE"), F.lit(";"),
            F.col("Strip.PMDX_SEQ_NO"), F.lit(";"),
            F.col("Strip.PMED_ID"),
        ),
    )
    .withColumn("PRVCY_DSCLSUR_SK", F.lit(0))
    .withColumn("PRVCY_MBR_UNIQ_KEY", F.col("Strip.PMED_CKE"))
    .withColumn("SEQ_NO", F.col("Strip.PMDX_SEQ_NO"))
    .withColumn(
        "PRVCY_MBR_SRC_CD",
        F.when(F.col("EntityLkup.ENEN_CKE").isNotNull(), F.lit("N")).otherwise(F.lit("F")),
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("PRVCY_DSCLSUR_ENTY", F.col("Strip.PMDX_DSC_ENEN_CKE"))
    .withColumn(
        "MBR",
        F.when(F.col("meme_lookup.MEME_CK").isNull(), F.lit(1)).otherwise(F.col("meme_lookup.MEME_CK")),
    )
    .withColumn("PRVCY_EXTRNL_MBR", F.col("Strip.PMED_CKE"))
    .withColumn("PRVCY_RECPNT_EXTRNL_ENTY", F.col("Strip.PMDX_RCP_ENEN_CKE"))
    .withColumn(
        "PRVCY_DSCLSUR_CAT_CD_1",
        F.when(
            F.col("Strip.PMDX_PZCD_CAT1").isNull() | (F.length(F.col("Strip.PMDX_PZCD_CAT1")) == 0),
            F.lit("NA"),
        ).otherwise(F.col("Strip.PMDX_PZCD_CAT1")),
    )
    .withColumn(
        "PRVCY_DSCLSUR_CAT_CD_2",
        F.when(
            F.col("Strip.PMDX_PZCD_CAT2").isNull() | (F.length(F.col("Strip.PMDX_PZCD_CAT2")) == 0),
            F.lit("NA"),
        ).otherwise(F.col("Strip.PMDX_PZCD_CAT2")),
    )
    .withColumn(
        "PRVCY_DSCLSUR_CAT_CD_3",
        F.when(
            F.col("Strip.PMDX_PZCD_CAT3").isNull() | (F.length(F.col("Strip.PMDX_PZCD_CAT3")) == 0),
            F.lit("NA"),
        ).otherwise(F.col("Strip.PMDX_PZCD_CAT3")),
    )
    .withColumn(
        "PRVCY_DSCLSUR_CAT_CD_4",
        F.when(
            F.col("Strip.PMDX_PZCD_CAT4").isNull() | (F.length(F.col("Strip.PMDX_PZCD_CAT4")) == 0),
            F.lit("NA"),
        ).otherwise(F.col("Strip.PMDX_PZCD_CAT4")),
    )
    .withColumn("PRVCY_DSCLSUR_METH_CD", F.col("Strip.PMDX_PZCD_MTYP"))
    .withColumn("PRVCY_DSCLSUR_RSN_CD", F.col("Strip.PMDX_PZCD_DRSN"))
    .withColumn("CRT_DT_SK", F.col("Strip.PMDX_CREATE_DTM"))
    .withColumn("DSCLSUR_DT_SK", F.col("Strip.PMDX_DISC_DTM"))
    .withColumn("DSCLSUR_DESC", F.col("Strip.PMDX_DESC"))
    .withColumn("SRC_SYS_LAST_UPDT_DT", F.col("Strip.PMDX_LAST_UPD_DTM"))
    .withColumn("SRC_SYS_LAST_UPDT_USER", F.col("Strip.PMDX_LAST_USUS_ID"))
)

jdbc_url_dummy, jdbc_props_dummy = get_db_config("<secret_name_for_dummy_table_if_needed>")
df_dummy_hf_prvcy_dsclsur = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_dummy)
    .options(**jdbc_props_dummy)
    .option(
        "query",
        "SELECT SRC_SYS_CD, PRVCY_MBR_UNIQ_KEY, SEQ_NO, PRVCY_MBR_SRC_CD, CRT_RUN_CYC_EXCTN_SK, PRVCY_DSCLSUR_SK FROM dummy_hf_prvcy_dsclsur"
    )
    .load()
)

df_enriched = (
    df_BusinessRules.alias("Transform")
    .join(
        df_dummy_hf_prvcy_dsclsur.alias("lkup"),
        (
            F.trim(F.col("Transform.SRC_SYS_CD")) == F.col("lkup.SRC_SYS_CD")
        )
        & (
            F.trim(F.col("Transform.PRVCY_MBR_UNIQ_KEY")) == F.col("lkup.PRVCY_MBR_UNIQ_KEY")
        )
        & (
            F.trim(F.col("Transform.SEQ_NO")) == F.col("lkup.SEQ_NO")
        )
        & (
            F.trim(F.col("Transform.PRVCY_MBR_SRC_CD")) == F.col("lkup.PRVCY_MBR_SRC_CD")
        ),
        "left",
    )
    .withColumn(
        "NewCurrRunCycExtcnSk",
        F.when(F.col("lkup.PRVCY_DSCLSUR_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK")),
    )
    .withColumn(
        "Sk",
        F.when(F.col("lkup.PRVCY_DSCLSUR_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.PRVCY_DSCLSUR_SK")),
    )
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"Sk",<schema>,<secret_name>)

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
    F.col("Sk").alias("PRVCY_DSCLSUR_SK"),
    F.col("Transform.PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
    F.col("Transform.SEQ_NO").alias("SEQ_NO"),
    F.when(
        F.col("Transform.PRVCY_MBR_SRC_CD") == F.lit("N"),
        F.lit("N")
    ).otherwise(F.lit("F")).alias("PRVCY_MBR_SRC_CD"),
    F.col("NewCurrRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.PRVCY_DSCLSUR_ENTY").alias("PRVCY_DSCLSUR_ENTY"),
    F.when(
        F.col("Transform.PRVCY_MBR_SRC_CD") == F.lit("F"),
        F.col("Transform.MBR")
    ).otherwise(F.lit("NA")).alias("MBR"),
    F.when(
        F.col("Transform.PRVCY_MBR_SRC_CD") == F.lit("N"),
        F.col("Transform.PRVCY_EXTRNL_MBR")
    ).otherwise(F.lit("NA")).alias("PRVCY_EXTRNL_MBR"),
    F.col("Transform.PRVCY_RECPNT_EXTRNL_ENTY").alias("PRVCY_RECPNT_EXTRNL_ENTY"),
    F.col("Transform.PRVCY_DSCLSUR_CAT_CD_1").alias("PRVCY_DSCLSUR_CAT_CD_1"),
    F.col("Transform.PRVCY_DSCLSUR_CAT_CD_2").alias("PRVCY_DSCLSUR_CAT_CD_2"),
    F.col("Transform.PRVCY_DSCLSUR_CAT_CD_3").alias("PRVCY_DSCLSUR_CAT_CD_3"),
    F.col("Transform.PRVCY_DSCLSUR_CAT_CD_4").alias("PRVCY_DSCLSUR_CAT_CD_4"),
    F.col("Transform.PRVCY_DSCLSUR_METH_CD").alias("PRVCY_DSCLSUR_METH_CD"),
    F.col("Transform.PRVCY_DSCLSUR_RSN_CD").alias("PRVCY_DSCLSUR_RSN_CD"),
    F.col("Transform.CRT_DT_SK").alias("CRT_DT_SK"),
    F.col("Transform.DSCLSUR_DT_SK").alias("DSCLSUR_DT_SK"),
    F.col("Transform.DSCLSUR_DESC").alias("DSCLSUR_DESC"),
    F.col("Transform.SRC_SYS_LAST_UPDT_DT").alias("SRC_SYS_LAST_UPDT_DT"),
    F.col("Transform.SRC_SYS_LAST_UPDT_USER").alias("SRC_SYS_LAST_UPDT_USER"),
)

df_updt = (
    df_enriched.filter(F.col("lkup.PRVCY_DSCLSUR_SK").isNull())
    .select(
        F.trim(F.col("Transform.SRC_SYS_CD")).alias("SRC_SYS_CD"),
        F.trim(F.col("Transform.PRVCY_MBR_UNIQ_KEY")).alias("PRVCY_MBR_UNIQ_KEY"),
        F.trim(F.col("Transform.SEQ_NO")).alias("SEQ_NO"),
        F.trim(F.col("Transform.PRVCY_MBR_SRC_CD")).alias("PRVCY_MBR_SRC_CD"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Sk").alias("PRVCY_DSCLSUR_SK"),
    )
)

df_IdsPrvcyDsclsurExtr = df_Key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("PRVCY_DSCLSUR_SK"),
    F.col("PRVCY_MBR_UNIQ_KEY"),
    F.col("SEQ_NO"),
    F.col("PRVCY_MBR_SRC_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PRVCY_DSCLSUR_ENTY"),
    F.col("MBR"),
    F.col("PRVCY_EXTRNL_MBR"),
    F.col("PRVCY_RECPNT_EXTRNL_ENTY"),
    F.rpad(F.col("PRVCY_DSCLSUR_CAT_CD_1"), 10, " ").alias("PRVCY_DSCLSUR_CAT_CD_1"),
    F.rpad(F.col("PRVCY_DSCLSUR_CAT_CD_2"), 10, " ").alias("PRVCY_DSCLSUR_CAT_CD_2"),
    F.rpad(F.col("PRVCY_DSCLSUR_CAT_CD_3"), 10, " ").alias("PRVCY_DSCLSUR_CAT_CD_3"),
    F.rpad(F.col("PRVCY_DSCLSUR_CAT_CD_4"), 10, " ").alias("PRVCY_DSCLSUR_CAT_CD_4"),
    F.rpad(F.col("PRVCY_DSCLSUR_METH_CD"), 10, " ").alias("PRVCY_DSCLSUR_METH_CD"),
    F.rpad(F.col("PRVCY_DSCLSUR_RSN_CD"), 10, " ").alias("PRVCY_DSCLSUR_RSN_CD"),
    F.rpad(F.col("CRT_DT_SK"), 10, " ").alias("CRT_DT_SK"),
    F.rpad(F.col("DSCLSUR_DT_SK"), 10, " ").alias("DSCLSUR_DT_SK"),
    F.col("DSCLSUR_DESC"),
    F.rpad(F.col("SRC_SYS_LAST_UPDT_DT"), 10, " ").alias("SRC_SYS_LAST_UPDT_DT"),
    F.col("SRC_SYS_LAST_UPDT_USER"),
)

write_files(
    df_IdsPrvcyDsclsurExtr,
    f"{adls_path}/key/FctsPrvcyDsclsurExtr.PrvcyDsclsur.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

jdbc_url_dummy_updt, jdbc_props_dummy_updt = get_db_config("<secret_name_for_dummy_table_if_needed>")

temp_table_name = "STAGING.FctsPrvcyDsclsurExtr_hf_prvcy_dsclsur_updt_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url_dummy_updt, jdbc_props_dummy_updt)

df_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url_dummy_updt) \
    .options(**jdbc_props_dummy_updt) \
    .option("dbtable", temp_table_name) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO dummy_hf_prvcy_dsclsur AS T
USING {temp_table_name} AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.PRVCY_MBR_UNIQ_KEY = S.PRVCY_MBR_UNIQ_KEY
    AND T.SEQ_NO = S.SEQ_NO
    AND T.PRVCY_MBR_SRC_CD = S.PRVCY_MBR_SRC_CD
WHEN MATCHED THEN
    UPDATE SET
        T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
        T.PRVCY_DSCLSUR_SK = S.PRVCY_DSCLSUR_SK
WHEN NOT MATCHED THEN
    INSERT (
        SRC_SYS_CD,
        PRVCY_MBR_UNIQ_KEY,
        SEQ_NO,
        PRVCY_MBR_SRC_CD,
        CRT_RUN_CYC_EXCTN_SK,
        PRVCY_DSCLSUR_SK
    )
    VALUES (
        S.SRC_SYS_CD,
        S.PRVCY_MBR_UNIQ_KEY,
        S.SEQ_NO,
        S.PRVCY_MBR_SRC_CD,
        S.CRT_RUN_CYC_EXCTN_SK,
        S.PRVCY_DSCLSUR_SK
    );
"""

execute_dml(merge_sql, jdbc_url_dummy_updt, jdbc_props_dummy_updt)