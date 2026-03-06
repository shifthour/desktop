# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsPrvcyDsclsurAcctgExtr
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               3/2/2007                                              Originally Programmed                              devlIDS30                        Steph Goddard            
# MAGIC              
# MAGIC Bhoomi Dasari                   2/18/2009      Prod Supp/15                Made logic changes to                            devlIDS
# MAGIC                                                                                                        'PRVCY_MBR_SRC_CD_SK'
# MAGIC 
# MAGIC Bhoomi Dasari                2/25/2009         Prod Supp/15                 Added 'RunDate' paramter instead         devlIDS                          Steph Goddard            02/28/2009
# MAGIC                                                                                                         of Format.Date routine
# MAGIC 
# MAGIC Bhoomi Dasari               3/17/2009         Prod Supp/15                  Added Mbr_sk new logic and                      devlIDS                     Steph Goddard             04/01/2009
# MAGIC                                                                                                         Prvcy_Mbr_Src_Cd_Sk
# MAGIC 
# MAGIC Steph Goddard               7/14/10             TTR-689                        changed primary key counter IDS_SK to   RebuildIntNewDevl          SAndrew                   2010-09-30
# MAGIC                                                                                                        PRVCY_DSCLSR_ACCTG_SK; brought up to current standards
# MAGIC 
# MAGIC Manasa Andru              7/8/2013           TTR - 778                 Added the business rule in PrimaryKey transformer     devlCurIDS               Kalyan Neelam          2013-07-10
# MAGIC                                                                                                      for MBR_SK and PRVCY_EXTRNL_MBR_SK
# MAGIC 
# MAGIC Anoop Nair                    2022-03-07         S2S Remediation      Added FACETS DSN Connection parameters      IntegrateDev5	Ken Bradmon	2022-06-03

# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Privacy Disclosure Accounting Data
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
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

df_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        """SELECT disc.PMED_CKE,
disc.PMDR_REQ_NO,
disc.PMDR_RTYP_IND,
disc.PMDR_RECEIVED_DT,
disc.PMDR_REQUEST_DT,
disc.PMDR_DSC_FROM_DT,
disc.PMDR_DSC_TO_DT,
disc.PTST_CURR_STS,
disc.PTST_CUR_PZCD_SRSN,
disc.PTST_CURR_STS_DTM,
disc.PMDR_DESC,
disc.PMDR_CREATE_DTM,
disc.PMDR_LAST_USUS_ID,
disc.PMDR_LAST_UPD_DTM,
member.PMED_ID
FROM
""" + FacetsOwner + """.FHP_PMDR_DISC_REQ_X disc,
""" + FacetsOwner + """.FHP_PMED_MEMBER_D member
WHERE
disc.PMED_CKE=member.PMED_CKE
"""
    )
    .load()
)

df_EntityExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        """SELECT ENEN_CKE, EXEN_REC
FROM """ + FacetsOwner + """.FHD_ENEN_ENTITY_D
WHERE EXEN_REC = 0
"""
    )
    .load()
)

df_MbrSk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        """SELECT
REQ.PMED_CKE,
MEMB.MEME_CK
FROM
""" + FacetsOwner + """.FHP_PMDR_DISC_REQ_X REQ,
""" + FacetsOwner + """.FHD_EXEN_BASE_D BASE,
""" + FacetsOwner + """.FHD_EXFM_FA_MEMB_D MEMB
WHERE
REQ.PMED_CKE = BASE.ENEN_CKE
AND BASE.EXEN_REC = MEMB.EXEN_REC
"""
    )
    .load()
)

df_Strip = df_Extract.select(
    F.col("PMED_CKE").alias("PMED_CKE"),
    F.col("PMDR_REQ_NO").alias("PMDR_REQ_NO"),
    F.col("PMDR_RTYP_IND").alias("PMDR_RTYP_IND"),
    F.date_format("PMDR_RECEIVED_DT", "yyyy-MM-dd").alias("PMDR_RECEIVED_DT"),
    F.date_format("PMDR_REQUEST_DT", "yyyy-MM-dd").alias("PMDR_REQUEST_DT"),
    F.date_format("PMDR_DSC_FROM_DT", "yyyy-MM-dd").alias("PMDR_DSC_FROM_DT"),
    F.date_format("PMDR_DSC_TO_DT", "yyyy-MM-dd").alias("PMDR_DSC_TO_DT"),
    F.col("PTST_CURR_STS").alias("PTST_CURR_STS"),
    strip_field(F.col("PTST_CUR_PZCD_SRSN")).alias("PTST_CUR_PZCD_SRSN"),
    F.date_format("PTST_CURR_STS_DTM", "yyyy-MM-dd").alias("PTST_CURR_STS_DTM"),
    strip_field(F.col("PMDR_DESC")).alias("PMDR_DESC"),
    F.date_format("PMDR_CREATE_DTM", "yyyy-MM-dd").alias("PMDR_CREATE_DTM"),
    strip_field(F.col("PMDR_LAST_USUS_ID")).alias("PMDR_LAST_USUS_ID"),
    F.date_format("PMDR_LAST_UPD_DTM", "yyyy-MM-dd").alias("PMDR_LAST_UPD_DTM"),
    strip_field(F.col("PMED_ID")).alias("PMED_ID"),
)

df_EntityLkup = dedup_sort(df_EntityExtr, ["ENEN_CKE"], [])
df_MbrSk_lkup = dedup_sort(df_MbrSk, ["PMED_CKE"], [])

df_BusinessRules = (
    df_Strip.alias("Strip")
    .join(
        df_EntityLkup.alias("EntityLkup"),
        F.col("Strip.PMED_CKE") == F.col("EntityLkup.ENEN_CKE"),
        "left",
    )
    .join(
        df_MbrSk_lkup.alias("MbrSk_lkup"),
        F.col("Strip.PMED_CKE") == F.col("MbrSk_lkup.PMED_CKE"),
        "left",
    )
    .select(
        F.lit("Y").alias("RowPassThru"),
        F.lit("FACETS").alias("svSrcSysCd"),
        F.col("Strip.PMED_CKE").alias("svPmedCke"),
        F.col("Strip.*"),
        F.col("EntityLkup.ENEN_CKE").alias("EntityLkup_ENEN_CKE"),
        F.col("EntityLkup.EXEN_REC").alias("EntityLkup_EXEN_REC"),
        F.col("MbrSk_lkup.PMED_CKE").alias("MbrSk_lkup_PMED_CKE"),
        F.col("MbrSk_lkup.MEME_CK").alias("MbrSk_lkup_MEME_CK"),
    )
)

df_BusinessRules = df_BusinessRules.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("RowPassThru").alias("PASS_THRU_IN"),
    F.lit(RunDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("svSrcSysCd").alias("SRC_SYS_CD"),
    F.concat_ws(";", F.col("svSrcSysCd"), F.col("svPmedCke"), F.col("PMDR_REQ_NO")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("PRVCY_DSCLSUR_ACCTG_SK"),
    F.col("PMED_CKE").alias("PRVCY_MBR_UNIQ_KEY"),
    F.col("PMDR_REQ_NO").alias("RQST_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        F.col("MbrSk_lkup_PMED_CKE").isNull()
        | (F.length(trim(F.col("MbrSk_lkup_PMED_CKE"))) == 0),
        "NA",
    ).otherwise(F.col("MbrSk_lkup_MEME_CK")).alias("MBR_SK"),
    F.col("svPmedCke").alias("PRVCY_EXTRNL_MBR_SK"),
    F.col("PMDR_RTYP_IND").alias("PRVCY_DSCLSUR_ACCTG_RQST_CD_SK"),
    F.col("PTST_CUR_PZCD_SRSN").alias("PRVCY_DSCLS_ACCT_STS_RSN_CD_SK"),
    F.when(F.col("EntityLkup_ENEN_CKE").isNotNull(), "N").otherwise("F").alias("PRVCY_MBR_SRC_CD_SK"),
    F.col("PMDR_CREATE_DTM").alias("CRT_DT_SK"),
    F.col("PMDR_DSC_FROM_DT").alias("FROM_DT_SK"),
    F.col("PMDR_RECEIVED_DT").alias("RCVD_DT_SK"),
    F.col("PMDR_REQUEST_DT").alias("RQST_DT_SK"),
    F.col("PTST_CURR_STS_DTM").alias("STTUS_DT_SK"),
    F.col("PMDR_DSC_TO_DT").alias("TO_DT_SK"),
    F.col("PMDR_DESC").alias("RQST_DESC"),
    F.col("PTST_CURR_STS").alias("STTUS_CD_TX"),
    F.col("PMDR_LAST_UPD_DTM").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("PMDR_LAST_USUS_ID").alias("SRC_SYS_LAST_UPDT_USER_SK"),
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_hf_prvcy_dsclsur_acctg_read = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        """SELECT
SRC_SYS_CD,
PRVCY_MBR_UNIQ_KEY,
RQST_NO,
CRT_RUN_CYC_EXCTN_SK,
PRVCY_DSCLSUR_ACCTG_SK
FROM IDS.dummy_hf_prvcy_dsclsur_acctg
"""
    )
    .load()
)

df_PrimaryKey = (
    df_BusinessRules.alias("Transform")
    .join(
        df_hf_prvcy_dsclsur_acctg_read.alias("lkup"),
        [
            F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
            F.col("Transform.PRVCY_MBR_UNIQ_KEY") == F.col("lkup.PRVCY_MBR_UNIQ_KEY"),
            F.col("Transform.RQST_NO") == F.col("lkup.RQST_NO"),
        ],
        "left",
    )
    .select(
        F.col("Transform.*"),
        F.col("lkup.CRT_RUN_CYC_EXCTN_SK").alias("lkup_CRT_RUN_CYC_EXCTN_SK"),
        F.col("lkup.PRVCY_DSCLSUR_ACCTG_SK").alias("lkup_PRVCY_DSCLSUR_ACCTG_SK"),
    )
)

df_enriched = df_PrimaryKey.withColumn(
    "NewCurrRunCycExtcnSk",
    F.when(F.col("lkup_PRVCY_DSCLSUR_ACCTG_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup_CRT_RUN_CYC_EXCTN_SK")),
).withColumn(
    "Sk",
    F.when(F.col("lkup_PRVCY_DSCLSUR_ACCTG_SK").isNull(), F.lit(None)).otherwise(F.col("lkup_PRVCY_DSCLSUR_ACCTG_SK")),
)

df_enriched = SurrogateKeyGen(df_enriched, <DB sequence name>, "Sk", <schema>, <secret_name>)

df_key = df_enriched.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("Sk").alias("PRVCY_DSCLSUR_ACCTG_SK"),
    F.col("PRVCY_MBR_UNIQ_KEY"),
    F.col("RQST_NO"),
    F.when(
        F.col("lkup_PRVCY_DSCLSUR_ACCTG_SK").isNull(),
        F.lit(CurrRunCycle),
    ).otherwise(F.col("NewCurrRunCycExtcnSk")).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("PRVCY_MBR_SRC_CD_SK") == "F", F.col("MBR_SK")).otherwise(F.lit("NA")).alias("MBR_SK"),
    F.when(F.col("PRVCY_MBR_SRC_CD_SK") == "N", F.col("PRVCY_EXTRNL_MBR_SK")).otherwise(F.lit("NA")).alias("PRVCY_EXTRNL_MBR_SK"),
    F.col("PRVCY_DSCLSUR_ACCTG_RQST_CD_SK"),
    F.col("PRVCY_DSCLS_ACCT_STS_RSN_CD_SK"),
    F.col("PRVCY_MBR_SRC_CD_SK"),
    F.rpad(F.col("CRT_DT_SK"), 10, " ").alias("CRT_DT_SK"),
    F.rpad(F.col("FROM_DT_SK"), 10, " ").alias("FROM_DT_SK"),
    F.rpad(F.col("RCVD_DT_SK"), 10, " ").alias("RCVD_DT_SK"),
    F.rpad(F.col("RQST_DT_SK"), 10, " ").alias("RQST_DT_SK"),
    F.rpad(F.col("STTUS_DT_SK"), 10, " ").alias("STTUS_DT_SK"),
    F.rpad(F.col("TO_DT_SK"), 10, " ").alias("TO_DT_SK"),
    F.col("RQST_DESC"),
    F.col("STTUS_CD_TX"),
    F.rpad(F.col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " ").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.rpad(F.col("SRC_SYS_LAST_UPDT_USER_SK"), 10, " ").alias("SRC_SYS_LAST_UPDT_USER_SK"),
)

write_files(
    df_key,
    f"{adls_path}/key/FctsPrvcyDsclsurAcctgExtr.PrvcyDsclsurAcctg.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_updt = df_enriched.filter(
    F.col("lkup_PRVCY_DSCLSUR_ACCTG_SK").isNull()
).select(
    F.col("SRC_SYS_CD"),
    F.col("PRVCY_MBR_UNIQ_KEY"),
    F.col("RQST_NO"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Sk").alias("PRVCY_DSCLSUR_ACCTG_SK"),
)

spark.sql("DROP TABLE IF EXISTS STAGING.FctsPrvcyDsclsurAcctgExtr_hf_prvcy_dsclsur_acctg_updt_temp")

(
    df_updt.write.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", "STAGING.FctsPrvcyDsclsurAcctgExtr_hf_prvcy_dsclsur_acctg_updt_temp")
    .mode("overwrite")
    .save()
)

merge_sql = """
MERGE IDS.dummy_hf_prvcy_dsclsur_acctg AS T
USING STAGING.FctsPrvcyDsclsurAcctgExtr_hf_prvcy_dsclsur_acctg_updt_temp AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.PRVCY_MBR_UNIQ_KEY = S.PRVCY_MBR_UNIQ_KEY
    AND T.RQST_NO = S.RQST_NO
WHEN MATCHED THEN UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.PRVCY_DSCLSUR_ACCTG_SK = S.PRVCY_DSCLSUR_ACCTG_SK
WHEN NOT MATCHED THEN INSERT 
(
    SRC_SYS_CD,
    PRVCY_MBR_UNIQ_KEY,
    RQST_NO,
    CRT_RUN_CYC_EXCTN_SK,
    PRVCY_DSCLSUR_ACCTG_SK
)
VALUES
(
    S.SRC_SYS_CD,
    S.PRVCY_MBR_UNIQ_KEY,
    S.RQST_NO,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.PRVCY_DSCLSUR_ACCTG_SK
);
"""

execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)

params = {
  "EnvProjectPath": f"dap/<...>/Jobs/Privacy/PrvcyDsclsurAcctg",
  "File_Path": "key",
  "File_Name": f"FctsPrvcyDsclsurAcctgExtr.PrvcyDsclsurAcctg.dat.{RunID}"
}
dbutils.notebook.run("../../../../sequencer_routines/Move_File", timeout_seconds=3600, arguments=params)