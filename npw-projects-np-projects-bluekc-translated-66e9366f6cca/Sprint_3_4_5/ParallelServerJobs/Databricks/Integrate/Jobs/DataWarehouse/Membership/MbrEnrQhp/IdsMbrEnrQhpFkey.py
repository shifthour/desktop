# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By: IdsIdsMbrEnrQhpExtCntl
# MAGIC 
# MAGIC Job Name : IdsMbrEnrQhpFkey_EE
# MAGIC 
# MAGIC Process Description: Perform Foreign Key Lookups to populate PROD_QHP table.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Jaideep Mankala      2016-07-22          5605                             Original Programming                                                                             IntegrateDev2           Kalyan Neelam             2016-08-10
# MAGIC Jaideep Mankala      2016-09-21          5605                             Modifed lookup logic for the data coming from db2_K_Qhp_Lkp          IntegrateDev2           Kalyan Neelam             2016-09-21

# MAGIC FKEY failures are written into this flat file.
# MAGIC This is a load ready file that will go into Load job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

DSJobName = "IdsMbrEnrQhpFkey"

# Retrieve parameter values
SrcSysCd = get_widget_value('SrcSysCd', '')
RunID = get_widget_value('RunID', '')
RunDateTime = get_widget_value('RunDateTime', '')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName', '')
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')

# Read seq_MBR_ENR_QHP (PxSequentialFile)
schema_seq_MBR_ENR_QHP = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), nullable=False),
    StructField("FIRST_RECYC_TS", TimestampType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("MBR_ENR_CLS_PLN_PROD_CAT_CD", StringType(), nullable=False),
    StructField("MBR_ENR_EFF_DT_SK", StringType(), nullable=False),
    StructField("MBR_ENR_QHP_EFF_DT_SK", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("MBR_ENR_QHP_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK", IntegerType(), nullable=False),
    StructField("MBR_ENR_TERM_DT_SK", StringType(), nullable=False),
    StructField("QHP_SK", IntegerType(), nullable=False),
    StructField("MBR_ENR_QHP_TERM_DT_SK", StringType(), nullable=False),
    StructField("MBR_ENR_SK", IntegerType(), nullable=False),
    StructField("QHP_ID", StringType(), nullable=False),
    StructField("QHP_EFF_DT_SK", StringType(), nullable=False),
    StructField("QHP_TERM_DT_SK", StringType(), nullable=False)
])

df_seq_MBR_ENR_QHP = (
    spark.read
    .option("sep", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("nullValue", None)
    .schema(schema_seq_MBR_ENR_QHP)
    .csv(f"{adls_path}/key/MBR_ENR_QHP.{SrcSysCd}.pkey.{RunID}.dat")
)

# db2_Clndr_Mbr_Eff_Dt_Lkp (DB2ConnectorPX)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_db2_Clndr_Mbr_Eff_Dt_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT DISTINCT CLNDR_DT_SK FROM {IDSOwner}.CLNDR_DT WHERE CLNDR_DT_SK NOT IN ('INVALID','NA','NULL','UNK')")
    .load()
)

# db2_K_Qhp_Lkp (DB2ConnectorPX)
df_db2_K_Qhp_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT DISTINCT QHP_ID, EFF_DT_SK, QHP_SK FROM {IDSOwner}.K_QHP")
    .load()
)

# db2_Clndr_Dt_Qhp_Eff_Dt_Lkp (DB2ConnectorPX)
df_db2_Clndr_Dt_Qhp_Eff_Dt_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT DISTINCT CLNDR_DT_SK FROM {IDSOwner}.CLNDR_DT WHERE CLNDR_DT_SK NOT IN ('INVALID','NA','NULL','UNK')")
    .load()
)

# Clndr_Dt_QHP_Term_Dt_Lkp (DB2ConnectorPX)
df_Clndr_Dt_QHP_Term_Dt_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT DISTINCT CLNDR_DT_SK FROM {IDSOwner}.CLNDR_DT WHERE CLNDR_DT_SK NOT IN ('INVALID','NA','NULL','UNK')")
    .load()
)

# lkp_Code_SKs (PxLookup)
df_lkp_Code_SKs = (
    df_seq_MBR_ENR_QHP.alias("Lnk_IdsMbrEnrQhpFkey_LKp1")
    .join(
        df_db2_Clndr_Dt_Qhp_Eff_Dt_Lkp.alias("qhp_eff_Dt"),
        F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.MBR_ENR_QHP_EFF_DT_SK") == F.col("qhp_eff_Dt.CLNDR_DT_SK"),
        "left"
    )
    .join(
        df_db2_Clndr_Mbr_Eff_Dt_Lkp.alias("Mbr_Eff_Dt"),
        F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.MBR_ENR_EFF_DT_SK") == F.col("Mbr_Eff_Dt.CLNDR_DT_SK"),
        "left"
    )
    .join(
        df_db2_K_Qhp_Lkp.alias("Ref_K_Qhp_In"),
        [
            F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.QHP_ID") == F.col("Ref_K_Qhp_In.QHP_ID"),
            F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.QHP_EFF_DT_SK") == F.col("Ref_K_Qhp_In.EFF_DT_SK")
        ],
        "left"
    )
    .join(
        df_Clndr_Dt_QHP_Term_Dt_Lkp.alias("qhp_Term_Dt"),
        F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.MBR_ENR_QHP_TERM_DT_SK") == F.col("qhp_Term_Dt.CLNDR_DT_SK"),
        "left"
    )
)

df_lkp_Code_SKs_selected = df_lkp_Code_SKs.select(
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.MBR_ENR_CLS_PLN_PROD_CAT_CD").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
    F.col("Mbr_Eff_Dt.CLNDR_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("qhp_eff_Dt.CLNDR_DT_SK").alias("MBR_ENR_QHP_EFF_DT_SK"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.LAST_RUN_CYC_EXCTN_SK").alias("LAST_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.MBR_ENR_QHP_SK").alias("MBR_ENR_QHP_SK"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("qhp_Term_Dt.CLNDR_DT_SK").alias("MBR_ENR_QHP_TERM_DT_SK"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.MBR_ENR_SK").alias("MBR_ENR_SK"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.QHP_ID").alias("QHP_ID"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK_Lkup"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.MBR_ENR_QHP_EFF_DT_SK").alias("MBR_ENR_QHP_EFF_DT_SK_Lkup"),
    F.col("Ref_K_Qhp_In.QHP_SK").alias("QHP_SK"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp1.MBR_ENR_QHP_TERM_DT_SK").alias("MBR_ENR_QHP_TERM_DT_SK_Lkup")
)

# xfm_CheckLkpResults (CTransformerStage)

# Create stage variable columns from the logic:
df_xfm_sv = df_lkp_Code_SKs_selected.withColumn(
    "svClsPlnProdCatCdLkpFailCheck",
    F.when(
        (F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK").isNull()) & (F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD") != F.lit("NA")),
        F.lit("Y")
    ).otherwise(F.lit("N"))
).withColumn(
    "svSrcSysCdFailCheck",
    F.when(
        (F.col("SRC_SYS_CD_SK").isNull()) & (F.col("SRC_SYS_CD") != F.lit("NA")),
        F.lit("Y")
    ).otherwise(F.lit("N"))
).withColumn(
    "svQhpTermDtLkpFailCheck",
    F.when(
        (F.col("MBR_ENR_QHP_TERM_DT_SK").isNull()) & (F.col("MBR_ENR_QHP_EFF_DT_SK_Lkup") != F.lit("NA")),
        F.lit("Y")
    ).otherwise(F.lit("N"))
).withColumn(
    "svQhpEffDtLkpFailCheck",
    F.when(
        (F.col("MBR_ENR_QHP_EFF_DT_SK").isNull()) & (F.col("MBR_ENR_QHP_EFF_DT_SK_Lkup") != F.lit("NA")),
        F.lit("Y")
    ).otherwise(F.lit("N"))
).withColumn(
    "svMbrEffDtLkpFailCheck",
    F.when(
        (F.col("MBR_ENR_EFF_DT_SK").isNull()) & (F.col("MBR_ENR_EFF_DT_SK_Lkup") != F.lit("NA")),
        F.lit("Y")
    ).otherwise(F.lit("N"))
).withColumn(
    "svKQhpLkpFailCheck",
    F.when(
        (F.col("QHP_SK").isNull()) & (F.col("QHP_ID") != F.lit("NA")),
        F.lit("Y")
    ).otherwise(F.lit("N"))
)

# We also need row_number for the UNK / NA outputs.
w = Window.orderBy(F.monotonically_increasing_id())
df_xfm = df_xfm_sv.withColumn("rownum", F.row_number().over(w))

# Lnk_MbrEnrQhpFkey_Main => all rows
df_Lnk_MbrEnrQhpFkey_Main = df_xfm.select(
    F.col("MBR_ENR_QHP_SK").alias("MBR_ENR_QHP_SK"),
    F.when(F.col("svSrcSysCdFailCheck") == "Y", F.lit(0)).otherwise(F.col("SRC_SYS_CD_SK")).alias("SRC_SYS_CD_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.when(F.col("svClsPlnProdCatCdLkpFailCheck") == "Y", F.lit(0)).otherwise(F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK")).alias("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK"),
    F.when(F.col("svMbrEffDtLkpFailCheck") == "Y", F.lit("1753-01-01")).otherwise(F.col("MBR_ENR_EFF_DT_SK")).alias("MBR_ENR_EFF_DT_SK"),
    F.when(F.col("svQhpEffDtLkpFailCheck") == "Y", F.lit("1753-01-01")).otherwise(F.col("MBR_ENR_QHP_EFF_DT_SK")).alias("MBR_ENR_QHP_EFF_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_ENR_SK").alias("MBR_ENR_SK"),
    F.when(F.col("svKQhpLkpFailCheck") == "Y", F.lit(0)).otherwise(F.col("QHP_SK")).alias("QHP_SK"),
    F.when(F.col("svQhpTermDtLkpFailCheck") == "Y", F.lit("1753-01-01")).otherwise(F.col("MBR_ENR_QHP_TERM_DT_SK")).alias("MBR_ENR_QHP_TERM_DT_SK")
)

# Lnk_MbrEnrQhpUNK => constraint: rownum=1
df_Lnk_MbrEnrQhpUNK = df_xfm.filter(F.col("rownum") == 1).select(
    F.lit(0).alias("MBR_ENR_QHP_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(0).alias("MBR_UNIQ_KEY"),
    F.lit(0).alias("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK"),
    F.lit("1753-01-01").alias("MBR_ENR_EFF_DT_SK"),
    F.lit("1753-01-01").alias("MBR_ENR_QHP_EFF_DT_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("MBR_ENR_SK"),
    F.lit(0).alias("QHP_SK"),
    F.lit("1753-01-01").alias("MBR_ENR_QHP_TERM_DT_SK")
)

# Lnk_MbrEnrQhp_NA => constraint: rownum=1
df_Lnk_MbrEnrQhp_NA = df_xfm.filter(F.col("rownum") == 1).select(
    F.lit(1).alias("MBR_ENR_QHP_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(1).alias("MBR_UNIQ_KEY"),
    F.lit(1).alias("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK"),
    F.lit("1753-01-01").alias("MBR_ENR_EFF_DT_SK"),
    F.lit("1753-01-01").alias("MBR_ENR_QHP_EFF_DT_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("MBR_ENR_SK"),
    F.lit(1).alias("QHP_SK"),
    F.lit("1753-01-01").alias("MBR_ENR_QHP_TERM_DT_SK")
)

# Lnk_K_MbrEffDt_Fail => filter svMbrEffDtLkpFailCheck='Y'
df_Lnk_K_MbrEffDt_Fail = df_xfm.filter(F.col("svMbrEffDtLkpFailCheck") == "Y").select(
    F.col("MBR_ENR_QHP_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("MBR_ENR_EFF_DT_SK_Lkup")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# Lnk_QhpEffEffDtLkp_Fail => filter svQhpEffDtLkpFailCheck='Y'
df_Lnk_QhpEffEffDtLkp_Fail = df_xfm.filter(F.col("svQhpEffDtLkpFailCheck") == "Y").select(
    F.col("MBR_ENR_QHP_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("MBR_ENR_QHP_EFF_DT_SK_Lkup")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# Lnk_QhpTermDtLkp_Fail => filter svQhpTermDtLkpFailCheck='Y'
df_Lnk_QhpTermDtLkp_Fail = df_xfm.filter(F.col("svQhpTermDtLkpFailCheck") == "Y").select(
    F.col("MBR_ENR_QHP_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("MBR_ENR_QHP_TERM_DT_SK_Lkup")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# Lnk_K_SrcSys_Fail => filter svSrcSysCdFailCheck='Y'
df_Lnk_K_SrcSys_Fail = df_xfm.filter(F.col("svSrcSysCdFailCheck") == "Y").select(
    F.col("MBR_ENR_QHP_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("SRC_SYS_CD").alias("PHYSCL_FILE_NM"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("SRC_SYS_CD")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# Lnk_K_Qhp_Fail => filter svKQhpLkpFailCheck='Y'
df_Lnk_K_Qhp_Fail = df_xfm.filter(F.col("svKQhpLkpFailCheck") == "Y").select(
    F.col("MBR_ENR_QHP_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_QHP").alias("PHYSCL_FILE_NM"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("QHP_ID"), F.lit(";"), F.col("MBR_ENR_QHP_EFF_DT_SK_Lkup")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# Lnk_ClsPlnProdCatCd_Fail => filter svClsPlnProdCatCdLkpFailCheck='Y'
df_Lnk_ClsPlnProdCatCd_Fail = df_xfm.filter(F.col("svClsPlnProdCatCdLkpFailCheck") == "Y").select(
    F.col("MBR_ENR_QHP_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("CLS_PLN_PRD").alias("PHYSCL_FILE_NM"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.lit("FACETS DBO"), F.lit(";"), F.lit("IDS"), F.lit(";"),
             F.lit("CLASS PLAN PRODUCT CATEGORY"), F.lit(";"), F.lit("CLASS PLAN PRODUCT CATEGORY"), F.lit(";"),
             F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# fnl_NA_UNK_Streams (PxFunnel) => union Main, UNK, NA
commonCols_funnel1 = [
    "MBR_ENR_QHP_SK",
    "SRC_SYS_CD_SK",
    "MBR_UNIQ_KEY",
    "MBR_ENR_CLS_PLN_PROD_CAT_CD_SK",
    "MBR_ENR_EFF_DT_SK",
    "MBR_ENR_QHP_EFF_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_ENR_SK",
    "QHP_SK",
    "MBR_ENR_QHP_TERM_DT_SK"
]

df_fnl_NA_UNK_Streams = (
    df_Lnk_MbrEnrQhpFkey_Main.select(commonCols_funnel1)
    .unionByName(df_Lnk_MbrEnrQhpUNK.select(commonCols_funnel1))
    .unionByName(df_Lnk_MbrEnrQhp_NA.select(commonCols_funnel1))
)

# seq_MBR_ENR_QHP_FKEY (PxSequentialFile) => final output from funnel
# Apply rpad for char(10) columns. The schema indicates MBR_ENR_EFF_DT_SK, MBR_ENR_QHP_EFF_DT_SK, MBR_ENR_QHP_TERM_DT_SK are char(10).
df_seq_MBR_ENR_QHP_FKEY = (
    df_fnl_NA_UNK_Streams
    .withColumn("MBR_ENR_EFF_DT_SK", F.rpad(F.col("MBR_ENR_EFF_DT_SK"), 10, " "))
    .withColumn("MBR_ENR_QHP_EFF_DT_SK", F.rpad(F.col("MBR_ENR_QHP_EFF_DT_SK"), 10, " "))
    .withColumn("MBR_ENR_QHP_TERM_DT_SK", F.rpad(F.col("MBR_ENR_QHP_TERM_DT_SK"), 10, " "))
)

# Write to file
write_files(
    df_seq_MBR_ENR_QHP_FKEY.select(
        "MBR_ENR_QHP_SK",
        "SRC_SYS_CD_SK",
        "MBR_UNIQ_KEY",
        "MBR_ENR_CLS_PLN_PROD_CAT_CD_SK",
        "MBR_ENR_EFF_DT_SK",
        "MBR_ENR_QHP_EFF_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MBR_ENR_SK",
        "QHP_SK",
        "MBR_ENR_QHP_TERM_DT_SK"
    ),
    f"{adls_path}/load/MBR_ENR_QHP.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)

# FnlFkeyFailures (PxFunnel)
commonCols_funnel2 = [
    "PRI_SK",
    "PRI_NAT_KEY_STRING",
    "SRC_SYS_CD_SK",
    "JOB_NM",
    "ERROR_TYP",
    "PHYSCL_FILE_NM",
    "FRGN_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "JOB_EXCTN_SK"
]

df_fnlFkeyFailures = (
    df_Lnk_K_MbrEffDt_Fail.select(commonCols_funnel2)
    .unionByName(df_Lnk_QhpEffEffDtLkp_Fail.select(commonCols_funnel2))
    .unionByName(df_Lnk_QhpTermDtLkp_Fail.select(commonCols_funnel2))
    .unionByName(df_Lnk_K_SrcSys_Fail.select(commonCols_funnel2))
    .unionByName(df_Lnk_K_Qhp_Fail.select(commonCols_funnel2))
    .unionByName(df_Lnk_ClsPlnProdCatCd_Fail.select(commonCols_funnel2))
)

# seq_FkeyFailedFile (PxSequentialFile)
# For char or varchar, apply rpad.  We do not have explicit lengths for these except we know they are string. Assume 100 for them:
df_seq_FkeyFailedFile = (
    df_fnlFkeyFailures
    .withColumn("PRI_NAT_KEY_STRING", F.rpad(F.col("PRI_NAT_KEY_STRING"), 100, " "))
    .withColumn("JOB_NM", F.rpad(F.col("JOB_NM"), 100, " "))
    .withColumn("ERROR_TYP", F.rpad(F.col("ERROR_TYP"), 100, " "))
    .withColumn("PHYSCL_FILE_NM", F.rpad(F.col("PHYSCL_FILE_NM"), 100, " "))
    .withColumn("FRGN_NAT_KEY_STRING", F.rpad(F.col("FRGN_NAT_KEY_STRING"), 100, " "))
)

write_files(
    df_seq_FkeyFailedFile.select(
        "PRI_SK",
        "PRI_NAT_KEY_STRING",
        "SRC_SYS_CD_SK",
        "JOB_NM",
        "ERROR_TYP",
        "PHYSCL_FILE_NM",
        "FRGN_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "JOB_EXCTN_SK"
    ),
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{DSJobName}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)