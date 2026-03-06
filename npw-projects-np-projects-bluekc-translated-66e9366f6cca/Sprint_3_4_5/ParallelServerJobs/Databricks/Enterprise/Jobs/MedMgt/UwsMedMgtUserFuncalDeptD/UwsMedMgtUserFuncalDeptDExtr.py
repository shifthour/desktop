# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 05/12/09 14:25:07 Batch  15108_51913 PROMOTE bckcetl edw10 dsadm bls for rt
# MAGIC ^1_1 05/12/09 14:16:44 Batch  15108_51406 INIT bckcett:31540 testEDW dsadm BLS FOR RT
# MAGIC ^1_1 05/01/09 08:54:06 Batch  15097_32051 PROMOTE bckcett testEDW u03651 steph for Ralph
# MAGIC ^1_1 05/01/09 08:51:22 Batch  15097_31885 INIT bckcett devlEDW u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     UwsMedMgtUserFuncalDeptDExtr
# MAGIC 
# MAGIC DESCRIPTION:  Extracts from UWS - MED_MGT_USER_FUNCAL_DEPT_D & IDS - APP_USER_D tables UM_ACTVTY to flatfile MED_MGT_USER_FUNCAL_DEPT_D.dat
# MAGIC       
# MAGIC OUTPUTS: 
# MAGIC                     Flatfile: MED_MGT_USER_FUNCAL_DEPT_D.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 3/31/2009          BICC/3808                  Original Programming.                                                                               devlEDW                 Steph Goddard             04/03/2009

# MAGIC Extracting from UWS table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


# Parameters (including database-owner parameters and their corresponding secret names)
CurrRunCycleDate = get_widget_value("CurrRunCycleDate","2009-04-01")
CurrRunCycle = get_widget_value("CurrRunCycle","100")
UWSOwner = get_widget_value("UWSOwner","")
IDSOwner = get_widget_value("IDSOwner","")
uws_secret_name = get_widget_value("uws_secret_name","")
ids_secret_name = get_widget_value("ids_secret_name","")
edw_secret_name = get_widget_value("edw_secret_name","")

# --------------------------------------------------------------------------------
# Scenario B Hashed File: hf_medmgt_usr_fncl_dept_d_lkup / hf_medmgt_usr_fncl_dept_d_updt
# Replace with a dummy table in EDW schema for read and write
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
df_hf_medmgt_usr_fncl_dept_d_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option(
        "query",
        "SELECT SRC_SYS_CD, MED_MGT_USER_ID, CRT_RUN_CYC_EXCTN_SK, MED_MGT_USER_FUNCAL_DEPT_SK FROM dummy_hf_medmgt_usr_fncl_dept"
    )
    .load()
)

# --------------------------------------------------------------------------------
# Stage: UWS (CODBCStage). Original query included "WHERE (MED_MGT_USER_ID = ?)". 
# Per instructions, remove the parameter marker and read entire table.
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)
df_UWS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option(
        "query",
        f"SELECT MED_MGT_USER_ID, ACTV_EMPL_IN, FUNCAL_DEPT_ID, MED_MGT_USER_DESC, USER_ID, LAST_UPDT_DT_SK FROM {UWSOwner}.MED_MGT_USER_FUNCAL_DEPT"
    )
    .load()
)

# --------------------------------------------------------------------------------
# Stage: Strip (CTransformerStage)
df_Strip = (
    df_UWS
    .withColumn(
        "MED_MGT_USER_ID",
        trim(strip_field(F.col("MED_MGT_USER_ID").substr(F.lit(1), F.lit(10))))
    )
    .withColumn("ACTV_EMPL_IN", F.col("ACTV_EMPL_IN"))
    .withColumn("FUNCAL_DEPT_ID", F.col("FUNCAL_DEPT_ID"))
    .withColumn("MED_MGT_USER_DESC", F.col("MED_MGT_USER_DESC"))
    .withColumn("USER_ID", F.col("USER_ID"))
    .withColumn("LAST_UPDT_DT_SK", F.col("LAST_UPDT_DT_SK"))
)

# --------------------------------------------------------------------------------
# Stage: IDS (DB2Connector)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT USER_ID, USER_SK FROM {IDSOwner}.APP_USER")
    .load()
)

# --------------------------------------------------------------------------------
# Stage: Logic (CTransformerStage)
df_logic = df_IDS.withColumn("USER_ID", trim(strip_field(F.col("USER_ID"))))

# --------------------------------------------------------------------------------
# Scenario A Hashed File: hf_uws_app_user_lkup
# Remove the hashed file stage and deduplicate on key column (USER_ID).
df_appusr = dedup_sort(df_logic, ["USER_ID"], [])

# --------------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage)
# 1) Simulate the presence of "Extract.ACTVTY_USER_SK" by creating a column to fulfill the join condition.
# 2) Left join with df_appusr (formerly hashed file) on user ID and the placeholder activity user SK.
# 3) Left join with df_hf_medmgt_usr_fncl_dept_d_lkup (dummy table for hashed file scenario B).

df_br_0 = df_Strip.withColumn("ACTVTY_USER_SK", F.lit(None).cast(IntegerType())).alias("lnk_uws")

df_br_1 = df_br_0.join(
    df_appusr.alias("AppUsr"),
    (F.col("lnk_uws.MED_MGT_USER_ID") == F.col("AppUsr.USER_ID"))
    & (F.col("lnk_uws.ACTVTY_USER_SK") == F.col("AppUsr.USER_SK")),
    how="left"
)

df_br_2 = df_br_1.join(
    df_hf_medmgt_usr_fncl_dept_d_lkup.alias("lkup"),
    (
        (F.lit("FACETS") == F.col("lkup.SRC_SYS_CD"))
        & (F.col("lnk_uws.MED_MGT_USER_ID").substr(F.lit(1), F.lit(10)) == F.col("lkup.MED_MGT_USER_ID"))
    ),
    how="left"
)

df_enriched = df_br_2

# Stage Variables:
# SK = if IsNull(lkup.MED_MGT_USER_FUNCAL_DEPT_SK) then KeyMgtGetNextValueConcurrent("EDW_SK") else lkup.MED_MGT_USER_FUNCAL_DEPT_SK
# Replace KeyMgtGetNextValueConcurrent with SurrogateKeyGen after setting a placeholder column "SK".

df_enriched = df_enriched.withColumn(
    "SK",
    F.col("lkup.MED_MGT_USER_FUNCAL_DEPT_SK")
)

# SurrogateKeyGen for SK
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

# NewCrtRunCycExtcnSk = if IsNull(lkup.MED_MGT_USER_FUNCAL_DEPT_SK) then CurrRunCycle else lkup.CRT_RUN_CYC_EXCTN_SK
df_enriched = df_enriched.withColumn(
    "NewCrtRunCycExtcnSk",
    F.when(F.col("lkup.MED_MGT_USER_FUNCAL_DEPT_SK").isNull(), F.lit(CurrRunCycle).cast(IntegerType()))
     .otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)

# --------------------------------------------------------------------------------
# Outputs from BusinessRules:
#   1) Load
#   2) updt
#   3) UNK
#   4) NA

# 1) Load link (no constraint). Columns as specified:
df_load = (
    df_enriched
    .withColumn("MED_MGT_USER_FUNCAL_DEPT_SK", F.col("SK"))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn("MED_MGT_USER_ID", F.col("lnk_uws.MED_MGT_USER_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrRunCycleDate))
    .withColumn(
        "MED_MGT_USER_SK",
        F.when(
            F.col("AppUsr.USER_SK").isNull() | (F.length(F.col("AppUsr.USER_SK")) == 0),
            F.lit(0)
        ).otherwise(F.col("AppUsr.USER_SK"))
    )
    .withColumn(
        "MED_MGT_ACTV_EMPL_IN",
        F.when(
            F.col("lnk_uws.ACTV_EMPL_IN").isNull() | (F.length(F.col("lnk_uws.ACTV_EMPL_IN")) == 0),
            F.lit("X")
        ).otherwise(F.col("lnk_uws.ACTV_EMPL_IN"))
    )
    .withColumn(
        "MED_MGT_FUNCAL_DEPT_ID",
        F.when(
            F.col("lnk_uws.FUNCAL_DEPT_ID").isNull() | (F.length(F.col("lnk_uws.FUNCAL_DEPT_ID")) == 0),
            F.lit("NA")
        ).otherwise(F.col("lnk_uws.FUNCAL_DEPT_ID"))
    )
    .withColumn(
        "MED_MGT_USER_DESC",
        F.when(
            F.col("lnk_uws.MED_MGT_USER_DESC").isNull() | (F.length(F.col("lnk_uws.MED_MGT_USER_DESC")) == 0),
            F.lit(None)
        ).otherwise(F.col("lnk_uws.MED_MGT_USER_DESC"))
    )
    .withColumn(
        "LAST_UPDT_USER_ID",
        F.when(
            F.col("lnk_uws.USER_ID").isNull() | (F.length(F.col("lnk_uws.USER_ID")) == 0),
            F.lit("NA")
        ).otherwise(F.col("lnk_uws.USER_ID"))
    )
    .withColumn(
        "LAST_UPDT_DT_SK",
        F.when(
            F.col("lnk_uws.LAST_UPDT_DT_SK").isNull() | (F.length(F.col("lnk_uws.LAST_UPDT_DT_SK")) == 0),
            F.lit("1753-01-01")
        ).otherwise(F.col("lnk_uws.LAST_UPDT_DT_SK"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("NewCrtRunCycExtcnSk"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle).cast(IntegerType()))
)

# 2) updt link (constraint = IsNull(lkup.MED_MGT_USER_FUNCAL_DEPT_SK) = @TRUE)
df_updt = (
    df_enriched
    .filter(F.col("lkup.MED_MGT_USER_FUNCAL_DEPT_SK").isNull())
    .select(
        F.lit("FACETS").alias("SRC_SYS_CD"),
        F.col("lnk_uws.MED_MGT_USER_ID").alias("MED_MGT_USER_ID"),
        F.lit(CurrRunCycle).cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("SK").alias("MED_MGT_USER_FUNCAL_DEPT_SK")
    )
)

# 3) UNK link (constraint = @INROWNUM = 1)
# 4) NA link (constraint = @INROWNUM = 1)
#   Both constraints are the same, so we filter rownum=1 from df_enriched.

w = Window.orderBy(F.lit(1))
df_with_rownum = df_enriched.withColumn("rownum", F.row_number().over(w))

df_unk = (
    df_with_rownum
    .filter(F.col("rownum") == 1)
    .select(
        F.lit(0).alias("MED_MGT_USER_FUNCAL_DEPT_SK"),
        F.lit("UNK").alias("SRC_SYS_CD"),
        F.lit("UNK").alias("MED_MGT_USER_ID"),
        F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(0).alias("MED_MGT_USER_SK"),
        F.lit("U").alias("MED_MGT_ACTV_EMPL_IN"),
        F.lit("UNK").alias("MED_MGT_FUNCAL_DEPT_ID"),
        F.lit("UNK").alias("MED_MGT_USER_DESC"),
        F.lit("UNK").alias("LAST_UPDT_USER_ID"),
        F.lit("1753-01-01").alias("LAST_UPDT_DT_SK"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

df_na = (
    df_with_rownum
    .filter(F.col("rownum") == 1)
    .select(
        F.lit(1).alias("MED_MGT_USER_FUNCAL_DEPT_SK"),
        F.lit("NA").alias("SRC_SYS_CD"),
        F.lit("NA").alias("MED_MGT_USER_ID"),
        F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(1).alias("MED_MGT_USER_SK"),
        F.lit("X").alias("MED_MGT_ACTV_EMPL_IN"),
        F.lit("NA").alias("MED_MGT_FUNCAL_DEPT_ID"),
        F.lit("NA").alias("MED_MGT_USER_DESC"),
        F.lit("NA").alias("LAST_UPDT_USER_ID"),
        F.lit("1753-01-01").alias("LAST_UPDT_DT_SK"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

# Collect links (CCollector): "Load", "UNK", "NA"
# The Link_Collector output columns (in the specified order) are:
#   MED_MGT_USER_FUNCAL_DEPT_SK
#   SRC_SYS_CD
#   MED_MGT_USER_ID (char(10))
#   CRT_RUN_CYC_EXCTN_DT_SK (char(10))
#   LAST_UPDT_RUN_CYC_EXCTN_DT_SK (char(10))
#   MED_MGT_USER_SK
#   MED_MGT_ACTV_EMPL_IN (char(1))
#   MED_MGT_FUNCAL_DEPT_ID
#   MED_MGT_USER_DESC
#   LAST_UPDT_USER_ID
#   LAST_UPDT_DT_SK (char(10))
#   CRT_RUN_CYC_EXCTN_SK
#   LAST_UPDT_RUN_CYC_EXCTN_SK

common_cols = [
    "MED_MGT_USER_FUNCAL_DEPT_SK",
    "SRC_SYS_CD",
    "MED_MGT_USER_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "MED_MGT_USER_SK",
    "MED_MGT_ACTV_EMPL_IN",
    "MED_MGT_FUNCAL_DEPT_ID",
    "MED_MGT_USER_DESC",
    "LAST_UPDT_USER_ID",
    "LAST_UPDT_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
]

df_load_sel = (
    df_load.select(
        F.col("MED_MGT_USER_FUNCAL_DEPT_SK").alias("MED_MGT_USER_FUNCAL_DEPT_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("MED_MGT_USER_ID").alias("MED_MGT_USER_ID"),
        F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("MED_MGT_USER_SK").alias("MED_MGT_USER_SK"),
        F.col("MED_MGT_ACTV_EMPL_IN").alias("MED_MGT_ACTV_EMPL_IN"),
        F.col("MED_MGT_FUNCAL_DEPT_ID").alias("MED_MGT_FUNCAL_DEPT_ID"),
        F.col("MED_MGT_USER_DESC").alias("MED_MGT_USER_DESC"),
        F.col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID"),
        F.col("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

df_unk_sel = df_unk.select(common_cols)
df_na_sel  = df_na.select(common_cols)

df_link_collector = df_load_sel.unionByName(df_unk_sel).unionByName(df_na_sel)

# --------------------------------------------------------------------------------
# Final output (CSeqFileStage): MED_MGT_USER_FUNCAL_DEPT_D.dat
# For char columns: rpad(col, length, ' ')
df_final = (
    df_link_collector
    .withColumn("MED_MGT_USER_ID", F.rpad(F.col("MED_MGT_USER_ID"), 10, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("MED_MGT_ACTV_EMPL_IN", F.rpad(F.col("MED_MGT_ACTV_EMPL_IN"), 1, " "))
    .withColumn("LAST_UPDT_DT_SK", F.rpad(F.col("LAST_UPDT_DT_SK"), 10, " "))
)

write_files(
    df_final.select(common_cols),
    f"{adls_path}/load/MED_MGT_USER_FUNCAL_DEPT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Scenario B write (hf_medmgt_usr_fncl_dept_d_updt) => Merge into the same dummy table
# Primary keys: SRC_SYS_CD, MED_MGT_USER_ID
# Columns to merge: SRC_SYS_CD, MED_MGT_USER_ID, CRT_RUN_CYC_EXCTN_SK, MED_MGT_USER_FUNCAL_DEPT_SK
# Create staging table and then merge

# Prepare df_updt with correct column ordering
df_updt_write = df_updt.select(
    "SRC_SYS_CD",
    "MED_MGT_USER_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "MED_MGT_USER_FUNCAL_DEPT_SK"
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.UwsMedMgtUserFuncalDeptDExtr_hf_medmgt_usr_fncl_dept_d_updt_temp", jdbc_url_edw, jdbc_props_edw)

# Write df_updt_write to staging table
(
    df_updt_write
    .write
    .format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("dbtable", "STAGING.UwsMedMgtUserFuncalDeptDExtr_hf_medmgt_usr_fncl_dept_d_updt_temp")
    .mode("overwrite")
    .save()
)

# Merge into dummy_hf_medmgt_usr_fncl_dept
merge_sql = """
MERGE INTO dummy_hf_medmgt_usr_fncl_dept AS T
USING STAGING.UwsMedMgtUserFuncalDeptDExtr_hf_medmgt_usr_fncl_dept_d_updt_temp AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.MED_MGT_USER_ID = S.MED_MGT_USER_ID
WHEN MATCHED THEN
    UPDATE SET 
        T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
        T.MED_MGT_USER_FUNCAL_DEPT_SK = S.MED_MGT_USER_FUNCAL_DEPT_SK
WHEN NOT MATCHED THEN
    INSERT (
        SRC_SYS_CD,
        MED_MGT_USER_ID,
        CRT_RUN_CYC_EXCTN_SK,
        MED_MGT_USER_FUNCAL_DEPT_SK
    )
    VALUES (
        S.SRC_SYS_CD,
        S.MED_MGT_USER_ID,
        S.CRT_RUN_CYC_EXCTN_SK,
        S.MED_MGT_USER_FUNCAL_DEPT_SK
    );
"""

execute_dml(merge_sql, jdbc_url_edw, jdbc_props_edw)