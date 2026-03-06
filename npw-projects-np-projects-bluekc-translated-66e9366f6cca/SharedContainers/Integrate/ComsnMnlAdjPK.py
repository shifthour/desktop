# Databricks notebook source
# MAGIC %run ./Utility

# COMMAND ----------

# MAGIC %run ./Routine_Functions

# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

"""
Shared Container: ComsnMnlAdjPK
Description:
* VC LOGS *
^1_1 02/20/09 11:01:04 Batch  15027_39667 PROMOTE bckcetl ids20 dsadm bls for sa
^1_1 02/20/09 10:38:04 Batch  15027_38287 INIT bckcett testIDS dsadm bls for sa
^1_2 02/19/09 15:36:15 Batch  15026_56186 PROMOTE bckcett testIDS u03651 steph for Sharon - primary key containers
^1_2 02/19/09 15:33:19 Batch  15026_56002 INIT bckcett devlIDS u03651 steffy
^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


DESCRIPTION:   Shared container used for Primary Keying of Comm Manual Adj job

CALLED BY : FctsComsnMnlAdjExtr
PROCESSING:    



MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Bhoomi Dasari    2008-09-15               Initial program                                                               3567 Primary Key    devlIDS                                 Steph Goddard           09/22/2008
"""

# IDS Primary Key Container for Commission Manual Adj
# Used by
#
# FctsComsnMnlAdjExtr
# Hash file (hf_comsn_mnl_adj_allcol) cleared in calling job
# SQL joins temp table with key table to assign known keys
# Temp table is tuncated before load and runstat done after load
# Load IDS temp. table
# join primary key info with table info
# update primary key table (K_COMSN_MNL_ADJ) with new keys created today
# primary key hash file only contains current run keys and is cleared before writing
# Assign primary surrogate key

def run_ComsnMnlAdjPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the ComsnMnlAdjPK shared container.

    Parameters
    ----------
    df_AllCol : DataFrame
        Container input link "AllCol".
    df_Transform : DataFrame
        Container input link "Transform".
    params : dict
        Runtime parameters.

    Returns
    -------
    DataFrame
        Container output link "Key".
    """

    # --------------------------------------------------
    # Unpack parameters (each exactly once)
    # --------------------------------------------------
    CurrRunCycle      = params["CurrRunCycle"]
    SrcSysCd          = params["SrcSysCd"]
    IDSOwner          = params["IDSOwner"]
    ids_secret_name   = params["ids_secret_name"]
    ids_jdbc_url      = params["ids_jdbc_url"]
    ids_jdbc_props    = params["ids_jdbc_props"]
    adls_path         = params["adls_path"]
    adls_path_raw     = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]
    FilePath          = params.get("FilePath", "")

    # --------------------------------------------------
    # Stage: hf_comsn_mnl_adj_allcol  (Scenario a: intermediate hash file)
    # Replace with dedup logic on key columns
    # --------------------------------------------------
    df_AllColOut = dedup_sort(
        df_AllCol,
        partition_cols=["SRC_SYS_CD_SK", "COCA_SEQ_NO", "COCE_ID"],
        sort_cols=[]
    )

    # --------------------------------------------------
    # Stage: K_COMSN_MNL_ADJ_TEMP (write to temp table)
    # --------------------------------------------------
    df_Transform.write.mode("overwrite").format("jdbc") \
        .option("url", ids_jdbc_url) \
        .options(**ids_jdbc_props) \
        .option("dbtable", f"{IDSOwner}.K_COMSN_MNL_ADJ_TEMP") \
        .save()

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_COMSN_MNL_ADJ_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # --------------------------------------------------
    # Stage: K_COMSN_MNL_ADJ_TEMP (extract)
    # --------------------------------------------------
    extract_query = f"""
    SELECT  k.COMSN_MNL_ADJ_SK,
            w.SRC_SYS_CD_SK,
            w.AGNT_ID,
            w.SEQ_NO,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_COMSN_MNL_ADJ_TEMP w,
         {IDSOwner}.K_COMSN_MNL_ADJ k
    WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
      AND w.AGNT_ID       = k.AGNT_ID
      AND w.SEQ_NO        = k.SEQ_NO
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.AGNT_ID,
           w2.SEQ_NO,
           {CurrRunCycle}
    FROM {IDSOwner}.K_COMSN_MNL_ADJ_TEMP w2
    WHERE NOT EXISTS (
        SELECT k2.COMSN_MNL_ADJ_SK
        FROM {IDSOwner}.K_COMSN_MNL_ADJ k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.SEQ_NO        = k2.SEQ_NO
          AND w2.AGNT_ID       = k2.AGNT_ID
    )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # --------------------------------------------------
    # Stage: PrimaryKey (transformer)
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt",
                    F.when(F.col("COMSN_MNL_ADJ_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U")))
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn("svCrtRunCycExctnSk",
                    F.when(F.col("COMSN_MNL_ADJ_SK") == F.lit(-1), F.lit(CurrRunCycle))
                     .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")))
        .withColumn("svAgntId", F.trim(F.col("AGNT_ID")))
    )

    # Surrogate key generation (mandatory call pattern)
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"COMSN_MNL_ADJ_SK",<schema>,<secret_name>)

    # Align surrogate key column reference
    df_enriched = df_enriched.withColumn("svSK", F.col("COMSN_MNL_ADJ_SK"))

    # --------------------------------------------------
    # Build output links from PrimaryKey transformer
    # --------------------------------------------------
    df_updt = (
        df_enriched.select(
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            F.col("svAgntId").alias("AGNT_ID"),
            F.col("SEQ_NO"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("COMSN_MNL_ADJ_SK")
        )
    )

    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == F.lit("I")).select(
            F.col("SRC_SYS_CD_SK"),
            F.col("svAgntId").alias("AGNT_ID"),
            F.col("SEQ_NO"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("COMSN_MNL_ADJ_SK")
        )
    )

    df_Keys = (
        df_enriched.select(
            F.col("svSK").alias("COMSN_MNL_ADJ_SK"),
            F.col("SRC_SYS_CD_SK"),
            F.col("svAgntId").alias("AGNT_ID"),
            F.col("SEQ_NO"),
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # --------------------------------------------------
    # Stage: hf_comsn_mnl_adj (write parquet)
    # --------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/ComsnMnlAdjPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # --------------------------------------------------
    # Stage: K_COMSN_MNL_ADJ (write sequential file)
    # --------------------------------------------------
    seq_relative_path = "load/K_COMSN_MNL_ADJ.dat"
    if "/landing/" in seq_relative_path:
        seq_base_path = adls_path_raw
    elif "/external/" in seq_relative_path:
        seq_base_path = adls_path_publish
    else:
        seq_base_path = adls_path
    seq_file_path = f"{seq_base_path}/{seq_relative_path}".replace("//", "/")

    write_files(
        df_NewKeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # --------------------------------------------------
    # Stage: Merge (join AllColOut with Keys)
    # --------------------------------------------------
    df_Key = (
        df_AllColOut.alias("all")
        .join(
            df_Keys.alias("k"),
            (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
            (F.col("all.COCA_SEQ_NO")   == F.col("k.SEQ_NO")) &
            (F.col("all.COCE_ID")       == F.col("k.AGNT_ID")),
            "left"
        )
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN").alias("DISCARD_IN"),
            F.col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("all.ERR_CT").alias("ERR_CT"),
            F.col("all.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("k.COMSN_MNL_ADJ_SK").alias("COMSN_MNL_ADJ_SK"),
            F.col("all.COCE_ID").alias("AGNT_ID"),
            F.col("all.COCA_SEQ_NO").alias("SEQ_NO"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.COAR_ID").alias("COAR_ID"),
            F.col("all.COCE_ID_PAYEE").alias("COCE_ID_PAYEE"),
            F.col("all.LOBD_ID").alias("LOBD_ID"),
            F.col("all.COCA_PYMT_TYPE").alias("COCA_PYMT_TYPE"),
            F.col("all.COCA_MCTR_RSN").alias("COCA_MCTR_RSN"),
            F.col("all.COCA_STS").alias("COCA_STS"),
            F.col("all.COAG_EFF_DT").alias("COAG_EFF_DT"),
            F.col("all.COCA_PAID_DT").alias("COCA_PAID_DT"),
            F.col("all.COCA_PYMT_AMT").alias("ADJ_AMT"),
            F.col("all.COCA_PYMT_DESC").alias("ADJ_DESC")
        )
    )

    # --------------------------------------------------
    # Return container output(s)
    # --------------------------------------------------
    return df_Key