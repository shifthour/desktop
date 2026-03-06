# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

PROCESSING: Shared container used for Primary Keying of VBB CMPNT RWRD

MODIFICATIONS:
Developer           Date            Change Description           Project/Altius #     Development Project    Code Reviewer    Date Reviewed
Kalyan Neelam       2013-06-11      Initial Programming          4963 VBB Phase III   IntegrateNewDevl       Bhoomi Dasari    7/17/2013
"""

# join primary key info with table info
# update primary key table (K_VBB_PLN) with new keys created today
# Assign primary surrogate key
# primary key hash file only contains current run keys and is cleared before writing
# Temp table is tuncated before load and runstat done after load
# SQL joins temp table with key table to assign known keys
# Load IDS temp. table
# Hash file (hf_vbb_cmpnt_rwrd_allcol) cleared in the calling program - IdsVbbCmpntRwrdExtr
# Hashfile cleared in calling program
# This container is used in:
# IdsVbbCmpntRwrdExtr
# These programs need to be re-compiled when logic changes

from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_VbbCmpntRwrdPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Translated PySpark implementation of the VbbCmpntRwrdPK shared container.
    Consumes:
        df_AllCol   : DataFrame for link 'AllCol'
        df_Transform: DataFrame for link 'Transform'
    Returns:
        DataFrame    : DataFrame for link 'Key'
    """

    # ------------------------------------------------------------------
    # Unpack parameters (each exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # Stage: hf_vbb_cmpnt_rwrd_allcol  (scenario a – intermediate hash file)
    # ------------------------------------------------------------------
    df_AllColOut = dedup_sort(
        df_AllCol,
        ["VBB_CMPNT_UNIQ_KEY", "VBB_CMPNT_RWRD_SEQ_NO", "SRC_SYS_CD_SK"],
        []
    )

    # ------------------------------------------------------------------
    # Stage: hf_vbb_cmpnt_rwrd_trnsfrm (scenario a – intermediate hash file)
    # ------------------------------------------------------------------
    df_Dedupe = dedup_sort(
        df_Transform,
        ["VBB_CMPNT_UNIQ_KEY", "VBB_CMPNT_RWRD_SEQ_NO", "SRC_SYS_CD_SK"],
        []
    )

    # ------------------------------------------------------------------
    # Stage: K_VBB_CMPNT_TEMP  (DB2Connector)
    # ------------------------------------------------------------------
    temp_tbl = f"{IDSOwner}.K_VBB_CMPNT_RWRD_TEMP"
    execute_dml(f"DROP TABLE IF EXISTS {temp_tbl}", ids_jdbc_url, ids_jdbc_props)

    create_sql = f"""
    CREATE TABLE {temp_tbl} (
        VBB_CMPNT_UNIQ_KEY  INTEGER NOT NULL,
        VBB_CMPNT_RWRD_SEQ_NO INTEGER NOT NULL,
        SRC_SYS_CD_SK       INTEGER NOT NULL,
        PRIMARY KEY (VBB_CMPNT_UNIQ_KEY, VBB_CMPNT_RWRD_SEQ_NO, SRC_SYS_CD_SK)
    )
    """
    execute_dml(create_sql, ids_jdbc_url, ids_jdbc_props)

    (
        df_Dedupe.write.format("jdbc")
        .mode("append")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", temp_tbl)
        .save()
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {temp_tbl} on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    extract_query = f"""
    SELECT k.VBB_CMPNT_RWRD_SK,
           w.VBB_CMPNT_UNIQ_KEY,
           w.VBB_CMPNT_RWRD_SEQ_NO,
           w.SRC_SYS_CD_SK,
           k.CRT_RUN_CYC_EXCTN_SK
      FROM {temp_tbl} w,
           {IDSOwner}.K_VBB_CMPNT_RWRD k
     WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
       AND w.VBB_CMPNT_UNIQ_KEY = k.VBB_CMPNT_UNIQ_KEY
       AND w.VBB_CMPNT_RWRD_SEQ_NO = k.VBB_CMPNT_RWRD_SEQ_NO
    UNION
    SELECT -1,
           w2.VBB_CMPNT_UNIQ_KEY,
           w2.VBB_CMPNT_RWRD_SEQ_NO,
           w2.SRC_SYS_CD_SK,
           {CurrRunCycle}
      FROM {temp_tbl} w2
     WHERE NOT EXISTS (
           SELECT 1
             FROM {IDSOwner}.K_VBB_CMPNT_RWRD k2
            WHERE w2.SRC_SYS_CD_SK        = k2.SRC_SYS_CD_SK
              AND w2.VBB_CMPNT_UNIQ_KEY   = k2.VBB_CMPNT_UNIQ_KEY
              AND w2.VBB_CMPNT_RWRD_SEQ_NO= k2.VBB_CMPNT_RWRD_SEQ_NO)
    """

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # ------------------------------------------------------------------
    # Stage: PrimaryKey (Transformer)
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("VBB_CMPNT_RWRD_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    df_enriched = SurrogateKeyGen(df_enriched, <DB sequence name>, "svSK", <schema>, <secret_name>)

    # Link: updt (to parquet hash-file replacement)
    df_updt = (
        df_enriched.select(
            "VBB_CMPNT_UNIQ_KEY",
            "VBB_CMPNT_RWRD_SEQ_NO",
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("VBB_CMPNT_RWRD_SK"),
        )
    )

    write_files(
        df_updt,
        f"{adls_path}/VbbCmpntRwrdPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # Link: NewKeys (to sequential file)
    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "VBB_CMPNT_UNIQ_KEY",
            "VBB_CMPNT_RWRD_SEQ_NO",
            "SRC_SYS_CD_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("VBB_CMPNT_RWRD_SK"),
        )
    )

    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_VBB_CMPNT_RWRD.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # Link: Keys (to Merge transformer)
    df_Keys = (
        df_enriched.select(
            F.col("svSK").alias("VBB_CMPNT_RWRD_SK"),
            "VBB_CMPNT_UNIQ_KEY",
            "VBB_CMPNT_RWRD_SEQ_NO",
            "SRC_SYS_CD_SK",
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        )
    )

    # ------------------------------------------------------------------
    # Stage: Merge (Transformer)
    # ------------------------------------------------------------------
    df_Merge = (
        df_AllColOut.alias("AllColOut")
        .join(
            df_Keys.alias("Keys"),
            (
                (F.col("AllColOut.VBB_CMPNT_UNIQ_KEY") == F.col("Keys.VBB_CMPNT_UNIQ_KEY")) &
                (F.col("AllColOut.VBB_CMPNT_RWRD_SEQ_NO") == F.col("Keys.VBB_CMPNT_RWRD_SEQ_NO")) &
                (F.col("AllColOut.SRC_SYS_CD_SK") == F.col("Keys.SRC_SYS_CD_SK"))
            ),
            "left"
        )
    )

    df_Key = df_Merge.select(
        F.col("AllColOut.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("Keys.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("AllColOut.DISCARD_IN").alias("DISCARD_IN"),
        F.col("AllColOut.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("AllColOut.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("AllColOut.ERR_CT").alias("ERR_CT"),
        F.col("AllColOut.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("Keys.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("AllColOut.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("Keys.VBB_CMPNT_RWRD_SK").alias("VBB_CMPNT_RWRD_SK"),
        F.col("AllColOut.VBB_CMPNT_UNIQ_KEY").alias("VBB_CMPNT_UNIQ_KEY"),
        F.col("AllColOut.VBB_CMPNT_RWRD_SEQ_NO").alias("VBB_CMPNT_RWRD_SEQ_NO"),
        F.col("AllColOut.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("Keys.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("AllColOut.VNVN_ID").alias("VNVN_ID"),
        F.col("AllColOut.VNRW_SEQ_NO").alias("VNRW_SEQ_NO"),
        F.col("AllColOut.ERN_RWRD_END_DT_TYP_CD").alias("ERN_RWRD_END_DT_TYP_CD"),
        F.col("AllColOut.ERN_RWRD_STRT_DT_TYP_CD").alias("ERN_RWRD_STRT_DT_TYP_CD"),
        F.col("AllColOut.VBB_CMPNT_RWRD_BEG_DT_SK").alias("VBB_CMPNT_RWRD_BEG_DT_SK"),
        F.col("AllColOut.VBB_CMPNT_RWRD_END_DT_SK").alias("VBB_CMPNT_RWRD_END_DT_SK"),
        F.col("AllColOut.ERN_RWRD_END_DT_SK").alias("ERN_RWRD_END_DT_SK"),
        F.col("AllColOut.ERN_RWRD_STRT_DT_SK").alias("ERN_RWRD_STRT_DT_SK"),
        F.col("AllColOut.SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
        F.col("AllColOut.SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
        F.col("AllColOut.ERN_RWRD_AMT").alias("ERN_RWRD_AMT"),
        F.col("AllColOut.RQRD_ACHV_LVL_NO").alias("RQRD_ACHV_LVL_NO"),
        F.col("AllColOut.VBB_CMPNT_RWRD_DESC").alias("VBB_CMPNT_RWRD_DESC"),
    )

    # ------------------------------------------------------------------
    # Return container output(s)
    # ------------------------------------------------------------------
    return df_Key