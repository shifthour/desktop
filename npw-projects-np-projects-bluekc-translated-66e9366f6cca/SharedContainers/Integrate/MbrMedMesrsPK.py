# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

# COMMAND ----------
"""
Shared Container: MbrMedMesrsPK
Folder Path   : Shared Containers/PrimaryKey
Job Category  : DS_Integrate
Description   : Shared container used for Primary Keying of Mbr Med Mesrs job

VC LOGS
^1_1 02/05/09 11:41:41 Batch  15012_42105 PROMOTE bckcetl ids20 dsadm bls for sg
^1_1 02/05/09 10:22:42 Batch  15012_37366 INIT bckcett testIDS dsadm bls for sg
^1_2 01/29/09 10:10:07 Batch  15005_36616 PROMOTE bckcett testIDS u03651 steph - primary key/balancing
^1_2 01/29/09 09:38:37 Batch  15005_34720 INIT bckcett devlIDS u03651 steffy
^1_1 01/26/09 14:40:15 Batch  15002_52819 INIT bckcett devlIDS u03651 steffy

Annotations:
- Used by  ImpProMbrMedMesrsExtr
- IDS Primary Key Container for Mbr Med Mesrs
- Hash file (hf_mbr_med_mesrs_allcol) cleared in calling job
- SQL joins temp table with key table to assign known keys
- Temp table is truncated before load and runstat done after load
- Load IDS temp. table
- join primary key info with table info
- update primary key table (K_MBR_MED_MESRS) with new keys created today
- primary key hash file only contains current run keys and is cleared before writing
- Assign primary surrogate key
"""
# COMMAND ----------
from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------
def run_MbrMedMesrsPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the MbrMedMesrsPK shared container.

    Parameters
    ----------
    df_AllCol   : DataFrame
        Incoming DataFrame for link 'AllCol'.
    df_Transform: DataFrame
        Incoming DataFrame for link 'Transform'.
    params      : dict
        Runtime parameters and configuration objects.

    Returns
    -------
    DataFrame
        DataFrame corresponding to output link 'Key'.
    """

    # ------------------------------------------------------------------
    # Un-pack required parameters (each exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle         = params["CurrRunCycle"]
    SrcSysCd             = params["SrcSysCd"]
    RunID                = params["RunID"]
    CurrentDate          = params["CurrentDate"]
    IDSOwner             = params["IDSOwner"]
    ids_secret_name      = params["ids_secret_name"]
    ids_jdbc_url         = params["ids_jdbc_url"]
    ids_jdbc_props       = params["ids_jdbc_props"]
    adls_path            = params["adls_path"]
    adls_path_raw        = params["adls_path_raw"]
    adls_path_publish    = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # 1. Replace intermediate hash file  hf_mbr_med_mesrs_allcol
    #    Scenario-a hashed file: deduplicate then forward
    # ------------------------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "MBR_UNIQ_KEY", "PRCS_YR_MO_SK"],
        []
    )

    # ------------------------------------------------------------------
    # 2. Handle DB2 temp-table  K_MBR_MED_MESRS_TEMP
    #    (write incoming rows, then extract)
    # ------------------------------------------------------------------
    temp_table = f"{IDSOwner}.K_MBR_MED_MESRS_TEMP"

    # Drop & Create temp table
    drop_stmt   = f"DROP TABLE {temp_table}"
    create_stmt = f"""
        CREATE TABLE {temp_table} (
            SRC_SYS_CD_SK      INTEGER      NOT NULL,
            MBR_UNIQ_KEY       VARCHAR(20)  NOT NULL,
            PRCS_YR_MO_SK      INTEGER      NOT NULL,
            PRIMARY KEY (SRC_SYS_CD_SK, MBR_UNIQ_KEY, PRCS_YR_MO_SK)
        )
    """
    execute_dml(drop_stmt,   ids_jdbc_url, ids_jdbc_props)
    execute_dml(create_stmt, ids_jdbc_url, ids_jdbc_props)

    # Write incoming rows into temp table
    (
        df_Transform
        .select(
            "SRC_SYS_CD_SK",
            "MBR_UNIQ_KEY",
            "PRCS_YR_MO_SK"
        )
        .write
        .format("jdbc")
        .mode("append")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", temp_table)
        .save()
    )

    # ------------------------------------------------------------------
    # 2.b Extract (W_Extract link)
    # ------------------------------------------------------------------
    extract_query = f"""
    SELECT  k.MBR_MED_MESRS_SK,
            w.SRC_SYS_CD_SK,
            w.MBR_UNIQ_KEY,
            w.PRCS_YR_MO_SK,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM     {temp_table}              w
    JOIN     {IDSOwner}.K_MBR_MED_MESRS k
           ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
          AND w.MBR_UNIQ_KEY  = k.MBR_UNIQ_KEY
          AND w.PRCS_YR_MO_SK = k.PRCS_YR_MO_SK
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.MBR_UNIQ_KEY,
           w2.PRCS_YR_MO_SK,
           {CurrRunCycle}
    FROM   {temp_table} w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_MBR_MED_MESRS k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.MBR_UNIQ_KEY  = k2.MBR_UNIQ_KEY
          AND w2.PRCS_YR_MO_SK = k2.PRCS_YR_MO_SK
    )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
             .option("url", ids_jdbc_url)
             .options(**ids_jdbc_props)
             .option("query", extract_query)
             .load()
    )

    # ------------------------------------------------------------------
    # 3. PrimaryKey transformer logic
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("MBR_MED_MESRS_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svSK", F.col("MBR_MED_MESRS_SK"))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # Surrogate Key Generation
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svSK",<schema>,<secret_name>)

    # ------------------------------------------------------------------
    # 3.a  updt link  (to hf_mbr_med_mesrs)
    # ------------------------------------------------------------------
    df_updt = (
        df_enriched
        .select(
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            "MBR_UNIQ_KEY",
            "PRCS_YR_MO_SK",
            "svCrtRunCycExctnSk".alias("CRT_RUN_CYC_EXCTN_SK"),
            "svSK".alias("MBR_MED_MESRS_SK")
        )
    )

    # Write to parquet (scenario-c hash file)
    parquet_path_updt = f"{adls_path}/MbrMedMesrsPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path_updt,
        mode="overwrite",
        is_pqruet=True
    )

    # ------------------------------------------------------------------
    # 3.b  NewKeys link  (sequential file)
    # ------------------------------------------------------------------
    df_newkeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == "I")
        .select(
            "SRC_SYS_CD_SK",
            "MBR_UNIQ_KEY",
            "PRCS_YR_MO_SK",
            "svCrtRunCycExctnSk".alias("CRT_RUN_CYC_EXCTN_SK"),
            "svSK".alias("MBR_MED_MESRS_SK")
        )
    )

    seq_path_newkeys = f"{adls_path}/load/K_MBR_MED_MESRS.dat"
    write_files(
        df_newkeys,
        seq_path_newkeys,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False
    )

    # ------------------------------------------------------------------
    # 3.c  Keys link  (to Merge transformer)
    # ------------------------------------------------------------------
    df_Keys = (
        df_enriched
        .select(
            "svSK"          .alias("MBR_MED_MESRS_SK"),
            "SRC_SYS_CD_SK",
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            "MBR_UNIQ_KEY",
            "PRCS_YR_MO_SK",
            "svInstUpdt"    .alias("INSRT_UPDT_CD"),
            "svCrtRunCycExctnSk".alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # 4. Merge transformer  (produce final Key output)
    # ------------------------------------------------------------------
    join_cols = ["SRC_SYS_CD_SK", "MBR_UNIQ_KEY", "PRCS_YR_MO_SK"]

    df_Key = (
        df_AllCol_dedup.alias("all")
        .join(df_Keys.alias("k"), on=join_cols, how="left")
        .select(
            "all.JOB_EXCTN_RCRD_ERR_SK",
            "k.INSRT_UPDT_CD",
            "all.DISCARD_IN",
            "all.PASS_THRU_IN",
            "all.FIRST_RECYC_DT",
            "all.ERR_CT",
            "all.RECYCLE_CT",
            "k.SRC_SYS_CD",
            "all.PRI_KEY_STRING",
            "k.MBR_MED_MESRS_SK",
            "all.MBR_UNIQ_KEY",
            "all.PRCS_YR_MO_SK",
            "k.CRT_RUN_CYC_EXCTN_SK",
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "all.MBR_SK",
            "all.CARE_OPP_ATCHD_IN",
            "all.CASE_DEFN_ATCHD_IN",
            "all.CLNCL_IN_ATCHD_IN",
            "all.MED_ALERT_ATCHD_IN",
            "all.ACTURL_UNDWRT_FTR_RISK_SCORE_N",
            "all.AGE_GNDR_RISK_SCORE_NO",
            "all.FTR_IP_RISK_SCORE_NO",
            "all.FTR_TOT_RISK_SCORE_NO",
            "all.IP_STAY_PROBLTY_NO"
        )
    )

    # ------------------------------------------------------------------
    # Return the single output DataFrame
    # ------------------------------------------------------------------
    return df_Key
# COMMAND ----------