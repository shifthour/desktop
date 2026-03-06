# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Job Name      : IndvBePopHlthPocProbTransPK
Job Type      : Server Job
Folder Path   : Shared Containers/PrimaryKey
Category      : DS_Integrate

Description:
Copyright 2010 Blue Cross/Blue Shield of Kansas City

PROCESSING: Primary Key Shared Container for INDV_BE_POP_HLTH_POC_PROB_TRANS.

MODIFICATIONS:
Developer                       Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------
Kalyan Neelam                2010-07-19       4297                             Original Programming                           RebuildIntNewDevl      Steph Goddard             07/21/2010

Annotations:
- Alineo INDV_BE_POP_HLTH_POC_PROB_TRANS Primary Key Shared Container
- Load IDS temp. table
- SQL joins temp table with key table to assign known keys
- primary key hash file only contains current run keys and is cleared before writing
- update primary key table (K_INDV_BE_POP_HLTH_POC_PROB_TRANS) with new keys created today
- Assign primary surrogate key
- join primary key info with table info
- This container is used in:
    AlineoIndvBePopHlthPocProbTransExtr
    These programs need to be re-compiled when logic changes
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def run_IndvBePopHlthPocProbTransPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # --------------------------------------------------
    # Unpack runtime parameters
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    SrcSysCd            = params["SrcSysCd"]
    CurrRunCycle        = params["CurrRunCycle"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params.get("adls_path_raw", adls_path)
    adls_path_publish   = params.get("adls_path_publish", adls_path)
    FilePath            = params.get("FilePath", "")
    # --------------------------------------------------
    # 1. Deduplicate incoming hashed-file intermediates
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["PGM_ENROLLMENT_ID", "PROB_ID", "ROW_EFF_DT_SK", "SRC_SYS_CD_SK"],
        []
    )
    df_Transform_dedup = dedup_sort(
        df_Transform,
        ["POP_HLTH_PGM_ENR_ID", "POC_PROB_ID", "ROW_EFF_DT_SK", "SRC_SYS_CD_SK"],
        []
    )
    # --------------------------------------------------
    # 2. Pull current key table from IDS
    extract_query = f"""
        SELECT
            INDV_BE_POP_HLTH_POC_PROB_T_SK,
            POP_HLTH_PGM_ENR_ID,
            POC_PROB_ID,
            ROW_EFF_DT_SK,
            SRC_SYS_CD_SK,
            CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_INDV_BE_POP_HLTH_POC_PROB_TRANS
    """
    df_key_tbl = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )
    # --------------------------------------------------
    # 3. Replicate DB2 UNION logic
    join_cond = (
        (F.col("w.POP_HLTH_PGM_ENR_ID") == F.col("k.POP_HLTH_PGM_ENR_ID")) &
        (F.col("w.POC_PROB_ID")        == F.col("k.POC_PROB_ID")) &
        (F.col("w.ROW_EFF_DT_SK")      == F.col("k.ROW_EFF_DT_SK")) &
        (F.col("w.SRC_SYS_CD_SK")      == F.col("k.SRC_SYS_CD_SK"))
    )
    df_existing = (
        df_Transform_dedup.alias("w")
          .join(df_key_tbl.alias("k"), join_cond, "inner")
          .select(
              F.col("k.INDV_BE_POP_HLTH_POC_PROB_T_SK"),
              F.col("w.POP_HLTH_PGM_ENR_ID"),
              F.col("w.POC_PROB_ID"),
              F.col("w.ROW_EFF_DT_SK"),
              F.col("w.SRC_SYS_CD_SK"),
              F.col("k.CRT_RUN_CYC_EXCTN_SK")
          )
    )
    df_new = (
        df_Transform_dedup.alias("w2")
          .join(df_key_tbl.alias("k2"), join_cond, "leftanti")
          .select(
              F.lit(-1).alias("INDV_BE_POP_HLTH_POC_PROB_T_SK"),
              F.col("w2.POP_HLTH_PGM_ENR_ID"),
              F.col("w2.POC_PROB_ID"),
              F.col("w2.ROW_EFF_DT_SK"),
              F.col("w2.SRC_SYS_CD_SK"),
              F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
          )
    )
    df_w_extract = df_existing.unionByName(df_new)
    # --------------------------------------------------
    # 4. Primary_Key transformer derivations
    df_enriched = (
        df_w_extract
          .withColumn(
              "INSRT_UPDT_CD",
              F.when(F.col("INDV_BE_POP_HLTH_POC_PROB_T_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
          )
          .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
          .withColumn(
              "CRT_RUN_CYC_EXCTN_SK",
              F.when(
                  F.col("INDV_BE_POP_HLTH_POC_PROB_T_SK") == F.lit(-1),
                  F.lit(CurrRunCycle)
              ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
          )
    )
    # Surrogate key generation (special placeholder call, per requirements)
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'INDV_BE_POP_HLTH_POC_PROB_T_SK',<schema>,<secret_name>)
    # --------------------------------------------------
    # 5. Split transformer outputs
    df_keys = df_enriched.select(
        "INDV_BE_POP_HLTH_POC_PROB_T_SK",
        "POP_HLTH_PGM_ENR_ID",
        "POC_PROB_ID",
        "ROW_EFF_DT_SK",
        "SRC_SYS_CD_SK",
        "SRC_SYS_CD",
        "INSRT_UPDT_CD",
        "CRT_RUN_CYC_EXCTN_SK"
    )
    df_updt = df_enriched.select(
        "POP_HLTH_PGM_ENR_ID",
        "POC_PROB_ID",
        "ROW_EFF_DT_SK",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "INDV_BE_POP_HLTH_POC_PROB_T_SK"
    )
    df_newkeys = (
        df_enriched.filter(F.col("INSRT_UPDT_CD") == F.lit("I"))
                   .select(
                       "POP_HLTH_PGM_ENR_ID",
                       "POC_PROB_ID",
                       "ROW_EFF_DT_SK",
                       "SRC_SYS_CD_SK",
                       "CRT_RUN_CYC_EXCTN_SK",
                       "INDV_BE_POP_HLTH_POC_PROB_T_SK"
                   )
    )
    # --------------------------------------------------
    # 6. Persist hashed-file and sequential-file outputs
    parquet_path_updt = f"{adls_path}/IndvBePopHlthPocProbTransPK_Updt.parquet"
    write_files(
        df_updt,
        parquet_path_updt,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )
    seq_file_path = f"{adls_path}/load/K_INDV_BE_POP_HLTH_POC_PROB_TRANS.dat"
    write_files(
        df_newkeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )
    # --------------------------------------------------
    # 7. Merge transformer
    merge_join_cond = (
        (F.col("all.PGM_ENROLLMENT_ID") == F.col("k.POP_HLTH_PGM_ENR_ID")) &
        (F.col("all.PROB_ID")          == F.col("k.POC_PROB_ID")) &
        (F.col("all.ROW_EFF_DT_SK")    == F.col("k.ROW_EFF_DT_SK")) &
        (F.col("all.SRC_SYS_CD_SK")    == F.col("k.SRC_SYS_CD_SK"))
    )
    df_key_out = (
        df_AllCol_dedup.alias("all")
          .join(df_keys.alias("k"), merge_join_cond, "left")
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
              "k.INDV_BE_POP_HLTH_POC_PROB_T_SK",
              "all.PGM_ENROLLMENT_ID",
              "all.PROB_ID",
              "all.ROW_EFF_DT_SK",
              "k.CRT_RUN_CYC_EXCTN_SK",
              F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
              "all.PROB_CODE",
              "all.PROB_OUTCOME_CTGY_CODE",
              "all.PROB_OUTCOME_CODE",
              "all.PROB_STS_CODE",
              "all.PROB_CREATED",
              "all.PROB_ACTUAL_COMPLTN_DATE",
              "all.PROB_START_DATE",
              "all.PROB_TGT_COMPLTN_DATE",
              "all.ROW_TERM_DT_SK",
              "all.INDV_BE_KEY",
              "all.PROB_MODIFIED"
          )
    )
    # --------------------------------------------------
    return df_key_out