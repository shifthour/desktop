
# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

"""
AlienoAppUserRoleTransPK  -  Primary Key Shared Container for HSTD_APP_USER_ROLE_TRANS

PROCESSING: 
  • SQL joins temp table with key table to assign known keys
  • Temp table is truncated before load and runstats done after load
  • Load IDS temp. table
  • join primary key info with table info
  • update primary key table (K_HSTD_APP_USER_ROLE_TRANS) with new keys created today
  • primary key hash file only contains current run keys and is cleared before writing
  • Assign primary surrogate key

MODIFICATIONS:
Developer            Date         Project/Altiris #   Change Description              Code Reviewer   Date Reviewed
------------------   ----------   ------------------   -----------------------------   -------------   -------------
Bhoomi Dasari        2010-07-19   4297/Alineo-2        Original Programming            Steph Goddard   07/26/2010

Annotations:
  – SQL joins temp table with key table to assign known keys
  – Temp table is tuncated before load and runstat done after load
  – Load IDS temp. table
  – join primary key info with table info
  – update primary key table (K_HSTD_APP_USER_ROLE_TRANS) with new keys created today
  – primary key hash file only contains current run keys and is cleared before writing
  – Assign primary surrogate key
  – This container is used in:
      AlineoHstdAppUserRoleExtr   (re-compile when logic changes)
  – Alineo HSTD_APP_USER_ROLE_TRANS Primary Key Shared Container
"""


def run_AlienoAppUserRoleTransPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict,
) -> DataFrame:
    """
    Translates DataStage shared container 'AlienoAppUserRoleTransPK'
    into an equivalent PySpark data-processing routine.
    """

    # ------------------------------------------------------------------
    # unpack required parameters
    # ------------------------------------------------------------------
    CurrRunCycle     = params["CurrRunCycle"]
    SrcSysCd         = params["SrcSysCd"]
    IDSOwner         = params["IDSOwner"]
    ids_secret_name  = params["ids_secret_name"]
    ids_jdbc_url     = params["ids_jdbc_url"]
    ids_jdbc_props   = params["ids_jdbc_props"]
    adls_path        = params["adls_path"]
    adls_path_raw    = params.get("adls_path_raw", "")
    adls_path_publish = params.get("adls_path_publish", "")
    # ------------------------------------------------------------------
    spark = SparkSession.getActiveSession()

    # ==============================================================
    # 1. Replace intermediate hash file hf_hstd_appuser_role_trans_allcol
    #    with duplicate-removal logic on df_AllCol
    # ==============================================================
    key_cols_hash_a = [
        "SRC_SYS_CD_SK",
        "HSTD_USER_ID",
        "ROW_EFF_DT_SK",
        "APP_USER_ROLE_ID",
    ]
    df_AllColOut = df_AllCol.dropDuplicates(key_cols_hash_a)

    # ==============================================================
    # 2. Fetch key table K_HSTD_APP_USER_ROLE_TRANS from IDS
    # ==============================================================
    key_table_query = f"""
        SELECT
            HSTD_APP_USER_ROLE_T_SK,
            SRC_SYS_CD_SK,
            HSTD_USER_ID,
            APP_USER_ROLE_ID,
            ROW_EFF_DT_SK,
            CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_HSTD_APP_USER_ROLE_TRANS
    """
    df_key_tbl = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", key_table_query)
        .load()
    )

    # ==============================================================
    # 3. Reproduce W_Extract logic
    # ==============================================================

    join_expr = [
        df_Transform["SRC_SYS_CD_SK"] == df_key_tbl["SRC_SYS_CD_SK"],
        df_Transform["HSTD_USER_ID"] == df_key_tbl["HSTD_USER_ID"],
        df_Transform["APP_USER_ROLE_ID"] == df_key_tbl["APP_USER_ROLE_ID"],
        df_Transform["ROW_EFF_DT_SK"] == df_key_tbl["ROW_EFF_DT_SK"],
    ]

    df_join = df_Transform.alias("w").join(
        df_key_tbl.alias("k"), join_expr, "left"
    )

    df_w_extract = (
        df_join.select(
            F.when(F.col("k.HSTD_APP_USER_ROLE_T_SK").isNull(), F.lit(-1))
            .otherwise(F.col("k.HSTD_APP_USER_ROLE_T_SK"))
            .alias("HSTD_APP_USER_ROLE_T_SK"),
            F.col("w.SRC_SYS_CD_SK"),
            F.col("w.HSTD_USER_ID"),
            F.col("w.APP_USER_ROLE_ID"),
            F.col("w.ROW_EFF_DT_SK"),
            F.when(F.col("k.CRT_RUN_CYC_EXCTN_SK").isNull(), F.lit(CurrRunCycle))
            .otherwise(F.col("k.CRT_RUN_CYC_EXCTN_SK"))
            .alias("CRT_RUN_CYC_EXCTN_SK"),
        )
    )

    # ==============================================================
    # 4. PrimaryKey Transformer logic
    # ==============================================================

    df_enriched = (
        df_w_extract.withColumn(
            "svInstUpdt",
            F.when(F.col("HSTD_APP_USER_ROLE_T_SK") == -1, F.lit("I")).otherwise(
                F.lit("U")
            ),
        )
    )

    # Surrogate key generation (KeyMgtGetNextValueConcurrent replacement)
    df_enriched = SurrogateKeyGen(
        df_enriched, <DB sequence name>, "HSTD_APP_USER_ROLE_T_SK", <schema>, <secret_name>
    )

    df_enriched = (
        df_enriched.withColumn("svSK", F.col("HSTD_APP_USER_ROLE_T_SK"))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(
                F.col("CRT_RUN_CYC_EXCTN_SK")
            ),
        )
    )

    # ----------------------------------------------
    # 4a. updt link (write to parquet hash file)
    # ----------------------------------------------
    df_updt = (
        df_enriched.select(
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            "HSTD_USER_ID",
            "APP_USER_ROLE_ID",
            "ROW_EFF_DT_SK",
            "svCrtRunCycExctnSk".alias("CRT_RUN_CYC_EXCTN_SK"),
            "svSK".alias("HSTD_APP_USER_ROLE_T_SK"),
        )
    )

    write_files(
        df_updt,
        f"{adls_path}/AlienoAppUserRoleTransPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None,
    )

    # ----------------------------------------------
    # 4b. NewKeys link (write sequential file)
    # ----------------------------------------------
    df_newkeys = (
        df_enriched.filter(F.col("svInstUpdt") == "I").select(
            "HSTD_USER_ID",
            "APP_USER_ROLE_ID",
            "ROW_EFF_DT_SK",
            "SRC_SYS_CD_SK",
            "svCrtRunCycExctnSk".alias("CRT_RUN_CYC_EXCTN_SK"),
            "svSK".alias("HSTD_APP_USER_ROLE_T_SK"),
        )
    )

    write_files(
        df_newkeys,
        f"{adls_path}/load/K_HSTD_APP_USER_ROLE_TRANS.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None,
    )

    # ----------------------------------------------
    # 4c. Keys link (for merge)
    # ----------------------------------------------
    df_keys = (
        df_enriched.select(
            "svSK".alias("HSTD_APP_USER_ROLE_T_SK"),
            "SRC_SYS_CD_SK",
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            "HSTD_USER_ID",
            "APP_USER_ROLE_ID",
            "ROW_EFF_DT_SK",
            "svInstUpdt".alias("INSRT_UPDT_CD"),
            "svCrtRunCycExctnSk".alias("CRT_RUN_CYC_EXCTN_SK"),
        )
    )

    # ==============================================================
    # 5. Merge Transformer logic
    # ==============================================================

    merge_join_expr = [
        df_AllColOut["SRC_SYS_CD_SK"] == df_keys["SRC_SYS_CD_SK"],
        df_AllColOut["HSTD_USER_ID"] == df_keys["HSTD_USER_ID"],
        df_AllColOut["APP_USER_ROLE_ID"] == df_keys["APP_USER_ROLE_ID"],
        df_AllColOut["ROW_EFF_DT_SK"] == df_keys["ROW_EFF_DT_SK"],
    ]

    df_Key = (
        df_AllColOut.alias("AllColOut")
        .join(df_keys.alias("Keys"), merge_join_expr, "left")
        .select(
            F.col("AllColOut.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("Keys.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("AllColOut.DISCARD_IN").alias("DISCARD_IN"),
            F.col("AllColOut.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("AllColOut.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("AllColOut.ERR_CT").alias("ERR_CT"),
            F.col("AllColOut.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("Keys.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("AllColOut.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("Keys.HSTD_APP_USER_ROLE_T_SK").alias("HSTD_APP_USER_ROLE_T_SK"),
            F.col("AllColOut.HSTD_USER_ID").alias("HSTD_USER_ID"),
            F.col("AllColOut.APP_USER_ROLE_ID").alias("APP_USER_ROLE_ID"),
            F.col("AllColOut.ROW_EFF_DT_SK").alias("ROW_EFF_DT_SK"),
            F.col("Keys.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("AllColOut.ACTV_IN").alias("ACTV_IN"),
            F.col("AllColOut.APP_USER_ROLE_CRT_DT_SK").alias("APP_USER_ROLE_CRT_DT_SK"),
            F.col("AllColOut.ROW_TERM_DT_SK").alias("ROW_TERM_DT_SK"),
            F.col("AllColOut.APP_USER_ROLE_DESC").alias("APP_USER_ROLE_DESC"),
            F.col("AllColOut.EFF_DATE").alias("EFF_DATE"),
            F.col("AllColOut.TERM_DATE").alias("TERM_DATE"),
            F.col("AllColOut.APPL_USER_ROLEROLE_CREATED").alias(
                "APPL_USER_ROLEROLE_CREATED"
            ),
            F.col("AllColOut.APPL_USERAPPL_USER_ROLEROLE_MODIFIED").alias(
                "APPL_USERAPPL_USER_ROLEROLE_MODIFIED"
            ),
        )
    )

    # ==============================================================
    # 6. return container output
    # ==============================================================

    return df_Key
