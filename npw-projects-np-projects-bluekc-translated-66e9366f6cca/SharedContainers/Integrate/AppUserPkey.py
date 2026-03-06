# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
JobName     : AppUserPkey
JobType     : Server Job
JobCategory : DS_Integrate
FolderPath  : Shared Containers

Description :
* VC LOGS *
^1_1 01/28/08 07:49:06 Batch  14638_28152 INIT bckcetl ids20 dsadm dsadm
^1_1 01/10/08 08:38:00 Batch  14620_31084 INIT bckcetl ids20 dsadm dsadm
^1_1 12/26/07 08:56:40 Batch  14605_32206 INIT bckcetl ids20 dsadm dsadm
^1_2 11/23/07 09:25:41 Batch  14572_33950 INIT bckcetl ids20 dsadm dsadm
^1_1 11/21/07 14:15:25 Batch  14570_51332 INIT bckcetl ids20 dsadm dsadm
^1_2 03/21/07 14:59:13 Batch  14325_53956 INIT bckcetl ids20 dsadm dsadm
^1_1 03/07/07 15:57:28 Batch  14311_57454 INIT bckcetl ids20 dsadm dsadm
^1_1 10/24/05 15:34:29 Batch  13812_56076 PROMOTE bckcetl ids20 dsadm GIna Parr
^1_1 10/24/05 15:29:11 Batch  13812_55756 INIT bckcett testIDS30 dsadm Gina Parr
^1_1 10/18/05 08:21:55 Batch  13806_30120 PROMOTE bckcett testIDS30 u03651 steffy
^1_1 10/18/05 08:16:03 Batch  13806_29765 INIT bckcett devlIDS30 u03651 steffy

Annotation :
This container is used in:
FctsEmpAppUserExtr
NpsEmpAppUserExtr

These programs need to be re-compiled when logic changes
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_AppUserPkey(
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Shared-container logic translated from DataStage job 'AppUserPkey'.
    Parameters
    ----------
    df_Transform : DataFrame
        Input stream corresponding to link 'Transform'.
    params : dict
        Runtime parameters already provided by the parent job.
    Returns
    -------
    DataFrame
        Output stream for link 'Key'.
    """

    # Unpack parameters
    CurrRunCycle = params["CurrRunCycle"]
    adls_path = params["adls_path"]

    # ------------------------------------------------------------------
    # Read lookup hashed-file (translated to parquet)
    lkup_path = f"{adls_path}/AppUserPkey_lkup.parquet"
    df_lkup = spark.read.parquet(lkup_path)

    # ------------------------------------------------------------------
    # Join input with lookup
    join_expr = [
        F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
        F.col("Transform.USER_ID") == F.col("lkup.USER_ID")
    ]

    df_joined = (
        df_Transform.alias("Transform")
        .join(df_lkup.alias("lkup"), join_expr, "left")
    )

    # ------------------------------------------------------------------
    # Derive columns
    df_enriched = (
        df_joined
        .withColumn("is_new_rec", F.col("lkup.USER_SK").isNull())
        .withColumn(
            "SK",
            F.when(F.col("lkup.USER_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.USER_SK"))
        )
        .withColumn(
            "CrtRunCcyExctnSK",
            F.when(F.col("lkup.USER_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn("LastUpdtRunCycExctnSK", F.lit(CurrRunCycle))
        .withColumn(
            "DEPT_ID",
            F.when(
                F.trim(F.col("Transform.DEPT_ID")) == F.lit(""),
                F.lit(" ")
            ).otherwise(F.substring(F.trim(F.col("Transform.DEPT_ID")), 1, 4))
        )
        .withColumn(
            "USER_DESCRIPTION",
            F.when(
                F.trim(F.col("Transform.USER_DESCRIPTION")) == F.lit(""),
                F.lit(" ")
            ).otherwise(F.substring(F.trim(F.col("Transform.USER_DESCRIPTION")), 1, 70))
        )
        .withColumnRenamed("Transform.JOB_EXCTN_RCRD_ERR_SK", "JOB_EXCTN_RCRD_ERR_SK")
        .withColumnRenamed("Transform.INSRT_UPDT_CD", "INSRT_UPDT_CD")
        .withColumnRenamed("Transform.DISCARD_IN", "DISCARD_IN")
        .withColumnRenamed("Transform.PASS_THRU_IN", "PASS_THRU_IN")
        .withColumnRenamed("Transform.FIRST_RECYC_DT", "FIRST_RECYC_DT")
        .withColumnRenamed("Transform.ERR_CT", "ERR_CT")
        .withColumnRenamed("Transform.RECYCLE_CT", "RECYCLE_CT")
        .withColumnRenamed("Transform.SRC_SYS_CD", "SRC_SYS_CD")
        .withColumnRenamed("Transform.PRI_KEY_STRING", "PRI_KEY_STRING")
        .withColumnRenamed("Transform.USER_ID", "USER_ID")
        .withColumnRenamed("Transform.REL_NASCO_USER_ID", "REL_NASCO_USER_ID")
        .withColumnRenamed("Transform.APP_USER_TYP_CD", "APP_USER_TYP_CD")
        .withColumnRenamed("Transform.CLERK_ID", "CLERK_ID")
    )

    # ------------------------------------------------------------------
    # Surrogate key generation
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

    # ------------------------------------------------------------------
    # Prepare update (updt) DataFrame for parquet write
    df_updt = (
        df_enriched
        .filter(F.col("is_new_rec"))
        .select(
            F.col("SRC_SYS_CD"),
            F.col("USER_ID"),
            F.col("CrtRunCcyExctnSK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("SK").alias("USER_SK")
        )
    )

    updt_path = f"{adls_path}/AppUserPkey_updt.parquet"
    write_files(
        df_updt,
        updt_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Final Key output
    df_Key = (
        df_enriched
        .select(
            "JOB_EXCTN_RCRD_ERR_SK",
            "INSRT_UPDT_CD",
            "DISCARD_IN",
            "PASS_THRU_IN",
            "FIRST_RECYC_DT",
            "ERR_CT",
            "RECYCLE_CT",
            "SRC_SYS_CD",
            "PRI_KEY_STRING",
            F.col("SK").alias("USER_SK"),
            F.col("CrtRunCcyExctnSK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("LastUpdtRunCycExctnSK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "USER_ID",
            "REL_NASCO_USER_ID",
            "APP_USER_TYP_CD",
            "CLERK_ID",
            "DEPT_ID",
            "USER_DESCRIPTION"
        )
    )

    return df_Key