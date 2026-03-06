# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit

"""
Shared container: ClmLnPcaPK

DESCRIPTION:
Shared container used for Primary Keying of Claim Line PCA job

CALLED BY:
FctsClmLnPcaExtr

MODIFICATIONS:
Developer    Date         Change Description         Project/Altius #   Development Project   Code Reviewer     Date Reviewed
Parik        2008-08-03   Initial program            3567(Primary Key)  devlIDS               Steph Goddard      2008-08-14

Annotations:
1. Primary key hash file created and loaded in FctsClmLnPKExtr, NascoClmLnPKExtr, PCSClmLnPKExtr and ArgusClmLnPKExtr processes
2. This job uses hf_clm_ln as the primary key hash file since it has the same natural key as the CLM_LN table
"""


def run_ClmLnPcaPK(df_Transform: DataFrame, params: dict) -> DataFrame:
    # Unpack parameters
    CurrRunCycle = params["CurrRunCycle"]
    adls_path = params["adls_path"]

    # Read hashed-file lookup (scenario c → Parquet)
    lkup_path = f"{adls_path}/ClmLnPcaPK_lkup.parquet"
    df_hf_clm_ln_lkup = (
        spark.read.parquet(lkup_path)
        .select(
            "SRC_SYS_CD",
            "CLM_ID",
            "CLM_LN_SEQ_NO",
            "CRT_RUN_CYC_EXCTN_SK",
            "CLM_LN_SK",
        )
    )

    # Join input rows with lookup
    df_join = (
        df_Transform.alias("transform")
        .join(
            df_hf_clm_ln_lkup.alias("lkup"),
            [
                col("transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD"),
                col("transform.CLM_ID") == col("lkup.CLM_ID"),
                col("transform.CLM_LN_SEQ_NO") == col("lkup.CLM_LN_SEQ_NO"),
            ],
            "left",
        )
    )

    # Apply column derivations
    df_enriched = (
        df_join
        .withColumn(
            "CLM_LN_SK",
            when(col("lkup.CLM_LN_SK").isNull(), lit(0)).otherwise(col("lkup.CLM_LN_SK")),
        )
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            when(col("lkup.CLM_LN_SK").isNull(), lit(CurrRunCycle)).otherwise(
                col("lkup.CRT_RUN_CYC_EXCTN_SK")
            ),
        )
        .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
    )

    # Surrogate-key generation for CLM_LN_SK
    df_enriched = SurrogateKeyGen(df_enriched, "<DB sequence name>", "CLM_LN_SK", "<schema>", "<secret_name>")

    # Final column ordering
    df_out = df_enriched.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "ROW_PASS_THRU",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CLM_LN_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK",
        "DSALW_EXCD_SK",
        "CLM_LN_PCA_LOB_CD_SK",
        "CLM_LN_PCA_PRCS_CD_SK",
        "CNSD_AMT",
        "DSALW_AMT",
        "NONCNSD_AMT",
        "PROV_PD_AMT",
        "SUB_PD_AMT",
        "PD_AMT",
    )

    return df_out