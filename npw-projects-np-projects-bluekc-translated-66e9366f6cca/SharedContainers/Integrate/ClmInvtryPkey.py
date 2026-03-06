# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared Container: ClmInvtryPkey
Primary Key logic for IDS CLM_INVTRY table

Jobs Using Container:
    FctsClmInvtryExtr, FctsXcClmInvtryExtr, IdsClmInvtryExtr, ImageNowClmInvtryExtr

Modification History:
    Parikshith Chada  12/18/2007  Original Programming – Change counter IDS_SK to CLM_INVTRY_SK
    ADasarathy        07/20/2015  Altiris #5407 – Added Column ASG_USER_SK at the end of trans

Control Job Rerun Information:
    Previous Run Successful: Restart, no other steps necessary
    Previous Run Aborted   : Restart, no other steps necessary
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit

# COMMAND ----------
def run_ClmInvtryPkey(
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the primary-key logic originally defined in the DataStage shared
    container ClmInvtryPkey.

    Parameters
    ----------
    df_Transform : DataFrame
        Upstream DataFrame corresponding to the container input link "Transform".
    params : dict
        Runtime parameters and JDBC configuration objects.

    Returns
    -------
    DataFrame
        DataFrame corresponding to the container output link "Key".
    """

    # ---- unpack parameters --------------------------------------------------
    CurrRunCycle      = params["CurrRunCycle"]
    DriverTable       = params.get("DriverTable", "")
    RunID             = params.get("RunID", "")
    CurrDate          = params.get("CurrDate", "")
    FacetsOwner       = params.get("FacetsOwner", "")
    facets_secret_name = params.get("facets_secret_name", "")
    jdbc_url          = params["jdbc_url"]
    jdbc_props        = params["jdbc_props"]

    # ---- read dummy table replacing hash file -------------------------------
    dummy_table_name = f"{FacetsOwner}.dummy_hf_clm_invtry" if FacetsOwner else "dummy_hf_clm_invtry"
    extract_query = f"""
        SELECT
            SRC_SYS_CD        AS SRC_SYS_CD_lkp,
            CLM_INVTRY_KEY_ID AS CLM_INVTRY_KEY_ID_lkp,
            CRT_RUN_CYC_EXCTN_SK AS CRT_RUN_CYC_EXCTN_SK_lkp,
            CLM_INVTRY_SK     AS CLM_INVTRY_SK_lkp
        FROM {dummy_table_name}
    """

    df_hf_clm_invtry = (
        spark.read.format("jdbc")
            .option("url", jdbc_url)
            .options(**jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # ---- transformer logic --------------------------------------------------
    join_expr = (
        (col("transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD_lkp")) &
        (col("transform.CLM_INVTRY_KEY_ID") == col("lkup.CLM_INVTRY_KEY_ID_lkp"))
    )

    df_join = (
        df_Transform.alias("transform")
        .join(df_hf_clm_invtry.alias("lkup"), join_expr, "left")
    )

    df_enriched = (
        df_join
        .withColumn("CLM_INVTRY_SK", col("CLM_INVTRY_SK_lkp"))
        .withColumn(
            "NewCrtRunCycExtcnSk",
            when(col("CLM_INVTRY_SK_lkp").isNull(), lit(CurrRunCycle))
            .otherwise(col("CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
        .withColumn("CRT_RUN_CYC_EXCTN_SK", col("NewCrtRunCycExtcnSk"))
        .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
    )

    # ---- surrogate key generation ------------------------------------------
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "CLM_INVTRY_SK",
        <schema>,
        <secret_name>
    )

    # ---- prepare rows for dummy-table insert --------------------------------
    df_updt = (
        df_enriched
        .filter(col("CLM_INVTRY_SK_lkp").isNull())
        .select(
            col("SRC_SYS_CD"),
            col("CLM_INVTRY_KEY_ID"),
            col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("CLM_INVTRY_SK")
        )
    )

    (
        df_updt.write.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("dbtable", dummy_table_name)
        .mode("append")
        .save()
    )

    # ---- build output link "Key" -------------------------------------------
    df_key = df_enriched.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CLM_INVTRY_SK",
        "CLM_INVTRY_KEY_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "GRP_ID",
        "OPS_WORK_UNIT_ID",
        "PDPD_ID",
        "PRPR_ID",
        "CLM_INVTRY_PEND_CAT_CD",
        "CLM_STTUS_CHG_RSN_CD",
        "CLST_STS",
        "CLCL_CL_SUB_TYPE",
        "CLCL_CL_TYPE",
        "INPT_DT_SK",
        "RCVD_DT_SK",
        "EXTR_DT_SK",
        "STTUS_DT_SK",
        "INVTRY_CT",
        "ASG_USER_SK",
        "WORK_ITEM_CT"
    )

    return df_key
# COMMAND ----------