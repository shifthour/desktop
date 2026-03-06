# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""***************************************************************************************************************************************************************
COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:    
PROCESSING:    Used in Evicore, NDBH, Telligen and Facets process

MODIFICATIONS:
Developer           Date          Change Description     Project #          Development Project     Code Reviewer     Date Reviewed  
----------------    ------------  --------------------   ---------------    --------------------    ---------------   ---------------
Ravi Singh          10/04/2018    Initial program        MTM-5841           Integrate Dev2
***************************************************************************************************************************************************************"""
# This container is used in:
# Evicore, NDBH and Telligen Appeals
# ......Extr
# These programs need to be re-compiled when logic changes
# Assign primary surrogate key
# This container is used in:
# EvicoreIdsAplLvlExtr
# FctsAplLvlExtr
# These programs need to be re-compiled when logic changes
# Assign primary surrogate key

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# -----------------------------------------------------------
# Main callable function for shared container: AplLinkPkey
# -----------------------------------------------------------
def run_AplLinkPkey(
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the shared container AplLinkPkey.

    Parameters
    ----------
    df_Transform : DataFrame
        Incoming DataFrame corresponding to the “Transform” link.
    params : dict
        Runtime parameters and JDBC configurations.

    Returns
    -------
    DataFrame
        DataFrame corresponding to the “Key” output link.
    """

    # -------------------------------------------------------
    # Unpack required parameters exactly once
    # -------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]

    # -------------------------------------------------------
    # Read from dummy table replacing hashed file hf_apl_link
    # -------------------------------------------------------
    dummy_table_name = f"{IDSOwner}.dummy_hf_apl_link"

    extract_query = f"""
        SELECT 
              SRC_SYS_CD               AS SRC_SYS_CD_lkp
            , APL_ID                   AS APL_ID_lkp
            , APL_LINK_TYP_CD_SK       AS APL_LINK_TYP_CD_SK_lkp
            , APL_LINK_ID              AS APL_LINK_ID_lkp
            , CRT_RUN_CYC_EXCTN_SK     AS CRT_RUN_CYC_EXCTN_SK_lkp
            , APL_LINK_SK              AS APL_LINK_SK_lkp
        FROM {dummy_table_name}
    """

    df_lkup = (
        spark.read.format("jdbc")
             .option("url", ids_jdbc_url)
             .options(**ids_jdbc_props)
             .option("query", extract_query)
             .load()
    )

    # -------------------------------------------------------
    # Join input with lookup table
    # -------------------------------------------------------
    join_expr = (
        (F.col("Transform.SRC_SYS_CD")           == F.col("lkup.SRC_SYS_CD_lkp"))        &
        (F.col("Transform.APL_ID")               == F.col("lkup.APL_ID_lkp"))            &
        (F.col("Transform.APL_LINK_TYP_CD_SK")   == F.col("lkup.APL_LINK_TYP_CD_SK_lkp"))&
        (F.col("Transform.APL_LINK_ID")          == F.col("lkup.APL_LINK_ID_lkp"))
    )

    df_joined = (
        df_Transform.alias("Transform")
        .join(df_lkup.alias("lkup"), join_expr, "left")
    )

    # -------------------------------------------------------
    # Derivations
    # -------------------------------------------------------
    df_enriched = (
        df_joined
        .withColumn(
            "SK",
            F.when(
                F.col("lkup.APL_LINK_SK_lkp").isNull(),
                F.lit(None)
            ).otherwise(F.col("lkup.APL_LINK_SK_lkp"))
        )
        .withColumn(
            "NewCrtRunCycExtcnSk",
            F.when(
                F.col("lkup.APL_LINK_SK_lkp").isNull(),
                F.lit(CurrRunCycle)
            ).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
    )

    # -------------------------------------------------------
    # Surrogate-key generation (mandatory call signature)
    # -------------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

    # -------------------------------------------------------
    # Build “updt” stream (new rows only) and write back
    # -------------------------------------------------------
    df_updt = (
        df_enriched
        .filter(F.col("lkup.APL_LINK_SK_lkp").isNull())
        .select(
            F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("Transform.APL_ID").alias("APL_ID"),
            F.col("Transform.APL_LINK_TYP_CD_SK").alias("APL_LINK_TYP_CD_SK"),
            F.col("Transform.APL_LINK_ID").alias("APL_LINK_ID"),
            F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("SK").alias("APL_LINK_SK")
        )
    )

    (
        df_updt.write
        .mode("append")
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", dummy_table_name)
        .save()
    )

    # -------------------------------------------------------
    # Build “Key” output stream
    # -------------------------------------------------------
    df_key = (
        df_enriched
        .select(
            F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
            F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("Transform.ERR_CT").alias("ERR_CT"),
            F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("SK").alias("APL_LINK_SK"),
            F.col("Transform.APL_ID").alias("APL_ID"),
            F.col("Transform.APL_LINK_TYP_CD_SK").alias("APL_LINK_TYP_CD_SK"),
            F.col("Transform.APL_LINK_ID").alias("APL_LINK_ID"),
            F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("Transform.APL_SK").alias("APL_SK"),
            F.col("Transform.CASE_MGT_SK").alias("CASE_MGT_SK"),
            F.col("Transform.CLM_SK").alias("CLM_SK"),
            F.col("Transform.CUST_SVC_SK").alias("CUST_SVC_SK"),
            F.col("Transform.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
            F.col("Transform.REL_APL_SK").alias("REL_APL_SK"),
            F.col("Transform.UM_SK").alias("UM_SK"),
            F.col("Transform.APL_LINK_RSN_CD_SK").alias("APL_LINK_RSN_CD_SK"),
            F.col("Transform.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
            F.col("Transform.APL_LINK_DESC").alias("APL_LINK_DESC")
        )
    )

    # -------------------------------------------------------
    # Return the container output(s)
    # -------------------------------------------------------
    return df_key