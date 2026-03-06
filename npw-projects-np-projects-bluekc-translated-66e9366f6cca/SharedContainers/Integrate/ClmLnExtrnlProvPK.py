# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
COPYRIGHT 2024 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

JOB NAME: ClmLnExtrnlProvPK

DESCRIPTION: 
Takes the sorted file and applies the primary key.   
Preps the file for the foreign key lookup process that follows.

INPUTS:  
Sequential file in common record format created in transform job.
  
HASH FILES:    
hf_clm_ln - this is both read from and written to.   

TRANSFORMS:  

PROCESSING:

OUTPUTS: Sequential file in common record format.

MODIFICATIONS:

Developer                     Date         Change Description                                   Project/Altius #   Development Project   Code Reviewer     Date Reviewed  
-----------------------       ----------   --------------------------------------------------   ----------------   -------------------   --------------    -------------  
Revathi BoojiReddy            2024-06-12   Original programming                                 US 616333           IntegrateDev1         Jeyaprasanna      2024-06-27
"""

# This container is used in:
# FctsClmLnExtrnlProvExtr
#
# These programs need to be re-compiled when logic changes
#
# This job uses hf_clm_ln as the primary key hash file since it has the same natural key as the CLM_LN table
#
# Primary key hash file created and loaded in FctsClmLnPKExtr, NascoClmLnPKExtr, PCSClmLnPKExtr and ArgusClmLnPKExtr processes

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit

spark: SparkSession = spark  # type: ignore


def run_ClmLnExtrnlProvPK(
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the shared-container logic for ClmLnExtrnlProvPK.

    Parameters
    ----------
    df_Transform : pyspark.sql.DataFrame
        Input DataFrame corresponding to the 'Transform' link.
    params : dict
        Runtime parameters and secrets passed from the parent job/notebook.

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame corresponding to the 'Key' output link.
    """

    # ------------------------------------------------------------------
    # Unpack required parameters exactly once
    # ------------------------------------------------------------------
    CurrRunCycle = params["CurrRunCycle"]
    adls_path = params["adls_path"]

    # ------------------------------------------------------------------
    # Read the hashed-file lookup as a Parquet file (scenario c)
    # ------------------------------------------------------------------
    df_lnkRef = spark.read.parquet(f"{adls_path}/ClmLnExtrnlProvPK_lnkRef.parquet")

    # ------------------------------------------------------------------
    # Perform the left join between the main stream and the lookup
    # ------------------------------------------------------------------
    df_join = (
        df_Transform.alias("Transform")
        .join(
            df_lnkRef.alias("lnkRef"),
            (
                (col("Transform.SRC_SYS_CD") == col("lnkRef.SRC_SYS_CD")) &
                (col("Transform.CLM_ID") == col("lnkRef.CLM_ID")) &
                (col("Transform.CLM_LN_SEQ_NO") == col("lnkRef.CLM_LN_SEQ_NO"))
            ),
            "left"
        )
    )

    # ------------------------------------------------------------------
    # Derive transformation columns
    # ------------------------------------------------------------------
    df_enriched = (
        df_join
        .withColumn(
            "SK",
            when(col("lnkRef.CLM_LN_SK").isNull(), lit(0)).otherwise(col("lnkRef.CLM_LN_SK"))
        )
        .withColumn(
            "NewCrtRunCcyExctnSK",
            when(col("lnkRef.CLM_LN_SK").isNull(), lit(CurrRunCycle))
            .otherwise(col("lnkRef.CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # ------------------------------------------------------------------
    # Project final columns for the 'Key' output link
    # ------------------------------------------------------------------
    df_Key = (
        df_enriched.select(
            col("Transform.CLM_ID").alias("CLM_ID"),
            col("Transform.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
            col("Transform.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
            col("NewCrtRunCcyExctnSK").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("SK").alias("CLM_LN_EXTRNL_PROV_SK"),
            col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            col("Transform.DISCARD_IN").alias("DISCARD_IN"),
            col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
            col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            col("Transform.ERR_CT").alias("ERR_CT"),
            col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
            col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            col("Transform.SVC_PROV_ID").alias("SVC_PROV_ID"),
            col("Transform.SVC_PROV_NPI").alias("SVC_PROV_NPI"),
            col("Transform.SVC_PROV_NM").alias("SVC_PROV_NM"),
            col("Transform.SVC_PROV_SPEC_TX").alias("SVC_PROV_SPEC_TX"),
            col("Transform.SVC_PROV_TYP_TX").alias("SVC_PROV_TYP_TX"),
            col("Transform.CDPP_TAXONOMY_CD").alias("CDPP_TAXONOMY_CD"),
            col("Transform.CDPP_SVC_FAC_ST").alias("CDPP_SVC_FAC_ST"),
            col("Transform.SVC_FCLTY_LOC_ZIP_CD").alias("SVC_FCLTY_LOC_ZIP_CD"),
            col("Transform.SVC_FCLTY_LOC_NO").alias("SVC_FCLTY_LOC_NO"),
            col("Transform.SVC_FCLTY_LOC_NO_QLFR_TX").alias("SVC_FCLTY_LOC_NO_QLFR_TX"),
            col("Transform.CDPP_CLASS_PROV").alias("CDPP_CLASS_PROV"),
            col("Transform.SVC_PROV_TYP_PPO_AVLBL_IN").alias("SVC_PROV_TYP_PPO_AVLBL_IN"),
            col("Transform.ADTNL_DATA_ELE_TX").alias("ADTNL_DATA_ELE_TX"),
            col("Transform.ATCHMT_SRC_ID_DTM").alias("ATCHMT_SRC_ID_DTM"),
            col("Transform.SVC_PROV_INDN_HLTH_SVC_IN").alias("SVC_PROV_INDN_HLTH_SVC_IN"),
            col("Transform.FLEX_NTWK_PROV_CST_GRPNG_TX").alias("FLEX_NTWK_PROV_CST_GRPNG_TX"),
            col("Transform.MKT_ID").alias("MKT_ID"),
            col("Transform.HOST_PROV_ITS_TIER_DSGTN_TX").alias("HOST_PROV_ITS_TIER_DSGTN_TX"),
            col("Transform.SVC_PROV_ZIP_CD").alias("SVC_PROV_ZIP_CD"),
            col("Transform.CLM_LN_SK").alias("CLM_LN_SK")
        )
    )

    # ------------------------------------------------------------------
    # Return the final DataFrame for the 'Key' link
    # ------------------------------------------------------------------
    return df_Key
# COMMAND ----------