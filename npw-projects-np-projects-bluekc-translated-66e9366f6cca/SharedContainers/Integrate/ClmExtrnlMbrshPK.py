# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Job Name: ClmExtrnlMbrshPK
Job Type: Server Job
Category: DS_Integrate
Folder: Shared Containers/PrimaryKey

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION: Shared container used for Primary Keying of Claim External Membership 

PROCESSING: Called by FctsClmExtrnlMbrshExtr


MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Bhoomi               2008-08-06               Initial program                                                               3567 Primary Key    devlIDS                                 Steph Goddard           08/15/2008
Nikhil Sinha       2017-02-28     Data Catalyst - 30001   Add Mbr_SK to CLM_EXTRNL_MBRSH                                                                       Kalyan Neelam            2017-03-07       

Annotations:
- Used by  FctsClmExtrnlMbrshExtr
- IDS Primary Key Container for Claim External Membership
- This container is used in: FctsClmExtrnlMbrshExtr, NascoClmExtrnlMbrshpExtr. These programs need to be re-compiled when logic changes
- This job uses hf_clm as the primary key hash file since it has the same natural key as the CLM table
- Assign primary surrogate key
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when


def run_ClmExtrnlMbrshPK(df_Transform: DataFrame, params: dict) -> DataFrame:
    """
    Executes the logic of the ClmExtrnlMbrshPK shared container.

    Parameters
    ----------
    df_Transform : DataFrame
        Incoming DataFrame corresponding to the 'Transform' input link.
    params : dict
        Runtime parameters dictionary.

    Returns
    -------
    DataFrame
        Resulting DataFrame corresponding to the 'Key' output link.
    """

    # ------------------------------------------------------------------
    # Unpack parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle   = params["CurrRunCycle"]
    adls_path      = params["adls_path"]

    # ------------------------------------------------------------------
    # Read hashed-file (treated as parquet under scenario c)
    # ------------------------------------------------------------------
    hf_clm_path = f"{adls_path}/ClmExtrnlMbrshPK_hf_clm.parquet"
    df_hf_clm = spark.read.parquet(hf_clm_path)

    # Deduplicate on primary keys to mimic hashed-file uniqueness
    df_hf_clm = dedup_sort(
        df_hf_clm,
        partition_cols=["SRC_SYS_CD", "CLM_ID"],
        sort_cols=[("CLM_SK", "D")]
    )

    # ------------------------------------------------------------------
    # Lookup join
    # ------------------------------------------------------------------
    df_joined = (
        df_Transform.alias("t")
        .join(
            df_hf_clm.alias("l"),
            (col("t.SRC_SYS_CD") == col("l.SRC_SYS_CD")) &
            (col("t.CLM_ID")     == col("l.CLM_ID")),
            "left"
        )
    )

    # ------------------------------------------------------------------
    # Derivations
    # ------------------------------------------------------------------
    df_enriched = (
        df_joined
        .withColumn(
            "INSRT_UPDT_CD",
            when(col("l.CLM_SK").isNull(), lit("I")).otherwise(lit("U"))
        )
        .withColumn(
            "CLM_EXTRNL_MBRSH_SK",
            when(col("l.CLM_SK").isNull(), lit(0)).otherwise(col("l.CLM_SK"))
        )
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            when(col("l.CLM_SK").isNull(), lit(CurrRunCycle))
            .otherwise(col("l.CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
        .select(
            col("t.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            col("INSRT_UPDT_CD"),
            col("t.DISCARD_IN").alias("DISCARD_IN"),
            col("t.PASS_THRU_IN").alias("PASS_THRU_IN"),
            col("t.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            col("t.ERR_CT").alias("ERR_CT"),
            col("t.RECYCLE_CT").alias("RECYCLE_CT"),
            col("t.SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("t.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            col("CLM_EXTRNL_MBRSH_SK"),
            col("t.CLM_ID").alias("CLM_ID"),
            col("CRT_RUN_CYC_EXCTN_SK"),
            col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("t.MBR_GNDR_CD_CD").alias("MBR_GNDR_CD_CD"),
            col("t.MBR_RELSHP_CD_CD").alias("MBR_RELSHP_CD_CD"),
            col("t.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
            col("t.GRP_NM").alias("GRP_NM"),
            col("t.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
            col("t.MBR_MIDINIT").alias("MBR_MIDINIT"),
            col("t.MBR_LAST_NM").alias("MBR_LAST_NM"),
            col("t.PCKG_CD_ID").alias("PCKG_CD_ID"),
            col("t.PATN_NTWK_SH_NM").alias("PATN_NTWK_SH_NM"),
            col("t.SUB_GRP_BASE_NO").alias("SUB_GRP_BASE_NO"),
            col("t.SUB_GRP_SECT_NO").alias("SUB_GRP_SECT_NO"),
            col("t.SUB_ID").alias("SUB_ID"),
            col("t.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
            col("t.SUB_MIDINIT").alias("SUB_MIDINIT"),
            col("t.SUB_LAST_NM").alias("SUB_LAST_NM"),
            col("t.SUB_ADDR_LN_1").alias("SUB_ADDR_LN_1"),
            col("t.SUB_ADDR_LN_2").alias("SUB_ADDR_LN_2"),
            col("t.SUB_CITY_NM").alias("SUB_CITY_NM"),
            col("t.CLM_EXTRNL_MBRSH_SUB_ST_CD").alias("CLM_EXTRNL_MBRSH_SUB_ST_CD"),
            col("t.SUB_CNTY_FIPS_ID").alias("SUB_CNTY_FIPS_ID"),
            col("t.SUB_POSTAL_CD").alias("SUB_POSTAL_CD"),
            col("t.SUBMT_SUB_ID").alias("SUBMT_SUB_ID"),
            col("t.ACTL_SUB_ID").alias("ACTL_SUB_ID"),
            col("t.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
        )
    )

    # ------------------------------------------------------------------
    # Surrogate-key generation
    # ------------------------------------------------------------------
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "CLM_EXTRNL_MBRSH_SK",
        <schema>,
        <secret_name>
    )

    # ------------------------------------------------------------------
    # Return the output link
    # ------------------------------------------------------------------
    return df_enriched