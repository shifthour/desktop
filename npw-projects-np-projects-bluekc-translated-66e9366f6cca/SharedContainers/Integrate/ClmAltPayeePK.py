# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
* VC LOGS *
^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
^1_2 02/10/09 13:07:43 Batch  15017_47268 INIT bckcetl ids20 dsadm dsadm
^1_1 01/26/09 11:49:42 Batch  15002_42592 PROMOTE bckcetl ids20 dsadm bls for bl
^1_1 01/26/09 11:23:51 Batch  15002_41034 INIT bckcett testIDS dsadm BLS FOR BL
^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


CALLE BY : FctsClmAltPayeeExtr


PROCESSING:    Shared container used for Primary Keying of Claim Alt Payee job
      

MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Bhoomi Dasari    2008-08-06               Initial program                                                               3567 Primary Key    devlIDS                                 Steph Goddard          08/15/2008
"""

# IDS Primary Key Container for Claim Note
# Used by
#
# FctsClmNoteExtr
# IDS Primary Key Container for Claim Alt Payee
# Used by
#
# FctsClmAltPayeeExtr
# This job uses hf_clm as the primary key hash file since it has the same natural key as the CLM table
# This container is used in:
# FctsClmAltPayeExtr
#
# These programs need to be re-compiled when logic changes
# Assign primary surrogate key

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def run_ClmAltPayeePK(df_Transform: DataFrame, params: dict) -> DataFrame:
    """
    PySpark translation of the DataStage shared container 'ClmAltPayeePK'.
    """

    # ------------------------------------------------------------------
    # Unpack required parameters
    # ------------------------------------------------------------------
    CurrRunCycle = params["CurrRunCycle"]
    adls_path = params["adls_path"]

    # ------------------------------------------------------------------
    # Read lookup data (converted hashed file to parquet)
    # ------------------------------------------------------------------
    lkup_path = f"{adls_path}/ClmAltPayeePK_lkup.parquet"
    df_lkup = spark.read.parquet(lkup_path)

    # ------------------------------------------------------------------
    # Join input stream with lookup and derive columns
    # ------------------------------------------------------------------
    df_join = (
        df_Transform.alias("transform")
        .join(
            df_lkup.alias("lkup"),
            [
                F.col("transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
                F.col("transform.CLM_ID") == F.col("lkup.CLM_ID"),
            ],
            "left",
        )
    )

    df_Key = (
        df_join
        .withColumn(
            "SK",
            F.when(F.col("lkup.CLM_SK").isNull(), F.lit(0)).otherwise(F.col("lkup.CLM_SK")),
        )
        .withColumn(
            "NewCrtRunCycExtcnSk",
            F.when(F.col("lkup.CLM_SK").isNull(), F.lit(CurrRunCycle)).otherwise(
                F.col("lkup.CRT_RUN_CYC_EXCTN_SK")
            ),
        )
        .select(
            F.col("transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("transform.DISCARD_IN").alias("DISCARD_IN"),
            F.col("transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("transform.ERR_CT").alias("ERR_CT"),
            F.col("transform.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("SK").alias("CLM_ALT_PAYE_SK"),
            F.col("transform.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
            F.col("transform.CLM_ID").alias("CLM_ID"),
            F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("transform.CLM_SK").alias("CLM_SK"),
            F.col("transform.ALT_PAYE_NM").alias("ALT_PAYE_NM"),
            F.col("transform.ADDR_LN_1").alias("ADDR_LN_1"),
            F.col("transform.ADDR_LN_2").alias("ADDR_LN_2"),
            F.col("transform.ADDR_LN_3").alias("ADDR_LN_3"),
            F.col("transform.CITY_NM").alias("CITY_NM"),
            F.col("transform.CLM_ALT_PAYE_ST_CD").alias("CLM_ALT_PAYE_ST_CD"),
            F.col("transform.POSTAL_CD").alias("POSTAL_CD"),
            F.col("transform.CNTY_NM").alias("CNTY_NM"),
            F.col("transform.CLM_ALT_PAYE_CTRY_CD").alias("CLM_ALT_PAYE_CTRY_CD"),
            F.col("transform.PHN_NO").alias("PHN_NO"),
            F.col("transform.PHN_NO_EXT").alias("PHN_NO_EXT"),
            F.col("transform.FAX_NO").alias("FAX_NO"),
            F.col("transform.FAX_NO_EXT").alias("FAX_NO_EXT"),
            F.col("transform.EMAIL_ADDR").alias("EMAIL_ADDR"),
        )
    )

    return df_Key