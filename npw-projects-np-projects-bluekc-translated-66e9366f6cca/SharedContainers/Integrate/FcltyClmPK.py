# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared Container: FcltyClmPK
Description:
* VC LOGS *
^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
^1_2 02/10/09 13:07:43 Batch  15017_47268 INIT bckcetl ids20 dsadm dsadm
^1_1 01/26/09 11:49:42 Batch  15002_42592 PROMOTE bckcetl ids20 dsadm bls for bl
^1_1 01/26/09 11:23:51 Batch  15002_41034 INIT bckcett testIDS dsadm BLS FOR BL
^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


DESCRIPTION:   Shared container used for Primary Keying of Facility Claim

CALLED BY: FctsClmFcltyTrns
                     NascoClmFcltyExtr



MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Bhoomi Dasari    2008-08-06               Initial program                                                               3567 Primary Key    devlIDS                                  Steph Goddard         08/15/2008

Annotations:
- IDS Primary Key Container for Facility Claim
- This job uses hf_clm as the primary key hash file since it has the same natural key as the CLM table
- This container is used in:
  FctsClmFcltyTrns
  NascoClmFcltyExtr

These programs need to be re-compiled when logic changes
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_FcltyClmPK(df_Transform: DataFrame, params: dict) -> DataFrame:
    """
    Executes the FcltyClmPK primary-keying logic.
    
    Parameters
    ----------
    df_Transform : DataFrame
        Input stream representing the 'Transform' link.
    params : dict
        Runtime parameters and configuration values.
    
    Returns
    -------
    DataFrame
        Output stream corresponding to the 'Pkey' link.
    """
    # ------------------------------------------------------------------
    # Unpack required parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle = params["CurrRunCycle"]
    adls_path    = params["adls_path"]
    # ------------------------------------------------------------------
    # Read hash-file lookup (translated to Parquet, scenario c)
    # ------------------------------------------------------------------
    lkup_path = f"{adls_path}/FcltyClmPK_lkup.parquet"
    df_lkup = spark.read.parquet(lkup_path)
    # ------------------------------------------------------------------
    # Join input with lookup
    # ------------------------------------------------------------------
    join_expr = (
        (F.col("t.SRC_SYS_CD") == F.col("l.src_sys_cd")) &
        (F.trim(F.col("t.CLM_ID")) == F.col("l.clm_id"))
    )
    df_joined = (
        df_Transform.alias("t")
        .join(df_lkup.alias("l"), join_expr, "left")
    )
    # ------------------------------------------------------------------
    # Derive transformation columns
    # ------------------------------------------------------------------
    df_enriched = (
        df_joined
        .withColumn(
            "SK",
            F.when(F.col("l.clm_sk").isNull(), F.lit(0)).otherwise(F.col("l.clm_sk"))
        )
        .withColumn(
            "NewCrtRunCycExtcnSk",
            F.when(
                F.col("l.clm_sk").isNull(),
                F.lit(CurrRunCycle)
            ).otherwise(F.col("l.crt_run_cyc_exctn_sk"))
        )
    )
    # ------------------------------------------------------------------
    # Select and map output columns
    # ------------------------------------------------------------------
    df_out = (
        df_enriched.select(
            F.col("t.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("t.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("t.DISCARD_IN").alias("DISCARD_IN"),
            F.col("t.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("t.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("t.ERR_CT").alias("ERR_CT"),
            F.col("t.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("t.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("t.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("SK").alias("FCLTY_CLM_SK"),
            F.col("t.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
            F.col("t.CLM_ID").alias("CLM_ID"),
            F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("t.CLM_SK").alias("CLM_SK"),
            F.col("t.FCLTY_CLM_ADMS_SRC_CD").alias("FCLTY_CLM_ADMS_SRC_CD"),
            F.col("t.FCLTY_CLM_ADMS_TYP_CD").alias("FCLTY_CLM_ADMS_TYP_CD"),
            F.col("t.FCLTY_CLM_BILL_CLS_CD").alias("FCLTY_CLM_BILL_CLS_CD"),
            F.col("t.FCLTY_CLM_BILL_FREQ_CD").alias("FCLTY_CLM_BILL_FREQ_CD"),
            F.col("t.FCLTY_CLM_DSCHG_STTUS_CD").alias("FCLTY_CLM_DSCHG_STTUS_CD"),
            F.col("t.FCLTY_CLM_SUBTYP_CD").alias("FCLTY_CLM_SUBTYP_CD"),
            F.col("t.FCLTY_CLM_PROC_BILL_METH_CD").alias("FCLTY_CLM_PROC_BILL_METH_CD"),
            F.col("t.FCLTY_TYP_CD").alias("FCLTY_TYP_CD"),
            F.col("t.IDS_GNRT_DRG_IN").alias("IDS_GNRT_DRG_IN"),
            F.col("t.ADMS_DT").alias("ADMS_DT"),
            F.col("t.BILL_STMNT_BEG_DT").alias("BILL_STMNT_BEG_DT"),
            F.col("t.BILL_STMNT_END_DT").alias("BILL_STMNT_END_DT"),
            F.col("t.DSCHG_DT").alias("DSCHG_DT"),
            F.col("t.HOSP_BEG_DT").alias("HOSP_BEG_DT"),
            F.col("t.HOSP_END_DT").alias("HOSP_END_DT"),
            F.col("t.ADMS_HR").alias("ADMS_HR"),
            F.col("t.DSCHG_HR").alias("DSCHG_HR"),
            F.col("t.HOSP_COV_DAYS").alias("HOSP_COV_DAYS"),
            F.col("t.LOS_DAYS").alias("LOS_DAYS"),
            F.col("t.ADM_PHYS_PROV_ID").alias("ADM_PHYS_PROV_ID"),
            F.col("t.FCLTY_BILL_TYP_TX").alias("FCLTY_BILL_TYP_TX"),
            F.col("t.GNRT_DRG_CD").alias("GNRT_DRG_CD"),
            F.col("t.MED_RCRD_NO").alias("MED_RCRD_NO"),
            F.col("t.OTHER_PROV_ID_1").alias("OTHER_PROV_ID_1"),
            F.col("t.OTHER_PROV_ID_2").alias("OTHER_PROV_ID_2"),
            F.col("t.SUBMT_DRG_CD").alias("SUBMT_DRG_CD"),
            F.col("t.NRMTV_DRG_CD").alias("NRMTV_DRG_CD"),
            F.col("t.DRG_METHOD_CD").alias("DRG_METHOD_CD")
        )
    )
    # ------------------------------------------------------------------
    # Return the single output stream
    # ------------------------------------------------------------------
    return df_out