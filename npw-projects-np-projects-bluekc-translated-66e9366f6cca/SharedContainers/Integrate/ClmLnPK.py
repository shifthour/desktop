# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared Container: ClmLnPK
Description:
Primary Key logic for IDS CLM_LN Table.

Maintenance History:
- BJ Luce (01/2006): add CLM_LN_SVC_LOC_TYP_CD and NON_PAR_SAV_AMT to common record format
- Steph Goddard (02/07/2007): Changed documentation, added PCTAClmDiagExtr
- Parik (2008-08-05): Primary Key process change
- Kalyan Neelam (12-30-2009): Changed documentation for Medicaid Drug
- Kalyan Neelam (2010-12-24): Updated documentation with new source Medtrak
- Rick Henry (2012-05-01): Added IPCD_TYPE, Proc Code Cate Code and Proc Code Type Code
- Kalyan Neelam (2013-02-19): Added VBB fields
- Manasa Andru (2014-10-17): Added ITS_SUPLMT_DSCNT_AMT and ITS_SRCHRG_AMT
- Manasa Andru (2014-11-17): Added scale adjustment for the two new fields
- Hari Pinnaka (2017-08-21): Added NDC related fields
- Jaideep Mankala (2017-11-20): Added MED / PDX claim indicator
- Madhavan B (2018-02-06): Changed datatype of NDC_UNIT_CT
- Rekha Radhakrishna (2020-08-17): Added APC fields
- Deepika C (2023-08-01): Added SNOMED_CT_CD, CVX_VCCN_CD

Annotations:
If primary key found, assign surrogate key; otherwise, get next key and update hash file.
Used in multiple claim-line extraction and transformation jobs.
Primary key hash file created and loaded by separate PK extraction processes.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_ClmLnPK(df_Transform: DataFrame, params: dict) -> DataFrame:
    """
    Executes the primary-key lookup and surrogate-key assignment for claim-line records.

    Parameters
    ----------
    df_Transform : DataFrame
        Incoming claim-line records requiring primary-key resolution.
    params : dict
        Runtime parameters, already provided by the caller.

    Returns
    -------
    DataFrame
        Claim-line records enriched with primary-key information.
    """
    # ------------------------------------------------------------------
    # Unpack runtime parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle = params["CurrRunCycle"]
    adls_path = params["adls_path"]

    # ------------------------------------------------------------------
    # Read hashed-file (lookup) as Parquet (scenario c)
    # ------------------------------------------------------------------
    lkup_path = f"{adls_path}/ClmLnPK_lkup.parquet"
    df_lkup = spark.read.parquet(lkup_path)

    # ------------------------------------------------------------------
    # Join transform input with lookup to enrich with existing keys
    # ------------------------------------------------------------------
    df_enriched = (
        df_Transform.alias("Transform")
        .join(
            df_lkup.alias("lkup"),
            [
                F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
                F.col("Transform.CLM_ID") == F.col("lkup.CLM_ID"),
                F.col("Transform.CLM_LN_SEQ_NO") == F.col("lkup.CLM_LN_SEQ_NO"),
            ],
            "left",
        )
        .select(
            F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.when(F.col("lkup.CLM_LN_SK").isNotNull(), F.lit("U"))
            .otherwise(F.lit("I"))
            .alias("INSRT_UPDT_CD"),
            F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
            F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("Transform.ERR_CT").alias("ERR_CT"),
            F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.when(F.col("lkup.CLM_LN_SK").isNotNull(), F.col("lkup.CLM_LN_SK"))
            .otherwise(F.lit(0))
            .alias("CLM_LN_SK"),
            F.col("Transform.CLM_ID").alias("CLM_ID"),
            F.col("Transform.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
            F.when(
                F.col("lkup.CLM_LN_SK").isNotNull(),
                F.col("lkup.CRT_RUN_CYC_EXCTN_SK"),
            )
            .otherwise(F.lit(CurrRunCycle))
            .alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("Transform.PROC_CD").alias("PROC_CD"),
            F.col("Transform.SVC_PROV_ID").alias("SVC_PROV_ID"),
            F.col("Transform.CLM_LN_DSALW_EXCD").alias("CLM_LN_DSALW_EXCD"),
            F.col("Transform.CLM_LN_EOB_EXCD").alias("CLM_LN_EOB_EXCD"),
            F.col("Transform.CLM_LN_FINL_DISP_CD").alias("CLM_LN_FINL_DISP_CD"),
            F.col("Transform.CLM_LN_LOB_CD").alias("CLM_LN_LOB_CD"),
            F.col("Transform.CLM_LN_POS_CD").alias("CLM_LN_POS_CD"),
            F.col("Transform.CLM_LN_PREAUTH_CD").alias("CLM_LN_PREAUTH_CD"),
            F.col("Transform.CLM_LN_PREAUTH_SRC_CD").alias("CLM_LN_PREAUTH_SRC_CD"),
            F.col("Transform.CLM_LN_PRICE_SRC_CD").alias("CLM_LN_PRICE_SRC_CD"),
            F.col("Transform.CLM_LN_RFRL_CD").alias("CLM_LN_RFRL_CD"),
            F.col("Transform.CLM_LN_RVNU_CD").alias("CLM_LN_RVNU_CD"),
            F.col("Transform.CLM_LN_ROOM_PRICE_METH_CD").alias(
                "CLM_LN_ROOM_PRICE_METH_CD"
            ),
            F.col("Transform.CLM_LN_ROOM_TYP_CD").alias("CLM_LN_ROOM_TYP_CD"),
            F.col("Transform.CLM_LN_TOS_CD").alias("CLM_LN_TOS_CD"),
            F.col("Transform.CLM_LN_UNIT_TYP_CD").alias("CLM_LN_UNIT_TYP_CD"),
            F.col("Transform.CAP_LN_IN").alias("CAP_LN_IN"),
            F.col("Transform.PRI_LOB_IN").alias("PRI_LOB_IN"),
            F.col("Transform.SVC_END_DT").alias("SVC_END_DT"),
            F.col("Transform.SVC_STRT_DT").alias("SVC_STRT_DT"),
            F.col("Transform.AGMNT_PRICE_AMT").alias("AGMNT_PRICE_AMT"),
            F.col("Transform.ALW_AMT").alias("ALW_AMT"),
            F.col("Transform.CHRG_AMT").alias("CHRG_AMT"),
            F.col("Transform.COINS_AMT").alias("COINS_AMT"),
            F.col("Transform.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
            F.col("Transform.COPAY_AMT").alias("COPAY_AMT"),
            F.col("Transform.DEDCT_AMT").alias("DEDCT_AMT"),
            F.col("Transform.DSALW_AMT").alias("DSALW_AMT"),
            F.col("Transform.ITS_HOME_DSCNT_AMT").alias("ITS_HOME_DSCNT_AMT"),
            F.col("Transform.NO_RESP_AMT").alias("NO_RESP_AMT"),
            F.col("Transform.MBR_LIAB_BSS_AMT").alias("MBR_LIAB_BSS_AMT"),
            F.col("Transform.PATN_RESP_AMT").alias("PATN_RESP_AMT"),
            F.col("Transform.PAYBL_AMT").alias("PAYBL_AMT"),
            F.col("Transform.PAYBL_TO_PROV_AMT").alias("PAYBL_TO_PROV_AMT"),
            F.col("Transform.PAYBL_TO_SUB_AMT").alias("PAYBL_TO_SUB_AMT"),
            F.col("Transform.PROC_TBL_PRICE_AMT").alias("PROC_TBL_PRICE_AMT"),
            F.col("Transform.PROFL_PRICE_AMT").alias("PROFL_PRICE_AMT"),
            F.col("Transform.PROV_WRT_OFF_AMT").alias("PROV_WRT_OFF_AMT"),
            F.col("Transform.RISK_WTHLD_AMT").alias("RISK_WTHLD_AMT"),
            F.col("Transform.SVC_PRICE_AMT").alias("SVC_PRICE_AMT"),
            F.col("Transform.SUPLMT_DSCNT_AMT").alias("SUPLMT_DSCNT_AMT"),
            F.col("Transform.ALW_PRICE_UNIT_CT").alias("ALW_PRICE_UNIT_CT"),
            F.col("Transform.UNIT_CT").alias("UNIT_CT"),
            F.col("Transform.DEDCT_AMT_ACCUM_ID").alias("DEDCT_AMT_ACCUM_ID"),
            F.col("Transform.PREAUTH_SVC_SEQ_NO").alias("PREAUTH_SVC_SEQ_NO"),
            F.col("Transform.RFRL_SVC_SEQ_NO").alias("RFRL_SVC_SEQ_NO"),
            F.col("Transform.LMT_PFX_ID").alias("LMT_PFX_ID"),
            F.col("Transform.PREAUTH_ID").alias("PREAUTH_ID"),
            F.col("Transform.PROD_CMPNT_DEDCT_PFX_ID").alias(
                "PROD_CMPNT_DEDCT_PFX_ID"
            ),
            F.col("Transform.PROD_CMPNT_SVC_PAYMT_ID").alias(
                "PROD_CMPNT_SVC_PAYMT_ID"
            ),
            F.col("Transform.RFRL_ID_TX").alias("RFRL_ID_TX"),
            F.col("Transform.SVC_ID").alias("SVC_ID"),
            F.col("Transform.SVC_PRICE_RULE_ID").alias("SVC_PRICE_RULE_ID"),
            F.col("Transform.SVC_RULE_TYP_TX").alias("SVC_RULE_TYP_TX"),
            F.col("Transform.SVC_LOC_TYP_CD").alias("SVC_LOC_TYP_CD"),
            F.col("Transform.NON_PAR_SAV_AMT").alias("NON_PAR_SAV_AMT"),
            F.col("Transform.PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD"),
            F.col("Transform.PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD"),
            F.col("Transform.VBB_RULE_ID").alias("VBB_RULE_ID"),
            F.col("Transform.VBB_EXCD_ID").alias("VBB_EXCD_ID"),
            F.col("Transform.CLM_LN_VBB_IN").alias("CLM_LN_VBB_IN"),
            F.col("Transform.ITS_SUPLMT_DSCNT_AMT").alias("ITS_SUPLMT_DSCNT_AMT"),
            F.col("Transform.ITS_SRCHRG_AMT").alias("ITS_SRCHRG_AMT"),
            F.col("Transform.NDC").alias("NDC"),
            F.col("Transform.NDC_DRUG_FORM_CD").alias("NDC_DRUG_FORM_CD"),
            F.col("Transform.NDC_UNIT_CT").alias("NDC_UNIT_CT"),
            F.col("Transform.MED_PDX_IND").alias("MED_PDX_IND"),
            F.col("Transform.APC_ID").alias("APC_ID"),
            F.col("Transform.APC_STTUS_ID").alias("APC_STTUS_ID"),
            F.col("Transform.SNOMED_CT_CD").alias("SNOMED_CT_CD"),
            F.col("Transform.CVX_VCCN_CD").alias("CVX_VCCN_CD"),
        )
    )

    # ------------------------------------------------------------------
    # Surrogate-key generation (mandatory placeholder call)
    # ------------------------------------------------------------------
    df_enriched = SurrogateKeyGen(
        df_enriched, <DB sequence name>, "CLM_LN_SK", <schema>, <secret_name>
    )

    # ------------------------------------------------------------------
    # Return the enriched DataFrame (container output link: Key)
    # ------------------------------------------------------------------
    return df_enriched