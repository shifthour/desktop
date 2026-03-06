# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
JobName     : ClmPK
JobType     : Server Job
JobCategory : DS_Integrate
FolderPath  : Shared Containers/PrimaryKey
Description :
MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #                   Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        --------------------------------------     ------------------------------------       ----------------------------      -------------------------
Bhoomi Dasari    2008-08-19              Initial Program                                                               3567 Primary Key                devlIDS                                 Steph Goddard           08/20/2008
Ralph Tucker     2009-10-28              Added new field: REMIT_SUPRSION_AMT                TTR-368                             devlIDS                                 Steph Goddard         08/21/2009
Kalyan Neelam   2009-12-30              Added documentation for Medicaid Drug                      4110                                    IntegrateCurDevl                   Steph Goddard         01/11/2010
Kalyan Neelam   2010-02-09              Added two new fields - MCAID_STTUS_ID,                  
                                                           PATN_PD_AMT                                                           4278                                    IntegrateCurDevl                   Steph Goddard         03/08/2010
Kalyan Neelam   2010-12-24               Updated documentation with new source Medtrak      4616                                    IntegrateNewDevl                 Steph Goddard         12/28/2010
Ralph Tucker     2012-04-24               Added one new field:
                                                                  CLM_SUBMT_ICD_VRSN_CD                             4896 - Edw Remediation     IntegrateNewDevl                SAndrew                     2012-05-20

Mohan Karnati    2019-06-13            Added one new field: CLM_TXNMY_CD                 73034-NP                             IntegrateDev1                                                              Kalyan Neelam            2019-07-01

Ramu                  2020-03-13      6131-PBM Replacement       Mapped new column BILL_PAYMT_EXCL_IN              IntegrateDev2                              Kalyan Neelam         2020-04-07

Annotation:
If primary key found, assign surrogate key, otherwise get next key and update hash file.
This container is used in:
ESIClmExtr
FctsClmExtr
MCSourceClmExtr
MedicaidClmExtr
NascoClmExtr
PcsClmExtr
PCTAClmExtr
WellDyneClmExtr
MedtrakClmExtr
BCBSSCClmExtr
BCBSKCCommClmExtr

These programs need to be re-compiled when logic changes
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

def run_ClmPK(
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes shared-container logic for primary-key assignment on claim records.
    
    Parameters
    ----------
    df_Transform : DataFrame
        Incoming DataFrame corresponding to the container input link “Transform”.
    params : dict
        Runtime parameters including JDBC configs, adls paths, owners, run-cycle keys, etc.
    
    Returns
    -------
    DataFrame
        DataFrame emitted on the container output link “Key”.
    """

    # --------------------------------------------------
    # Unpack parameters (once and only once)
    CurrRunCycle      = params["CurrRunCycle"]
    adls_path         = params["adls_path"]
    # --------------------------------------------------

    # --------------------------------------------------
    # Read lookup parquet that replaces hashed-file “hf_clm”
    lkup_path = f"{adls_path}/ClmPK_lkup.parquet"
    df_lkup_raw = spark.read.parquet(lkup_path)

    # Ensure uniqueness of key columns to mimic hashed-file behaviour
    df_lkup = dedup_sort(
        df_lkup_raw,
        partition_cols=["SRC_SYS_CD", "CLM_ID"],
        sort_cols=[("CRT_RUN_CYC_EXCTN_SK", "D")]
    )
    # --------------------------------------------------

    # --------------------------------------------------
    # Join main stream with lookup
    df_enriched = (
        df_Transform.alias("t")
        .join(
            df_lkup.alias("l"),
            (F.col("t.SRC_SYS_CD") == F.col("l.SRC_SYS_CD")) &
            (F.col("t.CLM_ID")    == F.col("l.CLM_ID")),
            how="left"
        )
        .withColumn(
            "SK",
            F.when(F.col("l.CLM_SK").isNull(), F.lit(0)).otherwise(F.col("l.CLM_SK"))
        )
        .withColumn(
            "NewCrtRunCycExtcnSk",
            F.when(F.col("l.CLM_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("l.CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # --------------------------------------------------
    # Surrogate-key generation when lookup miss (SK == 0)
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'CLM_SK',<schema>,<secret_name>)
    # --------------------------------------------------

    # --------------------------------------------------
    # Select and alias output columns
    df_key = (
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
            F.col("CLM_SK").alias("CLM_SK"),
            F.col("t.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
            F.col("t.CLM_ID").alias("CLM_ID"),
            F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("t.ADJ_FROM_CLM").alias("ADJ_FROM_CLM"),
            F.col("t.ADJ_TO_CLM").alias("ADJ_TO_CLM"),
            F.col("t.ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
            F.col("t.CLM_EOB_EXCD").alias("CLM_EOB_EXCD"),
            F.col("t.CLS").alias("CLS"),
            F.col("t.CLS_PLN").alias("CLS_PLN"),
            F.col("t.EXPRNC_CAT").alias("EXPRNC_CAT"),
            F.col("t.FNCL_LOB_NO").alias("FNCL_LOB_NO"),
            F.col("t.GRP").alias("GRP"),
            F.col("t.MBR_CK").alias("MBR_CK"),
            F.col("t.NTWK").alias("NTWK"),
            F.col("t.PROD").alias("PROD"),
            F.col("t.SUBGRP").alias("SUBGRP"),
            F.col("t.SUB_CK").alias("SUB_CK"),
            F.col("t.CLM_ACDNT_CD").alias("CLM_ACDNT_CD"),
            F.col("t.CLM_ACDNT_ST_CD").alias("CLM_ACDNT_ST_CD"),
            F.col("t.CLM_ACTIVATING_BCBS_PLN_CD").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
            F.col("t.CLM_AGMNT_SRC_CD").alias("CLM_AGMNT_SRC_CD"),
            F.col("t.CLM_BTCH_ACTN_CD").alias("CLM_BTCH_ACTN_CD"),
            F.col("t.CLM_CAP_CD").alias("CLM_CAP_CD"),
            F.col("t.CLM_CAT_CD").alias("CLM_CAT_CD"),
            F.col("t.CLM_CHK_CYC_OVRD_CD").alias("CLM_CHK_CYC_OVRD_CD"),
            F.col("t.CLM_COB_CD").alias("CLM_COB_CD"),
            F.col("t.FINL_DISP_CD").alias("FINL_DISP_CD"),
            F.col("t.CLM_INPT_METH_CD").alias("CLM_INPT_METH_CD"),
            F.col("t.CLM_INPT_SRC_CD").alias("CLM_INPT_SRC_CD"),
            F.col("t.CLM_IPP_CD").alias("CLM_IPP_CD"),
            F.col("t.CLM_NTWK_STTUS_CD").alias("CLM_NTWK_STTUS_CD"),
            F.col("t.CLM_NONPAR_PROV_PFX_CD").alias("CLM_NONPAR_PROV_PFX_CD"),
            F.col("t.CLM_OTHER_BNF_CD").alias("CLM_OTHER_BNF_CD"),
            F.col("t.CLM_PAYE_CD").alias("CLM_PAYE_CD"),
            F.col("t.CLM_PRCS_CTL_AGNT_PFX_CD").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
            F.col("t.CLM_SVC_DEFN_PFX_CD").alias("CLM_SVC_DEFN_PFX_CD"),
            F.col("t.CLM_SVC_PROV_SPEC_CD").alias("CLM_SVC_PROV_SPEC_CD"),
            F.col("t.CLM_SVC_PROV_TYP_CD").alias("CLM_SVC_PROV_TYP_CD"),
            F.col("t.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
            F.col("t.CLM_SUBMT_BCBS_PLN_CD").alias("CLM_SUBMT_BCBS_PLN_CD"),
            F.col("t.CLM_SUB_BCBS_PLN_CD").alias("CLM_SUB_BCBS_PLN_CD"),
            F.col("t.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
            F.col("t.CLM_TYP_CD").alias("CLM_TYP_CD"),
            F.col("t.ATCHMT_IN").alias("ATCHMT_IN"),
            F.col("t.CLM_CLNCL_EDIT_CD").alias("CLM_CLNCL_EDIT_CD"),
            F.col("t.COBRA_CLM_IN").alias("COBRA_CLM_IN"),
            F.col("t.FIRST_PASS_IN").alias("FIRST_PASS_IN"),
            F.col("t.HOST_IN").alias("HOST_IN"),
            F.col("t.LTR_IN").alias("LTR_IN"),
            F.col("t.MCARE_ASG_IN").alias("MCARE_ASG_IN"),
            F.col("t.NOTE_IN").alias("NOTE_IN"),
            F.col("t.PCA_AUDIT_IN").alias("PCA_AUDIT_IN"),
            F.col("t.PCP_SUBMT_IN").alias("PCP_SUBMT_IN"),
            F.col("t.PROD_OOA_IN").alias("PROD_OOA_IN"),
            F.col("t.ACDNT_DT").alias("ACDNT_DT"),
            F.col("t.INPT_DT").alias("INPT_DT"),
            F.col("t.MBR_PLN_ELIG_DT").alias("MBR_PLN_ELIG_DT"),
            F.col("t.NEXT_RVW_DT").alias("NEXT_RVW_DT"),
            F.col("t.PD_DT").alias("PD_DT"),
            F.col("t.PAYMT_DRAG_CYC_DT").alias("PAYMT_DRAG_CYC_DT"),
            F.col("t.PRCS_DT").alias("PRCS_DT"),
            F.col("t.RCVD_DT").alias("RCVD_DT"),
            F.col("t.SVC_STRT_DT").alias("SVC_STRT_DT"),
            F.col("t.SVC_END_DT").alias("SVC_END_DT"),
            F.col("t.SMLR_ILNS_DT").alias("SMLR_ILNS_DT"),
            F.col("t.STTUS_DT").alias("STTUS_DT"),
            F.col("t.WORK_UNABLE_BEG_DT").alias("WORK_UNABLE_BEG_DT"),
            F.col("t.WORK_UNABLE_END_DT").alias("WORK_UNABLE_END_DT"),
            F.col("t.ACDNT_AMT").alias("ACDNT_AMT"),
            F.col("t.ACTL_PD_AMT").alias("ACTL_PD_AMT"),
            F.col("t.ALLOW_AMT").alias("ALLOW_AMT"),
            F.col("t.DSALW_AMT").alias("DSALW_AMT"),
            F.col("t.COINS_AMT").alias("COINS_AMT"),
            F.col("t.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
            F.col("t.COPAY_AMT").alias("COPAY_AMT"),
            F.col("t.CHRG_AMT").alias("CHRG_AMT"),
            F.col("t.DEDCT_AMT").alias("DEDCT_AMT"),
            F.col("t.PAYBL_AMT").alias("PAYBL_AMT"),
            F.col("t.CLM_CT").alias("CLM_CT"),
            F.col("t.MBR_AGE").alias("MBR_AGE"),
            F.col("t.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
            F.col("t.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
            F.col("t.DOC_TX_ID").alias("DOC_TX_ID"),
            F.col("t.MCAID_RESUB_NO").alias("MCAID_RESUB_NO"),
            F.col("t.MCARE_ID").alias("MCARE_ID"),
            F.col("t.MBR_SFX_NO").alias("MBR_SFX_NO"),
            F.col("t.PATN_ACCT_NO").alias("PATN_ACCT_NO"),
            F.col("t.PAYMT_REF_ID").alias("PAYMT_REF_ID"),
            F.col("t.PROV_AGMNT_ID").alias("PROV_AGMNT_ID"),
            F.col("t.RFRNG_PROV_TX").alias("RFRNG_PROV_TX"),
            F.col("t.SUB_ID").alias("SUB_ID"),
            F.col("t.PRPR_ENTITY").alias("PRPR_ENTITY"),
            F.col("t.PCA_TYP_CD").alias("PCA_TYP_CD"),
            F.col("t.REL_PCA_CLM_ID").alias("REL_PCA_CLM_ID"),
            F.col("t.CLCL_MICRO_ID").alias("CLCL_MICRO_ID"),
            F.col("t.CLM_UPDT_SW").alias("CLM_UPDT_SW"),
            F.col("t.REMIT_SUPRSION_AMT").alias("REMIT_SUPRSION_AMT"),
            F.col("t.MCAID_STTUS_ID").alias("MCAID_STTUS_ID"),
            F.col("t.PATN_PD_AMT").alias("PATN_PD_AMT"),
            F.col("t.CLM_SUBMT_ICD_VRSN_CD").alias("CLM_SUBMT_ICD_VRSN_CD"),
            F.col("t.CLM_TXNMY_CD").alias("CLM_TXNMY_CD"),
            F.col("t.BILL_PAYMT_EXCL_IN").alias("BILL_PAYMT_EXCL_IN")
        )
    )
    # --------------------------------------------------
    return df_key
# COMMAND ----------