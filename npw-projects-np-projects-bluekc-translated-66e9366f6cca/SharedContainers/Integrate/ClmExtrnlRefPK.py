# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Job Name: ClmExtrnlRefPK
Folder: Shared Containers/PrimaryKey

* VC LOGS *
^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
^1_2 02/10/09 13:07:43 Batch  15017_47268 INIT bckcetl ids20 dsadm dsadm
^1_1 01/26/09 11:49:42 Batch  15002_42592 PROMOTE bckcetl ids20 dsadm bls for bl
^1_1 01/26/09 11:23:51 Batch  15002_41034 INIT bckcett testIDS dsadm BLS FOR BL
^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy

*****************************************************************************************************************************************************************************
COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

JOB NAME:     ClmExtrnlRefPkey

DESCRIPTION:  All the work is really done in the extract job.  This job is just a pass thru to have the Job Control loop correctly.

HASH FILES:   hf_cbl - this is both read from and written to.   

MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
------------------- ---------------------------- ---------------------------------------------------------------------------- --------------------------- ------------------------------------ ------------------------- 
Bhoomi Dasari      2008-08-05                   Initial program                                                               3567 Primary Key    devlIDS                                  Steph Goddard         08/15/2008
Kalyan Neelam      2010-02-19                   Changed length of the fields EXTR_REF_ID and PCA_EXTRNL_ID from to 255 (before 20)
Annotation:
Assign primary surrogate key
This container is used in:
FctsClmExtrnlRefDataExtr
These programs need to be re-compiled when logic changes
This job uses hf_clm as the primary key hash file since it has the same natural key as the CLM table
"""
from pyspark.sql import DataFrame, functions as F

def run_ClmExtrnlRefPK(
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # Unpack parameters
    CurrRunCycle = params["CurrRunCycle"]
    adls_path = params["adls_path"]

    # Read hashed-file as parquet (scenario c)
    df_hf_clm = spark.read.parquet(f"{adls_path}/hf_clm.parquet")

    # Deduplicate hashed file on primary keys
    df_hf_clm = dedup_sort(
        df_hf_clm,
        ["SRC_SYS_CD", "CLM_ID"],
        [("SRC_SYS_CD", "A"), ("CLM_ID", "A")]
    )

    # Join Transform stream with lookup
    df_join = (
        df_Transform.alias("Transform")
        .join(
            df_hf_clm.alias("lkup"),
            [
                F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
                F.col("Transform.CLM_ID") == F.col("lkup.CLM_ID")
            ],
            "left"
        )
    )

    # Apply transformation logic
    df_enriched = (
        df_join
        .withColumn(
            "SK",
            F.when(F.col("lkup.CLM_SK").isNull(), F.lit(0)).otherwise(F.col("lkup.CLM_SK"))
        )
        .withColumn(
            "NewCrtRunCycExtcnSk",
            F.when(
                F.col("lkup.CLM_SK").isNull(),
                F.lit(CurrRunCycle)
            ).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # Build output DataFrame
    df_output = df_enriched.select(
        F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
        F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("Transform.ERR_CT").alias("ERR_CT"),
        F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("SK").alias("CLM_SK"),
        F.col("Transform.CLM_ID").alias("CLM_ID"),
        F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Transform.EXTRNL_REF_ID").alias("EXTRNL_REF_ID"),
        F.col("Transform.PCA_EXTRNL_ID").alias("PCA_EXTRNL_ID"),
        F.col("Transform.PCA_SRC_NM").alias("PCA_SRC_NM"),
        F.col("Transform.TRDNG_PRTNR_NM").alias("TRDNG_PRTNR_NM")
    )

    return df_output