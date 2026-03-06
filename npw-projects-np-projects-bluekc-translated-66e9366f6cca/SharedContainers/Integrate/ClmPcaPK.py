# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

def run_ClmPcaPK(df_Transform: DataFrame, params: dict) -> DataFrame:
    """
    Job Name: ClmPcaPK
    
    * VC LOGS *
    ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
    ^1_2 02/10/09 13:07:43 Batch  15017_47268 INIT bckcetl ids20 dsadm dsadm
    ^1_1 01/26/09 11:49:42 Batch  15002_42592 PROMOTE bckcetl ids20 dsadm bls for bl
    ^1_1 01/26/09 11:23:51 Batch  15002_41034 INIT bckcett testIDS dsadm BLS FOR BL
    ^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
    ^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy
    
    *****************************************************************************************************************************************************************************
    COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
    
    JOB NAME:     ClmPcaPK
    
    DESCRIPTION:     All the work is really done in the extract job.  This job is just a pass thru to have the Job Control loop correctly.
    
    INPUTS:
    
    HASH FILES:     hf_clm - this is read.
    
    PROCESSING: Shared container used for Primary Keying of Claim PCA job               
    
    
    MODIFICATIONS:
    Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
    -----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
    Bhoomi Dasari    2008-08-19               Initial program                                                               3567 Primary Key    devlIDS                               Steph Goddard           08/20/2008
    
    Annotations:
    This container is used in:
    FctsClmPcaExtr
    
    These programs need to be re-compiled when logic changes
    
    Since the key to this table is src_sys_cd and clm_id, it is using the hf_clm hash file for primary key
    """
    # Unpack parameters
    CurrRunCycle = params["CurrRunCycle"]
    adls_path = params["adls_path"]
    
    # Read hash file (scenario c → parquet file)
    hf_clm_path = f"{adls_path}/ClmPcaPK_lkup.parquet"
    df_hf_clm = spark.read.parquet(hf_clm_path)
    
    # Deduplicate hashed file by key columns
    df_hf_clm = dedup_sort(df_hf_clm, ["SRC_SYS_CD", "CLM_ID"], [("CRT_RUN_CYC_EXCTN_SK", "D")])
    
    # Prepare aliases for join
    df_transform_alias = df_Transform.alias("Transform")
    df_hf_alias = df_hf_clm.alias("lkup")
    
    join_expr = [
        F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
        F.col("Transform.CLM_ID") == F.col("lkup.CLM_ID")
    ]
    
    df_joined = df_transform_alias.join(df_hf_alias, join_expr, "left")
    
    # Transformation columns
    df_joined = (
        df_joined
        .withColumn(
            "SK",
            F.when(F.col("lkup.CLM_SK").isNull(), F.lit(0)).otherwise(F.col("lkup.CLM_SK"))
        )
        .withColumn(
            "NewCrtRunCycExtcnSk",
            F.when(F.col("lkup.CLM_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
        )
    )
    
    # Output mapping
    df_Key = (
        df_joined
        .select(
            F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
            F.col("Transform.ROW_PASS_THRU").alias("ROW_PASS_THRU"),
            F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("Transform.ERR_CT").alias("ERR_CT"),
            F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("SK").alias("CLM_SK"),
            F.col("Transform.CLM_ID").alias("CLM_ID"),
            F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("Transform.TOT_CNSD_AMT").alias("TOT_CNSD_AMT"),
            F.col("Transform.TOT_PD_AMT").alias("TOT_PD_AMT"),
            F.col("Transform.SUB_TOT_PCA_AVLBL_TO_DT_AMT").alias("SUB_TOT_PCA_AVLBL_TO_DT_AMT"),
            F.col("Transform.SUB_TOT_PCA_PD_TO_DT_AMT").alias("SUB_TOT_PCA_PD_TO_DT_AMT")
        )
    )
    
    return df_Key