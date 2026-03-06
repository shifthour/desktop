# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_5 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_4 02/02/09 09:52:49 Batch  15009_35578 PROMOTE bckcetl ids20 dsadm bls for brent
# MAGIC ^1_4 01/26/09 13:03:15 Batch  15002_46999 INIT bckcett testIDS dsadm bls for bl
# MAGIC ^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
# MAGIC ^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_10 12/20/07 10:05:54 Batch  14599_36360 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_10 12/20/07 09:54:53 Batch  14599_35696 INIT bckcett testIDS30 dsadm bls for sa
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 10/02/07 14:11:36 Batch  14520_51104 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_3 10/02/07 13:36:29 Batch  14520_48996 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_2 09/30/07 15:50:06 Batch  14518_57011 PROMOTE bckcett testIDS30 u03651 staeffy
# MAGIC ^1_2 09/30/07 15:29:43 Batch  14518_55788 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_2 09/29/07 17:49:00 Batch  14517_64144 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 09/25/07 12:58:00 Batch  14513_46683 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_4 05/16/07 11:43:58 Batch  14381_42243 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_3 05/03/07 12:56:52 Batch  14368_46619 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_3 03/26/07 12:34:37 Batch  14330_45286 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 03/07/07 15:24:05 Batch  14311_55447 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_8 12/18/06 15:24:00 Batch  14232_55459 PROMOTE bckcetl ids20 dsadm Keith for Ralph install 12/18/2006
# MAGIC ^1_8 12/18/06 14:39:27 Batch  14232_52797 INIT bckcett testIDS30 dsadm Keith for Ralph CDHP Install 12182006
# MAGIC ^1_1 12/11/06 16:27:40 Batch  14225_59263 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsClmLnDsalwExtr
# MAGIC CALLED BY:  FctsClmExtr1Seq
# MAGIC  
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   create a row for CLM_LN_DSALW for each row on each input hash file
# MAGIC   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Steph Goddard        06/15/2004-                                   Originally Programmed
# MAGIC Steph Goddard       10/19/2004                                     Facets 4.11 changes - disallow amounts now on table 
# MAGIC                                                                                        CMC_CDMD_LI_DISALL not line item
# MAGIC BJ Luce                   03/07/2006                                    put all logic to determine rows for disallows in 
# MAGIC                                                                                        FctsClmLnHashFileExtr. pull in hash files and concatenate them 
# MAGIC                                                                                        together to build CLM_LN_DSALW  add primary key
# MAGIC                                 03/16/2006                                    add hf_clm_ln_dsalw_cap
# MAGIC Brent Leland           08/25/2006                                     Added logic to remove records of disallow type  DA when a 
# MAGIC                                                                                        corresponding DTDU rows exists for the same claim line 
# MAGIC                                                                                        sequence number.
# MAGIC Sanderw                 12/08/2006     Project 1576            Reversal logix added for new status codes 89 and  and 99
# MAGIC Brent Leland           05/02/2007     IAD Prod. Supp.      Added current timestamp parameter to replace FORMAT.DATE  devlIDS30
# MAGIC                                                                                        routine call to improve effiecency.
# MAGIC                                                                                        Made metadata from hashfile cddd_dsalw same as others.
# MAGIC Oliver Nielsen          08/15/2007    Balancing                 Added Balancing Snapshot File                                                    devlIDS30                    Steph Goddard             8/30/07
# MAGIC Brent Leland            12/18/2007    IAD Prod. Supp.      Added trim to Combine.DSALW_EXCD in Remove_DA_Rows     devlIDS30                    Steph Goddard            12/18/2007 
# MAGIC                                                                                        transform to eliminate FKey errors for EXCD lookup. 
# MAGIC 
# MAGIC Parik                       2008-07-25     3567(Primary Key)    Added the Primary key process to Claim Line Disallow job             devlIDS                         Steph Goddard           07/29/2008

# MAGIC When there is a DA and DTDU row for the same claim line sequence, discard the DA row.
# MAGIC Balancing
# MAGIC Input to this process is created in FctsClmLnHashFileExtr
# MAGIC Hash file (hf_clm_ln_dsalw_allcol) cleared from the container - ClmLnDsalwPK
# MAGIC Write output to .../key
# MAGIC Hash files created in the FctsClmLnHashFileExtr are also used in FctsClmLnDntlTrns.
# MAGIC 
# MAGIC Do Not Clear in this process
# MAGIC created in FctsClmLnHashFileExtr
# MAGIC              used in FctsClmLnTrns, FctsClmLnDntlTrns, FctsClmLnDsalwExtr
# MAGIC 
# MAGIC                    hf_clm_ln_dsalw_tu
# MAGIC                    hf_clm_ln_dsalw_a
# MAGIC                    hf_clm_ln_dsalw_x
# MAGIC                    hf_clm_ln_dsalw_cdmd
# MAGIC                    hf_clm_ln_dsalw_cdml 
# MAGIC                    hf_clm_ln_dsalw_cddd
# MAGIC                    hf_clm_ln_dsalw_cddl 
# MAGIC                    hf_clm_ln_dsalw_cap
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, length, concat_ws, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnDsalwPK
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

df_dntl_disalw_a_dsalw = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_a.parquet")
df_dntl_disalw_dtdu = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_tu.parquet")

df_has135_cdmd_dsalw = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_cdmd.parquet")
df_has135_cdml_dsalw = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_cdml.parquet")
df_has135_a_dsalw = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_a.parquet")
df_has135_x_dsalw = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_x.parquet")
df_has135_tu_dsalw = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_tu.parquet")
df_has135_cddl_dsalw = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_cddl.parquet")
df_has135_cddd_dsalw = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_cddd.parquet")
df_has135_cap_dsalw = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_cap.parquet")

df_cdmd_dsalw = df_has135_cdmd_dsalw.select(
    col("CLCL_ID"),
    col("CDML_SEQ_NO"),
    col("DSALW_TYP"),
    col("DSALW_AMT"),
    col("DSALW_EXCD"),
    col("EXCD_RESP_CD"),
    col("CDML_DISALL_AMT"),
    col("BYPS_IN")
)
df_cdml_dsalw = df_has135_cdml_dsalw.select(
    col("CLCL_ID"),
    col("CDML_SEQ_NO"),
    col("DSALW_TYP"),
    col("DSALW_AMT"),
    col("DSALW_EXCD"),
    col("EXCD_RESP_CD"),
    col("CDML_DISALL_AMT"),
    col("BYPS_IN")
)
df_a_dsalw_135 = df_has135_a_dsalw.select(
    col("CLCL_ID"),
    col("CDML_SEQ_NO"),
    col("DSALW_TYP"),
    col("DSALW_AMT"),
    col("DSALW_EXCD"),
    col("EXCD_RESP_CD"),
    col("CDML_DISALL_AMT"),
    col("BYPS_IN")
)
df_x_dsalw = df_has135_x_dsalw.select(
    col("CLCL_ID"),
    col("CDML_SEQ_NO"),
    col("DSALW_TYP"),
    col("DSALW_AMT"),
    col("DSALW_EXCD"),
    col("EXCD_RESP_CD"),
    col("CDML_DISALL_AMT"),
    col("BYPS_IN")
)
df_tu_dsalw_135 = df_has135_tu_dsalw.select(
    col("CLCL_ID"),
    col("CDML_SEQ_NO"),
    col("DSALW_TYP"),
    col("DSALW_AMT"),
    col("DSALW_EXCD"),
    col("EXCD_RESP_CD"),
    col("CDML_DISALL_AMT"),
    col("BYPS_IN")
)
df_cddl_dsalw = df_has135_cddl_dsalw.select(
    col("CLCL_ID"),
    col("CDML_SEQ_NO"),
    col("DSALW_TYP"),
    col("DSALW_AMT"),
    col("DSALW_EXCD"),
    col("EXCD_RESP_CD"),
    col("CDML_DISALL_AMT"),
    col("BYPS_IN")
)
df_cddd_dsalw = df_has135_cddd_dsalw.select(
    col("CLCL_ID"),
    col("CDML_SEQ_NO"),
    col("DSALW_TYP"),
    col("DSALW_AMT"),
    col("DSALW_EXCD"),
    col("EXCD_RESP_CD"),
    col("CDML_DISALL_AMT"),
    col("BYPS_IN")
)
df_cap_dsalw = df_has135_cap_dsalw.select(
    col("CLCL_ID"),
    col("CDML_SEQ_NO"),
    col("DSALW_TYP"),
    col("DSALW_AMT"),
    col("DSALW_EXCD"),
    col("EXCD_RESP_CD"),
    col("CDML_DISALL_AMT"),
    col("BYPS_IN")
)

df_combine = (
    df_cdmd_dsalw
    .unionByName(df_cdml_dsalw)
    .unionByName(df_a_dsalw_135)
    .unionByName(df_x_dsalw)
    .unionByName(df_tu_dsalw_135)
    .unionByName(df_cddl_dsalw)
    .unionByName(df_cddd_dsalw)
    .unionByName(df_cap_dsalw)
)

df_remove_da_join = (
    df_combine.alias("Combine")
    .join(
        df_dntl_disalw_a_dsalw.alias("a_dsalw"),
        [
            col("Combine.CLCL_ID") == col("a_dsalw.CLCL_ID"),
            col("Combine.CDML_SEQ_NO") == col("a_dsalw.CDML_SEQ_NO"),
            col("Combine.DSALW_TYP") == col("a_dsalw.DSALW_TYP")
        ],
        how="left"
    )
    .join(
        df_dntl_disalw_dtdu.alias("DTDU"),
        [
            col("Combine.CLCL_ID") == col("DTDU.CLCL_ID"),
            col("Combine.CDML_SEQ_NO") == col("DTDU.CDML_SEQ_NO"),
            lit("DTDU") == col("DTDU.DSALW_TYP")
        ],
        how="left"
    )
)

df_remove_da_vars = (
    df_remove_da_join
    .withColumn("CurrRowDATyp", (trim(col("Combine.DSALW_TYP")) == lit("DA")) & col("a_dsalw.CLCL_ID").isNotNull())
    .withColumn("DULkup", when(col("DTDU.CLCL_ID").isNotNull(), lit("Y")).otherwise(lit("N")))
    .withColumn(
        "Keep",
        when(
            col("CurrRowDATyp"),
            when(col("DULkup") == lit("Y"), lit(False)).otherwise(lit(True))
        ).otherwise(lit(True))
    )
)

df_disalw_out = (
    df_remove_da_vars
    .filter(col("Keep"))
    .select(
        col("Combine.CLCL_ID").alias("CLCL_ID"),
        col("Combine.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        col("Combine.DSALW_TYP").alias("DSALW_TYP"),
        col("Combine.DSALW_AMT").alias("DSALW_AMT"),
        trim(col("Combine.DSALW_EXCD")).alias("DSALW_EXCD"),
        col("Combine.EXCD_RESP_CD").alias("EXCD_RESP_CD"),
        col("Combine.CDML_DISALL_AMT").alias("CDML_DISALL_AMT"),
        col("Combine.BYPS_IN").alias("BYPS_IN")
    )
)

df_setup_crf_join = (
    df_disalw_out.alias("Disalw_out")
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        col("Disalw_out.CLCL_ID") == col("fcts_reversals.CLCL_ID"),
        how="left"
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        col("Disalw_out.CLCL_ID") == col("nasco_dup_lkup.CLM_ID"),
        how="left"
    )
)

df_setup_crf_vars = (
    df_setup_crf_join
    .withColumn("ClmId", trim(col("Disalw_out.CLCL_ID")))
    .withColumn("TypCd", trim(col("Disalw_out.DSALW_TYP")))
)

df_dsalws = (
    df_setup_crf_vars
    .filter(col("nasco_dup_lkup.CLM_ID").isNull())
    .select(
        lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(lit("N"), 1, " ").alias("DISCARD_IN"),
        rpad(lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        lit(CurrDate).alias("FIRST_RECYC_DT"),
        lit(0).alias("ERR_CT"),
        lit(0).alias("RECYCLE_CT"),
        col("SrcSysCd").alias("SRC_SYS_CD"),
        concat_ws(";", col("SrcSysCd"), col("ClmId"), col("Disalw_out.CDML_SEQ_NO"), col("TypCd")).alias("PRI_KEY_STRING"),
        lit(0).alias("CLM_LN_DSALW_SK"),
        rpad(col("ClmId"), 18, " ").alias("CLM_ID"),
        col("Disalw_out.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        rpad(col("TypCd"), 2, " ").alias("CLM_LN_DSALW_TYP_CD"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        rpad(
            when(length(trim(col("Disalw_out.DSALW_EXCD"))) > 0, col("Disalw_out.DSALW_EXCD")).otherwise(lit("NA")),
            3, " "
        ).alias("CLM_LN_DSALW_EXCD"),
        col("Disalw_out.DSALW_AMT").alias("DSALW_AMT")
    )
)

df_reversals = (
    df_setup_crf_vars
    .filter(
        col("fcts_reversals.CLCL_ID").isNotNull() &
        (
            (col("fcts_reversals.CLCL_CUR_STS") == lit("89")) |
            (col("fcts_reversals.CLCL_CUR_STS") == lit("91")) |
            (col("fcts_reversals.CLCL_CUR_STS") == lit("99"))
        )
    )
    .select(
        lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(lit("N"), 1, " ").alias("DISCARD_IN"),
        rpad(lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        lit(CurrDate).alias("FIRST_RECYC_DT"),
        lit(0).alias("ERR_CT"),
        lit(0).alias("RECYCLE_CT"),
        col("SrcSysCd").alias("SRC_SYS_CD"),
        concat_ws(";", col("SrcSysCd"), col("ClmId"), lit("R"), col("Disalw_out.CDML_SEQ_NO"), col("TypCd")).alias("PRI_KEY_STRING"),
        lit(0).alias("CLM_LN_DSALW_SK"),
        rpad(concat_ws("", col("ClmId"), lit("R")), 18, " ").alias("CLM_ID"),
        col("Disalw_out.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        rpad(col("TypCd"), 2, " ").alias("CLM_LN_DSALW_TYP_CD"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        rpad(
            when(length(trim(col("Disalw_out.DSALW_EXCD"))) > 0, col("Disalw_out.DSALW_EXCD")).otherwise(lit("NA")),
            3, " "
        ).alias("CLM_LN_DSALW_EXCD"),
        when(
            (col("Disalw_out.DSALW_AMT").cast("double").isNotNull()) &
            (col("Disalw_out.DSALW_AMT").cast("double") > 0),
            -col("Disalw_out.DSALW_AMT").cast("double")
        ).otherwise(col("Disalw_out.DSALW_AMT")).alias("DSALW_AMT")
    )
)

df_lnkIn = df_reversals.unionByName(df_dsalws)

df_AllCol = df_lnkIn.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("CLM_LN_DSALW_TYP_CD").alias("CLM_LN_DSALW_TYP_CD"),
    col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("CLM_LN_DSALW_SK").alias("CLM_LN_DSALW_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_LN_DSALW_EXCD").alias("CLM_LN_DSALW_EXCD"),
    col("DSALW_AMT").alias("DSALW_AMT")
)

df_Snapshot = df_lnkIn.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLCL_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("CLM_LN_DSALW_TYP_CD").alias("CLM_LN_DSALW_TYP_CD")
)

df_Transform = df_lnkIn.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("CLM_LN_DSALW_TYP_CD").alias("CLM_LN_DSALW_TYP_CD")
)

df_transformer_vars = df_Snapshot.withColumn(
    "svClmLnDslwTypCdSk",
    GetFkeyCodes('FACETS', lit(0), "CLAIM LINE DISALLOW TYPE", trim(col("CLM_LN_DSALW_TYP_CD")), 'X')
)

df_b_clm_ln_dsalw = df_transformer_vars.select(
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CLCL_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("svClmLnDslwTypCdSk").alias("CLM_LN_DSALW_TYP_CD_SK")
).withColumn(
    "CLM_ID",
    rpad(col("CLM_ID"), 18, " ")
)

write_files(
    df_b_clm_ln_dsalw,
    f"{adls_path}/load/B_CLM_LN_DSALW.FACETS.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

params_ClmLnDsalwPK = {
    "$IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd
}
df_InkOut = ClmLnDsalwPK(df_Transform, df_AllCol, params_ClmLnDsalwPK)

df_ClmLnDsalwCrf = df_InkOut.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_LN_DSALW_SK"),
    rpad(col("CLM_ID"), 18, " ").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    rpad(col("CLM_LN_DSALW_TYP_CD"), 2, " ").alias("CLM_LN_DSALW_TYP_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("CLM_LN_DSALW_EXCD"), 3, " ").alias("CLM_LN_DSALW_EXCD"),
    col("DSALW_AMT")
)

write_files(
    df_ClmLnDsalwCrf,
    f"{adls_path}/key/FctsClmLnDsalwExtr.FctsClmLnDsalw.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)