# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_4 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 02/02/09 09:52:49 Batch  15009_35578 PROMOTE bckcetl ids20 dsadm bls for brent
# MAGIC ^1_3 01/26/09 13:03:15 Batch  15002_46999 INIT bckcett testIDS dsadm bls for bl
# MAGIC ^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
# MAGIC ^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 08/30/07 07:19:35 Batch  14487_26382 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 10:06:05 Batch  14484_36371 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/26/07 12:34:37 Batch  14330_45286 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 15:24:05 Batch  14311_55447 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_5 12/18/06 11:16:18 Batch  14232_40614 INIT bckcetl ids20 dsadm Backup for 12/18 install
# MAGIC ^1_4 04/17/06 10:35:58 Batch  13987_38166 PROMOTE bckcetl ids20 dsadm J. mahaffey
# MAGIC ^1_4 04/17/06 10:24:16 Batch  13987_37468 INIT bckcett testIDS30 dsadm J. Mahaffey for B. Leland
# MAGIC ^1_3 04/13/06 08:47:39 Batch  13983_31666 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_3 04/13/06 07:36:31 Batch  13983_27397 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_2 04/02/06 15:43:48 Batch  13972_56632 INIT bckcett devlIDS30 dcg01 steffy
# MAGIC ^1_3 03/29/06 15:32:39 Batch  13968_55966 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_2 03/29/06 14:54:04 Batch  13968_53650 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_1 03/29/06 05:18:35 Batch  13968_19121 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 03/23/06 14:55:53 Batch  13962_53758 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_1 03/20/06 13:57:04 Batch  13959_50244 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_1 01/06/06 12:42:54 Batch  13886_45778 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC ********************************************************************************
# MAGIC 
# MAGIC Â© Copyright 2005 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC JOB NAME: FctsClmLnHashFile2Extr
# MAGIC 
# MAGIC DESCRIPTION:  One of three hash file extracts that must run in order (1,2,3)
# MAGIC 
# MAGIC \(9)Extract Claim Line Disallow Data extract from Facets, and load to hash files to be used in FctsClmLnTrns, FctsClmLnDntlTrns and FctsClmLnDsalwExtr
# MAGIC                 Extract rows from the disallow tables for W_CLM_LN_DSALW - pull all rows for claims on TMP_DRIVER
# MAGIC 
# MAGIC  SOURCE:
# MAGIC 
# MAGIC \(9)CMC_CDML_CL_LINE\(9) 
# MAGIC                 CMC_CDDL_CL_LINE
# MAGIC                 CMC_CDMD_LI_DISALL
# MAGIC                 CMC_CDDD_DNLI_DIS
# MAGIC                 CMC_CDOR_LI_OVR
# MAGIC                 CMC_CDDO_DNLI_OVR
# MAGIC                 CMC_CLCL_CLAIM
# MAGIC                 CMC_CLMI_MISC
# MAGIC                  CMC_CDCB_LI_COB
# MAGIC                 TMP_DRIVER
# MAGIC                  IDS CD_MPPNG
# MAGIC                  IDS DSALW_EXCD
# MAGIC 
# MAGIC 
# MAGIC  Hash Files
# MAGIC               used in FctsClmLnTrns, FctsClmLnDntlTrns, FctsClmLnDsalwExtr
# MAGIC 
# MAGIC                    hf_clm_ln_dsalw_tu
# MAGIC                    hf_clm_ln_dsalw_a
# MAGIC                    hf_clm_ln_dsalw_x
# MAGIC                    hf_clm_ln_dsalw_cdmd
# MAGIC                    hf_clm_ln_dsalw_cddd
# MAGIC                    hf_clm_ln_dsalw_cdml
# MAGIC                    hf_clm_ln_dsalw_cddl
# MAGIC                    hf_clm_ln_dsalw_cap
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:
# MAGIC 
# MAGIC \(9)W_CLM_LN_DSALW.dat to be loaded in IDS, must go through the delete process
# MAGIC 
# MAGIC  
# MAGIC                 IDS parameters
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC \(9)Developer\(9)Date\(9)\(9)Comment
# MAGIC \(9)-----------------------------\(9)---------------------------\(9)-----------------------------------------------------------
# MAGIC \(9)BJ Luce                  3/30/2006                initial program
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Ralph Tucker          08/01/2008      3657 Primary Key   Modified DSALW_EXCD ref link                                                 devlIDS                        
# MAGIC Prabhu ES              02/26/2022      S2S Remediation    MSSQL connection parameters added                                       IntegrateDev5\(9)Ken Bradmon\(9)2022-06-10

# MAGIC Facets Claim Line Hash File 2 Extract
# MAGIC These hashfiles are loaded in FctsClmLnHashFile1Extr
# MAGIC 
# MAGIC Also used in FctsClmLnHashFile3Extr
# MAGIC These hash files are used in FctsClmLnHashFile3Extr
# MAGIC Build hash files for dsalw rows to use in FctsClmLnTrns, FctsClmLnDntlTrns, and FctsClmLnDsalwExtr
# MAGIC Do not clear
# MAGIC                    hf_clm_ln_dsalw_tu
# MAGIC                    hf_clm_ln_dsalw_a
# MAGIC                    hf_clm_ln_dsalw_x
# MAGIC                    hf_clm_ln_dsalw_cdmd
# MAGIC                    hf_clm_ln_dsalw_cddd
# MAGIC                    hf_clm_ln_dsalw_cdml
# MAGIC                    hf_clm_ln_dsalw_cddl
# MAGIC Build hash files for dsalw rows to use in FctsClmLnTrns, FctsClmLnDntlTrns, and FctsClmLnDsalwExtr
# MAGIC Do not clear
# MAGIC                    hf_clm_ln_dsalw_tu
# MAGIC                    hf_clm_ln_dsalw_a
# MAGIC                    hf_clm_ln_dsalw_x
# MAGIC                    hf_clm_ln_dsalw_cdmd
# MAGIC                    hf_clm_ln_dsalw_cddd
# MAGIC                    hf_clm_ln_dsalw_cdml
# MAGIC                    hf_clm_ln_dsalw_cddl
# MAGIC                    hf_clm_ln_dsalw_cap
# MAGIC Used in:
# MAGIC FctsClmLnHashFile3Extr
# MAGIC Hash Files created in FctsClmLnExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Source = get_widget_value("Source","FACETS")
DriverTable = get_widget_value("DriverTable","")
RunCycle = get_widget_value("RunCycle","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
FacetsOwner = get_widget_value("FacetsOwner","")
facets_secret_name = get_widget_value("facets_secret_name","")
BCBSOwner = get_widget_value("BCBSOwner","")
bcbs_secret_name = get_widget_value("bcbs_secret_name","")

df_hash_x_tmp = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_x_tmp.parquet")
df_hash_a_tmp = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_a_tmp.parquet")
df_hash_tu_tmp = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_tu_tmp.parquet")

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

extract_query_cdmd_extr = (
    f"SELECT DIS.CLCL_ID,DIS.CDML_SEQ_NO,DIS.CDMD_TYPE,DIS.CDMD_DISALL_AMT,"
    f"DIS.EXCD_ID,CLAIM.CLCL_PAID_DT,LINE.CDML_DISALL_AMT "
    f"FROM {FacetsOwner}.CMC_CDMD_LI_DISALL DIS,{FacetsOwner}.CMC_CLCL_CLAIM CLAIM,"
    f"tempdb..{DriverTable} TMP,{FacetsOwner}.CMC_CDML_CL_LINE LINE WHERE DIS.CLCL_ID = TMP.CLM_ID "
    f"and DIS.CLCL_ID = CLAIM.CLCL_ID and DIS.CLCL_ID = LINE.CLCL_ID "
    f"and DIS.CDML_SEQ_NO  = LINE.CDML_SEQ_NO"
)
df_cddd_cdmd_overrides_cdmd_extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_cdmd_extr)
    .load()
)

extract_query_cddd_extr = (
    f"SELECT DIS.CLCL_ID,DIS.CDDL_SEQ_NO CDML_SEQ_NO,DIS.CDDD_TYPE CDMD_TYPE,"
    f"DIS.CDDD_DISALL_AMT CDMD_DISALL_AMT,DIS.EXCD_ID,CLAIM.CLCL_PAID_DT,"
    f"LINE.CDDL_DISALL_AMT CDML_DISALL_AMT FROM {FacetsOwner}.CMC_CDDD_DNLI_DIS DIS,"
    f"{FacetsOwner}.CMC_CLCL_CLAIM CLAIM,tempdb..{DriverTable} TMP,{FacetsOwner}.CMC_CDDL_CL_LINE LINE "
    f"WHERE DIS.CLCL_ID = TMP.CLM_ID and DIS.CLCL_ID = CLAIM.CLCL_ID "
    f"and DIS.CLCL_ID = LINE.CLCL_ID and DIS.CDDL_SEQ_NO  = LINE.CDDL_SEQ_NO"
)
df_cddd_cdmd_overrides_cddd_extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_cddd_extr)
    .load()
)

extract_query_dsalw_excd_cdml_cddl = (
    f"SELECT CL.CLCL_ID,CL.CDML_SEQ_NO,EXCD_RSPNSB_IN,EXCD_BYPS_IN "
    f"FROM {FacetsOwner}.CMC_CDML_CL_LINE CL, tempdb..{DriverTable} TMP, {FacetsOwner}.CMC_CLCL_CLAIM CLM,"
    f"{BCBSOwner}.CLM_DISALL_RSPNSB dalw "
    f"WHERE TMP.CLM_ID = CL.CLCL_ID and CLM.CLCL_ID = TMP.CLM_ID "
    f"and CL.CDML_DISALL_EXCD = dalw.CDML_DISALL_EXCD "
    f"and CLM.CLCL_PAID_DT >= dalw.EFF_DT and CLM.CLCL_PAID_DT <= dalw.TERM_DT"
)
df_DsalwExcdCdmlCddl = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_dsalw_excd_cdml_cddl)
    .load()
)

df_DsalwExcdCdmlCddl_dedup = dedup_sort(
    df_DsalwExcdCdmlCddl,
    partition_cols=["CLCL_ID", "CDML_SEQ_NO"],
    sort_cols=[]
)

df_DsalwExcdCdmd = df_DsalwExcdCdmlCddl_dedup
df_DsalwExcdCddd = df_DsalwExcdCdmlCddl_dedup

df_cdmd_extr = df_cddd_cdmd_overrides_cdmd_extr.alias("cdmd_extr")
df_cdmd_extr_joined = (
    df_cdmd_extr
    .join(
        df_hash_tu_tmp.alias("tu_lkup"),
        (
            (F.col("cdmd_extr.CLCL_ID") == F.col("tu_lkup.CLCL_ID"))
            & (F.col("cdmd_extr.CDML_SEQ_NO") == F.col("tu_lkup.CDML_SEQ_NO"))
            & (F.col("tu_lkup.DSALW_TYP") == F.lit("ATAU"))
        ),
        "left"
    )
    .join(
        df_hash_x_tmp.alias("x_lkup"),
        (
            (F.col("cdmd_extr.CLCL_ID") == F.col("x_lkup.CLCL_ID"))
            & (F.col("cdmd_extr.CDML_SEQ_NO") == F.col("x_lkup.CDML_SEQ_NO"))
            & (F.col("x_lkup.DSALW_TYP") == F.lit("AX"))
        ),
        "left"
    )
    .join(
        df_hash_a_tmp.alias("aa_lkup"),
        (
            (F.col("cdmd_extr.CLCL_ID") == F.col("aa_lkup.CLCL_ID"))
            & (F.col("cdmd_extr.CDML_SEQ_NO") == F.col("aa_lkup.CDML_SEQ_NO"))
            & (F.col("aa_lkup.DSALW_TYP") == F.lit("AA"))
        ),
        "left"
    )
    .join(
        df_DsalwExcdCdmd.alias("DsalwExcdCdmd"),
        (
            (F.col("cdmd_extr.CLCL_ID") == F.col("DsalwExcdCdmd.CLCL_ID"))
            & (F.col("cdmd_extr.CDML_SEQ_NO") == F.col("DsalwExcdCdmd.CDML_SEQ_NO"))
        ),
        "left"
    )
)

df_dsalw_cdmd = (
    df_cdmd_extr_joined
    .withColumn("svAAMatch", F.when(F.col("aa_lkup.CDML_SEQ_NO").isNotNull(), F.lit("Y")).otherwise(F.lit("N")))
    .withColumn("svATAUMatch", F.when(F.col("tu_lkup.CDML_SEQ_NO").isNotNull(), F.lit("Y")).otherwise(F.lit("N")))
    .withColumn("svAXMatch", F.when(F.col("x_lkup.CDML_SEQ_NO").isNotNull(), F.lit("Y")).otherwise(F.lit("N")))
    .withColumn(
        "svAANotEqual",
        F.when(
            (F.col("svAAMatch") == F.lit("Y"))
            & (F.col("aa_lkup.DSALW_AMT") != F.col("aa_lkup.CDML_DISALL_AMT")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svATAUNotEqual",
        F.when(
            (F.col("svATAUMatch") == F.lit("Y"))
            & (F.col("tu_lkup.DSALW_AMT") != F.col("tu_lkup.CDML_DISALL_AMT")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svAXNotEqual",
        F.when(
            (F.col("svAXMatch") == F.lit("Y"))
            & (F.col("x_lkup.DSALW_AMT") != F.col("x_lkup.CDML_DISALL_AMT")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svAABuild",
        F.when(
            (F.col("svAANotEqual") == F.lit("Y"))
            & (F.col("aa_lkup.DSALW_AMT") != F.col("cdmd_extr.CDML_DISALL_AMT")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svATAUBuild",
        F.when(
            (F.col("svATAUNotEqual") == F.lit("Y"))
            & (F.col("tu_lkup.DSALW_AMT") != F.col("cdmd_extr.CDML_DISALL_AMT")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svAXBuild",
        F.when(
            (F.col("svAXNotEqual") == F.lit("Y"))
            & (F.col("x_lkup.DSALW_AMT") != F.col("cdmd_extr.CDML_DISALL_AMT")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svBuildCdmd",
        F.when(
            F.col("cdmd_extr.CDMD_DISALL_AMT") == 0,
            F.lit("Y")
        ).otherwise(
            F.when(
                (F.col("svAABuild") == F.lit("Y"))
                | (F.col("svATAUBuild") == F.lit("Y"))
                | (F.col("svAXBuild") == F.lit("Y"))
                | (
                    (F.col("svAXMatch") == F.lit("N"))
                    & (F.col("svATAUMatch") == F.lit("N"))
                    & (F.col("svAAMatch") == F.lit("N"))
                ),
                F.lit("Y")
            ).otherwise(F.lit("N"))
        )
    )
    .filter(F.col("svBuildCdmd") == F.lit("Y"))
    .select(
        F.col("cdmd_extr.CLCL_ID").alias("CLCL_ID"),
        F.col("cdmd_extr.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        F.col("cdmd_extr.CDMD_TYPE").alias("DSALW_TYP"),
        F.col("cdmd_extr.CDMD_DISALL_AMT").alias("DSALW_AMT"),
        F.col("cdmd_extr.EXCD_ID").alias("DSALW_EXCD"),
        F.when(
            F.col("DsalwExcdCdmd.EXCD_RSPNSB_IN").isNull(),
            F.lit("NA")
        ).otherwise(F.col("DsalwExcdCdmd.EXCD_RSPNSB_IN")).alias("EXCD_RESP_CD"),
        F.col("cdmd_extr.CDML_DISALL_AMT").alias("CDML_DISALL_AMT"),
        F.when(
            F.col("DsalwExcdCdmd.EXCD_BYPS_IN").isNull(),
            F.lit("N")
        ).otherwise(F.col("DsalwExcdCdmd.EXCD_BYPS_IN")).alias("BYPS_IN")
    )
)

df_dsalw_cdmd_dedup = dedup_sort(
    df_dsalw_cdmd,
    partition_cols=["CLCL_ID", "CDML_SEQ_NO", "DSALW_TYP"],
    sort_cols=[]
)

df_sum_all_cdmd = df_dsalw_cdmd_dedup.groupBy("CLCL_ID","CDML_SEQ_NO").agg(F.sum("DSALW_AMT").alias("DSALW_AMT"))

df_sum_all_cdmd_final = df_sum_all_cdmd.select(
    F.rpad(F.col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    F.col("CDML_SEQ_NO"),
    F.col("DSALW_AMT")
)

write_files(
    df_sum_all_cdmd_final,
    "hf_clm_ln_dsalw_cdmd_sum.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_cddd_extr = df_cddd_cdmd_overrides_cddd_extr.alias("cddd_extr")
df_cddd_extr_joined = (
    df_cddd_extr
    .join(
        df_hash_tu_tmp.alias("tu_lkup_d"),
        (
            (F.col("cddd_extr.CLCL_ID") == F.col("tu_lkup_d.CLCL_ID"))
            & (F.col("cddd_extr.CDML_SEQ_NO") == F.col("tu_lkup_d.CDML_SEQ_NO"))
            & (F.col("tu_lkup_d.DSALW_TYP") == F.lit("DTDU"))
        ),
        "left"
    )
    .join(
        df_hash_x_tmp.alias("x_lkup_d"),
        (
            (F.col("cddd_extr.CLCL_ID") == F.col("x_lkup_d.CLCL_ID"))
            & (F.col("cddd_extr.CDML_SEQ_NO") == F.col("x_lkup_d.CDML_SEQ_NO"))
            & (F.col("x_lkup_d.DSALW_TYP") == F.lit("DX"))
        ),
        "left"
    )
    .join(
        df_hash_a_tmp.alias("da_lkup"),
        (
            (F.col("cddd_extr.CLCL_ID") == F.col("da_lkup.CLCL_ID"))
            & (F.col("cddd_extr.CDML_SEQ_NO") == F.col("da_lkup.CDML_SEQ_NO"))
            & (F.col("da_lkup.DSALW_TYP") == F.lit("DA"))
        ),
        "left"
    )
    .join(
        df_DsalwExcdCddd.alias("DsalwExcdCddd"),
        (
            (F.col("cddd_extr.CLCL_ID") == F.col("DsalwExcdCddd.CLCL_ID"))
            & (F.col("cddd_extr.CDML_SEQ_NO") == F.col("DsalwExcdCddd.CDML_SEQ_NO"))
        ),
        "left"
    )
)

df_dsalw_cddd = (
    df_cddd_extr_joined
    .withColumn("svAAMatch", F.when(F.col("da_lkup.CDML_SEQ_NO").isNotNull(), F.lit("Y")).otherwise(F.lit("N")))
    .withColumn("svATAUMatch", F.when(F.col("tu_lkup_d.CDML_SEQ_NO").isNotNull(), F.lit("Y")).otherwise(F.lit("N")))
    .withColumn("svAXMatch", F.when(F.col("x_lkup_d.CDML_SEQ_NO").isNotNull(), F.lit("Y")).otherwise(F.lit("N")))
    .withColumn(
        "svAANotEqual",
        F.when(
            (F.col("svAAMatch") == F.lit("Y"))
            & (F.col("da_lkup.DSALW_AMT") != F.col("da_lkup.CDML_DISALL_AMT")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svATAUNotEqual",
        F.when(
            (F.col("svATAUMatch") == F.lit("Y"))
            & (F.col("tu_lkup_d.DSALW_AMT") != F.col("tu_lkup_d.CDML_DISALL_AMT")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svAXNotEqual",
        F.when(
            (F.col("svAXMatch") == F.lit("Y"))
            & (F.col("x_lkup_d.DSALW_AMT") != F.col("x_lkup_d.CDML_DISALL_AMT")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svAABuild",
        F.when(
            (F.col("svAANotEqual") == F.lit("Y"))
            & (F.col("da_lkup.DSALW_AMT") != F.col("cddd_extr.CDML_DISALL_AMT")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svATAUBuild",
        F.when(
            (F.col("svATAUNotEqual") == F.lit("Y"))
            & (F.col("tu_lkup_d.DSALW_AMT") != F.col("cddd_extr.CDML_DISALL_AMT")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svAXBuild",
        F.when(
            (F.col("svAXNotEqual") == F.lit("Y"))
            & (F.col("x_lkup_d.DSALW_AMT") != F.col("cddd_extr.CDML_DISALL_AMT")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svBuildCdmd",
        F.when(
            F.col("cddd_extr.CDMD_DISALL_AMT") == 0,
            F.lit("Y")
        ).otherwise(
            F.when(
                (F.col("svAABuild") == F.lit("Y"))
                | (F.col("svATAUBuild") == F.lit("Y"))
                | (F.col("svAXBuild") == F.lit("Y"))
                | (
                    (F.col("svAXMatch") == F.lit("N"))
                    & (F.col("svATAUMatch") == F.lit("N"))
                    & (F.col("svAAMatch") == F.lit("N"))
                ),
                F.lit("Y")
            ).otherwise(F.lit("N"))
        )
    )
    .filter(F.col("svBuildCdmd") == F.lit("Y"))
    .select(
        F.col("cddd_extr.CLCL_ID").alias("CLCL_ID"),
        F.col("cddd_extr.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        F.col("cddd_extr.CDMD_TYPE").alias("DSALW_TYP"),
        F.col("cddd_extr.CDMD_DISALL_AMT").alias("DSALW_AMT"),
        F.col("cddd_extr.EXCD_ID").alias("DSALW_EXCD"),
        F.when(
            F.col("DsalwExcdCddd.EXCD_RSPNSB_IN").isNull(),
            F.lit("NA")
        ).otherwise(F.col("DsalwExcdCddd.EXCD_RSPNSB_IN")).alias("EXCD_RESP_CD"),
        F.col("cddd_extr.CDML_DISALL_AMT").alias("CDML_DISALL_AMT"),
        F.when(
            F.col("DsalwExcdCddd.EXCD_BYPS_IN").isNull(),
            F.lit("N")
        ).otherwise(F.col("DsalwExcdCddd.EXCD_BYPS_IN")).alias("BYPS_IN")
    )
)

df_dsalw_cddd_dedup = dedup_sort(
    df_dsalw_cddd,
    partition_cols=["CLCL_ID", "CDML_SEQ_NO", "DSALW_TYP"],
    sort_cols=[]
)

df_sum_all_cddd = df_dsalw_cddd_dedup.groupBy("CLCL_ID","CDML_SEQ_NO").agg(F.sum("DSALW_AMT").alias("DSALW_AMT"))

df_sum_all_cddd_final = df_sum_all_cddd.select(
    F.rpad(F.col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    F.col("CDML_SEQ_NO"),
    F.col("DSALW_AMT")
)

write_files(
    df_sum_all_cddd_final,
    "hf_clm_ln_dsalw_cddd_sum.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)