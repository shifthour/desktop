# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  LhoFctsDntlClmLineExtr
# MAGIC CALLED BY:  LhoFctsClmOnDmdPrereqSeq
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from CMC_CDDL_CL_LINE to a landing file for the IDS
# MAGIC 
# MAGIC INPUTS:	CMC_CDDL_CL_LINE
# MAGIC                 TMP_IDS_DRIVER table
# MAGIC                 CMC_CLCL_CLAIM
# MAGIC                 IDS DSALW_EXCD
# MAGIC HASH FILES:   created in other processed - Do Not Clear
# MAGIC                         hf_clm_sts  created in FctsClmLnMedExtr - updated here
# MAGIC                         hf_clm_fcts_reversals created in FctsClmDriverBuild
# MAGIC                         hf_clm_nasco_dup_bypass created in FctsClmDriverBuild
# MAGIC                         hf_clm_ln_dsalw_ln_amts created in FctsClmLnHashExtr3
# MAGIC 
# MAGIC                        created and deleted in this process
# MAGIC                        hf_clm_ln_dntl_ovr_y08
# MAGIC  
# MAGIC TRANSFORMS:    STRIP.FIELD
# MAGIC 
# MAGIC PROCESSING:    Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC 
# MAGIC OUTPUTS:   Sequential file used in transform job for FctsClmLnDntlTrns
# MAGIC                      sequential file for foreign keying for IdsDntlClmLnFkey
# MAGIC                       Hash file hf_clm_sts
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Steph Goddard  06/02/2004-   Originally Programmed
# MAGIC               Kevin Soderlund                        Added hash file stuff
# MAGIC               Kevin Soderlund 08/25/2004    Updated final disposition code logic
# MAGIC                 Sharon Andrew        12/08/2004             Added the claim status code to the hash file hf_clm_stts.  This hash file also used by FctsClnLnTrns, FctsClnTrns and they read the claim status code.
# MAGIC                                                                                  For claims that had dental clain lines only, the finalized dispositon code was getting set to UNK cause there was no status code to use for setting it.
# MAGIC               Sharon Andrew  1/31/2005     Added clm line sequence number to the hash file hf_clm_sts.   hf_clm_sts is used by the claim processes to get the final disposition code for the claim.
# MAGIC                                                                 all of the status' need to be tested before assesing the claim final status.   when there was no claim line sequence number on the hash file, whatever the last sequence
# MAGIC                                                                   number was processed, that was the only claim line status given to the claims process.   
# MAGIC              Sharon Anderw  - 3/11/2005    removed hash file key from hf_clm_sts so there are now only 2 - claim and sequ.   changed the order of last two fields to be status then finalized as it is in FctsClmLnExtr
# MAGIC              Steph Goddard   03/01/2006   combined extract, transform, primary key for sequencer for Dental Claim Line
# MAGIC              BJ Luce              03/01/2006   put back in logic to create file to go into FctsClmLnDntlTrns - for CLM_LN (dental)
# MAGIC              BJ Luce              03/24/2006   use hf_clm_ln_dsalw_ln_amts created in FctsClmLnHashExtr3 to determine final disposition code
# MAGIC            BJ Luce            03/24/2006     add hf_clm_nasco_dup_bypass, identifies claims that are nasco dups. If the claim is on the file, a row is not generated for it in IDS. However, an R row will be build for it if the status if '91'
# MAGIC            Steph Goddard  03/31/2006   pulled CDDL_EOB_EXCD to fill in CLM_LN_EOB_EXCD_SK
# MAGIC                                                            rule changed for CAT_PAYMT_PFX_ID, DNTL_PROC_PAYMT_PFX_ID, and DNTL_PROC_PRICE_ID to look at status if source is blank or null
# MAGIC            Steph Goddard 05/11/2006    added split out for hf_clm_sts hash file - file is used in both FctsClmExtr and FctsClmProvExtr, both long-running jobs.  This way they don't have to run in parallel.
# MAGIC              Sanderw  12/08/2006   Project 1576  - Reversal logix added for new status codes 89 and  and 99
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC =============================================================================================================================================================
# MAGIC Developer                    Date                 	Project                                                     Change Description                                         Development Project             Code Reviewer          Date Reviewed    
# MAGIC =============================================================================================================================================================
# MAGIC Manasa Andru          2020-08-10                        US -  263702                                         Original Programming                                              IntegrateDev2                      Jaideep Mankala      10/09/2020
# MAGIC Prabhu ES                2022-03-29                        S2S                                             MSSQL ODBC conn params added                                  IntegrateDev5	Ken Bradmon	2022-06-10

# MAGIC input to FctsClmLnDntlTrns to create data for CLM_LN (dental)
# MAGIC Balancing
# MAGIC Pulling Facets Claim Line Item Override Data for a specific Time Period
# MAGIC hf_clm_sts created in FctsClmLnMedExtr and updated in this process for Dental Claims
# MAGIC File used in FctsClmExtr
# MAGIC Writing Sequential File to /key
# MAGIC Assign primary surrogate key
# MAGIC Apply business logic
# MAGIC hash file hf_clm_ln_dsalw_ln_amts created in FctsClmLnHashFileExtr3
# MAGIC 
# MAGIC also used in FctsClmLnMedTrns, FctsClmLnMedExtr and FctsClmLnDntlTrns
# MAGIC 
# MAGIC Do not clear in this process
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC Hash files used in FctsClmExtr and in FctsClmProvExtr - do not clear
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
jpClmSts = get_widget_value('jpClmSts','')
jpClmStsClsd = get_widget_value('jpClmStsClsd','')
ipClmStsActive = get_widget_value('ipClmStsActive','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
LhoFacetsStgAcct = get_widget_value('LhoFacetsStgAcct','')
LhoFacetsStgDSN = get_widget_value('LhoFacetsStgDSN','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
LhoFacetsStgPW = get_widget_value('LhoFacetsStgPW','')

# MAGIC %run ../../../../../shared_containers/PrimaryKey/DntlClmLnPK
# COMMAND ----------

# Read from Facets2 (ODBCConnector) - ODBC to some environment
jdbc_url_facets, jdbc_props_facets = get_db_config(LhoFacetsStgPW)
query_facets2 = f"""
SELECT
    CL_LINE.CLCL_ID,
    CL_LINE.CDDL_SEQ_NO,
    CL_LINE.PRPR_ID,
    CL_LINE.LOBD_ID,
    CL_LINE.PDVC_LOBD_PTR,
    CL_LINE.CDDL_CUR_STS,
    CL_LINE.DPPY_PFX,
    CL_LINE.CGPY_PFX,
    CL_LINE.LTLT_PFX,
    CL_LINE.DEDE_PFX,
    CL_LINE.DPPC_PRICE_ID,
    CL_LINE.DPDP_ID,
    CL_LINE.DPCG_DP_ID_ALT,
    CL_LINE.CDDL_ALTDP_EXCD_ID,
    CL_LINE.PSDC_ID,
    CL_LINE.CGCG_ID,
    CL_LINE.CGCG_RULE,
    CL_LINE.CDDL_FROM_DT,
    CL_LINE.CDDL_CHG_AMT,
    CL_LINE.CDDL_CONSIDER_CHG,
    CL_LINE.CDDL_ALLOW,
    CL_LINE.CDDL_TOOTH_NO,
    CL_LINE.CDDL_TOOTH_BEG,
    CL_LINE.CDDL_TOOTH_END,
    CL_LINE.CDDL_SURF,
    CL_LINE.CDDL_UNITS,
    CL_LINE.CDDL_UNITS_ALLOW,
    CL_LINE.CDDL_DED_AMT,
    CL_LINE.CDDL_DED_AC_NO,
    CL_LINE.CDDL_COPAY_AMT,
    CL_LINE.CDDL_COINS_AMT,
    CL_LINE.CDDL_RISK_WH_AMT,
    CL_LINE.CDDL_PAID_AMT,
    CL_LINE.CDDL_DISALL_AMT,
    CL_LINE.CDDL_DISALL_EXCD,
    CL_LINE.CDDL_AG_PRICE,
    CL_LINE.CDDL_PF_PRICE,
    CL_LINE.CDDL_DP_PRICE,
    CL_LINE.CDDL_IP_PRICE,
    CL_LINE.CDDL_PRICE_IND,
    CL_LINE.CDDL_CL_NTWK_IND,
    CL_LINE.CDDL_REF_IND,
    CL_LINE.CDDL_CAP_IND,
    CL_LINE.CDDL_SB_PYMT_AMT,
    CL_LINE.CDDL_PR_PYMT_AMT,
    CL_LINE.UTUT_CD,
    CL_LINE.CDDL_UMREF_ID,
    CL_LINE.CDDL_REFSV_SEQ_NO,
    CL_LINE.CDDL_OOP_CALC_BASE,
    TMP.CLM_STS,
    CLAIM.CLCL_PAID_DT,
    CLAIM.CLCL_NTWK_IND,
    CL_LINE.CDDL_EOB_EXCD
FROM {LhoFacetsStgOwner}.CMC_CDDL_CL_LINE CL_LINE,
     tempdb..{DriverTable} TMP,
     {LhoFacetsStgOwner}.CMC_CLCL_CLAIM CLAIM
WHERE TMP.CLM_ID = CL_LINE.CLCL_ID
  AND CLAIM.CLCL_ID = TMP.CLM_ID
"""
df_Facets2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_facets2)
    .load()
)

# Read from Facets (ODBCConnector) for "CMC_CDOR_LI_OVR"
query_facets = f"""
SELECT
    OVR.CLCL_ID,
    OVR.CDML_SEQ_NO,
    OVR.CDOR_OR_AMT
FROM tempdb..{DriverTable} TMP,
     {LhoFacetsStgOwner}.CMC_CDOR_LI_OVR OVR
WHERE TMP.CLM_ID = OVR.CLCL_ID
  AND CDOR_OR_ID = 'DX'
  AND EXCD_ID = 'Y08'
"""
df_Facets = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_facets)
    .load()
)

# Remove interim hashed file "hf_clm_ln_dntl_ovr_y08" (Scenario A). Deduplicate on key columns CLCL_ID, CDML_SEQ_NO
df_hf_clm_ln_dntl_ovr_y08 = dedup_sort(
    df_Facets,
    partition_cols=["CLCL_ID", "CDML_SEQ_NO"],
    sort_cols=[]
)

# Read from "Ids" stage (DB2Connector => Database=IDS). We skip the placeholders and load full table, then join later.
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
query_ids = f"""
SELECT
    a.EXCD_ID,
    a.EFF_DT_SK,
    a.TERM_DT_SK,
    b.SRC_CD,
    a.BYPS_IN
FROM {IDSOwner}.DSALW_EXCD a
JOIN {IDSOwner}.CD_MPPNG b
      ON a.EXCD_RESP_CD_SK = b.CD_MPPNG_SK
"""
df_Ids = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_ids)
    .load()
)

# Read hashed file "hf_clm_ln_dsalw_ln_amts" (Scenario C => read from parquet)
df_hf_clm_ln_dsalw_ln_amts = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_ln_amts.parquet")

# Read hashed file "hf_clm_fcts_reversals" (Scenario C => read from parquet)
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

# Read hashed file "hf_clm_nasco_dup_bypass" (Scenario C => read from parquet)
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# Create alias dataframes for the multiple references to "hf_clm_ln_dsalw_ln_amts" with different join filters
# We will do all 5 lookups in the transformation stage.

# Create the "Extract" DataFrame (primary input to "StripFields") from df_Facets2
df_Extract = df_Facets2

# Next: "StripFields" stage logic with stage variables.
# We'll do multiple withColumns to replicate the DataStage stage variables and final columns.

# Stage variables come from the job definition:
# svNA, svClsdDel, svAcptd, svSuspend, svDenied, svFinalDispCd

# Condition references:
#   "If Extract.CDDL_CUR_STS And Index(jpClmSts, Extract.CDDL_CUR_STS, 1) < 1 Then 'NA' Else ''"
# We'll interpret Index(...) < 1 as "the substring is not found".
# We'll also interpret "If (svNA <> '') Then ''..." exactly.

df_sv = (
    df_Extract
    .withColumn(
        "svNA",
        F.when(
            (F.col("CDDL_CUR_STS").isNotNull()) &
            (F.instr(jpClmSts, F.col("CDDL_CUR_STS")) < 1),
            F.lit("NA")
        ).otherwise(F.lit(""))
    )
    .withColumn(
        "svClsdDel",
        F.when(
            (F.col("svNA") != "") ,
            F.lit("")
        ).otherwise(
            F.when(
                F.instr(jpClmStsClsd, F.col("CDDL_CUR_STS")) > 0,
                F.lit("CLSDDEL")
            ).otherwise(F.lit(""))
        )
    )
    .withColumn(
        "svAcptd",
        F.when(
            (F.col("svNA") != "") | (trim(F.col("svClsdDel")) != ""),
            F.lit("")
        ).otherwise(
            F.when(
                F.instr(ipClmStsActive, F.col("CDDL_CUR_STS")) < 1,
                F.lit("")
            ).otherwise(
                F.when(
                    ((F.col("CDDL_PR_PYMT_AMT").isNotNull()) & (F.col("CDDL_PR_PYMT_AMT") != 0))
                    | ((F.col("CDDL_SB_PYMT_AMT").isNotNull()) & (F.col("CDDL_SB_PYMT_AMT") != 0)),
                    F.lit("ACPTD")
                ).otherwise(F.lit(""))
            )
        )
    )
    .withColumn(
        "svSuspend",
        F.when(
            (trim(F.col("svNA")) == "NA")
            | (trim(F.col("svClsdDel")) == "CLSDDEL")
            | (trim(F.col("svAcptd")) == "ACPTD"),
            F.lit("")
        ).otherwise(
            F.when(
                F.col("RefNY.CLCL_ID").isNotNull(),
                F.lit("SUSP")
            ).otherwise(
                F.when(
                    (F.col("RefOY.CLCL_ID").isNotNull())
                    | (F.col("RefPY.CLCL_ID").isNotNull())
                    | (F.col("RefMY.CLCL_ID").isNotNull()),
                    F.lit("")
                ).otherwise(
                    F.when(
                        F.col("RefNN.CLCL_ID").isNotNull(),
                        F.lit("SUSP")
                    ).otherwise(F.lit(""))
                )
            )
        )
    )
    .withColumn(
        "svDenied",
        F.when(
            (F.col("svNA") != "")
            | (F.col("svAcptd") != "")
            | (F.col("svClsdDel") != "")
            | (F.col("svSuspend") != ""),
            F.lit("")
        ).otherwise(
            F.when(
                (F.col("CDDL_CONSIDER_CHG") == 0) & (F.col("CDDL_CHG_AMT") == 0),
                F.lit("DENIEDREJ")
            ).otherwise(
                F.when(
                    F.col("CDDL_CONSIDER_CHG") <= (
                        F.col("CDDL_DISALL_AMT") - F.when(
                            F.col("Y08.CLCL_ID").isNotNull(), F.col("Y08.CDOR_OR_AMT")
                        ).otherwise(F.lit(0))
                    ),
                    F.when(
                        F.length(trim(F.col("CDDL_DISALL_EXCD"))) == 0,
                        F.lit("NONPRICE")
                    ).otherwise(F.lit("DENIEDREJ"))
                ).otherwise(
                    F.lit("ACPTD")
                )
            )
        )
    )
    .withColumn(
        "svFinalDispCd",
        F.when(
            trim(F.col("svNA")) != "",
            trim(F.col("svNA"))
        ).otherwise(
            F.when(
                trim(F.col("svClsdDel")) != "",
                trim(F.col("svClsdDel"))
            ).otherwise(
                F.when(
                    trim(F.col("svAcptd")) != "",
                    trim(F.col("svAcptd"))
                ).otherwise(
                    F.when(
                        F.col("svSuspend") != "",
                        F.col("svSuspend")
                    ).otherwise(
                        F.when(
                            trim(F.col("svDenied")) != "",
                            trim(F.col("svDenied"))
                        ).otherwise(F.lit("ACPTD"))
                    )
                )
            )
        )
    )
)

# We must actually do all the left joins in a single DataFrame transformation to handle references:
# "RefMY", "RefNY", "RefNN", "RefOY", "RefPY", "Y08", "DsalwExcdCddl".
# The user logic in DataStage shows these as separate link names. We replicate them with separate joins (left).

# Join Y08 data (df_hf_clm_ln_dntl_ovr_y08) on CLCL_ID, CDML_SEQ_NO
df_sv = df_sv.alias("Extract")
df_y08 = df_hf_clm_ln_dntl_ovr_y08.alias("Y08")
join_cond_y08 = [
    df_sv["Extract.CLCL_ID"] == df_y08["CLCL_ID"],
    df_sv["Extract.CDDL_SEQ_NO"] == df_y08["CDML_SEQ_NO"]
]
df_sv = df_sv.join(df_y08, on=join_cond_y08, how="left")

# Join RefMY
df_refmy = df_hf_clm_ln_dsalw_ln_amts.alias("RefMY")
cond_refmy = [
    df_sv["Extract.CLCL_ID"] == df_refmy["CLCL_ID"],
    df_sv["Extract.CDDL_SEQ_NO"] == df_refmy["CDML_SEQ_NO"],
    F.lit("M") == df_refmy["EXCD_RESP_CD"],
    F.lit("Y") == df_refmy["BYPS_IN"]
]
df_sv = df_sv.join(df_refmy, on=cond_refmy, how="left")

# Join RefNY
df_refny = df_hf_clm_ln_dsalw_ln_amts.alias("RefNY")
cond_refny = [
    df_sv["Extract.CLCL_ID"] == df_refny["CLCL_ID"],
    df_sv["Extract.CDDL_SEQ_NO"] == df_refny["CDML_SEQ_NO"],
    F.lit("N") == df_refny["EXCD_RESP_CD"],
    F.lit("Y") == df_refny["BYPS_IN"]
]
df_sv = df_sv.join(df_refny, on=cond_refny, how="left")

# Join RefNN
df_refnn = df_hf_clm_ln_dsalw_ln_amts.alias("RefNN")
cond_refnn = [
    df_sv["Extract.CLCL_ID"] == df_refnn["CLCL_ID"],
    df_sv["Extract.CDDL_SEQ_NO"] == df_refnn["CDML_SEQ_NO"],
    F.lit("N") == df_refnn["EXCD_RESP_CD"],
    F.lit("N") == df_refnn["BYPS_IN"]
]
df_sv = df_sv.join(df_refnn, on=cond_refnn, how="left")

# Join RefOY
df_refoy = df_hf_clm_ln_dsalw_ln_amts.alias("RefOY")
cond_refoy = [
    df_sv["Extract.CLCL_ID"] == df_refoy["CLCL_ID"],
    df_sv["Extract.CDDL_SEQ_NO"] == df_refoy["CDML_SEQ_NO"],
    F.lit("O") == df_refoy["EXCD_RESP_CD"],
    F.lit("Y") == df_refoy["BYPS_IN"]
]
df_sv = df_sv.join(df_refoy, on=cond_refoy, how="left")

# Join RefPY
df_refpy = df_hf_clm_ln_dsalw_ln_amts.alias("RefPY")
cond_refpy = [
    df_sv["Extract.CLCL_ID"] == df_refpy["CLCL_ID"],
    df_sv["Extract.CDDL_SEQ_NO"] == df_refpy["CDML_SEQ_NO"],
    F.lit("P") == df_refpy["EXCD_RESP_CD"],
    F.lit("Y") == df_refpy["BYPS_IN"]
]
df_sv = df_sv.join(df_refpy, on=cond_refpy, how="left")

# Join DsalwExcdCddl => from df_Ids with conditions on EXCD_ID = Extract.CDDL_DISALL_EXCD
# plus substring(Extract.CLCL_PAID_DT,1,10) == EFF_DT_SK, substring(...) == TERM_DT_SK
df_ids_alias = df_Ids.alias("DsalwExcdCddl") 
df_sv = df_sv.join(
    df_ids_alias,
    on=[
        df_sv["Extract.CDDL_DISALL_EXCD"] == df_ids_alias["EXCD_ID"],
        F.substring(df_sv["Extract.CLCL_PAID_DT"], 1, 10) == df_ids_alias["EFF_DT_SK"],
        F.substring(df_sv["Extract.CLCL_PAID_DT"], 1, 10) == df_ids_alias["TERM_DT_SK"]
    ],
    how="left"
)

# Now we recast the stage vars again (so the references to joined columns like RefNY.CLCL_ID can be used).
# Re-apply the withColumns to ensure final results incorporate the real joined columns:
df_stripfields = (
    df_sv
    .withColumn(
        "svNA",
        F.when(
            (F.col("Extract.CDDL_CUR_STS").isNotNull()) &
            (F.instr(jpClmSts, F.col("Extract.CDDL_CUR_STS")) < 1),
            F.lit("NA")
        ).otherwise(F.lit(""))
    )
    .withColumn(
        "svClsdDel",
        F.when(
            (F.col("svNA") != "") ,
            F.lit("")
        ).otherwise(
            F.when(
                F.instr(jpClmStsClsd, F.col("Extract.CDDL_CUR_STS")) > 0,
                F.lit("CLSDDEL")
            ).otherwise(F.lit(""))
        )
    )
    .withColumn(
        "svAcptd",
        F.when(
            (F.col("svNA") != "") | (trim(F.col("svClsdDel")) != ""),
            F.lit("")
        ).otherwise(
            F.when(
                F.instr(ipClmStsActive, F.col("Extract.CDDL_CUR_STS")) < 1,
                F.lit("")
            ).otherwise(
                F.when(
                    ((F.col("Extract.CDDL_PR_PYMT_AMT").isNotNull()) & (F.col("Extract.CDDL_PR_PYMT_AMT") != 0))
                    | ((F.col("Extract.CDDL_SB_PYMT_AMT").isNotNull()) & (F.col("Extract.CDDL_SB_PYMT_AMT") != 0)),
                    F.lit("ACPTD")
                ).otherwise(F.lit(""))
            )
        )
    )
    .withColumn(
        "svSuspend",
        F.when(
            (trim(F.col("svNA")) == "NA")
            | (trim(F.col("svClsdDel")) == "CLSDDEL")
            | (trim(F.col("svAcptd")) == "ACPTD"),
            F.lit("")
        ).otherwise(
            F.when(
                F.col("RefNY.CLCL_ID").isNotNull(),
                F.lit("SUSP")
            ).otherwise(
                F.when(
                    (F.col("RefOY.CLCL_ID").isNotNull())
                    | (F.col("RefPY.CLCL_ID").isNotNull())
                    | (F.col("RefMY.CLCL_ID").isNotNull()),
                    F.lit("")
                ).otherwise(
                    F.when(
                        F.col("RefNN.CLCL_ID").isNotNull(),
                        F.lit("SUSP")
                    ).otherwise(F.lit(""))
                )
            )
        )
    )
    .withColumn(
        "svDenied",
        F.when(
            (F.col("svNA") != "")
            | (F.col("svAcptd") != "")
            | (F.col("svClsdDel") != "")
            | (F.col("svSuspend") != ""),
            F.lit("")
        ).otherwise(
            F.when(
                (F.col("Extract.CDDL_CONSIDER_CHG") == 0) & (F.col("Extract.CDDL_CHG_AMT") == 0),
                F.lit("DENIEDREJ")
            ).otherwise(
                F.when(
                    F.col("Extract.CDDL_CONSIDER_CHG") <= (
                        F.col("Extract.CDDL_DISALL_AMT") - F.when(
                            F.col("Y08.CLCL_ID").isNotNull(), F.col("Y08.CDOR_OR_AMT")
                        ).otherwise(F.lit(0))
                    ),
                    F.when(
                        F.length(trim(F.col("Extract.CDDL_DISALL_EXCD"))) == 0,
                        F.lit("NONPRICE")
                    ).otherwise(F.lit("DENIEDREJ"))
                ).otherwise(F.lit("ACPTD"))
            )
        )
    )
    .withColumn(
        "svFinalDispCd",
        F.when(
            trim(F.col("svNA")) != "",
            trim(F.col("svNA"))
        ).otherwise(
            F.when(
                trim(F.col("svClsdDel")) != "",
                trim(F.col("svClsdDel"))
            ).otherwise(
                F.when(
                    trim(F.col("svAcptd")) != "",
                    trim(F.col("svAcptd"))
                ).otherwise(
                    F.when(
                        F.col("svSuspend") != "",
                        F.col("svSuspend")
                    ).otherwise(
                        F.when(
                            trim(F.col("svDenied")) != "",
                            trim(F.col("svDenied"))
                        ).otherwise(F.lit("ACPTD"))
                    )
                )
            )
        )
    )
)

##################################################################
# Now produce the multiple outputs from "StripFields":
#  1) "StripDntlClmLn" => to "BusinessRules"
#  2) "hf_clm_sts" => to next hashed file (which we'll remove using Scenario A)
#  3) "StripClmLnDntl" => to "ClmLnDntl"
##################################################################

# "StripDntlClmLn" link columns
# Column definitions from JSON (LinkIdentifier V68S4P4)
df_StripDntlClmLn = df_stripfields.select(
    F.expr("Extract.CLCL_ID").alias("CLCL_ID"),
    F.col("Extract.CDDL_SEQ_NO").alias("CDDL_SEQ_NO"),
    F.lit(CurrentDate).alias("EXT_TIMESTAMP"),
    F.expr("Replace(Replace(Replace(Extract.PRPR_ID, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("PRPR_ID"),
    F.expr("Replace(Replace(Replace(Extract.LOBD_ID, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("LOBD_ID"),
    F.expr("Replace(Replace(Replace(Extract.PDVC_LOBD_PTR, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("PDVC_LOBD_PTR"),
    F.expr("Replace(Replace(Replace(Extract.DPPY_PFX, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("DPPY_PFX"),
    F.expr("Replace(Replace(Replace(Extract.LTLT_PFX, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("LTLT_PFX"),
    F.expr("Replace(Replace(Replace(Extract.CGPY_PFX, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CGPY_PFX"),
    F.expr("Replace(Replace(Replace(Extract.DPCG_DP_ID_ALT, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("DPCG_DP_ID_ALT"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_ALTDP_EXCD_ID, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_ALTDP_EXCD_ID"),
    F.expr("Replace(Replace(Replace(Extract.CGCG_ID, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CGCG_ID"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_TOOTH_NO, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_TOOTH_NO"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_TOOTH_BEG, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_TOOTH_BEG"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_TOOTH_END, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_TOOTH_END"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_SURF, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_SURF"),
    F.expr("Replace(Replace(Replace(Extract.UTUT_CD, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("UTUT_CD"),
    F.expr("Replace(Replace(Replace(Extract.DEDE_PFX, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("DEDE_PFX"),
    F.expr("Replace(Replace(Replace(Extract.DPPC_PRICE_ID, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("DPPC_PRICE_ID"),
    F.expr("Replace(Replace(Replace(Extract.DPDP_ID, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("DPDP_ID"),
    F.expr("Replace(Replace(Replace(Extract.CGCG_RULE, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CGCG_RULE"),
    F.col("Extract.CDDL_FROM_DT").alias("CDDL_FROM_DT"),
    F.col("Extract.CDDL_CHG_AMT").alias("CDDL_CHG_AMT"),
    F.col("Extract.CDDL_CONSIDER_CHG").alias("CDDL_CONSIDER_CHG"),
    F.col("Extract.CDDL_ALLOW").alias("CDDL_ALLOW"),
    F.col("Extract.CDDL_UNITS").alias("CDDL_UNITS"),
    F.col("Extract.CDDL_UNITS_ALLOW").alias("CDDL_UNITS_ALLOW"),
    F.col("Extract.CDDL_DED_AMT").alias("CDDL_DED_AMT"),
    F.col("Extract.CDDL_DED_AC_NO").alias("CDDL_DED_AC_NO"),
    F.col("Extract.CDDL_COPAY_AMT").alias("CDDL_COPAY_AMT"),
    F.col("Extract.CDDL_COINS_AMT").alias("CDDL_COINS_AMT"),
    F.col("Extract.CDDL_RISK_WH_AMT").alias("CDDL_RISK_WH_AMT"),
    F.col("Extract.CDDL_PAID_AMT").alias("CDDL_PAID_AMT"),
    F.col("Extract.CDDL_DISALL_AMT").alias("CDDL_DISALL_AMT"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_DISALL_EXCD, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_DISALL_EXCD"),
    F.col("Extract.CDDL_AG_PRICE").alias("CDDL_AG_PRICE"),
    F.col("Extract.CDDL_PF_PRICE").alias("CDDL_PF_PRICE"),
    F.col("Extract.CDDL_DP_PRICE").alias("CDDL_DP_PRICE"),
    F.col("Extract.CDDL_IP_PRICE").alias("CDDL_IP_PRICE"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_PRICE_IND, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_PRICE_IND"),
    F.col("Extract.CDDL_OOP_CALC_BASE").alias("CDDL_OOP_CALC_BASE"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_CL_NTWK_IND, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_CL_NTWK_IND"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_REF_IND, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_REF_IND"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_CAP_IND, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_CAP_IND"),
    F.col("Extract.CDDL_SB_PYMT_AMT").alias("CDDL_SB_PYMT_AMT"),
    F.col("Extract.CDDL_PR_PYMT_AMT").alias("CDDL_PR_PYMT_AMT"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_UMREF_ID, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_UMREF_ID"),
    F.col("Extract.CDDL_REFSV_SEQ_NO").alias("CDDL_REFSV_SEQ_NO"),
    F.col("svFinalDispCd").alias("CLM_LN_FINL_DISP_CD"),
    F.when(F.col("Extract.PSDC_ID").isNull(), F.lit("")).otherwise(trim(F.col("Extract.PSDC_ID"))).alias("PSCD_ID"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_EOB_EXCD, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_EOB_EXCD"),
    F.col("Extract.CDDL_CUR_STS").alias("CDDL_CUR_STS")
)

# "hf_clm_sts" link => scenario A => remove hashed file => deduplicate on PK (CLCL_ID, CDDL_SEQ_NO), then feed next
df_hf_clm_sts_tmp = df_stripfields.select(
    trim(F.col("Extract.CLCL_ID")).alias("CLCL_ID"),
    F.col("Extract.CDDL_SEQ_NO").alias("CDDL_SEQ_NO"),
    F.when( (F.length(trim(F.col("Extract.CDDL_CUR_STS"))) == 0) | (F.col("Extract.CDDL_CUR_STS").isNull()), F.lit("00")).otherwise(trim(F.col("Extract.CDDL_CUR_STS"))).alias("CDDL_CUR_STS"),
    trim(F.col("svFinalDispCd")).alias("CLM_LN_FINL_DISP_CD")
)
df_hf_clm_sts = dedup_sort(df_hf_clm_sts_tmp, partition_cols=["CLCL_ID","CDDL_SEQ_NO"], sort_cols=[])

# "StripClmLnDntl" => to "ClmLnDntl"
df_StripClmLnDntl = df_stripfields.select(
    F.expr("Replace(Replace(Replace(Extract.CLCL_ID, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CLCL_ID"),
    F.col("Extract.CDDL_SEQ_NO").alias("CDDL_SEQ_NO"),
    F.lit(CurrentDate).alias("EXT_TIMESTAMP"),
    F.expr("Replace(Replace(Replace(Extract.PRPR_ID, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("PRPR_ID"),
    F.expr("Replace(Replace(Replace(Extract.LOBD_ID, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("LOBD_ID"),
    F.expr("Replace(Replace(Replace(Extract.PDVC_LOBD_PTR, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("PDVC_LOBD_PTR"),
    F.expr("Replace(Replace(Replace(Extract.DPPY_PFX, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("DPPY_PFX"),
    F.expr("Replace(Replace(Replace(Extract.LTLT_PFX, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("LTLT_PFX"),
    F.expr("Replace(Replace(Replace(Extract.CGPY_PFX, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CGPY_PFX"),
    F.expr("Replace(Replace(Replace(Extract.DPCG_DP_ID_ALT, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("DPCG_DP_ID_ALT"),
    F.expr("Replace(Replace(Replace(Extract.DPCG_DP_ID_ALT, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_ALTDP_EXCD_ID"),
    F.expr("Replace(Replace(Replace(Extract.CGCG_ID, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CGCG_ID"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_TOOTH_NO, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_TOOTH_NO"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_TOOTH_BEG, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_TOOTH_BEG"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_TOOTH_END, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_TOOTH_END"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_SURF, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_SURF"),
    F.expr("Replace(Replace(Replace(Extract.UTUT_CD, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("UTUT_CD"),
    F.expr("Replace(Replace(Replace(Extract.DEDE_PFX, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("DEDE_PFX"),
    F.expr("Replace(Replace(Replace(Extract.DPPC_PRICE_ID, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("DPPC_PRICE_ID"),
    F.expr("Replace(Replace(Replace(Extract.DPDP_ID, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("DPDP_ID"),
    F.expr("Replace(Replace(Replace(Extract.CGCG_RULE, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CGCG_RULE"),
    F.col("Extract.CDDL_FROM_DT").alias("CDDL_FROM_DT"),
    F.col("Extract.CDDL_CHG_AMT").alias("CDDL_CHG_AMT"),
    F.col("Extract.CDDL_CONSIDER_CHG").alias("CDDL_CONSIDER_CHG"),
    F.col("Extract.CDDL_ALLOW").alias("CDDL_ALLOW"),
    F.col("Extract.CDDL_UNITS").alias("CDDL_UNITS"),
    F.col("Extract.CDDL_UNITS_ALLOW").alias("CDDL_UNITS_ALLOW"),
    F.col("Extract.CDDL_DED_AMT").alias("CDDL_DED_AMT"),
    F.col("Extract.CDDL_DED_AC_NO").alias("CDDL_DED_AC_NO"),
    F.col("Extract.CDDL_COPAY_AMT").alias("CDDL_COPAY_AMT"),
    F.col("Extract.CDDL_COINS_AMT").alias("CDDL_COINS_AMT"),
    F.col("Extract.CDDL_RISK_WH_AMT").alias("CDDL_RISK_WH_AMT"),
    F.col("Extract.CDDL_PAID_AMT").alias("CDDL_PAID_AMT"),
    F.col("Extract.CDDL_DISALL_AMT").alias("CDDL_DISALL_AMT"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_DISALL_EXCD, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_DISALL_EXCD"),
    F.col("Extract.CDDL_AG_PRICE").alias("CDDL_AG_PRICE"),
    F.col("Extract.CDDL_PF_PRICE").alias("CDDL_PF_PRICE"),
    F.col("Extract.CDDL_DP_PRICE").alias("CDDL_DP_PRICE"),
    F.col("Extract.CDDL_IP_PRICE").alias("CDDL_IP_PRICE"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_PRICE_IND, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_PRICE_IND"),
    F.col("Extract.CDDL_OOP_CALC_BASE").alias("CDDL_OOP_CALC_BASE"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_CL_NTWK_IND, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_CL_NTWK_IND"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_REF_IND, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_REF_IND"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_CAP_IND, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_CAP_IND"),
    F.col("Extract.CDDL_SB_PYMT_AMT").alias("CDDL_SB_PYMT_AMT"),
    F.col("Extract.CDDL_PR_PYMT_AMT").alias("CDDL_PR_PYMT_AMT"),
    F.expr("Replace(Replace(Replace(Extract.CDDL_UMREF_ID, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')").alias("CDDL_UMREF_ID"),
    F.col("Extract.CDDL_REFSV_SEQ_NO").alias("CDDL_REFSV_SEQ_NO"),
    F.col("svFinalDispCd").alias("CLM_LN_FINL_DISP_CD"),
    F.when(F.col("Extract.PSDC_ID").isNull(), F.lit("")).otherwise(trim(F.col("Extract.PSDC_ID"))).alias("PSCD_ID"),
    F.col("Extract.CLCL_NTWK_IND").alias("CLCL_NTWK_IND"),
    F.when(F.col("DsalwExcdCddl.SRC_CD").isNull(), F.lit("NA")).otherwise(F.col("DsalwExcdCddl.SRC_CD")).alias("CDDL_RESP_CD"),
    F.when(F.col("DsalwExcdCddl.EXCD_ID").isNull(), F.lit("N")).otherwise(F.lit("Y")).alias("EXCD_FOUND"),
    trim(F.col("Extract.CDDL_EOB_EXCD")).alias("CDDL_EOB_EXCD")
)

# Write "ClmLnDntl" to file => "verified/LhoFctsClmLnDntlExtr.ClmLnDntl.dat.#RunID#" (CSeqFileStage)
# For char/varchar columns, apply rpad to match length.
df_ClmLnDntl_write = df_StripClmLnDntl
char_cols_lengths = {
    "CLCL_ID":12,"PRPR_ID":12,"LOBD_ID":4,"PDVC_LOBD_PTR":1,"DPPY_PFX":4,"LTLT_PFX":4,"CGPY_PFX":4,
    "DPCG_DP_ID_ALT":5,"CDDL_ALTDP_EXCD_ID":3,"CGCG_ID":2,"CDDL_TOOTH_NO":2,"CDDL_TOOTH_BEG":2,
    "CDDL_TOOTH_END":2,"CDDL_SURF":6,"UTUT_CD":3,"DEDE_PFX":4,"DPPC_PRICE_ID":4,"DPDP_ID":5,
    "CGCG_RULE":3,"CDDL_PRICE_IND":1,"CDDL_CL_NTWK_IND":1,"CDDL_REF_IND":1,"CDDL_CAP_IND":1,
    "CDDL_UMREF_ID":9,"PSCD_ID":2,"CLCL_NTWK_IND":1,"CDDL_RESP_CD":?,"EXCD_FOUND":1, "CDDL_EOB_EXCD":3
}
# DataStage metadata did not specify length for "CDDL_RESP_CD" except "varchar". We must guess or skip. Suppose length=3.
char_cols_lengths["CDDL_RESP_CD"] = 3

for c, l in char_cols_lengths.items():
    df_ClmLnDntl_write = df_ClmLnDntl_write.withColumn(c, F.rpad(F.col(c), l, " "))

write_files(
    df_ClmLnDntl_write,
    f"{adls_path}/verified/LhoFctsClmLnDntlExtr.ClmLnDntl.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

###################################################
# Next stage: "by_status_type" for the data from df_hf_clm_sts
# "hf_clm_sts" => df_hf_clm_sts => "by_status_type" => "clmln_status" hashed file outputs
###################################################
# "by_status_type" is a transformer that splits data by constraints and passes them to separate links.

df_by_status_type_in = df_hf_clm_sts

df_Status_DeniedRej = df_by_status_type_in.filter(trim(F.col("CLM_LN_FINL_DISP_CD")) == "DENIEDREJ")
df_Status_Acptd = df_by_status_type_in.filter(trim(F.col("CLM_LN_FINL_DISP_CD")) == "ACPTD")
df_Status_Susp = df_by_status_type_in.filter(trim(F.col("CLM_LN_FINL_DISP_CD")) == "SUSP")
df_Status_ClsdDel = df_by_status_type_in.filter(trim(F.col("CLM_LN_FINL_DISP_CD")) == "CLSDDEL")
df_Status_NonPrice = df_by_status_type_in.filter(trim(F.col("CLM_LN_FINL_DISP_CD")) == "NONPRICE")

# Each of these goes to "clmln_status" hashed file but different file paths
# scenario C => we write them to parquet
df_Status_DeniedRej_write = df_Status_DeniedRej.select("CLCL_ID","CDDL_SEQ_NO","CDDL_CUR_STS","CLM_LN_FINL_DISP_CD")
write_files(
    df_Status_DeniedRej_write,
    f"{adls_path}/hf_clm_finalized_status_denied.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_Status_Acptd_write = df_Status_Acptd.select("CLCL_ID","CDDL_SEQ_NO","CDDL_CUR_STS","CLM_LN_FINL_DISP_CD")
write_files(
    df_Status_Acptd_write,
    f"{adls_path}/hf_clm_finalized_status_acptd.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_Status_Susp_write = df_Status_Susp.select("CLCL_ID","CDDL_SEQ_NO","CDDL_CUR_STS","CLM_LN_FINL_DISP_CD")
write_files(
    df_Status_Susp_write,
    f"{adls_path}/hf_clm_finalized_status_susp.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_Status_ClsdDel_write = df_Status_ClsdDel.select("CLCL_ID","CDDL_SEQ_NO","CDDL_CUR_STS","CLM_LN_FINL_DISP_CD")
write_files(
    df_Status_ClsdDel_write,
    f"{adls_path}/hf_clm_finalized_status_clsddel.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_Status_NonPrice_write = df_Status_NonPrice.select("CLCL_ID","CDDL_SEQ_NO","CDDL_CUR_STS","CLM_LN_FINL_DISP_CD")
write_files(
    df_Status_NonPrice_write,
    f"{adls_path}/hf_clm_finalized_status_nonprice.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

###################################################
# "BusinessRules" transformer with 3 inputs:
#   PrimaryLink => "StripDntlClmLn"
#   LookupLink => "fcts_reversals" => joined on CLCL_ID
#   LookupLink => "nasco_dup_lkup" => joined on CLCL_ID
# Then outputs => "DntlClmLine", "reversals".
###################################################

df_StripDntlClmLn_alias = df_StripDntlClmLn.alias("StripDntlClmLn")
df_fcts_reversals = df_hf_clm_fcts_reversals.alias("fcts_reversals")
df_nasco_dup_lkup = df_clm_nasco_dup_bypass.alias("nasco_dup_lkup")

# Join for fcts_reversals on (StripDntlClmLn.CLCL_ID == fcts_reversals.CLCL_ID)
df_br_join = df_StripDntlClmLn_alias.join(
    df_fcts_reversals,
    on=[ df_StripDntlClmLn_alias["CLCL_ID"] == df_fcts_reversals["CLCL_ID"] ],
    how="left"
)

# Then join for nasco_dup_lkup on (StripDntlClmLn.CLCL_ID == nasco_dup_lkup.CLM_ID)
df_br_join = df_br_join.join(
    df_nasco_dup_lkup,
    on=[ df_StripDntlClmLn_alias["CLCL_ID"] == df_nasco_dup_lkup["CLM_ID"] ],
    how="left"
)

# Two outputs: 
#   "DntlClmLine" constraint => IsNull(nasco_dup_lkup.CLM_ID) = @TRUE
#   "reversals" constraint => IsNull(fcts_reversals.CLCL_ID) = @FALSE and fcts_reversals.CLCL_CUR_STS in ('89','91','99')
df_DntlClmLine = df_br_join.filter(
    F.col("nasco_dup_lkup.CLM_ID").isNull()
)
df_reversals = df_br_join.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull())
    & (
        (F.col("fcts_reversals.CLCL_CUR_STS") == "89")
        | (F.col("fcts_reversals.CLCL_CUR_STS") == "91")
        | (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
    )
)

# DntlClmLine columns
df_DntlClmLine_out = df_DntlClmLine.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), trim(F.col("CLCL_ID")), F.lit(";"), F.col("CDDL_SEQ_NO")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("DNTL_CLM_LN_SK"),
    F.when(F.col("CLCL_ID").isNull() | (F.length(trim(F.col("CLCL_ID"))) == 0), F.lit("NA"))
     .otherwise(trim(F.col("CLCL_ID"))).alias("CLM_ID"),
    F.col("CDDL_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("DPCG_DP_ID_ALT").isNull() | (F.length(trim(F.col("DPCG_DP_ID_ALT"))) == 0), F.lit("NA"))
     .otherwise(F.upper(trim(F.col("DPCG_DP_ID_ALT")))).alias("ALT_PROC_CD"),
    F.when(F.col("CGCG_ID").isNull() | (F.length(trim(F.col("CGCG_ID"))) == 0), F.lit("NA"))
     .otherwise(F.upper(trim(F.col("CGCG_ID")))).alias("DNTL_CLM_LN_DNTL_CAT_CD"),
    F.when(F.col("CGCG_RULE").isNull() | (F.length(trim(F.col("CGCG_RULE"))) == 0), F.lit("NA"))
     .otherwise(F.upper(trim(F.col("CGCG_RULE")))).alias("DNTL_CLM_LN_DNTL_CAT_RULE_CD"),
    F.when(F.col("CDDL_ALTDP_EXCD_ID").isNull() | (F.length(trim(F.col("CDDL_ALTDP_EXCD_ID"))) == 0), F.lit("NA"))
     .otherwise(trim(F.col("CDDL_ALTDP_EXCD_ID"))).alias("DNTL_CLM_LN_ALT_PROC_EXCD"),
    F.when(F.trim(F.col("CGCG_ID")) == "PD", F.lit(1))
     .when((F.trim(F.col("CGCG_ID")) == "OS") | (F.trim(F.col("CGCG_ID")) == "BA") | (F.trim(F.col("CGCG_ID")) == "OX"), F.lit(2))
     .when((F.trim(F.col("CGCG_ID")) == "PE") | (F.trim(F.col("CGCG_ID")) == "MJ") | (F.trim(F.col("CGCG_ID")) == "TM"), F.lit(3))
     .when(F.trim(F.col("CGCG_ID")) == "OR", F.lit(4))
     .when((F.trim(F.col("CGCG_ID")) == "NC") | (F.trim(F.col("CGCG_ID")) == "XX"), F.lit("NB"))
     .otherwise(F.lit("NA")).alias("DNTL_CLM_LN_BNF_TYP_CD"),
    F.when(F.col("CDDL_SURF").isNull() | (F.length(trim(F.col("CDDL_SURF"))) == 0), F.lit("NA"))
     .otherwise(F.upper(trim(F.col("CDDL_SURF")))).alias("DNTL_CLM_LN_TOOTH_SRFC_CD"),
    F.when(F.col("UTUT_CD").isNull() | (F.length(trim(F.col("UTUT_CD"))) == 0), F.lit("NA"))
     .otherwise(F.upper(trim(F.col("UTUT_CD")))).alias("DNTL_CLM_LN_UTIL_EDIT_CD"),
    F.when(F.length(trim(F.col("CGPY_PFX"))) == 0,
           F.when(F.col("CDDL_CUR_STS") == "99", F.lit("NA")).otherwise(F.lit("UNK")))
     .otherwise(F.col("CGPY_PFX")).alias("CAT_PAYMT_PFX_ID"),
    F.when(F.length(trim(F.col("DPPY_PFX"))) == 0,
           F.when(F.col("CDDL_CUR_STS") == "99", F.lit("NA")).otherwise(F.lit("UNK")))
     .otherwise(F.col("DPPY_PFX")).alias("DNTL_PROC_PAYMT_PFX_ID"),
    F.when(F.length(trim(F.col("DPPC_PRICE_ID"))) == 0,
           F.when(F.col("CDDL_CUR_STS") == "99", F.lit("NA")).otherwise(F.lit("UNK")))
     .otherwise(F.col("DPPC_PRICE_ID")).alias("DNTL_PROC_PRICE_ID"),
    F.when(F.length(trim(F.col("CDDL_TOOTH_BEG"))) == 0, F.lit("NA"))
     .otherwise(F.col("CDDL_TOOTH_BEG").substr(F.lit(1), F.lit(2))).alias("TOOTH_BEG_NO"),
    F.when(F.length(trim(F.col("CDDL_TOOTH_END"))) == 0, F.lit("NA"))
     .otherwise(F.col("CDDL_TOOTH_END").substr(F.lit(1),F.lit(2))).alias("TOOTH_END_NO"),
    F.when(F.length(trim(F.col("CDDL_TOOTH_NO"))) == 0, F.lit("NA"))
     .otherwise(F.col("CDDL_TOOTH_NO").substr(F.lit(1),F.lit(2))).alias("TOOTH_NO")
)

df_reversals_out = df_reversals.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("StripDntlClmLn.CLCL_ID"), F.lit("R;"), F.col("StripDntlClmLn.CDDL_SEQ_NO")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("DNTL_CLM_LN_SK"),
    F.when(F.col("StripDntlClmLn.CLCL_ID").isNull() | (F.length(trim(F.col("StripDntlClmLn.CLCL_ID"))) == 0), F.lit("NA"))
     .otherwise(trim(F.col("StripDntlClmLn.CLCL_ID"))).substr(F.lit(1),F.lit(18)).alias("CLM_ID"),
    F.col("StripDntlClmLn.CDDL_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("StripDntlClmLn.DPCG_DP_ID_ALT").isNull() | (F.length(trim(F.col("StripDntlClmLn.DPCG_DP_ID_ALT"))) == 0), F.lit("NA"))
     .otherwise(F.upper(trim(F.col("StripDntlClmLn.DPCG_DP_ID_ALT")))).alias("ALT_PROC_CD"),
    F.when(F.col("StripDntlClmLn.CGCG_ID").isNull() | (F.length(trim(F.col("StripDntlClmLn.CGCG_ID"))) == 0), F.lit("NA"))
     .otherwise(F.upper(trim(F.col("StripDntlClmLn.CGCG_ID")))).alias("DNTL_CLM_LN_DNTL_CAT_CD"),
    F.when(F.col("StripDntlClmLn.CGCG_RULE").isNull() | (F.length(trim(F.col("StripDntlClmLn.CGCG_RULE"))) == 0), F.lit("NA"))
     .otherwise(F.upper(trim(F.col("StripDntlClmLn.CGCG_RULE")))).alias("DNTL_CLM_LN_DNTL_CAT_RULE_CD"),
    F.when(F.col("StripDntlClmLn.CDDL_ALTDP_EXCD_ID").isNull() | (F.length(trim(F.col("StripDntlClmLn.CDDL_ALTDP_EXCD_ID"))) == 0), F.lit("NA"))
     .otherwise(trim(F.col("StripDntlClmLn.CDDL_ALTDP_EXCD_ID"))).alias("DNTL_CLM_LN_ALT_PROC_EXCD"),
    F.when(F.trim(F.col("StripDntlClmLn.CGCG_ID")) == "PD", F.lit(1))
     .when((F.trim(F.col("StripDntlClmLn.CGCG_ID")) == "OS") | (F.trim(F.col("StripDntlClmLn.CGCG_ID")) == "BA") | (F.trim(F.col("StripDntlClmLn.CGCG_ID")) == "OX"), F.lit(2))
     .when((F.trim(F.col("StripDntlClmLn.CGCG_ID")) == "PE") | (F.trim(F.col("StripDntlClmLn.CGCG_ID")) == "MJ") | (F.trim(F.col("StripDntlClmLn.CGCG_ID")) == "TM"), F.lit(3))
     .when(F.trim(F.col("StripDntlClmLn.CGCG_ID")) == "OR", F.lit(4))
     .when((F.trim(F.col("StripDntlClmLn.CGCG_ID")) == "NC") | (F.trim(F.col("StripDntlClmLn.CGCG_ID")) == "XX"), F.lit("NB"))
     .otherwise(F.lit("NA")).alias("DNTL_CLM_LN_BNF_TYP_CD"),
    F.when(F.col("StripDntlClmLn.CDDL_SURF").isNull() | (F.length(trim(F.col("StripDntlClmLn.CDDL_SURF"))) == 0), F.lit("NA"))
     .otherwise(F.upper(trim(F.col("StripDntlClmLn.CDDL_SURF")))).alias("DNTL_CLM_LN_TOOTH_SRFC_CD"),
    F.when(F.col("StripDntlClmLn.UTUT_CD").isNull() | (F.length(trim(F.col("StripDntlClmLn.UTUT_CD"))) == 0), F.lit("NA"))
     .otherwise(F.upper(trim(F.col("StripDntlClmLn.UTUT_CD")))).alias("DNTL_CLM_LN_UTIL_EDIT_CD"),
    F.when(F.col("StripDntlClmLn.CGPY_PFX").isNull() | (F.length(trim(F.col("StripDntlClmLn.CGPY_PFX"))) == 0), F.lit("U"))
     .otherwise(F.upper(trim(F.col("StripDntlClmLn.CGPY_PFX")))).alias("CAT_PAYMT_PFX_ID"),
    F.when(F.col("StripDntlClmLn.DPPY_PFX").isNull() | (F.length(trim(F.col("StripDntlClmLn.DPPY_PFX"))) == 0), F.lit("U"))
     .otherwise(F.upper(trim(F.col("StripDntlClmLn.DPPY_PFX")))).alias("DNTL_PROC_PAYMT_PFX_ID"),
    F.when(F.col("StripDntlClmLn.DPPC_PRICE_ID").isNull() | (F.length(trim(F.col("StripDntlClmLn.DPPC_PRICE_ID"))) == 0), F.lit("U"))
     .otherwise(F.upper(trim(F.col("StripDntlClmLn.DPPC_PRICE_ID")))).alias("DNTL_PROC_PRICE_ID"),
    F.when(F.length(trim(F.col("StripDntlClmLn.CDDL_TOOTH_BEG"))) == 0, F.lit("NA"))
     .otherwise(F.col("StripDntlClmLn.CDDL_TOOTH_BEG").substr(F.lit(1),F.lit(2))).alias("TOOTH_BEG_NO"),
    F.when(F.length(trim(F.col("StripDntlClmLn.CDDL_TOOTH_END"))) == 0, F.lit("NA"))
     .otherwise(F.col("StripDntlClmLn.CDDL_TOOTH_END").substr(F.lit(1),F.lit(2))).alias("TOOTH_END_NO"),
    F.when(F.length(trim(F.col("StripDntlClmLn.CDDL_TOOTH_NO"))) == 0, F.lit("NA"))
     .otherwise(F.col("StripDntlClmLn.CDDL_TOOTH_NO").substr(F.lit(1),F.lit(2))).alias("TOOTH_NO")
)

###################################################
# "Collector" => merges "reversals" and "DntlClmLine"
# Round-robin => we just union. Then output => "Transform" => "Snapshot"
###################################################

df_collector = df_reversals_out.unionByName(df_DntlClmLine_out)

# "Snapshot" => Another transformer => output pins "Pkey", "Snapshot"
df_Snapshot_in = df_collector

df_Snapshot_Pkey = df_Snapshot_in.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("DNTL_CLM_LN_SK").alias("DNTL_CLM_LN_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ALT_PROC_CD").alias("ALT_PROC_CD"),
    F.col("DNTL_CLM_LN_DNTL_CAT_CD").alias("DNTL_CLM_LN_DNTL_CAT_CD"),
    F.col("DNTL_CLM_LN_DNTL_CAT_RULE_CD").alias("DNTL_CLM_LN_DNTL_CAT_RULE_CD"),
    F.col("DNTL_CLM_LN_ALT_PROC_EXCD").alias("DNTL_CLM_LN_ALT_PROC_EXCD"),
    F.col("DNTL_CLM_LN_BNF_TYP_CD").alias("DNTL_CLM_LN_BNF_TYP_CD"),
    F.col("DNTL_CLM_LN_TOOTH_SRFC_CD").alias("DNTL_CLM_LN_TOOTH_SRFC_CD"),
    F.col("DNTL_CLM_LN_UTIL_EDIT_CD").alias("DNTL_CLM_LN_UTIL_EDIT_CD"),
    F.col("CAT_PAYMT_PFX_ID").alias("CAT_PAYMT_PFX_ID"),
    F.col("DNTL_PROC_PAYMT_PFX_ID").alias("DNTL_PROC_PAYMT_PFX_ID"),
    F.col("DNTL_PROC_PRICE_ID").alias("DNTL_PROC_PRICE_ID"),
    F.col("TOOTH_BEG_NO").alias("TOOTH_BEG_NO"),
    F.col("TOOTH_END_NO").alias("TOOTH_END_NO"),
    F.col("TOOTH_NO").alias("TOOTH_NO")
)

df_Snapshot_Snapshot = df_Snapshot_in.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO")
)

# Write B_DNTL_CLM_LN => "load/B_DNTL_CLM_LN.dat.#RunID#"
df_B_DNTL_CLM_LN_write = df_Snapshot_Snapshot
write_files(
    df_B_DNTL_CLM_LN_write,
    f"{adls_path}/load/B_DNTL_CLM_LN.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

###################################################
# "DntlClmLnPK" => Shared Container => we pass "Pkey" => gets "Key"
# The container references transformations,
# but per instructions we simply call the shared container as a function
###################################################
params_DntlClmLnPK = {
    "CurrRunCycle": CurrRunCycle
}
df_pkey_output = DntlClmLnPK(df_Snapshot_Pkey, params_DntlClmLnPK)

###################################################
# "IdsDntlClmLineExtr" => CSeqFileStage => writes "key/LhoFctsDntlClmLnExtr.LhoFctsDntlClmLn.dat.#RunID#"
###################################################
df_IdsDntlClmLineExtr = df_pkey_output.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "DNTL_CLM_LN_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ALT_PROC_CD",
    "DNTL_CLM_LN_DNTL_CAT_CD",
    "DNTL_CLM_LN_DNTL_CAT_RULE_CD",
    "DNTL_CLM_LN_ALT_PROC_EXCD",
    "DNTL_CLM_LN_BNF_TYP_CD",
    "DNTL_CLM_LN_TOOTH_SRFC_CD",
    "DNTL_CLM_LN_UTIL_EDIT_CD",
    "CAT_PAYMT_PFX_ID",
    "DNTL_PROC_PAYMT_PFX_ID",
    "DNTL_PROC_PRICE_ID",
    "TOOTH_BEG_NO",
    "TOOTH_END_NO",
    "TOOTH_NO"
)

# For char/varchar columns, do rpad if lengths are known from the JSON. 
# The job definitions mention lengths for some columns. We match them if specified.
col_len_map_ids = {
    "INSRT_UPDT_CD":10,"DISCARD_IN":1,"PASS_THRU_IN":1,"CLM_ID":18,"ALT_PROC_CD":5,"DNTL_CLM_LN_ALT_PROC_EXCD":3,
    "DNTL_CLM_LN_BNF_TYP_CD":2,"DNTL_CLM_LN_TOOTH_SRFC_CD":10,"DNTL_CLM_LN_UTIL_EDIT_CD":3,"CAT_PAYMT_PFX_ID":4,
    "DNTL_PROC_PAYMT_PFX_ID":4,"DNTL_PROC_PRICE_ID":4,"TOOTH_BEG_NO":2,"TOOTH_END_NO":2,"TOOTH_NO":2
}
df_ids_out = df_IdsDntlClmLineExtr
for c, l in col_len_map_ids.items():
    df_ids_out = df_ids_out.withColumn(c, F.rpad(F.col(c), l, " "))

write_files(
    df_ids_out,
    f"{adls_path}/key/LhoFctsDntlClmLnExtr.LhoFctsDntlClmLn.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)