# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsDntlClmLineExtr
# MAGIC CALLED BY:  FctsClmPrereqSeq
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
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                         Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                                 --------------------------------       -------------------------------   ----------------------------    
# MAGIC O. Nielsen              08/15/2007        Balancing             Added Snapshot File extraction for Balancing Project                                                          devlIDS30                  Steph Goddard             8/30/07
# MAGIC 
# MAGIC 
# MAGIC Parik                         2008-08-06      3567(Primary Key)   Added the new primary keying process                                                                                devlIDS                    Steph Goddard            08/12/2008
# MAGIC  
# MAGIC                                                       
# MAGIC Rick Henry              2012-05-02        4896                     Cleaned up Diplay and Element for Shared container compatibility                                       NewDevl                       SAndrew                        2012-05-18
# MAGIC 
# MAGIC Dan Long                06/06/2013      TTR-1538             Changed the name of the Balancing file to B_DNTL_CLM_LN.dat.#RUNID#                  IntegrateNewDevl        Bhoomi Dasari            6/7/2013
# MAGIC                                                                                        and changed the name of the stage from B_CLM_LN to B_DNTL_CLM_LN.  
# MAGIC 
# MAGIC Manasa Andru        2015-04-22        TFS - 12493         Updated the field - DNTL_CLM_LN_ALT_PROC_EXCD to No Upcase                             IntegrateDev2                Jag Yelavarthi               2016-05-03
# MAGIC                                                                                                so that the field would find a match on the EXCD table.
# MAGIC Prabhu ES              2022-02-26       S2S Remediation    MSSQL connection parameters added                                                                               IntegrateDev5	Ken Bradmon	2022-06-10

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
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/DntlClmLnPK
# COMMAND ----------

RunID = get_widget_value('RunID','')
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
jpClmSts = get_widget_value('jpClmSts','')
jpClmStsClsd = get_widget_value('jpClmStsClsd','')
ipClmStsActive = get_widget_value('ipClmStsActive','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_Ids_all = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
        SELECT
            DSALW_EXCD.EXCD_ID,
            DSALW_EXCD.EFF_DT_SK,
            DSALW_EXCD.TERM_DT_SK,
            CD_MPPNG.SRC_CD,
            DSALW_EXCD.BYPS_IN
        FROM {IDSOwner}.DSALW_EXCD
        JOIN {IDSOwner}.CD_MPPNG
            ON DSALW_EXCD.EXCD_RESP_CD_SK = CD_MPPNG.CD_MPPNG_SK
        """
    )
    .load()
)

hf_clm_ln_dsalw_ln_amts_schema = StructType([
    StructField("CLCL_ID", StringType(), True),
    StructField("CDML_SEQ_NO", IntegerType(), True),
    StructField("EXCD_RESP_CD", StringType(), True),
    StructField("BYPS_IN", StringType(), True),
    StructField("DSALW_AMT", DecimalType(38,10), True)
])
df_hf_clm_ln_dsalw_ln_amts = spark.read.parquet(f"hf_clm_ln_dsalw_ln_amts.parquet")

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
df_Facets2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""
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
        FROM tempdb..{DriverTable} TMP
        JOIN {FacetsOwner}.CMC_CDDL_CL_LINE CL_LINE
            ON TMP.CLM_ID = CL_LINE.CLCL_ID
        JOIN {FacetsOwner}.CMC_CLCL_CLAIM CLAIM
            ON CLAIM.CLCL_ID = TMP.CLM_ID
        """
    )
    .load()
)

df_Facets = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""
        SELECT
            OVR.CLCL_ID,
            OVR.CDML_SEQ_NO,
            OVR.CDOR_OR_AMT
        FROM tempdb..{DriverTable} TMP,
             {FacetsOwner}.CMC_CDOR_LI_OVR OVR
        WHERE TMP.CLM_ID = OVR.CLCL_ID
          AND CDOR_OR_ID = 'DX'
          AND EXCD_ID = 'Y08'
        """
    )
    .load()
)

df_facets_dedup_y08 = dedup_sort(
    df_Facets,
    partition_cols=["CLCL_ID","CDML_SEQ_NO"],
    sort_cols=[]
)

hf_clm_fcts_reversals_schema = StructType([
    StructField("CLCL_ID", StringType(), True),
    StructField("CLCL_CUR_STS", StringType(), True),
    StructField("CLCL_PAID_DT", TimestampType(), True),
    StructField("CLCL_ID_ADJ_TO", StringType(), True),
    StructField("CLCL_ID_ADJ_FROM", StringType(), True)
])
df_hf_clm_fcts_reversals = spark.read.parquet(f"hf_clm_fcts_reversals.parquet")

clm_nasco_dup_bypass_schema = StructType([
    StructField("CLM_ID", StringType(), True)
])
df_clm_nasco_dup_bypass = spark.read.parquet(f"hf_clm_nasco_dup_bypass.parquet")

############################################################
# Primary link for StripFields is df_Facets2 as "Extract".
# Left lookups:
# 1) df_Ids_all as "DsalwExcdCddl" with conditions:
#    Extract.CDDL_DISALL_EXCD == DsalwExcdCddl.EXCD_ID
#    DsalwExcdCddl.EFF_DT_SK <= Extract.CLCL_PAID_DT.substr(0, 10)
#    DsalwExcdCddl.TERM_DT_SK >= Extract.CLCL_PAID_DT.substr(0, 10)
# 2) df_facets_dedup_y08 as "Y08" with:
#    Extract.CLCL_ID == Y08.CLCL_ID
#    Extract.CDDL_SEQ_NO == Y08.CDML_SEQ_NO
# 3) df_hf_clm_ln_dsalw_ln_amts joined multiple times with different aliases/filters:
#    RefMY:   EXCD_RESP_CD='M', BYPS_IN='Y'
#    RefNY:   EXCD_RESP_CD='N', BYPS_IN='Y'
#    RefNN:   EXCD_RESP_CD='N', BYPS_IN='N'
#    RefOY:   EXCD_RESP_CD='O', BYPS_IN='Y'
#    RefPY:   EXCD_RESP_CD='P', BYPS_IN='Y'
############################################################

df_extract_alias = df_Facets2.alias("Extract")
df_ids_alias = df_Ids_all.alias("DsalwExcdCddl")
df_y08_alias = df_facets_dedup_y08.alias("Y08")
df_amts_alias = df_hf_clm_ln_dsalw_ln_amts.alias("hfamts")

join_cond_dsalwexcd = [
    df_extract_alias["CDDL_DISALL_EXCD"] == df_ids_alias["EXCD_ID"],
    df_ids_alias["EFF_DT_SK"] <= F.substring(df_extract_alias["CLCL_PAID_DT"].cast(StringType()),1,10),
    df_ids_alias["TERM_DT_SK"] >= F.substring(df_extract_alias["CLCL_PAID_DT"].cast(StringType()),1,10)
]
df_join_dsalwexcd = df_extract_alias.join(df_ids_alias, on=join_cond_dsalwexcd, how="left")

join_cond_y08 = [
    df_join_dsalwexcd["CLCL_ID"] == df_y08_alias["CLCL_ID"],
    df_join_dsalwexcd["CDDL_SEQ_NO"] == df_y08_alias["CDML_SEQ_NO"]
]
df_join_y08 = df_join_dsalwexcd.join(df_y08_alias, on=join_cond_y08, how="left")

refmy = df_amts_alias.filter((F.col("EXCD_RESP_CD")=="M") & (F.col("BYPS_IN")=="Y")).alias("RefMY")
join_cond_refmy = [
    df_join_y08["CLCL_ID"] == refmy["CLCL_ID"],
    df_join_y08["CDDL_SEQ_NO"] == refmy["CDML_SEQ_NO"]
]
df_join_refmy = df_join_y08.join(refmy, on=join_cond_refmy, how="left")

refny = df_amts_alias.filter((F.col("EXCD_RESP_CD")=="N") & (F.col("BYPS_IN")=="Y")).alias("RefNY")
join_cond_refny = [
    df_join_refmy["CLCL_ID"] == refny["CLCL_ID"],
    df_join_refmy["CDDL_SEQ_NO"] == refny["CDML_SEQ_NO"]
]
df_join_refny = df_join_refmy.join(refny, on=join_cond_refny, how="left")

refnn = df_amts_alias.filter((F.col("EXCD_RESP_CD")=="N") & (F.col("BYPS_IN")=="N")).alias("RefNN")
join_cond_refnn = [
    df_join_refny["CLCL_ID"] == refnn["CLCL_ID"],
    df_join_refny["CDDL_SEQ_NO"] == refnn["CDML_SEQ_NO"]
]
df_join_refnn = df_join_refny.join(refnn, on=join_cond_refnn, how="left")

refoy = df_amts_alias.filter((F.col("EXCD_RESP_CD")=="O") & (F.col("BYPS_IN")=="Y")).alias("RefOY")
join_cond_refoy = [
    df_join_refnn["CLCL_ID"] == refoy["CLCL_ID"],
    df_join_refnn["CDDL_SEQ_NO"] == refoy["CDML_SEQ_NO"]
]
df_join_refoy = df_join_refnn.join(refoy, on=join_cond_refoy, how="left")

refpy = df_amts_alias.filter((F.col("EXCD_RESP_CD")=="P") & (F.col("BYPS_IN")=="Y")).alias("RefPY")
join_cond_refpy = [
    df_join_refoy["CLCL_ID"] == refpy["CLCL_ID"],
    df_join_refoy["CDDL_SEQ_NO"] == refpy["CDML_SEQ_NO"]
]
df_final_strip = df_join_refoy.join(refpy, on=join_cond_refpy, how="left")

df_final_strip = df_final_strip.withColumn("svNA", F.when(
    (F.col("CDDL_CUR_STS").isNotNull()) & (F.instr(jpClmSts, F.col("CDDL_CUR_STS")) < 1),
    F.lit("NA")
).otherwise(F.lit("")))

df_final_strip = df_final_strip.withColumn("svClsdDel", F.when(
    (F.col("svNA") != "") , F.lit("")
).otherwise(
    F.when(
        F.instr(jpClmStsClsd, F.col("CDDL_CUR_STS")) > 0,
        F.lit("CLSDDEL")
    ).otherwise(F.lit(""))
))

df_final_strip = df_final_strip.withColumn("svAcptd", F.when(
    (F.col("svNA") != "") | (F.trim(F.col("svClsdDel")) != ""),
    F.lit("")
).otherwise(
    F.when(
        F.instr(ipClmStsActive, F.col("CDDL_CUR_STS")) < 1,
        F.lit("")
    ).otherwise(
        F.when(
            (F.col("CDDL_PR_PYMT_AMT").isNotNull() & (F.col("CDDL_PR_PYMT_AMT") != 0))
            | (F.col("CDDL_SB_PYMT_AMT").isNotNull() & (F.col("CDDL_SB_PYMT_AMT") != 0)),
            F.lit("ACPTD")
        ).otherwise(F.lit(""))
    )
))

df_final_strip = df_final_strip.withColumn("svSuspend", F.when(
    (F.trim(F.col("svNA")) == "NA") | (F.trim(F.col("svClsdDel")) == "CLSDDEL") | (F.trim(F.col("svAcptd")) == "ACPTD"),
    F.lit("")
).otherwise(
    F.when(
        F.col("RefNY.CLCL_ID").isNotNull(),
        F.lit("SUSP")
    ).otherwise(
        F.when(
            (F.col("RefOY.CLCL_ID").isNotNull()) | (F.col("RefPY.CLCL_ID").isNotNull()) | (F.col("RefMY.CLCL_ID").isNotNull()),
            F.lit("")
        ).otherwise(
            F.when(
                F.col("RefNN.CLCL_ID").isNotNull(),
                F.lit("SUSP")
            ).otherwise(F.lit(""))
        )
    )
))

df_final_strip = df_final_strip.withColumn("svDenied", F.when(
    (F.col("svNA") != "") | (F.col("svAcptd") != "") | (F.col("svClsdDel") != "") | (F.col("svSuspend") != ""),
    F.lit("")
).otherwise(
    F.when(
        (F.col("CDDL_CONSIDER_CHG") == 0) & (F.col("CDDL_CHG_AMT") == 0),
        F.lit("DENIEDREJ")
    ).otherwise(
        F.when(
            (F.col("CDDL_CONSIDER_CHG") <= (F.col("CDDL_DISALL_AMT") - F.when(F.col("Y08.CLCL_ID").isNotNull(),F.col("Y08.CDOR_OR_AMT")).otherwise(F.lit(0)))),
            F.when(
                F.length(F.trim(F.col("CDDL_DISALL_EXCD"))) == 0,
                F.lit("NONPRICE")
            ).otherwise(F.lit("DENIEDREJ"))
        ).otherwise(
            F.lit("ACPTD")
        )
    )
))

df_final_strip = df_final_strip.withColumn("svFinalDispCd",
    F.when(
        F.trim(F.col("svNA")) != "",
        F.col("svNA")
    ).otherwise(
        F.when(
            F.trim(F.col("svClsdDel")) != "",
            F.trim(F.col("svClsdDel"))
        ).otherwise(
            F.when(
                F.trim(F.col("svAcptd")) != "",
                F.col("svAcptd")
            ).otherwise(
                F.when(
                    F.col("svSuspend") != "",
                    F.col("svSuspend")
                ).otherwise(
                    F.when(
                        F.trim(F.col("svDenied")) != "",
                        F.col("svDenied")
                    ).otherwise(F.lit("ACPTD"))
                )
            )
        )
    )
)

df_StripFields = df_final_strip

select_cols_StripDntlClmLn = [
    F.rpad(F.regexp_replace(F.col("CLCL_ID"), "[\\x0A\\x0D\\x09]", ""), 12, " ").alias("CLCL_ID"),
    F.col("CDDL_SEQ_NO").alias("CDDL_SEQ_NO"),
    current_date().alias("EXT_TIMESTAMP"),
    F.rpad(F.regexp_replace(F.col("PRPR_ID"), "[\\x0A\\x0D\\x09]", ""), 12, " ").alias("PRPR_ID"),
    F.rpad(F.regexp_replace(F.col("LOBD_ID"), "[\\x0A\\x0D\\x09]", ""), 4, " ").alias("LOBD_ID"),
    F.rpad(F.regexp_replace(F.col("PDVC_LOBD_PTR"), "[\\x0A\\x0D\\x09]", ""), 1, " ").alias("PDVC_LOBD_PTR"),
    F.rpad(F.regexp_replace(F.col("DPPY_PFX"), "[\\x0A\\x0D\\x09]", ""), 4, " ").alias("DPPY_PFX"),
    F.rpad(F.regexp_replace(F.col("LTLT_PFX"), "[\\x0A\\x0D\\x09]", ""), 4, " ").alias("LTLT_PFX"),
    F.rpad(F.regexp_replace(F.col("CGPY_PFX"), "[\\x0A\\x0D\\x09]", ""), 4, " ").alias("CGPY_PFX"),
    F.rpad(F.regexp_replace(F.col("DPCG_DP_ID_ALT"), "[\\x0A\\x0D\\x09]", ""), 5, " ").alias("DPCG_DP_ID_ALT"),
    F.rpad(F.regexp_replace(F.col("CDDL_ALTDP_EXCD_ID"), "[\\x0A\\x0D\\x09]", ""), 3, " ").alias("CDDL_ALTDP_EXCD_ID"),
    F.rpad(F.regexp_replace(F.col("CGCG_ID"), "[\\x0A\\x0D\\x09]", ""), 2, " ").alias("CGCG_ID"),
    F.rpad(F.regexp_replace(F.col("CDDL_TOOTH_NO"), "[\\x0A\\x0D\\x09]", ""), 2, " ").alias("CDDL_TOOTH_NO"),
    F.rpad(F.regexp_replace(F.col("CDDL_TOOTH_BEG"), "[\\x0A\\x0D\\x09]", ""), 2, " ").alias("CDDL_TOOTH_BEG"),
    F.rpad(F.regexp_replace(F.col("CDDL_TOOTH_END"), "[\\x0A\\x0D\\x09]", ""), 2, " ").alias("CDDL_TOOTH_END"),
    F.rpad(F.regexp_replace(F.col("CDDL_SURF"), "[\\x0A\\x0D\\x09]", ""), 6, " ").alias("CDDL_SURF"),
    F.rpad(F.regexp_replace(F.col("UTUT_CD"), "[\\x0A\\x0D\\x09]", ""), 3, " ").alias("UTUT_CD"),
    F.rpad(F.regexp_replace(F.col("DEDE_PFX"), "[\\x0A\\x0D\\x09]", ""), 4, " ").alias("DEDE_PFX"),
    F.rpad(F.regexp_replace(F.col("DPPC_PRICE_ID"), "[\\x0A\\x0D\\x09]", ""), 4, " ").alias("DPPC_PRICE_ID"),
    F.rpad(F.regexp_replace(F.col("DPDP_ID"), "[\\x0A\\x0D\\x09]", ""), 5, " ").alias("DPDP_ID"),
    F.rpad(F.regexp_replace(F.col("CGCG_RULE"), "[\\x0A\\x0D\\x09]", ""), 3, " ").alias("CGCG_RULE"),
    F.col("CDDL_FROM_DT").alias("CDDL_FROM_DT"),
    F.col("CDDL_CHG_AMT").alias("CDDL_CHG_AMT"),
    F.col("CDDL_CONSIDER_CHG").alias("CDDL_CONSIDER_CHG"),
    F.col("CDDL_ALLOW").alias("CDDL_ALLOW"),
    F.col("CDDL_UNITS").alias("CDDL_UNITS"),
    F.col("CDDL_UNITS_ALLOW").alias("CDDL_UNITS_ALLOW"),
    F.col("CDDL_DED_AMT").alias("CDDL_DED_AMT"),
    F.col("CDDL_DED_AC_NO").alias("CDDL_DED_AC_NO"),
    F.col("CDDL_COPAY_AMT").alias("CDDL_COPAY_AMT"),
    F.col("CDDL_COINS_AMT").alias("CDDL_COINS_AMT"),
    F.col("CDDL_RISK_WH_AMT").alias("CDDL_RISK_WH_AMT"),
    F.col("CDDL_PAID_AMT").alias("CDDL_PAID_AMT"),
    F.col("CDDL_DISALL_AMT").alias("CDDL_DISALL_AMT"),
    F.rpad(F.regexp_replace(F.col("CDDL_DISALL_EXCD"), "[\\x0A\\x0D\\x09]", ""), 3, " ").alias("CDDL_DISALL_EXCD"),
    F.col("CDDL_AG_PRICE").alias("CDDL_AG_PRICE"),
    F.col("CDDL_PF_PRICE").alias("CDDL_PF_PRICE"),
    F.col("CDDL_DP_PRICE").alias("CDDL_DP_PRICE"),
    F.col("CDDL_IP_PRICE").alias("CDDL_IP_PRICE"),
    F.rpad(F.regexp_replace(F.col("CDDL_PRICE_IND"), "[\\x0A\\x0D\\x09]", ""), 1, " ").alias("CDDL_PRICE_IND"),
    F.col("CDDL_OOP_CALC_BASE").alias("CDDL_OOP_CALC_BASE"),
    F.rpad(F.regexp_replace(F.col("CDDL_CL_NTWK_IND"), "[\\x0A\\x0D\\x09]", ""), 1, " ").alias("CDDL_CL_NTWK_IND"),
    F.rpad(F.regexp_replace(F.col("CDDL_REF_IND"), "[\\x0A\\x0D\\x09]", ""), 1, " ").alias("CDDL_REF_IND"),
    F.rpad(F.regexp_replace(F.col("CDDL_CAP_IND"), "[\\x0A\\x0D\\x09]", ""), 1, " ").alias("CDDL_CAP_IND"),
    F.col("CDDL_SB_PYMT_AMT").alias("CDDL_SB_PYMT_AMT"),
    F.col("CDDL_PR_PYMT_AMT").alias("CDDL_PR_PYMT_AMT"),
    F.rpad(F.regexp_replace(F.col("CDDL_UMREF_ID"), "[\\x0A\\x0D\\x09]", ""), 9, " ").alias("CDDL_UMREF_ID"),
    F.col("CDDL_REFSV_SEQ_NO").alias("CDDL_REFSV_SEQ_NO"),
    F.col("svFinalDispCd").alias("CLM_LN_FINL_DISP_CD"),
    F.rpad(F.when(F.col("PSDC_ID").isNull(), F.lit("")).otherwise(F.trim(F.col("PSDC_ID"))), 2, " ").alias("PSCD_ID"),
    F.rpad(F.regexp_replace(F.col("CDDL_EOB_EXCD"), "[\\x0A\\x0D\\x09]", ""), 3, " ").alias("CDDL_EOB_EXCD"),
    F.rpad(F.col("CDDL_CUR_STS"), 2, " ").alias("CDDL_CUR_STS")
]
df_StripDntlClmLn = df_StripFields.select(select_cols_StripDntlClmLn)

select_cols_hf_clm_sts = [
    F.rpad(F.trim(F.col("CLCL_ID")), 12, " ").alias("CLCL_ID"),
    F.col("CDDL_SEQ_NO").alias("CDDL_SEQ_NO"),
    F.rpad(
        F.when(
            (F.length(F.trim(F.col("CDDL_CUR_STS")))==0) | (F.col("CDDL_CUR_STS").isNull()),
            F.lit("00")
        ).otherwise(F.trim(F.col("CDDL_CUR_STS"))),
        2,
        " "
    ).alias("CDDL_CUR_STS"),
    F.rpad(F.trim(F.col("svFinalDispCd")), 10, " ").alias("CLM_LN_FINL_DISP_CD")
]
df_hf_clm_sts_out = df_StripFields.select(select_cols_hf_clm_sts)

select_cols_StripClmLnDntl = [
    F.rpad(F.regexp_replace(F.col("CLCL_ID"), "[\\x0A\\x0D\\x09]", ""), 12, " ").alias("CLCL_ID"),
    F.col("CDDL_SEQ_NO").alias("CDDL_SEQ_NO"),
    current_date().alias("EXT_TIMESTAMP"),
    F.rpad(F.regexp_replace(F.col("PRPR_ID"), "[\\x0A\\x0D\\x09]", ""), 12, " ").alias("PRPR_ID"),
    F.rpad(F.regexp_replace(F.col("LOBD_ID"), "[\\x0A\\x0D\\x09]", ""), 4, " ").alias("LOBD_ID"),
    F.rpad(F.regexp_replace(F.col("PDVC_LOBD_PTR"), "[\\x0A\\x0D\\x09]", ""), 1, " ").alias("PDVC_LOBD_PTR"),
    F.rpad(F.regexp_replace(F.col("DPPY_PFX"), "[\\x0A\\x0D\\x09]", ""), 4, " ").alias("DPPY_PFX"),
    F.rpad(F.regexp_replace(F.col("LTLT_PFX"), "[\\x0A\\x0D\\x09]", ""), 4, " ").alias("LTLT_PFX"),
    F.rpad(F.regexp_replace(F.col("CGPY_PFX"), "[\\x0A\\x0D\\x09]", ""), 4, " ").alias("CGPY_PFX"),
    F.rpad(F.regexp_replace(F.col("DPCG_DP_ID_ALT"), "[\\x0A\\x0D\\x09]", ""), 5, " ").alias("DPCG_DP_ID_ALT"),
    F.rpad(F.regexp_replace(F.col("DPCG_DP_ID_ALT"), "[\\x0A\\x0D\\x09]", ""), 3, " ").alias("CDDL_ALTDP_EXCD_ID"),
    F.rpad(F.regexp_replace(F.col("CGCG_ID"), "[\\x0A\\x0D\\x09]", ""), 2, " ").alias("CGCG_ID"),
    F.rpad(F.regexp_replace(F.col("CDDL_TOOTH_NO"), "[\\x0A\\x0D\\x09]", ""), 2, " ").alias("CDDL_TOOTH_NO"),
    F.rpad(F.regexp_replace(F.col("CDDL_TOOTH_BEG"), "[\\x0A\\x0D\\x09]", ""), 2, " ").alias("CDDL_TOOTH_BEG"),
    F.rpad(F.regexp_replace(F.col("CDDL_TOOTH_END"), "[\\x0A\\x0D\\x09]", ""), 2, " ").alias("CDDL_TOOTH_END"),
    F.rpad(F.regexp_replace(F.col("CDDL_SURF"), "[\\x0A\\x0D\\x09]", ""), 6, " ").alias("CDDL_SURF"),
    F.rpad(F.regexp_replace(F.col("UTUT_CD"), "[\\x0A\\x0D\\x09]", ""), 3, " ").alias("UTUT_CD"),
    F.rpad(F.regexp_replace(F.col("DEDE_PFX"), "[\\x0A\\x0D\\x09]", ""), 4, " ").alias("DEDE_PFX"),
    F.rpad(F.regexp_replace(F.col("DPPC_PRICE_ID"), "[\\x0A\\x0D\\x09]", ""), 4, " ").alias("DPPC_PRICE_ID"),
    F.rpad(F.regexp_replace(F.col("DPDP_ID"), "[\\x0A\\x0D\\x09]", ""), 5, " ").alias("DPDP_ID"),
    F.rpad(F.regexp_replace(F.col("CGCG_RULE"), "[\\x0A\\x0D\\x09]", ""), 3, " ").alias("CGCG_RULE"),
    F.col("CDDL_FROM_DT").alias("CDDL_FROM_DT"),
    F.col("CDDL_CHG_AMT").alias("CDDL_CHG_AMT"),
    F.col("CDDL_CONSIDER_CHG").alias("CDDL_CONSIDER_CHG"),
    F.col("CDDL_ALLOW").alias("CDDL_ALLOW"),
    F.col("CDDL_UNITS").alias("CDDL_UNITS"),
    F.col("CDDL_UNITS_ALLOW").alias("CDDL_UNITS_ALLOW"),
    F.col("CDDL_DED_AMT").alias("CDDL_DED_AMT"),
    F.col("CDDL_DED_AC_NO").alias("CDDL_DED_AC_NO"),
    F.col("CDDL_COPAY_AMT").alias("CDDL_COPAY_AMT"),
    F.col("CDDL_COINS_AMT").alias("CDDL_COINS_AMT"),
    F.col("CDDL_RISK_WH_AMT").alias("CDDL_RISK_WH_AMT"),
    F.col("CDDL_PAID_AMT").alias("CDDL_PAID_AMT"),
    F.col("CDDL_DISALL_AMT").alias("CDDL_DISALL_AMT"),
    F.rpad(F.regexp_replace(F.col("CDDL_DISALL_EXCD"), "[\\x0A\\x0D\\x09]", ""), 3, " ").alias("CDDL_DISALL_EXCD"),
    F.col("CDDL_AG_PRICE").alias("CDDL_AG_PRICE"),
    F.col("CDDL_PF_PRICE").alias("CDDL_PF_PRICE"),
    F.col("CDDL_DP_PRICE").alias("CDDL_DP_PRICE"),
    F.col("CDDL_IP_PRICE").alias("CDDL_IP_PRICE"),
    F.rpad(F.regexp_replace(F.col("CDDL_PRICE_IND"), "[\\x0A\\x0D\\x09]", ""), 1, " ").alias("CDDL_PRICE_IND"),
    F.col("CDDL_OOP_CALC_BASE").alias("CDDL_OOP_CALC_BASE"),
    F.rpad(F.regexp_replace(F.col("CDDL_CL_NTWK_IND"), "[\\x0A\\x0D\\x09]", ""), 1, " ").alias("CDDL_CL_NTWK_IND"),
    F.rpad(F.regexp_replace(F.col("CDDL_REF_IND"), "[\\x0A\\x0D\\x09]", ""), 1, " ").alias("CDDL_REF_IND"),
    F.rpad(F.regexp_replace(F.col("CDDL_CAP_IND"), "[\\x0A\\x0D\\x09]", ""), 1, " ").alias("CDDL_CAP_IND"),
    F.col("CDDL_SB_PYMT_AMT").alias("CDDL_SB_PYMT_AMT"),
    F.col("CDDL_PR_PYMT_AMT").alias("CDDL_PR_PYMT_AMT"),
    F.rpad(F.regexp_replace(F.col("CDDL_UMREF_ID"), "[\\x0A\\x0D\\x09]", ""), 9, " ").alias("CDDL_UMREF_ID"),
    F.col("CDDL_REFSV_SEQ_NO").alias("CDDL_REFSV_SEQ_NO"),
    F.col("svFinalDispCd").alias("CLM_LN_FINL_DISP_CD"),
    F.rpad(F.when(F.col("PSDC_ID").isNull(), F.lit("")).otherwise(F.trim(F.col("PSDC_ID"))), 2, " ").alias("PSCD_ID"),
    F.rpad(F.col("CLCL_NTWK_IND"), 1, " ").alias("CLCL_NTWK_IND"),
    F.rpad(F.when(F.col("EXCD_ID").isNull(), F.lit("NA")).otherwise(F.col("SRC_CD")), 3, " ").alias("CDDL_RESP_CD"),
    F.rpad(F.when(F.col("EXCD_ID").isNull(), F.lit("N")).otherwise(F.lit("Y")), 1, " ").alias("EXCD_FOUND"),
    F.rpad(F.trim(F.col("CDDL_EOB_EXCD")), 3, " ").alias("CDDL_EOB_EXCD")
]
df_StripClmLnDntl = df_StripFields.select(select_cols_StripClmLnDntl)

write_files(
    df_StripClmLnDntl,
    f"{adls_path}/verified/FctsClmLnDntlExtr.ClmLnDntl.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_hf_clm_sts_dedup = dedup_sort(df_hf_clm_sts_out, ["CLCL_ID","CDDL_SEQ_NO"], [])

df_by_status_type = df_hf_clm_sts_dedup

df_Status_DeniedRej = df_by_status_type.filter(F.trim(F.col("CLM_LN_FINL_DISP_CD"))=="DENIEDREJ").select(
    F.rpad(F.col("CLCL_ID"),12," ").alias("CLCL_ID")
)
df_Status_Acptd = df_by_status_type.filter(F.trim(F.col("CLM_LN_FINL_DISP_CD"))=="ACPTD").select(
    F.rpad(F.col("CLCL_ID"),12," ").alias("CLCL_ID")
)
df_Status_Susp = df_by_status_type.filter(F.trim(F.col("CLM_LN_FINL_DISP_CD"))=="SUSP").select(
    F.rpad(F.col("CLCL_ID"),12," ").alias("CLCL_ID")
)
df_Status_ClsdDel = df_by_status_type.filter(F.trim(F.col("CLM_LN_FINL_DISP_CD"))=="CLSDDEL").select(
    F.rpad(F.col("CLCL_ID"),12," ").alias("CLCL_ID")
)
df_Status_NonPrice = df_by_status_type.filter(F.trim(F.col("CLM_LN_FINL_DISP_CD"))=="NONPRICE").select(
    F.rpad(F.col("CLCL_ID"),12," ").alias("CLCL_ID")
)

write_files(
    df_Status_DeniedRej,
    f"hf_clm_finalized_status_denied.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote='"',
    nullValue=None
)
write_files(
    df_Status_Acptd,
    f"hf_clm_finalized_status_acptd.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote='"',
    nullValue=None
)
write_files(
    df_Status_Susp,
    f"hf_clm_finalized_status_susp.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote='"',
    nullValue=None
)
write_files(
    df_Status_ClsdDel,
    f"hf_clm_finalized_status_clsddel.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote='"',
    nullValue=None
)
write_files(
    df_Status_NonPrice,
    f"hf_clm_finalized_status_nonprice.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_hf_clm_fcts_reversals_alias = df_hf_clm_fcts_reversals.alias("fcts_reversals")
df_StripDntlClmLn_alias = df_StripDntlClmLn.alias("StripDntlClmLn")
df_clm_nasco_dup_bypass_alias = df_clm_nasco_dup_bypass.alias("nasco_dup_lkup")

join_cond_reversals = [df_StripDntlClmLn_alias["CLCL_ID"] == df_hf_clm_fcts_reversals_alias["CLCL_ID"]]
df_BusinessRules_left = df_StripDntlClmLn_alias.join(df_hf_clm_fcts_reversals_alias, on=join_cond_reversals, how="left")

join_cond_nasco = [df_StripDntlClmLn_alias["CLCL_ID"] == df_clm_nasco_dup_bypass_alias["CLM_ID"]]
df_BusinessRules_final = df_BusinessRules_left.join(df_clm_nasco_dup_bypass_alias, on=join_cond_nasco, how="left")

df_DntlClmLine = df_BusinessRules_final.filter(
    (F.col("nasco_dup_lkup.CLM_ID").isNull())
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"),1," ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"),1," ").alias("PASS_THRU_IN"),
    F.col("EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS;"),F.trim(F.col("StripDntlClmLn.CLCL_ID")),F.lit(";"),F.col("StripDntlClmLn.CDDL_SEQ_NO").cast(StringType())).alias("PRI_KEY_STRING"),
    F.lit(0).alias("DNTL_CLM_LN_SK"),
    F.rpad(F.when(F.col("StripDntlClmLn.CLCL_ID").isNull() | (F.length(F.trim(F.col("StripDntlClmLn.CLCL_ID")))==0),F.lit("NA"))
           .otherwise(F.trim(F.col("StripDntlClmLn.CLCL_ID"))),18," ").alias("CLM_ID"),
    F.col("StripDntlClmLn.CDDL_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.when(F.col("StripDntlClmLn.DPCG_DP_ID_ALT").isNull()| (F.length(F.trim(F.col("StripDntlClmLn.DPCG_DP_ID_ALT")))==0),F.lit("NA"))
           .otherwise(F.upper(F.trim(F.col("StripDntlClmLn.DPCG_DP_ID_ALT")))),5," ").alias("ALT_PROC_CD"),
    F.when(F.col("StripDntlClmLn.CGCG_ID").isNull() | (F.length(F.trim(F.col("StripDntlClmLn.CGCG_ID")))==0),F.lit("NA"))
     .otherwise(F.upper(F.trim(F.col("StripDntlClmLn.CGCG_ID")))).alias("DNTL_CLM_LN_DNTL_CAT_CD"),
    F.when(F.col("StripDntlClmLn.CGCG_RULE").isNull() | (F.length(F.trim(F.col("StripDntlClmLn.CGCG_RULE"))) == 0), F.lit("NA"))
     .otherwise(F.upper(F.trim(F.col("StripDntlClmLn.CGCG_RULE")))).alias("DNTL_CLM_LN_DNTL_CAT_RULE_CD"),
    F.rpad(F.when(F.col("StripDntlClmLn.CDDL_ALTDP_EXCD_ID").isNull() | (F.length(F.trim(F.col("StripDntlClmLn.CDDL_ALTDP_EXCD_ID")))==0),F.lit("NA"))
           .otherwise(F.trim(F.col("StripDntlClmLn.CDDL_ALTDP_EXCD_ID"))),3," ").alias("DNTL_CLM_LN_ALT_PROC_EXCD"),
    F.rpad(F.when(F.trim(F.col("StripDntlClmLn.CGCG_ID"))=="PD",F.lit("1"))
           .when((F.trim(F.col("StripDntlClmLn.CGCG_ID"))=="OS")|(F.trim(F.col("StripDntlClmLn.CGCG_ID"))=="BA")|(F.trim(F.col("StripDntlClmLn.CGCG_ID"))=="OX"),F.lit("2"))
           .when((F.trim(F.col("StripDntlClmLn.CGCG_ID"))=="PE")|(F.trim(F.col("StripDntlClmLn.CGCG_ID"))=="MJ")|(F.trim(F.col("StripDntlClmLn.CGCG_ID"))=="TM"),F.lit("3"))
           .when(F.trim(F.col("StripDntlClmLn.CGCG_ID"))=="OR",F.lit("4"))
           .when((F.trim(F.col("StripDntlClmLn.CGCG_ID"))=="NC")|(F.trim(F.col("StripDntlClmLn.CGCG_ID"))=="XX"),F.lit("NB"))
           .otherwise(F.lit("NA")),2," ").alias("DNTL_CLM_LN_BNF_TYP_CD"),
    F.rpad(F.when(F.col("StripDntlClmLn.CDDL_SURF").isNull() | (F.length(F.trim(F.col("StripDntlClmLn.CDDL_SURF")))==0),F.lit("NA"))
           .otherwise(F.upper(F.trim(F.col("StripDntlClmLn.CDDL_SURF")))),10," ").alias("DNTL_CLM_LN_TOOTH_SRFC_CD"),
    F.rpad(F.when(F.col("StripDntlClmLn.UTUT_CD").isNull() | (F.length(F.trim(F.col("StripDntlClmLn.UTUT_CD")))==0),F.lit("NA"))
           .otherwise(F.upper(F.trim(F.col("StripDntlClmLn.UTUT_CD")))),3," ").alias("DNTL_CLM_LN_UTIL_EDIT_CD"),
    F.rpad(F.when(F.length(F.trim(F.col("StripDntlClmLn.CGPY_PFX")))==0,
                  F.when(F.col("StripDntlClmLn.CDDL_CUR_STS")=="99",F.lit("NA")).otherwise(F.lit("UNK")))
           .otherwise(F.col("StripDntlClmLn.CGPY_PFX")),4," ").alias("CAT_PAYMT_PFX_ID"),
    F.rpad(F.when(F.length(F.trim(F.col("StripDntlClmLn.DPPY_PFX")))==0,
                  F.when(F.col("StripDntlClmLn.CDDL_CUR_STS")=="99",F.lit("NA")).otherwise(F.lit("UNK")))
           .otherwise(F.col("StripDntlClmLn.DPPY_PFX")),4," ").alias("DNTL_PROC_PAYMT_PFX_ID"),
    F.rpad(F.when(F.length(F.trim(F.col("StripDntlClmLn.DPPC_PRICE_ID")))==0,
                  F.when(F.col("StripDntlClmLn.CDDL_CUR_STS")=="99",F.lit("NA")).otherwise(F.lit("UNK")))
           .otherwise(F.col("StripDntlClmLn.DPPC_PRICE_ID")),4," ").alias("DNTL_PROC_PRICE_ID"),
    F.rpad(F.when(F.length(F.trim(F.col("StripDntlClmLn.CDDL_TOOTH_BEG")))==0,F.lit("NA"))
           .otherwise(F.col("StripDntlClmLn.CDDL_TOOTH_BEG").substr(1,2)),2," ").alias("TOOTH_BEG_NO"),
    F.rpad(F.when(F.length(F.trim(F.col("StripDntlClmLn.CDDL_TOOTH_END")))==0,F.lit("NA"))
           .otherwise(F.col("StripDntlClmLn.CDDL_TOOTH_END").substr(1,2)),2," ").alias("TOOTH_END_NO"),
    F.rpad(F.when(F.length(F.trim(F.col("StripDntlClmLn.CDDL_TOOTH_NO")))==0,F.lit("NA"))
           .otherwise(F.col("StripDntlClmLn.CDDL_TOOTH_NO").substr(1,2)),2," ").alias("TOOTH_NO")
)

df_reversals = df_BusinessRules_final.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
    (
      (F.col("fcts_reversals.CLCL_CUR_STS")=="89")|
      (F.col("fcts_reversals.CLCL_CUR_STS")=="91")|
      (F.col("fcts_reversals.CLCL_CUR_STS")=="99")
    )
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"),1," ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"),1," ").alias("PASS_THRU_IN"),
    F.col("StripDntlClmLn.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS"),F.lit(";"),F.col("StripDntlClmLn.CLCL_ID"),F.lit("R;"),F.col("StripDntlClmLn.CDDL_SEQ_NO").cast(StringType())).alias("PRI_KEY_STRING"),
    F.lit(0).alias("DNTL_CLM_LN_SK"),
    F.rpad(F.when(F.col("StripDntlClmLn.CLCL_ID").isNull() | (F.length(F.trim(F.col("StripDntlClmLn.CLCL_ID")))==0),F.lit("NA"))
           .otherwise(F.trim(F.col("StripDntlClmLn.CLCL_ID")))+"R",18," ").alias("CLM_ID"),
    F.col("StripDntlClmLn.CDDL_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.when(F.col("StripDntlClmLn.DPCG_DP_ID_ALT").isNull()| (F.length(F.trim(F.col("StripDntlClmLn.DPCG_DP_ID_ALT")))==0),F.lit("NA"))
           .otherwise(F.upper(F.trim(F.col("StripDntlClmLn.DPCG_DP_ID_ALT")))),5," ").alias("ALT_PROC_CD"),
    F.when(F.col("StripDntlClmLn.CGCG_ID").isNull() | (F.length(F.trim(F.col("StripDntlClmLn.CGCG_ID")))==0),F.lit("NA"))
     .otherwise(F.upper(F.trim(F.col("StripDntlClmLn.CGCG_ID")))).alias("DNTL_CLM_LN_DNTL_CAT_CD"),
    F.when(F.col("StripDntlClmLn.CGCG_RULE").isNull() | (F.length(F.trim(F.col("StripDntlClmLn.CGCG_RULE"))) == 0), F.lit("NA"))
     .otherwise(F.upper(F.trim(F.col("StripDntlClmLn.CGCG_RULE")))).alias("DNTL_CLM_LN_DNTL_CAT_RULE_CD"),
    F.rpad(F.when(F.col("StripDntlClmLn.CDDL_ALTDP_EXCD_ID").isNull() | (F.length(F.trim(F.col("StripDntlClmLn.CDDL_ALTDP_EXCD_ID")))==0),F.lit("NA"))
           .otherwise(F.trim(F.col("StripDntlClmLn.CDDL_ALTDP_EXCD_ID"))),3," ").alias("DNTL_CLM_LN_ALT_PROC_EXCD"),
    F.rpad(F.when(F.trim(F.col("StripDntlClmLn.CGCG_ID"))=="PD",F.lit("1"))
           .when((F.trim(F.col("StripDntlClmLn.CGCG_ID"))=="OS")|(F.trim(F.col("StripDntlClmLn.CGCG_ID"))=="BA")|(F.trim(F.col("StripDntlClmLn.CGCG_ID"))=="OX"),F.lit("2"))
           .when((F.trim(F.col("StripDntlClmLn.CGCG_ID"))=="PE")|(F.trim(F.col("StripDntlClmLn.CGCG_ID"))=="MJ")|(F.trim(F.col("StripDntlClmLn.CGCG_ID"))=="TM"),F.lit("3"))
           .when(F.trim(F.col("StripDntlClmLn.CGCG_ID"))=="OR",F.lit("4"))
           .when((F.trim(F.col("StripDntlClmLn.CGCG_ID"))=="NC")|(F.trim(F.col("StripDntlClmLn.CGCG_ID"))=="XX"),F.lit("NB"))
           .otherwise(F.lit("NA")),2," ").alias("DNTL_CLM_LN_BNF_TYP_CD"),
    F.rpad(F.when(F.col("StripDntlClmLn.CDDL_SURF").isNull() | (F.length(F.trim(F.col("StripDntlClmLn.CDDL_SURF")))==0),F.lit("NA"))
           .otherwise(F.upper(F.trim(F.col("StripDntlClmLn.CDDL_SURF")))),10," ").alias("DNTL_CLM_LN_TOOTH_SRFC_CD"),
    F.rpad(F.when(F.col("StripDntlClmLn.UTUT_CD").isNull() | (F.length(F.trim(F.col("StripDntlClmLn.UTUT_CD")))==0),F.lit("NA"))
           .otherwise(F.upper(F.trim(F.col("StripDntlClmLn.UTUT_CD")))),3," ").alias("DNTL_CLM_LN_UTIL_EDIT_CD"),
    F.rpad(F.when(F.col("StripDntlClmLn.CGPY_PFX").isNull() | (F.length(F.trim(F.col("StripDntlClmLn.CGPY_PFX"))) == 0),F.lit("U"))
           .otherwise(F.upper(F.trim(F.col("StripDntlClmLn.CGPY_PFX")))),4," ").alias("CAT_PAYMT_PFX_ID"),
    F.rpad(F.when(F.col("StripDntlClmLn.DPPY_PFX").isNull() | (F.length(F.trim(F.col("StripDntlClmLn.DPPY_PFX"))) == 0),F.lit("U"))
           .otherwise(F.upper(F.trim(F.col("StripDntlClmLn.DPPY_PFX")))),4," ").alias("DNTL_PROC_PAYMT_PFX_ID"),
    F.rpad(F.when(F.col("StripDntlClmLn.DPPC_PRICE_ID").isNull() | (F.length(F.trim(F.col("StripDntlClmLn.DPPC_PRICE_ID"))) == 0),F.lit("U"))
           .otherwise(F.upper(F.trim(F.col("StripDntlClmLn.DPPC_PRICE_ID")))),4," ").alias("DNTL_PROC_PRICE_ID"),
    F.rpad(F.when(F.length(F.trim(F.col("StripDntlClmLn.CDDL_TOOTH_BEG")))==0,F.lit("NA"))
           .otherwise(F.col("StripDntlClmLn.CDDL_TOOTH_BEG").substr(1,2)),2," ").alias("TOOTH_BEG_NO"),
    F.rpad(F.when(F.length(F.trim(F.col("StripDntlClmLn.CDDL_TOOTH_END")))==0,F.lit("NA"))
           .otherwise(F.col("StripDntlClmLn.CDDL_TOOTH_END").substr(1,2)),2," ").alias("TOOTH_END_NO"),
    F.rpad(F.when(F.length(F.trim(F.col("StripDntlClmLn.CDDL_TOOTH_NO")))==0,F.lit("NA"))
           .otherwise(F.col("StripDntlClmLn.CDDL_TOOTH_NO").substr(1,2)),2," ").alias("TOOTH_NO")
)

df_Collector = df_reversals.unionByName(df_DntlClmLine)

select_cols_Collector = [
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
]
df_Collector_out = df_Collector.select(select_cols_Collector)

select_cols_Snapshot = select_cols_Collector
df_Snapshot = df_Collector_out.select(select_cols_Snapshot)

df_Pkey = df_Snapshot.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(
        F.when(
            F.col("CRT_RUN_CYC_EXCTN_SK")==F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"), 
            F.lit("I")
        ).otherwise(F.lit("U")),
        10,
        " "
    ).alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.lit("SK").alias("DNTL_CLM_LN_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.lit("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit("CurrRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ALT_PROC_CD").alias("ALT_PROC_CD"),
    F.col("DNTL_CLM_LN_DNTL_CAT_CD").alias("DNTL_CLM_LN_DNTL_CAT_CD"),
    F.col("DNTL_CLM_LN_DNTL_CAT_RULE_CD").alias("DNTL_CLM_LN_DNTL_CAT_RULE_CD"),
    F.col("DNTL_CLM_LN_ALT_PROC_EXCD").alias("DNTL_CLM_LN_ALT_PROC_EXCD"),
    F.col("DNTL_CLM_LN_BNF_TYP_CD").alias("DNTL_CLM_LN_BNF_TYP_CD"),
    F.rpad(F.col("DNTL_CLM_LN_TOOTH_SRFC_CD"),10," ").alias("DNTL_CLM_LN_TOOTH_SRFC_CD"),
    F.col("DNTL_CLM_LN_UTIL_EDIT_CD").alias("DNTL_CLM_LN_UTIL_EDIT_CD"),
    F.col("CAT_PAYMT_PFX_ID").alias("CAT_PAYMT_PFX_ID"),
    F.col("DNTL_PROC_PAYMT_PFX_ID").alias("DNTL_PROC_PAYMT_PFX_ID"),
    F.col("DNTL_PROC_PRICE_ID").alias("DNTL_PROC_PRICE_ID"),
    F.col("TOOTH_BEG_NO").alias("TOOTH_BEG_NO"),
    F.col("TOOTH_END_NO").alias("TOOTH_END_NO"),
    F.col("TOOTH_NO").alias("TOOTH_NO")
)

params_DntlClmLnPK = {
    "CurrRunCycle": f"{CurrRunCycle}"
}
df_IdsDntlClmLineExtr = DntlClmLnPK(df_Pkey, params_DntlClmLnPK)

df_Snapshot_out = df_Snapshot.select(
    F.rpad(F.col("SrcSysCdSk"),10," ").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO")
)
write_files(
    df_Snapshot_out,
    f"{adls_path}/load/B_DNTL_CLM_LN.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_IdsDntlClmLineExtr_out = df_IdsDntlClmLineExtr.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("DNTL_CLM_LN_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ALT_PROC_CD"),
    F.col("DNTL_CLM_LN_DNTL_CAT_CD"),
    F.col("DNTL_CLM_LN_DNTL_CAT_RULE_CD"),
    F.col("DNTL_CLM_LN_ALT_PROC_EXCD"),
    F.col("DNTL_CLM_LN_BNF_TYP_CD"),
    F.col("DNTL_CLM_LN_TOOTH_SRFC_CD"),
    F.col("DNTL_CLM_LN_UTIL_EDIT_CD"),
    F.col("CAT_PAYMT_PFX_ID"),
    F.col("DNTL_PROC_PAYMT_PFX_ID"),
    F.col("DNTL_PROC_PRICE_ID"),
    F.col("TOOTH_BEG_NO"),
    F.col("TOOTH_END_NO"),
    F.col("TOOTH_NO")
)

write_files(
    df_IdsDntlClmLineExtr_out,
    f"{adls_path}/key/FctsDntlClmLnExtr.FctsDntlClmLn.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)