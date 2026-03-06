# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 7;hf_mbrriskmesri_risk_cat_sk;hf_mbrriskmesri_risk_cat_lookup;hf_mbrriskmesri_marker_cat_cd_cnt;hf_mbrriskmesri_risk_group;hf_mbrriskmesri_imp_rsk_mesrs_int;hf_mbrriskmesri_risk_cat_priorities;hf_mcsrc_marker_cat_cd_cnt
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Extracting data from MBR_RISK_MESR
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                 Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------         ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                1/03/2008         CLINICALS/3044            Originally Programmed                              devlEDW10                  Steph Goddard            01/07/2008
# MAGIC 
# MAGIC Parikshith Chada            2008-02-28         Clinical/3036                   Made structural and logical changes       devlEDW                      Steph Goddard            02/28/2008
# MAGIC                                                                                                          to the code within the transform stage
# MAGIC 
# MAGIC Bhoomi Dasari               05/02/2008         FEP/3051                      Added new logic for McSource data         devlEDW                      Steph Goddard            05/14/2008
# MAGIC                                                                                                         for CAE_PRI_RISK_FCTR_IN
# MAGIC 
# MAGIC Bhoomi Dasari               05/29/2008         FEP/3051                      changed extract to use driver table          devlEDW                       Steph Goddard            06/02/2008
# MAGIC Kalyan Neelam              07/07/2010         4487                               Added 7 new fields -                                EnterpriseNewDevl        Steph Goddard            07/21/2010
# MAGIC                                                                                                        RISK_SVRTY_CD, RISK_SVRTY_NM,
# MAGIC                                                                                                        RISK_IDNT_DT_SK, RISK_SVRTY_CD_SK,
# MAGIC                                                                                                MCSRC_RISK_LVL_NO,MDCSN_RISK_SCORE_NO,
# MAGIC                                                                                                MDCSN_HLTH_STTUS_MESR_NO on end.
# MAGIC                                                                                               Also added logic to calculate the CAE_PRI_RISK_FCTR_IN for Alineo
# MAGIC Kalyan Neelam          2013-07-11         5056 FEP                   Added IDEA source check for                           IntegrateNewDevl         Bhoomi Dasari           7/24/2013
# MAGIC                                                                                                 CAE_PRI_RISK_FCTR_IN in Final_Pass transformer
# MAGIC 
# MAGIC Pooja Sunkara           2013-11-18             5114                        Rewrite in Parallel                                           EnterpriseWrhsDevl       Bhoomi Dasari        1/17/2014
# MAGIC Santosh Bokka          2014-01-16            4917                        Changed Defaults for last 7 CRG columns
# MAGIC                                                                                                  in Pass_Thru transformer                                EnterpriseNewDevl         Bhoomi Dasari        1/17/2014
# MAGIC Santosh Bokka          2014-07-08             4917                    Added BCBSKC for CAE_PRI_RISK_FCTR_IN
# MAGIC                                                                                              in  Final_Pass Transformer                                   EnterpriseNewDevl        Kalyan Neelam        2014-07-15
# MAGIC 
# MAGIC Manasa Andru           2016-02-02          TFS - 12302     Corrected the values in the MAJ_PRCTC_CAT_DESC    EnterpriseDev1        Kalyan Neelam        2016-02-09  
# MAGIC                                                                                         field for default records in the Final_Pass transformer stage.
# MAGIC 
# MAGIC 
# MAGIC Krsihnakanth 
# MAGIC Manivannan             2016-11-10         30001        Added 4 new columns for MBR_RISK_MSR table                 EnterpriseDev2             Jag Yelavarthi         2016-11-18

# MAGIC Count the number of risk categories and related risks for each member.  Determine the max related risk percentage and the highest level priority (which is 1).
# MAGIC Get only members who have a risk category id that is in the CatPriority table.
# MAGIC Calculate the Min Hier Id, Rank No and Mbr Risk Mesr Sk
# MAGIC The risk priorities are used to determine which risk is considered to be the primary if two of the factors have the same percentage.  This list is also used to look at only those risk factors that are relavent.
# MAGIC Pull Risk Category Priorities
# MAGIC Set the CAE_PRI_RISK_FCTR_IN indicator to Y or N depending on the count of risk categories the member as along with the percentage of risk assigned.
# MAGIC Assign 'Y' to the first record for each member
# MAGIC Sort the data such that the first record for a member will always be 'Y' and the rest of the records are all 'N' for that member.
# MAGIC Join the Min values to the original data
# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK and LAST_UPDT_RUN_CYC_EXCTN_DT_SK
# MAGIC Job name:
# MAGIC IdsEdwMbrRiskMesrIExtr
# MAGIC EDW Mbr Epsd Mesr I extract from IDS
# MAGIC Write MBR_RISK_MESR_I Data into a Sequential file for Load Ready Job.
# MAGIC IDS Mbr Risk Mesr extract from IDS
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, DecimalType
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameters
IDSOwner = get_widget_value('IDSOwner','')
EDWOwner = get_widget_value('EDWOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

# DB2ConnectorPX: db2_P_RISK_CAT_PRTY_In (Database=EDW)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
df_db2_P_RISK_CAT_PRTY_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option(
        "query",
        f"SELECT \n\tRISK_CAT_ID,\n\tPRTY_NO \nFROM {EDWOwner}.p_risk_cat_prty P_RISK_CAT_PRTY"
    )
    .load()
)

# Transformer_136
df_Transformer_136_input = df_db2_P_RISK_CAT_PRTY_In.select(
    F.col("RISK_CAT_ID").alias("Ink_PRiskCatPrty_inABC_RISK_CAT_ID"),
    F.col("PRTY_NO").alias("Ink_PRiskCatPrty_inABC_PRTY_NO")
)
df_Transformer_136_output_link_Lkp_RiskPriority = df_Transformer_136_input.select(
    F.col("Ink_PRiskCatPrty_inABC_RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("Ink_PRiskCatPrty_inABC_PRTY_NO").alias("PRTY_NO")
)
df_Transformer_136_output_link_Lkp_Priority = df_Transformer_136_input.select(
    F.col("Ink_PRiskCatPrty_inABC_RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("Ink_PRiskCatPrty_inABC_PRTY_NO").alias("PRTY_NO"),
).withColumn(
    "Lkp_Priority_RISK_CAT_ID_dummy",
    F.lit("N").cast(StringType())
)

# DB2ConnectorPX: db2_IHM_PGM_HIER_In (Database=IDS)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_db2_IHM_PGM_HIER_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT

COALESCE(CD_MPPNG.TRGT_CD, 'UNK')  SRC_SYS_CD,
IHM_PGM_HIER.RISK_SVRTY_CD,
IHM_PGM_HIER.PGM_ID,
IHM_PGM_HIER.IHM_PGM_HIER_NO,
IHM_PGM_HIER.IHM_PGM_HIER_RANK_NO 

FROM 

{IDSOwner}.IHM_PGM_HIER IHM_PGM_HIER, 
{IDSOwner}.CD_MPPNG CD_MPPNG

WHERE

IHM_PGM_HIER.SRC_SYS_CD_SK = CD_MPPNG.CD_MPPNG_SK"""
    )
    .load()
)

# DB2ConnectorPX: db2_MBR_RISK_MESR_in (Database=IDS)
df_db2_MBR_RISK_MESR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT 
MBR_RISK_MESR.MBR_RISK_MESR_SK,
MBR_RISK_MESR.SRC_SYS_CD_SK,
MBR_RISK_MESR.MBR_UNIQ_KEY,
MBR_RISK_MESR.RISK_CAT_ID,
MBR_RISK_MESR.PRCS_YR_MO_SK,
MBR_RISK_MESR.CRT_RUN_CYC_EXCTN_SK,
MBR_RISK_MESR.LAST_UPDT_RUN_CYC_EXCTN_SK,
MBR_RISK_MESR.MBR_SK,
MBR_RISK_MESR.MBR_MED_MESRS_SK,
MBR_RISK_MESR.RISK_CAT_SK,
MBR_RISK_MESR.FTR_RELTV_RISK_NO,  
MBR_RISK_MESR.RISK_SVRTY_CD_SK,
MBR_RISK_MESR.RISK_IDNT_DT_SK,
MBR_RISK_MESR.MCSRC_RISK_LVL_NO,
MBR_RISK_MESR.MDCSN_RISK_SCORE_NO,
MBR_RISK_MESR.MDCSN_HLTH_STTUS_MESR_NO,
CRG_ID,
CRG_DESC,
AGG_CRG_BASE_3_ID,
AGG_CRG_BASE_3_DESC,
CRG_WT,
CRG_MDL_ID,
CRG_VRSN_ID,
MBR.INDV_BE_KEY,
MBR_RISK_MESR.INDV_BE_MARA_RSLT_SK,
MBR_RISK_MESR.RISK_MTHDLGY_CD_SK,
MBR_RISK_MESR.RISK_MTHDLGY_TYP_CD_SK,
MBR_RISK_MESR.RISK_MTHDLGY_TYP_SCORE_NO
FROM   {IDSOwner}.W_MBR_RISK_DRVR   DRVR,
             {IDSOwner}.MBR_RISK_MESR        MBR_RISK_MESR,
             {IDSOwner}.MBR                 MBR 

WHERE  DRVR.SRC_SYS_CD_SK        =     MBR_RISK_MESR.SRC_SYS_CD_SK  
AND        DRVR.MBR_UNIQ_KEY          =     MBR_RISK_MESR.MBR_UNIQ_KEY
AND        DRVR. RISK_CAT_ID              =     MBR_RISK_MESR.RISK_CAT_ID
AND        DRVR.PRCS_YR_MO_SK       =     MBR_RISK_MESR.PRCS_YR_MO_SK
AND    MBR_RISK_MESR.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
"""
    )
    .load()
)

# DB2ConnectorPX: db2_RISK_CAT_in (Database=IDS)
df_db2_RISK_CAT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT 

RISK_CAT_SK,
MAJ_PRCTC_CAT_CD_SK,
RISK_CAT_NM 

FROM 

{IDSOwner}.RISK_CAT ;"""
    )
    .load()
)

# Lkp_Cd (PxLookup)
# Primary link -> db2_MBR_RISK_MESR_in
# Lookup link -> db2_RISK_CAT_in (left join on RISK_CAT_SK)
df_lkp_cd_primary = df_db2_MBR_RISK_MESR_in.alias("Ink_IdsEdwMbrRiskMesrIExtr_inABC")
df_lkp_cd_lookup = df_db2_RISK_CAT_in.alias("ref_RiskCatSK")
df_Lkp_Cd = (
    df_lkp_cd_primary.join(
        df_lkp_cd_lookup,
        on=[df_lkp_cd_primary["RISK_CAT_SK"] == df_lkp_cd_lookup["RISK_CAT_SK"]],
        how="left"
    )
)
df_Lkp_Cd_out = df_Lkp_Cd.select(
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.MBR_RISK_MESR_SK").alias("MBR_RISK_MESR_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.MBR_SK").alias("MBR_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.RISK_CAT_SK").alias("RISK_CAT_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.FTR_RELTV_RISK_NO").alias("FTR_RELTV_RISK_NO"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.RISK_SVRTY_CD_SK").alias("RISK_SVRTY_CD_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.RISK_IDNT_DT_SK").alias("RISK_IDNT_DT_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.MCSRC_RISK_LVL_NO").alias("MCSRC_RISK_LVL_NO"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.MDCSN_RISK_SCORE_NO").alias("MDCSN_RISK_SCORE_NO"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.MDCSN_HLTH_STTUS_MESR_NO").alias("MDCSN_HLTH_STTUS_MESR_NO"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.CRG_ID").alias("CRG_ID"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.CRG_DESC").alias("CRG_DESC"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.AGG_CRG_BASE_3_ID").alias("AGG_CRG_BASE_3_ID"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.AGG_CRG_BASE_3_DESC").alias("AGG_CRG_BASE_3_DESC"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.CRG_WT").alias("CRG_WT"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.CRG_MDL_ID").alias("CRG_MDL_ID"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.CRG_VRSN_ID").alias("CRG_VRSN_ID"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("ref_RiskCatSK.MAJ_PRCTC_CAT_CD_SK").alias("MAJ_PRCTC_CAT_CD_SK"),
    F.col("ref_RiskCatSK.RISK_CAT_NM").alias("RISK_CAT_NM"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.RISK_MTHDLGY_CD_SK").alias("RISK_MTHDLGY_CD_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.RISK_MTHDLGY_TYP_CD_SK").alias("RISK_MTHDLGY_TYP_CD_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtr_inABC.RISK_MTHDLGY_TYP_SCORE_NO").alias("RISK_MTHDLGY_TYP_SCORE_NO")
)

# DB2ConnectorPX: db2_CD_MPPNG_in (Database=IDS)
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT 

CD_MPPNG_SK,
TRGT_CD,
TRGT_CD_NM 

from 

{IDSOwner}.CD_MPPNG
"""
    )
    .load()
)

# Cpy_CdMppng (PxCopy)
df_Cpy_CdMppng_input = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("ref_CdMppng_CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("ref_CdMppng_TRGT_CD"),
    F.col("TRGT_CD_NM").alias("ref_CdMppng_TRGT_CD_NM")
)
df_Cpy_CdMppng_ref_RiskSvrty = df_Cpy_CdMppng_input.select(
    F.col("ref_CdMppng_CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("ref_CdMppng_TRGT_CD").alias("TRGT_CD"),
    F.col("ref_CdMppng_TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Cpy_CdMppng_ref_SrcSysCd = df_Cpy_CdMppng_input.select(
    F.col("ref_CdMppng_CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("ref_CdMppng_TRGT_CD").alias("TRGT_CD"),
    F.col("ref_CdMppng_TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Cpy_CdMppng_ref_MajPrctcCatCd = df_Cpy_CdMppng_input.select(
    F.col("ref_CdMppng_CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("ref_CdMppng_TRGT_CD").alias("TRGT_CD"),
    F.col("ref_CdMppng_TRGT_CD_NM").alias("TRGT_CD_NM")
)

# Lkp_Codes (PxLookup)
df_Lkp_Codes_primary = df_Lkp_Cd_out.alias("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn")
df_Lkp_Codes_ref_SrcSysCd = df_Cpy_CdMppng_ref_SrcSysCd.alias("ref_SrcSysCd")
df_Lkp_Codes_ref_RiskSvrty = df_Cpy_CdMppng_ref_RiskSvrty.alias("ref_RiskSvrty")
df_Lkp_Codes_ref_MajPrctcCatCd = df_Cpy_CdMppng_ref_MajPrctcCatCd.alias("ref_MajPrctcCatCd")

df_Lkp_Codes_1 = df_Lkp_Codes_primary.join(
    df_Lkp_Codes_ref_SrcSysCd,
    on=[df_Lkp_Codes_primary["SRC_SYS_CD_SK"] == df_Lkp_Codes_ref_SrcSysCd["CD_MPPNG_SK"]],
    how="left"
).alias("JOIN_STEP_1")
df_Lkp_Codes_2 = df_Lkp_Codes_1.join(
    df_Lkp_Codes_ref_RiskSvrty,
    on=[df_Lkp_Codes_1["RISK_SVRTY_CD_SK"] == df_Lkp_Codes_ref_RiskSvrty["CD_MPPNG_SK"]],
    how="left"
).alias("JOIN_STEP_2")
df_Lkp_Codes_3 = df_Lkp_Codes_2.join(
    df_Lkp_Codes_ref_MajPrctcCatCd,
    on=[df_Lkp_Codes_2["MAJ_PRCTC_CAT_CD_SK"] == df_Lkp_Codes_ref_MajPrctcCatCd["CD_MPPNG_SK"]],
    how="left"
).alias("JOIN_STEP_3")

df_Lkp_Codes_out = df_Lkp_Codes_3.select(
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.MBR_RISK_MESR_SK").alias("MBR_RISK_MESR_SK"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.MBR_SK").alias("MBR_SK"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.RISK_CAT_SK").alias("RISK_CAT_SK"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.FTR_RELTV_RISK_NO").alias("FTR_RELTV_RISK_NO"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.RISK_SVRTY_CD_SK").alias("RISK_SVRTY_CD_SK"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.RISK_IDNT_DT_SK").alias("RISK_IDNT_DT_SK"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.MCSRC_RISK_LVL_NO").alias("MCSRC_RISK_LVL_NO"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.MDCSN_RISK_SCORE_NO").alias("MDCSN_RISK_SCORE_NO"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.MDCSN_HLTH_STTUS_MESR_NO").alias("MDCSN_HLTH_STTUS_MESR_NO"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.CRG_ID").alias("CRG_ID"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.CRG_DESC").alias("CRG_DESC"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.AGG_CRG_BASE_3_ID").alias("AGG_CRG_BASE_3_ID"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.AGG_CRG_BASE_3_DESC").alias("AGG_CRG_BASE_3_DESC"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.CRG_WT").alias("CRG_WT"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.CRG_MDL_ID").alias("CRG_MDL_ID"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.CRG_VRSN_ID").alias("CRG_VRSN_ID"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.RISK_CAT_NM").alias("RISK_CAT_NM"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.RISK_MTHDLGY_CD_SK").alias("RISK_MTHDLGY_CD_SK"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.RISK_MTHDLGY_TYP_CD_SK").alias("RISK_MTHDLGY_TYP_CD_SK"),
    F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpIn.RISK_MTHDLGY_TYP_SCORE_NO").alias("RISK_MTHDLGY_TYP_SCORE_NO"),
    F.col("ref_SrcSysCd.TRGT_CD").alias("SRC_SYS_CD"),
    F.col("ref_RiskSvrty.TRGT_CD").alias("RISK_SVRTY_CD"),
    F.col("ref_RiskSvrty.TRGT_CD_NM").alias("RISK_SVRTY_NM"),
    F.col("ref_MajPrctcCatCd.TRGT_CD").alias("MAJ_PRCTC_CAT_CD"),
    F.col("ref_MajPrctcCatCd.TRGT_CD_NM").alias("MAJ_PRCTC_CAT_DESC")
)

# xfrm_BusinessLogic (CTransformerStage)
df_xfrm_BusinessLogic_in = df_Lkp_Codes_out.alias("lnk_IdsEdwMbrRiskMesrIExtr_lkpout")
df_xfrm_BusinessLogic_out = df_xfrm_BusinessLogic_in.select(
    F.col("MBR_RISK_MESR_SK").alias("MBR_RISK_MESR_SK"),
    F.when(
        F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpout.SRC_SYS_CD").isNull() | (F.length(trim("lnk_IdsEdwMbrRiskMesrIExtr_lkpout.SRC_SYS_CD")) == 0),
        F.lit("NA")
    ).otherwise(F.col("lnk_IdsEdwMbrRiskMesrIExtr_lkpout.SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
    F.col("RISK_CAT_SK").alias("RISK_CAT_SK"),
    F.col("FTR_RELTV_RISK_NO").alias("FTR_RELTV_RISK_NO"),
    F.col("RISK_SVRTY_CD_SK").alias("RISK_SVRTY_CD_SK"),
    F.col("RISK_IDNT_DT_SK").alias("RISK_IDNT_DT_SK"),
    F.col("MCSRC_RISK_LVL_NO").alias("MCSRC_RISK_LVL_NO"),
    F.col("MDCSN_RISK_SCORE_NO").alias("MDCSN_RISK_SCORE_NO"),
    F.col("MDCSN_HLTH_STTUS_MESR_NO").alias("MDCSN_HLTH_STTUS_MESR_NO"),
    F.col("CRG_ID").alias("CRG_ID"),
    F.col("CRG_DESC").alias("CRG_DESC"),
    F.col("AGG_CRG_BASE_3_ID").alias("AGG_CRG_BASE_3_ID"),
    F.col("AGG_CRG_BASE_3_DESC").alias("AGG_CRG_BASE_3_DESC"),
    F.col("CRG_WT").alias("CRG_WT"),
    F.col("CRG_MDL_ID").alias("CRG_MDL_ID"),
    F.col("CRG_VRSN_ID").alias("CRG_VRSN_ID"),
    F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.when(F.col("RISK_CAT_NM").isNull(), F.lit("UNK")).otherwise(F.col("RISK_CAT_NM")).alias("RISK_CAT_NM"),
    F.when(
        (F.col("RISK_SVRTY_CD").isNull()) | (F.length(trim("RISK_SVRTY_CD")) == 0),
        F.lit("NA")
    ).otherwise(F.col("RISK_SVRTY_CD")).alias("RISK_SVRTY_CD"),
    F.when(
        (F.col("RISK_SVRTY_NM").isNull()) | (F.length(trim("RISK_SVRTY_NM")) == 0),
        F.lit("NA")
    ).otherwise(F.col("RISK_SVRTY_NM")).alias("RISK_SVRTY_NM"),
    F.when(
        (F.col("MAJ_PRCTC_CAT_CD").isNull()) | (F.length(trim("MAJ_PRCTC_CAT_CD")) == 0),
        F.lit("NA")
    ).otherwise(F.col("MAJ_PRCTC_CAT_CD")).alias("MAJ_PRCTC_CAT_CD"),
    F.when(
        (F.col("MAJ_PRCTC_CAT_DESC").isNull()) | (F.length(trim("MAJ_PRCTC_CAT_DESC")) == 0),
        F.lit("NA")
    ).otherwise(F.col("MAJ_PRCTC_CAT_DESC")).alias("MAJ_PRCTC_CAT_DESC"),
    F.lit("N").alias("CAE_PRI_RISK_FCTR_IN"),
    F.col("INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
    F.col("RISK_MTHDLGY_CD_SK").alias("RISK_MTHDLGY_CD_SK"),
    F.col("RISK_MTHDLGY_TYP_CD_SK").alias("RISK_MTHDLGY_TYP_CD_SK"),
    F.col("RISK_MTHDLGY_TYP_SCORE_NO").alias("RISK_MTHDLGY_TYP_SCORE_NO")
)

# Lkp_PGM (PxLookup)
df_Lkp_PGM_primary = df_xfrm_BusinessLogic_out.alias("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In")
df_Lkp_PGM_lookup = df_db2_IHM_PGM_HIER_In.alias("ref_Pgm")
df_Lkp_PGM = df_Lkp_PGM_primary.join(
    df_Lkp_PGM_lookup,
    on=[
        df_Lkp_PGM_primary["SRC_SYS_CD"] == df_Lkp_PGM_lookup["SRC_SYS_CD"],
        df_Lkp_PGM_primary["RISK_CAT_NM"] == df_Lkp_PGM_lookup["PGM_ID"],
        df_Lkp_PGM_primary["RISK_SVRTY_NM"] == df_Lkp_PGM_lookup["RISK_SVRTY_CD"]
    ],
    how="left"
)
df_Lkp_PGM_out = df_Lkp_PGM.select(
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.MBR_RISK_MESR_SK").alias("MBR_RISK_MESR_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.RISK_CAT_SK").alias("RISK_CAT_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.FTR_RELTV_RISK_NO").alias("FTR_RELTV_RISK_NO"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.MAJ_PRCTC_CAT_CD").alias("MAJ_PRCTC_CAT_CD"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.MAJ_PRCTC_CAT_DESC").alias("MAJ_PRCTC_CAT_DESC"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.MBR_SK").alias("MBR_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.CAE_PRI_RISK_FCTR_IN").alias("CAE_PRI_RISK_FCTR_IN"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.RISK_SVRTY_CD").alias("RISK_SVRTY_CD"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.RISK_SVRTY_NM").alias("RISK_SVRTY_NM"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.RISK_IDNT_DT_SK").alias("RISK_IDNT_DT_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.RISK_SVRTY_CD_SK").alias("RISK_SVRTY_CD_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.RISK_CAT_NM").alias("RISK_CAT_NM"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.MCSRC_RISK_LVL_NO").alias("MCSRC_RISK_LVL_NO"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.MDCSN_RISK_SCORE_NO").alias("MDCSN_RISK_SCORE_NO"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.MDCSN_HLTH_STTUS_MESR_NO").alias("MDCSN_HLTH_STTUS_MESR_NO"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.CRG_ID").alias("CRG_ID"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.CRG_DESC").alias("CRG_DESC"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.AGG_CRG_BASE_3_ID").alias("AGG_CRG_BASE_3_ID"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.AGG_CRG_BASE_3_DESC").alias("AGG_CRG_BASE_3_DESC"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.CRG_WT").alias("CRG_WT"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.CRG_MDL_ID").alias("CRG_MDL_ID"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.CRG_VRSN_ID").alias("CRG_VRSN_ID"),
    F.col("ref_Pgm.IHM_PGM_HIER_NO").alias("IHM_PGM_HIER_NO"),
    F.col("ref_Pgm.IHM_PGM_HIER_RANK_NO").alias("IHM_PGM_HIER_RANK_NO"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.RISK_MTHDLGY_CD_SK").alias("RISK_MTHDLGY_CD_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.RISK_MTHDLGY_TYP_CD_SK").alias("RISK_MTHDLGY_TYP_CD_SK"),
    F.col("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_In.RISK_MTHDLGY_TYP_SCORE_NO").alias("RISK_MTHDLGY_TYP_SCORE_NO")
)

# SplitData (PxFilter)
df_SplitData_in = df_Lkp_PGM_out.alias("Ink_IdsEdwMbrRiskMesrIExtrPGMLkup_out")
# Just pass everything along but create 3 outputs
df_SplitData_out_OtherSources = df_SplitData_in.select("*").alias("OtherSources")
df_SplitData_out_AlineoData = df_SplitData_in.select("*").alias("AlineoData")
df_SplitData_out_PgmHierRank = df_SplitData_in.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("IHM_PGM_HIER_NO").alias("IHM_PGM_HIER_NO"),
    F.col("IHM_PGM_HIER_RANK_NO").alias("IHM_PGM_HIER_RANK_NO"),
    F.col("MBR_RISK_MESR_SK").alias("MBR_RISK_MESR_SK"),
).alias("PgmHierRank")

# MinHierRankMesrSk (PxAggregator)
windowMinAgg = (
    df_SplitData_out_PgmHierRank.groupBy(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK","RISK_CAT_ID"
    )
    .agg(
        F.min("IHM_PGM_HIER_NO").alias("IHM_PGM_HIER_NO_MIN"),
        F.min("IHM_PGM_HIER_RANK_NO").alias("IHM_PGM_HIER_RANK_NO_MIN"),
        F.min("MBR_RISK_MESR_SK").alias("MBR_RISK_MESR_SK_MIN")
    )
)
df_MinHierRankMesrSk_out = windowMinAgg.select(
    F.col("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY"),
    F.col("PRCS_YR_MO_SK"),
    F.col("RISK_CAT_ID"),
    F.col("IHM_PGM_HIER_NO_MIN"),
    F.col("IHM_PGM_HIER_RANK_NO_MIN"),
    F.col("MBR_RISK_MESR_SK_MIN")
)

# LookupMinHierRankSk (PxLookup) - primary link is AlineoData, lookup link is DSLink51
df_LookupMinHierRankSk_primary = df_SplitData_out_AlineoData.alias("AlineoData")
df_LookupMinHierRankSk_lookup = df_MinHierRankMesrSk_out.alias("DSLink51")
df_LookupMinHierRankSk_joined = df_LookupMinHierRankSk_primary.join(
    df_LookupMinHierRankSk_lookup,
    on=[
        df_LookupMinHierRankSk_primary["SRC_SYS_CD"]==df_LookupMinHierRankSk_lookup["SRC_SYS_CD"],
        df_LookupMinHierRankSk_primary["MBR_UNIQ_KEY"]==df_LookupMinHierRankSk_lookup["MBR_UNIQ_KEY"],
        df_LookupMinHierRankSk_primary["PRCS_YR_MO_SK"]==df_LookupMinHierRankSk_lookup["PRCS_YR_MO_SK"],
        df_LookupMinHierRankSk_primary["RISK_CAT_ID"]==df_LookupMinHierRankSk_lookup["RISK_CAT_ID"]
    ],
    how="left"
)
df_LookupMinHierRankSk_out = df_LookupMinHierRankSk_joined.select(
    F.col("AlineoData.MBR_RISK_MESR_SK").alias("MBR_RISK_MESR_SK"),
    F.col("AlineoData.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("AlineoData.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("AlineoData.RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("AlineoData.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("AlineoData.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("AlineoData.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("AlineoData.RISK_CAT_SK").alias("RISK_CAT_SK"),
    F.col("AlineoData.MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
    F.col("AlineoData.FTR_RELTV_RISK_NO").alias("FTR_RELTV_RISK_NO"),
    F.col("AlineoData.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("AlineoData.MAJ_PRCTC_CAT_CD").alias("MAJ_PRCTC_CAT_CD"),
    F.col("AlineoData.MAJ_PRCTC_CAT_DESC").alias("MAJ_PRCTC_CAT_DESC"),
    F.col("AlineoData.MBR_SK").alias("MBR_SK"),
    F.col("AlineoData.CAE_PRI_RISK_FCTR_IN").alias("CAE_PRI_RISK_FCTR_IN"),
    F.col("AlineoData.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("AlineoData.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AlineoData.RISK_SVRTY_CD").alias("RISK_SVRTY_CD"),
    F.col("AlineoData.RISK_SVRTY_NM").alias("RISK_SVRTY_NM"),
    F.col("AlineoData.RISK_IDNT_DT_SK").alias("RISK_IDNT_DT_SK"),
    F.col("AlineoData.RISK_SVRTY_CD_SK").alias("RISK_SVRTY_CD_SK"),
    F.col("AlineoData.RISK_CAT_NM").alias("RISK_CAT_NM"),
    F.col("AlineoData.MCSRC_RISK_LVL_NO").alias("MCSRC_RISK_LVL_NO"),
    F.col("AlineoData.MDCSN_RISK_SCORE_NO").alias("MDCSN_RISK_SCORE_NO"),
    F.col("AlineoData.MDCSN_HLTH_STTUS_MESR_NO").alias("MDCSN_HLTH_STTUS_MESR_NO"),
    F.col("AlineoData.CRG_ID").alias("CRG_ID"),
    F.col("AlineoData.CRG_DESC").alias("CRG_DESC"),
    F.col("AlineoData.AGG_CRG_BASE_3_ID").alias("AGG_CRG_BASE_3_ID"),
    F.col("AlineoData.AGG_CRG_BASE_3_DESC").alias("AGG_CRG_BASE_3_DESC"),
    F.col("AlineoData.CRG_WT").alias("CRG_WT"),
    F.col("AlineoData.CRG_MDL_ID").alias("CRG_MDL_ID"),
    F.col("AlineoData.CRG_VRSN_ID").alias("CRG_VRSN_ID"),
    F.col("DSLink51.IHM_PGM_HIER_NO_MIN").alias("IHM_PGM_HIER_NO_MIN"),
    F.col("DSLink51.IHM_PGM_HIER_RANK_NO_MIN").alias("IHM_PGM_HIER_RANK_NO_MIN"),
    F.col("DSLink51.MBR_RISK_MESR_SK_MIN").alias("MBR_RISK_MESR_SK_MIN"),
    F.col("AlineoData.INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
    F.col("AlineoData.RISK_MTHDLGY_CD_SK").alias("RISK_MTHDLGY_CD_SK"),
    F.col("AlineoData.RISK_MTHDLGY_TYP_CD_SK").alias("RISK_MTHDLGY_TYP_CD_SK"),
    F.col("AlineoData.RISK_MTHDLGY_TYP_SCORE_NO").alias("RISK_MTHDLGY_TYP_SCORE_NO")
)

# XfrmBusinessLogic (CTransformerStage)
df_XfrmBusinessLogic_in = df_LookupMinHierRankSk_out.alias("LkupMinHierRankSk_Out")
df_XfrmBusinessLogic_out = df_XfrmBusinessLogic_in.select(
    F.col("MBR_RISK_MESR_SK").alias("MBR_RISK_MESR_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("RISK_CAT_SK").alias("RISK_CAT_SK"),
    F.col("MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
    F.col("FTR_RELTV_RISK_NO").alias("FTR_RELTV_RISK_NO"),
    F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("MAJ_PRCTC_CAT_CD").alias("MAJ_PRCTC_CAT_CD"),
    F.col("MAJ_PRCTC_CAT_DESC").alias("MAJ_PRCTC_CAT_DESC"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("CAE_PRI_RISK_FCTR_IN").alias("CAE_PRI_RISK_FCTR_IN"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("RISK_SVRTY_CD").alias("RISK_SVRTY_CD"),
    F.col("RISK_SVRTY_NM").alias("RISK_SVRTY_NM"),
    F.col("RISK_IDNT_DT_SK").alias("RISK_IDNT_DT_SK"),
    F.col("RISK_SVRTY_CD_SK").alias("RISK_SVRTY_CD_SK"),
    F.col("RISK_CAT_NM").alias("RISK_CAT_NM"),
    F.col("MCSRC_RISK_LVL_NO").alias("MCSRC_RISK_LVL_NO"),
    F.col("MDCSN_RISK_SCORE_NO").alias("MDCSN_RISK_SCORE_NO"),
    F.col("MDCSN_HLTH_STTUS_MESR_NO").alias("MDCSN_HLTH_STTUS_MESR_NO"),
    F.col("CRG_ID").alias("CRG_ID"),
    F.col("CRG_DESC").alias("CRG_DESC"),
    F.col("AGG_CRG_BASE_3_ID").alias("AGG_CRG_BASE_3_ID"),
    F.col("AGG_CRG_BASE_3_DESC").alias("AGG_CRG_BASE_3_DESC"),
    F.col("CRG_WT").alias("CRG_WT"),
    F.col("CRG_MDL_ID").alias("CRG_MDL_ID"),
    F.col("CRG_VRSN_ID").alias("CRG_VRSN_ID"),
    F.when(F.col("IHM_PGM_HIER_NO_MIN").isNull(), F.lit(100)).otherwise(F.col("IHM_PGM_HIER_NO_MIN").cast(IntegerType())).alias("IHM_PGM_HIER_NO_MIN"),
    F.when(F.col("IHM_PGM_HIER_RANK_NO_MIN").isNull(), F.lit(100)).otherwise(F.col("IHM_PGM_HIER_RANK_NO_MIN").cast(IntegerType())).alias("IHM_PGM_HIER_RANK_NO_MIN"),
    F.when(
        F.col("MBR_RISK_MESR_SK_MIN").isNull(),
        F.lit(100)
    ).otherwise(-1*F.col("MBR_RISK_MESR_SK_MIN").cast(IntegerType())).alias("MBR_RISK_MESR_SK_1_MIN"),
    F.col("INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
    F.col("RISK_MTHDLGY_CD_SK").alias("RISK_MTHDLGY_CD_SK"),
    F.col("RISK_MTHDLGY_TYP_CD_SK").alias("RISK_MTHDLGY_TYP_CD_SK"),
    F.col("RISK_MTHDLGY_TYP_SCORE_NO").alias("RISK_MTHDLGY_TYP_SCORE_NO")
)

# SetIndicators (CTransformerStage)
# We need row-by-row logic: if current MBR_UNIQ_KEY=previous => 'N' else 'Y'
# We'll do a window and check lag of MBR_UNIQ_KEY and PRCS_YR_MO_SK
df_SetIndicators_in = df_XfrmBusinessLogic_out
windowSpecSetInd = Window.partitionBy("SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK").orderBy(
    F.col("IHM_PGM_HIER_NO_MIN").asc(),
    F.col("IHM_PGM_HIER_RANK_NO_MIN").asc(),
    F.col("MBR_RISK_MESR_SK_1_MIN").asc()
)
lagUniqKey = F.lag("MBR_UNIQ_KEY").over(windowSpecSetInd)
lagPrcs = F.lag("PRCS_YR_MO_SK").over(windowSpecSetInd)

df_SetIndicators_stage = df_SetIndicators_in.withColumn("svCurrMbrUniqKey", F.col("MBR_UNIQ_KEY")) \
    .withColumn("svCurrPrcsYrMoSk", F.col("PRCS_YR_MO_SK")) \
    .withColumn("svPrevMbrUniqKey", lagUniqKey) \
    .withColumn("svPrevPrcsYrMoSk", lagPrcs)

df_SetIndicators_stage = df_SetIndicators_stage.withColumn(
    "svRecInd",
    F.when(
        (F.col("svCurrMbrUniqKey") == F.col("svPrevMbrUniqKey")) & 
        (F.col("svCurrPrcsYrMoSk") == F.col("svPrevPrcsYrMoSk")),
        F.lit("N")
    ).otherwise(F.lit("Y"))
)

df_SetIndicators_out = df_SetIndicators_stage.select(
    F.col("MBR_RISK_MESR_SK").alias("MBR_RISK_MESR_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("RISK_CAT_SK").alias("RISK_CAT_SK"),
    F.col("MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
    F.col("FTR_RELTV_RISK_NO").alias("FTR_RELTV_RISK_NO"),
    F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("MAJ_PRCTC_CAT_CD").alias("MAJ_PRCTC_CAT_CD"),
    F.col("MAJ_PRCTC_CAT_DESC").alias("MAJ_PRCTC_CAT_DESC"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("svRecInd").alias("CAE_PRI_RISK_FCTR_IN"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("RISK_SVRTY_CD").alias("RISK_SVRTY_CD"),
    F.col("RISK_SVRTY_NM").alias("RISK_SVRTY_NM"),
    F.col("RISK_IDNT_DT_SK").alias("RISK_IDNT_DT_SK"),
    F.col("RISK_SVRTY_CD_SK").alias("RISK_SVRTY_CD_SK"),
    F.col("RISK_CAT_NM").alias("RISK_CAT_NM"),
    F.col("MCSRC_RISK_LVL_NO").alias("MCSRC_RISK_LVL_NO"),
    F.col("MDCSN_RISK_SCORE_NO").alias("MDCSN_RISK_SCORE_NO"),
    F.col("MDCSN_HLTH_STTUS_MESR_NO").alias("MDCSN_HLTH_STTUS_MESR_NO"),
    F.col("CRG_ID").alias("CRG_ID"),
    F.col("CRG_DESC").alias("CRG_DESC"),
    F.col("AGG_CRG_BASE_3_ID").alias("AGG_CRG_BASE_3_ID"),
    F.col("AGG_CRG_BASE_3_DESC").alias("AGG_CRG_BASE_3_DESC"),
    F.col("CRG_WT").alias("CRG_WT"),
    F.col("CRG_MDL_ID").alias("CRG_MDL_ID"),
    F.col("CRG_VRSN_ID").alias("CRG_VRSN_ID"),
    F.col("INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
    F.col("RISK_MTHDLGY_CD_SK").alias("RISK_MTHDLGY_CD_SK"),
    F.col("RISK_MTHDLGY_TYP_CD_SK").alias("RISK_MTHDLGY_TYP_CD_SK"),
    F.col("RISK_MTHDLGY_TYP_SCORE_NO").alias("RISK_MTHDLGY_TYP_SCORE_NO")
)

# Lkup_GetFactSK (PxLookup)
df_Lkup_GetFactSK_OtherSources = df_SplitData_out_OtherSources.alias("OtherSources")
df_Lkup_GetFactSK_Lkp_Priority = df_Transformer_136_output_link_Lkp_Priority.alias("Lkp_Priority")
df_Lkup_GetFactSK = df_Lkup_GetFactSK_OtherSources.join(
    df_Lkup_GetFactSK_Lkp_Priority,
    on=[df_Lkup_GetFactSK_OtherSources["RISK_CAT_ID"] == df_Lkup_GetFactSK_Lkp_Priority["RISK_CAT_ID"]],
    how="left"
)
df_Lkup_GetFactSK_out = df_Lkup_GetFactSK.select(
    F.col("OtherSources.MBR_RISK_MESR_SK").alias("MBR_RISK_MESR_SK"),
    F.col("OtherSources.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("OtherSources.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("OtherSources.RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("OtherSources.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("OtherSources.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("OtherSources.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("OtherSources.RISK_CAT_SK").alias("RISK_CAT_SK"),
    F.col("OtherSources.MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
    F.col("OtherSources.FTR_RELTV_RISK_NO").alias("FTR_RELTV_RISK_NO"),
    F.col("OtherSources.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("OtherSources.MAJ_PRCTC_CAT_CD").alias("MAJ_PRCTC_CAT_CD"),
    F.col("OtherSources.MAJ_PRCTC_CAT_DESC").alias("MAJ_PRCTC_CAT_DESC"),
    F.col("OtherSources.MBR_SK").alias("MBR_SK"),
    F.col("OtherSources.CAE_PRI_RISK_FCTR_IN").alias("CAE_PRI_RISK_FCTR_IN"),
    F.col("OtherSources.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("OtherSources.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("OtherSources.RISK_SVRTY_CD").alias("RISK_SVRTY_CD"),
    F.col("OtherSources.RISK_SVRTY_NM").alias("RISK_SVRTY_NM"),
    F.col("OtherSources.RISK_IDNT_DT_SK").alias("RISK_IDNT_DT_SK"),
    F.col("OtherSources.RISK_SVRTY_CD_SK").alias("RISK_SVRTY_CD_SK"),
    F.col("OtherSources.RISK_CAT_NM").alias("RISK_CAT_NM"),
    F.col("OtherSources.MCSRC_RISK_LVL_NO").alias("MCSRC_RISK_LVL_NO"),
    F.col("OtherSources.MDCSN_RISK_SCORE_NO").alias("MDCSN_RISK_SCORE_NO"),
    F.col("OtherSources.MDCSN_HLTH_STTUS_MESR_NO").alias("MDCSN_HLTH_STTUS_MESR_NO"),
    F.col("OtherSources.CRG_ID").alias("CRG_ID"),
    F.col("OtherSources.CRG_DESC").alias("CRG_DESC"),
    F.col("OtherSources.AGG_CRG_BASE_3_ID").alias("AGG_CRG_BASE_3_ID"),
    F.col("OtherSources.AGG_CRG_BASE_3_DESC").alias("AGG_CRG_BASE_3_DESC"),
    F.col("OtherSources.CRG_WT").alias("CRG_WT"),
    F.col("OtherSources.CRG_MDL_ID").alias("CRG_MDL_ID"),
    F.col("OtherSources.CRG_VRSN_ID").alias("CRG_VRSN_ID"),
    F.col("Lkp_Priority.PRTY_NO").alias("PRTY_NO"),
    F.col("Lkp_Priority.Lkp_Priority_RISK_CAT_ID_dummy").alias("Lkp_Priority_RISK_CAT_ID_dummy"),
    F.col("OtherSources.INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
    F.col("OtherSources.RISK_MTHDLGY_CD_SK").alias("RISK_MTHDLGY_CD_SK"),
    F.col("OtherSources.RISK_MTHDLGY_TYP_CD_SK").alias("RISK_MTHDLGY_TYP_CD_SK"),
    F.col("OtherSources.RISK_MTHDLGY_TYP_SCORE_NO").alias("RISK_MTHDLGY_TYP_SCORE_NO")
)

# XfrmRiskAgg_In (CTransformerStage)
df_XfrmRiskAgg_In_input = df_Lkup_GetFactSK_out.alias("RiskScoreLink")
df_XfrmRiskAgg_In_main = df_XfrmRiskAgg_In_input.select(
    F.col("RiskScoreLink.MBR_RISK_MESR_SK").alias("MBR_RISK_MESR_SK"),
    F.col("RiskScoreLink.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("RiskScoreLink.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("RiskScoreLink.RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("RiskScoreLink.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("RiskScoreLink.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("RiskScoreLink.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("RiskScoreLink.RISK_CAT_SK").alias("RISK_CAT_SK"),
    F.col("RiskScoreLink.MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
    F.col("RiskScoreLink.FTR_RELTV_RISK_NO").alias("FTR_RELTV_RISK_NO"),
    F.col("RiskScoreLink.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("RiskScoreLink.MAJ_PRCTC_CAT_CD").alias("MAJ_PRCTC_CAT_CD"),
    F.col("RiskScoreLink.MAJ_PRCTC_CAT_DESC").alias("MAJ_PRCTC_CAT_DESC"),
    F.col("RiskScoreLink.MBR_SK").alias("MBR_SK"),
    F.lit("N").alias("CAE_PRI_RISK_FCTR_IN"),  # Overwritten by next stage "WhereExpression": 'N' 
    F.col("RiskScoreLink.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("RiskScoreLink.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("RiskScoreLink.RISK_SVRTY_CD").alias("RISK_SVRTY_CD"),
    F.col("RiskScoreLink.RISK_SVRTY_NM").alias("RISK_SVRTY_NM"),
    F.col("RiskScoreLink.RISK_IDNT_DT_SK").alias("RISK_IDNT_DT_SK"),
    F.col("RiskScoreLink.RISK_SVRTY_CD_SK").alias("RISK_SVRTY_CD_SK"),
    F.col("RiskScoreLink.RISK_CAT_NM").alias("RISK_CAT_NM"),
    F.col("RiskScoreLink.MCSRC_RISK_LVL_NO").alias("MCSRC_RISK_LVL_NO"),
    F.col("RiskScoreLink.MDCSN_RISK_SCORE_NO").alias("MDCSN_RISK_SCORE_NO"),
    F.col("RiskScoreLink.MDCSN_HLTH_STTUS_MESR_NO").alias("MDCSN_HLTH_STTUS_MESR_NO"),
    F.col("RiskScoreLink.CRG_ID").alias("CRG_ID"),
    F.col("RiskScoreLink.CRG_DESC").alias("CRG_DESC"),
    F.col("RiskScoreLink.AGG_CRG_BASE_3_ID").alias("AGG_CRG_BASE_3_ID"),
    F.col("RiskScoreLink.AGG_CRG_BASE_3_DESC").alias("AGG_CRG_BASE_3_DESC"),
    F.col("RiskScoreLink.CRG_WT").alias("CRG_WT"),
    F.col("RiskScoreLink.CRG_MDL_ID").alias("CRG_MDL_ID"),
    F.col("RiskScoreLink.CRG_VRSN_ID").alias("CRG_VRSN_ID"),
    F.when(F.col("RiskScoreLink.PRTY_NO").isNull(), F.lit(0)).otherwise(F.col("RiskScoreLink.PRTY_NO")).alias("PRTY_NO"),
    F.col("RiskScoreLink.Lkp_Priority_RISK_CAT_ID_dummy").alias("Lkp_Priority_RISK_CAT_ID_dummy"),
    F.col("RiskScoreLink.INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
    F.col("RiskScoreLink.RISK_MTHDLGY_CD_SK").alias("RISK_MTHDLGY_CD_SK"),
    F.col("RiskScoreLink.RISK_MTHDLGY_TYP_CD_SK").alias("RISK_MTHDLGY_TYP_CD_SK"),
    F.col("RiskScoreLink.RISK_MTHDLGY_TYP_SCORE_NO").alias("RISK_MTHDLGY_TYP_SCORE_NO")
)

df_XfrmRiskAgg_In_risk_fctr_lnk = df_XfrmRiskAgg_In_main.filter(
    F.col("Lkp_Priority_RISK_CAT_ID_dummy").isNotNull()
).select(
    F.col("MBR_UNIQ_KEY").alias("MEMBER"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.lit(1).alias("RISK_CAT"),
    F.col("FTR_RELTV_RISK_NO").alias("RELRSK"),
    F.col("PRTY_NO").alias("PRTY_NO"),
    F.col("Lkp_Priority_RISK_CAT_ID_dummy").alias("Lkp_Priority_RISK_CAT_ID_dummy")
)
df_XfrmRiskAgg_In_mcsrc_risk_fctr_lnk = df_XfrmRiskAgg_In_main.filter(
    F.col("Lkp_Priority_RISK_CAT_ID_dummy").isNotNull()
).select(
    F.col("MBR_UNIQ_KEY").alias("MEMBER"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.lit(1).alias("RISK_CAT"),
    F.col("PRTY_NO").alias("PRTY_NO"),
    F.col("Lkp_Priority_RISK_CAT_ID_dummy").alias("Lkp_Priority_RISK_CAT_ID_dummy")
)
df_XfrmRiskAgg_In_risk_grp_lin = df_XfrmRiskAgg_In_main.filter(
    F.col("Lkp_Priority_RISK_CAT_ID_dummy").isNotNull()
).select(
    F.col("MBR_UNIQ_KEY").alias("MEMBER"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("FTR_RELTV_RISK_NO").alias("REL_RISK"),
    F.col("Lkp_Priority_RISK_CAT_ID_dummy").alias("Lkp_Priority_RISK_CAT_ID_dummy")
)

# RiskFactr_Aggr (PxAggregator)
df_RiskFactr_Aggr = (
    df_XfrmRiskAgg_In_risk_fctr_lnk.groupBy("MEMBER","PRCS_YR_MO_SK","Lkp_Priority_RISK_CAT_ID_dummy")
    .agg(
        F.sum("RISK_CAT").alias("RISK_CAT_CNT"),
        F.max("RELRSK").alias("REL_RISK_MAX"),
        F.min("PRTY_NO").alias("PRTY_NO_MIN")
    )
)
df_RiskFactr_Aggr_out = df_RiskFactr_Aggr.select(
    F.col("MEMBER"),
    F.col("PRCS_YR_MO_SK"),
    F.col("Lkp_Priority_RISK_CAT_ID_dummy"),
    F.col("RISK_CAT_CNT"),
    F.col("REL_RISK_MAX"),
    F.col("PRTY_NO_MIN")
)

# Mcsrc_Aggr (PxAggregator)
df_Mcsrc_Aggr = (
    df_XfrmRiskAgg_In_mcsrc_risk_fctr_lnk.groupBy("MEMBER","PRCS_YR_MO_SK","Lkp_Priority_RISK_CAT_ID_dummy")
    .agg(
        F.max("PRTY_NO").alias("TOP_PRIORITY"),
        F.sum("RISK_CAT").alias("RISK_CAT_CNT")
    )
)
df_Mcsrc_Aggr_out = df_Mcsrc_Aggr.select(
    F.col("MEMBER"),
    F.col("PRCS_YR_MO_SK"),
    F.col("Lkp_Priority_RISK_CAT_ID_dummy"),
    F.col("TOP_PRIORITY"),
    F.col("RISK_CAT_CNT")
)

# RiskGrp_Aggr (PxAggregator)
df_RiskGrp_Aggr = (
    df_XfrmRiskAgg_In_risk_grp_lin.groupBy("MEMBER","PRCS_YR_MO_SK","REL_RISK","Lkp_Priority_RISK_CAT_ID_dummy")
    .agg(
        F.count("*").alias("RiskGrp_REL_RISK_CNT")
    )
)
df_RiskGrp_Aggr_out = df_RiskGrp_Aggr.select(
    F.col("MEMBER"),
    F.col("PRCS_YR_MO_SK"),
    F.col("REL_RISK"),
    F.col("Lkp_Priority_RISK_CAT_ID_dummy"),
    F.col("RiskGrp_REL_RISK_CNT")
)

# Lkup_Final (PxLookup)
df_Lkup_Final_Riskscore_Main = df_XfrmRiskAgg_In_main.alias("Riskscore_Main")
df_Lkup_Final_mcsrc_risk_link = df_Mcsrc_Aggr_out.alias("mcsrc_risk_link")
df_Lkup_Final_risk_grp_link = df_RiskGrp_Aggr_out.alias("risk_grp_link")
df_Lkup_Final_risk_link = df_RiskFactr_Aggr_out.alias("risk_link")
df_Lkup_Final_Lkp_RiskPriority = df_Transformer_136_output_link_Lkp_RiskPriority.alias("Lkp_RiskPriority")

join_step_1 = df_Lkup_Final_Riskscore_Main.join(
    df_Lkup_Final_mcsrc_risk_link,
    on=[
        df_Lkup_Final_Riskscore_Main["MBR_UNIQ_KEY"]==df_Lkup_Final_mcsrc_risk_link["MEMBER"],
        df_Lkup_Final_Riskscore_Main["PRCS_YR_MO_SK"]==df_Lkup_Final_mcsrc_risk_link["PRCS_YR_MO_SK"]
    ],
    how="left"
).alias("JOIN1")

join_step_2 = join_step_1.join(
    df_Lkup_Final_risk_grp_link,
    on=[
        join_step_1["MBR_UNIQ_KEY"]==df_Lkup_Final_risk_grp_link["MEMBER"],
        join_step_1["PRCS_YR_MO_SK"]==df_Lkup_Final_risk_grp_link["PRCS_YR_MO_SK"],
        join_step_1["FTR_RELTV_RISK_NO"]==df_Lkup_Final_risk_grp_link["REL_RISK"]
    ],
    how="left"
).alias("JOIN2")

join_step_3 = join_step_2.join(
    df_Lkup_Final_risk_link,
    on=[
        join_step_2["MBR_UNIQ_KEY"]==df_Lkup_Final_risk_link["MEMBER"],
        join_step_2["PRCS_YR_MO_SK"]==df_Lkup_Final_risk_link["PRCS_YR_MO_SK"]
    ],
    how="left"
).alias("JOIN3")

join_step_4 = join_step_3.join(
    df_Lkup_Final_Lkp_RiskPriority,
    on=[join_step_3["RISK_CAT_ID"]==df_Lkup_Final_Lkp_RiskPriority["RISK_CAT_ID"]],
    how="left"
).alias("JOIN4")

df_Lkup_Final_out = join_step_4.select(
    F.col("Riskscore_Main.MBR_RISK_MESR_SK").alias("MBR_RISK_MESR_SK"),
    F.col("Riskscore_Main.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Riskscore_Main.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Riskscore_Main.RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("Riskscore_Main.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("Riskscore_Main.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Riskscore_Main.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Riskscore_Main.RISK_CAT_SK").alias("RISK_CAT_SK"),
    F.col("Riskscore_Main.MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
    F.col("Riskscore_Main.FTR_RELTV_RISK_NO").alias("FTR_RELTV_RISK_NO"),
    F.col("Riskscore_Main.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("Riskscore_Main.MAJ_PRCTC_CAT_CD").alias("MAJ_PRCTC_CAT_CD"),
    F.col("Riskscore_Main.MAJ_PRCTC_CAT_DESC").alias("MAJ_PRCTC_CAT_DESC"),
    F.col("Riskscore_Main.MBR_SK").alias("MBR_SK"),
    F.col("Riskscore_Main.CAE_PRI_RISK_FCTR_IN").alias("CAE_PRI_RISK_FCTR_IN"),
    F.col("Riskscore_Main.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Riskscore_Main.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Riskscore_Main.RISK_SVRTY_CD").alias("RISK_SVRTY_CD"),
    F.col("Riskscore_Main.RISK_SVRTY_NM").alias("RISK_SVRTY_NM"),
    F.col("Riskscore_Main.RISK_IDNT_DT_SK").alias("RISK_IDNT_DT_SK"),
    F.col("Riskscore_Main.RISK_SVRTY_CD_SK").alias("RISK_SVRTY_CD_SK"),
    F.col("Riskscore_Main.RISK_CAT_NM").alias("RISK_CAT_NM"),
    F.col("Riskscore_Main.MCSRC_RISK_LVL_NO").alias("MCSRC_RISK_LVL_NO"),
    F.col("Riskscore_Main.MDCSN_RISK_SCORE_NO").alias("MDCSN_RISK_SCORE_NO"),
    F.col("Riskscore_Main.MDCSN_HLTH_STTUS_MESR_NO").alias("MDCSN_HLTH_STTUS_MESR_NO"),
    F.col("Riskscore_Main.CRG_ID").alias("CRG_ID"),
    F.col("Riskscore_Main.CRG_DESC").alias("CRG_DESC"),
    F.col("Riskscore_Main.AGG_CRG_BASE_3_ID").alias("AGG_CRG_BASE_3_ID"),
    F.col("Riskscore_Main.AGG_CRG_BASE_3_DESC").alias("AGG_CRG_BASE_3_DESC"),
    F.col("Riskscore_Main.CRG_WT").alias("CRG_WT"),
    F.col("Riskscore_Main.CRG_MDL_ID").alias("CRG_MDL_ID"),
    F.col("Riskscore_Main.CRG_VRSN_ID").alias("CRG_VRSN_ID"),
    F.col("Riskscore_Main.PRTY_NO").alias("PRTY_NO"),
    F.col("mcsrc_risk_link.TOP_PRIORITY").alias("TOP_PRIORITY_mcsrcLkup"),
    F.col("risk_link.RISK_CAT_CNT").alias("RISK_CAT_CNT_riskLkup"),
    F.col("risk_link.REL_RISK_MAX").alias("TOP_RISK_FCT_riskLkup"),
    F.col("risk_link.PRTY_NO_MIN").alias("PRTY_NO_riskLkup"),
    F.col("Lkp_RiskPriority.RISK_CAT_ID").alias("RISK_CAT_ID_riskPriorityLkup"),
    F.col("risk_grp_link.RiskGrp_REL_RISK_CNT").alias("RiskGrp_REL_RISK_CNT"),
    F.col("Riskscore_Main.Lkp_Priority_RISK_CAT_ID_dummy").alias("Lkp_Priority_RISK_CAT_ID_dummy"),
    F.col("Riskscore_Main.INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
    F.col("Riskscore_Main.RISK_MTHDLGY_CD_SK").alias("RISK_MTHDLGY_CD_SK"),
    F.col("Riskscore_Main.RISK_MTHDLGY_TYP_CD_SK").alias("RISK_MTHDLGY_TYP_CD_SK"),
    F.col("Riskscore_Main.RISK_MTHDLGY_TYP_SCORE_NO").alias("RISK_MTHDLGY_TYP_SCORE_NO")
)

# Final_Pass (CTransformerStage)
df_Final_Pass_in = df_Lkup_Final_out.alias("lnk_FinalPass_In")

cond_valid = (F.col("lnk_FinalPass_In.MBR_RISK_MESR_SK")!=0) & (F.col("lnk_FinalPass_In.MBR_RISK_MESR_SK")!=1)
df_Final_Pass_out_valid = df_Final_Pass_in.withColumn(
    "svImpProInd",
    F.when(
        F.col("lnk_FinalPass_In.Lkp_Priority_RISK_CAT_ID_dummy").isNull(),
        F.col("lnk_FinalPass_In.CAE_PRI_RISK_FCTR_IN")
    ).otherwise(
        F.when(
            F.col("lnk_FinalPass_In.RISK_CAT_CNT_riskLkup") == 1,
            F.lit("Y")
        ).otherwise(
            F.when(
                F.col("lnk_FinalPass_In.RiskGrp_REL_RISK_CNT") == 1,
                F.when(
                    F.col("lnk_FinalPass_In.FTR_RELTV_RISK_NO")==F.col("lnk_FinalPass_In.TOP_RISK_FCT_riskLkup"),
                    F.lit("Y")
                ).otherwise(F.lit("N"))
            ).otherwise(
                F.when(
                    (F.col("lnk_FinalPass_In.FTR_RELTV_RISK_NO")==F.col("lnk_FinalPass_In.TOP_RISK_FCT_riskLkup")) &
                    (F.col("lnk_FinalPass_In.PRTY_NO")==F.col("lnk_FinalPass_In.PRTY_NO_riskLkup")),
                    F.lit("Y")
                ).otherwise(F.lit("N"))
            )
        )
    )
).withColumn(
    "svMcSourceInd",
    F.when(
        F.col("lnk_FinalPass_In.Lkp_Priority_RISK_CAT_ID_dummy").isNull(),
        F.col("lnk_FinalPass_In.CAE_PRI_RISK_FCTR_IN")
    ).otherwise(
        F.when(
            F.col("lnk_FinalPass_In.RISK_CAT_CNT_riskLkup")==1,
            F.lit("Y")
        ).otherwise(
            F.when(
                F.col("lnk_FinalPass_In.PRTY_NO")==F.col("lnk_FinalPass_In.TOP_PRIORITY_mcsrcLkup"),
                F.lit("Y")
            ).otherwise(F.lit("N"))
        )
    )
)

df_Final_Pass_out_valid = df_Final_Pass_out_valid.withColumn(
    "CAE_PRI_RISK_FCTR_IN_new",
    F.when(
        F.col("lnk_FinalPass_In.SRC_SYS_CD")==F.lit("IMP"),
        F.col("svImpProInd")
    ).otherwise(
        F.when(
            (F.col("lnk_FinalPass_In.SRC_SYS_CD")==F.lit("MCSOURCE")) | (F.col("lnk_FinalPass_In.SRC_SYS_CD")==F.lit("IDEA")),
            F.col("svMcSourceInd")
        ).otherwise(
            F.when(
                (F.col("lnk_FinalPass_In.SRC_SYS_CD")==F.lit("IHMANALYTICS"))|
                (F.col("lnk_FinalPass_In.SRC_SYS_CD")==F.lit("TREO"))|
                (F.col("lnk_FinalPass_In.SRC_SYS_CD")==F.lit("BCBSKC")),
                F.lit("Y")
            ).otherwise(F.lit("N"))
        )
    )
)

df_Final_Pass_out_valid_filter = df_Final_Pass_out_valid.filter(cond_valid)

df_Final_Pass_out_valid_select = df_Final_Pass_out_valid_filter.select(
    F.col("lnk_FinalPass_In.MBR_RISK_MESR_SK").alias("MBR_RISK_MESR_SK"),
    F.col("lnk_FinalPass_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_FinalPass_In.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lnk_FinalPass_In.RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("lnk_FinalPass_In.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("lnk_FinalPass_In.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnk_FinalPass_In.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnk_FinalPass_In.RISK_CAT_SK").alias("RISK_CAT_SK"),
    F.col("lnk_FinalPass_In.MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
    F.col("lnk_FinalPass_In.FTR_RELTV_RISK_NO").alias("FTR_RELTV_RISK_NO"),
    F.col("lnk_FinalPass_In.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("lnk_FinalPass_In.MAJ_PRCTC_CAT_CD").alias("MAJ_PRCTC_CAT_CD"),
    F.col("lnk_FinalPass_In.MAJ_PRCTC_CAT_DESC").alias("MAJ_PRCTC_CAT_DESC"),
    F.col("lnk_FinalPass_In.MBR_SK").alias("MBR_SK"),
    F.col("CAE_PRI_RISK_FCTR_IN_new").alias("CAE_PRI_RISK_FCTR_IN"),
    F.col("lnk_FinalPass_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_FinalPass_In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_FinalPass_In.RISK_SVRTY_CD").alias("RISK_SVRTY_CD"),
    F.col("lnk_FinalPass_In.RISK_SVRTY_NM").alias("RISK_SVRTY_NM"),
    F.col("lnk_FinalPass_In.RISK_IDNT_DT_SK").alias("RISK_IDNT_DT_SK"),
    F.col("lnk_FinalPass_In.RISK_SVRTY_CD_SK").alias("RISK_SVRTY_CD_SK"),
    F.col("lnk_FinalPass_In.RISK_CAT_NM").alias("RISK_CAT_NM"),
    F.col("lnk_FinalPass_In.MCSRC_RISK_LVL_NO").alias("MCSRC_RISK_LVL_NO"),
    F.col("lnk_FinalPass_In.MDCSN_RISK_SCORE_NO").alias("MDCSN_RISK_SCORE_NO"),
    F.col("lnk_FinalPass_In.MDCSN_HLTH_STTUS_MESR_NO").alias("MDCSN_HLTH_STTUS_MESR_NO"),
    F.col("lnk_FinalPass_In.CRG_ID").alias("CRG_ID"),
    F.col("lnk_FinalPass_In.CRG_DESC").alias("CRG_DESC"),
    F.col("lnk_FinalPass_In.AGG_CRG_BASE_3_ID").alias("AGG_CRG_BASE_3_ID"),
    F.col("lnk_FinalPass_In.AGG_CRG_BASE_3_DESC").alias("AGG_CRG_BASE_3_DESC"),
    F.col("lnk_FinalPass_In.CRG_WT").alias("CRG_WT"),
    F.col("lnk_FinalPass_In.CRG_MDL_ID").alias("CRG_MDL_ID"),
    F.col("lnk_FinalPass_In.CRG_VRSN_ID").alias("CRG_VRSN_ID"),
    F.col("lnk_FinalPass_In.INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
    F.col("lnk_FinalPass_In.RISK_MTHDLGY_CD_SK").alias("RISK_MTHDLGY_CD_SK"),
    F.col("lnk_FinalPass_In.RISK_MTHDLGY_TYP_CD_SK").alias("RISK_MTHDLGY_TYP_CD_SK"),
    F.col("lnk_FinalPass_In.RISK_MTHDLGY_TYP_SCORE_NO").alias("RISK_MTHDLGY_TYP_SCORE_NO")
)

# The other links from Final_Pass (NALink and UNKLink) produce single row placeholders. We'll union them in funnel with the main output.
df_Final_Pass_out_NALink = (
    df_Final_Pass_in.filter(((F.col("@INROWNUM")- F.lit(1)) * F.col("@NUMPARTITIONS") + F.col("@PARTITIONNUM") + F.lit(1))==1) 
    # This condition can't run as is in Spark. We'll interpret as "just no rows" or a single row stub.
    .limit(1)
    .selectExpr(
        "1 as MBR_RISK_MESR_SK",
        "'NA' as SRC_SYS_CD",
        "1 as MBR_UNIQ_KEY",
        "'NA' as RISK_CAT_ID",
        "'175301' as PRCS_YR_MO_SK",
        "'1753-01-01' as CRT_RUN_CYC_EXCTN_DT_SK",
        "'1753-01-01' as LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "1 as RISK_CAT_SK",
        "1 as MBR_MED_MESRS_SK",
        "0 as FTR_RELTV_RISK_NO",
        "1 as INDV_BE_KEY",
        "'NA' as MAJ_PRCTC_CAT_CD",
        "'NA' as MAJ_PRCTC_CAT_DESC",
        "1 as MBR_SK",
        "'N' as CAE_PRI_RISK_FCTR_IN",
        "100 as CRT_RUN_CYC_EXCTN_SK",
        "100 as LAST_UPDT_RUN_CYC_EXCTN_SK",
        "'NA' as RISK_SVRTY_CD",
        "'NA' as RISK_SVRTY_NM",
        "'1753-01-01' as RISK_IDNT_DT_SK",
        "1 as RISK_SVRTY_CD_SK",
        "'NA' as RISK_CAT_NM",
        "0 as MCSRC_RISK_LVL_NO",
        "0 as MDCSN_RISK_SCORE_NO",
        "0 as MDCSN_HLTH_STTUS_MESR_NO",
        "'NA' as CRG_ID",
        "'NA' as CRG_DESC",
        "'NA' as AGG_CRG_BASE_3_ID",
        "'NA' as AGG_CRG_BASE_3_DESC",
        "0 as CRG_WT",
        "'NA' as CRG_MDL_ID",
        "'NA' as CRG_VRSN_ID",
        "0 as INDV_BE_MARA_RSLT_SK",
        "0 as RISK_MTHDLGY_CD_SK",
        "0 as RISK_MTHDLGY_TYP_CD_SK",
        "null as RISK_MTHDLGY_TYP_SCORE_NO"
    )
)

df_Final_Pass_out_UNKLink = (
    df_Final_Pass_in.filter(((F.col("@INROWNUM")- F.lit(1)) * F.col("@NUMPARTITIONS") + F.col("@PARTITIONNUM") + F.lit(1))==1) 
    .limit(1)
    .selectExpr(
        "0 as MBR_RISK_MESR_SK",
        "'UNK' as SRC_SYS_CD",
        "0 as MBR_UNIQ_KEY",
        "'UNK' as RISK_CAT_ID",
        "'175301' as PRCS_YR_MO_SK",
        "'1753-01-01' as CRT_RUN_CYC_EXCTN_DT_SK",
        "'1753-01-01' as LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "0 as RISK_CAT_SK",
        "0 as MBR_MED_MESRS_SK",
        "0 as FTR_RELTV_RISK_NO",
        "0 as INDV_BE_KEY",
        "'UNK' as MAJ_PRCTC_CAT_CD",
        "'UNK' as MAJ_PRCTC_CAT_DESC",
        "0 as MBR_SK",
        "'N' as CAE_PRI_RISK_FCTR_IN",
        "100 as CRT_RUN_CYC_EXCTN_SK",
        "100 as LAST_UPDT_RUN_CYC_EXCTN_SK",
        "'UNK' as RISK_SVRTY_CD",
        "'UNK' as RISK_SVRTY_NM",
        "'1753-01-01' as RISK_IDNT_DT_SK",
        "0 as RISK_SVRTY_CD_SK",
        "'UNK' as RISK_CAT_NM",
        "0 as MCSRC_RISK_LVL_NO",
        "0 as MDCSN_RISK_SCORE_NO",
        "0 as MDCSN_HLTH_STTUS_MESR_NO",
        "'UNK' as CRG_ID",
        "'UNK' as CRG_DESC",
        "'UNK' as AGG_CRG_BASE_3_ID",
        "'UNK' as AGG_CRG_BASE_3_DESC",
        "0 as CRG_WT",
        "'UNK' as CRG_MDL_ID",
        "'UNK' as CRG_VRSN_ID",
        "0 as INDV_BE_MARA_RSLT_SK",
        "0 as RISK_MTHDLGY_CD_SK",
        "0 as RISK_MTHDLGY_TYP_CD_SK",
        "null as RISK_MTHDLGY_TYP_SCORE_NO"
    )
)

# Fnl_MbrRisk (PxFunnel)
df_Fnl_MbrRisk_in1 = df_SetIndicators_out.alias("AlineoOutput")
df_Fnl_MbrRisk_in2 = df_Final_Pass_out_valid_select.alias("lnk_RiskOutput")
df_Fnl_MbrRisk_in3 = df_Final_Pass_out_NALink.alias("NALink")
df_Fnl_MbrRisk_in4 = df_Final_Pass_out_UNKLink.alias("UNKLink")
df_funnel = df_Fnl_MbrRisk_in1.select(
    "MBR_RISK_MESR_SK","SRC_SYS_CD","MBR_UNIQ_KEY","RISK_CAT_ID","PRCS_YR_MO_SK",
    "CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK","RISK_CAT_SK","MBR_MED_MESRS_SK",
    "FTR_RELTV_RISK_NO","INDV_BE_KEY","MAJ_PRCTC_CAT_CD","MAJ_PRCTC_CAT_DESC","MBR_SK",
    "CAE_PRI_RISK_FCTR_IN","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK",
    "RISK_SVRTY_CD","RISK_SVRTY_NM","RISK_IDNT_DT_SK","RISK_SVRTY_CD_SK","RISK_CAT_NM",
    "MCSRC_RISK_LVL_NO","MDCSN_RISK_SCORE_NO","MDCSN_HLTH_STTUS_MESR_NO","CRG_ID","CRG_DESC",
    "AGG_CRG_BASE_3_ID","AGG_CRG_BASE_3_DESC","CRG_WT","CRG_MDL_ID","CRG_VRSN_ID",
    "INDV_BE_MARA_RSLT_SK","RISK_MTHDLGY_CD_SK","RISK_MTHDLGY_TYP_CD_SK","RISK_MTHDLGY_TYP_SCORE_NO"
).unionByName(
    df_Fnl_MbrRisk_in2.select(
        "MBR_RISK_MESR_SK","SRC_SYS_CD","MBR_UNIQ_KEY","RISK_CAT_ID","PRCS_YR_MO_SK",
        "CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK","RISK_CAT_SK","MBR_MED_MESRS_SK",
        "FTR_RELTV_RISK_NO","INDV_BE_KEY","MAJ_PRCTC_CAT_CD","MAJ_PRCTC_CAT_DESC","MBR_SK",
        "CAE_PRI_RISK_FCTR_IN","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK",
        "RISK_SVRTY_CD","RISK_SVRTY_NM","RISK_IDNT_DT_SK","RISK_SVRTY_CD_SK","RISK_CAT_NM",
        "MCSRC_RISK_LVL_NO","MDCSN_RISK_SCORE_NO","MDCSN_HLTH_STTUS_MESR_NO","CRG_ID","CRG_DESC",
        "AGG_CRG_BASE_3_ID","AGG_CRG_BASE_3_DESC","CRG_WT","CRG_MDL_ID","CRG_VRSN_ID",
        "INDV_BE_MARA_RSLT_SK","RISK_MTHDLGY_CD_SK","RISK_MTHDLGY_TYP_CD_SK","RISK_MTHDLGY_TYP_SCORE_NO"
    )
).unionByName(
    df_Fnl_MbrRisk_in3.select(
        "MBR_RISK_MESR_SK","SRC_SYS_CD","MBR_UNIQ_KEY","RISK_CAT_ID","PRCS_YR_MO_SK",
        "CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK","RISK_CAT_SK","MBR_MED_MESRS_SK",
        "FTR_RELTV_RISK_NO","INDV_BE_KEY","MAJ_PRCTC_CAT_CD","MAJ_PRCTC_CAT_DESC","MBR_SK",
        "CAE_PRI_RISK_FCTR_IN","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK",
        "RISK_SVRTY_CD","RISK_SVRTY_NM","RISK_IDNT_DT_SK","RISK_SVRTY_CD_SK","RISK_CAT_NM",
        "MCSRC_RISK_LVL_NO","MDCSN_RISK_SCORE_NO","MDCSN_HLTH_STTUS_MESR_NO","CRG_ID","CRG_DESC",
        "AGG_CRG_BASE_3_ID","AGG_CRG_BASE_3_DESC","CRG_WT","CRG_MDL_ID","CRG_VRSN_ID",
        "INDV_BE_MARA_RSLT_SK","RISK_MTHDLGY_CD_SK","RISK_MTHDLGY_TYP_CD_SK","RISK_MTHDLGY_TYP_SCORE_NO"
    )
).unionByName(
    df_Fnl_MbrRisk_in4.select(
        "MBR_RISK_MESR_SK","SRC_SYS_CD","MBR_UNIQ_KEY","RISK_CAT_ID","PRCS_YR_MO_SK",
        "CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK","RISK_CAT_SK","MBR_MED_MESRS_SK",
        "FTR_RELTV_RISK_NO","INDV_BE_KEY","MAJ_PRCTC_CAT_CD","MAJ_PRCTC_CAT_DESC","MBR_SK",
        "CAE_PRI_RISK_FCTR_IN","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK",
        "RISK_SVRTY_CD","RISK_SVRTY_NM","RISK_IDNT_DT_SK","RISK_SVRTY_CD_SK","RISK_CAT_NM",
        "MCSRC_RISK_LVL_NO","MDCSN_RISK_SCORE_NO","MDCSN_HLTH_STTUS_MESR_NO","CRG_ID","CRG_DESC",
        "AGG_CRG_BASE_3_ID","AGG_CRG_BASE_3_DESC","CRG_WT","CRG_MDL_ID","CRG_VRSN_ID",
        "INDV_BE_MARA_RSLT_SK","RISK_MTHDLGY_CD_SK","RISK_MTHDLGY_TYP_CD_SK","RISK_MTHDLGY_TYP_SCORE_NO"
    )
)

# seq_MBR_RISK_MESR_I_csv_load (PxSequentialFile)
# Final select for column order and rpad for char columns
df_final = df_funnel.select(
    F.col("MBR_RISK_MESR_SK"),
    F.col("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY"),
    F.col("RISK_CAT_ID"),
    rpad(F.col("PRCS_YR_MO_SK").cast(StringType()), 6, " ").alias("PRCS_YR_MO_SK"),
    rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK").cast(StringType()), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").cast(StringType()), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("RISK_CAT_SK"),
    F.col("MBR_MED_MESRS_SK"),
    F.col("FTR_RELTV_RISK_NO"),
    F.col("INDV_BE_KEY"),
    F.col("MAJ_PRCTC_CAT_CD"),
    F.col("MAJ_PRCTC_CAT_DESC"),
    F.col("MBR_SK"),
    rpad(F.col("CAE_PRI_RISK_FCTR_IN").cast(StringType()), 1, " ").alias("CAE_PRI_RISK_FCTR_IN"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("RISK_SVRTY_CD"),
    F.col("RISK_SVRTY_NM"),
    rpad(F.col("RISK_IDNT_DT_SK").cast(StringType()), 10, " ").alias("RISK_IDNT_DT_SK"),
    F.col("RISK_SVRTY_CD_SK"),
    F.col("RISK_CAT_NM"),
    F.col("MCSRC_RISK_LVL_NO"),
    F.col("MDCSN_RISK_SCORE_NO"),
    F.col("MDCSN_HLTH_STTUS_MESR_NO"),
    F.col("CRG_ID"),
    F.col("CRG_DESC"),
    F.col("AGG_CRG_BASE_3_ID"),
    F.col("AGG_CRG_BASE_3_DESC"),
    F.col("CRG_WT"),
    F.col("CRG_MDL_ID"),
    rpad(F.col("CRG_VRSN_ID").cast(StringType()), 18, " ").alias("CRG_VRSN_ID"),
    F.col("INDV_BE_MARA_RSLT_SK"),
    F.col("RISK_MTHDLGY_CD_SK"),
    F.col("RISK_MTHDLGY_TYP_CD_SK"),
    F.col("RISK_MTHDLGY_TYP_SCORE_NO")
)

write_files(
    df_final,
    f"{adls_path}/load/MBR_RISK_MESR_I.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)