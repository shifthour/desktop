# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                                   Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                                   -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Ralph Tucker          5/21/2009        Web Realign 3500                             New ETL                                                                                     devlIDSNew                    Steph Goddard           05/28/2009
# MAGIC 
# MAGIC  SAndrew               2009-08-18         Production Support                            Brought down from ids20 to testIDS for emergency prod fix         testIDS                    
# MAGIC                                                                                                                   Changed the SQLServer Update Mode from Clear table to Insert/Update
# MAGIC                                                                                                                     Added tbl to the delete process 
# MAGIC Kalyan Neelam       2010-04-14         4428                                                 Added 19 new fields at the end                                                      IntegrateWrhsDevl         Steph Goddard           04/19/2010
# MAGIC 
# MAGIC Bhoomi Dasari        2011-10-17        4673                                              Changed WAIST_CRCMFR_NO to Decimal(7,2) from Integer(10)     IntegrateWrhsDevl           SAndrew                  2011-10-21       
# MAGIC   
# MAGIC Archana Palivela             08/08/2013          5114                                    Original Programming(Server to Parallel)                                        IntegrateWrhsDevl            Pete Marshall             10/23/2013

# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC HLTH_SCRN_MBR_GNDR_CD_SK
# MAGIC Write MBRSH_DM_MBR_HLTH_SCRNData into a Sequential file for Load Job IdsDmProdDmDedctCmpntLoad.
# MAGIC Read all the Data from IDS HLTH_SCRN Table; Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK.
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsDmMbrshDmMbrHlthScrnExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, lit, to_timestamp, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
DataMartRunCycle = get_widget_value('DataMartRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_HLTH_SCRN_in = f"""
SELECT 
HLTH_SCRN.HLTH_SCRN_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
HLTH_SCRN.MBR_UNIQ_KEY,
HLTH_SCRN.LAST_UPDT_RUN_CYC_EXCTN_SK,
HLTH_SCRN.HLTH_SCRN_MBR_GNDR_CD_SK,
HLTH_SCRN.BMI_NO,
HLTH_SCRN.CHLSTRL_RATIO_NO,
HLTH_SCRN.DIASTOLIC_BP_NO,
HLTH_SCRN.GLUCOSE_NO,
HLTH_SCRN.HDL_NO,
HLTH_SCRN.HT_INCH_NO,
HLTH_SCRN.LDL_NO,
HLTH_SCRN.MBR_AGE_NO,
HLTH_SCRN.PSA_NO,
HLTH_SCRN.SYSTOLIC_BP_NO,
HLTH_SCRN.TOT_CHLSTRL_NO,
HLTH_SCRN.TGL_NO,
HLTH_SCRN.WAIST_CRCMFR_NO,
HLTH_SCRN.WT_NO,
HLTH_SCRN.SCRN_DT_SK,
HLTH_SCRN.CUR_SMOKER_RISK_IN,
HLTH_SCRN.DBTC_RISK_IN,
HLTH_SCRN.FRMR_SMOKER_RISK_IN,
HLTH_SCRN.HEART_DSS_RISK_IN,
HLTH_SCRN.HI_CHLSTRL_RISK_IN,
HLTH_SCRN.HI_BP_RISK_IN,
HLTH_SCRN.EXRCS_LACK_RISK_IN,
HLTH_SCRN.OVERWT_RISK_IN,
HLTH_SCRN.STRESS_RISK_IN,
HLTH_SCRN.STROKE_RISK_IN,
HLTH_SCRN.BODY_FAT_PCT,
HLTH_SCRN.FSTNG_IN,
HLTH_SCRN.HA1C_NO,
HLTH_SCRN.PRGNCY_IN,
HLTH_SCRN.RFRL_TO_DM_IN,
HLTH_SCRN.RFRL_TO_PHYS_IN,
HLTH_SCRN.RESCRN_IN,
HLTH_SCRN.TSH_NO
FROM {IDSOwner}.HLTH_SCRN HLTH_SCRN,
     {IDSOwner}.CD_MPPNG CD
WHERE HLTH_SCRN.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
"""
df_db2_HLTH_SCRN_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_HLTH_SCRN_in)
    .load()
)

extract_query_DB2_HLTH_SCRN_Uniq = f"""
SELECT 
MBR_UNIQ_KEY, 
Max(SCRN_DT_SK) SCRN_DT_SK
FROM {IDSOwner}.HLTH_SCRN
WHERE MBR_UNIQ_KEY > 1
GROUP BY MBR_UNIQ_KEY
"""
df_DB2_HLTH_SCRN_Uniq = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_HLTH_SCRN_Uniq)
    .load()
)

extract_query_DB2_CD_MPPNG = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_DB2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_CD_MPPNG)
    .load()
)

df_lkp_Codes = (
    df_db2_HLTH_SCRN_in.alias("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC")
    .join(
        df_DB2_HLTH_SCRN_Uniq.alias("Lnk_Hlth_scrn"),
        (
            (col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.MBR_UNIQ_KEY") == col("Lnk_Hlth_scrn.MBR_UNIQ_KEY"))
            & (col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.SCRN_DT_SK") == col("Lnk_Hlth_scrn.SCRN_DT_SK"))
        ),
        "left"
    )
    .join(
        df_DB2_CD_MPPNG.alias("Lnk_CdMppng"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.HLTH_SCRN_MBR_GNDR_CD_SK") == col("Lnk_CdMppng.CD_MPPNG_SK"),
        "left"
    )
    .select(
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("Lnk_Hlth_scrn.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.SCRN_DT_SK").alias("SCRN_DT_SK"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.BMI_NO").alias("BMI_NO"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.CHLSTRL_RATIO_NO").alias("CHLSTRL_RATIO_NO"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.DIASTOLIC_BP_NO").alias("DIASTOLIC_BP_NO"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.GLUCOSE_NO").alias("GLUCOSE_NO"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.HDL_NO").alias("HDL_NO"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.HT_INCH_NO").alias("HT_INCH_NO"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.LDL_NO").alias("LDL_NO"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.MBR_AGE_NO").alias("MBR_AGE_NO"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.PSA_NO").alias("PSA_NO"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.SYSTOLIC_BP_NO").alias("SYSTOLIC_BP_NO"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.TOT_CHLSTRL_NO").alias("TOT_CHLSTRL_NO"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.TGL_NO").alias("TGL_NO"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.WAIST_CRCMFR_NO").alias("WAIST_CRCMFR_NO"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.WT_NO").alias("WT_NO"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.BODY_FAT_PCT").alias("BODY_FAT_PCT"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.CUR_SMOKER_RISK_IN").alias("CUR_SMOKER_RISK_IN"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.DBTC_RISK_IN").alias("DBTC_RISK_IN"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.EXRCS_LACK_RISK_IN").alias("EXRCS_LACK_RISK_IN"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.FRMR_SMOKER_RISK_IN").alias("FRMR_SMOKER_RISK_IN"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.FSTNG_IN").alias("FSTNG_IN"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.HA1C_NO").alias("HA1C_NO"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.HEART_DSS_RISK_IN").alias("HEART_DSS_RISK_IN"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.HI_BP_RISK_IN").alias("HI_BP_RISK_IN"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.HI_CHLSTRL_RISK_IN").alias("HI_CHLSTRL_RISK_IN"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.OVERWT_RISK_IN").alias("OVERWT_RISK_IN"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.PRGNCY_IN").alias("PRGNCY_IN"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.RFRL_TO_DM_IN").alias("RFRL_TO_DM_IN"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.RFRL_TO_PHYS_IN").alias("RFRL_TO_PHYS_IN"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.RESCRN_IN").alias("RESCRN_IN"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.STRESS_RISK_IN").alias("STRESS_RISK_IN"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.STROKE_RISK_IN").alias("STROKE_RISK_IN"),
        col("lnk_IdsDmMbrshDmMbrHlthScrnExtr_InABC.TSH_NO").alias("TSH_NO"),
        col("Lnk_CdMppng.TRGT_CD").alias("HLTH_SCRN_MBR_GNDR_CD"),
        col("Lnk_Hlth_scrn.SCRN_DT_SK").alias("SCRN_DT_SK_Luk")
    )
)

df_xfrm_BusinessLogic_tmp = df_lkp_Codes.withColumn(
    "svMaxScrnDt",
    when(trim(col("SCRN_DT_SK_Luk")) == '', lit('N')).otherwise(lit('Y'))
)

df_xfrm_BusinessLogic_flt = df_xfrm_BusinessLogic_tmp.filter(col("svMaxScrnDt") == lit('Y'))

df_xfrm_BusinessLogic = (
    df_xfrm_BusinessLogic_flt
    .withColumn(
        "LAST_HLTH_SCRN_DT",
        when(
            (trim(col("SCRN_DT_SK")) == 'UNK') | (trim(col("SCRN_DT_SK")) == 'NA'),
            lit('1753-01-01 00:00:00')
        ).otherwise(to_timestamp(col("SCRN_DT_SK"), 'yyyy-MM-dd'))
    )
    .withColumn("BMI_NO", col("BMI_NO").cast("int"))
    .withColumn("LAST_UPDT_RUN_CYC_NO", lit(DataMartRunCycle))
)

df_xfrm_BusinessLogic_final = df_xfrm_BusinessLogic.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("LAST_HLTH_SCRN_DT").alias("LAST_HLTH_SCRN_DT"),
    col("BMI_NO").alias("BMI_NO"),
    col("CHLSTRL_RATIO_NO").alias("CHLSTRL_RATIO_NO"),
    col("DIASTOLIC_BP_NO").alias("DIASTOLIC_BP_NO"),
    col("GLUCOSE_NO").alias("GLUCOSE_NO"),
    col("HDL_NO").alias("HDL_NO"),
    col("HT_INCH_NO").alias("HT_INCH_NO"),
    col("LDL_NO").alias("LDL_NO"),
    col("MBR_AGE_NO").alias("MBR_AGE_NO"),
    col("PSA_NO").alias("PSA_NO"),
    col("SYSTOLIC_BP_NO").alias("SYSTOLIC_BP_NO"),
    col("TOT_CHLSTRL_NO").alias("TOT_CHLSTRL_NO"),
    col("TGL_NO").alias("TGL_NO"),
    col("WAIST_CRCMFR_NO").alias("WAIST_CRCMFR_NO"),
    col("WT_NO").alias("WT_NO"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO"),
    col("BODY_FAT_PCT").alias("BODY_FAT_PCT"),
    col("CUR_SMOKER_RISK_IN").alias("CUR_SMOKER_RISK_IN"),
    col("DBTC_RISK_IN").alias("DBTC_RISK_IN"),
    col("EXRCS_LACK_RISK_IN").alias("EXRCS_LACK_RISK_IN"),
    col("FRMR_SMOKER_RISK_IN").alias("FRMR_SMOKER_RISK_IN"),
    col("FSTNG_IN").alias("FSTNG_IN"),
    col("HA1C_NO").alias("HA1C_NO"),
    col("HEART_DSS_RISK_IN").alias("HEART_DSS_RISK_IN"),
    col("HI_BP_RISK_IN").alias("HI_BP_RISK_IN"),
    col("HI_CHLSTRL_RISK_IN").alias("HI_CHLSTRL_RISK_IN"),
    col("HLTH_SCRN_MBR_GNDR_CD").alias("HLTH_SCRN_MBR_GNDR_CD"),
    col("OVERWT_RISK_IN").alias("OVERWT_RISK_IN"),
    col("PRGNCY_IN").alias("PRGNCY_IN"),
    col("RFRL_TO_DM_IN").alias("RFRL_TO_DM_IN"),
    col("RFRL_TO_PHYS_IN").alias("RFRL_TO_PHYS_IN"),
    col("RESCRN_IN").alias("RESCRN_IN"),
    col("STROKE_RISK_IN").alias("STROKE_RISK_IN"),
    col("STRESS_RISK_IN").alias("STRS_RISK_IN"),
    col("TSH_NO").alias("TSH_NO")
)

df_xfrm_BusinessLogic_rpad = (
    df_xfrm_BusinessLogic_final
    .withColumn("CUR_SMOKER_RISK_IN", rpad(col("CUR_SMOKER_RISK_IN"), 1, " "))
    .withColumn("DBTC_RISK_IN", rpad(col("DBTC_RISK_IN"), 1, " "))
    .withColumn("EXRCS_LACK_RISK_IN", rpad(col("EXRCS_LACK_RISK_IN"), 1, " "))
    .withColumn("FRMR_SMOKER_RISK_IN", rpad(col("FRMR_SMOKER_RISK_IN"), 1, " "))
    .withColumn("FSTNG_IN", rpad(col("FSTNG_IN"), 1, " "))
    .withColumn("HEART_DSS_RISK_IN", rpad(col("HEART_DSS_RISK_IN"), 1, " "))
    .withColumn("HI_BP_RISK_IN", rpad(col("HI_BP_RISK_IN"), 1, " "))
    .withColumn("HI_CHLSTRL_RISK_IN", rpad(col("HI_CHLSTRL_RISK_IN"), 1, " "))
    .withColumn("OVERWT_RISK_IN", rpad(col("OVERWT_RISK_IN"), 1, " "))
    .withColumn("PRGNCY_IN", rpad(col("PRGNCY_IN"), 1, " "))
    .withColumn("RFRL_TO_DM_IN", rpad(col("RFRL_TO_DM_IN"), 1, " "))
    .withColumn("RFRL_TO_PHYS_IN", rpad(col("RFRL_TO_PHYS_IN"), 1, " "))
    .withColumn("RESCRN_IN", rpad(col("RESCRN_IN"), 1, " "))
    .withColumn("STROKE_RISK_IN", rpad(col("STROKE_RISK_IN"), 1, " "))
    .withColumn("STRS_RISK_IN", rpad(col("STRS_RISK_IN"), 1, " "))
)

write_files(
    df_xfrm_BusinessLogic_rpad,
    f"{adls_path}/load/MBRSH_DM_MBR_HLTH_SCRN.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)