# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:    IdsMbrGrpRelEDIExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC                    IDS Membership Data Mart Group Related Entity EDI extract from IDS to Data Mart.  
# MAGIC 
# MAGIC INPUTS:
# MAGIC                    IDS:  GRP_REL_ENTY
# MAGIC 
# MAGIC HASH FILES:         
# MAGIC                     hf_etrnl_cd_mppng
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                     Lookups to extract fields
# MAGIC              
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                     Extracts from IDS and pulls to the Data Marts.  Perform no changes with the data.
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Data Mart:  MBRSH_DM_GRPREL_EDI
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                                   Change Description                                                                 Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                                   -----------------------------------------------------------------------                         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Terri O'Bryan            10-22-2009       3500 Addendum                                  Original Program                                                                      devlIDSnew                    Steph Goddard         10/29/2009        
# MAGIC Shiva Devagiri           07/17/2013     5114                                                  Original Programming(Server to Parallel)                                   IntegrateWhseDevl      Peter Marshall            10/21/2013
# MAGIC 
# MAGIC Pooja Sunkara          07-30-2104       TFS 9537                                        Renamed load file from MBRSHP_DM_GRP_REL_EDI.dat to     IntegrateNewDevl         Jag Yelavarthi          2014-08-01        
# MAGIC                                                                                                                  MBRSH_DM_GRP_REL_EDI.dat
# MAGIC Kalyan Neelam          2015-11-04         5212                                                Added Hosted_Grp_Lookup to drop Host group records          IntegrateDev1              Bhoomi Dasari            11/5/2015

# MAGIC Drop Hosted Groups
# MAGIC Read from source table GRP_REL_ENTY from IDS.
# MAGIC Job Name: IdsDmGrpRelEdiExtr
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC SRC_SYS_CD_SK
# MAGIC Add Defaults and Null Handling.
# MAGIC Write MBRSHP_DM_GRP_REL_EDI Data into a Sequential file for Load Job IdsDmGrpRelEdiLoad.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, lit, to_timestamp, date_format, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

jdbc_url = None
jdbc_props = None

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrDate = get_widget_value('CurrDate','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT DISTINCT G.GRP_ID FROM {IDSOwner}.GRP G, {IDSOwner}.PRNT_GRP PG WHERE G.CLNT_SK = PG.CLNT_SK AND PG.CLNT_ID <> 'HS' AND G.GRP_ID NOT LIKE '90%'"
df_db2_Hosted_GRP_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""SELECT 
GRE.GRP_REL_ENTY_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
GRE.GRP_ID,
GRE.GRP_REL_ENTY_CAT_CD,
GRE.GRP_REL_ENTY_TYP_CD,
GRE.EFF_DT_SK,
GRE.CRT_RUN_CYC_EXCTN_SK,
GRE.LAST_UPDT_RUN_CYC_EXCTN_SK,
GRE.GRP_SK,
GRE.GRP_REL_ENTY_CAT_CD_SK,
GRE.GRP_REL_ENTY_TERM_RSN_CD_SK,
GRE.GRP_REL_ENTY_TYP_CD_SK,
GRE.TERM_DT_SK,
GRE.REL_ENTY_ID,
GRE.REL_ENTY_NM
FROM {IDSOwner}.GRP_REL_ENTY GRE
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON GRE.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE 
GRP_SK NOT IN (0, 1)
AND EFF_DT_SK <= '{CurrDate}' AND TERM_DT_SK >= '{CurrDate}'
AND (GRP_REL_ENTY_TYP_CD LIKE ('ED%') OR GRP_REL_ENTY_TYP_CD LIKE ('EX%'))
AND GRP_ID IN (
  SELECT DISTINCT GRP2.GRP_ID FROM {IDSOwner}.GRP_REL_ENTY GRP2
  WHERE GRP2.REL_ENTY_ID LIKE ('EDI%')
  AND GRP2.EFF_DT_SK <= '{CurrDate}' AND GRP2.TERM_DT_SK >= '{CurrDate}'
)
"""
df_db2_GRP_REL_ENTY_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT CD_MPPNG_SK, COALESCE(TRGT_CD,'UNK') TRGT_CD, COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM FROM {IDSOwner}.CD_MPPNG"
df_db2_CD_MPPNG_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_lkp_Codes = df_db2_GRP_REL_ENTY_in.alias("lnk_IdsDmMbrshpDmGrpRelEdiExtr_InAbc").join(
    df_db2_CD_MPPNG_In.alias("Grp_Rel_Enty_Typ_Lookup"),
    col("lnk_IdsDmMbrshpDmGrpRelEdiExtr_InAbc.GRP_REL_ENTY_TYP_CD_SK") == col("Grp_Rel_Enty_Typ_Lookup.CD_MPPNG_SK"),
    how="left"
)
df_lkp_Codes_out = df_lkp_Codes.select(
    col("lnk_IdsDmMbrshpDmGrpRelEdiExtr_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_IdsDmMbrshpDmGrpRelEdiExtr_InAbc.GRP_ID").alias("GRP_ID"),
    col("lnk_IdsDmMbrshpDmGrpRelEdiExtr_InAbc.REL_ENTY_ID").alias("GRP_REL_ENTY_ID"),
    col("lnk_IdsDmMbrshpDmGrpRelEdiExtr_InAbc.GRP_REL_ENTY_TYP_CD").alias("GRP_REL_ENTY_TYP_CD"),
    col("lnk_IdsDmMbrshpDmGrpRelEdiExtr_InAbc.EFF_DT_SK").alias("EFF_DT"),
    col("lnk_IdsDmMbrshpDmGrpRelEdiExtr_InAbc.TERM_DT_SK").alias("TERM_DT"),
    col("lnk_IdsDmMbrshpDmGrpRelEdiExtr_InAbc.REL_ENTY_NM").alias("GRP_REL_ENTY_NM"),
    col("Grp_Rel_Enty_Typ_Lookup.TRGT_CD_NM").alias("GRP_REL_ENTY_TYP_NM")
)

df_hosted_grp_lookup = df_lkp_Codes_out.alias("lnkCodesLkpDataOut1").join(
    df_db2_Hosted_GRP_In.alias("Host_Grp_Id_lookup"),
    col("lnkCodesLkpDataOut1.GRP_ID") == col("Host_Grp_Id_lookup.GRP_ID"),
    how="inner"
)
df_hosted_grp_lookup_out = df_hosted_grp_lookup.select(
    col("lnkCodesLkpDataOut1.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkCodesLkpDataOut1.GRP_ID").alias("GRP_ID"),
    col("lnkCodesLkpDataOut1.GRP_REL_ENTY_ID").alias("GRP_REL_ENTY_ID"),
    col("lnkCodesLkpDataOut1.GRP_REL_ENTY_TYP_CD").alias("GRP_REL_ENTY_TYP_CD"),
    col("lnkCodesLkpDataOut1.EFF_DT").alias("EFF_DT"),
    col("lnkCodesLkpDataOut1.TERM_DT").alias("TERM_DT"),
    col("lnkCodesLkpDataOut1.GRP_REL_ENTY_NM").alias("GRP_REL_ENTY_NM"),
    col("lnkCodesLkpDataOut1.GRP_REL_ENTY_TYP_NM").alias("GRP_REL_ENTY_TYP_NM")
)

df_xfm_BusinessLogic = df_hosted_grp_lookup_out.select(
    when(col("SRC_SYS_CD").isNull(), "UNK ").otherwise(col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    col("GRP_ID").alias("GRP_ID"),
    col("GRP_REL_ENTY_ID").alias("GRP_REL_ENTY_ID"),
    col("GRP_REL_ENTY_TYP_CD").alias("GRP_REL_ENTY_TYP_CD"),
    to_timestamp(col("EFF_DT"), 'yyyy-MM-dd').alias("EFF_DT"),
    to_timestamp(col("TERM_DT"), 'yyyy-MM-dd').alias("TERM_DT"),
    col("GRP_REL_ENTY_NM").alias("GRP_REL_ENTY_NM"),
    when(col("GRP_REL_ENTY_TYP_NM").isNull(), "UNK ").otherwise(col("GRP_REL_ENTY_TYP_NM")).alias("GRP_REL_ENTY_TYP_NM"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_NO")
)

final_df = df_xfm_BusinessLogic.select(
    col("SRC_SYS_CD"),
    col("GRP_ID"),
    col("GRP_REL_ENTY_ID"),
    col("GRP_REL_ENTY_TYP_CD"),
    rpad(date_format(col("EFF_DT"), "yyyy-MM-dd"), 10, " ").alias("EFF_DT"),
    rpad(date_format(col("TERM_DT"), "yyyy-MM-dd"), 10, " ").alias("TERM_DT"),
    col("GRP_REL_ENTY_NM"),
    col("GRP_REL_ENTY_TYP_NM"),
    col("LAST_UPDT_RUN_CYC_NO")
)

write_files(
    final_df,
    f"{adls_path}/load/MBRSH_DM_GRP_REL_EDI.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)