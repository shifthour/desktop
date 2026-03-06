# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2006, 2009 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     IdsMbrshDmExtrSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     *
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restart, no other steps necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                                            Code                   Date
# MAGIC Developer           Date              Altiris #                    Change Description                                                                                            Reviewer            Reviewed
# MAGIC -------------------------  -------------------   -------------   -           ----------------------------------------------------------------------------------------------------------------          -------------------------  -------------------
# MAGIC Tao Luo              02/22/2006                              Original Programming
# MAGIC Hugh Sisson       06/12/2009   3500                      Added 2 columns for prov phone number and extension                                    Steph Goddard   07/01/2009
# MAGIC Bhoomi D           07/08/2009    3500                     Changed lokup for Prov_id's and Prov_nm's                                                       Steph Goddard  07/09/2009
# MAGIC SAndrew            2009-10-01     4113 Mbr 360    Took out build of hash file from here and put into                                                  Steph Goddard   10/17/2009
# MAGIC                                                                              driver / IdsMbrDataMartPrereq    
# MAGIC 
# MAGIC Nagesh Bandi      2013-08-05     #5114               Original Programming(Server to Parallel)                                                              Peter Marshall      10/23/2013

# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC MBR_PCP_TYP_CD_SK
# MAGIC MBR_PCP_TERM_RSN_CD_SK
# MAGIC MBR_SK
# MAGIC PROV_SK
# MAGIC TERM_DT_SK
# MAGIC Write PROV Data into a Sequential file for Load Job IdsDmMbrshDmMbrPriCareProvLoad.
# MAGIC Read all the Data from IDS MBR_PCPTable,Apply Run Cycle filters to get just the needed rows forward.
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: IdsDmMbrshDmMbrPriCarExtr
# MAGIC Code SK Join for Denormalization
# MAGIC 
# MAGIC Join Keys:
# MAGIC MBR_SK
# MAGIC Used seprate join as MBR extract has more than 3 million records.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
CurrRunDt = get_widget_value('CurrRunDt','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = F"""
SELECT DISTINCT 
MBR.MBR_SK,
GRP.GRP_ID,
GRP.GRP_UNIQ_KEY,
GRP.GRP_NM,
'Y' AS GRP_IND
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.GRP GRP
WHERE MBR.SUB_SK = SUB.SUB_SK
  AND SUB.GRP_SK = GRP.GRP_SK
"""
df_db2_GRP_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = F"""
SELECT 
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
MBR_PCP.MBR_UNIQ_KEY,
MBR_PCP.MBR_PCP_TYP_CD_SK,
MBR_PCP.EFF_DT_SK,
MBR_PCP.MBR_SK,
MBR_PCP.PROV_SK,
MBR_PCP.MBR_PCP_TERM_RSN_CD_SK,
MBR_PCP.TERM_DT_SK
FROM {IDSOwner}.MBR_PCP MBR_PCP
LEFT JOIN {IDSOwner}.CD_MPPNG CD
  ON MBR_PCP.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE MBR_PCP.MBR_PCP_SK NOT IN (0,1)
--AND MBR_PCP.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
"""
df_db2_MBR_PCP_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = F"""
SELECT DISTINCT
PROV.PROV_SK,
PROV_ADDR.PRCTC_LOC_IN,
PROV_ADDR.PHN_NO,
PROV_ADDR.PHN_NO_EXT,
PROV_LOC.PRI_ADDR_IN
FROM {IDSOwner}.PROV PROV,
     {IDSOwner}.PROV_LOC PROV_LOC,
     {IDSOwner}.PROV_ADDR PROV_ADDR
WHERE PROV.PROV_SK = PROV_LOC.PROV_SK
  AND PROV.PROV_ADDR_ID = PROV_LOC.PROV_ADDR_ID
  AND PROV_LOC.PROV_ADDR_SK = PROV_ADDR.PROV_ADDR_SK
  AND PROV_LOC.PROV_ADDR_TYP_CD_SK = PROV_ADDR.PROV_ADDR_TYP_CD_SK
  AND PROV_ADDR.PRCTC_LOC_IN = 'Y'
  AND PROV_LOC.PRI_ADDR_IN = 'Y'
  AND PROV_ADDR.TERM_DT_SK >= '{CurrRunDt}'
  AND PROV_LOC.PROV_ADDR_EFF_DT_SK = PROV_ADDR.PROV_ADDR_EFF_DT_SK
"""
df_db2_PROV_ADDR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = F"""
SELECT DISTINCT
PROV.PROV_SK,
PROV.PROV_ID,
PROV.PROV_NM
FROM {IDSOwner}.PROV PROV,
     {IDSOwner}.MBR_PCP MBR_PCP
WHERE PROV.SRC_SYS_CD_SK = MBR_PCP.SRC_SYS_CD_SK
  AND PROV.PROV_SK = MBR_PCP.PROV_SK
"""
df_db2_PROV_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = F"""
SELECT DISTINCT
CLNDR_DT_SK,
CLNDR_DT
FROM {IDSOwner}.CLNDR_DT
"""
df_db2_CLNDR_DT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = F"""
SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_cpy_cd_mppng = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM")
)

df_lkp_Codes = (
    df_db2_MBR_PCP_Extr.alias("lnk_IdsDmMbrshDmMbrPriCaExtr_InABC")
    .join(
        df_cpy_cd_mppng.alias("Mbr_Pcp_Typ_Cd_lkp"),
        F.col("lnk_IdsDmMbrshDmMbrPriCaExtr_InABC.MBR_PCP_TYP_CD_SK") == F.col("Mbr_Pcp_Typ_Cd_lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Mbr_Pcp_Term_Rsn_Cd_lkp"),
        F.col("lnk_IdsDmMbrshDmMbrPriCaExtr_InABC.MBR_PCP_TERM_RSN_CD_SK") == F.col("Mbr_Pcp_Term_Rsn_Cd_lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_db2_PROV_ADDR_in.alias("Phn_No_lkp"),
        F.col("lnk_IdsDmMbrshDmMbrPriCaExtr_InABC.PROV_SK") == F.col("Phn_No_lkp.PROV_SK"),
        "left"
    )
    .join(
        df_db2_PROV_in.alias("Prov_Nm_lkp"),
        F.col("lnk_IdsDmMbrshDmMbrPriCaExtr_InABC.PROV_SK") == F.col("Prov_Nm_lkp.PROV_SK"),
        "left"
    )
    .join(
        df_db2_CLNDR_DT_in.alias("MBR_PCP_EFF_DT_lkp"),
        F.col("lnk_IdsDmMbrshDmMbrPriCaExtr_InABC.EFF_DT_SK") == F.col("MBR_PCP_EFF_DT_lkp.CLNDR_DT_SK"),
        "left"
    )
)

df_lkp_Codes_out = df_lkp_Codes.select(
    F.col("lnk_IdsDmMbrshDmMbrPriCaExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsDmMbrshDmMbrPriCaExtr_InABC.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lnk_IdsDmMbrshDmMbrPriCaExtr_InABC.MBR_PCP_TYP_CD_SK").alias("MBR_PCP_TYP_CD_SK"),
    F.col("MBR_PCP_EFF_DT_lkp.CLNDR_DT").alias("EFF_DT_SK"),
    F.col("lnk_IdsDmMbrshDmMbrPriCaExtr_InABC.MBR_SK").alias("MBR_SK"),
    F.col("lnk_IdsDmMbrshDmMbrPriCaExtr_InABC.PROV_SK").alias("PROV_SK"),
    F.col("lnk_IdsDmMbrshDmMbrPriCaExtr_InABC.MBR_PCP_TERM_RSN_CD_SK").alias("MBR_PCP_TERM_RSN_CD_SK"),
    F.col("lnk_IdsDmMbrshDmMbrPriCaExtr_InABC.TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("Mbr_Pcp_Typ_Cd_lkp.TRGT_CD").alias("MBR_PCP_TYP_CD"),
    F.col("Mbr_Pcp_Typ_Cd_lkp.TRGT_CD_NM").alias("MBR_PCP_TYP_NM"),
    F.col("Mbr_Pcp_Term_Rsn_Cd_lkp.TRGT_CD").alias("MBR_PCP_TERM_RSN_CD"),
    F.col("Mbr_Pcp_Term_Rsn_Cd_lkp.TRGT_CD_NM").alias("MBR_PCP_TERM_RSN_NM"),
    F.col("Prov_Nm_lkp.PROV_ID").alias("PROV_ID"),
    F.col("Prov_Nm_lkp.PROV_NM").alias("PROV_NM"),
    F.col("Phn_No_lkp.PRCTC_LOC_IN").alias("PRCTC_LOC_IN"),
    F.col("Phn_No_lkp.PHN_NO").alias("PHN_NO"),
    F.col("Phn_No_lkp.PHN_NO_EXT").alias("PHN_NO_EXT"),
    F.col("Phn_No_lkp.PRI_ADDR_IN").alias("PRI_ADDR_IN")
)

df_Jn_Mbr_Sk = df_lkp_Codes_out.alias("lnk_Codes_Out").join(
    df_db2_GRP_Extr.alias("GRP_Nm_lkp"),
    F.col("lnk_Codes_Out.MBR_SK") == F.col("GRP_Nm_lkp.MBR_SK"),
    "inner"
)

df_Jn_Mbr_Sk_out = df_Jn_Mbr_Sk.select(
    F.col("lnk_Codes_Out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_Codes_Out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lnk_Codes_Out.MBR_PCP_TYP_CD_SK").alias("MBR_PCP_TYP_CD_SK"),
    F.col("lnk_Codes_Out.EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("lnk_Codes_Out.MBR_SK").alias("MBR_SK"),
    F.col("lnk_Codes_Out.PROV_SK").alias("PROV_SK"),
    F.col("lnk_Codes_Out.MBR_PCP_TERM_RSN_CD_SK").alias("MBR_PCP_TERM_RSN_CD_SK"),
    F.col("lnk_Codes_Out.TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("lnk_Codes_Out.MBR_PCP_TYP_CD").alias("MBR_PCP_TYP_CD"),
    F.col("lnk_Codes_Out.MBR_PCP_TYP_NM").alias("MBR_PCP_TYP_NM"),
    F.col("lnk_Codes_Out.MBR_PCP_TERM_RSN_CD").alias("MBR_PCP_TERM_RSN_CD"),
    F.col("lnk_Codes_Out.MBR_PCP_TERM_RSN_NM").alias("MBR_PCP_TERM_RSN_NM"),
    F.col("lnk_Codes_Out.PROV_ID").alias("PROV_ID"),
    F.col("lnk_Codes_Out.PROV_NM").alias("PROV_NM"),
    F.col("lnk_Codes_Out.PRCTC_LOC_IN").alias("PRCTC_LOC_IN"),
    F.col("lnk_Codes_Out.PHN_NO").alias("PHN_NO"),
    F.col("lnk_Codes_Out.PHN_NO_EXT").alias("PHN_NO_EXT"),
    F.col("lnk_Codes_Out.PRI_ADDR_IN").alias("PRI_ADDR_IN"),
    F.col("GRP_Nm_lkp.GRP_ID").alias("GRP_ID"),
    F.col("GRP_Nm_lkp.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.col("GRP_Nm_lkp.GRP_NM").alias("GRP_NM"),
    F.col("GRP_Nm_lkp.GRP_IND").alias("GRP_IND")
)

df_xfrm_temp = (
    df_Jn_Mbr_Sk_out
    .withColumn("svFormatPhoneNo", F.regexp_replace(F.col("PHN_NO"), "-", ""))
    .withColumn("svFilter", F.when(F.col("GRP_IND").isNull(), F.lit(0)).otherwise(F.lit(1)))
    .withColumn(
        "svLocIn",
        F.when(F.trim(F.col("PRCTC_LOC_IN")) == "", 1)
         .otherwise(F.when(F.col("PRCTC_LOC_IN").isin("X", "Y", "U"), 1).otherwise(0))
    )
    .withColumn(
        "svAddrIn",
        F.when(F.trim(F.col("PRI_ADDR_IN")) == "", 1)
         .otherwise(F.when(F.col("PRI_ADDR_IN").isin("X", "Y", "U"), 1).otherwise(0))
    )
    .withColumn("svCheck", F.col("svLocIn") + F.col("svAddrIn") + F.col("svFilter"))
)

df_xfrm_BusinessLogic = df_xfrm_temp.filter(F.col("svCheck") == 3).select(
    F.when(F.trim(F.col("SRC_SYS_CD")) == "", F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.when(F.trim(F.col("MBR_UNIQ_KEY")) == "", F.lit(0)).otherwise(F.col("MBR_UNIQ_KEY")).alias("MBR_UNIQ_KEY"),
    F.when(F.trim(F.col("MBR_PCP_TYP_CD")) == "", F.lit("UNK")).otherwise(F.col("MBR_PCP_TYP_CD")).alias("MBR_PCP_TYP_CD"),
    F.to_timestamp(
        F.concat(F.date_format(F.col("EFF_DT_SK"), "yyyy-MM-dd"), F.lit(" 00:00:00.000")),
        "yyyy-MM-dd HH:mm:ss.SSS"
    ).alias("MBR_PCP_EFF_DT"),
    F.when(F.trim(F.col("MBR_PCP_TERM_RSN_CD")) == "", F.lit("UNK")).otherwise(F.col("MBR_PCP_TERM_RSN_CD")).alias("MBR_PCP_TERM_RSN_CD"),
    F.when(F.trim(F.col("MBR_PCP_TERM_RSN_NM")) == "", F.lit("UNK")).otherwise(F.col("MBR_PCP_TERM_RSN_NM")).alias("MBR_PCP_TERM_RSN_NM"),
    F.when(F.trim(F.col("MBR_PCP_TYP_NM")) == "", F.lit("UNK")).otherwise(F.col("MBR_PCP_TYP_NM")).alias("MBR_PCP_TYP_NM"),
    F.to_timestamp(
        F.concat(F.col("TERM_DT_SK"), F.lit(" 00:00:00.000")),
        "yyyy-MM-dd HH:mm:ss.SSS"
    ).alias("MBR_PCP_TERM_DT"),
    F.when(F.trim(F.col("GRP_ID")) == "", F.lit("UNK")).otherwise(F.col("GRP_ID")).alias("GRP_ID"),
    F.col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.when(F.col("GRP_NM").isNull(), F.lit("UNK"))
     .otherwise(F.when(F.trim(F.col("GRP_NM")) == "", F.lit("UNK")).otherwise(F.col("GRP_NM"))).alias("GRP_NM"),
    F.when(F.trim(F.col("PROV_ID")) == "", F.lit(" ")).otherwise(F.col("PROV_ID")).alias("PROV_ID"),
    F.when(F.col("PROV_NM").isNull(), F.lit(" "))
     .otherwise(F.when(F.trim(F.col("PROV_NM")) == "", F.lit(" ")).otherwise(F.col("PROV_NM"))).alias("PROV_NM"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_NO"),
    F.when(
        F.length(
            F.trim(
                F.when(F.col("svFormatPhoneNo").isNotNull(), F.col("svFormatPhoneNo")).otherwise(F.lit(" "))
            )
        ) == 0,
        F.lit(" ")
    ).otherwise(
        F.when(F.trim(F.col("svFormatPhoneNo")) == "", F.lit(" ")).otherwise(F.col("svFormatPhoneNo"))
    ).alias("PROV_PRI_PRCTC_ADDR_PHN_NO"),
    F.when(
        F.length(
            F.trim(
                F.when(F.col("PHN_NO_EXT").isNotNull(), F.col("PHN_NO_EXT")).otherwise(F.lit(" "))
            )
        ) == 0,
        F.lit(" ")
    ).otherwise(
        F.when(F.trim(F.col("PHN_NO_EXT")) == "", F.lit(" ")).otherwise(F.col("PHN_NO_EXT"))
    ).alias("PROV_PRI_PRCTC_ADDR_PHN_NO_EXT")
)

write_files(
    df_xfrm_BusinessLogic,
    f"{adls_path}/load/MBRSH_DM_MBR_PRI_CARE_PROV.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)