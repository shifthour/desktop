# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 07/28/09 10:25:18 Batch  15185_37522 PROMOTE bckcetl:31540 edw10 dsadm bls for sa
# MAGIC ^1_2 07/28/09 10:22:35 Batch  15185_37357 INIT bckcett:31540 testEDW dsadm BLS FOR SA
# MAGIC ^1_2 07/27/09 10:28:00 Batch  15184_37691 PROMOTE bckcett:31540 testEDW u150906 TTR346-ClsPlnMetroRural_Sharon_testEDW                       Maddy
# MAGIC ^1_2 07/27/09 10:21:27 Batch  15184_37293 INIT bckcett:31540 devlEDW u150906 TTR346-ClsPlnMetroRural_Sharon_devlEDW                       Maddy
# MAGIC ^1_1 12/27/07 14:43:49 Batch  14606_53035 PROMOTE bckcett devlEDW dsadm dsadm
# MAGIC ^1_1 12/27/07 14:18:21 Batch  14606_51505 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 08/31/07 14:03:45 Batch  14488_50629 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_2 04/10/07 07:31:29 Batch  14345_27095 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 04/03/07 07:30:27 Batch  14338_27032 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_2 10/09/06 16:19:30 Batch  14162_58782 PROMOTE bckcetl edw10 dsadm Keith for Sharon
# MAGIC ^1_2 10/09/06 16:13:42 Batch  14162_58430 INIT bckcett testEDW10 dsadm Keith for Sharon
# MAGIC ^1_1 10/09/06 15:55:59 Batch  14162_57367 INIT bckcett testEDW10 dsadm Keith for Sharon
# MAGIC ^1_3 10/05/06 15:17:39 Batch  14158_55099 PROMOTE bckcett testEDW10 u10157 sa
# MAGIC ^1_3 10/05/06 15:07:23 Batch  14158_54445 INIT bckcett devlEDW10 u10157 sa
# MAGIC ^1_2 09/25/06 16:34:21 Batch  14148_59663 PROMOTE bckcett devlEDW10 u10157 sa
# MAGIC ^1_2 09/25/06 16:06:37 Batch  14148_57999 INIT bckcett testEDW10 u10157 sa
# MAGIC ^1_1 09/21/06 11:04:42 Batch  14144_39885 PROMOTE bckcett testEDW10 u03651 steffy
# MAGIC ^1_1 09/19/06 11:15:50 Batch  14142_40554 INIT bckcett devlEDW10 u03651 steffy
# MAGIC ^1_9 05/16/06 08:19:29 Batch  14016_29974 INIT bckcett devlEDW10 u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from ids and edw to compare and update edw
# MAGIC   Loads the data from EDW and IDS into two hash files for subsequent comparison.
# MAGIC   The comparison will determine rows that have changed and whether to generate a new history record.
# MAGIC       
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                   EDW - Read from source and dump to hf. Do nothing but trim the fields.
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Each of the stream output hash files
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Tom Harrocks 08/02/2004-   Originally Programmed
# MAGIC               Steph Goddard 02/10/2005 - EDW 1.0 updates - source from CLS_PLN_DTL instead of CLS_PLN
# MAGIC               SANdrew        11/21/2005 - EDW 4.0 updates - Added Buy UP Indicator.
# MAGIC             Sharon Andrew   07/07/2006    Balancing.   
# MAGIC                                                              Changed the value moved to the EDW Last Activity Run Cycle to be the EDW Run Cyle No from the P_RUN_CYC table and not the IDS' Last Activity Run Cycle.   
# MAGIC                                                              Renamed from EdwClsPlnExt  to IdsClsPlnExtr
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                           Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                                   --------------------------------       -------------------------------   ----------------------------       
# MAGIC 
# MAGIC SharonAndrew        2009-07-06       TTR346                Added field ClsPlnMetroRuralCdSK to ODBC call and                                                            devlEDW
# MAGIC                                                                                       Added 3 fields to EDW table  ClsPlnMetroRuralCdSK, ClsPlnMetroRuralCd and ClsPlnMetroRuralNm
# MAGIC Kalyan Neelam        2012-12-07       4963                    Added new column PCMH_IN on end                                                                                     EnterpriseNewDevl    Bhoomi Dasari            12/21/2012
# MAGIC 
# MAGIC Rama Kamjula         2013-06-24      5114                     Converted from Server to Parallel Job                                                                                     EnterpriseWrhsDevl    Peter Marshall              9/16/2013

# MAGIC Extracting Group Contract state data from GRP_REL_ENTRY  in IDS
# MAGIC GRP_CNTR_ST_D data is loaded to Dataset
# MAGIC JobName: IdsEdwGrpCntrStDExtr
# MAGIC 
# MAGIC This job extracts Group contract data from GRP_REL_ENTRY table in IDS.
# MAGIC Remove the duplicates on Natural keys for the given sort order and get Max of EFF_DT If still there are duplicates then Get the Max of TERM_DT records.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

jdbc_url_db2_CD_MPPNG, jdbc_props_db2_CD_MPPNG = get_db_config(ids_secret_name)
extract_query_db2_CD_MPPNG = f"""SELECT
CD.CD_MPPNG_SK,
COALESCE(CD.TRGT_CD,'UNK') TRGT_CD,
CD.TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG CD
"""
df_db2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CD_MPPNG)
    .options(**jdbc_props_db2_CD_MPPNG)
    .option("query", extract_query_db2_CD_MPPNG)
    .load()
)

jdbc_url_db2_Grp_Rel_Enty, jdbc_props_db2_Grp_Rel_Enty = get_db_config(ids_secret_name)
extract_query_db2_Grp_Rel_Enty = f"""SELECT 
GRP_REL_ENTY.GRP_ID, 
GRP_REL_ENTY.EFF_DT_SK, 
COALESCE(CD.TRGT_CD, 'UNK') SRC_SYS_CD, 
GRP_REL_ENTY.GRP_SK, 
GRP_REL_ENTY.GRP_REL_ENTY_TYP_CD, 
GRP_REL_ENTY.GRP_REL_ENTY_TYP_CD_SK, 
GRP_REL_ENTY.TERM_DT_SK, 
GRP_REL_ENTY.LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.GRP_REL_ENTY GRP_REL_ENTY
LEFT JOIN {IDSOwner}.CD_MPPNG CD ON GRP_REL_ENTY.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE GRP_REL_ENTY_TYP_CD IN('KS', 'MO') 
AND GRP_REL_ENTY_CAT_CD = 'OT'  
ORDER BY 
GRP_ID, 
GRP_REL_ENTY_TYP_CD, 
TERM_DT_SK,
SRC_SYS_CD_SK,
EFF_DT_SK
"""
df_db2_Grp_Rel_Enty = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_Grp_Rel_Enty)
    .options(**jdbc_props_db2_Grp_Rel_Enty)
    .option("query", extract_query_db2_Grp_Rel_Enty)
    .load()
)

df_rdp_Grp_Rel_Enty = dedup_sort(
    df_db2_Grp_Rel_Enty,
    ["GRP_ID", "EFF_DT_SK", "SRC_SYS_CD"],
    [("GRP_ID", "D"), ("EFF_DT_SK", "D"), ("SRC_SYS_CD", "D")]
)
df_rdp_Grp_Rel_Enty = df_rdp_Grp_Rel_Enty.select(
    F.col("GRP_ID"),
    F.col("EFF_DT_SK"),
    F.col("SRC_SYS_CD"),
    F.col("GRP_REL_ENTY_TYP_CD"),
    F.col("GRP_SK"),
    F.col("GRP_REL_ENTY_TYP_CD_SK"),
    F.col("TERM_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_lkp_Codes = df_rdp_Grp_Rel_Enty.alias("lnk_GrpRelEnty_Dup_In").join(
    df_db2_CD_MPPNG.alias("lnk_CdMppng_In"),
    on=(F.col("lnk_GrpRelEnty_Dup_In.GRP_REL_ENTY_TYP_CD_SK") == F.col("lnk_CdMppng_In.CD_MPPNG_SK")),
    how="left"
).select(
    F.col("lnk_GrpRelEnty_Dup_In.GRP_ID").alias("GRP_ID"),
    F.col("lnk_GrpRelEnty_Dup_In.EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("lnk_GrpRelEnty_Dup_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_GrpRelEnty_Dup_In.GRP_REL_ENTY_TYP_CD").alias("GRP_REL_ENTY_TYP_CD"),
    F.col("lnk_GrpRelEnty_Dup_In.GRP_SK").alias("GRP_SK"),
    F.col("lnk_GrpRelEnty_Dup_In.GRP_REL_ENTY_TYP_CD_SK").alias("GRP_REL_ENTY_TYP_CD_SK"),
    F.col("lnk_GrpRelEnty_Dup_In.TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("lnk_GrpRelEnty_Dup_In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_CdMppng_In.TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_xmf_BusinessLogic_1 = df_lkp_Codes.select(
    F.lit(1).alias("GRP_CNTR_ST_SK"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("EFF_DT_SK").alias("GRP_CNTR_ST_EFF_DT_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("GRP_REL_ENTY_TYP_CD").alias("GRP_CNTR_ST_CD"),
    F.when(
        F.col("TRGT_CD_NM").isNull() | (trim(F.col("TRGT_CD_NM")) == ""),
        F.lit("UNK")
    ).otherwise(F.col("TRGT_CD_NM")).alias("GRP_CNTR_ST_NM"),
    F.col("TERM_DT_SK").alias("GRP_CNTR_ST_TERM_DT_SK"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_REL_ENTY_TYP_CD_SK").alias("GRP_CNTR_ST_CD_SK")
)

df_xmf_BusinessLogic_2 = spark.createDataFrame(
    [
        (
            1,
            "NA",
            "1753-01-01",
            "NA",
            "1753-01-01",
            EDWRunCycleDate,
            1,
            "NA",
            "NA",
            "1753-01-01",
            100,
            0,
            EDWRunCycle,
            1
        )
    ],
    [
        "GRP_CNTR_ST_SK",
        "GRP_ID",
        "GRP_CNTR_ST_EFF_DT_SK",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "GRP_SK",
        "GRP_CNTR_ST_CD",
        "GRP_CNTR_ST_NM",
        "GRP_CNTR_ST_TERM_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "GRP_CNTR_ST_CD_SK"
    ]
)

df_xmf_BusinessLogic_3 = spark.createDataFrame(
    [
        (
            0,
            "UNK",
            "1753-01-01",
            "UNK",
            "1753-01-01",
            EDWRunCycleDate,
            0,
            "UNK",
            "UNK",
            "1753-01-01",
            100,
            0,
            EDWRunCycle,
            0
        )
    ],
    [
        "GRP_CNTR_ST_SK",
        "GRP_ID",
        "GRP_CNTR_ST_EFF_DT_SK",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "GRP_SK",
        "GRP_CNTR_ST_CD",
        "GRP_CNTR_ST_NM",
        "GRP_CNTR_ST_TERM_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "GRP_CNTR_ST_CD_SK"
    ]
)

df_fnl_Data = df_xmf_BusinessLogic_1.unionByName(df_xmf_BusinessLogic_2).unionByName(df_xmf_BusinessLogic_3)

df_fnl_Data_final = df_fnl_Data.select(
    F.col("GRP_CNTR_ST_SK"),
    F.col("GRP_ID"),
    F.rpad(F.col("GRP_CNTR_ST_EFF_DT_SK"), 10, " ").alias("GRP_CNTR_ST_EFF_DT_SK"),
    F.col("SRC_SYS_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("GRP_SK"),
    F.col("GRP_CNTR_ST_CD"),
    F.col("GRP_CNTR_ST_NM"),
    F.rpad(F.col("GRP_CNTR_ST_TERM_DT_SK"), 10, " ").alias("GRP_CNTR_ST_TERM_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_CNTR_ST_CD_SK")
)

write_files(
    df_fnl_Data_final,
    f"{adls_path}/ds/GRP_CNTR_ST_D.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)