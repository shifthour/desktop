# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwNtwkDimExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from ids and edw to compare and update edw
# MAGIC   Loads the data from EDW and IDS into two hash files for subsequent comparison.
# MAGIC   The comparison will determine rows that have changed and whether to generate a new history record.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS:  NTWK
# MAGIC                 EDW:  NTWK_D
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma_codes         - Load the CDMA codes to get cd a nd cd_nm from cd_sk
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                   EDW - Read from source and dump to hf. Do nothing but trim the fields.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Each of the stream output hash files
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Tom Harrocks 08/02/2004-   Originally Programmed
# MAGIC               Brent Leland   07/26/2005    Removed compare process and changed output to sequential load file.
# MAGIC               Oliver Nielsen  09/07/2005   Convert to run with Sequencer
# MAGIC               Oliver Nielsen  12/15/2005   Added 4 columns to be populated by Blue Access
# MAGIC               Ralph Tucker  08/14/2006   Added uws tables and logic to populate NTWK_DIR_CD & NTWK_DIR_NM; previously UNK.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                       
# MAGIC DEVELOPER                         DATE                 PROJECT                                                  DESCRIPTION                                        DATASTAGE   ENVIRONMENT                     CODE  REVIEW            REVIEW DATE                    
# MAGIC -----------------------------                 ----------------------      ------------------------------                                  -----------------------------------                               ------------------------------                                          ------------------------------      --------------------
# MAGIC Nagesh Bandi                         06/24/2013        5114-Enterprise Efficiencies                 Move ids to edw for NTWK D                       EnterpriseWrhseDevl                                      Bhoomi Dasari             9/3/2013

# MAGIC Filter on SRC_DOMAIN_NM =   'NETWORK DIRECTORY' AND SRC_CLCTN_CD = 'UNIFIEDWAREHOUSESPPT'
# MAGIC This is Source extract data from an IDS table and apply Code denormalization, create a load ready file.
# MAGIC 
# MAGIC Job Name:
# MAGIC IdsEdwNtwkDExtr
# MAGIC 
# MAGIC Table:
# MAGIC NTWK_D
# MAGIC Read from source table NTWK from IDS.
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write NTWK_D Data into a Sequential file for Load Job.
# MAGIC Add Defaults and Null Handling
# MAGIC ID lookups to pull CD
# MAGIC 
# MAGIC Lookup Keys: 
# MAGIC NTWK_ID
# MAGIC Code lookups for Dir Name
# MAGIC 
# MAGIC Lookup Keys: 
# MAGIC NTWK_DIR_CD
# MAGIC NTWK_TYP_CD_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import rpad, when, col, lit
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
UWSOwner = get_widget_value('UWSOwner', '')
uws_secret_name = get_widget_value('uws_secret_name', '')
EDWRunCycle = get_widget_value('EDWRunCycle', '')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate', '')

# db2_NTWK_Extr
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_db2_NTWK_Extr = """SELECT
NTWK.NTWK_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
NTWK.NTWK_ID,
NTWK.NTWK_TYP_CD_SK,
NTWK.NTWK_NM,
NTWK.NTWK_SH_NM
FROM {0}.NTWK NTWK
LEFT JOIN {0}.CD_MPPNG CD
ON NTWK.SRC_SYS_CD_SK = CD.CD_MPPNG_SK;""".format(IDSOwner)
df_db2_NTWK_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_NTWK_Extr)
    .load()
)

# ODBC_UWS_NTWK_Extr
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)
extract_query_ODBC_UWS_NTWK_Extr = """SELECT
NTWK_ID,
NTWK_DIR_CD,
USER_ID,
LAST_UPDT_DT_SK
FROM {0}.NTWK;""".format(UWSOwner)
df_ODBC_UWS_NTWK_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("query", extract_query_ODBC_UWS_NTWK_Extr)
    .load()
)

# lkp_Ntwk_Dir_Cd (left join with no join condition)
df_lkp_Ntwk_Dir_Cd = (
    df_db2_NTWK_Extr.alias("lnk_IdsEdwNtwkDExt_inABC")
    .crossJoin(df_ODBC_UWS_NTWK_Extr.alias("Ntwk_Dir_CdLkup"))
    .select(
        F.col("lnk_IdsEdwNtwkDExt_inABC.NTWK_SK").alias("NTWK_SK"),
        F.col("lnk_IdsEdwNtwkDExt_inABC.NTWK_ID").alias("NTWK_ID"),
        F.col("lnk_IdsEdwNtwkDExt_inABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwNtwkDExt_inABC.NTWK_NM").alias("NTWK_NM"),
        F.col("lnk_IdsEdwNtwkDExt_inABC.NTWK_TYP_CD_SK").alias("NTWK_TYP_CD_SK"),
        F.col("lnk_IdsEdwNtwkDExt_inABC.NTWK_SH_NM").alias("NTWK_SH_NM"),
        F.col("Ntwk_Dir_CdLkup.NTWK_DIR_CD").alias("NTWK_DIR_CD"),
        F.col("Ntwk_Dir_CdLkup.USER_ID").alias("USER_ID"),
        F.col("Ntwk_Dir_CdLkup.LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK"),
    )
)

# db2_CD_MPPNG_Extr
extract_query_db2_CD_MPPNG_Extr = """SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM,
SRC_DOMAIN_NM,
SRC_CLCTN_CD
FROM {0}.CD_MPPNG""".format(IDSOwner)
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

# flt_Src_Dom_Nm outputs
df_flt_Src_Dom_Nm_0 = df_db2_CD_MPPNG_Extr.filter("CD_MPPNG_SK IS NOT NULL")  # NtwkTypCd_CdLkup
df_flt_Src_Dom_Nm_1 = df_db2_CD_MPPNG_Extr.filter(
    "SRC_DOMAIN_NM = 'NETWORK DIRECTORY' AND SRC_CLCTN_CD = 'UNIFIEDWAREHOUSESPPT'"
)  # Ntwk_Dir_NmLkup

# lkp_Ntwk_Dir_Nm
df_lkp_Ntwk_Dir_Nm = (
    df_lkp_Ntwk_Dir_Cd.alias("lnk_Ntwk_Dir_CdLkpData_out")
    .join(
        df_flt_Src_Dom_Nm_1.alias("Ntwk_Dir_NmLkup"),
        F.col("lnk_Ntwk_Dir_CdLkpData_out.NTWK_DIR_CD") == F.col("Ntwk_Dir_NmLkup.TRGT_CD"),
        how="left",
    )
    .join(
        df_flt_Src_Dom_Nm_0.alias("NtwkTypCd_CdLkup"),
        F.col("lnk_Ntwk_Dir_CdLkpData_out.NTWK_TYP_CD_SK") == F.col("NtwkTypCd_CdLkup.CD_MPPNG_SK"),
        how="left",
    )
    .select(
        F.col("lnk_Ntwk_Dir_CdLkpData_out.NTWK_SK").alias("NTWK_SK"),
        F.col("lnk_Ntwk_Dir_CdLkpData_out.NTWK_ID").alias("NTWK_ID"),
        F.col("lnk_Ntwk_Dir_CdLkpData_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_Ntwk_Dir_CdLkpData_out.NTWK_NM").alias("NTWK_NM"),
        F.col("lnk_Ntwk_Dir_CdLkpData_out.NTWK_TYP_CD_SK").alias("NTWK_TYP_CD_SK"),
        F.col("lnk_Ntwk_Dir_CdLkpData_out.NTWK_SH_NM").alias("NTWK_SH_NM"),
        F.col("lnk_Ntwk_Dir_CdLkpData_out.NTWK_DIR_CD").alias("NTWK_DIR_CD"),
        F.col("lnk_Ntwk_Dir_CdLkpData_out.USER_ID").alias("USER_ID"),
        F.col("lnk_Ntwk_Dir_CdLkpData_out.LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK"),
        F.col("Ntwk_Dir_NmLkup.TRGT_CD_NM").alias("NTWK_DIR_NM"),
        F.col("NtwkTypCd_CdLkup.TRGT_CD").alias("NTWK_TYP_CD"),
        F.col("NtwkTypCd_CdLkup.TRGT_CD_NM").alias("NTWK_TYP_NM"),
    )
)

# xfm_BusinessLogic
df_xfm_BusinessLogic = (
    df_lkp_Ntwk_Dir_Nm
    .withColumn("NTWK_ID", trim(col("NTWK_ID")))
    .withColumn("NTWK_DIR_CD",
        when(
            F.trim(
                when(col("NTWK_DIR_CD").isNotNull(), col("NTWK_DIR_CD")).otherwise(" ")
            ) == "",
            " "
        ).otherwise(col("NTWK_DIR_CD"))
    )
    .withColumn("NTWK_DIR_NM",
        when(
            F.trim(
                when(col("NTWK_DIR_NM").isNotNull(), col("NTWK_DIR_NM")).otherwise(" "))
            == "",
            " "
        ).otherwise(col("NTWK_DIR_NM"))
    )
    .withColumn("NTWK_NM", trim(col("NTWK_NM")))
    .withColumn("NTWK_TYP_CD",
        when(
            F.trim(
                when(col("NTWK_TYP_CD").isNotNull(), col("NTWK_TYP_CD")).otherwise(" "))
            == "",
            "UNK"
        ).otherwise(col("NTWK_TYP_CD"))
    )
    .withColumn("NTWK_TYP_NM",
        when(
            F.trim(
                when(col("NTWK_TYP_NM").isNotNull(), col("NTWK_TYP_NM")).otherwise(" "))
            == "",
            "UNK"
        ).otherwise(col("NTWK_TYP_NM"))
    )
    .withColumn("USER_ID",
        when(
            F.trim(col("USER_ID")) == "",
            "UNK"
        ).otherwise(col("USER_ID"))
    )
    .withColumn("LAST_UPDT_DT_SK",
        when(
            F.length(F.trim(
                when(col("LAST_UPDT_DT_SK").isNotNull(), col("LAST_UPDT_DT_SK")).otherwise(" ")
            )) == 0,
            "UNK"
        ).otherwise(col("LAST_UPDT_DT_SK"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
    .withColumn("NTWK_TYP_CD_SK", trim(col("NTWK_TYP_CD_SK")))
)

# Apply rpad for char(10) columns
df_xfm_BusinessLogic = (
    df_xfm_BusinessLogic
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_DT_SK", rpad(col("LAST_UPDT_DT_SK"), 10, " "))
)

# seq_NTWK_D_Load
df_final = df_xfm_BusinessLogic.select(
    "NTWK_SK",
    "SRC_SYS_CD",
    "NTWK_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "NTWK_DIR_CD",
    "NTWK_DIR_NM",
    "NTWK_NM",
    "NTWK_SH_NM",
    "NTWK_TYP_CD",
    "NTWK_TYP_NM",
    "USER_ID",
    "LAST_UPDT_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "NTWK_TYP_CD_SK"
)

write_files(
    df_final,
    f"{adls_path}/load/NTWK_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)