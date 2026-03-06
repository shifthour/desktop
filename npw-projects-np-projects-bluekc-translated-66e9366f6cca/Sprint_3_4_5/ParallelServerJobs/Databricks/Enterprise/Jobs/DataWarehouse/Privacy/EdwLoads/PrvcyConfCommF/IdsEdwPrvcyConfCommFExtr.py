# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                             Date                  Project/Altiris #               Change Description                             Development Project     Code Reviewer            Date Reviewed
# MAGIC ----------------------------------        -------------------      -----------------------------------    ---------------------------------------------------------      ----------------------------------    ---------------------------------    -------------------------   
# MAGIC Parikshith Chada                 2/21/2007                                                   Originally Programmed                        devlEDW10                  Steph Goddard       
# MAGIC Steph Goddard                  7/15/10            TTR-630                         changed field name from                        EnterpriseNewDevl        Sandrew                      2010-10-01
# MAGIC                                                                                                            PRVCY_EXTRNL_ENTY_PHN_TYP_CD_S to PRVCY_EXTL_ENTY_PHN_TYP_CD_SK      
# MAGIC 
# MAGIC Bhoomi Dasari                  2/14/2013             TTR-1534                  Updated three Address fields to               EnterpriseNewDevl        Kalyan Neelam             2013-03-01
# MAGIC                                                                                                          from length of 40 to 80.
# MAGIC 
# MAGIC 
# MAGIC DEVELOPER                         DATE                 PROJECT                                                DESCRIPTION                                            DATASTAGE                                            CODE                            REVIEW
# MAGIC                                                                                                                                                                                                               ENVIRONMENT                                       REVIEWER                   DATE                                              
# MAGIC ------------------------------------------   ----------------------      ------------------------------------------------                ---------------------------------------------------------------     ---------------------------------------------------              ------------------------------          --------------------
# MAGIC Balkarn                                10/08/2013                          5114                                       Original Programming                                   EnterpriseWrhsDevl                                  Peter Marshall                 1/7/2014
# MAGIC                                                                                                                                           (Server to Parallel Conversion)

# MAGIC Job Name:
# MAGIC IdsEdwPrvcyConfCommFExtr
# MAGIC Read from source table PRVCY_CONF_COMM
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC 1) PROC_CD_SK
# MAGIC 2) FCLTY_CLM_PROC_ORDNL_CD_SK
# MAGIC Add Defaults and Null Handling.
# MAGIC Write PRVCY_CONF_COMM_F Data into a Sequential file for Load Job IdsEdwPrvcyConfCommFLoad
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
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
EDWOwner = get_widget_value("EDWOwner","")
edw_secret_name = get_widget_value("edw_secret_name","")
CurrRunCycleDate = get_widget_value("CurrRunCycleDate","")
CurrRunCycle = get_widget_value("CurrRunCycle","")

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

df_db2_PRVCY_CONF_COMM_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT COMM.PRVCY_CONF_COMM_SK, COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD, COMM.PRVCY_MBR_UNIQ_KEY, COMM.SEQ_NO, COMM.PRVCY_MBR_SRC_CD_SK, COMM.PRVCY_CONF_COMM_TYP_CD_SK, COMM.CRT_RUN_CYC_EXCTN_SK, COMM.LAST_UPDT_RUN_CYC_EXCTN_SK, COMM.MBR_SK, COMM.PRVCY_EXTRNL_MBR_SK, COMM.PRVCY_CONF_COMM_RQST_RSN_CD_SK, COMM.PRVCY_CONF_COMM_STTUS_CD_SK, COMM.EFF_DT_SK, COMM.RCVD_DT_SK, COMM.TERM_DT_SK, COMM.ADDR_SEQ_NO, COMM.ADDREE_FIRST_NM, COMM.ADDREE_LAST_NM, COMM.CONF_COMM_DESC, COMM.EMAIL_ADDR_TX "
        f"FROM {IDSOwner}.PRVCY_CONF_COMM COMM "
        f"LEFT JOIN {IDSOwner}.CD_MPPNG CD ON COMM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK"
    )
    .load()
)

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT CD_MPPNG_SK, COALESCE(TRGT_CD,'UNK') TRGT_CD, COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM FROM {IDSOwner}.CD_MPPNG"
    )
    .load()
)

df_cpy = df_db2_CD_MPPNG_Extr

df_db2_PRVCY_EXTRNL_MBR_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option(
        "query",
        f"SELECT EXTRNL_MBR.PRVCY_EXTRNL_MBR_SK, EXTRNL_MBR.PRVCY_EXTRNL_MBR_ID FROM {EDWOwner}.PRVCY_EXTRNL_MBR_D EXTRNL_MBR"
    )
    .load()
)

df_db2_PRVCY_EXTRNL_ENTY_ADDR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT ADDR.PRVCY_EXTRNL_ENTY_UNIQ_KEY, ADDR.PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK, ADDR.SEQ_NO, ADDR.ADDR_LN_1, ADDR.ADDR_LN_2, ADDR.ADDR_LN_3, ADDR.CITY_NM, ADDR.PRVCY_EXTL_ENTY_ADDR_ST_CD_SK, ADDR.POSTAL_CD, ADDR.CNTY_NM "
        f"FROM {IDSOwner}.PRVCY_EXTRNL_ENTY_ADDR ADDR, {IDSOwner}.CD_MPPNG MPPNG "
        "WHERE ADDR.PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK=MPPNG.CD_MPPNG_SK AND MPPNG.TRGT_CD='CONFCOMMADDR'"
    )
    .load()
)

df_db2_PRVCY_EXTRNL_ENTY_PHN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT PHN.PRVCY_EXTRNL_ENTY_UNIQ_KEY, PHN.SEQ_NO, PHN.PHN_NO "
        f"FROM {IDSOwner}.PRVCY_EXTRNL_ENTY_PHN PHN, {IDSOwner}.CD_MPPNG MPPNG "
        "WHERE PHN.PRVCY_EXTL_ENTY_PHN_TYP_CD_SK = MPPNG.CD_MPPNG_SK AND MPPNG.TRGT_CD='CONFCOMMPHN'"
    )
    .load()
)

df_db2_MBR_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option(
        "query",
        f"SELECT MBR_D.MBR_SK, MBR_D.MBR_ID FROM {EDWOwner}.MBR_D MBR_D"
    )
    .load()
)

df_lkp_ProcCD = (
    df_db2_PRVCY_CONF_COMM_in.alias("CO")
    .join(
        df_db2_PRVCY_EXTRNL_ENTY_ADDR.alias("AE"),
        [
            F.col("CO.PRVCY_MBR_UNIQ_KEY") == F.col("AE.PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
            F.col("CO.ADDR_SEQ_NO") == F.col("AE.SEQ_NO")
        ],
        "left"
    )
    .join(
        df_db2_PRVCY_EXTRNL_ENTY_PHN.alias("PH"),
        [
            F.col("CO.PRVCY_MBR_UNIQ_KEY") == F.col("PH.PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
            F.col("CO.ADDR_SEQ_NO") == F.col("PH.SEQ_NO")
        ],
        "left"
    )
    .join(
        df_db2_PRVCY_EXTRNL_MBR_D.alias("EM"),
        F.col("CO.PRVCY_EXTRNL_MBR_SK") == F.col("EM.PRVCY_EXTRNL_MBR_SK"),
        "left"
    )
    .join(
        df_db2_MBR_D.alias("MB"),
        F.col("CO.MBR_SK") == F.col("MB.MBR_SK"),
        "left"
    )
    .select(
        F.col("CO.PRVCY_CONF_COMM_SK").alias("PRVCY_CONF_COMM_SK"),
        F.col("CO.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CO.PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
        F.col("CO.SEQ_NO").alias("SEQ_NO"),
        F.col("CO.PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
        F.col("CO.MBR_SK").alias("MBR_SK"),
        F.col("CO.ADDR_SEQ_NO").alias("ADDR_SEQ_NO"),
        F.col("CO.ADDREE_FIRST_NM").alias("ADDREE_FIRST_NM"),
        F.col("CO.ADDREE_LAST_NM").alias("ADDREE_LAST_NM"),
        F.col("CO.CONF_COMM_DESC").alias("CONF_COMM_DESC"),
        F.col("CO.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("CO.EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
        F.col("CO.RCVD_DT_SK").alias("RCVD_DT_SK"),
        F.col("CO.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("AE.ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("AE.ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("AE.ADDR_LN_3").alias("ADDR_LN_3"),
        F.col("AE.CITY_NM").alias("CITY_NM"),
        F.col("AE.PRVCY_EXTL_ENTY_ADDR_ST_CD_SK").alias("PRVCY_EXTL_ENTY_ADDR_ST_CD_SK"),
        F.col("AE.POSTAL_CD").alias("POSTAL_CD"),
        F.col("AE.CNTY_NM").alias("CNTY_NM"),
        F.col("PH.PHN_NO").alias("PHN_NO"),
        F.col("CO.PRVCY_CONF_COMM_RQST_RSN_CD_SK").alias("PRVCY_CONF_COMM_RQST_RSN_CD_SK"),
        F.col("CO.PRVCY_CONF_COMM_STTUS_CD_SK").alias("PRVCY_CONF_COMM_STTUS_CD_SK"),
        F.col("CO.PRVCY_CONF_COMM_TYP_CD_SK").alias("PRVCY_CONF_COMM_TYP_CD_SK"),
        F.col("CO.PRVCY_MBR_SRC_CD_SK").alias("PRVCY_MBR_SRC_CD_SK"),
        F.col("AE.PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK").alias("PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK"),
        F.col("EM.PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK_1"),
        F.col("EM.PRVCY_EXTRNL_MBR_ID").alias("PRVCY_EXTRNL_MBR_ID"),
        F.col("MB.MBR_ID").alias("MBR_ID")
    )
)

st = df_cpy.alias("st")
rr = df_cpy.alias("rr")
et = df_cpy.alias("et")
ps = df_cpy.alias("ps")
ct = df_cpy.alias("ct")
sc = df_cpy.alias("sc")

df_lkp_Codes = (
    df_lkp_ProcCD.alias("lk")
    .join(
        st,
        F.col("lk.PRVCY_EXTL_ENTY_ADDR_ST_CD_SK") == F.col("st.CD_MPPNG_SK"),
        "left"
    )
    .join(
        rr,
        F.col("lk.PRVCY_CONF_COMM_RQST_RSN_CD_SK") == F.col("rr.CD_MPPNG_SK"),
        "left"
    )
    .join(
        et,
        F.col("lk.PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK") == F.col("et.CD_MPPNG_SK"),
        "left"
    )
    .join(
        ps,
        F.col("lk.PRVCY_MBR_SRC_CD_SK") == F.col("ps.CD_MPPNG_SK"),
        "left"
    )
    .join(
        ct,
        F.col("lk.PRVCY_CONF_COMM_TYP_CD_SK") == F.col("ct.CD_MPPNG_SK"),
        "left"
    )
    .join(
        sc,
        F.col("lk.PRVCY_CONF_COMM_STTUS_CD_SK") == F.col("sc.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("lk.PRVCY_CONF_COMM_SK").alias("PRVCY_CONF_COMM_SK"),
        F.col("lk.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lk.PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
        F.col("lk.SEQ_NO").alias("SEQ_NO"),
        F.col("ps.TRGT_CD").alias("PRVCY_MBR_SRC_CD"),
        F.col("ct.TRGT_CD").alias("PRVCY_CONF_COMM_TYP_CD"),
        F.col("lk.PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
        F.col("lk.MBR_SK").alias("MBR_SK"),
        F.col("lk.ADDR_SEQ_NO").alias("ADDR_SEQ_NO"),
        F.col("lk.ADDREE_FIRST_NM").alias("ADDREE_FIRST_NM"),
        F.col("lk.ADDREE_LAST_NM").alias("ADDREE_LAST_NM"),
        F.col("lk.CONF_COMM_DESC").alias("CONF_COMM_DESC"),
        F.col("lk.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("lk.EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
        F.col("lk.RCVD_DT_SK").alias("RCVD_DT_SK"),
        F.col("rr.TRGT_CD").alias("PRVCY_CONF_COMM_RQST_RSN_CD"),
        F.col("rr.TRGT_CD_NM").alias("PRVCY_CONF_COMM_RQST_RSN_NM"),
        F.col("sc.TRGT_CD").alias("PRVCY_CONF_COMM_STTUS_CD"),
        F.col("sc.TRGT_CD_NM").alias("PRVCY_CONF_COMM_STTUS_NM"),
        F.col("lk.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("ct.TRGT_CD_NM").alias("PRVCY_CONF_COMM_TYP_NM"),
        F.col("lk.ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("lk.ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("lk.ADDR_LN_3").alias("ADDR_LN_3"),
        F.col("lk.CITY_NM").alias("CITY_NM"),
        F.col("st.TRGT_CD").alias("PRVCY_EXTRNL_ENTY_ADDR_ST_CD"),
        F.col("st.TRGT_CD_NM").alias("PRVCY_EXTRNL_ENTY_ADDR_ST_NM"),
        F.col("lk.POSTAL_CD").alias("POSTAL_CD"),
        F.col("lk.CNTY_NM").alias("CNTY_NM"),
        F.col("et.TRGT_CD").alias("PRVCY_EXTRNL_ENTY_ADDR_TYP_CD"),
        F.col("et.TRGT_CD_NM").alias("PRVCY_EXTRNL_ENTY_ADDR_TYP_NM"),
        F.col("lk.PHN_NO").alias("PHN_NO"),
        F.col("lk.PRVCY_EXTRNL_MBR_SK_1").alias("PRVCY_EXTRNL_MBR_SK_1"),
        F.col("lk.PRVCY_EXTRNL_MBR_ID").alias("PRVCY_EXTRNL_MBR_ID"),
        F.col("lk.MBR_ID").alias("MBR_ID"),
        F.col("ps.TRGT_CD_NM").alias("PRVCY_MBR_SRC_NM"),
        F.col("lk.PRVCY_CONF_COMM_RQST_RSN_CD_SK").alias("PRVCY_CONF_COMM_RQST_RSN_CD_SK"),
        F.col("lk.PRVCY_CONF_COMM_STTUS_CD_SK").alias("PRVCY_CONF_COMM_STTUS_CD_SK"),
        F.col("lk.PRVCY_CONF_COMM_TYP_CD_SK").alias("PRVCY_CONF_COMM_TYP_CD_SK"),
        F.col("lk.PRVCY_MBR_SRC_CD_SK").alias("PRVCY_MBR_SRC_CD_SK"),
        F.col("lk.PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK").alias("PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK")
    )
)

df_xfm_input = df_lkp_Codes

df_main_pre = (
    df_xfm_input
    .filter(
       (F.col("PRVCY_CONF_COMM_SK") != 0) & (F.col("PRVCY_CONF_COMM_SK") != 1)
    )
    .withColumn(
        "SRC_SYS_CD",
        F.when(
            F.col("SRC_SYS_CD").isNull() | (F.trim(F.col("SRC_SYS_CD")) == ""),
            F.lit("NA")
        ).otherwise(F.col("SRC_SYS_CD"))
    )
    .withColumn(
        "PRVCY_MBR_SRC_CD",
        F.when(
            F.col("PRVCY_MBR_SRC_CD").isNull() | (F.trim(F.col("PRVCY_MBR_SRC_CD")) == ""),
            F.lit("NA")
        ).otherwise(F.col("PRVCY_MBR_SRC_CD"))
    )
    .withColumn(
        "PRVCY_CONF_COMM_TYP_CD",
        F.when(
            F.col("PRVCY_CONF_COMM_TYP_CD").isNull() | (F.trim(F.col("PRVCY_CONF_COMM_TYP_CD")) == ""),
            F.lit("NA")
        ).otherwise(F.col("PRVCY_CONF_COMM_TYP_CD"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrRunCycleDate))
    .withColumn("PRVCY_CONF_COMM_RQST_RSN_CD",
        F.when(
            F.col("PRVCY_CONF_COMM_RQST_RSN_CD").isNull() | (F.trim(F.col("PRVCY_CONF_COMM_RQST_RSN_CD")) == ""),
            F.lit("NA")
        ).otherwise(F.col("PRVCY_CONF_COMM_RQST_RSN_CD"))
    )
    .withColumn("PRVCY_CONF_COMM_RQST_RSN_NM",
        F.when(
            F.col("PRVCY_CONF_COMM_RQST_RSN_NM").isNull() | (F.trim(F.col("PRVCY_CONF_COMM_RQST_RSN_NM")) == ""),
            F.lit("NA")
        ).otherwise(F.col("PRVCY_CONF_COMM_RQST_RSN_NM"))
    )
    .withColumn("PRVCY_CONF_COMM_STTUS_CD",
        F.when(
            F.col("PRVCY_CONF_COMM_STTUS_CD").isNull() | (F.trim(F.col("PRVCY_CONF_COMM_STTUS_CD")) == ""),
            F.lit("NA")
        ).otherwise(F.col("PRVCY_CONF_COMM_STTUS_CD"))
    )
    .withColumn("PRVCY_CONF_COMM_STTUS_NM",
        F.when(
            F.col("PRVCY_CONF_COMM_STTUS_NM").isNull() | (F.trim(F.col("PRVCY_CONF_COMM_STTUS_NM")) == ""),
            F.lit("NA")
        ).otherwise(F.col("PRVCY_CONF_COMM_STTUS_NM"))
    )
    .withColumn("PRVCY_CONF_COMM_TYP_NM",
        F.when(
            F.col("PRVCY_CONF_COMM_TYP_NM").isNull() | (F.trim(F.col("PRVCY_CONF_COMM_TYP_NM")) == ""),
            F.lit("NA")
        ).otherwise(F.col("PRVCY_CONF_COMM_TYP_NM"))
    )
    .withColumn("PRVCY_EXTRNL_ENTY_ADDR_LN_1",
        F.when(
            F.col("ADDR_LN_1").isNull() | (F.trim(F.col("ADDR_LN_1")) == ""),
            F.lit("NA")
        ).otherwise(F.col("ADDR_LN_1"))
    )
    .withColumn("PRVCY_EXTRNL_ENTY_ADDR_LN_2",
        F.when(
            F.col("ADDR_LN_2").isNull() | (F.trim(F.col("ADDR_LN_2")) == ""),
            F.lit("NA")
        ).otherwise(F.col("ADDR_LN_2"))
    )
    .withColumn("PRVCY_EXTRNL_ENTY_ADDR_LN_3",
        F.when(
            F.col("ADDR_LN_3").isNull() | (F.trim(F.col("ADDR_LN_3")) == ""),
            F.lit("NA")
        ).otherwise(F.col("ADDR_LN_3"))
    )
    .withColumn("PRVCY_EXTRNL_ENTY_CITY_NM",
        F.when(
            F.col("CITY_NM").isNull() | (F.trim(F.col("CITY_NM")) == ""),
            F.lit("NA")
        ).otherwise(F.col("CITY_NM"))
    )
    .withColumn("PRVCY_EXTRNL_ENTY_ADDR_ST_CD",
        F.when(
            F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_CD").isNull() | (F.trim(F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_CD")) == ""),
            F.lit("NA")
        ).otherwise(F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_CD"))
    )
    .withColumn("PRVCY_EXTRNL_ENTY_ADDR_ST_NM",
        F.when(
            F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_NM").isNull() | (F.trim(F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_NM")) == ""),
            F.lit("NA")
        ).otherwise(F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_NM"))
    )
    .withColumn("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_5",
        F.when(
            F.col("POSTAL_CD").isNull() | (F.trim(F.col("POSTAL_CD")) == ""),
            F.lit("NA")
        ).otherwise(F.trim(F.substring(F.col("POSTAL_CD"),1,5)))
    )
    .withColumn("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_4",
        F.when(
            F.col("POSTAL_CD").isNull() | (F.trim(F.col("POSTAL_CD")) == ""),
            F.lit("NA")
        ).otherwise(F.trim(F.substring(F.col("POSTAL_CD"),6,4)))
    )
    .withColumn("PRVCY_EXTRNL_ENTY_CNTY_NM",
        F.when(
            F.col("CNTY_NM").isNull() | (F.trim(F.col("CNTY_NM")) == ""),
            F.lit("NA")
        ).otherwise(F.col("CNTY_NM"))
    )
    .withColumn("PRVCY_EXTRNL_ENTY_ADDR_TYP_CD",
        F.when(
            F.col("PRVCY_EXTRNL_ENTY_ADDR_TYP_CD").isNull() | (F.trim(F.col("PRVCY_EXTRNL_ENTY_ADDR_TYP_CD")) == ""),
            F.lit("NA")
        ).otherwise(F.col("PRVCY_EXTRNL_ENTY_ADDR_TYP_CD"))
    )
    .withColumn("PRVCY_EXTRNL_ENTY_ADDR_TYP_NM",
        F.when(
            F.col("PRVCY_EXTRNL_ENTY_ADDR_TYP_NM").isNull() | (F.trim(F.col("PRVCY_EXTRNL_ENTY_ADDR_TYP_NM")) == ""),
            F.lit("NA")
        ).otherwise(F.col("PRVCY_EXTRNL_ENTY_ADDR_TYP_NM"))
    )
    .withColumn("PRVCY_EXTRNL_ENTY_PHN_NO",
        F.when(
            F.col("PHN_NO").isNull() | (F.trim(F.col("PHN_NO")) == ""),
            F.lit("NA")
        ).otherwise(F.col("PHN_NO"))
    )
    .withColumn("PRVCY_MBR_ID",
        F.when(
            F.col("PRVCY_MBR_SRC_CD") == F.lit("FACETS"),
            F.when(
                F.col("MBR_ID").isNull(),
                F.lit("NA")
            ).otherwise(F.col("MBR_ID"))
        ).otherwise(
            F.when(
                F.col("PRVCY_EXTRNL_MBR_SK_1").isNull() | (F.trim(F.col("PRVCY_EXTRNL_MBR_SK_1")) == ""),
                F.lit("NA")
            ).otherwise(F.col("PRVCY_EXTRNL_MBR_ID"))
        )
    )
    .withColumn("PRVCY_MBR_SRC_NM",
        F.when(
            F.col("PRVCY_MBR_SRC_NM").isNull() | (F.trim(F.col("PRVCY_MBR_SRC_NM")) == ""),
            F.lit("NA")
        ).otherwise(F.col("PRVCY_MBR_SRC_NM"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .select(
        F.col("PRVCY_CONF_COMM_SK"),
        F.col("SRC_SYS_CD"),
        F.col("PRVCY_MBR_UNIQ_KEY"),
        F.col("SEQ_NO").alias("PRVCY_CONF_COMM_SEQ_NO"),
        F.col("PRVCY_MBR_SRC_CD"),
        F.col("PRVCY_CONF_COMM_TYP_CD"),
        F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("PRVCY_EXTRNL_MBR_SK"),
        F.col("MBR_SK"),
        F.col("ADDR_SEQ_NO").alias("PRVCY_CONF_COMM_ADDR_SEQ_NO"),
        F.col("ADDREE_FIRST_NM").alias("PRVCY_CONF_COMM_ADDRE_FIRST_NM"),
        F.col("ADDREE_LAST_NM").alias("PRVCY_CONF_COMM_ADDRE_LAST_NM"),
        F.col("CONF_COMM_DESC").alias("PRVCY_CONF_COMM_DESC"),
        F.col("EFF_DT_SK").alias("PRVCY_CONF_COMM_EFF_DT_SK"),
        F.col("EMAIL_ADDR_TX").alias("PRVCY_CONF_COMM_EMAIL_ADDR_TX"),
        F.col("RCVD_DT_SK").alias("PRVCY_CONF_COMM_RCVD_DT_SK"),
        F.col("PRVCY_CONF_COMM_RQST_RSN_CD"),
        F.col("PRVCY_CONF_COMM_RQST_RSN_NM"),
        F.col("PRVCY_CONF_COMM_STTUS_CD"),
        F.col("PRVCY_CONF_COMM_STTUS_NM"),
        F.col("TERM_DT_SK").alias("PRVCY_CONF_COMM_TERM_DT_SK"),
        F.col("PRVCY_CONF_COMM_TYP_NM"),
        F.col("PRVCY_EXTRNL_ENTY_ADDR_LN_1"),
        F.col("PRVCY_EXTRNL_ENTY_ADDR_LN_2"),
        F.col("PRVCY_EXTRNL_ENTY_ADDR_LN_3"),
        F.col("PRVCY_EXTRNL_ENTY_CITY_NM"),
        F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_CD"),
        F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_NM"),
        F.col("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_5"),
        F.col("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_4"),
        F.col("PRVCY_EXTRNL_ENTY_CNTY_NM"),
        F.col("PRVCY_EXTRNL_ENTY_ADDR_TYP_CD"),
        F.col("PRVCY_EXTRNL_ENTY_ADDR_TYP_NM"),
        F.col("PRVCY_EXTRNL_ENTY_PHN_NO"),
        F.col("PRVCY_MBR_ID"),
        F.col("PRVCY_MBR_SRC_NM"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PRVCY_CONF_COMM_RQST_RSN_CD_SK"),
        F.col("PRVCY_CONF_COMM_STTUS_CD_SK"),
        F.col("PRVCY_CONF_COMM_TYP_CD_SK"),
        F.col("PRVCY_MBR_SRC_CD_SK"),
        F.col("PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK").alias("PRVCY_EXT_ENTY_ADDR_TYP_CD_SK")
    )
)

df_na_base = df_xfm_input.limit(1)
df_na = (
    df_na_base
    .select(
        F.lit(1).alias("PRVCY_CONF_COMM_SK"),
        F.lit("NA").alias("SRC_SYS_CD"),
        F.lit(1).alias("PRVCY_MBR_UNIQ_KEY"),
        F.lit(0).alias("PRVCY_CONF_COMM_SEQ_NO"),
        F.lit("NA").alias("PRVCY_MBR_SRC_CD"),
        F.lit("NA").alias("PRVCY_CONF_COMM_TYP_CD"),
        F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(1).alias("PRVCY_EXTRNL_MBR_SK"),
        F.lit(1).alias("MBR_SK"),
        F.lit(0).alias("PRVCY_CONF_COMM_ADDR_SEQ_NO"),
        F.lit("NA").alias("PRVCY_CONF_COMM_ADDRE_FIRST_NM"),
        F.lit("NA").alias("PRVCY_CONF_COMM_ADDRE_LAST_NM"),
        F.lit("").alias("PRVCY_CONF_COMM_DESC"),
        F.lit("1753-01-01").alias("PRVCY_CONF_COMM_EFF_DT_SK"),
        F.lit("").alias("PRVCY_CONF_COMM_EMAIL_ADDR_TX"),
        F.lit("1753-01-01").alias("PRVCY_CONF_COMM_RCVD_DT_SK"),
        F.lit("NA").alias("PRVCY_CONF_COMM_RQST_RSN_CD"),
        F.lit("NA").alias("PRVCY_CONF_COMM_RQST_RSN_NM"),
        F.lit("NA").alias("PRVCY_CONF_COMM_STTUS_CD"),
        F.lit("NA").alias("PRVCY_CONF_COMM_STTUS_NM"),
        F.lit("1753-01-01").alias("PRVCY_CONF_COMM_TERM_DT_SK"),
        F.lit("NA").alias("PRVCY_CONF_COMM_TYP_NM"),
        F.lit("").alias("PRVCY_EXTRNL_ENTY_ADDR_LN_1"),
        F.lit("").alias("PRVCY_EXTRNL_ENTY_ADDR_LN_2"),
        F.lit("").alias("PRVCY_EXTRNL_ENTY_ADDR_LN_3"),
        F.lit("NA").alias("PRVCY_EXTRNL_ENTY_CITY_NM"),
        F.lit("NA").alias("PRVCY_EXTRNL_ENTY_ADDR_ST_CD"),
        F.lit("NA").alias("PRVCY_EXTRNL_ENTY_ADDR_ST_NM"),
        F.lit("").alias("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_5"),
        F.lit("").alias("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_4"),
        F.lit("NA").alias("PRVCY_EXTRNL_ENTY_CNTY_NM"),
        F.lit("NA").alias("PRVCY_EXTRNL_ENTY_ADDR_TYP_CD"),
        F.lit("NA").alias("PRVCY_EXTRNL_ENTY_ADDR_TYP_NM"),
        F.lit("").alias("PRVCY_EXTRNL_ENTY_PHN_NO"),
        F.lit("NA").alias("PRVCY_MBR_ID"),
        F.lit("NA").alias("PRVCY_MBR_SRC_NM"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("PRVCY_CONF_COMM_RQST_RSN_CD_SK"),
        F.lit(1).alias("PRVCY_CONF_COMM_STTUS_CD_SK"),
        F.lit(1).alias("PRVCY_CONF_COMM_TYP_CD_SK"),
        F.lit(1).alias("PRVCY_MBR_SRC_CD_SK"),
        F.lit(1).alias("PRVCY_EXT_ENTY_ADDR_TYP_CD_SK")
    )
)

df_unk_base = df_xfm_input.limit(1)
df_unk = (
    df_unk_base
    .select(
        F.lit(0).alias("PRVCY_CONF_COMM_SK"),
        F.lit("UNK").alias("SRC_SYS_CD"),
        F.lit(0).alias("PRVCY_MBR_UNIQ_KEY"),
        F.lit(0).alias("PRVCY_CONF_COMM_SEQ_NO"),
        F.lit("UNK").alias("PRVCY_MBR_SRC_CD"),
        F.lit("UNK").alias("PRVCY_CONF_COMM_TYP_CD"),
        F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(0).alias("PRVCY_EXTRNL_MBR_SK"),
        F.lit(0).alias("MBR_SK"),
        F.lit(0).alias("PRVCY_CONF_COMM_ADDR_SEQ_NO"),
        F.lit("UNK").alias("PRVCY_CONF_COMM_ADDRE_FIRST_NM"),
        F.lit("UNK").alias("PRVCY_CONF_COMM_ADDRE_LAST_NM"),
        F.lit("").alias("PRVCY_CONF_COMM_DESC"),
        F.lit("1753-01-01").alias("PRVCY_CONF_COMM_EFF_DT_SK"),
        F.lit("").alias("PRVCY_CONF_COMM_EMAIL_ADDR_TX"),
        F.lit("1753-01-01").alias("PRVCY_CONF_COMM_RCVD_DT_SK"),
        F.lit("UNK").alias("PRVCY_CONF_COMM_RQST_RSN_CD"),
        F.lit("UNK").alias("PRVCY_CONF_COMM_RQST_RSN_NM"),
        F.lit("UNK").alias("PRVCY_CONF_COMM_STTUS_CD"),
        F.lit("UNK").alias("PRVCY_CONF_COMM_STTUS_NM"),
        F.lit("1753-01-01").alias("PRVCY_CONF_COMM_TERM_DT_SK"),
        F.lit("UNK").alias("PRVCY_CONF_COMM_TYP_NM"),
        F.lit("").alias("PRVCY_EXTRNL_ENTY_ADDR_LN_1"),
        F.lit("").alias("PRVCY_EXTRNL_ENTY_ADDR_LN_2"),
        F.lit("").alias("PRVCY_EXTRNL_ENTY_ADDR_LN_3"),
        F.lit("UNK").alias("PRVCY_EXTRNL_ENTY_CITY_NM"),
        F.lit("UNK").alias("PRVCY_EXTRNL_ENTY_ADDR_ST_CD"),
        F.lit("UNK").alias("PRVCY_EXTRNL_ENTY_ADDR_ST_NM"),
        F.lit("").alias("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_5"),
        F.lit("").alias("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_4"),
        F.lit("UNK").alias("PRVCY_EXTRNL_ENTY_CNTY_NM"),
        F.lit("UNK").alias("PRVCY_EXTRNL_ENTY_ADDR_TYP_CD"),
        F.lit("UNK").alias("PRVCY_EXTRNL_ENTY_ADDR_TYP_NM"),
        F.lit("").alias("PRVCY_EXTRNL_ENTY_PHN_NO"),
        F.lit("UNK").alias("PRVCY_MBR_ID"),
        F.lit("UNK").alias("PRVCY_MBR_SRC_NM"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("PRVCY_CONF_COMM_RQST_RSN_CD_SK"),
        F.lit(0).alias("PRVCY_CONF_COMM_STTUS_CD_SK"),
        F.lit(0).alias("PRVCY_CONF_COMM_TYP_CD_SK"),
        F.lit(0).alias("PRVCY_MBR_SRC_CD_SK"),
        F.lit(0).alias("PRVCY_EXT_ENTY_ADDR_TYP_CD_SK")
    )
)

df_funnel_pre = df_na.unionByName(df_main_pre).unionByName(df_unk)

final_columns_in_order = [
    "PRVCY_CONF_COMM_SK",
    "SRC_SYS_CD",
    "PRVCY_MBR_UNIQ_KEY",
    "PRVCY_CONF_COMM_SEQ_NO",
    "PRVCY_MBR_SRC_CD",
    "PRVCY_CONF_COMM_TYP_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "PRVCY_EXTRNL_MBR_SK",
    "MBR_SK",
    "PRVCY_CONF_COMM_ADDR_SEQ_NO",
    "PRVCY_CONF_COMM_ADDRE_FIRST_NM",
    "PRVCY_CONF_COMM_ADDRE_LAST_NM",
    "PRVCY_CONF_COMM_DESC",
    "PRVCY_CONF_COMM_EFF_DT_SK",
    "PRVCY_CONF_COMM_EMAIL_ADDR_TX",
    "PRVCY_CONF_COMM_RCVD_DT_SK",
    "PRVCY_CONF_COMM_RQST_RSN_CD",
    "PRVCY_CONF_COMM_RQST_RSN_NM",
    "PRVCY_CONF_COMM_STTUS_CD",
    "PRVCY_CONF_COMM_STTUS_NM",
    "PRVCY_CONF_COMM_TERM_DT_SK",
    "PRVCY_CONF_COMM_TYP_NM",
    "PRVCY_EXTRNL_ENTY_ADDR_LN_1",
    "PRVCY_EXTRNL_ENTY_ADDR_LN_2",
    "PRVCY_EXTRNL_ENTY_ADDR_LN_3",
    "PRVCY_EXTRNL_ENTY_CITY_NM",
    "PRVCY_EXTRNL_ENTY_ADDR_ST_CD",
    "PRVCY_EXTRNL_ENTY_ADDR_ST_NM",
    "PRVCY_RECPNT_EXT_ENTY_ZIP_CD_5",
    "PRVCY_RECPNT_EXT_ENTY_ZIP_CD_4",
    "PRVCY_EXTRNL_ENTY_CNTY_NM",
    "PRVCY_EXTRNL_ENTY_ADDR_TYP_CD",
    "PRVCY_EXTRNL_ENTY_ADDR_TYP_NM",
    "PRVCY_EXTRNL_ENTY_PHN_NO",
    "PRVCY_MBR_ID",
    "PRVCY_MBR_SRC_NM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRVCY_CONF_COMM_RQST_RSN_CD_SK",
    "PRVCY_CONF_COMM_STTUS_CD_SK",
    "PRVCY_CONF_COMM_TYP_CD_SK",
    "PRVCY_MBR_SRC_CD_SK",
    "PRVCY_EXT_ENTY_ADDR_TYP_CD_SK"
]

df_funnel = df_funnel_pre.select(final_columns_in_order)

df_funnel = df_funnel \
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
    ) \
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
    ) \
    .withColumn(
        "PRVCY_CONF_COMM_EFF_DT_SK",
        F.rpad(F.col("PRVCY_CONF_COMM_EFF_DT_SK"), 10, " ")
    ) \
    .withColumn(
        "PRVCY_CONF_COMM_RCVD_DT_SK",
        F.rpad(F.col("PRVCY_CONF_COMM_RCVD_DT_SK"), 10, " ")
    ) \
    .withColumn(
        "PRVCY_CONF_COMM_TERM_DT_SK",
        F.rpad(F.col("PRVCY_CONF_COMM_TERM_DT_SK"), 10, " ")
    ) \
    .withColumn(
        "PRVCY_RECPNT_EXT_ENTY_ZIP_CD_5",
        F.rpad(F.col("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_5"), 5, " ")
    ) \
    .withColumn(
        "PRVCY_RECPNT_EXT_ENTY_ZIP_CD_4",
        F.rpad(F.col("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_4"), 4, " ")
    )

write_files(
    df_funnel,
    f"{adls_path}/load/PRVCY_CONF_COMM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)