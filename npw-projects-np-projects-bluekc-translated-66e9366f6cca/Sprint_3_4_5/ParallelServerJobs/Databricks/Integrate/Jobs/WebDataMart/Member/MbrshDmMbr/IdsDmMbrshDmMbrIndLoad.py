# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Rama Kamjula     09/20/2013          5114                             Original Programming                                                                                 IntegrateWrhsDevl       Peter Marshall               10/23/2013   
# MAGIC 
# MAGIC Kalyan Neelam         2013-11-27           5234 Compass                Added new column CMPSS_COV_IN                                                IntegrateNewDevl         Bhoomi Dasari            11/28/2013
# MAGIC 
# MAGIC Jag Yelavarthi      2013-12-28              #5114 - Daptiv#653       Added columns VBB_ENR_IN, VBB_MDL_CD and                          IntegrateNewDevl           Bhoomi Dasari              2014-01-03
# MAGIC                                                                                                     ALNO_HOME_PG_ID      
# MAGIC 
# MAGIC Pooja Sunkara     2014-01-24               5114-Daptiv #696         Corrected field names   MBR_WELNS_BNF_LVL_NM                       IntegrateNewDevl          Jag Yelavarthi              2014-01-27
# MAGIC                                                                                                      and MBR_ACTV_PROD_IN                     
# MAGIC 
# MAGIC Karthik Chintalapani  2013-01-21        5108                              Added new column  PCMH_IN                                                            IntegrateNewDevl             Kalyan Neelam            2014-03-31
# MAGIC                                                                                                   as per the new mapping rules. 
# MAGIC 
# MAGIC 
# MAGIC Pooja Sunkara            2015-04-25        TFS#7916                   SRC_SYS_CD is added to update strategy since                               IntegrateNewDevl              Kalyan Neelam            2015-04-28
# MAGIC                                                                                                   it is part of natural key in MBR_DM_MBR
# MAGIC                        
# MAGIC Abhiram Dasarathy	2016-12-06         5217 Dental Network	Added DNTL_NTWRK_IN field to the end of the columns		IntegrateDev2                Kalyan Neelam            2016-12-06

# MAGIC Job Name: IdsDmMbrshDmMbrIndLoad
# MAGIC Read Load File created in the IdsDmMbrshDmMbrExtr Job
# MAGIC Load Method: Update
# MAGIC Load rejects are redirected into a Reject file
# MAGIC Copy Stage for buffer
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

# --------------------------------------------------------------------------------
# seq_MBRSH_DM_MBR_IND (PxSequentialFile) - Read "load/MBRSH_DM_MBR_IND.dat"
# --------------------------------------------------------------------------------
schema_seq_MBRSH_DM_MBR_IND = StructType([
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("MBR_MNTL_HLTH_VNDR_ID", StringType(), nullable=True),
    StructField("MBR_PBM_VNDR_CD", StringType(), nullable=True),
    StructField("MBR_WELNS_BNF_LVL_CD", StringType(), nullable=True),
    StructField("MBR_WELNS_BNF_LVL_NM", StringType(), nullable=True),
    StructField("MBR_WELNS_BNF_VNDR_ID", StringType(), nullable=False),
    StructField("MBR_ACTV_PROD_IN", StringType(), nullable=True),
    StructField("MBR_EAP_CAT_CD", StringType(), nullable=True),
    StructField("MBR_EAP_CAT_DESC", StringType(), nullable=True),
    StructField("MBR_VSN_BNF_VNDR_ID", StringType(), nullable=True),
    StructField("MBR_VSN_RTN_EXAM_IN", StringType(), nullable=True),
    StructField("MBR_VSN_HRDWR_IN", StringType(), nullable=True),
    StructField("MBR_VSN_OUT_OF_NTWK_EXAM_IN", StringType(), nullable=True),
    StructField("MBR_OTHR_CAR_PRI_MED_IN", StringType(), nullable=True),
    StructField("MBR_PT_POD_DPLY_IN", StringType(), nullable=True),
    StructField("CLS_DESC", StringType(), nullable=True),
    StructField("GRP_NM", StringType(), nullable=True),
    StructField("SUBGRP_NM", StringType(), nullable=True),
    StructField("MBR_PT_EFF_DT", TimestampType(), nullable=True),
    StructField("MBR_PT_TERM_DT", TimestampType(), nullable=True),
    StructField("CMPSS_COV_IN", StringType(), nullable=True),
    StructField("ALNO_HOME_PG_ID", StringType(), nullable=True),
    StructField("VBB_ENR_IN", StringType(), nullable=True),
    StructField("VBB_MDL_CD", StringType(), nullable=True),
    StructField("PCMH_IN", StringType(), nullable=True),
    StructField("DNTL_RWRD_IN", StringType(), nullable=True)
])

df_seq_MBRSH_DM_MBR_IND = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_seq_MBRSH_DM_MBR_IND)
    .load(f"{adls_path}/load/MBRSH_DM_MBR_IND.dat")
)

# --------------------------------------------------------------------------------
# Cpy (PxCopy)
# --------------------------------------------------------------------------------
df_Cpy = df_seq_MBRSH_DM_MBR_IND.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_MNTL_HLTH_VNDR_ID").alias("MBR_MNTL_HLTH_VNDR_ID"),
    F.col("MBR_PBM_VNDR_CD").alias("MBR_PBM_VNDR_CD"),
    F.col("MBR_WELNS_BNF_LVL_CD").alias("MBR_WELNS_BNF_LVL_CD"),
    F.col("MBR_WELNS_BNF_LVL_NM").alias("MBR_WELNS_BNF_LVL_NM"),
    F.col("MBR_WELNS_BNF_VNDR_ID").alias("MBR_WELNS_BNF_VNDR_ID"),
    F.col("MBR_ACTV_PROD_IN").alias("MBR_ACTV_PROD_IN"),
    F.col("MBR_EAP_CAT_CD").alias("MBR_EAP_CAT_CD"),
    F.col("MBR_EAP_CAT_DESC").alias("MBR_EAP_CAT_DESC"),
    F.col("MBR_VSN_BNF_VNDR_ID").alias("MBR_VSN_BNF_VNDR_ID"),
    F.col("MBR_VSN_RTN_EXAM_IN").alias("MBR_VSN_RTN_EXAM_IN"),
    F.col("MBR_VSN_HRDWR_IN").alias("MBR_VSN_HRDWR_IN"),
    F.col("MBR_VSN_OUT_OF_NTWK_EXAM_IN").alias("MBR_VSN_OUT_OF_NTWK_EXAM_IN"),
    F.col("MBR_OTHR_CAR_PRI_MED_IN").alias("MBR_OTHR_CAR_PRI_MED_IN"),
    F.col("MBR_PT_POD_DPLY_IN").alias("MBR_PT_POD_DPLY_IN"),
    F.col("CLS_DESC").alias("CLS_DESC"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("SUBGRP_NM").alias("SUBGRP_NM"),
    F.col("MBR_PT_EFF_DT").alias("MBR_PT_EFF_DT"),
    F.col("MBR_PT_TERM_DT").alias("MBR_PT_TERM_DT"),
    F.col("CMPSS_COV_IN").alias("CMPSS_COV_IN"),
    F.col("ALNO_HOME_PG_ID").alias("ALNO_HOME_PG_ID"),
    F.col("VBB_ENR_IN").alias("VBB_ENR_IN"),
    F.col("VBB_MDL_CD").alias("VBB_MDL_CD"),
    F.col("PCMH_IN").alias("PCMH_IN"),
    F.col("DNTL_RWRD_IN").alias("DNTL_RWRD_IN")
)

# --------------------------------------------------------------------------------
# odbc_MBRSH_DM_MBR (ODBCConnectorPX) - Merge into #$ClmMartOwner#.MBRSH_DM_MBR
# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)
temp_table_name = "STAGING.IdsDmMbrshDmMbrIndLoad_odbc_MBRSH_DM_MBR_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)

df_Cpy.write.jdbc(
    url=jdbc_url,
    table=temp_table_name,
    mode="append",
    properties=jdbc_props
)

merge_sql = f"""
MERGE INTO {ClmMartOwner}.MBRSH_DM_MBR AS T
USING {temp_table_name} AS S
ON 
    T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
    UPDATE SET 
        T.MBR_MNTL_HLTH_VNDR_ID = S.MBR_MNTL_HLTH_VNDR_ID,
        T.MBR_PBM_VNDR_CD = S.MBR_PBM_VNDR_CD,
        T.MBR_WELNS_BNF_LVL_CD = S.MBR_WELNS_BNF_LVL_CD,
        T.MBR_WELNS_BNF_LVL_NM = S.MBR_WELNS_BNF_LVL_NM,
        T.MBR_WELNS_BNF_VNDR_ID = S.MBR_WELNS_BNF_VNDR_ID,
        T.MBR_ACTV_PROD_IN = S.MBR_ACTV_PROD_IN,
        T.MBR_EAP_CAT_CD = S.MBR_EAP_CAT_CD,
        T.MBR_EAP_CAT_DESC = S.MBR_EAP_CAT_DESC,
        T.MBR_VSN_BNF_VNDR_ID = S.MBR_VSN_BNF_VNDR_ID,
        T.MBR_VSN_RTN_EXAM_IN = S.MBR_VSN_RTN_EXAM_IN,
        T.MBR_VSN_HRDWR_IN = S.MBR_VSN_HRDWR_IN,
        T.MBR_VSN_OUT_OF_NTWK_EXAM_IN = S.MBR_VSN_OUT_OF_NTWK_EXAM_IN,
        T.MBR_OTHR_CAR_PRI_MED_IN = S.MBR_OTHR_CAR_PRI_MED_IN,
        T.MBR_PT_POD_DPLY_IN = S.MBR_PT_POD_DPLY_IN,
        T.CLS_DESC = S.CLS_DESC,
        T.GRP_NM = S.GRP_NM,
        T.SUBGRP_NM = S.SUBGRP_NM,
        T.MBR_PT_EFF_DT = S.MBR_PT_EFF_DT,
        T.MBR_PT_TERM_DT = S.MBR_PT_TERM_DT,
        T.CMPSS_COV_IN = S.CMPSS_COV_IN,
        T.ALNO_HOME_PG_ID = S.ALNO_HOME_PG_ID,
        T.VBB_ENR_IN = S.VBB_ENR_IN,
        T.VBB_MDL_CD = S.VBB_MDL_CD,
        T.PCMH_IN = S.PCMH_IN,
        T.DNTL_RWRD_IN = S.DNTL_RWRD_IN
WHEN NOT MATCHED THEN
    INSERT (
        MBR_UNIQ_KEY,
        SRC_SYS_CD,
        MBR_MNTL_HLTH_VNDR_ID,
        MBR_PBM_VNDR_CD,
        MBR_WELNS_BNF_LVL_CD,
        MBR_WELNS_BNF_LVL_NM,
        MBR_WELNS_BNF_VNDR_ID,
        MBR_ACTV_PROD_IN,
        MBR_EAP_CAT_CD,
        MBR_EAP_CAT_DESC,
        MBR_VSN_BNF_VNDR_ID,
        MBR_VSN_RTN_EXAM_IN,
        MBR_VSN_HRDWR_IN,
        MBR_VSN_OUT_OF_NTWK_EXAM_IN,
        MBR_OTHR_CAR_PRI_MED_IN,
        MBR_PT_POD_DPLY_IN,
        CLS_DESC,
        GRP_NM,
        SUBGRP_NM,
        MBR_PT_EFF_DT,
        MBR_PT_TERM_DT,
        CMPSS_COV_IN,
        ALNO_HOME_PG_ID,
        VBB_ENR_IN,
        VBB_MDL_CD,
        PCMH_IN,
        DNTL_RWRD_IN
    )
    VALUES (
        S.MBR_UNIQ_KEY,
        S.SRC_SYS_CD,
        S.MBR_MNTL_HLTH_VNDR_ID,
        S.MBR_PBM_VNDR_CD,
        S.MBR_WELNS_BNF_LVL_CD,
        S.MBR_WELNS_BNF_LVL_NM,
        S.MBR_WELNS_BNF_VNDR_ID,
        S.MBR_ACTV_PROD_IN,
        S.MBR_EAP_CAT_CD,
        S.MBR_EAP_CAT_DESC,
        S.MBR_VSN_BNF_VNDR_ID,
        S.MBR_VSN_RTN_EXAM_IN,
        S.MBR_VSN_HRDWR_IN,
        S.MBR_VSN_OUT_OF_NTWK_EXAM_IN,
        S.MBR_OTHR_CAR_PRI_MED_IN,
        S.MBR_PT_POD_DPLY_IN,
        S.CLS_DESC,
        S.GRP_NM,
        S.SUBGRP_NM,
        S.MBR_PT_EFF_DT,
        S.MBR_PT_TERM_DT,
        S.CMPSS_COV_IN,
        S.ALNO_HOME_PG_ID,
        S.VBB_ENR_IN,
        S.VBB_MDL_CD,
        S.PCMH_IN,
        S.DNTL_RWRD_IN
    );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

# --------------------------------------------------------------------------------
# seq_MBRSH_DM_MBR_Rej (PxSequentialFile) - Write "load/MBRSH_DM_MBR_IND_Rej.dat"
# --------------------------------------------------------------------------------
# Simulate reject rows schema (includes input columns plus ERRORCODE, ERRORTEXT)
schema_odbc_MBRSH_DM_MBR_rej = StructType([
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("MBR_MNTL_HLTH_VNDR_ID", StringType(), True),
    StructField("MBR_PBM_VNDR_CD", StringType(), True),
    StructField("MBR_WELNS_BNF_LVL_CD", StringType(), True),
    StructField("MBR_WELNS_BNF_LVL_NM", StringType(), True),
    StructField("MBR_WELNS_BNF_VNDR_ID", StringType(), True),
    StructField("MBR_ACTV_PROD_IN", StringType(), True),
    StructField("MBR_EAP_CAT_CD", StringType(), True),
    StructField("MBR_EAP_CAT_DESC", StringType(), True),
    StructField("MBR_VSN_BNF_VNDR_ID", StringType(), True),
    StructField("MBR_VSN_RTN_EXAM_IN", StringType(), True),
    StructField("MBR_VSN_HRDWR_IN", StringType(), True),
    StructField("MBR_VSN_OUT_OF_NTWK_EXAM_IN", StringType(), True),
    StructField("MBR_OTHR_CAR_PRI_MED_IN", StringType(), True),
    StructField("MBR_PT_POD_DPLY_IN", StringType(), True),
    StructField("CLS_DESC", StringType(), True),
    StructField("GRP_NM", StringType(), True),
    StructField("SUBGRP_NM", StringType(), True),
    StructField("MBR_PT_EFF_DT", TimestampType(), True),
    StructField("MBR_PT_TERM_DT", TimestampType(), True),
    StructField("CMPSS_COV_IN", StringType(), True),
    StructField("ALNO_HOME_PG_ID", StringType(), True),
    StructField("VBB_ENR_IN", StringType(), True),
    StructField("VBB_MDL_CD", StringType(), True),
    StructField("PCMH_IN", StringType(), True),
    StructField("DNTL_RWRD_IN", StringType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_odbc_MBRSH_DM_MBR_rej = spark.createDataFrame([], schema_odbc_MBRSH_DM_MBR_rej)

# Apply rpad if char or varchar; pass through numeric/timestamp. 
df_seq_MBRSH_DM_MBR_Rej_final = df_odbc_MBRSH_DM_MBR_rej.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("MBR_MNTL_HLTH_VNDR_ID"), <...>, " ").alias("MBR_MNTL_HLTH_VNDR_ID"),
    F.rpad(F.col("MBR_PBM_VNDR_CD"), <...>, " ").alias("MBR_PBM_VNDR_CD"),
    F.rpad(F.col("MBR_WELNS_BNF_LVL_CD"), <...>, " ").alias("MBR_WELNS_BNF_LVL_CD"),
    F.rpad(F.col("MBR_WELNS_BNF_LVL_NM"), <...>, " ").alias("MBR_WELNS_BNF_LVL_NM"),
    F.rpad(F.col("MBR_WELNS_BNF_VNDR_ID"), <...>, " ").alias("MBR_WELNS_BNF_VNDR_ID"),
    F.rpad(F.col("MBR_ACTV_PROD_IN"), 1, " ").alias("MBR_ACTV_PROD_IN"),
    F.rpad(F.col("MBR_EAP_CAT_CD"), <...>, " ").alias("MBR_EAP_CAT_CD"),
    F.rpad(F.col("MBR_EAP_CAT_DESC"), <...>, " ").alias("MBR_EAP_CAT_DESC"),
    F.rpad(F.col("MBR_VSN_BNF_VNDR_ID"), <...>, " ").alias("MBR_VSN_BNF_VNDR_ID"),
    F.rpad(F.col("MBR_VSN_RTN_EXAM_IN"), 1, " ").alias("MBR_VSN_RTN_EXAM_IN"),
    F.rpad(F.col("MBR_VSN_HRDWR_IN"), 1, " ").alias("MBR_VSN_HRDWR_IN"),
    F.rpad(F.col("MBR_VSN_OUT_OF_NTWK_EXAM_IN"), 1, " ").alias("MBR_VSN_OUT_OF_NTWK_EXAM_IN"),
    F.rpad(F.col("MBR_OTHR_CAR_PRI_MED_IN"), 1, " ").alias("MBR_OTHR_CAR_PRI_MED_IN"),
    F.rpad(F.col("MBR_PT_POD_DPLY_IN"), 1, " ").alias("MBR_PT_POD_DPLY_IN"),
    F.rpad(F.col("CLS_DESC"), <...>, " ").alias("CLS_DESC"),
    F.rpad(F.col("GRP_NM"), <...>, " ").alias("GRP_NM"),
    F.rpad(F.col("SUBGRP_NM"), <...>, " ").alias("SUBGRP_NM"),
    F.col("MBR_PT_EFF_DT").alias("MBR_PT_EFF_DT"),
    F.col("MBR_PT_TERM_DT").alias("MBR_PT_TERM_DT"),
    F.rpad(F.col("CMPSS_COV_IN"), 1, " ").alias("CMPSS_COV_IN"),
    F.rpad(F.col("ALNO_HOME_PG_ID"), <...>, " ").alias("ALNO_HOME_PG_ID"),
    F.rpad(F.col("VBB_ENR_IN"), 1, " ").alias("VBB_ENR_IN"),
    F.rpad(F.col("VBB_MDL_CD"), <...>, " ").alias("VBB_MDL_CD"),
    F.rpad(F.col("PCMH_IN"), 1, " ").alias("PCMH_IN"),
    F.rpad(F.col("DNTL_RWRD_IN"), 1, " ").alias("DNTL_RWRD_IN"),
    F.rpad(F.col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    F.rpad(F.col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_seq_MBRSH_DM_MBR_Rej_final,
    f"{adls_path}/load/MBRSH_DM_MBR_IND_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)