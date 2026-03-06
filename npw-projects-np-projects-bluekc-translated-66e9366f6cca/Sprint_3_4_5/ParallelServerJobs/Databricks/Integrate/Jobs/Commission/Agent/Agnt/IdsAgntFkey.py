# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsAgntFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  Primary Key output file
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes
# MAGIC                             GetFkeyGrp
# MAGIC                             GetFkeyErrorCnt
# MAGIC 
# MAGIC PROCESSING:  Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:   CLS table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Ralph Tucker  -  1/20/2005  -  Originally programmed
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari          2008-09-05              Added SrcSysCdSk parameter                                                          3567                 devlIDS                              Steph Goddard          09/22/2008
# MAGIC 
# MAGIC Abhiram D	2013-03-27	Changed the AGNT_NM field to NOT_NULLABLE 	          TTR - 1541	    IntegrateNewDevl             Bhoomi Dasari           4/12/2013	
# MAGIC 				in the load file table definition.
# MAGIC 
# MAGIC Dan Long                2014-06-17               Add fields COCE_MCTR_VIP and AGNT_TIER to the process.      TFS-1079          IntegrateNewDevl               Kalyan Neelam           2014-07-08
# MAGIC 
# MAGIC Shanmugam A         2018-02-05               Updated logic for the field AGNT_TIER_CD_SK                             TFS-21033        Integrate Dev1                    Jaideep Mankala             2018-02-22

# MAGIC Set all foreign surragote keys
# MAGIC 
# MAGIC The NA row MUST include a -1 in the AGNT_UNIQ_KEY field for processes downline.
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F, types as T
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile","IdsAgntExtr.tmp")
OutFile = get_widget_value("OutFile","AGNT.dat")
Logging = get_widget_value("Logging","$PROJDEF")
SrcSysCdSk = get_widget_value("SrcSysCdSk","105838")

schema_IdsAgntExtr = T.StructType([
    T.StructField("JOB_EXCTN_RCRD_ERR_SK", T.IntegerType(), False),
    T.StructField("INSRT_UPDT_CD", T.StringType(), False),
    T.StructField("DISCARD_IN", T.StringType(), False),
    T.StructField("PASS_THRU_IN", T.StringType(), False),
    T.StructField("FIRST_RECYC_DT", T.TimestampType(), False),
    T.StructField("ERR_CT", T.IntegerType(), False),
    T.StructField("RECYCLE_CT", T.DecimalType(38, 0), False),
    T.StructField("SRC_SYS_CD", T.StringType(), False),
    T.StructField("PRI_KEY_STRING", T.StringType(), False),
    T.StructField("AGNT_SK", T.IntegerType(), False),
    T.StructField("AGNT_ID", T.StringType(), False),
    T.StructField("CRT_RUN_CYC_EXTCN_SK", T.IntegerType(), False),
    T.StructField("LAST_UPDT_RUN_CYC_EXTCN_SK", T.IntegerType(), False),
    T.StructField("AGNT_INDV", T.StringType(), False),
    T.StructField("TAX_DMGRPHC", T.StringType(), False),
    T.StructField("AGNT_ADDR_MAIL_TYP_CD", T.StringType(), False),
    T.StructField("AGNT_ADDR_REMIT_TYP_CD", T.StringType(), False),
    T.StructField("AGNT_PD_AGNT_TYP_CD", T.StringType(), False),
    T.StructField("AGNT_PAYMT_METH_CD", T.StringType(), False),
    T.StructField("AGNT_TERM_RSN_CD", T.StringType(), False),
    T.StructField("AGNT_TYP_CD", T.StringType(), False),
    T.StructField("LAST_STMNT_FROM_DT", T.TimestampType(), False),
    T.StructField("LAST_STMNT_THRU_DT", T.TimestampType(), False),
    T.StructField("TERM_DT", T.TimestampType(), False),
    T.StructField("AGNT_UNIQ_KEY", T.IntegerType(), False),
    T.StructField("AGNT_INDV_ID", T.StringType(), False),
    T.StructField("AGNT_NM", T.StringType(), False),
    T.StructField("COCE_MCTR_VIP", T.StringType(), False)
])

df_IdsAgntExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IdsAgntExtr)
    .load(f"{adls_path}/key/{InFile}")
)

dfForeignKeyVars = (
    df_IdsAgntExtr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn(
        "svAgntTierCdString",
        F.when(
            F.substring(F.col("AGNT_ID"), 1, 4) == "0000",
            F.lit("THRD")
        )
        .when(
            F.col("COCE_MCTR_VIP").isNull() | (F.length(trim(F.col("COCE_MCTR_VIP"))) == 0),
            F.lit("NA")
        )
        .otherwise(F.col("COCE_MCTR_VIP"))
    )
    .withColumn("svAgntIndvSk", GetFkeyAgntIndv(F.col("SRC_SYS_CD"), F.col("AGNT_SK"), F.col("AGNT_INDV"), Logging))
    .withColumn("svTaxDmgrphcSk", GetFkeyTaxDmgrphc(F.col("SRC_SYS_CD"), F.col("AGNT_SK"), F.col("TAX_DMGRPHC"), Logging))
    .withColumn("svAddrMailSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("AGNT_SK"), F.lit("ADDRESS TYPE"), F.col("AGNT_ADDR_MAIL_TYP_CD"), Logging))
    .withColumn("svAddrRemitSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("AGNT_SK"), F.lit("ADDRESS TYPE"), F.col("AGNT_ADDR_REMIT_TYP_CD"), Logging))
    .withColumn("svAgntPdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("AGNT_SK"), F.lit("AGENT PAID AGENT TYPE"), F.col("AGNT_PD_AGNT_TYP_CD"), Logging))
    .withColumn("svAgntPayMethSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("AGNT_SK"), F.lit("AGENT PAYMENT METHOD"), F.col("AGNT_PAYMT_METH_CD"), Logging))
    .withColumn("svAgntTermSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("AGNT_SK"), F.lit("AGENT TERMINATION REASON"), F.col("AGNT_TERM_RSN_CD"), Logging))
    .withColumn("svAgntTypSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("AGNT_SK"), F.lit("GROUP AGENT ROLE"), F.col("AGNT_TYP_CD"), Logging))
    .withColumn("svAgntTierSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("AGNT_SK"), F.lit("AGENT TIER LEVEL"), F.col("svAgntTierCdString"), Logging))
    .withColumn("svFromDtSk", GetFkeyDate(F.lit("IDS"), F.col("AGNT_SK"), F.col("LAST_STMNT_FROM_DT"), Logging))
    .withColumn("svThruDtSk", GetFkeyDate(F.lit("IDS"), F.col("AGNT_SK"), F.col("LAST_STMNT_THRU_DT"), Logging))
    .withColumn("svTermDtSk", GetFkeyDate(F.lit("IDS"), F.col("AGNT_SK"), F.col("TERM_DT"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("AGNT_SK")))
)

dfFkey = (
    dfForeignKeyVars
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("AGNT_SK").alias("AGNT_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("AGNT_ID").alias("AGNT_ID"),
        F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svAgntIndvSk").alias("AGNT_INDV_SK"),
        F.col("svTaxDmgrphcSk").alias("TAX_DMGRPHC_SK"),
        F.col("svAddrMailSk").alias("AGNT_ADDR_MAIL_TYP_CD_SK"),
        F.col("svAddrRemitSk").alias("AGNT_ADDR_REMIT_TYP_CD_SK"),
        F.col("svAgntPdSk").alias("AGNT_PD_AGNT_TYP_CD_SK"),
        F.col("svAgntPayMethSk").alias("AGNT_PAYMT_METH_CD_SK"),
        F.col("svAgntTermSk").alias("AGNT_TERM_RSN_CD_SK"),
        F.col("svAgntTypSk").alias("AGNT_TYP_CD_SK"),
        F.col("svFromDtSk").alias("LAST_STMNT_FROM_DT_SK"),
        F.col("svThruDtSk").alias("LAST_STMNT_THRU_DT_SK"),
        F.col("svTermDtSk").alias("TERM_DT_SK"),
        F.col("AGNT_UNIQ_KEY").alias("AGNT_UNIQ_KEY"),
        F.col("AGNT_INDV_ID").alias("AGNT_INDV_ID"),
        F.col("AGNT_NM").alias("AGNT_NM"),
        F.col("svAgntTierSk").alias("AGNT_TIER_CD_SK")
    )
)

dfRecycle = (
    dfForeignKeyVars
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("AGNT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("AGNT_SK").alias("AGNT_SK"),
        F.col("AGNT_ID").alias("AGNT_ID"),
        F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
        F.col("AGNT_INDV").alias("AGNT_INDV"),
        F.col("TAX_DMGRPHC").alias("TAX_DMGRPHC"),
        F.col("AGNT_ADDR_MAIL_TYP_CD").alias("AGNT_ADDR_MAIL_TYP_CD"),
        F.col("AGNT_ADDR_REMIT_TYP_CD").alias("AGNT_ADDR_REMIT_TYP_CD"),
        F.col("AGNT_PD_AGNT_TYP_CD").alias("AGNT_PD_AGNT_TYP_CD"),
        F.col("AGNT_PAYMT_METH_CD").alias("AGNT_PAYMT_METH_CD"),
        F.col("AGNT_TERM_RSN_CD").alias("AGNT_TERM_RSN_CD"),
        F.col("AGNT_TYP_CD").alias("AGNT_TYP_CD"),
        F.col("LAST_STMNT_FROM_DT").alias("LAST_STMNT_FROM_DT"),
        F.col("LAST_STMNT_THRU_DT").alias("LAST_STMNT_THRU_DT"),
        F.col("TERM_DT").alias("TERM_DT"),
        F.col("AGNT_UNIQ_KEY").alias("AGNT_UNIQ_KEY"),
        F.col("AGNT_INDV_ID").alias("AGNT_INDV_ID"),
        F.col("AGNT_NM").alias("AGNT_NM"),
        F.when(
            F.col("COCE_MCTR_VIP").isNull() |
            (F.col("COCE_MCTR_VIP") == " ") |
            (F.col("COCE_MCTR_VIP") == "0"),
            F.lit(1)
        )
        .otherwise(F.col("svAgntTierSk"))
        .alias("AGNT_TIER_CD_SK")
    )
)

write_files(
    dfRecycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

dfDefaultUNKtemp = dfForeignKeyVars.limit(1)
dfDefaultUNK = dfDefaultUNKtemp.select(
    F.lit(0).alias("AGNT_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("AGNT_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("AGNT_INDV_SK"),
    F.lit(0).alias("TAX_DMGRPHC_SK"),
    F.lit(0).alias("AGNT_ADDR_MAIL_TYP_CD_SK"),
    F.lit(0).alias("AGNT_ADDR_REMIT_TYP_CD_SK"),
    F.lit(0).alias("AGNT_PD_AGNT_TYP_CD_SK"),
    F.lit(0).alias("AGNT_PAYMT_METH_CD_SK"),
    F.lit(0).alias("AGNT_TERM_RSN_CD_SK"),
    F.lit(0).alias("AGNT_TYP_CD_SK"),
    F.lit("UNK").alias("LAST_STMNT_FROM_DT_SK"),
    F.lit("UNK").alias("LAST_STMNT_THRU_DT_SK"),
    F.lit("UNK").alias("TERM_DT_SK"),
    F.lit(0).alias("AGNT_UNIQ_KEY"),
    F.lit("UNK").alias("AGNT_INDV_ID"),
    F.lit("UNK").alias("AGNT_NM"),
    F.lit(0).alias("AGNT_TIER_CD_SK")
)

dfDefaultNAtemp = dfForeignKeyVars.limit(1)
dfDefaultNA = dfDefaultNAtemp.select(
    F.lit(1).alias("AGNT_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("AGNT_ID"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("AGNT_INDV_SK"),
    F.lit(1).alias("TAX_DMGRPHC_SK"),
    F.lit(1).alias("AGNT_ADDR_MAIL_TYP_CD_SK"),
    F.lit(1).alias("AGNT_ADDR_REMIT_TYP_CD_SK"),
    F.lit(1).alias("AGNT_PD_AGNT_TYP_CD_SK"),
    F.lit(1).alias("AGNT_PAYMT_METH_CD_SK"),
    F.lit(1).alias("AGNT_TERM_RSN_CD_SK"),
    F.lit(1).alias("AGNT_TYP_CD_SK"),
    F.lit("NA").alias("LAST_STMNT_FROM_DT_SK"),
    F.lit("NA").alias("LAST_STMNT_THRU_DT_SK"),
    F.lit("NA").alias("TERM_DT_SK"),
    F.lit(-1).alias("AGNT_UNIQ_KEY"),
    F.lit("NA").alias("AGNT_INDV_ID"),
    F.lit("NA").alias("AGNT_NM"),
    F.lit(1).alias("AGNT_TIER_CD_SK")
)

dfCollector = (
    dfFkey.select(
        F.col("AGNT_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.col("AGNT_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("AGNT_INDV_SK"),
        F.col("TAX_DMGRPHC_SK"),
        F.col("AGNT_ADDR_MAIL_TYP_CD_SK"),
        F.col("AGNT_ADDR_REMIT_TYP_CD_SK"),
        F.col("AGNT_PD_AGNT_TYP_CD_SK"),
        F.col("AGNT_PAYMT_METH_CD_SK"),
        F.col("AGNT_TERM_RSN_CD_SK"),
        F.col("AGNT_TYP_CD_SK"),
        F.col("LAST_STMNT_FROM_DT_SK"),
        F.col("LAST_STMNT_THRU_DT_SK"),
        F.col("TERM_DT_SK"),
        F.col("AGNT_UNIQ_KEY"),
        F.col("AGNT_INDV_ID"),
        F.col("AGNT_NM"),
        F.col("AGNT_TIER_CD_SK").alias("AGNT_TIER")
    )
    .unionByName(
        dfDefaultUNK.select(
            F.col("AGNT_SK"),
            F.col("SRC_SYS_CD_SK"),
            F.col("AGNT_ID"),
            F.col("CRT_RUN_CYC_EXCTN_SK"),
            F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("AGNT_INDV_SK"),
            F.col("TAX_DMGRPHC_SK"),
            F.col("AGNT_ADDR_MAIL_TYP_CD_SK"),
            F.col("AGNT_ADDR_REMIT_TYP_CD_SK"),
            F.col("AGNT_PD_AGNT_TYP_CD_SK"),
            F.col("AGNT_PAYMT_METH_CD_SK"),
            F.col("AGNT_TERM_RSN_CD_SK"),
            F.col("AGNT_TYP_CD_SK"),
            F.col("LAST_STMNT_FROM_DT_SK"),
            F.col("LAST_STMNT_THRU_DT_SK"),
            F.col("TERM_DT_SK"),
            F.col("AGNT_UNIQ_KEY"),
            F.col("AGNT_INDV_ID"),
            F.col("AGNT_NM"),
            F.col("AGNT_TIER_CD_SK").alias("AGNT_TIER")
        )
    )
    .unionByName(
        dfDefaultNA.select(
            F.col("AGNT_SK"),
            F.col("SRC_SYS_CD_SK"),
            F.col("AGNT_ID"),
            F.col("CRT_RUN_CYC_EXCTN_SK"),
            F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("AGNT_INDV_SK"),
            F.col("TAX_DMGRPHC_SK"),
            F.col("AGNT_ADDR_MAIL_TYP_CD_SK"),
            F.col("AGNT_ADDR_REMIT_TYP_CD_SK"),
            F.col("AGNT_PD_AGNT_TYP_CD_SK"),
            F.col("AGNT_PAYMT_METH_CD_SK"),
            F.col("AGNT_TERM_RSN_CD_SK"),
            F.col("AGNT_TYP_CD_SK"),
            F.col("LAST_STMNT_FROM_DT_SK"),
            F.col("LAST_STMNT_THRU_DT_SK"),
            F.col("TERM_DT_SK"),
            F.col("AGNT_UNIQ_KEY"),
            F.col("AGNT_INDV_ID"),
            F.col("AGNT_NM"),
            F.col("AGNT_TIER_CD_SK").alias("AGNT_TIER")
        )
    )
)

dfCollectorFinal = (
    dfCollector
    .withColumn("AGNT_ID", F.rpad(F.col("AGNT_ID"), 10, " "))
    .withColumn("LAST_STMNT_FROM_DT_SK", F.rpad(F.col("LAST_STMNT_FROM_DT_SK"), 10, " "))
    .withColumn("LAST_STMNT_THRU_DT_SK", F.rpad(F.col("LAST_STMNT_THRU_DT_SK"), 10, " "))
    .withColumn("TERM_DT_SK", F.rpad(F.col("TERM_DT_SK"), 10, " "))
    .withColumn("AGNT_INDV_ID", F.rpad(F.col("AGNT_INDV_ID"), 12, " "))
    .withColumn("AGNT_NM", F.rpad(F.col("AGNT_NM"), 55, " "))
    .select(
        "AGNT_SK",
        "SRC_SYS_CD_SK",
        "AGNT_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "AGNT_INDV_SK",
        "TAX_DMGRPHC_SK",
        "AGNT_ADDR_MAIL_TYP_CD_SK",
        "AGNT_ADDR_REMIT_TYP_CD_SK",
        "AGNT_PD_AGNT_TYP_CD_SK",
        "AGNT_PAYMT_METH_CD_SK",
        "AGNT_TERM_RSN_CD_SK",
        "AGNT_TYP_CD_SK",
        "LAST_STMNT_FROM_DT_SK",
        "LAST_STMNT_THRU_DT_SK",
        "TERM_DT_SK",
        "AGNT_UNIQ_KEY",
        "AGNT_INDV_ID",
        "AGNT_NM",
        "AGNT_TIER"
    )
)

write_files(
    dfCollectorFinal,
    f"{adls_path}/load/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)