# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2012 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  BCAFEPMedClmLnExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Reads the BCAFepMedClm_Landing.dat file and runs through primary key using shared container ClmLnProcCdModPK
# MAGIC     
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                           Date                 	Project/Altiris #	       Change Description			      Development Project	  Code Reviewer	    Date Reviewed       
# MAGIC ------------------                       --------------------     	------------------------	-----------------------------------------------------------------------	--------------------------------	-------------------------------	    ---------------------------       
# MAGIC Sudhir Bomshetty               2017-09-27                    5781 HEDIS                   Initial Programming                                                    IntegrateDev2                  Kalyan Neelam            2017-10-18
# MAGIC 
# MAGIC Saikiran Mahenderker        2018-03-28                   5781 HEDIS                   Added new Columns                                                  IntegrateDev2                 Jaideep Mankal           04/02/2018
# MAGIC                                                                                                                       PERFORMING_NTNL_PROV_ID

# MAGIC Hash file (hf_clm_ln_proc_cd_mod_allcol) cleared in calling program
# MAGIC Split the different proc-mod fields into rows
# MAGIC Transforming BCA Fep ClmLn  to ClmLnProcCdMod
# MAGIC Writing Sequential File to ../key
# MAGIC Assign primary surrogate key
# MAGIC Apply business logic
# MAGIC This container is used in:
# MAGIC FctsClmLnProcCdModExtr
# MAGIC NascoClmLnProcCdModExtr
# MAGIC BCBSSCClmLnProcCdModExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC The primary-key string is made up here by concat-ing the ordinal-position field
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnProcCdModPK
# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_BCAFepClmLnLanding = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_NO", DecimalType(38,10), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("DOB", StringType(), True),
    StructField("GNDR_CD", StringType(), True),
    StructField("SUB_ID", StringType(), False),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("SRC_SYS", StringType(), False),
    StructField("RCRD_ID", StringType(), False),
    StructField("ADJ_NO", StringType(), True),
    StructField("PERFORMING_PROV_ID", StringType(), True),
    StructField("FEP_PROD", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("BILL_TYP__CD", StringType(), True),
    StructField("FEP_SVC_LOC_CD", StringType(), True),
    StructField("IP_CLM_TYP_IN", StringType(), True),
    StructField("DRG_VRSN_IN", StringType(), True),
    StructField("DRG_CD", StringType(), True),
    StructField("PATN_STTUS_CD", StringType(), True),
    StructField("CLM_CLS_IN", StringType(), True),
    StructField("CLM_DENIED_FLAG", StringType(), True),
    StructField("ILNS_DT", StringType(), True),
    StructField("IP_CLM_BEG_DT", StringType(), True),
    StructField("IP_CLM_DSCHG_DT", StringType(), True),
    StructField("CLM_SVC_DT_BEG", StringType(), True),
    StructField("CLM_SVC_DT_END", StringType(), True),
    StructField("FCLTY_CLM_STMNT_BEG_DT", StringType(), True),
    StructField("FCLTY_CLM_STMNT_END_DT", StringType(), True),
    StructField("CLM_PRCS_DT", StringType(), True),
    StructField("IP_ADMS_CT", StringType(), True),
    StructField("NO_COV_DAYS", IntegerType(), True),
    StructField("DIAG_CDNG_TYP", StringType(), True),
    StructField("PRI_DIAG_CD", StringType(), True),
    StructField("PRI_DIAG_POA_IN", StringType(), True),
    StructField("ADM_DIAG_CD", StringType(), True),
    StructField("ADM_DIAG_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_1", StringType(), True),
    StructField("OTHR_DIAG_CD_1_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_2", StringType(), True),
    StructField("OTHR_DIAG_CD_2_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_3", StringType(), True),
    StructField("OTHR_DIAG_CD_3_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_4", StringType(), True),
    StructField("OTHR_DIAG_CD_4_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_5", StringType(), True),
    StructField("OTHR_DIAG_CD_5_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_6", StringType(), True),
    StructField("OTHR_DIAG_CD_6_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_7", StringType(), True),
    StructField("OTHR_DIAG_CD_7_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_8", StringType(), True),
    StructField("OTHR_DIAG_CD_8_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_9", StringType(), True),
    StructField("OTHR_DIAG_CD_9_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_10", StringType(), True),
    StructField("OTHR_DIAG_CD_10_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_11", StringType(), True),
    StructField("OTHR_DIAG_CD_11_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_12", StringType(), True),
    StructField("OTHR_DIAG_CD_12_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_13", StringType(), True),
    StructField("OTHR_DIAG_CD_13_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_14", StringType(), True),
    StructField("OTHR_DIAG_CD_14_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_15", StringType(), True),
    StructField("OTHR_DIAG_CD_15_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_16", StringType(), True),
    StructField("OTHR_DIAG_CD_16_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_17", StringType(), True),
    StructField("OTHR_DIAG_CD_17_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_18", StringType(), True),
    StructField("OTHR_DIAG_CD_18_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_19", StringType(), True),
    StructField("OTHR_DIAG_CD_19_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_20", StringType(), True),
    StructField("OTHR_DIAG_CD_20_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_21", StringType(), True),
    StructField("OTHR_DIAG_CD_21_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_22", StringType(), True),
    StructField("OTHR_DIAG_CD_22_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_23", StringType(), True),
    StructField("OTHR_DIAG_CD_23_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_24", StringType(), True),
    StructField("OTHR_DIAG_CD_24_POA_IN", StringType(), True),
    StructField("PROC_CDNG_TYP", StringType(), True),
    StructField("PRINCIPLE_PROC_CD", StringType(), True),
    StructField("PRINCIPLE_PROC_CD_DT", StringType(), True),
    StructField("OTHR_PROC_CD_1", StringType(), True),
    StructField("OTHR_PROC_CD_1_DT", StringType(), True),
    StructField("OTHR_PROC_CD_2", StringType(), True),
    StructField("OTHR_PROC_CD_2_DT", StringType(), True),
    StructField("OTHR_PROC_CD_3", StringType(), True),
    StructField("OTHR_PROC_CD_3_DT", StringType(), True),
    StructField("OTHR_PROC_CD_4", StringType(), True),
    StructField("OTHR_PROC_CD_4_DT", StringType(), True),
    StructField("OTHR_PROC_CD_5", StringType(), True),
    StructField("OTHR_PROC_CD_5_DT", StringType(), True),
    StructField("OTHR_PROC_CD_6", StringType(), True),
    StructField("OTHR_PROC_CD_6_DT", StringType(), True),
    StructField("OTHR_PROC_CD_7", StringType(), True),
    StructField("OTHR_PROC_CD_7_DT", StringType(), True),
    StructField("OTHR_PROC_CD_8", StringType(), True),
    StructField("OTHR_PROC_CD_8_DT", StringType(), True),
    StructField("OTHR_PROC_CD_9", StringType(), True),
    StructField("OTHR_PROC_CD_9_DT", StringType(), True),
    StructField("OTHR_PROC_CD_10", StringType(), True),
    StructField("OTHR_PROC_CD_10_DT", StringType(), True),
    StructField("OTHR_PROC_CD_11", StringType(), True),
    StructField("OTHR_PROC_CD_11_DT", StringType(), True),
    StructField("OTHR_PROC_CD_12", StringType(), True),
    StructField("OTHR_PROC_CD_12_DT", StringType(), True),
    StructField("OTHR_PROC_CD_13", StringType(), True),
    StructField("OTHR_PROC_CD_13_DT", StringType(), True),
    StructField("OTHR_PROC_CD_14", StringType(), True),
    StructField("OTHR_PROC_CD_14_DT", StringType(), True),
    StructField("OTHR_PROC_CD_15", StringType(), True),
    StructField("OTHR_PROC_CD_15_DT", StringType(), True),
    StructField("OTHR_PROC_CD_16", StringType(), True),
    StructField("OTHR_PROC_CD_16_DT", StringType(), True),
    StructField("OTHR_PROC_CD_17", StringType(), True),
    StructField("OTHR_PROC_CD_17_DT", StringType(), True),
    StructField("OTHR_PROC_CD_18", StringType(), True),
    StructField("OTHR_PROC_CD_18_DT", StringType(), True),
    StructField("OTHR_PROC_CD_19", StringType(), True),
    StructField("OTHR_PROC_CD_19_DT", StringType(), True),
    StructField("OTHR_PROC_CD_20", StringType(), True),
    StructField("OTHR_PROC_CD_20_DT", StringType(), True),
    StructField("OTHR_PROC_CD_21", StringType(), True),
    StructField("OTHR_PROC_CD_21_DT", StringType(), True),
    StructField("OTHR_PROC_CD_22", StringType(), True),
    StructField("OTHR_PROC_CD_22_DT", StringType(), True),
    StructField("OTHR_PROC_CD_23", StringType(), True),
    StructField("OTHR_PROC_CD_23_DT", StringType(), True),
    StructField("OTHR_PROC_CD_24", StringType(), True),
    StructField("OTHR_PROC_CD_24_DT", StringType(), True),
    StructField("RVNU_CD", StringType(), True),
    StructField("PROC_CD_NON_ICD", StringType(), True),
    StructField("PROC_CD_MOD_1", StringType(), True),
    StructField("PROC_CD_MOD_2", StringType(), True),
    StructField("PROC_CD_MOD_3", StringType(), True),
    StructField("PROC_CD_MOD_4", StringType(), True),
    StructField("CLM_UNIT", DecimalType(38,10), True),
    StructField("CLM_LN_TOT_ALL_SVC_CHRG_AMT", DecimalType(38,10), True),
    StructField("CLM_LN_PD_AMT", DecimalType(38,10), True),
    StructField("CLM_ALT_AMT_PD_1", DecimalType(38,10), True),
    StructField("CLM_ALT_AMT_PD_2", DecimalType(38,10), True),
    StructField("LOINC_CD", StringType(), True),
    StructField("CLM_TST_RSLT", StringType(), True),
    StructField("ALT_CLM_TST_RSLT", StringType(), True),
    StructField("DRUG_CD", StringType(), True),
    StructField("DRUG_CLM_INCUR_DT", StringType(), True),
    StructField("CLM_TREAT_DURATN", StringType(), True),
    StructField("DATA_SRC", StringType(), True),
    StructField("SUPLMT_DATA_SRC_TYP", StringType(), True),
    StructField("ADTR_APRV_STTUS", StringType(), True),
    StructField("RUN_DT", StringType(), True),
    StructField("PERFORMING_NTNL_PROV_ID", StringType(), True)
])

df_BCAFepClmLnLanding = (
    spark.read
    .schema(schema_BCAFepClmLnLanding)
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .csv(f"{adls_path}/verified/BCAFEPCMedClm_ClmLnlanding.dat.{RunID}")
)

df_businessRules = (
    df_BCAFepClmLnLanding
    .withColumn(
        "PKString",
        F.concat(
            F.lit(SrcSysCd),
            F.lit(";"),
            F.col("CLM_ID"),
            F.lit(";"),
            F.col("CLM_LN_NO").cast(StringType())
        )
    )
    .withColumn(
        "ProcCdMod1",
        F.when(
            (F.trim(F.col("PROC_CD_MOD_1")) == "?")
            | (F.col("PROC_CD_MOD_1").isNull())
            | (F.length(F.trim(F.col("PROC_CD_MOD_1"))) == 0),
            F.lit(0)
        ).otherwise(F.lit(1))
    )
    .withColumn(
        "ProcCdMod2",
        F.when(
            (F.trim(F.col("PROC_CD_MOD_2")) == "?")
            | (F.col("PROC_CD_MOD_2").isNull())
            | (F.length(F.trim(F.col("PROC_CD_MOD_2"))) == 0),
            F.lit(0)
        ).otherwise(F.lit(2))
    )
    .withColumn(
        "ProcCdMod3",
        F.when(
            (F.trim(F.col("PROC_CD_MOD_3")) == "?")
            | (F.col("PROC_CD_MOD_3").isNull())
            | (F.length(F.trim(F.col("PROC_CD_MOD_3"))) == 0),
            F.lit(0)
        ).otherwise(F.lit(3))
    )
    .withColumn(
        "ProcCdMod4",
        F.when(
            (F.trim(F.col("PROC_CD_MOD_4")) == "?")
            | (F.col("PROC_CD_MOD_4").isNull())
            | (F.length(F.trim(F.col("PROC_CD_MOD_4"))) == 0),
            F.lit(0)
        ).otherwise(F.lit(4))
    )
    .withColumn(
        "svProcMod2",
        F.when(
            (F.col("ProcCdMod1") == 0) & (F.col("ProcCdMod2") != 0),
            F.lit(1)
        ).otherwise(
            F.when(
                (F.col("ProcCdMod1") == 0) & (F.col("ProcCdMod2") == 0),
                F.lit(0)
            ).otherwise(F.lit(2))
        )
    )
    .withColumn(
        "svProcMod3",
        F.when(
            F.col("ProcCdMod3") == 0,
            F.lit(0)
        ).otherwise(
            F.when(
                F.col("svProcMod2") == 0,
                F.lit(1)
            ).otherwise(
                F.when(
                    F.col("svProcMod2") == 1,
                    F.lit(2)
                ).otherwise(F.lit(3))
            )
        )
    )
    .withColumn(
        "svProcMod4",
        F.when(
            F.col("ProcCdMod4") == 0,
            F.lit(0)
        ).otherwise(
            F.when(
                F.col("svProcMod3") == 0,
                F.lit(1)
            ).otherwise(
                F.when(
                    F.col("svProcMod3") == 1,
                    F.lit(2)
                ).otherwise(
                    F.when(
                        F.col("svProcMod3") == 2,
                        F.lit(3)
                    ).otherwise(F.lit(4))
                )
            )
        )
    )
)

df_lnkPcmIn1 = (
    df_businessRules
    .filter(F.col("ProcCdMod1") != 0)
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(F.col("PKString"), F.lit("1")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
        F.rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID"),
        F.col("CLM_LN_NO").alias("CLM_LN_SEQ_NO"),
        F.lit(1).alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PROC_CD_MOD_1").alias("PROC_CD_MOD_CD")
    )
)

df_lnkPcmIn2 = (
    df_businessRules
    .filter(F.col("ProcCdMod2") != 0)
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(F.col("PKString"), F.col("svProcMod2").cast(StringType())).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
        F.rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID"),
        F.col("CLM_LN_NO").alias("CLM_LN_SEQ_NO"),
        F.col("svProcMod2").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PROC_CD_MOD_2").alias("PROC_CD_MOD_CD")
    )
)

df_lnkPcmIn3 = (
    df_businessRules
    .filter(F.col("ProcCdMod3") != 0)
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(F.col("PKString"), F.col("svProcMod3").cast(StringType())).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
        F.rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID"),
        F.col("CLM_LN_NO").alias("CLM_LN_SEQ_NO"),
        F.col("svProcMod3").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PROC_CD_MOD_3").alias("PROC_CD_MOD_CD")
    )
)

df_lnkPcmIn4 = (
    df_businessRules
    .filter(F.col("ProcCdMod4") != 0)
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(F.col("PKString"), F.col("svProcMod4").cast(StringType())).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
        F.rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID"),
        F.col("CLM_LN_NO").alias("CLM_LN_SEQ_NO"),
        F.col("svProcMod4").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PROC_CD_MOD_4").alias("PROC_CD_MOD_CD")
    )
)

df_Collector = (
    df_lnkPcmIn1
    .unionByName(df_lnkPcmIn2)
    .unionByName(df_lnkPcmIn3)
    .unionByName(df_lnkPcmIn4)
)

df_SnapshotAllCol = (
    df_Collector
    .select(
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
        F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ERR_CT").alias("ERR_CT"),
        F.col("RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("CLM_LN_PROC_CD_MOD_SK").alias("CLM_LN_PROC_CD_MOD_SK"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PROC_CD_MOD_CD").alias("PROC_CD_MOD_CD")
    )
)

df_SnapshotOnly = (
    df_Collector
    .select(
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD")
    )
)

df_TransformOnly = (
    df_Collector
    .select(
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD")
    )
)

params_ClmLnProcCdModPK = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd
}
df_Key = ClmLnProcCdModPK(df_TransformOnly, df_SnapshotAllCol, params_ClmLnProcCdModPK)

df_Key_final = df_Key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_LN_PROC_CD_MOD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROC_CD_MOD_CD")
)

df_Key_final_padded = (
    df_Key_final
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))
)

write_files(
    df_Key_final_padded,
    f"{adls_path}/key/BCAFEPMedClmLnProcCdModExtr.BCAFEPMedClmLnProcCdMod.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_SnapshotOnly_forTransformer = df_SnapshotOnly.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD")
)

df_Transformer = (
    df_SnapshotOnly_forTransformer
    .withColumn(
        "svOrdnlCdSk",
        GetFkeyCodes(
            SrcSysCd,
            F.lit(0),
            F.lit("PROCEDURE ORDINAL"),
            F.trim(F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD")),
            F.lit("X")
        )
    )
)

df_RowCount = df_Transformer.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("svOrdnlCdSk").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK")
)

df_RowCount_final = df_RowCount.select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_PROC_CD_MOD_ORDNL_CD_SK"
)

write_files(
    df_RowCount_final,
    f"{adls_path}/load/B_CLM_LN_PROC_CD_MOD.BCAFEP.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)