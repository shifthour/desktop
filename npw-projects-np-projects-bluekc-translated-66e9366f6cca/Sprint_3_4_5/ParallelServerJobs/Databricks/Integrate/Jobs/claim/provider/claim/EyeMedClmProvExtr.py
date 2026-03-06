# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY : EyeMedClmExtrSeq
# MAGIC 
# MAGIC PROCESSING : Reads the EyeMed Land file and puts the data into the claim  provider common record format and runs through primary key using Shared container ClmProvPkey
# MAGIC 
# MAGIC Developer                    Date         Project #                     Change Description                                        Development Project          Code Reviewer               Date Reviewed  
# MAGIC ------------------           ------------------      ----------------                  --------------------------------------------------------------         ------------------------------------       ----------------------------            ----------------
# MAGIC Madhavan B         2017-09-28      5744                           Initial Programming                                          IntegrateDev2                    Kalyan Neelam                2018-04-04
# MAGIC 
# MAGIC Goutham K         2021-06-02         US-366403            New Provider file Change to include Loc
# MAGIC                                                                                           and Svc loc id   to the PROV_ID                 IntegrateDev1                     Jeyaprasanna                  2021-06-08

# MAGIC EyeMed Claim Provider Extract
# MAGIC This container is used in:
# MAGIC ArgusClmProvExtr
# MAGIC PCSClmProvExtr
# MAGIC NascoClmProvExtr
# MAGIC FctsClmProvExtr
# MAGIC WellDyneClmProvExtr
# MAGIC MCSourceClmProvExtr
# MAGIC MedicaidClmProvExtr
# MAGIC MedtrakClmProvExtr
# MAGIC BCBSSCClmProvExtr
# MAGIC BCBSSCMedClmProvExtr
# MAGIC BCAClmProvExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Writing Sequential File to /key
# MAGIC The file is created in the EyeMedClmPreProcExtr job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmProvPK
# COMMAND ----------

schema_EyeMedClmLand = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_NO", DecimalType(38,10), True),
    StructField("ADJ_FROM_CLM_ID", StringType(), True),
    StructField("ADJ_TO_CLM_ID", StringType(), True),
    StructField("CLM_STTUS_CD", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("GNDR_CD", StringType(), True),
    StructField("SUB_ID_IDS", StringType(), True),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("RCRD_TYP", StringType(), True),
    StructField("ADJ_VOID_FLAG", StringType(), True),
    StructField("EYEMED_GRP_ID", StringType(), True),
    StructField("EYEMED_SUBGRP_ID", StringType(), True),
    StructField("BILL_TYP_IN", StringType(), True),
    StructField("CLM_NO", StringType(), True),
    StructField("LN_CTR", StringType(), True),
    StructField("DT_OF_SVC", StringType(), True),
    StructField("INVC_NO", StringType(), True),
    StructField("INVC_DT", StringType(), True),
    StructField("BILL_AMT", StringType(), True),
    StructField("FFS_ADM_FEE", StringType(), True),
    StructField("RTL_AMT", StringType(), True),
    StructField("MBR_OOP", StringType(), True),
    StructField("3RD_PARTY_DSCNT", StringType(), True),
    StructField("COPAY_AMT", StringType(), True),
    StructField("COV_AMT", StringType(), True),
    StructField("FLR_1", StringType(), True),
    StructField("FLR_2", StringType(), True),
    StructField("NTWK_IN", StringType(), True),
    StructField("SVC_CD", StringType(), True),
    StructField("SVC_DESC", StringType(), True),
    StructField("MOD_CD_1", StringType(), True),
    StructField("MOD_CD_2", StringType(), True),
    StructField("MOD_CD_3", StringType(), True),
    StructField("MOD_CD_4", StringType(), True),
    StructField("MOD_CD_5", StringType(), True),
    StructField("MOD_CD_6", StringType(), True),
    StructField("MOD_CD_7", StringType(), True),
    StructField("MOD_CD_8", StringType(), True),
    StructField("ICD_CD_SET", StringType(), True),
    StructField("DIAG_CD_1", StringType(), True),
    StructField("DIAG_CD_2", StringType(), True),
    StructField("DIAG_CD_3", StringType(), True),
    StructField("DIAG_CD_4", StringType(), True),
    StructField("DIAG_CD_5", StringType(), True),
    StructField("DIAG_CD_6", StringType(), True),
    StructField("DIAG_CD_7", StringType(), True),
    StructField("DIAG_CD_8", StringType(), True),
    StructField("PATN_ID", StringType(), True),
    StructField("PATN_SSN", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_MIDINIT", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("PATN_GNDR", StringType(), True),
    StructField("PATN_FMLY_RELSHP", StringType(), True),
    StructField("PATN_DOB", StringType(), True),
    StructField("PATN_ADDR", StringType(), True),
    StructField("PATN_ADDR_2", StringType(), True),
    StructField("PATN_CITY", StringType(), True),
    StructField("PATN_ST", StringType(), True),
    StructField("PATN_ZIP", StringType(), True),
    StructField("PATN_ZIP4", StringType(), True),
    StructField("CLNT_GRP_NO", StringType(), True),
    StructField("CO_CD", StringType(), True),
    StructField("DIV_CD", StringType(), True),
    StructField("LOC_CD", StringType(), True),
    StructField("CLNT_RPTNG_1", StringType(), True),
    StructField("CLNT_RPTNG_2", StringType(), True),
    StructField("CLNT_RPTNG_3", StringType(), True),
    StructField("CLNT_RPTNG_4", StringType(), True),
    StructField("CLNT_RPTNG_5", StringType(), True),
    StructField("CLS_PLN_ID", StringType(), True),
    StructField("SUB_ID", StringType(), True),
    StructField("SUB_SSN", StringType(), True),
    StructField("SUB_FIRST_NM", StringType(), True),
    StructField("SUB_MIDINIT", StringType(), True),
    StructField("SUB_LAST_NM", StringType(), True),
    StructField("SUB_GNDR", StringType(), True),
    StructField("SUB_DOB", StringType(), True),
    StructField("SUB_ADDR", StringType(), True),
    StructField("SUB_ADDR_2", StringType(), True),
    StructField("SUB_CITY", StringType(), True),
    StructField("SUB_ST", StringType(), True),
    StructField("SUB_ZIP", StringType(), True),
    StructField("SUB_ZIP_PLUS_4", StringType(), True),
    StructField("PROV_ID", StringType(), True),
    StructField("PROV_NPI", StringType(), True),
    StructField("TAX_ENTY_NPI", StringType(), True),
    StructField("PROV_FIRST_NM", StringType(), True),
    StructField("PROV_LAST_NM", StringType(), True),
    StructField("BUS_NM", StringType(), True),
    StructField("PROV_ADDR", StringType(), True),
    StructField("PROV_ADDR_2", StringType(), True),
    StructField("PROV_CITY", StringType(), True),
    StructField("PROV_ST", StringType(), True),
    StructField("PROV_ZIP", StringType(), True),
    StructField("PROV_ZIP_PLUS_4", StringType(), True),
    StructField("PROF_DSGTN", StringType(), True),
    StructField("TAX_ENTY_ID", StringType(), True),
    StructField("TXNMY_CD", StringType(), True),
    StructField("CLM_RCVD_DT", StringType(), True),
    StructField("ADJDCT_DT", StringType(), True),
    StructField("CHK_DT", StringType(), True),
    StructField("DENIAL_RSN_CD", StringType(), True),
    StructField("SVC_TYP", StringType(), True),
    StructField("UNIT_OF_SVC", StringType(), True),
    StructField("LOC_ID", StringType(), True),
    StructField("SVC_LOC_ID", StringType(), True),
    StructField("FLR", StringType(), True)
])

df_EyeMedClmLand = (
    spark.read.csv(
        path=f"{adls_path}/verified/EyeMedClm_ClaimsLanding.dat.{RunID}",
        header=False,
        schema=schema_EyeMedClmLand,
        sep=",",
        quote="\""
    )
)

df_Snapshot_input = df_EyeMedClmLand.alias("EyeMed")

df_Snapshot8AllCol = df_Snapshot_input.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("EyeMed.CLM_ID").alias("CLM_ID"),
    F.lit("SVC").alias("CLM_PROV_ROLE_TYP_CD"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("EyeMed.CLM_ID"), F.lit(";"), F.lit("SVC")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_PROV_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        (F.col("EyeMed.PROV_ID").isNull()) | (F.length(trim(F.col("EyeMed.PROV_ID"))) == 0),
        "NA"
    ).otherwise(
        F.concat(
            trim(F.col("EyeMed.PROV_ID")),
            trim(F.col("EyeMed.LOC_ID")),
            trim(F.col("EyeMed.SVC_LOC_ID")),
            F.substring(
                trim(F.col("EyeMed.TAX_ENTY_ID")),
                F.length(trim(F.col("EyeMed.TAX_ENTY_ID"))) - 3,
                4
            )
        )
    ).alias("PROV_ID"),
    F.when(
        (F.col("EyeMed.TAX_ENTY_ID").isNull()) | (F.length(trim(F.col("EyeMed.TAX_ENTY_ID"))) == 0),
        "NA"
    ).otherwise(
        trim(F.col("EyeMed.TAX_ENTY_ID"))
    ).alias("TAX_ID"),
    F.lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID"),
    F.lit("NA").alias("NTNL_PROV_ID")
)

df_Snapshot8Transform = df_Snapshot_input.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("EyeMed.CLM_ID").alias("CLM_ID"),
    F.lit("SVC").alias("CLM_PROV_ROLE_TYP_CD")
)

df_Snapshot8SnapShot = df_Snapshot_input.select(
    F.col("EyeMed.CLM_ID").alias("CLM_ID"),
    F.lit("SVC").alias("CLM_PROV_ROLE_TYP_CD"),
    F.when(
        (F.col("EyeMed.PROV_ID").isNull()) | (F.length(trim(F.col("EyeMed.PROV_ID"))) == 0),
        "NA"
    ).otherwise(
        trim(F.col("EyeMed.PROV_ID"))
    ).alias("PROV_ID")
)

params_ClmProvPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}

df_ClmProvPK_output = ClmProvPK(df_Snapshot8AllCol, df_Snapshot8Transform, params_ClmProvPK)

df_EyeMedClmProvExtr = df_ClmProvPK_output.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_PROV_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_PROV_ROLE_TYP_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_ID",
    "TAX_ID",
    "SVC_FCLTY_LOC_NTNL_PROV_ID",
    "NTNL_PROV_ID"
).withColumn(
    "INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " ")
).withColumn(
    "DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " ")
).withColumn(
    "PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " ")
).withColumn(
    "TAX_ID", F.rpad(F.col("TAX_ID"), 9, " ")
)

write_files(
    df_EyeMedClmProvExtr,
    f"{adls_path}/key/EyeMedClmProvExtr.EyeMedClmProv.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parqet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Transformer_input = df_Snapshot8SnapShot

df_Transformer = df_Transformer_input.withColumn(
    "CLM_PROV_ROLE_TYP_CD_SK",
    GetFkeyCodes(
        SrcSysCd,
        F.lit(0),
        F.lit("CLAIM PROVIDER ROLE TYPE"),
        F.col("CLM_PROV_ROLE_TYP_CD"),
        F.lit("X")
    )
)

df_Transformer_output = df_Transformer.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD_SK").alias("CLM_PROV_ROLE_TYP_CD_SK"),
    F.col("PROV_ID").alias("PROV_ID")
)

write_files(
    df_Transformer_output.select(
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_PROV_ROLE_TYP_CD_SK",
        "PROV_ID"
    ),
    f"{adls_path}/load/B_CLM_PROV.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parqet=False,
    header=False,
    quote="\"",
    nullValue=None
)