# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME: IdsMpiIndvBeKeyXWalkExtr
# MAGIC 
# MAGIC CALLED BY:  MpiBcbsExtrSeq
# MAGIC 
# MAGIC PROCESSING:  This is a daily process. These records are written to the MPI BCBS Extension Database from the MPI tool and provides any existing BE_KEY's that have received a new BE_KEY.
# MAGIC 
# MAGIC DEPENDENCIES: MPI Tool process complete and files outputted. MPI FILE ---> MPI BCBS Extension : Membership : INDV_BE_CRSWALK job completed.
# MAGIC 
# MAGIC MODIFICATIONS:             
# MAGIC                                                                                                                                                                                    
# MAGIC 
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Abhiram Dasarathy            07-19-2012      4426 - MPI                     Original Programming                                                                            IntegrateWrhsDevl       Bhoomi Dasari            08/16/2012 
# MAGIC Sharon Andrew                  2012-10-11                                         added stage variables svProcessOwnerId and svSrcSysId                     IntegrateWrhsDevl       Bhoomi Dasari            10/16/2012  
# MAGIC                                                                                                      changed natural key to Prinary Key file
# MAGIC                                                                                                      changed order of fields written to balancing file as it was not in synch to the physical table
# MAGIC                                                                                                     changed source field used to populate Transform.PRCS_OWNER_SRC_SYS_ENVRN_ID from mpi.ENV_ID to mpi_id_lkup.PRCS_OWNER_SRC_SYS_ENVRN_ID
# MAGIC                                                                                                       moved transformation rules for fields written to both the balancing file and the output file to the place where the balancing file was being written to - fields would not be in synch and never balance
# MAGIC 
# MAGIC Abhiram Dasarathy	          2012-12-27      4426 - MPI 	     Added to fields MBR_SK and MBR_UNIQ_KEY to the sequential file     IntegrateNewDevl          Bhoomi Dasari            01/14/2013
# MAGIC 						     Added the MBR Lookup hash file.                                                                 
# MAGIC Sharon Andrew                  2013-01-17                                        Removed IDS parms being passed to MpiBCBSIndvBeKeyXWalkExtr    IntegrateNewDevl           Bhoomi Dasari            01/24/2013
# MAGIC 						    in MpiBcbsBekeyExtrSeq 	
# MAGIC 
# MAGIC Goutham Kalidindi             2023-07027       US-590099		  Updated Extract SQL to EXCLUDE data WHERE                                      IntegrateDev2 Reddy Sanam          07/28/2023
# MAGIC                                                                                                    PRCS_OWNER_SRC_SYS_ENVRN_ID = 'BLUESC' 
# MAGIC                                                                                                     added new parameter to pass the EXLUDE Value

# MAGIC Process owner source environment ID lookup
# MAGIC Apply primary key logic
# MAGIC Create hash file lookup
# MAGIC Apply business transforms
# MAGIC IDS MPI INDV CROSSWALK Extract job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, length, concat, to_date
from pyspark.sql.types import IntegerType, StringType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
MPI_BCBSOwner = get_widget_value('MPI_BCBSOwner','')
mpi_bcbs_secret_name = get_widget_value('mpi_bcbs_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
PrevPrcRunDtm = get_widget_value('PrevPrcRunDtm','')
CurrDtm = get_widget_value('CurrDtm','')
CurrDate = get_widget_value('CurrDate','')
MPI_BCBSEnvrnID = get_widget_value('MPI_BCBSEnvrnID','')
SrcSysExclude = get_widget_value('SrcSysExclude','')

# CODBCStage: INDV_BE_CRSWALK
jdbc_url_indv, jdbc_props_indv = get_db_config(mpi_bcbs_secret_name)
extract_query_indv = (
    f"SELECT A.MPI_MBR_ID, A.PRCS_OWNER_SRC_SYS_ENVRN_CK, A.PRCS_RUN_DTM, A.PREV_INDV_BE_KEY, A.NEW_INDV_BE_KEY, B.ENVRN_ID "
    f"FROM {MPI_BCBSOwner}.INDV_BE_CRSWALK A, {MPI_BCBSOwner}.PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK B "
    f"WHERE A.PRCS_OWNER_SRC_SYS_ENVRN_CK = B.PRCS_OWNER_SRC_SYS_ENVRN_CK "
    f"AND A.PRCS_RUN_DTM > '{PrevPrcRunDtm}' "
    f"AND B.PRCS_OWNER_SRC_SYS_ENVRN_ID NOT IN ({SrcSysExclude})"
)
df_INDV_BE_CRSWALK = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_indv)
    .options(**jdbc_props_indv)
    .option("query", extract_query_indv)
    .load()
)
df_INDV_BE_CRSWALK = df_INDV_BE_CRSWALK.select(
    col("MPI_MBR_ID").alias("INDV_BE_CRSWALK.MPI_MBR_ID"),
    col("PRCS_OWNER_SRC_SYS_ENVRN_CK").alias("INDV_BE_CRSWALK.PRCS_OWNER_SRC_SYS_ENVRN_CK"),
    col("PRCS_RUN_DTM").alias("INDV_BE_CRSWALK.PRCS_RUN_DTM"),
    col("PREV_INDV_BE_KEY").alias("INDV_BE_CRSWALK.PREV_INDV_BE_KEY"),
    col("NEW_INDV_BE_KEY").alias("INDV_BE_CRSWALK.NEW_INDV_BE_KEY"),
    col("ENVRN_ID").alias("PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK.ENVRN_ID")
)

# CODBCStage: PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK
jdbc_url_prcs, jdbc_props_prcs = get_db_config(mpi_bcbs_secret_name)
extract_query_prcs = (
    f"SELECT PRCS_OWNER_SRC_SYS_ENVRN_CK, PRCS_OWNER_SRC_SYS_ENVRN_ID, PRCS_OWNER_ID, SRC_SYS_ID, ENVRN_ID "
    f"FROM {MPI_BCBSOwner}.PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK"
)
df_PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_prcs)
    .options(**jdbc_props_prcs)
    .option("query", extract_query_prcs)
    .load()
)
df_PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK = df_PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK.select(
    col("PRCS_OWNER_SRC_SYS_ENVRN_CK").alias("PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK.PRCS_OWNER_SRC_SYS_ENVRN_CK"),
    col("PRCS_OWNER_SRC_SYS_ENVRN_ID").alias("PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK.PRCS_OWNER_SRC_SYS_ENVRN_ID"),
    col("PRCS_OWNER_ID").alias("PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK.PROCESS_OWNER_ID"),
    col("SRC_SYS_ID").alias("PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK.SRC_SYS_ID"),
    col("ENVRN_ID").alias("PRCS_OWNER_SRC_SYS_ENVRN_CRSWALK.ENVRN_ID")
)

# CHashedFileStage (scenario C): hf_etrnl_mbr_uniq_key
df_hf_etrnl_mbr_uniq_key = spark.read.parquet(f"{adls_path}/hf_etrnl_mbr_uniq_key.parquet")
df_hf_etrnl_mbr_uniq_key = df_hf_etrnl_mbr_uniq_key.select(
    col("MBR_UNIQ_KEY").alias("mbr_uniq_key_lkup.MBR_UNIQ_KEY"),
    col("LOAD_RUN_CYCLE").alias("mbr_uniq_key_lkup.LOAD_RUN_CYCLE"),
    col("MBR_ID").alias("mbr_uniq_key_lkup.MBR_ID"),
    col("MBR_SK").alias("mbr_uniq_key_lkup.MBR_SK"),
    col("SUBGRP_SK").alias("mbr_uniq_key_lkup.SUBGRP_SK"),
    col("SUBGRP_ID").alias("mbr_uniq_key_lkup.SUBGRP_ID"),
    col("SUB_SK").alias("mbr_uniq_key_lkup.SUB_SK"),
    col("SUB_ID").alias("mbr_uniq_key_lkup.SUB_ID"),
    col("MBR_SFX_NO").alias("mbr_uniq_key_lkup.MBR_SFX_NO"),
    col("MBR_GNDR_CD_SK").alias("mbr_uniq_key_lkup.MBR_GNDR_CD_SK"),
    col("MBR_GNDR_CD").alias("mbr_uniq_key_lkup.MBR_GNDR_CD"),
    col("BRTH_DT_SK").alias("mbr_uniq_key_lkup.BRTH_DT_SK"),
    col("DCSD_DT_SK").alias("mbr_uniq_key_lkup.DCSD_DT_SK"),
    col("ORIG_EFF_DT_SK").alias("mbr_uniq_key_lkup.ORIG_EFF_DT_SK"),
    col("TERM_DT_SK").alias("mbr_uniq_key_lkup.TERM_DT_SK"),
    col("INDV_BE_KEY").alias("mbr_uniq_key_lkup.INDV_BE_KEY"),
    col("FIRST_NM").alias("mbr_uniq_key_lkup.FIRST_NM"),
    col("MIDINIT").alias("mbr_uniq_key_lkup.MIDINIT"),
    col("LAST_NM").alias("mbr_uniq_key_lkup.LAST_NM"),
    col("SSN").alias("mbr_uniq_key_lkup.SSN"),
    col("SUB_UNIQ_KEY").alias("mbr_uniq_key_lkup.SUB_UNIQ_KEY"),
    col("GRP_SK").alias("mbr_uniq_key_lkup.GRP_SK"),
    col("GRP_ID").alias("mbr_uniq_key_lkup.GRP_ID"),
    col("MBR_RELSHP_CD_SK").alias("mbr_uniq_key_lkup.MBR_RELSHP_CD_SK"),
    col("MBR_RELSHP_CD").alias("mbr_uniq_key_lkup.MBR_RELSHP_CD"),
    col("CLNT_ID").alias("mbr_uniq_key_lkup.CLNT_ID"),
    col("CLNT_NM").alias("mbr_uniq_key_lkup.CLNT_NM")
)

# CHashedFileStage (scenario C): hf_mpi_idsmpiindvbekeyxwalkextr_id
df_hf_mpi_idsmpiindvbekeyxwalkextr_id = spark.read.parquet(f"{adls_path}/hf_mpi_idsmpiindvbekeyxwalkextr_id.parquet")
df_hf_mpi_idsmpiindvbekeyxwalkextr_id = df_hf_mpi_idsmpiindvbekeyxwalkextr_id.select(
    col("PRCS_OWNER_SRC_SYS_ENVRN_CK").alias("mpi_id_lkup.PRCS_OWNER_SRC_SYS_ENVRN_CK"),
    col("PRCS_OWNER_SRC_SYS_ENVRN_ID").alias("mpi_id_lkup.PRCS_OWNER_SRC_SYS_ENVRN_ID"),
    col("PROCESS_OWNER_ID").alias("mpi_id_lkup.PROCESS_OWNER_ID"),
    col("SRC_SYS_ID").alias("mpi_id_lkup.SRC_SYS_ID"),
    col("ENVRN_ID").alias("mpi_id_lkup.ENVRN_ID")
)

# CHashedFileStage B scenario: hf_lkup_mpi_crswalk / hf_mpi_crswalk => replaced with dummy_hf_mpi_indv_be_crswalk
jdbc_url_hash, jdbc_props_hash = get_db_config("ids_secret_name")
df_hf_lkup_mpi_crswalk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_hash)
    .options(**jdbc_props_hash)
    .option(
        "query",
        "SELECT MPI_MBR_ID, PRCS_OWNER_ID, SRC_SYS_ID, PRCS_RUN_DTM, CRT_RUN_CYC_EXCTN_SK, MPI_INDV_BE_CRSWALK_SK "
        "FROM dummy_hf_mpi_indv_be_crswalk"
    )
    .load()
)
df_hf_lkup_mpi_crswalk = df_hf_lkup_mpi_crswalk.select(
    col("MPI_MBR_ID").alias("lkup.MPI_MBR_ID"),
    col("PRCS_OWNER_ID").alias("lkup.PRCS_OWNER_ID"),
    col("SRC_SYS_ID").alias("lkup.SRC_SYS_ID"),
    col("PRCS_RUN_DTM").alias("lkup.PRCS_RUN_DTM"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("lkup.CRT_RUN_CYC_EXCTN_SK"),
    col("MPI_INDV_BE_CRSWALK_SK").alias("lkup.MPI_INDV_BE_CRSWALK_SK")
)

# Transformer (stage: "Transformer")
df_Transformer_base = (
    df_INDV_BE_CRSWALK.alias("IndvBeCrswalk")
    .join(
        df_hf_mpi_idsmpiindvbekeyxwalkextr_id.alias("mpi_id_lkup"),
        col("IndvBeCrswalk.INDV_BE_CRSWALK.PRCS_OWNER_SRC_SYS_ENVRN_CK") == col("mpi_id_lkup.PRCS_OWNER_SRC_SYS_ENVRN_CK"),
        "left"
    )
    .join(
        df_hf_etrnl_mbr_uniq_key.alias("mbr_uniq_key_lkup"),
        col("IndvBeCrswalk.INDV_BE_CRSWALK.MPI_MBR_ID") == col("mbr_uniq_key_lkup.MBR_UNIQ_KEY"),
        "left"
    )
)

df_Transformer = (
    df_Transformer_base
    .withColumn(
        "svProcessOwnerId",
        when(
            col("mpi_id_lkup.PROCESS_OWNER_ID").isNull() |
            (length(trim(col("mpi_id_lkup.PROCESS_OWNER_ID"))) == 0),
            lit("UNK")
        ).otherwise(col("mpi_id_lkup.PROCESS_OWNER_ID"))
    )
    .withColumn(
        "svSrcSysId",
        when(
            col("mpi_id_lkup.SRC_SYS_ID").isNull() |
            (length(trim(col("mpi_id_lkup.SRC_SYS_ID"))) == 0),
            lit("0")
        ).otherwise(col("mpi_id_lkup.SRC_SYS_ID"))
    )
    .withColumn(
        "svProcessOwnerSrcSysEnvID",
        when(
            col("mpi_id_lkup.PRCS_OWNER_SRC_SYS_ENVRN_ID").isNull() |
            (length(trim(col("mpi_id_lkup.PRCS_OWNER_SRC_SYS_ENVRN_ID"))) == 0),
            lit("UNK")
        ).otherwise(col("mpi_id_lkup.PRCS_OWNER_SRC_SYS_ENVRN_ID"))
    )
    .withColumn(
        "svProcessRunDtm",
        col("IndvBeCrswalk.INDV_BE_CRSWALK.PRCS_RUN_DTM")
    )
    .withColumn(
        "svProcessRunDate",
        to_date(col("IndvBeCrswalk.INDV_BE_CRSWALK.PRCS_RUN_DTM"), "yyyy-MM-dd")
    )
    .withColumn(
        "MBR_SK",
        when(
            col("mbr_uniq_key_lkup.MBR_SK").isNull() |
            (length(trim(col("mbr_uniq_key_lkup.MBR_SK"))) == 0),
            lit("0")
        ).otherwise(col("mbr_uniq_key_lkup.MBR_SK"))
    )
    .withColumn(
        "MBR_UNIQ_KEY",
        when(
            col("mbr_uniq_key_lkup.MBR_UNIQ_KEY").isNull() |
            (length(trim(col("mbr_uniq_key_lkup.MBR_UNIQ_KEY"))) == 0),
            col("IndvBeCrswalk.INDV_BE_CRSWALK.MPI_MBR_ID")
        ).otherwise(col("mbr_uniq_key_lkup.MBR_UNIQ_KEY"))
    )
)

df_TransformerTrans = df_Transformer.select(
    col("IndvBeCrswalk.INDV_BE_CRSWALK.MPI_MBR_ID").alias("MPI_MBR_ID"),
    col("IndvBeCrswalk.INDV_BE_CRSWALK.PRCS_OWNER_SRC_SYS_ENVRN_CK").alias("PRCS_OWNER_SRC_SYS_ENVRN_CK"),
    col("svProcessOwnerId").alias("PRCS_OWNER_ID"),
    col("svSrcSysId").alias("SRC_SYS_ID"),
    col("svProcessRunDtm").alias("PRCS_RUN_DTM"),
    col("svProcessRunDate").cast(StringType()).alias("PRCS_RUN_DT_SK"),
    col("IndvBeCrswalk.INDV_BE_CRSWALK.PREV_INDV_BE_KEY").alias("PREV_INDV_BE_KEY"),
    col("IndvBeCrswalk.INDV_BE_CRSWALK.NEW_INDV_BE_KEY").alias("NEW_INDV_BE_KEY"),
    col("svProcessOwnerSrcSysEnvID").alias("PRCS_OWNER_SRC_SYS_ENVRN_ID"),
    col("MBR_SK").alias("MBR_SK"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

df_TransformerBalancing = df_Transformer.select(
    col("IndvBeCrswalk.INDV_BE_CRSWALK.MPI_MBR_ID").alias("MPI_MBR_ID"),
    col("svProcessOwnerId").alias("PRCS_OWNER_ID"),
    col("svSrcSysId").alias("SRC_SYS_ID"),
    col("svProcessRunDtm").alias("PRCS_RUN_DTM")
)

# CSeqFileStage: B_INDV_BE_CRSWALK
df_B_INDV_BE_CRSWALK_out = df_TransformerBalancing.select(
    "MPI_MBR_ID",
    "PRCS_OWNER_ID",
    "SRC_SYS_ID",
    "PRCS_RUN_DTM"
)
# Apply rpad if needed (types unknown, marking char/varchar length as <...> if not provided)
df_B_INDV_BE_CRSWALK_out = df_B_INDV_BE_CRSWALK_out \
    .withColumn("MPI_MBR_ID", rpad("MPI_MBR_ID", <...>, " ")) \
    .withColumn("PRCS_OWNER_ID", rpad("PRCS_OWNER_ID", <...>, " ")) \
    .withColumn("SRC_SYS_ID", rpad("SRC_SYS_ID", <...>, " "))

write_files(
    df_B_INDV_BE_CRSWALK_out,
    f"{adls_path}/load/B_MPI_INDV_BE_CRSWALK.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# BusinessRules Transformer
df_BusinessRules = df_TransformerTrans.withColumn("RowPassThru", lit("Y"))
df_BusinessRules_out = df_BusinessRules.select(
    lit("0").alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    col("RowPassThru").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit("0").alias("ERR_CT"),
    lit("0").alias("RECYCLE_CT"),
    lit("NA").alias("SRC_SYS_CD"),
    concat(
        col("MPI_MBR_ID"), lit(";"),
        col("PRCS_OWNER_SRC_SYS_ENVRN_CK"), lit(";"),
        col("PRCS_RUN_DTM"), lit(";"),
        col("PRCS_OWNER_ID"), lit(";"),
        col("SRC_SYS_ID")
    ).alias("PRI_KEY_STRING"),
    col("MPI_MBR_ID").alias("MPI_MBR_ID"),
    col("PRCS_OWNER_ID").alias("PRCS_OWNER_ID"),
    col("SRC_SYS_ID").alias("SRC_SYS_ID"),
    col("PRCS_RUN_DTM").alias("PRCS_RUN_DTM"),
    lit("0").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit("0").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PRCS_RUN_DT_SK").alias("PRCS_RUN_DT_SK"),
    col("NEW_INDV_BE_KEY").alias("NEW_INDV_BE_KEY"),
    col("PREV_INDV_BE_KEY").alias("PREV_INDV_BE_KEY"),
    col("PRCS_OWNER_SRC_SYS_ENVRN_ID").alias("PRCS_OWNER_SRC_SYS_ENVRN_ID"),
    col("MBR_SK").alias("MBR_SK"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

# PrimaryKey Transformer
df_PrimaryKey_joined = df_BusinessRules_out.alias("Transform").join(
    df_hf_lkup_mpi_crswalk.alias("lkup"),
    (
        (col("Transform.MPI_MBR_ID") == col("lkup.MPI_MBR_ID")) &
        (col("Transform.PRCS_OWNER_ID") == col("lkup.PRCS_OWNER_ID")) &
        (col("Transform.SRC_SYS_ID") == col("lkup.SRC_SYS_ID")) &
        (col("Transform.PRCS_RUN_DTM") == col("lkup.PRCS_RUN_DTM"))
    ),
    "left"
)

df_PrimaryKey = (
    df_PrimaryKey_joined
    .withColumn(
        "temp_MPI_INDV_BE_CRSWALK_SK",
        when(col("lkup.MPI_INDV_BE_CRSWALK_SK").isNull(), lit(None)).otherwise(col("lkup.MPI_INDV_BE_CRSWALK_SK"))
    )
    .withColumn(
        "temp_CRT_RUN_CYC_EXCTN_SK",
        when(col("lkup.MPI_INDV_BE_CRSWALK_SK").isNull(), lit(CurrRunCycle)).otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn("temp_LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
)

df_enriched = df_PrimaryKey.select(
    col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("Transform.ERR_CT").alias("ERR_CT"),
    col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("temp_MPI_INDV_BE_CRSWALK_SK").alias("MPI_INDV_BE_CRSWALK_SK"),
    col("Transform.MPI_MBR_ID").alias("MPI_MBR_ID"),
    col("Transform.PRCS_OWNER_ID").alias("PRCS_OWNER_ID"),
    col("Transform.SRC_SYS_ID").alias("SRC_SYS_ID"),
    col("Transform.PRCS_RUN_DTM").alias("PRCS_RUN_DTM"),
    col("temp_CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("temp_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Transform.PRCS_RUN_DT_SK").alias("PRCS_RUN_DT_SK"),
    col("Transform.NEW_INDV_BE_KEY").alias("NEW_INDV_BE_KEY"),
    col("Transform.PREV_INDV_BE_KEY").alias("PREV_INDV_BE_KEY"),
    col("Transform.PRCS_OWNER_SRC_SYS_ENVRN_ID").alias("PRCS_OWNER_SRC_SYS_ENVRN_ID"),
    col("Transform.MBR_SK").alias("MBR_SK"),
    col("Transform.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

df_enriched = df_enriched.withColumn(
    "MPI_INDV_BE_CRSWALK_SK",
    when(col("MPI_INDV_BE_CRSWALK_SK").isNull(), lit(None)).otherwise(col("MPI_INDV_BE_CRSWALK_SK"))
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"MPI_INDV_BE_CRSWALK_SK",<schema>,<secret_name>)

df_MpiIndvBeCrswalk = df_enriched.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "MPI_INDV_BE_CRSWALK_SK",
    "MPI_MBR_ID",
    "PRCS_OWNER_ID",
    "SRC_SYS_ID",
    "PRCS_RUN_DTM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRCS_RUN_DT_SK",
    "NEW_INDV_BE_KEY",
    "PREV_INDV_BE_KEY",
    "PRCS_OWNER_SRC_SYS_ENVRN_ID",
    "MBR_SK",
    "MBR_UNIQ_KEY"
)

df_updt = df_enriched.filter(col("lkup.MPI_INDV_BE_CRSWALK_SK").isNull())
df_updt_for_write = df_updt.select(
    col("MPI_MBR_ID"),
    col("PRCS_OWNER_ID"),
    col("SRC_SYS_ID"),
    col("PRCS_RUN_DTM"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("MPI_INDV_BE_CRSWALK_SK")
)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.{'MpiBCBSIndvBeKeyXWalkExtr'}_{'hf_mpi_crswalk'}_temp",
    jdbc_url_hash,
    jdbc_props_hash
)
df_updt_for_write.write \
    .format("jdbc") \
    .option("url", jdbc_url_hash) \
    .options(**jdbc_props_hash) \
    .option("dbtable", f"STAGING.{'MpiBCBSIndvBeKeyXWalkExtr'}_{'hf_mpi_crswalk'}_temp") \
    .mode("append") \
    .save()

merge_sql = (
    "MERGE dummy_hf_mpi_indv_be_crswalk AS T "
    f"USING STAGING.{'MpiBCBSIndvBeKeyXWalkExtr'}_{'hf_mpi_crswalk'}_temp AS S "
    "ON T.MPI_MBR_ID = S.MPI_MBR_ID AND T.PRCS_OWNER_ID=S.PRCS_OWNER_ID AND T.SRC_SYS_ID=S.SRC_SYS_ID AND T.PRCS_RUN_DTM=S.PRCS_RUN_DTM "
    "WHEN MATCHED THEN "
    "  UPDATE SET T.MPI_MBR_ID = T.MPI_MBR_ID "
    "WHEN NOT MATCHED THEN "
    "  INSERT (MPI_MBR_ID, PRCS_OWNER_ID, SRC_SYS_ID, PRCS_RUN_DTM, CRT_RUN_CYC_EXCTN_SK, MPI_INDV_BE_CRSWALK_SK) "
    "  VALUES (S.MPI_MBR_ID, S.PRCS_OWNER_ID, S.SRC_SYS_ID, S.PRCS_RUN_DTM, S.CRT_RUN_CYC_EXCTN_SK, S.MPI_INDV_BE_CRSWALK_SK);"
)
execute_dml(merge_sql, jdbc_url_hash, jdbc_props_hash)

# CSeqFileStage: MPI_INDV_BE_CRSWALK_Extr
df_MPI_INDV_BE_CRSWALK_Extr_out = df_MpiIndvBeCrswalk
df_MPI_INDV_BE_CRSWALK_Extr_out = df_MPI_INDV_BE_CRSWALK_Extr_out \
    .withColumn("INSRT_UPDT_CD", rpad("INSRT_UPDT_CD", <...>, " ")) \
    .withColumn("DISCARD_IN", rpad("DISCARD_IN", <...>, " ")) \
    .withColumn("PASS_THRU_IN", rpad("PASS_THRU_IN", <...>, " "))
write_files(
    df_MPI_INDV_BE_CRSWALK_Extr_out,
    f"{adls_path}/key/IdsMpiIndvBeCrswalk.MpiIndvBeCrswalk.uniq",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)