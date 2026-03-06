#!/usr/bin/python3

from npadf import *

def BCBSKCCommClmRemitHistExtrActivities(ctx):
  def dfBCBSKCCommClmRemitHistExtr():
    def BCBSCommClmLand():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sources_inline = DataFlowSource(name='BCBSCommClmLand', linked_service=ds_ls_blob)
       return DataflowSegment([df_sources_inline])
    def BusinessRules():
       return DataflowSegment([
         Transformation(name='BusinessRulesDerived'),
         Transformation(name='BusinessRules')])
    def SnapshotV0S61P4():
       return DataflowSegment([
         Transformation(name='SnapshotV0S61P4Derived'),
         Transformation(name='SnapshotV0S61P4')])
    def BCLMREMITHIST():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sinks_inline = DataFlowSink(name='BCLMREMITHIST', linked_service=ds_ls_blob)
       return DataflowSegment([df_sinks_inline])
    def SnapshotV0S61P2():
       return DataflowSegment([
         Transformation(name='SnapshotV0S61P2Derived'),
         Transformation(name='SnapshotV0S61P2')])
    def ClmRemitHistPK():
       return ctx.sharedContainer(
         scriptModel=1,
         containerName='ClmRemitHistPK',
         sourceName='SnapshotV0S61P2',
         context=ctx)
    def BCBSKCCommClmRemitHistExtr():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sinks_inline = DataFlowSink(name='BCBSKCCommClmRemitHistExtr', linked_service=ds_ls_blob)
       return DataflowSegment([df_sinks_inline])
    dataflowName = "BCBSKCCommClmRemitHistExtr"
    buffer = DataflowBuffer()
    buffer.append(BCBSCommClmLand())
    buffer.append(BusinessRules())
    buffer.append(SnapshotV0S61P4())
    buffer.append(BCLMREMITHIST())
    buffer.append(SnapshotV0S61P2())
    buffer.append(ClmRemitHistPK())
    buffer.append(BCBSKCCommClmRemitHistExtr())
    dataflow = MappingDataFlow(
      folder = DataFlowFolder(name="claim/RemitHist"),
      sources = buffer.sources,
      sinks = buffer.sinks,
      transformations = buffer.transformations,
      script = f"""parameters{{
        FilePath as string ("{ctx.FilePath}"),
        CurrentDate as string ,
        RunCycle as integer (toInteger("1")),
        RunID as string ,
        SrcSysCd as string ,
        SrcSysCdSk as string 
      }}
    source(output(
        SRC_SYS_CD as string,
        FILE_RCVD_DT as string,
        RCRD_ID as decimal(2, 0),
        PRCSR_NO as string,
        BTCH_NO as decimal(5, 0),
        PDX_NO as decimal(12, 0),
        RX_NO as decimal(9, 0),
        FILL_DT as string,
        NDC as decimal(20, 0),
        DRUG_DESC as string,
        NEW_OR_RFL_CD as integer,
        METRIC_QTY as decimal(10, 3),
        DAYS_SUPL as decimal(3, 0),
        BSS_OF_CST_DTRM as string,
        INGR_CST_AMT as decimal(13, 2),
        DISPNS_FEE_AMT as decimal(13, 2),
        COPAY_AMT as decimal(13, 2),
        SLS_TAX_AMT as decimal(13, 2),
        BILL_AMT as decimal(13, 2),
        PATN_FIRST_NM as string,
        PATN_LAST_NM as string,
        BRTH_DT as string,
        SEX_CD as decimal(1, 0),
        CARDHLDR_ID_NO as string,
        RELSHP_CD as decimal(1, 0),
        GRP_NO as string,
        HOME_PLN as string,
        HOST_PLN as string,
        PRSCRBR_ID as string,
        DIAG_CD as string,
        CARDHLDR_FIRST_NM as string,
        CARDHLDR_LAST_NM as string,
        PRAUTH_NO as string,
        PA_MC_SC_NO as string,
        CUST_LOC as decimal(2, 0),
        RESUB_CYC_CT as decimal(2, 0),
        RX_DT as string,
        DISPENSE_AS_WRTN_PROD_SEL_CD as string,
        PRSN_CD as string,
        OTHR_COV_CD as decimal(2, 0),
        ELIG_CLRFCTN_CD as decimal(1, 0),
        CMPND_CD as decimal(1, 0),
        NO_OF_RFLS_AUTH as decimal(2, 0),
        LVL_OF_SVC as decimal(2, 0),
        RX_ORIG_CD as decimal(1, 0),
        RX_DENIAL_CLRFCTN as decimal(2, 0),
        PRI_PRSCRBR as string,
        CLNC_ID_NO as decimal(5, 0),
        DRUG_TYP as decimal(1, 0),
        PRSCRBR_LAST_NM as string,
        POSTAGE_AMT as decimal(13, 2),
        UNIT_DOSE_IN as decimal(1, 0),
        OTHR_PAYOR_AMT as decimal(13, 2),
        BSS_OF_DAYS_SUPL_DTRM as decimal(1, 0),
        FULL_AVG_WHLSL_PRICE as decimal(13, 2),
        EXPNSN_AREA as string,
        MSTR_CAR as string,
        SUBCAR as string,
        CLM_TYP as string,
        SUBGRP as string,
        PLN_DSGNR as string,
        ADJDCT_DT as string,
        ADMIN_FEE_AMT as decimal(13, 2),
        CAP_AMT as decimal(13, 2),
        INGR_CST_SUB_AMT as decimal(13, 2),
        MBR_NON_COPAY_AMT as decimal(13, 2),
        MBR_PAY_CD as string,
        INCNTV_FEE_AMT as decimal(13, 2),
        CLM_ADJ_AMT as decimal(13, 2),
        CLM_ADJ_CD as string,
        FRMLRY_FLAG as string,
        GNRC_CLS_NO as string,
        THRPTC_CLS_AHFS as string,
        PDX_TYP as string,
        BILL_BSS_CD as string,
        USL_AND_CUST_CHRG_AMT as decimal(13, 2),
        PD_DT as string,
        BNF_CD as string,
        DRUG_STRG as string,
        ORIG_MBR as string,
        INJRY_DT as string,
        FEE_AMT as decimal(13, 2),
        REF_NO as string,
        CLNT_CUST_ID as string,
        PLN_TYP as string,
        ADJDCT_REF_NO as decimal(9, 0),
        ANCLRY_AMT as decimal(13, 2),
        CLNT_GNRL_PRPS_AREA as string,
        PRTL_FILL_STTUS_CD as string,
        BILL_DT as string,
        FSA_VNDR_CD as string,
        PICA_DRUG_CD as string,
        CLM_AMT as decimal(13, 2),
        DSALW_AMT as decimal(13, 2),
        FED_DRUG_CLS_CD as string,
        DEDCT_AMT as decimal(13, 2),
        BNF_COPAY_100 as string,
        CLM_PRCS_TYP as string,
        INDEM_HIER_TIER_NO as decimal(4, 0),
        MCARE_D_COV_DRUG as string,
        RETRO_LICS_CD as string,
        RETRO_LICS_AMT as decimal(13, 2),
        LICS_SBSDY_AMT as decimal(13, 2),
        MCARE_B_DRUG as string,
        MCARE_B_CLM as string,
        PRSCRBR_QLFR as string,
        PRSCRBR_NTNL_PROV_ID as string,
        PDX_QLFR as string,
        PDX_NTNL_PROV_ID as string,
        HLTH_RMBRMT_ARGMT_APLD_AMT as decimal(11, 0),
        THER_CLS as decimal(6, 0),
        HIC_NO as string,
        HLTH_RMBRMT_ARGMT_FLAG as string,
        DOSE_CD as decimal(4, 0),
        LOW_INCM as string,
        RTE_OF_ADMIN as string,
        DEA_SCHD as decimal(1, 0),
        COPAY_BNF_OPT as decimal(10, 0),
        GNRC_PROD_IN as decimal(14, 0),
        PRSCRBR_SPEC as string,
        VAL_CD as string,
        PRI_CARE_PDX as string,
        OFC_OF_INSPECTOR_GNRL as string,
        PATN_SSN as string,
        CARDHLDR_SSN as string,
        CARDHLDR_BRTH_DT as string,
        CARDHLDR_ADDR as string,
        CARDHLDR_CITY as string,
        CHADHLDR_ST as string,
        CARDHLDR_ZIP_CD as string,
        PSL_FMLY_MET_AMT as decimal(13, 2),
        PSL_MBR_MET_AMT as decimal(13, 2),
        PSL_FMLY_AMT as decimal(13, 2),
        DEDCT_FMLY_MET_AMT as string,
        DEDCT_FMLY_AMT as decimal(13, 2),
        MOPS_FMLY_AMT as decimal(13, 2),
        MOPS_FMLY_MET_AMT as decimal(13, 2),
        MOPS_MBR_MET_AMT as decimal(13, 2),
        DEDCT_MBR_MET_AMT as decimal(13, 2),
        PSL_APLD_AMT as decimal(13, 2),
        MOPS_APLD_AMT as decimal(13, 2),
        PAR_PDX_IN as string,
        COPAY_PCT_AMT as decimal(13, 2),
        COPAY_FLAT_AMT as decimal(13, 2),
        CLM_TRNSMSN_METH as string,
        RX_NO_2012 as decimal(12, 0),
        CLM_ID as string,
        CLM_STTUS_CD as string,
        ADJ_FROM_CLM_ID as string,
        ADJ_TO_CLM_ID as string,
        SUBMT_PROD_ID_QLFR as string,
        CNTNGNT_THER_FLAG as string,
        CNTNGNT_THER_SCHD as string,
        CLNT_PATN_PAY_ATRBD_PROD_AMT as decimal(13, 2),
        CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT as decimal(13, 2),
        CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT as decimal(13, 2),
        CLNT_PATN_PAY_ATRBD_NTWK_AMT as decimal(13, 2),
        LOB_IN as string),
      allowSchemaDrift: true,
      validateSchema: false,
      ignoreNoFilesFound: false,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: ({ctx.getFolderPath("$FilePath + '/verified/PDX_CLM_STD_INPT_Land_' + $SrcSysCd + '.dat.' + $RunID")}),
      fileName: ({ctx.getFileName("$FilePath + '/verified/PDX_CLM_STD_INPT_Land_' + $SrcSysCd + '.dat.' + $RunID")}) ,
      columnDelimiter: ',',
      columnNamesAsHeader:false) ~> BCBSCommClmLand
    BCBSCommClmLand
        derive(
              JOB_EXCTN_RCRD_ERR_SK = 0,
              INSRT_UPDT_CD = "I",
              DISCARD_IN = "N",
              PASS_THRU_IN = "Y",
              FIRST_RECYC_DT = $CurrentDate,
              ERR_CT = 0,
              RECYCLE_CT = 0,
              SRC_SYS_CD = $SrcSysCd,
              PRI_KEY_STRING = :PkString,
              CLM_REMIT_HIST_SK = 0,
              SRC_SYS_CD_SK = 0,
              CLM_ID = :ClmId,
              CRT_RUN_CYC_EXCTN_SK = 0,
              LAST_UPDT_RUN_CYC_EXCTN_SK = 0,
              CLM_SK = 0,
              CALC_ACTL_PD_AMT_IN = "N",
              SUPRESS_EOB_IN = "N",
              SUPRESS_REMIT_IN = "N",
              ACTL_PD_AMT = BILL_AMT,
              COB_PD_AMT = OTHR_PAYOR_AMT,
              COINS_AMT = COPAY_PCT_AMT,
              CNSD_CHRG_AMT = FULL_AVG_WHLSL_PRICE,
              COPAY_AMT = COPAY_AMT,
              DEDCT_AMT = DEDCT_AMT,
              DSALW_AMT = 0.00,
              ER_COPAY_AMT = 0.00,
              INTRST_AMT = 0.00,
              NO_RESP_AMT = 0.00,
              PATN_RESP_AMT =  case(SRC_SYS_CD == "OPTUMRX", npw_DS_TRIM_1(CLM_AMT), COPAY_AMT),
              WRTOFF_AMT = 0.00,
              PCA_PD_AMT = 0.00,
              ALT_CHRG_IN = "N",
              ALT_CHRG_PROV_WRTOFF_AMT = 0.00,
              ClmId = :ClmId,
              PkString = :PkString,
              PassThru = :PassThru,
              ClmId := {'CLM_ID'.replace("BCBSKCComm.","")},
              PkString := {'$SrcSysCd + ";" + :ClmId'.replace("BCBSKCComm.","")},
              PassThru := {'"Y"'.replace("BCBSKCComm.","")}) ~> BusinessRulesDerived
        BusinessRulesDerived
        select(mapColumn(
            JOB_EXCTN_RCRD_ERR_SK,
            INSRT_UPDT_CD,
            DISCARD_IN,
            PASS_THRU_IN,
            FIRST_RECYC_DT,
            ERR_CT,
            RECYCLE_CT,
            SRC_SYS_CD,
            PRI_KEY_STRING,
            CLM_REMIT_HIST_SK,
            SRC_SYS_CD_SK,
            CLM_ID,
            CRT_RUN_CYC_EXCTN_SK,
            LAST_UPDT_RUN_CYC_EXCTN_SK,
            CLM_SK,
            CALC_ACTL_PD_AMT_IN,
            SUPRESS_EOB_IN,
            SUPRESS_REMIT_IN,
            ACTL_PD_AMT,
            COB_PD_AMT,
            COINS_AMT,
            CNSD_CHRG_AMT,
            COPAY_AMT,
            DEDCT_AMT,
            DSALW_AMT,
            ER_COPAY_AMT,
            INTRST_AMT,
            NO_RESP_AMT,
            PATN_RESP_AMT,
            WRTOFF_AMT,
            PCA_PD_AMT,
            ALT_CHRG_IN,
            ALT_CHRG_PROV_WRTOFF_AMT
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> BusinessRules
    BusinessRules
        derive(
              SRC_SYS_CD_SK = $SrcSysCdSk,
              CLM_ID = CLM_ID) ~> SnapshotV0S61P4Derived
        SnapshotV0S61P4Derived
        select(mapColumn(
            SRC_SYS_CD_SK,
            CLM_ID
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> SnapshotV0S61P4
    SnapshotV0S61P4 sink(
      allowSchemaDrift: true,
      validateSchema: false,
      partitionFileNames:[(""" + ctx.getFileName("""$FilePath + '/load/B_CLM_REMIT_HIST.' + $SrcSysCd + '.dat.' + $RunID""") + f""")],
      partitionBy('hash', 1),
      skipDuplicateMapInputs: true,
      skipDuplicateMapOutputs: true,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: (""" + ctx.getFolderPath("""$FilePath + '/load/B_CLM_REMIT_HIST.' + $SrcSysCd + '.dat.' + $RunID""") + f""") ,
      columnDelimiter: ',',
      escapeChar:""" + '\\\\' + f""",
      quoteChar: '\\"',
      columnNamesAsHeader:false) ~> BCLMREMITHIST
    BusinessRules
        derive(
              JOB_EXCTN_RCRD_ERR_SK = JOB_EXCTN_RCRD_ERR_SK,
              INSRT_UPDT_CD = INSRT_UPDT_CD,
              DISCARD_IN = DISCARD_IN,
              PASS_THRU_IN = PASS_THRU_IN,
              FIRST_RECYC_DT = FIRST_RECYC_DT,
              ERR_CT = ERR_CT,
              RECYCLE_CT = RECYCLE_CT,
              SRC_SYS_CD = SRC_SYS_CD,
              PRI_KEY_STRING = PRI_KEY_STRING,
              CLM_REMIT_HIST_SK = CLM_REMIT_HIST_SK,
              SRC_SYS_CD_SK = SRC_SYS_CD_SK,
              CLM_ID = CLM_ID,
              CRT_RUN_CYC_EXCTN_SK = CRT_RUN_CYC_EXCTN_SK,
              LAST_UPDT_RUN_CYC_EXCTN_SK = LAST_UPDT_RUN_CYC_EXCTN_SK,
              CLM_SK = CLM_SK,
              CALC_ACTL_PD_AMT_IN = CALC_ACTL_PD_AMT_IN,
              SUPRESS_EOB_IN = SUPRESS_EOB_IN,
              SUPRESS_REMIT_IN = SUPRESS_REMIT_IN,
              ACTL_PD_AMT = ACTL_PD_AMT,
              COB_PD_AMT = COB_PD_AMT,
              COINS_AMT = COINS_AMT,
              CNSD_CHRG_AMT = CNSD_CHRG_AMT,
              COPAY_AMT = COPAY_AMT,
              DEDCT_AMT = DEDCT_AMT,
              DSALW_AMT = DSALW_AMT,
              ER_COPAY_AMT = ER_COPAY_AMT,
              INTRST_AMT = INTRST_AMT,
              NO_RESP_AMT = NO_RESP_AMT,
              PATN_RESP_AMT = PATN_RESP_AMT,
              WRTOFF_AMT = WRTOFF_AMT,
              PCA_PD_AMT = PCA_PD_AMT,
              ALT_CHRG_IN = ALT_CHRG_IN,
              ALT_CHRG_PROV_WRTOFF_AMT = ALT_CHRG_PROV_WRTOFF_AMT) ~> SnapshotV0S61P2Derived
        SnapshotV0S61P2Derived
        select(mapColumn(
            JOB_EXCTN_RCRD_ERR_SK,
            INSRT_UPDT_CD,
            DISCARD_IN,
            PASS_THRU_IN,
            FIRST_RECYC_DT,
            ERR_CT,
            RECYCLE_CT,
            SRC_SYS_CD,
            PRI_KEY_STRING,
            CLM_REMIT_HIST_SK,
            SRC_SYS_CD_SK,
            CLM_ID,
            CRT_RUN_CYC_EXCTN_SK,
            LAST_UPDT_RUN_CYC_EXCTN_SK,
            CLM_SK,
            CALC_ACTL_PD_AMT_IN,
            SUPRESS_EOB_IN,
            SUPRESS_REMIT_IN,
            ACTL_PD_AMT,
            COB_PD_AMT,
            COINS_AMT,
            CNSD_CHRG_AMT,
            COPAY_AMT,
            DEDCT_AMT,
            DSALW_AMT,
            ER_COPAY_AMT,
            INTRST_AMT,
            NO_RESP_AMT,
            PATN_RESP_AMT,
            WRTOFF_AMT,
            PCA_PD_AMT,
            ALT_CHRG_IN,
            ALT_CHRG_PROV_WRTOFF_AMT
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> SnapshotV0S61P2
    {ctx.sharedContainer(
      scriptModel=0,
      containerName='ClmRemitHistPK',
      sourceName='SnapshotV0S61P2',
      context=ctx)}
    ClmRemitHistPK sink(
      allowSchemaDrift: true,
      validateSchema: false,
      partitionFileNames:[(""" + ctx.getFileName("""$FilePath + '/key/BCBSKCCommClmRemitHistExtr_' + $SrcSysCd + '.DrugClmRemitHist.dat.' + $RunID""") + f""")],
      partitionBy('hash', 1),
      skipDuplicateMapInputs: true,
      skipDuplicateMapOutputs: true,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: (""" + ctx.getFolderPath("""$FilePath + '/key/BCBSKCCommClmRemitHistExtr_' + $SrcSysCd + '.DrugClmRemitHist.dat.' + $RunID""") + f""") ,
      columnDelimiter: ',',
      escapeChar:""" + '\\\\' + f""",
      quoteChar: '\\"',
      columnNamesAsHeader:false) ~> BCBSKCCommClmRemitHistExtr""", type='MappingDataFlow')
    artifacts = buffer.artifacts + [(dataflowName, dataflow)]
    return artifacts, dataflowName
  artifacts, dataflowName = dfBCBSKCCommClmRemitHistExtr()
  activityName = "BCBSKCCommClmRemitHistExtr"
  activityParameters = {"FilePath":  {"value": "'@{pipeline().parameters.FilePath}'","type": "Expression"},
  "CurrentDate":  {"value": "'@{pipeline().parameters.CurrentDate}'","type": "Expression"},
  "RunCycle":  {"value": "'@{pipeline().parameters.RunCycle}'","type": "Expression"},
  "RunID":  {"value": "'@{pipeline().parameters.RunID}'","type": "Expression"},
  "SrcSysCd":  {"value": "'@{pipeline().parameters.SrcSysCd}'","type": "Expression"},
  "SrcSysCdSk":  {"value": "'@{pipeline().parameters.SrcSysCdSk}'","type": "Expression"}}
  activity = ExecuteDataFlowActivity(
    name = activityName,
    data_flow = DataFlowReference(reference_name=dataflowName,
      parameters=activityParameters, type='DataFlowReference'))
  return artifacts, [activity]

def BCBSKCCommClmRemitHistExtr(ctx):
  name = "BCBSKCCommClmRemitHistExtr"
  artifacts, activities = BCBSKCCommClmRemitHistExtrActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="claim/RemitHist"),
    activities = activities,
    description = """
      BCBSKCCOMMONClaim Remit History
      ********************************************************************************************
      COPYRIGHT 2008, 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

      CALLED BY:          OptumDrugExtrSeq

      DESCRIPTION:     Reads the Optum landing file created in  OptumClmLand  job and puts the data into the claim  remit history common record format and runs through primary key using shared container ClmRemiHistPkey

      MODIFICATIONS:

      Developer                    Date              Project/Altiris #   Change Description                                                                                      Development Project   Code Reviewer          Date Reviewed       
      ---------------------------------   ------------------    ------------------------   ------------------------------------------------------------------------------------------------------------------   ---------------------------------   -------------------------------   ----------------------------       
      Srinivas Garimella      2019-10-15   #6131                         Original Programming                                                                                IntegrateDev2              Kalyan Neelam          2019-11-20
      Sri Nannapaneni       2020-07-15      6131 PBM                Added 7 new fields to source seq file                                                       IntegrateDev2                 
      Sri Nannapaneni        08/13/2020    6131- PBM Replacement    Added LOB_IN field to source seq file                                          IntegrateDev2                Sravya Gorla             2020-12-09
      Parameters:
      -----------
      FilePath:
        IDS File Path
      CurrentDate:
        Current Date
      RunCycle:
        Current cycle
      RunID:
        Run ID
      SrcSysCd:
        Source System Code
      SrcSysCdSk:
        Source System Code Surrogate Key""",
    parameters = {
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "CurrentDate": ParameterSpecification(type="String"),
      "RunCycle": ParameterSpecification(type="Int", default_value="1"),
      "RunID": ParameterSpecification(type="String"),
      "SrcSysCd": ParameterSpecification(type="String"),
      "SrcSysCdSk": ParameterSpecification(type="String")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (BCBSKCCommClmRemitHistExtr(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
