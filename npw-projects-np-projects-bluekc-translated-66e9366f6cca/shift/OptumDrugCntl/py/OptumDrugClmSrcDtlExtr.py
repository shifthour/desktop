#!/usr/bin/python3

from npadf import *

def OptumDrugClmSrcDtlExtrActivities(ctx):
  def dfOptumDrugClmSrcDtlExtr():
    def db2Clm():
       ds_ls_db = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.sqldb_linked_service)
       df_sources_inline = DataFlowSource(name='db2Clm',linked_service=ds_ls_db)
       return DataflowSegment([df_sources_inline])
    def db2ClmSrcDtl():
       ds_ls_db = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.sqldb_linked_service)
       df_sources_inline = DataFlowSource(name='db2ClmSrcDtl',linked_service=ds_ls_db)
       return DataflowSegment([df_sources_inline])
    def seqPDXCLMSTDINPTLand():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sources_inline = DataFlowSource(name='seqPDXCLMSTDINPTLand', linked_service=ds_ls_blob)
       return DataflowSegment([df_sources_inline])
    def Xfm1V0S161P2():
       return DataflowSegment([
         Transformation(name='Xfm1V0S161P2Derived'),
         Transformation(name='Xfm1V0S161P2')])
    def Xfm1V0S161P5filter():
       return DataflowSegment([
         Transformation(name='Xfm1V0S161P5filter')])
    def Xfm1V0S161P5():
       return DataflowSegment([
         Transformation(name='Xfm1V0S161P5Derived'),
         Transformation(name='Xfm1V0S161P5')])
    def Fnl():
       return DataflowSegment([
         Transformation(name='FnlUnion'),
         Transformation(name='Fnl')])
    def LkpClm():
       return DataflowSegment([
         Transformation(name="LkpClm"),
         Transformation(name="LkpClmJoin1"),Transformation(name="LkpClmDerived1"),
         Transformation(name="LkpClmJoin2"),Transformation(name="LkpClmDerived2")])
    def XfmLoadfilter():
       return DataflowSegment([
         Transformation(name='XfmLoadfilter')])
    def XfmLoad():
       return DataflowSegment([
         Transformation(name='XfmLoadDerived'),
         Transformation(name='XfmLoad')])
    def SeqTgtCLMSRCDTL():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sinks_inline = DataFlowSink(name='SeqTgtCLMSRCDTL', linked_service=ds_ls_blob)
       return DataflowSegment([df_sinks_inline])
    dataflowName = "OptumDrugClmSrcDtlExtr"
    buffer = DataflowBuffer()
    buffer.append(db2Clm())
    buffer.append(db2ClmSrcDtl())
    buffer.append(seqPDXCLMSTDINPTLand())
    buffer.append(Xfm1V0S161P2())
    buffer.append(Xfm1V0S161P5filter())
    buffer.append(Xfm1V0S161P5())
    buffer.append(Fnl())
    buffer.append(LkpClm())
    buffer.append(XfmLoadfilter())
    buffer.append(XfmLoad())
    buffer.append(SeqTgtCLMSRCDTL())
    dataflow = MappingDataFlow(
      folder = DataFlowFolder(name="claim/claim/Optum"),
      sources = buffer.sources,
      sinks = buffer.sinks,
      transformations = buffer.transformations,
      script = f"""parameters{{
        IDSDB as string ("{ctx.IDSDB}"),
        IDSOwner as string ("{ctx.IDSOwner}"),
        IDSPW as string ,
        IDSAcct as string ,
        FilePath as string ("{ctx.FilePath}"),
        SrcSysCd as string ,
        RunID as string ,
        IDSRunCycle as string ,
        SrcSysCdSK as integer ,
        LOB as string 
      }}
    source(output(
        CLM_SK as integer,
        CLM_ID as string),
      allowSchemaDrift: true,
      validateSchema: false,
      format: 'query',
      store: 'sqlserver',
      query: ("SELECT  \\n\\nCLM_SK,\\nCLM_ID\\n \\nFROM " + toString($IDSOwner) + ".CLM\\n\\nwhere \\nLAST_UPDT_RUN_CYC_EXCTN_SK='" + toString($IDSRunCycle) + "'  AND\\nSRC_SYS_CD_SK=" + toString($SrcSysCdSK) + "\\n\\n  \\n \\n\\n"),
      schemaName: '',
      tableName: '',
      isolationLevel: 'READ_UNCOMMITTED') ~> db2Clm
    source(output(
        CLM_ID as string,
        CRT_RUN_CYC_EXCTN_SK as integer,
        LAST_UPDT_RUN_CYC_EXCTN_SK as integer),
      allowSchemaDrift: true,
      validateSchema: false,
      format: 'query',
      store: 'sqlserver',
      query: ("SELECT \\n\\nCLM_ID,\\nCRT_RUN_CYC_EXCTN_SK,\\nLAST_UPDT_RUN_CYC_EXCTN_SK\\n\\n \\n\\n FROM " + toString($IDSOwner) + ".CLM_SRC_DTL\\n \\n\\n"),
      schemaName: '',
      tableName: '',
      isolationLevel: 'READ_UNCOMMITTED') ~> db2ClmSrcDtl
    source(output(
        SRC_SYS_CD as string,
        FILE_RCVD_DT as string,
        RCRD_ID as integer,
        PRCSR_NO as string,
        BTCH_NO as integer,
        PDX_NO as string,
        RX_NO as string,
        FILL_DT as string,
        NDC as string,
        DRUG_DESC as string,
        NEW_OR_RFL_CD as integer,
        METRIC_QTY as decimal(10, 3),
        DAYS_SUPL as integer,
        BSS_OF_CST_DTRM as string,
        INGR_CST_AMT as decimal(13, 2),
        DISPNS_FEE_AMT as decimal(13, 2),
        COPAY_AMT as decimal(13, 2),
        SLS_TAX_AMT as decimal(13, 2),
        BILL_AMT as decimal(13, 2),
        PATN_FIRST_NM as string,
        PATN_LAST_NM as string,
        BRTH_DT as string,
        SEX_CD as integer,
        CARDHLDR_ID_NO as string,
        RELSHP_CD as integer,
        GRP_NO as string,
        HOME_PLN as string,
        HOST_PLN as string,
        PRSCRBR_ID as string,
        DIAG_CD as string,
        CARDHLDR_FIRST_NM as string,
        CARDHLDR_LAST_NM as string,
        PRAUTH_NO as string,
        PA_MC_SC_NO as string,
        CUST_LOC as integer,
        RESUB_CYC_CT as integer,
        RX_DT as string,
        DISPENSE_AS_WRTN_PROD_SEL_CD as string,
        PRSN_CD as string,
        OTHR_COV_CD as integer,
        ELIG_CLRFCTN_CD as integer,
        CMPND_CD as integer,
        NO_OF_RFLS_AUTH as integer,
        LVL_OF_SVC as integer,
        RX_ORIG_CD as integer,
        RX_DENIAL_CLRFCTN as integer,
        PRI_PRSCRBR as string,
        CLNC_ID_NO as integer,
        DRUG_TYP as integer,
        PRSCRBR_LAST_NM as string,
        POSTAGE_AMT as decimal(13, 2),
        UNIT_DOSE_IN as integer,
        OTHR_PAYOR_AMT as decimal(13, 2),
        BSS_OF_DAYS_SUPL_DTRM as integer,
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
        ADJDCT_REF_NO as integer,
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
        INDEM_HIER_TIER_NO as string,
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
        HLTH_RMBRMT_ARGMT_APLD_AMT as decimal(13, 2),
        THER_CLS as string,
        HIC_NO as string,
        HLTH_RMBRMT_ARGMT_FLAG as string,
        DOSE_CD as string,
        LOW_INCM as string,
        RTE_OF_ADMIN as string,
        DEA_SCHD as string,
        COPAY_BNF_OPT as string,
        GNRC_PROD_IN as string,
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
        DEDCT_FMLY_MET_AMT as decimal(13, 2),
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
        RX_NO_2012 as string,
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
      rowDelimiter: '\\n',
      columnNamesAsHeader:false) ~> seqPDXCLMSTDINPTLand
    seqPDXCLMSTDINPTLand
        derive(
              CLM_ID = CLM_ID) ~> Xfm1V0S161P2Derived
        Xfm1V0S161P2Derived
        select(mapColumn(
            CLM_ID
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> Xfm1V0S161P2
    seqPDXCLMSTDINPTLand
        filter(
            substring(CLM_TYP, 1, 1) == "X" || substring(CLM_TYP, 1, 1) == "R"
        ) ~> Xfm1V0S161P5filter
    Xfm1V0S161P5filter
        derive(
              CLM_ID = substring(CLM_ID, 1, length(npw_DS_TRIM_1(CLM_ID)) - 1)) ~> Xfm1V0S161P5Derived
        Xfm1V0S161P5Derived
        select(mapColumn(
            CLM_ID
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> Xfm1V0S161P5
    Xfm1V0S161P2, Xfm1V0S161P5 union(byName: true) ~> FnlUnion
    FnlUnion select(mapColumn(
            CLM_ID = CLM_ID
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> Fnl
    {ctx.joinScript(joinType='left',
      master=['Fnl', 'CLM_ID'],
      transformationName='LkpClm',
      references=[['db2Clm', ctx.joinConditionForLookup(master={'name':'Fnl','alias':'Lnk_seq','types':{'CLM_ID':'string'}}, reference={'name':'db2Clm','alias':'Ref_db2_Clm','types':{'CLM_SK':'integer','CLM_ID':'string'}}, joinFields=[{'master':'Lnk_seq.CLM_ID','reference':'Ref_db2_Clm.CLM_ID', 'operator':'=='}]), 'CLM_SK,CLM_ID'], 
    ['db2ClmSrcDtl', ctx.joinConditionForLookup(master={'name':'Fnl','alias':'Lnk_seq','types':{'CLM_ID':'string'}}, reference={'name':'db2ClmSrcDtl','alias':'Ref_db2_ClmSrcDtl','types':{'CLM_ID':'string','CRT_RUN_CYC_EXCTN_SK':'integer','LAST_UPDT_RUN_CYC_EXCTN_SK':'integer'}}, joinFields=[{'master':'Lnk_seq.CLM_ID','reference':'Ref_db2_ClmSrcDtl.CLM_ID', 'operator':'=='}]), 'CLM_ID,CRT_RUN_CYC_EXCTN_SK,LAST_UPDT_RUN_CYC_EXCTN_SK']],
      schema="CLM_SK,CRT_RUN_CYC_EXCTN_SK,CLM_ID",
      map=['Ref_db2_Clm.CLM_SK','Ref_db2_ClmSrcDtl.CRT_RUN_CYC_EXCTN_SK','Lnk_seq.CLM_ID'],
      aliasFrom="Lnk_seq,Ref_db2_Clm,Ref_db2_ClmSrcDtl",
      aliasTo="Fnl,db2Clm,db2ClmSrcDtl")}
    LkpClm
        filter(
            !isNull(CLM_SK)
        ) ~> XfmLoadfilter
    XfmLoadfilter
        derive(
              CLM_SK = CLM_SK,
              SRC_SYS_CD_SK = $SrcSysCdSK,
              CLM_ID = CLM_ID,
              CRT_RUN_CYC_EXCTN_SK =  case(isNull(CRT_RUN_CYC_EXCTN_SK), $IDSRunCycle, CRT_RUN_CYC_EXCTN_SK),
              LAST_UPDT_RUN_CYC_EXCTN_SK = $IDSRunCycle,
              SRC_FILE_ID = $LOB) ~> XfmLoadDerived
        XfmLoadDerived
        select(mapColumn(
            CLM_SK,
            SRC_SYS_CD_SK,
            CLM_ID,
            CRT_RUN_CYC_EXCTN_SK,
            LAST_UPDT_RUN_CYC_EXCTN_SK,
            SRC_FILE_ID
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> XfmLoad
    XfmLoad sink(
      allowSchemaDrift: true,
      validateSchema: false,
      partitionFileNames:[(""" + ctx.getFileName("""$FilePath + '/load/CLM_SRC_DTL_' + $SrcSysCd + '.dat.' + $RunID""") + f""")],
      partitionBy('hash', 1),
      skipDuplicateMapInputs: true,
      skipDuplicateMapOutputs: true,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: (""" + ctx.getFolderPath("""$FilePath + '/load/CLM_SRC_DTL_' + $SrcSysCd + '.dat.' + $RunID""") + f""") ,
      columnDelimiter: ',',
      escapeChar:""" + '\\\\' + f""",
      rowDelimiter: '\\n',
      quoteChar: '',
      columnNamesAsHeader:false) ~> SeqTgtCLMSRCDTL""", type='MappingDataFlow')
    artifacts = buffer.artifacts + [(dataflowName, dataflow)]
    return artifacts, dataflowName
  artifacts, dataflowName = dfOptumDrugClmSrcDtlExtr()
  activityName = "OptumDrugClmSrcDtlExtr"
  activityParameters = {"IDSDB":  {"value": "'@{pipeline().parameters.IDSDB}'","type": "Expression"},
  "IDSOwner":  {"value": "'@{pipeline().parameters.IDSOwner}'","type": "Expression"},
  "IDSPW":  {"value": "'@{pipeline().parameters.IDSPW}'","type": "Expression"},
  "IDSAcct":  {"value": "'@{pipeline().parameters.IDSAcct}'","type": "Expression"},
  "FilePath":  {"value": "'@{pipeline().parameters.FilePath}'","type": "Expression"},
  "SrcSysCd":  {"value": "'@{pipeline().parameters.SrcSysCd}'","type": "Expression"},
  "RunID":  {"value": "'@{pipeline().parameters.RunID}'","type": "Expression"},
  "IDSRunCycle":  {"value": "'@{pipeline().parameters.IDSRunCycle}'","type": "Expression"},
  "SrcSysCdSK":  {"value": "'@{pipeline().parameters.SrcSysCdSK}'","type": "Expression"},
  "LOB":  {"value": "'@{pipeline().parameters.LOB}'","type": "Expression"}}
  activity = ExecuteDataFlowActivity(
    name = activityName,
    data_flow = DataFlowReference(reference_name=dataflowName,
      parameters=activityParameters, type='DataFlowReference'))
  return artifacts, [activity]

def OptumDrugClmSrcDtlExtr(ctx):
  name = "OptumDrugClmSrcDtlExtr"
  artifacts, activities = OptumDrugClmSrcDtlExtrActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="claim/claim/Optum"),
    activities = activities,
    description = """
      Optum ClaimSourceDetail Extract
      © Copyright 2020 Blue Cross/Blue Shield of Kansas City

      CALLED BY :  OptumACADrugCntl/OptumDrugCntl/OptumMedDDrugCntl
       
      DESCRIPTION:  This Job Create the Load File from Sequential File created in Extract Job.

      MODIFICATIONS:

      Developer                         Date                 Project/Altiris #              Change Description                                 Development Project      Code Reviewer          Date Reviewed
      ------------------                      --------------------      ----------------------               ------------------------------------                             --------------------------------    -------------------------------      ----------------------------              
      Revathi BoojiReddy        10-11-2022            US 559701                  Original Programming                              IntegrateDevB               Jeyaprasanna              2023-01-14
      Parameters:
      -----------
      IDSDB:
        IDS Database
      IDSOwner:
        IDS Table Owner
      IDSPW:
        IDS Password
      IDSAcct:
        IDS Account
      FilePath:
        File Path
      SrcSysCd:
        SrcSysCd
      RunID:
        RunID
      IDSRunCycle:
        IDSRunCycle
      SrcSysCdSK:
        SrcSysCdSK
      LOB:
        LOB""",
    parameters = {
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "IDSPW": ParameterSpecification(type="String"),
      "IDSAcct": ParameterSpecification(type="String"),
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "SrcSysCd": ParameterSpecification(type="String"),
      "RunID": ParameterSpecification(type="String"),
      "IDSRunCycle": ParameterSpecification(type="String"),
      "SrcSysCdSK": ParameterSpecification(type="Int"),
      "LOB": ParameterSpecification(type="String")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (OptumDrugClmSrcDtlExtr(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
