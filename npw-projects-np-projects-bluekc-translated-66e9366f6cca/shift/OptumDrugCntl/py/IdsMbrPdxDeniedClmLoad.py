#!/usr/bin/python3

from npadf import *

def IdsMbrPdxDeniedClmLoadActivities(ctx):
  def dfIdsMbrPdxDeniedClmLoad():
    def seqMBRPDXDENIEDTRANSload():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sources_inline = DataFlowSource(name='seqMBRPDXDENIEDTRANSload', linked_service=ds_ls_blob)
       return DataflowSegment([df_sources_inline])
    def cpyforBuffer():
       return DataflowSegment([
         Transformation(name='cpyforBufferDerived'),
         Transformation(name='cpyforBuffer')])
    def Db2ExtMBRPDXDENIEDTRANSData():
       ds_ls_db = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.sqldb_linked_service)
       df_sinks_inline = DataFlowSink(name='Db2ExtMBRPDXDENIEDTRANSData',linked_service=ds_ls_db)
       df_alterrows = Transformation(name='Db2ExtMBRPDXDENIEDTRANSDataAlterRows')
       return DataflowSegment([df_sinks_inline, df_alterrows])
    dataflowName = "IdsMbrPdxDeniedClmLoad"
    buffer = DataflowBuffer()
    buffer.append(seqMBRPDXDENIEDTRANSload())
    buffer.append(cpyforBuffer())
    buffer.append(Db2ExtMBRPDXDENIEDTRANSData())
    dataflow = MappingDataFlow(
      folder = DataFlowFolder(name="DataWarehouse/Claim/DeniedPDXClaims"),
      sources = buffer.sources,
      sinks = buffer.sinks,
      transformations = buffer.transformations,
      script = f"""parameters{{
        FilePath as string ("{ctx.FilePath}"),
        IDSInstance as string ("{ctx.IDSInstance}"),
        IDSAcct as string ,
        IDSPW as string ,
        IDSDB as string ("{ctx.IDSDB}"),
        IDSOwner as string ("{ctx.IDSOwner}"),
        IDSDB2ArraySize as string ("{ctx.IDSDB2ArraySize}"),
        IDSDB2RecordCount as string ("{ctx.IDSDB2RecordCount}"),
        SrcSysCd as string ,
        RunID as string 
      }}
    source(output(
        MBR_PDX_DENIED_TRANS_SK as integer,
        MBR_UNIQ_KEY as integer,
        PDX_NTNL_PROV_ID as string,
        TRANS_TYP_CD as string,
        TRANS_DENIED_DT as date,
        RX_NO as string,
        PRCS_DT as date,
        SRC_SYS_CD as string,
        CRT_RUN_CYC_EXCTN_SK as integer,
        LAST_UPDT_RUN_CYC_EXCTN_SK as integer,
        GRP_SK as integer,
        MBR_SK as integer,
        NDC_SK as integer,
        PRSCRB_PROV_SK as integer,
        PROV_SPEC_CD_SK as integer,
        SRV_PROV_SK as integer,
        SUB_SK as integer,
        MEDIA_TYP_CD_SK as integer,
        MBR_GNDR_CD_SK as integer,
        MBR_RELSHP_CD_SK as integer,
        PDX_RSPN_RSN_CD_SK as integer,
        PDX_RSPN_TYP_CD_SK as integer,
        TRANS_TYP_CD_SK as integer,
        MAIL_ORDER_IN as string,
        MBR_BRTH_DT as date,
        SRC_SYS_CLM_RCVD_DT as date,
        SRC_SYS_CLM_RCVD_TM as timestamp,
        SUB_BRTH_DT as date,
        BILL_RX_DISPENSE_FEE_AMT as decimal(13, 2),
        BILL_RX_GROS_APRV_AMT as decimal(13, 2),
        BILL_RX_NET_CHK_AMT as decimal(13, 2),
        BILL_RX_PATN_PAY_AMT as decimal(13, 2),
        INGR_CST_ALW_AMT as decimal(13, 2),
        TRANS_MO_NO as integer,
        TRANS_YR_NO as integer,
        MBR_ID as string,
        MBR_SFX_NO as string,
        MBR_FIRST_NM as string,
        MBR_LAST_NM as string,
        MBR_SSN as string,
        PDX_NM as string,
        PDX_PHN_NO as string,
        PHYS_DEA_NO as string,
        PHYS_NTNL_PROV_ID as string,
        PHYS_FIRST_NM as string,
        PHYS_LAST_NM as string,
        PHYS_ST_ADDR_LN as string,
        PHYS_CITY_NM as string,
        PHYS_ST_CD as string,
        PHYS_POSTAL_CD as string,
        RX_LABEL_TX as string,
        SVC_PROV_NABP_NM as string,
        SRC_SYS_CAR_ID as string,
        SRC_SYS_CLNT_ORG_ID as string,
        SRC_SYS_CLNT_ORG_NM as string,
        SRC_SYS_CNTR_ID as string,
        SRC_SYS_GRP_ID as string,
        SUB_ID as string,
        SUB_FIRST_NM as string,
        SUB_LAST_NM as string),
      allowSchemaDrift: true,
      validateSchema: false,
      ignoreNoFilesFound: false,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: ({ctx.getFolderPath("$FilePath + '/load/MBR_PDX_DENIED_TRANS.' + $SrcSysCd + '.' + $RunID + '.dat'")}),
      fileName: ({ctx.getFileName("$FilePath + '/load/MBR_PDX_DENIED_TRANS.' + $SrcSysCd + '.' + $RunID + '.dat'")}) ,
      columnDelimiter: '|',
      rowDelimiter: '\\n',
      quoteChar: '',
      columnNamesAsHeader:false) ~> seqMBRPDXDENIEDTRANSload
    seqMBRPDXDENIEDTRANSload
        derive(
              MBR_PDX_DENIED_TRANS_SK = MBR_PDX_DENIED_TRANS_SK,
              MBR_UNIQ_KEY = MBR_UNIQ_KEY,
              PDX_NTNL_PROV_ID = PDX_NTNL_PROV_ID,
              TRANS_TYP_CD = TRANS_TYP_CD,
              TRANS_DENIED_DT = TRANS_DENIED_DT,
              RX_NO = RX_NO,
              PRCS_DT = PRCS_DT,
              SRC_SYS_CD = SRC_SYS_CD,
              CRT_RUN_CYC_EXCTN_SK = CRT_RUN_CYC_EXCTN_SK,
              LAST_UPDT_RUN_CYC_EXCTN_SK = LAST_UPDT_RUN_CYC_EXCTN_SK,
              GRP_SK = GRP_SK,
              MBR_SK = MBR_SK,
              NDC_SK = NDC_SK,
              PRSCRB_PROV_SK = PRSCRB_PROV_SK,
              PROV_SPEC_CD_SK = PROV_SPEC_CD_SK,
              SRV_PROV_SK = SRV_PROV_SK,
              SUB_SK = SUB_SK,
              MEDIA_TYP_CD_SK = MEDIA_TYP_CD_SK,
              MBR_GNDR_CD_SK = MBR_GNDR_CD_SK,
              MBR_RELSHP_CD_SK = MBR_RELSHP_CD_SK,
              PDX_RSPN_RSN_CD_SK = PDX_RSPN_RSN_CD_SK,
              PDX_RSPN_TYP_CD_SK = PDX_RSPN_TYP_CD_SK,
              TRANS_TYP_CD_SK = TRANS_TYP_CD_SK,
              MAIL_ORDER_IN = MAIL_ORDER_IN,
              MBR_BRTH_DT = MBR_BRTH_DT,
              SRC_SYS_CLM_RCVD_DT = SRC_SYS_CLM_RCVD_DT,
              SRC_SYS_CLM_RCVD_TM = SRC_SYS_CLM_RCVD_TM,
              SUB_BRTH_DT = SUB_BRTH_DT,
              BILL_RX_DISPENSE_FEE_AMT = BILL_RX_DISPENSE_FEE_AMT,
              BILL_RX_GROS_APRV_AMT = BILL_RX_GROS_APRV_AMT,
              BILL_RX_NET_CHK_AMT = BILL_RX_NET_CHK_AMT,
              BILL_RX_PATN_PAY_AMT = BILL_RX_PATN_PAY_AMT,
              INGR_CST_ALW_AMT = INGR_CST_ALW_AMT,
              TRANS_MO_NO = TRANS_MO_NO,
              TRANS_YR_NO = TRANS_YR_NO,
              MBR_ID = MBR_ID,
              MBR_SFX_NO = MBR_SFX_NO,
              MBR_FIRST_NM = MBR_FIRST_NM,
              MBR_LAST_NM = MBR_LAST_NM,
              MBR_SSN = MBR_SSN,
              PDX_NM = PDX_NM,
              PDX_PHN_NO = PDX_PHN_NO,
              PHYS_DEA_NO = PHYS_DEA_NO,
              PHYS_NTNL_PROV_ID = PHYS_NTNL_PROV_ID,
              PHYS_FIRST_NM = PHYS_FIRST_NM,
              PHYS_LAST_NM = PHYS_LAST_NM,
              PHYS_ST_ADDR_LN = PHYS_ST_ADDR_LN,
              PHYS_CITY_NM = PHYS_CITY_NM,
              PHYS_ST_CD = PHYS_ST_CD,
              PHYS_POSTAL_CD = PHYS_POSTAL_CD,
              RX_LABEL_TX = RX_LABEL_TX,
              SVC_PROV_NABP_NM = SVC_PROV_NABP_NM,
              SRC_SYS_CAR_ID = SRC_SYS_CAR_ID,
              SRC_SYS_CLNT_ORG_ID = SRC_SYS_CLNT_ORG_ID,
              SRC_SYS_CLNT_ORG_NM = SRC_SYS_CLNT_ORG_NM,
              SRC_SYS_CNTR_ID = SRC_SYS_CNTR_ID,
              SRC_SYS_GRP_ID = SRC_SYS_GRP_ID,
              SUB_ID = SUB_ID,
              SUB_FIRST_NM = SUB_FIRST_NM,
              SUB_LAST_NM = SUB_LAST_NM) ~> cpyforBufferDerived
        cpyforBufferDerived
        select(mapColumn(
            MBR_PDX_DENIED_TRANS_SK,
            MBR_UNIQ_KEY,
            PDX_NTNL_PROV_ID,
            TRANS_TYP_CD,
            TRANS_DENIED_DT,
            RX_NO,
            PRCS_DT,
            SRC_SYS_CD,
            CRT_RUN_CYC_EXCTN_SK,
            LAST_UPDT_RUN_CYC_EXCTN_SK,
            GRP_SK,
            MBR_SK,
            NDC_SK,
            PRSCRB_PROV_SK,
            PROV_SPEC_CD_SK,
            SRV_PROV_SK,
            SUB_SK,
            MEDIA_TYP_CD_SK,
            MBR_GNDR_CD_SK,
            MBR_RELSHP_CD_SK,
            PDX_RSPN_RSN_CD_SK,
            PDX_RSPN_TYP_CD_SK,
            TRANS_TYP_CD_SK,
            MAIL_ORDER_IN,
            MBR_BRTH_DT,
            SRC_SYS_CLM_RCVD_DT,
            SRC_SYS_CLM_RCVD_TM,
            SUB_BRTH_DT,
            BILL_RX_DISPENSE_FEE_AMT,
            BILL_RX_GROS_APRV_AMT,
            BILL_RX_NET_CHK_AMT,
            BILL_RX_PATN_PAY_AMT,
            INGR_CST_ALW_AMT,
            TRANS_MO_NO,
            TRANS_YR_NO,
            MBR_ID,
            MBR_SFX_NO,
            MBR_FIRST_NM,
            MBR_LAST_NM,
            MBR_SSN,
            PDX_NM,
            PDX_PHN_NO,
            PHYS_DEA_NO,
            PHYS_NTNL_PROV_ID,
            PHYS_FIRST_NM,
            PHYS_LAST_NM,
            PHYS_ST_ADDR_LN,
            PHYS_CITY_NM,
            PHYS_ST_CD,
            PHYS_POSTAL_CD,
            RX_LABEL_TX,
            SVC_PROV_NABP_NM,
            SRC_SYS_CAR_ID,
            SRC_SYS_CLNT_ORG_ID,
            SRC_SYS_CLNT_ORG_NM,
            SRC_SYS_CNTR_ID,
            SRC_SYS_GRP_ID,
            SUB_ID,
            SUB_FIRST_NM,
            SUB_LAST_NM
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> cpyforBuffer
    cpyforBuffer alterRow(upsertIf(1==1)) ~> Db2ExtMBRPDXDENIEDTRANSDataAlterRows
    Db2ExtMBRPDXDENIEDTRANSDataAlterRows sink(
      allowSchemaDrift: true,
      validateSchema: false,
      format: 'table',
      store: 'sqlserver',
      schemaName: (""" + ctx.getSchemaName("""$IDSOwner + '.MBR_PDX_DENIED_TRANS'""") + f"""),
      tableName: (""" + ctx.getTableName("""$IDSOwner + '.MBR_PDX_DENIED_TRANS'""") + f"""),
      insertable:{'true' if 'upsertIf' == 'insertIf' else 'false'},
      updateable:{'true' if 'upsertIf' == 'updateIf' else 'false'},
      deletable:{'true' if 'upsertIf' == 'deleteIf' else 'false'},
      upsertable:{'true' if 'upsertIf' == 'upsertIf' else 'false'},
      keys:['MBR_PDX_DENIED_TRANS_SK'],
      preSQLs: [],
      postSQLs: [],
      skipDuplicateMapInputs: true,
      skipDuplicateMapOutputs: true,
      errorHandlingOption: 'stopOnFirstError') ~> Db2ExtMBRPDXDENIEDTRANSData""", type='MappingDataFlow')
    artifacts = buffer.artifacts + [(dataflowName, dataflow)]
    return artifacts, dataflowName
  artifacts, dataflowName = dfIdsMbrPdxDeniedClmLoad()
  activityName = "IdsMbrPdxDeniedClmLoad"
  activityParameters = {"FilePath":  {"value": "'@{pipeline().parameters.FilePath}'","type": "Expression"},
  "IDSInstance":  {"value": "'@{pipeline().parameters.IDSInstance}'","type": "Expression"},
  "IDSAcct":  {"value": "'@{pipeline().parameters.IDSAcct}'","type": "Expression"},
  "IDSPW":  {"value": "'@{pipeline().parameters.IDSPW}'","type": "Expression"},
  "IDSDB":  {"value": "'@{pipeline().parameters.IDSDB}'","type": "Expression"},
  "IDSOwner":  {"value": "'@{pipeline().parameters.IDSOwner}'","type": "Expression"},
  "IDSDB2ArraySize":  {"value": "'@{pipeline().parameters.IDSDB2ArraySize}'","type": "Expression"},
  "IDSDB2RecordCount":  {"value": "'@{pipeline().parameters.IDSDB2RecordCount}'","type": "Expression"},
  "SrcSysCd":  {"value": "'@{pipeline().parameters.SrcSysCd}'","type": "Expression"},
  "RunID":  {"value": "'@{pipeline().parameters.RunID}'","type": "Expression"}}
  activity = ExecuteDataFlowActivity(
    name = activityName,
    data_flow = DataFlowReference(reference_name=dataflowName,
      parameters=activityParameters, type='DataFlowReference'))
  return artifacts, [activity]

def IdsMbrPdxDeniedClmLoad(ctx):
  name = "IdsMbrPdxDeniedClmLoad"
  artifacts, activities = IdsMbrPdxDeniedClmLoadActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="DataWarehouse/Claim/DeniedPDXClaims"),
    activities = activities,
    description = """
      This Job Loads Data from Sequential File created in FKEY Job and loads into MBR_PDX_DENIED_TRANS table Triggered by: OptumIdsMbrPdxDeniedTransLoadSeq
      **********************************************************************************************************************************************************************
      COPYRIGHT 2014 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


      DESCRIPTION:  OPTUM Claims denied data is extracted from source file and loaded into the  MBR_PDX_DENIED_TRANS Table


      Called By: OptumIdsMbrPdxDeniedTransLoadSeq


      Initial Development: Peter Gichiri 
                                                                                                                                                                                                                      DATASTAGE               CODE                           DATE
      DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
      -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
                                                  

      Ravi Singh               2018-08-14              5796                             Original Programming                                                                         IntegrateDev1             Kalyan Neelam            2018-09-25
      Peter Gichiri              2019-10-02             6131 - PBM REPLACEMENT  to OPTUMRX                                                                                                             Kalyan Neelam            2019-11-21
      Parameters:
      -----------
      FilePath:
        EDW File Path
      IDSInstance:
        IDS Instance
      IDSAcct:
        IDS Account
      IDSPW:
        IDS Password
      IDSDB:
        IDS Database
      IDSOwner:
        IDS Table Owner
      IDSDB2ArraySize:
        IDS DB2 Array Size for Load
      IDSDB2RecordCount:
        IDS DB2 Record Count for Load
      SrcSysCd:
        SrcSysCd
      RunID:
        RunID""",
    parameters = {
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "IDSInstance": ParameterSpecification(type="String", default_value=ctx.IDSInstance),
      "IDSAcct": ParameterSpecification(type="String"),
      "IDSPW": ParameterSpecification(type="String"),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "IDSDB2ArraySize": ParameterSpecification(type="String", default_value=ctx.IDSDB2ArraySize),
      "IDSDB2RecordCount": ParameterSpecification(type="String", default_value=ctx.IDSDB2RecordCount),
      "SrcSysCd": ParameterSpecification(type="String"),
      "RunID": ParameterSpecification(type="String")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (IdsMbrPdxDeniedClmLoad(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
