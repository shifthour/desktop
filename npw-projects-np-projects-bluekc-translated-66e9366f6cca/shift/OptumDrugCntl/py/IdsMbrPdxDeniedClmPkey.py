#!/usr/bin/python3

from npadf import *

def IdsMbrPdxDeniedClmPkeyActivities(ctx):
  def dfIdsMbrPdxDeniedClmPkey():
    def db2KMBRPDXDENIEDTRANSin():
       ds_ls_db = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.sqldb_linked_service)
       df_sources_inline = DataFlowSource(name='db2KMBRPDXDENIEDTRANSin',linked_service=ds_ls_db)
       return DataflowSegment([df_sources_inline])
    def dsMBRPDXDENIEDTRANSxfm():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sources_dataset = DataFlowSource(name='dsMBRPDXDENIEDTRANSxfm', linked_service=ds_ls_blob)
       return DataflowSegment([df_sources_dataset])
    def CpyV0S33P5():
       return DataflowSegment([
         Transformation(name='CpyV0S33P5Derived'),
         Transformation(name='CpyV0S33P5')])
    def rdupNaturalKeys():
       return DataflowSegment([
         Transformation(name='rdupNaturalKeysAggregate'),
         Transformation(name='rdupNaturalKeys')
         ])
    def jnMbrPdx():
       return DataflowSegment([Transformation(name='jnMbrPdx'),
         Transformation(name="jnMbrPdxJoin1"),Transformation(name="jnMbrPdxDerived1")])
    def xfrmPKEYgenV116S6P2():
       return DataflowSegment([
         Transformation(name='xfrmPKEYgenV116S6P2Derived'),
         Transformation(name='xfrmPKEYgenV116S6P2')])
    def xfrmPKEYgenV116S6P3map():
       return DataflowSegment([
         Transformation(name='xfrmPKEYgenV116S6P3mapDerived'),
         Transformation(name='xfrmPKEYgenV116S6P3map')])
    def xfrmPKEYgenV116S6P3():
       return DataflowSegment([
         Transformation(name='xfrmPKEYgenV116S6P3')])
    def db2KMBRPDXDENIEDTRANSload():
       ds_ls_db = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.sqldb_linked_service)
       df_sinks_inline = DataFlowSink(name='db2KMBRPDXDENIEDTRANSload',linked_service=ds_ls_db)
       df_alterrows = Transformation(name='db2KMBRPDXDENIEDTRANSloadAlterRows')
       return DataflowSegment([df_sinks_inline, df_alterrows])
    def CpyV0S33P4():
       return DataflowSegment([
         Transformation(name='CpyV0S33P4Derived'),
         Transformation(name='CpyV0S33P4')])
    def jnPKey():
       return DataflowSegment([Transformation(name='jnPKey'),
         Transformation(name="jnPKeyJoin1"),Transformation(name="jnPKeyDerived1")])
    def seqMBRPDXDENIEDTRANSPkey():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sinks_inline = DataFlowSink(name='seqMBRPDXDENIEDTRANSPkey', linked_service=ds_ls_blob)
       return DataflowSegment([df_sinks_inline])
    dataflowName = "IdsMbrPdxDeniedClmPkey"
    buffer = DataflowBuffer()
    buffer.append(db2KMBRPDXDENIEDTRANSin())
    buffer.append(dsMBRPDXDENIEDTRANSxfm())
    buffer.append(CpyV0S33P5())
    buffer.append(rdupNaturalKeys())
    buffer.append(jnMbrPdx())
    buffer.append(xfrmPKEYgenV116S6P2())
    buffer.append(xfrmPKEYgenV116S6P3map())
    buffer.append(xfrmPKEYgenV116S6P3())
    buffer.append(db2KMBRPDXDENIEDTRANSload())
    buffer.append(CpyV0S33P4())
    buffer.append(jnPKey())
    buffer.append(seqMBRPDXDENIEDTRANSPkey())
    dataflow = MappingDataFlow(
      folder = DataFlowFolder(name="DataWarehouse/Claim/DeniedPDXClaims"),
      sources = buffer.sources,
      sinks = buffer.sinks,
      transformations = buffer.transformations,
      script = f"""parameters{{
        FilePath as string ("{ctx.FilePath}"),
        APT_CONFIG_FILE as string ("{ctx.APT_CONFIG_FILE}"),
        IDSDB as string ("{ctx.IDSDB}"),
        IDSOwner as string ("{ctx.IDSOwner}"),
        IDSAcct as string ,
        IDSPW as string ,
        IDSRunCycle as string ,
        SrcSysCd as string ,
        IDSDB2ArraySize as string ("{ctx.IDSDB2ArraySize}"),
        IDSDB2RecordCount as string ("{ctx.IDSDB2RecordCount}"),
        RunID as string 
      }}
    source(output(
        MBR_UNIQ_KEY as integer,
        PDX_NTNL_PROV_ID as string,
        TRANS_TYP_CD as string,
        TRANS_DENIED_DT as date,
        RX_NO as string,
        PRCS_DT as date,
        SRC_SYS_CLM_RCVD_DT as date,
        SRC_SYS_CLM_RCVD_TM as timestamp,
        SRC_SYS_CD as string,
        CRT_RUN_CYC_EXCTN_SK as integer,
        MBR_PDX_DENIED_TRANS_SK as integer),
      allowSchemaDrift: true,
      validateSchema: false,
      format: 'query',
      store: 'sqlserver',
      query: ("SELECT\\nMBR_UNIQ_KEY,\\nPDX_NTNL_PROV_ID,\\nTRANS_TYP_CD,\\nTRANS_DENIED_DT,\\nRX_NO,\\nPRCS_DT AS PRCS_DT,\\nSRC_SYS_CLM_RCVD_DT  AS SRC_SYS_CLM_RCVD_DT,\\nSRC_SYS_CLM_RCVD_TM AS SRC_SYS_CLM_RCVD_TM,\\nSRC_SYS_CD,\\nCRT_RUN_CYC_EXCTN_SK,\\nMBR_PDX_DENIED_TRANS_SK\\n\\nFROM\\n" + toString($IDSOwner) + ".K_MBR_PDX_DENIED_TRANS"),
      schemaName: '',
      tableName: '',
      isolationLevel: 'READ_UNCOMMITTED') ~> db2KMBRPDXDENIEDTRANSin
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
        MEDIA_TYP_CD_SK as string,
        MBR_GNDR_CD_SK as integer,
        MBR_RELSHP_CD_SK as integer,
        PDX_RSPN_RSN_CD_SK as string,
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
        SUB_LAST_NM as string,
        CLNT_ELIG_MBRSH_ID as string,
        PRSN_NO as string,
        RELSHP_CD as string,
        GNDR_CD as string,
        CLM_RCVD_TM as string,
        MSG_TYP_CD as string,
        MSG_TYP as string,
        MSG_TX as string,
        CLNT_MBRSH_ID as string,
        MEDIA_TYP_CD as string,
        RSPN_CD as string,
        DESC as string,
        CHAPTER_ID as string,
        CHAPTER_DESC as string,
        PDX_NPI as string,
        NDC as string),
      allowSchemaDrift: true,
      validateSchema: false,
      ignoreNoFilesFound: false,
      format: 'parquet',
      fileSystem: '{ctx.file_container}',
      wildcardPaths:[($FilePath + '/ds/MBR_PDX_DENIED_TRANS.' + $SrcSysCd + '.xfrm.' + $RunID + '.ds' + '/*.parquet')]) ~> dsMBRPDXDENIEDTRANSxfm
    dsMBRPDXDENIEDTRANSxfm
        derive(
              MBR_UNIQ_KEY = MBR_UNIQ_KEY,
              PDX_NTNL_PROV_ID = PDX_NTNL_PROV_ID,
              TRANS_TYP_CD = TRANS_TYP_CD,
              TRANS_DENIED_DT = TRANS_DENIED_DT,
              RX_NO = RX_NO,
              PRCS_DT = PRCS_DT,
              SRC_SYS_CD = SRC_SYS_CD,
              SRC_SYS_CLM_RCVD_DT = SRC_SYS_CLM_RCVD_DT,
              SRC_SYS_CLM_RCVD_TM = SRC_SYS_CLM_RCVD_TM) ~> CpyV0S33P5Derived
        CpyV0S33P5Derived
        select(mapColumn(
            MBR_UNIQ_KEY,
            PDX_NTNL_PROV_ID,
            TRANS_TYP_CD,
            TRANS_DENIED_DT,
            RX_NO,
            PRCS_DT,
            SRC_SYS_CD,
            SRC_SYS_CLM_RCVD_DT,
            SRC_SYS_CLM_RCVD_TM
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> CpyV0S33P5
    CpyV0S33P5 aggregate(groupBy(mycols = sha2(256, 'MBR_UNIQ_KEY', 'PDX_NTNL_PROV_ID', 'TRANS_TYP_CD', 'TRANS_DENIED_DT', 'RX_NO', 'PRCS_DT', 'SRC_SYS_CLM_RCVD_DT', 'SRC_SYS_CLM_RCVD_TM', 'SRC_SYS_CD')),
    	each(match(true()), $$ = first($$))) ~> rdupNaturalKeysAggregate
    rdupNaturalKeysAggregate
        select(mapColumn(
            {ctx.formatSelectMap("MBR_UNIQ_KEY", "lnk_FctsIdsMbr_PdxDeniedPkey_dedup.MBR_UNIQ_KEY")},
            {ctx.formatSelectMap("PDX_NTNL_PROV_ID", "lnk_FctsIdsMbr_PdxDeniedPkey_dedup.PDX_NTNL_PROV_ID")},
            {ctx.formatSelectMap("TRANS_TYP_CD", "lnk_FctsIdsMbr_PdxDeniedPkey_dedup.TRANS_TYP_CD")},
            {ctx.formatSelectMap("TRANS_DENIED_DT", "lnk_FctsIdsMbr_PdxDeniedPkey_dedup.TRANS_DENIED_DT")},
            {ctx.formatSelectMap("RX_NO", "lnk_FctsIdsMbr_PdxDeniedPkey_dedup.RX_NO")},
            {ctx.formatSelectMap("PRCS_DT", "lnk_FctsIdsMbr_PdxDeniedPkey_dedup.PRCS_DT")},
            {ctx.formatSelectMap("SRC_SYS_CLM_RCVD_DT", "lnk_FctsIdsMbr_PdxDeniedPkey_dedup.SRC_SYS_CLM_RCVD_DT")},
            {ctx.formatSelectMap("SRC_SYS_CLM_RCVD_TM", "lnk_FctsIdsMbr_PdxDeniedPkey_dedup.SRC_SYS_CLM_RCVD_TM")},
            {ctx.formatSelectMap("SRC_SYS_CD", "lnk_FctsIdsMbr_PdxDeniedPkey_dedup.SRC_SYS_CD")}
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> rdupNaturalKeys
    {ctx.joinScript(joinType='left', 
      master=['rdupNaturalKeys', 'MBR_UNIQ_KEY,PDX_NTNL_PROV_ID,TRANS_TYP_CD,TRANS_DENIED_DT,RX_NO,PRCS_DT,SRC_SYS_CLM_RCVD_DT,SRC_SYS_CLM_RCVD_TM,SRC_SYS_CD'],
      transformationName='jnMbrPdx', 
      references=[['db2KMBRPDXDENIEDTRANSin', ctx.joinConditionAndFixDataTypeMismatch(masterName='rdupNaturalKeys', masterDataTypes={'MBR_UNIQ_KEY':'integer','PDX_NTNL_PROV_ID':'string','TRANS_TYP_CD':'string','TRANS_DENIED_DT':'date','RX_NO':'string','PRCS_DT':'date','SRC_SYS_CLM_RCVD_DT':'date','SRC_SYS_CLM_RCVD_TM':'timestamp','SRC_SYS_CD':'string'}, refName='db2KMBRPDXDENIEDTRANSin', refDataTypes={'MBR_UNIQ_KEY':'integer','PDX_NTNL_PROV_ID':'string','TRANS_TYP_CD':'string','TRANS_DENIED_DT':'date','RX_NO':'string','PRCS_DT':'date','SRC_SYS_CLM_RCVD_DT':'date','SRC_SYS_CLM_RCVD_TM':'timestamp','SRC_SYS_CD':'string','CRT_RUN_CYC_EXCTN_SK':'integer','MBR_PDX_DENIED_TRANS_SK':'integer'}, joinFields='MBR_UNIQ_KEY,PDX_NTNL_PROV_ID,TRANS_TYP_CD,TRANS_DENIED_DT,RX_NO,SRC_SYS_CD,PRCS_DT,SRC_SYS_CLM_RCVD_DT,SRC_SYS_CLM_RCVD_TM'), 'MBR_UNIQ_KEY,PDX_NTNL_PROV_ID,TRANS_TYP_CD,TRANS_DENIED_DT,RX_NO,PRCS_DT,SRC_SYS_CLM_RCVD_DT,SRC_SYS_CLM_RCVD_TM,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,MBR_PDX_DENIED_TRANS_SK']],
      schema="MBR_UNIQ_KEY,PDX_NTNL_PROV_ID,TRANS_TYP_CD,TRANS_DENIED_DT,RX_NO,PRCS_DT,SRC_SYS_CLM_RCVD_DT,SRC_SYS_CLM_RCVD_TM,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,MBR_PDX_DENIED_TRANS_SK",
      map=['iifNull(lnk_Natural_Keys_out.MBR_UNIQ_KEY, 0, lnk_Natural_Keys_out.MBR_UNIQ_KEY)','lnk_Natural_Keys_out.PDX_NTNL_PROV_ID','lnk_Natural_Keys_out.TRANS_TYP_CD','lnk_Natural_Keys_out.TRANS_DENIED_DT','lnk_Natural_Keys_out.RX_NO','lnk_Natural_Keys_out.PRCS_DT','lnk_Natural_Keys_out.SRC_SYS_CLM_RCVD_DT','lnk_Natural_Keys_out.SRC_SYS_CLM_RCVD_TM','lnk_Natural_Keys_out.SRC_SYS_CD','iifNull(lnk_KMbrPdxDeniedTransPkey_extr.CRT_RUN_CYC_EXCTN_SK, 0, lnk_KMbrPdxDeniedTransPkey_extr.CRT_RUN_CYC_EXCTN_SK)','iifNull(lnk_KMbrPdxDeniedTransPkey_extr.MBR_PDX_DENIED_TRANS_SK, 0, lnk_KMbrPdxDeniedTransPkey_extr.MBR_PDX_DENIED_TRANS_SK)'],
      aliasFrom="lnk_Natural_Keys_out,lnk_KMbrPdxDeniedTransPkey_extr", 
      aliasTo="rdupNaturalKeys,db2KMBRPDXDENIEDTRANSin",
      fillNull="True")}
    jnMbrPdx
        derive(
              MBR_UNIQ_KEY = MBR_UNIQ_KEY,
              PDX_NTNL_PROV_ID = PDX_NTNL_PROV_ID,
              TRANS_TYP_CD = TRANS_TYP_CD,
              TRANS_DENIED_DT = TRANS_DENIED_DT,
              RX_NO = RX_NO,
              PRCS_DT =  case(( case(!isNull((MBR_PDX_DENIED_TRANS_SK)), (MBR_PDX_DENIED_TRANS_SK), "")) == "", currentDate(), PRCS_DT),
              SRC_SYS_CLM_RCVD_DT = SRC_SYS_CLM_RCVD_DT,
              SRC_SYS_CLM_RCVD_TM = SRC_SYS_CLM_RCVD_TM,
              SRC_SYS_CD = SRC_SYS_CD,
              CRT_RUN_CYC_EXCTN_SK =  case(( case(!isNull((MBR_PDX_DENIED_TRANS_SK)), (MBR_PDX_DENIED_TRANS_SK), "")) == "", $IDSRunCycle, CRT_RUN_CYC_EXCTN_SK),
              MBR_PDX_DENIED_TRANS_SK = :svMbrPdxDeniedTransSK,
              LAST_UPDT_RUN_CYC_EXCTN_SK = $IDSRunCycle,
              svMbrPdxDeniedTransSK = :svMbrPdxDeniedTransSK,
              svMbrPdxDeniedTransSK := {' case(( case(!isNull((MBR_PDX_DENIED_TRANS_SK)), (MBR_PDX_DENIED_TRANS_SK), 0)) == 0, NextSurrogateKey(), npw_DS_TRIM_1(MBR_PDX_DENIED_TRANS_SK))'.replace("lnk_Mbr_PdxDeniedJnData_out.","")}) ~> xfrmPKEYgenV116S6P2Derived
        xfrmPKEYgenV116S6P2Derived
        select(mapColumn(
            MBR_UNIQ_KEY,
            PDX_NTNL_PROV_ID,
            TRANS_TYP_CD,
            TRANS_DENIED_DT,
            RX_NO,
            PRCS_DT,
            SRC_SYS_CLM_RCVD_DT,
            SRC_SYS_CLM_RCVD_TM,
            SRC_SYS_CD,
            CRT_RUN_CYC_EXCTN_SK,
            MBR_PDX_DENIED_TRANS_SK,
            LAST_UPDT_RUN_CYC_EXCTN_SK
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> xfrmPKEYgenV116S6P2
    jnMbrPdx
        derive(
              MBR_UNIQ_KEY = MBR_UNIQ_KEY,
              PDX_NTNL_PROV_ID = PDX_NTNL_PROV_ID,
              TRANS_TYP_CD = TRANS_TYP_CD,
              TRANS_DENIED_DT = TRANS_DENIED_DT,
              RX_NO = RX_NO,
              PRCS_DT = currentDate(),
              SRC_SYS_CLM_RCVD_DT = SRC_SYS_CLM_RCVD_DT,
              SRC_SYS_CLM_RCVD_TM = SRC_SYS_CLM_RCVD_TM,
              SRC_SYS_CD = SRC_SYS_CD,
              CRT_RUN_CYC_EXCTN_SK = $IDSRunCycle,
              MBR_PDX_DENIED_TRANS_SK = :svMbrPdxDeniedTransSK,
              svMbrPdxDeniedTransSK = :svMbrPdxDeniedTransSK,
              svMbrPdxDeniedTransSK := {' case(( case(!isNull((MBR_PDX_DENIED_TRANS_SK)), (MBR_PDX_DENIED_TRANS_SK), 0)) == 0, NextSurrogateKey(), npw_DS_TRIM_1(MBR_PDX_DENIED_TRANS_SK))'.replace("lnk_Mbr_PdxDeniedJnData_out.","")}) ~> xfrmPKEYgenV116S6P3mapDerived
        xfrmPKEYgenV116S6P3mapDerived
        select(mapColumn(
            MBR_UNIQ_KEY,
            PDX_NTNL_PROV_ID,
            TRANS_TYP_CD,
            TRANS_DENIED_DT,
            RX_NO,
            PRCS_DT,
            SRC_SYS_CLM_RCVD_DT,
            SRC_SYS_CLM_RCVD_TM,
            SRC_SYS_CD,
            CRT_RUN_CYC_EXCTN_SK,
            MBR_PDX_DENIED_TRANS_SK,
            svMbrPdxDeniedTransSK
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> xfrmPKEYgenV116S6P3map
    xfrmPKEYgenV116S6P3map
        filter(
            ( case(!isNull((MBR_PDX_DENIED_TRANS_SK)), (MBR_PDX_DENIED_TRANS_SK), 0)) == 0
        ) ~> xfrmPKEYgenV116S6P3
    xfrmPKEYgenV116S6P3 alterRow(insertIf(1==1)) ~> db2KMBRPDXDENIEDTRANSloadAlterRows
    db2KMBRPDXDENIEDTRANSloadAlterRows sink(
      allowSchemaDrift: true,
      validateSchema: false,
      format: 'table',
      store: 'sqlserver',
      schemaName: (""" + ctx.getSchemaName("""$IDSOwner + '.K_MBR_PDX_DENIED_TRANS'""") + f"""),
      tableName: (""" + ctx.getTableName("""$IDSOwner + '.K_MBR_PDX_DENIED_TRANS'""") + f"""),
      insertable:{'true' if 'insertIf' == 'insertIf' else 'false'},
      updateable:{'true' if 'insertIf' == 'updateIf' else 'false'},
      deletable:{'true' if 'insertIf' == 'deleteIf' else 'false'},
      upsertable:{'true' if 'insertIf' == 'upsertIf' else 'false'},
      keys:['MBR_UNIQ_KEY', 'PDX_NTNL_PROV_ID', 'TRANS_TYP_CD', 'TRANS_DENIED_DT', 'RX_NO', 'PRCS_DT', 'SRC_SYS_CLM_RCVD_DT', 'SRC_SYS_CLM_RCVD_TM', 'SRC_SYS_CD'],
      preSQLs: [],
      postSQLs: [],
      skipDuplicateMapInputs: true,
      skipDuplicateMapOutputs: true,
      errorHandlingOption: 'stopOnFirstError') ~> db2KMBRPDXDENIEDTRANSload
    dsMBRPDXDENIEDTRANSxfm
        derive(
              MBR_UNIQ_KEY = MBR_UNIQ_KEY,
              PDX_NTNL_PROV_ID = PDX_NTNL_PROV_ID,
              TRANS_TYP_CD = TRANS_TYP_CD,
              TRANS_DENIED_DT = TRANS_DENIED_DT,
              RX_NO = RX_NO,
              SRC_SYS_CD = SRC_SYS_CD,
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
              SUB_LAST_NM = SUB_LAST_NM,
              CLNT_ELIG_MBRSH_ID = CLNT_ELIG_MBRSH_ID,
              PRSN_NO = PRSN_NO,
              RELSHP_CD = RELSHP_CD,
              GNDR_CD = GNDR_CD,
              CLM_RCVD_TM = CLM_RCVD_TM,
              MSG_TYP_CD = MSG_TYP_CD,
              MSG_TYP = MSG_TYP,
              MSG_TX = MSG_TX,
              CLNT_MBRSH_ID = CLNT_MBRSH_ID,
              MEDIA_TYP_CD = MEDIA_TYP_CD,
              RSPN_CD = RSPN_CD,
              DESC = DESC,
              CHAPTER_ID = CHAPTER_ID,
              CHAPTER_DESC = CHAPTER_DESC,
              PDX_NPI = PDX_NPI,
              NDC = NDC) ~> CpyV0S33P4Derived
        CpyV0S33P4Derived
        select(mapColumn(
            MBR_UNIQ_KEY,
            PDX_NTNL_PROV_ID,
            TRANS_TYP_CD,
            TRANS_DENIED_DT,
            RX_NO,
            SRC_SYS_CD,
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
            SUB_LAST_NM,
            CLNT_ELIG_MBRSH_ID,
            PRSN_NO,
            RELSHP_CD,
            GNDR_CD,
            CLM_RCVD_TM,
            MSG_TYP_CD,
            MSG_TYP,
            MSG_TX,
            CLNT_MBRSH_ID,
            MEDIA_TYP_CD,
            RSPN_CD,
            DESC,
            CHAPTER_ID,
            CHAPTER_DESC,
            PDX_NPI,
            NDC
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> CpyV0S33P4
    {ctx.joinScript(joinType='inner', 
      master=['xfrmPKEYgenV116S6P2', 'MBR_UNIQ_KEY,PDX_NTNL_PROV_ID,TRANS_TYP_CD,TRANS_DENIED_DT,RX_NO,PRCS_DT,SRC_SYS_CLM_RCVD_DT,SRC_SYS_CLM_RCVD_TM,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,MBR_PDX_DENIED_TRANS_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,svMbrPdxDeniedTransSK'],
      transformationName='jnPKey', 
      references=[['CpyV0S33P4', ctx.joinConditionAndFixDataTypeMismatch(masterName='xfrmPKEYgenV116S6P2', masterDataTypes={'MBR_UNIQ_KEY':'integer','PDX_NTNL_PROV_ID':'string','TRANS_TYP_CD':'string','TRANS_DENIED_DT':'date','RX_NO':'string','PRCS_DT':'date','SRC_SYS_CLM_RCVD_DT':'date','SRC_SYS_CLM_RCVD_TM':'timestamp','SRC_SYS_CD':'string','CRT_RUN_CYC_EXCTN_SK':'integer','MBR_PDX_DENIED_TRANS_SK':'integer','LAST_UPDT_RUN_CYC_EXCTN_SK':'integer','svMbrPdxDeniedTransSK':'integer'}, refName='CpyV0S33P4', refDataTypes={'MBR_UNIQ_KEY':'integer','PDX_NTNL_PROV_ID':'string','TRANS_TYP_CD':'string','TRANS_DENIED_DT':'date','RX_NO':'string','SRC_SYS_CD':'string','GRP_SK':'integer','MBR_SK':'integer','NDC_SK':'integer','PRSCRB_PROV_SK':'integer','PROV_SPEC_CD_SK':'integer','SRV_PROV_SK':'integer','SUB_SK':'integer','MEDIA_TYP_CD_SK':'string','MBR_GNDR_CD_SK':'integer','MBR_RELSHP_CD_SK':'integer','PDX_RSPN_RSN_CD_SK':'string','PDX_RSPN_TYP_CD_SK':'integer','TRANS_TYP_CD_SK':'integer','MAIL_ORDER_IN':'string','MBR_BRTH_DT':'date','SRC_SYS_CLM_RCVD_DT':'date','SRC_SYS_CLM_RCVD_TM':'timestamp','SUB_BRTH_DT':'date','BILL_RX_DISPENSE_FEE_AMT':'decimal(13, 2)','BILL_RX_GROS_APRV_AMT':'decimal(13, 2)','BILL_RX_NET_CHK_AMT':'decimal(13, 2)','BILL_RX_PATN_PAY_AMT':'decimal(13, 2)','INGR_CST_ALW_AMT':'decimal(13, 2)','TRANS_MO_NO':'integer','TRANS_YR_NO':'integer','MBR_ID':'string','MBR_SFX_NO':'string','MBR_FIRST_NM':'string','MBR_LAST_NM':'string','MBR_SSN':'string','PDX_NM':'string','PDX_PHN_NO':'string','PHYS_DEA_NO':'string','PHYS_NTNL_PROV_ID':'string','PHYS_FIRST_NM':'string','PHYS_LAST_NM':'string','PHYS_ST_ADDR_LN':'string','PHYS_CITY_NM':'string','PHYS_ST_CD':'string','PHYS_POSTAL_CD':'string','RX_LABEL_TX':'string','SVC_PROV_NABP_NM':'string','SRC_SYS_CAR_ID':'string','SRC_SYS_CLNT_ORG_ID':'string','SRC_SYS_CLNT_ORG_NM':'string','SRC_SYS_CNTR_ID':'string','SRC_SYS_GRP_ID':'string','SUB_ID':'string','SUB_FIRST_NM':'string','SUB_LAST_NM':'string','CLNT_ELIG_MBRSH_ID':'string','PRSN_NO':'string','RELSHP_CD':'string','GNDR_CD':'string','CLM_RCVD_TM':'string','MSG_TYP_CD':'string','MSG_TYP':'string','MSG_TX':'string','CLNT_MBRSH_ID':'string','MEDIA_TYP_CD':'string','RSPN_CD':'string','DESC':'string','CHAPTER_ID':'string','CHAPTER_DESC':'string','PDX_NPI':'string','NDC':'string'}, joinFields='MBR_UNIQ_KEY,PDX_NTNL_PROV_ID,TRANS_TYP_CD,TRANS_DENIED_DT,RX_NO,SRC_SYS_CLM_RCVD_DT,SRC_SYS_CLM_RCVD_TM,SRC_SYS_CD'), 'MBR_UNIQ_KEY,PDX_NTNL_PROV_ID,TRANS_TYP_CD,TRANS_DENIED_DT,RX_NO,SRC_SYS_CD,GRP_SK,MBR_SK,NDC_SK,PRSCRB_PROV_SK,PROV_SPEC_CD_SK,SRV_PROV_SK,SUB_SK,MEDIA_TYP_CD_SK,MBR_GNDR_CD_SK,MBR_RELSHP_CD_SK,PDX_RSPN_RSN_CD_SK,PDX_RSPN_TYP_CD_SK,TRANS_TYP_CD_SK,MAIL_ORDER_IN,MBR_BRTH_DT,SRC_SYS_CLM_RCVD_DT,SRC_SYS_CLM_RCVD_TM,SUB_BRTH_DT,BILL_RX_DISPENSE_FEE_AMT,BILL_RX_GROS_APRV_AMT,BILL_RX_NET_CHK_AMT,BILL_RX_PATN_PAY_AMT,INGR_CST_ALW_AMT,TRANS_MO_NO,TRANS_YR_NO,MBR_ID,MBR_SFX_NO,MBR_FIRST_NM,MBR_LAST_NM,MBR_SSN,PDX_NM,PDX_PHN_NO,PHYS_DEA_NO,PHYS_NTNL_PROV_ID,PHYS_FIRST_NM,PHYS_LAST_NM,PHYS_ST_ADDR_LN,PHYS_CITY_NM,PHYS_ST_CD,PHYS_POSTAL_CD,RX_LABEL_TX,SVC_PROV_NABP_NM,SRC_SYS_CAR_ID,SRC_SYS_CLNT_ORG_ID,SRC_SYS_CLNT_ORG_NM,SRC_SYS_CNTR_ID,SRC_SYS_GRP_ID,SUB_ID,SUB_FIRST_NM,SUB_LAST_NM,CLNT_ELIG_MBRSH_ID,PRSN_NO,RELSHP_CD,GNDR_CD,CLM_RCVD_TM,MSG_TYP_CD,MSG_TYP,MSG_TX,CLNT_MBRSH_ID,MEDIA_TYP_CD,RSPN_CD,DESC,CHAPTER_ID,CHAPTER_DESC,PDX_NPI,NDC']],
      schema="MBR_PDX_DENIED_TRANS_SK,MBR_UNIQ_KEY,PDX_NTNL_PROV_ID,TRANS_TYP_CD,TRANS_DENIED_DT,RX_NO,PRCS_DT,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,GRP_SK,MBR_SK,NDC_SK,PRSCRB_PROV_SK,PROV_SPEC_CD_SK,SRV_PROV_SK,SUB_SK,MEDIA_TYP_CD_SK,MBR_GNDR_CD_SK,MBR_RELSHP_CD_SK,PDX_RSPN_RSN_CD_SK,PDX_RSPN_TYP_CD_SK,TRANS_TYP_CD_SK,MAIL_ORDER_IN,MBR_BRTH_DT,SRC_SYS_CLM_RCVD_DT,SRC_SYS_CLM_RCVD_TM,SUB_BRTH_DT,BILL_RX_DISPENSE_FEE_AMT,BILL_RX_GROS_APRV_AMT,BILL_RX_NET_CHK_AMT,BILL_RX_PATN_PAY_AMT,INGR_CST_ALW_AMT,TRANS_MO_NO,TRANS_YR_NO,MBR_ID,MBR_SFX_NO,MBR_FIRST_NM,MBR_LAST_NM,MBR_SSN,PDX_NM,PDX_PHN_NO,PHYS_DEA_NO,PHYS_NTNL_PROV_ID,PHYS_FIRST_NM,PHYS_LAST_NM,PHYS_ST_ADDR_LN,PHYS_CITY_NM,PHYS_ST_CD,PHYS_POSTAL_CD,RX_LABEL_TX,SVC_PROV_NABP_NM,SRC_SYS_CAR_ID,SRC_SYS_CLNT_ORG_ID,SRC_SYS_CLNT_ORG_NM,SRC_SYS_CNTR_ID,SRC_SYS_GRP_ID,SUB_ID,SUB_FIRST_NM,SUB_LAST_NM,CLNT_ELIG_MBRSH_ID,PRSN_NO,RELSHP_CD,GNDR_CD,CLM_RCVD_TM,MSG_TYP_CD,MSG_TYP,MSG_TX,CLNT_MBRSH_ID,MEDIA_TYP_CD,RSPN_CD,DESC,CHAPTER_ID,CHAPTER_DESC,PDX_NPI,NDC",
      map=['lnk_Pkey_out.MBR_PDX_DENIED_TRANS_SK','lnk_Pkey_out.MBR_UNIQ_KEY','lnk_Pkey_out.PDX_NTNL_PROV_ID','lnk_Pkey_out.TRANS_TYP_CD','lnk_Pkey_out.TRANS_DENIED_DT','lnk_Pkey_out.RX_NO','lnk_Pkey_out.PRCS_DT','lnk_Pkey_out.SRC_SYS_CD','lnk_Pkey_out.CRT_RUN_CYC_EXCTN_SK','lnk_Pkey_out.LAST_UPDT_RUN_CYC_EXCTN_SK','lnk_IdsMbr_PdxDeniedPkey_All.GRP_SK','lnk_IdsMbr_PdxDeniedPkey_All.MBR_SK','lnk_IdsMbr_PdxDeniedPkey_All.NDC_SK','lnk_IdsMbr_PdxDeniedPkey_All.PRSCRB_PROV_SK','lnk_IdsMbr_PdxDeniedPkey_All.PROV_SPEC_CD_SK','lnk_IdsMbr_PdxDeniedPkey_All.SRV_PROV_SK','lnk_IdsMbr_PdxDeniedPkey_All.SUB_SK','lnk_IdsMbr_PdxDeniedPkey_All.MEDIA_TYP_CD_SK','lnk_IdsMbr_PdxDeniedPkey_All.MBR_GNDR_CD_SK','lnk_IdsMbr_PdxDeniedPkey_All.MBR_RELSHP_CD_SK','lnk_IdsMbr_PdxDeniedPkey_All.PDX_RSPN_RSN_CD_SK','lnk_IdsMbr_PdxDeniedPkey_All.PDX_RSPN_TYP_CD_SK','lnk_IdsMbr_PdxDeniedPkey_All.TRANS_TYP_CD_SK','lnk_IdsMbr_PdxDeniedPkey_All.MAIL_ORDER_IN','lnk_IdsMbr_PdxDeniedPkey_All.MBR_BRTH_DT','lnk_Pkey_out.SRC_SYS_CLM_RCVD_DT','lnk_Pkey_out.SRC_SYS_CLM_RCVD_TM','lnk_IdsMbr_PdxDeniedPkey_All.SUB_BRTH_DT','lnk_IdsMbr_PdxDeniedPkey_All.BILL_RX_DISPENSE_FEE_AMT','lnk_IdsMbr_PdxDeniedPkey_All.BILL_RX_GROS_APRV_AMT','lnk_IdsMbr_PdxDeniedPkey_All.BILL_RX_NET_CHK_AMT','lnk_IdsMbr_PdxDeniedPkey_All.BILL_RX_PATN_PAY_AMT','lnk_IdsMbr_PdxDeniedPkey_All.INGR_CST_ALW_AMT','lnk_IdsMbr_PdxDeniedPkey_All.TRANS_MO_NO','lnk_IdsMbr_PdxDeniedPkey_All.TRANS_YR_NO','lnk_IdsMbr_PdxDeniedPkey_All.MBR_ID','lnk_IdsMbr_PdxDeniedPkey_All.MBR_SFX_NO','lnk_IdsMbr_PdxDeniedPkey_All.MBR_FIRST_NM','lnk_IdsMbr_PdxDeniedPkey_All.MBR_LAST_NM','lnk_IdsMbr_PdxDeniedPkey_All.MBR_SSN','lnk_IdsMbr_PdxDeniedPkey_All.PDX_NM','lnk_IdsMbr_PdxDeniedPkey_All.PDX_PHN_NO','lnk_IdsMbr_PdxDeniedPkey_All.PHYS_DEA_NO','lnk_IdsMbr_PdxDeniedPkey_All.PHYS_NTNL_PROV_ID','lnk_IdsMbr_PdxDeniedPkey_All.PHYS_FIRST_NM','lnk_IdsMbr_PdxDeniedPkey_All.PHYS_LAST_NM','lnk_IdsMbr_PdxDeniedPkey_All.PHYS_ST_ADDR_LN','lnk_IdsMbr_PdxDeniedPkey_All.PHYS_CITY_NM','lnk_IdsMbr_PdxDeniedPkey_All.PHYS_ST_CD','lnk_IdsMbr_PdxDeniedPkey_All.PHYS_POSTAL_CD','lnk_IdsMbr_PdxDeniedPkey_All.RX_LABEL_TX','lnk_IdsMbr_PdxDeniedPkey_All.SVC_PROV_NABP_NM','lnk_IdsMbr_PdxDeniedPkey_All.SRC_SYS_CAR_ID','lnk_IdsMbr_PdxDeniedPkey_All.SRC_SYS_CLNT_ORG_ID','lnk_IdsMbr_PdxDeniedPkey_All.SRC_SYS_CLNT_ORG_NM','lnk_IdsMbr_PdxDeniedPkey_All.SRC_SYS_CNTR_ID','lnk_IdsMbr_PdxDeniedPkey_All.SRC_SYS_GRP_ID','lnk_IdsMbr_PdxDeniedPkey_All.SUB_ID','lnk_IdsMbr_PdxDeniedPkey_All.SUB_FIRST_NM','lnk_IdsMbr_PdxDeniedPkey_All.SUB_LAST_NM','lnk_IdsMbr_PdxDeniedPkey_All.CLNT_ELIG_MBRSH_ID','lnk_IdsMbr_PdxDeniedPkey_All.PRSN_NO','lnk_IdsMbr_PdxDeniedPkey_All.RELSHP_CD','lnk_IdsMbr_PdxDeniedPkey_All.GNDR_CD','lnk_IdsMbr_PdxDeniedPkey_All.CLM_RCVD_TM','lnk_IdsMbr_PdxDeniedPkey_All.MSG_TYP_CD','lnk_IdsMbr_PdxDeniedPkey_All.MSG_TYP','lnk_IdsMbr_PdxDeniedPkey_All.MSG_TX','lnk_IdsMbr_PdxDeniedPkey_All.CLNT_MBRSH_ID','lnk_IdsMbr_PdxDeniedPkey_All.MEDIA_TYP_CD','lnk_IdsMbr_PdxDeniedPkey_All.RSPN_CD','lnk_IdsMbr_PdxDeniedPkey_All.DESC','lnk_IdsMbr_PdxDeniedPkey_All.CHAPTER_ID','lnk_IdsMbr_PdxDeniedPkey_All.CHAPTER_DESC','lnk_IdsMbr_PdxDeniedPkey_All.PDX_NPI','lnk_IdsMbr_PdxDeniedPkey_All.NDC'],
      aliasFrom="lnk_Pkey_out,lnk_IdsMbr_PdxDeniedPkey_All", 
      aliasTo="xfrmPKEYgenV116S6P2,CpyV0S33P4")}
    jnPKey sink(
      allowSchemaDrift: true,
      validateSchema: false,
      partitionFileNames:[(""" + ctx.getFileName("""$FilePath + '/key/MBR_PDX_DENIED_TRANS.' + $SrcSysCd + '.pkey.' + $RunID + '.dat'""") + f""")],
      partitionBy('hash', 1),
      skipDuplicateMapInputs: true,
      skipDuplicateMapOutputs: true,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: (""" + ctx.getFolderPath("""$FilePath + '/key/MBR_PDX_DENIED_TRANS.' + $SrcSysCd + '.pkey.' + $RunID + '.dat'""") + f""") ,
      columnDelimiter: ',',
      escapeChar:""" + '\\\\' + f""",
      rowDelimiter: '\\n',
      quoteChar: '',
      columnNamesAsHeader:false) ~> seqMBRPDXDENIEDTRANSPkey""", type='MappingDataFlow')
    artifacts = buffer.artifacts + [(dataflowName, dataflow)]
    return artifacts, dataflowName
  artifacts, dataflowName = dfIdsMbrPdxDeniedClmPkey()
  activityName = "IdsMbrPdxDeniedClmPkey"
  activityParameters = {"FilePath":  {"value": "'@{pipeline().parameters.FilePath}'","type": "Expression"},
  "APT_CONFIG_FILE":  {"value": "'@{pipeline().parameters.APT_CONFIG_FILE}'","type": "Expression"},
  "IDSDB":  {"value": "'@{pipeline().parameters.IDSDB}'","type": "Expression"},
  "IDSOwner":  {"value": "'@{pipeline().parameters.IDSOwner}'","type": "Expression"},
  "IDSAcct":  {"value": "'@{pipeline().parameters.IDSAcct}'","type": "Expression"},
  "IDSPW":  {"value": "'@{pipeline().parameters.IDSPW}'","type": "Expression"},
  "IDSRunCycle":  {"value": "'@{pipeline().parameters.IDSRunCycle}'","type": "Expression"},
  "SrcSysCd":  {"value": "'@{pipeline().parameters.SrcSysCd}'","type": "Expression"},
  "IDSDB2ArraySize":  {"value": "'@{pipeline().parameters.IDSDB2ArraySize}'","type": "Expression"},
  "IDSDB2RecordCount":  {"value": "'@{pipeline().parameters.IDSDB2RecordCount}'","type": "Expression"},
  "RunID":  {"value": "'@{pipeline().parameters.RunID}'","type": "Expression"}}
  activity = ExecuteDataFlowActivity(
    name = activityName,
    data_flow = DataFlowReference(reference_name=dataflowName,
      parameters=activityParameters, type='DataFlowReference'))
  return artifacts, [activity]

def IdsMbrPdxDeniedClmPkey(ctx):
  name = "IdsMbrPdxDeniedClmPkey"
  artifacts, activities = IdsMbrPdxDeniedClmPkeyActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="DataWarehouse/Claim/DeniedPDXClaims"),
    activities = activities,
    description = """
      Build primary key for  MBR_PDX_DENIED_TRANS.  K table will be read to see if there is an SK already available for the Natural Keys. 
      **********************************************************************************************************************************************************************
      COPYRIGHT 2014 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


      DESCRIPTION:  OPTUM Claims denied data is extracted from source file and loaded into the  MBR_PDX_DENIED_TRANS Table


      Called By: OptumIdsMbrPdxDeniedTransLoadSeq


      DEVELOPER             DATE                PROJECT                                                          DESCRIPTION                                                                                     ENVIRONMENT          REVIEWER                 REVIEW
      -----------------------------    ----------------------     ------------------------------                                             -----------------------------------------------------------------------------------                           ------------------------------    ------------------------------       --------------------                                          
      Peter Gichiri              2019-10-02           6131 - PBM REPLACEMENT  to OPTUMRX  Initial Devlopment                                                                                  Integrate2                     Kalyan Neelam             2019-11-21
      Peter Gichiri              2020-01-09           6131 - PBM REPLACEMENT                          If NullToEmpty(lnk_Mbr_PdxDeniedJnData_out.MBR_PDX_DE-                                               Kalyan Neelam             2020-01-09
                                                                                                                                            NIED_TRANS_SK) ='' then IDSRunCycle else  lnk_Mbr_PdxDen-
                                                                                                                                            iedJnData_out.CRT_RUN_CYC_EXCTN_SK
      Velmani Kondappan  20200-04-02      6131 - PBM REPLACEMEN                             Changed the sorting order within the rdup_Natural_Keys to remove        IntegrateDev2                     Kalyan Neelam            2020-04-09
                                                                                                                                          error warinings

      Rekha Radhakrishna 2020-10-20        6343 - PhaseII governmanet program             Added Keys in Join to K_ table                                                                 IntegrateDev2
      Parameters:
      -----------
      FilePath:
        IDS File Path
      APT_CONFIG_FILE:
        Configuration file
        The Parallel job configuration file.
      IDSDB:
        IDS Database
      IDSOwner:
        IDS Table Owner
      IDSAcct:
        IDS Account
      IDSPW:
        IDS Password
      IDSRunCycle:
        IDSRunCycle
      SrcSysCd:
        SrcSysCd
      IDSDB2ArraySize:
        IDS DB2 Array Size for Load
      IDSDB2RecordCount:
        IDS DB2 Record Count for Load
      RunID:
        RunID""",
    parameters = {
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "APT_CONFIG_FILE": ParameterSpecification(type="String", default_value=ctx.APT_CONFIG_FILE),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "IDSAcct": ParameterSpecification(type="String"),
      "IDSPW": ParameterSpecification(type="String"),
      "IDSRunCycle": ParameterSpecification(type="String"),
      "SrcSysCd": ParameterSpecification(type="String"),
      "IDSDB2ArraySize": ParameterSpecification(type="String", default_value=ctx.IDSDB2ArraySize),
      "IDSDB2RecordCount": ParameterSpecification(type="String", default_value=ctx.IDSDB2RecordCount),
      "RunID": ParameterSpecification(type="String")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (IdsMbrPdxDeniedClmPkey(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
