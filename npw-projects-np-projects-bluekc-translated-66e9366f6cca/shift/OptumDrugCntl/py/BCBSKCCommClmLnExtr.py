#!/usr/bin/python3

from npadf import *

def BCBSKCCommClmLnExtrActivities(ctx):
  def dfBCBSKCCommClmLnExtr():
    def BCBSKCCommClmLand():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sources_inline = DataFlowSource(name='BCBSKCCommClmLand', linked_service=ds_ls_blob)
       return DataflowSegment([df_sources_inline])
    def IDSNPI():
       ds_ls_db = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.sqldb_linked_service)
       df_sources_inline = DataFlowSource(name='IDSNPI',linked_service=ds_ls_db)
       return DataflowSegment([df_sources_inline])
    def hfbcbskccommprov():
       return DataflowSegment([
         Transformation(name='hfbcbskccommprovDerived'),
         Transformation(name='hfbcbskccommprov')])
    def BusinessRules():
       return DataflowSegment([
         Transformation(name="BusinessRules"),
         Transformation(name="BusinessRulesJoin1"),Transformation(name="BusinessRulesDerived1")])
    def SnapshotV0S133P2():
       return DataflowSegment([
         Transformation(name='SnapshotV0S133P2Derived'),
         Transformation(name='SnapshotV0S133P2')])
    def ClmLnPK():
       return ctx.sharedContainer(
         scriptModel=1,
         containerName='ClmLnPK',
         sourceName='SnapshotV0S133P2',
         context=ctx)
    def BCBSKCCommClmLnExtr():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sinks_inline = DataFlowSink(name='BCBSKCCommClmLnExtr', linked_service=ds_ls_blob)
       return DataflowSegment([df_sinks_inline])
    def SnapshotV0S133P3():
       return DataflowSegment([
         Transformation(name='SnapshotV0S133P3Derived'),
         Transformation(name='SnapshotV0S133P3')])
    def Transformer():
       return DataflowSegment([
         Transformation(name='TransformerDerived'),
         Transformation(name='Transformer')])
    def BCLMLN():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sinks_inline = DataFlowSink(name='BCLMLN', linked_service=ds_ls_blob)
       return DataflowSegment([df_sinks_inline])
    dataflowName = "BCBSKCCommClmLnExtr"
    buffer = DataflowBuffer()
    buffer.append(BCBSKCCommClmLand())
    buffer.append(IDSNPI())
    buffer.append(hfbcbskccommprov())
    buffer.append(BusinessRules())
    buffer.append(SnapshotV0S133P2())
    buffer.append(ClmLnPK())
    buffer.append(BCBSKCCommClmLnExtr())
    buffer.append(SnapshotV0S133P3())
    buffer.append(Transformer())
    buffer.append(BCLMLN())
    dataflow = MappingDataFlow(
      folder = DataFlowFolder(name="claim/ClaimLine/ClmLn/BCBSKCCommon"),
      sources = buffer.sources,
      sinks = buffer.sinks,
      transformations = buffer.transformations,
      script = f"""parameters{{
        FilePath as string ("{ctx.FilePath}"),
        IDSInstance as string ("{ctx.IDSInstance}"),
        IDSDB as string ("{ctx.IDSDB}"),
        IDSOwner as string ("{ctx.IDSOwner}"),
        IDSAcct as string ,
        IDSPW as string ,
        CurrentDate as string ,
        SrcSysCd as string ,
        SrcSysCdSk as string ,
        RunID as string ,
        RunCycle as integer 
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
      columnNamesAsHeader:false) ~> BCBSKCCommClmLand
    source(output(
        NTNL_PROV_ID as string,
        PROV_ID as string),
      allowSchemaDrift: true,
      validateSchema: false,
      format: 'query',
      store: 'sqlserver',
      query: ("SELECT \\n\\nPROV.NTNL_PROV_ID,\\nPROV.PROV_ID\\n\\nFROM\\n" + toString($IDSOwner) + ".PROV PROV\\n"),
      schemaName: '',
      tableName: '',
      isolationLevel: 'READ_UNCOMMITTED') ~> IDSNPI
    IDSNPI
        derive(
              NTNL_PROV_ID = NTNL_PROV_ID,
              PROV_ID = PROV_ID) ~> hfbcbskccommprovDerived
        hfbcbskccommprovDerived
        select(mapColumn(
            NTNL_PROV_ID,
            PROV_ID
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> hfbcbskccommprov
    {ctx.joinScript(joinType='left',
      master=['BCBSKCCommClmLand', 'SRC_SYS_CD,FILE_RCVD_DT,RCRD_ID,PRCSR_NO,BTCH_NO,PDX_NO,RX_NO,FILL_DT,NDC,DRUG_DESC,NEW_OR_RFL_CD,METRIC_QTY,DAYS_SUPL,BSS_OF_CST_DTRM,INGR_CST_AMT,DISPNS_FEE_AMT,COPAY_AMT,SLS_TAX_AMT,BILL_AMT,PATN_FIRST_NM,PATN_LAST_NM,BRTH_DT,SEX_CD,CARDHLDR_ID_NO,RELSHP_CD,GRP_NO,HOME_PLN,HOST_PLN,PRSCRBR_ID,DIAG_CD,CARDHLDR_FIRST_NM,CARDHLDR_LAST_NM,PRAUTH_NO,PA_MC_SC_NO,CUST_LOC,RESUB_CYC_CT,RX_DT,DISPENSE_AS_WRTN_PROD_SEL_CD,PRSN_CD,OTHR_COV_CD,ELIG_CLRFCTN_CD,CMPND_CD,NO_OF_RFLS_AUTH,LVL_OF_SVC,RX_ORIG_CD,RX_DENIAL_CLRFCTN,PRI_PRSCRBR,CLNC_ID_NO,DRUG_TYP,PRSCRBR_LAST_NM,POSTAGE_AMT,UNIT_DOSE_IN,OTHR_PAYOR_AMT,BSS_OF_DAYS_SUPL_DTRM,FULL_AVG_WHLSL_PRICE,EXPNSN_AREA,MSTR_CAR,SUBCAR,CLM_TYP,SUBGRP,PLN_DSGNR,ADJDCT_DT,ADMIN_FEE_AMT,CAP_AMT,INGR_CST_SUB_AMT,MBR_NON_COPAY_AMT,MBR_PAY_CD,INCNTV_FEE_AMT,CLM_ADJ_AMT,CLM_ADJ_CD,FRMLRY_FLAG,GNRC_CLS_NO,THRPTC_CLS_AHFS,PDX_TYP,BILL_BSS_CD,USL_AND_CUST_CHRG_AMT,PD_DT,BNF_CD,DRUG_STRG,ORIG_MBR,INJRY_DT,FEE_AMT,REF_NO,CLNT_CUST_ID,PLN_TYP,ADJDCT_REF_NO,ANCLRY_AMT,CLNT_GNRL_PRPS_AREA,PRTL_FILL_STTUS_CD,BILL_DT,FSA_VNDR_CD,PICA_DRUG_CD,CLM_AMT,DSALW_AMT,FED_DRUG_CLS_CD,DEDCT_AMT,BNF_COPAY_100,CLM_PRCS_TYP,INDEM_HIER_TIER_NO,MCARE_D_COV_DRUG,RETRO_LICS_CD,RETRO_LICS_AMT,LICS_SBSDY_AMT,MCARE_B_DRUG,MCARE_B_CLM,PRSCRBR_QLFR,PRSCRBR_NTNL_PROV_ID,PDX_QLFR,PDX_NTNL_PROV_ID,HLTH_RMBRMT_ARGMT_APLD_AMT,THER_CLS,HIC_NO,HLTH_RMBRMT_ARGMT_FLAG,DOSE_CD,LOW_INCM,RTE_OF_ADMIN,DEA_SCHD,COPAY_BNF_OPT,GNRC_PROD_IN,PRSCRBR_SPEC,VAL_CD,PRI_CARE_PDX,OFC_OF_INSPECTOR_GNRL,PATN_SSN,CARDHLDR_SSN,CARDHLDR_BRTH_DT,CARDHLDR_ADDR,CARDHLDR_CITY,CHADHLDR_ST,CARDHLDR_ZIP_CD,PSL_FMLY_MET_AMT,PSL_MBR_MET_AMT,PSL_FMLY_AMT,DEDCT_FMLY_MET_AMT,DEDCT_FMLY_AMT,MOPS_FMLY_AMT,MOPS_FMLY_MET_AMT,MOPS_MBR_MET_AMT,DEDCT_MBR_MET_AMT,PSL_APLD_AMT,MOPS_APLD_AMT,PAR_PDX_IN,COPAY_PCT_AMT,COPAY_FLAT_AMT,CLM_TRNSMSN_METH,RX_NO_2012,CLM_ID,CLM_STTUS_CD,ADJ_FROM_CLM_ID,ADJ_TO_CLM_ID,SUBMT_PROD_ID_QLFR,CNTNGNT_THER_FLAG,CNTNGNT_THER_SCHD,CLNT_PATN_PAY_ATRBD_PROD_AMT,CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT,CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT,CLNT_PATN_PAY_ATRBD_NTWK_AMT,LOB_IN'],
      transformationName='BusinessRules',
      references=[['hfbcbskccommprov', ctx.joinConditionForLookup(master={'name':'BCBSKCCommClmLand','alias':'Extract','types':{'SRC_SYS_CD':'string','FILE_RCVD_DT':'string','RCRD_ID':'decimal(2, 0)','PRCSR_NO':'string','BTCH_NO':'decimal(5, 0)','PDX_NO':'decimal(12, 0)','RX_NO':'decimal(9, 0)','FILL_DT':'string','NDC':'decimal(20, 0)','DRUG_DESC':'string','NEW_OR_RFL_CD':'integer','METRIC_QTY':'decimal(10, 3)','DAYS_SUPL':'decimal(3, 0)','BSS_OF_CST_DTRM':'string','INGR_CST_AMT':'decimal(13, 2)','DISPNS_FEE_AMT':'decimal(13, 2)','COPAY_AMT':'decimal(13, 2)','SLS_TAX_AMT':'decimal(13, 2)','BILL_AMT':'decimal(13, 2)','PATN_FIRST_NM':'string','PATN_LAST_NM':'string','BRTH_DT':'string','SEX_CD':'decimal(1, 0)','CARDHLDR_ID_NO':'string','RELSHP_CD':'decimal(1, 0)','GRP_NO':'string','HOME_PLN':'string','HOST_PLN':'string','PRSCRBR_ID':'string','DIAG_CD':'string','CARDHLDR_FIRST_NM':'string','CARDHLDR_LAST_NM':'string','PRAUTH_NO':'string','PA_MC_SC_NO':'string','CUST_LOC':'decimal(2, 0)','RESUB_CYC_CT':'decimal(2, 0)','RX_DT':'string','DISPENSE_AS_WRTN_PROD_SEL_CD':'string','PRSN_CD':'string','OTHR_COV_CD':'decimal(2, 0)','ELIG_CLRFCTN_CD':'decimal(1, 0)','CMPND_CD':'decimal(1, 0)','NO_OF_RFLS_AUTH':'decimal(2, 0)','LVL_OF_SVC':'decimal(2, 0)','RX_ORIG_CD':'decimal(1, 0)','RX_DENIAL_CLRFCTN':'decimal(2, 0)','PRI_PRSCRBR':'string','CLNC_ID_NO':'decimal(5, 0)','DRUG_TYP':'decimal(1, 0)','PRSCRBR_LAST_NM':'string','POSTAGE_AMT':'decimal(13, 2)','UNIT_DOSE_IN':'decimal(1, 0)','OTHR_PAYOR_AMT':'decimal(13, 2)','BSS_OF_DAYS_SUPL_DTRM':'decimal(1, 0)','FULL_AVG_WHLSL_PRICE':'decimal(13, 2)','EXPNSN_AREA':'string','MSTR_CAR':'string','SUBCAR':'string','CLM_TYP':'string','SUBGRP':'string','PLN_DSGNR':'string','ADJDCT_DT':'string','ADMIN_FEE_AMT':'decimal(13, 2)','CAP_AMT':'decimal(13, 2)','INGR_CST_SUB_AMT':'decimal(13, 2)','MBR_NON_COPAY_AMT':'decimal(13, 2)','MBR_PAY_CD':'string','INCNTV_FEE_AMT':'decimal(13, 2)','CLM_ADJ_AMT':'decimal(13, 2)','CLM_ADJ_CD':'string','FRMLRY_FLAG':'string','GNRC_CLS_NO':'string','THRPTC_CLS_AHFS':'string','PDX_TYP':'string','BILL_BSS_CD':'string','USL_AND_CUST_CHRG_AMT':'decimal(13, 2)','PD_DT':'string','BNF_CD':'string','DRUG_STRG':'string','ORIG_MBR':'string','INJRY_DT':'string','FEE_AMT':'decimal(13, 2)','REF_NO':'string','CLNT_CUST_ID':'string','PLN_TYP':'string','ADJDCT_REF_NO':'decimal(9, 0)','ANCLRY_AMT':'decimal(13, 2)','CLNT_GNRL_PRPS_AREA':'string','PRTL_FILL_STTUS_CD':'string','BILL_DT':'string','FSA_VNDR_CD':'string','PICA_DRUG_CD':'string','CLM_AMT':'decimal(13, 2)','DSALW_AMT':'decimal(13, 2)','FED_DRUG_CLS_CD':'string','DEDCT_AMT':'decimal(13, 2)','BNF_COPAY_100':'string','CLM_PRCS_TYP':'string','INDEM_HIER_TIER_NO':'decimal(4, 0)','MCARE_D_COV_DRUG':'string','RETRO_LICS_CD':'string','RETRO_LICS_AMT':'decimal(13, 2)','LICS_SBSDY_AMT':'decimal(13, 2)','MCARE_B_DRUG':'string','MCARE_B_CLM':'string','PRSCRBR_QLFR':'string','PRSCRBR_NTNL_PROV_ID':'string','PDX_QLFR':'string','PDX_NTNL_PROV_ID':'string','HLTH_RMBRMT_ARGMT_APLD_AMT':'decimal(11, 0)','THER_CLS':'decimal(6, 0)','HIC_NO':'string','HLTH_RMBRMT_ARGMT_FLAG':'string','DOSE_CD':'decimal(4, 0)','LOW_INCM':'string','RTE_OF_ADMIN':'string','DEA_SCHD':'decimal(1, 0)','COPAY_BNF_OPT':'decimal(10, 0)','GNRC_PROD_IN':'decimal(14, 0)','PRSCRBR_SPEC':'string','VAL_CD':'string','PRI_CARE_PDX':'string','OFC_OF_INSPECTOR_GNRL':'string','PATN_SSN':'string','CARDHLDR_SSN':'string','CARDHLDR_BRTH_DT':'string','CARDHLDR_ADDR':'string','CARDHLDR_CITY':'string','CHADHLDR_ST':'string','CARDHLDR_ZIP_CD':'string','PSL_FMLY_MET_AMT':'decimal(13, 2)','PSL_MBR_MET_AMT':'decimal(13, 2)','PSL_FMLY_AMT':'decimal(13, 2)','DEDCT_FMLY_MET_AMT':'string','DEDCT_FMLY_AMT':'decimal(13, 2)','MOPS_FMLY_AMT':'decimal(13, 2)','MOPS_FMLY_MET_AMT':'decimal(13, 2)','MOPS_MBR_MET_AMT':'decimal(13, 2)','DEDCT_MBR_MET_AMT':'decimal(13, 2)','PSL_APLD_AMT':'decimal(13, 2)','MOPS_APLD_AMT':'decimal(13, 2)','PAR_PDX_IN':'string','COPAY_PCT_AMT':'decimal(13, 2)','COPAY_FLAT_AMT':'decimal(13, 2)','CLM_TRNSMSN_METH':'string','RX_NO_2012':'decimal(12, 0)','CLM_ID':'string','CLM_STTUS_CD':'string','ADJ_FROM_CLM_ID':'string','ADJ_TO_CLM_ID':'string','SUBMT_PROD_ID_QLFR':'string','CNTNGNT_THER_FLAG':'string','CNTNGNT_THER_SCHD':'string','CLNT_PATN_PAY_ATRBD_PROD_AMT':'decimal(13, 2)','CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT':'decimal(13, 2)','CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT':'decimal(13, 2)','CLNT_PATN_PAY_ATRBD_NTWK_AMT':'decimal(13, 2)','LOB_IN':'string'}}, reference={'name':'hfbcbskccommprov','alias':'natln_prov','types':{'NTNL_PROV_ID':'string','PROV_ID':'string'}}, joinFields=[{'master':'Extract.PDX_NTNL_PROV_ID','reference':'natln_prov.NTNL_PROV_ID', 'operator':'=='}]), 'NTNL_PROV_ID,PROV_ID']],
      schema="JOB_EXCTN_RCRD_ERR_SK,INSRT_UPDT_CD,DISCARD_IN,PASS_THRU_IN,FIRST_RECYC_DT,ERR_CT,RECYCLE_CT,SRC_SYS_CD,PRI_KEY_STRING,CLM_LN_SK,CLM_ID,CLM_LN_SEQ_NO,CRT_RUN_CYC_EXCTN_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,PROC_CD,SVC_PROV_ID,CLM_LN_DSALW_EXCD,CLM_LN_EOB_EXCD,CLM_LN_FINL_DISP_CD,CLM_LN_LOB_CD,CLM_LN_POS_CD,CLM_LN_PREAUTH_CD,CLM_LN_PREAUTH_SRC_CD,CLM_LN_PRICE_SRC_CD,CLM_LN_RFRL_CD,CLM_LN_RVNU_CD,CLM_LN_ROOM_PRICE_METH_CD,CLM_LN_ROOM_TYP_CD,CLM_LN_TOS_CD,CLM_LN_UNIT_TYP_CD,CAP_LN_IN,PRI_LOB_IN,SVC_END_DT,SVC_STRT_DT,AGMNT_PRICE_AMT,ALW_AMT,CHRG_AMT,COINS_AMT,CNSD_CHRG_AMT,COPAY_AMT,DEDCT_AMT,DSALW_AMT,ITS_HOME_DSCNT_AMT,NO_RESP_AMT,MBR_LIAB_BSS_AMT,PATN_RESP_AMT,PAYBL_AMT,PAYBL_TO_PROV_AMT,PAYBL_TO_SUB_AMT,PROC_TBL_PRICE_AMT,PROFL_PRICE_AMT,PROV_WRT_OFF_AMT,RISK_WTHLD_AMT,SVC_PRICE_AMT,SUPLMT_DSCNT_AMT,ALW_PRICE_UNIT_CT,UNIT_CT,DEDCT_AMT_ACCUM_ID,PREAUTH_SVC_SEQ_NO,RFRL_SVC_SEQ_NO,LMT_PFX_ID,PREAUTH_ID,PROD_CMPNT_DEDCT_PFX_ID,PROD_CMPNT_SVC_PAYMT_ID,RFRL_ID_TX,SVC_ID,SVC_PRICE_RULE_ID,SVC_RULE_TYP_TX,CLM_TYPE,SVC_LOC_TYP_CD,NON_PAR_SAV_AMT,PROC_CD_TYP_CD,PROC_CD_CAT_CD,NDC,APC_ID,APC_STTUS_ID,SNOMED_CT_CD,CVX_VCCN_CD",
      map=['0','"I"','"N"','"Y"','$CurrentDate','0','0','$SrcSysCd','$SrcSysCd + ";" + Extract.CLM_ID + ";" + "1"','0','Extract.CLM_ID','1','0','0','"NA"',':svServicingProvID','"NA"','"NA"','"ACPTD"','"NA"',' case(Extract.SRC_SYS_CD == "MEDIMPACT", npw_DS_TRIM_1(Extract.CLNT_GNRL_PRPS_AREA),  case(length(npw_DS_TRIM_1(substring(Extract.CLNT_GNRL_PRPS_AREA, 65, 2))) == 0, "NA",  case(Extract.SRC_SYS_CD == "OPTUMRX", npw_DS_TRIM_1(substring(Extract.CLNT_GNRL_PRPS_AREA, 65, 2)), "NA")))','"NA"','"NA"','"NA"','"NA"','"NA"','"NA"','"NA"','"NA"','"NA"','"N"','"N"','Extract.FILL_DT','Extract.FILL_DT','0.00',' case(Extract.SRC_SYS_CD == "OPTUMRX", :svAllowedAmt,  case(Extract.SRC_SYS_CD == "MEDIMPACT", :svMIAllowedAmt, 0.00))',':svChrgAmt',':svCoinsAmt',':svCnsdChrgAmt',' case(isNull(Extract.COPAY_AMT) || length(npw_DS_TRIM_1(Extract.COPAY_AMT)) == 0, "0.00", npw_DS_TRIM_1(Extract.COPAY_AMT))',' case(isNull(Extract.DEDCT_AMT) || length(npw_DS_TRIM_1(Extract.DEDCT_AMT)) == 0, "0.00", npw_DS_TRIM_1(Extract.DEDCT_AMT))','0.00','0.00','0.00','0.00',' case(Extract.SRC_SYS_CD == "OPTUMRX", npw_DS_TRIM_1(Extract.CLM_AMT),  case((isNull(Extract.COPAY_AMT) || length(npw_DS_TRIM_1(Extract.COPAY_AMT)) == 0), "0.00", npw_DS_TRIM_1(Extract.COPAY_AMT)))',' case(isNull(Extract.BILL_AMT) || length(npw_DS_TRIM_1(Extract.BILL_AMT)) == 0, "0.00",  case(Extract.SRC_SYS_CD == "OPTUMRX" || Extract.SRC_SYS_CD == "MEDIMPACT", npw_DS_TRIM_1(Extract.BILL_AMT), "0.00"))','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0','0','"NA"','"NA"','"NA"','"NA"','"NA"','"NA"','"NA"',' case(Extract.SRC_SYS_CD == "OPTUMRX", @Null, "NA")','"NA"','"NA"',' case(Extract.SRC_SYS_CD == "OPTUMRX", @Null, "NA")','"M"','"NA"','0.00','"NA"','"NA"',' case(Extract.SRC_SYS_CD == "MEDIMPACT", Extract.NDC,  case(((isNull(Extract.SRC_SYS_CD) && !isNull("OPTUMRX")) || (!isNull(Extract.SRC_SYS_CD) && isNull("OPTUMRX")) || Extract.SRC_SYS_CD != "OPTUMRX"), "NA",  case(isNull(npw_DS_TRIM_1(Extract.NDC)) == @True, "UNK",  case(Extract.CMPND_CD == "2", "99999999999", Extract.NDC))))',' case(((isNull(Extract.SRC_SYS_CD) && !isNull("OPTUMRX")) || (!isNull(Extract.SRC_SYS_CD) && isNull("OPTUMRX")) || Extract.SRC_SYS_CD != "OPTUMRX"), @Null, @Null)',' case(((isNull(Extract.SRC_SYS_CD) && !isNull("OPTUMRX")) || (!isNull(Extract.SRC_SYS_CD) && isNull("OPTUMRX")) || Extract.SRC_SYS_CD != "OPTUMRX"), @Null, @Null)','"NA"','"NA"'],
      aliasFrom="Extract,natln_prov",
      aliasTo="BCBSKCCommClmLand,hfbcbskccommprov")}
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
              CLM_LN_SK = CLM_LN_SK,
              CLM_ID = CLM_ID,
              CLM_LN_SEQ_NO = CLM_LN_SEQ_NO,
              CRT_RUN_CYC_EXCTN_SK = CRT_RUN_CYC_EXCTN_SK,
              LAST_UPDT_RUN_CYC_EXCTN_SK = LAST_UPDT_RUN_CYC_EXCTN_SK,
              PROC_CD = PROC_CD,
              SVC_PROV_ID = SVC_PROV_ID,
              CLM_LN_DSALW_EXCD = CLM_LN_DSALW_EXCD,
              CLM_LN_EOB_EXCD = CLM_LN_EOB_EXCD,
              CLM_LN_FINL_DISP_CD = CLM_LN_FINL_DISP_CD,
              CLM_LN_LOB_CD = CLM_LN_LOB_CD,
              CLM_LN_POS_CD = CLM_LN_POS_CD,
              CLM_LN_PREAUTH_CD = CLM_LN_PREAUTH_CD,
              CLM_LN_PREAUTH_SRC_CD = CLM_LN_PREAUTH_SRC_CD,
              CLM_LN_PRICE_SRC_CD = CLM_LN_PRICE_SRC_CD,
              CLM_LN_RFRL_CD = CLM_LN_RFRL_CD,
              CLM_LN_RVNU_CD = CLM_LN_RVNU_CD,
              CLM_LN_ROOM_PRICE_METH_CD = CLM_LN_ROOM_PRICE_METH_CD,
              CLM_LN_ROOM_TYP_CD = CLM_LN_ROOM_TYP_CD,
              CLM_LN_TOS_CD = CLM_LN_TOS_CD,
              CLM_LN_UNIT_TYP_CD = CLM_LN_UNIT_TYP_CD,
              CAP_LN_IN = CAP_LN_IN,
              PRI_LOB_IN = PRI_LOB_IN,
              SVC_END_DT = SVC_END_DT,
              SVC_STRT_DT = SVC_STRT_DT,
              AGMNT_PRICE_AMT = AGMNT_PRICE_AMT,
              ALW_AMT = ALW_AMT,
              CHRG_AMT = CHRG_AMT,
              COINS_AMT = COINS_AMT,
              CNSD_CHRG_AMT = CNSD_CHRG_AMT,
              COPAY_AMT = COPAY_AMT,
              DEDCT_AMT = DEDCT_AMT,
              DSALW_AMT = DSALW_AMT,
              ITS_HOME_DSCNT_AMT = ITS_HOME_DSCNT_AMT,
              NO_RESP_AMT = NO_RESP_AMT,
              MBR_LIAB_BSS_AMT = MBR_LIAB_BSS_AMT,
              PATN_RESP_AMT = PATN_RESP_AMT,
              PAYBL_AMT = PAYBL_AMT,
              PAYBL_TO_PROV_AMT = PAYBL_TO_PROV_AMT,
              PAYBL_TO_SUB_AMT = PAYBL_TO_SUB_AMT,
              PROC_TBL_PRICE_AMT = PROC_TBL_PRICE_AMT,
              PROFL_PRICE_AMT = PROFL_PRICE_AMT,
              PROV_WRT_OFF_AMT = PROV_WRT_OFF_AMT,
              RISK_WTHLD_AMT = RISK_WTHLD_AMT,
              SVC_PRICE_AMT = SVC_PRICE_AMT,
              SUPLMT_DSCNT_AMT = SUPLMT_DSCNT_AMT,
              ALW_PRICE_UNIT_CT = ALW_PRICE_UNIT_CT,
              UNIT_CT = UNIT_CT,
              DEDCT_AMT_ACCUM_ID = DEDCT_AMT_ACCUM_ID,
              PREAUTH_SVC_SEQ_NO = PREAUTH_SVC_SEQ_NO,
              RFRL_SVC_SEQ_NO = RFRL_SVC_SEQ_NO,
              LMT_PFX_ID = LMT_PFX_ID,
              PREAUTH_ID = PREAUTH_ID,
              PROD_CMPNT_DEDCT_PFX_ID = PROD_CMPNT_DEDCT_PFX_ID,
              PROD_CMPNT_SVC_PAYMT_ID = PROD_CMPNT_SVC_PAYMT_ID,
              RFRL_ID_TX = RFRL_ID_TX,
              SVC_ID = SVC_ID,
              SVC_PRICE_RULE_ID = SVC_PRICE_RULE_ID,
              SVC_RULE_TYP_TX = SVC_RULE_TYP_TX,
              SVC_LOC_TYP_CD = SVC_LOC_TYP_CD,
              NON_PAR_SAV_AMT = NON_PAR_SAV_AMT,
              PROC_CD_TYP_CD = PROC_CD_TYP_CD,
              PROC_CD_CAT_CD = PROC_CD_CAT_CD,
              VBB_RULE_ID = "NA",
              VBB_EXCD_ID = "NA",
              CLM_LN_VBB_IN = "N",
              ITS_SUPLMT_DSCNT_AMT = 0.00,
              ITS_SRCHRG_AMT = 0.00,
              NDC = NDC,
              NDC_DRUG_FORM_CD = "NA",
              NDC_UNIT_CT = @Null,
              MED_PDX_IND = "PDX",
              APC_ID = APC_ID,
              APC_STTUS_ID = APC_STTUS_ID,
              SNOMED_CT_CD = SNOMED_CT_CD,
              CVX_VCCN_CD = CVX_VCCN_CD) ~> SnapshotV0S133P2Derived
        SnapshotV0S133P2Derived
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
            CLM_LN_SK,
            CLM_ID,
            CLM_LN_SEQ_NO,
            CRT_RUN_CYC_EXCTN_SK,
            LAST_UPDT_RUN_CYC_EXCTN_SK,
            PROC_CD,
            SVC_PROV_ID,
            CLM_LN_DSALW_EXCD,
            CLM_LN_EOB_EXCD,
            CLM_LN_FINL_DISP_CD,
            CLM_LN_LOB_CD,
            CLM_LN_POS_CD,
            CLM_LN_PREAUTH_CD,
            CLM_LN_PREAUTH_SRC_CD,
            CLM_LN_PRICE_SRC_CD,
            CLM_LN_RFRL_CD,
            CLM_LN_RVNU_CD,
            CLM_LN_ROOM_PRICE_METH_CD,
            CLM_LN_ROOM_TYP_CD,
            CLM_LN_TOS_CD,
            CLM_LN_UNIT_TYP_CD,
            CAP_LN_IN,
            PRI_LOB_IN,
            SVC_END_DT,
            SVC_STRT_DT,
            AGMNT_PRICE_AMT,
            ALW_AMT,
            CHRG_AMT,
            COINS_AMT,
            CNSD_CHRG_AMT,
            COPAY_AMT,
            DEDCT_AMT,
            DSALW_AMT,
            ITS_HOME_DSCNT_AMT,
            NO_RESP_AMT,
            MBR_LIAB_BSS_AMT,
            PATN_RESP_AMT,
            PAYBL_AMT,
            PAYBL_TO_PROV_AMT,
            PAYBL_TO_SUB_AMT,
            PROC_TBL_PRICE_AMT,
            PROFL_PRICE_AMT,
            PROV_WRT_OFF_AMT,
            RISK_WTHLD_AMT,
            SVC_PRICE_AMT,
            SUPLMT_DSCNT_AMT,
            ALW_PRICE_UNIT_CT,
            UNIT_CT,
            DEDCT_AMT_ACCUM_ID,
            PREAUTH_SVC_SEQ_NO,
            RFRL_SVC_SEQ_NO,
            LMT_PFX_ID,
            PREAUTH_ID,
            PROD_CMPNT_DEDCT_PFX_ID,
            PROD_CMPNT_SVC_PAYMT_ID,
            RFRL_ID_TX,
            SVC_ID,
            SVC_PRICE_RULE_ID,
            SVC_RULE_TYP_TX,
            SVC_LOC_TYP_CD,
            NON_PAR_SAV_AMT,
            PROC_CD_TYP_CD,
            PROC_CD_CAT_CD,
            VBB_RULE_ID,
            VBB_EXCD_ID,
            CLM_LN_VBB_IN,
            ITS_SUPLMT_DSCNT_AMT,
            ITS_SRCHRG_AMT,
            NDC,
            NDC_DRUG_FORM_CD,
            NDC_UNIT_CT,
            MED_PDX_IND,
            APC_ID,
            APC_STTUS_ID,
            SNOMED_CT_CD,
            CVX_VCCN_CD
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> SnapshotV0S133P2
    {ctx.sharedContainer(
      scriptModel=0,
      containerName='ClmLnPK',
      sourceName='SnapshotV0S133P2',
      context=ctx)}
    ClmLnPK sink(
      allowSchemaDrift: true,
      validateSchema: false,
      partitionFileNames:[(""" + ctx.getFileName("""$FilePath + '/key/BCBSKCCommClmLnExtr_' + $SrcSysCd + '.DrugClmLn.dat.' + $RunID""") + f""")],
      partitionBy('hash', 1),
      skipDuplicateMapInputs: true,
      skipDuplicateMapOutputs: true,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: (""" + ctx.getFolderPath("""$FilePath + '/key/BCBSKCCommClmLnExtr_' + $SrcSysCd + '.DrugClmLn.dat.' + $RunID""") + f""") ,
      columnDelimiter: ',',
      escapeChar:""" + '\\\\' + f""",
      quoteChar: '\\"',
      columnNamesAsHeader:false) ~> BCBSKCCommClmLnExtr
    BusinessRules
        derive(
              CLCL_ID = CLM_ID,
              CLM_LN_SEQ_NO = CLM_LN_SEQ_NO,
              PROC_CD = PROC_CD,
              CLM_LN_RVNU_CD = CLM_LN_RVNU_CD,
              ALW_AMT = ALW_AMT,
              CHRG_AMT = CHRG_AMT,
              PAYBL_AMT = PAYBL_AMT,
              CLM_TYPE = CLM_TYPE,
              PROC_CD_TYP_CD = PROC_CD_TYP_CD,
              PROC_CD_CAT_CD = PROC_CD_CAT_CD) ~> SnapshotV0S133P3Derived
        SnapshotV0S133P3Derived
        select(mapColumn(
            CLCL_ID,
            CLM_LN_SEQ_NO,
            PROC_CD,
            CLM_LN_RVNU_CD,
            ALW_AMT,
            CHRG_AMT,
            PAYBL_AMT,
            CLM_TYPE,
            PROC_CD_TYP_CD,
            PROC_CD_CAT_CD
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> SnapshotV0S133P3
    SnapshotV0S133P3
        derive(
              SRC_SYS_CD_SK = $SrcSysCdSk,
              CLM_ID = CLCL_ID,
              CLM_LN_SEQ_NO = CLM_LN_SEQ_NO,
              PROC_CD_SK = :ProcCdSk,
              CLM_LN_RVNU_CD_SK = :ClmLnRvnuCdSk,
              ALW_AMT = ALW_AMT,
              CHRG_AMT = CHRG_AMT,
              PAYBL_AMT = PAYBL_AMT,
              ProcCdSk = :ProcCdSk,
              ClmLnRvnuCdSk = :ClmLnRvnuCdSk,
              ProcCdSk := {'GetFkeyProcCd("FACETS", 0, substring(PROC_CD, 1, 5), PROC_CD_TYP_CD, PROC_CD_CAT_CD, "X")'.replace("Snapshot.","")},
              ClmLnRvnuCdSk := {'GetFkeyRvnu("FACETS", 0, CLM_LN_RVNU_CD, "X")'.replace("Snapshot.","")}) ~> TransformerDerived
        TransformerDerived
        select(mapColumn(
            SRC_SYS_CD_SK,
            CLM_ID,
            CLM_LN_SEQ_NO,
            PROC_CD_SK,
            CLM_LN_RVNU_CD_SK,
            ALW_AMT,
            CHRG_AMT,
            PAYBL_AMT
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> Transformer
    Transformer sink(
      allowSchemaDrift: true,
      validateSchema: false,
      partitionFileNames:[(""" + ctx.getFileName("""$FilePath + '/load/B_CLM_LN.' + $SrcSysCd + '.dat.' + $RunID""") + f""")],
      partitionBy('hash', 1),
      skipDuplicateMapInputs: true,
      skipDuplicateMapOutputs: true,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: (""" + ctx.getFolderPath("""$FilePath + '/load/B_CLM_LN.' + $SrcSysCd + '.dat.' + $RunID""") + f""") ,
      columnDelimiter: ',',
      escapeChar:""" + '\\\\' + f""",
      quoteChar: '\\"',
      columnNamesAsHeader:false) ~> BCLMLN""", type='MappingDataFlow')
    artifacts = buffer.artifacts + [(dataflowName, dataflow)]
    return artifacts, dataflowName
  artifacts, dataflowName = dfBCBSKCCommClmLnExtr()
  activityName = "BCBSKCCommClmLnExtr"
  activityParameters = {"FilePath":  {"value": "'@{pipeline().parameters.FilePath}'","type": "Expression"},
  "IDSInstance":  {"value": "'@{pipeline().parameters.IDSInstance}'","type": "Expression"},
  "IDSDB":  {"value": "'@{pipeline().parameters.IDSDB}'","type": "Expression"},
  "IDSOwner":  {"value": "'@{pipeline().parameters.IDSOwner}'","type": "Expression"},
  "IDSAcct":  {"value": "'@{pipeline().parameters.IDSAcct}'","type": "Expression"},
  "IDSPW":  {"value": "'@{pipeline().parameters.IDSPW}'","type": "Expression"},
  "CurrentDate":  {"value": "'@{pipeline().parameters.CurrentDate}'","type": "Expression"},
  "SrcSysCd":  {"value": "'@{pipeline().parameters.SrcSysCd}'","type": "Expression"},
  "SrcSysCdSk":  {"value": "'@{pipeline().parameters.SrcSysCdSk}'","type": "Expression"},
  "RunID":  {"value": "'@{pipeline().parameters.RunID}'","type": "Expression"},
  "RunCycle":  {"value": "'@{pipeline().parameters.RunCycle}'","type": "Expression"}}
  activity = ExecuteDataFlowActivity(
    name = activityName,
    data_flow = DataFlowReference(reference_name=dataflowName,
      parameters=activityParameters, type='DataFlowReference'))
  return artifacts, [activity,
    CustomActivity(
      name = ctx.removeAlias("DSU.HASH.CLEAR\\1;hf_bcbskccomm_drg_clm_ln_prov"),
      command = ctx.getRoutineCommand(ctx.removeAlias("DSU.HASH.CLEAR\\1;hf_bcbskccomm_drg_clm_ln_prov"), isSubroutine=True),
      linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service),
      depends_on = [ActivityDependency( activity = activityName,
        dependency_conditions = [DependencyCondition.COMPLETED])]),
    FailActivity(
      name = "Fail",
      message = "Dataflow execution failed",
      error_code = "1",
      depends_on = [ActivityDependency(activity = activityName,
        dependency_conditions = [DependencyCondition.FAILED]),
        ActivityDependency(activity = ctx.removeAlias("DSU.HASH.CLEAR\\1;hf_bcbskccomm_drg_clm_ln_prov"),
        dependency_conditions = [DependencyCondition.COMPLETED])])]

def BCBSKCCommClmLnExtr(ctx):
  name = "BCBSKCCommClmLnExtr"
  artifacts, activities = BCBSKCCommClmLnExtrActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="claim/ClaimLine/ClmLn/BCBSKCCommon"),
    activities = activities,
    description = """
      BCBSKCComm Claim Line
      COPYRIGHT 2018 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


      CALLED BY:  BCBSKCCommDrugExtrSeq

      DESCRIPTION:  
          Reads the PDX_CLM_STD_INPT_Land file and runs through primary key using shared container ClmLnPk
          

      MODIFICATIONS:

      Developer                       Date                 Project/Altiris #      Change Description                                                                                              Development Project      Code Reviewer          Date Reviewed       
      ------------------                     --------------------     ------------------------      -------------------------------------------------------------------------------------------------------------------------    --------------------------------       -------------------------------   ----------------------------       
      Kaushik Kapoor              2018-02-26      5828                       Original Programming: Reads data from PDX_CLM_STD_INPT_Land.dat file     IntegrateDev2                 Kalyan Neelam           2018-02-26
                                                                                                    and creates an extract file for CLM_LN which will be read 
                                                                                                     by IdsClmLnFKey job to create CLM_LN load file
      Kaushik Kapoor              2018-03-16      5828                       Changed SVC_PRICE_RULE_ID from ' ' to 'NA'                                                  IntegrateDev2	   Jaideep Mankala       03/20/2018
      Madhavan B                   2018-02-06      5792 	              Changed the datatype of the column                                                                   IntegrateDev1                  Kalyan Neelam          2018-02-08
                                                                   		              NDC_UNIT_CT from SMALLINT to DECIMAL (21,10)
      Rekha Radhakrishna     2020-02-27       6131                      Changed ALW_AMT calculation for OPTUMRX                                                  IntegrateDev2                  Hugh Sisson              2020-03-06

      Goutham Kalidindi        10/22/2020       US-283560          Added MEDIMPACT  derivation for CLM_LN_POS_CD                                         IntegrateDev2                Reddy Sanam             2020-11-09
                                                                                                   and PAYBL_AMT


      Goutham Kalidindi        10/22/2020       US-283560          Added MEDIMPACT derivation for   ALLWD_AMT                                                IntegrateDev2                  Kalyan Neelam        2020-11-10


      Goutham Kalidindi        11/11/2020       US-283560                           Changed MEDIMPACT derivation for   ALLWD_AMT                 IntegrateDev2                
                                                                                                                 Added MEDIMPACT derivation for CNSD_CHRG_AMT
                                                                                                                 CHRG_AMT  

                                                                                                  


      Rekha Radhakrishna     2020-07-27       6131                     Modified common file layout to include157 columns                                              IntegrateDev2                 Kalyan Neelam         2020-11-12
                                                                                                   and updated CLM_LN_POS_CD mapping for MedD and ACA

      Rekha Radhakrishna     2020-08-13       6131                    Modified source file common layout to include LOB_IN                                         IntegrateDev2       
      Rekha Radhakrishna     2020-08-17       6131                    Mapped APC_ID and APC_STTUS_ID to Null for 'OPTUMRX'                            IntegrateDev2 
                          
      Rekha Radhakrishna     2020-09-25       6131                    UpdatedCLM_LN_POS_CD for  'OPTUMRX'                                                         IntegrateDev2             Sravya Gorla              2020-09-12       

      Amritha A J                      2023-07-31   US 589700           Added two new fields SNOMED_CT_CD,CVX_VCCN_CD with a default                IntegrateDevB	Harsha Ravuri	2023-08-31
                                                                                                  value in BusinessRules stage and mapped it till target
      Parameters:
      -----------
      FilePath:
        IDS File Path
      IDSInstance:
        IDS Instance
      IDSDB:
        IDS Database
      IDSOwner:
        IDS Table Owner
      IDSAcct:
        IDS Account
      IDSPW:
        IDS Password
      CurrentDate:
        Current Date
      SrcSysCd:
        Source System Code
      SrcSysCdSk:
        Source System Code Surrogate Key
      RunID:
        Run ID
      RunCycle:
        Current Run Cycle""",
    parameters = {
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "IDSInstance": ParameterSpecification(type="String", default_value=ctx.IDSInstance),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "IDSAcct": ParameterSpecification(type="String"),
      "IDSPW": ParameterSpecification(type="String"),
      "CurrentDate": ParameterSpecification(type="String"),
      "SrcSysCd": ParameterSpecification(type="String"),
      "SrcSysCdSk": ParameterSpecification(type="String"),
      "RunID": ParameterSpecification(type="String"),
      "RunCycle": ParameterSpecification(type="Int")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (BCBSKCCommClmLnExtr(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
