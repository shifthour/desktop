model:
  config:
    origin: datastage
  jobs:
    - name: IdsMbrPdxDeniedClmFkey
      origin: datastage
      type: dataflow
      folder: "DataWarehouse/Claim/DeniedPDXClaims"
      runAfterSubrOnJobSuccess: enabled
      description: "Perform Foreign Key Lookups to populate MBR_PDX_DENIED_TRANS table."
      fullDescription: "**********************************************************************************************************************************************************************\nCOPYRIGHT 2014 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\n\nDESCRIPTION:  OPTUM Claims denied data is extracted from source file and loaded into the  MBR_PDX_DENIED_TRANS Table\n\n\nCalled By: OptumIdsMbrPdxDeniedTransLoadSeq\n\n\nInitial Development: Peter Gichiri \n                                                                                                                                                                                                                DATASTAGE               CODE                           DATE\nDEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW\n-----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------\nPeter Gichiri              2019-10-02             6131 - PBM REPLACEMENT  to OPTUMRX     Initial Devlopment Integrate                                                            Kalyan Neelam             2019-11-21"
      parameters:
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
        - name: APT_CONFIG_FILE
          type: string
          description: "Configuration file"
          longDescription: "The Parallel job configuration file."
          defaultType: project
        - name: SrcSysCd
          type: string
          description: SrcSysCd
          defaultType: expr
          defaultValue: OPTUMRX
        - name: IDSRunCycle
          type: string
          description: IDSRunCycle
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSAcct
          type: string
          description: "IDS Account"
        - name: IDSOwner
          type: string
          description: "IDS Table Owner"
          defaultType: project
        - name: IDSPW
          type: encrypted
          description: "IDS Password"
        - name: RunID
          type: string
          description: RunID
        - name: PrefixFkeyFailedFileName
          type: string
          description: PrefixFkeyFailedFileName
          defaultType: expr
          defaultValue: PDX_FileName
      steps:
        - name: Lkup_Fkey
          type: lookup
          option:
            joinType: left
          sources: 
            - name: seq_mbr_pdx_dined_Pkey
              alias: Lnk_IdsProvLocFkey_EE_InAbc
              joinFilter: ""
              joinExpression: []
            - name: fltr_FilterData.V124S0P3
              alias: ref0_Media_Type_SK_Lkp
              joinFilter: ""
              joinExpression: 
              - fromExpr: ref0_Media_Type_SK_Lkp.SRC_CD
                toExpr: Lnk_IdsProvLocFkey_EE_InAbc.MEDIA_TYP_CD_SK
                caseInSensitive: false
            - name: fltr_FilterData.V124S0P6
              alias: ref1_Resp_type_cd_lkp
              joinFilter: ""
              joinExpression: 
              - fromExpr: ref1_Resp_type_cd_lkp.SRC_SYS_CD
                toExpr: Lnk_IdsProvLocFkey_EE_InAbc.SRC_SYS_CD
                caseInSensitive: false
            - name: fltr_FilterData.V124S0P7
              alias: ref2_trans_typ_cd_Lkp
              joinFilter: ""
              joinExpression: 
              - fromExpr: ref2_trans_typ_cd_Lkp.SRC_SYS_CD
                toExpr: Lnk_IdsProvLocFkey_EE_InAbc.SRC_SYS_CD
                caseInSensitive: false
            - name: db2_Grp_Lkp
              alias: ref_grp_sk_LKp
              joinFilter: ""
              joinExpression: 
              - fromExpr: ref_grp_sk_LKp.GRP_ID
                toExpr: Lnk_IdsProvLocFkey_EE_InAbc.SRC_SYS_GRP_ID
                caseInSensitive: false
              - fromExpr: ref_grp_sk_LKp.MBR_ID
                toExpr: Lnk_IdsProvLocFkey_EE_InAbc.MBR_ID
                caseInSensitive: false
            - name: db2_MBR
              alias: lnk_mbr_extr
              joinFilter: ""
              joinExpression: 
              - fromExpr: lnk_mbr_extr.SRC_SYS_GRP_ID
                toExpr: Lnk_IdsProvLocFkey_EE_InAbc.SRC_SYS_GRP_ID
                caseInSensitive: false
              - fromExpr: lnk_mbr_extr.MBR_ID
                toExpr: Lnk_IdsProvLocFkey_EE_InAbc.MBR_ID
                caseInSensitive: false
            - name: db2_NDC
              alias: lnk_ndc_extr
              joinFilter: ""
              joinExpression: 
              - fromExpr: lnk_ndc_extr.NDC
                toExpr: Lnk_IdsProvLocFkey_EE_InAbc.NDC
                caseInSensitive: false
            - name: fltr_FilterData.V124S0P9
              alias: ref3_rsp_rsn_CD_lk
              joinFilter: ""
              joinExpression: 
              - fromExpr: ref3_rsp_rsn_CD_lk.SRC_CD
                toExpr: Lnk_IdsProvLocFkey_EE_InAbc.PDX_RSPN_RSN_CD_SK
                caseInSensitive: false
            # mutiple reference detected
          joinCondition: 
            - expression: ref0_Media_Type_SK_Lkp.SRC_CD = Lnk_IdsProvLocFkey_EE_InAbc.MEDIA_TYP_CD_SK
              caseInSensitive: false
            - expression: ref1_Resp_type_cd_lkp.SRC_SYS_CD = Lnk_IdsProvLocFkey_EE_InAbc.SRC_SYS_CD
              caseInSensitive: false
            - expression: ref2_trans_typ_cd_Lkp.SRC_SYS_CD = Lnk_IdsProvLocFkey_EE_InAbc.SRC_SYS_CD
              caseInSensitive: false
            - expression: ref_grp_sk_LKp.GRP_ID = Lnk_IdsProvLocFkey_EE_InAbc.SRC_SYS_GRP_ID
              caseInSensitive: false
            - expression: ref_grp_sk_LKp.MBR_ID = Lnk_IdsProvLocFkey_EE_InAbc.MBR_ID
              caseInSensitive: false
            - expression: lnk_mbr_extr.SRC_SYS_GRP_ID = Lnk_IdsProvLocFkey_EE_InAbc.SRC_SYS_GRP_ID
              caseInSensitive: false
            - expression: lnk_mbr_extr.MBR_ID = Lnk_IdsProvLocFkey_EE_InAbc.MBR_ID
              caseInSensitive: false
            - expression: lnk_ndc_extr.NDC = Lnk_IdsProvLocFkey_EE_InAbc.NDC
              caseInSensitive: false
            - expression: ref3_rsp_rsn_CD_lk.SRC_CD = Lnk_IdsProvLocFkey_EE_InAbc.PDX_RSPN_RSN_CD_SK
              caseInSensitive: false
          schema: 
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.MBR_PDX_DENIED_TRANS_SK"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.MBR_UNIQ_KEY"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.PDX_NTNL_PROV_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.TRANS_TYP_CD"
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.TRANS_DENIED_DT"
            - name: RX_NO
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.RX_NO"
            - name: PRCS_DT
              type: date
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.PRCS_DT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.SRC_SYS_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "ref_grp_sk_LKp.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_mbr_extr.MBR_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_ndc_extr.NDC_SK"
            - name: PRSCRB_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.PRSCRB_PROV_SK"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.PROV_SPEC_CD_SK"
            - name: SRV_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.SRV_PROV_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_mbr_extr.SUB_SK"
            - name: MEDIA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ref0_Media_Type_SK_Lkp.CD_MPPNG_SK"
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.MBR_GNDR_CD_SK"
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.MBR_RELSHP_CD_SK"
            - name: PDX_RSPN_RSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ref3_rsp_rsn_CD_lk.CD_MPPNG_SK"
            - name: PDX_RSPN_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ref1_Resp_type_cd_lkp.CD_MPPNG_SK"
            - name: TRANS_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "ref2_trans_typ_cd_Lkp.CD_MPPNG_SK"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.MAIL_ORDER_IN"
            - name: MBR_BRTH_DT
              type: date
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.MBR_BRTH_DT"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.SRC_SYS_CLM_RCVD_TM"
            - name: SUB_BRTH_DT
              type: date
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.SUB_BRTH_DT"
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.BILL_RX_DISPENSE_FEE_AMT"
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.BILL_RX_GROS_APRV_AMT"
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.BILL_RX_NET_CHK_AMT"
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.BILL_RX_PATN_PAY_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.INGR_CST_ALW_AMT"
            - name: TRANS_MO_NO
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.TRANS_MO_NO"
            - name: TRANS_YR_NO
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.TRANS_YR_NO"
            - name: MBR_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.MBR_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.MBR_SFX_NO"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.MBR_FIRST_NM"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.MBR_LAST_NM"
            - name: MBR_SSN
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.MBR_SSN"
            - name: PDX_NM
              type: varchar
              width: 35
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.PDX_NM"
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.PDX_PHN_NO"
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.PHYS_DEA_NO"
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.PHYS_NTNL_PROV_ID"
            - name: PHYS_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.PHYS_FIRST_NM"
            - name: PHYS_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.PHYS_LAST_NM"
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 40
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.PHYS_ST_ADDR_LN"
            - name: PHYS_CITY_NM
              type: varchar
              width: 75
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.PHYS_CITY_NM"
            - name: PHYS_ST_CD
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.PHYS_ST_CD"
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.PHYS_POSTAL_CD"
            - name: RX_LABEL_TX
              type: varchar
              width: 75
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.RX_LABEL_TX"
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 75
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.SVC_PROV_NABP_NM"
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.SRC_SYS_CAR_ID"
            - name: SRC_SYS_CLNT_ORG_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.SRC_SYS_CLNT_ORG_ID"
            - name: SRC_SYS_CLNT_ORG_NM
              type: varchar
              width: 35
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.SRC_SYS_CLNT_ORG_NM"
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.SRC_SYS_CNTR_ID"
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.SRC_SYS_GRP_ID"
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.SUB_ID"
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.SUB_FIRST_NM"
            - name: SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "Lnk_IdsProvLocFkey_EE_InAbc.SUB_LAST_NM"
            - name: MEDIA_TYP_CD
              type: varchar
              width: 20
              map: "Lnk_IdsProvLocFkey_EE_InAbc.MEDIA_TYP_CD"
            - name: RSPN_CD
              type: varchar
              width: 20
              map: "Lnk_IdsProvLocFkey_EE_InAbc.RSPN_CD"
            - name: MSG_TYP_CD
              type: varchar
              width: 10
              map: "Lnk_IdsProvLocFkey_EE_InAbc.MSG_TYP_CD"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "ref_grp_sk_LKp.GRP_ID"
            - name: NDC
              type: varchar
              width: 20
              map: "Lnk_IdsProvLocFkey_EE_InAbc.NDC"
          keys:
            - MBR_PDX_DENIED_TRANS_SK
        - name: seq_mbr_pdx_dined_Pkey
          type: fileReader
          urls: 
            - "#$FilePath#/key/MBR_PDX_DENIED_TRANS.#SrcSysCd#.pkey.#RunID#.dat"
          rejectMode: continue
          format: 
            header: false
            quotes: "^"
            fieldDelimiter: ","
            rowDelimiter: "\n"
            fixedLength: false
          schema: 
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              nullable: N
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              nullable: N
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
            - name: RX_NO
              type: varchar
              width: 20
              nullable: N
            - name: PRCS_DT
              type: date
              nullable: N
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
            - name: PRSCRB_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: SRV_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
            - name: MEDIA_TYP_CD_SK
              type: varchar
              width: 1
              nullable: N
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              nullable: N
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PDX_RSPN_RSN_CD_SK
              type: varchar
              width: 3
              nullable: N
            - name: PDX_RSPN_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: TRANS_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
            - name: MBR_BRTH_DT
              type: date
              nullable: N
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
            - name: SUB_BRTH_DT
              type: date
              nullable: N
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: TRANS_MO_NO
              type: int
              width: 4
              nullable: N
            - name: TRANS_YR_NO
              type: int
              width: 4
              nullable: N
            - name: MBR_ID
              type: varchar
              width: 20
              nullable: N
            - name: MBR_SFX_NO
              type: char
              width: 2
              nullable: N
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              nullable: N
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              nullable: N
            - name: MBR_SSN
              type: varchar
              width: 20
              nullable: N
            - name: PDX_NM
              type: varchar
              width: 35
              nullable: N
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              nullable: N
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              nullable: N
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: PHYS_FIRST_NM
              type: varchar
              width: 35
              nullable: N
            - name: PHYS_LAST_NM
              type: varchar
              width: 75
              nullable: N
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 40
              nullable: N
            - name: PHYS_CITY_NM
              type: varchar
              width: 75
              nullable: N
            - name: PHYS_ST_CD
              type: varchar
              width: 20
              nullable: N
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              nullable: N
            - name: RX_LABEL_TX
              type: varchar
              width: 75
              nullable: N
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 75
              nullable: N
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 20
              nullable: N
            - name: SRC_SYS_CLNT_ORG_ID
              type: varchar
              width: 20
              nullable: N
            - name: SRC_SYS_CLNT_ORG_NM
              type: varchar
              width: 35
              nullable: N
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              nullable: N
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 20
              nullable: N
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
            - name: SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
            - name: CLNT_ELIG_MBRSH_ID
              type: varchar
              width: 20
            - name: PRSN_NO
              type: varchar
              width: 20
            - name: RELSHP_CD
              type: varchar
              width: 20
            - name: GNDR_CD
              type: varchar
              width: 20
            - name: CLM_RCVD_TM
              type: varchar
              width: 20
            - name: MSG_TYP_CD
              type: varchar
              width: 10
            - name: MSG_TYP
              type: varchar
              width: 20
            - name: MSG_TX
              type: varchar
              width: 200
            - name: CLNT_MBRSH_ID
              type: varchar
              width: 50
            - name: MEDIA_TYP_CD
              type: varchar
              width: 20
            - name: RSPN_CD
              type: varchar
              width: 20
            - name: DESC
              type: varchar
              width: 500
            - name: CHAPTER_ID
              type: varchar
              width: 20
            - name: CHAPTER_DESC
              type: varchar
              width: 500
            - name: PDX_NPI
              type: varchar
              width: 20
            - name: NDC
              type: varchar
              width: 20
          keys:
            - MBR_PDX_DENIED_TRANS_SK
        - name: seq_failure_file
          type: fileWriter
          urls: 
            - "#$FilePath#/fkey_failures/#PrefixFkeyFailedFileName#.#DSJobName#.dat"
          source: fnl_all_fail
          rejectMode: continue
          option: 
            updateMode: overwrite
            writeMode: SingleFile
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
            rowDelimiter: "\n"
            fixedLength: false
        - name: seq_MBR_PDX_DENIED_TRANS_FKey
          type: fileWriter
          urls: 
            - "#$FilePath#/load/MBR_PDX_DENIED_TRANS.#SrcSysCd#.#RunID#.dat"
          source: "xfm_CheckLkpResults.V0S11P11"
          rejectMode: continue
          option: 
            updateMode: overwrite
            writeMode: SingleFile
          format: 
            header: false
            quotes: ""
            fieldDelimiter: "|"
            rowDelimiter: "\n"
            fixedLength: false
        - name: fltr_FilterData.V124S0P3
          type: filter
          source: db2_ds_CD_MPPNG_LkpData
          alias: lnk_Codes
          filter: "(SRC_SYS_CD =  '#SrcSysCd#'  AND SRC_CLCTN_CD = '#SrcSysCd#'  AND SRC_DOMAIN_NM = 'MEDIA TYPE' AND TRGT_CLCTN_CD = 'IDS'  AND TRGT_DOMAIN_NM = 'MEDIA TYPE')"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_Codes.CD_MPPNG_SK"
            - name: SRC_CD
              type: varchar
              width: 20
              map: "lnk_Codes.SRC_CD"
          keys:
            - SRC_CD
        - name: fltr_FilterData.V124S0P6
          type: filter
          source: db2_ds_CD_MPPNG_LkpData
          alias: lnk_Codes
          filter: "(SRC_SYS_CD = '#SrcSysCd#' AND SRC_CLCTN_CD = '#SrcSysCd#' AND SRC_DOMAIN_NM = 'PHARMACY MESSAGE REPONSE TYPE' AND TRGT_CLCTN_CD = 'IDS'  AND TRGT_DOMAIN_NM = 'PHARMACY MESSAGE REPONSE TYPE' AND SRC_CD='R' AND SRC_CD_NM='REJECT')"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_Codes.CD_MPPNG_SK"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_Codes.SRC_SYS_CD"
          keys:
            - SRC_SYS_CD
        - name: fltr_FilterData.V124S0P7
          type: filter
          source: db2_ds_CD_MPPNG_LkpData
          alias: lnk_Codes
          filter: "(SRC_SYS_CD = '#SrcSysCd#'  AND  SRC_CLCTN_CD =  '#SrcSysCd#'  AND  SRC_DOMAIN_NM = 'PHARMACY TRANSACTION TYPE'  AND TRGT_CLCTN_CD = 'IDS' AND SRC_CD_NM = 'PHARMACY CLAIM')"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_Codes.CD_MPPNG_SK"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_Codes.SRC_SYS_CD"
          keys:
            - SRC_SYS_CD
        - name: fltr_FilterData.V124S0P9
          type: filter
          source: db2_ds_CD_MPPNG_LkpData
          alias: lnk_Codes
          filter: "(SRC_SYS_CD = '#SrcSysCd#'   AND SRC_CLCTN_CD = '#SrcSysCd#'   AND  SRC_DOMAIN_NM = 'PHARMACY RESPONSE REASON'  AND  TRGT_CLCTN_CD = 'IDS' AND  TRGT_DOMAIN_NM = 'PHARMACY RESPONSE REASON')"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_Codes.CD_MPPNG_SK"
            - name: SRC_CD
              type: varchar
              width: 20
              map: "lnk_Codes.SRC_CD"
          keys:
            - SRC_CD
        - name: db2_ds_CD_MPPNG_LkpData
          type: dbReader
          originalDb: db2
          query: "SELECT DISTINCT\nCD_MPPNG_SK,\n--CD_MPPNG_ID,\n--CRT_RUN_CYC_EXCTN_SK,\n--LAST_UPDT_RUN_CYC_EXCTN_SK,\n--SRC_CD_SK,\n--TRGT_CD_SK,\n--CD_MPPNG_STTUS_CD,\n--CD_MPPNG_TYP_CD,\nSRC_CD,\nSRC_CD_NM,\nSRC_CLCTN_CD,\nSRC_DRVD_LKUP_VAL,\nSRC_DOMAIN_NM,\nSRC_SYS_CD,\nTRGT_CD,\nTRGT_CD_NM,\nTRGT_CLCTN_CD,\nTRGT_DOMAIN_NM\n--TRGT_SRC_SYS_CD\n\nFROM #$IDSOwner#.CD_MPPNG;\n"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_CD
              type: varchar
              width: 20
            - name: SRC_CD_NM
              type: varchar
              width: 255
            - name: SRC_CLCTN_CD
              type: varchar
              width: 20
              nullable: N
            - name: SRC_DRVD_LKUP_VAL
              type: varchar
              width: 20
            - name: SRC_DOMAIN_NM
              type: varchar
              width: 50
              nullable: N
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
            - name: TRGT_CD_NM
              type: varchar
              width: 255
            - name: TRGT_CLCTN_CD
              type: varchar
              width: 20
            - name: TRGT_DOMAIN_NM
              type: varchar
              width: 50
          keys:
            - CD_MPPNG_SK
        - name: db2_Grp_Lkp
          type: dbReader
          originalDb: db2
          query: "SELECT DISTINCT\nG.GRP_SK,\nG.GRP_ID, \nM.MBR_UNIQ_KEY AS MBR_UNIQ_KEY, \nS.SUB_ID||M.MBR_SFX_NO AS MBR_ID\n\nFROM #$IDSOwner#.MBR M, #$IDSOwner#.SUB S,#$IDSOwner#.GRP G\nWHERE\nS.SUB_SK = M.SUB_SK AND\nS.GRP_SK=G.GRP_SK\n\n"
          schema: 
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
            - name: MBR_ID
              type: varchar
              width: 22
              nullable: N
          keys:
            - GRP_ID
            - MBR_ID
        - name: db2_MBR
          type: dbReader
          originalDb: db2
          query: "SELECT\nM.MBR_UNIQ_KEY as MBR_UNIQ_KEY,\nS.SUB_ID||M.MBR_SFX_NO AS MBR_ID,\nM.MBR_SK AS MBR_SK,\n---S.GRP_SK AS GRP_SK,\nS.SUB_SK AS SUB_SK,\nG.GRP_ID as SRC_SYS_GRP_ID,\nM.MBR_GNDR_CD_SK MBR_GNDR_CD_SK,\nM.MBR_RELSHP_CD_SK MBR_RELSHP_CD_SK,\nM.MBR_SFX_NO MBR_SFX_NO \nFROM #$IDSOwner#.MBR M, #$IDSOwner#.SUB S,#$IDSOwner#.GRP G\n\nWHERE\nS.SUB_SK = M.SUB_SK\nAND \nS.GRP_SK = G.GRP_SK\n with ur;"
          schema: 
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 22
              nullable: N
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
            - name: MBR_ID
              type: varchar
              width: 22
              nullable: N
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              nullable: N
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: MBR_SFX_NO
              type: char
              width: 2
          keys:
            - SRC_SYS_GRP_ID
            - MBR_ID
        - name: db2_NDC
          type: dbReader
          originalDb: db2
          query: "SELECT \nNDC_SK,\nNDC\nFROM #$IDSOwner#.NDC;"
          schema: 
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
            - name: NDC
              type: varchar
              width: 20
              nullable: N
          keys:
            - NDC
        - name: xfm_CheckLkpResults.V0S11P11
          type: map
          source: Lkup_Fkey
          alias: lnk_IdsPdxDeniedFkey_EE_Lkup_Out
          stageVars: 
            - name: SvFKeyLkpCheckMediaTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckRspCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckMsgTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckTransTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckGrpCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckMbrCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckNDC
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
          schema: 
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_PDX_DENIED_TRANS_SK"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_UNIQ_KEY"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_NTNL_PROV_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD"
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_DENIED_DT"
            - name: RX_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RX_NO"
            - name: PRCS_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRCS_DT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK"
            - name: PRSCRB_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRSCRB_PROV_SK"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PROV_SPEC_CD_SK"
            - name: SRV_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRV_PROV_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_SK"
            - name: MEDIA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK"
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_GNDR_CD_SK"
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_RELSHP_CD_SK"
            - name: PDX_RSPN_RSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK"
            - name: PDX_RSPN_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK"
            - name: TRANS_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MAIL_ORDER_IN"
            - name: MBR_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_BRTH_DT"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLM_RCVD_TM"
            - name: SUB_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_BRTH_DT"
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_DISPENSE_FEE_AMT"
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_GROS_APRV_AMT"
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_NET_CHK_AMT"
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_PATN_PAY_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.INGR_CST_ALW_AMT"
            - name: TRANS_MO_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_MO_NO"
            - name: TRANS_YR_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_YR_NO"
            - name: MBR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SFX_NO"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_FIRST_NM"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_LAST_NM"
            - name: MBR_SSN
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SSN"
            - name: PDX_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_NM"
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_PHN_NO"
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_DEA_NO"
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_NTNL_PROV_ID"
            - name: PHYS_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_FIRST_NM"
            - name: PHYS_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_LAST_NM"
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 40
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_ST_ADDR_LN"
            - name: PHYS_CITY_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_CITY_NM"
            - name: PHYS_ST_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_ST_CD"
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_POSTAL_CD"
            - name: RX_LABEL_TX
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RX_LABEL_TX"
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SVC_PROV_NABP_NM"
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CAR_ID"
            - name: SRC_SYS_CLNT_ORG_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLNT_ORG_ID"
            - name: SRC_SYS_CLNT_ORG_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLNT_ORG_NM"
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CNTR_ID"
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_GRP_ID"
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_ID"
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_FIRST_NM"
            - name: SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_LAST_NM"
            - name: SvFKeyLkpCheckMediaTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMediaTypeCd
              dropStageVar: true
              initValue: ""
            - name: SvFKeyLkpCheckRspCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckRspCd
              dropStageVar: true
              initValue: ""
            - name: SvFKeyLkpCheckMsgTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMsgTypeCd
              dropStageVar: true
              initValue: ""
            - name: SvFKeyLkpCheckTransTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckTransTypeCd
              dropStageVar: true
              initValue: ""
            - name: SvFKeyLkpCheckGrpCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckGrpCd
              dropStageVar: true
              initValue: ""
            - name: SvFKeyLkpCheckMbrCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMbrCd
              dropStageVar: true
              initValue: ""
            - name: SvFKeyLkpCheckNDC
              type: varchar
              width: 255
              map: SvFKeyLkpCheckNDC
              dropStageVar: true
              initValue: ""
        - name: xfm_CheckLkpResults.V0S11P13.map
          type: map
          source: Lkup_Fkey
          alias: lnk_IdsPdxDeniedFkey_EE_Lkup_Out
          stageVars: 
            - name: SvFKeyLkpCheckMediaTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckRspCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckMsgTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckTransTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckGrpCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckMbrCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckNDC
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
          schema: 
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_PDX_DENIED_TRANS_SK"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_UNIQ_KEY"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_NTNL_PROV_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD"
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_DENIED_DT"
            - name: RX_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RX_NO"
            - name: PRCS_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRCS_DT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK"
            - name: PRSCRB_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRSCRB_PROV_SK"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PROV_SPEC_CD_SK"
            - name: SRV_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRV_PROV_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_SK"
            - name: MEDIA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK"
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_GNDR_CD_SK"
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_RELSHP_CD_SK"
            - name: PDX_RSPN_RSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK"
            - name: PDX_RSPN_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK"
            - name: TRANS_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MAIL_ORDER_IN"
            - name: MBR_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_BRTH_DT"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLM_RCVD_TM"
            - name: SUB_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_BRTH_DT"
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_DISPENSE_FEE_AMT"
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_GROS_APRV_AMT"
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_NET_CHK_AMT"
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_PATN_PAY_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.INGR_CST_ALW_AMT"
            - name: TRANS_MO_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_MO_NO"
            - name: TRANS_YR_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_YR_NO"
            - name: MBR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SFX_NO"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_FIRST_NM"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_LAST_NM"
            - name: MBR_SSN
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SSN"
            - name: PDX_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_NM"
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_PHN_NO"
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_DEA_NO"
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_NTNL_PROV_ID"
            - name: PHYS_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_FIRST_NM"
            - name: PHYS_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_LAST_NM"
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 40
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_ST_ADDR_LN"
            - name: PHYS_CITY_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_CITY_NM"
            - name: PHYS_ST_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_ST_CD"
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_POSTAL_CD"
            - name: RX_LABEL_TX
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RX_LABEL_TX"
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SVC_PROV_NABP_NM"
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CAR_ID"
            - name: SRC_SYS_CLNT_ORG_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLNT_ORG_ID"
            - name: SRC_SYS_CLNT_ORG_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLNT_ORG_NM"
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CNTR_ID"
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_GRP_ID"
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_ID"
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_FIRST_NM"
            - name: SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_LAST_NM"
            - name: MEDIA_TYP_CD
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD"
            - name: RSPN_CD
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RSPN_CD"
            - name: MSG_TYP_CD
              type: varchar
              width: 10
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MSG_TYP_CD"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_ID"
            - name: NDC
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC"
            - name: PRI_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK"
            - name: PRI_NAT_KEY_STRING
              type: varchar
              width: 1000
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD : lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK"
            - name: JOB_NM
              type: varchar
              width: 100
              nullable: N
              map: DSJobName
            - name: ERROR_TYP
              type: varchar
              width: 50
              nullable: N
              map: "'CDLOOKUP'"
            - name: PHYSCL_FILE_NM
              type: varchar
              width: 100
              nullable: N
              map: "'CD_MPPNG'"
            - name: FRGN_NAT_KEY_STRING
              type: varchar
              width: 1000
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD : ';' : 'OPTUMRX' : ';' : 'IDS' : ';' : 'MEDIA TYPE' : ';' : 'MEDIA TYPE' : ';' : lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD"
            - name: FIRST_RECYC_TS
              type: timestamp
              nullable: N
              map: "StringToTimestamp(DateToString(lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRCS_DT, \"%yyyy-%mm-%dd\"), \"%yyyy-%mm-%dd\")"
            - name: JOB_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: SvFKeyLkpCheckMediaTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMediaTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckRspCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckRspCd
              initValue: ""
            - name: SvFKeyLkpCheckMsgTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMsgTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckTransTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckTransTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckGrpCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckGrpCd
              initValue: ""
            - name: SvFKeyLkpCheckMbrCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMbrCd
              initValue: ""
            - name: SvFKeyLkpCheckNDC
              type: varchar
              width: 255
              map: SvFKeyLkpCheckNDC
              initValue: ""
        - name: xfm_CheckLkpResults.V0S11P13
          type: filter
          source: xfm_CheckLkpResults.V0S11P13.map
          filter: "SvFKeyLkpCheckMediaTypeCd = 'Y'"
          schema: 
            - name: PRI_SK
              type: int
              width: 4
              nullable: N
            - name: PRI_NAT_KEY_STRING
              type: varchar
              width: 1000
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: JOB_NM
              type: varchar
              width: 100
              nullable: N
            - name: ERROR_TYP
              type: varchar
              width: 50
              nullable: N
            - name: PHYSCL_FILE_NM
              type: varchar
              width: 100
              nullable: N
            - name: FRGN_NAT_KEY_STRING
              type: varchar
              width: 1000
            - name: FIRST_RECYC_TS
              type: timestamp
              nullable: N
            - name: JOB_EXCTN_SK
              type: int
              width: 4
              nullable: N
        - name: xfm_CheckLkpResults.V0S11P14.map
          type: map
          source: Lkup_Fkey
          alias: lnk_IdsPdxDeniedFkey_EE_Lkup_Out
          stageVars: 
            - name: SvFKeyLkpCheckMediaTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckRspCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckMsgTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckTransTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckGrpCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckMbrCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckNDC
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
          schema: 
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_PDX_DENIED_TRANS_SK"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_UNIQ_KEY"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_NTNL_PROV_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD"
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_DENIED_DT"
            - name: RX_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RX_NO"
            - name: PRCS_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRCS_DT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK"
            - name: PRSCRB_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRSCRB_PROV_SK"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PROV_SPEC_CD_SK"
            - name: SRV_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRV_PROV_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_SK"
            - name: MEDIA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK"
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_GNDR_CD_SK"
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_RELSHP_CD_SK"
            - name: PDX_RSPN_RSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK"
            - name: PDX_RSPN_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK"
            - name: TRANS_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MAIL_ORDER_IN"
            - name: MBR_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_BRTH_DT"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLM_RCVD_TM"
            - name: SUB_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_BRTH_DT"
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_DISPENSE_FEE_AMT"
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_GROS_APRV_AMT"
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_NET_CHK_AMT"
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_PATN_PAY_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.INGR_CST_ALW_AMT"
            - name: TRANS_MO_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_MO_NO"
            - name: TRANS_YR_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_YR_NO"
            - name: MBR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SFX_NO"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_FIRST_NM"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_LAST_NM"
            - name: MBR_SSN
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SSN"
            - name: PDX_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_NM"
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_PHN_NO"
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_DEA_NO"
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_NTNL_PROV_ID"
            - name: PHYS_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_FIRST_NM"
            - name: PHYS_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_LAST_NM"
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 40
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_ST_ADDR_LN"
            - name: PHYS_CITY_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_CITY_NM"
            - name: PHYS_ST_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_ST_CD"
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_POSTAL_CD"
            - name: RX_LABEL_TX
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RX_LABEL_TX"
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SVC_PROV_NABP_NM"
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CAR_ID"
            - name: SRC_SYS_CLNT_ORG_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLNT_ORG_ID"
            - name: SRC_SYS_CLNT_ORG_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLNT_ORG_NM"
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CNTR_ID"
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_GRP_ID"
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_ID"
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_FIRST_NM"
            - name: SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_LAST_NM"
            - name: MEDIA_TYP_CD
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD"
            - name: RSPN_CD
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RSPN_CD"
            - name: MSG_TYP_CD
              type: varchar
              width: 10
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MSG_TYP_CD"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_ID"
            - name: NDC
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC"
            - name: PRI_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK"
            - name: PRI_NAT_KEY_STRING
              type: varchar
              width: 1000
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RSPN_CD : lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK"
            - name: JOB_NM
              type: varchar
              width: 100
              nullable: N
              map: DSJobName
            - name: ERROR_TYP
              type: varchar
              width: 50
              nullable: N
              map: "'CDLOOKUP'"
            - name: PHYSCL_FILE_NM
              type: varchar
              width: 100
              nullable: N
              map: "'CD_MPPNG'"
            - name: FRGN_NAT_KEY_STRING
              type: varchar
              width: 1000
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD : ';' : 'ESI' : ';' : 'IDS' : ';' : 'PHARMACY RESPONSE REASON' : ';' : 'PHARMACY RESPONSE REASON' : ';' : lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RSPN_CD"
            - name: FIRST_RECYC_TS
              type: timestamp
              nullable: N
              map: "StringToTimestamp(DateToString(lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRCS_DT, \"%yyyy-%mm-%dd\"), \"%yyyy-%mm-%dd\")"
            - name: JOB_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: SvFKeyLkpCheckMediaTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMediaTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckRspCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckRspCd
              initValue: ""
            - name: SvFKeyLkpCheckMsgTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMsgTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckTransTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckTransTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckGrpCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckGrpCd
              initValue: ""
            - name: SvFKeyLkpCheckMbrCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMbrCd
              initValue: ""
            - name: SvFKeyLkpCheckNDC
              type: varchar
              width: 255
              map: SvFKeyLkpCheckNDC
              initValue: ""
        - name: xfm_CheckLkpResults.V0S11P14
          type: filter
          source: xfm_CheckLkpResults.V0S11P14.map
          filter: "SvFKeyLkpCheckRspCd = 'Y'"
          schema: 
            - name: PRI_SK
              type: int
              width: 4
              nullable: N
            - name: PRI_NAT_KEY_STRING
              type: varchar
              width: 1000
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: JOB_NM
              type: varchar
              width: 100
              nullable: N
            - name: ERROR_TYP
              type: varchar
              width: 50
              nullable: N
            - name: PHYSCL_FILE_NM
              type: varchar
              width: 100
              nullable: N
            - name: FRGN_NAT_KEY_STRING
              type: varchar
              width: 1000
            - name: FIRST_RECYC_TS
              type: timestamp
              nullable: N
            - name: JOB_EXCTN_SK
              type: int
              width: 4
              nullable: N
        - name: xfm_CheckLkpResults.V0S11P15.map
          type: map
          source: Lkup_Fkey
          alias: lnk_IdsPdxDeniedFkey_EE_Lkup_Out
          stageVars: 
            - name: SvFKeyLkpCheckMediaTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckRspCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckMsgTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckTransTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckGrpCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckMbrCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckNDC
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
          schema: 
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_PDX_DENIED_TRANS_SK"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_UNIQ_KEY"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_NTNL_PROV_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD"
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_DENIED_DT"
            - name: RX_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RX_NO"
            - name: PRCS_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRCS_DT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK"
            - name: PRSCRB_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRSCRB_PROV_SK"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PROV_SPEC_CD_SK"
            - name: SRV_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRV_PROV_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_SK"
            - name: MEDIA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK"
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_GNDR_CD_SK"
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_RELSHP_CD_SK"
            - name: PDX_RSPN_RSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK"
            - name: PDX_RSPN_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK"
            - name: TRANS_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MAIL_ORDER_IN"
            - name: MBR_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_BRTH_DT"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLM_RCVD_TM"
            - name: SUB_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_BRTH_DT"
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_DISPENSE_FEE_AMT"
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_GROS_APRV_AMT"
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_NET_CHK_AMT"
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_PATN_PAY_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.INGR_CST_ALW_AMT"
            - name: TRANS_MO_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_MO_NO"
            - name: TRANS_YR_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_YR_NO"
            - name: MBR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SFX_NO"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_FIRST_NM"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_LAST_NM"
            - name: MBR_SSN
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SSN"
            - name: PDX_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_NM"
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_PHN_NO"
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_DEA_NO"
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_NTNL_PROV_ID"
            - name: PHYS_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_FIRST_NM"
            - name: PHYS_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_LAST_NM"
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 40
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_ST_ADDR_LN"
            - name: PHYS_CITY_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_CITY_NM"
            - name: PHYS_ST_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_ST_CD"
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_POSTAL_CD"
            - name: RX_LABEL_TX
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RX_LABEL_TX"
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SVC_PROV_NABP_NM"
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CAR_ID"
            - name: SRC_SYS_CLNT_ORG_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLNT_ORG_ID"
            - name: SRC_SYS_CLNT_ORG_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLNT_ORG_NM"
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CNTR_ID"
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_GRP_ID"
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_ID"
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_FIRST_NM"
            - name: SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_LAST_NM"
            - name: MEDIA_TYP_CD
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD"
            - name: RSPN_CD
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RSPN_CD"
            - name: MSG_TYP_CD
              type: varchar
              width: 10
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MSG_TYP_CD"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_ID"
            - name: NDC
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC"
            - name: PRI_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK"
            - name: PRI_NAT_KEY_STRING
              type: varchar
              width: 1000
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MSG_TYP_CD : lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK"
            - name: JOB_NM
              type: varchar
              width: 100
              nullable: N
              map: DSJobName
            - name: ERROR_TYP
              type: varchar
              width: 50
              nullable: N
              map: "'CDLOOKUP'"
            - name: PHYSCL_FILE_NM
              type: varchar
              width: 100
              nullable: N
              map: "'CD_MPPNG'"
            - name: FRGN_NAT_KEY_STRING
              type: varchar
              width: 1000
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD : ';' : 'OPTUMRX' : ';' : 'IDS' : ';' : 'PHARMACY MESSAGE REPONSE TYPE' : ';' : 'PHARMACY MESSAGE REPONSE TYPE' : ';' : lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MSG_TYP_CD"
            - name: FIRST_RECYC_TS
              type: timestamp
              nullable: N
              map: "StringToTimestamp(DateToString(lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRCS_DT, \"%yyyy-%mm-%dd\"), \"%yyyy-%mm-%dd\")"
            - name: JOB_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: SvFKeyLkpCheckMediaTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMediaTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckRspCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckRspCd
              initValue: ""
            - name: SvFKeyLkpCheckMsgTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMsgTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckTransTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckTransTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckGrpCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckGrpCd
              initValue: ""
            - name: SvFKeyLkpCheckMbrCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMbrCd
              initValue: ""
            - name: SvFKeyLkpCheckNDC
              type: varchar
              width: 255
              map: SvFKeyLkpCheckNDC
              initValue: ""
        - name: xfm_CheckLkpResults.V0S11P15
          type: filter
          source: xfm_CheckLkpResults.V0S11P15.map
          filter: "SvFKeyLkpCheckMsgTypeCd = 'Y'"
          schema: 
            - name: PRI_SK
              type: int
              width: 4
              nullable: N
            - name: PRI_NAT_KEY_STRING
              type: varchar
              width: 1000
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: JOB_NM
              type: varchar
              width: 100
              nullable: N
            - name: ERROR_TYP
              type: varchar
              width: 50
              nullable: N
            - name: PHYSCL_FILE_NM
              type: varchar
              width: 100
              nullable: N
            - name: FRGN_NAT_KEY_STRING
              type: varchar
              width: 1000
            - name: FIRST_RECYC_TS
              type: timestamp
              nullable: N
            - name: JOB_EXCTN_SK
              type: int
              width: 4
              nullable: N
        - name: xfm_CheckLkpResults.V0S11P16.map
          type: map
          source: Lkup_Fkey
          alias: lnk_IdsPdxDeniedFkey_EE_Lkup_Out
          stageVars: 
            - name: SvFKeyLkpCheckMediaTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckRspCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckMsgTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckTransTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckGrpCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckMbrCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckNDC
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
          schema: 
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_PDX_DENIED_TRANS_SK"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_UNIQ_KEY"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_NTNL_PROV_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD"
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_DENIED_DT"
            - name: RX_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RX_NO"
            - name: PRCS_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRCS_DT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK"
            - name: PRSCRB_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRSCRB_PROV_SK"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PROV_SPEC_CD_SK"
            - name: SRV_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRV_PROV_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_SK"
            - name: MEDIA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK"
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_GNDR_CD_SK"
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_RELSHP_CD_SK"
            - name: PDX_RSPN_RSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK"
            - name: PDX_RSPN_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK"
            - name: TRANS_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MAIL_ORDER_IN"
            - name: MBR_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_BRTH_DT"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLM_RCVD_TM"
            - name: SUB_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_BRTH_DT"
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_DISPENSE_FEE_AMT"
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_GROS_APRV_AMT"
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_NET_CHK_AMT"
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_PATN_PAY_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.INGR_CST_ALW_AMT"
            - name: TRANS_MO_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_MO_NO"
            - name: TRANS_YR_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_YR_NO"
            - name: MBR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SFX_NO"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_FIRST_NM"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_LAST_NM"
            - name: MBR_SSN
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SSN"
            - name: PDX_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_NM"
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_PHN_NO"
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_DEA_NO"
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_NTNL_PROV_ID"
            - name: PHYS_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_FIRST_NM"
            - name: PHYS_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_LAST_NM"
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 40
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_ST_ADDR_LN"
            - name: PHYS_CITY_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_CITY_NM"
            - name: PHYS_ST_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_ST_CD"
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_POSTAL_CD"
            - name: RX_LABEL_TX
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RX_LABEL_TX"
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SVC_PROV_NABP_NM"
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CAR_ID"
            - name: SRC_SYS_CLNT_ORG_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLNT_ORG_ID"
            - name: SRC_SYS_CLNT_ORG_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLNT_ORG_NM"
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CNTR_ID"
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_GRP_ID"
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_ID"
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_FIRST_NM"
            - name: SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_LAST_NM"
            - name: MEDIA_TYP_CD
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD"
            - name: RSPN_CD
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RSPN_CD"
            - name: MSG_TYP_CD
              type: varchar
              width: 10
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MSG_TYP_CD"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_ID"
            - name: NDC
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC"
            - name: PRI_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK"
            - name: PRI_NAT_KEY_STRING
              type: varchar
              width: 1000
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD : lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK"
            - name: JOB_NM
              type: varchar
              width: 100
              nullable: N
              map: DSJobName
            - name: ERROR_TYP
              type: varchar
              width: 50
              nullable: N
              map: "'CDLOOKUP'"
            - name: PHYSCL_FILE_NM
              type: varchar
              width: 100
              nullable: N
              map: "'CD_MPPNG'"
            - name: FRGN_NAT_KEY_STRING
              type: varchar
              width: 1000
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD : ';' : 'OPTUMRX' : ';' : 'IDS' : ';' : 'PHARMACY TRANSACTION TYPE' : ';' : 'PHARMACY TRANSACTION TYPE' : ';' : lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD"
            - name: FIRST_RECYC_TS
              type: timestamp
              nullable: N
              map: "StringToTimestamp(DateToString(lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRCS_DT, \"%yyyy-%mm-%dd\"), \"%yyyy-%mm-%dd\")"
            - name: JOB_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: SvFKeyLkpCheckMediaTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMediaTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckRspCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckRspCd
              initValue: ""
            - name: SvFKeyLkpCheckMsgTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMsgTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckTransTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckTransTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckGrpCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckGrpCd
              initValue: ""
            - name: SvFKeyLkpCheckMbrCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMbrCd
              initValue: ""
            - name: SvFKeyLkpCheckNDC
              type: varchar
              width: 255
              map: SvFKeyLkpCheckNDC
              initValue: ""
        - name: xfm_CheckLkpResults.V0S11P16
          type: filter
          source: xfm_CheckLkpResults.V0S11P16.map
          filter: "SvFKeyLkpCheckTransTypeCd = 'Y'"
          schema: 
            - name: PRI_SK
              type: int
              width: 4
              nullable: N
            - name: PRI_NAT_KEY_STRING
              type: varchar
              width: 1000
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: JOB_NM
              type: varchar
              width: 100
              nullable: N
            - name: ERROR_TYP
              type: varchar
              width: 50
              nullable: N
            - name: PHYSCL_FILE_NM
              type: varchar
              width: 100
              nullable: N
            - name: FRGN_NAT_KEY_STRING
              type: varchar
              width: 1000
            - name: FIRST_RECYC_TS
              type: timestamp
              nullable: N
            - name: JOB_EXCTN_SK
              type: int
              width: 4
              nullable: N
        - name: xfm_CheckLkpResults.V0S11P17.map
          type: map
          source: Lkup_Fkey
          alias: lnk_IdsPdxDeniedFkey_EE_Lkup_Out
          stageVars: 
            - name: SvFKeyLkpCheckMediaTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckRspCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckMsgTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckTransTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckGrpCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckMbrCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckNDC
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
          schema: 
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_PDX_DENIED_TRANS_SK"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_UNIQ_KEY"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_NTNL_PROV_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD"
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_DENIED_DT"
            - name: RX_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RX_NO"
            - name: PRCS_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRCS_DT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK"
            - name: PRSCRB_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRSCRB_PROV_SK"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PROV_SPEC_CD_SK"
            - name: SRV_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRV_PROV_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_SK"
            - name: MEDIA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK"
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_GNDR_CD_SK"
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_RELSHP_CD_SK"
            - name: PDX_RSPN_RSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK"
            - name: PDX_RSPN_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK"
            - name: TRANS_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MAIL_ORDER_IN"
            - name: MBR_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_BRTH_DT"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLM_RCVD_TM"
            - name: SUB_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_BRTH_DT"
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_DISPENSE_FEE_AMT"
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_GROS_APRV_AMT"
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_NET_CHK_AMT"
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_PATN_PAY_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.INGR_CST_ALW_AMT"
            - name: TRANS_MO_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_MO_NO"
            - name: TRANS_YR_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_YR_NO"
            - name: MBR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SFX_NO"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_FIRST_NM"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_LAST_NM"
            - name: MBR_SSN
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SSN"
            - name: PDX_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_NM"
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_PHN_NO"
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_DEA_NO"
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_NTNL_PROV_ID"
            - name: PHYS_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_FIRST_NM"
            - name: PHYS_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_LAST_NM"
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 40
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_ST_ADDR_LN"
            - name: PHYS_CITY_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_CITY_NM"
            - name: PHYS_ST_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_ST_CD"
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_POSTAL_CD"
            - name: RX_LABEL_TX
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RX_LABEL_TX"
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SVC_PROV_NABP_NM"
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CAR_ID"
            - name: SRC_SYS_CLNT_ORG_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLNT_ORG_ID"
            - name: SRC_SYS_CLNT_ORG_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLNT_ORG_NM"
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CNTR_ID"
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_GRP_ID"
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_ID"
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_FIRST_NM"
            - name: SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_LAST_NM"
            - name: MEDIA_TYP_CD
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD"
            - name: RSPN_CD
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RSPN_CD"
            - name: MSG_TYP_CD
              type: varchar
              width: 10
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MSG_TYP_CD"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_ID"
            - name: NDC
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC"
            - name: PRI_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK"
            - name: PRI_NAT_KEY_STRING
              type: varchar
              width: 1000
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_ID : lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK"
            - name: JOB_NM
              type: varchar
              width: 100
              nullable: N
              map: DSJobName
            - name: ERROR_TYP
              type: varchar
              width: 50
              nullable: N
              map: "'FKEYLOOKUP'"
            - name: PHYSCL_FILE_NM
              type: varchar
              width: 100
              nullable: N
              map: "'GRP'"
            - name: FRGN_NAT_KEY_STRING
              type: varchar
              width: 1000
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD : ';' : 'OPTUMRX' : ';' : 'IDS' : ';' : 'GRP_ID' : ';' : 'GROUP_TYPE' : ';' : lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_ID"
            - name: FIRST_RECYC_TS
              type: timestamp
              nullable: N
              map: "StringToTimestamp(DateToString(lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRCS_DT, \"%yyyy-%mm-%dd\"), \"%yyyy-%mm-%dd\")"
            - name: JOB_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: SvFKeyLkpCheckMediaTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMediaTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckRspCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckRspCd
              initValue: ""
            - name: SvFKeyLkpCheckMsgTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMsgTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckTransTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckTransTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckGrpCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckGrpCd
              initValue: ""
            - name: SvFKeyLkpCheckMbrCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMbrCd
              initValue: ""
            - name: SvFKeyLkpCheckNDC
              type: varchar
              width: 255
              map: SvFKeyLkpCheckNDC
              initValue: ""
        - name: xfm_CheckLkpResults.V0S11P17
          type: filter
          source: xfm_CheckLkpResults.V0S11P17.map
          filter: "SvFKeyLkpCheckGrpCd = 'Y'"
          schema: 
            - name: PRI_SK
              type: int
              width: 4
              nullable: N
            - name: PRI_NAT_KEY_STRING
              type: varchar
              width: 1000
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: JOB_NM
              type: varchar
              width: 100
              nullable: N
            - name: ERROR_TYP
              type: varchar
              width: 50
              nullable: N
            - name: PHYSCL_FILE_NM
              type: varchar
              width: 100
              nullable: N
            - name: FRGN_NAT_KEY_STRING
              type: varchar
              width: 1000
            - name: FIRST_RECYC_TS
              type: timestamp
              nullable: N
            - name: JOB_EXCTN_SK
              type: int
              width: 4
              nullable: N
        - name: xfm_CheckLkpResults.V0S11P18.map
          type: map
          source: Lkup_Fkey
          alias: lnk_IdsPdxDeniedFkey_EE_Lkup_Out
          stageVars: 
            - name: SvFKeyLkpCheckMediaTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckRspCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckMsgTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckTransTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckGrpCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckMbrCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckNDC
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
          schema: 
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_PDX_DENIED_TRANS_SK"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_UNIQ_KEY"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_NTNL_PROV_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD"
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_DENIED_DT"
            - name: RX_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RX_NO"
            - name: PRCS_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRCS_DT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK"
            - name: PRSCRB_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRSCRB_PROV_SK"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PROV_SPEC_CD_SK"
            - name: SRV_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRV_PROV_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_SK"
            - name: MEDIA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK"
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_GNDR_CD_SK"
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_RELSHP_CD_SK"
            - name: PDX_RSPN_RSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK"
            - name: PDX_RSPN_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK"
            - name: TRANS_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MAIL_ORDER_IN"
            - name: MBR_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_BRTH_DT"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLM_RCVD_TM"
            - name: SUB_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_BRTH_DT"
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_DISPENSE_FEE_AMT"
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_GROS_APRV_AMT"
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_NET_CHK_AMT"
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_PATN_PAY_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.INGR_CST_ALW_AMT"
            - name: TRANS_MO_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_MO_NO"
            - name: TRANS_YR_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_YR_NO"
            - name: MBR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SFX_NO"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_FIRST_NM"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_LAST_NM"
            - name: MBR_SSN
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SSN"
            - name: PDX_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_NM"
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_PHN_NO"
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_DEA_NO"
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_NTNL_PROV_ID"
            - name: PHYS_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_FIRST_NM"
            - name: PHYS_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_LAST_NM"
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 40
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_ST_ADDR_LN"
            - name: PHYS_CITY_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_CITY_NM"
            - name: PHYS_ST_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_ST_CD"
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_POSTAL_CD"
            - name: RX_LABEL_TX
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RX_LABEL_TX"
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SVC_PROV_NABP_NM"
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CAR_ID"
            - name: SRC_SYS_CLNT_ORG_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLNT_ORG_ID"
            - name: SRC_SYS_CLNT_ORG_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLNT_ORG_NM"
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CNTR_ID"
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_GRP_ID"
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_ID"
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_FIRST_NM"
            - name: SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_LAST_NM"
            - name: MEDIA_TYP_CD
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD"
            - name: RSPN_CD
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RSPN_CD"
            - name: MSG_TYP_CD
              type: varchar
              width: 10
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MSG_TYP_CD"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_ID"
            - name: NDC
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC"
            - name: PRI_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK"
            - name: PRI_NAT_KEY_STRING
              type: varchar
              width: 1000
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_ID : lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK"
            - name: JOB_NM
              type: varchar
              width: 100
              nullable: N
              map: DSJobName
            - name: ERROR_TYP
              type: varchar
              width: 50
              nullable: N
              map: "'FKEYLOOKUP'"
            - name: PHYSCL_FILE_NM
              type: varchar
              width: 100
              nullable: N
              map: "'MBR'"
            - name: FRGN_NAT_KEY_STRING
              type: varchar
              width: 1000
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD : ';' : 'OPTUMRXI' : ';' : 'IDS' : ';' : 'MBR_ID' : ';' : 'MBR_ID' : ';' : lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_ID"
            - name: FIRST_RECYC_TS
              type: timestamp
              nullable: N
              map: "StringToTimestamp(DateToString(lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRCS_DT, \"%yyyy-%mm-%dd\"), \"%yyyy-%mm-%dd\")"
            - name: JOB_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: SvFKeyLkpCheckMediaTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMediaTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckRspCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckRspCd
              initValue: ""
            - name: SvFKeyLkpCheckMsgTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMsgTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckTransTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckTransTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckGrpCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckGrpCd
              initValue: ""
            - name: SvFKeyLkpCheckMbrCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMbrCd
              initValue: ""
            - name: SvFKeyLkpCheckNDC
              type: varchar
              width: 255
              map: SvFKeyLkpCheckNDC
              initValue: ""
        - name: xfm_CheckLkpResults.V0S11P18
          type: filter
          source: xfm_CheckLkpResults.V0S11P18.map
          filter: "SvFKeyLkpCheckMbrCd = 'Y'"
          schema: 
            - name: PRI_SK
              type: int
              width: 4
              nullable: N
            - name: PRI_NAT_KEY_STRING
              type: varchar
              width: 1000
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: JOB_NM
              type: varchar
              width: 100
              nullable: N
            - name: ERROR_TYP
              type: varchar
              width: 50
              nullable: N
            - name: PHYSCL_FILE_NM
              type: varchar
              width: 100
              nullable: N
            - name: FRGN_NAT_KEY_STRING
              type: varchar
              width: 1000
            - name: FIRST_RECYC_TS
              type: timestamp
              nullable: N
            - name: JOB_EXCTN_SK
              type: int
              width: 4
              nullable: N
        - name: xfm_CheckLkpResults.V0S11P19.map
          type: map
          source: Lkup_Fkey
          alias: lnk_IdsPdxDeniedFkey_EE_Lkup_Out
          stageVars: 
            - name: SvFKeyLkpCheckMediaTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckRspCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckMsgTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckTransTypeCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckGrpCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckMbrCd
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: SvFKeyLkpCheckNDC
              type: varchar
              width: 255
              map: " IF Trim(( IF IsNotNull((lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK)) THEN (lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK) ELSE 0)) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
          schema: 
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_PDX_DENIED_TRANS_SK"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_UNIQ_KEY"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_NTNL_PROV_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD"
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_DENIED_DT"
            - name: RX_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RX_NO"
            - name: PRCS_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRCS_DT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK"
            - name: PRSCRB_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRSCRB_PROV_SK"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PROV_SPEC_CD_SK"
            - name: SRV_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRV_PROV_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_SK"
            - name: MEDIA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD_SK"
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_GNDR_CD_SK"
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_RELSHP_CD_SK"
            - name: PDX_RSPN_RSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_RSN_CD_SK"
            - name: PDX_RSPN_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_RSPN_TYP_CD_SK"
            - name: TRANS_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_TYP_CD_SK"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MAIL_ORDER_IN"
            - name: MBR_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_BRTH_DT"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLM_RCVD_TM"
            - name: SUB_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_BRTH_DT"
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_DISPENSE_FEE_AMT"
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_GROS_APRV_AMT"
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_NET_CHK_AMT"
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.BILL_RX_PATN_PAY_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.INGR_CST_ALW_AMT"
            - name: TRANS_MO_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_MO_NO"
            - name: TRANS_YR_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.TRANS_YR_NO"
            - name: MBR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SFX_NO"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_FIRST_NM"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_LAST_NM"
            - name: MBR_SSN
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MBR_SSN"
            - name: PDX_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_NM"
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PDX_PHN_NO"
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_DEA_NO"
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_NTNL_PROV_ID"
            - name: PHYS_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_FIRST_NM"
            - name: PHYS_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_LAST_NM"
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 40
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_ST_ADDR_LN"
            - name: PHYS_CITY_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_CITY_NM"
            - name: PHYS_ST_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_ST_CD"
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PHYS_POSTAL_CD"
            - name: RX_LABEL_TX
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RX_LABEL_TX"
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SVC_PROV_NABP_NM"
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CAR_ID"
            - name: SRC_SYS_CLNT_ORG_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLNT_ORG_ID"
            - name: SRC_SYS_CLNT_ORG_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CLNT_ORG_NM"
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CNTR_ID"
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_GRP_ID"
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_ID"
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_FIRST_NM"
            - name: SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SUB_LAST_NM"
            - name: MEDIA_TYP_CD
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MEDIA_TYP_CD"
            - name: RSPN_CD
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.RSPN_CD"
            - name: MSG_TYP_CD
              type: varchar
              width: 10
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.MSG_TYP_CD"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.GRP_ID"
            - name: NDC
              type: varchar
              width: 20
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC"
            - name: PRI_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK"
            - name: PRI_NAT_KEY_STRING
              type: varchar
              width: 1000
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC : lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC_SK"
            - name: JOB_NM
              type: varchar
              width: 100
              nullable: N
              map: DSJobName
            - name: ERROR_TYP
              type: varchar
              width: 50
              nullable: N
              map: "'FKEYLOOKUP'"
            - name: PHYSCL_FILE_NM
              type: varchar
              width: 100
              nullable: N
              map: "'NDC'"
            - name: FRGN_NAT_KEY_STRING
              type: varchar
              width: 1000
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.SRC_SYS_CD : ';' : 'OPTUMRX' : ';' : 'IDS' : ';' : 'NDC' : ';' : 'NDC' : ';' : lnk_IdsPdxDeniedFkey_EE_Lkup_Out.NDC"
            - name: FIRST_RECYC_TS
              type: timestamp
              nullable: N
              map: "StringToTimestamp(DateToString(lnk_IdsPdxDeniedFkey_EE_Lkup_Out.PRCS_DT, \"%yyyy-%mm-%dd\"), \"%yyyy-%mm-%dd\")"
            - name: JOB_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsPdxDeniedFkey_EE_Lkup_Out.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: SvFKeyLkpCheckMediaTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMediaTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckRspCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckRspCd
              initValue: ""
            - name: SvFKeyLkpCheckMsgTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMsgTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckTransTypeCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckTransTypeCd
              initValue: ""
            - name: SvFKeyLkpCheckGrpCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckGrpCd
              initValue: ""
            - name: SvFKeyLkpCheckMbrCd
              type: varchar
              width: 255
              map: SvFKeyLkpCheckMbrCd
              initValue: ""
            - name: SvFKeyLkpCheckNDC
              type: varchar
              width: 255
              map: SvFKeyLkpCheckNDC
              initValue: ""
        - name: xfm_CheckLkpResults.V0S11P19
          type: filter
          source: xfm_CheckLkpResults.V0S11P19.map
          filter: "SvFKeyLkpCheckNDC = 'Y'"
          schema: 
            - name: PRI_SK
              type: int
              width: 4
              nullable: N
            - name: PRI_NAT_KEY_STRING
              type: varchar
              width: 1000
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: JOB_NM
              type: varchar
              width: 100
              nullable: N
            - name: ERROR_TYP
              type: varchar
              width: 50
              nullable: N
            - name: PHYSCL_FILE_NM
              type: varchar
              width: 100
              nullable: N
            - name: FRGN_NAT_KEY_STRING
              type: varchar
              width: 1000
            - name: FIRST_RECYC_TS
              type: timestamp
              nullable: N
            - name: JOB_EXCTN_SK
              type: int
              width: 4
              nullable: N
        - name: fnl_all_fail
          type: union
          sources: 
            - xfm_CheckLkpResults.V0S11P13
            - xfm_CheckLkpResults.V0S11P14
            - xfm_CheckLkpResults.V0S11P15
            - xfm_CheckLkpResults.V0S11P16
            - xfm_CheckLkpResults.V0S11P17
            - xfm_CheckLkpResults.V0S11P18
            - xfm_CheckLkpResults.V0S11P19
          option: 
            unionType: all
          schema: 
            - name: PRI_SK
              type: int
              width: 4
              nullable: N
              map: PRI_SK
            - name: PRI_NAT_KEY_STRING
              type: varchar
              width: 1000
              map: PRI_NAT_KEY_STRING
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: SRC_SYS_CD_SK
            - name: JOB_NM
              type: varchar
              width: 100
              nullable: N
              map: JOB_NM
            - name: ERROR_TYP
              type: varchar
              width: 50
              nullable: N
              map: ERROR_TYP
            - name: PHYSCL_FILE_NM
              type: varchar
              width: 100
              nullable: N
              map: PHYSCL_FILE_NM
            - name: FRGN_NAT_KEY_STRING
              type: varchar
              width: 1000
              map: FRGN_NAT_KEY_STRING
            - name: FIRST_RECYC_TS
              type: timestamp
              nullable: N
              map: FIRST_RECYC_TS
            - name: JOB_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: JOB_EXCTN_SK


