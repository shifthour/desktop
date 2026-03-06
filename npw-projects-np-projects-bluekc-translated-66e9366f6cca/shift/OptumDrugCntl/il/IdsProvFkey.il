model:
  config:
    origin: datastage
  jobs:
    - name: IdsProvFkey
      origin: datastage
      type: dataflow
      folder: "provider/Provider"
      runAfterSubrOnJobSuccess: enabled
      description: "IDSProvider foreign keying. "
      fullDescription: "*****************************************************************************************************************************************************************************\nCOPYRIGHT 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\n\nDESCRIPTION:     Takes the sorted file and applies the foreign key.      \n\n\nPROCESSING:\n\n      \nMODIFICATIONS:\nDeveloper                Date                 Project/Altiris #                               Change Description                  Development Project      Code Reviewer          Date Reviewed       \n--------------------            --------------------     ------------------------                              ----------------------------------------           --------------------------------       -------------------------------   ----------------------------       \nGiri Mallavaram        10/16/2019      6131 PBM Replacement               Initial Programming                       IntegrateDevl                 Kalyan Neelam          10/21/2019\n\nBhargava Rampilla   2019-10-24   6131-PBM REPLACEMENT        Modified feilds as per the              IntegrateDEV1                   Kalyan Neelam           2019-11-20         \n                                                                                                                                OptumRX transformation rules:\n                                                                                                                                 EDI_DEST_QLFR_ TX\n                                                                                                                                 NTNL_PROV_ID"
      parameters:
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
        - name: InFile
          type: string
          description: InFile
          defaultType: expr
          defaultValue: "IdsProvExtr.DrugProv.uniq"
        - name: Logging
          type: string
          description: "Log Errors"
          defaultType: expr
          defaultValue: Y
      afterSubr:
        subrType: "DSU.HASH.CLEAR\\\\3;hf_unk_fclty_typ;hf_unk_spec_cd;hf_unk_prov_typ"
        subr: "3;hf_unk_fclty_typ;hf_unk_spec_cd;hf_unk_prov_typ"
      steps:
        - name: IdsProvExtr
          type: fileReader
          urls: 
            - "#$FilePath#/key/#InFile#"
          rejectMode: continue
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ID
              type: char
              width: 12
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: CMN_PRCT
              type: char
              width: 12
              nullable: N
            - name: PROV_BILL_SVC_SK
              type: char
              width: 10
              nullable: N
            - name: REL_GRP_PROV
              type: char
              width: 12
              nullable: N
            - name: REL_IPA_PROV
              type: char
              width: 12
              nullable: N
            - name: PROV_CAP_PAYMT_EFT_METH_CD
              type: char
              width: 1
              nullable: N
            - name: PROV_CLM_PAYMT_EFT_METH_CD
              type: char
              width: 1
              nullable: N
            - name: PROV_CLM_PAYMT_METH_CD
              type: char
              width: 1
              nullable: N
            - name: PROV_ENTY_CD
              type: char
              width: 1
              nullable: N
            - name: PROV_FCLTY_TYP_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_PRCTC_TYP_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_SVC_CAT_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_SPEC_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_STTUS_CD
              type: char
              width: 2
              nullable: N
            - name: PROV_TERM_RSN_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_TYP_CD
              type: char
              width: 4
              nullable: N
            - name: TERM_DT
              type: timestamp
              nullable: N
            - name: PAYMT_HOLD_DT
              type: timestamp
              nullable: N
            - name: CLRNGHOUSE_ID
              type: char
              width: 30
              nullable: N
            - name: EDI_DEST_ID
              type: char
              width: 15
              nullable: N
            - name: EDI_DEST_QUAL
              type: char
              width: 2
              nullable: N
            - name: NTNL_PROV_ID
              type: char
              width: 10
              nullable: N
            - name: PROV_ADDR_ID
              type: char
              width: 12
              nullable: N
            - name: PROV_NM
              type: varchar
              width: 75
            - name: TAX_ID
              type: char
              width: 9
            - name: TXNMY_CD
              type: varchar
              width: 20
              nullable: N
          keys:
            - PROV_SK
        - name: ProvTmp.DSLink96
          type: fileReader
          urls: 
            - "#$FilePath#/load/PROV.tmp"
          rejectMode: continue
          executeAfter:
            - ProvTmp.LoadFile
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
          schema: 
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ID
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
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_BILL_SVC_SK
              type: int
              width: 4
              nullable: N
            - name: REL_GRP_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: REL_IPA_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CAP_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CLM_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CLM_PAYMT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ENTY_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_FCLTY_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_PRCTC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_SVC_CAT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_TERM_RSN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PAYMT_HOLD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: CLRNGHSE_ID
              type: varchar
              width: 40
              nullable: N
            - name: EDI_DEST_ID
              type: varchar
              width: 20
              nullable: N
            - name: EDI_DEST_QLFR_TX
              type: varchar
              width: 20
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROV_ADDR_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROV_NM
              type: varchar
              width: 75
            - name: TAX_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROV_FCLTY_TYP_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_SPEC_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_TYP_CD
              type: char
              width: 4
              nullable: N
            - name: NTNL_PROV_SK
              type: int
              width: 4
            - name: TXNMY_CD
              type: varchar
              width: 20
              nullable: N
          keys:
            - PROV_SK
        - name: UnkSpecCd
          type: fileWriter
          urls: 
            - "#$FilePath#/load/BADPROVSPECCD.dat"
          source: hf_unk_spec_cd
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: UnkProvTyp
          type: fileWriter
          urls: 
            - "#$FilePath#/load/BADPROVTYP.dat"
          source: hf_unk_prov_typ
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: ProvTmp.LoadFile
          type: fileWriter
          urls: 
            - "#$FilePath#/load/PROV.tmp"
          executeBefore:
            - ProvTmp.DSLink96
          source: Collector
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: Prov
          type: fileWriter
          urls: 
            - "#$FilePath#/load/PROV.dat"
          source: "Transformer_95.V0S95P2"
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: UnkFcltyTyp
          type: fileWriter
          urls: 
            - "#$FilePath#/load/BADPROVFCLTYTYP.dat"
          source: hf_unk_fclty_typ
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: hf_recycle
          type: dbWriter
          originalDb: hashedFile
          table: "/hf_recycle"
          tableAction: append
          source: "ForeignKey1.V0S20P4"
            # Hashed File has no directory specified
        - name: hf_unk_fclty_typ
          type: map
          source: "Transformer_95.V0S95P3"
          alias: EmailUnkFcltyTyp
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: PROV_FCLTY_TYP_CD
              type: char
              width: 4
              nullable: N
              map: "EmailUnkFcltyTyp.PROV_FCLTY_TYP_CD"
        - name: hf_unk_spec_cd
          type: map
          source: "Transformer_95.V0S95P4"
          alias: EmailUnkSpecCd
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: PROV_SPEC_CD
              type: char
              width: 4
              nullable: N
              map: "EmailUnkSpecCd.PROV_SPEC_CD"
        - name: hf_unk_prov_typ
          type: map
          source: "Transformer_95.V0S95P5"
          alias: EmailUnkProvTyp
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: PROV_TYP_CD
              type: char
              width: 4
              nullable: N
              map: "EmailUnkProvTyp.PROV_TYP_CD"
        - name: ForeignKey1.V0S20P3.map
          type: map
          source: IdsProvExtr
          alias: lnkProv1
          stageVars: 
            - name: SrcSysCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"IDS\", lnkProv1.PROV_SK, \"SOURCE SYSTEM\", lnkProv1.SRC_SYS_CD, Logging)"
              initValue: "0"
            - name: RelGrpProvSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyProv(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, lnkProv1.REL_GRP_PROV, Logging)"
              initValue: "0"
            - name: RelIpaProvSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyProv(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, lnkProv1.REL_IPA_PROV, Logging)"
              initValue: "0"
            - name: ProvFcltyTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyFcltyTyp(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, lnkProv1.PROV_FCLTY_TYP_CD, Logging)"
              initValue: "0"
            - name: ProvPrctcTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER PRACTICE TYPE\", lnkProv1.PROV_PRCTC_TYP_CD, Logging)"
              initValue: "0"
            - name: ProvSvcCatCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER SERVICE CATEGORY\", lnkProv1.PROV_SVC_CAT_CD, Logging)"
              initValue: "0"
            - name: ProvEntyCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER ENTITY\", lnkProv1.PROV_ENTY_CD, Logging)"
              initValue: "0"
            - name: ProvSpecCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyProvSpec(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, lnkProv1.PROV_SPEC_CD, Logging)"
              initValue: "0"
            - name: ProvSttusCdSK
              type: decimal
              width: 0
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER STATUS\", lnkProv1.PROV_STTUS_CD, Logging)"
              initValue: "0"
            - name: ProvTermRsnCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER TERMINATION REASON\", lnkProv1.PROV_TERM_RSN_CD, Logging)"
              initValue: "0"
            - name: ProvTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyProvTyp(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, lnkProv1.PROV_TYP_CD, Logging)"
              initValue: "0"
            - name: ProvCapPaymtEftMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER CAPITATION PAYMENT EFT METHOD\", lnkProv1.PROV_CAP_PAYMT_EFT_METH_CD, Logging)"
              initValue: "0"
            - name: ProvClmPaymtEftMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER CLAIM PAYMENT EFT METHOD\", lnkProv1.PROV_CLM_PAYMT_EFT_METH_CD, Logging)"
              initValue: "0"
            - name: ProvClmPaymtMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER CLAIM PAYMENT METHOD\", lnkProv1.PROV_CLM_PAYMT_METH_CD, Logging)"
              initValue: "0"
            - name: PaymtHoldDtSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate(\"IDS\", lnkProv1.PROV_SK, lnkProv1.PAYMT_HOLD_DT, Logging)"
              initValue: "0"
            - name: ProvBillSvcSk
              type: decimal
              width: 18
              precision: 4
              map: " If lnkProv1.PROV_BILL_SVC_SK = \"NA\" then 1 else lnkProv1.PROV_BILL_SVC_SK"
              initValue: "0"
            - name: TermDtSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate(\"IDS\", lnkProv1.PROV_SK, lnkProv1.TERM_DT, Logging)"
              initValue: "0"
            - name: CmnPrtFacetsSK
              type: decimal
              width: 0
              map: "GetFkeyCmnPrct('FACETS', lnkProv1.PROV_SK, lnkProv1.CMN_PRCT, 'X')"
              initValue: "0"
            - name: CmnPrtVcacSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCmnPrct('VCAC', lnkProv1.PROV_SK, lnkProv1.CMN_PRCT, 'X')"
              initValue: "0"
            - name: CmnPrctSk
              type: decimal
              width: 18
              precision: 4
              map: " If len(TRIM(lnkProv1.CMN_PRCT)) = 0 Then 1 Else  If CmnPrtFacetsSK = 0 then CmnPrtVcacSK else CmnPrtFacetsSK"
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: "lnkProv1.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(lnkProv1.PROV_SK)"
              initValue: "0"
            - name: svNtnlProvSK
              type: decimal
              width: 0
              map: "GetFkeyNtnlProvId('CMS', lnkProv1.PROV_SK, lnkProv1.NTNL_PROV_ID, Logging)"
              initValue: "0"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "lnkProv1.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "lnkProv1.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "lnkProv1.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "lnkProv1.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "lnkProv1.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnkProv1.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "lnkProv1.PRI_KEY_STRING"
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnkProv1.PROV_SK"
            - name: PROV_ID
              type: char
              width: 12
              nullable: N
              map: "lnkProv1.PROV_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnkProv1.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnkProv1.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: CMN_PRCT
              type: char
              width: 12
              nullable: N
              map: "lnkProv1.CMN_PRCT"
            - name: PROV_BILL_SVC_SK
              type: int
              width: 4
              nullable: N
              map: " If lnkProv1.SRC_SYS_CD = \"OPTUMRX\" THEN 1 ELSE ProvBillSvcSk"
            - name: REL_GRP_PROV
              type: char
              width: 12
              nullable: N
              map: "lnkProv1.REL_GRP_PROV"
            - name: REL_IPA_PROV
              type: char
              width: 12
              nullable: N
              map: "lnkProv1.REL_IPA_PROV"
            - name: PROV_CAP_PAYMT_EFT_METH_CD
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PROV_CAP_PAYMT_EFT_METH_CD"
            - name: PROV_CLM_PAYMT_EFT_METH_CD
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PROV_CLM_PAYMT_EFT_METH_CD"
            - name: PROV_CLM_PAYMT_METH_CD
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PROV_CLM_PAYMT_METH_CD"
            - name: PROV_ENTY_CD
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PROV_ENTY_CD"
            - name: PROV_FCLTY_TYP_CD
              type: char
              width: 4
              nullable: N
              map: "lnkProv1.PROV_FCLTY_TYP_CD"
            - name: PROV_PRCTC_TYP_CD
              type: char
              width: 4
              nullable: N
              map: "lnkProv1.PROV_PRCTC_TYP_CD"
            - name: PROV_SVC_CAT_CD
              type: char
              width: 4
              nullable: N
              map: "lnkProv1.PROV_SVC_CAT_CD"
            - name: PROV_SPEC_CD
              type: char
              width: 4
              nullable: N
              map: "lnkProv1.PROV_SPEC_CD"
            - name: PROV_STTUS_CD
              type: char
              width: 2
              nullable: N
              map: "lnkProv1.PROV_STTUS_CD"
            - name: PROV_TERM_RSN_CD
              type: char
              width: 4
              nullable: N
              map: "lnkProv1.PROV_TERM_RSN_CD"
            - name: PROV_TYP_CD
              type: char
              width: 4
              nullable: N
              map: "lnkProv1.PROV_TYP_CD"
            - name: TERM_DT
              type: timestamp
              nullable: N
              map: "lnkProv1.TERM_DT"
            - name: PAYMT_HOLD_DT
              type: timestamp
              nullable: N
              map: "lnkProv1.PAYMT_HOLD_DT"
            - name: CLRNGHOUSE_ID
              type: char
              width: 30
              nullable: N
              map: "lnkProv1.CLRNGHOUSE_ID"
            - name: EDI_DEST_ID
              type: char
              width: 15
              nullable: N
              map: "lnkProv1.EDI_DEST_ID"
            - name: EDI_DEST_QUAL
              type: char
              width: 2
              nullable: N
              map: "lnkProv1.EDI_DEST_QUAL"
            - name: NTNL_PROV_ID
              type: char
              width: 10
              nullable: N
              map: " If (IsNull(lnkProv1.NTNL_PROV_ID) = @True or Trim(lnkProv1.NTNL_PROV_ID) = '') then 1 else lnkProv1.NTNL_PROV_ID"
            - name: PROV_ADDR_ID
              type: char
              width: 12
              nullable: N
              map: "lnkProv1.PROV_ADDR_ID"
            - name: PROV_NM
              type: varchar
              width: 75
              map: "lnkProv1.PROV_NM"
            - name: TAX_ID
              type: char
              width: 9
              map: "lnkProv1.TAX_ID"
            - name: TXNMY_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnkProv1.TXNMY_CD"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: SrcSysCdSk
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
              map: " If lnkProv1.SRC_SYS_CD = \"OPTUMRX\" THEN 1 else CmnPrctSk"
            - name: REL_GRP_PROV_SK
              type: int
              width: 4
              nullable: N
              map: RelGrpProvSk
            - name: REL_IPA_PROV_SK
              type: int
              width: 4
              nullable: N
              map: RelIpaProvSk
            - name: PROV_CAP_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: ProvCapPaymtEftMethCdSk
            - name: PROV_CLM_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: ProvClmPaymtEftMethCdSk
            - name: PROV_CLM_PAYMT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: ProvClmPaymtMethCdSk
            - name: PROV_ENTY_CD_SK
              type: int
              width: 4
              nullable: N
              map: ProvEntyCdSk
            - name: PROV_FCLTY_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: ProvFcltyTypCdSk
            - name: PROV_PRCTC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: ProvPrctcTypCdSk
            - name: PROV_SVC_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: ProvSvcCatCdSk
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: ProvSpecCdSk
            - name: PROV_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: ProvSttusCdSK
            - name: PROV_TERM_RSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: ProvTermRsnCdSk
            - name: PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: ProvTypCdSk
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
              map: TermDtSk
            - name: PAYMT_HOLD_DT_SK
              type: char
              width: 10
              nullable: N
              map: PaymtHoldDtSk
            - name: CLRNGHSE_ID
              type: char
              width: 30
              nullable: N
              map: "lnkProv1.CLRNGHOUSE_ID"
            - name: EDI_DEST_QLFR_TX
              type: char
              width: 2
              nullable: N
              map: " If lnkProv1.SRC_SYS_CD = \"OPTUMRX\" then @NULL else lnkProv1.EDI_DEST_QUAL"
            - name: NTNL_PROV_SK
              type: int
              width: 4
              map: " If lnkProv1.NTNL_PROV_ID = 'NA' Then 1 Else  If IsNull(lnkProv1.NTNL_PROV_ID) = @TRUE then 0 Else svNtnlProvSK"
            - name: SrcSysCdSk
              type: decimal
              width: 18
              precision: 4
              map: SrcSysCdSk
              initValue: "0"
            - name: RelGrpProvSk
              type: decimal
              width: 18
              precision: 4
              map: RelGrpProvSk
              initValue: "0"
            - name: RelIpaProvSk
              type: decimal
              width: 18
              precision: 4
              map: RelIpaProvSk
              initValue: "0"
            - name: ProvFcltyTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvFcltyTypCdSk
              initValue: "0"
            - name: ProvPrctcTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvPrctcTypCdSk
              initValue: "0"
            - name: ProvSvcCatCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvSvcCatCdSk
              initValue: "0"
            - name: ProvEntyCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvEntyCdSk
              initValue: "0"
            - name: ProvSpecCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvSpecCdSk
              initValue: "0"
            - name: ProvSttusCdSK
              type: decimal
              width: 0
              map: ProvSttusCdSK
              initValue: "0"
            - name: ProvTermRsnCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvTermRsnCdSk
              initValue: "0"
            - name: ProvTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvTypCdSk
              initValue: "0"
            - name: ProvCapPaymtEftMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvCapPaymtEftMethCdSk
              initValue: "0"
            - name: ProvClmPaymtEftMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvClmPaymtEftMethCdSk
              initValue: "0"
            - name: ProvClmPaymtMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvClmPaymtMethCdSk
              initValue: "0"
            - name: PaymtHoldDtSk
              type: decimal
              width: 18
              precision: 4
              map: PaymtHoldDtSk
              initValue: "0"
            - name: ProvBillSvcSk
              type: decimal
              width: 18
              precision: 4
              map: ProvBillSvcSk
              initValue: "0"
            - name: TermDtSk
              type: decimal
              width: 18
              precision: 4
              map: TermDtSk
              initValue: "0"
            - name: CmnPrtFacetsSK
              type: decimal
              width: 0
              map: CmnPrtFacetsSK
              initValue: "0"
            - name: CmnPrtVcacSK
              type: decimal
              width: 18
              precision: 4
              map: CmnPrtVcacSK
              initValue: "0"
            - name: CmnPrctSk
              type: decimal
              width: 18
              precision: 4
              map: CmnPrctSk
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: ErrCount
              initValue: "0"
            - name: svNtnlProvSK
              type: decimal
              width: 0
              map: svNtnlProvSK
              initValue: "0"
        - name: ForeignKey1.V0S20P3
          type: filter
          source: ForeignKey1.V0S20P3.map
          filter: "ErrCount = 0 Or PassThru = 'Y'"
          schema: 
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ID
              type: char
              width: 12
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_BILL_SVC_SK
              type: int
              width: 4
              nullable: N
            - name: REL_GRP_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: REL_IPA_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CAP_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CLM_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CLM_PAYMT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ENTY_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_FCLTY_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_PRCTC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_SVC_CAT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_TERM_RSN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PAYMT_HOLD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: CLRNGHSE_ID
              type: char
              width: 30
              nullable: N
            - name: EDI_DEST_ID
              type: char
              width: 15
              nullable: N
            - name: EDI_DEST_QLFR_TX
              type: char
              width: 2
              nullable: N
            - name: NTNL_PROV_ID
              type: char
              width: 10
              nullable: N
            - name: PROV_ADDR_ID
              type: char
              width: 12
              nullable: N
            - name: PROV_NM
              type: varchar
              width: 75
            - name: TAX_ID
              type: char
              width: 9
            - name: PROV_FCLTY_TYP_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_SPEC_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_TYP_CD
              type: char
              width: 4
              nullable: N
            - name: NTNL_PROV_SK
              type: int
              width: 4
            - name: TXNMY_CD
              type: varchar
              width: 20
              nullable: N
          keys:
            - PROV_SK
        - name: ForeignKey1.V0S20P4.map
          type: map
          source: IdsProvExtr
          alias: lnkProv1
          stageVars: 
            - name: SrcSysCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"IDS\", lnkProv1.PROV_SK, \"SOURCE SYSTEM\", lnkProv1.SRC_SYS_CD, Logging)"
              initValue: "0"
            - name: RelGrpProvSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyProv(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, lnkProv1.REL_GRP_PROV, Logging)"
              initValue: "0"
            - name: RelIpaProvSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyProv(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, lnkProv1.REL_IPA_PROV, Logging)"
              initValue: "0"
            - name: ProvFcltyTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyFcltyTyp(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, lnkProv1.PROV_FCLTY_TYP_CD, Logging)"
              initValue: "0"
            - name: ProvPrctcTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER PRACTICE TYPE\", lnkProv1.PROV_PRCTC_TYP_CD, Logging)"
              initValue: "0"
            - name: ProvSvcCatCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER SERVICE CATEGORY\", lnkProv1.PROV_SVC_CAT_CD, Logging)"
              initValue: "0"
            - name: ProvEntyCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER ENTITY\", lnkProv1.PROV_ENTY_CD, Logging)"
              initValue: "0"
            - name: ProvSpecCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyProvSpec(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, lnkProv1.PROV_SPEC_CD, Logging)"
              initValue: "0"
            - name: ProvSttusCdSK
              type: decimal
              width: 0
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER STATUS\", lnkProv1.PROV_STTUS_CD, Logging)"
              initValue: "0"
            - name: ProvTermRsnCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER TERMINATION REASON\", lnkProv1.PROV_TERM_RSN_CD, Logging)"
              initValue: "0"
            - name: ProvTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyProvTyp(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, lnkProv1.PROV_TYP_CD, Logging)"
              initValue: "0"
            - name: ProvCapPaymtEftMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER CAPITATION PAYMENT EFT METHOD\", lnkProv1.PROV_CAP_PAYMT_EFT_METH_CD, Logging)"
              initValue: "0"
            - name: ProvClmPaymtEftMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER CLAIM PAYMENT EFT METHOD\", lnkProv1.PROV_CLM_PAYMT_EFT_METH_CD, Logging)"
              initValue: "0"
            - name: ProvClmPaymtMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER CLAIM PAYMENT METHOD\", lnkProv1.PROV_CLM_PAYMT_METH_CD, Logging)"
              initValue: "0"
            - name: PaymtHoldDtSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate(\"IDS\", lnkProv1.PROV_SK, lnkProv1.PAYMT_HOLD_DT, Logging)"
              initValue: "0"
            - name: ProvBillSvcSk
              type: decimal
              width: 18
              precision: 4
              map: " If lnkProv1.PROV_BILL_SVC_SK = \"NA\" then 1 else lnkProv1.PROV_BILL_SVC_SK"
              initValue: "0"
            - name: TermDtSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate(\"IDS\", lnkProv1.PROV_SK, lnkProv1.TERM_DT, Logging)"
              initValue: "0"
            - name: CmnPrtFacetsSK
              type: decimal
              width: 0
              map: "GetFkeyCmnPrct('FACETS', lnkProv1.PROV_SK, lnkProv1.CMN_PRCT, 'X')"
              initValue: "0"
            - name: CmnPrtVcacSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCmnPrct('VCAC', lnkProv1.PROV_SK, lnkProv1.CMN_PRCT, 'X')"
              initValue: "0"
            - name: CmnPrctSk
              type: decimal
              width: 18
              precision: 4
              map: " If len(TRIM(lnkProv1.CMN_PRCT)) = 0 Then 1 Else  If CmnPrtFacetsSK = 0 then CmnPrtVcacSK else CmnPrtFacetsSK"
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: "lnkProv1.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(lnkProv1.PROV_SK)"
              initValue: "0"
            - name: svNtnlProvSK
              type: decimal
              width: 0
              map: "GetFkeyNtnlProvId('CMS', lnkProv1.PROV_SK, lnkProv1.NTNL_PROV_ID, Logging)"
              initValue: "0"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "GetRecycleKey(lnkProv1.PROV_SK)"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "lnkProv1.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "lnkProv1.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: ErrCount
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "lnkProv1.RECYCLE_CT + 1"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnkProv1.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "lnkProv1.PRI_KEY_STRING"
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnkProv1.PROV_SK"
            - name: PROV_ID
              type: char
              width: 12
              nullable: N
              map: "lnkProv1.PROV_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnkProv1.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnkProv1.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: CMN_PRCT
              type: char
              width: 12
              nullable: N
              map: "lnkProv1.CMN_PRCT"
            - name: PROV_BILL_SVC_SK
              type: char
              width: 10
              nullable: N
              map: "lnkProv1.PROV_BILL_SVC_SK"
            - name: REL_GRP_PROV
              type: char
              width: 12
              nullable: N
              map: "lnkProv1.REL_GRP_PROV"
            - name: REL_IPA_PROV
              type: char
              width: 12
              nullable: N
              map: "lnkProv1.REL_IPA_PROV"
            - name: PROV_CAP_PAYMT_EFT_METH_CD
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PROV_CAP_PAYMT_EFT_METH_CD"
            - name: PROV_CLM_PAYMT_EFT_METH_CD
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PROV_CLM_PAYMT_EFT_METH_CD"
            - name: PROV_CLM_PAYMT_METH_CD
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PROV_CLM_PAYMT_METH_CD"
            - name: PROV_ENTY_CD
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PROV_ENTY_CD"
            - name: PROV_FCLTY_TYP_CD
              type: char
              width: 4
              nullable: N
              map: "lnkProv1.PROV_FCLTY_TYP_CD"
            - name: PROV_PRCTC_TYP_CD
              type: char
              width: 4
              nullable: N
              map: "lnkProv1.PROV_PRCTC_TYP_CD"
            - name: PROV_SVC_CAT_CD
              type: char
              width: 4
              nullable: N
              map: "lnkProv1.PROV_SVC_CAT_CD"
            - name: PROV_SPEC_CD
              type: char
              width: 4
              nullable: N
              map: "lnkProv1.PROV_SPEC_CD"
            - name: PROV_STTUS_CD
              type: char
              width: 2
              nullable: N
              map: "lnkProv1.PROV_STTUS_CD"
            - name: PROV_TERM_RSN_CD
              type: char
              width: 4
              nullable: N
              map: "lnkProv1.PROV_TERM_RSN_CD"
            - name: PROV_TYP_CD
              type: char
              width: 4
              nullable: N
              map: "lnkProv1.PROV_TYP_CD"
            - name: TERM_DT
              type: timestamp
              nullable: N
              map: "lnkProv1.TERM_DT"
            - name: PAYMT_HOLD_DT
              type: timestamp
              nullable: N
              map: "lnkProv1.PAYMT_HOLD_DT"
            - name: CLRNGHOUSE_ID
              type: char
              width: 30
              nullable: N
              map: "lnkProv1.CLRNGHOUSE_ID"
            - name: EDI_DEST_ID
              type: char
              width: 15
              nullable: N
              map: "lnkProv1.EDI_DEST_ID"
            - name: EDI_DEST_QUAL
              type: char
              width: 2
              nullable: N
              map: "lnkProv1.EDI_DEST_QUAL"
            - name: NTNL_PROV_ID
              type: char
              width: 10
              nullable: N
              map: "lnkProv1.NTNL_PROV_ID"
            - name: PROV_ADDR_ID
              type: char
              width: 12
              nullable: N
              map: "lnkProv1.PROV_ADDR_ID"
            - name: PROV_NM
              type: varchar
              width: 75
              map: "lnkProv1.PROV_NM"
            - name: TAX_ID
              type: char
              width: 9
              map: "lnkProv1.TAX_ID"
            - name: TXNMY_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnkProv1.TXNMY_CD"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnkProv1.SRC_SYS_CD"
            - name: CMN_PRCT_ID
              type: char
              width: 12
              nullable: N
              map: "lnkProv1.CMN_PRCT"
            - name: REL_GRP_PROV_ID
              type: char
              width: 12
              nullable: N
              map: "lnkProv1.REL_GRP_PROV"
            - name: REL_IPA_PROV_ID
              type: char
              width: 12
              nullable: N
              map: "lnkProv1.REL_IPA_PROV"
            - name: PROV_CAP_PAYMT_EFT_METH_CD_SK
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PROV_CAP_PAYMT_EFT_METH_CD"
            - name: PROV_CLM_PAYMT_EFT_METH_CD_SK
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PROV_CLM_PAYMT_EFT_METH_CD"
            - name: PROV_CLM_PAYMT_METH_CD_SK
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PROV_CLM_PAYMT_METH_CD"
            - name: PROV_FCLTY_TYPE_CD
              type: char
              width: 4
              nullable: N
              map: "lnkProv1.PROV_FCLTY_TYP_CD"
            - name: PROV_TYPE_CD
              type: char
              width: 4
              nullable: N
              map: "lnkProv1.PROV_TYP_CD"
            - name: PAYMT_HOLD_DT_SK
              type: timestamp
              nullable: N
              map: "lnkProv1.PAYMT_HOLD_DT"
            - name: CLRNGHSE_ID
              type: char
              width: 30
              nullable: N
              map: "lnkProv1.CLRNGHOUSE_ID"
            - name: EDI_DEST_QLFR_TX
              type: char
              width: 2
              nullable: N
              map: "lnkProv1.EDI_DEST_QUAL"
            - name: SrcSysCdSk
              type: decimal
              width: 18
              precision: 4
              map: SrcSysCdSk
              initValue: "0"
            - name: RelGrpProvSk
              type: decimal
              width: 18
              precision: 4
              map: RelGrpProvSk
              initValue: "0"
            - name: RelIpaProvSk
              type: decimal
              width: 18
              precision: 4
              map: RelIpaProvSk
              initValue: "0"
            - name: ProvFcltyTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvFcltyTypCdSk
              initValue: "0"
            - name: ProvPrctcTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvPrctcTypCdSk
              initValue: "0"
            - name: ProvSvcCatCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvSvcCatCdSk
              initValue: "0"
            - name: ProvEntyCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvEntyCdSk
              initValue: "0"
            - name: ProvSpecCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvSpecCdSk
              initValue: "0"
            - name: ProvSttusCdSK
              type: decimal
              width: 0
              map: ProvSttusCdSK
              initValue: "0"
            - name: ProvTermRsnCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvTermRsnCdSk
              initValue: "0"
            - name: ProvTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvTypCdSk
              initValue: "0"
            - name: ProvCapPaymtEftMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvCapPaymtEftMethCdSk
              initValue: "0"
            - name: ProvClmPaymtEftMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvClmPaymtEftMethCdSk
              initValue: "0"
            - name: ProvClmPaymtMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvClmPaymtMethCdSk
              initValue: "0"
            - name: PaymtHoldDtSk
              type: decimal
              width: 18
              precision: 4
              map: PaymtHoldDtSk
              initValue: "0"
            - name: ProvBillSvcSk
              type: decimal
              width: 18
              precision: 4
              map: ProvBillSvcSk
              initValue: "0"
            - name: TermDtSk
              type: decimal
              width: 18
              precision: 4
              map: TermDtSk
              initValue: "0"
            - name: CmnPrtFacetsSK
              type: decimal
              width: 0
              map: CmnPrtFacetsSK
              initValue: "0"
            - name: CmnPrtVcacSK
              type: decimal
              width: 18
              precision: 4
              map: CmnPrtVcacSK
              initValue: "0"
            - name: CmnPrctSk
              type: decimal
              width: 18
              precision: 4
              map: CmnPrctSk
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: ErrCount
              initValue: "0"
            - name: svNtnlProvSK
              type: decimal
              width: 0
              map: svNtnlProvSK
              initValue: "0"
        - name: ForeignKey1.V0S20P4
          type: filter
          source: ForeignKey1.V0S20P4.map
          filter: "ErrCount > 0"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ID
              type: char
              width: 12
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: CMN_PRCT_ID
              type: char
              width: 12
              nullable: N
            - name: PROV_BILL_SVC_SK
              type: char
              width: 10
              nullable: N
            - name: REL_GRP_PROV_ID
              type: char
              width: 12
              nullable: N
            - name: REL_IPA_PROV_ID
              type: char
              width: 12
              nullable: N
            - name: PROV_CAP_PAYMT_EFT_METH_CD_SK
              type: char
              width: 1
              nullable: N
            - name: PROV_CLM_PAYMT_EFT_METH_CD_SK
              type: char
              width: 1
              nullable: N
            - name: PROV_CLM_PAYMT_METH_CD_SK
              type: char
              width: 1
              nullable: N
            - name: PROV_ENTY_CD
              type: char
              width: 1
              nullable: N
            - name: PROV_FCLTY_TYPE_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_PRCTC_TYP_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_SVC_CAT_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_SPEC_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_STTUS_CD
              type: char
              width: 2
              nullable: N
            - name: PROV_TERM_RSN_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_TYPE_CD
              type: char
              width: 4
              nullable: N
            - name: TERM_DT
              type: timestamp
              nullable: N
            - name: PAYMT_HOLD_DT_SK
              type: timestamp
              nullable: N
            - name: CLRNGHSE_ID
              type: char
              width: 30
              nullable: N
            - name: EDI_DEST_ID
              type: char
              width: 15
              nullable: N
            - name: EDI_DEST_QLFR_TX
              type: char
              width: 2
              nullable: N
            - name: NTNL_PROV_ID
              type: char
              width: 10
              nullable: N
            - name: PROV_ADDR_ID
              type: char
              width: 12
              nullable: N
            - name: PROV_NM
              type: varchar
              width: 75
            - name: TAX_ID
              type: char
              width: 9
            - name: TXNMY_CD
              type: varchar
              width: 20
              nullable: N
          keys:
            - JOB_EXCTN_RCRD_ERR_SK
        - name: ForeignKey1.V0S20P6.map
          type: map
          source: IdsProvExtr
          alias: lnkProv1
          stageVars: 
            - name: SrcSysCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"IDS\", lnkProv1.PROV_SK, \"SOURCE SYSTEM\", lnkProv1.SRC_SYS_CD, Logging)"
              initValue: "0"
            - name: RelGrpProvSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyProv(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, lnkProv1.REL_GRP_PROV, Logging)"
              initValue: "0"
            - name: RelIpaProvSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyProv(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, lnkProv1.REL_IPA_PROV, Logging)"
              initValue: "0"
            - name: ProvFcltyTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyFcltyTyp(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, lnkProv1.PROV_FCLTY_TYP_CD, Logging)"
              initValue: "0"
            - name: ProvPrctcTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER PRACTICE TYPE\", lnkProv1.PROV_PRCTC_TYP_CD, Logging)"
              initValue: "0"
            - name: ProvSvcCatCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER SERVICE CATEGORY\", lnkProv1.PROV_SVC_CAT_CD, Logging)"
              initValue: "0"
            - name: ProvEntyCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER ENTITY\", lnkProv1.PROV_ENTY_CD, Logging)"
              initValue: "0"
            - name: ProvSpecCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyProvSpec(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, lnkProv1.PROV_SPEC_CD, Logging)"
              initValue: "0"
            - name: ProvSttusCdSK
              type: decimal
              width: 0
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER STATUS\", lnkProv1.PROV_STTUS_CD, Logging)"
              initValue: "0"
            - name: ProvTermRsnCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER TERMINATION REASON\", lnkProv1.PROV_TERM_RSN_CD, Logging)"
              initValue: "0"
            - name: ProvTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyProvTyp(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, lnkProv1.PROV_TYP_CD, Logging)"
              initValue: "0"
            - name: ProvCapPaymtEftMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER CAPITATION PAYMENT EFT METHOD\", lnkProv1.PROV_CAP_PAYMT_EFT_METH_CD, Logging)"
              initValue: "0"
            - name: ProvClmPaymtEftMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER CLAIM PAYMENT EFT METHOD\", lnkProv1.PROV_CLM_PAYMT_EFT_METH_CD, Logging)"
              initValue: "0"
            - name: ProvClmPaymtMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER CLAIM PAYMENT METHOD\", lnkProv1.PROV_CLM_PAYMT_METH_CD, Logging)"
              initValue: "0"
            - name: PaymtHoldDtSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate(\"IDS\", lnkProv1.PROV_SK, lnkProv1.PAYMT_HOLD_DT, Logging)"
              initValue: "0"
            - name: ProvBillSvcSk
              type: decimal
              width: 18
              precision: 4
              map: " If lnkProv1.PROV_BILL_SVC_SK = \"NA\" then 1 else lnkProv1.PROV_BILL_SVC_SK"
              initValue: "0"
            - name: TermDtSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate(\"IDS\", lnkProv1.PROV_SK, lnkProv1.TERM_DT, Logging)"
              initValue: "0"
            - name: CmnPrtFacetsSK
              type: decimal
              width: 0
              map: "GetFkeyCmnPrct('FACETS', lnkProv1.PROV_SK, lnkProv1.CMN_PRCT, 'X')"
              initValue: "0"
            - name: CmnPrtVcacSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCmnPrct('VCAC', lnkProv1.PROV_SK, lnkProv1.CMN_PRCT, 'X')"
              initValue: "0"
            - name: CmnPrctSk
              type: decimal
              width: 18
              precision: 4
              map: " If len(TRIM(lnkProv1.CMN_PRCT)) = 0 Then 1 Else  If CmnPrtFacetsSK = 0 then CmnPrtVcacSK else CmnPrtFacetsSK"
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: "lnkProv1.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(lnkProv1.PROV_SK)"
              initValue: "0"
            - name: svNtnlProvSK
              type: decimal
              width: 0
              map: "GetFkeyNtnlProvId('CMS', lnkProv1.PROV_SK, lnkProv1.NTNL_PROV_ID, Logging)"
              initValue: "0"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "lnkProv1.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "lnkProv1.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "lnkProv1.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "lnkProv1.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "lnkProv1.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnkProv1.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "lnkProv1.PRI_KEY_STRING"
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PROV_ID
              type: char
              width: 12
              nullable: N
              map: "\"UNK\""
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CMN_PRCT
              type: char
              width: 12
              nullable: N
              map: "lnkProv1.CMN_PRCT"
            - name: PROV_BILL_SVC_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: REL_GRP_PROV
              type: char
              width: 12
              nullable: N
              map: "lnkProv1.REL_GRP_PROV"
            - name: REL_IPA_PROV
              type: char
              width: 12
              nullable: N
              map: "lnkProv1.REL_IPA_PROV"
            - name: PROV_CAP_PAYMT_EFT_METH_CD
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PROV_CAP_PAYMT_EFT_METH_CD"
            - name: PROV_CLM_PAYMT_EFT_METH_CD
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PROV_CLM_PAYMT_EFT_METH_CD"
            - name: PROV_CLM_PAYMT_METH_CD
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PROV_CLM_PAYMT_METH_CD"
            - name: PROV_ENTY_CD
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PROV_ENTY_CD"
            - name: PROV_FCLTY_TYP_CD
              type: char
              width: 4
              nullable: N
              map: "\"UNK\""
            - name: PROV_PRCTC_TYP_CD
              type: char
              width: 4
              nullable: N
              map: "lnkProv1.PROV_PRCTC_TYP_CD"
            - name: PROV_SVC_CAT_CD
              type: char
              width: 4
              nullable: N
              map: "lnkProv1.PROV_SVC_CAT_CD"
            - name: PROV_SPEC_CD
              type: char
              width: 4
              nullable: N
              map: "\"UNK\""
            - name: PROV_STTUS_CD
              type: char
              width: 2
              nullable: N
              map: "lnkProv1.PROV_STTUS_CD"
            - name: PROV_TERM_RSN_CD
              type: char
              width: 4
              nullable: N
              map: "lnkProv1.PROV_TERM_RSN_CD"
            - name: PROV_TYP_CD
              type: char
              width: 4
              nullable: N
              map: "\"UNK\""
            - name: TERM_DT
              type: timestamp
              nullable: N
              map: "lnkProv1.TERM_DT"
            - name: PAYMT_HOLD_DT
              type: timestamp
              nullable: N
              map: "lnkProv1.PAYMT_HOLD_DT"
            - name: CLRNGHOUSE_ID
              type: char
              width: 30
              nullable: N
              map: "lnkProv1.CLRNGHOUSE_ID"
            - name: EDI_DEST_ID
              type: char
              width: 15
              nullable: N
              map: "\"UNK\""
            - name: EDI_DEST_QUAL
              type: char
              width: 2
              nullable: N
              map: "lnkProv1.EDI_DEST_QUAL"
            - name: NTNL_PROV_ID
              type: char
              width: 10
              nullable: N
              map: "\"UNK\""
            - name: PROV_ADDR_ID
              type: char
              width: 12
              nullable: N
              map: "\"UNK\""
            - name: PROV_NM
              type: varchar
              width: 75
              map: "\"UNK\""
            - name: TAX_ID
              type: char
              width: 9
              map: "\"UNK\""
            - name: TXNMY_CD
              type: varchar
              width: 20
              nullable: N
              map: "'UNK'"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: REL_GRP_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: REL_IPA_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PROV_CAP_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PROV_CLM_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PROV_CLM_PAYMT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PROV_ENTY_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PROV_FCLTY_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PROV_PRCTC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PROV_SVC_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PROV_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PROV_TERM_RSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
              map: "\"1753-01-01\""
            - name: PAYMT_HOLD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "\"1753-01-01\""
            - name: CLRNGHSE_ID
              type: char
              width: 30
              nullable: N
              map: "\"UNK\""
            - name: EDI_DEST_QLFR_TX
              type: char
              width: 2
              nullable: N
              map: "\"UNK\""
            - name: NTNL_PROV_SK
              type: int
              width: 4
              map: "0"
            - name: SrcSysCdSk
              type: decimal
              width: 18
              precision: 4
              map: SrcSysCdSk
              initValue: "0"
            - name: RelGrpProvSk
              type: decimal
              width: 18
              precision: 4
              map: RelGrpProvSk
              initValue: "0"
            - name: RelIpaProvSk
              type: decimal
              width: 18
              precision: 4
              map: RelIpaProvSk
              initValue: "0"
            - name: ProvFcltyTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvFcltyTypCdSk
              initValue: "0"
            - name: ProvPrctcTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvPrctcTypCdSk
              initValue: "0"
            - name: ProvSvcCatCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvSvcCatCdSk
              initValue: "0"
            - name: ProvEntyCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvEntyCdSk
              initValue: "0"
            - name: ProvSpecCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvSpecCdSk
              initValue: "0"
            - name: ProvSttusCdSK
              type: decimal
              width: 0
              map: ProvSttusCdSK
              initValue: "0"
            - name: ProvTermRsnCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvTermRsnCdSk
              initValue: "0"
            - name: ProvTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvTypCdSk
              initValue: "0"
            - name: ProvCapPaymtEftMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvCapPaymtEftMethCdSk
              initValue: "0"
            - name: ProvClmPaymtEftMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvClmPaymtEftMethCdSk
              initValue: "0"
            - name: ProvClmPaymtMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvClmPaymtMethCdSk
              initValue: "0"
            - name: PaymtHoldDtSk
              type: decimal
              width: 18
              precision: 4
              map: PaymtHoldDtSk
              initValue: "0"
            - name: ProvBillSvcSk
              type: decimal
              width: 18
              precision: 4
              map: ProvBillSvcSk
              initValue: "0"
            - name: TermDtSk
              type: decimal
              width: 18
              precision: 4
              map: TermDtSk
              initValue: "0"
            - name: CmnPrtFacetsSK
              type: decimal
              width: 0
              map: CmnPrtFacetsSK
              initValue: "0"
            - name: CmnPrtVcacSK
              type: decimal
              width: 18
              precision: 4
              map: CmnPrtVcacSK
              initValue: "0"
            - name: CmnPrctSk
              type: decimal
              width: 18
              precision: 4
              map: CmnPrctSk
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: ErrCount
              initValue: "0"
            - name: svNtnlProvSK
              type: decimal
              width: 0
              map: svNtnlProvSK
              initValue: "0"
        - name: ForeignKey1.V0S20P6
          type: filter
          source: ForeignKey1.V0S20P6.map
          filter: "@INROWNUM = 1"
          schema: 
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ID
              type: char
              width: 12
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_BILL_SVC_SK
              type: int
              width: 4
              nullable: N
            - name: REL_GRP_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: REL_IPA_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CAP_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CLM_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CLM_PAYMT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ENTY_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_FCLTY_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_PRCTC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_SVC_CAT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_TERM_RSN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PAYMT_HOLD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: CLRNGHSE_ID
              type: char
              width: 30
              nullable: N
            - name: EDI_DEST_ID
              type: char
              width: 15
              nullable: N
            - name: EDI_DEST_QLFR_TX
              type: char
              width: 2
              nullable: N
            - name: NTNL_PROV_ID
              type: char
              width: 10
              nullable: N
            - name: PROV_ADDR_ID
              type: char
              width: 12
              nullable: N
            - name: PROV_NM
              type: varchar
              width: 75
            - name: TAX_ID
              type: char
              width: 9
            - name: PROV_FCLTY_TYP_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_SPEC_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_TYP_CD
              type: char
              width: 4
              nullable: N
            - name: NTNL_PROV_SK
              type: int
              width: 4
            - name: TXNMY_CD
              type: varchar
              width: 20
              nullable: N
          keys:
            - PROV_SK
        - name: ForeignKey1.V0S20P7.map
          type: map
          source: IdsProvExtr
          alias: lnkProv1
          stageVars: 
            - name: SrcSysCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"IDS\", lnkProv1.PROV_SK, \"SOURCE SYSTEM\", lnkProv1.SRC_SYS_CD, Logging)"
              initValue: "0"
            - name: RelGrpProvSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyProv(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, lnkProv1.REL_GRP_PROV, Logging)"
              initValue: "0"
            - name: RelIpaProvSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyProv(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, lnkProv1.REL_IPA_PROV, Logging)"
              initValue: "0"
            - name: ProvFcltyTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyFcltyTyp(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, lnkProv1.PROV_FCLTY_TYP_CD, Logging)"
              initValue: "0"
            - name: ProvPrctcTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER PRACTICE TYPE\", lnkProv1.PROV_PRCTC_TYP_CD, Logging)"
              initValue: "0"
            - name: ProvSvcCatCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER SERVICE CATEGORY\", lnkProv1.PROV_SVC_CAT_CD, Logging)"
              initValue: "0"
            - name: ProvEntyCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER ENTITY\", lnkProv1.PROV_ENTY_CD, Logging)"
              initValue: "0"
            - name: ProvSpecCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyProvSpec(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, lnkProv1.PROV_SPEC_CD, Logging)"
              initValue: "0"
            - name: ProvSttusCdSK
              type: decimal
              width: 0
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER STATUS\", lnkProv1.PROV_STTUS_CD, Logging)"
              initValue: "0"
            - name: ProvTermRsnCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER TERMINATION REASON\", lnkProv1.PROV_TERM_RSN_CD, Logging)"
              initValue: "0"
            - name: ProvTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyProvTyp(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, lnkProv1.PROV_TYP_CD, Logging)"
              initValue: "0"
            - name: ProvCapPaymtEftMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER CAPITATION PAYMENT EFT METHOD\", lnkProv1.PROV_CAP_PAYMT_EFT_METH_CD, Logging)"
              initValue: "0"
            - name: ProvClmPaymtEftMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER CLAIM PAYMENT EFT METHOD\", lnkProv1.PROV_CLM_PAYMT_EFT_METH_CD, Logging)"
              initValue: "0"
            - name: ProvClmPaymtMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(lnkProv1.SRC_SYS_CD, lnkProv1.PROV_SK, \"PROVIDER CLAIM PAYMENT METHOD\", lnkProv1.PROV_CLM_PAYMT_METH_CD, Logging)"
              initValue: "0"
            - name: PaymtHoldDtSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate(\"IDS\", lnkProv1.PROV_SK, lnkProv1.PAYMT_HOLD_DT, Logging)"
              initValue: "0"
            - name: ProvBillSvcSk
              type: decimal
              width: 18
              precision: 4
              map: " If lnkProv1.PROV_BILL_SVC_SK = \"NA\" then 1 else lnkProv1.PROV_BILL_SVC_SK"
              initValue: "0"
            - name: TermDtSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate(\"IDS\", lnkProv1.PROV_SK, lnkProv1.TERM_DT, Logging)"
              initValue: "0"
            - name: CmnPrtFacetsSK
              type: decimal
              width: 0
              map: "GetFkeyCmnPrct('FACETS', lnkProv1.PROV_SK, lnkProv1.CMN_PRCT, 'X')"
              initValue: "0"
            - name: CmnPrtVcacSK
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCmnPrct('VCAC', lnkProv1.PROV_SK, lnkProv1.CMN_PRCT, 'X')"
              initValue: "0"
            - name: CmnPrctSk
              type: decimal
              width: 18
              precision: 4
              map: " If len(TRIM(lnkProv1.CMN_PRCT)) = 0 Then 1 Else  If CmnPrtFacetsSK = 0 then CmnPrtVcacSK else CmnPrtFacetsSK"
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: "lnkProv1.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(lnkProv1.PROV_SK)"
              initValue: "0"
            - name: svNtnlProvSK
              type: decimal
              width: 0
              map: "GetFkeyNtnlProvId('CMS', lnkProv1.PROV_SK, lnkProv1.NTNL_PROV_ID, Logging)"
              initValue: "0"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "lnkProv1.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "lnkProv1.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "lnkProv1.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "lnkProv1.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "lnkProv1.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnkProv1.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "lnkProv1.PRI_KEY_STRING"
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: PROV_ID
              type: char
              width: 12
              nullable: N
              map: "\"NA\""
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CMN_PRCT
              type: char
              width: 12
              nullable: N
              map: "lnkProv1.CMN_PRCT"
            - name: PROV_BILL_SVC_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: REL_GRP_PROV
              type: char
              width: 12
              nullable: N
              map: "lnkProv1.REL_GRP_PROV"
            - name: REL_IPA_PROV
              type: char
              width: 12
              nullable: N
              map: "lnkProv1.REL_IPA_PROV"
            - name: PROV_CAP_PAYMT_EFT_METH_CD
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PROV_CAP_PAYMT_EFT_METH_CD"
            - name: PROV_CLM_PAYMT_EFT_METH_CD
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PROV_CLM_PAYMT_EFT_METH_CD"
            - name: PROV_CLM_PAYMT_METH_CD
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PROV_CLM_PAYMT_METH_CD"
            - name: PROV_ENTY_CD
              type: char
              width: 1
              nullable: N
              map: "lnkProv1.PROV_ENTY_CD"
            - name: PROV_FCLTY_TYP_CD
              type: char
              width: 4
              nullable: N
              map: "\"NA\""
            - name: PROV_PRCTC_TYP_CD
              type: char
              width: 4
              nullable: N
              map: "lnkProv1.PROV_PRCTC_TYP_CD"
            - name: PROV_SVC_CAT_CD
              type: char
              width: 4
              nullable: N
              map: "lnkProv1.PROV_SVC_CAT_CD"
            - name: PROV_SPEC_CD
              type: char
              width: 4
              nullable: N
              map: "\"NA\""
            - name: PROV_STTUS_CD
              type: char
              width: 2
              nullable: N
              map: "lnkProv1.PROV_STTUS_CD"
            - name: PROV_TERM_RSN_CD
              type: char
              width: 4
              nullable: N
              map: "lnkProv1.PROV_TERM_RSN_CD"
            - name: PROV_TYP_CD
              type: char
              width: 4
              nullable: N
              map: "\"NA\""
            - name: TERM_DT
              type: timestamp
              nullable: N
              map: "lnkProv1.TERM_DT"
            - name: PAYMT_HOLD_DT
              type: timestamp
              nullable: N
              map: "lnkProv1.PAYMT_HOLD_DT"
            - name: CLRNGHOUSE_ID
              type: char
              width: 30
              nullable: N
              map: "lnkProv1.CLRNGHOUSE_ID"
            - name: EDI_DEST_ID
              type: char
              width: 15
              nullable: N
              map: "\"NA\""
            - name: EDI_DEST_QUAL
              type: char
              width: 2
              nullable: N
              map: "lnkProv1.EDI_DEST_QUAL"
            - name: NTNL_PROV_ID
              type: char
              width: 10
              nullable: N
              map: "\"NA\""
            - name: PROV_ADDR_ID
              type: char
              width: 12
              nullable: N
              map: "\"NA\""
            - name: PROV_NM
              type: varchar
              width: 75
              map: "\"NA\""
            - name: TAX_ID
              type: char
              width: 9
              map: "\"NA\""
            - name: TXNMY_CD
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: REL_GRP_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: REL_IPA_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: PROV_CAP_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: PROV_CLM_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: PROV_CLM_PAYMT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: PROV_ENTY_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: PROV_FCLTY_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: PROV_PRCTC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: PROV_SVC_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: PROV_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: PROV_TERM_RSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
              map: "\"1753-01-01\""
            - name: PAYMT_HOLD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "\"1753-01-01\""
            - name: CLRNGHSE_ID
              type: char
              width: 30
              nullable: N
              map: "\"NA\""
            - name: EDI_DEST_QLFR_TX
              type: char
              width: 2
              nullable: N
              map: "\"NA\""
            - name: NTNL_PROV_SK
              type: int
              width: 4
              map: "1"
            - name: SrcSysCdSk
              type: decimal
              width: 18
              precision: 4
              map: SrcSysCdSk
              initValue: "0"
            - name: RelGrpProvSk
              type: decimal
              width: 18
              precision: 4
              map: RelGrpProvSk
              initValue: "0"
            - name: RelIpaProvSk
              type: decimal
              width: 18
              precision: 4
              map: RelIpaProvSk
              initValue: "0"
            - name: ProvFcltyTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvFcltyTypCdSk
              initValue: "0"
            - name: ProvPrctcTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvPrctcTypCdSk
              initValue: "0"
            - name: ProvSvcCatCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvSvcCatCdSk
              initValue: "0"
            - name: ProvEntyCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvEntyCdSk
              initValue: "0"
            - name: ProvSpecCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvSpecCdSk
              initValue: "0"
            - name: ProvSttusCdSK
              type: decimal
              width: 0
              map: ProvSttusCdSK
              initValue: "0"
            - name: ProvTermRsnCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvTermRsnCdSk
              initValue: "0"
            - name: ProvTypCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvTypCdSk
              initValue: "0"
            - name: ProvCapPaymtEftMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvCapPaymtEftMethCdSk
              initValue: "0"
            - name: ProvClmPaymtEftMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvClmPaymtEftMethCdSk
              initValue: "0"
            - name: ProvClmPaymtMethCdSk
              type: decimal
              width: 18
              precision: 4
              map: ProvClmPaymtMethCdSk
              initValue: "0"
            - name: PaymtHoldDtSk
              type: decimal
              width: 18
              precision: 4
              map: PaymtHoldDtSk
              initValue: "0"
            - name: ProvBillSvcSk
              type: decimal
              width: 18
              precision: 4
              map: ProvBillSvcSk
              initValue: "0"
            - name: TermDtSk
              type: decimal
              width: 18
              precision: 4
              map: TermDtSk
              initValue: "0"
            - name: CmnPrtFacetsSK
              type: decimal
              width: 0
              map: CmnPrtFacetsSK
              initValue: "0"
            - name: CmnPrtVcacSK
              type: decimal
              width: 18
              precision: 4
              map: CmnPrtVcacSK
              initValue: "0"
            - name: CmnPrctSk
              type: decimal
              width: 18
              precision: 4
              map: CmnPrctSk
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: ErrCount
              initValue: "0"
            - name: svNtnlProvSK
              type: decimal
              width: 0
              map: svNtnlProvSK
              initValue: "0"
        - name: ForeignKey1.V0S20P7
          type: filter
          source: ForeignKey1.V0S20P7.map
          filter: "@INROWNUM = 1"
          schema: 
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ID
              type: char
              width: 12
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_BILL_SVC_SK
              type: int
              width: 4
              nullable: N
            - name: REL_GRP_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: REL_IPA_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CAP_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CLM_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CLM_PAYMT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ENTY_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_FCLTY_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_PRCTC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_SVC_CAT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_TERM_RSN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PAYMT_HOLD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: CLRNGHSE_ID
              type: char
              width: 30
              nullable: N
            - name: EDI_DEST_ID
              type: char
              width: 15
              nullable: N
            - name: EDI_DEST_QLFR_TX
              type: char
              width: 2
              nullable: N
            - name: NTNL_PROV_ID
              type: char
              width: 10
              nullable: N
            - name: PROV_ADDR_ID
              type: char
              width: 12
              nullable: N
            - name: PROV_NM
              type: varchar
              width: 75
            - name: TAX_ID
              type: char
              width: 9
            - name: PROV_FCLTY_TYP_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_SPEC_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_TYP_CD
              type: char
              width: 4
              nullable: N
            - name: NTNL_PROV_SK
              type: int
              width: 4
            - name: TXNMY_CD
              type: varchar
              width: 20
              nullable: N
          keys:
            - PROV_SK
        - name: Transformer_95.V0S95P2
          type: map
          source: ProvTmp.DSLink96
          alias: DSLink96
          schema: 
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
              map: "DSLink96.PROV_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DSLink96.SRC_SYS_CD_SK"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "DSLink96.PROV_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "DSLink96.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "DSLink96.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
              map: "DSLink96.CMN_PRCT_SK"
            - name: PROV_BILL_SVC_SK
              type: int
              width: 4
              nullable: N
              map: "DSLink96.PROV_BILL_SVC_SK"
            - name: REL_GRP_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "DSLink96.REL_GRP_PROV_SK"
            - name: REL_IPA_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "DSLink96.REL_IPA_PROV_SK"
            - name: PROV_CAP_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DSLink96.PROV_CAP_PAYMT_EFT_METH_CD_SK"
            - name: PROV_CLM_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DSLink96.PROV_CLM_PAYMT_EFT_METH_CD_SK"
            - name: PROV_CLM_PAYMT_METH_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DSLink96.PROV_CLM_PAYMT_METH_CD_SK"
            - name: PROV_ENTY_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DSLink96.PROV_ENTY_CD_SK"
            - name: PROV_FCLTY_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DSLink96.PROV_FCLTY_TYP_CD_SK"
            - name: PROV_PRCTC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DSLink96.PROV_PRCTC_TYP_CD_SK"
            - name: PROV_SVC_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DSLink96.PROV_SVC_CAT_CD_SK"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DSLink96.PROV_SPEC_CD_SK"
            - name: PROV_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DSLink96.PROV_STTUS_CD_SK"
            - name: PROV_TERM_RSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DSLink96.PROV_TERM_RSN_CD_SK"
            - name: PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DSLink96.PROV_TYP_CD_SK"
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DSLink96.TERM_DT_SK"
            - name: PAYMT_HOLD_DT_SK
              type: char
              width: 10
              nullable: N
              map: "DSLink96.PAYMT_HOLD_DT_SK"
            - name: CLRNGHSE_ID
              type: varchar
              width: 40
              nullable: N
              map: "DSLink96.CLRNGHSE_ID"
            - name: EDI_DEST_ID
              type: varchar
              width: 20
              nullable: N
              map: "DSLink96.EDI_DEST_ID"
            - name: EDI_DEST_QLFR_TX
              type: varchar
              width: 20
              map: "DSLink96.EDI_DEST_QLFR_TX"
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "DSLink96.NTNL_PROV_ID"
            - name: PROV_ADDR_ID
              type: varchar
              width: 20
              nullable: N
              map: "DSLink96.PROV_ADDR_ID"
            - name: PROV_NM
              type: varchar
              width: 75
              map: "DSLink96.PROV_NM"
            - name: TAX_ID
              type: varchar
              width: 20
              nullable: N
              map: "DSLink96.TAX_ID"
            - name: NTNL_PROV_SK
              type: int
              width: 4
              map: "DSLink96.NTNL_PROV_SK"
            - name: TXNMY_CD
              type: varchar
              width: 20
              nullable: N
              map: "DSLink96.TXNMY_CD"
          keys:
            - PROV_SK
        - name: Transformer_95.V0S95P3
          type: map
          source: Transformer_95.V0S95P3.filter
          alias: DSLink96
          schema: 
            - name: PROV_FCLTY_TYP_CD
              type: char
              width: 4
              nullable: N
              map: "DSLink96.PROV_FCLTY_TYP_CD"
        - name: Transformer_95.V0S95P3.filter
          type: filter
          source: ProvTmp.DSLink96
          filter: "PROV_FCLTY_TYP_CD_SK = 0 and PROV_FCLTY_TYP_CD <> 'UNK'"
          schema: 
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ID
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
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_BILL_SVC_SK
              type: int
              width: 4
              nullable: N
            - name: REL_GRP_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: REL_IPA_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CAP_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CLM_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CLM_PAYMT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ENTY_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_FCLTY_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_PRCTC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_SVC_CAT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_TERM_RSN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PAYMT_HOLD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: CLRNGHSE_ID
              type: varchar
              width: 40
              nullable: N
            - name: EDI_DEST_ID
              type: varchar
              width: 20
              nullable: N
            - name: EDI_DEST_QLFR_TX
              type: varchar
              width: 20
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROV_ADDR_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROV_NM
              type: varchar
              width: 75
            - name: TAX_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROV_FCLTY_TYP_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_SPEC_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_TYP_CD
              type: char
              width: 4
              nullable: N
            - name: NTNL_PROV_SK
              type: int
              width: 4
            - name: TXNMY_CD
              type: varchar
              width: 20
              nullable: N
          keys:
            - PROV_SK
        - name: Transformer_95.V0S95P4
          type: map
          source: Transformer_95.V0S95P4.filter
          alias: DSLink96
          schema: 
            - name: PROV_SPEC_CD
              type: char
              width: 4
              nullable: N
              map: "DSLink96.PROV_SPEC_CD"
        - name: Transformer_95.V0S95P4.filter
          type: filter
          source: ProvTmp.DSLink96
          filter: "PROV_SPEC_CD_SK = 0 and PROV_SPEC_CD <> 'UNK'"
          schema: 
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ID
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
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_BILL_SVC_SK
              type: int
              width: 4
              nullable: N
            - name: REL_GRP_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: REL_IPA_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CAP_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CLM_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CLM_PAYMT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ENTY_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_FCLTY_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_PRCTC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_SVC_CAT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_TERM_RSN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PAYMT_HOLD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: CLRNGHSE_ID
              type: varchar
              width: 40
              nullable: N
            - name: EDI_DEST_ID
              type: varchar
              width: 20
              nullable: N
            - name: EDI_DEST_QLFR_TX
              type: varchar
              width: 20
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROV_ADDR_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROV_NM
              type: varchar
              width: 75
            - name: TAX_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROV_FCLTY_TYP_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_SPEC_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_TYP_CD
              type: char
              width: 4
              nullable: N
            - name: NTNL_PROV_SK
              type: int
              width: 4
            - name: TXNMY_CD
              type: varchar
              width: 20
              nullable: N
          keys:
            - PROV_SK
        - name: Transformer_95.V0S95P5
          type: map
          source: Transformer_95.V0S95P5.filter
          alias: DSLink96
          schema: 
            - name: PROV_TYP_CD
              type: char
              width: 4
              nullable: N
              map: "DSLink96.PROV_TYP_CD"
        - name: Transformer_95.V0S95P5.filter
          type: filter
          source: ProvTmp.DSLink96
          filter: "PROV_TYP_CD_SK = 0 and PROV_TYP_CD <> 'UNK'"
          schema: 
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ID
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
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_BILL_SVC_SK
              type: int
              width: 4
              nullable: N
            - name: REL_GRP_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: REL_IPA_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CAP_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CLM_PAYMT_EFT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_CLM_PAYMT_METH_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ENTY_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_FCLTY_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_PRCTC_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_SVC_CAT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_TERM_RSN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PAYMT_HOLD_DT_SK
              type: char
              width: 10
              nullable: N
            - name: CLRNGHSE_ID
              type: varchar
              width: 40
              nullable: N
            - name: EDI_DEST_ID
              type: varchar
              width: 20
              nullable: N
            - name: EDI_DEST_QLFR_TX
              type: varchar
              width: 20
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROV_ADDR_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROV_NM
              type: varchar
              width: 75
            - name: TAX_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROV_FCLTY_TYP_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_SPEC_CD
              type: char
              width: 4
              nullable: N
            - name: PROV_TYP_CD
              type: char
              width: 4
              nullable: N
            - name: NTNL_PROV_SK
              type: int
              width: 4
            - name: TXNMY_CD
              type: varchar
              width: 20
              nullable: N
          keys:
            - PROV_SK
        - name: Collector
          type: unknown
          source: UnkSpecCdUnkProvTypProvTmp.LoadFileProvUnkFcltyTyphf_recycle
          alias: 
          origType: CCollector


