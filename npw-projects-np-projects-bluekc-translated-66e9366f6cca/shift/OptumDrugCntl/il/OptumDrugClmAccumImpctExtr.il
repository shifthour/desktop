model:
  config:
    origin: datastage
  jobs:
    - name: OptumDrugClmAccumImpctExtr
      origin: datastage
      type: dataflow
      folder: "claim/claim/Optum"
      description: "Extract from Optum daily claims file for loading into table DRUG_CLM_ACCUM_IMPCT"
      fullDescription: "© Copyright 2020 Blue Cross/Blue Shield of Kansas City\n\nCALLED BY :  OptumDrugLandSeq\n \nDESCRIPTION:  Join extract file DRUG_CLM_ACCUM_IMPCT_prep.dat (created in job OptumClmLandExtr that extracts from the daily claims file) to table K_CLM (already created for table CLM in OptumDrugLandSeq) to get the surrogate key values for columns DRUG_CLM_SK and CRT_RUN_CYC_EXCTN_SK and write to file DRUG_CLM_ACCUM_IMPCT.dat.\n\nMODIFICATIONS:\n\nDeveloper                     Date               Project                 Change Description                                                                                                   Development Project    Code Reviewer               Date Reviewed\n--------------------------          --------------------    ----------------------      ------------------------------------------------------------------------------------------------------------------------------    ---------------------------------    ------------------------------------    ----------------------------              \nBill Schroeder              07/14/2023     US-586570          Original Programming                                                                                                 IntegrateDev2               Jeyaprasanna                  2023-08-14"
      parameters:
        - name: FilePath
          type: string
          description: "File Path"
          defaultType: project
        - name: SrcSysCd
          type: string
          description: SrcSysCd
        - name: SrcSysCdSK
          type: string
          description: SrcSysCdSK
        - name: CurrRunCycle
          type: string
          description: RunCycle
        - name: RunID
          type: string
          description: RunID
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSOwner
          type: string
          description: "IDS Table Owner"
          defaultType: project
        - name: IDSAcct
          type: string
          description: "IDS Account"
        - name: IDSPW
          type: encrypted
          description: "IDS Password"
      steps:
        - name: Lkp_Src_Cd
          type: lookup
          option:
            joinType: left
          sources: 
            - name: Lkp_K_CLM
              alias: Lnk_Accum1
              joinFilter: ""
              joinExpression: []
            - name: db2_CD_MPPNG
              alias: Ref_db2_CD_MPPNG
              joinFilter: ""
              joinExpression: 
              - fromExpr: Ref_db2_CD_MPPNG.SRC_CD
                toExpr: Lnk_Accum1.CLIENTDEF3
                caseInSensitive: false
          joinCondition: 
            - expression: Ref_db2_CD_MPPNG.SRC_CD = Lnk_Accum1.CLIENTDEF3
              caseInSensitive: false
          schema: 
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Accum1.DRUG_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Accum1.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_Accum1.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Accum1.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Accum1.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: SRC_CD
              type: varchar
              width: 20
              map: "Ref_db2_CD_MPPNG.SRC_CD"
            - name: TRGT_CD_NM
              type: varchar
              width: 255
              map: "Ref_db2_CD_MPPNG.TRGT_CD_NM"
            - name: CCAA_COUPON_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum1.CCAA_COUPON_AMT"
            - name: FMLY_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum1.FMLY_ACCUM_DEDCT_AMT"
            - name: FMLY_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum1.FMLY_ACCUM_OOP_AMT"
            - name: INDV_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum1.INDV_ACCUM_DEDCT_AMT"
            - name: INDV_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum1.INDV_ACCUM_OOP_AMT"
            - name: INDV_APLD_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum1.INDV_APLD_DEDCT_AMT"
            - name: INDV_APLD_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum1.INDV_APLD_OOP_AMT"
            - name: SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Ref_db2_CD_MPPNG.SRC_CD_SK"
          keys:
            - DRUG_CLM_SK
        - name: Lkp_K_CLM
          type: lookup
          option:
            joinType: inner
          sources: 
            - name: DRUG_CLM_ACCUM_IMPCT_prep
              alias: Lnk_Accum_extr
              joinFilter: ""
              joinExpression: []
            - name: db2_K_CLM
              alias: Ref_db2_K_CLM
              joinFilter: ""
              joinExpression: 
              - fromExpr: Ref_db2_K_CLM.SRC_SYS_CD_SK
                toExpr: Lnk_Accum_extr.SRC_SYS_CD_SK
                caseInSensitive: false
              - fromExpr: Ref_db2_K_CLM.CLM_ID
                toExpr: Lnk_Accum_extr.CLM_ID
                caseInSensitive: false
          joinCondition: 
            - expression: Ref_db2_K_CLM.SRC_SYS_CD_SK = Lnk_Accum_extr.SRC_SYS_CD_SK
              caseInSensitive: false
            - expression: Ref_db2_K_CLM.CLM_ID = Lnk_Accum_extr.CLM_ID
              caseInSensitive: false
          schema: 
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Ref_db2_K_CLM.CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Accum_extr.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_Accum_extr.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Ref_db2_K_CLM.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Accum_extr.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: CCAA_COUPON_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum_extr.CCAA_COUPON_AMT"
            - name: FMLY_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum_extr.FMLY_ACCUM_DEDCT_AMT"
            - name: FMLY_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum_extr.FMLY_ACCUM_OOP_AMT"
            - name: INDV_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum_extr.INDV_ACCUM_DEDCT_AMT"
            - name: INDV_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum_extr.INDV_ACCUM_OOP_AMT"
            - name: INDV_APLD_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum_extr.INDV_APLD_DEDCT_AMT"
            - name: INDV_APLD_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum_extr.INDV_APLD_OOP_AMT"
            - name: CLIENTDEF3
              type: varchar
              width: 10
              nullable: N
              map: "Lnk_Accum_extr.CLIENTDEF3"
          keys:
            - DRUG_CLM_SK
        - name: DRUG_CLM_ACCUM_IMPCT_prep
          type: fileReader
          urls: 
            - "#$FilePath#/load/DRUG_CLM_ACCUM_IMPCT_prep.dat"
          rejectMode: continue
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
            rowDelimiter: "\n"
            fixedLength: false
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: CLIENTDEF3
              type: varchar
              width: 10
              nullable: N
            - name: CCAA_COUPON_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: FMLY_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: FMLY_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INDV_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INDV_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INDV_APLD_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INDV_APLD_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
        - name: DRUG_CLM_ACCUM_IMPCT
          type: fileWriter
          urls: 
            - "#$FilePath#/load/DRUG_CLM_ACCUM_IMPCT.dat"
          source: Trans_Accum
          rejectMode: continue
          option: 
            updateMode: overwrite
            writeMode: SingleFile
          format: 
            header: false
            quotes: ""
            fieldDelimiter: ","
            rowDelimiter: "\n"
            fixedLength: false
        - name: db2_K_CLM
          type: dbReader
          originalDb: db2
          query: "SELECT SRC_SYS_CD_SK, CLM_ID, CRT_RUN_CYC_EXCTN_SK, CLM_SK\nFROM #$IDSOwner#.K_CLM where SRC_SYS_CD_SK=#SrcSysCdSK#"
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
          keys:
            - SRC_SYS_CD_SK
            - CLM_ID
        - name: db2_CD_MPPNG
          type: dbReader
          originalDb: db2
          query: "SELECT \nSRC_CD, TRGT_CD_NM ,CD_MPPNG_SK as SRC_CD_SK\n  FROM #$IDSOwner#.CD_MPPNG \n WHERE SRC_CLCTN_CD   = 'OPTUMRX' \n   AND SRC_SYS_CD     = 'OPTUMRX' \n   AND SRC_DOMAIN_NM  = 'NCP COUPON TYPE' \n   AND TRGT_CLCTN_CD  = 'IDS' \n   AND TRGT_DOMAIN_NM = 'NCP COUPON TYPE'\nunion\n SELECT \nSRC_CD, TRGT_CD_NM ,cast(CD_MPPNG_SK as integer) as SRC_CD_SK\n  FROM #$IDSOwner#.CD_MPPNG where  CD_MPPNG_SK='1'\nWITH UR;"
          schema: 
            - name: SRC_CD
              type: varchar
              width: 20
            - name: TRGT_CD_NM
              type: varchar
              width: 255
            - name: SRC_CD_SK
              type: int
              width: 4
              nullable: N
          keys:
            - SRC_CD
        - name: Trans_Accum
          type: map
          source: Lkp_Src_Cd
          alias: Lnk_Accum
          schema: 
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Accum.DRUG_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Accum.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_Accum.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Accum.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Accum.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: NCP_COUPON_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Trim(Lnk_Accum.SRC_CD_SK)"
            - name: CCAA_APLD_IN
              type: char
              width: 1
              nullable: N
              map: " If Trim(Lnk_Accum.SRC_CD) = '99' and Lnk_Accum.CCAA_COUPON_AMT > 0 Then 'Y' Else 'N'"
            - name: CCAA_COUPON_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum.CCAA_COUPON_AMT"
            - name: FMLY_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum.FMLY_ACCUM_DEDCT_AMT"
            - name: FMLY_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum.FMLY_ACCUM_OOP_AMT"
            - name: INDV_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum.INDV_ACCUM_DEDCT_AMT"
            - name: INDV_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum.INDV_ACCUM_OOP_AMT"
            - name: INDV_APLD_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum.INDV_APLD_DEDCT_AMT"
            - name: INDV_APLD_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum.INDV_APLD_OOP_AMT"
          keys:
            - DRUG_CLM_SK
            - SRC_SYS_CD_SK
            - CLM_ID


