model:
  config:
    origin: datastage
  jobs:
    - name: IdsClmRemitHistFkey
      origin: datastage
      type: dataflow
      folder: "claim/RemitHist"
      runAfterSubrOnJobSuccess: enabled
      description: "IDS Claim Remit History Foreign Keying. "
      fullDescription: "* VC LOGS *\n^1_7 09/22/09 15:32:09 Batch  15241_56279 PROMOTE bckcetl:31540 ids20 dsadm bls for sa\n^1_7 09/22/09 15:22:02 Batch  15241_55325 INIT bckcett:31540 testIDSnew dsadm bls for sa\n^1_5 09/22/09 09:19:44 Batch  15241_33586 INIT bckcett:31540 testIDSnew dsadm bls for sa\n^1_2 07/06/09 11:52:09 Batch  15163_42760 PROMOTE bckcett:31540 testIDSnew u150906 3833-RemitAlternateCharge_Sharon_testIDSnew             Maddy\n^1_2 07/06/09 11:37:10 Batch  15163_41931 INIT bckcett:31540 devlIDSnew u150906 3833-RemitAlternateCharge_Sharon_devlIDSnew                  Maddy\n^1_1 06/29/09 11:44:53 Batch  15156_42345 INIT bckcett:31540 devlIDSnew u150906 3833-RemitAlternateChargeIDS_Sharon_devlIDSnew              Maddy\n^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS\n^1_4 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm\n^1_3 02/02/09 09:52:49 Batch  15009_35578 PROMOTE bckcetl ids20 dsadm bls for brent\n^1_3 01/26/09 13:03:15 Batch  15002_46999 INIT bckcett testIDS dsadm bls for bl\n^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying\n^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy\n^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm\n^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm\n^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm\n^1_2 08/30/07 07:19:35 Batch  14487_26382 INIT bckcetl ids20 dsadm dsadm\n^1_1 08/27/07 10:06:05 Batch  14484_36371 INIT bckcetl ids20 dsadm dsadm\n^1_3 03/26/07 12:34:37 Batch  14330_45286 INIT bckcetl ids20 dsadm dsadm\n^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm\n^1_2 03/07/07 15:24:05 Batch  14311_55447 INIT bckcetl ids20 dcg01 sa\n^1_8 12/18/06 15:24:00 Batch  14232_55459 PROMOTE bckcetl ids20 dsadm Keith for Ralph install 12/18/2006\n^1_8 12/18/06 14:39:27 Batch  14232_52797 INIT bckcett testIDS30 dsadm Keith for Ralph CDHP Install 12182006\n^1_1 11/17/06 13:46:03 Batch  14201_49568 PROMOTE bckcett testIDS30 u06640 Ralph\n^1_1 11/17/06 11:12:19 Batch  14201_40344 INIT bckcett devlIDS30 u06640 Ralph\n^1_3 09/29/06 07:52:17 Batch  14152_28340 INIT bckcett devlIDS30 u03651 steffy\n^1_2 08/22/06 09:38:57 Batch  14114_34741 INIT bckcett devlIDS30 u08717 Brent\n^1_6 04/17/06 10:35:58 Batch  13987_38166 PROMOTE bckcetl ids20 dsadm J. mahaffey\n^1_6 04/17/06 10:24:16 Batch  13987_37468 INIT bckcett testIDS30 dsadm J. Mahaffey for B. Leland\n^1_2 04/13/06 08:47:39 Batch  13983_31666 PROMOTE bckcett testIDS30 u08717 Brent\n^1_2 04/13/06 07:36:31 Batch  13983_27397 INIT bckcett devlIDS30 u08717 Brent\n^1_4 03/29/06 15:32:39 Batch  13968_55966 INIT bckcett devlIDS30 dsadm Brent\n^1_3 03/29/06 14:54:04 Batch  13968_53650 INIT bckcett devlIDS30 dsadm Brent\n^1_1 03/29/06 05:18:35 Batch  13968_19121 INIT bckcett devlIDS30 u08717 Brent\n^1_2 03/20/06 13:57:04 Batch  13959_50244 INIT bckcett devlIDS30 dsadm Brent\n\n\n*****************************************************************************************************************************************************************************\nCOPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\n\nJOB NAME:     IdsClmRemitHistFkey\n\nDESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.\n      \n\nINPUTS: Primary Key output file.\n\x09\n  \nHASH FILES:     hf_recycle  \n\n\nTRANSFORMS:  \n                           \n\nPROCESSING:\nProduct key lookup - \n                GetFkeyProd( TRIM(  ClsPlnCrfIn.SRC_SYS_CD ),   ClsPlnCrfIn.CLS_PLN_SK,  TRIM (ClsPlnCrfIn.PROD_ID ),   \"Y\")\n\n\nOUTPUTS: Load File for remit history table.\n\nMODIFICATIONS:\n            Steph Goddard   04/2004  -   Originally Programmed\n            Brent Leland       09/10/2004  -  Added default rows for UNK and NA\n                                                           -  Added link partitioner\n            Steph Goddard   09/13/2005    Added calculated payment amount indicator\n            Steph Goddard   02/16/2006   sequencer changes\n            Brent Leland       08/07/2006    Added current run cycle to output records.  Without the current run cycle, error recycle records would have an old run cycle and would be removed in delete process.\n            Ralph Tucker    10/03/2006 Removed the following fields: CLM_REMIT_HIST-pAYMTOVRD_CD_SK\n                                                                                                          CHK_PD_DT_SK\n                                                                                                          CHK_NET_PAYMT_AMT\n                                                                                                          CHK_NO\n                                                                                                          CHK_SEQ_NO\n                                                                                                          CHK_PAYE_NM\n                                                                                                          CHK_PAYMT_REF_ID\n                                                                                                          CLM_REMIT_HIST_PAYMTMETH_CD_SK\n                                                             Added field:  PCA_PD_AMT          \n\n\nMODIFICATIONS:\nDeveloper                Date                 Project/Altiris #      Change Description                                                                                                Development Project      Code Reviewer          Date Reviewed       \n------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                         --------------------------------       -------------------------------   ----------------------------       \nBhoomi Dasari         2008-08-12      3567(Primary Key)    Added new parameter SrcSysCdSk and passing it directly from source                  devlIDS                          Steph Goddard         08/18/2008\nSAndrew                 2009-06-23      #3833 Remit Alt Chrg      Added two new fields to end of file and to shared container                             devlIDSnew                   Steph Goddard           07/01/2009\n                                                                                               ALT_CHRG_IN and ALT_CHRG_PROV_WRTOFF_AMT"
      parameters:
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
        - name: Source
          type: string
          description: "Source system"
          defaultType: expr
          defaultValue: FACETS
        - name: SrcSysCdSk
          type: integer
          description: "Source System Code Sk"
        - name: InFile
          type: string
          description: InFile
          defaultType: expr
          defaultValue: "IdsClmRemitHistPKey.RemitHistTmp.RUNID"
        - name: Logging
          type: string
          description: Logging
          defaultType: expr
          defaultValue: Y
      steps:
        - name: ClmRemitHistCrf
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
            - name: CLM_REMIT_HIST_SK
              type: int
              width: 4
              nullable: N
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
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: CALC_ACTL_PD_AMT_IN
              type: char
              width: 1
              nullable: N
            - name: SUPRESS_EOB_IN
              type: char
              width: 1
              nullable: N
            - name: SUPRESS_REMIT_IN
              type: char
              width: 1
              nullable: N
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ER_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INTRST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PCA_PAID_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALT_CHRG_IN
              type: char
              width: 1
              nullable: N
            - name: ALT_CHRG_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
        - name: ClmRemitHistFkey
          type: fileWriter
          urls: 
            - "#$FilePath#/load/CLM_REMIT_HIST.#Source#.dat"
          source: Collector
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
          source: "ForeignKey.V0S20P4"
            # Hashed File has no directory specified
        - name: hf_claim_recycle_keys
          type: dbWriter
          originalDb: hashedFile
          table: "/hf_claim_recycle_keys"
          tableAction: append
          source: "ForeignKey.V0S20P8"
            # Hashed File has no directory specified
        - name: ForeignKey.V0S20P3.map
          type: map
          source: ClmRemitHistCrf
          alias: Key
          stageVars: 
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyClm(Key.SRC_SYS_CD, Key.CLM_REMIT_HIST_SK, Key.CLM_ID, Logging)"
              initValue: "1"
            - name: PassThru
              type: decimal
              width: 0
              map: "Key.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyErrorCnt(Key.CLM_REMIT_HIST_SK)"
              initValue: "-1"
            - name: JobExecRecSK
              type: decimal
              width: 0
              map: " If ErrCount > 0 Then GetRecycleKey(Key.CLM_REMIT_HIST_SK) Else 0"
              initValue: "-1"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "Key.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "Key.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "Key.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "Key.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "Key.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "Key.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "Key.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Key.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "Key.PRI_KEY_STRING"
            - name: CLM_REMIT_HIST_SK
              type: int
              width: 4
              nullable: N
              map: "Key.CLM_REMIT_HIST_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: SrcSysCdSk
            - name: CLM_ID
              type: char
              width: 20
              nullable: N
              map: "Key.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Key.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Key.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ClmSk
            - name: CALC_ACTL_PD_AMT_IN
              type: char
              width: 1
              nullable: N
              map: "Key.CALC_ACTL_PD_AMT_IN"
            - name: SUPRESS_EOB_IN
              type: char
              width: 1
              nullable: N
              map: "Key.SUPRESS_EOB_IN"
            - name: SUPRESS_REMIT_IN
              type: char
              width: 1
              nullable: N
              map: "Key.SUPRESS_REMIT_IN"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.ACTL_PD_AMT"
            - name: COB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.COB_PD_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.COPAY_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.DEDCT_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.DSALW_AMT"
            - name: ER_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.ER_COPAY_AMT"
            - name: INTRST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.INTRST_AMT"
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.NO_RESP_AMT"
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.PATN_RESP_AMT"
            - name: WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.WRTOFF_AMT"
            - name: PCA_PAID_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.PCA_PAID_AMT"
            - name: ALT_CHRG_IN
              type: char
              width: 1
              nullable: N
              map: "Key.ALT_CHRG_IN"
            - name: ALT_CHRG_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.ALT_CHRG_PROV_WRTOFF_AMT"
            - name: PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.WRTOFF_AMT"
            - name: PCA_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.PCA_PAID_AMT"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: ClmSk
              initValue: "1"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 18
              precision: 4
              map: ErrCount
              initValue: "-1"
            - name: JobExecRecSK
              type: decimal
              width: 0
              map: JobExecRecSK
              initValue: "-1"
        - name: ForeignKey.V0S20P3
          type: filter
          source: ForeignKey.V0S20P3.map
          filter: "ErrCount > 0 Or PassThru = 'Y'"
          schema: 
            - name: CLM_REMIT_HIST_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: char
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
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: SUPRESS_EOB_IN
              type: char
              width: 1
              nullable: N
            - name: SUPRESS_REMIT_IN
              type: char
              width: 1
              nullable: N
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ER_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INTRST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CALC_ACTL_PD_AMT_IN
              type: char
              width: 1
              nullable: N
            - name: PCA_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALT_CHRG_IN
              type: char
              width: 1
              nullable: N
            - name: ALT_CHRG_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
          keys:
            - CLM_REMIT_HIST_SK
        - name: ForeignKey.V0S20P4.map
          type: map
          source: ClmRemitHistCrf
          alias: Key
          stageVars: 
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyClm(Key.SRC_SYS_CD, Key.CLM_REMIT_HIST_SK, Key.CLM_ID, Logging)"
              initValue: "1"
            - name: PassThru
              type: decimal
              width: 0
              map: "Key.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyErrorCnt(Key.CLM_REMIT_HIST_SK)"
              initValue: "-1"
            - name: JobExecRecSK
              type: decimal
              width: 0
              map: " If ErrCount > 0 Then GetRecycleKey(Key.CLM_REMIT_HIST_SK) Else 0"
              initValue: "-1"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: JobExecRecSK
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "Key.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "Key.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "Key.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "Key.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: ErrCount
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "Key.RECYCLE_CT + 1"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Key.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "Key.PRI_KEY_STRING"
            - name: CLM_REMIT_HIST_SK
              type: int
              width: 4
              nullable: N
              map: "Key.CLM_REMIT_HIST_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Key.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Key.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Key.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Key.CLM_SK"
            - name: CALC_ACTL_PD_AMT_IN
              type: char
              width: 1
              nullable: N
              map: "Key.CALC_ACTL_PD_AMT_IN"
            - name: SUPRESS_EOB_IN
              type: char
              width: 1
              nullable: N
              map: "Key.SUPRESS_EOB_IN"
            - name: SUPRESS_REMIT_IN
              type: char
              width: 1
              nullable: N
              map: "Key.SUPRESS_REMIT_IN"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.ACTL_PD_AMT"
            - name: COB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.COB_PD_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.COPAY_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.DEDCT_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.DSALW_AMT"
            - name: ER_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.ER_COPAY_AMT"
            - name: INTRST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.INTRST_AMT"
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.NO_RESP_AMT"
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.PATN_RESP_AMT"
            - name: WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.WRTOFF_AMT"
            - name: PCA_PAID_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.PCA_PAID_AMT"
            - name: ALT_CHRG_IN
              type: char
              width: 1
              nullable: N
              map: "Key.ALT_CHRG_IN"
            - name: ALT_CHRG_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.ALT_CHRG_PROV_WRTOFF_AMT"
            - name: PCA_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.PCA_PAID_AMT"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: ClmSk
              initValue: "1"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 18
              precision: 4
              map: ErrCount
              initValue: "-1"
            - name: JobExecRecSK
              type: decimal
              width: 0
              map: JobExecRecSK
              initValue: "-1"
        - name: ForeignKey.V0S20P4
          type: filter
          source: ForeignKey.V0S20P4.map
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
            - name: CLM_REMIT_HIST_SK
              type: int
              width: 4
              nullable: N
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
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: SUPRESS_EOB_IN
              type: char
              width: 1
              nullable: N
            - name: SUPRESS_REMIT_IN
              type: char
              width: 1
              nullable: N
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ER_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INTRST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CALC_ACTL_PD_AMT_IN
              type: char
              width: 1
              nullable: N
            - name: PCA_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALT_CHRG_IN
              type: char
              width: 1
              nullable: N
            - name: ALT_CHRG_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
          keys:
            - JOB_EXCTN_RCRD_ERR_SK
        - name: ForeignKey.V0S20P5.map
          type: map
          source: ClmRemitHistCrf
          alias: Key
          stageVars: 
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyClm(Key.SRC_SYS_CD, Key.CLM_REMIT_HIST_SK, Key.CLM_ID, Logging)"
              initValue: "1"
            - name: PassThru
              type: decimal
              width: 0
              map: "Key.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyErrorCnt(Key.CLM_REMIT_HIST_SK)"
              initValue: "-1"
            - name: JobExecRecSK
              type: decimal
              width: 0
              map: " If ErrCount > 0 Then GetRecycleKey(Key.CLM_REMIT_HIST_SK) Else 0"
              initValue: "-1"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "Key.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "Key.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "Key.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "Key.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "Key.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "Key.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "Key.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Key.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "Key.PRI_KEY_STRING"
            - name: CLM_REMIT_HIST_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_ID
              type: char
              width: 20
              nullable: N
              map: "'UNK'"
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
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CALC_ACTL_PD_AMT_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: SUPRESS_EOB_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: SUPRESS_REMIT_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: COB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: ER_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: INTRST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.WRTOFF_AMT"
            - name: PCA_PAID_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.PCA_PAID_AMT"
            - name: ALT_CHRG_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: ALT_CHRG_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: PCA_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: ClmSk
              initValue: "1"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 18
              precision: 4
              map: ErrCount
              initValue: "-1"
            - name: JobExecRecSK
              type: decimal
              width: 0
              map: JobExecRecSK
              initValue: "-1"
        - name: ForeignKey.V0S20P5
          type: filter
          source: ForeignKey.V0S20P5.map
          filter: "@INROWNUM = 1"
          schema: 
            - name: CLM_REMIT_HIST_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: char
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
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: SUPRESS_EOB_IN
              type: char
              width: 1
              nullable: N
            - name: SUPRESS_REMIT_IN
              type: char
              width: 1
              nullable: N
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ER_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INTRST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CALC_ACTL_PD_AMT_IN
              type: char
              width: 1
              nullable: N
            - name: PCA_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALT_CHRG_IN
              type: char
              width: 1
              nullable: N
            - name: ALT_CHRG_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
          keys:
            - CLM_REMIT_HIST_SK
        - name: ForeignKey.V0S20P6.map
          type: map
          source: ClmRemitHistCrf
          alias: Key
          stageVars: 
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyClm(Key.SRC_SYS_CD, Key.CLM_REMIT_HIST_SK, Key.CLM_ID, Logging)"
              initValue: "1"
            - name: PassThru
              type: decimal
              width: 0
              map: "Key.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyErrorCnt(Key.CLM_REMIT_HIST_SK)"
              initValue: "-1"
            - name: JobExecRecSK
              type: decimal
              width: 0
              map: " If ErrCount > 0 Then GetRecycleKey(Key.CLM_REMIT_HIST_SK) Else 0"
              initValue: "-1"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "Key.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "Key.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "Key.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "Key.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "Key.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "Key.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "Key.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Key.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "Key.PRI_KEY_STRING"
            - name: CLM_REMIT_HIST_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CLM_ID
              type: char
              width: 20
              nullable: N
              map: "'NA'"
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
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CALC_ACTL_PD_AMT_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: SUPRESS_EOB_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: SUPRESS_REMIT_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: COB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: ER_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: INTRST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.WRTOFF_AMT"
            - name: PCA_PAID_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.PCA_PAID_AMT"
            - name: ALT_CHRG_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: ALT_CHRG_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: PCA_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: ClmSk
              initValue: "1"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 18
              precision: 4
              map: ErrCount
              initValue: "-1"
            - name: JobExecRecSK
              type: decimal
              width: 0
              map: JobExecRecSK
              initValue: "-1"
        - name: ForeignKey.V0S20P6
          type: filter
          source: ForeignKey.V0S20P6.map
          filter: "@INROWNUM = 1"
          schema: 
            - name: CLM_REMIT_HIST_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: char
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
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: SUPRESS_EOB_IN
              type: char
              width: 1
              nullable: N
            - name: SUPRESS_REMIT_IN
              type: char
              width: 1
              nullable: N
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ER_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INTRST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CALC_ACTL_PD_AMT_IN
              type: char
              width: 1
              nullable: N
            - name: PCA_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALT_CHRG_IN
              type: char
              width: 1
              nullable: N
            - name: ALT_CHRG_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
          keys:
            - CLM_REMIT_HIST_SK
        - name: ForeignKey.V0S20P8.map
          type: map
          source: ClmRemitHistCrf
          alias: Key
          stageVars: 
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyClm(Key.SRC_SYS_CD, Key.CLM_REMIT_HIST_SK, Key.CLM_ID, Logging)"
              initValue: "1"
            - name: PassThru
              type: decimal
              width: 0
              map: "Key.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyErrorCnt(Key.CLM_REMIT_HIST_SK)"
              initValue: "-1"
            - name: JobExecRecSK
              type: decimal
              width: 0
              map: " If ErrCount > 0 Then GetRecycleKey(Key.CLM_REMIT_HIST_SK) Else 0"
              initValue: "-1"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "Key.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "Key.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "Key.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "Key.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "Key.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "Key.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "Key.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Key.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "Key.PRI_KEY_STRING"
            - name: CLM_REMIT_HIST_SK
              type: int
              width: 4
              nullable: N
              map: "Key.CLM_REMIT_HIST_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Key.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Key.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Key.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Key.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Key.CLM_SK"
            - name: CALC_ACTL_PD_AMT_IN
              type: char
              width: 1
              nullable: N
              map: "Key.CALC_ACTL_PD_AMT_IN"
            - name: SUPRESS_EOB_IN
              type: char
              width: 1
              nullable: N
              map: "Key.SUPRESS_EOB_IN"
            - name: SUPRESS_REMIT_IN
              type: char
              width: 1
              nullable: N
              map: "Key.SUPRESS_REMIT_IN"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.ACTL_PD_AMT"
            - name: COB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.COB_PD_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.COPAY_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.DEDCT_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.DSALW_AMT"
            - name: ER_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.ER_COPAY_AMT"
            - name: INTRST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.INTRST_AMT"
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.NO_RESP_AMT"
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.PATN_RESP_AMT"
            - name: WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.WRTOFF_AMT"
            - name: PCA_PAID_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.PCA_PAID_AMT"
            - name: ALT_CHRG_IN
              type: char
              width: 1
              nullable: N
              map: "Key.ALT_CHRG_IN"
            - name: ALT_CHRG_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Key.ALT_CHRG_PROV_WRTOFF_AMT"
            - name: ClmSk
              type: decimal
              width: 18
              precision: 4
              map: ClmSk
              initValue: "1"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 18
              precision: 4
              map: ErrCount
              initValue: "-1"
            - name: JobExecRecSK
              type: decimal
              width: 0
              map: JobExecRecSK
              initValue: "-1"
        - name: ForeignKey.V0S20P8
          type: filter
          source: ForeignKey.V0S20P8.map
          filter: "ErrCount > 0"
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
          keys:
            - SRC_SYS_CD
            - CLM_ID
        - name: Collector
          type: unknown
          source: ClmRemitHistFkeyhf_recyclehf_claim_recycle_keys
          alias: 
          origType: CCollector


