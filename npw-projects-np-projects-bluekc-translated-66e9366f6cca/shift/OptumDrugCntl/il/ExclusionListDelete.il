model:
  config:
    origin: datastage
  jobs:
    - name: ExclusionListDelete
      origin: datastage
      type: dataflow
      folder: "JobSequencer/CommonComponents"
      description: "Delete entries for a subject in DB2 - W_TASK_EXCL and UV - TASK_EXCL"
      fullDescription: "* VC LOGS *\n^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm\n^1_1 12/26/07 08:56:40 Batch  14605_32206 INIT bckcetl ids20 dsadm dsadm\n^1_1 11/21/07 15:09:17 Batch  14570_54560 INIT bckcetl ids20 dsadm dsadm\n^1_1 08/23/07 08:22:09 Batch  14480_30134 INIT bckcetl ids20 dsadm dsadm\n^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm\n^1_1 01/26/06 07:54:36 Batch  13906_28483 INIT bckcetl ids20 dsadm Gina\n^1_2 07/29/05 14:39:17 Batch  13725_52764 INIT bckcetl ids20 dsadm Brent\n^1_1 07/29/05 13:02:36 Batch  13725_46962 INIT bckcetl ids20 dsadm Brent\n^1_1 07/06/05 08:44:13 Batch  13702_31459 PROMOTE bckcetl ids20 dsadm Gina Parr\n^1_1 07/06/05 08:41:57 Batch  13702_31322 INIT bckcett testIDS30 dsadm Gina Parr\n^1_4 06/20/05 09:33:39 Batch  13686_34424 INIT bckcett devlIDS30 u08717 Brent\n^1_3 06/15/05 11:06:14 Batch  13681_39980 PROMOTE bckcett VERSION u08717 Brent\n^1_3 06/15/05 11:05:50 Batch  13681_39953 INIT bckcett devlIDS30 u08717 Brent\n^1_2 05/25/05 14:24:33 Batch  13660_51876 INIT bckcett devlIDS30 u08717 Brent\n^1_1 05/25/05 14:13:44 Batch  13660_51232 PROMOTE bckcett VERSION u08717 Brent\n^1_1 05/25/05 14:02:38 Batch  13660_50563 INIT bckccdt ids20 dsadm Brent\n\nCOPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\n\nJOB NAME:    ExclusionListDelete\n\n\nDESCRIPTION:   Remove entries in the DB2 - W_TASK_EXCL and Universe TASK_EXCL tables for a subject.\n      \n\nINPUTS:\x09 Subject name\n\n  \nHASH FILES:  TASK_EXCL\n\n\nTRANSFORMS:  None\n                        \n   \nPROCESSING:   Two rows are generated in the transform to initiate the delete in each environment.\n \n\nOUTPUTS:  None\n                    \n\n\nMODIFICATIONS:\n              B Leland           05/25/2005  -  Initial programming"
      parameters:
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: expr
          defaultValue: idsts2nw
        - name: IDSOwner
          type: string
          description: "IDS DB Owner"
          defaultType: expr
          defaultValue: db2test
        - name: IDSAcct
          type: string
          description: "IDS DB Account"
          defaultType: expr
          defaultValue: u08717
        - name: IDSPW
          type: encrypted
          description: "IDS DB Password"
          defaultType: expr
          defaultValue: "HM9@@KV@N9;L0G5I4HJ41KIFB5O"
        - name: Subject
          type: string
          description: Subject
          defaultType: expr
          defaultValue: TEST
      steps:
        - name: W_TASK_EXCL
          type: dbReader
          originalDb: db2
          query: "SELECT Distinct(TASK_SUBJ_NM) as TASK_SUBJ_NM FROM #$IDSOwner#.W_TASK_EXCL WHERE TASK_SUBJ_NM = '#Subject#'"
          table: "#$IDSOwner#.W_TASK_EXCL"
          tableAction: append
          schema: 
            - name: TASK_SUBJ_NM
              type: char
              width: 50
              nullable: N
        - name: DB2
          type: dbWriter
          originalDb: db2
          table: "#$IDSOwner#.W_TASK_EXCL"
          tableAction: append
          writeMode: delete
          source: "X_TRIGGER.V0S1P1"
        - name: X_TRIGGER.V0S1P1.map
          type: map
          source: W_TASK_EXCL
          alias: extract
          stageVars: 
            - name: TaskSubject
              type: decimal
              width: 0
              map: Subject
              initValue: "\"\""
          schema: 
            - name: TASK_SUBJ_NM
              type: char
              width: 50
              nullable: N
              map: TaskSubject
            - name: TaskSubject
              type: decimal
              width: 0
              map: TaskSubject
              initValue: "\"\""
        - name: X_TRIGGER.V0S1P1
          type: filter
          source: X_TRIGGER.V0S1P1.map
          filter: "IsNull(TASK_SUBJ_NM) = @FALSE"
          schema: 
            - name: TASK_SUBJ_NM
              type: char
              width: 50
              nullable: N
          keys:
            - TASK_SUBJ_NM
        - name: X_TRIGGER.V0S1P2.map
          type: map
          source: W_TASK_EXCL
          alias: extract
          stageVars: 
            - name: TaskSubject
              type: decimal
              width: 0
              map: Subject
              initValue: "\"\""
          schema: 
            - name: TASK_SUBJ_NM
              type: char
              width: 50
              nullable: N
              map: "extract.TASK_SUBJ_NM"
            - name: SUBJECT
              type: char
              width: 50
              nullable: N
              map: TaskSubject
            - name: TaskSubject
              type: decimal
              width: 0
              map: TaskSubject
              initValue: "\"\""
        - name: X_TRIGGER.V0S1P2
          type: filter
          source: X_TRIGGER.V0S1P2.map
          filter: "IsNull(TASK_SUBJ_NM) = @FALSE"
          schema: 
            - name: SUBJECT
              type: char
              width: 50
              nullable: N
          keys:
            - SUBJECT
        - name: task_excld
          type: unknown
          source: DB2task_excld
          origType: CUvStage


