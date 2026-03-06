-- OPTIMIZED LOAN QUERY WITH PERFORMANCE IMPROVEMENTS
-- Key optimizations:
-- 1. Using CTEs for better readability and performance
-- 2. Simplified CASE statements
-- 3. Proper indexing suggestions
-- 4. Reduced subqueries
-- 5. Optimized JOIN conditions

WITH LoanBase AS (
    -- Base loan data with initial filters
    SELECT 
        SRV.LOAN_NBR_NUM AS LoanId,
        ISNULL(EC.evalid, 0) AS EvalId,
        CONVERT(DATE, CAST(SRV.ENTER_DATE AS VARCHAR(8)), 110) AS DateModification,
        ISNULL(EC.STATUS, 'LSAMS') AS ModificationType,
        SR.smx220,
        SR.smm030,
        SR.smm040,
        SR.smm050,
        SR.SMX060 AS DaysToLateCharge
    FROM LSAMS.dbo.SRVCOLI SRV WITH (NOLOCK)
    INNER JOIN asgn.LoanData LN WITH (NOLOCK) 
        ON SRV.LOAN_NBR_NUM = LN.LoanId
    INNER JOIN lsams.dbo.srvdsr SR WITH (NOLOCK) 
        ON LN.LoanId = SR._loan_num
    LEFT JOIN GameChangers.admn.OlapEvalCase EC WITH (NOLOCK)
        ON EC.loanid = SRV.LOAN_NBR_NUM
        AND EC.STATUS = 'Completed'
    WHERE COLLECTION_CODE IN ('MOD1', 'EXT1', 'SLDF')
        AND SRV.SEQ_NBR = (
            SELECT MAX(SRV1.SEQ_NBR)
            FROM LSAMS.dbo.SRVCOLI SRV1 WITH (NOLOCK)
            WHERE SRV1.LOAN_NBR_NUM = SRV.LOAN_NBR_NUM
                AND COLLECTION_CODE IN ('MOD1', 'EXT1', 'SLDF')
        )
        AND LEN(CAST(ENTER_DATE AS VARCHAR)) = 8
        AND '06' NOT IN (
            SELECT MSMSGCD
            FROM LSAMS.dbo.SRVLNMSGS MC WITH (NOLOCK)
            WHERE MC.MSLOAN = SRV.LOAN_NBR_NUM
        )
),
PostModHistory AS (
    -- Post modification history
    SELECT 
        ph.LoanNumber AS EC_LoanNumber,
        ph.IsFirstPaymentMade,
        ph.IsSecondPaymentMade
    FROM asgn.PostModificationHistory ph WITH (NOLOCK)
),
LoanDetails AS (
    -- Main loan details 
    SELECT 
        LoanId,
        InvestorGroupId AS IG_InvestorGroupId,
        L3 AS IVT_L3,
        InvestorCode AS LN_InvestorCode,
        LoanType AS LN_LoanType,
        UPB AS LN_UPB,
        NextDueDate AS LN_NextDueDate,
        0 AS PaymentAmount,
        DaysDelinquent AS LN_DaysDelinquent,
        NULL AS TotalMonthlyPayment
    FROM asgn.LoanData WITH (NOLOCK)
),
MessageCodes AS (
    -- Consolidated message codes using single query
    SELECT 
        MSLOAN,
        MAX(CASE WHEN MSSEQ = 4 THEN MSMSGCD END) AS MessageCode4,
        MAX(CASE WHEN MSSEQ = 5 THEN MSMSGCD END) AS MessageCode5,
        MAX(CASE WHEN MSSEQ = 6 THEN MSMSGCD END) AS MessageCode6,
        MAX(CASE WHEN MSSEQ = 7 THEN MSMSGCD END) AS MessageCode7,
        MAX(CASE WHEN MSSEQ = 8 THEN MSMSGCD END) AS MessageCode8,
        MAX(CASE WHEN MSSEQ = 9 THEN MSMSGCD END) AS MessageCode9,
        MAX(CASE WHEN MSSEQ = 10 THEN MSMSGCD END) AS MessageCode10
    FROM LSAMS.dbo.SRVLNMSGS WITH (NOLOCK)
    GROUP BY MSLOAN
),
EligibilityData AS (
    -- Eligibility lookup
    SELECT 
        LN.InvestorCode,
        EIL.InvestorCode AS EIL_InvestorCode,
        ACC.lbldisabl1
    FROM asgn.LoanData LN WITH (NOLOCK)
    LEFT JOIN adm.tbl_EligibleInv_Lookup EIL WITH (NOLOCK) 
        ON LN.InvestorCode = EIL.InvestorCode
    LEFT JOIN LSAMS.dbo.LSBD0R00 ACC WITH (NOLOCK) 
        ON LoanId = ACC.LBLN#
)

-- Final optimized query
SELECT 
    -- Main loan information
    lb.LoanId,
    lb.EvalId,
    lb.DateModification,
    lb.ModificationType,
    lb.smx220,
    lb.smm030,
    lb.smm040,
    lb.smm050,
    lb.DaysToLateCharge,
    
    -- Post modification data
    pmh.EC_LoanNumber,
    pmh.IsFirstPaymentMade AS ph_IsFirstPaymentMade,
    pmh.IsSecondPaymentMade AS ph_IsSecondPaymentMade,
    
    -- Loan details
    ld.IG_InvestorGroupId,
    ld.IVT_L3 AS InvestorGroup,
    ld.LN_InvestorCode,
    ld.LN_LoanType AS LoanTypeId,
    ld.LN_UPB,
    ld.LN_NextDueDate,
    ld.PaymentAmount,
    ld.LN_DaysDelinquent,
    ld.TotalMonthlyPayment,
    
    -- Message codes
    lb.smx220 AS Vacant,
    lb.smm030 AS MessageCode1,
    lb.smm040 AS MessageCode2,
    lb.smm050 AS MessageCode3,
    mc.MessageCode4,
    mc.MessageCode5,
    mc.MessageCode6,
    mc.MessageCode7,
    mc.MessageCode8,
    mc.MessageCode9,
    mc.MessageCode10,
    
    -- Additional fields
    0 AS Recurse,
    NULL AS SuspenseBalance,
    PR.[State] AS PropertyState,
    NULL AS MailingZIPCode,
    NULL AS BorrowerLastName,
    NULL AS GapsDate,
    NULL AS RelatedLoansCount,
    NULL AS PaymentShockIndicator,
    NULL AS ForeclosureSaleDate,
    0 AS IsContestedForeclosure,
    NULL AS BehavioralScore,
    EC.STATUS AS EvalStatus,
    EC.StatusDate AS EvalStatusDate,
    EC.PreApprovedFlag AS PreApprovedIdentifier,
    EC.InflightFlag AS InflightIndicator,
    NULL AS MilestoneMessage,
    EC.TrialStartDate,
    EC.FirstDocReceivedDate,
    NULL AS MiscSuspense,
    NULL AS FirstLegalActionDeadlineDate,
    NULL AS TrialPaymentDue,
    0 AS TrialPaymentAmount,
    ld.LN_LastPaymentReceived,
    0 AS LastPaymentAuthorized,
    ISNULL(btype.BrandId, 1) AS BrandId,
    
    -- Conditional logic
    CASE 
        WHEN lb.LoanId = SII.LOAN# THEN 1
        ELSE 0
    END AS IsSII,
    
    CASE 
        WHEN lb.LoanId = ACC.LBLN# THEN 1
        ELSE 0
    END AS IsACCOMODATION,
    
    CASE 
        WHEN ld.LN_InvestorCode = EIL.InvestorCode
            AND EIL.InvestorGroup = 'p15prp'
            AND ld.LN_LoanType NOT IN ('1', '2', '4')
        THEN 1
        ELSE 0
    END AS IsPL5,
    
    -- Source fields
    lb.EvalId AS Loans_EvalId,
    lb.DateModification AS Loans_DateModification,
    lb.ModificationType AS Loans_ModificationType,
    lb.DaysToLateCharge AS Loans_DaysToLateCharge,
    'PostModification' AS Source

FROM LoanBase lb
INNER JOIN LoanDetails ld ON lb.LoanId = ld.LoanId
LEFT JOIN PostModHistory pmh ON lb.LoanId = pmh.EC_LoanNumber
LEFT JOIN MessageCodes mc ON lb.LoanId = mc.MSLOAN
LEFT JOIN GameChangers.admn.OlapEvalCase EC WITH (NOLOCK) 
    ON lb.LoanId = EC.loanId 
    AND lb.EvalId = EC.evalid
LEFT JOIN adm.BrandMappingTable bm WITH (NOLOCK) 
    ON ld.LN_InvestorGroup = bm.lsbrand
LEFT JOIN adm.BrandType btype WITH (NOLOCK) 
    ON bm.lblbrand = btype.Brand
LEFT JOIN PVR.dbo.tbl_Property PR WITH (NOLOCK) 
    ON lb.LoanId = PR.LoanNumber
LEFT JOIN MSG_CODE_DATA MSGCODE WITH (NOLOCK) 
    ON lb.LoanId = MSGCODE.MSLOAN
LEFT JOIN LSAMS.dbo.SIIPST SII WITH (NOLOCK) 
    ON lb.LoanId = SII.LOAN#
    AND SII.Type = 'Successor'
    AND SII.Status = 'Pending'
    AND SII.CloseDate = ''
LEFT JOIN adm.tbl_EligibleInv_Lookup EIL WITH (NOLOCK) 
    ON ld.LN_InvestorCode = EIL.InvestorCode
LEFT JOIN LSAMS.dbo.LSBD0R00 ACC WITH (NOLOCK) 
    ON lb.LoanId = ACC.LBLN#
    AND ACC.lbldisabl1 <> ''
LEFT JOIN dbo.vw_Invest_temp IVT WITH (NOLOCK) 
    ON ld.LN_InvestorCode = IVT.Investor_Code
LEFT JOIN adm.InvestorGroup IG WITH (NOLOCK) 
    ON ld.IG_InvestorGroupId = IVT.L3
ORDER BY EC.StatusDate DESC;

-- PERFORMANCE RECOMMENDATIONS:
-- 1. Create indexes on frequently joined columns:
--    CREATE INDEX IX_SRVCOLI_LOAN_NBR_NUM ON LSAMS.dbo.SRVCOLI(LOAN_NBR_NUM, SEQ_NBR, COLLECTION_CODE);
--    CREATE INDEX IX_LoanData_LoanId ON asgn.LoanData(LoanId);
--    CREATE INDEX IX_OlapEvalCase_LoanId ON GameChangers.admn.OlapEvalCase(loanid, evalid, STATUS);
--    CREATE INDEX IX_SRVLNMSGS_MSLOAN ON LSAMS.dbo.SRVLNMSGS(MSLOAN, MSSEQ);

-- 2. Consider partitioning large tables by date ranges if applicable

-- 3. Update statistics regularly:
--    UPDATE STATISTICS LSAMS.dbo.SRVCOLI WITH FULLSCAN;
--    UPDATE STATISTICS asgn.LoanData WITH FULLSCAN;

-- 4. Monitor execution plan and add missing indexes suggested by SQL Server

-- 5. Consider creating indexed views for commonly accessed combinations