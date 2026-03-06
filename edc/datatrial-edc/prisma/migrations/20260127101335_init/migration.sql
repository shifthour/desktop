-- CreateTable
CREATE TABLE "User" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "email" TEXT NOT NULL,
    "password" TEXT NOT NULL,
    "firstName" TEXT NOT NULL,
    "lastName" TEXT NOT NULL,
    "phone" TEXT,
    "organization" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "lastLogin" DATETIME,
    "failedLoginAttempts" INTEGER NOT NULL DEFAULT 0,
    "lockedUntil" DATETIME,
    "passwordChangedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "mustChangePassword" BOOLEAN NOT NULL DEFAULT false,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" DATETIME NOT NULL
);

-- CreateTable
CREATE TABLE "Role" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "roleCode" TEXT NOT NULL,
    "roleName" TEXT NOT NULL,
    "description" TEXT,
    "isBlinded" BOOLEAN NOT NULL DEFAULT true,
    "isSystemRole" BOOLEAN NOT NULL DEFAULT false,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CreateTable
CREATE TABLE "Permission" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "permissionCode" TEXT NOT NULL,
    "permissionName" TEXT NOT NULL,
    "module" TEXT NOT NULL,
    "description" TEXT,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CreateTable
CREATE TABLE "RolePermission" (
    "roleId" TEXT NOT NULL,
    "permissionId" TEXT NOT NULL,

    PRIMARY KEY ("roleId", "permissionId"),
    CONSTRAINT "RolePermission_roleId_fkey" FOREIGN KEY ("roleId") REFERENCES "Role" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "RolePermission_permissionId_fkey" FOREIGN KEY ("permissionId") REFERENCES "Permission" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "UserStudyAssignment" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "userId" TEXT NOT NULL,
    "studyId" TEXT NOT NULL,
    "roleId" TEXT NOT NULL,
    "siteId" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "assignedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "assignedBy" TEXT,
    CONSTRAINT "UserStudyAssignment_userId_fkey" FOREIGN KEY ("userId") REFERENCES "User" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "UserStudyAssignment_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "Study" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "UserStudyAssignment_roleId_fkey" FOREIGN KEY ("roleId") REFERENCES "Role" ("id") ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT "UserStudyAssignment_siteId_fkey" FOREIGN KEY ("siteId") REFERENCES "Site" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "Study" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "studyCode" TEXT NOT NULL,
    "studyName" TEXT NOT NULL,
    "protocolNumber" TEXT,
    "sponsorName" TEXT,
    "phase" TEXT,
    "status" TEXT NOT NULL DEFAULT 'draft',
    "therapeuticArea" TEXT,
    "indication" TEXT,
    "startDate" DATETIME,
    "endDate" DATETIME,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" DATETIME NOT NULL,
    "createdById" TEXT,
    CONSTRAINT "Study_createdById_fkey" FOREIGN KEY ("createdById") REFERENCES "User" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "Site" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "studyId" TEXT NOT NULL,
    "siteCode" TEXT NOT NULL,
    "siteName" TEXT NOT NULL,
    "address" TEXT,
    "city" TEXT,
    "state" TEXT,
    "country" TEXT,
    "piName" TEXT,
    "piEmail" TEXT,
    "status" TEXT NOT NULL DEFAULT 'inactive',
    "screeningCap" INTEGER NOT NULL DEFAULT 0,
    "randomizationCap" INTEGER NOT NULL DEFAULT 0,
    "screeningAllocation" INTEGER NOT NULL DEFAULT 0,
    "randomizationAllocation" INTEGER NOT NULL DEFAULT 0,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" DATETIME NOT NULL,
    CONSTRAINT "Site_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "Study" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "StudyArm" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "studyId" TEXT NOT NULL,
    "armCode" TEXT NOT NULL,
    "armName" TEXT NOT NULL,
    "armType" TEXT,
    "ratio" INTEGER NOT NULL DEFAULT 1,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "StudyArm_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "Study" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "Visit" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "studyId" TEXT NOT NULL,
    "visitCode" TEXT NOT NULL,
    "visitName" TEXT NOT NULL,
    "visitType" TEXT NOT NULL,
    "sequenceNumber" INTEGER NOT NULL,
    "durationDays" INTEGER NOT NULL DEFAULT 0,
    "windowBefore" INTEGER NOT NULL DEFAULT 0,
    "windowAfter" INTEGER NOT NULL DEFAULT 0,
    "isSchedulable" BOOLEAN NOT NULL DEFAULT true,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "Visit_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "Study" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "CRFForm" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "studyId" TEXT NOT NULL,
    "formCode" TEXT NOT NULL,
    "formName" TEXT NOT NULL,
    "formType" TEXT,
    "isRepeating" BOOLEAN NOT NULL DEFAULT false,
    "isLogForm" BOOLEAN NOT NULL DEFAULT false,
    "sequenceNumber" INTEGER,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "CRFForm_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "Study" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "VisitForm" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "visitId" TEXT NOT NULL,
    "formId" TEXT NOT NULL,
    "isRequired" BOOLEAN NOT NULL DEFAULT false,
    "sequenceNumber" INTEGER,
    CONSTRAINT "VisitForm_visitId_fkey" FOREIGN KEY ("visitId") REFERENCES "Visit" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "VisitForm_formId_fkey" FOREIGN KEY ("formId") REFERENCES "CRFForm" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "CRFField" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "formId" TEXT NOT NULL,
    "fieldCode" TEXT NOT NULL,
    "fieldLabel" TEXT NOT NULL,
    "fieldType" TEXT NOT NULL,
    "dataType" TEXT,
    "isRequired" BOOLEAN NOT NULL DEFAULT false,
    "isReadonly" BOOLEAN NOT NULL DEFAULT false,
    "isHidden" BOOLEAN NOT NULL DEFAULT false,
    "defaultValue" TEXT,
    "placeholder" TEXT,
    "helpText" TEXT,
    "minValue" REAL,
    "maxValue" REAL,
    "minLength" INTEGER,
    "maxLength" INTEGER,
    "regexPattern" TEXT,
    "decimalPlaces" INTEGER,
    "unit" TEXT,
    "sequenceNumber" INTEGER NOT NULL,
    "rowNumber" INTEGER NOT NULL DEFAULT 1,
    "columnNumber" INTEGER NOT NULL DEFAULT 1,
    "colspan" INTEGER NOT NULL DEFAULT 1,
    "lovListId" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "CRFField_formId_fkey" FOREIGN KEY ("formId") REFERENCES "CRFForm" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "CRFField_lovListId_fkey" FOREIGN KEY ("lovListId") REFERENCES "LOVList" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "LOVList" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "studyId" TEXT NOT NULL,
    "listCode" TEXT NOT NULL,
    "listName" TEXT NOT NULL,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "LOVList_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "Study" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "LOVItem" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "listId" TEXT NOT NULL,
    "itemCode" TEXT NOT NULL,
    "itemValue" TEXT NOT NULL,
    "sequenceNumber" INTEGER,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "LOVItem_listId_fkey" FOREIGN KEY ("listId") REFERENCES "LOVList" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "EditCheck" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "studyId" TEXT NOT NULL,
    "checkCode" TEXT NOT NULL,
    "checkName" TEXT NOT NULL,
    "checkType" TEXT NOT NULL,
    "sourceFieldId" TEXT,
    "targetFieldId" TEXT,
    "conditionExpression" TEXT,
    "errorMessage" TEXT NOT NULL,
    "severity" TEXT NOT NULL DEFAULT 'error',
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "EditCheck_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "Study" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "EditCheck_sourceFieldId_fkey" FOREIGN KEY ("sourceFieldId") REFERENCES "CRFField" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "EditCheck_targetFieldId_fkey" FOREIGN KEY ("targetFieldId") REFERENCES "CRFField" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "Subject" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "studyId" TEXT NOT NULL,
    "siteId" TEXT NOT NULL,
    "screeningNumber" TEXT NOT NULL,
    "randomizationNumber" TEXT,
    "subjectInitials" TEXT,
    "status" TEXT NOT NULL DEFAULT 'screened',
    "screeningDate" DATETIME,
    "randomizationDate" DATETIME,
    "discontinuationDate" DATETIME,
    "discontinuationReason" TEXT,
    "armId" TEXT,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" DATETIME NOT NULL,
    "createdById" TEXT,
    CONSTRAINT "Subject_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "Study" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "Subject_siteId_fkey" FOREIGN KEY ("siteId") REFERENCES "Site" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "Subject_armId_fkey" FOREIGN KEY ("armId") REFERENCES "StudyArm" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "Subject_createdById_fkey" FOREIGN KEY ("createdById") REFERENCES "User" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "SubjectVisit" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "subjectId" TEXT NOT NULL,
    "visitId" TEXT NOT NULL,
    "visitDate" DATETIME,
    "scheduledDate" DATETIME,
    "status" TEXT NOT NULL DEFAULT 'not_started',
    "isUnscheduled" BOOLEAN NOT NULL DEFAULT false,
    "unscheduledNumber" INTEGER,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" DATETIME NOT NULL,
    CONSTRAINT "SubjectVisit_subjectId_fkey" FOREIGN KEY ("subjectId") REFERENCES "Subject" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "SubjectVisit_visitId_fkey" FOREIGN KEY ("visitId") REFERENCES "Visit" ("id") ON DELETE RESTRICT ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "CRFData" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "subjectVisitId" TEXT NOT NULL,
    "formId" TEXT NOT NULL,
    "repeatNumber" INTEGER NOT NULL DEFAULT 1,
    "status" TEXT NOT NULL DEFAULT 'incomplete',
    "submittedAt" DATETIME,
    "submittedById" TEXT,
    "sdvAt" DATETIME,
    "sdvById" TEXT,
    "dmReviewedAt" DATETIME,
    "dmReviewedById" TEXT,
    "qaReviewedAt" DATETIME,
    "qaReviewedById" TEXT,
    "piReviewedAt" DATETIME,
    "piReviewedById" TEXT,
    "lockedAt" DATETIME,
    "lockedById" TEXT,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" DATETIME NOT NULL,
    CONSTRAINT "CRFData_subjectVisitId_fkey" FOREIGN KEY ("subjectVisitId") REFERENCES "SubjectVisit" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "CRFData_formId_fkey" FOREIGN KEY ("formId") REFERENCES "CRFForm" ("id") ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT "CRFData_submittedById_fkey" FOREIGN KEY ("submittedById") REFERENCES "User" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "CRFData_sdvById_fkey" FOREIGN KEY ("sdvById") REFERENCES "User" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "CRFData_dmReviewedById_fkey" FOREIGN KEY ("dmReviewedById") REFERENCES "User" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "CRFData_lockedById_fkey" FOREIGN KEY ("lockedById") REFERENCES "User" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "CRFFieldValue" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "crfDataId" TEXT NOT NULL,
    "fieldId" TEXT NOT NULL,
    "fieldValue" TEXT,
    "previousValue" TEXT,
    "changeReason" TEXT,
    "isSystemPopulated" BOOLEAN NOT NULL DEFAULT false,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" DATETIME NOT NULL,
    "updatedById" TEXT,
    CONSTRAINT "CRFFieldValue_crfDataId_fkey" FOREIGN KEY ("crfDataId") REFERENCES "CRFData" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "CRFFieldValue_fieldId_fkey" FOREIGN KEY ("fieldId") REFERENCES "CRFField" ("id") ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT "CRFFieldValue_updatedById_fkey" FOREIGN KEY ("updatedById") REFERENCES "User" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "Query" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "queryNumber" TEXT NOT NULL,
    "subjectId" TEXT NOT NULL,
    "crfDataId" TEXT,
    "fieldId" TEXT,
    "queryType" TEXT NOT NULL,
    "queryCategory" TEXT,
    "status" TEXT NOT NULL DEFAULT 'open',
    "queryText" TEXT NOT NULL,
    "responseText" TEXT,
    "resolutionText" TEXT,
    "raisedById" TEXT NOT NULL,
    "raisedByRole" TEXT,
    "raisedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "respondedById" TEXT,
    "respondedAt" DATETIME,
    "closedById" TEXT,
    "closedAt" DATETIME,
    "reopenedById" TEXT,
    "reopenedAt" DATETIME,
    "reopenCount" INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT "Query_subjectId_fkey" FOREIGN KEY ("subjectId") REFERENCES "Subject" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "Query_crfDataId_fkey" FOREIGN KEY ("crfDataId") REFERENCES "CRFData" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "Query_fieldId_fkey" FOREIGN KEY ("fieldId") REFERENCES "CRFField" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "Query_raisedById_fkey" FOREIGN KEY ("raisedById") REFERENCES "User" ("id") ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT "Query_respondedById_fkey" FOREIGN KEY ("respondedById") REFERENCES "User" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "Query_closedById_fkey" FOREIGN KEY ("closedById") REFERENCES "User" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "QueryHistory" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "queryId" TEXT NOT NULL,
    "action" TEXT NOT NULL,
    "actionBy" TEXT NOT NULL,
    "actionAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "oldStatus" TEXT,
    "newStatus" TEXT,
    "comment" TEXT,
    CONSTRAINT "QueryHistory_queryId_fkey" FOREIGN KEY ("queryId") REFERENCES "Query" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "KitDefinition" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "studyId" TEXT NOT NULL,
    "kitTypeCode" TEXT NOT NULL,
    "kitTypeName" TEXT NOT NULL,
    "armId" TEXT,
    "tabletsPerKit" INTEGER,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "KitDefinition_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "Study" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "KitDefinition_armId_fkey" FOREIGN KEY ("armId") REFERENCES "StudyArm" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "IPInventory" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "studyId" TEXT NOT NULL,
    "kitDefinitionId" TEXT NOT NULL,
    "kitNumber" TEXT NOT NULL,
    "batchNumber" TEXT,
    "manufactureDate" DATETIME,
    "expiryDate" DATETIME,
    "locationType" TEXT NOT NULL,
    "siteId" TEXT,
    "subjectId" TEXT,
    "armId" TEXT,
    "status" TEXT NOT NULL DEFAULT 'available',
    "receivedAt" DATETIME,
    "dispensedAt" DATETIME,
    "dispensedById" TEXT,
    "returnedAt" DATETIME,
    "tabletsReturned" INTEGER,
    "tabletsDispensed" INTEGER,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" DATETIME NOT NULL,
    CONSTRAINT "IPInventory_kitDefinitionId_fkey" FOREIGN KEY ("kitDefinitionId") REFERENCES "KitDefinition" ("id") ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT "IPInventory_siteId_fkey" FOREIGN KEY ("siteId") REFERENCES "Site" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "IPInventory_subjectId_fkey" FOREIGN KEY ("subjectId") REFERENCES "Subject" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "IPInventory_dispensedById_fkey" FOREIGN KEY ("dispensedById") REFERENCES "User" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "IPInventory_armId_fkey" FOREIGN KEY ("armId") REFERENCES "StudyArm" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "Shipment" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shipmentNumber" TEXT NOT NULL,
    "studyId" TEXT NOT NULL,
    "shipmentType" TEXT NOT NULL,
    "fromSiteId" TEXT,
    "toSiteId" TEXT,
    "status" TEXT NOT NULL DEFAULT 'created',
    "shippedDate" DATETIME,
    "expectedDeliveryDate" DATETIME,
    "actualDeliveryDate" DATETIME,
    "trackingNumber" TEXT,
    "carrier" TEXT,
    "temperatureExcursion" BOOLEAN NOT NULL DEFAULT false,
    "comments" TEXT,
    "createdById" TEXT,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "dispatchedById" TEXT,
    "dispatchedAt" DATETIME,
    "receivedById" TEXT,
    "receivedAt" DATETIME,
    CONSTRAINT "Shipment_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "Study" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "Shipment_fromSiteId_fkey" FOREIGN KEY ("fromSiteId") REFERENCES "Site" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "Shipment_toSiteId_fkey" FOREIGN KEY ("toSiteId") REFERENCES "Site" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "Shipment_createdById_fkey" FOREIGN KEY ("createdById") REFERENCES "User" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "Shipment_dispatchedById_fkey" FOREIGN KEY ("dispatchedById") REFERENCES "User" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "Shipment_receivedById_fkey" FOREIGN KEY ("receivedById") REFERENCES "User" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "ShipmentItem" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shipmentId" TEXT NOT NULL,
    "kitId" TEXT NOT NULL,
    "status" TEXT,
    "receivedCondition" TEXT,
    "comments" TEXT,
    CONSTRAINT "ShipmentItem_shipmentId_fkey" FOREIGN KEY ("shipmentId") REFERENCES "Shipment" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "ShipmentItem_kitId_fkey" FOREIGN KEY ("kitId") REFERENCES "IPInventory" ("id") ON DELETE RESTRICT ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "KitRequest" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "requestNumber" TEXT NOT NULL,
    "studyId" TEXT NOT NULL,
    "siteId" TEXT NOT NULL,
    "requestedById" TEXT NOT NULL,
    "requestedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "quantityRequested" INTEGER NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'pending',
    "approvedById" TEXT,
    "approvedAt" DATETIME,
    "declineReason" TEXT,
    "comments" TEXT,
    CONSTRAINT "KitRequest_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "Study" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "KitRequest_siteId_fkey" FOREIGN KEY ("siteId") REFERENCES "Site" ("id") ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT "KitRequest_requestedById_fkey" FOREIGN KEY ("requestedById") REFERENCES "User" ("id") ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT "KitRequest_approvedById_fkey" FOREIGN KEY ("approvedById") REFERENCES "User" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "RandomizationEntry" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "studyId" TEXT NOT NULL,
    "randomizationNumber" TEXT NOT NULL,
    "armId" TEXT NOT NULL,
    "siteId" TEXT,
    "isUsed" BOOLEAN NOT NULL DEFAULT false,
    "usedAt" DATETIME,
    "subjectId" TEXT,
    "sequenceNumber" INTEGER,
    CONSTRAINT "RandomizationEntry_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "Study" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "RandomizationEntry_armId_fkey" FOREIGN KEY ("armId") REFERENCES "StudyArm" ("id") ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT "RandomizationEntry_siteId_fkey" FOREIGN KEY ("siteId") REFERENCES "Site" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "RandomizationEntry_subjectId_fkey" FOREIGN KEY ("subjectId") REFERENCES "Subject" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "AuditLog" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "tableName" TEXT NOT NULL,
    "recordId" TEXT NOT NULL,
    "action" TEXT NOT NULL,
    "oldValues" TEXT,
    "newValues" TEXT,
    "changedFields" TEXT,
    "changeReason" TEXT,
    "userId" TEXT,
    "userEmail" TEXT,
    "userRole" TEXT,
    "ipAddress" TEXT,
    "userAgent" TEXT,
    "sessionId" TEXT,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "AuditLog_userId_fkey" FOREIGN KEY ("userId") REFERENCES "User" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "ESignature" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "recordType" TEXT NOT NULL,
    "recordId" TEXT NOT NULL,
    "signatureMeaning" TEXT NOT NULL,
    "signedById" TEXT NOT NULL,
    "signedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "userEmail" TEXT NOT NULL,
    "userFullName" TEXT NOT NULL,
    "ipAddress" TEXT,
    "signatureHash" TEXT NOT NULL,
    CONSTRAINT "ESignature_signedById_fkey" FOREIGN KEY ("signedById") REFERENCES "User" ("id") ON DELETE RESTRICT ON UPDATE CASCADE
);

-- CreateIndex
CREATE UNIQUE INDEX "User_email_key" ON "User"("email");

-- CreateIndex
CREATE UNIQUE INDEX "Role_roleCode_key" ON "Role"("roleCode");

-- CreateIndex
CREATE UNIQUE INDEX "Permission_permissionCode_key" ON "Permission"("permissionCode");

-- CreateIndex
CREATE UNIQUE INDEX "UserStudyAssignment_userId_studyId_roleId_siteId_key" ON "UserStudyAssignment"("userId", "studyId", "roleId", "siteId");

-- CreateIndex
CREATE UNIQUE INDEX "Study_studyCode_key" ON "Study"("studyCode");

-- CreateIndex
CREATE UNIQUE INDEX "Site_studyId_siteCode_key" ON "Site"("studyId", "siteCode");

-- CreateIndex
CREATE UNIQUE INDEX "StudyArm_studyId_armCode_key" ON "StudyArm"("studyId", "armCode");

-- CreateIndex
CREATE UNIQUE INDEX "Visit_studyId_visitCode_key" ON "Visit"("studyId", "visitCode");

-- CreateIndex
CREATE UNIQUE INDEX "CRFForm_studyId_formCode_key" ON "CRFForm"("studyId", "formCode");

-- CreateIndex
CREATE UNIQUE INDEX "VisitForm_visitId_formId_key" ON "VisitForm"("visitId", "formId");

-- CreateIndex
CREATE UNIQUE INDEX "CRFField_formId_fieldCode_key" ON "CRFField"("formId", "fieldCode");

-- CreateIndex
CREATE UNIQUE INDEX "LOVList_studyId_listCode_key" ON "LOVList"("studyId", "listCode");

-- CreateIndex
CREATE UNIQUE INDEX "LOVItem_listId_itemCode_key" ON "LOVItem"("listId", "itemCode");

-- CreateIndex
CREATE UNIQUE INDEX "EditCheck_studyId_checkCode_key" ON "EditCheck"("studyId", "checkCode");

-- CreateIndex
CREATE UNIQUE INDEX "Subject_studyId_screeningNumber_key" ON "Subject"("studyId", "screeningNumber");

-- CreateIndex
CREATE UNIQUE INDEX "CRFFieldValue_crfDataId_fieldId_key" ON "CRFFieldValue"("crfDataId", "fieldId");

-- CreateIndex
CREATE UNIQUE INDEX "Query_queryNumber_key" ON "Query"("queryNumber");

-- CreateIndex
CREATE UNIQUE INDEX "KitDefinition_studyId_kitTypeCode_key" ON "KitDefinition"("studyId", "kitTypeCode");

-- CreateIndex
CREATE UNIQUE INDEX "IPInventory_kitNumber_key" ON "IPInventory"("kitNumber");

-- CreateIndex
CREATE UNIQUE INDEX "Shipment_shipmentNumber_key" ON "Shipment"("shipmentNumber");

-- CreateIndex
CREATE UNIQUE INDEX "KitRequest_requestNumber_key" ON "KitRequest"("requestNumber");

-- CreateIndex
CREATE UNIQUE INDEX "RandomizationEntry_studyId_randomizationNumber_key" ON "RandomizationEntry"("studyId", "randomizationNumber");

-- CreateIndex
CREATE INDEX "AuditLog_tableName_recordId_idx" ON "AuditLog"("tableName", "recordId");

-- CreateIndex
CREATE INDEX "AuditLog_userId_idx" ON "AuditLog"("userId");

-- CreateIndex
CREATE INDEX "AuditLog_createdAt_idx" ON "AuditLog"("createdAt");
