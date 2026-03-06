/*
  Warnings:

  - You are about to drop the `AuditLog` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `CRFData` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `CRFField` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `CRFFieldValue` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `CRFForm` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `ESignature` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `EditCheck` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `IPInventory` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `KitDefinition` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `KitRequest` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `LOVItem` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `LOVList` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `Permission` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `Query` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `QueryHistory` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `RandomizationEntry` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `Role` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `RolePermission` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `Shipment` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `ShipmentItem` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `Site` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `Study` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `StudyArm` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `Subject` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `SubjectVisit` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `User` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `UserStudyAssignment` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `Visit` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `VisitForm` table. If the table is not empty, all the data it contains will be lost.

*/
-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "AuditLog";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "CRFData";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "CRFField";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "CRFFieldValue";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "CRFForm";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "ESignature";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "EditCheck";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "IPInventory";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "KitDefinition";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "KitRequest";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "LOVItem";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "LOVList";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "Permission";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "Query";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "QueryHistory";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "RandomizationEntry";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "Role";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "RolePermission";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "Shipment";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "ShipmentItem";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "Site";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "Study";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "StudyArm";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "Subject";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "SubjectVisit";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "User";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "UserStudyAssignment";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "Visit";
PRAGMA foreign_keys=on;

-- DropTable
PRAGMA foreign_keys=off;
DROP TABLE "VisitForm";
PRAGMA foreign_keys=on;

-- CreateTable
CREATE TABLE "datatrial_users" (
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
CREATE TABLE "datatrial_roles" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "roleCode" TEXT NOT NULL,
    "roleName" TEXT NOT NULL,
    "description" TEXT,
    "isBlinded" BOOLEAN NOT NULL DEFAULT true,
    "isSystemRole" BOOLEAN NOT NULL DEFAULT false,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CreateTable
CREATE TABLE "datatrial_permissions" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "permissionCode" TEXT NOT NULL,
    "permissionName" TEXT NOT NULL,
    "module" TEXT NOT NULL,
    "description" TEXT,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CreateTable
CREATE TABLE "datatrial_role_permissions" (
    "roleId" TEXT NOT NULL,
    "permissionId" TEXT NOT NULL,

    PRIMARY KEY ("roleId", "permissionId"),
    CONSTRAINT "datatrial_role_permissions_roleId_fkey" FOREIGN KEY ("roleId") REFERENCES "datatrial_roles" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "datatrial_role_permissions_permissionId_fkey" FOREIGN KEY ("permissionId") REFERENCES "datatrial_permissions" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_user_study_assignments" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "userId" TEXT NOT NULL,
    "studyId" TEXT NOT NULL,
    "roleId" TEXT NOT NULL,
    "siteId" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "assignedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "assignedBy" TEXT,
    CONSTRAINT "datatrial_user_study_assignments_userId_fkey" FOREIGN KEY ("userId") REFERENCES "datatrial_users" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "datatrial_user_study_assignments_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "datatrial_studies" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "datatrial_user_study_assignments_roleId_fkey" FOREIGN KEY ("roleId") REFERENCES "datatrial_roles" ("id") ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT "datatrial_user_study_assignments_siteId_fkey" FOREIGN KEY ("siteId") REFERENCES "datatrial_sites" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_studies" (
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
    CONSTRAINT "datatrial_studies_createdById_fkey" FOREIGN KEY ("createdById") REFERENCES "datatrial_users" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_sites" (
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
    CONSTRAINT "datatrial_sites_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "datatrial_studies" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_study_arms" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "studyId" TEXT NOT NULL,
    "armCode" TEXT NOT NULL,
    "armName" TEXT NOT NULL,
    "armType" TEXT,
    "ratio" INTEGER NOT NULL DEFAULT 1,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "datatrial_study_arms_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "datatrial_studies" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_visits" (
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
    CONSTRAINT "datatrial_visits_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "datatrial_studies" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_crf_forms" (
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
    CONSTRAINT "datatrial_crf_forms_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "datatrial_studies" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_visit_forms" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "visitId" TEXT NOT NULL,
    "formId" TEXT NOT NULL,
    "isRequired" BOOLEAN NOT NULL DEFAULT false,
    "sequenceNumber" INTEGER,
    CONSTRAINT "datatrial_visit_forms_visitId_fkey" FOREIGN KEY ("visitId") REFERENCES "datatrial_visits" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "datatrial_visit_forms_formId_fkey" FOREIGN KEY ("formId") REFERENCES "datatrial_crf_forms" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_crf_fields" (
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
    CONSTRAINT "datatrial_crf_fields_formId_fkey" FOREIGN KEY ("formId") REFERENCES "datatrial_crf_forms" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "datatrial_crf_fields_lovListId_fkey" FOREIGN KEY ("lovListId") REFERENCES "datatrial_lov_lists" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_lov_lists" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "studyId" TEXT NOT NULL,
    "listCode" TEXT NOT NULL,
    "listName" TEXT NOT NULL,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "datatrial_lov_lists_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "datatrial_studies" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_lov_items" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "listId" TEXT NOT NULL,
    "itemCode" TEXT NOT NULL,
    "itemValue" TEXT NOT NULL,
    "sequenceNumber" INTEGER,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "datatrial_lov_items_listId_fkey" FOREIGN KEY ("listId") REFERENCES "datatrial_lov_lists" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_edit_checks" (
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
    CONSTRAINT "datatrial_edit_checks_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "datatrial_studies" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "datatrial_edit_checks_sourceFieldId_fkey" FOREIGN KEY ("sourceFieldId") REFERENCES "datatrial_crf_fields" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "datatrial_edit_checks_targetFieldId_fkey" FOREIGN KEY ("targetFieldId") REFERENCES "datatrial_crf_fields" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_subjects" (
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
    CONSTRAINT "datatrial_subjects_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "datatrial_studies" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "datatrial_subjects_siteId_fkey" FOREIGN KEY ("siteId") REFERENCES "datatrial_sites" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "datatrial_subjects_armId_fkey" FOREIGN KEY ("armId") REFERENCES "datatrial_study_arms" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "datatrial_subjects_createdById_fkey" FOREIGN KEY ("createdById") REFERENCES "datatrial_users" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_subject_visits" (
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
    CONSTRAINT "datatrial_subject_visits_subjectId_fkey" FOREIGN KEY ("subjectId") REFERENCES "datatrial_subjects" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "datatrial_subject_visits_visitId_fkey" FOREIGN KEY ("visitId") REFERENCES "datatrial_visits" ("id") ON DELETE RESTRICT ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_crf_data" (
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
    CONSTRAINT "datatrial_crf_data_subjectVisitId_fkey" FOREIGN KEY ("subjectVisitId") REFERENCES "datatrial_subject_visits" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "datatrial_crf_data_formId_fkey" FOREIGN KEY ("formId") REFERENCES "datatrial_crf_forms" ("id") ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT "datatrial_crf_data_submittedById_fkey" FOREIGN KEY ("submittedById") REFERENCES "datatrial_users" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "datatrial_crf_data_sdvById_fkey" FOREIGN KEY ("sdvById") REFERENCES "datatrial_users" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "datatrial_crf_data_dmReviewedById_fkey" FOREIGN KEY ("dmReviewedById") REFERENCES "datatrial_users" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "datatrial_crf_data_lockedById_fkey" FOREIGN KEY ("lockedById") REFERENCES "datatrial_users" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_crf_field_values" (
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
    CONSTRAINT "datatrial_crf_field_values_crfDataId_fkey" FOREIGN KEY ("crfDataId") REFERENCES "datatrial_crf_data" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "datatrial_crf_field_values_fieldId_fkey" FOREIGN KEY ("fieldId") REFERENCES "datatrial_crf_fields" ("id") ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT "datatrial_crf_field_values_updatedById_fkey" FOREIGN KEY ("updatedById") REFERENCES "datatrial_users" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_queries" (
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
    CONSTRAINT "datatrial_queries_subjectId_fkey" FOREIGN KEY ("subjectId") REFERENCES "datatrial_subjects" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "datatrial_queries_crfDataId_fkey" FOREIGN KEY ("crfDataId") REFERENCES "datatrial_crf_data" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "datatrial_queries_fieldId_fkey" FOREIGN KEY ("fieldId") REFERENCES "datatrial_crf_fields" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "datatrial_queries_raisedById_fkey" FOREIGN KEY ("raisedById") REFERENCES "datatrial_users" ("id") ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT "datatrial_queries_respondedById_fkey" FOREIGN KEY ("respondedById") REFERENCES "datatrial_users" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "datatrial_queries_closedById_fkey" FOREIGN KEY ("closedById") REFERENCES "datatrial_users" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_query_history" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "queryId" TEXT NOT NULL,
    "action" TEXT NOT NULL,
    "actionBy" TEXT NOT NULL,
    "actionAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "oldStatus" TEXT,
    "newStatus" TEXT,
    "comment" TEXT,
    CONSTRAINT "datatrial_query_history_queryId_fkey" FOREIGN KEY ("queryId") REFERENCES "datatrial_queries" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_kit_definitions" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "studyId" TEXT NOT NULL,
    "kitTypeCode" TEXT NOT NULL,
    "kitTypeName" TEXT NOT NULL,
    "armId" TEXT,
    "tabletsPerKit" INTEGER,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "datatrial_kit_definitions_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "datatrial_studies" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "datatrial_kit_definitions_armId_fkey" FOREIGN KEY ("armId") REFERENCES "datatrial_study_arms" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_ip_inventory" (
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
    CONSTRAINT "datatrial_ip_inventory_kitDefinitionId_fkey" FOREIGN KEY ("kitDefinitionId") REFERENCES "datatrial_kit_definitions" ("id") ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT "datatrial_ip_inventory_siteId_fkey" FOREIGN KEY ("siteId") REFERENCES "datatrial_sites" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "datatrial_ip_inventory_subjectId_fkey" FOREIGN KEY ("subjectId") REFERENCES "datatrial_subjects" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "datatrial_ip_inventory_dispensedById_fkey" FOREIGN KEY ("dispensedById") REFERENCES "datatrial_users" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "datatrial_ip_inventory_armId_fkey" FOREIGN KEY ("armId") REFERENCES "datatrial_study_arms" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_shipments" (
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
    CONSTRAINT "datatrial_shipments_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "datatrial_studies" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "datatrial_shipments_fromSiteId_fkey" FOREIGN KEY ("fromSiteId") REFERENCES "datatrial_sites" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "datatrial_shipments_toSiteId_fkey" FOREIGN KEY ("toSiteId") REFERENCES "datatrial_sites" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "datatrial_shipments_createdById_fkey" FOREIGN KEY ("createdById") REFERENCES "datatrial_users" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "datatrial_shipments_dispatchedById_fkey" FOREIGN KEY ("dispatchedById") REFERENCES "datatrial_users" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "datatrial_shipments_receivedById_fkey" FOREIGN KEY ("receivedById") REFERENCES "datatrial_users" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_shipment_items" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "shipmentId" TEXT NOT NULL,
    "kitId" TEXT NOT NULL,
    "status" TEXT,
    "receivedCondition" TEXT,
    "comments" TEXT,
    CONSTRAINT "datatrial_shipment_items_shipmentId_fkey" FOREIGN KEY ("shipmentId") REFERENCES "datatrial_shipments" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "datatrial_shipment_items_kitId_fkey" FOREIGN KEY ("kitId") REFERENCES "datatrial_ip_inventory" ("id") ON DELETE RESTRICT ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_kit_requests" (
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
    CONSTRAINT "datatrial_kit_requests_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "datatrial_studies" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "datatrial_kit_requests_siteId_fkey" FOREIGN KEY ("siteId") REFERENCES "datatrial_sites" ("id") ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT "datatrial_kit_requests_requestedById_fkey" FOREIGN KEY ("requestedById") REFERENCES "datatrial_users" ("id") ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT "datatrial_kit_requests_approvedById_fkey" FOREIGN KEY ("approvedById") REFERENCES "datatrial_users" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_randomization_entries" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "studyId" TEXT NOT NULL,
    "randomizationNumber" TEXT NOT NULL,
    "armId" TEXT NOT NULL,
    "siteId" TEXT,
    "isUsed" BOOLEAN NOT NULL DEFAULT false,
    "usedAt" DATETIME,
    "subjectId" TEXT,
    "sequenceNumber" INTEGER,
    CONSTRAINT "datatrial_randomization_entries_studyId_fkey" FOREIGN KEY ("studyId") REFERENCES "datatrial_studies" ("id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "datatrial_randomization_entries_armId_fkey" FOREIGN KEY ("armId") REFERENCES "datatrial_study_arms" ("id") ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT "datatrial_randomization_entries_siteId_fkey" FOREIGN KEY ("siteId") REFERENCES "datatrial_sites" ("id") ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT "datatrial_randomization_entries_subjectId_fkey" FOREIGN KEY ("subjectId") REFERENCES "datatrial_subjects" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_audit_logs" (
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
    CONSTRAINT "datatrial_audit_logs_userId_fkey" FOREIGN KEY ("userId") REFERENCES "datatrial_users" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "datatrial_esignatures" (
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
    CONSTRAINT "datatrial_esignatures_signedById_fkey" FOREIGN KEY ("signedById") REFERENCES "datatrial_users" ("id") ON DELETE RESTRICT ON UPDATE CASCADE
);

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_users_email_key" ON "datatrial_users"("email");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_roles_roleCode_key" ON "datatrial_roles"("roleCode");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_permissions_permissionCode_key" ON "datatrial_permissions"("permissionCode");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_user_study_assignments_userId_studyId_roleId_siteId_key" ON "datatrial_user_study_assignments"("userId", "studyId", "roleId", "siteId");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_studies_studyCode_key" ON "datatrial_studies"("studyCode");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_sites_studyId_siteCode_key" ON "datatrial_sites"("studyId", "siteCode");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_study_arms_studyId_armCode_key" ON "datatrial_study_arms"("studyId", "armCode");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_visits_studyId_visitCode_key" ON "datatrial_visits"("studyId", "visitCode");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_crf_forms_studyId_formCode_key" ON "datatrial_crf_forms"("studyId", "formCode");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_visit_forms_visitId_formId_key" ON "datatrial_visit_forms"("visitId", "formId");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_crf_fields_formId_fieldCode_key" ON "datatrial_crf_fields"("formId", "fieldCode");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_lov_lists_studyId_listCode_key" ON "datatrial_lov_lists"("studyId", "listCode");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_lov_items_listId_itemCode_key" ON "datatrial_lov_items"("listId", "itemCode");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_edit_checks_studyId_checkCode_key" ON "datatrial_edit_checks"("studyId", "checkCode");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_subjects_studyId_screeningNumber_key" ON "datatrial_subjects"("studyId", "screeningNumber");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_crf_field_values_crfDataId_fieldId_key" ON "datatrial_crf_field_values"("crfDataId", "fieldId");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_queries_queryNumber_key" ON "datatrial_queries"("queryNumber");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_kit_definitions_studyId_kitTypeCode_key" ON "datatrial_kit_definitions"("studyId", "kitTypeCode");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_ip_inventory_kitNumber_key" ON "datatrial_ip_inventory"("kitNumber");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_shipments_shipmentNumber_key" ON "datatrial_shipments"("shipmentNumber");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_kit_requests_requestNumber_key" ON "datatrial_kit_requests"("requestNumber");

-- CreateIndex
CREATE UNIQUE INDEX "datatrial_randomization_entries_studyId_randomizationNumber_key" ON "datatrial_randomization_entries"("studyId", "randomizationNumber");

-- CreateIndex
CREATE INDEX "datatrial_audit_logs_tableName_recordId_idx" ON "datatrial_audit_logs"("tableName", "recordId");

-- CreateIndex
CREATE INDEX "datatrial_audit_logs_userId_idx" ON "datatrial_audit_logs"("userId");

-- CreateIndex
CREATE INDEX "datatrial_audit_logs_createdAt_idx" ON "datatrial_audit_logs"("createdAt");
