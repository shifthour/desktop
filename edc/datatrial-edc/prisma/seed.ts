import { PrismaClient } from "@prisma/client";
import { PrismaBetterSqlite3 } from "@prisma/adapter-better-sqlite3";
import * as bcrypt from "bcryptjs";
import path from "path";

const adapter = new PrismaBetterSqlite3({
  url: `file:${path.join(__dirname, "dev.db")}`,
});

const prisma = new PrismaClient({ adapter });

async function main() {
  console.log("Starting database seed...");

  // Create default permissions
  const permissions = [
    // Study Management
    { permissionCode: "STUDY_CREATE", permissionName: "Create Study", module: "Study" },
    { permissionCode: "STUDY_EDIT", permissionName: "Edit Study", module: "Study" },
    { permissionCode: "STUDY_VIEW", permissionName: "View Study", module: "Study" },
    { permissionCode: "STUDY_DELETE", permissionName: "Delete Study", module: "Study" },

    // Site Management
    { permissionCode: "SITE_CREATE", permissionName: "Create Site", module: "Site" },
    { permissionCode: "SITE_EDIT", permissionName: "Edit Site", module: "Site" },
    { permissionCode: "SITE_VIEW", permissionName: "View Site", module: "Site" },

    // User Management
    { permissionCode: "USER_CREATE", permissionName: "Create User", module: "User" },
    { permissionCode: "USER_EDIT", permissionName: "Edit User", module: "User" },
    { permissionCode: "USER_VIEW", permissionName: "View User", module: "User" },
    { permissionCode: "USER_ASSIGN_ROLE", permissionName: "Assign User Roles", module: "User" },

    // CRF Builder
    { permissionCode: "CRF_DESIGN", permissionName: "Design CRF Forms", module: "CRF" },
    { permissionCode: "CRF_VIEW", permissionName: "View CRF Forms", module: "CRF" },

    // Data Entry
    { permissionCode: "DATA_ENTRY", permissionName: "Enter CRF Data", module: "Data" },
    { permissionCode: "DATA_EDIT", permissionName: "Edit CRF Data", module: "Data" },
    { permissionCode: "DATA_VIEW", permissionName: "View CRF Data", module: "Data" },
    { permissionCode: "DATA_SUBMIT", permissionName: "Submit CRF Data", module: "Data" },

    // SDV (Source Data Verification)
    { permissionCode: "SDV_PERFORM", permissionName: "Perform SDV", module: "SDV" },
    { permissionCode: "SDV_VIEW", permissionName: "View SDV Status", module: "SDV" },

    // Data Management
    { permissionCode: "DM_REVIEW", permissionName: "Data Manager Review", module: "DataManagement" },
    { permissionCode: "DATA_LOCK", permissionName: "Lock/Unlock Data", module: "DataManagement" },
    { permissionCode: "DATA_EXPORT", permissionName: "Export Data", module: "DataManagement" },

    // Query Management
    { permissionCode: "QUERY_CREATE", permissionName: "Create Query", module: "Query" },
    { permissionCode: "QUERY_RESPOND", permissionName: "Respond to Query", module: "Query" },
    { permissionCode: "QUERY_CLOSE", permissionName: "Close Query", module: "Query" },
    { permissionCode: "QUERY_VIEW", permissionName: "View Queries", module: "Query" },

    // Subject Management
    { permissionCode: "SUBJECT_CREATE", permissionName: "Create Subject", module: "Subject" },
    { permissionCode: "SUBJECT_EDIT", permissionName: "Edit Subject", module: "Subject" },
    { permissionCode: "SUBJECT_VIEW", permissionName: "View Subject", module: "Subject" },
    { permissionCode: "SUBJECT_RANDOMIZE", permissionName: "Randomize Subject", module: "Subject" },

    // IWRS
    { permissionCode: "KIT_MANAGE", permissionName: "Manage Kit Definitions", module: "IWRS" },
    { permissionCode: "IP_MANAGE", permissionName: "Manage IP Inventory", module: "IWRS" },
    { permissionCode: "SHIPMENT_CREATE", permissionName: "Create Shipment", module: "IWRS" },
    { permissionCode: "SHIPMENT_RECEIVE", permissionName: "Receive Shipment", module: "IWRS" },
    { permissionCode: "KIT_DISPENSE", permissionName: "Dispense Kit", module: "IWRS" },
    { permissionCode: "KIT_REQUEST", permissionName: "Request Kit", module: "IWRS" },
    { permissionCode: "KIT_REQUEST_APPROVE", permissionName: "Approve Kit Request", module: "IWRS" },

    // Reports
    { permissionCode: "REPORT_VIEW", permissionName: "View Reports", module: "Reports" },
    { permissionCode: "REPORT_EXPORT", permissionName: "Export Reports", module: "Reports" },

    // Audit
    { permissionCode: "AUDIT_VIEW", permissionName: "View Audit Trail", module: "Audit" },

    // E-Signature
    { permissionCode: "ESIGN_PERFORM", permissionName: "Perform E-Signature", module: "ESignature" },

    // QA Review
    { permissionCode: "QA_REVIEW", permissionName: "QA Review", module: "QA" },

    // PI Review
    { permissionCode: "PI_REVIEW", permissionName: "PI Review and Sign", module: "PI" },
  ];

  for (const perm of permissions) {
    await prisma.permission.upsert({
      where: { permissionCode: perm.permissionCode },
      update: {},
      create: perm,
    });
  }
  console.log(`Created ${permissions.length} permissions`);

  // Create default roles
  const roles = [
    {
      roleCode: "ADMIN",
      roleName: "System Administrator",
      description: "Full system access",
      isBlinded: false,
      isSystemRole: true,
      permissions: permissions.map(p => p.permissionCode),
    },
    {
      roleCode: "PROJECT_MANAGER",
      roleName: "Project Manager",
      description: "Manages study setup, sites, and users",
      isBlinded: true,
      isSystemRole: false,
      permissions: [
        "STUDY_CREATE", "STUDY_EDIT", "STUDY_VIEW",
        "SITE_CREATE", "SITE_EDIT", "SITE_VIEW",
        "USER_CREATE", "USER_EDIT", "USER_VIEW", "USER_ASSIGN_ROLE",
        "CRF_DESIGN", "CRF_VIEW",
        "SUBJECT_VIEW",
        "DATA_VIEW", "SDV_VIEW",
        "QUERY_VIEW",
        "KIT_MANAGE", "KIT_REQUEST_APPROVE",
        "REPORT_VIEW", "REPORT_EXPORT",
        "AUDIT_VIEW",
      ],
    },
    {
      roleCode: "DATA_MANAGER",
      roleName: "Data Manager",
      description: "Reviews data quality and manages data flow",
      isBlinded: true,
      isSystemRole: false,
      permissions: [
        "STUDY_VIEW", "SITE_VIEW",
        "CRF_VIEW",
        "DATA_VIEW", "DM_REVIEW", "DATA_LOCK", "DATA_EXPORT",
        "QUERY_CREATE", "QUERY_CLOSE", "QUERY_VIEW",
        "SUBJECT_VIEW",
        "REPORT_VIEW", "REPORT_EXPORT",
        "AUDIT_VIEW",
      ],
    },
    {
      roleCode: "CRA",
      roleName: "Clinical Research Associate",
      description: "Monitors sites and performs SDV",
      isBlinded: true,
      isSystemRole: false,
      permissions: [
        "STUDY_VIEW", "SITE_VIEW",
        "CRF_VIEW",
        "DATA_VIEW", "SDV_PERFORM", "SDV_VIEW",
        "QUERY_CREATE", "QUERY_VIEW",
        "SUBJECT_VIEW",
        "KIT_REQUEST",
        "REPORT_VIEW",
        "AUDIT_VIEW",
      ],
    },
    {
      roleCode: "SITE_PI",
      roleName: "Principal Investigator",
      description: "Site principal investigator",
      isBlinded: true,
      isSystemRole: false,
      permissions: [
        "STUDY_VIEW", "SITE_VIEW",
        "CRF_VIEW",
        "DATA_VIEW", "DATA_ENTRY", "DATA_EDIT", "DATA_SUBMIT",
        "PI_REVIEW",
        "QUERY_RESPOND", "QUERY_VIEW",
        "SUBJECT_CREATE", "SUBJECT_EDIT", "SUBJECT_VIEW", "SUBJECT_RANDOMIZE",
        "KIT_DISPENSE", "SHIPMENT_RECEIVE",
        "ESIGN_PERFORM",
        "REPORT_VIEW",
      ],
    },
    {
      roleCode: "SITE_CRC",
      roleName: "Site Coordinator",
      description: "Site clinical research coordinator",
      isBlinded: true,
      isSystemRole: false,
      permissions: [
        "STUDY_VIEW", "SITE_VIEW",
        "CRF_VIEW",
        "DATA_VIEW", "DATA_ENTRY", "DATA_EDIT", "DATA_SUBMIT",
        "QUERY_RESPOND", "QUERY_VIEW",
        "SUBJECT_CREATE", "SUBJECT_EDIT", "SUBJECT_VIEW", "SUBJECT_RANDOMIZE",
        "KIT_DISPENSE", "SHIPMENT_RECEIVE",
        "REPORT_VIEW",
      ],
    },
    {
      roleCode: "MEDICAL_MONITOR",
      roleName: "Medical Monitor",
      description: "Reviews medical eligibility and safety",
      isBlinded: true,
      isSystemRole: false,
      permissions: [
        "STUDY_VIEW", "SITE_VIEW",
        "CRF_VIEW",
        "DATA_VIEW",
        "QUERY_CREATE", "QUERY_VIEW",
        "SUBJECT_VIEW",
        "REPORT_VIEW",
      ],
    },
    {
      roleCode: "DEPOT_MANAGER",
      roleName: "Depot Manager",
      description: "Manages drug inventory and shipments (Unblinded)",
      isBlinded: false,
      isSystemRole: false,
      permissions: [
        "STUDY_VIEW", "SITE_VIEW",
        "KIT_MANAGE", "IP_MANAGE",
        "SHIPMENT_CREATE", "SHIPMENT_RECEIVE",
        "KIT_REQUEST_APPROVE",
        "REPORT_VIEW", "REPORT_EXPORT",
      ],
    },
    {
      roleCode: "SPONSOR",
      roleName: "Sponsor",
      description: "View-only access to study progress",
      isBlinded: true,
      isSystemRole: false,
      permissions: [
        "STUDY_VIEW", "SITE_VIEW",
        "CRF_VIEW",
        "DATA_VIEW",
        "SUBJECT_VIEW",
        "REPORT_VIEW",
        "AUDIT_VIEW",
      ],
    },
    {
      roleCode: "QA",
      roleName: "Quality Assurance",
      description: "QA review of data",
      isBlinded: true,
      isSystemRole: false,
      permissions: [
        "STUDY_VIEW", "SITE_VIEW",
        "CRF_VIEW",
        "DATA_VIEW", "QA_REVIEW",
        "QUERY_CREATE", "QUERY_VIEW",
        "SUBJECT_VIEW",
        "REPORT_VIEW",
        "AUDIT_VIEW",
      ],
    },
    {
      roleCode: "LAB",
      roleName: "Lab Personnel",
      description: "Lab data entry and viewing",
      isBlinded: true,
      isSystemRole: false,
      permissions: [
        "STUDY_VIEW", "SITE_VIEW",
        "CRF_VIEW",
        "DATA_VIEW", "DATA_ENTRY",
        "SUBJECT_VIEW",
      ],
    },
  ];

  for (const roleData of roles) {
    const { permissions: rolePermissions, ...roleInfo } = roleData;

    const role = await prisma.role.upsert({
      where: { roleCode: roleInfo.roleCode },
      update: roleInfo,
      create: roleInfo,
    });

    // Assign permissions to role
    for (const permCode of rolePermissions) {
      const permission = await prisma.permission.findUnique({
        where: { permissionCode: permCode },
      });

      if (permission) {
        await prisma.rolePermission.upsert({
          where: {
            roleId_permissionId: {
              roleId: role.id,
              permissionId: permission.id,
            },
          },
          update: {},
          create: {
            roleId: role.id,
            permissionId: permission.id,
          },
        });
      }
    }
  }
  console.log(`Created ${roles.length} roles with permissions`);

  // Create default admin user
  const hashedPassword = await bcrypt.hash("Admin@123", 10);

  await prisma.user.upsert({
    where: { email: "admin@datatrial.com" },
    update: {},
    create: {
      email: "admin@datatrial.com",
      password: hashedPassword,
      firstName: "System",
      lastName: "Administrator",
      organization: "DataTrial",
      isActive: true,
      mustChangePassword: true,
    },
  });
  console.log("Created admin user: admin@datatrial.com / Admin@123");

  console.log("Database seeding completed!");
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
