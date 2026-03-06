import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/lib/auth";
import prisma from "@/lib/prisma";
import { createAuditLog } from "@/lib/audit";

// Field types as per FSD: SELECT LIST, NUMBER, TEXT, DATE
const VALID_FIELD_TYPES = ["TEXT", "NUMBER", "DATE", "SELECT_LIST", "TEXTAREA", "CHECKBOX", "RADIO"];
const VALID_DATA_TYPES = ["STRING", "INTEGER", "DECIMAL", "DATE", "DATETIME", "BOOLEAN"];

// GET /api/crf/fields - List all fields for a form
export async function GET(request: Request) {
  try {
    const session = await getServerSession(authOptions);
    if (!session?.user) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const { searchParams } = new URL(request.url);
    const formId = searchParams.get("formId");

    if (!formId) {
      return NextResponse.json(
        { error: "Form ID is required" },
        { status: 400 }
      );
    }

    const fields = await prisma.cRFField.findMany({
      where: { formId },
      orderBy: [{ rowNumber: "asc" }, { sequenceNumber: "asc" }],
      include: {
        lovList: {
          include: {
            items: {
              where: { isActive: true },
              orderBy: { sequenceNumber: "asc" },
            },
          },
        },
      },
    });

    return NextResponse.json(fields);
  } catch (error) {
    console.error("Error fetching CRF fields:", error);
    return NextResponse.json(
      { error: "Failed to fetch CRF fields" },
      { status: 500 }
    );
  }
}

// POST /api/crf/fields - Create a new CRF field
export async function POST(request: Request) {
  try {
    const session = await getServerSession(authOptions);
    if (!session?.user) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const body = await request.json();
    const {
      formId,
      fieldCode,
      fieldLabel,
      fieldType,
      dataType,
      isRequired,
      isReadonly,
      isHidden,
      defaultValue,
      placeholder,
      helpText,
      minValue,
      maxValue,
      minLength,
      maxLength,
      regexPattern,
      decimalPlaces,
      unit,
      rowNumber,
      columnNumber,
      colspan,
      lovListId,
    } = body;

    // Validate required fields
    if (!formId || !fieldCode || !fieldLabel || !fieldType) {
      return NextResponse.json(
        { error: "Form ID, field code, label, and type are required" },
        { status: 400 }
      );
    }

    // Validate field type
    if (!VALID_FIELD_TYPES.includes(fieldType)) {
      return NextResponse.json(
        { error: `Invalid field type. Must be one of: ${VALID_FIELD_TYPES.join(", ")}` },
        { status: 400 }
      );
    }

    // Check if field code already exists in this form
    const existing = await prisma.cRFField.findUnique({
      where: {
        formId_fieldCode: { formId, fieldCode },
      },
    });

    if (existing) {
      return NextResponse.json(
        { error: "Field code already exists in this form" },
        { status: 400 }
      );
    }

    // Get next sequence number
    const lastField = await prisma.cRFField.findFirst({
      where: { formId },
      orderBy: { sequenceNumber: "desc" },
    });
    const sequenceNumber = (lastField?.sequenceNumber || 0) + 1;

    const field = await prisma.cRFField.create({
      data: {
        formId,
        fieldCode,
        fieldLabel,
        fieldType,
        dataType: dataType || "STRING",
        isRequired: isRequired || false,
        isReadonly: isReadonly || false,
        isHidden: isHidden || false,
        defaultValue,
        placeholder,
        helpText,
        minValue: minValue ? parseFloat(minValue) : null,
        maxValue: maxValue ? parseFloat(maxValue) : null,
        minLength: minLength ? parseInt(minLength) : null,
        maxLength: maxLength ? parseInt(maxLength) : null,
        regexPattern,
        decimalPlaces: decimalPlaces ? parseInt(decimalPlaces) : null,
        unit,
        sequenceNumber,
        rowNumber: rowNumber || 1,
        columnNumber: columnNumber || 1,
        colspan: colspan || 1,
        lovListId: lovListId || null,
        isActive: true,
      },
    });

    // Create audit log
    const userId = (session.user as { id?: string }).id;
    await createAuditLog({
      tableName: "CRFField",
      recordId: field.id,
      action: "CREATE",
      newValues: field as unknown as Record<string, unknown>,
      userId: userId,
      userEmail: session.user.email || undefined,
    });

    return NextResponse.json(field, { status: 201 });
  } catch (error) {
    console.error("Error creating CRF field:", error);
    return NextResponse.json(
      { error: "Failed to create CRF field" },
      { status: 500 }
    );
  }
}
