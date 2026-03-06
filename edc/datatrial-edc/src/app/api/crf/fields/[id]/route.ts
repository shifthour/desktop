import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/lib/auth";
import prisma from "@/lib/prisma";
import { createAuditLog } from "@/lib/audit";

// GET /api/crf/fields/[id] - Get single field
export async function GET(
  request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const session = await getServerSession(authOptions);
    if (!session?.user) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const { id } = await params;

    const field = await prisma.cRFField.findUnique({
      where: { id },
      include: {
        lovList: {
          include: {
            items: {
              where: { isActive: true },
              orderBy: { sequenceNumber: "asc" },
            },
          },
        },
        form: {
          select: {
            formCode: true,
            formName: true,
          },
        },
      },
    });

    if (!field) {
      return NextResponse.json({ error: "Field not found" }, { status: 404 });
    }

    return NextResponse.json(field);
  } catch (error) {
    console.error("Error fetching field:", error);
    return NextResponse.json(
      { error: "Failed to fetch field" },
      { status: 500 }
    );
  }
}

// PUT /api/crf/fields/[id] - Update field
export async function PUT(
  request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const session = await getServerSession(authOptions);
    if (!session?.user) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const { id } = await params;
    const body = await request.json();
    const {
      fieldLabel,
      fieldType,
      isRequired,
      maxLength,
      minValue,
      maxValue,
      decimalPlaces,
      regexPattern,
      lovListId,
      isActive,
      sequenceNumber,
    } = body;

    // Get existing field for audit
    const existingField = await prisma.cRFField.findUnique({
      where: { id },
    });

    if (!existingField) {
      return NextResponse.json({ error: "Field not found" }, { status: 404 });
    }

    // Validate LOV list if SELECT_LIST
    const newFieldType = fieldType ?? existingField.fieldType;
    const newLovListId = lovListId ?? existingField.lovListId;

    if (newFieldType === "SELECT_LIST" && !newLovListId) {
      return NextResponse.json(
        { error: "LOV list is required for SELECT_LIST field type" },
        { status: 400 }
      );
    }

    const updatedField = await prisma.cRFField.update({
      where: { id },
      data: {
        fieldLabel: fieldLabel ?? existingField.fieldLabel,
        fieldType: newFieldType,
        isRequired: isRequired ?? existingField.isRequired,
        maxLength: maxLength !== undefined ? maxLength : existingField.maxLength,
        minValue: minValue !== undefined ? minValue : existingField.minValue,
        maxValue: maxValue !== undefined ? maxValue : existingField.maxValue,
        decimalPlaces: decimalPlaces !== undefined ? decimalPlaces : existingField.decimalPlaces,
        regexPattern: regexPattern !== undefined ? regexPattern : existingField.regexPattern,
        lovListId: newFieldType === "SELECT_LIST" ? newLovListId : null,
        isActive: isActive ?? existingField.isActive,
        sequenceNumber: sequenceNumber ?? existingField.sequenceNumber,
      },
      include: {
        lovList: {
          select: {
            listCode: true,
            listName: true,
          },
        },
      },
    });

    // Create audit log
    const userId = (session.user as { id?: string }).id;
    await createAuditLog({
      tableName: "CRFField",
      recordId: id,
      action: "UPDATE",
      oldValues: existingField as unknown as Record<string, unknown>,
      newValues: updatedField as unknown as Record<string, unknown>,
      userId: userId,
      userEmail: session.user.email || undefined,
    });

    return NextResponse.json(updatedField);
  } catch (error) {
    console.error("Error updating field:", error);
    return NextResponse.json(
      { error: "Failed to update field" },
      { status: 500 }
    );
  }
}

// DELETE /api/crf/fields/[id] - Soft delete field
export async function DELETE(
  request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const session = await getServerSession(authOptions);
    if (!session?.user) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const { id } = await params;

    // Check if field has data entries (cannot delete if has data)
    const fieldWithData = await prisma.cRFField.findUnique({
      where: { id },
      include: {
        _count: {
          select: { dataEntries: true },
        },
      },
    });

    if (!fieldWithData) {
      return NextResponse.json({ error: "Field not found" }, { status: 404 });
    }

    if (fieldWithData._count.dataEntries > 0) {
      return NextResponse.json(
        { error: "Cannot delete field with existing data entries" },
        { status: 400 }
      );
    }

    // Soft delete - set isActive to false
    await prisma.cRFField.update({
      where: { id },
      data: { isActive: false },
    });

    // Create audit log
    const userId = (session.user as { id?: string }).id;
    await createAuditLog({
      tableName: "CRFField",
      recordId: id,
      action: "DELETE",
      oldValues: fieldWithData as unknown as Record<string, unknown>,
      userId: userId,
      userEmail: session.user.email || undefined,
    });

    return NextResponse.json({ message: "Field deleted successfully" });
  } catch (error) {
    console.error("Error deleting field:", error);
    return NextResponse.json(
      { error: "Failed to delete field" },
      { status: 500 }
    );
  }
}
