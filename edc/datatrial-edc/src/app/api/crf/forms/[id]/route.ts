import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/lib/auth";
import prisma from "@/lib/prisma";
import { createAuditLog } from "@/lib/audit";

// GET /api/crf/forms/[id] - Get single form with details
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

    const form = await prisma.cRFForm.findUnique({
      where: { id },
      include: {
        study: {
          select: {
            id: true,
            studyCode: true,
            studyName: true,
          },
        },
        fields: {
          where: { isActive: true },
          orderBy: { sequenceNumber: "asc" },
          include: {
            lovList: {
              select: {
                listCode: true,
                listName: true,
              },
            },
          },
        },
        visitForms: {
          include: {
            visit: {
              select: {
                visitCode: true,
                visitName: true,
              },
            },
          },
        },
        _count: {
          select: {
            fields: true,
            visitForms: true,
          },
        },
      },
    });

    if (!form) {
      return NextResponse.json({ error: "Form not found" }, { status: 404 });
    }

    return NextResponse.json(form);
  } catch (error) {
    console.error("Error fetching form:", error);
    return NextResponse.json(
      { error: "Failed to fetch form" },
      { status: 500 }
    );
  }
}

// PUT /api/crf/forms/[id] - Update form
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
    const { formName, formType, isRepeating, isLogForm, isActive, sequenceNumber } = body;

    // Get existing form for audit
    const existingForm = await prisma.cRFForm.findUnique({
      where: { id },
    });

    if (!existingForm) {
      return NextResponse.json({ error: "Form not found" }, { status: 404 });
    }

    const updatedForm = await prisma.cRFForm.update({
      where: { id },
      data: {
        formName: formName ?? existingForm.formName,
        formType: formType ?? existingForm.formType,
        isRepeating: isRepeating ?? existingForm.isRepeating,
        isLogForm: isLogForm ?? existingForm.isLogForm,
        isActive: isActive ?? existingForm.isActive,
        sequenceNumber: sequenceNumber ?? existingForm.sequenceNumber,
      },
    });

    // Create audit log
    const userId = (session.user as { id?: string }).id;
    await createAuditLog({
      tableName: "CRFForm",
      recordId: id,
      action: "UPDATE",
      oldValues: existingForm as unknown as Record<string, unknown>,
      newValues: updatedForm as unknown as Record<string, unknown>,
      userId: userId,
      userEmail: session.user.email || undefined,
    });

    return NextResponse.json(updatedForm);
  } catch (error) {
    console.error("Error updating form:", error);
    return NextResponse.json(
      { error: "Failed to update form" },
      { status: 500 }
    );
  }
}

// DELETE /api/crf/forms/[id] - Soft delete form
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

    // Check if form has data entries (cannot delete if has data)
    const formWithData = await prisma.cRFForm.findUnique({
      where: { id },
      include: {
        _count: {
          select: { dataEntries: true },
        },
      },
    });

    if (!formWithData) {
      return NextResponse.json({ error: "Form not found" }, { status: 404 });
    }

    if (formWithData._count.dataEntries > 0) {
      return NextResponse.json(
        { error: "Cannot delete form with existing data entries" },
        { status: 400 }
      );
    }

    // Soft delete - set isActive to false
    const deletedForm = await prisma.cRFForm.update({
      where: { id },
      data: { isActive: false },
    });

    // Create audit log
    const userId = (session.user as { id?: string }).id;
    await createAuditLog({
      tableName: "CRFForm",
      recordId: id,
      action: "DELETE",
      oldValues: formWithData as unknown as Record<string, unknown>,
      userId: userId,
      userEmail: session.user.email || undefined,
    });

    return NextResponse.json({ message: "Form deleted successfully" });
  } catch (error) {
    console.error("Error deleting form:", error);
    return NextResponse.json(
      { error: "Failed to delete form" },
      { status: 500 }
    );
  }
}
