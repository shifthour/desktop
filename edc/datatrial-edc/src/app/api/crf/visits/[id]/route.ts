import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/lib/auth";
import prisma from "@/lib/prisma";
import { createAuditLog } from "@/lib/audit";

// GET /api/crf/visits/[id] - Get single visit
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

    const visit = await prisma.visit.findUnique({
      where: { id },
      include: {
        visitForms: {
          include: {
            form: {
              select: {
                id: true,
                formCode: true,
                formName: true,
              },
            },
          },
          orderBy: { sequenceNumber: "asc" },
        },
        _count: {
          select: { subjectVisits: true },
        },
      },
    });

    if (!visit) {
      return NextResponse.json({ error: "Visit not found" }, { status: 404 });
    }

    return NextResponse.json(visit);
  } catch (error) {
    console.error("Error fetching visit:", error);
    return NextResponse.json(
      { error: "Failed to fetch visit" },
      { status: 500 }
    );
  }
}

// PUT /api/crf/visits/[id] - Update visit
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
      visitName,
      visitType,
      durationDays,
      windowBefore,
      windowAfter,
      isSchedulable,
      isActive,
      sequenceNumber,
      formIds,
    } = body;

    // Get existing visit for audit
    const existingVisit = await prisma.visit.findUnique({
      where: { id },
      include: {
        visitForms: true,
      },
    });

    if (!existingVisit) {
      return NextResponse.json({ error: "Visit not found" }, { status: 404 });
    }

    // Update visit and form associations in a transaction
    const updatedVisit = await prisma.$transaction(async (tx) => {
      // Update visit
      const visit = await tx.visit.update({
        where: { id },
        data: {
          visitName: visitName ?? existingVisit.visitName,
          visitType: visitType ?? existingVisit.visitType,
          durationDays: durationDays ?? existingVisit.durationDays,
          windowBefore: windowBefore ?? existingVisit.windowBefore,
          windowAfter: windowAfter ?? existingVisit.windowAfter,
          isSchedulable: isSchedulable ?? existingVisit.isSchedulable,
          isActive: isActive ?? existingVisit.isActive,
          sequenceNumber: sequenceNumber ?? existingVisit.sequenceNumber,
        },
      });

      // Update form associations if provided
      if (formIds && Array.isArray(formIds)) {
        // Remove existing associations
        await tx.visitForm.deleteMany({
          where: { visitId: id },
        });

        // Create new associations
        for (let i = 0; i < formIds.length; i++) {
          await tx.visitForm.create({
            data: {
              visitId: id,
              formId: formIds[i],
              isRequired: true,
              sequenceNumber: i + 1,
            },
          });
        }
      }

      return visit;
    });

    // Create audit log
    const userId = (session.user as { id?: string }).id;
    await createAuditLog({
      tableName: "Visit",
      recordId: id,
      action: "UPDATE",
      oldValues: existingVisit as unknown as Record<string, unknown>,
      newValues: { ...updatedVisit, formIds } as unknown as Record<string, unknown>,
      userId: userId,
      userEmail: session.user.email || undefined,
    });

    return NextResponse.json(updatedVisit);
  } catch (error) {
    console.error("Error updating visit:", error);
    return NextResponse.json(
      { error: "Failed to update visit" },
      { status: 500 }
    );
  }
}

// DELETE /api/crf/visits/[id] - Soft delete visit
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

    // Check if visit has subject visits (cannot delete if has data)
    const visitWithData = await prisma.visit.findUnique({
      where: { id },
      include: {
        _count: {
          select: { subjectVisits: true },
        },
      },
    });

    if (!visitWithData) {
      return NextResponse.json({ error: "Visit not found" }, { status: 404 });
    }

    if (visitWithData._count.subjectVisits > 0) {
      return NextResponse.json(
        { error: "Cannot delete visit with existing subject visits" },
        { status: 400 }
      );
    }

    // Soft delete - set isActive to false
    await prisma.visit.update({
      where: { id },
      data: { isActive: false },
    });

    // Create audit log
    const userId = (session.user as { id?: string }).id;
    await createAuditLog({
      tableName: "Visit",
      recordId: id,
      action: "DELETE",
      oldValues: visitWithData as unknown as Record<string, unknown>,
      userId: userId,
      userEmail: session.user.email || undefined,
    });

    return NextResponse.json({ message: "Visit deleted successfully" });
  } catch (error) {
    console.error("Error deleting visit:", error);
    return NextResponse.json(
      { error: "Failed to delete visit" },
      { status: 500 }
    );
  }
}
