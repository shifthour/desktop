import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/lib/auth";
import prisma from "@/lib/prisma";
import { createAuditLog } from "@/lib/audit";

// Visit types as per FSD: First Visit, Visit, Common Forms, Unscheduled
const VALID_VISIT_TYPES = ["First Visit", "Visit", "Common Forms", "Unscheduled", "Screening", "Randomization", "Treatment", "Follow-up", "End of Study"];

// GET /api/crf/visits - List all visits for a study
export async function GET(request: Request) {
  try {
    const session = await getServerSession(authOptions);
    if (!session?.user) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const { searchParams } = new URL(request.url);
    const studyId = searchParams.get("studyId");

    if (!studyId) {
      return NextResponse.json(
        { error: "Study ID is required" },
        { status: 400 }
      );
    }

    const visits = await prisma.visit.findMany({
      where: { studyId },
      orderBy: { sequenceNumber: "asc" },
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

    return NextResponse.json(visits);
  } catch (error) {
    console.error("Error fetching visits:", error);
    return NextResponse.json(
      { error: "Failed to fetch visits" },
      { status: 500 }
    );
  }
}

// POST /api/crf/visits - Create a new visit
export async function POST(request: Request) {
  try {
    const session = await getServerSession(authOptions);
    if (!session?.user) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const body = await request.json();
    const {
      studyId,
      visitCode,
      visitName,
      visitType,
      durationDays,
      windowBefore,
      windowAfter,
      isSchedulable,
      formIds, // Array of form IDs to associate
    } = body;

    // Validate required fields
    if (!studyId || !visitCode || !visitName || !visitType) {
      return NextResponse.json(
        { error: "Study ID, visit code, name, and type are required" },
        { status: 400 }
      );
    }

    // Validate visit type
    if (!VALID_VISIT_TYPES.includes(visitType)) {
      return NextResponse.json(
        { error: `Invalid visit type. Must be one of: ${VALID_VISIT_TYPES.join(", ")}` },
        { status: 400 }
      );
    }

    // Check if visit code already exists in this study
    const existing = await prisma.visit.findUnique({
      where: {
        studyId_visitCode: { studyId, visitCode },
      },
    });

    if (existing) {
      return NextResponse.json(
        { error: "Visit code already exists in this study" },
        { status: 400 }
      );
    }

    // Get next sequence number
    const lastVisit = await prisma.visit.findFirst({
      where: { studyId },
      orderBy: { sequenceNumber: "desc" },
    });
    const sequenceNumber = (lastVisit?.sequenceNumber || 0) + 1;

    // Create visit with associated forms in a transaction
    const visit = await prisma.$transaction(async (tx) => {
      const newVisit = await tx.visit.create({
        data: {
          studyId,
          visitCode,
          visitName,
          visitType,
          sequenceNumber,
          durationDays: durationDays || 0,
          windowBefore: windowBefore || 0,
          windowAfter: windowAfter || 0,
          isSchedulable: isSchedulable !== false,
          isActive: true,
        },
      });

      // Associate forms if provided
      if (formIds && Array.isArray(formIds) && formIds.length > 0) {
        for (let i = 0; i < formIds.length; i++) {
          await tx.visitForm.create({
            data: {
              visitId: newVisit.id,
              formId: formIds[i],
              isRequired: true,
              sequenceNumber: i + 1,
            },
          });
        }
      }

      return newVisit;
    });

    // Create audit log
    const userId = (session.user as { id?: string }).id;
    await createAuditLog({
      tableName: "Visit",
      recordId: visit.id,
      action: "CREATE",
      newValues: { ...visit, formIds } as unknown as Record<string, unknown>,
      userId: userId,
      userEmail: session.user.email || undefined,
    });

    return NextResponse.json(visit, { status: 201 });
  } catch (error) {
    console.error("Error creating visit:", error);
    return NextResponse.json(
      { error: "Failed to create visit" },
      { status: 500 }
    );
  }
}
