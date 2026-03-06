import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/lib/auth";
import prisma from "@/lib/prisma";
import { createAuditLog } from "@/lib/audit";

// GET /api/crf/forms - List all CRF forms for a study
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

    const forms = await prisma.cRFForm.findMany({
      where: { studyId },
      orderBy: { sequenceNumber: "asc" },
      include: {
        _count: {
          select: { fields: true, visitForms: true },
        },
        visitForms: {
          include: {
            visit: {
              select: { visitCode: true, visitName: true },
            },
          },
        },
      },
    });

    return NextResponse.json(forms);
  } catch (error) {
    console.error("Error fetching CRF forms:", error);
    return NextResponse.json(
      { error: "Failed to fetch CRF forms" },
      { status: 500 }
    );
  }
}

// POST /api/crf/forms - Create a new CRF form
export async function POST(request: Request) {
  try {
    const session = await getServerSession(authOptions);
    if (!session?.user) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const body = await request.json();
    const {
      studyId,
      formCode,
      formName,
      formType,
      isRepeating,
      isLogForm,
      sequenceNumber,
    } = body;

    // Validate required fields
    if (!studyId || !formCode || !formName) {
      return NextResponse.json(
        { error: "Study ID, form code, and form name are required" },
        { status: 400 }
      );
    }

    // Check if form code already exists in this study
    const existing = await prisma.cRFForm.findUnique({
      where: {
        studyId_formCode: { studyId, formCode },
      },
    });

    if (existing) {
      return NextResponse.json(
        { error: "Form code already exists in this study" },
        { status: 400 }
      );
    }

    // Get next sequence number if not provided
    let seq = sequenceNumber;
    if (!seq) {
      const lastForm = await prisma.cRFForm.findFirst({
        where: { studyId },
        orderBy: { sequenceNumber: "desc" },
      });
      seq = (lastForm?.sequenceNumber || 0) + 1;
    }

    const form = await prisma.cRFForm.create({
      data: {
        studyId,
        formCode,
        formName,
        formType: formType || "Simple CRF",
        isRepeating: isRepeating || false,
        isLogForm: isLogForm || false,
        sequenceNumber: seq,
        isActive: true,
      },
    });

    // Create audit log
    const userId = (session.user as { id?: string }).id;
    await createAuditLog({
      tableName: "CRFForm",
      recordId: form.id,
      action: "CREATE",
      newValues: form as unknown as Record<string, unknown>,
      userId: userId,
      userEmail: session.user.email || undefined,
    });

    return NextResponse.json(form, { status: 201 });
  } catch (error) {
    console.error("Error creating CRF form:", error);
    return NextResponse.json(
      { error: "Failed to create CRF form" },
      { status: 500 }
    );
  }
}
