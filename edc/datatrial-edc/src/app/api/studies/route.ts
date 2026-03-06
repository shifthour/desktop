import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/lib/auth";
import prisma from "@/lib/prisma";
import { createAuditLog } from "@/lib/audit";

// GET /api/studies - List all studies
export async function GET() {
  try {
    const session = await getServerSession(authOptions);
    if (!session?.user) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const studies = await prisma.study.findMany({
      orderBy: { createdAt: "desc" },
      include: {
        _count: {
          select: {
            sites: true,
            subjects: true,
          },
        },
      },
    });

    return NextResponse.json(studies);
  } catch (error) {
    console.error("Error fetching studies:", error);
    return NextResponse.json(
      { error: "Failed to fetch studies" },
      { status: 500 }
    );
  }
}

// POST /api/studies - Create a new study
export async function POST(request: Request) {
  try {
    const session = await getServerSession(authOptions);
    if (!session?.user) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const body = await request.json();
    const {
      studyCode,
      studyName,
      protocolNumber,
      sponsorName,
      phase,
      therapeuticArea,
      indication,
      startDate,
      endDate,
    } = body;

    // Validate required fields
    if (!studyCode || !studyName) {
      return NextResponse.json(
        { error: "Study code and name are required" },
        { status: 400 }
      );
    }

    // Check if study code already exists
    const existing = await prisma.study.findUnique({
      where: { studyCode },
    });

    if (existing) {
      return NextResponse.json(
        { error: "Study code already exists" },
        { status: 400 }
      );
    }

    const userId = (session.user as { id?: string }).id;

    const study = await prisma.study.create({
      data: {
        studyCode,
        studyName,
        protocolNumber,
        sponsorName,
        phase,
        therapeuticArea,
        indication,
        startDate: startDate ? new Date(startDate) : null,
        endDate: endDate ? new Date(endDate) : null,
        status: "draft",
        createdById: userId,
      },
    });

    // Create audit log
    await createAuditLog({
      tableName: "Study",
      recordId: study.id,
      action: "CREATE",
      newValues: study as unknown as Record<string, unknown>,
      userId: userId,
      userEmail: session.user.email || undefined,
    });

    return NextResponse.json(study, { status: 201 });
  } catch (error) {
    console.error("Error creating study:", error);
    return NextResponse.json(
      { error: "Failed to create study" },
      { status: 500 }
    );
  }
}
