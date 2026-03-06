import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/lib/auth";
import prisma from "@/lib/prisma";
import { createAuditLog } from "@/lib/audit";

// GET /api/sites - List all sites (optionally filter by study)
export async function GET(request: Request) {
  try {
    const session = await getServerSession(authOptions);
    if (!session?.user) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const { searchParams } = new URL(request.url);
    const studyId = searchParams.get("studyId");

    const where = studyId ? { studyId } : {};

    const sites = await prisma.site.findMany({
      where,
      orderBy: { siteCode: "asc" },
      include: {
        study: {
          select: {
            studyCode: true,
            studyName: true,
          },
        },
        _count: {
          select: {
            subjects: true,
            users: true,
            shipments: true,
          },
        },
      },
    });

    return NextResponse.json(sites);
  } catch (error) {
    console.error("Error fetching sites:", error);
    return NextResponse.json(
      { error: "Failed to fetch sites" },
      { status: 500 }
    );
  }
}

// POST /api/sites - Create a new site
export async function POST(request: Request) {
  try {
    const session = await getServerSession(authOptions);
    if (!session?.user) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const body = await request.json();
    const {
      studyId,
      siteCode,
      siteName,
      address,
      city,
      state,
      country,
      postalCode,
      phone,
      email,
      principalInvestigator,
      targetEnrollment,
    } = body;

    // Validate required fields
    if (!studyId || !siteCode || !siteName) {
      return NextResponse.json(
        { error: "Study ID, site code, and site name are required" },
        { status: 400 }
      );
    }

    // Check if site code already exists in this study
    const existing = await prisma.site.findUnique({
      where: {
        studyId_siteCode: { studyId, siteCode },
      },
    });

    if (existing) {
      return NextResponse.json(
        { error: "Site code already exists in this study" },
        { status: 400 }
      );
    }

    const site = await prisma.site.create({
      data: {
        studyId,
        siteCode,
        siteName,
        address: address || null,
        city: city || null,
        state: state || null,
        country: country || null,
        postalCode: postalCode || null,
        phone: phone || null,
        email: email || null,
        principalInvestigator: principalInvestigator || null,
        targetEnrollment: targetEnrollment || null,
        status: "Active",
        isActive: true,
      },
      include: {
        study: {
          select: {
            studyCode: true,
            studyName: true,
          },
        },
      },
    });

    // Create audit log
    const userId = (session.user as { id?: string }).id;
    await createAuditLog({
      tableName: "Site",
      recordId: site.id,
      action: "CREATE",
      newValues: site as unknown as Record<string, unknown>,
      userId: userId,
      userEmail: session.user.email || undefined,
    });

    return NextResponse.json(site, { status: 201 });
  } catch (error) {
    console.error("Error creating site:", error);
    return NextResponse.json(
      { error: "Failed to create site" },
      { status: 500 }
    );
  }
}
