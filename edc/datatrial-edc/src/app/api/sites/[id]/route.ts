import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/lib/auth";
import prisma from "@/lib/prisma";
import { createAuditLog } from "@/lib/audit";

// GET /api/sites/[id] - Get single site
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

    const site = await prisma.site.findUnique({
      where: { id },
      include: {
        study: {
          select: {
            id: true,
            studyCode: true,
            studyName: true,
          },
        },
        users: {
          select: {
            id: true,
            firstName: true,
            lastName: true,
            email: true,
            role: {
              select: { roleName: true },
            },
          },
        },
        _count: {
          select: {
            subjects: true,
            shipments: true,
            ipInventory: true,
          },
        },
      },
    });

    if (!site) {
      return NextResponse.json({ error: "Site not found" }, { status: 404 });
    }

    return NextResponse.json(site);
  } catch (error) {
    console.error("Error fetching site:", error);
    return NextResponse.json(
      { error: "Failed to fetch site" },
      { status: 500 }
    );
  }
}

// PUT /api/sites/[id] - Update site
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
      status,
      isActive,
    } = body;

    const existingSite = await prisma.site.findUnique({
      where: { id },
    });

    if (!existingSite) {
      return NextResponse.json({ error: "Site not found" }, { status: 404 });
    }

    const updatedSite = await prisma.site.update({
      where: { id },
      data: {
        siteName: siteName ?? existingSite.siteName,
        address: address !== undefined ? address : existingSite.address,
        city: city !== undefined ? city : existingSite.city,
        state: state !== undefined ? state : existingSite.state,
        country: country !== undefined ? country : existingSite.country,
        postalCode: postalCode !== undefined ? postalCode : existingSite.postalCode,
        phone: phone !== undefined ? phone : existingSite.phone,
        email: email !== undefined ? email : existingSite.email,
        principalInvestigator: principalInvestigator !== undefined ? principalInvestigator : existingSite.principalInvestigator,
        targetEnrollment: targetEnrollment !== undefined ? targetEnrollment : existingSite.targetEnrollment,
        status: status ?? existingSite.status,
        isActive: isActive ?? existingSite.isActive,
      },
    });

    // Create audit log
    const userId = (session.user as { id?: string }).id;
    await createAuditLog({
      tableName: "Site",
      recordId: id,
      action: "UPDATE",
      oldValues: existingSite as unknown as Record<string, unknown>,
      newValues: updatedSite as unknown as Record<string, unknown>,
      userId: userId,
      userEmail: session.user.email || undefined,
    });

    return NextResponse.json(updatedSite);
  } catch (error) {
    console.error("Error updating site:", error);
    return NextResponse.json(
      { error: "Failed to update site" },
      { status: 500 }
    );
  }
}

// DELETE /api/sites/[id] - Soft delete site
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

    const siteWithData = await prisma.site.findUnique({
      where: { id },
      include: {
        _count: {
          select: { subjects: true },
        },
      },
    });

    if (!siteWithData) {
      return NextResponse.json({ error: "Site not found" }, { status: 404 });
    }

    if (siteWithData._count.subjects > 0) {
      return NextResponse.json(
        { error: "Cannot delete site with enrolled subjects" },
        { status: 400 }
      );
    }

    // Soft delete
    await prisma.site.update({
      where: { id },
      data: { isActive: false, status: "Closed" },
    });

    // Create audit log
    const userId = (session.user as { id?: string }).id;
    await createAuditLog({
      tableName: "Site",
      recordId: id,
      action: "DELETE",
      oldValues: siteWithData as unknown as Record<string, unknown>,
      userId: userId,
      userEmail: session.user.email || undefined,
    });

    return NextResponse.json({ message: "Site deleted successfully" });
  } catch (error) {
    console.error("Error deleting site:", error);
    return NextResponse.json(
      { error: "Failed to delete site" },
      { status: 500 }
    );
  }
}
