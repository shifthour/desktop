import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/lib/auth";
import prisma from "@/lib/prisma";
import { createAuditLog } from "@/lib/audit";

// GET /api/crf/lov - List all LOV lists for a study
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

    const lovLists = await prisma.lOVList.findMany({
      where: { studyId },
      orderBy: { listName: "asc" },
      include: {
        items: {
          where: { isActive: true },
          orderBy: { sequenceNumber: "asc" },
        },
        _count: {
          select: { fields: true },
        },
      },
    });

    return NextResponse.json(lovLists);
  } catch (error) {
    console.error("Error fetching LOV lists:", error);
    return NextResponse.json(
      { error: "Failed to fetch LOV lists" },
      { status: 500 }
    );
  }
}

// POST /api/crf/lov - Create a new LOV list with items
export async function POST(request: Request) {
  try {
    const session = await getServerSession(authOptions);
    if (!session?.user) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const body = await request.json();
    const {
      studyId,
      listCode,
      listName,
      items, // Array of { itemCode, itemValue }
    } = body;

    // Validate required fields
    if (!studyId || !listCode || !listName) {
      return NextResponse.json(
        { error: "Study ID, list code, and list name are required" },
        { status: 400 }
      );
    }

    // Check if list code already exists in this study
    const existing = await prisma.lOVList.findUnique({
      where: {
        studyId_listCode: { studyId, listCode },
      },
    });

    if (existing) {
      return NextResponse.json(
        { error: "LOV list code already exists in this study" },
        { status: 400 }
      );
    }

    // Create LOV list with items in a transaction
    const lovList = await prisma.$transaction(async (tx) => {
      const newList = await tx.lOVList.create({
        data: {
          studyId,
          listCode,
          listName,
          isActive: true,
        },
      });

      // Create items if provided
      if (items && Array.isArray(items) && items.length > 0) {
        for (let i = 0; i < items.length; i++) {
          await tx.lOVItem.create({
            data: {
              listId: newList.id,
              itemCode: items[i].itemCode,
              itemValue: items[i].itemValue,
              sequenceNumber: i + 1,
              isActive: true,
            },
          });
        }
      }

      return newList;
    });

    // Fetch the complete list with items
    const completeList = await prisma.lOVList.findUnique({
      where: { id: lovList.id },
      include: {
        items: {
          orderBy: { sequenceNumber: "asc" },
        },
      },
    });

    // Create audit log
    const userId = (session.user as { id?: string }).id;
    await createAuditLog({
      tableName: "LOVList",
      recordId: lovList.id,
      action: "CREATE",
      newValues: completeList as unknown as Record<string, unknown>,
      userId: userId,
      userEmail: session.user.email || undefined,
    });

    return NextResponse.json(completeList, { status: 201 });
  } catch (error) {
    console.error("Error creating LOV list:", error);
    return NextResponse.json(
      { error: "Failed to create LOV list" },
      { status: 500 }
    );
  }
}
