import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/lib/auth";
import prisma from "@/lib/prisma";
import { createAuditLog } from "@/lib/audit";

// GET /api/crf/lov/[id] - Get single LOV list with items
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

    const lovList = await prisma.lOVList.findUnique({
      where: { id },
      include: {
        items: {
          orderBy: { sequenceNumber: "asc" },
        },
        _count: {
          select: { fields: true },
        },
      },
    });

    if (!lovList) {
      return NextResponse.json({ error: "LOV list not found" }, { status: 404 });
    }

    return NextResponse.json(lovList);
  } catch (error) {
    console.error("Error fetching LOV list:", error);
    return NextResponse.json(
      { error: "Failed to fetch LOV list" },
      { status: 500 }
    );
  }
}

// PUT /api/crf/lov/[id] - Update LOV list and items
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
    const { listName, isActive, items } = body;

    // Get existing LOV list for audit
    const existingList = await prisma.lOVList.findUnique({
      where: { id },
      include: { items: true },
    });

    if (!existingList) {
      return NextResponse.json({ error: "LOV list not found" }, { status: 404 });
    }

    // Update LOV list and items in a transaction
    const updatedList = await prisma.$transaction(async (tx) => {
      // Update list
      const list = await tx.lOVList.update({
        where: { id },
        data: {
          listName: listName ?? existingList.listName,
          isActive: isActive ?? existingList.isActive,
        },
      });

      // Update items if provided
      if (items && Array.isArray(items)) {
        // Soft delete existing items (set isActive to false)
        await tx.lOVItem.updateMany({
          where: { listId: id },
          data: { isActive: false },
        });

        // Create or update items
        for (let i = 0; i < items.length; i++) {
          const item = items[i];
          if (item.id) {
            // Update existing item
            await tx.lOVItem.update({
              where: { id: item.id },
              data: {
                itemCode: item.itemCode,
                itemValue: item.itemValue,
                sequenceNumber: i + 1,
                isActive: true,
              },
            });
          } else {
            // Create new item
            await tx.lOVItem.create({
              data: {
                listId: id,
                itemCode: item.itemCode,
                itemValue: item.itemValue,
                sequenceNumber: i + 1,
                isActive: true,
              },
            });
          }
        }
      }

      return list;
    });

    // Fetch complete list with items
    const completeList = await prisma.lOVList.findUnique({
      where: { id },
      include: {
        items: {
          where: { isActive: true },
          orderBy: { sequenceNumber: "asc" },
        },
      },
    });

    // Create audit log
    const userId = (session.user as { id?: string }).id;
    await createAuditLog({
      tableName: "LOVList",
      recordId: id,
      action: "UPDATE",
      oldValues: existingList as unknown as Record<string, unknown>,
      newValues: completeList as unknown as Record<string, unknown>,
      userId: userId,
      userEmail: session.user.email || undefined,
    });

    return NextResponse.json(completeList);
  } catch (error) {
    console.error("Error updating LOV list:", error);
    return NextResponse.json(
      { error: "Failed to update LOV list" },
      { status: 500 }
    );
  }
}

// DELETE /api/crf/lov/[id] - Soft delete LOV list
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

    // Check if LOV list is used by any fields
    const listWithFields = await prisma.lOVList.findUnique({
      where: { id },
      include: {
        _count: {
          select: { fields: true },
        },
      },
    });

    if (!listWithFields) {
      return NextResponse.json({ error: "LOV list not found" }, { status: 404 });
    }

    if (listWithFields._count.fields > 0) {
      return NextResponse.json(
        { error: "Cannot delete LOV list that is used by fields" },
        { status: 400 }
      );
    }

    // Soft delete - set isActive to false
    await prisma.lOVList.update({
      where: { id },
      data: { isActive: false },
    });

    // Create audit log
    const userId = (session.user as { id?: string }).id;
    await createAuditLog({
      tableName: "LOVList",
      recordId: id,
      action: "DELETE",
      oldValues: listWithFields as unknown as Record<string, unknown>,
      userId: userId,
      userEmail: session.user.email || undefined,
    });

    return NextResponse.json({ message: "LOV list deleted successfully" });
  } catch (error) {
    console.error("Error deleting LOV list:", error);
    return NextResponse.json(
      { error: "Failed to delete LOV list" },
      { status: 500 }
    );
  }
}
