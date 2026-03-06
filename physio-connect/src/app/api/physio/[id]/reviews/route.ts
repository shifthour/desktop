import { NextRequest, NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";
import { getReviewsByPhysioId } from "@/lib/data";

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;

    const reviews = await prisma.review.findMany({
      where: { physioId: id },
      orderBy: { date: "desc" },
    });

    return NextResponse.json(reviews);
  } catch (error) {
    console.error("DB unavailable, falling back to static data:", error);
    const { id } = await params;
    const staticReviews = getReviewsByPhysioId(id);
    return NextResponse.json(staticReviews);
  }
}
