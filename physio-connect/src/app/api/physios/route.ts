import { NextRequest, NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";
import { transformPhysio, physioIncludes } from "@/lib/transforms";
import { physiotherapists as staticPhysios } from "@/lib/data";

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);

    const specialization = searchParams.get("specialization");
    const visitType = searchParams.get("visitType");
    const minRating = searchParams.get("minRating");
    const gender = searchParams.get("gender");
    const area = searchParams.get("area");

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const where: any = {};

    if (specialization) {
      where.specializations = {
        some: { specialization: { name: specialization } },
      };
    }
    if (visitType) {
      where.visitTypes = {
        some: { visitType },
      };
    }
    if (minRating) {
      where.rating = { gte: parseFloat(minRating) };
    }
    if (gender && gender !== "any") {
      where.gender = gender;
    }
    if (area) {
      where.locationArea = area;
    }

    const physios = await prisma.physiotherapist.findMany({
      where,
      include: physioIncludes,
      orderBy: { rating: "desc" },
    });

    const result = physios.map((p, i) => transformPhysio(p, i));
    return NextResponse.json(result);
  } catch (error) {
    console.error("DB unavailable, falling back to static data:", error);
    // Fallback to static data when database is unavailable
    return NextResponse.json(staticPhysios);
  }
}
