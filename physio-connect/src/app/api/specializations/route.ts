import { NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";
import { specializations as staticSpecs } from "@/lib/data";

export async function GET() {
  try {
    const specs = await prisma.specialization.findMany({
      orderBy: { name: "asc" },
    });

    return NextResponse.json(specs);
  } catch (error) {
    console.error("DB unavailable, falling back to static data:", error);
    // Fallback to static data
    return NextResponse.json(staticSpecs);
  }
}
