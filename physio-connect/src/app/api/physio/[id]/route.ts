import { NextRequest, NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";
import { transformPhysio, physioIncludes } from "@/lib/transforms";
import { getPhysioById } from "@/lib/data";

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;

    const physio = await prisma.physiotherapist.findUnique({
      where: { id },
      include: physioIncludes,
    });

    if (!physio) {
      // Try static data fallback
      const staticPhysio = getPhysioById(id);
      if (staticPhysio) return NextResponse.json(staticPhysio);
      return NextResponse.json({ error: "Not found" }, { status: 404 });
    }

    return NextResponse.json(transformPhysio(physio));
  } catch (error) {
    console.error("DB unavailable, falling back to static data:", error);
    const { id } = await params;
    const staticPhysio = getPhysioById(id);
    if (staticPhysio) return NextResponse.json(staticPhysio);
    return NextResponse.json(
      { error: "Failed to fetch physiotherapist" },
      { status: 500 }
    );
  }
}
