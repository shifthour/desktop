import { NextRequest, NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";
import { transformPhysio, physioIncludes } from "@/lib/transforms";
import { getPhysioBySlug } from "@/lib/data";

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ slug: string }> }
) {
  try {
    const { slug } = await params;

    const physio = await prisma.physiotherapist.findUnique({
      where: { slug },
      include: physioIncludes,
    });

    if (!physio) {
      // Try static data fallback
      const staticPhysio = getPhysioBySlug(slug);
      if (staticPhysio) return NextResponse.json(staticPhysio);
      return NextResponse.json({ error: "Not found" }, { status: 404 });
    }

    return NextResponse.json(transformPhysio(physio));
  } catch (error) {
    console.error("DB unavailable, falling back to static data:", error);
    const { slug } = await params;
    const staticPhysio = getPhysioBySlug(slug);
    if (staticPhysio) return NextResponse.json(staticPhysio);
    return NextResponse.json(
      { error: "Failed to fetch physiotherapist" },
      { status: 500 }
    );
  }
}
