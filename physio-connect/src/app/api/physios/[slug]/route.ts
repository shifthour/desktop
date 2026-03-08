import { NextRequest, NextResponse } from "next/server";
import { supabase } from "@/lib/supabase";
import { transformPhysio, PHYSIO_SELECT } from "@/lib/transforms";
import { getPhysioBySlug } from "@/lib/data";

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ slug: string }> }
) {
  try {
    const { slug } = await params;

    const { data: physio, error } = await supabase
      .from("physioconnect_physiotherapists")
      .select(PHYSIO_SELECT)
      .eq("slug", slug)
      .single();

    if (error || !physio) {
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
