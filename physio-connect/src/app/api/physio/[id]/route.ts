import { NextRequest, NextResponse } from "next/server";
import { supabase } from "@/lib/supabase";
import { transformPhysio, PHYSIO_SELECT } from "@/lib/transforms";
import { getPhysioById } from "@/lib/data";

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;

    const { data: physio, error } = await supabase
      .from("physioconnect_physiotherapists")
      .select(PHYSIO_SELECT)
      .eq("id", id)
      .single();

    if (error || !physio) {
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
