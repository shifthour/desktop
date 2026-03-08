import { NextRequest, NextResponse } from "next/server";
import { supabase } from "@/lib/supabase";
import { transformPhysio, PHYSIO_SELECT } from "@/lib/transforms";
import { physiotherapists as staticPhysios } from "@/lib/data";

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);

    const specialization = searchParams.get("specialization");
    const visitType = searchParams.get("visitType");
    const minRating = searchParams.get("minRating");
    const gender = searchParams.get("gender");
    const area = searchParams.get("area");

    let query = supabase
      .from("physioconnect_physiotherapists")
      .select(PHYSIO_SELECT)
      .order("rating", { ascending: false });

    if (minRating) {
      query = query.gte("rating", parseFloat(minRating));
    }
    if (gender && gender !== "any") {
      query = query.eq("gender", gender);
    }
    if (area) {
      query = query.eq("locationArea", area);
    }

    const { data: physios, error } = await query;
    if (error) throw error;

    let result = (physios || []).map((p, i) => transformPhysio(p, i));

    // Filter by specialization (nested relation — done client-side)
    if (specialization) {
      result = result.filter((p) => p.specializations.includes(specialization));
    }
    // Filter by visitType (nested relation — done client-side)
    if (visitType) {
      result = result.filter((p) => p.visitTypes.includes(visitType as "clinic" | "home" | "online"));
    }

    return NextResponse.json(result);
  } catch (error) {
    console.error("DB unavailable, falling back to static data:", error);
    return NextResponse.json(staticPhysios);
  }
}
