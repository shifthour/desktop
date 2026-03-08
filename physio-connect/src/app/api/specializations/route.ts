import { NextResponse } from "next/server";
import { supabase } from "@/lib/supabase";
import { specializations as staticSpecs } from "@/lib/data";

export async function GET() {
  try {
    const { data: specs, error } = await supabase
      .from("physioconnect_specializations")
      .select("*")
      .order("name", { ascending: true });

    if (error) throw error;

    return NextResponse.json(specs);
  } catch (error) {
    console.error("DB unavailable, falling back to static data:", error);
    return NextResponse.json(staticSpecs);
  }
}
