import { NextRequest, NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase-server";
import { validateSession } from "@/lib/auth";

export async function GET(request: NextRequest) {
  try {
    const session = await validateSession();
    if (!session) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const { searchParams } = new URL(request.url);
    const status = searchParams.get("status"); // pending, approved, rejected, needs_info, or null for all

    let query = supabaseAdmin
      .from("physioconnect_applications")
      .select("id, firstName, lastName, email, phone, hcpcNumber, professionalBody, specialisations, yearsExperience, status, createdAt, reviewedAt")
      .order("createdAt", { ascending: false });

    if (status && status !== "all") {
      query = query.eq("status", status);
    }

    const { data, error } = await query;

    if (error) {
      console.error("Applications list error:", error);
      return NextResponse.json({ error: error.message }, { status: 500 });
    }

    return NextResponse.json({ applications: data || [] });
  } catch (err) {
    console.error("Applications list failed:", err);
    return NextResponse.json({ error: "Failed to load applications" }, { status: 500 });
  }
}
