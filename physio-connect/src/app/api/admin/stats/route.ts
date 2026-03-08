import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase-server";
import { validateSession } from "@/lib/auth";

export async function GET() {
  try {
    const session = await validateSession();
    if (!session) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    // Run all counts in parallel
    const [physioRes, pendingRes, bookingRes, approvedRes, rejectedRes, recentAppsRes] = await Promise.all([
      supabaseAdmin.from("physioconnect_physiotherapists").select("id", { count: "exact", head: true }),
      supabaseAdmin.from("physioconnect_applications").select("id", { count: "exact", head: true }).eq("status", "pending"),
      supabaseAdmin.from("physioconnect_bookings").select("id", { count: "exact", head: true }),
      supabaseAdmin.from("physioconnect_applications").select("id", { count: "exact", head: true }).eq("status", "approved"),
      supabaseAdmin.from("physioconnect_applications").select("id", { count: "exact", head: true }).eq("status", "rejected"),
      supabaseAdmin
        .from("physioconnect_applications")
        .select("id, firstName, lastName, email, status, createdAt")
        .order("createdAt", { ascending: false })
        .limit(5),
    ]);

    return NextResponse.json({
      totalProviders: physioRes.count || 0,
      pendingApplications: pendingRes.count || 0,
      totalBookings: bookingRes.count || 0,
      approvedApplications: approvedRes.count || 0,
      rejectedApplications: rejectedRes.count || 0,
      recentApplications: recentAppsRes.data || [],
    });
  } catch (err) {
    console.error("Stats error:", err);
    return NextResponse.json({ error: "Failed to load stats" }, { status: 500 });
  }
}
