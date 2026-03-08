import { NextRequest, NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase-server";
import { validateSession } from "@/lib/auth";

// GET — single application with signed file URLs
export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const session = await validateSession();
    if (!session) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const { id } = await params;

    const { data, error } = await supabaseAdmin
      .from("physioconnect_applications")
      .select("*")
      .eq("id", id)
      .single();

    if (error || !data) {
      return NextResponse.json({ error: "Application not found" }, { status: 404 });
    }

    // Generate signed URLs for uploaded files (valid for 1 hour)
    const fileFields = ["profilePhotoUrl", "resumeUrl", "idProofUrl", "visaDocUrl", "passportPage1Url", "passportPage2Url"] as const;
    const signedUrls: Record<string, string | null> = {};

    for (const field of fileFields) {
      const path = data[field];
      if (path) {
        const { data: urlData } = await supabaseAdmin.storage
          .from("applications")
          .createSignedUrl(path, 3600);
        signedUrls[field] = urlData?.signedUrl || null;
      } else {
        signedUrls[field] = null;
      }
    }

    return NextResponse.json({ application: data, signedUrls });
  } catch (err) {
    console.error("Application detail error:", err);
    return NextResponse.json({ error: "Failed to load application" }, { status: 500 });
  }
}

// PATCH — reject or request info
export async function PATCH(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const session = await validateSession();
    if (!session) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const { id } = await params;
    const body = await request.json();
    const { status, reviewNotes } = body;

    if (!status || !["rejected", "needs_info"].includes(status)) {
      return NextResponse.json({ error: "Invalid status. Use 'rejected' or 'needs_info'" }, { status: 400 });
    }

    const { data, error } = await supabaseAdmin
      .from("physioconnect_applications")
      .update({
        status,
        reviewNotes: reviewNotes || null,
        reviewedBy: session.adminId,
        reviewedAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      })
      .eq("id", id)
      .select()
      .single();

    if (error) {
      console.error("Application update error:", error);
      return NextResponse.json({ error: error.message }, { status: 500 });
    }

    return NextResponse.json({ application: data });
  } catch (err) {
    console.error("Application update failed:", err);
    return NextResponse.json({ error: "Update failed" }, { status: 500 });
  }
}
