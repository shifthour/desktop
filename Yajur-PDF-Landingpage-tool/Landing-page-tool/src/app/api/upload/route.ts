import { NextRequest, NextResponse } from "next/server";
import { requireAuth } from "@/lib/auth-helpers";
import { createAdminClient } from "@/lib/supabase/admin";
import { logActivity } from "@/lib/activity";
import { getDefaultPrompt } from "@/lib/prompt-template";
import { v4 as uuidv4 } from "uuid";

export async function POST(request: NextRequest) {
  try {
    const { user } = await requireAuth();

    const formData = await request.formData();
    const file = formData.get("pdf") as File | null;

    if (!file) {
      return NextResponse.json({ error: "No file uploaded" }, { status: 400 });
    }

    if (!file.name.endsWith(".pdf")) {
      return NextResponse.json(
        { error: "Only PDF files are accepted" },
        { status: 400 }
      );
    }

    if (file.size > 32 * 1024 * 1024) {
      return NextResponse.json(
        { error: "File must be under 32MB" },
        { status: 400 }
      );
    }

    // Upload to Supabase Storage using admin client (bypasses RLS)
    const supabase = createAdminClient();
    const fileId = uuidv4();
    const storagePath = `${user.id}/${fileId}-${file.name}`;

    const { error: uploadError } = await supabase.storage
      .from("yajur-pdfs")
      .upload(storagePath, file, {
        contentType: "application/pdf",
        upsert: false,
      });

    if (uploadError) {
      return NextResponse.json(
        { error: `Upload failed: ${uploadError.message}` },
        { status: 500 }
      );
    }

    await logActivity(user.id, "pdf_uploaded", "pdf", fileId, {
      filename: file.name,
      size: file.size,
    });

    return NextResponse.json({
      pdf_storage_path: storagePath,
      pdf_original_name: file.name,
      defaultPrompt: getDefaultPrompt(),
    });
  } catch (error) {
    console.error("Upload error:", error);
    const message =
      error instanceof Error ? error.message : "Upload failed";
    return NextResponse.json({ error: message }, { status: 401 });
  }
}
