import { NextRequest, NextResponse } from "next/server";
import { writeFile, mkdir } from "fs/promises";
import path from "path";
import { v4 as uuidv4 } from "uuid";
import { getDefaultPrompt } from "@/lib/prompt-template";

const PDFS_DIR = path.join(process.cwd(), "data", "pdfs");

export async function POST(request: NextRequest) {
  try {
    const formData = await request.formData();
    const file = formData.get("pdf") as File | null;

    if (!file) {
      return NextResponse.json({ error: "No PDF file provided" }, { status: 400 });
    }

    if (!file.name.toLowerCase().endsWith(".pdf")) {
      return NextResponse.json({ error: "File must be a PDF" }, { status: 400 });
    }

    // 32MB limit
    if (file.size > 32 * 1024 * 1024) {
      return NextResponse.json({ error: "File must be under 32MB" }, { status: 400 });
    }

    await mkdir(PDFS_DIR, { recursive: true });

    const fileId = uuidv4();
    const fileName = `${fileId}-${file.name.replace(/[^a-zA-Z0-9.-]/g, "_")}`;
    const filePath = path.join(PDFS_DIR, fileName);

    const bytes = await file.arrayBuffer();
    await writeFile(filePath, Buffer.from(bytes));

    const relativePath = `data/pdfs/${fileName}`;

    return NextResponse.json({
      pdfPath: relativePath,
      pdfOriginalName: file.name,
      defaultPrompt: getDefaultPrompt(),
    });
  } catch (error) {
    console.error("Upload error:", error);
    return NextResponse.json(
      { error: "Failed to upload file" },
      { status: 500 }
    );
  }
}
