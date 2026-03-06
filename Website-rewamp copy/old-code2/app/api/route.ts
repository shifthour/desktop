import { NextResponse } from "next/server"
import { supabaseAdmin } from "@/lib/supabase-admin"

export async function POST(request: Request) {
  try {
    const { name, phone, email, source, status_of_save, action, preferred_type } = await request.json()

    if (!name || !phone) {
      return NextResponse.json({ success: false, error: "Name and phone are required" }, { status: 400 })
    }

    if (action === "update") {
      // Update existing record by phone number
      const { data, error } = await supabaseAdmin
        .from("flatrix_leads")
        .update({
          email: email?.trim() || null,
          status_of_save: status_of_save || "saved",
          preferred_type: preferred_type || null,
          updated_at: new Date().toISOString(),
        })
        .eq("phone", phone.trim())
        .eq("project_name", "Anahata")
        .select()

      if (error) {
        console.error("Supabase update error:", error)
        return NextResponse.json({ success: false, error: "Failed to update lead data" }, { status: 500 })
      }

      if (data && data.length > 0) {
        console.log("Lead updated successfully:", data)
        return NextResponse.json({
          success: true,
          message: "Lead data updated successfully",
          data: data,
        })
      } else {
        // If no record found to update, insert new one
        console.log("No existing record found, inserting new record")
      }
    }

    // Insert new record (default action or fallback)
    const { data, error } = await supabaseAdmin
      .from("flatrix_leads")
      .insert([
        {
          project_name: "Anahata",
          name: name.trim(),
          phone: phone.trim(),
          email: email?.trim() || null,
          source: source || "unknown",
          status_of_save: status_of_save || "saved",
          preferred_type: preferred_type || null,
        },
      ])
      .select()

    if (error) {
      console.error("Supabase insert error:", error)
      return NextResponse.json({ success: false, error: "Failed to save lead data" }, { status: 500 })
    }

    console.log("Lead inserted successfully:", data)

    return NextResponse.json({
      success: true,
      message: "Lead data saved successfully",
      data: data,
    })
  } catch (error) {
    console.error("API error:", error)
    return NextResponse.json({ success: false, error: "Internal server error" }, { status: 500 })
  }
}
