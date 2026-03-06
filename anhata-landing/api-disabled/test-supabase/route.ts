import { NextResponse } from "next/server"
import { supabaseAdmin } from "@/lib/supabase-admin"

export async function GET() {
  try {
    // Test Supabase connection and configuration
    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
    const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY
    
    console.log("Supabase URL:", supabaseUrl ? "✓ Set" : "✗ Missing")
    console.log("Service Role Key:", serviceRoleKey ? "✓ Set" : "✗ Missing")
    
    // First check if table exists by trying to get table info
    const { data: tableData, error: tableError } = await supabaseAdmin
      .from("information_schema.tables")
      .select("table_name")
      .eq("table_schema", "public")
      .eq("table_name", "flatrix_leads")
    
    if (tableError) {
      console.error("Table check error:", tableError)
      return NextResponse.json({
        success: false,
        error: "Cannot check table existence",
        details: tableError.message,
        config: {
          url: supabaseUrl ? "configured" : "missing",
          key: serviceRoleKey ? "configured" : "missing"
        }
      }, { status: 500 })
    }
    
    const tableExists = tableData && tableData.length > 0
    
    // Try a simple query to test connection
    const { data, error, count } = await supabaseAdmin
      .from("flatrix_leads")
      .select("*", { count: "exact", head: true })
      .limit(1)
    
    if (error) {
      console.error("Supabase connection error:", error)
      return NextResponse.json({
        success: false,
        error: "Database connection failed",
        details: error.message,
        config: {
          url: supabaseUrl ? "configured" : "missing",
          key: serviceRoleKey ? "configured" : "missing"
        }
      }, { status: 500 })
    }
    
    return NextResponse.json({
      success: true,
      message: "Supabase connection successful",
      tableExists,
      leadsCount: count,
      config: {
        url: supabaseUrl ? "configured" : "missing", 
        key: serviceRoleKey ? "configured" : "missing"
      }
    })
    
  } catch (error) {
    console.error("Test API error:", error)
    return NextResponse.json({
      success: false,
      error: "Internal server error",
      details: error instanceof Error ? error.message : "Unknown error"
    }, { status: 500 })
  }
}