import { createClient } from "@supabase/supabase-js"

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || "https://placeholder-project.supabase.co"
const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY || "placeholder-service-role-key"

// Service-role client (bypasses RLS) –  ONLY used on the server
export const supabaseAdmin = createClient(supabaseUrl, serviceRoleKey, {
  auth: { persistSession: false },
})
