import { createClient } from "@supabase/supabase-js"

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || "https://placeholder-project.supabase.co"
const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || "placeholder-anon-key"

export const supabase = createClient(supabaseUrl, supabaseAnonKey)

// Types for our database
export interface Lead {
  id?: string
  project_name: string
  name: string
  phone: string
  email?: string
  source: string
  status_of_save: string
  created_at?: string
  updated_at?: string
}
