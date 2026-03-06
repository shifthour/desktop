import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

export const supabase = createClient(supabaseUrl, supabaseAnonKey)

// For server-side operations that require elevated permissions
export const createServiceSupabase = () => {
  if (!process.env.SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error('Missing SUPABASE_SERVICE_ROLE_KEY')
  }
  
  return createClient(supabaseUrl, process.env.SUPABASE_SERVICE_ROLE_KEY, {
    auth: {
      autoRefreshToken: false,
      persistSession: false
    }
  })
}

// Database types
export interface Database {
  public: {
    Tables: {
      companies: {
        Row: {
          id: string
          name: string
          domain: string
          max_users: number
          current_users: number
          subscription_status: 'active' | 'inactive' | 'suspended'
          created_at: string
          updated_at: string
        }
        Insert: {
          id?: string
          name: string
          domain: string
          max_users: number
          current_users?: number
          subscription_status?: 'active' | 'inactive' | 'suspended'
          created_at?: string
          updated_at?: string
        }
        Update: {
          id?: string
          name?: string
          domain?: string
          max_users?: number
          current_users?: number
          subscription_status?: 'active' | 'inactive' | 'suspended'
          created_at?: string
          updated_at?: string
        }
      }
      user_roles: {
        Row: {
          id: string
          name: string
          description: string
          permissions: string[]
          created_at: string
        }
        Insert: {
          id?: string
          name: string
          description: string
          permissions: string[]
          created_at?: string
        }
        Update: {
          id?: string
          name?: string
          description?: string
          permissions?: string[]
          created_at?: string
        }
      }
      users: {
        Row: {
          id: string
          company_id: string
          email: string
          full_name: string
          role_id: string
          is_admin: boolean
          is_active: boolean
          created_at: string
          updated_at: string
          last_login: string | null
        }
        Insert: {
          id?: string
          company_id: string
          email: string
          full_name: string
          role_id: string
          is_admin?: boolean
          is_active?: boolean
          created_at?: string
          updated_at?: string
          last_login?: string | null
        }
        Update: {
          id?: string
          company_id?: string
          email?: string
          full_name?: string
          role_id?: string
          is_admin?: boolean
          is_active?: boolean
          created_at?: string
          updated_at?: string
          last_login?: string | null
        }
      }
    }
  }
}