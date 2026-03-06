import { createClient, SupabaseClient } from '@supabase/supabase-js'
import { createBrowserClient } from '@supabase/ssr'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!

// Client for browser usage (untyped for flexibility)
export const supabase: SupabaseClient = createClient(supabaseUrl, supabaseAnonKey)

// Server client with service role (full access)
export const supabaseAdmin: SupabaseClient = createClient(supabaseUrl, supabaseServiceKey, {
  auth: {
    autoRefreshToken: false,
    persistSession: false,
  },
})

// Helper to create server-side client (for API routes)
// IMPORTANT: Disable Next.js fetch caching to always get fresh data
export function createServerClient(): SupabaseClient {
  return createClient(supabaseUrl, supabaseServiceKey, {
    auth: {
      autoRefreshToken: false,
      persistSession: false,
    },
    global: {
      fetch: (url, options = {}) => {
        return fetch(url, {
          ...options,
          cache: 'no-store',
        })
      },
    },
  })
}

// Browser client for auth (use in client components)
export function createBrowserSupabaseClient() {
  return createBrowserClient(supabaseUrl, supabaseAnonKey)
}
