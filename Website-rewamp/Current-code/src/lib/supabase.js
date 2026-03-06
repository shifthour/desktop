import { createClient } from '@supabase/supabase-js';

const supabaseUrl = import.meta.env.VITE_SUPABASE_URL;
const supabaseAnonKey = import.meta.env.VITE_SUPABASE_ANON_KEY;

export const supabase = (supabaseUrl && supabaseAnonKey)
  ? createClient(supabaseUrl, supabaseAnonKey)
  : null;

/**
 * Submit a lead to the Supabase "flatrix_leads" table.
 *
 * Expected table schema (flatrix_leads):
 *   id              - uuid (auto-generated via gen_random_uuid())
 *   created_at      - timestamptz (auto-generated)
 *   updated_at      - timestamptz (auto-generated)
 *   project_name    - varchar(100) NOT NULL, default 'Anahata'
 *   name            - varchar(255) NOT NULL
 *   phone           - varchar(20) NOT NULL
 *   email           - varchar(255) nullable
 *   source          - varchar(100) nullable (e.g. "contact_form", "site_visit", "brochure_download")
 *   status_of_save  - varchar(50) nullable, default 'saved'
 *   preferred_type  - text nullable (e.g. "2 BHK", "3 BHK 2T", "3 BHK 3T")
 */
export async function submitLead({
  name,
  email,
  phone,
  source = 'contact_form',
  project_name = 'Anahata',
  status_of_save = 'saved',
  preferred_type = null,
}) {
  if (!supabase) {
    console.warn('Supabase not configured. Lead not saved.');
    return { success: false, error: 'Supabase not configured' };
  }

  try {
    const { data, error } = await supabase
      .from('flatrix_leads')
      .insert([{
        name,
        email: email || null,
        phone,
        source,
        project_name,
        status_of_save,
        preferred_type,
      }])
      .select();

    if (error) {
      console.error('Supabase insert error:', error);
      return { success: false, error: error.message };
    }

    return { success: true, data };
  } catch (err) {
    console.error('Lead submission failed:', err);
    return { success: false, error: err.message };
  }
}
