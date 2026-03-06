import { NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supabase = createClient(supabaseUrl, supabaseServiceKey);

export async function POST() {
  try {
    // Update "99 Acres" to "99acres" for consistency
    const { data: updated, error } = await supabase
      .from('flatrix_leads')
      .update({ source: '99acres' })
      .eq('source', '99 Acres')
      .select();

    if (error) throw error;

    return NextResponse.json({
      success: true,
      message: `Updated ${updated?.length || 0} leads from "99 Acres" to "99acres"`,
      updatedLeads: updated || []
    });

  } catch (error: any) {
    console.error('Error fixing source:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message
      },
      { status: 500 }
    );
  }
}
