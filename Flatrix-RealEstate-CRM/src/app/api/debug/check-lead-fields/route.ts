import { NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supabase = createClient(supabaseUrl, supabaseServiceKey);

export async function GET() {
  try {
    const leadIds = [
      '90a1d950-8f80-475c-9ed2-43ffe4f74f2c', // Naresh
      '1c489852-dd11-4f47-b535-40568f6ed4a5', // Aparna
      'd0768aa6-daf4-448c-9dcf-1f7f227ac1ed'  // Sujeet
    ];

    // Get ALL fields for these leads
    const { data: leads, error } = await supabase
      .from('flatrix_leads')
      .select('*')
      .in('id', leadIds);

    if (error) throw error;

    // Also get a sample of other QUALIFIED leads for comparison
    const { data: sampleLeads, error: sampleError } = await supabase
      .from('flatrix_leads')
      .select('*')
      .eq('status', 'QUALIFIED')
      .limit(5);

    if (sampleError) {
      console.error('Sample leads error:', sampleError);
    }

    return NextResponse.json({
      success: true,
      targetLeads: leads,
      sampleQualifiedLeads: sampleLeads,
      comparison: {
        naresh: leads?.find(l => l.name?.includes('Naresh')),
        aparna: leads?.find(l => l.name?.includes('Aparna')),
        sujeet: leads?.find(l => l.name?.includes('Sujeet'))
      }
    });

  } catch (error: any) {
    console.error('[DEBUG] Error:', error);
    return NextResponse.json({
      success: false,
      error: error.message
    }, { status: 500 });
  }
}
