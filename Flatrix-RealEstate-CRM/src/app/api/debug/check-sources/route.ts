import { NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supabase = createClient(supabaseUrl, supabaseServiceKey);

export async function GET() {
  try {
    // Get all distinct source values
    const { data: allLeads, error } = await supabase
      .from('flatrix_leads')
      .select('source')
      .not('source', 'is', null);

    if (error) throw error;

    // Count occurrences of each source
    const sourceCounts = (allLeads || []).reduce((acc: Record<string, number>, lead) => {
      const source = lead.source || 'NULL';
      acc[source] = (acc[source] || 0) + 1;
      return acc;
    }, {});

    // Get leads with 99 in source name
    const { data: ninetyNineLeads, error: ninetyNineError } = await supabase
      .from('flatrix_leads')
      .select('id, name, source, created_at')
      .ilike('source', '%99%')
      .order('created_at', { ascending: false })
      .limit(20);

    if (ninetyNineError) throw ninetyNineError;

    return NextResponse.json({
      success: true,
      sourceCounts,
      totalLeads: allLeads?.length || 0,
      ninetyNineLeads: ninetyNineLeads || [],
      ninetyNineLeadsCount: ninetyNineLeads?.length || 0
    });

  } catch (error: any) {
    console.error('Error checking sources:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message
      },
      { status: 500 }
    );
  }
}
