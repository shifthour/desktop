import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supabase = createClient(supabaseUrl, supabaseServiceKey);

export async function GET(request: NextRequest) {
  try {
    // Get all deals created or updated in November 2025
    const { data: deals, error } = await supabase
      .from('flatrix_deals')
      .select(`
        id,
        conversion_status,
        conversion_date,
        site_visit_status,
        site_visit_date,
        created_at,
        updated_at,
        lead_id
      `)
      .or('created_at.gte.2025-11-01,updated_at.gte.2025-11-01,conversion_date.gte.2025-11-01,site_visit_date.gte.2025-11-01')
      .order('created_at', { ascending: false });

    if (error) throw error;

    // Get lead details for each deal
    const dealsWithLeads = await Promise.all(
      (deals || []).map(async (deal) => {
        const { data: lead } = await supabase
          .from('flatrix_leads')
          .select('id, first_name, last_name, phone')
          .eq('id', deal.lead_id)
          .single();

        const leadName = lead
          ? [lead.first_name, lead.last_name].filter(Boolean).join(' ') || 'Unknown'
          : 'Unknown';

        return {
          dealId: deal.id,
          leadName,
          phone: lead?.phone || 'N/A',
          conversionStatus: deal.conversion_status,
          conversionDate: deal.conversion_date,
          siteVisitStatus: deal.site_visit_status,
          siteVisitDate: deal.site_visit_date,
          createdAt: deal.created_at,
          updatedAt: deal.updated_at
        };
      })
    );

    return NextResponse.json({
      success: true,
      total: dealsWithLeads.length,
      deals: dealsWithLeads
    });

  } catch (error: any) {
    console.error('Error querying deals:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message
      },
      { status: 500 }
    );
  }
}
