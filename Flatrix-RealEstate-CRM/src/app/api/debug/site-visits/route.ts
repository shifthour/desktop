import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supabase = createClient(supabaseUrl, supabaseServiceKey);

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const startDateParam = searchParams.get('startDate') || '2025-11-10';
    const endDateParam = searchParams.get('endDate') || '2025-11-16';

    // Set start date to beginning of day
    const start = new Date(startDateParam);
    start.setHours(0, 0, 0, 0);
    const startDate = start.toISOString();

    // Set end date to end of day
    const end = new Date(endDateParam);
    end.setHours(23, 59, 59, 999);
    const endDate = end.toISOString();

    console.log(`Querying site visits from ${startDate} to ${endDate}`);

    // Get all completed site visits in date range with lead details
    const { data: deals, error } = await supabase
      .from('flatrix_deals')
      .select(`
        id,
        site_visit_status,
        site_visit_date,
        conversion_status,
        attended_by,
        created_at,
        lead_id
      `)
      .eq('site_visit_status', 'COMPLETED')
      .not('site_visit_date', 'is', null)
      .gte('site_visit_date', startDate)
      .lte('site_visit_date', endDate)
      .order('site_visit_date', { ascending: true });

    if (error) throw error;

    // Get lead details for each deal
    const dealsWithLeads = await Promise.all(
      (deals || []).map(async (deal) => {
        const { data: lead } = await supabase
          .from('flatrix_leads')
          .select('id, first_name, last_name, phone, email, assigned_to_id')
          .eq('id', deal.lead_id)
          .single();

        // Get assigned to user
        let assignedTo = 'Unassigned';
        if (lead?.assigned_to_id) {
          const { data: user } = await supabase
            .from('flatrix_users')
            .select('name')
            .eq('id', lead.assigned_to_id)
            .single();
          assignedTo = user?.name || 'Unassigned';
        }

        const leadName = lead
          ? [lead.first_name, lead.last_name].filter(Boolean).join(' ') || 'Unknown'
          : 'Unknown';

        return {
          dealId: deal.id,
          leadId: deal.lead_id,
          leadName,
          phone: lead?.phone || 'N/A',
          email: lead?.email || 'N/A',
          siteVisitDate: deal.site_visit_date,
          siteVisitDateFormatted: new Date(deal.site_visit_date).toLocaleString('en-US', {
            month: 'short',
            day: 'numeric',
            year: 'numeric',
            hour: 'numeric',
            minute: '2-digit',
            hour12: true
          }),
          attendedBy: deal.attended_by || 'N/A',
          assignedTo,
          conversionStatus: deal.conversion_status,
          dealCreatedAt: deal.created_at,
          dealCreatedAtFormatted: new Date(deal.created_at).toLocaleString('en-US', {
            month: 'short',
            day: 'numeric',
            year: 'numeric',
            hour: 'numeric',
            minute: '2-digit',
            hour12: true
          })
        };
      })
    );

    return NextResponse.json({
      success: true,
      count: dealsWithLeads.length,
      dateRange: {
        start: startDate,
        end: endDate,
        startFormatted: start.toLocaleDateString('en-US'),
        endFormatted: end.toLocaleDateString('en-US')
      },
      siteVisits: dealsWithLeads
    });

  } catch (error: any) {
    console.error('Error querying site visits:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message
      },
      { status: 500 }
    );
  }
}
