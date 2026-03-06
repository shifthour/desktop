import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supabase = createClient(supabaseUrl, supabaseServiceKey);

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const startDateParam = searchParams.get('startDate') || '2025-11-24';
    const endDateParam = searchParams.get('endDate') || '2025-11-30';

    // Set start date to beginning of day
    const start = new Date(startDateParam);
    start.setHours(0, 0, 0, 0);
    const startDate = start.toISOString();

    // Set end date to end of day
    const end = new Date(endDateParam);
    end.setHours(23, 59, 59, 999);
    const endDate = end.toISOString();

    console.log(`Querying bookings from ${startDate} to ${endDate}`);

    // Get all booked deals with full details
    const { data: bookedDeals, error } = await supabase
      .from('flatrix_deals')
      .select(`
        id,
        conversion_status,
        conversion_date,
        created_at,
        updated_at,
        lead_id
      `)
      .eq('conversion_status', 'BOOKED')
      .order('created_at', { ascending: true });

    if (error) throw error;

    // Get lead details for each booking
    const bookingsWithLeads = await Promise.all(
      (bookedDeals || []).map(async (deal) => {
        const { data: lead } = await supabase
          .from('flatrix_leads')
          .select('id, first_name, last_name, phone, email')
          .eq('id', deal.lead_id)
          .single();

        const leadName = lead
          ? [lead.first_name, lead.last_name].filter(Boolean).join(' ') || 'Unknown'
          : 'Unknown';

        // Determine which date to use
        const dateToCheck = deal.conversion_date || deal.created_at;
        const dealDate = new Date(dateToCheck);
        const isInRange = dealDate >= start && dealDate <= end;

        return {
          dealId: deal.id,
          leadId: deal.lead_id,
          leadName,
          phone: lead?.phone || 'N/A',
          conversionStatus: deal.conversion_status,
          conversionDate: deal.conversion_date,
          conversionDateFormatted: deal.conversion_date
            ? new Date(deal.conversion_date).toLocaleString('en-US', {
                month: 'short',
                day: 'numeric',
                year: 'numeric',
                hour: 'numeric',
                minute: '2-digit',
                hour12: true
              })
            : 'NULL',
          createdAt: deal.created_at,
          createdAtFormatted: new Date(deal.created_at).toLocaleString('en-US', {
            month: 'short',
            day: 'numeric',
            year: 'numeric',
            hour: 'numeric',
            minute: '2-digit',
            hour12: true
          }),
          dateUsedForFiltering: dateToCheck,
          dateUsedForFilteringFormatted: new Date(dateToCheck).toLocaleString('en-US', {
            month: 'short',
            day: 'numeric',
            year: 'numeric',
            hour: 'numeric',
            minute: '2-digit',
            hour12: true
          }),
          isInDateRange: isInRange
        };
      })
    );

    const bookingsInRange = bookingsWithLeads.filter(b => b.isInDateRange);

    return NextResponse.json({
      success: true,
      dateRange: {
        start: startDate,
        end: endDate,
        startFormatted: start.toLocaleDateString('en-US'),
        endFormatted: end.toLocaleDateString('en-US')
      },
      totalBookings: bookingsWithLeads.length,
      bookingsInRange: bookingsInRange.length,
      allBookings: bookingsWithLeads,
      bookingsMatchingRange: bookingsInRange
    });

  } catch (error: any) {
    console.error('Error querying bookings:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message
      },
      { status: 500 }
    );
  }
}
