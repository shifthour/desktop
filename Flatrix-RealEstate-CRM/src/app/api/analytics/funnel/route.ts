import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

// Initialize Supabase client
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supabase = createClient(supabaseUrl, supabaseServiceKey);

// Date range presets in days
const DATE_RANGES = {
  '7_DAYS': 7,
  '15_DAYS': 15,
  '1_MONTH': 30,
  '3_MONTHS': 90,
  '6_MONTHS': 180,
  '1_YEAR': 365,
};

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const dateRange = searchParams.get('dateRange') || '1_MONTH';
    const startDateParam = searchParams.get('startDate');
    const endDateParam = searchParams.get('endDate');

    let startDate: string;
    let endDate: string = new Date().toISOString();

    // Calculate start date based on range or custom dates
    if (startDateParam && endDateParam) {
      // Set start date to beginning of day
      const start = new Date(startDateParam);
      start.setHours(0, 0, 0, 0);
      startDate = start.toISOString();

      // Set end date to end of day (23:59:59.999)
      const end = new Date(endDateParam);
      end.setHours(23, 59, 59, 999);
      endDate = end.toISOString();
    } else {
      const daysToSubtract = DATE_RANGES[dateRange as keyof typeof DATE_RANGES] || 30;
      const start = new Date();
      start.setDate(start.getDate() - daysToSubtract);
      start.setHours(0, 0, 0, 0);
      startDate = start.toISOString();

      // Set end date to end of current day
      const end = new Date();
      end.setHours(23, 59, 59, 999);
      endDate = end.toISOString();
    }

    console.log(`Fetching funnel data from ${startDate} to ${endDate}`);

    // 1. Total Leads (created in date range)
    const { count: totalLeads } = await supabase
      .from('flatrix_leads')
      .select('*', { count: 'exact', head: true })
      .gte('created_at', startDate)
      .lte('created_at', endDate);

    // 2. Qualified Leads
    const { count: qualifiedLeads } = await supabase
      .from('flatrix_leads')
      .select('*', { count: 'exact', head: true })
      .eq('status', 'QUALIFIED')
      .gte('created_at', startDate)
      .lte('created_at', endDate);

    // 3. Disqualified Leads (status = LOST)
    const { count: disqualifiedLeads } = await supabase
      .from('flatrix_leads')
      .select('*', { count: 'exact', head: true })
      .eq('status', 'LOST')
      .gte('created_at', startDate)
      .lte('created_at', endDate);

    // 4. Site Visits Completed (deals with site_visit_status = COMPLETED)
    // Filter by site_visit_date instead of created_at
    // Exclude cancelled site visits
    const { count: siteVisitsCompleted } = await supabase
      .from('flatrix_deals')
      .select('*', { count: 'exact', head: true })
      .eq('site_visit_status', 'COMPLETED')
      .neq('conversion_status', 'CANCELLED')
      .not('site_visit_date', 'is', null)
      .gte('site_visit_date', startDate)
      .lte('site_visit_date', endDate);

    // 5. Site Visits Scheduled
    // Filter by site_visit_date instead of created_at
    const { count: siteVisitsScheduled } = await supabase
      .from('flatrix_deals')
      .select('*', { count: 'exact', head: true })
      .eq('site_visit_status', 'SCHEDULED')
      .not('site_visit_date', 'is', null)
      .gte('site_visit_date', startDate)
      .lte('site_visit_date', endDate);

    // 6. Booked (deals with conversion_status = BOOKED)
    // Filter by conversion_date if available, otherwise fall back to created_at
    const { data: bookedDeals, error: bookedError } = await supabase
      .from('flatrix_deals')
      .select('*')
      .eq('conversion_status', 'BOOKED');

    if (bookedError) {
      console.error('Error fetching booked deals:', bookedError);
    }

    // Filter bookings based on conversion_date or created_at
    const bookedInRange = (bookedDeals || []).filter(deal => {
      // Use conversion_date if available, otherwise use created_at
      const dateToCheck = deal.conversion_date || deal.created_at;
      if (!dateToCheck) return false;

      const dealDate = new Date(dateToCheck);
      const start = new Date(startDate);
      const end = new Date(endDate);

      return dealDate >= start && dealDate <= end;
    });

    const booked = bookedInRange.length;

    // 7. Won Deals (status = WON)
    // Keep using created_at for won deals
    const { count: wonDeals } = await supabase
      .from('flatrix_deals')
      .select('*', { count: 'exact', head: true })
      .eq('status', 'WON')
      .gte('created_at', startDate)
      .lte('created_at', endDate);

    // 8. Lost/Not Booked (conversion_status = NOT_BOOKED or LOST)
    // Filter by conversion_date if available, otherwise created_at
    const { data: notBookedDeals, error: notBookedError } = await supabase
      .from('flatrix_deals')
      .select('*')
      .in('conversion_status', ['NOT_BOOKED', 'LOST']);

    if (notBookedError) {
      console.error('Error fetching not booked deals:', notBookedError);
    }

    // Filter not booked based on conversion_date or created_at
    const notBookedInRange = (notBookedDeals || []).filter(deal => {
      // Use conversion_date if available, otherwise use created_at
      const dateToCheck = deal.conversion_date || deal.created_at;
      if (!dateToCheck) return false;

      const dealDate = new Date(dateToCheck);
      const start = new Date(startDate);
      const end = new Date(endDate);

      return dealDate >= start && dealDate <= end;
    });

    const notBooked = notBookedInRange.length;

    // Calculate conversion rates
    const totalLeadsNum = totalLeads || 0;
    const qualifiedLeadsNum = qualifiedLeads || 0;
    const siteVisitsCompletedNum = siteVisitsCompleted || 0;
    const bookedNum = booked || 0;

    const funnelData = {
      totalLeads: totalLeadsNum,
      qualifiedLeads: qualifiedLeadsNum,
      disqualifiedLeads: disqualifiedLeads || 0,
      siteVisitsScheduled: siteVisitsScheduled || 0,
      siteVisitsCompleted: siteVisitsCompletedNum,
      booked: bookedNum,
      wonDeals: wonDeals || 0,
      notBooked: notBooked || 0,

      // Conversion rates (as percentages)
      conversionRates: {
        leadsToQualified: totalLeadsNum > 0 ? ((qualifiedLeadsNum / totalLeadsNum) * 100).toFixed(1) : '0',
        qualifiedToSiteVisit: qualifiedLeadsNum > 0 ? ((siteVisitsCompletedNum / qualifiedLeadsNum) * 100).toFixed(1) : '0',
        siteVisitToBooked: siteVisitsCompletedNum > 0 ? ((bookedNum / siteVisitsCompletedNum) * 100).toFixed(1) : '0',
        leadsToBooked: totalLeadsNum > 0 ? ((bookedNum / totalLeadsNum) * 100).toFixed(1) : '0',
      },

      dateRange: {
        start: startDate,
        end: endDate,
        preset: dateRange
      }
    };

    return NextResponse.json({
      success: true,
      data: funnelData
    });

  } catch (error: any) {
    console.error('Error fetching funnel data:', error);
    return NextResponse.json(
      {
        success: false,
        error: 'Internal server error',
        message: error.message || 'Failed to fetch funnel data'
      },
      { status: 500 }
    );
  }
}
