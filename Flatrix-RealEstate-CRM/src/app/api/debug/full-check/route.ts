import { NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supabase = createClient(supabaseUrl, supabaseServiceKey);

export async function GET() {
  try {
    // Get ALL QUALIFIED leads
    const { data: allQualifiedLeads, error: qualError } = await supabase
      .from('flatrix_leads')
      .select('id, name, status, phone')
      .eq('status', 'QUALIFIED')
      .order('created_at', { ascending: false });

    if (qualError) throw qualError;

    console.log('Total QUALIFIED leads:', allQualifiedLeads?.length);

    // Get ALL deals
    const { data: allDeals, error: dealsError } = await supabase
      .from('flatrix_deals')
      .select('id, lead_id, status, conversion_status, conversion_date')
      .order('created_at', { ascending: false });

    if (dealsError) throw dealsError;

    console.log('Total deals:', allDeals?.length);

    // Find which QUALIFIED leads have BOOKED deals
    const qualifiedWithBooked = allQualifiedLeads?.map(lead => {
      const deal = allDeals?.find(d => d.lead_id === lead.id);
      const hasBookedDeal = deal && (deal as any).conversion_status === 'BOOKED';

      return {
        lead_id: lead.id,
        lead_name: lead.name,
        lead_phone: lead.phone,
        lead_status: lead.status,
        has_deal: !!deal,
        deal_id: deal?.id,
        deal_status: deal?.status,
        conversion_status: (deal as any)?.conversion_status,
        conversion_date: (deal as any)?.conversion_date,
        should_show_in_commissions: hasBookedDeal
      };
    }).filter(item => item.should_show_in_commissions);

    console.log('QUALIFIED leads with BOOKED deals:', qualifiedWithBooked?.length);

    // Check specifically for Naresh and Aparna
    const nareshCheck = allQualifiedLeads?.find(l => l.name?.includes('Naresh'));
    const aparnaCheck = allQualifiedLeads?.find(l => l.name?.includes('Aparna'));

    let nareshDeal = null;
    let aparnaDeal = null;

    if (nareshCheck) {
      nareshDeal = allDeals?.find(d => d.lead_id === nareshCheck.id);
    }
    if (aparnaCheck) {
      aparnaDeal = allDeals?.find(d => d.lead_id === aparnaCheck.id);
    }

    return NextResponse.json({
      success: true,
      summary: {
        total_qualified_leads: allQualifiedLeads?.length,
        total_deals: allDeals?.length,
        qualified_with_booked_deals: qualifiedWithBooked?.length
      },
      qualified_with_booked: qualifiedWithBooked,
      naresh_check: {
        found_in_qualified: !!nareshCheck,
        lead_data: nareshCheck,
        deal_data: nareshDeal,
        has_booked_deal: nareshDeal && (nareshDeal as any).conversion_status === 'BOOKED'
      },
      aparna_check: {
        found_in_qualified: !!aparnaCheck,
        lead_data: aparnaCheck,
        deal_data: aparnaDeal,
        has_booked_deal: aparnaDeal && (aparnaDeal as any).conversion_status === 'BOOKED'
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
