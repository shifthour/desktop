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

    // Get leads info
    const { data: leads, error: leadsError } = await supabase
      .from('flatrix_leads')
      .select('id, name, status, assigned_to_id')
      .in('id', leadIds);

    if (leadsError) throw leadsError;

    // Get deals for these leads
    const { data: deals, error: dealsError } = await supabase
      .from('flatrix_deals')
      .select('*')
      .in('lead_id', leadIds);

    if (dealsError) throw dealsError;

    // Get all commissions
    const { data: allCommissions, error: commissionsError } = await supabase
      .from('flatrix_commissions')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(200);

    if (commissionsError) throw commissionsError;

    // Check which deals have commissions
    const leadsWithDetails = leads?.map(lead => {
      const deal = deals?.find(d => d.lead_id === lead.id);
      const commission = allCommissions?.find(c => c.deal_id === deal?.id);

      return {
        lead: {
          id: lead.id,
          name: lead.name,
          status: lead.status,
          assigned_to_id: lead.assigned_to_id
        },
        deal: deal ? {
          id: deal.id,
          status: deal.status,
          conversion_status: (deal as any).conversion_status,
          conversion_date: (deal as any).conversion_date,
          deal_value: deal.deal_value,
          commission_amount: deal.commission_amount,
          commission_percentage: deal.commission_percentage,
          created_at: deal.created_at
        } : null,
        commission: commission ? {
          id: commission.id,
          amount: commission.amount,
          status: commission.status,
          created_at: commission.created_at
        } : null,
        hasCommission: !!commission
      };
    });

    return NextResponse.json({
      success: true,
      leadsWithDetails,
      totalCommissions: allCommissions?.length || 0
    });

  } catch (error: any) {
    console.error('[DEBUG] Error:', error);
    return NextResponse.json({
      success: false,
      error: error.message
    }, { status: 500 });
  }
}
