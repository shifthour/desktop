import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

const supabase = createClient(supabaseUrl, supabaseServiceKey)

export async function GET(request: NextRequest) {
  try {
    // Fetch ALL counts from all tables without filtering
    const [
      leadsResult,
      contactsResult,
      accountsResult,
      dealsResult,
      productsResult,
      quotationsResult,
      salesOrdersResult,
      installationsResult,
      amcResult,
      complaintsResult,
      activitiesResult
    ] = await Promise.all([
      supabase.from('leads').select('id', { count: 'exact', head: true }),
      supabase.from('contacts').select('id', { count: 'exact', head: true }),
      supabase.from('accounts').select('id', { count: 'exact', head: true }),
      supabase.from('deals').select('id', { count: 'exact', head: true }),
      supabase.from('products').select('id', { count: 'exact', head: true }),
      // Add other tables as they become available
      Promise.resolve({ count: 0 }), // quotations - not implemented yet
      Promise.resolve({ count: 0 }), // sales_orders - not implemented yet  
      Promise.resolve({ count: 0 }), // installations - not implemented yet
      Promise.resolve({ count: 0 }), // amc - not implemented yet
      Promise.resolve({ count: 0 }), // complaints - not implemented yet
      Promise.resolve({ count: 0 })  // activities - not implemented yet
    ])

    const stats = {
      leads: leadsResult.count || 0,
      contacts: contactsResult.count || 0,
      accounts: accountsResult.count || 0,
      deals: dealsResult.count || 0,
      products: productsResult.count || 0,
      quotations: quotationsResult.count || 0,
      salesOrders: salesOrdersResult.count || 0,
      installations: installationsResult.count || 0,
      amc: amcResult.count || 0,
      complaints: complaintsResult.count || 0,
      activities: activitiesResult.count || 0
    }

    return NextResponse.json({ stats })
  } catch (error) {
    console.error('Error fetching navigation stats:', error)
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    )
  }
}