import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

// Initialize Supabase Admin client
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

const supabase = createClient(supabaseUrl, supabaseServiceKey)

export async function POST(request: NextRequest) {
  try {
    const { action } = await request.json()
    
    if (action === 'check_table') {
      // Check if accounts table exists
      const { data, error } = await supabase
        .from('accounts')
        .select('id')
        .limit(1)
      
      if (error && error.message.includes('relation "public.accounts" does not exist')) {
        return NextResponse.json({ 
          exists: false, 
          message: 'Accounts table does not exist. Please create it using the SQL migration.' 
        })
      }
      
      if (error) {
        return NextResponse.json({ error: error.message }, { status: 500 })
      }
      
      // Get count of existing accounts
      const { count } = await supabase
        .from('accounts')
        .select('*', { count: 'exact', head: true })
      
      return NextResponse.json({ 
        exists: true, 
        count: count || 0,
        message: `Accounts table exists with ${count || 0} records.`
      })
    }
    
    if (action === 'get_stats') {
      // Get statistics about accounts
      const { data: totalCount } = await supabase
        .from('accounts')
        .select('*', { count: 'exact', head: true })
      
      const { data: byIndustry } = await supabase
        .from('accounts')
        .select('industry')
        .not('industry', 'is', null)
      
      const { data: byCity } = await supabase
        .from('accounts')
        .select('billing_city')
        .not('billing_city', 'is', null)
      
      const industryCount = byIndustry?.reduce((acc: any, item) => {
        acc[item.industry] = (acc[item.industry] || 0) + 1
        return acc
      }, {})
      
      const cityCount = byCity?.reduce((acc: any, item) => {
        acc[item.billing_city] = (acc[item.billing_city] || 0) + 1
        return acc
      }, {})
      
      return NextResponse.json({
        total: totalCount,
        byIndustry: industryCount,
        byCity: Object.entries(cityCount || {})
          .sort((a: any, b: any) => b[1] - a[1])
          .slice(0, 10)
          .reduce((acc: any, [key, val]) => {
            acc[key] = val
            return acc
          }, {})
      })
    }
    
    return NextResponse.json({ error: 'Invalid action' }, { status: 400 })
    
  } catch (error: any) {
    console.error('Setup API error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}