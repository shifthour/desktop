import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

export async function GET(request: NextRequest) {
  try {
    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
    const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY
    const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY
    
    // Log what we have
    const debug = {
      hasUrl: !!supabaseUrl,
      hasServiceKey: !!supabaseServiceKey,
      hasAnonKey: !!supabaseAnonKey,
      url: supabaseUrl?.substring(0, 30) + '...',
      usingKey: supabaseServiceKey ? 'service' : 'anon'
    }
    
    if (!supabaseUrl || (!supabaseServiceKey && !supabaseAnonKey)) {
      return NextResponse.json({ 
        error: 'Missing Supabase credentials',
        debug 
      }, { status: 500 })
    }
    
    const supabase = createClient(
      supabaseUrl, 
      supabaseServiceKey || supabaseAnonKey!
    )
    
    // Try to fetch counts from each table
    const results: any = {}
    
    // Test accounts table
    const { count: accountsCount, error: accountsError } = await supabase
      .from('accounts')
      .select('*', { count: 'exact', head: true })
    
    results.accounts = {
      count: accountsCount,
      error: accountsError?.message
    }
    
    // Test leads table
    const { count: leadsCount, error: leadsError } = await supabase
      .from('leads')
      .select('*', { count: 'exact', head: true })
    
    results.leads = {
      count: leadsCount,
      error: leadsError?.message
    }
    
    // Test products table
    const { count: productsCount, error: productsError } = await supabase
      .from('products')
      .select('*', { count: 'exact', head: true })
    
    results.products = {
      count: productsCount,
      error: productsError?.message
    }
    
    // Test deals table
    const { count: dealsCount, error: dealsError } = await supabase
      .from('deals')
      .select('*', { count: 'exact', head: true })
    
    results.deals = {
      count: dealsCount,
      error: dealsError?.message
    }
    
    // Try to fetch first account with details
    const { data: firstAccount, error: firstAccountError } = await supabase
      .from('accounts')
      .select('id, account_name, created_at')
      .limit(1)
    
    results.firstAccount = {
      data: firstAccount,
      error: firstAccountError?.message
    }
    
    return NextResponse.json({
      success: true,
      debug,
      results,
      timestamp: new Date().toISOString()
    })
    
  } catch (error: any) {
    return NextResponse.json({ 
      error: error.message,
      stack: error.stack
    }, { status: 500 })
  }
}