import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

// Calculate daily cost based on frequency
function calculateDailyCost(amount: number, frequency: string): number {
  switch (frequency) {
    case 'daily':
      return amount
    case 'weekly':
      return amount / 7
    case 'monthly':
      // Use average days in a month (30.44)
      return amount / 30.44
    case 'quarterly':
      // 3 months = ~91.31 days
      return amount / 91.31
    case 'half_yearly':
      // 6 months = ~182.62 days
      return amount / 182.62
    case 'annually':
      return amount / 365
    default:
      return amount / 30.44 // Default to monthly
  }
}

export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)
    const search = searchParams.get('search') || ''
    const frequency = searchParams.get('frequency') || ''
    const category = searchParams.get('category') || ''

    let query = supabase
      .from('jc_expenses')
      .select('*', { count: 'exact' })
      .eq('is_active', true)
      .order('created_at', { ascending: false })

    if (search) {
      query = query.or(`expense_name.ilike.%${search}%,description.ilike.%${search}%`)
    }

    if (frequency) {
      query = query.eq('frequency', frequency)
    }

    if (category) {
      query = query.eq('category', category)
    }

    const { data, error, count } = await query

    if (error) throw error

    // Calculate totals
    const expenses = data || []
    const totalAmount = expenses.reduce((sum, e) => sum + (e.amount || 0), 0)
    const totalDailyCost = expenses.reduce((sum, e) => sum + (e.daily_cost || 0), 0)

    return NextResponse.json({
      expenses,
      total: count || 0,
      stats: {
        total_expenses: expenses.length,
        total_amount: totalAmount,
        total_daily_cost: totalDailyCost,
        total_monthly_cost: totalDailyCost * 30.44,
        total_annual_cost: totalDailyCost * 365,
      }
    })
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

export async function POST(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const body = await request.json()

    if (!body.expense_name || !body.amount || !body.frequency) {
      return NextResponse.json(
        { error: 'Expense name, amount and frequency are required' },
        { status: 400 }
      )
    }

    const amount = parseFloat(body.amount)
    const dailyCost = calculateDailyCost(amount, body.frequency)

    const { data, error } = await supabase
      .from('jc_expenses')
      .insert({
        expense_name: body.expense_name,
        description: body.description || null,
        amount: amount,
        frequency: body.frequency,
        daily_cost: Math.round(dailyCost * 100) / 100, // Round to 2 decimals
        category: body.category || null,
      })
      .select()
      .single()

    if (error) throw error

    return NextResponse.json({ expense: data, message: 'Expense created successfully' })
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
