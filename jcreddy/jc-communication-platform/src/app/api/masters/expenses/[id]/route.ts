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
      return amount / 30.44
    case 'quarterly':
      return amount / 91.31
    case 'half_yearly':
      return amount / 182.62
    case 'annually':
      return amount / 365
    default:
      return amount / 30.44
  }
}

export async function PUT(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params
    const supabase = createServerClient()
    const body = await request.json()

    const amount = parseFloat(body.amount)
    const dailyCost = calculateDailyCost(amount, body.frequency)

    const { data, error } = await supabase
      .from('jc_expenses')
      .update({
        expense_name: body.expense_name,
        description: body.description,
        amount: amount,
        frequency: body.frequency,
        daily_cost: Math.round(dailyCost * 100) / 100,
        category: body.category,
        updated_at: new Date().toISOString(),
      })
      .eq('id', id)
      .select()
      .single()

    if (error) throw error

    return NextResponse.json({ expense: data, message: 'Expense updated successfully' })
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

export async function DELETE(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params
    const supabase = createServerClient()

    const { error } = await supabase
      .from('jc_expenses')
      .update({ is_active: false })
      .eq('id', id)

    if (error) throw error

    return NextResponse.json({ message: 'Expense deleted successfully' })
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
