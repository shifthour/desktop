import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
)

export async function POST(request: NextRequest) {
  try {
    console.log('Setting up deal_products table...')
    
    // Create deal_products table
    const { error: tableError } = await supabase.rpc('exec_sql', {
      sql: `
        -- Create deal_products table for multiple products support in deals
        CREATE TABLE IF NOT EXISTS deal_products (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            deal_id UUID NOT NULL REFERENCES deals(id) ON DELETE CASCADE,
            product_id UUID REFERENCES products(id) ON DELETE SET NULL,
            product_name TEXT NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 1,
            price_per_unit DECIMAL(15,2),
            total_amount DECIMAL(15,2) GENERATED ALWAYS AS (quantity * price_per_unit) STORED,
            notes TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL
        );

        -- Add indexes for better performance
        CREATE INDEX IF NOT EXISTS idx_deal_products_deal_id ON deal_products(deal_id);
        CREATE INDEX IF NOT EXISTS idx_deal_products_product_id ON deal_products(product_id);
      `
    })

    if (tableError) {
      console.error('Error creating deal_products table:', tableError)
      return NextResponse.json({ error: 'Failed to create table', details: tableError }, { status: 500 })
    }

    console.log('deal_products table created successfully')
    
    return NextResponse.json({ 
      success: true, 
      message: 'deal_products table created successfully' 
    })
  } catch (error) {
    console.error('Error in setup-deal-products:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}