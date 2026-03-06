import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || ''
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || ''

const supabase = createClient(supabaseUrl, supabaseKey)

export async function GET(request: NextRequest) {
  try {
    // Get ALL products without filtering
    const { data: products, error } = await supabase
      .from('products')
      .select('*')
      .order('created_at', { ascending: false })

    if (error) {
      console.error('Error fetching products:', error)
      return NextResponse.json({ error: 'Failed to fetch products' }, { status: 500 })
    }

    return NextResponse.json(products || [])
  } catch (error) {
    console.error('Unexpected error:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const { companyId, ...productData } = body

    if (!companyId) {
      return NextResponse.json({ error: 'Company ID is required' }, { status: 400 })
    }

    // Fetch enabled field configurations for this company
    const { data: fieldConfigs, error: configError } = await supabase
      .from('product_field_configurations')
      .select('field_name, is_enabled')
      .eq('company_id', companyId)
      .eq('is_enabled', true)

    if (configError) {
      console.error('Error fetching field configurations:', configError)
      return NextResponse.json({
        error: 'Failed to load field configurations',
        details: configError.message
      }, { status: 500 })
    }

    // Get list of enabled field names
    const enabledFieldNames = new Set(
      (fieldConfigs || []).map(config => config.field_name)
    )

    // Clean and filter the product data
    // Only include fields that are enabled in the configuration
    const cleanedProductData: any = {}
    Object.keys(productData).forEach(key => {
      // Only process fields that are in the enabled configuration
      if (enabledFieldNames.has(key)) {
        const value = productData[key]
        // Convert empty strings to null, keep other values as-is
        if (value === '' || value === undefined) {
          cleanedProductData[key] = null
        } else if (typeof value === 'string' && value.trim() === '') {
          cleanedProductData[key] = null
        } else {
          cleanedProductData[key] = value
        }
      }
    })

    // Build insert data with only enabled fields
    const insertData: any = {
      company_id: companyId,
      ...cleanedProductData
    }

    const { data, error } = await supabase
      .from('products')
      .insert([insertData])
      .select()
      .single()

    if (error) {
      console.error('Error creating product:', error)
      console.error('Error details:', JSON.stringify(error, null, 2))
      console.error('Insert data:', JSON.stringify(insertData, null, 2))

      // Format error response properly
      return NextResponse.json({
        error: error.message || 'Failed to create product',
        details: error.details || `Error code: ${error.code || 'UNKNOWN'}`,
        hint: error.hint || 'Check the product field configuration to ensure all fields are properly set up',
        code: error.code
      }, { status: 500 })
    }

    return NextResponse.json(data)
  } catch (error) {
    console.error('Unexpected error:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}

export async function PUT(request: NextRequest) {
  try {
    const body = await request.json()
    const { id, companyId, ...productData } = body

    if (!id || !companyId) {
      return NextResponse.json({ error: 'Product ID and Company ID are required' }, { status: 400 })
    }

    // Fetch enabled field configurations for this company
    const { data: fieldConfigs, error: configError } = await supabase
      .from('product_field_configurations')
      .select('field_name, is_enabled')
      .eq('company_id', companyId)
      .eq('is_enabled', true)

    let enabledFieldNames: Set<string> | null = null
    if (!configError && fieldConfigs) {
      enabledFieldNames = new Set(
        fieldConfigs.map(config => config.field_name)
      )
    }

    // Clean the update data - convert empty strings to null
    const cleanedProductData: any = {}
    Object.keys(productData).forEach(key => {
      // If we have field configurations, only process enabled fields
      if (enabledFieldNames && !enabledFieldNames.has(key)) {
        return // Skip this field if it's not enabled
      }

      const value = productData[key]
      // Convert empty strings to null, keep other values as-is
      if (value === '' || value === undefined) {
        cleanedProductData[key] = null
      } else if (typeof value === 'string' && value.trim() === '') {
        cleanedProductData[key] = null
      } else {
        cleanedProductData[key] = value
      }
    })

    const { data, error } = await supabase
      .from('products')
      .update(cleanedProductData)
      .eq('id', id)
      .eq('company_id', companyId)
      .select()
      .single()

    if (error) {
      console.error('Error updating product:', error)
      console.error('Error details:', JSON.stringify(error, null, 2))
      console.error('Update data:', JSON.stringify(cleanedProductData, null, 2))
      return NextResponse.json({ error: 'Failed to update product' }, { status: 500 })
    }

    return NextResponse.json(data)
  } catch (error) {
    console.error('Unexpected error:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}

export async function DELETE(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams
    const id = searchParams.get('id')
    const companyId = searchParams.get('companyId')

    if (!id || !companyId) {
      return NextResponse.json({ error: 'Product ID and Company ID are required' }, { status: 400 })
    }

    const { error } = await supabase
      .from('products')
      .delete()
      .eq('id', id)
      .eq('company_id', companyId)

    if (error) {
      console.error('Error deleting product:', error)
      return NextResponse.json({ error: 'Failed to delete product' }, { status: 500 })
    }

    return NextResponse.json({ message: 'Product deleted successfully' })
  } catch (error) {
    console.error('Unexpected error:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}