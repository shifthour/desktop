import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'
import * as XLSX from 'xlsx'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

const supabase = createClient(supabaseUrl, supabaseServiceKey)

// Map industry names
function mapIndustry(industry: string | null): string | null {
  if (!industry) return null
  const industryMap: { [key: string]: string } = {
    'Educational institutions': 'Education',
    'Educational institution': 'Education',
    'Biotech Company': 'Biotechnology',
    'Diagnostics': 'Healthcare',
    'Diagnostic': 'Healthcare',
    'Dairy': 'Food & Beverage',
    'Distillery': 'Food & Beverage',
    'Environmental': 'Environmental Services',
    'Food Testing': 'Food & Beverage',
    'Instrumentation': 'Manufacturing',
    'Research Institute': 'Research'
  }
  return industryMap[industry] || industry
}

// Parse date from DD/MM/YYYY to ISO format
function parseExcelDate(dateStr: string | null): string | null {
  if (!dateStr) return null
  const parts = dateStr.split('/')
  if (parts.length === 3) {
    const [day, month, year] = parts
    return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`
  }
  return dateStr
}

// Clean phone numbers
function cleanPhone(phone: string | null): string | null {
  if (!phone) return null
  return phone.replace(/\s+/g, ' ').trim()
}

export async function POST(request: NextRequest) {
  try {
    const formData = await request.formData()
    const file = formData.get('file') as File
    const companyId = formData.get('companyId') as string
    const ownerId = formData.get('ownerId') as string
    
    if (!file) {
      return NextResponse.json({ error: 'No file provided' }, { status: 400 })
    }
    
    if (!companyId || !ownerId) {
      return NextResponse.json({ error: 'Company ID and Owner ID are required' }, { status: 400 })
    }
    
    // Read the Excel file
    const bytes = await file.arrayBuffer()
    const buffer = Buffer.from(bytes)
    const workbook = XLSX.read(buffer, { type: 'buffer' })
    
    // Get the first sheet
    const sheetName = workbook.SheetNames[0]
    const worksheet = workbook.Sheets[sheetName]
    
    // Convert to JSON
    const data = XLSX.utils.sheet_to_json(worksheet, { defval: null })
    
    if (!data || data.length === 0) {
      return NextResponse.json({ error: 'No data found in file' }, { status: 400 })
    }
    
    // Process and import accounts
    const results = {
      total: data.length,
      imported: 0,
      skipped: 0,
      errors: [] as any[],
      duplicates: [] as any[]
    }
    
    const batchSize = 50
    const accountsToImport = []
    
    for (let i = 0; i < data.length; i++) {
      const row: any = data[i]
      
      // Skip if no account name
      if (!row['Account Name'] || row['Account Name'].trim() === '') {
        results.skipped++
        continue
      }
      
      const accountName = row['Account Name'].trim()
      
      // Check for duplicate
      const { data: existing } = await supabase
        .from('accounts')
        .select('id')
        .eq('company_id', companyId)
        .eq('account_name', accountName)
        .single()
      
      if (existing) {
        results.duplicates.push(accountName)
        results.skipped++
        continue
      }
      
      // Prepare account data
      const account = {
        company_id: companyId,
        account_name: accountName,
        industry: mapIndustry(row['Industry']),
        account_type: 'Customer',
        website: row['Website'],
        phone: cleanPhone(row['Billing Phone']),
        description: row['Description'],
        
        // Billing address
        billing_street: row['Billing Address'],
        billing_city: row['Billing City'],
        billing_state: row['Billing State/Province'],
        billing_country: row['Billing Country'],
        billing_postal_code: row['Billing Zip/PostalCode'],
        
        // Shipping address
        shipping_street: row['Shipping Address'],
        shipping_city: row['Shipping City'],
        shipping_state: row['Shipping State/Province'],
        shipping_country: row['Shipping Country'],
        shipping_postal_code: row['Shipping Zip/PostalCode'],
        
        // Business info
        turnover_range: row['TurnOver'],
        credit_days: row['Credit Days'] ? parseInt(row['Credit Days']) : null,
        credit_amount: row['Credit Amount'] ? parseFloat(row['Credit Amount']) : 0,
        
        // Tax info
        gstin: row['GSTIN'],
        pan_number: row['PAN No'],
        vat_tin: row['VAT TIN'],
        cst_number: row['CST NO'],
        
        // Metadata
        owner_id: ownerId,
        status: 'Active',
        original_id: row['Sr No']?.toString(),
        original_created_by: row['Created By'],
        original_modified_by: row['Last Modified by'],
        original_created_at: parseExcelDate(row['Created By Date']),
        original_modified_at: parseExcelDate(row['Last Modified Date']),
        assigned_to_names: row['AssignTo']
      }
      
      // Remove null/undefined values
      Object.keys(account).forEach(key => {
        if (account[key as keyof typeof account] === null || 
            account[key as keyof typeof account] === undefined || 
            account[key as keyof typeof account] === '') {
          delete account[key as keyof typeof account]
        }
      })
      
      accountsToImport.push(account)
      
      // Import in batches
      if (accountsToImport.length >= batchSize) {
        const { data: imported, error } = await supabase
          .from('accounts')
          .insert(accountsToImport)
          .select()
        
        if (error) {
          results.errors.push({ batch: i, error: error.message })
        } else {
          results.imported += imported.length
        }
        
        accountsToImport.length = 0 // Clear array
      }
    }
    
    // Import remaining accounts
    if (accountsToImport.length > 0) {
      const { data: imported, error } = await supabase
        .from('accounts')
        .insert(accountsToImport)
        .select()
      
      if (error) {
        results.errors.push({ batch: 'final', error: error.message })
      } else {
        results.imported += imported.length
      }
    }
    
    return NextResponse.json({
      success: true,
      results,
      message: `Successfully imported ${results.imported} accounts out of ${results.total}`
    })
    
  } catch (error: any) {
    console.error('Import error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}