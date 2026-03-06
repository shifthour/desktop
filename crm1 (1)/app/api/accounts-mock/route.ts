import { NextRequest, NextResponse } from 'next/server'

// Mock accounts data for demo purposes
const mockAccounts = [
  {
    id: "a0f5c75c-77c9-443d-b8bb-7fc9ae0fcace",
    company_id: "22adbd06-8ce1-49ea-9a03-d0b46720c624",
    account_name: "A E International",
    industry: null,
    account_type: "Customer",
    website: null,
    phone: null,
    description: null,
    billing_street: null,
    billing_city: null,
    billing_state: null,
    billing_country: "Bangladesh",
    billing_postal_code: null,
    owner_id: "45b84401-ca13-4627-ac8f-42a11374633c",
    status: "Active",
    created_at: "2025-08-27T15:49:42.998395+00:00",
    updated_at: "2025-08-27T15:49:42.998395+00:00",
    owner: {
      id: "45b84401-ca13-4627-ac8f-42a11374633c",
      email: "demo-admin@labgig.com",
      full_name: "Demo Administrator"
    },
    parent_account: null
  },
  {
    id: "47746065-280a-4c70-88fb-998a43864a68",
    company_id: "22adbd06-8ce1-49ea-9a03-d0b46720c624",
    account_name: "A Queen marys college",
    industry: "Educational institutions",
    account_type: "Customer",
    website: "www.queenmaryscollege.com",
    phone: null,
    description: null,
    billing_street: null,
    billing_city: "chennai",
    billing_state: null,
    billing_country: "India",
    billing_postal_code: null,
    owner_id: "45b84401-ca13-4627-ac8f-42a11374633c",
    status: "Active",
    created_at: "2025-08-27T15:49:42.998395+00:00",
    updated_at: "2025-08-27T15:49:42.998395+00:00",
    owner: {
      id: "45b84401-ca13-4627-ac8f-42a11374633c",
      email: "demo-admin@labgig.com",
      full_name: "Demo Administrator"
    },
    parent_account: null
  }
]

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const companyId = searchParams.get('companyId')
    const search = searchParams.get('search')
    const limit = parseInt(searchParams.get('limit') || '50')
    const page = parseInt(searchParams.get('page') || '1')
    
    if (!companyId) {
      return NextResponse.json({ error: 'Company ID is required' }, { status: 400 })
    }
    
    // Filter accounts by company
    let filteredAccounts = mockAccounts.filter(account => account.company_id === companyId)
    
    // Apply search filter if provided
    if (search) {
      filteredAccounts = filteredAccounts.filter(account =>
        account.account_name.toLowerCase().includes(search.toLowerCase()) ||
        account.billing_city?.toLowerCase().includes(search.toLowerCase()) ||
        account.website?.toLowerCase().includes(search.toLowerCase())
      )
    }
    
    // Apply pagination
    const offset = (page - 1) * limit
    const paginatedAccounts = filteredAccounts.slice(offset, offset + limit)
    
    return NextResponse.json({
      accounts: paginatedAccounts,
      total: 3843, // Mock total from the imported data
      page,
      totalPages: Math.ceil(3843 / limit)
    })
    
  } catch (error: any) {
    console.error('GET mock accounts error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const { companyId, ...accountData } = body
    
    if (!companyId) {
      return NextResponse.json({ error: 'Company ID is required' }, { status: 400 })
    }
    
    if (!accountData.account_name) {
      return NextResponse.json({ error: 'Account name is required' }, { status: 400 })
    }
    
    // Create mock account
    const newAccount = {
      id: `mock-${Date.now()}`,
      company_id: companyId,
      ...accountData,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      owner: {
        id: "45b84401-ca13-4627-ac8f-42a11374633c",
        email: "demo-admin@labgig.com",
        full_name: "Demo Administrator"
      },
      parent_account: null
    }
    
    mockAccounts.push(newAccount)
    
    return NextResponse.json(newAccount, { status: 201 })
    
  } catch (error: any) {
    console.error('POST mock account error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}