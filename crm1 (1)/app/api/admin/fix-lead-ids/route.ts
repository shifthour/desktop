import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
)

export async function POST(request: NextRequest) {
  try {
    const { companyId } = await request.json()

    if (!companyId) {
      return NextResponse.json({ error: 'Company ID is required' }, { status: 400 })
    }

    // Fetch all leads with null account_id or contact_id
    const { data: leads, error: leadsError } = await supabase
      .from('leads')
      .select('id, account_name, contact_name, company_id')
      .eq('company_id', companyId)
      .or('account_id.is.null,contact_id.is.null')

    if (leadsError) {
      console.error('Error fetching leads:', leadsError)
      return NextResponse.json({ error: 'Failed to fetch leads' }, { status: 500 })
    }

    if (!leads || leads.length === 0) {
      return NextResponse.json({ message: 'No leads need fixing', updated: 0 })
    }

    // Fetch all accounts for this company
    const { data: accounts, error: accountsError } = await supabase
      .from('accounts')
      .select('id, account_name')
      .eq('company_id', companyId)

    if (accountsError) {
      console.error('Error fetching accounts:', accountsError)
      return NextResponse.json({ error: 'Failed to fetch accounts' }, { status: 500 })
    }

    // Fetch all contacts for this company
    const { data: contacts, error: contactsError } = await supabase
      .from('contacts')
      .select('id, first_name, last_name')
      .eq('company_id', companyId)

    if (contactsError) {
      console.error('Error fetching contacts:', contactsError)
      return NextResponse.json({ error: 'Failed to fetch contacts' }, { status: 500 })
    }

    let updatedCount = 0

    // Update each lead
    for (const lead of leads) {
      const updates: any = {}

      // Find matching account
      if (lead.account_name) {
        const matchedAccount = accounts?.find(
          a => a.account_name?.toLowerCase() === lead.account_name.toLowerCase()
        )
        if (matchedAccount) {
          updates.account_id = matchedAccount.id
        }
      }

      // Find matching contact (by concatenating first_name and last_name)
      if (lead.contact_name) {
        const matchedContact = contacts?.find(c => {
          const fullName = `${c.first_name} ${c.last_name}`.trim()
          return fullName.toLowerCase() === lead.contact_name.toLowerCase()
        })
        if (matchedContact) {
          updates.contact_id = matchedContact.id
        }
      }

      // Only update if we found at least one match
      if (Object.keys(updates).length > 0) {
        const { error: updateError } = await supabase
          .from('leads')
          .update(updates)
          .eq('id', lead.id)

        if (!updateError) {
          updatedCount++
        } else {
          console.error(`Error updating lead ${lead.id}:`, updateError)
        }
      }
    }

    return NextResponse.json({
      message: 'Leads updated successfully',
      total: leads.length,
      updated: updatedCount
    })
  } catch (error) {
    console.error('Error fixing lead IDs:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}
