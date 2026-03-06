import { NextRequest, NextResponse } from 'next/server'

// GET - Return standard deal field configurations (hardcoded for consistency)
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const companyId = searchParams.get('companyId')

    if (!companyId) {
      return NextResponse.json({ error: 'Company ID is required' }, { status: 400 })
    }

    // Standard deal fields configuration
    const dealFields = [
      {
        field_name: 'deal_name',
        field_label: 'Deal Name',
        field_type: 'text',
        is_mandatory: true,
        is_enabled: true,
        placeholder: 'Enter deal name',
        help_text: 'Name or title of this deal',
        display_order: 1
      },
      {
        field_name: 'account_name',
        field_label: 'Account Name',
        field_type: 'text',
        is_mandatory: true,
        is_enabled: true,
        placeholder: 'Enter account name',
        help_text: 'Company or organization name',
        display_order: 2
      },
      {
        field_name: 'contact_person',
        field_label: 'Contact Person',
        field_type: 'text',
        is_mandatory: true,
        is_enabled: true,
        placeholder: 'Enter contact person',
        help_text: 'Primary contact for this deal',
        display_order: 3
      },
      {
        field_name: 'email',
        field_label: 'Email Address',
        field_type: 'email',
        is_mandatory: false,
        is_enabled: true,
        placeholder: 'Enter email',
        help_text: 'Contact email address',
        display_order: 4
      },
      {
        field_name: 'phone',
        field_label: 'Phone Number',
        field_type: 'tel',
        is_mandatory: false,
        is_enabled: true,
        placeholder: 'Enter phone number',
        help_text: 'Contact phone number',
        display_order: 5
      },
      {
        field_name: 'whatsapp',
        field_label: 'WhatsApp Number',
        field_type: 'tel',
        is_mandatory: false,
        is_enabled: true,
        placeholder: 'Enter WhatsApp number',
        help_text: 'WhatsApp number for contact',
        display_order: 6
      },
      {
        field_name: 'department',
        field_label: 'Department',
        field_type: 'text',
        is_mandatory: false,
        is_enabled: true,
        placeholder: 'Enter department',
        help_text: 'Department of contact person',
        display_order: 7
      },
      {
        field_name: 'product',
        field_label: 'Primary Product',
        field_type: 'text',
        is_mandatory: true,
        is_enabled: true,
        placeholder: 'Enter product/service',
        help_text: 'Main product or service for this deal',
        display_order: 8
      },
      {
        field_name: 'value',
        field_label: 'Deal Amount',
        field_type: 'number',
        is_mandatory: true,
        is_enabled: true,
        placeholder: 'Enter deal value',
        help_text: 'Total value of the deal in INR',
        display_order: 9
      },
      {
        field_name: 'stage',
        field_label: 'Sales Stage',
        field_type: 'select',
        is_mandatory: true,
        is_enabled: true,
        field_options: ['Qualification', 'Demo', 'Proposal', 'Negotiation', 'Closed Won', 'Closed Lost'],
        placeholder: 'Select stage',
        help_text: 'Current stage of the deal',
        display_order: 10
      },
      {
        field_name: 'probability',
        field_label: 'Win Probability (%)',
        field_type: 'number',
        is_mandatory: false,
        is_enabled: true,
        placeholder: 'Enter probability (0-100)',
        help_text: 'Likelihood of winning this deal (0-100)',
        display_order: 11
      },
      {
        field_name: 'expected_close_date',
        field_label: 'Expected Close Date',
        field_type: 'date',
        is_mandatory: false,
        is_enabled: true,
        placeholder: 'Select date',
        help_text: 'Expected date to close this deal',
        display_order: 12
      },
      {
        field_name: 'source',
        field_label: 'Lead Source',
        field_type: 'select',
        is_mandatory: false,
        is_enabled: true,
        field_options: ['Website', 'Referral', 'Cold Call', 'Email Campaign', 'Social Media', 'Trade Show', 'Partner', 'Other'],
        placeholder: 'Select source',
        help_text: 'How this deal originated',
        display_order: 13
      },
      {
        field_name: 'assigned_to',
        field_label: 'Assigned To',
        field_type: 'text',
        is_mandatory: false,
        is_enabled: true,
        placeholder: 'Enter assigned account',
        help_text: 'Account responsible for this deal',
        display_order: 14
      },
      {
        field_name: 'priority',
        field_label: 'Priority',
        field_type: 'select',
        is_mandatory: false,
        is_enabled: true,
        field_options: ['High', 'Medium', 'Low'],
        placeholder: 'Select priority',
        help_text: 'Priority level of this deal',
        display_order: 15
      },
      {
        field_name: 'city',
        field_label: 'City',
        field_type: 'text',
        is_mandatory: false,
        is_enabled: true,
        placeholder: 'Enter city',
        help_text: 'City location',
        display_order: 16
      },
      {
        field_name: 'state',
        field_label: 'State',
        field_type: 'text',
        is_mandatory: false,
        is_enabled: true,
        placeholder: 'Enter state',
        help_text: 'State location',
        display_order: 17
      },
      {
        field_name: 'notes',
        field_label: 'Notes',
        field_type: 'textarea',
        is_mandatory: false,
        is_enabled: true,
        placeholder: 'Enter additional notes',
        help_text: 'Additional information about this deal',
        display_order: 18
      }
    ]

    return NextResponse.json(dealFields)
  } catch (error: any) {
    console.error('GET deal fields error:', error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}
