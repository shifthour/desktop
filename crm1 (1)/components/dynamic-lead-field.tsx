"use client"

import { useState, useEffect } from "react"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"

interface FieldConfig {
  field_name: string
  field_label: string
  field_type: string
  is_mandatory: boolean
  is_enabled: boolean
  field_options?: string[]
  placeholder?: string
  validation_rules?: any
  help_text?: string
}

interface DynamicLeadFieldProps {
  config: FieldConfig
  value: any
  onChange: (fieldName: string, value: any) => void
  error?: string
  // For dependent dropdowns and auto-population
  dependentValues?: Record<string, any>
  onAccountSelect?: (accountData: any) => void
  onContactSelect?: (contactData: any) => void
}

export function DynamicLeadField({
  config,
  value,
  onChange,
  error,
  dependentValues = {},
  onAccountSelect,
  onContactSelect
}: DynamicLeadFieldProps) {
  const [accounts, setAccounts] = useState<any[]>([])
  const [contacts, setContacts] = useState<any[]>([])
  const [users, setUsers] = useState<any[]>([])
  const [loading, setLoading] = useState(false)

  // Fetch accounts when field is 'account'
  useEffect(() => {
    if (config.field_name === 'account' && config.field_type === 'select_dependent') {
      fetchAccounts()
    }
  }, [config.field_name, config.field_type])

  // Fetch contacts when field is 'contact' and account is selected
  useEffect(() => {
    if (config.field_name === 'contact' && config.field_type === 'select_dependent') {
      console.log('Contact field checking for account_id:', dependentValues.account_id)
      console.log('All dependent values:', dependentValues)
      if (dependentValues.account_id) {
        console.log('Fetching contacts for account:', dependentValues.account_id)
        fetchContacts(dependentValues.account_id)
      } else {
        console.log('No account_id found, clearing contacts')
        setContacts([])
      }
    }
  }, [config.field_name, config.field_type, dependentValues.account_id])

  // Fetch users when field is 'assigned_to'
  useEffect(() => {
    if (config.field_name === 'assigned_to' && config.field_type === 'select_dependent') {
      fetchUsers()
    }
  }, [config.field_name, config.field_type])

  const fetchAccounts = async () => {
    try {
      setLoading(true)
      const response = await fetch('/api/accounts')
      if (response.ok) {
        const data = await response.json()
        setAccounts(data.accounts || [])
      }
    } catch (error) {
      console.error('Error fetching accounts:', error)
    } finally {
      setLoading(false)
    }
  }

  const fetchContacts = async (accountId: string) => {
    try {
      setLoading(true)
      const response = await fetch(`/api/contacts?accountId=${accountId}`)
      if (response.ok) {
        const data = await response.json()
        setContacts(data.contacts || [])
      }
    } catch (error) {
      console.error('Error fetching contacts:', error)
    } finally {
      setLoading(false)
    }
  }

  const fetchUsers = async () => {
    try {
      setLoading(true)
      const user = localStorage.getItem('user')
      if (!user) return

      const parsedUser = JSON.parse(user)
      const companyId = parsedUser.company_id

      const response = await fetch(`/api/users?companyId=${companyId}`)
      if (response.ok) {
        const data = await response.json()
        setUsers(data || [])
      }
    } catch (error) {
      console.error('Error fetching users:', error)
    } finally {
      setLoading(false)
    }
  }

  const handleChange = (newValue: any) => {
    // For account dropdown, find the account ID by matching the display name
    if (config.field_name === 'account' && config.field_type === 'select_dependent') {
      const selectedAccount = accounts.find(acc => {
        const displayName = acc.account_name || acc.name || `Account ${acc.id}`
        return displayName === newValue
      })

      if (selectedAccount) {
        onChange(config.field_name, selectedAccount.id)
        if (onAccountSelect) {
          onAccountSelect(selectedAccount)
        }
      }
      return
    }

    // For contact dropdown, find the contact ID by matching the display name
    if (config.field_name === 'contact' && config.field_type === 'select_dependent') {
      const selectedContact = contacts.find(cont => {
        const firstName = cont.first_name || ''
        const lastName = cont.last_name || ''
        const displayName = `${firstName} ${lastName}`.trim() || cont.email || `Contact ${cont.id}`
        return displayName === newValue
      })

      if (selectedContact) {
        onChange(config.field_name, selectedContact.id)
        if (onContactSelect) {
          onContactSelect(selectedContact)
        }
      }
      return
    }

    // For assigned_to dropdown, find the user full_name by matching the display name
    if (config.field_name === 'assigned_to' && config.field_type === 'select_dependent') {
      const selectedUser = users.find(user => user.full_name === newValue)
      if (selectedUser) {
        onChange(config.field_name, selectedUser.full_name)
      }
      return
    }

    // For all other fields, use the value as-is
    onChange(config.field_name, newValue)
  }

  const getOptions = () => {
    // Special handling for account dropdown
    if (config.field_name === 'account' && config.field_type === 'select_dependent') {
      return Array.isArray(accounts) ? accounts.map(acc => acc.account_name || acc.name || `Account ${acc.id}`) : []
    }

    // Special handling for contact dropdown
    if (config.field_name === 'contact' && config.field_type === 'select_dependent') {
      return Array.isArray(contacts) ? contacts.map(cont => {
        const firstName = cont.first_name || ''
        const lastName = cont.last_name || ''
        return `${firstName} ${lastName}`.trim() || cont.email || `Contact ${cont.id}`
      }) : []
    }

    // Special handling for assigned_to dropdown
    if (config.field_name === 'assigned_to' && config.field_type === 'select_dependent') {
      return Array.isArray(users) ? users.map(user => user.full_name || user.email || `User ${user.id}`) : []
    }

    // Special handling for lead_source field
    if (config.field_name === 'lead_source') {
      return [
        'Website_Form',
        'Email_Campaign',
        'Social_Media',
        'Referral',
        'Trade_Show',
        'Cold_Call',
        'Partnership',
        'Advertisement',
        'Content_Marketing',
        'Webinar',
        'Direct'
      ]
    }

    // Special handling for lead_type field
    if (config.field_name === 'lead_type') {
      return [
        'New_Business',
        'Existing_Customer',
        'Cross_Sell',
        'Up_Sell',
        'Renewal',
        'Partner',
        'Referral',
        'Inbound',
        'Outbound',
        'Prospect'
      ]
    }

    // Special handling for lead_status field
    if (config.field_name === 'lead_status') {
      return ['New', 'Contacted', 'Qualified', 'Nurturing', 'Lost', 'Unqualified', 'Recycled']
    }

    // Special handling for lead_stage field
    if (config.field_name === 'lead_stage') {
      return [
        'Initial_Contact',
        'Information_Gathering',
        'Qualification',
        'Needs_Analysis',
        'Proposal',
        'Negotiation',
        'Decision',
        'Conversion',
        'Follow_Up'
      ]
    }

    // Special handling for priority_level field
    if (config.field_name === 'priority_level') {
      return ['High', 'Medium', 'Low']
    }

    // Special handling for consent_status field
    if (config.field_name === 'consent_status') {
      return ['True', 'False']
    }

    // For regular select fields
    return config.field_options || []
  }

  const getDisplayValue = () => {
    if (!value) return ''

    // For account dropdown, convert ID to display name
    if (config.field_name === 'account' && config.field_type === 'select_dependent') {
      const selectedAccount = accounts.find(acc => acc.id === value)
      if (selectedAccount) {
        return selectedAccount.account_name || selectedAccount.name || `Account ${selectedAccount.id}`
      }
      return ''
    }

    // For contact dropdown, convert ID to display name
    if (config.field_name === 'contact' && config.field_type === 'select_dependent') {
      const selectedContact = contacts.find(cont => cont.id === value)
      if (selectedContact) {
        const firstName = selectedContact.first_name || ''
        const lastName = selectedContact.last_name || ''
        return `${firstName} ${lastName}`.trim() || selectedContact.email || `Contact ${selectedContact.id}`
      }
      return ''
    }

    return value
  }

  const renderField = () => {
    switch (config.field_type) {
      case 'text':
      case 'email':
      case 'tel':
      case 'url':
        return (
          <Input
            id={config.field_name}
            type={config.field_type}
            value={value || ''}
            onChange={(e) => handleChange(e.target.value)}
            placeholder={config.placeholder}
            className={error ? 'border-red-500' : ''}
          />
        )

      case 'number':
        return (
          <Input
            id={config.field_name}
            type="number"
            value={value || ''}
            onChange={(e) => handleChange(e.target.value)}
            placeholder={config.placeholder}
            className={error ? 'border-red-500' : ''}
          />
        )

      case 'textarea':
        return (
          <Textarea
            id={config.field_name}
            value={value || ''}
            onChange={(e) => handleChange(e.target.value)}
            placeholder={config.placeholder}
            className={error ? 'border-red-500' : ''}
            rows={3}
          />
        )

      case 'date':
        return (
          <Input
            id={config.field_name}
            type="date"
            value={value || ''}
            onChange={(e) => handleChange(e.target.value)}
            className={error ? 'border-red-500' : ''}
          />
        )

      case 'select':
      case 'select_dependent':
        const options = getOptions()
        const displayValue = getDisplayValue()

        return (
          <Select
            value={displayValue}
            onValueChange={handleChange}
            disabled={loading || (config.field_name === 'contact' && !dependentValues.account_id)}
          >
            <SelectTrigger className={error ? 'border-red-500' : ''}>
              <SelectValue
                placeholder={
                  loading
                    ? 'Loading...'
                    : config.field_name === 'contact' && !dependentValues.account_id
                    ? 'Select account first'
                    : config.placeholder || `Select ${config.field_label.toLowerCase()}`
                }
              />
            </SelectTrigger>
            <SelectContent>
              {options.map((option, index) => (
                <SelectItem key={`${config.field_name}-${index}`} value={option}>
                  {option}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        )

      default:
        return (
          <Input
            id={config.field_name}
            value={value || ''}
            onChange={(e) => handleChange(e.target.value)}
            placeholder={config.placeholder}
            className={error ? 'border-red-500' : ''}
          />
        )
    }
  }

  if (!config.is_enabled) {
    return null
  }

  return (
    <div className="space-y-2">
      <Label htmlFor={config.field_name}>
        {config.field_label}
        {config.is_mandatory && <span className="text-red-500 ml-1">*</span>}
      </Label>
      {renderField()}
      {config.help_text && (
        <p className="text-xs text-gray-500">{config.help_text}</p>
      )}
      {error && (
        <p className="text-xs text-red-500">{error}</p>
      )}
    </div>
  )
}
