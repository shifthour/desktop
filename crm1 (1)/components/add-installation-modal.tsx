"use client"

import { useState, useEffect } from "react"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Calendar, CalendarDays, Clock, User, MapPin, Package, Wrench, AlertCircle, FileText } from "lucide-react"
import { useToast } from "@/hooks/use-toast"

interface AddInstallationModalProps {
  isOpen: boolean
  onClose: () => void
  onSave: (installationData: any) => void
  editingInstallation?: any
  sourceType?: 'sales_order' | 'invoice' | 'direct'
  sourceData?: any // Sales order or invoice data
}

export function AddInstallationModal({
  isOpen,
  onClose,
  onSave,
  editingInstallation,
  sourceType = 'direct',
  sourceData
}: AddInstallationModalProps) {
  const { toast } = useToast()
  const [formData, setFormData] = useState({
    // Source information
    source_type: sourceType,
    source_reference: '',
    
    // Customer Information
    customer_name: '',
    contact_person: '',
    customer_phone: '',
    customer_email: '',
    installation_address: '',
    city: '',
    state: '',
    pincode: '',
    
    // Product Information
    product_name: '',
    product_model: '',
    serial_number: '',
    quantity: 1,
    equipment_value: '',
    
    // Installation Details
    installation_type: 'new_installation',
    priority: 'Medium',
    status: 'Scheduled',
    
    // Scheduling
    scheduled_date: '',
    scheduled_time: '',
    estimated_duration: 2, // hours
    
    // Assignment
    assigned_technician: '',
    technician_phone: '',
    
    // Notes
    pre_installation_notes: '',
    special_instructions: '',
    required_tools: '',
    safety_requirements: '',
    
    // Warranty
    warranty_period: 24, // months
    warranty_terms: '',
    
    // Financial
    installation_cost: '',
    additional_charges: '',
    total_cost: '',
    annual_maintenance_charges: ''
  })

  // Pre-populate form when source data is provided
  useEffect(() => {
    if (sourceData) {
      // Function to extract city, state, and pincode from address
      const extractAddressComponents = (address: string) => {
        if (!address) return { city: '', state: '', pincode: '' }
        
        console.log('Extracting from address:', address) // Debug log
        console.error('=== ADDRESS EXTRACTION DEBUG ===')
        console.error('Address input:', address)
        
        let city = '', state = '', pincode = ''
        
        // Extract pincode (6-digit number) - comprehensive patterns
        const pincodePatterns = [
          /\b(\d{6})\b/,                    // Standard 6-digit pattern anywhere
          /PIN[:\s-]*(\d{6})/i,             // "PIN: 123456" or "PIN 123456" or "PIN-123456"
          /PINCODE[:\s-]*(\d{6})/i,         // "PINCODE: 123456" 
          /(\d{6})(?:\s*$)/,                // 6 digits at end
          /(\d{6})(?:\s*,)/,                // 6 digits before comma
          /-\s*(\d{6})/,                    // "- 123456"
          /.*?(\d{6}).*$/,                  // Any 6-digit number in the string (last resort)
          /(\d{6})/                         // Just any 6-digit number (most permissive)
        ]
        
        for (const pattern of pincodePatterns) {
          const pincodeMatch = address.match(pattern)
          if (pincodeMatch) {
            pincode = pincodeMatch[1]
            console.log(`Pincode extracted using pattern ${pattern}:`, pincode)
            console.error(`Pincode extracted using pattern ${pattern}:`, pincode)
            break
          }
        }
        
        if (!pincode) {
          console.error('No pincode found in address:', address)
        }
        
        // Common patterns to extract city and state
        const patterns = [
          // Pattern: "City, State PIN" - most common
          /([^,]+),\s*([^,\d]+)(?:\s*\d{6})?$/,
          // Pattern: "Address, City, State PIN"
          /(?:.*,\s*)?([^,]+),\s*([^,\d]+)(?:\s*\d{6})?$/,
          // Pattern: "City State PIN" (without commas)
          /([A-Za-z\s]+?)\s+([A-Za-z\s]+?)\s+\d{6}$/,
          // Pattern: "City PIN" (extract city only)
          /([A-Za-z\s]+?)\s+\d{6}$/
        ]
        
        // Try to match patterns
        for (let i = 0; i < patterns.length; i++) {
          const pattern = patterns[i]
          const match = address.match(pattern)
          if (match) {
            console.log(`Pattern ${i} matched:`, match) // Debug log
            if (i <= 1) {
              // City, State patterns
              city = match[1].trim()
              state = match[2].trim()
            } else if (i === 2) {
              // City State PIN pattern
              const words = match[1].trim().split(/\s+/)
              const stateWords = match[2].trim().split(/\s+/)
              city = words.join(' ')
              state = stateWords.join(' ')
            } else {
              // City PIN pattern
              city = match[1].trim()
            }
            break
          }
        }
        
        // Common state mappings and city-to-state mapping
        const stateMap: { [key: string]: string } = {
          // State codes
          'KA': 'Karnataka', 'Karnataka': 'Karnataka',
          'MH': 'Maharashtra', 'Maharashtra': 'Maharashtra', 
          'TN': 'Tamil Nadu', 'Tamil Nadu': 'Tamil Nadu',
          'DL': 'Delhi', 'Delhi': 'Delhi',
          'UP': 'Uttar Pradesh', 'Uttar Pradesh': 'Uttar Pradesh',
          'WB': 'West Bengal', 'West Bengal': 'West Bengal',
          'GJ': 'Gujarat', 'Gujarat': 'Gujarat',
          'RJ': 'Rajasthan', 'Rajasthan': 'Rajasthan',
          'AP': 'Andhra Pradesh', 'Andhra Pradesh': 'Andhra Pradesh',
          'TS': 'Telangana', 'Telangana': 'Telangana',
          'KL': 'Kerala', 'Kerala': 'Kerala',
          'OR': 'Odisha', 'Odisha': 'Odisha',
          'PB': 'Punjab', 'Punjab': 'Punjab',
          'HR': 'Haryana', 'Haryana': 'Haryana',
          'JH': 'Jharkhand', 'Jharkhand': 'Jharkhand',
          'AS': 'Assam', 'Assam': 'Assam',
          'BR': 'Bihar', 'Bihar': 'Bihar',
          'CG': 'Chhattisgarh', 'Chhattisgarh': 'Chhattisgarh',
          // Major cities to state mapping
          'Bangalore': 'Karnataka', 'Bengaluru': 'Karnataka',
          'Mumbai': 'Maharashtra', 'Pune': 'Maharashtra', 'Nagpur': 'Maharashtra',
          'Chennai': 'Tamil Nadu', 'Coimbatore': 'Tamil Nadu',
          'Hyderabad': 'Telangana', 'Secunderabad': 'Telangana',
          'Ahmedabad': 'Gujarat', 'Surat': 'Gujarat', 'Vadodara': 'Gujarat',
          'Kolkata': 'West Bengal', 'Howrah': 'West Bengal',
          'Kochi': 'Kerala', 'Thiruvananthapuram': 'Kerala', 'Kozhikode': 'Kerala',
          'Jaipur': 'Rajasthan', 'Jodhpur': 'Rajasthan', 'Udaipur': 'Rajasthan',
          'Lucknow': 'Uttar Pradesh', 'Kanpur': 'Uttar Pradesh', 'Agra': 'Uttar Pradesh',
          'Bhopal': 'Madhya Pradesh', 'Indore': 'Madhya Pradesh',
          'Chandigarh': 'Chandigarh', 'Gurgaon': 'Haryana', 'Faridabad': 'Haryana',
          'Noida': 'Uttar Pradesh', 'Ghaziabad': 'Uttar Pradesh'
        }
        
        // Map state if found in mapping
        if (state && stateMap[state]) {
          state = stateMap[state]
        } else if (city && stateMap[city]) {
          // If city is found in mapping, use it to determine state
          state = stateMap[city]
        }
        
        console.log('Extracted components:', { city, state, pincode }) // Debug log
        return { city, state, pincode }
      }

      // Function to extract and format multiple products
      const extractProducts = (sourceData: any) => {
        console.log('=== PRODUCT EXTRACTION DEBUG START ===')
        console.log('Full source data:', JSON.stringify(sourceData, null, 2)) // Complete debug log
        console.error('=== PRODUCT EXTRACTION DEBUG START ===') // Also log to browser console
        console.error('Full source data:', JSON.stringify(sourceData, null, 2))
        
        let products: string[] = []
        
        // Log all possible product-related fields
        console.log('Checking for product fields:')
        console.log('- line_items:', sourceData.line_items)
        console.log('- items:', sourceData.items) 
        console.log('- dealProducts:', sourceData.dealProducts)
        console.log('- products_quoted:', sourceData.products_quoted)
        console.log('- product:', sourceData.product)
        console.log('- products:', sourceData.products)
        console.log('- product_name:', sourceData.product_name)
        console.log('- item_details:', sourceData.item_details)
        console.log('- quotation_items:', sourceData.quotation_items)
        console.log('- sales_order_items:', sourceData.sales_order_items)
        console.log('- order_items:', sourceData.order_items)
        console.log('- line_items_data:', sourceData.line_items_data)
        
        // Priority 1: Check for direct product fields (sales orders, simple formats)
        // First check products_quoted - most reliable for actual product names
        if (sourceData.products_quoted && typeof sourceData.products_quoted === 'string') {
          const value = sourceData.products_quoted.trim()
          // Only skip if it's clearly a reference number
          if (!value.match(/^(QTN|SO|INST|INV)-\d{4}-\d+$/)) {
            console.log('Found products_quoted:', value)
            console.error('Using products_quoted:', value)
            if (value.includes(',')) {
              products = value.split(',').map((p: string) => p.trim())
            } else {
              products = [value]
            }
          }
        }
        
        // If no products yet, check product_name field
        if (products.length === 0 && sourceData.product_name && typeof sourceData.product_name === 'string') {
          const value = sourceData.product_name.trim()
          if (!value.match(/^(QTN|SO|INST|INV)-\d{4}-\d+$/)) {
            console.log('Found product_name:', value)
            console.error('Using product_name:', value)
            products = [value]
          }
        }
        
        // If no products yet, check products field
        if (products.length === 0 && sourceData.products && typeof sourceData.products === 'string') {
          const value = sourceData.products.trim()
          if (!value.match(/^(QTN|SO|INST|INV)-\d{4}-\d+$/) && value !== 'Product/Service') {
            console.log('Found products:', value)
            console.error('Using products field:', value)
            if (value.includes(',')) {
              products = value.split(',').map((p: string) => p.trim())
            } else {
              products = [value]
            }
          }
        }
        
        // If no products yet, check product field
        if (products.length === 0 && sourceData.product && typeof sourceData.product === 'string') {
          const value = sourceData.product.trim()
          if (!value.match(/^(QTN|SO|INST|INV)-\d{4}-\d+$/) && value !== 'Product/Service') {
            console.log('Found product:', value)
            console.error('Using product field:', value)
            if (value.includes(',')) {
              products = value.split(',').map((p: string) => p.trim())
            } else {
              products = [value]
            }
          }
        }
        // Priority 2: Check for multiple products in different formats
        else if (sourceData.line_items && Array.isArray(sourceData.line_items)) {
          // From line items (quotations/invoices)
          console.log('Found line_items:', sourceData.line_items)
          products = sourceData.line_items.map((item: any) => 
            item.product || item.description || item.name || item.product_name || item.item_name || 'Product'
          )
        } else if (sourceData.items && Array.isArray(sourceData.items)) {
          // From items array 
          console.log('Found items:', sourceData.items)
          products = sourceData.items.map((item: any) => 
            item.product || item.description || item.name || item.product_name || item.item_name || 'Product'
          )
        } else if (sourceData.sales_order_items && Array.isArray(sourceData.sales_order_items)) {
          // From sales order items
          console.log('Found sales_order_items:', sourceData.sales_order_items)
          products = sourceData.sales_order_items.map((item: any) => 
            item.product_name || item.product || item.name || item.description || item.item_name || 'Product'
          )
        } else if (sourceData.order_items && Array.isArray(sourceData.order_items)) {
          // From order items
          console.log('Found order_items:', sourceData.order_items)
          products = sourceData.order_items.map((item: any) => 
            item.product_name || item.product || item.name || item.description || item.item_name || 'Product'
          )
        } else if (sourceData.quotation_items && Array.isArray(sourceData.quotation_items)) {
          // From quotation items
          console.log('Found quotation_items:', sourceData.quotation_items)
          products = sourceData.quotation_items.map((item: any) => 
            item.product_name || item.product || item.name || item.description || item.item_name || 'Product'
          )
        } else if (sourceData.dealProducts && Array.isArray(sourceData.dealProducts)) {
          // From deal products (sales orders from deals)
          console.log('Found dealProducts:', sourceData.dealProducts)
          products = sourceData.dealProducts.map((item: any) => 
            item.product_name || item.product || item.name || 'Product'
          )
        } else if (sourceData.products_quoted) {
          // From comma-separated products_quoted or single product
          console.log('Found products_quoted:', sourceData.products_quoted)
          if (sourceData.products_quoted.includes(',')) {
            products = sourceData.products_quoted.split(',').map((p: string) => p.trim())
          } else {
            products = [sourceData.products_quoted]
          }
        }
        
        // Fallback: Try to extract from any field with "product" in the name
        if (products.length === 0) {
          console.log('No products found, trying fallback extraction...')
          const possibleProductFields = Object.keys(sourceData).filter(key => 
            key.toLowerCase().includes('product') || 
            key.toLowerCase().includes('item')
          ).filter(key => !key.toLowerCase().includes('quotation')) // Exclude quotation-related fields
          console.log('Possible product fields found:', possibleProductFields)
          
          for (const field of possibleProductFields) {
            const value = sourceData[field]
            if (typeof value === 'string' && value.trim() && 
                !value.startsWith('QTN-') && !value.startsWith('SO-') && !value.startsWith('INST-') && 
                !value.includes('quotation') && !value.includes('Quotation')) {
              console.log(`Using fallback field ${field}:`, value)
              console.error(`Using fallback field ${field}:`, value)
              products = [value.trim()]
              break
            } else if (Array.isArray(value) && value.length > 0) {
              console.log(`Using fallback array field ${field}:`, value)
              products = value.map((item: any) => {
                if (typeof item === 'string') return item.trim()
                return item.product_name || item.product || item.name || item.description || 'Product'
              }).filter(p => p && !p.startsWith('QTN-') && !p.startsWith('SO-'))
              if (products.length > 0) break
            }
          }
        }
        
        // Final fallback: Check if there's a quotation reference and try to use that data
        if (products.length === 0 && sourceData.quote_number) {
          console.error('No valid product found, checking quote_number field:', sourceData.quote_number)
          // Don't use the quote number as product name
        }
        
        // Last resort: Leave empty for user to fill, don't use generic name
        if (products.length === 0) {
          console.error('No valid product found, leaving empty for user to fill')
          products = []
        }
        
        // Clean up product names and remove duplicates
        products = [...new Set(products.filter(p => p && p.trim() !== '' && p !== 'Product'))].map(p => p.trim())
        
        console.log('Final extracted products:', products) // Debug log
        console.log('=== PRODUCT EXTRACTION DEBUG END ===')
        
        return {
          productString: products.length > 0 ? products.join(', ') : '',
          quantity: products.length || 1
        }
      }

      // Extract address information
      const addressInfo = sourceData.billing_address || sourceData.shipping_address || sourceData.installation_address || sourceData.address || ''
      const { city, state, pincode } = extractAddressComponents(addressInfo)
      
      // Extract product information  
      const { productString, quantity } = extractProducts(sourceData)

      console.log('Final extracted data:', { // Debug log
        city, state, pincode, productString, quantity,
        addressInfo,
        sourceFields: {
          city: sourceData.city,
          state: sourceData.state, 
          product: sourceData.product,
          products: sourceData.products,
          products_quoted: sourceData.products_quoted
        }
      })
      
      // More visible debugging
      console.error('=== FINAL EXTRACTION RESULTS ===')
      console.error('Product extracted:', productString)
      console.error('Address extracted - City:', city, 'State:', state, 'Pincode:', pincode)
      console.error('Source reference will be:', sourceData.order_id || sourceData.invoice_number || sourceData.id)

      const updatedFormData = {
        ...formData,
        source_type: sourceType,
        source_reference: sourceData.order_id || sourceData.invoice_number || sourceData.id || '',
        
        // Customer information from source
        customer_name: sourceData.customer_name || sourceData.accountName || '',
        contact_person: sourceData.contact_person || sourceData.contactPerson || sourceData.contactName || '',
        customer_phone: sourceData.customer_phone || sourceData.customerPhone || sourceData.phone || '',
        customer_email: sourceData.customer_email || sourceData.customerEmail || sourceData.email || '',
        installation_address: addressInfo,
        city: sourceData.city || city || '',
        state: sourceData.state || state || '',
        pincode: sourceData.pincode || pincode || '',
        
        // Product information from source (improved) - prioritize extracted products  
        // Ensure we don't use IDs as product names
        product_name: productString || 
          (sourceData.products_quoted && !sourceData.products_quoted.startsWith('QTN-') && !sourceData.products_quoted.startsWith('SO-') ? sourceData.products_quoted : '') ||
          (sourceData.product && !sourceData.product.startsWith('QTN-') && !sourceData.product.startsWith('SO-') && sourceData.product !== sourceData.order_id ? sourceData.product : '') ||
          (sourceData.products && !sourceData.products.startsWith('QTN-') && !sourceData.products.startsWith('SO-') && sourceData.products !== sourceData.order_id ? sourceData.products : '') || 
          '',
        quantity: quantity,
        equipment_value: sourceData.total_amount || sourceData.amount || sourceData.value || '',
        
        // Pre-fill notes with more details
        pre_installation_notes: `Generated from ${sourceType.replace('_', ' ')}: ${sourceData.order_id || sourceData.invoice_number || sourceData.id}${productString ? `\nProducts: ${productString}` : ''}`,
        
        // Set default scheduled date (7 days from now)
        scheduled_date: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0],
        scheduled_time: '10:00',
        
        // Annual maintenance charges
        annual_maintenance_charges: ''
      }
      setFormData(updatedFormData)
    }
  }, [sourceData, sourceType])

  // Pre-populate form when editing
  useEffect(() => {
    if (editingInstallation) {
      setFormData({
        ...editingInstallation,
        required_tools: Array.isArray(editingInstallation.required_tools)
          ? editingInstallation.required_tools.join(', ')
          : editingInstallation.required_tools || ''
      })
    }
  }, [editingInstallation])

  // Auto-calculate total cost when installation_cost or additional_charges change
  useEffect(() => {
    const installationCost = parseFloat(formData.installation_cost?.toString()) || 0
    const additionalCharges = parseFloat(formData.additional_charges?.toString()) || 0
    const totalCost = installationCost + additionalCharges
    
    if (totalCost > 0 && totalCost.toString() !== formData.total_cost) {
      setFormData(prev => ({
        ...prev,
        total_cost: totalCost.toString()
      }))
    }
  }, [formData.installation_cost, formData.additional_charges])

  const handleInputChange = (field: string, value: any) => {
    setFormData(prev => ({
      ...prev,
      [field]: value
    }))
    
    // Auto-calculate total cost when financial fields change (Installation Cost + Additional Charges only)
    if (field === 'installation_cost' || field === 'additional_charges') {
      const installationCost = parseFloat(field === 'installation_cost' ? value : formData.installation_cost) || 0
      const additionalCharges = parseFloat(field === 'additional_charges' ? value : formData.additional_charges) || 0
      const totalCost = installationCost + additionalCharges
      
      setFormData(prev => ({
        ...prev,
        [field]: value,
        total_cost: totalCost.toString()
      }))
    }
  }

  const handleSave = () => {
    // Validation
    if (!formData.customer_name || !formData.contact_person) {
      toast({
        title: "Validation Error",
        description: "Please fill in all required fields (Account Name, Contact Person)",
        variant: "destructive"
      })
      return
    }

    if (!formData.scheduled_date) {
      toast({
        title: "Validation Error", 
        description: "Please select a scheduled date",
        variant: "destructive"
      })
      return
    }

    // Process the form data
    const processedData = {
      ...formData,
      quantity: parseInt(formData.quantity.toString()) || 1,
      estimated_duration: parseInt(formData.estimated_duration.toString()) || 2,
      warranty_period: parseInt(formData.warranty_period.toString()) || 24,
      equipment_value: parseFloat(formData.equipment_value.toString()) || 0,
      installation_cost: parseFloat(formData.installation_cost.toString()) || 0,
      additional_charges: parseFloat(formData.additional_charges.toString()) || 0,
      total_cost: parseFloat(formData.total_cost?.toString()) || 0,
      annual_maintenance_charges: parseFloat(formData.annual_maintenance_charges?.toString()) || 0,
      
      // Convert comma-separated strings to arrays
      required_tools: formData.required_tools ? formData.required_tools.split(',').map(s => s.trim()) : [],
      
      // Set warranty dates
      warranty_start_date: formData.scheduled_date,
      warranty_end_date: formData.warranty_period > 0 ? 
        new Date(new Date(formData.scheduled_date).setMonth(
          new Date(formData.scheduled_date).getMonth() + parseInt(formData.warranty_period.toString())
        )).toISOString().split('T')[0] : null
    }

    // Filter to only include fields that exist in current database schema
    // Map frontend field names to database field names
    const fieldMapping = {
      customer_name: 'account_name', // Map customer_name to account_name for consistency
      customer_phone: 'contact_phone', // Map to existing field
      customer_email: 'contact_email', // Map to existing field
      assigned_technician: 'technician_name', // Map to existing field
      scheduled_date: 'installation_date', // Map to existing field
      pre_installation_notes: 'installation_notes', // Map to existing field
      // All other fields exist in database as-is after running the SQL script
      city: 'city',
      state: 'state', 
      pincode: 'pincode',
      source_type: 'source_type',
      source_reference: 'source_reference',
      quantity: 'quantity',
      equipment_value: 'equipment_value',
      installation_type: 'installation_type',
      scheduled_time: 'scheduled_time',
      estimated_duration: 'estimated_duration',
      technician_phone: 'technician_phone',
      special_instructions: 'special_instructions',
      required_tools: 'required_tools',
      safety_requirements: 'safety_requirements',
      warranty_period: 'warranty_period',
      warranty_terms: 'warranty_terms',
      installation_cost: 'installation_cost',
      additional_charges: 'additional_charges',
      total_cost: 'total_cost',
      annual_maintenance_charges: 'annual_maintenance_charges',
      product_model: 'product_model'
    }

    // Create filtered data with only existing database columns
    const filteredData = {}
    for (const [key, value] of Object.entries(processedData)) {
      const dbField = fieldMapping[key]
      if (dbField === null) {
        // Skip fields that don't exist in database
        continue
      } else if (dbField) {
        // Map to correct database field name
        filteredData[dbField] = value
      } else {
        // Keep field as-is (exists in database with same name)
        filteredData[key] = value
      }
    }

    console.log('Filtered installation data:', filteredData)
    onSave(filteredData)
  }

  const resetForm = () => {
    setFormData({
      source_type: 'direct',
      source_reference: '',
      customer_name: '', contact_person: '', customer_phone: '', customer_email: '',
      installation_address: '', city: '', state: '', pincode: '',
      product_name: '', product_model: '', serial_number: '', quantity: 1, equipment_value: '',
      installation_type: 'new_installation', priority: 'Medium', status: 'Scheduled',
      scheduled_date: '', scheduled_time: '', estimated_duration: 2,
      assigned_technician: '', technician_phone: '',
      pre_installation_notes: '', special_instructions: '', required_tools: '', safety_requirements: '',
      warranty_period: 24, warranty_terms: '',
      installation_cost: '', additional_charges: '', total_cost: '', annual_maintenance_charges: ''
    })
  }

  const handleClose = () => {
    resetForm()
    onClose()
  }

  return (
    <Dialog open={isOpen} onOpenChange={handleClose}>
      <DialogContent className="max-w-4xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Wrench className="w-5 h-5" />
            {editingInstallation ? 'Edit Installation' : 'Schedule New Installation'}
          </DialogTitle>
          <DialogDescription>
            {sourceType !== 'direct' && sourceData ? 
              `Create installation from ${sourceType.replace('_', ' ')}: ${sourceData.order_id || sourceData.invoice_number || sourceData.id}` :
              'Schedule a new installation for your customer'
            }
          </DialogDescription>
        </DialogHeader>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 py-4">
          {/* Source Information */}
          {sourceType !== 'direct' && (
            <div className="md:col-span-2 p-4 bg-blue-50 rounded-lg border border-blue-200">
              <div className="flex items-center gap-2 mb-2">
                <AlertCircle className="w-4 h-4 text-blue-600" />
                <span className="font-medium text-blue-800">Source Information</span>
              </div>
              <div className="text-sm text-blue-700">
                Creating installation from {sourceType.replace('_', ' ')}: <strong>{formData.source_reference}</strong>
              </div>
            </div>
          )}

          {/* Customer Information */}
          <div className="space-y-4">
            <h3 className="text-lg font-semibold flex items-center gap-2">
              <User className="w-4 h-4" />
              Account Information
            </h3>
            
            <div className="space-y-2">
              <Label htmlFor="customer_name">Account Name *</Label>
              <Input
                id="customer_name"
                value={formData.customer_name}
                onChange={(e) => handleInputChange('customer_name', e.target.value)}
                placeholder="Enter account name"
              />
            </div>
            
            {/* Add Sales Order/Invoice Number field */}
            {sourceType !== 'direct' && (
              <div className="space-y-2">
                <Label htmlFor="source_reference">
                  {sourceType === 'sales_order' ? 'Sales Order Number' : sourceType === 'invoice' ? 'Invoice Number' : 'Reference Number'}
                </Label>
                <Input
                  id="source_reference"
                  value={formData.source_reference}
                  readOnly
                  className="bg-gray-50"
                />
              </div>
            )}
            
            <div className="space-y-2">
              <Label htmlFor="contact_person">Contact Person *</Label>
              <Input
                id="contact_person"
                value={formData.contact_person}
                onChange={(e) => handleInputChange('contact_person', e.target.value)}
                placeholder="Enter contact person name"
              />
            </div>
            
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="customer_phone">Phone</Label>
                <Input
                  id="customer_phone"
                  value={formData.customer_phone}
                  onChange={(e) => handleInputChange('customer_phone', e.target.value)}
                  placeholder="+91-XXXXXXXXXX"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="customer_email">Email</Label>
                <Input
                  id="customer_email"
                  type="email"
                  value={formData.customer_email}
                  onChange={(e) => handleInputChange('customer_email', e.target.value)}
                  placeholder="customer@example.com"
                />
              </div>
            </div>
          </div>

          {/* Installation Address */}
          <div className="space-y-4">
            <h3 className="text-lg font-semibold flex items-center gap-2">
              <MapPin className="w-4 h-4" />
              Installation Address
            </h3>
            
            <div className="space-y-2">
              <Label htmlFor="installation_address">Address</Label>
              <Textarea
                id="installation_address"
                value={formData.installation_address}
                onChange={(e) => handleInputChange('installation_address', e.target.value)}
                placeholder="Enter complete installation address"
                rows={3}
              />
            </div>
            
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="city">City</Label>
                <Input
                  id="city"
                  value={formData.city}
                  onChange={(e) => handleInputChange('city', e.target.value)}
                  placeholder="City"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="state">State</Label>
                <Input
                  id="state"
                  value={formData.state}
                  onChange={(e) => handleInputChange('state', e.target.value)}
                  placeholder="State"
                />
              </div>
            </div>
          </div>

          {/* Product Information */}
          <div className="space-y-4">
            <h3 className="text-lg font-semibold flex items-center gap-2">
              <Package className="w-4 h-4" />
              Product Information
            </h3>
            
            {/* PDF link for Sales Order/Invoice */}
            {sourceType !== 'direct' && formData.source_reference && (
              <div className="mt-2">
                <Button
                  variant="outline"
                  size="sm"
                  type="button"
                  onClick={async () => {
                    // Generate professional HTML document for printing (same format as sales order PDF)
                    const printWindow = window.open('', '_blank')
                    if (printWindow) {
                      const title = sourceType === 'sales_order' ? 'Sales Order' : 'Invoice'
                      const refNumber = formData.source_reference
                      
                      // Extract products information (same logic as sales order PDF)
                      let itemsData = [{
                        product: sourceData.product || sourceData.products || 'Product/Service',
                        description: sourceData.product_description || sourceData.description || '',
                        quantity: sourceData.quantity || 1,
                        unitPrice: sourceData.unit_price || sourceData.total_amount || sourceData.amount || 0,
                        discount: sourceData.discount || 0,
                        amount: sourceData.total_amount || sourceData.amount || 0
                      }]

                      // If there's a quotation reference, try to fetch detailed items
                      if (sourceData.quotation_ref) {
                        try {
                          const response = await fetch(`/api/quotations?companyId=de19ccb7-e90d-4507-861d-a3aecf5e3f29`)
                          if (response.ok) {
                            const data = await response.json()
                            const quotations = data.quotations || []
                            
                            const originalQuotation = quotations.find((q: any) => 
                              q.quote_number === sourceData.quotation_ref || 
                              q.quotationId === sourceData.quotation_ref ||
                              q.id === sourceData.quotation_ref
                            )
                            
                            if (originalQuotation && originalQuotation.line_items) {
                              console.log("Found original quotation items for PDF:", originalQuotation.line_items)
                              itemsData = originalQuotation.line_items.map((item: any) => ({
                                product: item.product || item.name || 'Product/Service',
                                description: item.description || '',
                                quantity: item.quantity || 1,
                                unitPrice: item.unitPrice || item.unit_price || 0,
                                discount: item.discount || 0,
                                amount: item.amount || 0
                              }))
                            }
                          }
                        } catch (error) {
                          console.log('Could not fetch quotation details for PDF:', error)
                        }
                      }
                      
                      const html = `
                        <!DOCTYPE html>
                        <html>
                        <head>
                          <title>${title}: ${refNumber}</title>
                          <style>
                            @page { size: A4; margin: 20mm; }
                            body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
                            .header { border-bottom: 2px solid #2563eb; padding-bottom: 20px; margin-bottom: 30px; }
                            .company-name { font-size: 24px; font-weight: bold; color: #1e40af; }
                            .order-title { text-align: center; font-size: 28px; color: #1e40af; margin: 30px 0; font-weight: bold; }
                            .info-section { margin-bottom: 20px; }
                            .field { margin: 8px 0; }
                            .field label { font-weight: bold; display: inline-block; width: 150px; }
                            .customer-section { background: #f3f4f6; padding: 15px; border-radius: 8px; margin-bottom: 30px; }
                            .items-table { width: 100%; border-collapse: collapse; margin-bottom: 30px; }
                            .items-table th { background: #2563eb; color: white; padding: 10px; text-align: left; }
                            .items-table td { padding: 10px; border-bottom: 1px solid #e5e7eb; }
                            .totals-section { text-align: right; margin-top: 20px; }
                            .total-row { display: flex; justify-content: flex-end; margin: 5px 0; }
                            .total-label { width: 150px; text-align: right; margin-right: 20px; }
                            .total-value { width: 150px; text-align: right; }
                            .grand-total { font-size: 18px; font-weight: bold; color: #1e40af; border-top: 2px solid #2563eb; padding-top: 10px; }
                            .footer { margin-top: 30px; text-align: center; color: #666; font-size: 12px; }
                          </style>
                        </head>
                        <body>
                          <div class="header">
                            <div class="company-name">Your Company Name</div>
                          </div>
                          
                          <div class="order-title">${title}</div>
                          
                          <div class="info-section">
                            <div class="field">
                              <label>${title} Number:</label> ${refNumber}
                            </div>
                            <div class="field">
                              <label>Date:</label> ${sourceData.order_date || sourceData.date || new Date().toLocaleDateString()}
                            </div>
                          </div>
                          
                          <div class="customer-section">
                            <h3>Customer Information</h3>
                            <div class="field">
                              <label>Account Name:</label> ${sourceData.customer_name || sourceData.accountName || 'N/A'}
                            </div>
                            <div class="field">
                              <label>Contact Person:</label> ${sourceData.contact_person || sourceData.contactPerson || 'N/A'}
                            </div>
                            <div class="field">
                              <label>Email:</label> ${sourceData.customer_email || sourceData.customerEmail || sourceData.email || 'N/A'}
                            </div>
                            <div class="field">
                              <label>Phone:</label> ${sourceData.customer_phone || sourceData.customerPhone || sourceData.phone || 'N/A'}
                            </div>
                            <div class="field">
                              <label>Address:</label> ${sourceData.billing_address || sourceData.shipping_address || sourceData.address || 'N/A'}
                            </div>
                          </div>
                          
                          <table class="items-table">
                            <thead>
                              <tr>
                                <th style="width: 5%;">S.No</th>
                                <th style="width: 35%;">Product/Service</th>
                                <th style="width: 25%;">Description</th>
                                <th style="width: 8%;">Qty</th>
                                <th style="width: 12%;">Unit Price</th>
                                <th style="width: 8%;">Discount</th>
                                <th style="width: 12%;">Amount</th>
                              </tr>
                            </thead>
                            <tbody>
                              ${itemsData.map((item, index) => `
                                <tr>
                                  <td>${index + 1}</td>
                                  <td>${item.product}</td>
                                  <td>${item.description}</td>
                                  <td style="text-align: center;">${item.quantity}</td>
                                  <td style="text-align: right;">₹${(item.unitPrice || 0).toLocaleString()}</td>
                                  <td style="text-align: center;">${item.discount}%</td>
                                  <td style="text-align: right;">₹${(item.amount || 0).toLocaleString()}</td>
                                </tr>
                              `).join('')}
                            </tbody>
                          </table>
                          
                          <div class="totals-section">
                            <div class="total-row grand-total">
                              <span class="total-label">Total Amount:</span>
                              <span class="total-value">₹${(sourceData.total_amount || sourceData.amount || 0).toLocaleString()}</span>
                            </div>
                          </div>
                          
                          <div class="footer">
                            <p><small>Generated on: ${new Date().toLocaleString()}</small></p>
                          </div>
                        </body>
                        </html>
                      `
                      printWindow.document.write(html)
                      printWindow.document.close()
                      printWindow.focus()
                      setTimeout(() => {
                        printWindow.print()
                      }, 250)
                    }
                  }}
                  className="text-blue-600 hover:text-blue-800"
                >
                  <FileText className="w-4 h-4 mr-2" />
                  View {sourceType === 'sales_order' ? 'Sales Order' : 'Invoice'} PDF
                </Button>
              </div>
            )}
            
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="product_model">Model</Label>
                <Input
                  id="product_model"
                  value={formData.product_model}
                  onChange={(e) => handleInputChange('product_model', e.target.value)}
                  placeholder="Model number"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="serial_number">Serial Number</Label>
                <Input
                  id="serial_number"
                  value={formData.serial_number}
                  onChange={(e) => handleInputChange('serial_number', e.target.value)}
                  placeholder="Serial number"
                />
              </div>
            </div>
            
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="quantity">Quantity</Label>
                <Input
                  id="quantity"
                  type="number"
                  min="1"
                  value={formData.quantity}
                  onChange={(e) => handleInputChange('quantity', e.target.value)}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="equipment_value">Equipment Value (₹)</Label>
                <Input
                  id="equipment_value"
                  type="number"
                  value={formData.equipment_value}
                  onChange={(e) => handleInputChange('equipment_value', e.target.value)}
                  placeholder="0.00"
                />
              </div>
            </div>
          </div>

          {/* Installation Details */}
          <div className="space-y-4">
            <h3 className="text-lg font-semibold flex items-center gap-2">
              <Wrench className="w-4 h-4" />
              Installation Details
            </h3>
            
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="installation_type">Installation Type</Label>
                <Select value={formData.installation_type} onValueChange={(value) => handleInputChange('installation_type', value)}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="new_installation">New Installation</SelectItem>
                    <SelectItem value="replacement">Replacement</SelectItem>
                    <SelectItem value="upgrade">Upgrade</SelectItem>
                    <SelectItem value="repair">Repair</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label htmlFor="priority">Priority</Label>
                <Select value={formData.priority} onValueChange={(value) => handleInputChange('priority', value)}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="Low">Low</SelectItem>
                    <SelectItem value="Medium">Medium</SelectItem>
                    <SelectItem value="High">High</SelectItem>
                    <SelectItem value="Critical">Critical</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          </div>

          {/* Scheduling */}
          <div className="space-y-4">
            <h3 className="text-lg font-semibold flex items-center gap-2">
              <Calendar className="w-4 h-4" />
              Scheduling
            </h3>
            
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="scheduled_date">Scheduled Date *</Label>
                <Input
                  id="scheduled_date"
                  type="date"
                  value={formData.scheduled_date}
                  onChange={(e) => handleInputChange('scheduled_date', e.target.value)}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="scheduled_time">Scheduled Time</Label>
                <Input
                  id="scheduled_time"
                  type="time"
                  value={formData.scheduled_time}
                  onChange={(e) => handleInputChange('scheduled_time', e.target.value)}
                />
              </div>
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="estimated_duration">Estimated Duration (hours)</Label>
              <Input
                id="estimated_duration"
                type="number"
                min="0.5"
                step="0.5"
                value={formData.estimated_duration}
                onChange={(e) => handleInputChange('estimated_duration', e.target.value)}
              />
            </div>
          </div>

          {/* Assignment */}
          <div className="space-y-4">
            <h3 className="text-lg font-semibold">Assignment</h3>
            
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="assigned_technician">Assigned Technician</Label>
                <Input
                  id="assigned_technician"
                  value={formData.assigned_technician}
                  onChange={(e) => handleInputChange('assigned_technician', e.target.value)}
                  placeholder="Technician name"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="technician_phone">Technician Phone</Label>
                <Input
                  id="technician_phone"
                  value={formData.technician_phone}
                  onChange={(e) => handleInputChange('technician_phone', e.target.value)}
                  placeholder="+91-XXXXXXXXXX"
                />
              </div>
            </div>
            
          </div>

          {/* Warranty Information */}
          <div className="space-y-4">
            <h3 className="text-lg font-semibold">Warranty Information</h3>
            
            <div className="space-y-2">
              <Label htmlFor="warranty_period">Warranty Period (months)</Label>
              <Input
                id="warranty_period"
                type="number"
                min="0"
                value={formData.warranty_period}
                onChange={(e) => handleInputChange('warranty_period', e.target.value)}
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="warranty_terms">Warranty Terms</Label>
              <Textarea
                id="warranty_terms"
                value={formData.warranty_terms}
                onChange={(e) => handleInputChange('warranty_terms', e.target.value)}
                placeholder="Warranty terms and conditions"
                rows={3}
              />
            </div>
          </div>

          {/* Notes and Instructions */}
          <div className="md:col-span-2 space-y-4">
            <h3 className="text-lg font-semibold">Notes and Instructions</h3>
            
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="pre_installation_notes">Pre-Installation Notes</Label>
                <Textarea
                  id="pre_installation_notes"
                  value={formData.pre_installation_notes}
                  onChange={(e) => handleInputChange('pre_installation_notes', e.target.value)}
                  placeholder="Notes before installation"
                  rows={3}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="special_instructions">Special Instructions</Label>
                <Textarea
                  id="special_instructions"
                  value={formData.special_instructions}
                  onChange={(e) => handleInputChange('special_instructions', e.target.value)}
                  placeholder="Special requirements or instructions"
                  rows={3}
                />
              </div>
            </div>
            
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="required_tools">Required Tools (comma separated)</Label>
                <Input
                  id="required_tools"
                  value={formData.required_tools}
                  onChange={(e) => handleInputChange('required_tools', e.target.value)}
                  placeholder="Screwdriver, Drill, Calibration Kit"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="safety_requirements">Safety Requirements</Label>
                <Input
                  id="safety_requirements"
                  value={formData.safety_requirements}
                  onChange={(e) => handleInputChange('safety_requirements', e.target.value)}
                  placeholder="Safety gear, protocols"
                />
              </div>
            </div>
          </div>

          {/* Financial Information */}
          <div className="md:col-span-2 space-y-4">
            <h3 className="text-lg font-semibold">Financial Information</h3>
            
            <div className="grid grid-cols-3 gap-4">
              <div className="space-y-2">
                <Label htmlFor="installation_cost">Installation Cost (₹)</Label>
                <Input
                  id="installation_cost"
                  type="number"
                  value={formData.installation_cost}
                  onChange={(e) => handleInputChange('installation_cost', e.target.value)}
                  placeholder="0.00"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="additional_charges">Additional Charges (₹)</Label>
                <Input
                  id="additional_charges"
                  type="number"
                  value={formData.additional_charges}
                  onChange={(e) => handleInputChange('additional_charges', e.target.value)}
                  placeholder="0.00"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="annual_maintenance_charges">Annual Maintenance Charges (₹)</Label>
                <Input
                  id="annual_maintenance_charges"
                  type="number"
                  value={formData.annual_maintenance_charges}
                  onChange={(e) => handleInputChange('annual_maintenance_charges', e.target.value)}
                  placeholder="0.00"
                />
              </div>
            </div>
          </div>
        </div>

        <div className="flex justify-end space-x-2 pt-4">
          <Button variant="outline" onClick={handleClose}>
            Cancel
          </Button>
          <Button onClick={handleSave} className="bg-blue-600 hover:bg-blue-700">
            <Wrench className="w-4 h-4 mr-2" />
            {editingInstallation ? 'Update Installation' : 'Schedule Installation'}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  )
}