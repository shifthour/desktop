"use client"

import { useState, useRef } from "react"
import dynamic from 'next/dynamic'
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Progress } from "@/components/ui/progress"
import { Separator } from "@/components/ui/separator"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  Upload,
  Download,
  FileSpreadsheet,
  CheckCircle,
  AlertCircle,
  X,
  ArrowRight,
  ArrowLeft,
  FileText,
  Database,
  MapPin,
  Package,
  ShoppingCart,
  Receipt,
  Zap
} from "lucide-react"

interface DataImportModalProps {
  isOpen: boolean
  onClose: () => void
  moduleType: 'leads' | 'accounts' | 'contacts' | 'opportunities' | 'products' | 'quotations' | 'sales orders' | 'invoices'
  onImport: (data: any[]) => void
}

interface FieldMapping {
  csvField: string
  crmField: string
  required: boolean
}

interface ImportStep {
  step: number
  title: string
  description: string
}

export function DataImportModal({ isOpen, onClose, moduleType, onImport }: DataImportModalProps) {
  const [currentStep, setCurrentStep] = useState(1)
  const [uploadedFile, setUploadedFile] = useState<File | null>(null)
  const [csvData, setCsvData] = useState<any[]>([])
  const [csvHeaders, setCsvHeaders] = useState<string[]>([])
  const [fieldMappings, setFieldMappings] = useState<FieldMapping[]>([])
  const [validationErrors, setValidationErrors] = useState<string[]>([])
  const [importProgress, setImportProgress] = useState(0)
  const [isImporting, setIsImporting] = useState(false)
  const [importSummary, setImportSummary] = useState({ successful: 0, failed: 0, total: 0 })
  
  const fileInputRef = useRef<HTMLInputElement>(null)

  const moduleConfigs = {
    leads: {
      title: "Import Leads",
      icon: <Zap className="w-5 h-5" />,
      fields: [
        { key: "leadName", label: "Lead/Company Name", required: true },
        { key: "contactName", label: "Contact Person", required: true },
        { key: "email", label: "Email Address", required: false },
        { key: "phone", label: "Phone Number", required: false },
        { key: "mobile", label: "Mobile Number", required: false },
        { key: "whatsapp", label: "WhatsApp Number", required: false },
        { key: "industry", label: "Industry", required: false },
        { key: "leadSource", label: "Lead Source", required: false },
        { key: "salesStage", label: "Sales Stage", required: false },
        { key: "priority", label: "Priority", required: false },
        { key: "estimatedValue", label: "Deal Value", required: false },
        { key: "closingDate", label: "Expected Closing", required: false },
        { key: "leadOwner", label: "Assigned To", required: false },
        { key: "city", label: "City", required: false },
        { key: "state", label: "State", required: false },
        { key: "country", label: "Country", required: false },
        { key: "website", label: "Website", required: false },
        { key: "notes", label: "Notes", required: false },
        { key: "productNames", label: "Product Names (comma-separated)", required: false },
        { key: "productQuantities", label: "Product Quantities (comma-separated)", required: false }
      ]
    },
    accounts: {
      title: "Import Accounts",
      icon: <Database className="w-5 h-5" />,
      fields: [
        { key: "accountName", label: "Account Name", required: true },
        { key: "industry", label: "Industry", required: false },
        { key: "contactName", label: "Primary Contact", required: false },
        { key: "contactNo", label: "Contact Number", required: false },
        { key: "email", label: "Email Address", required: false },
        { key: "website", label: "Website", required: false },
        { key: "address", label: "Address", required: false },
        { key: "city", label: "City", required: false },
        { key: "state", label: "State", required: false },
        { key: "country", label: "Country", required: false },
        { key: "assignedTo", label: "Assigned To", required: false },
        { key: "status", label: "Account Status", required: false }
      ]
    },
    contacts: {
      title: "Import Contacts", 
      icon: <MapPin className="w-5 h-5" />,
      fields: [
        { key: "firstName", label: "First Name", required: true },
        { key: "lastName", label: "Last Name", required: true },
        { key: "email", label: "Email Address", required: true },
        { key: "phone", label: "Phone Number", required: false },
        { key: "mobile", label: "Mobile Number", required: false },
        { key: "whatsapp", label: "WhatsApp", required: false },
        { key: "accountName", label: "Account Name", required: true },
        { key: "title", label: "Job Title", required: false },
        { key: "department", label: "Department", required: true },
        { key: "linkedin", label: "LinkedIn Profile", required: false },
        { key: "city", label: "City", required: false },
        { key: "state", label: "State", required: false },
        { key: "country", label: "Country", required: false },
        { key: "priority", label: "Priority Level", required: false },
        { key: "status", label: "Contact Status", required: false }
      ]
    },
    opportunities: {
      title: "Import Opportunities",
      icon: <FileText className="w-5 h-5" />,
      fields: [
        { key: "opportunityName", label: "Opportunity Name", required: true },
        { key: "accountName", label: "Account Name", required: true },
        { key: "contactName", label: "Contact Person", required: false },
        { key: "amount", label: "Deal Amount", required: true },
        { key: "stage", label: "Sales Stage", required: true },
        { key: "probability", label: "Win Probability (%)", required: false },
        { key: "closingDate", label: "Expected Close Date", required: false },
        { key: "leadSource", label: "Lead Source", required: false },
        { key: "owner", label: "Opportunity Owner", required: false },
        { key: "product", label: "Primary Product", required: false },
        { key: "competitorInfo", label: "Competitor Info", required: false },
        { key: "nextStep", label: "Next Steps", required: false },
        { key: "description", label: "Description", required: false }
      ]
    },
    products: {
      title: "Import Products",
      icon: <Package className="w-5 h-5" />,
      fields: [
        { key: "product_name", label: "Product Name", required: true },
        { key: "product_reference_no", label: "Product Reference No/ID", required: false },
        { key: "description", label: "Description", required: false },
        { key: "principal", label: "Principal", required: false },
        { key: "category", label: "Category", required: false },
        { key: "sub_category", label: "Sub Category", required: false },
        { key: "price", label: "Price", required: true },
        { key: "branch", label: "Branch/Division", required: false },
        { key: "status", label: "Status", required: false },
        { key: "assigned_to", label: "Assigned To", required: false },
        { key: "model", label: "Model", required: false }
      ]
    },
    quotations: {
      title: "Import Quotations",
      icon: <FileText className="w-5 h-5" />,
      fields: [
        { key: "quotationId", label: "Quotation ID", required: true },
        { key: "date", label: "Date", required: true },
        { key: "accountName", label: "Account Name", required: true },
        { key: "contactName", label: "Contact Person", required: false },
        { key: "product", label: "Product/Service", required: true },
        { key: "amount", label: "Amount", required: true },
        { key: "status", label: "Status", required: false },
        { key: "validUntil", label: "Valid Until", required: false },
        { key: "assignedTo", label: "Assigned To", required: false },
        { key: "revision", label: "Revision", required: false },
        { key: "terms", label: "Terms & Conditions", required: false },
        { key: "notes", label: "Notes", required: false }
      ]
    },
    "sales orders": {
      title: "Import Sales Orders",
      icon: <ShoppingCart className="w-5 h-5" />,
      fields: [
        { key: "orderId", label: "Order ID", required: true },
        { key: "date", label: "Order Date", required: true },
        { key: "account", label: "Account Name", required: true },
        { key: "contactName", label: "Contact Person", required: false },
        { key: "product", label: "Product/Service", required: true },
        { key: "amount", label: "Amount", required: true },
        { key: "status", label: "Status", required: false },
        { key: "deliveryDate", label: "Delivery Date", required: false },
        { key: "assignedTo", label: "Assigned To", required: false },
        { key: "paymentStatus", label: "Payment Status", required: false },
        { key: "shippingAddress", label: "Shipping Address", required: false },
        { key: "specialInstructions", label: "Special Instructions", required: false }
      ]
    },
    invoices: {
      title: "Import Invoices",
      icon: <Receipt className="w-5 h-5" />,
      fields: [
        { key: "invoiceId", label: "Invoice ID", required: true },
        { key: "customer", label: "Customer Name", required: true },
        { key: "amount", label: "Amount", required: true },
        { key: "issueDate", label: "Issue Date", required: true },
        { key: "dueDate", label: "Due Date", required: true },
        { key: "status", label: "Status", required: false },
        { key: "paymentMethod", label: "Payment Method", required: false },
        { key: "items", label: "Items/Description", required: false },
        { key: "contact", label: "Contact Person", required: false },
        { key: "gstNumber", label: "GST Number", required: false },
        { key: "salesOrderRef", label: "Sales Order Reference", required: false },
        { key: "notes", label: "Notes", required: false }
      ]
    }
  }

  const currentConfig = moduleConfigs[moduleType] || {
    title: `Import ${moduleType}`,
    icon: <FileText className="w-5 h-5" />,
    fields: []
  }
  
  const importSteps: ImportStep[] = [
    {
      step: 1,
      title: "Upload File",
      description: "Upload your CSV or Excel file containing the data"
    },
    {
      step: 2,
      title: "Map Fields",
      description: "Map your file columns to CRM fields"
    },
    {
      step: 3,
      title: "Preview & Validate", 
      description: "Review data and fix any validation errors"
    },
    {
      step: 4,
      title: "Import Data",
      description: "Import the validated data into your CRM"
    }
  ]

  const handleFileUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    console.log('File upload handler called')
    try {
      const file = event.target.files?.[0]
      if (!file) {
        console.log('No file selected')
        return
      }

      console.log('File selected:', file.name, file.type, file.size)
      setUploadedFile(file)
      
      const fileExtension = file.name.split('.').pop()?.toLowerCase()
      console.log('File extension:', fileExtension)
      
      if (fileExtension === 'csv') {
        // Handle CSV files
        const reader = new FileReader()
        reader.onload = (e) => {
          try {
            const content = e.target?.result as string
            const lines = content.split('\n')
            const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''))
            const data = lines.slice(1).filter(line => line.trim()).map(line => {
              const values = line.split(',').map(v => v.trim().replace(/"/g, ''))
              const row: any = {}
              headers.forEach((header, index) => {
                row[header] = values[index] || ''
              })
              return row
            })

            setCsvHeaders(headers)
            setCsvData(data)
            
            // Initialize field mappings
            const mappings: FieldMapping[] = currentConfig.fields.map(field => ({
              csvField: '',
              crmField: field.key,
              required: field.required
            }))
            setFieldMappings(mappings)
            
            setCurrentStep(2)
          } catch (error) {
            console.error('Error parsing CSV:', error)
            alert('Error reading CSV file. Please check the file format.')
          }
        }
        reader.readAsText(file)
      } else if (fileExtension === 'xlsx' || fileExtension === 'xls') {
        // Handle Excel files with dynamic import to avoid SSR issues
        console.log('Processing Excel file...')
        
        try {
          // Dynamic import of XLSX to avoid build/SSR issues
          const XLSX = await import('xlsx')
          console.log('XLSX library loaded dynamically')
          
          const reader = new FileReader()
          reader.onload = (e) => {
            try {
              console.log('FileReader loaded successfully')
              const data = e.target?.result
              if (!data) {
                throw new Error('Failed to read file data')
              }
              
              console.log('Reading workbook with XLSX...')
              const workbook = XLSX.read(data, { type: 'array' })
              console.log('Workbook loaded, sheet names:', workbook.SheetNames)
              
              if (!workbook.SheetNames || workbook.SheetNames.length === 0) {
                throw new Error('No worksheets found in Excel file')
              }
              
              // Get the first worksheet
              const firstSheetName = workbook.SheetNames[0]
              const worksheet = workbook.Sheets[firstSheetName]
              console.log('Processing worksheet:', firstSheetName)
              
              // Convert to JSON
              const jsonData = XLSX.utils.sheet_to_json(worksheet, { header: 1 }) as any[][]
              console.log('JSON data extracted, rows:', jsonData.length)
              
              if (jsonData.length === 0) {
                alert('The Excel file appears to be empty')
                return
              }
              
              // Extract headers from first row
              const headers = jsonData[0].map(h => String(h || '').trim())
              console.log('Headers extracted:', headers)
              
              // Extract data from remaining rows
              const dataRows = jsonData.slice(1).filter(row => row && row.some(cell => cell)).map(row => {
                const rowData: any = {}
                headers.forEach((header, index) => {
                  rowData[header] = String(row[index] || '').trim()
                })
                return rowData
              })
              
              console.log('Data rows processed:', dataRows.length)
              
              setCsvHeaders(headers)
              setCsvData(dataRows)
              
              // Initialize field mappings
              const mappings: FieldMapping[] = currentConfig.fields.map(field => ({
                csvField: '',
                crmField: field.key,
                required: field.required
              }))
              setFieldMappings(mappings)
              
              console.log('Moving to step 2')
              setCurrentStep(2)
            } catch (error) {
              console.error('Error parsing Excel file:', error)
              alert(`Error reading Excel file: ${error instanceof Error ? error.message : 'Unknown error'}`)
            }
          }
          
          reader.onerror = (error) => {
            console.error('FileReader error:', error)
            alert('Error reading file. Please try again.')
          }
          
          console.log('Starting to read file as ArrayBuffer...')
          reader.readAsArrayBuffer(file)
        } catch (importError) {
          console.error('Error loading XLSX library:', importError)
          alert('Failed to load Excel processing library. Please refresh the page and try again.')
        }
      } else {
        console.log('Unsupported file type:', fileExtension)
        alert('Please upload a CSV or Excel file')
      }
    } catch (error) {
      console.error('Error in file upload handler:', error)
      alert('An error occurred while processing the file. Please try again.')
    }
  }

  const handleFieldMapping = (crmField: string, csvField: string) => {
    setFieldMappings(prev => prev.map(mapping => 
      mapping.crmField === crmField 
        ? { ...mapping, csvField }
        : mapping
    ))
  }

  const validateData = () => {
    const errors: string[] = []
    const requiredMappings = fieldMappings.filter(m => m.required && !m.csvField)
    
    if (requiredMappings.length > 0) {
      errors.push(`Missing required field mappings: ${requiredMappings.map(m => 
        currentConfig.fields.find(f => f.key === m.crmField)?.label
      ).join(', ')}`)
    }

    // Validate sample data
    const sampleData = csvData.slice(0, 5)
    sampleData.forEach((row, index) => {
      fieldMappings.forEach(mapping => {
        if (mapping.required && mapping.csvField && !row[mapping.csvField]) {
          errors.push(`Row ${index + 1}: Missing required field "${currentConfig.fields.find(f => f.key === mapping.crmField)?.label}"`)
        }
      })
    })

    setValidationErrors(errors)
    return errors.length === 0
  }

  const handlePreview = () => {
    if (validateData()) {
      setCurrentStep(3)
    }
  }

  const handleImport = async () => {
    setIsImporting(true)
    setCurrentStep(4)
    
    try {
      // Simulate import process
      const mappedData = csvData.map(row => {
        const mappedRow: any = {}
        fieldMappings.forEach(mapping => {
          if (mapping.csvField && row[mapping.csvField]) {
            mappedRow[mapping.crmField] = row[mapping.csvField]
          }
        })
        return mappedRow
      })

      // Simulate progress
      for (let i = 0; i <= 100; i += 10) {
        await new Promise(resolve => setTimeout(resolve, 200))
        setImportProgress(i)
      }

      setImportSummary({
        successful: mappedData.length,
        failed: 0,
        total: mappedData.length
      })

      onImport(mappedData)
      
    } catch (error) {
      console.error('Import failed:', error)
    } finally {
      setIsImporting(false)
    }
  }

  const downloadTemplate = () => {
    const headers = currentConfig.fields.map(field => field.label)
    const csvContent = headers.join(',') + '\n'
    console.log('Template headers:', headers) // Debug log
    
    // Add sample row
    const sampleRow = currentConfig.fields.map(field => {
      switch (field.key) {
        case 'leadName':
        case 'accountName':
          return 'Sample Company Ltd.'
        case 'contactName':
        case 'firstName':
          return 'John'
        case 'lastName':
          return 'Doe'
        case 'email':
          return 'john.doe@example.com'
        case 'phone':
        case 'mobile':
          return '+91 98765 43210'
        case 'industry':
          return 'Biotechnology'
        case 'address':
          return '123 MG Road, Koramangala, Bangalore'
        case 'city':
          return 'Bangalore'
        case 'state':
          return 'Karnataka'
        case 'country':
          return 'India'
        case 'estimatedValue':
        case 'amount':
          return '500000'
        case 'priority':
          return 'High'
        case 'salesStage':
        case 'stage':
          return 'Qualified'
        case 'productNames':
          return 'Headphones, Mouse, Keyboard'
        case 'productQuantities':
          return '2, 5, 3'
        default:
          return 'Sample Value'
      }
    })
    
    const finalContent = csvContent + sampleRow.join(',')
    
    const blob = new Blob([finalContent], { type: 'text/csv' })
    const url = window.URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = `${moduleType}_import_template.csv`
    link.click()
    window.URL.revokeObjectURL(url)
  }

  const resetModal = () => {
    setCurrentStep(1)
    setUploadedFile(null)
    setCsvData([])
    setCsvHeaders([])
    setFieldMappings([])
    setValidationErrors([])
    setImportProgress(0)
    setIsImporting(false)
    setImportSummary({ successful: 0, failed: 0, total: 0 })
  }

  const handleClose = () => {
    resetModal()
    onClose()
  }

  const renderStep1 = () => (
    <div className="space-y-6">
      <div className="text-center">
        <div className="mx-auto w-16 h-16 bg-blue-100 rounded-full flex items-center justify-center mb-4">
          <Upload className="w-8 h-8 text-blue-600" />
        </div>
        <h3 className="text-lg font-semibold mb-2">Upload Your Data File</h3>
        <p className="text-gray-600 mb-6">
          Upload a CSV or Excel file containing your {moduleType} data
        </p>
      </div>

      <Card className="border-dashed border-2 border-gray-300 hover:border-blue-400 transition-colors">
        <CardContent className="p-8">
          <div className="text-center">
            <FileSpreadsheet className="w-12 h-12 text-gray-400 mx-auto mb-4" />
            <div className="space-y-4">
              <Button onClick={() => fileInputRef.current?.click()} className="bg-blue-600 hover:bg-blue-700">
                <Upload className="w-4 h-4 mr-2" />
                Choose File
              </Button>
              <input
                ref={fileInputRef}
                type="file"
                accept=".csv,.xlsx,.xls"
                onChange={handleFileUpload}
                className="hidden"
              />
              <p className="text-sm text-gray-500">
                Supported formats: CSV, Excel (.xlsx, .xls)
              </p>
            </div>
          </div>
        </CardContent>
      </Card>

      <div className="flex items-center space-x-4 p-4 bg-gray-50 rounded-lg">
        <Download className="w-5 h-5 text-blue-600" />
        <div className="flex-1">
          <p className="text-sm font-medium">Need a template?</p>
          <p className="text-xs text-gray-600">Download our template with the correct column headers</p>
        </div>
        <Button variant="outline" size="sm" onClick={downloadTemplate}>
          <Download className="w-4 h-4 mr-2" />
          Download Template
        </Button>
      </div>

      {uploadedFile && (
        <Card className="bg-green-50 border-green-200">
          <CardContent className="p-4">
            <div className="flex items-center space-x-3">
              <CheckCircle className="w-5 h-5 text-green-600" />
              <div>
                <p className="font-medium text-green-800">File uploaded successfully</p>
                <p className="text-sm text-green-600">
                  {uploadedFile.name} • {csvData.length} records found
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )

  const renderStep2 = () => (
    <div className="space-y-6">
      <div className="text-center mb-6">
        <div className="mx-auto w-16 h-16 bg-purple-100 rounded-full flex items-center justify-center mb-4">
          <ArrowRight className="w-8 h-8 text-purple-600" />
        </div>
        <h3 className="text-lg font-semibold mb-2">Map Your Fields</h3>
        <p className="text-gray-600">
          Match your file columns to the CRM fields
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Field Mapping</CardTitle>
          <div className="text-sm text-gray-600">
            Required fields are marked with <Badge variant="destructive" className="text-xs">Required</Badge>
          </div>
        </CardHeader>
        <CardContent>
          <div className="space-y-4 max-h-96 overflow-y-auto">
            {fieldMappings.map((mapping, index) => {
              const field = currentConfig.fields.find(f => f.key === mapping.crmField)
              return (
                <div key={mapping.crmField} className="flex items-center space-x-4 p-3 border rounded-lg">
                  <div className="flex-1">
                    <div className="flex items-center space-x-2">
                      <Label className="font-medium">{field?.label}</Label>
                      {mapping.required && (
                        <Badge variant="destructive" className="text-xs">Required</Badge>
                      )}
                    </div>
                  </div>
                  <ArrowRight className="w-4 h-4 text-gray-400" />
                  <div className="flex-1">
                    <Select
                      value={mapping.csvField}
                      onValueChange={(value) => handleFieldMapping(mapping.crmField, value)}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Select column" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="">-- Skip this field --</SelectItem>
                        {csvHeaders.map(header => (
                          <SelectItem key={header} value={header}>{header}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </div>
              )
            })}
          </div>
        </CardContent>
      </Card>

      {validationErrors.length > 0 && (
        <Card className="border-red-200 bg-red-50">
          <CardContent className="p-4">
            <div className="flex items-start space-x-3">
              <AlertCircle className="w-5 h-5 text-red-600 flex-shrink-0 mt-0.5" />
              <div>
                <p className="font-medium text-red-800 mb-2">Validation Errors</p>
                <ul className="text-sm text-red-600 space-y-1">
                  {validationErrors.map((error, index) => (
                    <li key={index}>• {error}</li>
                  ))}
                </ul>
              </div>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )

  const renderStep3 = () => (
    <div className="space-y-6">
      <div className="text-center mb-6">
        <div className="mx-auto w-16 h-16 bg-orange-100 rounded-full flex items-center justify-center mb-4">
          <CheckCircle className="w-8 h-8 text-orange-600" />
        </div>
        <h3 className="text-lg font-semibold mb-2">Preview Your Data</h3>
        <p className="text-gray-600">
          Review the first few records before importing
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center justify-between">
            Data Preview
            <Badge variant="secondary">{csvData.length} total records</Badge>
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  {fieldMappings
                    .filter(m => m.csvField)
                    .slice(0, 6)
                    .map(mapping => {
                      const field = currentConfig.fields.find(f => f.key === mapping.crmField)
                      return (
                        <TableHead key={mapping.crmField} className="text-xs">
                          {field?.label}
                          {mapping.required && <span className="text-red-500 ml-1">*</span>}
                        </TableHead>
                      )
                    })}
                </TableRow>
              </TableHeader>
              <TableBody>
                {csvData.slice(0, 5).map((row, index) => (
                  <TableRow key={index}>
                    {fieldMappings
                      .filter(m => m.csvField)
                      .slice(0, 6)
                      .map(mapping => (
                        <TableCell key={mapping.crmField} className="text-xs">
                          {row[mapping.csvField] || '-'}
                        </TableCell>
                      ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
          {csvData.length > 5 && (
            <p className="text-xs text-gray-500 mt-2 text-center">
              Showing first 5 rows of {csvData.length} total records
            </p>
          )}
        </CardContent>
      </Card>
    </div>
  )

  const renderStep4 = () => (
    <div className="space-y-6">
      <div className="text-center mb-6">
        <div className="mx-auto w-16 h-16 bg-green-100 rounded-full flex items-center justify-center mb-4">
          {isImporting ? (
            <div className="w-6 h-6 border-2 border-green-600 border-t-transparent rounded-full animate-spin" />
          ) : (
            <CheckCircle className="w-8 h-8 text-green-600" />
          )}
        </div>
        <h3 className="text-lg font-semibold mb-2">
          {isImporting ? 'Importing Data...' : 'Import Complete!'}
        </h3>
        <p className="text-gray-600">
          {isImporting 
            ? 'Please wait while we import your data'
            : 'Your data has been successfully imported'
          }
        </p>
      </div>

      {isImporting && (
        <Card>
          <CardContent className="p-6">
            <div className="space-y-4">
              <div className="flex justify-between text-sm">
                <span>Import Progress</span>
                <span>{importProgress}%</span>
              </div>
              <Progress value={importProgress} />
              <p className="text-xs text-gray-500 text-center">
                Processing {csvData.length} records...
              </p>
            </div>
          </CardContent>
        </Card>
      )}

      {!isImporting && importSummary.total > 0 && (
        <Card className="bg-green-50 border-green-200">
          <CardContent className="p-6">
            <div className="text-center space-y-3">
              <CheckCircle className="w-12 h-12 text-green-600 mx-auto" />
              <div>
                <p className="font-semibold text-green-800 text-lg">
                  {importSummary.successful} records imported successfully!
                </p>
                {importSummary.failed > 0 && (
                  <p className="text-sm text-orange-600">
                    {importSummary.failed} records failed to import
                  </p>
                )}
              </div>
              <div className="grid grid-cols-3 gap-4 pt-4 text-center">
                <div>
                  <p className="text-2xl font-bold text-green-600">{importSummary.successful}</p>
                  <p className="text-xs text-gray-600">Successful</p>
                </div>
                <div>
                  <p className="text-2xl font-bold text-orange-600">{importSummary.failed}</p>
                  <p className="text-xs text-gray-600">Failed</p>
                </div>
                <div>
                  <p className="text-2xl font-bold text-blue-600">{importSummary.total}</p>
                  <p className="text-xs text-gray-600">Total</p>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )

  if (!isOpen) return null

  return (
    <Dialog open={isOpen} onOpenChange={handleClose}>
      <DialogContent className="max-w-4xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center space-x-2">
            {currentConfig.icon}
            <span>{currentConfig.title}</span>
          </DialogTitle>
          <DialogDescription>
            Import your existing {moduleType} data from Excel or CSV files
          </DialogDescription>
        </DialogHeader>

        {/* Progress Steps */}
        <div className="flex items-center justify-between mb-6 px-4">
          {importSteps.map((step, index) => (
            <div key={step.step} className="flex items-center">
              <div className={`
                w-8 h-8 rounded-full flex items-center justify-center text-sm font-medium
                ${currentStep >= step.step 
                  ? 'bg-blue-600 text-white' 
                  : 'bg-gray-200 text-gray-600'
                }
              `}>
                {step.step}
              </div>
              <div className="ml-2 hidden sm:block">
                <p className={`text-sm font-medium ${currentStep >= step.step ? 'text-blue-600' : 'text-gray-600'}`}>
                  {step.title}
                </p>
              </div>
              {index < importSteps.length - 1 && (
                <div className={`w-12 h-px mx-4 ${currentStep > step.step ? 'bg-blue-600' : 'bg-gray-300'}`} />
              )}
            </div>
          ))}
        </div>

        <div className="min-h-[400px]">
          {currentStep === 1 && renderStep1()}
          {currentStep === 2 && renderStep2()}
          {currentStep === 3 && renderStep3()}
          {currentStep === 4 && renderStep4()}
        </div>

        <DialogFooter className="flex justify-between">
          <div>
            {currentStep > 1 && currentStep < 4 && !isImporting && (
              <Button variant="outline" onClick={() => setCurrentStep(currentStep - 1)}>
                <ArrowLeft className="w-4 h-4 mr-2" />
                Previous
              </Button>
            )}
          </div>
          <div className="flex space-x-2">
            {currentStep < 4 && (
              <Button variant="outline" onClick={handleClose}>
                Cancel
              </Button>
            )}
            {currentStep === 1 && uploadedFile && (
              <Button onClick={() => setCurrentStep(2)}>
                Next Step
                <ArrowRight className="w-4 h-4 ml-2" />
              </Button>
            )}
            {currentStep === 2 && (
              <Button onClick={handlePreview}>
                Preview Data
                <ArrowRight className="w-4 h-4 ml-2" />
              </Button>
            )}
            {currentStep === 3 && (
              <Button onClick={handleImport} className="bg-green-600 hover:bg-green-700">
                <Upload className="w-4 h-4 mr-2" />
                Import Data
              </Button>
            )}
            {currentStep === 4 && !isImporting && (
              <Button onClick={handleClose} className="bg-blue-600 hover:bg-blue-700">
                <CheckCircle className="w-4 h-4 mr-2" />
                Done
              </Button>
            )}
          </div>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}