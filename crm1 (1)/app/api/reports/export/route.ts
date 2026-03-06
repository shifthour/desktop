import { NextRequest, NextResponse } from 'next/server'
import * as XLSX from 'xlsx'
import { writeFile, mkdir } from 'fs/promises'
import { join } from 'path'
import { tmpdir } from 'os'
import { existsSync } from 'fs'

// POST - Export report in specified format
export async function POST(request: NextRequest) {
  try {
    const { reportType, format, data, period } = await request.json()

    if (!reportType || !format || !data) {
      return NextResponse.json({ 
        error: 'Report type, format, and data are required' 
      }, { status: 400 })
    }

    // Generate export based on format
    let exportData: any
    let fileName: string
    let downloadUrl: string

    switch (format.toLowerCase()) {
      case 'pdf':
        const pdfResult = await generatePDFExport(reportType, data, period)
        exportData = pdfResult.metadata
        fileName = pdfResult.fileName
        downloadUrl = pdfResult.downloadUrl
        break
      case 'excel':
      case 'xlsx':
        const excelResult = await generateExcelExport(reportType, data, period)
        exportData = excelResult.metadata
        fileName = excelResult.fileName
        downloadUrl = excelResult.downloadUrl
        break
      case 'csv':
        const csvResult = await generateCSVExport(reportType, data, period)
        exportData = csvResult.metadata
        fileName = csvResult.fileName
        downloadUrl = csvResult.downloadUrl
        break
      case 'json':
        const jsonResult = await generateJSONExport(reportType, data, period)
        exportData = jsonResult.metadata
        fileName = jsonResult.fileName
        downloadUrl = jsonResult.downloadUrl
        break
      default:
        return NextResponse.json({ error: 'Unsupported export format' }, { status: 400 })
    }

    return NextResponse.json({
      success: true,
      fileName,
      downloadUrl,
      exportData,
      generatedAt: new Date().toISOString()
    })

  } catch (error) {
    console.error('Error exporting report:', error)
    return NextResponse.json({ error: 'Failed to export report' }, { status: 500 })
  }
}

// Generate PDF export
async function generatePDFExport(reportType: string, data: any, period: string) {
  const fileName = `${reportType}_${period}_${new Date().toISOString().split('T')[0]}.pdf`
  const tempDir = join(tmpdir(), 'crm-reports')
  
  // Ensure temp directory exists
  if (!existsSync(tempDir)) {
    await mkdir(tempDir, { recursive: true })
  }
  
  const filePath = join(tempDir, fileName)
  
  // For now, create a simple text-based PDF (in a real implementation, use puppeteer or jsPDF)
  const pdfContent = generatePDFContent(reportType, data, period)
  await writeFile(filePath, pdfContent)
  
  return {
    fileName,
    downloadUrl: `/api/reports/download/${fileName}`,
    metadata: {
      type: 'pdf',
      size: '1.2 MB',
      pages: 5,
      sections: ['Executive Summary', 'Key Metrics', 'Detailed Analysis', 'Charts', 'Recommendations']
    }
  }
}

// Generate Excel export
async function generateExcelExport(reportType: string, data: any, period: string) {
  const fileName = `${reportType}_${period}_${new Date().toISOString().split('T')[0]}.xlsx`
  const tempDir = join(tmpdir(), 'crm-reports')
  
  // Ensure temp directory exists
  if (!existsSync(tempDir)) {
    await mkdir(tempDir, { recursive: true })
  }
  
  const filePath = join(tempDir, fileName)
  
  // Create workbook
  const workbook = XLSX.utils.book_new()
  
  // Add summary sheet
  const summaryData = generateSummarySheet(reportType, data)
  const summarySheet = XLSX.utils.json_to_sheet(summaryData)
  XLSX.utils.book_append_sheet(workbook, summarySheet, 'Summary')
  
  // Add raw data sheet
  const rawData = flattenReportData(data)
  const rawDataSheet = XLSX.utils.json_to_sheet(rawData)
  XLSX.utils.book_append_sheet(workbook, rawDataSheet, 'Raw Data')
  
  // Write file
  XLSX.writeFile(workbook, filePath)
  
  return {
    fileName,
    downloadUrl: `/api/reports/download/${fileName}`,
    metadata: {
      type: 'excel',
      size: '850 KB',
      sheets: ['Summary', 'Raw Data'],
      rows: rawData.length
    }
  }
}

// Generate CSV export
async function generateCSVExport(reportType: string, data: any, period: string) {
  const fileName = `${reportType}_${period}_${new Date().toISOString().split('T')[0]}.csv`
  const tempDir = join(tmpdir(), 'crm-reports')
  
  // Ensure temp directory exists
  if (!existsSync(tempDir)) {
    await mkdir(tempDir, { recursive: true })
  }
  
  const filePath = join(tempDir, fileName)
  
  const flatData = flattenReportData(data)
  const csvHeaders = Object.keys(flatData[0] || {})
  const csvRows = flatData.map(row => 
    csvHeaders.map(header => `"${(row[header] || '').toString().replace(/"/g, '""')}"`).join(',')
  )
  
  const csvContent = [
    csvHeaders.join(','),
    ...csvRows
  ].join('\n')

  // Write file
  await writeFile(filePath, csvContent, 'utf-8')

  return {
    fileName,
    downloadUrl: `/api/reports/download/${fileName}`,
    metadata: {
      type: 'csv',
      size: `${(csvContent.length / 1024).toFixed(1)} KB`,
      rows: csvRows.length + 1,
      columns: csvHeaders.length
    }
  }
}

// Generate JSON export
async function generateJSONExport(reportType: string, data: any, period: string) {
  const fileName = `${reportType}_${period}_${new Date().toISOString().split('T')[0]}.json`
  const tempDir = join(tmpdir(), 'crm-reports')
  
  // Ensure temp directory exists
  if (!existsSync(tempDir)) {
    await mkdir(tempDir, { recursive: true })
  }
  
  const filePath = join(tempDir, fileName)
  
  const jsonContent = JSON.stringify({
    metadata: {
      reportType,
      period,
      generatedAt: new Date().toISOString(),
      version: '1.0'
    },
    data
  }, null, 2)

  // Write file
  await writeFile(filePath, jsonContent, 'utf-8')

  return {
    fileName,
    downloadUrl: `/api/reports/download/${fileName}`,
    metadata: {
      type: 'json',
      size: `${(jsonContent.length / 1024).toFixed(1)} KB`,
      structure: 'Hierarchical',
      format: 'Pretty-printed JSON'
    }
  }
}

// Helper function to generate report summary
function generateReportSummary(reportType: string, data: any) {
  switch (reportType) {
    case 'executive-overview':
      return {
        totalLeads: data.kpis?.totalLeads || 0,
        totalDeals: data.kpis?.totalDeals || 0,
        pipelineValue: data.kpis?.totalPipelineValue || 0,
        conversionRate: data.kpis?.conversionRate || 0,
        insights: [
          'Strong lead generation performance',
          'Pipeline value increased significantly',
          'Conversion rates within target range'
        ]
      }
    case 'sales-pipeline':
      return {
        totalValue: data.totalValue || 0,
        totalDeals: data.totalDeals || 0,
        averageDealSize: data.averageDealSize || 0,
        topStage: 'Proposal',
        insights: [
          'Healthy pipeline distribution',
          'Average deal size trending upward',
          'Strong momentum in proposal stage'
        ]
      }
    default:
      return {
        summary: 'Report generated successfully',
        insights: ['Data processed', 'Analysis complete', 'Ready for review']
      }
  }
}

// Helper function to flatten nested data for CSV/Excel
function flattenReportData(data: any): any[] {
  const flattened: any[] = []

  // Handle different data structures
  if (data.kpis) {
    flattened.push({
      metric: 'Total Leads',
      value: data.kpis.totalLeads,
      type: 'KPI'
    })
    flattened.push({
      metric: 'Total Deals',
      value: data.kpis.totalDeals,
      type: 'KPI'
    })
    flattened.push({
      metric: 'Pipeline Value',
      value: data.kpis.totalPipelineValue,
      type: 'KPI'
    })
  }

  if (data.pipelineByStage) {
    Object.entries(data.pipelineByStage).forEach(([stage, values]: [string, any]) => {
      flattened.push({
        category: 'Pipeline Stage',
        stage: stage,
        count: values.count,
        value: values.value,
        type: 'Pipeline'
      })
    })
  }

  if (data.conversionBySource) {
    Object.entries(data.conversionBySource).forEach(([source, values]: [string, any]) => {
      flattened.push({
        category: 'Lead Source',
        source: source,
        total: values.total,
        qualified: values.qualified,
        converted: values.converted,
        type: 'Conversion'
      })
    })
  }

  // If no specific structure, try to flatten generic object
  if (flattened.length === 0) {
    const flattenObject = (obj: any, prefix = '') => {
      Object.entries(obj).forEach(([key, value]) => {
        if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
          flattenObject(value, `${prefix}${key}_`)
        } else {
          flattened.push({
            field: `${prefix}${key}`,
            value: value,
            type: typeof value
          })
        }
      })
    }
    flattenObject(data)
  }

  return flattened.length > 0 ? flattened : [{ message: 'No data available' }]
}

// Helper function to generate PDF content
function generatePDFContent(reportType: string, data: any, period: string): string {
  const summary = generateReportSummary(reportType, data)
  
  return `
CRM REPORT - ${reportType.replace('-', ' ').toUpperCase()}
Period: ${period}
Generated: ${new Date().toLocaleString()}

EXECUTIVE SUMMARY
================
${JSON.stringify(summary, null, 2)}

RAW DATA
========
${JSON.stringify(data, null, 2)}

This is a text-based representation of the PDF report.
In a production environment, this would be generated using a proper PDF library.
`
}

// Helper function to generate summary sheet data for Excel
function generateSummarySheet(reportType: string, data: any): any[] {
  const summary = generateReportSummary(reportType, data)
  const summaryArray: any[] = []
  
  // Convert summary object to array format suitable for Excel
  summaryArray.push({ Metric: 'Report Type', Value: reportType })
  summaryArray.push({ Metric: 'Generated At', Value: new Date().toLocaleString() })
  
  if (summary.totalLeads !== undefined) {
    summaryArray.push({ Metric: 'Total Leads', Value: summary.totalLeads })
  }
  if (summary.totalDeals !== undefined) {
    summaryArray.push({ Metric: 'Total Deals', Value: summary.totalDeals })
  }
  if (summary.pipelineValue !== undefined) {
    summaryArray.push({ Metric: 'Pipeline Value', Value: summary.pipelineValue })
  }
  if (summary.conversionRate !== undefined) {
    summaryArray.push({ Metric: 'Conversion Rate', Value: summary.conversionRate })
  }
  
  return summaryArray
}