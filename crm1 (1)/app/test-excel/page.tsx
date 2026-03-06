"use client"

import { SimpleExcelTest } from "@/components/simple-excel-test"

export default function TestExcelPage() {
  return (
    <div className="container mx-auto py-8">
      <h1 className="text-2xl font-bold mb-6">Excel Upload Test</h1>
      <SimpleExcelTest />
    </div>
  )
}