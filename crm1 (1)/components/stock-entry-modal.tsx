"use client"

import { useState, useEffect } from "react"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Badge } from "@/components/ui/badge"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Calendar } from "@/components/ui/calendar"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { CalendarIcon, Plus, Trash2, Package } from "lucide-react"
import { format } from "date-fns"
import { cn } from "@/lib/utils"

interface StockEntryItem {
  product_id: string
  quantity: number
  unit_price: number
  batch_number?: string
  serial_number?: string
  expiry_date?: Date
  bin_location?: string
}

interface StockEntryModalProps {
  open: boolean
  onClose: () => void
  onSave: (entryData: any) => void
  entryType: 'inward' | 'outward'
  products: any[]
  companyId: string
  userId: string
}

export function StockEntryModal({
  open,
  onClose,
  onSave,
  entryType,
  products,
  companyId,
  userId
}: StockEntryModalProps) {
  const [entryDate, setEntryDate] = useState<Date>(new Date())
  const [referenceType, setReferenceType] = useState("")
  const [referenceNumber, setReferenceNumber] = useState("")
  const [partyType, setPartyType] = useState("")
  const [partyName, setPartyName] = useState("")
  const [warehouseLocation, setWarehouseLocation] = useState("")
  const [remarks, setRemarks] = useState("")
  const [status, setStatus] = useState("draft")
  const [items, setItems] = useState<StockEntryItem[]>([])
  const [selectedProduct, setSelectedProduct] = useState("")
  const [quantity, setQuantity] = useState("")
  const [unitPrice, setUnitPrice] = useState("")
  const [batchNumber, setBatchNumber] = useState("")
  const [binLocation, setBinLocation] = useState("")

  const handleProductSelect = (productId: string) => {
    setSelectedProduct(productId)

    // Auto-populate price from product
    const product = products.find(p => p.id === productId)
    console.log('Selected Product:', product)

    if (product) {
      // Get price from product (the field name is 'price' in the database)
      const price = product.price || 0
      console.log('Selected product price:', price)
      setUnitPrice(price.toString())
    }
  }

  const getAvailableStock = (productId: string) => {
    const product = products.find(p => p.id === productId)
    return product?.stock_quantity || 0
  }

  const handleAddItem = () => {
    if (!selectedProduct || !quantity || parseFloat(quantity) <= 0) {
      alert("Please select a product and enter a valid quantity")
      return
    }

    // Validate stock for outward entries
    if (entryType === 'outward') {
      const availableStock = getAvailableStock(selectedProduct)
      const requestedQty = parseFloat(quantity)

      if (requestedQty > availableStock) {
        const product = products.find(p => p.id === selectedProduct)
        alert(`❌ Insufficient Stock!\n\nProduct: ${product?.product_name}\nAvailable Stock: ${availableStock} units\nRequested: ${requestedQty} units\n\nYou cannot dispatch more than available stock.`)
        return
      }
    }

    const newItem: StockEntryItem = {
      product_id: selectedProduct,
      quantity: parseFloat(quantity),
      unit_price: parseFloat(unitPrice) || 0,
      batch_number: batchNumber || undefined,
      bin_location: binLocation || undefined,
    }

    setItems([...items, newItem])

    // Reset item form
    setSelectedProduct("")
    setQuantity("")
    setUnitPrice("")
    setBatchNumber("")
    setBinLocation("")
  }

  const handleRemoveItem = (index: number) => {
    setItems(items.filter((_, i) => i !== index))
  }

  const handleSave = () => {
    if (items.length === 0) {
      alert("⚠️ Please add at least one item!\n\n1. Fill in the product details in the 'Add Items' section\n2. Click the 'Add Item to List' button\n3. Then click 'Save' to create the entry")
      return
    }

    const entryData = {
      companyId,
      userId,
      entry_type: entryType,
      entry_date: format(entryDate, 'yyyy-MM-dd'),
      reference_type: referenceType || null,
      reference_number: referenceNumber || null,
      party_type: partyType || null,
      party_name: partyName || null,
      warehouse_location: warehouseLocation || null,
      remarks: remarks || null,
      status,
      items
    }

    onSave(entryData)
    handleClose()
  }

  const handleClose = () => {
    // Reset form
    setEntryDate(new Date())
    setReferenceType("")
    setReferenceNumber("")
    setPartyType("")
    setPartyName("")
    setWarehouseLocation("")
    setRemarks("")
    setStatus("draft")
    setItems([])
    setSelectedProduct("")
    setQuantity("")
    setUnitPrice("")
    setBatchNumber("")
    setBinLocation("")
    onClose()
  }

  const getProductName = (productId: string) => {
    const product = products.find(p => p.id === productId)
    return product ? product.product_name : 'Unknown Product'
  }

  const totalQuantity = items.reduce((sum, item) => sum + item.quantity, 0)
  const totalValue = items.reduce((sum, item) => sum + (item.quantity * item.unit_price), 0)

  useEffect(() => {
    console.log('StockEntryModal - open state changed:', open, 'entryType:', entryType)
  }, [open, entryType])

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className="max-w-6xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>
            {entryType === 'inward' ? 'Stock Inward Entry' : 'Stock Outward Entry'}
          </DialogTitle>
          <DialogDescription>
            {entryType === 'inward'
              ? 'Add new stock to inventory'
              : 'Remove stock from inventory'}
          </DialogDescription>
        </DialogHeader>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Entry Date */}
          <div className="space-y-2">
            <Label>Entry Date *</Label>
            <Popover>
              <PopoverTrigger asChild>
                <Button
                  variant="outline"
                  className={cn(
                    "w-full justify-start text-left font-normal",
                    !entryDate && "text-muted-foreground"
                  )}
                >
                  <CalendarIcon className="mr-2 h-4 w-4" />
                  {entryDate ? format(entryDate, "PPP") : <span>Pick a date</span>}
                </Button>
              </PopoverTrigger>
              <PopoverContent className="w-auto p-0">
                <Calendar
                  mode="single"
                  selected={entryDate}
                  onSelect={(date) => date && setEntryDate(date)}
                  initialFocus
                />
              </PopoverContent>
            </Popover>
          </div>

          {/* Reference Type */}
          <div className="space-y-2">
            <Label>Reference Type</Label>
            <Select value={referenceType} onValueChange={setReferenceType}>
              <SelectTrigger>
                <SelectValue placeholder="Select reference type" />
              </SelectTrigger>
              <SelectContent>
                {entryType === 'inward' ? (
                  <>
                    <SelectItem value="purchase_order">Purchase Order</SelectItem>
                    <SelectItem value="return">Sales Return</SelectItem>
                    <SelectItem value="transfer">Transfer In</SelectItem>
                    <SelectItem value="adjustment">Stock Adjustment</SelectItem>
                  </>
                ) : (
                  <>
                    <SelectItem value="sales_order">Sales Order</SelectItem>
                    <SelectItem value="return">Purchase Return</SelectItem>
                    <SelectItem value="transfer">Transfer Out</SelectItem>
                    <SelectItem value="adjustment">Stock Adjustment</SelectItem>
                  </>
                )}
              </SelectContent>
            </Select>
          </div>

          {/* Reference Number */}
          <div className="space-y-2">
            <Label>Reference Number</Label>
            <Input
              value={referenceNumber}
              onChange={(e) => setReferenceNumber(e.target.value)}
              placeholder="Enter reference number"
            />
          </div>

          {/* Party Type */}
          <div className="space-y-2">
            <Label>Party Type</Label>
            <Select value={partyType} onValueChange={setPartyType}>
              <SelectTrigger>
                <SelectValue placeholder="Select party type" />
              </SelectTrigger>
              <SelectContent>
                {entryType === 'inward' ? (
                  <>
                    <SelectItem value="supplier">Supplier</SelectItem>
                    <SelectItem value="customer">Customer (Return)</SelectItem>
                    <SelectItem value="warehouse">Warehouse</SelectItem>
                  </>
                ) : (
                  <>
                    <SelectItem value="customer">Customer</SelectItem>
                    <SelectItem value="supplier">Supplier (Return)</SelectItem>
                    <SelectItem value="warehouse">Warehouse</SelectItem>
                  </>
                )}
              </SelectContent>
            </Select>
          </div>

          {/* Party Name */}
          <div className="space-y-2">
            <Label>Party Name</Label>
            <Input
              value={partyName}
              onChange={(e) => setPartyName(e.target.value)}
              placeholder="Enter party name"
            />
          </div>

          {/* Warehouse Location */}
          <div className="space-y-2">
            <Label>Warehouse Location</Label>
            <Input
              value={warehouseLocation}
              onChange={(e) => setWarehouseLocation(e.target.value)}
              placeholder="Enter warehouse location"
            />
          </div>

          {/* Status */}
          <div className="space-y-2">
            <Label>Status</Label>
            <Select value={status} onValueChange={setStatus}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="draft">Draft</SelectItem>
                <SelectItem value="submitted">Submitted</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>

        {/* Remarks */}
        <div className="space-y-2">
          <Label>Remarks</Label>
          <Textarea
            value={remarks}
            onChange={(e) => setRemarks(e.target.value)}
            placeholder="Enter any additional remarks"
            rows={3}
          />
        </div>

        {/* Add Items Section */}
        <div className="border-2 border-blue-200 rounded-lg p-4 space-y-4 bg-blue-50">
          <div className="flex items-center justify-between">
            <h3 className="text-lg font-semibold text-blue-900">Add Items to Entry</h3>
            <Badge variant="outline" className="bg-white">
              {items.length} item(s) added
            </Badge>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-6 gap-4">
            <div className="md:col-span-2 space-y-2">
              <Label>Product *</Label>
              <Select value={selectedProduct} onValueChange={handleProductSelect}>
                <SelectTrigger>
                  <SelectValue placeholder="Select product" />
                </SelectTrigger>
                <SelectContent>
                  {products.map((product) => (
                    <SelectItem key={product.id} value={product.id}>
                      {product.product_name} ({product.product_reference_no})
                      {entryType === 'outward' && ` - Stock: ${product.stock_quantity || 0}`}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {selectedProduct && entryType === 'outward' && (
                <p className="text-xs font-medium text-orange-600">
                  ⚠️ Available Stock: {getAvailableStock(selectedProduct)} units
                </p>
              )}
            </div>

            <div className="space-y-2">
              <Label>Quantity *</Label>
              <Input
                type="number"
                value={quantity}
                onChange={(e) => setQuantity(e.target.value)}
                placeholder="0"
                min="0"
                step="1"
              />
            </div>

            <div className="space-y-2">
              <Label>Unit Price (Auto-filled)</Label>
              <Input
                type="number"
                value={unitPrice}
                onChange={(e) => setUnitPrice(e.target.value)}
                placeholder="Auto-filled from product"
                min="0"
                step="0.01"
                className="bg-blue-50"
              />
              <p className="text-xs text-gray-500 italic">Auto-populated from product price</p>
            </div>

            <div className="space-y-2">
              <Label>Batch/Lot No.</Label>
              <Input
                value={batchNumber}
                onChange={(e) => setBatchNumber(e.target.value)}
                placeholder="Batch number"
              />
            </div>

            <div className="space-y-2">
              <Label>Bin Location</Label>
              <Input
                value={binLocation}
                onChange={(e) => setBinLocation(e.target.value)}
                placeholder="A-1-5"
              />
            </div>
          </div>

          <Button
            onClick={handleAddItem}
            className="bg-blue-600 hover:bg-blue-700 text-white"
            size="lg"
          >
            <Plus className="w-5 h-5 mr-2" />
            Add Item to List
          </Button>
          <p className="text-sm text-gray-600 italic">
            ⚠️ Remember to click "Add Item to List" button after filling the fields above
          </p>
        </div>

        {/* Items Table */}
        {items.length > 0 ? (
          <div className="border rounded-lg bg-white">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Product</TableHead>
                  <TableHead className="text-right">Quantity</TableHead>
                  <TableHead className="text-right">Unit Price</TableHead>
                  <TableHead className="text-right">Total</TableHead>
                  <TableHead>Batch No.</TableHead>
                  <TableHead>Bin Location</TableHead>
                  <TableHead className="w-20"></TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {items.map((item, index) => (
                  <TableRow key={index}>
                    <TableCell>{getProductName(item.product_id)}</TableCell>
                    <TableCell className="text-right">{item.quantity}</TableCell>
                    <TableCell className="text-right">₹{item.unit_price.toFixed(2)}</TableCell>
                    <TableCell className="text-right">₹{(item.quantity * item.unit_price).toFixed(2)}</TableCell>
                    <TableCell>{item.batch_number || '-'}</TableCell>
                    <TableCell>{item.bin_location || '-'}</TableCell>
                    <TableCell>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => handleRemoveItem(index)}
                      >
                        <Trash2 className="w-4 h-4 text-red-600" />
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
                <TableRow className="font-semibold bg-gray-50">
                  <TableCell>Total</TableCell>
                  <TableCell className="text-right">{totalQuantity}</TableCell>
                  <TableCell></TableCell>
                  <TableCell className="text-right">₹{totalValue.toFixed(2)}</TableCell>
                  <TableCell colSpan={3}></TableCell>
                </TableRow>
              </TableBody>
            </Table>
          </div>
        ) : (
          <div className="border-2 border-dashed border-gray-300 rounded-lg p-8 text-center bg-gray-50">
            <Package className="w-12 h-12 text-gray-400 mx-auto mb-3" />
            <p className="text-gray-600 font-medium">No items added yet</p>
            <p className="text-sm text-gray-500 mt-1">Fill in the product details above and click "Add Item to List"</p>
          </div>
        )}

        <DialogFooter>
          <Button variant="outline" onClick={handleClose}>
            Cancel
          </Button>
          <Button onClick={handleSave}>
            Save {entryType === 'inward' ? 'Inward' : 'Outward'} Entry
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
