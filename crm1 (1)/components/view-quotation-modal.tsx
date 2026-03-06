"use client"

import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Separator } from "@/components/ui/separator"
import { FileText, Printer, Download, Send, X, Calendar, User, Building, Phone, Mail, MapPin, MessageCircle } from "lucide-react"

interface ViewQuotationModalProps {
  isOpen: boolean
  onClose: () => void
  quotation: any
  onPrint?: () => void
  onSend?: () => void
  onDownload?: () => void
  onSendEmail?: () => void
  onSendWhatsApp?: () => void
}

export function ViewQuotationModal({ 
  isOpen, 
  onClose, 
  quotation, 
  onPrint,
  onSend,
  onDownload,
  onSendEmail,
  onSendWhatsApp
}: ViewQuotationModalProps) {
  
  // Custom WhatsApp Icon Component
  const WhatsAppIcon = ({ className }: { className?: string }) => (
    <svg className={className} viewBox="0 0 24 24" fill="currentColor">
      <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.669.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.569-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.890-5.335 11.893-11.893A11.821 11.821 0 0020.465 3.516"/>
    </svg>
  )
  
  if (!quotation) return null

  const getStatusColor = (status: string) => {
    switch (status?.toLowerCase()) {
      case "draft":
        return "bg-gray-100 text-gray-800"
      case "sent":
        return "bg-blue-100 text-blue-800"
      case "under review":
        return "bg-yellow-100 text-yellow-800"
      case "accepted":
        return "bg-green-100 text-green-800"
      case "rejected":
        return "bg-red-100 text-red-800"
      case "expired":
        return "bg-orange-100 text-orange-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-5xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <div className="flex items-center justify-between">
            <DialogTitle className="flex items-center space-x-2 text-xl">
              <FileText className="w-6 h-6 text-blue-600" />
              <span>Quotation Details</span>
            </DialogTitle>
            <div className="flex gap-2">
              {onSendEmail && (
                <Button 
                  variant="outline" 
                  size="sm" 
                  onClick={onSendEmail}
                  className="text-orange-600 hover:text-orange-800 border-orange-200 hover:border-orange-300"
                >
                  <Mail className="w-4 h-4 mr-2" />
                  Email
                </Button>
              )}
              {onSendWhatsApp && (
                <Button 
                  variant="outline" 
                  size="sm" 
                  onClick={onSendWhatsApp}
                  className="text-green-600 hover:text-green-800 border-green-200 hover:border-green-300"
                >
                  <WhatsAppIcon className="w-4 h-4 mr-2" />
                  WhatsApp
                </Button>
              )}
              {onPrint && (
                <Button variant="outline" size="sm" onClick={onPrint}>
                  <Printer className="w-4 h-4 mr-2" />
                  Print
                </Button>
              )}
              {onSend && (
                <Button variant="outline" size="sm" onClick={onSend}>
                  <Send className="w-4 h-4 mr-2" />
                  Send
                </Button>
              )}
              {onDownload && (
                <Button variant="outline" size="sm" onClick={onDownload}>
                  <Download className="w-4 h-4 mr-2" />
                  Download
                </Button>
              )}
            </div>
          </div>
        </DialogHeader>

        <div className="space-y-6 mt-4">
          {/* Quotation Header Info */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div>
              <p className="text-sm text-gray-500">Quotation ID</p>
              <p className="font-semibold">{quotation.quote_number || quotation.quotationId || quotation.id}</p>
            </div>
            <div>
              <p className="text-sm text-gray-500">Date</p>
              <p className="font-semibold">{quotation.quote_date || quotation.date}</p>
            </div>
            <div>
              <p className="text-sm text-gray-500">Valid Until</p>
              <p className="font-semibold">{quotation.valid_until || quotation.validUntil}</p>
            </div>
            <div>
              <p className="text-sm text-gray-500">Status</p>
              <Badge className={getStatusColor(quotation.status)}>{quotation.status}</Badge>
            </div>
          </div>

          <Separator />

          {/* Customer Information */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg flex items-center gap-2">
                <Building className="w-5 h-5" />
                Customer Information
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <div className="flex items-center gap-2 mb-2">
                    <Building className="w-4 h-4 text-gray-500" />
                    <span className="text-sm text-gray-500">Account</span>
                  </div>
                  <p className="font-medium">{quotation.customer_name || quotation.accountName}</p>
                </div>
                <div>
                  <div className="flex items-center gap-2 mb-2">
                    <User className="w-4 h-4 text-gray-500" />
                    <span className="text-sm text-gray-500">Contact Person</span>
                  </div>
                  <p className="font-medium">{quotation.contact_person || quotation.contactPerson || quotation.contactName}</p>
                </div>
                {(quotation.customer_email || quotation.customerEmail) && (
                  <div>
                    <div className="flex items-center gap-2 mb-2">
                      <Mail className="w-4 h-4 text-gray-500" />
                      <span className="text-sm text-gray-500">Email</span>
                    </div>
                    <p className="font-medium">{quotation.customer_email || quotation.customerEmail}</p>
                  </div>
                )}
                {(quotation.customer_phone || quotation.customerPhone) && (
                  <div>
                    <div className="flex items-center gap-2 mb-2">
                      <Phone className="w-4 h-4 text-gray-500" />
                      <span className="text-sm text-gray-500">Phone</span>
                    </div>
                    <p className="font-medium">{quotation.customer_phone || quotation.customerPhone}</p>
                  </div>
                )}
                {(quotation.billing_address || quotation.billingAddress) && (
                  <div className="md:col-span-2">
                    <div className="flex items-center gap-2 mb-2">
                      <MapPin className="w-4 h-4 text-gray-500" />
                      <span className="text-sm text-gray-500">Billing Address</span>
                    </div>
                    <p className="font-medium">{quotation.billing_address || quotation.billingAddress}</p>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>

          {/* Quotation Items */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Items / Products</CardTitle>
            </CardHeader>
            <CardContent>
              {(quotation.line_items || quotation.items) && (quotation.line_items || quotation.items).length > 0 ? (
                <>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Product/Service</TableHead>
                        <TableHead>Description</TableHead>
                        <TableHead className="text-center">Qty</TableHead>
                        <TableHead className="text-right">Unit Price</TableHead>
                        <TableHead className="text-center">Discount</TableHead>
                        <TableHead className="text-center">Tax</TableHead>
                        <TableHead className="text-right">Amount</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {(quotation.line_items || quotation.items).map((item: any, index: number) => (
                        <TableRow key={item.id || index}>
                          <TableCell className="font-medium">{item.product}</TableCell>
                          <TableCell>{item.description}</TableCell>
                          <TableCell className="text-center">{item.quantity}</TableCell>
                          <TableCell className="text-right">₹{item.unitPrice?.toLocaleString()}</TableCell>
                          <TableCell className="text-center">{item.discount}%</TableCell>
                          <TableCell className="text-center">{item.taxRate}%</TableCell>
                          <TableCell className="text-right font-medium">
                            ₹{item.amount?.toLocaleString()}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                  
                  {/* Totals */}
                  {quotation.totals && (
                    <div className="mt-6 flex justify-end">
                      <div className="w-80 space-y-2">
                        <div className="flex justify-between">
                          <span>Subtotal:</span>
                          <span>₹{quotation.totals.subtotal?.toLocaleString()}</span>
                        </div>
                        {quotation.totals.totalDiscount > 0 && (
                          <div className="flex justify-between">
                            <span>Discount:</span>
                            <span className="text-red-600">- ₹{quotation.totals.totalDiscount?.toLocaleString()}</span>
                          </div>
                        )}
                        {quotation.totals.totalTax > 0 && (
                          <div className="flex justify-between">
                            <span>Tax (GST):</span>
                            <span>₹{quotation.totals.totalTax?.toLocaleString()}</span>
                          </div>
                        )}
                        <Separator />
                        <div className="flex justify-between text-lg font-bold">
                          <span>Grand Total:</span>
                          <span className="text-blue-600">₹{quotation.totals.grandTotal?.toLocaleString()}</span>
                        </div>
                      </div>
                    </div>
                  )}
                </>
              ) : (
                <div className="py-4">
                  <p className="font-medium">{quotation.products_quoted || quotation.product}</p>
                  <p className="text-2xl font-bold text-blue-600 mt-2">₹{(quotation.total_amount || quotation.amount)?.toLocaleString()}</p>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Terms and Notes */}
          {(quotation.terms_conditions || quotation.terms || quotation.notes) && (
            <Card>
              <CardHeader>
                <CardTitle className="text-lg">Additional Information</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                {(quotation.terms_conditions || quotation.terms) && (
                  <div>
                    <p className="text-sm text-gray-500 mb-2">Terms & Conditions</p>
                    <pre className="whitespace-pre-wrap font-sans text-sm">{quotation.terms_conditions || quotation.terms}</pre>
                  </div>
                )}
                {quotation.notes && (
                  <div>
                    <p className="text-sm text-gray-500 mb-2">Notes</p>
                    <p className="text-sm">{quotation.notes}</p>
                  </div>
                )}
              </CardContent>
            </Card>
          )}

          {/* Additional Details */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Other Details</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                {quotation.assignedTo && (
                  <div>
                    <p className="text-sm text-gray-500">Assigned To</p>
                    <p className="font-medium">{quotation.assignedTo}</p>
                  </div>
                )}
                {quotation.revision && (
                  <div>
                    <p className="text-sm text-gray-500">Revision</p>
                    <Badge variant="outline">{quotation.revision}</Badge>
                  </div>
                )}
                {quotation.reference && (
                  <div>
                    <p className="text-sm text-gray-500">Reference</p>
                    <p className="font-medium">{quotation.reference}</p>
                  </div>
                )}
                {quotation.paymentTerms && (
                  <div>
                    <p className="text-sm text-gray-500">Payment Terms</p>
                    <p className="font-medium">{quotation.paymentTerms}</p>
                  </div>
                )}
                {quotation.deliveryTerms && (
                  <div>
                    <p className="text-sm text-gray-500">Delivery Terms</p>
                    <p className="font-medium">{quotation.deliveryTerms}</p>
                  </div>
                )}
                {quotation.priority && (
                  <div>
                    <p className="text-sm text-gray-500">Priority</p>
                    <Badge variant={quotation.priority === 'High' ? 'destructive' : quotation.priority === 'Medium' ? 'default' : 'secondary'}>
                      {quotation.priority}
                    </Badge>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
        </div>
      </DialogContent>
    </Dialog>
  )
}