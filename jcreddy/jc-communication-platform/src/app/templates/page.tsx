'use client'

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { DashboardLayout } from '@/components/dashboard/dashboard-layout'
import { Header } from '@/components/ui/header'
import { StatusBadge } from '@/components/ui/status-badge'
import {
  MessageSquare,
  Phone,
  Mail,
  Plus,
  Edit,
  Trash2,
  Eye,
  Copy,
  CheckCircle,
  X,
  Loader2,
  Save,
} from 'lucide-react'

async function fetchTemplates() {
  const res = await fetch('/api/templates')
  if (!res.ok) throw new Error('Failed to fetch templates')
  return res.json()
}

async function createTemplate(data: any) {
  const res = await fetch('/api/templates', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  if (!res.ok) {
    const error = await res.json()
    throw new Error(error.error || 'Failed to create template')
  }
  return res.json()
}

async function updateTemplate(data: any) {
  const res = await fetch('/api/templates', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  if (!res.ok) {
    const error = await res.json()
    throw new Error(error.error || 'Failed to update template')
  }
  return res.json()
}

const channelIcons: Record<string, any> = {
  whatsapp: MessageSquare,
  sms: Phone,
  email: Mail,
}

const channelColors: Record<string, string> = {
  whatsapp: 'bg-green-100 text-green-600',
  sms: 'bg-blue-100 text-blue-600',
  email: 'bg-purple-100 text-purple-600',
}

const typeLabels: Record<string, { label: string; color: string }> = {
  booking_confirmation: { label: 'Booking Confirmation', color: 'bg-blue-100 text-blue-700' },
  reminder_24h: { label: '24h Reminder', color: 'bg-yellow-100 text-yellow-700' },
  reminder_2h: { label: '2h Reminder', color: 'bg-orange-100 text-orange-700' },
  cancellation_ack: { label: 'Cancellation', color: 'bg-red-100 text-red-700' },
  delay_alert: { label: 'Delay Alert', color: 'bg-purple-100 text-purple-700' },
  wakeup_call: { label: 'Wake-up Call', color: 'bg-indigo-100 text-indigo-700' },
}

export default function TemplatesPage() {
  const [selectedTemplate, setSelectedTemplate] = useState<any>(null)
  const [copied, setCopied] = useState(false)
  const [showCreateModal, setShowCreateModal] = useState(false)
  const [showEditModal, setShowEditModal] = useState(false)
  const [editingTemplate, setEditingTemplate] = useState<any>(null)
  const [newTemplate, setNewTemplate] = useState({
    template_code: '',
    template_name: '',
    notification_type: 'booking_confirmation',
    channel: 'whatsapp',
    language: 'en',
    body: '',
    whatsapp_template_name: '', // AISensy campaign name
  })

  const queryClient = useQueryClient()

  const { data, isLoading } = useQuery({
    queryKey: ['templates'],
    queryFn: fetchTemplates,
  })

  const createMutation = useMutation({
    mutationFn: createTemplate,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['templates'] })
      setShowCreateModal(false)
      setNewTemplate({
        template_code: '',
        template_name: '',
        notification_type: 'booking_confirmation',
        channel: 'whatsapp',
        language: 'en',
        body: '',
        whatsapp_template_name: '',
      })
    },
  })

  const updateMutation = useMutation({
    mutationFn: updateTemplate,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['templates'] })
      setShowEditModal(false)
      setEditingTemplate(null)
    },
  })

  const handleEditClick = (template: any) => {
    setEditingTemplate({ ...template })
    setShowEditModal(true)
  }

  const templates = data?.templates || []

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  // Group templates by type
  const groupedTemplates = templates.reduce((acc: any, template: any) => {
    const type = template.notification_type
    if (!acc[type]) acc[type] = []
    acc[type].push(template)
    return acc
  }, {})

  return (
    <DashboardLayout>
      <Header title="Message Templates" subtitle="Manage notification templates" />

      <div className="p-4 sm:p-6">
        {/* Header Actions */}
        <div className="mb-4 sm:mb-6 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          <div className="flex items-center gap-2 sm:gap-4 overflow-x-auto pb-2 sm:pb-0">
            <div className="flex items-center gap-2 rounded-lg sm:rounded-xl bg-green-100 px-3 sm:px-4 py-2 flex-shrink-0">
              <MessageSquare className="h-4 w-4 text-green-600" />
              <span className="text-xs sm:text-sm font-medium text-green-700">WhatsApp</span>
              <span className="rounded-full bg-green-600 px-2 py-0.5 text-xs text-white">
                {templates.filter((t: any) => t.channel === 'whatsapp').length}
              </span>
            </div>
            <div className="flex items-center gap-2 rounded-lg sm:rounded-xl bg-blue-100 px-3 sm:px-4 py-2 flex-shrink-0">
              <Phone className="h-4 w-4 text-blue-600" />
              <span className="text-xs sm:text-sm font-medium text-blue-700">SMS</span>
              <span className="rounded-full bg-blue-600 px-2 py-0.5 text-xs text-white">
                {templates.filter((t: any) => t.channel === 'sms').length}
              </span>
            </div>
          </div>
          <button
            onClick={() => setShowCreateModal(true)}
            className="flex items-center justify-center gap-2 rounded-xl bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 sm:w-auto w-full">
            <Plus className="h-4 w-4" />
            Add Template
          </button>
        </div>

        {isLoading ? (
          <div className="flex items-center justify-center py-12">
            <div className="h-8 w-8 animate-spin rounded-full border-4 border-blue-600 border-t-transparent" />
          </div>
        ) : templates.length === 0 ? (
          <div className="flex flex-col items-center justify-center rounded-xl sm:rounded-2xl border bg-white py-12 px-4">
            <MessageSquare className="mb-4 h-12 w-12 text-gray-300" />
            <p className="text-lg font-medium text-gray-900">No templates found</p>
            <p className="text-gray-500 text-center">Create your first message template to get started</p>
            <button className="mt-4 flex items-center gap-2 rounded-xl bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700">
              <Plus className="h-4 w-4" />
              Create Template
            </button>
          </div>
        ) : (
          <div className="space-y-6 sm:space-y-8">
            {Object.entries(groupedTemplates).map(([type, typeTemplates]: [string, any]) => (
              <div key={type}>
                <div className="mb-3 sm:mb-4 flex flex-col sm:flex-row sm:items-center gap-2 sm:gap-3">
                  <span className={`inline-flex self-start rounded-lg px-3 py-1 text-xs sm:text-sm font-medium ${typeLabels[type]?.color || 'bg-gray-100 text-gray-700'}`}>
                    {typeLabels[type]?.label || type}
                  </span>
                  <span className="text-xs sm:text-sm text-gray-500">{typeTemplates.length} templates</span>
                </div>
                <div className="grid gap-3 sm:gap-4 grid-cols-1 md:grid-cols-2 lg:grid-cols-3">
                  {typeTemplates.map((template: any) => {
                    const Icon = channelIcons[template.channel] || MessageSquare
                    return (
                      <div
                        key={template.id}
                        className="group relative rounded-xl sm:rounded-2xl border bg-white p-4 sm:p-6 transition-all hover:border-blue-200 hover:shadow-lg"
                      >
                        {/* Header */}
                        <div className="mb-3 sm:mb-4 flex items-start justify-between">
                          <div className="flex items-center gap-2 sm:gap-3">
                            <div className={`flex h-9 w-9 sm:h-10 sm:w-10 items-center justify-center rounded-lg sm:rounded-xl ${channelColors[template.channel]}`}>
                              <Icon className="h-4 w-4 sm:h-5 sm:w-5" />
                            </div>
                            <div className="min-w-0">
                              <p className="font-semibold text-gray-900 text-sm sm:text-base truncate">{template.template_name}</p>
                              <p className="text-xs text-gray-500 truncate">{template.template_code}</p>
                            </div>
                          </div>
                          <StatusBadge status={template.is_active ? 'active' : 'inactive'} size="sm" />
                        </div>

                        {/* Language & Channel */}
                        <div className="mb-3 sm:mb-4 flex items-center gap-2 flex-wrap">
                          <span className="rounded-lg bg-gray-100 px-2 py-1 text-xs font-medium text-gray-600 uppercase">
                            {template.language}
                          </span>
                          <span className="rounded-lg bg-gray-100 px-2 py-1 text-xs font-medium text-gray-600 capitalize">
                            {template.channel}
                          </span>
                        </div>

                        {/* Preview */}
                        <div className="mb-3 sm:mb-4 rounded-lg bg-gray-50 p-2 sm:p-3">
                          <p className="line-clamp-3 text-xs sm:text-sm text-gray-600">
                            {template.body?.slice(0, 150)}...
                          </p>
                        </div>

                        {/* Variables */}
                        {template.variables?.length > 0 && (
                          <div className="mb-3 sm:mb-4">
                            <p className="mb-1 sm:mb-2 text-xs font-medium text-gray-500">VARIABLES</p>
                            <div className="flex flex-wrap gap-1">
                              {template.variables.slice(0, 4).map((v: string) => (
                                <span key={v} className="rounded bg-blue-50 px-2 py-0.5 text-xs text-blue-600">
                                  {`{{${v}}}`}
                                </span>
                              ))}
                              {template.variables.length > 4 && (
                                <span className="rounded bg-gray-100 px-2 py-0.5 text-xs text-gray-500">
                                  +{template.variables.length - 4} more
                                </span>
                              )}
                            </div>
                          </div>
                        )}

                        {/* Actions */}
                        <div className="flex items-center gap-2 border-t pt-3 sm:pt-4">
                          <button
                            onClick={() => setSelectedTemplate(template)}
                            className="flex flex-1 items-center justify-center gap-1 rounded-lg border py-2 text-xs sm:text-sm font-medium text-gray-600 hover:bg-gray-50"
                          >
                            <Eye className="h-4 w-4" />
                            View
                          </button>
                          <button
                            onClick={() => handleEditClick(template)}
                            className="flex flex-1 items-center justify-center gap-1 rounded-lg border py-2 text-xs sm:text-sm font-medium text-gray-600 hover:bg-gray-50"
                          >
                            <Edit className="h-4 w-4" />
                            Edit
                          </button>
                        </div>
                      </div>
                    )
                  })}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Template Detail Modal */}
      {selectedTemplate && (
        <div className="fixed inset-0 z-50 flex items-end sm:items-center justify-center bg-black/50 p-0 sm:p-4">
          <div className="max-h-[90vh] w-full sm:max-w-2xl overflow-y-auto rounded-t-2xl sm:rounded-2xl bg-white">
            <div className="sticky top-0 flex items-center justify-between border-b bg-white p-4">
              <div className="flex items-center gap-3 min-w-0">
                {(() => {
                  const Icon = channelIcons[selectedTemplate.channel] || MessageSquare
                  return (
                    <div className={`flex h-9 w-9 sm:h-10 sm:w-10 items-center justify-center rounded-lg sm:rounded-xl ${channelColors[selectedTemplate.channel]} flex-shrink-0`}>
                      <Icon className="h-4 w-4 sm:h-5 sm:w-5" />
                    </div>
                  )
                })()}
                <div className="min-w-0">
                  <h2 className="text-base sm:text-lg font-semibold truncate">{selectedTemplate.template_name}</h2>
                  <p className="text-xs sm:text-sm text-gray-500 truncate">{selectedTemplate.template_code}</p>
                </div>
              </div>
              <button
                onClick={() => setSelectedTemplate(null)}
                className="flex h-8 w-8 items-center justify-center rounded-lg text-gray-400 hover:bg-gray-100 flex-shrink-0"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            <div className="p-4 space-y-4">
              {/* Meta Info */}
              <div className="grid gap-3 sm:gap-4 grid-cols-3">
                <div className="rounded-lg bg-gray-50 p-2 sm:p-3">
                  <p className="text-xs text-gray-500">Type</p>
                  <p className="font-medium capitalize text-sm sm:text-base truncate">{selectedTemplate.notification_type.replace(/_/g, ' ')}</p>
                </div>
                <div className="rounded-lg bg-gray-50 p-2 sm:p-3">
                  <p className="text-xs text-gray-500">Channel</p>
                  <p className="font-medium capitalize text-sm sm:text-base">{selectedTemplate.channel}</p>
                </div>
                <div className="rounded-lg bg-gray-50 p-2 sm:p-3">
                  <p className="text-xs text-gray-500">Language</p>
                  <p className="font-medium uppercase text-sm sm:text-base">{selectedTemplate.language}</p>
                </div>
              </div>

              {/* Template Body */}
              <div>
                <div className="mb-2 flex items-center justify-between">
                  <p className="text-xs sm:text-sm font-medium text-gray-500">MESSAGE BODY</p>
                  <button
                    onClick={() => copyToClipboard(selectedTemplate.body)}
                    className="flex items-center gap-1 text-xs sm:text-sm text-blue-600 hover:text-blue-700"
                  >
                    {copied ? (
                      <>
                        <CheckCircle className="h-4 w-4" />
                        Copied!
                      </>
                    ) : (
                      <>
                        <Copy className="h-4 w-4" />
                        Copy
                      </>
                    )}
                  </button>
                </div>
                <div className="rounded-lg bg-gray-900 p-3 sm:p-4">
                  <pre className="whitespace-pre-wrap text-xs sm:text-sm text-green-400 overflow-x-auto">
                    {selectedTemplate.body}
                  </pre>
                </div>
              </div>

              {/* Variables */}
              {selectedTemplate.variables?.length > 0 && (
                <div>
                  <p className="mb-2 text-xs sm:text-sm font-medium text-gray-500">VARIABLES ({selectedTemplate.variables.length})</p>
                  <div className="flex flex-wrap gap-2">
                    {selectedTemplate.variables.map((v: string) => (
                      <span key={v} className="rounded-lg bg-blue-100 px-2 sm:px-3 py-1 text-xs sm:text-sm font-medium text-blue-700">
                        {`{{${v}}}`}
                      </span>
                    ))}
                  </div>
                </div>
              )}

              {/* WhatsApp Info */}
              {selectedTemplate.channel === 'whatsapp' && selectedTemplate.whatsapp_template_name && (
                <div className="rounded-lg border border-green-200 bg-green-50 p-3 sm:p-4">
                  <p className="mb-2 text-xs sm:text-sm font-medium text-green-700">WhatsApp Template Info</p>
                  <div className="grid gap-2 text-xs sm:text-sm">
                    <div className="flex flex-col sm:flex-row sm:justify-between gap-1">
                      <span className="text-green-600">Template Name</span>
                      <span className="font-medium text-green-800 truncate">{selectedTemplate.whatsapp_template_name}</span>
                    </div>
                    {selectedTemplate.whatsapp_namespace && (
                      <div className="flex flex-col sm:flex-row sm:justify-between gap-1">
                        <span className="text-green-600">Namespace</span>
                        <span className="font-medium text-green-800 truncate">{selectedTemplate.whatsapp_namespace}</span>
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Create Template Modal */}
      {showCreateModal && (
        <div className="fixed inset-0 z-50 flex items-end sm:items-center justify-center bg-black/50 p-0 sm:p-4">
          <div className="max-h-[90vh] w-full sm:max-w-2xl overflow-y-auto rounded-t-2xl sm:rounded-2xl bg-white">
            <div className="sticky top-0 flex items-center justify-between border-b bg-white p-4">
              <div>
                <h2 className="text-lg font-semibold">Create WhatsApp Template</h2>
                <p className="text-sm text-gray-500">Add a new message template</p>
              </div>
              <button
                onClick={() => setShowCreateModal(false)}
                className="flex h-8 w-8 items-center justify-center rounded-lg text-gray-400 hover:bg-gray-100"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            <div className="p-4 space-y-4">
              {/* Template Name */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Template Name *
                </label>
                <input
                  type="text"
                  value={newTemplate.template_name}
                  onChange={(e) => setNewTemplate({ ...newTemplate, template_name: e.target.value })}
                  placeholder="e.g., Booking Confirmation"
                  className="w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>

              {/* Template Code */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Template Code *
                </label>
                <input
                  type="text"
                  value={newTemplate.template_code}
                  onChange={(e) => setNewTemplate({ ...newTemplate, template_code: e.target.value.toUpperCase() })}
                  placeholder="e.g., BOOKING_CONFIRM"
                  className="w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono"
                />
              </div>

              {/* AISensy Campaign Name */}
              <div className="rounded-lg border border-green-200 bg-green-50 p-4">
                <label className="block text-sm font-medium text-green-700 mb-1">
                  AISensy Campaign Name *
                </label>
                <input
                  type="text"
                  value={newTemplate.whatsapp_template_name}
                  onChange={(e) => setNewTemplate({ ...newTemplate, whatsapp_template_name: e.target.value })}
                  placeholder="e.g., booking_confirmtion_testing"
                  className="w-full px-3 py-2 border border-green-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-green-500 bg-white"
                />
                <p className="mt-1 text-xs text-green-600">
                  This must match the campaign name in your AISensy dashboard
                </p>
              </div>

              {/* Type and Channel */}
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Notification Type *
                  </label>
                  <select
                    value={newTemplate.notification_type}
                    onChange={(e) => setNewTemplate({ ...newTemplate, notification_type: e.target.value })}
                    className="w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                  >
                    <option value="booking_confirmation">Booking Confirmation</option>
                    <option value="reminder_24h">24h Reminder</option>
                    <option value="reminder_2h">2h Reminder</option>
                    <option value="cancellation_ack">Cancellation</option>
                    <option value="delay_alert">Delay Alert</option>
                    <option value="wakeup_call">Wake-up Call</option>
                    <option value="custom">Custom</option>
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Channel
                  </label>
                  <select
                    value={newTemplate.channel}
                    onChange={(e) => setNewTemplate({ ...newTemplate, channel: e.target.value })}
                    className="w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                  >
                    <option value="whatsapp">WhatsApp</option>
                    <option value="sms">SMS</option>
                  </select>
                </div>
              </div>

              {/* Message Body */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Message Body *
                </label>
                <textarea
                  value={newTemplate.body}
                  onChange={(e) => setNewTemplate({ ...newTemplate, body: e.target.value })}
                  placeholder="Enter your message template with variables like {{1}}, {{2}}, etc."
                  rows={6}
                  className="w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono text-sm"
                />
                <p className="mt-1 text-xs text-gray-500">
                  Use {"{{1}}"}, {"{{2}}"}, etc. for dynamic values (passenger name, PNR, etc.)
                </p>
              </div>

              {/* Error Message */}
              {createMutation.error && (
                <div className="rounded-lg bg-red-50 border border-red-200 p-3 text-sm text-red-700">
                  {(createMutation.error as Error).message}
                </div>
              )}

              {/* Actions */}
              <div className="flex gap-3 pt-4 border-t">
                <button
                  onClick={() => setShowCreateModal(false)}
                  className="flex-1 px-4 py-2 border rounded-lg text-gray-700 hover:bg-gray-50"
                >
                  Cancel
                </button>
                <button
                  onClick={() => createMutation.mutate(newTemplate)}
                  disabled={createMutation.isPending || !newTemplate.template_name || !newTemplate.template_code || !newTemplate.whatsapp_template_name || !newTemplate.body}
                  className="flex-1 flex items-center justify-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {createMutation.isPending ? (
                    <>
                      <Loader2 className="h-4 w-4 animate-spin" />
                      Creating...
                    </>
                  ) : (
                    <>
                      <Save className="h-4 w-4" />
                      Create Template
                    </>
                  )}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Edit Template Modal */}
      {showEditModal && editingTemplate && (
        <div className="fixed inset-0 z-50 flex items-end sm:items-center justify-center bg-black/50 p-0 sm:p-4">
          <div className="max-h-[90vh] w-full sm:max-w-2xl overflow-y-auto rounded-t-2xl sm:rounded-2xl bg-white">
            <div className="sticky top-0 flex items-center justify-between border-b bg-white p-4">
              <div>
                <h2 className="text-lg font-semibold">Edit Template</h2>
                <p className="text-sm text-gray-500">Update template details</p>
              </div>
              <button
                onClick={() => {
                  setShowEditModal(false)
                  setEditingTemplate(null)
                }}
                className="flex h-8 w-8 items-center justify-center rounded-lg text-gray-400 hover:bg-gray-100"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            <div className="p-4 space-y-4">
              {/* Template Name */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Template Name *
                </label>
                <input
                  type="text"
                  value={editingTemplate.template_name}
                  onChange={(e) => setEditingTemplate({ ...editingTemplate, template_name: e.target.value })}
                  className="w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>

              {/* Template Code */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Template Code
                </label>
                <input
                  type="text"
                  value={editingTemplate.template_code}
                  disabled
                  className="w-full px-3 py-2 border rounded-lg bg-gray-100 text-gray-500 cursor-not-allowed"
                />
              </div>

              {/* AISensy Campaign Name */}
              <div className="rounded-lg border border-green-200 bg-green-50 p-4">
                <label className="block text-sm font-medium text-green-700 mb-1">
                  AISensy Campaign Name *
                </label>
                <input
                  type="text"
                  value={editingTemplate.whatsapp_template_name || ''}
                  onChange={(e) => setEditingTemplate({ ...editingTemplate, whatsapp_template_name: e.target.value })}
                  className="w-full px-3 py-2 border border-green-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-green-500 bg-white"
                />
                <p className="mt-1 text-xs text-green-600">
                  This must match the campaign name in your AISensy dashboard
                </p>
              </div>

              {/* Type and Channel */}
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Notification Type
                  </label>
                  <select
                    value={editingTemplate.notification_type}
                    onChange={(e) => setEditingTemplate({ ...editingTemplate, notification_type: e.target.value })}
                    className="w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                  >
                    <option value="booking_confirmation">Booking Confirmation</option>
                    <option value="reminder_24h">24h Reminder</option>
                    <option value="reminder_2h">2h Reminder</option>
                    <option value="cancellation_ack">Cancellation</option>
                    <option value="delay_alert">Delay Alert</option>
                    <option value="wakeup_call">Wake-up Call</option>
                    <option value="custom">Custom</option>
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Status
                  </label>
                  <select
                    value={editingTemplate.is_active ? 'active' : 'inactive'}
                    onChange={(e) => setEditingTemplate({ ...editingTemplate, is_active: e.target.value === 'active' })}
                    className="w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                  >
                    <option value="active">Active</option>
                    <option value="inactive">Inactive</option>
                  </select>
                </div>
              </div>

              {/* Message Body */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Message Body
                </label>
                <textarea
                  value={editingTemplate.body || ''}
                  onChange={(e) => setEditingTemplate({ ...editingTemplate, body: e.target.value })}
                  rows={6}
                  className="w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono text-sm"
                />
              </div>

              {/* Error Message */}
              {updateMutation.error && (
                <div className="rounded-lg bg-red-50 border border-red-200 p-3 text-sm text-red-700">
                  {(updateMutation.error as Error).message}
                </div>
              )}

              {/* Actions */}
              <div className="flex gap-3 pt-4 border-t">
                <button
                  onClick={() => {
                    setShowEditModal(false)
                    setEditingTemplate(null)
                  }}
                  className="flex-1 px-4 py-2 border rounded-lg text-gray-700 hover:bg-gray-50"
                >
                  Cancel
                </button>
                <button
                  onClick={() => updateMutation.mutate(editingTemplate)}
                  disabled={updateMutation.isPending || !editingTemplate.template_name}
                  className="flex-1 flex items-center justify-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {updateMutation.isPending ? (
                    <>
                      <Loader2 className="h-4 w-4 animate-spin" />
                      Saving...
                    </>
                  ) : (
                    <>
                      <Save className="h-4 w-4" />
                      Save Changes
                    </>
                  )}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </DashboardLayout>
  )
}
