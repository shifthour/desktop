'use client'

import { useState } from 'react'
import { DashboardLayout } from '@/components/dashboard/dashboard-layout'
import { Header } from '@/components/ui/header'
import {
  Settings,
  Bell,
  MessageSquare,
  Phone,
  Mail,
  Shield,
  Database,
  Globe,
  Clock,
  Save,
  RefreshCw,
  CheckCircle,
  AlertTriangle,
  Key,
  Server,
  Webhook,
} from 'lucide-react'

export default function SettingsPage() {
  const [activeTab, setActiveTab] = useState('general')
  const [saved, setSaved] = useState(false)

  // General settings state
  const [generalSettings, setGeneralSettings] = useState({
    operatorName: 'Mythri Travels',
    operatorCode: 'MYTHRI',
    timezone: 'Asia/Kolkata',
    defaultLanguage: 'en',
    dateFormat: 'DD/MM/YYYY',
    currency: 'INR',
  })

  // Notification settings state
  const [notificationSettings, setNotificationSettings] = useState({
    bookingConfirmation: true,
    reminder24h: true,
    reminder2h: true,
    cancellationAlert: true,
    delayNotification: true,
    wakeUpCall: false,
    defaultChannel: 'whatsapp',
    fallbackChannel: 'sms',
    retryAttempts: 3,
    retryDelay: 5,
  })

  // API settings state
  const [apiSettings, setApiSettings] = useState({
    bitlaApiUrl: 'http://myth.mythribus.com/api/',
    syncInterval: 15,
    webhookSecret: '••••••••••••••••',
    enableWebhooks: true,
    enablePolling: true,
  })

  // WhatsApp settings state
  const [whatsappSettings, setWhatsappSettings] = useState({
    provider: 'meta',
    businessAccountId: '',
    phoneNumberId: '',
    accessToken: '••••••••••••••••',
    webhookVerifyToken: '••••••••••••••••',
    templateNamespace: '',
  })

  const handleSave = () => {
    setSaved(true)
    setTimeout(() => setSaved(false), 3000)
  }

  const tabs = [
    { id: 'general', label: 'General', icon: Settings },
    { id: 'notifications', label: 'Notifications', icon: Bell },
    { id: 'api', label: 'API & Sync', icon: Server },
    { id: 'whatsapp', label: 'WhatsApp', icon: MessageSquare },
    { id: 'sms', label: 'SMS', icon: Phone },
    { id: 'security', label: 'Security', icon: Shield },
  ]

  return (
    <DashboardLayout>
      <Header title="Settings" subtitle="Configure system preferences" />

      <div className="p-4 sm:p-6">
        <div className="flex flex-col lg:flex-row gap-4 sm:gap-6">
          {/* Sidebar Tabs - Horizontal scroll on mobile */}
          <div className="lg:w-64 flex-shrink-0">
            {/* Mobile Tab Bar */}
            <div className="flex lg:hidden overflow-x-auto gap-2 pb-2 -mx-4 px-4 sm:-mx-6 sm:px-6">
              {tabs.map((tab) => {
                const Icon = tab.icon
                return (
                  <button
                    key={tab.id}
                    onClick={() => setActiveTab(tab.id)}
                    className={`flex items-center gap-2 rounded-lg px-3 py-2 text-xs font-medium whitespace-nowrap flex-shrink-0 ${
                      activeTab === tab.id
                        ? 'bg-blue-600 text-white'
                        : 'bg-white border text-gray-600'
                    }`}
                  >
                    <Icon className="h-4 w-4" />
                    {tab.label}
                  </button>
                )
              })}
            </div>
            {/* Desktop Tab List */}
            <div className="hidden lg:block rounded-2xl border bg-white p-2">
              {tabs.map((tab) => {
                const Icon = tab.icon
                return (
                  <button
                    key={tab.id}
                    onClick={() => setActiveTab(tab.id)}
                    className={`w-full flex items-center gap-3 rounded-xl px-4 py-3 text-left text-sm font-medium transition-colors ${
                      activeTab === tab.id
                        ? 'bg-blue-50 text-blue-700'
                        : 'text-gray-600 hover:bg-gray-50'
                    }`}
                  >
                    <Icon className="h-5 w-5" />
                    {tab.label}
                  </button>
                )
              })}
            </div>
          </div>

          {/* Content Area */}
          <div className="flex-1 min-w-0">
            <div className="rounded-xl sm:rounded-2xl border bg-white">
              {/* General Settings */}
              {activeTab === 'general' && (
                <div>
                  <div className="border-b p-4 sm:p-6">
                    <h2 className="text-base sm:text-lg font-semibold text-gray-900">General Settings</h2>
                    <p className="text-xs sm:text-sm text-gray-500">Basic configuration for your operator account</p>
                  </div>
                  <div className="p-4 sm:p-6 space-y-4 sm:space-y-6">
                    <div className="grid gap-4 sm:gap-6 sm:grid-cols-2">
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-2">
                          Operator Name
                        </label>
                        <input
                          type="text"
                          value={generalSettings.operatorName}
                          onChange={(e) => setGeneralSettings({ ...generalSettings, operatorName: e.target.value })}
                          className="w-full rounded-xl border border-gray-200 px-4 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                        />
                      </div>
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-2">
                          Operator Code
                        </label>
                        <input
                          type="text"
                          value={generalSettings.operatorCode}
                          onChange={(e) => setGeneralSettings({ ...generalSettings, operatorCode: e.target.value })}
                          className="w-full rounded-xl border border-gray-200 px-4 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                        />
                      </div>
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-2">
                          Timezone
                        </label>
                        <select
                          value={generalSettings.timezone}
                          onChange={(e) => setGeneralSettings({ ...generalSettings, timezone: e.target.value })}
                          className="w-full rounded-xl border border-gray-200 px-4 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                        >
                          <option value="Asia/Kolkata">Asia/Kolkata (IST)</option>
                          <option value="UTC">UTC</option>
                        </select>
                      </div>
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-2">
                          Default Language
                        </label>
                        <select
                          value={generalSettings.defaultLanguage}
                          onChange={(e) => setGeneralSettings({ ...generalSettings, defaultLanguage: e.target.value })}
                          className="w-full rounded-xl border border-gray-200 px-4 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                        >
                          <option value="en">English</option>
                          <option value="te">Telugu</option>
                          <option value="hi">Hindi</option>
                          <option value="ta">Tamil</option>
                        </select>
                      </div>
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-2">
                          Date Format
                        </label>
                        <select
                          value={generalSettings.dateFormat}
                          onChange={(e) => setGeneralSettings({ ...generalSettings, dateFormat: e.target.value })}
                          className="w-full rounded-xl border border-gray-200 px-4 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                        >
                          <option value="DD/MM/YYYY">DD/MM/YYYY</option>
                          <option value="MM/DD/YYYY">MM/DD/YYYY</option>
                          <option value="YYYY-MM-DD">YYYY-MM-DD</option>
                        </select>
                      </div>
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-2">
                          Currency
                        </label>
                        <select
                          value={generalSettings.currency}
                          onChange={(e) => setGeneralSettings({ ...generalSettings, currency: e.target.value })}
                          className="w-full rounded-xl border border-gray-200 px-4 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                        >
                          <option value="INR">Indian Rupee (INR)</option>
                          <option value="USD">US Dollar (USD)</option>
                        </select>
                      </div>
                    </div>
                  </div>
                </div>
              )}

              {/* Notification Settings */}
              {activeTab === 'notifications' && (
                <div>
                  <div className="border-b p-4 sm:p-6">
                    <h2 className="text-base sm:text-lg font-semibold text-gray-900">Notification Settings</h2>
                    <p className="text-xs sm:text-sm text-gray-500">Configure automated notifications</p>
                  </div>
                  <div className="p-4 sm:p-6 space-y-4 sm:space-y-6">
                    {/* Notification Types */}
                    <div>
                      <h3 className="text-xs sm:text-sm font-semibold text-gray-900 mb-3 sm:mb-4">Notification Types</h3>
                      <div className="space-y-2 sm:space-y-3">
                        {[
                          { key: 'bookingConfirmation', label: 'Booking Confirmation', desc: 'Send confirmation after successful booking' },
                          { key: 'reminder24h', label: '24-Hour Reminder', desc: 'Remind passengers 24 hours before travel' },
                          { key: 'reminder2h', label: '2-Hour Reminder', desc: 'Remind passengers 2 hours before departure' },
                          { key: 'cancellationAlert', label: 'Cancellation Alert', desc: 'Notify on booking cancellation' },
                          { key: 'delayNotification', label: 'Delay Notification', desc: 'Alert passengers about service delays' },
                          { key: 'wakeUpCall', label: 'Wake-up Call', desc: 'Early morning wake-up notification' },
                        ].map((item) => (
                          <label key={item.key} className="flex items-center justify-between rounded-lg sm:rounded-xl border p-3 sm:p-4 cursor-pointer hover:bg-gray-50">
                            <div className="min-w-0 flex-1 pr-3">
                              <p className="font-medium text-gray-900 text-sm sm:text-base">{item.label}</p>
                              <p className="text-xs sm:text-sm text-gray-500 truncate">{item.desc}</p>
                            </div>
                            <div className="relative">
                              <input
                                type="checkbox"
                                checked={notificationSettings[item.key as keyof typeof notificationSettings] as boolean}
                                onChange={(e) => setNotificationSettings({ ...notificationSettings, [item.key]: e.target.checked })}
                                className="sr-only peer"
                              />
                              <div className="w-11 h-6 bg-gray-200 rounded-full peer peer-checked:bg-blue-600 transition-colors"></div>
                              <div className="absolute left-1 top-1 w-4 h-4 bg-white rounded-full transition-transform peer-checked:translate-x-5"></div>
                            </div>
                          </label>
                        ))}
                      </div>
                    </div>

                    {/* Channel Settings */}
                    <div className="border-t pt-4 sm:pt-6">
                      <h3 className="text-xs sm:text-sm font-semibold text-gray-900 mb-3 sm:mb-4">Channel Preferences</h3>
                      <div className="grid gap-4 sm:gap-6 sm:grid-cols-2">
                        <div>
                          <label className="block text-sm font-medium text-gray-700 mb-2">
                            Default Channel
                          </label>
                          <select
                            value={notificationSettings.defaultChannel}
                            onChange={(e) => setNotificationSettings({ ...notificationSettings, defaultChannel: e.target.value })}
                            className="w-full rounded-xl border border-gray-200 px-4 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                          >
                            <option value="whatsapp">WhatsApp</option>
                            <option value="sms">SMS</option>
                            <option value="email">Email</option>
                          </select>
                        </div>
                        <div>
                          <label className="block text-sm font-medium text-gray-700 mb-2">
                            Fallback Channel
                          </label>
                          <select
                            value={notificationSettings.fallbackChannel}
                            onChange={(e) => setNotificationSettings({ ...notificationSettings, fallbackChannel: e.target.value })}
                            className="w-full rounded-xl border border-gray-200 px-4 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                          >
                            <option value="sms">SMS</option>
                            <option value="whatsapp">WhatsApp</option>
                            <option value="email">Email</option>
                          </select>
                        </div>
                        <div>
                          <label className="block text-sm font-medium text-gray-700 mb-2">
                            Retry Attempts
                          </label>
                          <input
                            type="number"
                            min="0"
                            max="5"
                            value={notificationSettings.retryAttempts}
                            onChange={(e) => setNotificationSettings({ ...notificationSettings, retryAttempts: parseInt(e.target.value) })}
                            className="w-full rounded-xl border border-gray-200 px-4 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                          />
                        </div>
                        <div>
                          <label className="block text-sm font-medium text-gray-700 mb-2">
                            Retry Delay (minutes)
                          </label>
                          <input
                            type="number"
                            min="1"
                            max="60"
                            value={notificationSettings.retryDelay}
                            onChange={(e) => setNotificationSettings({ ...notificationSettings, retryDelay: parseInt(e.target.value) })}
                            className="w-full rounded-xl border border-gray-200 px-4 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                          />
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              )}

              {/* API & Sync Settings */}
              {activeTab === 'api' && (
                <div>
                  <div className="border-b p-4 sm:p-6">
                    <h2 className="text-base sm:text-lg font-semibold text-gray-900">API & Sync Settings</h2>
                    <p className="text-xs sm:text-sm text-gray-500">Configure Bitla API and data synchronization</p>
                  </div>
                  <div className="p-4 sm:p-6 space-y-4 sm:space-y-6">
                    {/* Connection Status */}
                    <div className="rounded-lg sm:rounded-xl border border-green-200 bg-green-50 p-3 sm:p-4">
                      <div className="flex items-center gap-3">
                        <CheckCircle className="h-5 w-5 text-green-600 flex-shrink-0" />
                        <div className="min-w-0">
                          <p className="font-medium text-green-800 text-sm sm:text-base">Connected to Bitla API</p>
                          <p className="text-xs sm:text-sm text-green-600">Last sync: 5 minutes ago</p>
                        </div>
                      </div>
                    </div>

                    {/* API Configuration */}
                    <div>
                      <h3 className="text-xs sm:text-sm font-semibold text-gray-900 mb-3 sm:mb-4">Bitla API Configuration</h3>
                      <div className="grid gap-4 sm:gap-6 sm:grid-cols-2">
                        <div className="sm:col-span-2">
                          <label className="block text-sm font-medium text-gray-700 mb-2">
                            API Base URL
                          </label>
                          <input
                            type="url"
                            value={apiSettings.bitlaApiUrl}
                            onChange={(e) => setApiSettings({ ...apiSettings, bitlaApiUrl: e.target.value })}
                            className="w-full rounded-xl border border-gray-200 px-4 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                          />
                        </div>
                        <div>
                          <label className="block text-sm font-medium text-gray-700 mb-2">
                            Sync Interval (minutes)
                          </label>
                          <input
                            type="number"
                            min="5"
                            max="60"
                            value={apiSettings.syncInterval}
                            onChange={(e) => setApiSettings({ ...apiSettings, syncInterval: parseInt(e.target.value) })}
                            className="w-full rounded-xl border border-gray-200 px-4 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                          />
                        </div>
                        <div>
                          <label className="block text-sm font-medium text-gray-700 mb-2">
                            Webhook Secret
                          </label>
                          <div className="relative">
                            <input
                              type="password"
                              value={apiSettings.webhookSecret}
                              onChange={(e) => setApiSettings({ ...apiSettings, webhookSecret: e.target.value })}
                              className="w-full rounded-xl border border-gray-200 px-4 py-2.5 pr-12 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                            />
                            <button className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600">
                              <Key className="h-4 w-4" />
                            </button>
                          </div>
                        </div>
                      </div>
                    </div>

                    {/* Sync Options */}
                    <div className="border-t pt-4 sm:pt-6">
                      <h3 className="text-xs sm:text-sm font-semibold text-gray-900 mb-3 sm:mb-4">Sync Options</h3>
                      <div className="space-y-2 sm:space-y-3">
                        <label className="flex items-center justify-between rounded-lg sm:rounded-xl border p-3 sm:p-4 cursor-pointer hover:bg-gray-50">
                          <div className="flex items-center gap-2 sm:gap-3 min-w-0 flex-1 pr-3">
                            <Webhook className="h-5 w-5 text-gray-400 flex-shrink-0" />
                            <div className="min-w-0">
                              <p className="font-medium text-gray-900 text-sm sm:text-base">Enable Webhooks</p>
                              <p className="text-xs sm:text-sm text-gray-500 truncate">Receive real-time updates from Bitla</p>
                            </div>
                          </div>
                          <div className="relative">
                            <input
                              type="checkbox"
                              checked={apiSettings.enableWebhooks}
                              onChange={(e) => setApiSettings({ ...apiSettings, enableWebhooks: e.target.checked })}
                              className="sr-only peer"
                            />
                            <div className="w-11 h-6 bg-gray-200 rounded-full peer peer-checked:bg-blue-600 transition-colors"></div>
                            <div className="absolute left-1 top-1 w-4 h-4 bg-white rounded-full transition-transform peer-checked:translate-x-5"></div>
                          </div>
                        </label>
                        <label className="flex items-center justify-between rounded-lg sm:rounded-xl border p-3 sm:p-4 cursor-pointer hover:bg-gray-50">
                          <div className="flex items-center gap-2 sm:gap-3 min-w-0 flex-1 pr-3">
                            <RefreshCw className="h-5 w-5 text-gray-400 flex-shrink-0" />
                            <div className="min-w-0">
                              <p className="font-medium text-gray-900 text-sm sm:text-base">Enable Polling</p>
                              <p className="text-xs sm:text-sm text-gray-500 truncate">Periodically fetch data from Bitla API</p>
                            </div>
                          </div>
                          <div className="relative">
                            <input
                              type="checkbox"
                              checked={apiSettings.enablePolling}
                              onChange={(e) => setApiSettings({ ...apiSettings, enablePolling: e.target.checked })}
                              className="sr-only peer"
                            />
                            <div className="w-11 h-6 bg-gray-200 rounded-full peer peer-checked:bg-blue-600 transition-colors"></div>
                            <div className="absolute left-1 top-1 w-4 h-4 bg-white rounded-full transition-transform peer-checked:translate-x-5"></div>
                          </div>
                        </label>
                      </div>
                    </div>

                    {/* Webhook Endpoints */}
                    <div className="border-t pt-4 sm:pt-6">
                      <h3 className="text-xs sm:text-sm font-semibold text-gray-900 mb-3 sm:mb-4">Webhook Endpoints</h3>
                      <div className="rounded-lg sm:rounded-xl bg-gray-50 p-3 sm:p-4 space-y-2">
                        {[
                          { event: 'Booking', path: '/api/webhooks/bitla/booking' },
                          { event: 'Cancellation', path: '/api/webhooks/bitla/cancellation' },
                          { event: 'Boarding Status', path: '/api/webhooks/bitla/boarding-status' },
                          { event: 'Vehicle Assignment', path: '/api/webhooks/bitla/vehicle-assign' },
                          { event: 'Service Update', path: '/api/webhooks/bitla/service-update' },
                        ].map((endpoint) => (
                          <div key={endpoint.event} className="flex flex-col sm:flex-row sm:items-center sm:justify-between py-2 gap-1">
                            <span className="text-xs sm:text-sm font-medium text-gray-600">{endpoint.event}</span>
                            <code className="rounded bg-gray-200 px-2 py-1 text-[10px] sm:text-xs text-gray-700 overflow-x-auto">
                              POST {endpoint.path}
                            </code>
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                </div>
              )}

              {/* WhatsApp Settings */}
              {activeTab === 'whatsapp' && (
                <div>
                  <div className="border-b p-4 sm:p-6">
                    <h2 className="text-base sm:text-lg font-semibold text-gray-900">WhatsApp Business API</h2>
                    <p className="text-xs sm:text-sm text-gray-500">Configure WhatsApp messaging integration</p>
                  </div>
                  <div className="p-4 sm:p-6 space-y-4 sm:space-y-6">
                    {/* Status */}
                    <div className="rounded-lg sm:rounded-xl border border-yellow-200 bg-yellow-50 p-3 sm:p-4">
                      <div className="flex items-center gap-3">
                        <AlertTriangle className="h-5 w-5 text-yellow-600 flex-shrink-0" />
                        <div className="min-w-0">
                          <p className="font-medium text-yellow-800 text-sm sm:text-base">Not Configured</p>
                          <p className="text-xs sm:text-sm text-yellow-600">WhatsApp Business API credentials required</p>
                        </div>
                      </div>
                    </div>

                    <div className="grid gap-4 sm:gap-6 sm:grid-cols-2">
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-2">
                          Provider
                        </label>
                        <select
                          value={whatsappSettings.provider}
                          onChange={(e) => setWhatsappSettings({ ...whatsappSettings, provider: e.target.value })}
                          className="w-full rounded-xl border border-gray-200 px-4 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                        >
                          <option value="meta">Meta (Official)</option>
                          <option value="twilio">Twilio</option>
                          <option value="gupshup">Gupshup</option>
                        </select>
                      </div>
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-2">
                          Business Account ID
                        </label>
                        <input
                          type="text"
                          value={whatsappSettings.businessAccountId}
                          onChange={(e) => setWhatsappSettings({ ...whatsappSettings, businessAccountId: e.target.value })}
                          placeholder="Enter Business Account ID"
                          className="w-full rounded-xl border border-gray-200 px-4 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                        />
                      </div>
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-2">
                          Phone Number ID
                        </label>
                        <input
                          type="text"
                          value={whatsappSettings.phoneNumberId}
                          onChange={(e) => setWhatsappSettings({ ...whatsappSettings, phoneNumberId: e.target.value })}
                          placeholder="Enter Phone Number ID"
                          className="w-full rounded-xl border border-gray-200 px-4 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                        />
                      </div>
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-2">
                          Template Namespace
                        </label>
                        <input
                          type="text"
                          value={whatsappSettings.templateNamespace}
                          onChange={(e) => setWhatsappSettings({ ...whatsappSettings, templateNamespace: e.target.value })}
                          placeholder="Enter Template Namespace"
                          className="w-full rounded-xl border border-gray-200 px-4 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                        />
                      </div>
                      <div className="sm:col-span-2">
                        <label className="block text-sm font-medium text-gray-700 mb-2">
                          Access Token
                        </label>
                        <div className="relative">
                          <input
                            type="password"
                            value={whatsappSettings.accessToken}
                            onChange={(e) => setWhatsappSettings({ ...whatsappSettings, accessToken: e.target.value })}
                            placeholder="Enter Access Token"
                            className="w-full rounded-xl border border-gray-200 px-4 py-2.5 pr-12 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                          />
                          <button className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600">
                            <Key className="h-4 w-4" />
                          </button>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              )}

              {/* SMS Settings */}
              {activeTab === 'sms' && (
                <div>
                  <div className="border-b p-4 sm:p-6">
                    <h2 className="text-base sm:text-lg font-semibold text-gray-900">SMS Gateway</h2>
                    <p className="text-xs sm:text-sm text-gray-500">Configure SMS messaging provider</p>
                  </div>
                  <div className="p-4 sm:p-6 space-y-4 sm:space-y-6">
                    {/* Status */}
                    <div className="rounded-lg sm:rounded-xl border border-yellow-200 bg-yellow-50 p-3 sm:p-4">
                      <div className="flex items-center gap-3">
                        <AlertTriangle className="h-5 w-5 text-yellow-600 flex-shrink-0" />
                        <div className="min-w-0">
                          <p className="font-medium text-yellow-800 text-sm sm:text-base">Not Configured</p>
                          <p className="text-xs sm:text-sm text-yellow-600">SMS gateway credentials required</p>
                        </div>
                      </div>
                    </div>

                    <div className="grid gap-4 sm:gap-6 sm:grid-cols-2">
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-2">
                          SMS Provider
                        </label>
                        <select
                          className="w-full rounded-xl border border-gray-200 px-4 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                        >
                          <option value="msg91">MSG91</option>
                          <option value="twilio">Twilio</option>
                          <option value="textlocal">TextLocal</option>
                        </select>
                      </div>
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-2">
                          Sender ID
                        </label>
                        <input
                          type="text"
                          placeholder="e.g., MYTHRI"
                          className="w-full rounded-xl border border-gray-200 px-4 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                        />
                      </div>
                      <div className="sm:col-span-2">
                        <label className="block text-sm font-medium text-gray-700 mb-2">
                          API Key
                        </label>
                        <div className="relative">
                          <input
                            type="password"
                            placeholder="Enter API Key"
                            className="w-full rounded-xl border border-gray-200 px-4 py-2.5 pr-12 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                          />
                          <button className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600">
                            <Key className="h-4 w-4" />
                          </button>
                        </div>
                      </div>
                    </div>

                    {/* DLT Info */}
                    <div className="border-t pt-4 sm:pt-6">
                      <h3 className="text-xs sm:text-sm font-semibold text-gray-900 mb-3 sm:mb-4">DLT Registration (India)</h3>
                      <div className="grid gap-4 sm:gap-6 sm:grid-cols-2">
                        <div>
                          <label className="block text-sm font-medium text-gray-700 mb-2">
                            Entity ID
                          </label>
                          <input
                            type="text"
                            placeholder="Enter DLT Entity ID"
                            className="w-full rounded-xl border border-gray-200 px-4 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                          />
                        </div>
                        <div>
                          <label className="block text-sm font-medium text-gray-700 mb-2">
                            Template ID
                          </label>
                          <input
                            type="text"
                            placeholder="Enter DLT Template ID"
                            className="w-full rounded-xl border border-gray-200 px-4 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                          />
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              )}

              {/* Security Settings */}
              {activeTab === 'security' && (
                <div>
                  <div className="border-b p-4 sm:p-6">
                    <h2 className="text-base sm:text-lg font-semibold text-gray-900">Security Settings</h2>
                    <p className="text-xs sm:text-sm text-gray-500">Manage access and security preferences</p>
                  </div>
                  <div className="p-4 sm:p-6 space-y-4 sm:space-y-6">
                    <div className="space-y-2 sm:space-y-3">
                      <label className="flex items-center justify-between rounded-lg sm:rounded-xl border p-3 sm:p-4 cursor-pointer hover:bg-gray-50">
                        <div className="min-w-0 flex-1 pr-3">
                          <p className="font-medium text-gray-900 text-sm sm:text-base">Require Webhook Signature</p>
                          <p className="text-xs sm:text-sm text-gray-500 truncate">Validate X-Webhook-Secret header</p>
                        </div>
                        <div className="relative">
                          <input type="checkbox" defaultChecked className="sr-only peer" />
                          <div className="w-11 h-6 bg-gray-200 rounded-full peer peer-checked:bg-blue-600 transition-colors"></div>
                          <div className="absolute left-1 top-1 w-4 h-4 bg-white rounded-full transition-transform peer-checked:translate-x-5"></div>
                        </div>
                      </label>
                      <label className="flex items-center justify-between rounded-lg sm:rounded-xl border p-3 sm:p-4 cursor-pointer hover:bg-gray-50">
                        <div className="min-w-0 flex-1 pr-3">
                          <p className="font-medium text-gray-900 text-sm sm:text-base">IP Whitelist</p>
                          <p className="text-xs sm:text-sm text-gray-500 truncate">Only accept webhooks from whitelisted IPs</p>
                        </div>
                        <div className="relative">
                          <input type="checkbox" className="sr-only peer" />
                          <div className="w-11 h-6 bg-gray-200 rounded-full peer peer-checked:bg-blue-600 transition-colors"></div>
                          <div className="absolute left-1 top-1 w-4 h-4 bg-white rounded-full transition-transform peer-checked:translate-x-5"></div>
                        </div>
                      </label>
                      <label className="flex items-center justify-between rounded-lg sm:rounded-xl border p-3 sm:p-4 cursor-pointer hover:bg-gray-50">
                        <div className="min-w-0 flex-1 pr-3">
                          <p className="font-medium text-gray-900 text-sm sm:text-base">Audit Logging</p>
                          <p className="text-xs sm:text-sm text-gray-500 truncate">Log all API requests and admin actions</p>
                        </div>
                        <div className="relative">
                          <input type="checkbox" defaultChecked className="sr-only peer" />
                          <div className="w-11 h-6 bg-gray-200 rounded-full peer peer-checked:bg-blue-600 transition-colors"></div>
                          <div className="absolute left-1 top-1 w-4 h-4 bg-white rounded-full transition-transform peer-checked:translate-x-5"></div>
                        </div>
                      </label>
                      <label className="flex items-center justify-between rounded-lg sm:rounded-xl border p-3 sm:p-4 cursor-pointer hover:bg-gray-50">
                        <div className="min-w-0 flex-1 pr-3">
                          <p className="font-medium text-gray-900 text-sm sm:text-base">Rate Limiting</p>
                          <p className="text-xs sm:text-sm text-gray-500 truncate">Limit API requests per minute</p>
                        </div>
                        <div className="relative">
                          <input type="checkbox" defaultChecked className="sr-only peer" />
                          <div className="w-11 h-6 bg-gray-200 rounded-full peer peer-checked:bg-blue-600 transition-colors"></div>
                          <div className="absolute left-1 top-1 w-4 h-4 bg-white rounded-full transition-transform peer-checked:translate-x-5"></div>
                        </div>
                      </label>
                    </div>
                  </div>
                </div>
              )}

              {/* Save Button */}
              <div className="border-t p-4 sm:p-6">
                <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
                  <p className="text-xs sm:text-sm text-gray-500 text-center sm:text-left">
                    {saved ? (
                      <span className="flex items-center justify-center sm:justify-start gap-2 text-green-600">
                        <CheckCircle className="h-4 w-4" />
                        Settings saved successfully
                      </span>
                    ) : (
                      'Changes will be applied after saving'
                    )}
                  </p>
                  <button
                    onClick={handleSave}
                    className="flex items-center justify-center gap-2 rounded-xl bg-blue-600 px-6 py-2.5 text-sm font-medium text-white hover:bg-blue-700 transition-colors w-full sm:w-auto"
                  >
                    <Save className="h-4 w-4" />
                    Save Changes
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </DashboardLayout>
  )
}
