'use client'

import { useState } from 'react'
import { 
  Search, 
  Plus, 
  Filter, 
  Phone, 
  Mail,
  Building2,
  MapPin,
  DollarSign,
  TrendingUp,
  Award,
  MoreVertical,
  Edit,
  Eye,
  UserCheck
} from 'lucide-react'

const mockPartners = [
  {
    id: 1,
    companyName: 'Elite Realty Solutions',
    contactPerson: 'Rahul Sharma',
    email: 'rahul@eliterealty.com',
    phone: '+91 98765 43210',
    registrationNo: 'REG2023001',
    gstNumber: '29ABCDE1234F1Z5',
    address: 'MG Road, Bangalore',
    city: 'Bangalore',
    state: 'Karnataka',
    commissionRate: 2.5,
    totalEarnings: 4500000,
    pendingPayments: 350000,
    activeLeads: 23,
    closedDeals: 45,
    isActive: true,
    joinedDate: '2023-01-15',
    rating: 4.8
  },
  {
    id: 2,
    companyName: 'Property Hub Associates',
    contactPerson: 'Priya Patel',
    email: 'priya@propertyhub.com',
    phone: '+91 87654 32109',
    registrationNo: 'REG2023002',
    gstNumber: '29FGHIJ5678K2M6',
    address: 'Koramangala, Bangalore',
    city: 'Bangalore',
    state: 'Karnataka',
    commissionRate: 3.0,
    totalEarnings: 3200000,
    pendingPayments: 180000,
    activeLeads: 18,
    closedDeals: 32,
    isActive: true,
    joinedDate: '2023-02-20',
    rating: 4.5
  },
  {
    id: 3,
    companyName: 'Dream Homes Consultancy',
    contactPerson: 'Amit Verma',
    email: 'amit@dreamhomes.com',
    phone: '+91 76543 21098',
    registrationNo: 'REG2023003',
    gstNumber: '29LMNOP9012Q3R7',
    address: 'Whitefield, Bangalore',
    city: 'Bangalore',
    state: 'Karnataka',
    commissionRate: 2.0,
    totalEarnings: 2800000,
    pendingPayments: 0,
    activeLeads: 15,
    closedDeals: 28,
    isActive: true,
    joinedDate: '2023-03-10',
    rating: 4.6
  }
]

const performanceData = [
  { month: 'Jan', leads: 45, deals: 12 },
  { month: 'Feb', leads: 52, deals: 15 },
  { month: 'Mar', leads: 48, deals: 18 },
  { month: 'Apr', leads: 61, deals: 22 },
  { month: 'May', leads: 58, deals: 20 },
  { month: 'Jun', leads: 65, deals: 25 }
]

export default function PartnersPage() {
  const [partners] = useState(mockPartners)
  const [searchTerm, setSearchTerm] = useState('')
  const [showAddModal, setShowAddModal] = useState(false)
  const [selectedPartner, setSelectedPartner] = useState<any>(null)

  const filteredPartners = partners.filter(partner =>
    partner.companyName.toLowerCase().includes(searchTerm.toLowerCase()) ||
    partner.contactPerson.toLowerCase().includes(searchTerm.toLowerCase()) ||
    partner.email.toLowerCase().includes(searchTerm.toLowerCase())
  )

  const formatCurrency = (amount: number) => {
    if (amount >= 10000000) return `₹${(amount / 10000000).toFixed(1)}Cr`
    if (amount >= 100000) return `₹${(amount / 100000).toFixed(1)}L`
    return `₹${amount.toLocaleString()}`
  }

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900">Channel Partners</h1>
        <p className="text-gray-600 mt-2">Manage your channel partner network and performance</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-600">Total Partners</p>
              <p className="text-2xl font-bold text-gray-900 mt-1">{partners.length}</p>
            </div>
            <UserCheck className="h-8 w-8 text-blue-600" />
          </div>
        </div>
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-600">Active Leads</p>
              <p className="text-2xl font-bold text-gray-900 mt-1">
                {partners.reduce((sum, p) => sum + p.activeLeads, 0)}
              </p>
            </div>
            <TrendingUp className="h-8 w-8 text-green-600" />
          </div>
        </div>
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-600">Total Deals</p>
              <p className="text-2xl font-bold text-gray-900 mt-1">
                {partners.reduce((sum, p) => sum + p.closedDeals, 0)}
              </p>
            </div>
            <Award className="h-8 w-8 text-purple-600" />
          </div>
        </div>
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-600">Pending Payments</p>
              <p className="text-2xl font-bold text-gray-900 mt-1">
                {formatCurrency(partners.reduce((sum, p) => sum + p.pendingPayments, 0))}
              </p>
            </div>
            <DollarSign className="h-8 w-8 text-yellow-600" />
          </div>
        </div>
      </div>

      <div className="bg-white rounded-lg shadow">
        <div className="p-6 border-b border-gray-200">
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
            <div className="flex items-center space-x-4 flex-1">
              <div className="relative flex-1 max-w-md">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-5 w-5 text-gray-400" />
                <input
                  type="text"
                  placeholder="Search partners..."
                  className="pl-10 pr-4 py-2 w-full border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                />
              </div>
              <button className="flex items-center space-x-2 px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition">
                <Filter className="h-5 w-5" />
                <span>Filter</span>
              </button>
            </div>
            <button
              onClick={() => setShowAddModal(true)}
              className="flex items-center space-x-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition"
            >
              <Plus className="h-5 w-5" />
              <span>Add Partner</span>
            </button>
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Partner Details
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Contact
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Performance
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Financials
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Status
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {filteredPartners.map((partner) => (
                <tr key={partner.id} className="hover:bg-gray-50 transition">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div>
                      <div className="text-sm font-medium text-gray-900 flex items-center">
                        <Building2 className="h-4 w-4 mr-2 text-gray-400" />
                        {partner.companyName}
                      </div>
                      <div className="text-sm text-gray-500 mt-1">
                        Reg: {partner.registrationNo}
                      </div>
                      <div className="flex items-center mt-1">
                        {[...Array(5)].map((_, i) => (
                          <svg
                            key={i}
                            className={`h-4 w-4 ${
                              i < Math.floor(partner.rating)
                                ? 'text-yellow-400'
                                : 'text-gray-300'
                            }`}
                            fill="currentColor"
                            viewBox="0 0 20 20"
                          >
                            <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
                          </svg>
                        ))}
                        <span className="text-xs text-gray-500 ml-1">{partner.rating}</span>
                      </div>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div>
                      <div className="text-sm text-gray-900">{partner.contactPerson}</div>
                      <div className="text-sm text-gray-500 flex items-center mt-1">
                        <Mail className="h-3 w-3 mr-1" />
                        {partner.email}
                      </div>
                      <div className="text-sm text-gray-500 flex items-center mt-1">
                        <Phone className="h-3 w-3 mr-1" />
                        {partner.phone}
                      </div>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div>
                      <div className="text-sm text-gray-900">
                        {partner.activeLeads} active leads
                      </div>
                      <div className="text-sm text-gray-500">
                        {partner.closedDeals} closed deals
                      </div>
                      <div className="text-xs text-gray-500 mt-1">
                        Joined: {new Date(partner.joinedDate).toLocaleDateString()}
                      </div>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div>
                      <div className="text-sm font-medium text-gray-900">
                        {formatCurrency(partner.totalEarnings)}
                      </div>
                      <div className="text-sm text-gray-500">
                        Commission: {partner.commissionRate}%
                      </div>
                      {partner.pendingPayments > 0 && (
                        <div className="text-sm text-orange-600 font-medium mt-1">
                          Pending: {formatCurrency(partner.pendingPayments)}
                        </div>
                      )}
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className={`px-2 py-1 text-xs rounded-full ${
                      partner.isActive 
                        ? 'bg-green-100 text-green-700' 
                        : 'bg-red-100 text-red-700'
                    }`}>
                      {partner.isActive ? 'Active' : 'Inactive'}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                    <div className="flex items-center space-x-2">
                      <button 
                        onClick={() => setSelectedPartner(partner)}
                        className="text-blue-600 hover:text-blue-900"
                      >
                        <Eye className="h-4 w-4" />
                      </button>
                      <button className="text-blue-600 hover:text-blue-900">
                        <Edit className="h-4 w-4" />
                      </button>
                      <button className="text-gray-400 hover:text-gray-600">
                        <MoreVertical className="h-4 w-4" />
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {showAddModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
          <div className="bg-white rounded-lg max-w-3xl w-full max-h-[90vh] overflow-y-auto">
            <div className="p-6 border-b border-gray-200">
              <h2 className="text-xl font-semibold text-gray-900">Add New Channel Partner</h2>
            </div>
            <div className="p-6">
              <form className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Company Name</label>
                    <input type="text" className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Registration Number</label>
                    <input type="text" className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" />
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Contact Person</label>
                    <input type="text" className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Email</label>
                    <input type="email" className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" />
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Phone</label>
                    <input type="tel" className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Commission Rate (%)</label>
                    <input type="number" step="0.1" className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" />
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">GST Number</label>
                    <input type="text" className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">PAN Number</label>
                    <input type="text" className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" />
                  </div>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Address</label>
                  <textarea rows={2} className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"></textarea>
                </div>
                <div className="grid grid-cols-3 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">City</label>
                    <input type="text" className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">State</label>
                    <input type="text" className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Pincode</label>
                    <input type="text" className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none" />
                  </div>
                </div>
              </form>
            </div>
            <div className="p-6 border-t border-gray-200 flex justify-end space-x-3">
              <button
                onClick={() => setShowAddModal(false)}
                className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition"
              >
                Cancel
              </button>
              <button className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition">
                Add Partner
              </button>
            </div>
          </div>
        </div>
      )}

      {selectedPartner && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
          <div className="bg-white rounded-lg max-w-4xl w-full max-h-[90vh] overflow-y-auto">
            <div className="p-6 border-b border-gray-200">
              <div className="flex items-center justify-between">
                <h2 className="text-xl font-semibold text-gray-900">{selectedPartner.companyName}</h2>
                <button
                  onClick={() => setSelectedPartner(null)}
                  className="text-gray-400 hover:text-gray-600"
                >
                  <svg className="h-6 w-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>
            </div>
            <div className="p-6">
              <div className="grid grid-cols-3 gap-6 mb-6">
                <div className="bg-gray-50 p-4 rounded-lg">
                  <p className="text-sm text-gray-600">Total Revenue</p>
                  <p className="text-2xl font-bold text-gray-900 mt-1">
                    {formatCurrency(selectedPartner.totalEarnings)}
                  </p>
                </div>
                <div className="bg-gray-50 p-4 rounded-lg">
                  <p className="text-sm text-gray-600">Active Leads</p>
                  <p className="text-2xl font-bold text-gray-900 mt-1">{selectedPartner.activeLeads}</p>
                </div>
                <div className="bg-gray-50 p-4 rounded-lg">
                  <p className="text-sm text-gray-600">Closed Deals</p>
                  <p className="text-2xl font-bold text-gray-900 mt-1">{selectedPartner.closedDeals}</p>
                </div>
              </div>

              <div className="mb-6">
                <h3 className="text-lg font-semibold text-gray-900 mb-4">Performance Overview</h3>
                <div className="bg-gray-50 p-4 rounded-lg">
                  <div className="flex items-end space-x-2 h-32">
                    {performanceData.map((data, index) => (
                      <div key={index} className="flex-1 flex flex-col items-center">
                        <div className="w-full bg-blue-200 rounded-t" style={{height: `${(data.leads / 65) * 100}%`}}></div>
                        <span className="text-xs text-gray-600 mt-2">{data.month}</span>
                      </div>
                    ))}
                  </div>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-6">
                <div>
                  <h3 className="text-lg font-semibold text-gray-900 mb-3">Contact Information</h3>
                  <div className="space-y-2">
                    <div className="flex items-center text-sm">
                      <span className="text-gray-600 w-32">Contact Person:</span>
                      <span className="text-gray-900">{selectedPartner.contactPerson}</span>
                    </div>
                    <div className="flex items-center text-sm">
                      <span className="text-gray-600 w-32">Email:</span>
                      <span className="text-gray-900">{selectedPartner.email}</span>
                    </div>
                    <div className="flex items-center text-sm">
                      <span className="text-gray-600 w-32">Phone:</span>
                      <span className="text-gray-900">{selectedPartner.phone}</span>
                    </div>
                    <div className="flex items-center text-sm">
                      <span className="text-gray-600 w-32">Address:</span>
                      <span className="text-gray-900">{selectedPartner.address}, {selectedPartner.city}</span>
                    </div>
                  </div>
                </div>
                <div>
                  <h3 className="text-lg font-semibold text-gray-900 mb-3">Business Details</h3>
                  <div className="space-y-2">
                    <div className="flex items-center text-sm">
                      <span className="text-gray-600 w-32">Registration No:</span>
                      <span className="text-gray-900">{selectedPartner.registrationNo}</span>
                    </div>
                    <div className="flex items-center text-sm">
                      <span className="text-gray-600 w-32">GST Number:</span>
                      <span className="text-gray-900">{selectedPartner.gstNumber}</span>
                    </div>
                    <div className="flex items-center text-sm">
                      <span className="text-gray-600 w-32">Commission Rate:</span>
                      <span className="text-gray-900">{selectedPartner.commissionRate}%</span>
                    </div>
                    <div className="flex items-center text-sm">
                      <span className="text-gray-600 w-32">Joined Date:</span>
                      <span className="text-gray-900">{new Date(selectedPartner.joinedDate).toLocaleDateString()}</span>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}