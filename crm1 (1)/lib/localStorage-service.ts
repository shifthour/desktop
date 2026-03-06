// Local Storage Service for temporary data persistence
// This service provides CRUD operations for all CRM modules using browser localStorage

type StorageKey = 
  | 'accounts' 
  | 'contacts' 
  | 'leads' 
  | 'opportunities' 
  | 'products' 
  | 'quotations' 
  | 'salesOrders' 
  | 'invoices'
  | 'followUps'
  | 'activities'

class LocalStorageService {
  // Generic CRUD operations
  private getItems<T>(key: StorageKey): T[] {
    if (typeof window === 'undefined') return []
    try {
      const data = localStorage.getItem(key)
      return data ? JSON.parse(data) : []
    } catch (error) {
      console.error(`Error reading ${key} from localStorage:`, error)
      return []
    }
  }

  private setItems<T>(key: StorageKey, items: T[]): void {
    if (typeof window === 'undefined') return
    try {
      localStorage.setItem(key, JSON.stringify(items))
    } catch (error) {
      console.error(`Error saving ${key} to localStorage:`, error)
    }
  }

  private generateId(): string {
    return `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`
  }

  // Create operation
  create<T extends { id?: string | number }>(key: StorageKey, item: T): T {
    const items = this.getItems<T>(key)
    const newItem = {
      ...item,
      id: item.id || this.generateId(),
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    }
    items.push(newItem)
    this.setItems(key, items)
    return newItem
  }

  // Read operations
  getAll<T>(key: StorageKey): T[] {
    return this.getItems<T>(key)
  }

  getById<T extends { id?: string | number }>(key: StorageKey, id: string | number): T | null {
    const items = this.getItems<T>(key)
    return items.find(item => item.id === id) || null
  }

  // Update operation
  update<T extends { id?: string | number }>(key: StorageKey, id: string | number, updates: Partial<T>): T | null {
    const items = this.getItems<T>(key)
    const index = items.findIndex(item => item.id === id)
    
    if (index === -1) return null
    
    items[index] = {
      ...items[index],
      ...updates,
      updatedAt: new Date().toISOString()
    }
    
    this.setItems(key, items)
    return items[index]
  }

  // Delete operation
  delete<T extends { id?: string | number }>(key: StorageKey, id: string | number): boolean {
    const items = this.getItems<T>(key)
    const filteredItems = items.filter(item => item.id !== id)
    
    if (items.length === filteredItems.length) return false
    
    this.setItems(key, filteredItems)
    return true
  }

  // Bulk operations
  createMany<T extends { id?: string | number }>(key: StorageKey, items: T[]): T[] {
    const existingItems = this.getItems<T>(key)
    const newItems = items.map(item => ({
      ...item,
      id: item.id || this.generateId(),
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    }))
    
    this.setItems(key, [...existingItems, ...newItems])
    return newItems
  }

  deleteAll(key: StorageKey): void {
    if (typeof window === 'undefined') return
    localStorage.removeItem(key)
  }

  // Search and filter operations
  search<T>(key: StorageKey, searchTerm: string, fields: string[]): T[] {
    const items = this.getItems<T>(key)
    const lowerSearchTerm = searchTerm.toLowerCase()
    
    return items.filter(item => {
      return fields.some(field => {
        const value = (item as any)[field]
        if (value) {
          return value.toString().toLowerCase().includes(lowerSearchTerm)
        }
        return false
      })
    })
  }

  filter<T>(key: StorageKey, filters: Record<string, any>): T[] {
    const items = this.getItems<T>(key)
    
    return items.filter(item => {
      return Object.entries(filters).every(([filterKey, filterValue]) => {
        if (!filterValue || filterValue === 'All') return true
        return (item as any)[filterKey] === filterValue
      })
    })
  }

  // Statistics operations
  getCount(key: StorageKey): number {
    return this.getItems(key).length
  }

  getStats(key: StorageKey): {
    total: number
    todayCount: number
    thisWeekCount: number
    thisMonthCount: number
  } {
    const items = this.getItems<any>(key)
    const today = new Date()
    today.setHours(0, 0, 0, 0)
    
    const weekAgo = new Date()
    weekAgo.setDate(weekAgo.getDate() - 7)
    
    const monthAgo = new Date()
    monthAgo.setMonth(monthAgo.getMonth() - 1)
    
    return {
      total: items.length,
      todayCount: items.filter(item => new Date(item.createdAt) >= today).length,
      thisWeekCount: items.filter(item => new Date(item.createdAt) >= weekAgo).length,
      thisMonthCount: items.filter(item => new Date(item.createdAt) >= monthAgo).length
    }
  }

  // Initialize sample data
  initializeSampleData(): void {
    // Only initialize if no data exists
    if (this.getCount('accounts') > 0) return

    // Sample Accounts
    const sampleAccounts = [
      {
        id: this.generateId(),
        accountName: "TSAR Labcare",
        industry: "Healthcare",
        contactName: "Mr. Mahesh",
        contactNo: "+91 98765 43210",
        email: "mahesh@tsarlabcare.com",
        website: "www.tsarlabcare.com",
        city: "Bangalore",
        state: "Karnataka",
        region: "South",
        assignedTo: "Pauline",
        status: "Active",
        turnover: "₹50L",
        employees: "50-100"
      },
      {
        id: this.generateId(),
        accountName: "Eurofins Advinus",
        industry: "Research",
        contactName: "Dr. Research Head",
        contactNo: "+91 98765 43211",
        email: "research@eurofins.com",
        website: "www.eurofins.com",
        city: "Chennai",
        state: "Tamil Nadu",
        region: "South",
        assignedTo: "Hari Kumar K",
        status: "Active",
        turnover: "₹1Cr+",
        employees: "100-500"
      }
    ]
    this.createMany('accounts', sampleAccounts)

    // Sample Leads
    const sampleLeads = [
      {
        id: this.generateId(),
        leadName: "Bio-Tech Solutions",
        contactName: "Dr. Sharma",
        email: "sharma@biotech.com",
        phone: "+91 98765 43212",
        mobile: "+91 98765 43212",
        whatsapp: "+91 98765 43212",
        industry: "Biotechnology",
        leadSource: "Website",
        salesStage: "Qualification",
        priority: "High",
        estimatedValue: "₹15L",
        closingDate: "2025-09-30",
        leadOwner: "Pauline",
        city: "Mumbai",
        state: "Maharashtra",
        country: "India",
        website: "www.biotech-solutions.com",
        notes: "Interested in PCR equipment"
      }
    ]
    this.createMany('leads', sampleLeads)

    // Sample Products
    const sampleProducts = [
      {
        id: this.generateId(),
        productName: "TRICOLOR MULTICHANNEL FIBRINOMETER",
        branch: "Medical Equipment",
        category: "Diagnostic Equipment",
        principal: "Affinite Instrument",
        refNo: "P4PRO-AFFI",
        price: "₹8,50,000",
        status: "Active",
        assignedTo: "Hari Kumar K",
        description: "Advanced fibrinometer for clinical diagnostics",
        specifications: "Multi-channel, automated testing",
        warranty: "2 years",
        supplier: "Affinite Instrument",
        model: "P4PRO"
      }
    ]
    this.createMany('products', sampleProducts)
  }

  // Clear all data
  clearAllData(): void {
    const keys: StorageKey[] = [
      'accounts', 'contacts', 'leads', 'opportunities',
      'products', 'quotations', 'salesOrders', 'invoices',
      'followUps', 'activities'
    ]
    keys.forEach(key => this.deleteAll(key))
  }

  // Export data
  exportData(): Record<StorageKey, any[]> {
    const keys: StorageKey[] = [
      'accounts', 'contacts', 'leads', 'opportunities',
      'products', 'quotations', 'salesOrders', 'invoices',
      'followUps', 'activities'
    ]
    
    const data: Record<string, any[]> = {}
    keys.forEach(key => {
      data[key] = this.getAll(key)
    })
    
    return data as Record<StorageKey, any[]>
  }

  // Import data
  importData(data: Partial<Record<StorageKey, any[]>>): void {
    Object.entries(data).forEach(([key, items]) => {
      if (items && Array.isArray(items)) {
        this.setItems(key as StorageKey, items)
      }
    })
  }
}

// Create singleton instance
const storageService = new LocalStorageService()

// Initialize sample data on first load
if (typeof window !== 'undefined') {
  storageService.initializeSampleData()
}

export default storageService
export type { StorageKey }