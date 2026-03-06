'use client'

import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog'
import { Plus } from 'lucide-react'
import { toast } from 'sonner'

interface Project {
  id: string
  name: string
}

interface AddLeadDialogProps {
  projects: Project[]
  onLeadAdded: () => void
}

export function AddLeadDialog({ projects, onLeadAdded }: AddLeadDialogProps) {
  const [open, setOpen] = useState(false)
  const [loading, setLoading] = useState(false)
  const [formData, setFormData] = useState({
    name: '',
    phone: '',
    email: '',
    source: '',
    campaign: '',
    bedroomsPref: '',
    budgetMin: '',
    budgetMax: '',
    locationPref: '',
    projectId: ''
  })

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    
    if (!formData.phone || !formData.source) {
      toast.error('Phone number and source are required')
      return
    }

    setLoading(true)
    try {
      const payload = {
        ...formData,
        bedroomsPref: formData.bedroomsPref ? parseInt(formData.bedroomsPref) : undefined,
        budgetMin: formData.budgetMin ? parseInt(formData.budgetMin) * 100000 : undefined, // Convert to paisa
        budgetMax: formData.budgetMax ? parseInt(formData.budgetMax) * 100000 : undefined,
        email: formData.email || undefined,
        campaign: formData.campaign || undefined,
        locationPref: formData.locationPref || undefined,
        projectId: formData.projectId || undefined
      }

      const response = await fetch('/api/leads', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      })

      if (response.ok) {
        const result = await response.json()
        toast.success(`Lead created successfully! Score: ${result.score}`)
        setFormData({
          name: '',
          phone: '',
          email: '',
          source: '',
          campaign: '',
          bedroomsPref: '',
          budgetMin: '',
          budgetMax: '',
          locationPref: '',
          projectId: ''
        })
        setOpen(false)
        onLeadAdded()
      } else {
        const error = await response.json()
        toast.error(error.message || 'Failed to create lead')
      }
    } catch (error) {
      toast.error('An error occurred')
    } finally {
      setLoading(false)
    }
  }

  const handleInputChange = (field: string, value: string) => {
    setFormData(prev => ({ ...prev, [field]: value }))
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button>
          <Plus className="w-4 h-4 mr-2" />
          Add Lead
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Add New Lead</DialogTitle>
          <DialogDescription>
            Create a new lead entry with contact details and preferences
          </DialogDescription>
        </DialogHeader>
        
        <form onSubmit={handleSubmit} className="space-y-4">
          {/* Contact Information */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <Label htmlFor="name">Name</Label>
              <Input
                id="name"
                value={formData.name}
                onChange={(e) => handleInputChange('name', e.target.value)}
                placeholder="Lead's full name"
              />
            </div>
            <div>
              <Label htmlFor="phone">Phone *</Label>
              <Input
                id="phone"
                value={formData.phone}
                onChange={(e) => handleInputChange('phone', e.target.value)}
                placeholder="+91 9876543210"
                required
              />
            </div>
            <div>
              <Label htmlFor="email">Email</Label>
              <Input
                id="email"
                type="email"
                value={formData.email}
                onChange={(e) => handleInputChange('email', e.target.value)}
                placeholder="lead@example.com"
              />
            </div>
            <div>
              <Label htmlFor="source">Source *</Label>
              <Select value={formData.source} onValueChange={(value) => handleInputChange('source', value)}>
                <SelectTrigger>
                  <SelectValue placeholder="Select source" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="google_ads">Google Ads</SelectItem>
                  <SelectItem value="meta">Meta/Facebook</SelectItem>
                  <SelectItem value="website">Website</SelectItem>
                  <SelectItem value="csv">CSV Import</SelectItem>
                  <SelectItem value="referral">Referral</SelectItem>
                  <SelectItem value="walk_in">Walk-in</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>

          <div>
            <Label htmlFor="campaign">Campaign</Label>
            <Input
              id="campaign"
              value={formData.campaign}
              onChange={(e) => handleInputChange('campaign', e.target.value)}
              placeholder="Campaign name (optional)"
            />
          </div>

          {/* Property Preferences */}
          <div className="border-t pt-4">
            <h4 className="font-medium mb-3">Property Preferences</h4>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <Label htmlFor="bedroomsPref">Bedrooms</Label>
                <Select value={formData.bedroomsPref} onValueChange={(value) => handleInputChange('bedroomsPref', value)}>
                  <SelectTrigger>
                    <SelectValue placeholder="BHK" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="1">1 BHK</SelectItem>
                    <SelectItem value="2">2 BHK</SelectItem>
                    <SelectItem value="3">3 BHK</SelectItem>
                    <SelectItem value="4">4 BHK</SelectItem>
                    <SelectItem value="5">5+ BHK</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div>
                <Label htmlFor="budgetMin">Budget Min (Lakhs)</Label>
                <Input
                  id="budgetMin"
                  type="number"
                  value={formData.budgetMin}
                  onChange={(e) => handleInputChange('budgetMin', e.target.value)}
                  placeholder="50"
                />
              </div>
              <div>
                <Label htmlFor="budgetMax">Budget Max (Lakhs)</Label>
                <Input
                  id="budgetMax"
                  type="number"
                  value={formData.budgetMax}
                  onChange={(e) => handleInputChange('budgetMax', e.target.value)}
                  placeholder="120"
                />
              </div>
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <Label htmlFor="locationPref">Location Preference</Label>
              <Input
                id="locationPref"
                value={formData.locationPref}
                onChange={(e) => handleInputChange('locationPref', e.target.value)}
                placeholder="Whitefield, Bangalore"
              />
            </div>
            <div>
              <Label htmlFor="projectId">Interested Project</Label>
              <Select value={formData.projectId} onValueChange={(value) => handleInputChange('projectId', value)}>
                <SelectTrigger>
                  <SelectValue placeholder="Select project" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="">No specific project</SelectItem>
                  {projects.map((project) => (
                    <SelectItem key={project.id} value={project.id}>
                      {project.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          <div className="flex justify-end space-x-3 pt-4">
            <Button
              type="button"
              variant="outline"
              onClick={() => setOpen(false)}
              disabled={loading}
            >
              Cancel
            </Button>
            <Button type="submit" disabled={loading}>
              {loading ? 'Creating...' : 'Create Lead'}
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  )
}