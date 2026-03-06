"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Plus, Edit, Eye, Building2, Users, UserCircle, Save, MapPin } from "lucide-react";

interface Study {
  id: string;
  studyCode: string;
  studyName: string;
}

interface Site {
  id: string;
  siteCode: string;
  siteName: string;
  city: string | null;
  country: string | null;
  principalInvestigator: string | null;
  targetEnrollment: number | null;
  status: string;
  isActive: boolean;
  study: {
    studyCode: string;
    studyName: string;
  };
  _count: {
    subjects: number;
    users: number;
  };
}

interface SiteFormData {
  siteCode: string;
  siteName: string;
  address: string;
  city: string;
  state: string;
  country: string;
  postalCode: string;
  phone: string;
  email: string;
  principalInvestigator: string;
  targetEnrollment: number | null;
}

const initialSiteData: SiteFormData = {
  siteCode: "",
  siteName: "",
  address: "",
  city: "",
  state: "",
  country: "",
  postalCode: "",
  phone: "",
  email: "",
  principalInvestigator: "",
  targetEnrollment: null,
};

export default function SitesPage() {
  const [studies, setStudies] = useState<Study[]>([]);
  const [selectedStudy, setSelectedStudy] = useState<string>("");
  const [sites, setSites] = useState<Site[]>([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");

  // Dialog state
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingSite, setEditingSite] = useState<Site | null>(null);
  const [siteData, setSiteData] = useState<SiteFormData>(initialSiteData);

  useEffect(() => {
    fetchStudies();
  }, []);

  useEffect(() => {
    if (selectedStudy) {
      fetchSites(selectedStudy);
    }
  }, [selectedStudy]);

  const fetchStudies = async () => {
    try {
      const response = await fetch("/api/studies");
      if (response.ok) {
        const data = await response.json();
        setStudies(data);
        if (data.length > 0) {
          setSelectedStudy(data[0].id);
        }
      }
    } catch (error) {
      console.error("Failed to fetch studies:", error);
    }
  };

  const fetchSites = async (studyId: string) => {
    setLoading(true);
    try {
      const response = await fetch(`/api/sites?studyId=${studyId}`);
      if (response.ok) {
        const data = await response.json();
        setSites(data);
      }
    } catch (error) {
      console.error("Failed to fetch sites:", error);
    } finally {
      setLoading(false);
    }
  };

  const handleOpenAddDialog = () => {
    setEditingSite(null);
    setSiteData(initialSiteData);
    setDialogOpen(true);
  };

  const handleOpenEditDialog = (site: Site) => {
    setEditingSite(site);
    setSiteData({
      siteCode: site.siteCode,
      siteName: site.siteName,
      address: "",
      city: site.city || "",
      state: "",
      country: site.country || "",
      postalCode: "",
      phone: "",
      email: "",
      principalInvestigator: site.principalInvestigator || "",
      targetEnrollment: site.targetEnrollment,
    });
    setDialogOpen(true);
  };

  const handleSiteChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value, type } = e.target;
    setSiteData((prev) => ({
      ...prev,
      [name]: type === "number" ? (value ? parseInt(value) : null) : value,
    }));
  };

  const handleSaveSite = async () => {
    setError("");
    setSaving(true);

    try {
      const payload = {
        ...siteData,
        studyId: selectedStudy,
      };

      let response;
      if (editingSite) {
        response = await fetch(`/api/sites/${editingSite.id}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
      } else {
        response = await fetch("/api/sites", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
      }

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || "Failed to save site");
      }

      await fetchSites(selectedStudy);
      setDialogOpen(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to save site");
    } finally {
      setSaving(false);
    }
  };

  const getStatusBadge = (status: string) => {
    const variants: Record<string, "success" | "secondary" | "destructive" | "outline"> = {
      Active: "success",
      "On Hold": "secondary",
      Closed: "destructive",
    };
    return <Badge variant={variants[status] || "outline"}>{status}</Badge>;
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Site Management</h2>
          <p className="text-gray-500">Manage clinical trial sites</p>
        </div>
        <Button onClick={handleOpenAddDialog} disabled={!selectedStudy}>
          <Plus className="h-4 w-4 mr-2" />
          Add Site
        </Button>
      </div>

      {error && (
        <div className="p-3 text-sm text-red-600 bg-red-50 border border-red-200 rounded-md">
          {error}
        </div>
      )}

      {/* Study Selector */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Select Study</CardTitle>
        </CardHeader>
        <CardContent>
          <Select value={selectedStudy} onValueChange={setSelectedStudy}>
            <SelectTrigger className="w-full md:w-96">
              <SelectValue placeholder="Select a study" />
            </SelectTrigger>
            <SelectContent>
              {studies.map((study) => (
                <SelectItem key={study.id} value={study.id}>
                  {study.studyCode} - {study.studyName}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </CardContent>
      </Card>

      {/* Sites List */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center">
            <Building2 className="h-5 w-5 mr-2" />
            Sites
          </CardTitle>
        </CardHeader>
        <CardContent>
          {!selectedStudy ? (
            <div className="text-center py-8 text-gray-500">
              Please select a study to view sites
            </div>
          ) : loading ? (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
            </div>
          ) : sites.length === 0 ? (
            <div className="text-center py-8">
              <p className="text-gray-500 mb-4">No sites defined for this study</p>
              <Button onClick={handleOpenAddDialog}>
                <Plus className="h-4 w-4 mr-2" />
                Add First Site
              </Button>
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Site Code</TableHead>
                  <TableHead>Site Name</TableHead>
                  <TableHead>Location</TableHead>
                  <TableHead>PI</TableHead>
                  <TableHead>Enrollment</TableHead>
                  <TableHead>Users</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {sites.map((site) => (
                  <TableRow key={site.id}>
                    <TableCell className="font-medium">{site.siteCode}</TableCell>
                    <TableCell>{site.siteName}</TableCell>
                    <TableCell>
                      <div className="flex items-center text-sm text-gray-500">
                        <MapPin className="h-3 w-3 mr-1" />
                        {[site.city, site.country].filter(Boolean).join(", ") || "-"}
                      </div>
                    </TableCell>
                    <TableCell>{site.principalInvestigator || "-"}</TableCell>
                    <TableCell>
                      <div className="flex items-center">
                        <UserCircle className="h-4 w-4 mr-1 text-gray-400" />
                        {site._count.subjects}/{site.targetEnrollment || "∞"}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center">
                        <Users className="h-4 w-4 mr-1 text-gray-400" />
                        {site._count.users}
                      </div>
                    </TableCell>
                    <TableCell>{getStatusBadge(site.status)}</TableCell>
                    <TableCell className="text-right">
                      <div className="flex justify-end space-x-1">
                        <Link href={`/sites/${site.id}`}>
                          <Button variant="ghost" size="icon">
                            <Eye className="h-4 w-4" />
                          </Button>
                        </Link>
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => handleOpenEditDialog(site)}
                        >
                          <Edit className="h-4 w-4" />
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {/* Add/Edit Site Dialog */}
      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>
              {editingSite ? "Edit Site" : "Add New Site"}
            </DialogTitle>
          </DialogHeader>

          <div className="grid gap-4 py-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="siteCode">Site Code *</Label>
                <Input
                  id="siteCode"
                  name="siteCode"
                  value={siteData.siteCode}
                  onChange={handleSiteChange}
                  placeholder="e.g., SITE001"
                  maxLength={20}
                  disabled={!!editingSite}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="siteName">Site Name *</Label>
                <Input
                  id="siteName"
                  name="siteName"
                  value={siteData.siteName}
                  onChange={handleSiteChange}
                  placeholder="e.g., City General Hospital"
                />
              </div>
            </div>

            <div className="space-y-2">
              <Label htmlFor="address">Address</Label>
              <Input
                id="address"
                name="address"
                value={siteData.address}
                onChange={handleSiteChange}
                placeholder="Street address"
              />
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="city">City</Label>
                <Input
                  id="city"
                  name="city"
                  value={siteData.city}
                  onChange={handleSiteChange}
                  placeholder="City"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="state">State/Province</Label>
                <Input
                  id="state"
                  name="state"
                  value={siteData.state}
                  onChange={handleSiteChange}
                  placeholder="State or Province"
                />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="country">Country</Label>
                <Input
                  id="country"
                  name="country"
                  value={siteData.country}
                  onChange={handleSiteChange}
                  placeholder="Country"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="postalCode">Postal Code</Label>
                <Input
                  id="postalCode"
                  name="postalCode"
                  value={siteData.postalCode}
                  onChange={handleSiteChange}
                  placeholder="Postal/ZIP code"
                />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="phone">Phone</Label>
                <Input
                  id="phone"
                  name="phone"
                  value={siteData.phone}
                  onChange={handleSiteChange}
                  placeholder="+1 (555) 123-4567"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="email">Email</Label>
                <Input
                  id="email"
                  name="email"
                  type="email"
                  value={siteData.email}
                  onChange={handleSiteChange}
                  placeholder="site@hospital.com"
                />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="principalInvestigator">Principal Investigator</Label>
                <Input
                  id="principalInvestigator"
                  name="principalInvestigator"
                  value={siteData.principalInvestigator}
                  onChange={handleSiteChange}
                  placeholder="Dr. John Smith"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="targetEnrollment">Target Enrollment</Label>
                <Input
                  id="targetEnrollment"
                  name="targetEnrollment"
                  type="number"
                  value={siteData.targetEnrollment ?? ""}
                  onChange={handleSiteChange}
                  placeholder="e.g., 50"
                  min={0}
                />
              </div>
            </div>
          </div>

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setDialogOpen(false)}
              disabled={saving}
            >
              Cancel
            </Button>
            <Button onClick={handleSaveSite} disabled={saving}>
              <Save className="h-4 w-4 mr-2" />
              {saving ? "Saving..." : "Save Site"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
