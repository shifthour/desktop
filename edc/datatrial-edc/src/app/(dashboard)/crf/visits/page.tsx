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
import { Checkbox } from "@/components/ui/checkbox";
import { Plus, Edit, Trash2, Calendar, FileText, Save } from "lucide-react";

// Visit types as per FSD
const VISIT_TYPES = [
  "First Visit",
  "Visit",
  "Common Forms",
  "Unscheduled",
  "Screening",
  "Randomization",
  "Treatment",
  "Follow-up",
  "End of Study",
];

interface Study {
  id: string;
  studyCode: string;
  studyName: string;
}

interface CRFForm {
  id: string;
  formCode: string;
  formName: string;
}

interface Visit {
  id: string;
  visitCode: string;
  visitName: string;
  visitType: string;
  sequenceNumber: number;
  durationDays: number;
  windowBefore: number;
  windowAfter: number;
  isSchedulable: boolean;
  isActive: boolean;
  visitForms: Array<{
    form: {
      id: string;
      formCode: string;
      formName: string;
    };
  }>;
  _count: {
    subjectVisits: number;
  };
}

interface VisitFormData {
  visitCode: string;
  visitName: string;
  visitType: string;
  durationDays: number;
  windowBefore: number;
  windowAfter: number;
  isSchedulable: boolean;
  formIds: string[];
}

const initialVisitData: VisitFormData = {
  visitCode: "",
  visitName: "",
  visitType: "Visit",
  durationDays: 0,
  windowBefore: 0,
  windowAfter: 0,
  isSchedulable: true,
  formIds: [],
};

export default function VisitsPage() {
  const [studies, setStudies] = useState<Study[]>([]);
  const [selectedStudy, setSelectedStudy] = useState<string>("");
  const [visits, setVisits] = useState<Visit[]>([]);
  const [forms, setForms] = useState<CRFForm[]>([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");

  // Dialog state
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingVisit, setEditingVisit] = useState<Visit | null>(null);
  const [visitData, setVisitData] = useState<VisitFormData>(initialVisitData);

  useEffect(() => {
    fetchStudies();
  }, []);

  useEffect(() => {
    if (selectedStudy) {
      fetchVisits(selectedStudy);
      fetchForms(selectedStudy);
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

  const fetchVisits = async (studyId: string) => {
    setLoading(true);
    try {
      const response = await fetch(`/api/crf/visits?studyId=${studyId}`);
      if (response.ok) {
        const data = await response.json();
        setVisits(data);
      }
    } catch (error) {
      console.error("Failed to fetch visits:", error);
    } finally {
      setLoading(false);
    }
  };

  const fetchForms = async (studyId: string) => {
    try {
      const response = await fetch(`/api/crf/forms?studyId=${studyId}`);
      if (response.ok) {
        const data = await response.json();
        setForms(data);
      }
    } catch (error) {
      console.error("Failed to fetch forms:", error);
    }
  };

  const handleOpenAddDialog = () => {
    setEditingVisit(null);
    setVisitData(initialVisitData);
    setDialogOpen(true);
  };

  const handleOpenEditDialog = (visit: Visit) => {
    setEditingVisit(visit);
    setVisitData({
      visitCode: visit.visitCode,
      visitName: visit.visitName,
      visitType: visit.visitType,
      durationDays: visit.durationDays,
      windowBefore: visit.windowBefore,
      windowAfter: visit.windowAfter,
      isSchedulable: visit.isSchedulable,
      formIds: visit.visitForms.map((vf) => vf.form.id),
    });
    setDialogOpen(true);
  };

  const handleVisitChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value, type } = e.target;
    setVisitData((prev) => ({
      ...prev,
      [name]: type === "number" ? parseInt(value) || 0 : value,
    }));
  };

  const handleFormToggle = (formId: string) => {
    setVisitData((prev) => ({
      ...prev,
      formIds: prev.formIds.includes(formId)
        ? prev.formIds.filter((id) => id !== formId)
        : [...prev.formIds, formId],
    }));
  };

  const handleSaveVisit = async () => {
    setError("");
    setSaving(true);

    try {
      const payload = {
        ...visitData,
        studyId: selectedStudy,
      };

      let response;
      if (editingVisit) {
        response = await fetch(`/api/crf/visits/${editingVisit.id}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
      } else {
        response = await fetch("/api/crf/visits", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
      }

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || "Failed to save visit");
      }

      await fetchVisits(selectedStudy);
      setDialogOpen(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to save visit");
    } finally {
      setSaving(false);
    }
  };

  const handleDeleteVisit = async (visitId: string) => {
    if (!confirm("Are you sure you want to delete this visit?")) {
      return;
    }

    try {
      const response = await fetch(`/api/crf/visits/${visitId}`, {
        method: "DELETE",
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || "Failed to delete visit");
      }

      await fetchVisits(selectedStudy);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to delete visit");
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Visit Schedule</h2>
          <p className="text-gray-500">
            Define study visits and assign CRF forms
          </p>
        </div>
        <Button onClick={handleOpenAddDialog} disabled={!selectedStudy}>
          <Plus className="h-4 w-4 mr-2" />
          Add Visit
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

      {/* Visits List */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center">
            <Calendar className="h-5 w-5 mr-2" />
            Study Visits
          </CardTitle>
        </CardHeader>
        <CardContent>
          {!selectedStudy ? (
            <div className="text-center py-8 text-gray-500">
              Please select a study to view visits
            </div>
          ) : loading ? (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
            </div>
          ) : visits.length === 0 ? (
            <div className="text-center py-8">
              <p className="text-gray-500 mb-4">
                No visits defined for this study
              </p>
              <Button onClick={handleOpenAddDialog}>
                <Plus className="h-4 w-4 mr-2" />
                Add First Visit
              </Button>
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Seq</TableHead>
                  <TableHead>Visit Code</TableHead>
                  <TableHead>Visit Name</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Window</TableHead>
                  <TableHead>Forms</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {visits.map((visit) => (
                  <TableRow key={visit.id}>
                    <TableCell>{visit.sequenceNumber}</TableCell>
                    <TableCell className="font-medium">
                      {visit.visitCode}
                    </TableCell>
                    <TableCell>{visit.visitName}</TableCell>
                    <TableCell>
                      <Badge variant="outline">{visit.visitType}</Badge>
                    </TableCell>
                    <TableCell>
                      {visit.isSchedulable ? (
                        <span className="text-xs text-gray-500">
                          Day {visit.durationDays} (-{visit.windowBefore}/+
                          {visit.windowAfter})
                        </span>
                      ) : (
                        <span className="text-xs text-gray-400">
                          Unscheduled
                        </span>
                      )}
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center">
                        <FileText className="h-4 w-4 mr-1 text-gray-400" />
                        {visit.visitForms.length}
                      </div>
                    </TableCell>
                    <TableCell>
                      <Badge variant={visit.isActive ? "success" : "secondary"}>
                        {visit.isActive ? "Active" : "Inactive"}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-right">
                      <div className="flex justify-end space-x-1">
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => handleOpenEditDialog(visit)}
                        >
                          <Edit className="h-4 w-4" />
                        </Button>
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => handleDeleteVisit(visit.id)}
                          disabled={visit._count.subjectVisits > 0}
                        >
                          <Trash2 className="h-4 w-4 text-red-500" />
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

      {/* Add/Edit Visit Dialog */}
      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>
              {editingVisit ? "Edit Visit" : "Add New Visit"}
            </DialogTitle>
          </DialogHeader>

          <div className="grid gap-4 py-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="visitCode">Visit Code *</Label>
                <Input
                  id="visitCode"
                  name="visitCode"
                  value={visitData.visitCode}
                  onChange={handleVisitChange}
                  placeholder="e.g., V1, SCR, EOS"
                  maxLength={20}
                  disabled={!!editingVisit}
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="visitName">Visit Name *</Label>
                <Input
                  id="visitName"
                  name="visitName"
                  value={visitData.visitName}
                  onChange={handleVisitChange}
                  placeholder="e.g., Screening Visit, Week 4"
                />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="visitType">Visit Type *</Label>
                <Select
                  value={visitData.visitType}
                  onValueChange={(value) =>
                    setVisitData((prev) => ({ ...prev, visitType: value }))
                  }
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {VISIT_TYPES.map((type) => (
                      <SelectItem key={type} value={type}>
                        {type}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="flex items-center space-x-2 pt-8">
                <Checkbox
                  id="isSchedulable"
                  checked={visitData.isSchedulable}
                  onCheckedChange={(checked) =>
                    setVisitData((prev) => ({
                      ...prev,
                      isSchedulable: checked === true,
                    }))
                  }
                />
                <Label htmlFor="isSchedulable" className="font-normal">
                  Scheduled Visit
                </Label>
              </div>
            </div>

            {visitData.isSchedulable && (
              <div className="grid grid-cols-3 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="durationDays">Study Day</Label>
                  <Input
                    id="durationDays"
                    name="durationDays"
                    type="number"
                    value={visitData.durationDays}
                    onChange={handleVisitChange}
                    min={0}
                  />
                  <p className="text-xs text-gray-500">
                    Days from first visit
                  </p>
                </div>

                <div className="space-y-2">
                  <Label htmlFor="windowBefore">Window Before (days)</Label>
                  <Input
                    id="windowBefore"
                    name="windowBefore"
                    type="number"
                    value={visitData.windowBefore}
                    onChange={handleVisitChange}
                    min={0}
                  />
                </div>

                <div className="space-y-2">
                  <Label htmlFor="windowAfter">Window After (days)</Label>
                  <Input
                    id="windowAfter"
                    name="windowAfter"
                    type="number"
                    value={visitData.windowAfter}
                    onChange={handleVisitChange}
                    min={0}
                  />
                </div>
              </div>
            )}

            {/* Form Assignment */}
            <div className="space-y-2">
              <Label>Assign CRF Forms</Label>
              <div className="border rounded-md p-3 max-h-48 overflow-y-auto">
                {forms.length === 0 ? (
                  <p className="text-sm text-gray-500">
                    No forms available. Create forms first.
                  </p>
                ) : (
                  <div className="space-y-2">
                    {forms.map((form) => (
                      <div
                        key={form.id}
                        className="flex items-center space-x-2"
                      >
                        <Checkbox
                          id={`form-${form.id}`}
                          checked={visitData.formIds.includes(form.id)}
                          onCheckedChange={() => handleFormToggle(form.id)}
                        />
                        <Label
                          htmlFor={`form-${form.id}`}
                          className="font-normal"
                        >
                          {form.formCode} - {form.formName}
                        </Label>
                      </div>
                    ))}
                  </div>
                )}
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
            <Button onClick={handleSaveVisit} disabled={saving}>
              <Save className="h-4 w-4 mr-2" />
              {saving ? "Saving..." : "Save Visit"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
