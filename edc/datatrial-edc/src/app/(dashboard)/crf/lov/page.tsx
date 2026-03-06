"use client";

import { useEffect, useState } from "react";
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
import { Plus, Edit, Trash2, List, Save, X } from "lucide-react";

interface Study {
  id: string;
  studyCode: string;
  studyName: string;
}

interface LOVItem {
  id?: string;
  itemCode: string;
  itemValue: string;
  sequenceNumber?: number;
  isActive?: boolean;
}

interface LOVList {
  id: string;
  listCode: string;
  listName: string;
  isActive: boolean;
  items: LOVItem[];
  _count: {
    fields: number;
  };
}

interface LOVFormData {
  listCode: string;
  listName: string;
  items: LOVItem[];
}

const initialLOVData: LOVFormData = {
  listCode: "",
  listName: "",
  items: [{ itemCode: "", itemValue: "" }],
};

export default function LOVPage() {
  const [studies, setStudies] = useState<Study[]>([]);
  const [selectedStudy, setSelectedStudy] = useState<string>("");
  const [lovLists, setLovLists] = useState<LOVList[]>([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");

  // Dialog state
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingLOV, setEditingLOV] = useState<LOVList | null>(null);
  const [lovData, setLOVData] = useState<LOVFormData>(initialLOVData);

  useEffect(() => {
    fetchStudies();
  }, []);

  useEffect(() => {
    if (selectedStudy) {
      fetchLOVLists(selectedStudy);
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

  const fetchLOVLists = async (studyId: string) => {
    setLoading(true);
    try {
      const response = await fetch(`/api/crf/lov?studyId=${studyId}`);
      if (response.ok) {
        const data = await response.json();
        setLovLists(data);
      }
    } catch (error) {
      console.error("Failed to fetch LOV lists:", error);
    } finally {
      setLoading(false);
    }
  };

  const handleOpenAddDialog = () => {
    setEditingLOV(null);
    setLOVData(initialLOVData);
    setDialogOpen(true);
  };

  const handleOpenEditDialog = (lov: LOVList) => {
    setEditingLOV(lov);
    setLOVData({
      listCode: lov.listCode,
      listName: lov.listName,
      items: lov.items.length > 0 ? lov.items : [{ itemCode: "", itemValue: "" }],
    });
    setDialogOpen(true);
  };

  const handleLOVChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value } = e.target;
    setLOVData((prev) => ({ ...prev, [name]: value }));
  };

  const handleItemChange = (
    index: number,
    field: "itemCode" | "itemValue",
    value: string
  ) => {
    setLOVData((prev) => {
      const newItems = [...prev.items];
      newItems[index] = { ...newItems[index], [field]: value };
      return { ...prev, items: newItems };
    });
  };

  const handleAddItem = () => {
    setLOVData((prev) => ({
      ...prev,
      items: [...prev.items, { itemCode: "", itemValue: "" }],
    }));
  };

  const handleRemoveItem = (index: number) => {
    if (lovData.items.length <= 1) return;
    setLOVData((prev) => ({
      ...prev,
      items: prev.items.filter((_, i) => i !== index),
    }));
  };

  const handleSaveLOV = async () => {
    setError("");
    setSaving(true);

    // Validate items
    const validItems = lovData.items.filter(
      (item) => item.itemCode.trim() && item.itemValue.trim()
    );

    if (validItems.length === 0) {
      setError("At least one item with code and value is required");
      setSaving(false);
      return;
    }

    try {
      const payload = {
        studyId: selectedStudy,
        listCode: lovData.listCode,
        listName: lovData.listName,
        items: validItems.map((item, index) => ({
          ...item,
          id: item.id,
          itemCode: item.itemCode.trim(),
          itemValue: item.itemValue.trim(),
        })),
      };

      let response;
      if (editingLOV) {
        response = await fetch(`/api/crf/lov/${editingLOV.id}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
      } else {
        response = await fetch("/api/crf/lov", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
      }

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || "Failed to save LOV list");
      }

      await fetchLOVLists(selectedStudy);
      setDialogOpen(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to save LOV list");
    } finally {
      setSaving(false);
    }
  };

  const handleDeleteLOV = async (lovId: string) => {
    if (!confirm("Are you sure you want to delete this LOV list?")) {
      return;
    }

    try {
      const response = await fetch(`/api/crf/lov/${lovId}`, {
        method: "DELETE",
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || "Failed to delete LOV list");
      }

      await fetchLOVLists(selectedStudy);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Failed to delete LOV list"
      );
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">
            List of Values (LOV)
          </h2>
          <p className="text-gray-500">
            Manage dropdown lists for CRF fields
          </p>
        </div>
        <Button onClick={handleOpenAddDialog} disabled={!selectedStudy}>
          <Plus className="h-4 w-4 mr-2" />
          Add LOV List
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

      {/* LOV Lists */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center">
            <List className="h-5 w-5 mr-2" />
            LOV Lists
          </CardTitle>
        </CardHeader>
        <CardContent>
          {!selectedStudy ? (
            <div className="text-center py-8 text-gray-500">
              Please select a study to view LOV lists
            </div>
          ) : loading ? (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
            </div>
          ) : lovLists.length === 0 ? (
            <div className="text-center py-8">
              <p className="text-gray-500 mb-4">
                No LOV lists defined for this study
              </p>
              <Button onClick={handleOpenAddDialog}>
                <Plus className="h-4 w-4 mr-2" />
                Add First LOV List
              </Button>
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>List Code</TableHead>
                  <TableHead>List Name</TableHead>
                  <TableHead>Items</TableHead>
                  <TableHead>Used By</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {lovLists.map((lov) => (
                  <TableRow key={lov.id}>
                    <TableCell className="font-medium">{lov.listCode}</TableCell>
                    <TableCell>{lov.listName}</TableCell>
                    <TableCell>
                      <div className="flex flex-wrap gap-1">
                        {lov.items.slice(0, 3).map((item, idx) => (
                          <Badge
                            key={idx}
                            variant="outline"
                            className="text-xs"
                          >
                            {item.itemValue}
                          </Badge>
                        ))}
                        {lov.items.length > 3 && (
                          <Badge variant="secondary" className="text-xs">
                            +{lov.items.length - 3} more
                          </Badge>
                        )}
                      </div>
                    </TableCell>
                    <TableCell>
                      <span className="text-sm text-gray-500">
                        {lov._count.fields} field(s)
                      </span>
                    </TableCell>
                    <TableCell>
                      <Badge variant={lov.isActive ? "success" : "secondary"}>
                        {lov.isActive ? "Active" : "Inactive"}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-right">
                      <div className="flex justify-end space-x-1">
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => handleOpenEditDialog(lov)}
                        >
                          <Edit className="h-4 w-4" />
                        </Button>
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => handleDeleteLOV(lov.id)}
                          disabled={lov._count.fields > 0}
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

      {/* Add/Edit LOV Dialog */}
      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>
              {editingLOV ? "Edit LOV List" : "Add New LOV List"}
            </DialogTitle>
          </DialogHeader>

          <div className="grid gap-4 py-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="listCode">List Code *</Label>
                <Input
                  id="listCode"
                  name="listCode"
                  value={lovData.listCode}
                  onChange={handleLOVChange}
                  placeholder="e.g., YESNO, SEX, RACE"
                  maxLength={30}
                  disabled={!!editingLOV}
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="listName">List Name *</Label>
                <Input
                  id="listName"
                  name="listName"
                  value={lovData.listName}
                  onChange={handleLOVChange}
                  placeholder="e.g., Yes/No Options, Sex, Race"
                />
              </div>
            </div>

            {/* Items */}
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <Label>List Items *</Label>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={handleAddItem}
                >
                  <Plus className="h-3 w-3 mr-1" />
                  Add Item
                </Button>
              </div>

              <div className="border rounded-md p-3 space-y-2 max-h-64 overflow-y-auto">
                {lovData.items.map((item, index) => (
                  <div key={index} className="flex items-center gap-2">
                    <Input
                      value={item.itemCode}
                      onChange={(e) =>
                        handleItemChange(index, "itemCode", e.target.value)
                      }
                      placeholder="Code (e.g., Y, N)"
                      className="w-1/3"
                      maxLength={20}
                    />
                    <Input
                      value={item.itemValue}
                      onChange={(e) =>
                        handleItemChange(index, "itemValue", e.target.value)
                      }
                      placeholder="Display Value (e.g., Yes, No)"
                      className="flex-1"
                    />
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      onClick={() => handleRemoveItem(index)}
                      disabled={lovData.items.length <= 1}
                    >
                      <X className="h-4 w-4 text-red-500" />
                    </Button>
                  </div>
                ))}
              </div>
              <p className="text-xs text-gray-500">
                Code is stored in database, Display Value is shown to users
              </p>
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
            <Button onClick={handleSaveLOV} disabled={saving}>
              <Save className="h-4 w-4 mr-2" />
              {saving ? "Saving..." : "Save LOV List"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
