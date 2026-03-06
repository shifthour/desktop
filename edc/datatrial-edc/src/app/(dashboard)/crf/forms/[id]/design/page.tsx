"use client";

import { useState, useEffect, use } from "react";
import { useRouter } from "next/navigation";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Checkbox } from "@/components/ui/checkbox";
import { Badge } from "@/components/ui/badge";
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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  ArrowLeft,
  Plus,
  Edit,
  Trash2,
  GripVertical,
  Save,
  Layers,
} from "lucide-react";
import Link from "next/link";

// Field types as per FSD
const FIELD_TYPES = [
  { value: "TEXT", label: "Text" },
  { value: "NUMBER", label: "Number" },
  { value: "DATE", label: "Date" },
  { value: "SELECT_LIST", label: "Select List (LOV)" },
  { value: "TEXTAREA", label: "Text Area" },
  { value: "CHECKBOX", label: "Checkbox" },
  { value: "RADIO", label: "Radio Button" },
];

interface CRFForm {
  id: string;
  formCode: string;
  formName: string;
  formType: string | null;
  studyId: string;
  study: {
    studyCode: string;
    studyName: string;
  };
}

interface CRFField {
  id: string;
  fieldCode: string;
  fieldLabel: string;
  fieldType: string;
  sequenceNumber: number;
  isRequired: boolean;
  isActive: boolean;
  maxLength: number | null;
  minValue: number | null;
  maxValue: number | null;
  decimalPlaces: number | null;
  regexPattern: string | null;
  lovListId: string | null;
  lovList: {
    listCode: string;
    listName: string;
  } | null;
}

interface LOVList {
  id: string;
  listCode: string;
  listName: string;
}

interface FieldFormData {
  fieldCode: string;
  fieldLabel: string;
  fieldType: string;
  isRequired: boolean;
  maxLength: number | null;
  minValue: number | null;
  maxValue: number | null;
  decimalPlaces: number | null;
  regexPattern: string;
  lovListId: string;
}

const initialFieldData: FieldFormData = {
  fieldCode: "",
  fieldLabel: "",
  fieldType: "TEXT",
  isRequired: false,
  maxLength: null,
  minValue: null,
  maxValue: null,
  decimalPlaces: null,
  regexPattern: "",
  lovListId: "",
};

export default function FormDesignPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id: formId } = use(params);
  const router = useRouter();

  const [form, setForm] = useState<CRFForm | null>(null);
  const [fields, setFields] = useState<CRFField[]>([]);
  const [lovLists, setLovLists] = useState<LOVList[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");

  // Dialog state
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingField, setEditingField] = useState<CRFField | null>(null);
  const [fieldData, setFieldData] = useState<FieldFormData>(initialFieldData);

  useEffect(() => {
    fetchFormDetails();
  }, [formId]);

  const fetchFormDetails = async () => {
    try {
      // Fetch form details
      const formResponse = await fetch(`/api/crf/forms/${formId}`);
      if (!formResponse.ok) {
        throw new Error("Form not found");
      }
      const formData = await formResponse.json();
      setForm(formData);

      // Fetch fields for this form
      const fieldsResponse = await fetch(`/api/crf/fields?formId=${formId}`);
      if (fieldsResponse.ok) {
        const fieldsData = await fieldsResponse.json();
        setFields(fieldsData);
      }

      // Fetch LOV lists for this study
      const lovResponse = await fetch(
        `/api/crf/lov?studyId=${formData.studyId}`
      );
      if (lovResponse.ok) {
        const lovData = await lovResponse.json();
        setLovLists(lovData);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load form");
    } finally {
      setLoading(false);
    }
  };

  const handleOpenAddDialog = () => {
    setEditingField(null);
    setFieldData(initialFieldData);
    setDialogOpen(true);
  };

  const handleOpenEditDialog = (field: CRFField) => {
    setEditingField(field);
    setFieldData({
      fieldCode: field.fieldCode,
      fieldLabel: field.fieldLabel,
      fieldType: field.fieldType,
      isRequired: field.isRequired,
      maxLength: field.maxLength,
      minValue: field.minValue,
      maxValue: field.maxValue,
      decimalPlaces: field.decimalPlaces,
      regexPattern: field.regexPattern || "",
      lovListId: field.lovListId || "",
    });
    setDialogOpen(true);
  };

  const handleFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    setFieldData((prev) => ({
      ...prev,
      [name]:
        type === "number" ? (value ? parseFloat(value) : null) : value,
    }));
  };

  const handleSaveField = async () => {
    setError("");
    setSaving(true);

    try {
      const payload = {
        ...fieldData,
        formId,
        lovListId: fieldData.lovListId || null,
        regexPattern: fieldData.regexPattern || null,
      };

      let response;
      if (editingField) {
        // Update existing field
        response = await fetch(`/api/crf/fields/${editingField.id}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
      } else {
        // Create new field
        response = await fetch("/api/crf/fields", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
      }

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || "Failed to save field");
      }

      // Refresh fields list
      await fetchFormDetails();
      setDialogOpen(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to save field");
    } finally {
      setSaving(false);
    }
  };

  const handleDeleteField = async (fieldId: string) => {
    if (!confirm("Are you sure you want to delete this field?")) {
      return;
    }

    try {
      const response = await fetch(`/api/crf/fields/${fieldId}`, {
        method: "DELETE",
      });

      if (!response.ok) {
        throw new Error("Failed to delete field");
      }

      // Refresh fields list
      await fetchFormDetails();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to delete field");
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  if (!form) {
    return (
      <div className="text-center py-8">
        <p className="text-red-600 mb-4">Form not found</p>
        <Link href="/crf/forms">
          <Button>Back to Forms</Button>
        </Link>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <Link href="/crf/forms">
            <Button variant="ghost" size="icon">
              <ArrowLeft className="h-4 w-4" />
            </Button>
          </Link>
          <div>
            <h2 className="text-2xl font-bold text-gray-900">
              Form Designer: {form.formCode}
            </h2>
            <p className="text-gray-500">
              {form.formName} - {form.study.studyCode}
            </p>
          </div>
        </div>
        <Button onClick={handleOpenAddDialog}>
          <Plus className="h-4 w-4 mr-2" />
          Add Field
        </Button>
      </div>

      {error && (
        <div className="p-3 text-sm text-red-600 bg-red-50 border border-red-200 rounded-md">
          {error}
        </div>
      )}

      {/* Form Info Card */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center">
            <Layers className="h-4 w-4 mr-2" />
            Form Information
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
            <div>
              <span className="text-gray-500">Form Code:</span>
              <p className="font-medium">{form.formCode}</p>
            </div>
            <div>
              <span className="text-gray-500">Form Name:</span>
              <p className="font-medium">{form.formName}</p>
            </div>
            <div>
              <span className="text-gray-500">Type:</span>
              <p className="font-medium">{form.formType || "Simple CRF"}</p>
            </div>
            <div>
              <span className="text-gray-500">Total Fields:</span>
              <p className="font-medium">{fields.length}</p>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Fields List */}
      <Card>
        <CardHeader>
          <CardTitle>Fields</CardTitle>
        </CardHeader>
        <CardContent>
          {fields.length === 0 ? (
            <div className="text-center py-8">
              <p className="text-gray-500 mb-4">
                No fields defined for this form yet
              </p>
              <Button onClick={handleOpenAddDialog}>
                <Plus className="h-4 w-4 mr-2" />
                Add First Field
              </Button>
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-12">Seq</TableHead>
                  <TableHead>Field Code</TableHead>
                  <TableHead>Label</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Validation</TableHead>
                  <TableHead>Required</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {fields.map((field) => (
                  <TableRow key={field.id}>
                    <TableCell>
                      <div className="flex items-center">
                        <GripVertical className="h-4 w-4 text-gray-400 mr-1" />
                        {field.sequenceNumber}
                      </div>
                    </TableCell>
                    <TableCell className="font-medium">
                      {field.fieldCode}
                    </TableCell>
                    <TableCell>{field.fieldLabel}</TableCell>
                    <TableCell>
                      <Badge variant="outline">{field.fieldType}</Badge>
                      {field.lovList && (
                        <span className="ml-2 text-xs text-gray-500">
                          ({field.lovList.listCode})
                        </span>
                      )}
                    </TableCell>
                    <TableCell>
                      <div className="text-xs text-gray-500">
                        {field.fieldType === "TEXT" && field.maxLength && (
                          <span>Max: {field.maxLength}</span>
                        )}
                        {field.fieldType === "NUMBER" && (
                          <>
                            {field.minValue !== null && (
                              <span>Min: {field.minValue} </span>
                            )}
                            {field.maxValue !== null && (
                              <span>Max: {field.maxValue} </span>
                            )}
                            {field.decimalPlaces !== null && (
                              <span>Dec: {field.decimalPlaces}</span>
                            )}
                          </>
                        )}
                        {field.regexPattern && <span>Pattern</span>}
                      </div>
                    </TableCell>
                    <TableCell>
                      {field.isRequired ? (
                        <Badge variant="destructive" className="text-xs">
                          Required
                        </Badge>
                      ) : (
                        <span className="text-gray-400 text-xs">Optional</span>
                      )}
                    </TableCell>
                    <TableCell>
                      <Badge variant={field.isActive ? "success" : "secondary"}>
                        {field.isActive ? "Active" : "Inactive"}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-right">
                      <div className="flex justify-end space-x-1">
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => handleOpenEditDialog(field)}
                        >
                          <Edit className="h-4 w-4" />
                        </Button>
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => handleDeleteField(field.id)}
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

      {/* Add/Edit Field Dialog */}
      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>
              {editingField ? "Edit Field" : "Add New Field"}
            </DialogTitle>
          </DialogHeader>

          <div className="grid gap-4 py-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="fieldCode">Field Code (Column Name) *</Label>
                <Input
                  id="fieldCode"
                  name="fieldCode"
                  value={fieldData.fieldCode}
                  onChange={handleFieldChange}
                  placeholder="e.g., BRTHDAT, WEIGHT"
                  maxLength={30}
                  disabled={!!editingField}
                />
                <p className="text-xs text-gray-500">
                  Database column identifier (no spaces)
                </p>
              </div>

              <div className="space-y-2">
                <Label htmlFor="fieldLabel">Field Label *</Label>
                <Input
                  id="fieldLabel"
                  name="fieldLabel"
                  value={fieldData.fieldLabel}
                  onChange={handleFieldChange}
                  placeholder="e.g., Date of Birth, Weight (kg)"
                />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="fieldType">Field Type *</Label>
                <Select
                  value={fieldData.fieldType}
                  onValueChange={(value) =>
                    setFieldData((prev) => ({ ...prev, fieldType: value }))
                  }
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {FIELD_TYPES.map((type) => (
                      <SelectItem key={type.value} value={type.value}>
                        {type.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="flex items-center space-x-2 pt-8">
                <Checkbox
                  id="isRequired"
                  checked={fieldData.isRequired}
                  onCheckedChange={(checked) =>
                    setFieldData((prev) => ({
                      ...prev,
                      isRequired: checked === true,
                    }))
                  }
                />
                <Label htmlFor="isRequired" className="font-normal">
                  Required Field
                </Label>
              </div>
            </div>

            {/* LOV Selection for SELECT_LIST type */}
            {fieldData.fieldType === "SELECT_LIST" && (
              <div className="space-y-2">
                <Label htmlFor="lovListId">List of Values (LOV) *</Label>
                <Select
                  value={fieldData.lovListId}
                  onValueChange={(value) =>
                    setFieldData((prev) => ({ ...prev, lovListId: value }))
                  }
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Select a LOV list" />
                  </SelectTrigger>
                  <SelectContent>
                    {lovLists.map((lov) => (
                      <SelectItem key={lov.id} value={lov.id}>
                        {lov.listCode} - {lov.listName}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                {lovLists.length === 0 && (
                  <p className="text-xs text-amber-600">
                    No LOV lists available. Create one in LOV Management first.
                  </p>
                )}
              </div>
            )}

            {/* Text field options */}
            {(fieldData.fieldType === "TEXT" ||
              fieldData.fieldType === "TEXTAREA") && (
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="maxLength">Maximum Length</Label>
                  <Input
                    id="maxLength"
                    name="maxLength"
                    type="number"
                    value={fieldData.maxLength ?? ""}
                    onChange={handleFieldChange}
                    placeholder="e.g., 200"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="regexPattern">Validation Pattern (Regex)</Label>
                  <Input
                    id="regexPattern"
                    name="regexPattern"
                    value={fieldData.regexPattern}
                    onChange={handleFieldChange}
                    placeholder="e.g., ^[A-Z]{2}[0-9]{4}$"
                  />
                </div>
              </div>
            )}

            {/* Number field options */}
            {fieldData.fieldType === "NUMBER" && (
              <div className="grid grid-cols-3 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="minValue">Minimum Value</Label>
                  <Input
                    id="minValue"
                    name="minValue"
                    type="number"
                    value={fieldData.minValue ?? ""}
                    onChange={handleFieldChange}
                    placeholder="e.g., 0"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="maxValue">Maximum Value</Label>
                  <Input
                    id="maxValue"
                    name="maxValue"
                    type="number"
                    value={fieldData.maxValue ?? ""}
                    onChange={handleFieldChange}
                    placeholder="e.g., 999"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="decimalPlaces">Decimal Places</Label>
                  <Input
                    id="decimalPlaces"
                    name="decimalPlaces"
                    type="number"
                    value={fieldData.decimalPlaces ?? ""}
                    onChange={handleFieldChange}
                    placeholder="e.g., 2"
                    min={0}
                    max={10}
                  />
                </div>
              </div>
            )}
          </div>

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setDialogOpen(false)}
              disabled={saving}
            >
              Cancel
            </Button>
            <Button onClick={handleSaveField} disabled={saving}>
              <Save className="h-4 w-4 mr-2" />
              {saving ? "Saving..." : "Save Field"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
