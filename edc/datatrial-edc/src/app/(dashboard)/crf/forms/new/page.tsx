"use client";

import { useState, useEffect } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { ArrowLeft, Save } from "lucide-react";
import Link from "next/link";

// CRF Types as per FSD
const CRF_TYPES = [
  "Simple CRF",
  "Visit Parent Page",
  "Visit Child Page",
  "Dynamic Pages",
  "Static Pages",
  "Multi Column",
];

interface Study {
  id: string;
  studyCode: string;
  studyName: string;
}

export default function NewCRFFormPage() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const studyIdParam = searchParams.get("studyId");

  const [studies, setStudies] = useState<Study[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [formData, setFormData] = useState({
    studyId: studyIdParam || "",
    formCode: "",
    formName: "",
    formType: "Simple CRF",
    isRepeating: false,
    isLogForm: false,
  });

  useEffect(() => {
    fetchStudies();
  }, []);

  useEffect(() => {
    if (studyIdParam) {
      setFormData((prev) => ({ ...prev, studyId: studyIdParam }));
    }
  }, [studyIdParam]);

  const fetchStudies = async () => {
    try {
      const response = await fetch("/api/studies");
      if (response.ok) {
        const data = await response.json();
        setStudies(data);
      }
    } catch (error) {
      console.error("Failed to fetch studies:", error);
    }
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value } = e.target;
    setFormData((prev) => ({ ...prev, [name]: value }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    setLoading(true);

    try {
      const response = await fetch("/api/crf/forms", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(formData),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || "Failed to create form");
      }

      const createdForm = await response.json();
      router.push(`/crf/forms/${createdForm.id}/design`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create form");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center space-x-4">
        <Link href="/crf/forms">
          <Button variant="ghost" size="icon">
            <ArrowLeft className="h-4 w-4" />
          </Button>
        </Link>
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Create New CRF Form</h2>
          <p className="text-gray-500">Define a new Case Report Form</p>
        </div>
      </div>

      <form onSubmit={handleSubmit}>
        <Card>
          <CardHeader>
            <CardTitle>Form Information</CardTitle>
          </CardHeader>
          <CardContent className="space-y-6">
            {error && (
              <div className="p-3 text-sm text-red-600 bg-red-50 border border-red-200 rounded-md">
                {error}
              </div>
            )}

            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div className="space-y-2">
                <Label htmlFor="studyId">Study *</Label>
                <Select
                  value={formData.studyId}
                  onValueChange={(value) =>
                    setFormData((prev) => ({ ...prev, studyId: value }))
                  }
                >
                  <SelectTrigger>
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
              </div>

              <div className="space-y-2">
                <Label htmlFor="formType">CRF Type *</Label>
                <Select
                  value={formData.formType}
                  onValueChange={(value) =>
                    setFormData((prev) => ({ ...prev, formType: value }))
                  }
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {CRF_TYPES.map((type) => (
                      <SelectItem key={type} value={type}>
                        {type}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="space-y-2">
                <Label htmlFor="formCode">Form Code (Short Name) *</Label>
                <Input
                  id="formCode"
                  name="formCode"
                  value={formData.formCode}
                  onChange={handleChange}
                  placeholder="e.g., DMG, VS, PE"
                  required
                  maxLength={20}
                />
                <p className="text-xs text-gray-500">
                  Short identifier used in database
                </p>
              </div>

              <div className="space-y-2">
                <Label htmlFor="formName">Form Name *</Label>
                <Input
                  id="formName"
                  name="formName"
                  value={formData.formName}
                  onChange={handleChange}
                  placeholder="e.g., Demographics, Vital Signs"
                  required
                />
              </div>

              <div className="space-y-4 md:col-span-2">
                <Label>Form Options</Label>
                <div className="flex flex-wrap gap-6">
                  <div className="flex items-center space-x-2">
                    <Checkbox
                      id="isRepeating"
                      checked={formData.isRepeating}
                      onCheckedChange={(checked) =>
                        setFormData((prev) => ({
                          ...prev,
                          isRepeating: checked === true,
                        }))
                      }
                    />
                    <Label htmlFor="isRepeating" className="font-normal">
                      Repeating Form (Multiple rows allowed)
                    </Label>
                  </div>

                  <div className="flex items-center space-x-2">
                    <Checkbox
                      id="isLogForm"
                      checked={formData.isLogForm}
                      onCheckedChange={(checked) =>
                        setFormData((prev) => ({
                          ...prev,
                          isLogForm: checked === true,
                        }))
                      }
                    />
                    <Label htmlFor="isLogForm" className="font-normal">
                      Log Form (e.g., Adverse Events, Medications)
                    </Label>
                  </div>
                </div>
              </div>
            </div>

            <div className="flex justify-end space-x-4 pt-4 border-t">
              <Link href="/crf/forms">
                <Button variant="outline" type="button">
                  Cancel
                </Button>
              </Link>
              <Button type="submit" disabled={loading || !formData.studyId}>
                <Save className="h-4 w-4 mr-2" />
                {loading ? "Creating..." : "Create & Design Fields"}
              </Button>
            </div>
          </CardContent>
        </Card>
      </form>
    </div>
  );
}
