"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { ArrowLeft, Save } from "lucide-react";
import Link from "next/link";

const phases = ["Phase I", "Phase II", "Phase IIa", "Phase IIb", "Phase III", "Phase IV"];
const therapeuticAreas = [
  "Oncology",
  "Cardiology",
  "Neurology",
  "Immunology",
  "Infectious Disease",
  "Respiratory",
  "Dermatology",
  "Gastroenterology",
  "Endocrinology",
  "Other",
];

export default function NewStudyPage() {
  const router = useRouter();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [formData, setFormData] = useState({
    studyCode: "",
    studyName: "",
    protocolNumber: "",
    sponsorName: "",
    phase: "",
    therapeuticArea: "",
    indication: "",
    startDate: "",
    endDate: "",
  });

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    setFormData((prev) => ({ ...prev, [name]: value }));
  };

  const handleSelectChange = (name: string, value: string) => {
    setFormData((prev) => ({ ...prev, [name]: value }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    setLoading(true);

    try {
      const response = await fetch("/api/studies", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(formData),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || "Failed to create study");
      }

      router.push("/studies");
      router.refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create study");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center space-x-4">
        <Link href="/studies">
          <Button variant="ghost" size="icon">
            <ArrowLeft className="h-4 w-4" />
          </Button>
        </Link>
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Create New Study</h2>
          <p className="text-gray-500">Set up a new clinical trial study</p>
        </div>
      </div>

      <form onSubmit={handleSubmit}>
        <Card>
          <CardHeader>
            <CardTitle>Study Information</CardTitle>
          </CardHeader>
          <CardContent className="space-y-6">
            {error && (
              <div className="p-3 text-sm text-red-600 bg-red-50 border border-red-200 rounded-md">
                {error}
              </div>
            )}

            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div className="space-y-2">
                <Label htmlFor="studyCode">Study Code *</Label>
                <Input
                  id="studyCode"
                  name="studyCode"
                  value={formData.studyCode}
                  onChange={handleChange}
                  placeholder="e.g., DT-001"
                  required
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="protocolNumber">Protocol Number</Label>
                <Input
                  id="protocolNumber"
                  name="protocolNumber"
                  value={formData.protocolNumber}
                  onChange={handleChange}
                  placeholder="e.g., PROTO-2026-001"
                />
              </div>

              <div className="space-y-2 md:col-span-2">
                <Label htmlFor="studyName">Study Name *</Label>
                <Input
                  id="studyName"
                  name="studyName"
                  value={formData.studyName}
                  onChange={handleChange}
                  placeholder="Enter full study name"
                  required
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="sponsorName">Sponsor Name</Label>
                <Input
                  id="sponsorName"
                  name="sponsorName"
                  value={formData.sponsorName}
                  onChange={handleChange}
                  placeholder="Enter sponsor name"
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="phase">Phase</Label>
                <Select
                  value={formData.phase}
                  onValueChange={(value) => handleSelectChange("phase", value)}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Select phase" />
                  </SelectTrigger>
                  <SelectContent>
                    {phases.map((phase) => (
                      <SelectItem key={phase} value={phase}>
                        {phase}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="space-y-2">
                <Label htmlFor="therapeuticArea">Therapeutic Area</Label>
                <Select
                  value={formData.therapeuticArea}
                  onValueChange={(value) =>
                    handleSelectChange("therapeuticArea", value)
                  }
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Select area" />
                  </SelectTrigger>
                  <SelectContent>
                    {therapeuticAreas.map((area) => (
                      <SelectItem key={area} value={area}>
                        {area}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="space-y-2">
                <Label htmlFor="indication">Indication</Label>
                <Input
                  id="indication"
                  name="indication"
                  value={formData.indication}
                  onChange={handleChange}
                  placeholder="e.g., Type 2 Diabetes"
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="startDate">Start Date</Label>
                <Input
                  id="startDate"
                  name="startDate"
                  type="date"
                  value={formData.startDate}
                  onChange={handleChange}
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="endDate">End Date</Label>
                <Input
                  id="endDate"
                  name="endDate"
                  type="date"
                  value={formData.endDate}
                  onChange={handleChange}
                />
              </div>
            </div>

            <div className="flex justify-end space-x-4 pt-4 border-t">
              <Link href="/studies">
                <Button variant="outline" type="button">
                  Cancel
                </Button>
              </Link>
              <Button type="submit" disabled={loading}>
                <Save className="h-4 w-4 mr-2" />
                {loading ? "Creating..." : "Create Study"}
              </Button>
            </div>
          </CardContent>
        </Card>
      </form>
    </div>
  );
}
