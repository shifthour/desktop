export interface Project {
  id: string;
  name: string;
  slug: string;
  pdfPath: string;
  pdfOriginalName: string;
  prompt: string;
  generatedHtmlPath: string | null;
  githubRepoUrl: string | null;
  vercelUrl: string | null;
  vercelProjectName: string | null;
  status: "draft" | "generating" | "deploying" | "deployed" | "error";
  errorMessage: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface CreateProjectRequest {
  name: string;
  prompt: string;
  pdfFileName: string;
}

export interface GenerateRequest {
  projectId: string;
}

export interface DeployRequest {
  projectId: string;
}
