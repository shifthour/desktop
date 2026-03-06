import { readdir, readFile, writeFile, mkdir, unlink } from "fs/promises";
import path from "path";
import { v4 as uuidv4 } from "uuid";
import type { Project } from "./types";

const DATA_DIR = path.join(process.cwd(), "data");
const PROJECTS_DIR = path.join(DATA_DIR, "projects");
const PDFS_DIR = path.join(DATA_DIR, "pdfs");
const GENERATED_DIR = path.join(DATA_DIR, "generated");

async function ensureDirs() {
  await mkdir(PROJECTS_DIR, { recursive: true });
  await mkdir(PDFS_DIR, { recursive: true });
  await mkdir(GENERATED_DIR, { recursive: true });
}

function slugify(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
    .substring(0, 50);
}

export async function getAllProjects(): Promise<Project[]> {
  await ensureDirs();
  try {
    const files = await readdir(PROJECTS_DIR);
    const jsonFiles = files.filter((f) => f.endsWith(".json"));
    const projects = await Promise.all(
      jsonFiles.map(async (f) => {
        const content = await readFile(path.join(PROJECTS_DIR, f), "utf-8");
        return JSON.parse(content) as Project;
      })
    );
    return projects.sort(
      (a, b) =>
        new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime()
    );
  } catch {
    return [];
  }
}

export async function getProject(id: string): Promise<Project | null> {
  await ensureDirs();
  try {
    const content = await readFile(
      path.join(PROJECTS_DIR, `${id}.json`),
      "utf-8"
    );
    return JSON.parse(content) as Project;
  } catch {
    return null;
  }
}

export async function createProject(
  data: Pick<Project, "name" | "prompt" | "pdfPath" | "pdfOriginalName">
): Promise<Project> {
  await ensureDirs();
  const id = uuidv4();
  const now = new Date().toISOString();
  const project: Project = {
    id,
    name: data.name,
    slug: slugify(data.name),
    pdfPath: data.pdfPath,
    pdfOriginalName: data.pdfOriginalName,
    prompt: data.prompt,
    generatedHtmlPath: null,
    githubRepoUrl: null,
    vercelUrl: null,
    vercelProjectName: null,
    status: "draft",
    errorMessage: null,
    createdAt: now,
    updatedAt: now,
  };
  await writeFile(
    path.join(PROJECTS_DIR, `${id}.json`),
    JSON.stringify(project, null, 2)
  );
  return project;
}

export async function updateProject(
  id: string,
  data: Partial<Project>
): Promise<Project> {
  const existing = await getProject(id);
  if (!existing) throw new Error(`Project ${id} not found`);
  const updated: Project = {
    ...existing,
    ...data,
    id: existing.id,
    createdAt: existing.createdAt,
    updatedAt: new Date().toISOString(),
  };
  await writeFile(
    path.join(PROJECTS_DIR, `${id}.json`),
    JSON.stringify(updated, null, 2)
  );
  return updated;
}

export async function deleteProject(id: string): Promise<void> {
  const project = await getProject(id);
  if (!project) return;
  try {
    await unlink(path.join(PROJECTS_DIR, `${id}.json`));
  } catch {}
  if (project.pdfPath) {
    try {
      await unlink(path.join(process.cwd(), project.pdfPath));
    } catch {}
  }
  if (project.generatedHtmlPath) {
    try {
      await unlink(path.join(process.cwd(), project.generatedHtmlPath));
    } catch {}
  }
}

export { PDFS_DIR, GENERATED_DIR, ensureDirs };
