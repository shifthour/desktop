import Anthropic from "@anthropic-ai/sdk";
import { readFile } from "fs/promises";
import path from "path";
import { getPromptTemplate } from "./prompt-template";

const client = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY!,
});

export async function analyzePdfAndGenerate(
  pdfRelativePath: string,
  userPrompt: string
): Promise<string> {
  const pdfPath = path.join(process.cwd(), pdfRelativePath);
  const pdfBuffer = await readFile(pdfPath);
  const pdfBase64 = pdfBuffer.toString("base64");

  const fullPrompt = getPromptTemplate(userPrompt);

  const message = await client.messages.create({
    model: "claude-sonnet-4-20250514",
    max_tokens: 16000,
    messages: [
      {
        role: "user",
        content: [
          {
            type: "document",
            source: {
              type: "base64",
              media_type: "application/pdf",
              data: pdfBase64,
            },
          },
          {
            type: "text",
            text: fullPrompt,
          },
        ],
      },
    ],
  });

  const textContent = message.content.find((c) => c.type === "text");
  if (!textContent || textContent.type !== "text") {
    throw new Error("No text content in Claude response");
  }

  return extractHtml(textContent.text);
}

function extractHtml(response: string): string {
  // Try to extract from ```html ... ``` code fences
  const htmlMatch = response.match(/```html\s*([\s\S]*?)\s*```/);
  if (htmlMatch) return htmlMatch[1].trim();

  // Try to extract from <!DOCTYPE or <html
  const docMatch = response.match(/(<!DOCTYPE[\s\S]*<\/html>)/i);
  if (docMatch) return docMatch[1].trim();

  // Check if it starts with DOCTYPE or html tag
  const trimmed = response.trim();
  if (
    trimmed.startsWith("<!DOCTYPE") ||
    trimmed.startsWith("<html") ||
    trimmed.startsWith("<HTML")
  ) {
    return trimmed;
  }

  throw new Error(
    "Claude did not return valid HTML. Response starts with: " +
      response.substring(0, 100)
  );
}
