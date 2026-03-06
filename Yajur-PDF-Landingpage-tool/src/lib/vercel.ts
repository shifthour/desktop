const VERCEL_API = "https://api.vercel.com";

function getToken() {
  return process.env.VERCEL_TOKEN!;
}

export async function deployToVercel(
  projectName: string,
  htmlContent: string
): Promise<{ url: string; deploymentId: string }> {
  const token = getToken();

  const response = await fetch(`${VERCEL_API}/v13/deployments`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      name: projectName,
      files: [
        {
          file: "index.html",
          data: Buffer.from(htmlContent).toString("base64"),
          encoding: "base64",
        },
      ],
      projectSettings: {
        framework: null,
        outputDirectory: ".",
      },
      target: "production",
    }),
  });

  if (!response.ok) {
    const error = await response.json();
    throw new Error(
      `Vercel deployment failed: ${JSON.stringify(error.error || error)}`
    );
  }

  const data = await response.json();

  // The URL from Vercel can be either the alias or the deployment URL
  const deployUrl = data.alias?.[0]
    ? `https://${data.alias[0]}`
    : `https://${data.url}`;

  return {
    url: deployUrl,
    deploymentId: data.id,
  };
}

export async function redeployToVercel(
  projectName: string,
  htmlContent: string
): Promise<{ url: string; deploymentId: string }> {
  return deployToVercel(projectName, htmlContent);
}
