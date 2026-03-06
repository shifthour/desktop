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
  const deploymentId = data.id;

  // The default URL from Vercel (may include team slug — very long)
  const defaultUrl = data.alias?.[0]
    ? `https://${data.alias[0]}`
    : `https://${data.url}`;

  // ─── Try to set a shorter alias ───
  // On team accounts, Vercel appends the team slug making URLs very long.
  // We set a custom alias like "yajur-project-name.vercel.app" for cleaner URLs.
  const shortAlias = `${projectName}.vercel.app`;

  try {
    const aliasResponse = await fetch(
      `${VERCEL_API}/v2/deployments/${deploymentId}/aliases`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          alias: shortAlias,
        }),
      }
    );

    if (aliasResponse.ok) {
      const aliasData = await aliasResponse.json();
      const cleanAlias = aliasData.alias || shortAlias;
      const cleanUrl = cleanAlias.startsWith("https://")
        ? cleanAlias
        : `https://${cleanAlias}`;

      console.log(`Short alias set: ${cleanUrl}`);

      return {
        url: cleanUrl,
        deploymentId,
      };
    } else {
      const aliasError = await aliasResponse.json().catch(() => ({}));
      console.warn(
        `Could not set short alias "${shortAlias}":`,
        JSON.stringify(aliasError)
      );
      // Fall through to default URL
    }
  } catch (aliasErr) {
    console.warn(
      "Failed to set short alias:",
      aliasErr instanceof Error ? aliasErr.message : String(aliasErr)
    );
    // Fall through to default URL
  }

  return {
    url: defaultUrl,
    deploymentId,
  };
}

export async function redeployToVercel(
  projectName: string,
  htmlContent: string
): Promise<{ url: string; deploymentId: string }> {
  return deployToVercel(projectName, htmlContent);
}
