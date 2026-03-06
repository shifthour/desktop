import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();

        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        // Get Google Drive access token
        const accessToken = await base44.asServiceRole.connectors.getAccessToken('googledrive');

        // Fetch all projects
        const projects = await base44.asServiceRole.entities.Project.list();

        const results = [];

        for (const project of projects) {
            if (!project.image) {
                results.push({
                    project: project.name,
                    status: 'skipped',
                    reason: 'No image URL'
                });
                continue;
            }

            try {
                // Download the image
                const imageResponse = await fetch(project.image);
                if (!imageResponse.ok) {
                    results.push({
                        project: project.name,
                        status: 'failed',
                        reason: 'Failed to download image'
                    });
                    continue;
                }

                const imageBlob = await imageResponse.blob();
                const imageBuffer = await imageBlob.arrayBuffer();

                // Determine file extension
                const urlParts = project.image.split('?')[0].split('.');
                const extension = urlParts[urlParts.length - 1] || 'jpg';
                const fileName = `${project.name.replace(/[^a-zA-Z0-9]/g, '_')}.${extension}`;

                // Upload to Google Drive
                const metadata = {
                    name: fileName,
                    mimeType: imageBlob.type || 'image/jpeg'
                };

                const form = new FormData();
                form.append('metadata', new Blob([JSON.stringify(metadata)], { type: 'application/json' }));
                form.append('file', new Blob([imageBuffer], { type: imageBlob.type || 'image/jpeg' }));

                const uploadResponse = await fetch(
                    'https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart',
                    {
                        method: 'POST',
                        headers: {
                            'Authorization': `Bearer ${accessToken}`
                        },
                        body: form
                    }
                );

                if (uploadResponse.ok) {
                    const uploadResult = await uploadResponse.json();
                    results.push({
                        project: project.name,
                        status: 'success',
                        fileId: uploadResult.id,
                        fileName: fileName
                    });
                } else {
                    const error = await uploadResponse.text();
                    results.push({
                        project: project.name,
                        status: 'failed',
                        reason: error
                    });
                }
            } catch (error) {
                results.push({
                    project: project.name,
                    status: 'failed',
                    reason: error.message
                });
            }
        }

        return Response.json({
            message: 'Sync completed',
            total: projects.length,
            results: results
        });
    } catch (error) {
        return Response.json({ error: error.message }, { status: 500 });
    }
});