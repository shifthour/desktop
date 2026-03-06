import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();

        if (user?.role !== 'admin') {
            return Response.json({ error: 'Forbidden: Admin access required' }, { status: 403 });
        }

        // Valid project names that exist in frontend
        const validProjects = ['Anahata', 'Vashishta', 'Krishna', 'White Pearl', 'Advaitha'];

        // Get all projects
        const allProjects = await base44.asServiceRole.entities.Project.list();

        // Find duplicates and invalid projects
        const projectsByName = {};
        const toDelete = [];

        allProjects.forEach(project => {
            const name = project.name;
            
            // If project name is not in valid list, mark for deletion
            if (!validProjects.includes(name)) {
                toDelete.push(project.id);
                return;
            }

            // Keep track of projects by name
            if (!projectsByName[name]) {
                projectsByName[name] = [];
            }
            projectsByName[name].push(project);
        });

        // For each valid project name, keep only the most recent one
        Object.keys(projectsByName).forEach(name => {
            const projects = projectsByName[name];
            if (projects.length > 1) {
                // Sort by updated_date, keep the newest
                projects.sort((a, b) => new Date(b.updated_date) - new Date(a.updated_date));
                // Mark older ones for deletion
                for (let i = 1; i < projects.length; i++) {
                    toDelete.push(projects[i].id);
                }
            }
        });

        // Delete marked projects
        const deletePromises = toDelete.map(id => 
            base44.asServiceRole.entities.Project.delete(id)
        );
        await Promise.all(deletePromises);

        return Response.json({ 
            success: true, 
            deletedCount: toDelete.length,
            deletedIds: toDelete,
            message: `Cleaned up ${toDelete.length} duplicate/invalid projects`
        });

    } catch (error) {
        return Response.json({ error: error.message }, { status: 500 });
    }
});