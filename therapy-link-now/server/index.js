import express from 'express';
import cors from 'cors';

import authRoutes from './routes/auth.js';
import physioRoutes from './routes/physiotherapists.js';
import appointmentRoutes from './routes/appointments.js';
import reviewRoutes from './routes/reviews.js';
import blockedSlotRoutes from './routes/blocked-slots.js';
import availabilityOverrideRoutes from './routes/availability-overrides.js';
import uploadRoutes from './routes/upload.js';

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors());
app.use(express.json());

// API routes
app.use('/api/auth', authRoutes);
app.use('/api/physiotherapists', physioRoutes);
app.use('/api/appointments', appointmentRoutes);
app.use('/api/reviews', reviewRoutes);
app.use('/api/blocked-slots', blockedSlotRoutes);
app.use('/api/availability-overrides', availabilityOverrideRoutes);
app.use('/api/upload', uploadRoutes);

// Health check
app.get('/api/health', (req, res) => res.json({ status: 'ok' }));

export default app;

// Only listen locally — Vercel uses the export above
if (!process.env.VERCEL) {
  app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
  });
}
