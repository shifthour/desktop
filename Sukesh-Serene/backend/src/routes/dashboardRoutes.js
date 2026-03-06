import express from 'express';
import { getDashboardStats, getMonthlyReport } from '../controllers/dashboardController.js';
import { authenticate } from '../middleware/auth.js';

const router = express.Router();

router.use(authenticate);

router.get('/stats', getDashboardStats);
router.get('/report', getMonthlyReport);

export default router;
