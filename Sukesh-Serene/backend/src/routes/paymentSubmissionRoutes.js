import express from 'express';
import {
  createPaymentSubmission,
  getAllPaymentSubmissions,
  getTenantPaymentSubmissions,
  getNotPaidTenants,
  reviewPaymentSubmission
} from '../controllers/paymentSubmissionController.js';
import { authenticate } from '../middleware/auth.js';

const router = express.Router();

// Public route for tenant submission (tenant uses tenant auth token)
router.post('/', createPaymentSubmission);

// Protected routes for admin/supervisor
router.get('/', authenticate, getAllPaymentSubmissions);
router.get('/not-paid', authenticate, getNotPaidTenants);
router.get('/tenant/:tenant_id', getTenantPaymentSubmissions);
router.put('/:id/review', authenticate, reviewPaymentSubmission);

export default router;
