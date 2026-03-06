import express from 'express';
import {
  createComplaint,
  getAllComplaints,
  getTenantComplaints,
  updateComplaintStatus,
  resolveComplaint,
  deleteComplaint
} from '../controllers/complaintController.js';

const router = express.Router();

router.get('/', getAllComplaints);
router.get('/tenant/:tenant_id', getTenantComplaints);
router.post('/', createComplaint);
router.put('/:id/status', updateComplaintStatus);
router.put('/:id/resolve', resolveComplaint);
router.delete('/:id', deleteComplaint);

export default router;
