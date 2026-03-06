import express from 'express';
import {
  getAllTenants,
  getTenantById,
  createTenant,
  updateTenant,
  deleteTenant
} from '../controllers/tenantController.js';
import { authenticate } from '../middleware/auth.js';

const router = express.Router();

router.use(authenticate);

router.get('/', getAllTenants);
router.get('/:id', getTenantById);
router.post('/', createTenant);
router.put('/:id', updateTenant);
router.delete('/:id', deleteTenant);

export default router;
