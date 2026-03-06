import express from 'express';
import {
  getAllPayments,
  createPayment,
  updatePayment,
  deletePayment
} from '../controllers/rentController.js';
import { authenticate } from '../middleware/auth.js';

const router = express.Router();

router.use(authenticate);

router.get('/', getAllPayments);
router.post('/', createPayment);
router.put('/:id', updatePayment);
router.delete('/:id', deletePayment);

export default router;
