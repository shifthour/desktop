import express from 'express';
import {
  getAllPGs,
  getPGById,
  createPG,
  updatePG,
  deletePG
} from '../controllers/pgController.js';
import { authenticate } from '../middleware/auth.js';

const router = express.Router();

router.use(authenticate);

router.get('/', getAllPGs);
router.get('/:id', getPGById);
router.post('/', createPG);
router.put('/:id', updatePG);
router.delete('/:id', deletePG);

export default router;
