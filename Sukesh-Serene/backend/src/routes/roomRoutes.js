import express from 'express';
import {
  getAllRooms,
  getRoomById,
  createRoom,
  updateRoom,
  deleteRoom
} from '../controllers/roomController.js';
import { authenticate } from '../middleware/auth.js';

const router = express.Router();

router.use(authenticate);

router.get('/', getAllRooms);
router.get('/:id', getRoomById);
router.post('/', createRoom);
router.put('/:id', updateRoom);
router.delete('/:id', deleteRoom);

export default router;
