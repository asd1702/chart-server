import { Router } from 'express';
import { candleController } from './candle.controller';
import { asyncHandler } from '../../shared';

const router = Router();

// GET /api/candles/:symbol/:timeframe
router.get(
  '/:symbol/:timeframe',
  asyncHandler((req, res) => candleController.getCandles(req as any, res))
);

export default router;
