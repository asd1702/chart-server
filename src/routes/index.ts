import { Router } from 'express';

// Candle 모듈
import { candleRoutes, aggregateRoutes } from '../modules/candle';

const router = Router();

// 캔들 데이터 API
router.use('/candles', candleRoutes);
router.use('/aggregate', aggregateRoutes);

export default router;
