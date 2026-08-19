import { Router } from 'express';
import { candleController } from './candle.controller';
import { asyncHandler } from '../../shared';

const router = Router();

/**
 * POST /api/aggregate/refresh
 * 
 * TimescaleDB Continuous Aggregate 뷰를 수동으로 새로고침합니다.
 * 
 * 사용 사례:
 * 1. 1분봉 데이터를 백필한 후 상위 타임프레임에 즉시 반영
 * 2. 데이터 정합성 문제 발생 시 수동 갱신
 * 
 * 참고:
 * - TimescaleDB가 자동으로 집계를 수행하지만, 백필 후 즉시 반영하려면 이 API 사용
 * - CA 정책의 schedule_interval을 기다리지 않고 수동으로 트리거
 */
router.post(
  '/refresh',
  asyncHandler((req, res) => candleController.refreshAggregation(req as any, res))
);

export default router;
