import { PrismaClient } from '@prisma/client';
import { assertDestructiveDbScriptAllowed } from './script-safety';
import { getErrorMessage, logger } from '../shared/utils/logger';

assertDestructiveDbScriptAllowed('reset-db');

const prisma = new PrismaClient();

async function reset() {
  logger.info('Architecture Lab 캔들 데이터 삭제를 시작합니다.', {
    component: 'reset-db',
    event: 'reset_db_started',
  });
  const deleted1m = await prisma.candle1m.deleteMany();
  logger.info('Architecture Lab 캔들 데이터 삭제를 완료했습니다.', {
    component: 'reset-db',
    event: 'reset_db_completed',
    count: deleted1m.count,
  });
}

reset()
  .then(() => prisma.$disconnect())
  .catch(async (e) => {
    logger.error('Architecture Lab 캔들 데이터 삭제에 실패했습니다.', {
      component: 'reset-db',
      event: 'reset_db_failed',
      error: getErrorMessage(e),
    });
    await prisma.$disconnect();
    process.exit(1);
  });
