import { PrismaClient } from '@prisma/client';
import { assertDestructiveDbScriptAllowed } from './script-safety';

assertDestructiveDbScriptAllowed('reset-db');

const prisma = new PrismaClient();

async function reset() {
  console.log('Deleting architecture-lab candle data...');
  const deleted1m = await prisma.candle1m.deleteMany();
  console.log(`Deleted ${deleted1m.count} Candle1m rows.`);
}

reset()
  .then(() => prisma.$disconnect())
  .catch(async (e) => {
    console.error(e);
    await prisma.$disconnect();
    process.exit(1);
  });
