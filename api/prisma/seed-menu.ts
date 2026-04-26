/**
 * Standalone скрипт: добавляет демо-меню для всех существующих филиалов,
 * не трогая других данных. Запуск: `npx ts-node prisma/seed-menu.ts`
 */
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

const DEMO_MENU = [
  { name: 'Beshqozon shashlik (4 dona)', description: 'Mol go\'shti, tutun ta\'mi', category: 'Asosiy', price: 65000, sortOrder: 10 },
  { name: 'Tovuq tabaka', description: 'Lavash bilan', category: 'Asosiy', price: 75000, sortOrder: 20 },
  { name: 'Manti (5 dona)', description: 'Qatiq bilan', category: 'Asosiy', price: 45000, sortOrder: 30 },
  { name: 'Norin', description: 'Klassik retsept', category: 'Asosiy', price: 55000, sortOrder: 40 },
  { name: 'Achchiq-chuchuk salat', description: 'Pomidor, piyoz, achchiq', category: 'Salat', price: 22000, sortOrder: 50 },
  { name: 'Olivier', category: 'Salat', price: 28000, sortOrder: 60 },
  { name: 'Lag\'mon (1 kosa)', description: 'Mol go\'shti', category: 'Birinchi taom', price: 38000, sortOrder: 70 },
  { name: 'Mastava', description: 'Yorma, mol go\'shti', category: 'Birinchi taom', price: 32000, sortOrder: 80 },
  { name: 'Coca-Cola 0.5L', category: 'Ichimliklar', price: 12000, sortOrder: 90 },
  { name: 'Yashil choy', category: 'Ichimliklar', price: 8000, sortOrder: 100 },
  { name: 'Limonad', category: 'Ichimliklar', price: 18000, sortOrder: 110 },
  { name: 'Medovik tort (1 bo\'lak)', category: 'Shirinliklar', price: 25000, sortOrder: 120 },
] as const;

async function main() {
  const branches = await prisma.branch.findMany({ select: { id: true, name: true } });
  for (const b of branches) {
    const existing = await prisma.menuItem.count({ where: { branchId: b.id } });
    if (existing > 0) {
      console.log(`[skip] ${b.name}: ${existing} items already`);
      continue;
    }
    await prisma.menuItem.createMany({
      data: DEMO_MENU.map((m) => ({ ...m, branchId: b.id })),
    });
    console.log(`[ok] ${b.name}: +${DEMO_MENU.length} items`);
  }
}

main()
  .then(() => prisma.$disconnect())
  .catch(async (e) => {
    console.error(e);
    await prisma.$disconnect();
    process.exit(1);
  });
