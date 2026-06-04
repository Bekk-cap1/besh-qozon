/**
 * Безопасно добавляет столы в зоны, где их ещё нет (ничего не удаляет).
 * Запуск: $env:DATABASE_URL="..."; npx ts-node --transpile-only prisma/add-tables.ts
 */
import { PrismaClient, TableType } from '@prisma/client';

const prisma = new PrismaClient();

// Набор столов на зону: места + тип. Позиции расставляем сеткой 4xN.
const LAYOUT: { seats: number; type: TableType }[] = [
  { seats: 2, type: TableType.STANDARD },
  { seats: 2, type: TableType.STANDARD },
  { seats: 4, type: TableType.STANDARD },
  { seats: 4, type: TableType.STANDARD },
  { seats: 4, type: TableType.STANDARD },
  { seats: 6, type: TableType.FAMILY },
  { seats: 6, type: TableType.FAMILY },
  { seats: 8, type: TableType.FAMILY },
];

async function main() {
  const zones = await prisma.zone.findMany({
    include: { _count: { select: { tables: true } }, branch: { select: { name: true } } },
  });

  let created = 0;
  for (const z of zones) {
    if (z._count.tables > 0) continue; // в этой зоне уже есть столы — пропускаем

    const isVip = z.type === 'VIP';
    const prefix = isVip ? 'V' : 'T';
    const data = LAYOUT.map((t, i) => {
      const col = i % 4;
      const row = Math.floor(i / 4);
      return {
        zoneId: z.id,
        number: `${prefix}-${z.id.slice(0, 3)}${(i + 1).toString().padStart(2, '0')}`,
        seats: t.seats,
        type: isVip ? TableType.VIP : t.type,
        xPos: 80 + col * 120,
        yPos: 70 + row * 100,
        shape: 'rect',
      };
    });
    await prisma.restaurantTable.createMany({ data });
    created += data.length;
    console.log(`+ ${data.length} stol → ${z.branch.name} / ${z.name}`);
  }

  console.log(`Tayyor. Jami qo'shildi: ${created} stol.`);
}

main()
  .then(() => prisma.$disconnect())
  .catch(async (e) => {
    console.error(e);
    await prisma.$disconnect();
    process.exit(1);
  });
