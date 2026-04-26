import {
  PrismaClient,
  TableType,
  UserRole,
  ZoneType,
} from '@prisma/client';

const prisma = new PrismaClient();

const workHours = { open: '10:00', close: '22:00' };

export async function main() {
  await prisma.payment.deleteMany();
  await prisma.reservation.deleteMany();
  await prisma.restaurantTable.deleteMany();
  await prisma.zone.deleteMany();
  await prisma.branch.deleteMany();
  await prisma.user.deleteMany();

  const b1 = await prisma.branch.create({
    data: {
      name: 'Glinka',
      address: "Toshkent, Glinka ko'chasi, 41A",
      workHours,
    },
  });
  const b2 = await prisma.branch.create({
    data: {
      name: 'Nurafshon',
      address: "Shayxontohur tumani, 3-Aklan ko'chasi, 1",
      workHours,
    },
  });
  const b3 = await prisma.branch.create({
    data: {
      name: 'Sergeli',
      address: 'Toshkent, Sergeli tumani, Sohibqiron MFY',
      workHours,
    },
  });
  const b4 = await prisma.branch.create({
    data: {
      name: 'Yunusobod',
      address: "Iftixor ko'chasi, 1, Yunusobod tumani",
      workHours,
    },
  });

  const z1 = await prisma.zone.create({
    data: {
      branchId: b1.id,
      name: '1-qavat (ichki zal)',
      type: ZoneType.INDOOR,
      floor: 1,
    },
  });
  await prisma.zone.create({
    data: {
      branchId: b1.id,
      name: 'VIP xonalar',
      type: ZoneType.VIP,
      floor: 2,
    },
  });

  await prisma.restaurantTable.createMany({
    data: [
      { zoneId: z1.id, number: 'T-101', seats: 4, type: TableType.STANDARD, xPos: 80, yPos: 70, shape: 'rect' },
      { zoneId: z1.id, number: 'T-102', seats: 2, type: TableType.STANDARD, xPos: 200, yPos: 70, shape: 'rect' },
      { zoneId: z1.id, number: 'T-103', seats: 6, type: TableType.FAMILY, xPos: 320, yPos: 70, shape: 'rect' },
      { zoneId: z1.id, number: 'V-201', seats: 4, type: TableType.VIP, xPos: 140, yPos: 160, shape: 'rect' },
    ],
  });

  await prisma.user.create({
    data: {
      phone: '+998901112233',
      name: 'Admin',
      role: UserRole.ADMIN,
      smsVerified: true,
    },
  });

  await prisma.zone.create({
    data: { branchId: b2.id, name: 'Asosiy zal', type: ZoneType.INDOOR, floor: 1 },
  });
  await prisma.zone.create({
    data: { branchId: b3.id, name: 'Asosiy zal', type: ZoneType.INDOOR, floor: 1 },
  });
  await prisma.zone.create({
    data: { branchId: b4.id, name: 'Asosiy zal', type: ZoneType.INDOOR, floor: 1 },
  });

  // Демо-меню для каждого филиала
  const demoMenu: Array<{
    name: string;
    description?: string;
    category: string;
    price: number;
    sortOrder: number;
  }> = [
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
  ];
  for (const branchId of [b1.id, b2.id, b3.id, b4.id]) {
    await prisma.menuItem.createMany({
      data: demoMenu.map((m) => ({ ...m, branchId })),
    });
  }
}

if (require.main === module) {
  main()
    .then(() => prisma.$disconnect())
    .catch(async (e) => {
      console.error(e);
      await prisma.$disconnect();
      process.exit(1);
    });
}
