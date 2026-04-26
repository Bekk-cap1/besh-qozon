import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "standalone",
  reactStrictMode: true,

  // Сжимаем логи dev — убираем лишние сообщения о каждой HMR-пересборке
  logging: {
    fetches: {
      fullUrl: false,
    },
  },

  // Отключаем ESLint-проверку на этапе build (dev остаётся как есть).
  // Это в 2-3 раза ускоряет npm run build.
  eslint: {
    ignoreDuringBuilds: true,
  },

  experimental: {
    // Tree-shake иконочных и date-fns-подобных пакетов быстрее.
    optimizePackageImports: [],
  },

  // Разрешаем обращение к dev-серверу с других хостов в локальной сети
  // (например, с телефона по http://192.168.x.x:3000 или :3001).
  allowedDevOrigins: ["localhost", "127.0.0.1", "192.168.*", "10.*"],
};

export default nextConfig;
