/**
 * WEB_ORIGIN: bir yoki vergul bilan ajratilgan bir nechta manzillar.
 * Masalan: http://localhost:3000 yoki https://app.example.com,https://admin.example.com
 */
export function parseWebOrigins(raw: string | undefined): string[] {
  const s = (raw ?? 'http://localhost:3000').trim();
  return s
    .split(',')
    .map((x) => x.trim())
    .filter(Boolean);
}

export function isOriginAllowed(origin: string | undefined, allowed: string[]): boolean {
  if (!origin) return true;
  for (const a of allowed) {
    if (a === '*') return true;
    if (a === origin) return true;
    if (a.includes('*')) {
      // Поддержка wildcards в духе http://192.168.*:3000
      const pattern = '^' + a.replace(/[.+?^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*') + '$';
      if (new RegExp(pattern).test(origin)) return true;
    }
  }
  return false;
}
