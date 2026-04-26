/**
 * Базовый URL API.
 *
 * Логика:
 *  1) Если задан NEXT_PUBLIC_API_URL — используем его (например, для прода).
 *  2) Иначе в браузере определяем API по hostname текущей страницы:
 *     - открыли web на localhost  -> API на localhost:4000
 *     - открыли web на 192.168.x  -> API на том же IP, порт 4000
 *     Это позволяет одному билду работать и с компьютера, и с телефона
 *     в одной Wi-Fi сети без правок .env.
 *  3) На сервере (SSR) fallback на localhost:4000.
 */
export function apiBase(): string {
  const env = process.env.NEXT_PUBLIC_API_URL;
  if (env && env.trim()) return env.replace(/\/$/, "");

  // На проде без NEXT_PUBLIC_API_URL фолбэк на window.location:4000
  // даст неработающий URL вида https://my-app.vercel.app:4000/api.
  // Лучше явно сообщить разработчику и не плодить странных запросов.
  if (process.env.NODE_ENV === "production") {
    if (typeof window !== "undefined") {
      console.error(
        "[api] NEXT_PUBLIC_API_URL is not set. Set it in your hosting env vars.",
      );
    }
    return "/api"; // относительный путь — пусть упадёт на этом же домене, чем на :4000
  }

  if (typeof window !== "undefined") {
    const host = window.location.hostname || "localhost";
    return `http://${host}:4000/api`;
  }
  return "http://localhost:4000/api";
}

/** Socket.IO server (Nest root, /api emas) */
export function wsOrigin(): string {
  return apiBase().replace(/\/api$/, "");
}

export function apiUrl(path: string): string {
  const p = path.startsWith("/") ? path : `/${path}`;
  return `${apiBase()}${p}`;
}

export async function apiFetch<T>(
  path: string,
  init?: RequestInit & { token?: string | null },
): Promise<T> {
  const { token, headers: hdrs, ...rest } = (init ?? {}) as RequestInit & { token?: string | null };
  const headers = new Headers(hdrs);
  headers.set("Content-Type", "application/json");
  if (token) headers.set("Authorization", `Bearer ${token}`);
  const res = await fetch(apiUrl(path), { ...rest, headers });
  if (!res.ok) {
    const text = await res.text();
    let msg = text;
    try {
      const j = JSON.parse(text) as { message?: string | string[] };
      msg = Array.isArray(j.message) ? j.message.join(", ") : j.message ?? text;
    } catch {
      /* ignore */
    }
    throw new Error(msg || `HTTP ${res.status}`);
  }
  if (res.status === 204) return undefined as T;
  return res.json() as Promise<T>;
}
