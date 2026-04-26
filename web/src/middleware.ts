import { NextResponse } from "next/server";

function securityHeaders(res: NextResponse) {
  res.headers.set("X-Frame-Options", "DENY");
  res.headers.set("X-Content-Type-Options", "nosniff");
  res.headers.set("Referrer-Policy", "strict-origin-when-cross-origin");
  res.headers.set("X-DNS-Prefetch-Control", "off");
  res.headers.set("Permissions-Policy", "camera=(), microphone=(), geolocation=(), interest-cohort=()");
  if (process.env.NEXT_PUBLIC_ENABLE_HSTS === "true") {
    res.headers.set("Strict-Transport-Security", "max-age=15552000; includeSubDomains");
  }
}

export function middleware() {
  const res = NextResponse.next();
  securityHeaders(res);

  if (process.env.NODE_ENV === "production") {
    const api = process.env.NEXT_PUBLIC_API_URL ?? "";
    let connect = "'self'";
    try {
      if (api.startsWith("http")) {
        const u = new URL(api);
        connect = `'self' ${u.origin} ws://${u.hostname}:* wss://${u.hostname}:*`;
      }
    } catch {
      connect = "'self' https: wss:";
    }
    res.headers.set(
      "Content-Security-Policy",
      [
        "default-src 'self'",
        "script-src 'self' 'unsafe-inline' 'unsafe-eval'",
        "style-src 'self' 'unsafe-inline'",
        // QR-коды, аватарки филиалов, картинки меню — могут быть с любых HTTPS-доменов
        "img-src 'self' blob: data: https:",
        "font-src 'self' data:",
        `connect-src ${connect}`,
        "frame-ancestors 'none'",
        "base-uri 'self'",
        "form-action 'self'",
      ].join("; "),
    );
  }

  return res;
}

export const config = {
  matcher: [
    // Пропускаем всю статику Next.js и часто запрашиваемые файлы — middleware
    // не выполняется → переходы между страницами быстрее.
    "/((?!_next/static|_next/image|_next/data|favicon.ico|robots.txt|sitemap.xml|.*\\..*).*)",
  ],
};
