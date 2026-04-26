/**
 * Показывается при переходах между admin-маршрутами.
 * Sidebar остаётся от layout, заменяется только main-area (topbar + контент).
 */
export default function Loading() {
  return (
    <>
      <header className="sticky top-0 z-20 flex items-center gap-3 border-b border-[color:var(--border)] bg-white/90 px-4 py-3 backdrop-blur lg:px-8">
        <div className="h-10 w-10 rounded-xl bg-[color:var(--muted-2)]/40 lg:hidden" />
        <div className="min-w-0 flex-1 space-y-1.5">
          <div className="h-5 w-48 rounded bg-[color:var(--muted-2)]/50" />
          <div className="h-3 w-64 max-w-full rounded bg-[color:var(--muted-2)]/30" />
        </div>
      </header>
      <main className="flex-1 animate-pulse px-4 py-6 lg:px-8 lg:py-8">
        <div className="mb-6 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <div
              key={i}
              className="h-28 rounded-2xl border border-[color:var(--border)] bg-white"
            />
          ))}
        </div>
        <div className="h-72 rounded-2xl border border-[color:var(--border)] bg-white" />
        <div className="mt-6 grid gap-3 sm:grid-cols-2">
          <div className="h-40 rounded-2xl border border-[color:var(--border)] bg-white" />
          <div className="h-40 rounded-2xl border border-[color:var(--border)] bg-white" />
        </div>
      </main>
    </>
  );
}
