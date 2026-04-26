/** Скелет-страница — показывается мгновенно до загрузки реального контента. */
export function PageSkeleton({ rows = 3 }: { rows?: number }) {
  return (
    <div className="mx-auto max-w-6xl animate-pulse px-4 py-8 sm:px-6 lg:px-8">
      <div className="mb-6 h-8 w-64 rounded-lg bg-[color:var(--muted-2)]" />
      <div className="mb-8 h-4 w-96 max-w-full rounded bg-[color:var(--muted-2)]" />
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {Array.from({ length: rows * 3 }).map((_, i) => (
          <div
            key={i}
            className="h-40 rounded-2xl border border-[color:var(--border)] bg-[color:var(--bg-soft)]"
          />
        ))}
      </div>
    </div>
  );
}

export function ListSkeleton({ rows = 6 }: { rows?: number }) {
  return (
    <div className="mx-auto max-w-4xl animate-pulse px-4 py-8 sm:px-6">
      <div className="mb-6 h-8 w-48 rounded-lg bg-[color:var(--muted-2)]" />
      <div className="space-y-3">
        {Array.from({ length: rows }).map((_, i) => (
          <div
            key={i}
            className="h-20 rounded-2xl border border-[color:var(--border)] bg-[color:var(--bg-soft)]"
          />
        ))}
      </div>
    </div>
  );
}

export function AdminSkeleton() {
  return (
    <div className="flex min-h-screen animate-pulse">
      <div className="hidden w-64 shrink-0 border-r border-[color:var(--border)] bg-[color:var(--bg-soft)] p-4 md:block">
        <div className="mb-6 h-8 w-32 rounded bg-[color:var(--muted-2)]" />
        {Array.from({ length: 7 }).map((_, i) => (
          <div key={i} className="mb-2 h-10 rounded-lg bg-[color:var(--muted-2)]/60" />
        ))}
      </div>
      <div className="flex-1 p-6">
        <div className="mb-6 h-8 w-64 rounded-lg bg-[color:var(--muted-2)]" />
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <div
              key={i}
              className="h-28 rounded-2xl border border-[color:var(--border)] bg-[color:var(--bg-soft)]"
            />
          ))}
        </div>
        <div className="mt-6 h-72 rounded-2xl border border-[color:var(--border)] bg-[color:var(--bg-soft)]" />
      </div>
    </div>
  );
}
