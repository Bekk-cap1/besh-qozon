export default function Loading() {
  return (
    <div className="mx-auto max-w-5xl animate-pulse px-4 py-8">
      {/* Шаги */}
      <div className="mb-8 flex items-center gap-3">
        {Array.from({ length: 7 }).map((_, i) => (
          <div
            key={i}
            className="h-9 w-9 shrink-0 rounded-full bg-[color:var(--muted-2)]"
          />
        ))}
      </div>
      {/* Контент шага */}
      <div className="mb-6 h-10 w-80 max-w-full rounded-lg bg-[color:var(--muted-2)]" />
      <div className="mb-8 h-4 w-96 max-w-full rounded bg-[color:var(--muted-2)]" />
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {Array.from({ length: 6 }).map((_, i) => (
          <div
            key={i}
            className="h-44 rounded-2xl border border-[color:var(--border)] bg-[color:var(--bg-soft)]"
          />
        ))}
      </div>
    </div>
  );
}
