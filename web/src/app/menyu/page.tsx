import Link from "next/link";

export const metadata = {
  title: "Menyu — Beshqozon",
  description: "Beshqozon taomnomasi: plov, shashlik, salatlar, ichimliklar.",
};

export default function MenyuPage() {
  return (
    <div className="bq-container py-16">
      <div className="mx-auto max-w-2xl text-center">
        <span className="bq-chip">Menyu</span>
        <h1 className="mt-4 text-4xl font-extrabold tracking-tight text-[color:var(--fg)] md:text-5xl">
          Taomnoma tez orada
        </h1>
        <p className="mt-4 text-[color:var(--muted)]">
          Biz menyuni to&apos;liq katalog ko&apos;rinishida tayyorlayapmiz —
          plov, shashlik, milliy salatlar va ichimliklar. Shu bilan birga
          bron qilish chog&apos;ida oldindan buyurtma berish imkoniyati ham
          qo&apos;shiladi.
        </p>
        <div className="mt-8 flex flex-wrap justify-center gap-3">
          <Link href="/bron" className="bq-btn bq-btn-primary px-7">
            Stol bron qilish
          </Link>
          <Link href="/" className="bq-btn bq-btn-ghost px-7">
            Bosh sahifa
          </Link>
        </div>
      </div>
    </div>
  );
}
