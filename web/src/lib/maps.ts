/**
 * Сформировать URL-ы на карты. Если есть координаты — используем геометку,
 * иначе — поиск по адресу.
 */
export type MapLinks = {
  yandex: string;
  google: string;
  primary: string;
  label: string;
};

export function buildMapLinks(opts: {
  name?: string | null;
  address?: string | null;
  lat?: number | null;
  lng?: number | null;
}): MapLinks | null {
  const hasCoords =
    typeof opts.lat === "number" &&
    typeof opts.lng === "number" &&
    !Number.isNaN(opts.lat) &&
    !Number.isNaN(opts.lng);
  const q = [opts.name, opts.address].filter(Boolean).join(", ").trim();
  if (!hasCoords && !q) return null;

  const yandex = hasCoords
    ? `https://yandex.uz/maps/?pt=${opts.lng},${opts.lat}&z=17&l=map&rtext=~${opts.lat},${opts.lng}`
    : `https://yandex.uz/maps/?text=${encodeURIComponent(q)}`;

  const google = hasCoords
    ? `https://www.google.com/maps/search/?api=1&query=${opts.lat},${opts.lng}`
    : `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(q)}`;

  return {
    yandex,
    google,
    primary: yandex,
    label: hasCoords ? "Xaritada ko'rish" : "Manzilni ko'rish",
  };
}
