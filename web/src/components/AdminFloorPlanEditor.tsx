"use client";

import dynamic from "next/dynamic";
import type { ComponentType } from "react";

export type EditorTable = {
  id: string;
  number: string;
  seats: number;
  type: string;
  status: string;
  xPos: number;
  yPos: number;
};

type Props = {
  zoneId: string;
  zoneName: string;
  token: string;
  tables: EditorTable[];
  onSaved: () => void;
  onSelect?: (id: string | null) => void;
  selectedId?: string | null;
  onStatusChange?: (id: string, status: string) => void;
  onDuplicate?: (id: string) => void;
  onDelete?: (id: string) => void;
  floorConfig?: string | null;
};

const Inner = dynamic<Props>(
  () => import("./AdminFloorPlanEditorInner").then((m) => m.default),
  {
    ssr: false,
    loading: () => (
      <div
        className="flex w-full items-center justify-center rounded-2xl border border-zinc-300 bg-white text-sm text-zinc-500 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-400"
        style={{ minHeight: 480 }}
      >
        Xarita yuklanmoqda…
      </div>
    ),
  },
) as ComponentType<Props>;

export function AdminFloorPlanEditor(props: Props) {
  return <Inner {...props} />;
}
