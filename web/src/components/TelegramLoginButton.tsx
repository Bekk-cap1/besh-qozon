"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useAuth } from "@/context/AuthContext";

type Stage =
  | { kind: "idle" }
  | { kind: "starting" }
  | { kind: "waiting"; deepLink: string; botUsername: string; loginToken: string }
  | { kind: "started"; deepLink: string; botUsername: string; loginToken: string }
  | { kind: "linked" }
  | { kind: "expired"; message: string }
  | { kind: "disabled"; message: string }
  | { kind: "error"; message: string };

/**
 * Кнопка «Kirish Telegram orqali».
 * Открывает модалку с QR (deep link), стартует поллинг статуса.
 * По готовности проставляет сессию через AuthContext.
 */
export function TelegramLoginButton({
  className,
  onSuccess,
}: {
  className?: string;
  onSuccess?: () => void;
}) {
  const { telegramInit, telegramStatus } = useAuth();
  const [stage, setStage] = useState<Stage>({ kind: "idle" });
  const [open, setOpen] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const stopPoll = useCallback(() => {
    if (pollRef.current) clearInterval(pollRef.current);
    pollRef.current = null;
  }, []);

  const closeModal = useCallback(() => {
    stopPoll();
    setOpen(false);
    setStage({ kind: "idle" });
  }, [stopPoll]);

  // Poll loop
  useEffect(() => {
    if (stage.kind !== "waiting" && stage.kind !== "started") return;
    const { loginToken } = stage;

    let stopped = false;
    const tick = async () => {
      try {
        const r = await telegramStatus(loginToken);
        if (stopped) return;
        if (r.status === "linked") {
          setStage({ kind: "linked" });
          stopPoll();
          setTimeout(() => {
            onSuccess?.();
            closeModal();
          }, 700);
        } else if (r.status === "started" && stage.kind === "waiting") {
          setStage((s) =>
            s.kind === "waiting"
              ? { kind: "started", deepLink: s.deepLink, botUsername: s.botUsername, loginToken: s.loginToken }
              : s,
          );
        } else if (r.status === "expired") {
          setStage({
            kind: "expired",
            message: "Havola muddati tugadi. Qaytadan urinib ko'ring.",
          });
          stopPoll();
        }
      } catch (e) {
        if (!stopped) {
          setStage({ kind: "error", message: (e as Error).message });
          stopPoll();
        }
      }
    };

    pollRef.current = setInterval(tick, 2000);
    void tick();
    return () => {
      stopped = true;
      stopPoll();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [stage.kind === "waiting" || stage.kind === "started" ? (stage as { loginToken: string }).loginToken : null]);

  const start = useCallback(async () => {
    setOpen(true);
    setStage({ kind: "starting" });
    try {
      const r = await telegramInit();
      if (!r.enabled) {
        setStage({
          kind: "disabled",
          message:
            "Telegram login hozircha yoqilmagan. Administrator bilan bog'laning yoki telefon + SMS orqali kiring.",
        });
        return;
      }
      setStage({
        kind: "waiting",
        deepLink: r.deepLink,
        botUsername: r.botUsername,
        loginToken: r.loginToken,
      });
    } catch (e) {
      setStage({ kind: "error", message: (e as Error).message });
    }
  }, [telegramInit]);

  return (
    <>
      <button
        type="button"
        onClick={() => void start()}
        className={
          className ??
          "flex h-11 w-full items-center justify-center gap-2 rounded-xl border-2 border-[#26A5E4]/30 bg-white px-4 font-semibold text-[#1E7BB8] transition hover:border-[#26A5E4] hover:bg-[#26A5E4]/5"
        }
      >
        <TelegramIcon />
        Telegram orqali kirish
      </button>

      {open ? (
        <Modal onClose={closeModal}>
          <TelegramLoginModalContent stage={stage} onRetry={start} onClose={closeModal} />
        </Modal>
      ) : null}
    </>
  );
}

function TelegramLoginModalContent({
  stage,
  onRetry,
  onClose,
}: {
  stage: Stage;
  onRetry: () => void;
  onClose: () => void;
}) {
  if (stage.kind === "starting" || stage.kind === "idle") {
    return (
      <div className="flex flex-col items-center gap-3 py-6 text-center">
        <Spinner />
        <div className="text-sm text-[color:var(--muted)]">Havola tayyorlanmoqda…</div>
      </div>
    );
  }

  if (stage.kind === "disabled" || stage.kind === "error" || stage.kind === "expired") {
    return (
      <div className="flex flex-col items-center gap-3 py-4 text-center">
        <div className="flex h-12 w-12 items-center justify-center rounded-full bg-amber-100 text-amber-700">
          <svg viewBox="0 0 24 24" className="h-6 w-6" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M12 9v4M12 17h.01" />
            <circle cx="12" cy="12" r="10" />
          </svg>
        </div>
        <div className="text-sm text-[color:var(--fg)]">{stage.message}</div>
        <div className="flex gap-2">
          {stage.kind !== "disabled" ? (
            <button onClick={onRetry} className="bq-btn bq-btn-primary h-10 px-4 text-sm">
              Qayta urinish
            </button>
          ) : null}
          <button onClick={onClose} className="bq-btn bq-btn-secondary h-10 px-4 text-sm">
            Yopish
          </button>
        </div>
      </div>
    );
  }

  if (stage.kind === "linked") {
    return (
      <div className="flex flex-col items-center gap-3 py-6 text-center">
        <div className="flex h-14 w-14 items-center justify-center rounded-full bg-emerald-100 text-emerald-700">
          <svg viewBox="0 0 24 24" className="h-7 w-7" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
            <path d="M5 12l4.5 4.5L19 7" />
          </svg>
        </div>
        <div className="text-base font-semibold text-[color:var(--fg)]">Muvaffaqiyatli kirildi!</div>
        <div className="text-sm text-[color:var(--muted)]">Davom etamiz…</div>
      </div>
    );
  }

  // waiting or started
  const waiting = stage.kind === "waiting";
  const { deepLink, botUsername } = stage;
  const qrUrl = `https://api.qrserver.com/v1/create-qr-code/?size=220x220&margin=8&data=${encodeURIComponent(deepLink)}`;

  return (
    <div className="flex flex-col gap-4">
      <div className="flex items-center gap-3">
        <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-[#26A5E4] text-white">
          <TelegramIcon />
        </div>
        <div>
          <div className="text-base font-semibold text-[color:var(--fg)]">Telegram orqali kirish</div>
          <div className="text-xs text-[color:var(--muted)]">@{botUsername}</div>
        </div>
      </div>

      <div className="grid gap-4 sm:grid-cols-[220px_1fr]">
        <div className="mx-auto overflow-hidden rounded-xl border border-[color:var(--border)] bg-white p-2">
          {/* eslint-disable-next-line @next/next/no-img-element */}
          <img src={qrUrl} alt="QR" width={220} height={220} className="block" />
        </div>
        <ol className="list-inside list-decimal space-y-2 text-sm text-[color:var(--fg)]">
          <li>
            Tugmani bosing yoki QR-kodni Telegram bilan skanerlang.
          </li>
          <li>
            Botda <b>«START»</b> tugmasini bosing.
          </li>
          <li>
            So'ralganda <b>«📱 Raqamni yuborish»</b> tugmasi bilan telefon raqamingizni yuboring.
          </li>
        </ol>
      </div>

      <a
        href={deepLink}
        target="_blank"
        rel="noreferrer"
        className="bq-btn bq-btn-primary h-11 w-full"
      >
        Telegramni ochish
      </a>

      <div className="flex items-center justify-center gap-2 text-xs text-[color:var(--muted)]">
        {waiting ? (
          <>
            <Spinner small />
            Kutilmoqda: botga START bosing…
          </>
        ) : (
          <>
            <Spinner small />
            Telefon raqamini kutmoqdamiz…
          </>
        )}
      </div>

      <button onClick={onClose} className="text-xs text-[color:var(--muted)] hover:underline">
        Bekor qilish
      </button>
    </div>
  );
}

function Modal({ children, onClose }: { children: React.ReactNode; onClose: () => void }) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    document.body.style.overflow = "hidden";
    return () => {
      window.removeEventListener("keydown", onKey);
      document.body.style.overflow = "";
    };
  }, [onClose]);

  return (
    <div
      role="dialog"
      aria-modal="true"
      className="fixed inset-0 z-[60] flex items-center justify-center bg-black/50 p-4"
      onClick={onClose}
    >
      <div
        className="w-full max-w-md rounded-2xl bg-white p-5 shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        {children}
      </div>
    </div>
  );
}

function Spinner({ small }: { small?: boolean }) {
  const size = small ? "h-3 w-3" : "h-6 w-6";
  return (
    <span
      aria-hidden
      className={`inline-block ${size} animate-spin rounded-full border-2 border-[color:var(--brand)]/30 border-t-[color:var(--brand)]`}
    />
  );
}

function TelegramIcon() {
  return (
    <svg viewBox="0 0 24 24" className="h-5 w-5" fill="currentColor" aria-hidden>
      <path d="M9.78 15.27l-.39 3.83c.56 0 .8-.24 1.09-.53l2.62-2.5 5.43 3.97c1 .55 1.72.26 1.97-.92L22.9 5.4c.35-1.5-.54-2.09-1.5-1.74L3.27 10.9c-1.44.56-1.42 1.37-.25 1.74l4.66 1.45 10.81-6.82c.51-.33.98-.15.6.18L9.78 15.27z" />
    </svg>
  );
}
