"use client";

import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent,
} from "react";
import { TelegramLoginButton } from "./TelegramLoginButton";

const OTP_LEN = 6;
const RESEND_SEC = 45;

/** Форматирует любой ввод в строку вида "+998 XX XXX XX XX" (до 12 цифр). */
export function formatUzPhone(raw: string): string {
  const digits = raw.replace(/\D/g, "").replace(/^998/, "");
  const limited = digits.slice(0, 9);
  let out = "+998";
  if (limited.length > 0) out += " " + limited.slice(0, 2);
  if (limited.length > 2) out += " " + limited.slice(2, 5);
  if (limited.length > 5) out += " " + limited.slice(5, 7);
  if (limited.length > 7) out += " " + limited.slice(7, 9);
  return out;
}

function phoneE164(raw: string): string {
  const digits = raw.replace(/\D/g, "");
  if (!digits) return "";
  const withCountry = digits.startsWith("998") ? digits : `998${digits}`;
  return `+${withCountry.slice(0, 12)}`;
}

function isValidUzPhone(raw: string): boolean {
  const digits = raw.replace(/\D/g, "");
  return digits.startsWith("998") ? digits.length === 12 : digits.length === 9;
}

type PhoneOtpStepProps = {
  phone: string;
  onPhoneChange: (v: string) => void;
  code: string;
  onCodeChange: (v: string) => void;
  name: string;
  onNameChange: (v: string) => void;
  onRequestOtp: (phone: string) => Promise<void> | void;
  onVerifyOtp: (phone: string, code: string, name?: string) => Promise<void> | void;
  busy?: boolean;
  hint?: string | null;
  /** Если true — значит пользователь уже авторизован */
  authenticated?: boolean;
  /** Текущий номер авторизованного пользователя (для сообщения) */
  authPhone?: string | null;
};

export function PhoneOtpStep({
  phone,
  onPhoneChange,
  code,
  onCodeChange,
  name,
  onNameChange,
  onRequestOtp,
  onVerifyOtp,
  busy = false,
  hint = null,
  authenticated = false,
  authPhone = null,
}: PhoneOtpStepProps) {
  const [stage, setStage] = useState<"phone" | "code">("phone");
  const [resendLeft, setResendLeft] = useState(0);
  const inputsRef = useRef<Array<HTMLInputElement | null>>([]);

  // Автофокус первого OTP-поля когда появляется стадия кода
  useEffect(() => {
    if (stage === "code") {
      setTimeout(() => inputsRef.current[0]?.focus(), 30);
    }
  }, [stage]);

  // Таймер повторной отправки
  useEffect(() => {
    if (resendLeft <= 0) return;
    const id = setInterval(() => setResendLeft((n) => Math.max(0, n - 1)), 1000);
    return () => clearInterval(id);
  }, [resendLeft]);

  const otpDigits = useMemo(() => {
    const cleaned = code.replace(/\D/g, "").slice(0, OTP_LEN);
    return Array.from({ length: OTP_LEN }, (_, i) => cleaned[i] ?? "");
  }, [code]);

  const canRequest = !busy && isValidUzPhone(phone);
  const canVerify = !busy && otpDigits.every((d) => d !== "");

  const handleRequest = useCallback(async () => {
    if (!canRequest) return;
    try {
      await onRequestOtp(phoneE164(phone));
      setStage("code");
      setResendLeft(RESEND_SEC);
    } catch {
      /* ошибка обрабатывается снаружи */
    }
  }, [canRequest, onRequestOtp, phone]);

  const handleResend = useCallback(async () => {
    if (resendLeft > 0 || busy) return;
    await onRequestOtp(phoneE164(phone));
    setResendLeft(RESEND_SEC);
  }, [busy, onRequestOtp, phone, resendLeft]);

  const handleVerify = useCallback(async () => {
    if (!canVerify) return;
    await onVerifyOtp(phoneE164(phone), otpDigits.join(""), name || undefined);
  }, [canVerify, onVerifyOtp, phone, otpDigits, name]);

  const setDigit = (idx: number, v: string) => {
    const d = v.replace(/\D/g, "").slice(-1);
    const arr = [...otpDigits];
    arr[idx] = d;
    const next = arr.join("");
    onCodeChange(next);
    if (d && idx < OTP_LEN - 1) inputsRef.current[idx + 1]?.focus();
  };

  const onKeyDown = (idx: number) => (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Backspace" && !otpDigits[idx] && idx > 0) {
      inputsRef.current[idx - 1]?.focus();
    } else if (e.key === "ArrowLeft" && idx > 0) {
      inputsRef.current[idx - 1]?.focus();
    } else if (e.key === "ArrowRight" && idx < OTP_LEN - 1) {
      inputsRef.current[idx + 1]?.focus();
    } else if (e.key === "Enter") {
      void handleVerify();
    }
  };

  const onPaste = (e: React.ClipboardEvent<HTMLInputElement>) => {
    const text = e.clipboardData.getData("text").replace(/\D/g, "").slice(0, OTP_LEN);
    if (!text) return;
    e.preventDefault();
    onCodeChange(text);
    if (text.length >= OTP_LEN) inputsRef.current[OTP_LEN - 1]?.focus();
    else inputsRef.current[text.length]?.focus();
  };

  if (authenticated) {
    return (
      <div className="rounded-xl border border-[color:var(--brand-100)] bg-[color:var(--brand-50)] p-4 text-sm text-[color:var(--brand-700)]">
        <div className="flex items-center gap-2">
          <span className="inline-flex h-7 w-7 items-center justify-center rounded-full bg-[color:var(--brand)] text-white">
            ✓
          </span>
          <div>
            Siz tizimga kirdingiz{authPhone ? ` (${authPhone})` : ""}. &laquo;Davom etish&raquo; tugmasini bosing.
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Этап ввода телефона */}
      {stage === "phone" ? (
        <div className="space-y-3">
          <TelegramLoginButton />
          <div className="flex items-center gap-3 text-xs text-[color:var(--muted-2)]">
            <div className="h-px flex-1 bg-[color:var(--border)]" />
            <span>yoki SMS orqali</span>
            <div className="h-px flex-1 bg-[color:var(--border)]" />
          </div>
          <p className="text-sm text-[color:var(--muted)]">
            Telefon raqamingizni kiriting — biz SMS orqali 6-raqamli kod yuboramiz.
          </p>
          <div>
            <label className="mb-1.5 block text-xs font-semibold uppercase tracking-wider text-[color:var(--muted)]">
              Telefon raqami
            </label>
            <div className="flex gap-2">
              <input
                className="bq-input h-12 flex-1 text-lg font-semibold tracking-wider"
                value={formatUzPhone(phone)}
                onChange={(e) => onPhoneChange(formatUzPhone(e.target.value))}
                placeholder="+998 __ ___ __ __"
                inputMode="tel"
                autoComplete="tel"
                onKeyDown={(e) => {
                  if (e.key === "Enter") void handleRequest();
                }}
              />
              <button
                type="button"
                onClick={() => void handleRequest()}
                disabled={!canRequest}
                className="bq-btn bq-btn-primary h-12 min-w-[140px] px-5"
              >
                Kod yuborish
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {/* Этап ввода кода */}
      {stage === "code" ? (
        <div className="space-y-3">
          <div className="flex items-center justify-between gap-2">
            <div>
              <div className="text-xs font-semibold uppercase tracking-wider text-[color:var(--muted)]">
                SMS kodi yuborildi
              </div>
              <div className="mt-0.5 flex items-center gap-2 text-sm">
                <span className="font-semibold text-[color:var(--fg)]">
                  {formatUzPhone(phone)}
                </span>
                <button
                  type="button"
                  onClick={() => setStage("phone")}
                  className="text-xs font-semibold text-[color:var(--brand-700)] underline-offset-2 hover:underline"
                >
                  o&apos;zgartirish
                </button>
              </div>
            </div>
            {resendLeft > 0 ? (
              <div className="text-xs text-[color:var(--muted)]">
                Qayta yuborish:{" "}
                <b className="font-mono text-[color:var(--fg)]">
                  0:{resendLeft.toString().padStart(2, "0")}
                </b>
              </div>
            ) : (
              <button
                type="button"
                onClick={() => void handleResend()}
                disabled={busy}
                className="text-xs font-semibold text-[color:var(--brand-700)] underline-offset-2 hover:underline disabled:opacity-50"
              >
                Qayta yuborish
              </button>
            )}
          </div>

          <div className="flex gap-2">
            {otpDigits.map((d, i) => (
              <input
                key={i}
                ref={(el) => {
                  inputsRef.current[i] = el;
                }}
                value={d}
                onChange={(e) => setDigit(i, e.target.value)}
                onKeyDown={onKeyDown(i)}
                onPaste={onPaste}
                inputMode="numeric"
                pattern="[0-9]*"
                maxLength={1}
                aria-label={`Raqam ${i + 1}`}
                className="h-14 w-12 rounded-xl border-2 border-[color:var(--border)] bg-white text-center text-2xl font-extrabold text-[color:var(--fg)] outline-none transition focus:border-[color:var(--brand)] focus:ring-4 focus:ring-[color:var(--brand-ring)]"
              />
            ))}
          </div>

          {hint ? (
            <div className="rounded-xl border border-amber-300 bg-amber-50 px-3 py-2 text-xs text-amber-900">
              {hint}
            </div>
          ) : null}

          <div>
            <label className="mb-1.5 block text-xs font-semibold uppercase tracking-wider text-[color:var(--muted)]">
              Ism familiya <span className="normal-case text-[color:var(--muted-2)]">(ixtiyoriy)</span>
            </label>
            <input
              className="bq-input h-11 w-full"
              value={name}
              onChange={(e) => onNameChange(e.target.value)}
              placeholder="Masalan: Ali Valiyev"
            />
          </div>

          <button
            type="button"
            onClick={() => void handleVerify()}
            disabled={!canVerify}
            className="bq-btn bq-btn-primary h-12 w-full text-base"
          >
            {busy ? "Tekshirilmoqda…" : "Tasdiqlash va kirish"}
          </button>
        </div>
      ) : null}
    </div>
  );
}
