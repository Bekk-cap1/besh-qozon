export const DEPOSIT_UZS_DEFAULT = 50_000;
export const DEPOSIT_UZS_VIP = 100_000;
export const SLOT_STEP_MINUTES = 30;
/** Длительность брони по умолчанию (если гость не выбрал). */
export const RESERVATION_DURATION_MINUTES = 120;
/** Гость может выбрать, сколько хочет сидеть (минуты). Кратно SLOT_STEP_MINUTES. */
export const MIN_DURATION_MINUTES = 60;
export const MAX_DURATION_MINUTES = 240;
/** Допустимые варианты длительности для выбора гостем. */
export const DURATION_OPTIONS_MINUTES = [60, 90, 120, 150, 180, 240];
/** За сколько минут до конца слота отправить мягкое напоминание гостю. */
export const SLOT_ENDING_REMINDER_MINUTES = 15;
export const HOLD_MINUTES = 10;
export const MAX_DAYS_AHEAD = 14;
export const MAX_GUESTS = 20;
export const MAX_ACTIVE_RESERVATIONS_PER_DAY = 3;

/** Сколько завершённых броней = +1 бонус (бесплатный депозит). */
export const LOYALTY_THRESHOLD = 5;
/** Номинал бонуса в UZS — списывается из депозита при использовании. */
export const LOYALTY_BONUS_VALUE_UZS = 50_000;
