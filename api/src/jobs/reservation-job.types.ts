export type BeshJob =
  | { type: 'EXPIRE_HOLD'; reservationId: string }
  | { type: 'REMINDER_SMS'; reservationId: string }
  | { type: 'NO_SHOW'; reservationId: string };

export type SmsJob = { phone: string; text: string };
