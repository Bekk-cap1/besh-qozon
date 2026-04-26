export type Lang = "uz" | "ru";

export const STRINGS = {
  uz: {
    "nav.menu": "Menyu",
    "nav.book": "Bron qilish",
    "nav.branches": "Filiallar",
    "nav.my": "Bronlarim",
    "nav.profile": "Profil",
    "nav.logout": "Chiqish",
    "nav.admin": "Admin",

    "hero.title": "Beshqozon — stolni oldindan bron qiling",
    "hero.sub": "Filial tanlang, sana va vaqtni belgilang, stolni tanlang — hammasi 7 qadamda.",
    "hero.cta": "Bron qilish",

    "steps.branch": "Filial",
    "steps.datetime": "Sana va vaqt",
    "steps.guests": "Mehmonlar",
    "steps.zone": "Zona",
    "steps.table": "Stol",
    "steps.login": "Kirish",
    "steps.confirm": "Tasdiqlash",

    "common.back": "Orqaga",
    "common.next": "Keyingi",
    "common.save": "Saqlash",
    "common.cancel": "Bekor qilish",
    "common.loading": "Yuklanmoqda…",

    "profile.title": "Profilim",
    "profile.name": "Ismingiz",
    "profile.edit": "Tahrirlash",
    "profile.stats.bookings": "Jami bronlar",
    "profile.stats.bonuses": "Bonuslar",
    "profile.stats.noshow": "No-show",
  },
  ru: {
    "nav.menu": "Меню",
    "nav.book": "Забронировать",
    "nav.branches": "Филиалы",
    "nav.my": "Мои брони",
    "nav.profile": "Профиль",
    "nav.logout": "Выход",
    "nav.admin": "Админ",

    "hero.title": "Beshqozon — забронируйте столик заранее",
    "hero.sub": "Выберите филиал, дату, время и столик — всё в 7 шагов.",
    "hero.cta": "Забронировать",

    "steps.branch": "Филиал",
    "steps.datetime": "Дата и время",
    "steps.guests": "Гости",
    "steps.zone": "Зона",
    "steps.table": "Столик",
    "steps.login": "Вход",
    "steps.confirm": "Подтверждение",

    "common.back": "Назад",
    "common.next": "Далее",
    "common.save": "Сохранить",
    "common.cancel": "Отмена",
    "common.loading": "Загрузка…",

    "profile.title": "Мой профиль",
    "profile.name": "Ваше имя",
    "profile.edit": "Изменить",
    "profile.stats.bookings": "Всего броней",
    "profile.stats.bonuses": "Бонусы",
    "profile.stats.noshow": "No-show",
  },
} as const;

export type StringKey = keyof (typeof STRINGS)["uz"];
