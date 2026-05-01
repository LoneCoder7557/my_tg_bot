PATCH15_FINAL — Anime Battle Multiverse🌒

Главное:
- Это полный проект, не replace-only патч.
- Загружать нужно содержимое архива в корень GitHub-репозитория.
- Сам ZIP в GitHub НЕ загружать.
- /var/data и Persistent Disk на Render не трогать.

Структура архива:
bot.py
cards.json
requirements.txt
Procfile
.gitignore
promo_codes.json
owner_ids.txt
README_PATCH15_FINAL.txt
media/
media_packs/

Что исправлено в PATCH15:
- Короткий красивый /start: “🌌 Anime Battle Multiverse — Выбери свой путь.”
- Главное меню очищено: Режимы, Коллекция, Награды, Профиль, Правила, Путь Луфи для новичков.
- Подсказки перенесены внутрь разделов.
- Луфи-путь на 10 дней: личный прогресс, вступление один раз, галочки дней, safe-migration по уже выданным картам.
- Бесплатный сундук: раз в 3 часа, выдаёт карту/персонажа, дубли превращает в фрагменты.
- Уведомления: notify_free_pack включён по умолчанию, статус и кнопка в профиле.
- Бои усилены: сила, редкость, уровень, HP, артефакты, арена, роль, синергия, плюс/минус и случайность.
- Перед боями добавлен выбор: своя колода, авто-колода, ручной выбор.
- Коллекция и карточка персонажа оформлены чище; ID скрыты для обычных игроков.
- Награды/магазин очищены; Stars, кейсы, промокоды и мультипасс спрятаны внутрь донат-раздела.
- Профиль очищен; правила вынесены отдельно; владелец отображается как владелец мультивселенной.
- Админка считает всех игроков из DATA["users"], не скрывает старых без last_seen.
- Убран риск Bad Request: button user privacy restricted — список игроков не использует обязательные tg://user?id кнопки.
- /storage показывает DATA_DIR, DATA_FILE, DB_FILE, LOG_FILE, /var/data, количество игроков, JSON/DB, last_save и unknown cards.
- Режимы, арены, артефакты, рейды и оспаривание боя дооформлены.
- Добавлена команда владельца /compensate_patch15 с защитой от повторной выдачи.
- Валюта для игроков: 💎 Фисташки и 🐉 Драконит. Старое название эссенции убрано из интерфейса.
- Редкости показываются как Origin/Rare/Epic/Legendary/Absolute, внутренние ключи сохранены русскими.

Render настройки:
Root Directory: пустой
Build Command: pip install -r requirements.txt
Start Command: python bot.py

Environment:
BOT_TOKEN = твой токен в Render
DATA_DIR = /var/data

Persistent Disk:
mount path = /var/data

Важно по данным:
- НЕ удалять Persistent Disk.
- НЕ удалять /var/data.
- НЕ загружать anime_battle_data.json или anime_battle_data.db в GitHub.
- DATA_FILE должен оставаться /var/data/anime_battle_data.json.
- DB_FILE должен оставаться /var/data/anime_battle_data.db.
- LOG_FILE должен оставаться /var/data/bot_runtime.log.
- Если DATA_DIR не /var/data, бот предупредит в /storage, но не упадёт.

Как заливать в GitHub:
1. Скачать PATCH15_FINAL_FULL_PROJECT.zip.
2. Распаковать архив.
3. Открыть GitHub repository.
4. Если нужна чистая установка кода — удалить мусорные старые файлы в репозитории, но НЕ трогать Render /var/data.
5. Загрузить СОДЕРЖИМОЕ распакованной папки в корень репозитория.
6. Сделать Commit changes.
7. Render → Manual Deploy → Clear build cache & deploy.

После деплоя проверить в Telegram:
/start
Профиль
Коллекция
Награды
Бесплатный сундук
Путь Луфи
Режимы
Арена
Рейд
Админка
Все игроки
/storage
/compensate_patch15

Компенсация:
- /compensate_patch15 запускает только владелец.
- Выдаёт игрокам один раз:
  +2000 💎 Фисташек
  +20 🐉 Драконита
  +600 очков мультипасса
- Owner пропускается для чистой статистики.
- Повторный запуск не дублирует награду.

Проверки перед сборкой архива:
- bot.py компилируется.
- cards.json валидный.
- Дублей id в cards.json нет.
- Статические callback_data не длиннее 64 байт.
- Конфликтующих callback-хендлеров не найдено.
- tg://user?id в обязательных кнопках списка игроков не используется.
- owner_ids.txt лежит в корне.
- Procfile сохранён: web: python bot.py.
