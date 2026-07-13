ANIME BATTLE MULTIVERSE
PATCH40_FINAL_STABLE_HOTFIX
storage_version: 40.1 | schema_version: 40

НАЗНАЧЕНИЕ ЭТОГО АРХИВА
Это полный проект для полной замены файлов репозитория, а не набор отдельных изменённых веток. Архив основан на PATCH40_FINAL_MULTIVERSE и сохраняет его Neon/PostgreSQL, payment ledger, сезоны, onboarding, карты, media_packs, команды, callback-алиасы и все игровые режимы.

КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ
Причина сообщения «Действие не завершилось…» воспроизведена фактически.
PATCH40 сохранял часть дат как timezone-aware ISO, например 2026-07-13T12:00:00+00:00, но в get_user_data использовал datetime.now() без часового пояса. При любом действии существующего игрока Python выполнял вычитание aware и naive datetime и выбрасывал:
TypeError: can't subtract offset-naive and offset-aware datetimes

Так как get_user_data вызывается почти в каждом handler, ошибка затрагивала /menu, коллекцию, призыв и остальные разделы.

Исправление:
- добавлен единый совместимый ISO-парсер;
- старые naive даты и новые timezone-aware даты преобразуются в aware UTC;
- все сравнения активности, cooldown, daily, pass, рейдов, друзей, очередей и автоочистки переведены на единое UTC-время;
- новые runtime timestamps сохраняются с timezone;
- старые данные не нужно удалять или сбрасывать;
- error handler теперь выдаёт короткий ID ошибки, а владельцу показывает тип безопасно очищенного исключения и пишет полный traceback в лог Render.

ГЛАВНОЕ МЕНЮ
В основном блоке оставлены только:
- имя и титул;
- выбранный аниме-мир;
- клан;
- сила отряда;
- «Призывы: N»;
- фисташки и драконит.

Из главного меню убраны:
- количество персонажей — оно находится в профиле;
- подробный статус бесплатного призыва;
- таймер сезона и SP;
- MultiPass и его время;
- ежедневная награда.

СУНДУКИ И ПРИЗЫВЫ
Основная экономика унифицирована:
- игрок покупает попытки призыва, а не отдельные сундуки;
- одна попытка выдаёт одного полного персонажа;
- если персонаж уже есть, дубликат превращается во фрагменты прокачки;
- старые кнопки сундуков остаются рабочими алиасами и ведут в обычный призыв;
- старый инвентарь сундуков один раз конвертируется 1:1 в дополнительные попытки и обнуляется;
- bulk-призыв x3/x5/x10 списывает попытки атомарно: либо все, либо ни одной.

Шансы любого обычного призыва/legacy-сундука:
- Origin — 50%;
- Rare — 30%;
- Epic — 12.5%;
- Legendary — 5%;
- Absolute — 2.5%.

Super Absolute не входит в обычный пул. Pity/гарант сохранён и по-прежнему способен повысить фактическую редкость по установленным порогам.

ЧТО СОХРАНЕНО
- 17 641 исходная карта с прежними уникальными ID;
- 896 настоящих media ID в двух media_packs;
- Neon/PostgreSQL как authoritative storage при DATABASE_URL;
- fail-closed запуск при недоступном Neon;
- revision snapshots, dirty/retry/backoff и финальный flush;
- /storage и /flush_data;
- per-user последовательность действий;
- Telegram Stars ledger и защита charge_id;
- drop_pending_updates=False;
- рефералы, промокоды, MultiPass, сезон, daily, кланы, друзья, коллекция, крафт, артефакты, PvP, арена, рейды, события, рейтинги, админка и обращения;
- permanent tombstones и безопасный recovery;
- старые команды, callback_data и нижняя клавиатура;
- custom emoji из PATCH40, управляемые CUSTOM_BUTTON_EMOJI.

ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ
Обязательные на Render:
BOT_TOKEN=
DATABASE_URL=
OWNER_ID= или OWNER_IDS=

Основные настройки уже описаны в .env.example. Рекомендуемые безопасные значения:
APP_TIMEZONE=UTC
CUSTOM_BUTTON_EMOJI=1
ABM_ENABLE_RIGHT_HAND_PERMISSIONS=0
ABM_ALLOW_LEGACY_RIGHT_HAND_ID=0
ABM_CURATED_DRAW_POOL=0
ABM_ALLOW_DANGEROUS_RECOVERY_MERGE=0

PYTHON И ЗАВИСИМОСТИ
.python-version содержит 3.12.
requirements.txt:
aiogram==3.27.0
aiohttp==3.13.5
Pillow==10.4.0
psycopg[binary]>=3.2.0

ПОЛНОЕ ОБНОВЛЕНИЕ С PATCH40
1. Создай backup/branch Neon.
2. Убедись, что одновременно работает только один экземпляр бота.
3. Удали старые файлы проекта в GitHub, как обычно.
4. Распакуй этот ZIP и загрузи ВСЕ файлы из его корня.
5. Не загружай в Git реальные BOT_TOKEN, DATABASE_URL и runtime DATA.
6. В Render сохрани прежние Environment variables.
7. Выполни Clear build cache & deploy.
8. После запуска проверь /menu, затем /storage и /flush_data.
9. Проверь «Призвать», «Коллекция», «Профиль», «Сундуки/Шансы призыва».

Повторно запускать /reset_patch40 CONFIRM для этого hotfix НЕ НУЖНО. Он исправляет код и совместим со сложившимися данными.

BACKUP NEON
Самый безопасный вариант — создать Neon branch/restore point перед deploy. Дополнительно допустим pg_dump с доверенного компьютера. Не публикуй DATABASE_URL в GitHub, ZIP, логах или скриншотах.

ПРОВЕРКА /storage
У владельца команда должна показать PostgreSQL/Neon как основное хранилище при заданном DATABASE_URL. После /flush_data должна быть подтверждена облачная запись. Если Neon недоступен, бот обязан не запускать polling на локальном fallback — это защитное поведение.

ОТКАТ
1. Останови текущий Render service.
2. Сохрани backup текущего Neon.
3. Верни предыдущий код с той же DATABASE_URL.
4. Не импортируй старый локальный JSON/SQLite поверх Neon.
5. Для отката данных используй заранее созданную Neon branch/restore point.

РЕАЛЬНО ВЫПОЛНЕННЫЕ ПРОВЕРКИ
Подробный список находится в TEST_REPORT_PATCH40_HOTFIX.txt. Выполнены:
- py_compile;
- pyflakes;
- static callback/command/media audit;
- 58 core simulations;
- 29 PATCH40 feature simulations;
- 14 UI smoke screens;
- 19 hotfix runtime regressions;
- runtime import с чистым DATA_DIR;
- точное воспроизведение и устранение naive/aware datetime crash;
- проверка 17 641 карт, 896 media ID и двух media ZIP;
- ZIP integrity и secret/runtime-junk audit перед выдачей.

ОГРАНИЧЕНИЯ
- Production Neon и реальная транзакция Telegram Stars не доступны в тестовой среде, поэтому живой E2E с ними не заявляется.
- Настоящих артов 896; остальным картам бот создаёт безопасный локальный баннер.
- Pity меняет итоговое распределение для игроков, достигших гаранта; базовая таблица обычного roll остаётся указанной выше.
