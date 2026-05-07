PATCH20 SPEED CLEAN — Anime Battle Multiverse

Цель патча:
- убрать лаги 10-15 секунд при нажатии кнопок;
- не покупать Render Disk;
- сохранить Neon PostgreSQL через DATABASE_URL;
- не трогать cards.json, media_packs, /var/data и прогресс игроков.

Что изменено в bot.py:
1. Убран прямой save_json(DATA_FILE, DATA) почти с каждого клика.
2. Добавлен debounce-save:
   - игрок нажимает кнопки быстро;
   - бот помечает DATA как изменённый;
   - реальное сохранение идёт пачкой в фоне примерно раз в 4 секунды.
3. JSON/SQLite/Neon сохранение теперь выполняется через asyncio.to_thread(), чтобы не замораживать event loop бота.
4. AutoCleanCallbackMiddleware больше не ждёт удаления старого сообщения. Удаление идёт фоном.
5. UserTouchMiddleware больше не вызывает get_user_data дважды на один клик.
6. Добавлена команда владельца /flush_data — принудительно сохранить DATA в Neon/локальный fallback.
7. /storage показывает статус SPEED-save:
   - dirty да/нет;
   - сколько раз реально сохранялось;
   - последнее сохранение;
   - последняя причина;
   - ошибка, если была.
8. При успешной оплате Stars/Multipass данные сохраняются сразу через flush_data_now_async(), а не ждут пачки.
9. При shutdown бот пытается сохранить dirty DATA перед выходом.

Что заменить в GitHub:
- заменить только bot.py из корня репозитория;
- добавить README_PATCH20_SPEED_CLEAN.txt по желанию.

Что НЕ заменять:
- cards.json не трогать;
- requirements.txt не трогать;
- Procfile не трогать;
- owner_ids.txt не трогать;
- promo_codes.json не трогать;
- media_packs/ не трогать;
- media/ не перезаливать.

Что можно удалить из GitHub:
- старые README_PATCH*.txt можно удалить, если они путают;
- особенно README_PATCH15_FINAL.txt можно заменить этим README.

Что нельзя удалять:
- bot.py;
- cards.json;
- requirements.txt;
- Procfile;
- owner_ids.txt;
- promo_codes.json;
- media_packs/card_images_bundle_1.zip;
- media_packs/card_images_bundle_2.zip.

Render:
- Root Directory: пустой;
- Build Command: pip install -r requirements.txt;
- Start Command: python bot.py;
- DATABASE_URL должен быть включён;
- DATA_DIR=/var/data можно оставить, но покупать Disk не нужно, если прогресс хранится в Neon.

После деплоя проверить:
1. /start
2. быстро нажать Меню / Профиль / Коллекция / Награды
3. /storage
4. DATABASE_URL/Neon должен быть: есть
5. SPEED-save должен появиться в /storage
6. Через 4-6 секунд после действий dirty должен стать нет, а saves увеличиться.

Если что-то пошло не так:
- вернуть прошлый bot.py из GitHub history;
- не трогать Neon и /var/data;
- прислать сюда ошибку из Render Logs и /storage.
