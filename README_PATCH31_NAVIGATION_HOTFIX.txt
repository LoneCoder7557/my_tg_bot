PATCH31_NAVIGATION_HOTFIX

Что исправлено:
1. /start, Главное меню и кнопки выхода в меню могли молчать при успешном старте Render.
2. Причина: в PATCH30 часть Telegram custom emoji ID была malformed: 20-22 цифры вместо нормального Telegram-sized numeric id.
3. Эти ID попадали в <tg-emoji> и icon_custom_emoji_id. Telegram может отклонять весь sendMessage/editMessage с inline keyboard, поэтому polling живой, Render пишет success, но базовые handlers визуально «не отвечают».

Изменения в bot.py:
- PATCH_VERSION = PATCH31_NAVIGATION_HOTFIX
- start emoji заменён на сохранённый ID: 5215377245639549895
- collection emoji заменён на сохранённый ID: 5469741319330996757
- добавлена valid_custom_emoji_id()
- ce() теперь возвращает обычный fallback emoji, если custom emoji id повреждён
- button() больше не добавляет icon_custom_emoji_id, если ID повреждён

Что не трогалось:
- cards.json не менялся
- media_packs не менялись
- DATABASE_URL / Neon логика не удалялась
- /var/data не трогался
- прогресс игроков не переносился и не чистился

Как ставить:
1. Замени bot.py в корне GitHub-репозитория.
2. README можно добавить для истории патча, но он не обязателен.
3. Commit в GitHub.
4. Render → Manual Deploy → Clear build cache & deploy.
5. После старта проверить в Telegram: /start, 🏠 Меню, кнопка ⬅️ Меню из MultiPass, /storage.
