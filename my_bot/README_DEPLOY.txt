Anime Battle — файлы для GitHub/хостинга

ВАЖНО:
1. Не заливай настоящий токен в GitHub.
2. На хостинге создай переменную окружения:
   BOT_TOKEN = твой токен от BotFather
3. Если запускаешь локально на ПК, можешь вставить токен в token.txt.

Что загружать в GitHub:
- bot.py
- cards.json
- promo_codes.json
- requirements.txt
- Procfile
- media/_sample.gif

Что НЕ загружать с реальными данными:
- token.txt с настоящим токеном
- anime_battle_data.json, если не хочешь отдавать прогресс игроков

Команда запуска:
python bot.py

Стартовая команда для Render/Railway:
python bot.py

requirements.txt:
aiogram==3.27.0
