УСТАНОВКА PATCH 6

1. Распакуй архив.
2. В GitHub замени:
   bot.py
   cards.json
   promo_codes.json
   requirements.txt
   Procfile
   media/
   CHANGELOG_PATCH6.txt

3. Не трогай:
   anime_battle_data.json
   owner_ids.txt
   right_hand_ids.txt

4. После commit в GitHub:
   Render → Manual Deploy → Deploy latest commit

Важно:
- anime_battle_data.db не загружай вручную, бот создаст/обновит его сам.
- Если часть старых пользователей не показывалась, после patch6 бот объединяет JSON и SQLite.
- Пользователь появится в админке гарантированно после любого нового сообщения или нажатия кнопки.
