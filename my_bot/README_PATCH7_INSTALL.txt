Как поставить PATCH 7

1. Распакуй архив.
2. В GitHub замени/загрузи:
   - bot.py
   - cards.json
   - promo_codes.json
   - requirements.txt
   - Procfile
   - media/
   - CHANGELOG_PATCH7.txt
   - README_PATCH7_INSTALL.txt
3. НЕ трогай:
   - anime_battle_data.json
   - anime_battle_data.db
   - owner_ids.txt
   - right_hand_ids.txt
   - token.txt
4. Сделай Commit changes.
5. Render → Manual Deploy → Deploy latest commit.

Медиа персонажей:
- Лучший новый путь: media/cards/card_id.jpg или media/cards/card_id.gif
- Для файлов с водяным знаком: media/cards_watermarked/card_id.gif
- Старый путь media/card_id.jpg тоже поддерживается.

Stars:
- Premium pass остаётся за 199 Stars.
- Stars-наборы лежат в Магазин / награды → Stars-наборы.
- Владелец видит оплаты и покупки через /admin → Оплаты / покупки.
