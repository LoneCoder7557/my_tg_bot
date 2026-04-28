Как поставить PATCH 8

1. Распакуй архив.
2. В GitHub замени/загрузи:
   - bot.py
   - cards.json
   - promo_codes.json
   - requirements.txt
   - Procfile
   - media/
   - tools_generate_card_assets.py
   - CHANGELOG_PATCH8.txt
   - README_PATCH8_INSTALL.txt
3. НЕ трогай и НЕ загружай в публичный репозиторий:
   - anime_battle_data.json
   - anime_battle_data.db
   - anime_battle_data.db-*
   - owner_ids.txt
   - right_hand_ids.txt
   - token.txt
4. Сделай Commit changes.
5. Render → Manual Deploy → Deploy latest commit.

Медиа карт:
- В PATCH 8 создано 862 изображения для всех НЕ мифических карт.
- Мифики специально пропущены: положи свои GIF/MP4 вручную сюда:
  media/cards/<card_id>.gif
  media/cards/<card_id>.mp4
  media/cards_watermarked/<card_id>.gif
- Список статусов лежит в media/asset_manifest.csv.

Stars:
- Premium pass теперь активируется автоматически после оплаты.
- Stars-наборы тоже выдаются автоматически.
- Если платёж пришёл второй раз с тем же payment_id, повторной выдачи не будет.

Проверка перед запуском:
python -m py_compile bot.py

Если нужно пересоздать безопасные карточные баннеры:
python tools_generate_card_assets.py
