Anime Battle Multiverse — медиа

1) Медиа карт:
   Клади файлы прямо в media/ с именем ID карты из cards.json.
   Примеры:
   media/naruto_baryon.gif
   media/gojo_infinity.jpg
   media/luffy_gear5.png

   Приоритет показа: gif → mp4 → jpg → jpeg → png → webp.
   Для мифических лучше ставить gif/mp4, для остальных хватит jpg/png.

2) Автобаннеры:
   Если файла карты нет, bot.py сам создаёт нейтральный карточный баннер в:
   media/generated_cards/<card_id>.png
   Для этого используется Pillow.

3) Арены:
   Уже добавлены 6 базовых фонов:
   media/arenas/ruins.jpg
   media/arenas/city.jpg
   media/arenas/void.jpg
   media/arenas/forest.jpg
   media/arenas/desert.jpg
   media/arenas/temple.jpg

4) Важно:
   Не заливай тяжёлые видео в GitHub. Лучше короткие gif/mp4 до 20–25 MB.
