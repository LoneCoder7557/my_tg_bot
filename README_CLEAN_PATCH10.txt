CLEAN PATCH 10 — ЧИСТЫЙ РЕПОЗИТОРИЙ ДЛЯ GITHUB/RENDER

Что было плохо в твоём ZIP:
1. В корне лежал старый bot.py.
2. Настоящий новый бот лежал внутри папки my_bot/.
3. Render запускал python bot.py из корня и мог брать старую/неполную версию.
4. cards.json лежал не рядом с корневым bot.py.
5. В репозитории были служебные token.txt, owner_ids.txt, right_hand_ids.txt.
6. Не было нормального .gitignore в корне.

Что сделано:
1. Новый bot.py лежит сразу в корне.
2. cards.json, requirements.txt, Procfile, media/ лежат сразу в корне.
3. Добавлена защита прогресса через DATA_DIR=/var/data.
4. Убраны token.txt, owner_ids.txt, right_hand_ids.txt.
5. Добавлен нормальный .gitignore.

Render Environment:
BOT_TOKEN=твой токен
OWNER_IDS=твой Telegram ID
RIGHT_HAND_IDS=ID помощника, если нужен
DATA_DIR=/var/data

Render Disk:
Disks -> Add Disk -> Mount Path: /var/data
Потом Manual Deploy -> Clear build cache & deploy.

После запуска напиши в боте: /storage
Должно показать DATA_DIR: /var/data.
