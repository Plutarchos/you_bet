@echo off
cd /d C:\Users\Emmet\you_bet

echo [%date% %time%] Starting odds update... >> logs\update.log

:: Fetch odds from API
uv run python -m src.main fetch-odds >> logs\update.log 2>&1

:: Fetch results for completed matches
uv run python -m src.main fetch-results >> logs\update.log 2>&1

:: Export to JSON for website
uv run python -m src.export_json >> logs\update.log 2>&1

echo [%date% %time%] Update complete. >> logs\update.log
echo. >> logs\update.log
