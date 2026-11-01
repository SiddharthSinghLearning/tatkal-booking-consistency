@echo off
cd /d "%~dp0"
python tatkal_offline.py --sweep-seconds 20
pause
