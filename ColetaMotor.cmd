@echo off
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\coleta.ps1" %*
set CODE=%ERRORLEVEL%
echo.
if not "%CODE%"=="0" echo Coleta finalizada com erro. Codigo: %CODE%
if "%CODE%"=="0" echo Coleta finalizada com sucesso.
pause
exit /b %CODE%
