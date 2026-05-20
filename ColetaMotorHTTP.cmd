@echo off
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\bronze_http.ps1" %*
set CODE=%ERRORLEVEL%
echo.
if not "%CODE%"=="0" echo Acionamento HTTP finalizado com erro. Codigo: %CODE%
if "%CODE%"=="0" echo Acionamento HTTP enviado com sucesso.
pause
exit /b %CODE%
