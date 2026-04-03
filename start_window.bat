@echo off
echo ===========================================
echo Starting Brokerage ETL (Windows Mode)
echo ===========================================

:: find current directory and change \ to / for Docker compatibility
set CURRENT_DIR=%cd:\=/%

if not exist .env (
    if exist .env.example (
        echo [INFO] .env not found. Copying from .env.example...
        copy .env.example .env >nul
    ) else (
        echo [WARNING] .env.example not found! Creating an empty .env file...
        type nul > .env
    )
)
:: Remove old HOST_PWD if exists and append new one
findstr /v "^HOST_PWD=" .env > .env.tmp
move /y .env.tmp .env >nul
echo HOST_PWD=%CURRENT_DIR%>> .env
echo [INFO] Updated .env with HOST_PWD=%CURRENT_DIR%
