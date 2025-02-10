@echo off

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Define values for parameters
set LIFESPAN=on
set HOST=0.0.0.0
set PORT=8000
set LOOP=asyncio
set HTTP=h11
set LOG_LEVEL=info
set LIMIT_CONCURENCY=4096
set BACKLOG=8192
set TIMEOUT_KEEP_ALIVE=60

REM Check Uvicorn version
uvicorn --version

REM Start Uvicorn server
uvicorn main:app ^
    --lifespan %LIFESPAN% ^
    --host %HOST% ^
    --port %PORT% ^
    --loop %LOOP% ^
    --http %HTTP% ^
    --log-level %LOG_LEVEL% ^
    --limit-concurrency %LIMIT_CONCURENCY% ^
    --backlog %BACKLOG% ^
    --timeout-keep-alive %TIMEOUT_KEEP_ALIVE% ^
    --reload
