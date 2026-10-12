@echo off
cd /d "%~dp0.."
if not exist data mkdir data
bin\server_test.exe --listen=1@127.0.0.1:50051 --peer=2@127.0.0.1:50052 --peer=3@127.0.0.1:50053
pause
