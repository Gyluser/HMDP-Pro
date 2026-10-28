@echo off
setlocal
set "AI_EMBEDDING_PORT=8088"
set "AI_EMBEDDING_MODEL_PATH=D:\A_Stu\VSCode_WP\_7_Shix\_1_agent\all-in-rag\models\bge-small-zh-v1.5"
"D:\A_Stu\VSCode_WP\_7_Shix\_1_agent\learn-claude-code\LCC\Scripts\python.exe" "%~dp0local_embedding_server.py"
endlocal
