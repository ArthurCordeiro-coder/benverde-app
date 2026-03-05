@echo off
cd /d "C:\Users\pesso\OneDrive\Documentos\benverde\MeuAppGerencia"

:: Ativa o ambiente virtual
call env_app\Scripts\activate.bat

ngrok http 8501

:: Inicia o Streamlit em segundo plano
start "" env_app\Scripts\streamlit.exe run app.py --server.port 8501 --server.headless true

:: Aguarda 8 segundos para o servidor subir
timeout /t 8 /nobreak >nul

:: Abre o navegador
start "" "http://localhost:8501/bv_9m4k2r"

exit