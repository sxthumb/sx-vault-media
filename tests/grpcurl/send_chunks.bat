@echo off
setlocal enabledelayedexpansion

:: Configurações do servidor e arquivo proto
set SERVER=localhost:50051
set SERVICE=media.MediaService/UploadVideo
set PROTO_FILE=..\proto\media.proto
set CHUNKS_FILE=chunks_base64.txt
set TEMP_STREAM_FILE=temp_stream_payload.json

echo Preparando payload de stream...

:: Limpa arquivo temporario se existir
if exist "%TEMP_STREAM_FILE%" del "%TEMP_STREAM_FILE%"

:: Converte cada linha de base64 no objeto JSON esperado pelo gRPC
for /f "usebackq tokens=*" %%A in ("%CHUNKS_FILE%") do (
    echo {"chunk_data": "%%A"} >> "%TEMP_STREAM_FILE%"
)

echo Iniciando envio do stream gRPC para %SERVER%...
echo ----------------------------------------------------

:: Envia o stream completo mantendo a mesma conexao gRPC
type "%TEMP_STREAM_FILE%" | grpcurl -plaintext -proto "%PROTO_FILE%" -d @ %SERVER% %SERVICE%

:: Limpeza do arquivo temporario
if exist "%TEMP_STREAM_FILE%" del "%TEMP_STREAM_FILE%"

echo.
echo ----------------------------------------------------
echo Envio concluido com sucesso!
pause