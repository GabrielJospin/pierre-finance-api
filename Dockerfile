# 1. Usa uma imagem leve do Python (ajuste a versão se necessário, ex: 3.9, 3.11)
FROM python:3.10-slim

# 2. Define o diretório de trabalho dentro do container
WORKDIR /app

# 4. Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copia todo o restante do código da pasta src para a pasta /app do container
COPY src/ .

# 6. O comando que inicia a sua aplicação
# O Cloud Run injeta a variável de ambiente PORT (padrão 8080).
# Certifique-se que seu main.py escuta nessa porta.
CMD ["python", "main.py"]