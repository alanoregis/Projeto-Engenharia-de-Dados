# Imagem base oficial do Airflow com Python 3.12
FROM apache/airflow:2.9.1-python3.11

# Instala o uv — gerenciador de pacotes rápido que substitui o pip
# A versão é fixada para garantir reprodutibilidade
COPY --from=ghcr.io/astral-sh/uv:0.9.0 /uv /home/airflow/.local/bin/uv

# Copia os arquivos de definição de dependências
# O uv.lock garante que todo mundo instala exatamente as mesmas versões
COPY pyproject.toml uv.lock /opt/airflow/

# Instala as dependências usando o lockfile
# uv export converte o lockfile para requirements.txt
# pip install garante que os pacotes vão para /home/airflow/.local (usuário airflow)
WORKDIR /opt/airflow
RUN uv export --frozen --no-dev --no-emit-project -o /tmp/requirements.txt && \
    pip install --no-cache-dir -r /tmp/requirements.txt