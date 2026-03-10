import os
import yfinance as yf
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import logging
import time

load_dotenv()


# --- Logger (criado uma vez no nível do módulo) ---

def criar_logger(nome: str) -> logging.Logger:
    """
    Cria um logger com dois handlers:
    - Console: mostra INFO+ no terminal
    - Arquivo: salva DEBUG+ em logs/acoes_pipeline.log

    Guarda handlers existentes para evitar duplicação se o módulo
    for importado mais de uma vez na mesma sessão Python.
    """
    logger = logging.getLogger(nome)
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    formato = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formato)

    Path('logs').mkdir(exist_ok=True)
    arquivo = logging.FileHandler('logs/acoes_pipeline.log', encoding='utf-8')
    arquivo.setLevel(logging.DEBUG)
    arquivo.setFormatter(formato)

    logger.addHandler(console)
    logger.addHandler(arquivo)
    return logger


logger = criar_logger('extracao_pipeline')


# --- Schema esperado ---

SCHEMA = {
    'Open':   'float64',
    'High':   'float64',
    'Low':    'float64',
    'Close':  'float64',
    'Volume': 'int64',
}


# --- Configuração via .env ---

hoje = datetime.today()
formatado = hoje.strftime('%Y-%m-%d')

data_inicio = os.getenv('DATA_INICIAL')
data_fim    = os.getenv('DATA_FINAL')
acoes       = [i.strip() for i in os.getenv("ACAO").split(',')]

pasta_do_script = Path(__file__).parent


# --- Funções ---
def _storage_options() -> dict:
    return {
        "endpoint_url": os.getenv('MINIO_ENDPOINT'),
        "aws_access_key_id": os.getenv('MINIO_ACCESS_KEY'),
        "aws_secret_access_key": os.getenv('MINIO_SECRET_KEY'),
        "aws_allow_http": "true",   # MinIO runs HTTP, not HTTPS locally
    }

 
def _output_path(ticker: str) -> str:
    env = os.getenv('ENV','dev')
    nome = ticker.replace('^', '').replace('.SA', '')

    if env == 'dev':
                   bucket = os.getenv("STORAGE_BUCKET")
                   return f"s3://{bucket}/delta/stocks/{nome}_{formatado}.parquet"
             #elif enviroment == 'prod'
             #    os.getenv("STORAGE_BUCKET_PROD")
    else:
      raise NotImplementedError('prod not configured yet')
      

def validar_schema(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Aplica o schema esperado ao DataFrame bruto do yfinance:
    - Achata colunas MultiIndex (yfinance >= 0.2 retorna nível duplo)
    - Seleciona apenas colunas OHLCV
    - Força os tipos corretos
    - Adiciona colunas de contexto (ticker, ingested_at)
    - Loga colunas com valores nulos
    """
    # yfinance >= 0.2 retorna MultiIndex: (Price, Ticker)
    # ex: ('Close', 'PETR4.SA') → pegamos só o primeiro nível
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    colunas_disponiveis = [c for c in SCHEMA if c in df.columns]
    df = df[colunas_disponiveis].copy()

    for col, dtype in SCHEMA.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)

    df['ticker']      = ticker
    df['ingested_at'] = pd.Timestamp.now()

    nulos = df[colunas_disponiveis].isnull().sum()
    if nulos.any():
        logger.warning(f"{ticker} | colunas com nulos: {nulos[nulos > 0].to_dict()}")

    return df


def busca_acao(acao: list, start_date: str, end_date: str = formatado):
    logger.info('iniciando processo de extracao')

    pasta_saida = (pasta_do_script / '..' / 'data' / 'raw').resolve()
    pasta_saida.mkdir(parents=True, exist_ok=True)

    for ticker in acao:
        logger.info(f"baixando: {ticker}")

        df = yf.download(ticker, start=start_date, end=end_date, auto_adjust=True)

        if df.empty:
            logger.warning(f"{ticker} | nenhum dado retornado, pulando")
            continue

        linhas_brutas = len(df)
        logger.debug(f"{ticker} | {linhas_brutas} linhas recebidas do yfinance")

        df = validar_schema(df, ticker=ticker)

        nome_arquivo = ticker.replace('^', '').replace('.SA', '')
        path = pasta_saida / f'{nome_arquivo}_{formatado}.parquet'

        df.to_parquet(path, index=True)

        # Validação de contagem: relê o arquivo para confirmar integridade
        linhas_salvas = len(pd.read_parquet(path))
        logger.info(f"{ticker} | salvo: {path.name} | linhas: {linhas_brutas} → {linhas_salvas}")

        if linhas_brutas != linhas_salvas:
            logger.error(f"{ticker} | DIVERGENCIA de contagem: esperado {linhas_brutas}, salvo {linhas_salvas}")

        time.sleep(10)

    logger.info('extracao finalizada')



if __name__ == '__main__':
    busca_acao(acoes, data_inicio, data_fim)
