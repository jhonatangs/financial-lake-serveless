import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import boto3
import awswrangler as wr
import pandas as pd

# Configuração de logging
logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# Clientes AWS
s3_client = boto3.client("s3")

# Variáveis de ambiente
TRUSTED_BUCKET = os.getenv("TRUSTED_BUCKET")
GLUE_DATABASE_NAME = os.getenv("GLUE_DATABASE_NAME", "lakehouse_db")
ICEBERG_LOCATION_PREFIX = os.getenv("ICEBERG_LOCATION_PREFIX", "iceberg/")

# Configurar awswrangler para usar o Glue Data Catalog
wr.config.s3_endpoint_url = None  # Usar padrão da AWS

def extract_source_from_key(s3_key: str) -> Optional[str]:
    """
    Extrai a origem (coingecko ou yfinance) do caminho S3.
    Retorna 'coingecko', 'yfinance' ou None.
    """
    if "producer_coingecko" in s3_key:
        return "coingecko"
    elif "producer_yfinance" in s3_key:
        return "yfinance"
    else:
        return None

def extract_partition_date(s3_key: str) -> Optional[str]:
    """
    Extrai a data de ingestão do caminho S3 (formato year=YYYY/month=MM/day=DD).
    Retorna string no formato 'YYYY-MM-DD'.
    """
    try:
        # Procura padrão year=XXXX/month=XX/day=XX
        import re
        pattern = r"year=(\d{4})/month=(\d{2})/day=(\d{2})"
        match = re.search(pattern, s3_key)
        if match:
            year, month, day = match.groups()
            return f"{year}-{month}-{day}"
    except Exception:
        pass
    return None

def transform_coingecko_data(raw_json: list, s3_key: str) -> pd.DataFrame:
    """
    Transforma dados do CoinGecko (array de arrays) em DataFrame.
    """
    records = []
    for item in raw_json:
        if len(item) >= 5:
            timestamp_ms, open_price, high, low, close = item[:5]
            # Converter milissegundos para datetime UTC
            dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
            records.append({
                "timestamp": dt,
                "open": float(open_price),
                "high": float(high),
                "low": float(low),
                "close": float(close)
            })
    
    df = pd.DataFrame(records)
    if not df.empty:
        # Extrair coin_id do caminho (último diretório antes do filename)
        parts = s3_key.split("/")
        # O padrão é raw/producer_coingecko/year=.../.../coin_id/arquivo.json
        # Vamos pegar o diretório imediatamente antes do arquivo
        if len(parts) >= 2:
            coin_id_candidate = parts[-2]
            # Se não for um padrão de data (year=...), assume como coin_id
            if not coin_id_candidate.startswith(("year=", "month=", "day=")):
                df["coin_id"] = coin_id_candidate
            else:
                df["coin_id"] = "unknown"
        else:
            df["coin_id"] = "unknown"
        
        # Adicionar ingestion_date
        ingestion_date = extract_partition_date(s3_key)
        if ingestion_date:
            df["ingestion_date"] = pd.to_datetime(ingestion_date).date()
        else:
            df["ingestion_date"] = pd.Timestamp.now().date()
    
    return df

def transform_yfinance_data(raw_json: dict, s3_key: str) -> pd.DataFrame:
    """
    Transforma dados do Yahoo Finance (JSON com campos OHLCV) em DataFrame.
    """
    # O formato esperado é um dict com campos como:
    # {"ticker": "AAPL", "open": 182.63, "high": 183.0, "low": 181.5, "close": 182.8, "volume": 1000000, "timestamp": "2025-04-05T18:00:00Z"}
    records = []
    
    # Se for uma lista de registros, processa cada um
    if isinstance(raw_json, list):
        data_list = raw_json
    else:
        data_list = [raw_json]
    
    for item in data_list:
        if isinstance(item, dict):
            # Extrair timestamp
            ts_str = item.get("timestamp")
            if ts_str:
                try:
                    dt = pd.to_datetime(ts_str, utc=True)
                except:
                    dt = pd.NaT
            else:
                dt = pd.NaT
            
            record = {
                "timestamp": dt,
                "open": item.get("open"),
                "high": item.get("high"),
                "low": item.get("low"),
                "close": item.get("close"),
                "volume": item.get("volume"),
                "ticker": item.get("ticker", "unknown")
            }
            records.append(record)
    
    df = pd.DataFrame(records)
    if not df.empty:
        # Adicionar ingestion_date
        ingestion_date = extract_partition_date(s3_key)
        if ingestion_date:
            df["ingestion_date"] = pd.to_datetime(ingestion_date).date()
        else:
            df["ingestion_date"] = pd.Timestamp.now().date()
    
    return df

def write_to_iceberg(df: pd.DataFrame, table_name: str, database: str, bucket: str, location_prefix: str) -> bool:
    """
    Escreve DataFrame em tabela Iceberg usando awswrangler.
    """
    if df.empty:
        logger.warning(f"DataFrame vazio para tabela {table_name}, ignorando.")
        return True
    
    try:
        # Localização S3 para a tabela Iceberg
        table_location = f"s3://{bucket}/{location_prefix}{table_name}/"
        
        # Verificar se a tabela existe
        table_exists = False
        try:
            wr.catalog.table(database=database, table=table_name)
            table_exists = True
        except Exception:
            table_exists = False
        
        # Se não existir, criar a tabela
        if not table_exists:
            logger.info(f"Criando tabela Iceberg {database}.{table_name}")
            # Definir schema a partir do DataFrame
            # O awswrangler.to_iceberg criará a tabela automaticamente se não existir
            pass
        
        # Escrever dados (append)
        wr.dataframes.to_iceberg(
            df=df,
            database=database,
            table=table_name,
            table_location=table_location,
            partition_cols=["ingestion_date"],
            mode="append"
        )
        logger.info(f"Dados escritos na tabela Iceberg {database}.{table_name} ({len(df)} registros)")
        return True
    except Exception as e:
        logger.error(f"Erro ao escrever no Iceberg {database}.{table_name}: {str(e)}")
        return False

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Handler principal da Lambda Transformer Trusted.
    """
    logger.info(f"Evento recebido: {json.dumps(event)}")
    
    # Processar cada registro do evento S3
    for record in event.get("Records", []):
        try:
            s3_bucket = record["s3"]["bucket"]["name"]
            s3_key = record["s3"]["object"]["key"]
            
            logger.info(f"Processando arquivo: s3://{s3_bucket}/{s3_key}")
            
            # Identificar origem
            source = extract_source_from_key(s3_key)
            if not source:
                logger.warning(f"Origem desconhecida para key {s3_key}, ignorando.")
                continue
            
            # Baixar e carregar JSON do S3
            response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
            raw_content = response["Body"].read().decode("utf-8")
            raw_json = json.loads(raw_content)
            
            # Aplicar transformação conforme a origem
            if source == "coingecko":
                df = transform_coingecko_data(raw_json, s3_key)
                table_name = "trusted_crypto_ohlc"
            elif source == "yfinance":
                df = transform_yfinance_data(raw_json, s3_key)
                table_name = "trusted_stocks_ohlcv"
            else:
                logger.error(f"Origem {source} não suportada")
                continue
            
            logger.info(f"DataFrame transformado: {len(df)} linhas, {len(df.columns)} colunas")
            
            # Escrever no Iceberg
            success = write_to_iceberg(
                df=df,
                table_name=table_name,
                database=GLUE_DATABASE_NAME,
                bucket=TRUSTED_BUCKET,
                location_prefix=ICEBERG_LOCATION_PREFIX
            )
            
            if success:
                logger.info(f"Processamento concluído com sucesso para {s3_key}")
            else:
                logger.error(f"Falha ao processar {s3_key}")
                # Em produção, poderia enviar para DLQ
                
        except Exception as e:
            logger.error(f"Erro ao processar registro: {str(e)}")
            # Continuar processando outros registros
    
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Processamento concluído"})
    }
