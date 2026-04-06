import os
import json
import logging
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from urllib.parse import unquote_plus
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
        import re
        pattern = r"year=(\d{4})/month=(\d{2})/day=(\d{2})"
        match = re.search(pattern, s3_key)
        if match:
            year, month, day = match.groups()
            return f"{year}-{month}-{day}"
    except Exception:
        pass
    return None

def transform_coingecko_data(raw_json: Any, s3_key: str) -> pd.DataFrame:
    """
    Transforma dados do CoinGecko em DataFrame.
    Espera um dicionário com 'coin_id' e 'ohlc_data'.
    """
    records = []
    
    # Verificar se o JSON é um dicionário
    if not isinstance(raw_json, dict):
        logger.error(f"JSON inesperado para CoinGecko: tipo {type(raw_json)}. Esperado dict.")
        return pd.DataFrame()
    
    # Extrair coin_id do JSON
    coin_id = raw_json.get("coin_id", "unknown")
    
    # Extrair ohlc_data
    ohlc_data = raw_json.get("ohlc_data", [])
    if not isinstance(ohlc_data, list):
        logger.error(f"ohlc_data não é uma lista: tipo {type(ohlc_data)}")
        return pd.DataFrame()
    
    # Processar cada item OHLC
    for item in ohlc_data:
        if isinstance(item, list) and len(item) >= 5:
            timestamp_ms, open_price, high, low, close = item[:5]
            # Converter milissegundos para datetime UTC
            try:
                dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
            except (ValueError, OSError):
                dt = pd.NaT
            records.append({
                "timestamp": dt,
                "open": float(open_price) if open_price is not None else None,
                "high": float(high) if high is not None else None,
                "low": float(low) if low is not None else None,
                "close": float(close) if close is not None else None
            })
        else:
            logger.warning(f"Item em ohlc_data não é uma lista válida: {item}")
    
    df = pd.DataFrame(records)
    if not df.empty:
        # Usar coin_id extraído do JSON
        df["coin_id"] = coin_id
        
        # Adicionar ingestion_date a partir do caminho S3
        ingestion_date = extract_partition_date(s3_key)
        if ingestion_date:
            df["ingestion_date"] = pd.to_datetime(ingestion_date).date()
        else:
            df["ingestion_date"] = pd.Timestamp.now(tz=timezone.utc).date()
    
    return df

def transform_yfinance_data(raw_json: dict, s3_key: str) -> pd.DataFrame:
    """
    Transforma dados do Yahoo Finance (JSON com campos OHLCV) em DataFrame.
    """
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
                except Exception:
                    dt = pd.NaT
            else:
                dt = pd.NaT
            
            # Garantir que volume seja inteiro
            volume = item.get("volume")
            if volume is not None:
                try:
                    volume = int(volume)
                except (ValueError, TypeError):
                    volume = None
            
            record = {
                "timestamp": dt,
                "open": item.get("open"),
                "high": item.get("high"),
                "low": item.get("low"),
                "close": item.get("close"),
                "volume": volume,
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
            df["ingestion_date"] = pd.Timestamp.now(tz=timezone.utc).date()
    
    return df

def prepare_dataframe_for_iceberg(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ajusta tipos de colunas para compatibilidade com Iceberg.
    """
    if df.empty:
        return df
    
    # Converter timestamp para datetime[ns, UTC]
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    
    # Garantir que colunas numéricas sejam float
    numeric_cols = ["open", "high", "low", "close"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    # Volume deve ser Int64 (permite NaN)
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")
    
    # ingestion_date deve ser date
    if "ingestion_date" in df.columns:
        df["ingestion_date"] = pd.to_datetime(df["ingestion_date"]).dt.date
    
    return df

def write_to_iceberg(df: pd.DataFrame, table_name: str, database: str, bucket: str, location_prefix: str) -> bool:
    """
    Escreve DataFrame em tabela Iceberg usando awswrangler via Athena.
    """
    if df.empty:
        logger.warning(f"DataFrame vazio para tabela {table_name}, ignorando.")
        return True
    
    try:
        # Localização S3 para a tabela Iceberg
        table_location = f"s3://{bucket}/{location_prefix}{table_name}/"
        
        # Preparar DataFrame
        df_prepared = prepare_dataframe_for_iceberg(df)
        
        # Escrever usando wr.athena.to_iceberg
        wr.athena.to_iceberg(
            df=df_prepared,
            database=database,
            table=table_name,
            table_location=table_location,
            temp_path=f"s3://{bucket}/{location_prefix}temp/",
            partition_cols=["ingestion_date"]
        )
        logger.info(f"Dados escritos na tabela Iceberg {database}.{table_name} via wr.athena.to_iceberg")
        return True
    except Exception as e:
        logger.error(f"Erro ao escrever no Iceberg {database}.{table_name}: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Handler principal da Lambda Transformer Trusted.
    """
    logger.info(f"Evento recebido com {len(event.get('Records', []))} registros")
    logger.info(f"Versão do awswrangler: {wr.__version__}")
    
    processed = 0
    failed = 0
    
    # Processar cada registro do evento S3
    for record in event.get("Records", []):
        try:
            s3_bucket = record["s3"]["bucket"]["name"]
            # Decodificar a chave S3 (pode conter url-encoding, ex: '=' -> '%3D')
            s3_key = unquote_plus(record["s3"]["object"]["key"])
            
            logger.info(f"Processando arquivo: s3://{s3_bucket}/{s3_key}")
            
            # Identificar origem (a chave já está decodificada)
            source = extract_source_from_key(s3_key)
            if not source:
                logger.warning(f"Origem desconhecida para key {s3_key}, ignorando.")
                failed += 1
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
                failed += 1
                continue
            
            if df.empty:
                logger.warning(f"DataFrame vazio após transformação para {s3_key}")
                processed += 1
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
                processed += 1
            else:
                logger.error(f"Falha ao processar {s3_key}")
                failed += 1
                # Em produção, poderia enviar para DLQ
                
        except Exception as e:
            logger.error(f"Erro ao processar registro: {str(e)}")
            logger.error(traceback.format_exc())
            failed += 1
            # Continuar processando outros registros
    
    logger.info(f"Processamento finalizado: {processed} sucessos, {failed} falhas")
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Processamento concluído",
            "processed": processed,
            "failed": failed
        })
    }
