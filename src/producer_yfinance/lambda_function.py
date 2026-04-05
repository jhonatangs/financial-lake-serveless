"""
AWS Lambda para coleta de dados OHLCV do Yahoo Finance via yfinance.
Acionada diariamente às 18:00 (horário de Brasília) via EventBridge.
Coleta dados para tickers configurados em TICKERS_LIST e publica em fila SQS.
"""
import os
import json
import time
import logging
from typing import Dict, List, Optional, Any

import yfinance as yf
import boto3
from botocore.exceptions import ClientError

# Configuração de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def fetch_ticker_data(
    ticker: str,
    max_retries: int = 3,
    retry_delay_sec: int = 2
) -> Optional[Dict[str, Any]]:
    """
    Busca dados OHLCV para um ticker específico com retentativas.
    
    Args:
        ticker: Símbolo do ativo (ex: AAPL).
        max_retries: Número máximo de tentativas.
        retry_delay_sec: Delay entre tentativas em segundos.
    
    Returns:
        Dicionário com os campos OHLCV, timestamp e ticker, ou None em caso de falha.
    """
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Tentativa {attempt}/{max_retries} para ticker {ticker}")
            # Baixa dados do dia atual (period="1d")
            data = yf.download(
                ticker,
                period="1d",
                progress=False,
                group_by="ticker"
            )
            
            # Verifica se os dados estão vazios
            if data.empty:
                logger.warning(f"Dados vazios para {ticker} na tentativa {attempt}")
                raise ValueError(f"No data for {ticker}")
            
            # Extrai a última linha (último registro do dia)
            last_row = data.iloc[-1]
            
            # Constrói o dicionário de resposta
            result = {
                "ticker": ticker,
                "timestamp": data.index[-1].isoformat(),
                "open": float(last_row["Open"]),
                "high": float(last_row["High"]),
                "low": float(last_row["Low"]),
                "close": float(last_row["Close"]),
                "volume": int(last_row["Volume"])
            }
            
            logger.info(f"Sucesso ao buscar {ticker}: {result['close']}")
            return result
            
        except Exception as e:
            logger.error(f"Erro na tentativa {attempt} para {ticker}: {str(e)}")
            if attempt == max_retries:
                logger.error(f"Falha final após {max_retries} tentativas para {ticker}")
                return None
            time.sleep(retry_delay_sec)
    
    return None

def send_to_sqs(
    message_body: Dict[str, Any],
    queue_url: str,
    message_attributes: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Envia uma mensagem para a fila SQS.
    
    Args:
        message_body: Conteúdo da mensagem (será serializado para JSON).
        queue_url: URL da fila SQS de destino.
        message_attributes: Atributos opcionais da mensagem.
    
    Returns:
        True se a mensagem foi enviada com sucesso, False caso contrário.
    """
    try:
        sqs_client = boto3.client('sqs')
        
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body),
            MessageAttributes=message_attributes or {}
        )
        
        logger.info(f"Mensagem enviada para SQS: MessageId={response.get('MessageId')}")
        return True
        
    except ClientError as e:
        logger.error(f"Erro ao enviar para SQS: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Erro inesperado ao enviar para SQS: {str(e)}")
        return False

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Handler principal da AWS Lambda.
    
    Args:
        event: Dicionário com dados do evento (não utilizado diretamente, mas mantido para compatibilidade).
        context: Objeto de contexto Lambda.
    
    Returns:
        Dicionário com statusCode e body contendo métricas do processamento.
    """
    logger.info("Iniciando processamento do producer yfinance")
    
    # Leitura das variáveis de ambiente
    tickers_list_str = os.getenv("TICKERS_LIST", "")
    sqs_queue_url = os.getenv("SQS_QUEUE_URL")
    
    if not tickers_list_str:
        logger.error("Variável de ambiente TICKERS_LIST não configurada")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "TICKERS_LIST environment variable not set"})
        }
    
    if not sqs_queue_url:
        logger.error("Variável de ambiente SQS_QUEUE_URL não configurada")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "SQS_QUEUE_URL environment variable not set"})
        }
    
    # Configurações de retry (com valores padrão conforme spec)
    max_retries = int(os.getenv("MAX_RETRIES", "3"))
    retry_delay_sec = int(os.getenv("RETRY_DELAY_SEC", "2"))
    
    # Processa cada ticker
    tickers = [t.strip() for t in tickers_list_str.split(",") if t.strip()]
    logger.info(f"Tickers a processar: {tickers}")
    
    processed = 0
    failed_fetch = 0
    failed_send = 0
    
    for ticker in tickers:
        # Busca dados do ticker
        ticker_data = fetch_ticker_data(ticker, max_retries, retry_delay_sec)
        
        if ticker_data is None:
            failed_fetch += 1
            continue
        
        # Prepara atributos da mensagem (conforme spec)
        message_attributes = {
            "source": {
                "DataType": "String",
                "StringValue": "producer_yfinance"
            }
        }
        
        # Envia para SQS
        success = send_to_sqs(ticker_data, sqs_queue_url, message_attributes)
        
        if success:
            processed += 1
        else:
            failed_send += 1
    
    # Log final e métricas
    total = len(tickers)
    logger.info(
        f"Processamento concluído. "
        f"Total: {total}, Processados: {processed}, "
        f"Falhas busca: {failed_fetch}, Falhas envio: {failed_send}"
    )
    
    # Retorno compatível com API Gateway (se usado via HTTP)
    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed": processed,
            "failed_fetch": failed_fetch,
            "failed_send": failed_send,
            "total_tickers": total
        })
    }
