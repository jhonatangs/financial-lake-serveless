import os
import json
import time
import random
import logging
from typing import Dict, List, Any, Optional
import requests
import boto3
from botocore.exceptions import ClientError

# Configuração de logging
logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# Clientes AWS
sqs_client = boto3.client("sqs")

# URLs das filas a partir das variáveis de ambiente
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
SQS_DLQ_URL = os.getenv("SQS_DLQ_URL")

# Constantes da API CoinGecko
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
OHLC_ENDPOINT = "/coins/{id}/ohlc"
RATE_LIMIT_DELAY = 2  # segundos entre requisições (30 req/min)
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 1  # segundo

def fetch_ohlc(coin_id: str) -> Dict[str, Any]:
    """
    Busca dados OHLC diários para uma moeda específica.
    Implementa retry com backoff exponencial para erros 429 e 5xx.
    """
    url = f"{COINGECKO_BASE_URL}{OHLC_ENDPOINT.format(id=coin_id)}"
    params = {"days": 1, "vs_currency": "usd"}
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Tentativa {attempt} para {coin_id}")
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Sucesso ao obter dados para {coin_id}. {len(data)} pontos.")
                return {"success": True, "data": data}
            
            # Erros que não devem ser retentados
            if response.status_code in (400, 404):
                logger.error(f"Erro não recuperável {response.status_code} para {coin_id}: {response.text}")
                return {
                    "success": False,
                    "error": response.text,
                    "http_status": response.status_code
                }
            
            # Erros que devem ser retentados
            if response.status_code == 429 or response.status_code >= 500:
                logger.warning(f"Erro {response.status_code} para {coin_id}. Tentando novamente...")
                if attempt < MAX_RETRIES:
                    delay = INITIAL_RETRY_DELAY * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                    time.sleep(delay)
                    continue
                else:
                    logger.error(f"Esgotadas tentativas para {coin_id}. Último status: {response.status_code}")
                    return {
                        "success": False,
                        "error": response.text,
                        "http_status": response.status_code
                    }
            
            # Outros códigos de erro (ex: 403, 401)
            logger.error(f"Erro inesperado {response.status_code} para {coin_id}: {response.text}")
            return {
                "success": False,
                "error": response.text,
                "http_status": response.status_code
            }
            
        except requests.exceptions.Timeout:
            logger.error(f"Timeout na tentativa {attempt} para {coin_id}")
            if attempt < MAX_RETRIES:
                delay = INITIAL_RETRY_DELAY * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                time.sleep(delay)
                continue
            else:
                return {
                    "success": False,
                    "error": "Timeout após todas as tentativas",
                    "http_status": 0
                }
        except Exception as e:
            logger.error(f"Exceção não esperada para {coin_id}: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "http_status": 0
            }
    
    # Nunca deve chegar aqui, mas por segurança
    return {
        "success": False,
        "error": "Erro desconhecido após todas as tentativas",
        "http_status": 0
    }

def send_to_sqs(queue_url: str, message_body: Dict[str, Any], message_attributes: Optional[Dict[str, Any]] = None):
    """
    Envia uma mensagem para a fila SQS especificada.
    """
    try:
        params = {
            "QueueUrl": queue_url,
            "MessageBody": json.dumps(message_body)
        }
        if message_attributes:
            params["MessageAttributes"] = message_attributes
        
        response = sqs_client.send_message(**params)
        logger.info(f"Mensagem enviada para {queue_url}. MessageId: {response['MessageId']}")
        return True
    except ClientError as e:
        logger.error(f"Erro ao enviar para SQS {queue_url}: {str(e)}")
        return False

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Handler principal da Lambda.
    """
    logger.info(f"Evento recebido: {json.dumps(event)}")
    
    # Validação do payload
    coins = event.get("coins", [])
    if not isinstance(coins, list) or len(coins) == 0:
        logger.error("Payload inválido: campo 'coins' ausente ou vazio")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Campo 'coins' ausente ou vazio"})
        }
    
    logger.info(f"Processando {len(coins)} moedas: {coins}")
    
    success_count = 0
    failure_count = 0
    
    # Processamento sequencial com rate limit
    for idx, coin_id in enumerate(coins):
        coin_id = coin_id.strip().lower()
        
        # Aplica rate limit entre requisições (exceto antes da primeira)
        if idx > 0:
            time.sleep(RATE_LIMIT_DELAY)
        
        # Busca dados OHLC
        result = fetch_ohlc(coin_id)
        
        if result["success"]:
            # Prepara mensagem de sucesso
            message_body = {
                "coin_id": coin_id,
                "ohlc_data": result["data"],
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "vs_currency": "usd"
            }
            message_attributes = {
                "source": {
                    "DataType": "String",
                    "StringValue": "producer_coingecko"
                }
            }
            
            # Envia para fila principal
            if send_to_sqs(SQS_QUEUE_URL, message_body, message_attributes):
                success_count += 1
                logger.info(f"Moeda {coin_id} processada com sucesso.")
            else:
                failure_count += 1
                logger.error(f"Falha ao enviar {coin_id} para fila principal.")
                # Envia para DLQ como falha de envio
                dlq_message = {
                    "coin_id": coin_id,
                    "error": "Falha ao enviar para SQS principal",
                    "http_status": 0,
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "retry_count": 0
                }
                send_to_sqs(SQS_DLQ_URL, dlq_message)
        else:
            # Falha na obtenção dos dados
            failure_count += 1
            logger.error(f"Falha ao obter dados para {coin_id}: {result.get('error')}")
            
            # Prepara mensagem de falha para DLQ
            dlq_message = {
                "coin_id": coin_id,
                "error": result.get("error", "Erro desconhecido"),
                "http_status": result.get("http_status", 0),
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "retry_count": MAX_RETRIES
            }
            send_to_sqs(SQS_DLQ_URL, dlq_message)
    
    logger.info(f"Processamento concluído. Sucessos: {success_count}, Falhas: {failure_count}")
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed_coins": len(coins),
            "successful": success_count,
            "failed": failure_count
        })
    }
