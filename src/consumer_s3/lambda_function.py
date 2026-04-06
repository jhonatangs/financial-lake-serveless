import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import boto3
from botocore.exceptions import ClientError

# Configuração de logging
logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# Clientes AWS
s3_client = boto3.client("s3")

# Variáveis de ambiente
RAW_BUCKET = os.getenv("RAW_BUCKET")
if not RAW_BUCKET:
    logger.error("Variável de ambiente RAW_BUCKET não configurada")
    raise ValueError("RAW_BUCKET environment variable is required")

# Fontes conhecidas (para validação)
KNOWN_SOURCES = {"producer_yfinance", "producer_coingecko"}

def extract_message_data(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extrai e valida os dados da mensagem SQS.
    Retorna um dicionário com message_id, source e body se válido.
    Retorna None se a mensagem for inválida.
    """
    try:
        message_id = record.get("messageId")
        if not message_id:
            logger.warning("Record sem messageId")
            return None
        
        # Extrair source dos messageAttributes
        message_attrs = record.get("messageAttributes", {})
        source_attr = message_attrs.get("source")
        if not source_attr:
            logger.warning(f"Mensagem {message_id} sem atributo 'source'")
            return None
        
        source = source_attr.get("stringValue")
        if not source:
            logger.warning(f"Mensagem {message_id} com source vazio")
            return None
        
        # Validar source conhecido
        if source not in KNOWN_SOURCES:
            logger.warning(f"Mensagem {message_id} com source desconhecido: {source}")
            return None
        
        # Extrair body
        body_str = record.get("body")
        if not body_str:
            logger.warning(f"Mensagem {message_id} sem body")
            return None
        
        # Validar que body é JSON válido
        try:
            json.loads(body_str)
        except json.JSONDecodeError:
            logger.warning(f"Mensagem {message_id} com body JSON inválido")
            return None
        
        return {
            "message_id": message_id,
            "source": source,
            "body": body_str
        }
    
    except Exception as e:
        logger.error(f"Erro ao extrair dados da mensagem: {str(e)}")
        return None

def build_s3_key(source: str, message_id: str, ingest_time: datetime) -> str:
    """
    Constrói a chave S3 no formato particionado:
    raw/{source}/year=YYYY/month=MM/day=DD/{message_id}.json
    """
    year = ingest_time.strftime("%Y")
    month = ingest_time.strftime("%m")
    day = ingest_time.strftime("%d")
    
    return f"raw/{source}/year={year}/month={month}/day={day}/{message_id}.json"

def upload_to_s3(bucket: str, key: str, body: str) -> bool:
    """
    Faz upload do conteúdo para o S3.
    Retorna True se bem-sucedido, False caso contrário.
    """
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
            # Server-side encryption padrão do bucket será aplicada
            # Storage class padrão (STANDARD)
        )
        logger.info(f"Upload S3 bem-sucedido: s3://{bucket}/{key}")
        return True
    except ClientError as e:
        logger.error(f"Erro no upload S3 para {key}: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Erro inesperado no upload S3 para {key}: {str(e)}")
        return False

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Handler principal da Lambda Consumer S3.
    Processa mensagens SQS em lote e retorna falhas parciais.
    """
    logger.info(f"Iniciando processamento de {len(event.get('Records', []))} mensagens")
    
    batch_item_failures = []
    ingest_time = datetime.now(timezone.utc)
    
    for record in event.get("Records", []):
        message_id = record.get("messageId", "unknown")
        
        try:
            # Extrair e validar dados da mensagem
            message_data = extract_message_data(record)
            if not message_data:
                logger.warning(f"Mensagem {message_id} inválida, adicionando a falhas")
                batch_item_failures.append({"itemIdentifier": message_id})
                continue
            
            # Construir chave S3
            s3_key = build_s3_key(
                source=message_data["source"],
                message_id=message_data["message_id"],
                ingest_time=ingest_time
            )
            
            # Fazer upload para S3
            success = upload_to_s3(
                bucket=RAW_BUCKET,
                key=s3_key,
                body=message_data["body"]
            )
            
            if not success:
                logger.error(f"Falha no upload S3 para {message_id}")
                batch_item_failures.append({"itemIdentifier": message_id})
            else:
                logger.info(f"Mensagem {message_id} processada com sucesso")
                
        except Exception as e:
            logger.error(f"Erro inesperado ao processar mensagem {message_id}: {str(e)}")
            batch_item_failures.append({"itemIdentifier": message_id})
    
    # Log do resultado do processamento
    total_messages = len(event.get("Records", []))
    failed_count = len(batch_item_failures)
    success_count = total_messages - failed_count
    
    logger.info(
        f"Processamento concluído. "
        f"Total: {total_messages}, "
        f"Sucessos: {success_count}, "
        f"Falhas: {failed_count}"
    )
    
    # Retornar falhas parciais conforme especificação SQS
    return {"batchItemFailures": batch_item_failures}
