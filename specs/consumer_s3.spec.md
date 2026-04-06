# Especificação Técnica: Lambda Consumer S3

## 1. Visão Geral
Função AWS Lambda em Python que consome mensagens de uma fila SQS, extrai os dados e os armazena como arquivos JSON particionados no S3 (camada Raw). A função é projetada para processamento em lote com suporte a resposta parcial de falhas (Partial Batch Response).

## 2. Gatilho (Trigger)
- **Serviço**: Amazon Simple Queue Service (SQS)
- **Tipo de Evento**: Eventos de fila SQS em lote
- **Configuração**:
  - Tamanho máximo do lote: 100 mensagens
  - Janela máxima de agrupamento: 60 segundos
  - Relatório de falhas de itens do lote habilitado (`ReportBatchItemFailures`)
- **Comportamento**: A Lambda é invocada automaticamente quando há mensagens disponíveis na fila SQS `financial-api-ingestion-queue`.

## 3. Processamento

### 3.1. Estrutura do Evento SQS
```json
{
  "Records": [
    {
      "messageId": "19dd0b57-b21e-4ac1-bd88-01bbb068cb78",
      "receiptHandle": "MessageReceiptHandle",
      "body": "{\"ticker\":\"AAPL\",\"price\":182.63,\"timestamp\":\"2025-04-05T18:00:00Z\"}",
      "attributes": {...},
      "messageAttributes": {
        "source": {
          "stringValue": "producer_yfinance",
          "dataType": "String"
        }
      },
      "md5OfBody": "...",
      "eventSource": "aws:sqs",
      "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:financial-api-ingestion-queue",
      "awsRegion": "us-east-1"
    }
  ]
}
```

### 3.2. Fluxo de Processamento
1. **Iteração sobre Records**: Para cada mensagem no array `Records`:
   - Extrair `messageId`
   - Extrair `source` de `messageAttributes.source.stringValue`
   - Extrair `body` (string JSON)
2. **Validação**:
   - Verificar se `source` está presente e é um valor conhecido (`producer_yfinance`, `producer_coingecko`)
   - Validar que `body` é um JSON válido
3. **Construção do Caminho S3**:
   - Usar a data atual de ingestão (UTC) para particionamento
   - Formato: `raw/{source}/year=YYYY/month=MM/day=DD/{message_id}.json`
   - Exemplo: `raw/producer_yfinance/year=2025/month=04/day=05/19dd0b57-b21e-4ac1-bd88-01bbb068cb78.json`
4. **Upload para S3**:
   - Bucket: Variável de ambiente `RAW_BUCKET`
   - Conteúdo: O `body` original (string JSON) convertido para bytes UTF-8
   - Content-Type: `application/json`

## 4. Armazenamento S3 (Camada Raw)

### 4.1. Estrutura de Partições
```
s3://{RAW_BUCKET}/
├── raw/
│   ├── producer_yfinance/
│   │   ├── year=2025/
│   │   │   ├── month=04/
│   │   │   │   ├── day=05/
│   │   │   │   │   ├── 19dd0b57-b21e-4ac1-bd88-01bbb068cb78.json
│   │   │   │   │   └── ...
│   ├── producer_coingecko/
│   │   ├── year=2025/
│   │   │   ├── month=04/
│   │   │   │   ├── day=05/
│   │   │   │   │   ├── a1b2c3d4-e5f6-7890-1234-567890abcdef.json
│   │   │   │   │   └── ...
```

### 4.2. Metadados do Objeto
- **Storage Class**: Standard (padrão)
- **Encryption**: Server-Side Encryption (SSE-S3) padrão do bucket
- **Tags**: Opcionalmente adicionar tags para identificação (ex: `source={source}`)

## 5. Resiliência: Partial Batch Response

### 5.1. Objetivo
Garantir que apenas mensagens processadas com sucesso sejam removidas da fila SQS. Mensagens que falharem no processamento permanecerão visíveis para reprocessamento.

### 5.2. Implementação
- A função deve coletar os `messageId` de todas as mensagens que falharem durante o upload para o S3.
- No final do processamento, retornar um JSON com a lista de falhas:
  ```json
  {
    "batchItemFailures": [
      { "itemIdentifier": "failed_message_id_1" },
      { "itemIdentifier": "failed_message_id_2" }
    ]
  }
  ```
- A AWS SQS usará essa resposta para deletar apenas as mensagens bem-sucedidas da fila.

### 5.3. Cenários de Falha
1. **Falha de Upload S3**: Timeout, permissões insuficientes, bucket não encontrado.
2. **Mensagem Inválida**: JSON malformado, `source` ausente ou desconhecido.
3. **Erro Interno da Lambda**: Exceção não tratada, timeout da função.

## 6. Variáveis de Ambiente
| Nome | Descrição | Exemplo |
|------|-----------|---------|
| `RAW_BUCKET` | Nome do bucket S3 para armazenamento na camada Raw | `lakehouse-raw-abc123` |
| `LOG_LEVEL` | Nível de log da aplicação (DEBUG, INFO, WARN, ERROR) | `INFO` |

## 7. Requisitos Não Funcionais

### 7.1. Performance
- **Timeout Lambda**: 120 segundos (configurado via Terraform)
- **Memória**: 256 MB (suficiente para processar lotes de 100 mensagens)
- **Concorrência**: Pode ser ajustada conforme o volume de mensagens

### 7.2. Resiliência
- **Retry Nativo**: A fila SQS possui DLQ configurada com `maxReceiveCount = 3`
- **Logs**: Todos os eventos devem ser registrados no CloudWatch Logs com contexto (messageId, source, status)
- **Métricas**:
  - `MessagesProcessed`: Contador de mensagens processadas com sucesso
  - `MessagesFailed`: Contador de mensagens que falharam
  - `S3UploadLatency`: Tempo médio de upload para S3

### 7.3. Segurança
- **Permissões IAM**: Apenas permissões necessárias para:
  - Ler mensagens da fila SQS (`sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:GetQueueAttributes`)
  - Escrever objetos no bucket S3 Raw (`s3:PutObject`)
  - Escrever logs no CloudWatch
- **Criptografia**: Dados em trânsito (HTTPS) e em repouso (SSE-S3)

## 8. Fluxo de Execução

```mermaid
graph TD
    A[SQS Trigger Batch] --> B[Lambda Invocation]
    B --> C[Initialize batchItemFailures = []]
    C --> D{For each Record}
    D --> E[Extract source & body]
    E --> F{Valid source & body?}
    F -->|No| G[Add messageId to batchItemFailures]
    F -->|Yes| H[Build S3 path with partitioning]
    H --> I[Upload JSON to S3]
    I --> J{Upload succeeded?}
    J -->|No| G
    J -->|Yes| K[Log success]
    G --> L{More Records?}
    K --> L
    L -->|Yes| D
    L -->|No| M[Return batchItemFailures]
```

## 9. Considerações de Implementação

### 9.1. Código Python
- **Versão**: Python 3.12
- **Bibliotecas principais**:
  - `boto3` para interação com S3 e SQS
  - `json` para parsing/validação
  - `datetime` para geração de partições de data

### 9.2. Estrutura do Código
```python
import os
import json
import logging
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

s3_client = boto3.client("s3")
RAW_BUCKET = os.getenv("RAW_BUCKET")

def lambda_handler(event, context):
    batch_item_failures = []
    
    for record in event.get("Records", []):
        message_id = record.get("messageId")
        try:
            # Extrair source e body
            # Validar dados
            # Construir caminho S3
            # Fazer upload
            pass
        except Exception as e:
            logger.error(f"Failed to process message {message_id}: {str(e)}")
            batch_item_failures.append({"itemIdentifier": message_id})
    
    return {"batchItemFailures": batch_item_failures}
```

### 9.3. Testes
- **Unitários**: Mock de S3 e SQS, simulação de falhas
- **Integração**: Teste com bucket S3 real (ambiente de desenvolvimento)
- **Carga**: Processamento de lotes com 100 mensagens para validar performance

## 10. Monitoramento e Alertas

### 10.1. Métricas CloudWatch
- `MessagesProcessed`: Contador de mensagens processadas com sucesso
- `MessagesFailed`: Contador de mensagens que falharam
- `S3UploadLatency`: Tempo médio de upload para S3

### 10.2. Alertas
- **Alerta Crítico**: Taxa de falha superior a 10% por 5 minutos consecutivos
- **Alerta de Performance**: Latência média de upload S3 acima de 5 segundos
- **Alerta de Volume**: Mais de 1000 mensagens na DLQ

## 11. Rollback e Versionamento
- **Versionamento**: Cada deploy deve ter uma tag semântica
- **Rollback**: Manter versões anteriores da Lambda por 7 dias
- **Aliases**: Usar `prod` e `dev` para gerenciamento de ambientes

---

*Documento gerado em: 2025-04-05*
*Última revisão: -*
