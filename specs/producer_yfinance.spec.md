# Especificação Técnica: Lambda Producer yFinance

## 1. Visão Geral
### 1.1. Contexto
Esta função AWS Lambda tem como objetivo automatizar a coleta diária de dados financeiros OHLCV (Open, High, Low, Close, Volume) do Yahoo Finance via biblioteca `yfinance`, no horário de fechamento do mercado (18h). Os dados coletados são estruturados em JSON e encaminhados para uma fila SQS para posterior consumo por sistemas downstream (processamento ETL, análise, armazenamento).

### 1.2. Escopo
- **Gatilho**: EventBridge agendado diariamente às 18:00 (horário de Brasília).
- **Entrada**: Payload JSON no formato `{"tickers": ["AAPL", "MSFT", "PETR4.SA"]}`. Se o gatilho for via EventBridge schedule, o payload pode ser configurado na regra.
- **Processo**: Para cada ticker, busca dados OHLCV do dia atual via `yfinance`, aplica tratamento de erros e retentativas.
- **Saída**: Mensagem JSON publicada em fila SQS configurável, com atributos de mensagem para identificação da origem.
- **Fora do Escopo**:
  - Armazenamento persistente direto (S3, banco de dados).
  - Transformações complexas nos dados (agregações, cálculos derivados).
  - Interface de usuário ou API de consulta.

### 1.3. Definições e Acrônimos
- **OHLCV**: Open, High, Low, Close, Volume – dados candlestick de mercado.
- **SQS**: Simple Queue Service – serviço de filas gerenciado da AWS.
- **DLQ**: Dead-Letter Queue – fila para mensagens que falharam após retentativas.
- **IAM**: Identity and Access Management – serviço de permissões da AWS.
- **VPC**: Virtual Private Cloud – rede isolada na AWS.

## 2. Requisitos Funcionais
| ID   | Descrição                                                                                              | Prioridade |
|------|--------------------------------------------------------------------------------------------------------|------------|
| RF01 | A função deve ser acionada automaticamente todos os dias às 18:00 via Amazon EventBridge.              | Alta       |
| RF02 | A lista de tickers a serem consultados deve ser lida do payload do evento no formato JSON `{"tickers": ["AAPL", "MSFT"]}`. | Alta       |
| RF03 | Para cada ticker, a função deve utilizar a biblioteca `yfinance` para buscar dados OHLCV do dia atual (período "1d"). | Alta       |
| RF04 | Cada registro de saída deve conter os campos: `ticker`, `timestamp`, `open`, `high`, `low`, `close`, `volume`. | Alta       |
| RF05 | Implementar mecanismo de retentativa (retry) em caso de falha na consulta à API do Yahoo Finance ou resposta vazia. | Alta       |
| RF06 | Publicar cada registro como mensagem JSON na fila SQS cuja URL é configurada via variável de ambiente `SQS_QUEUE_URL`. | Alta       |
| RF07 | Adicionar um atributo de mensagem (`MessageAttribute`) chamado `source` com valor `producer_yfinance` para identificação da origem. | Média      |
| RF08 | Registrar em logs (CloudWatch) o sucesso/falha de cada ticker processado, incluindo métricas de contagem. | Média      |
| RF09 | Em caso de falha persistente após retentativas, enviar a mensagem para uma DLQ configurada no Lambda.  | Baixa      |

## 3. Requisitos Não‑Funcionais
| ID    | Descrição                                                                                              | Métrica de Aceitação                                                                 |
|-------|--------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------|
| RNF01 | **Confiabilidade**: Taxa de sucesso ≥ 99% na entrega de mensagens para a SQS (considerando retentativas). | CloudWatch Metric `SuccessRate` ≥ 99% em janela móvel de 7 dias.                     |
| RNF02 | **Performance**: Processar até 50 tickers em menos de 3 minutos (timeout padrão do Lambda).            | 95% das execuções completam em ≤ 2 minutos com 512 MB de memória.                    |
| RNF03 | **Disponibilidade**: A função deve ser executada na AWS Lambda, que garante disponibilidade regional.  | SLA da AWS Lambda (99,95% regional).                                                 |
| RNF04 | **Segurança**: Todas as credenciais e URLs sensíveis devem ser injetadas via variáveis de ambiente, nunca hard‑coded. | Verificação de código e revisão de segurança.                                        |
| RNF05 | **Monitorabilidade**: Logs detalhados devem ser enviados para CloudWatch Logs com retenção de 30 dias. | Log group `/aws/lambda/producer_yfinance` criado e com política de retenção aplicada.|
| RNF06 | **Manutenibilidade**: Código em Python 3.12, seguindo PEP‑8, com cobertura de testes ≥ 80%.            | Pipeline de CI/CD rejeita se coverage < 80% ou se houver violações críticas de lint. |

## 4. Design da Arquitetura
### 4.1. Diagrama de Componentes
```
EventBridge (cron: 0 18 * * ? *)  
        ↓
AWS Lambda (producer_yfinance)
        ↓
    Amazon SQS (fila principal)
        ↓ (falha após retentativas)
    Amazon SQS (DLQ)
```

### 4.2. Fluxo de Execução
1. **Trigger**: EventBridge dispara a Lambda diariamente às 18:00 (expressão cron: `cron(0 18 * * ? *)`). O payload pode ser configurado na regra do EventBridge.
2. **Configuração**:
   - A função lê o payload do evento, extrai a lista de tickers do campo `tickers`.
   - Valida se a lista existe e não está vazia. Caso contrário, retorna erro 400.
   - Lê `SQS_QUEUE_URL` das variáveis de ambiente.
3. **Coleta de Dados**:
   - Para cada ticker no array:
     - Chama `yfinance.download(ticker, period="1d")`.
     - Se a resposta for vazia ou ocorrer exceção, aguarda 2 segundos e repete (máximo 3 tentativas).
     - Após 3 falhas, registra erro e passa para o próximo ticker.
4. **Transformação**:
   - Converte o DataFrame resultante para um dicionário Python.
   - Extrai os valores OHLCV e o timestamp (último índice do DataFrame).
   - Formata o timestamp como string ISO 8601.
5. **Publicação**:
   - Cria um objeto JSON com a estrutura:
     ```json
     {
       "ticker": "AAPL",
       "timestamp": "2025-04-05T20:00:00Z",
       "open": 170.25,
       "high": 172.50,
       "low": 169.80,
       "close": 171.75,
       "volume": 75000000
     }
     ```
   - Envia a mensagem para a SQS usando `boto3.client('sqs').send_message()`.
   - Adiciona o `MessageAttribute` `source` com valor `producer_yfinance`.
6. **Tratamento de Erros**:
   - Exceções não tratadas são capturadas e registradas.
   - O Lambda está configurado com uma DLQ (SQS) para receber eventos que falharam após todas as retentativas.

## 5. Especificações Técnicas Detalhadas
### 5.1. Variáveis de Ambiente
| Nome              | Obrigatório | Descrição                                                                 | Exemplo                     |
|-------------------|-------------|---------------------------------------------------------------------------|-----------------------------|
| `SQS_QUEUE_URL`   | Sim         | URL completa da fila SQS de destino.                                      | `https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue` |
| `MAX_RETRIES`     | Não         | Número máximo de tentativas por ticker (padrão: 3).                       | `3`                         |
| `RETRY_DELAY_SEC` | Não         | Delay entre tentativas em segundos (padrão: 2).                           | `2`                         |

### 5.2. Estrutura da Mensagem SQS
```json
{
  "Id": "unique-id",
  "MessageBody": "{\"ticker\":\"AAPL\",\"timestamp\":\"2025-04-05T20:00:00Z\",\"open\":170.25,\"high\":172.50,\"low\":169.80,\"close\":171.75,\"volume\":75000000}",
  "MessageAttributes": {
    "source": {
      "DataType": "String",
      "StringValue": "producer_yfinance"
    }
  }
}
```

### 5.3. Permissões IAM (Política Mínima)
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sqs:SendMessage",
        "sqs:GetQueueAttributes"
      ],
      "Resource": [
        "arn:aws:sqs:us-east-1:123456789012:MyQueue",
        "arn:aws:sqs:us-east-1:123456789012:MyQueue_DLQ"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    }
  ]
}
```

### 5.4. Estrutura do Evento de Entrada
O evento pode ser configurado no EventBridge para incluir um payload personalizado. A Lambda espera:

```json
{
  "tickers": ["AAPL", "MSFT", "PETR4.SA"]
}
```

Para gatilhos via EventBridge schedule, o payload pode ser configurado na regra. Exemplo de evento completo:

```json
{
  "version": "0",
  "id": "event-id",
  "detail-type": "Scheduled Event",
  "source": "aws.events",
  "account": "123456789012",
  "time": "2025-04-05T18:00:00Z",
  "region": "us-east-1",
  "resources": ["arn:aws:events:us-east-1:123456789012:rule/daily-market-close"],
  "detail": {
    "tickers": ["AAPL", "MSFT", "PETR4.SA"]
  }
}
```

A função extrairá a lista de `tickers` do campo `detail` (se existir) ou do próprio corpo do evento.

## 6. Considerações de Implementação
### 6.1. Código Python (Esboço)
```python
import os
import json
import time
import yfinance as yf
import boto3
from typing import Dict, List, Optional

def fetch_ticker_data(ticker: str, max_retries: int = 3, retry_delay: int = 2) -> Optional[Dict]:
    for attempt in range(max_retries):
        try:
            data = yf.download(ticker, period="1d", progress=False)
            if data.empty:
                raise ValueError(f"No data for {ticker}")
            last_row = data.iloc[-1]
            return {
                "ticker": ticker,
                "timestamp": data.index[-1].isoformat(),
                "open": float(last_row["Open"]),
                "high": float(last_row["High"]),
                "low": float(last_row["Low"]),
                "close": float(last_row["Close"]),
                "volume": int(last_row["Volume"])
            }
        except Exception as e:
            if attempt == max_retries - 1:
                return None
            time.sleep(retry_delay)
    return None

def lambda_handler(event, context):
    # Extrai a lista de tickers do payload do evento
    # Suporta tanto eventos diretos {"tickers": [...]} quanto eventos do EventBridge com campo 'detail'
    if "detail" in event and isinstance(event["detail"], dict) and "tickers" in event["detail"]:
        tickers = event["detail"]["tickers"]
    elif "tickers" in event:
        tickers = event["tickers"]
    else:
        tickers = []
    
    # Validação da lista de tickers
    if not tickers or not isinstance(tickers, list) or len(tickers) == 0:
        logger.error("Lista de tickers ausente, vazia ou em formato inválido no payload do evento")
        return {
            "statusCode": 400,
            "body": json.dumps({
                "error": "A lista de tickers deve ser fornecida no campo 'tickers' do payload e não pode estar vazia",
                "received_event": event
            })
        }
    
    sqs_queue_url = os.getenv("SQS_QUEUE_URL")
    max_retries = int(os.getenv("MAX_RETRIES", "3"))
    retry_delay = int(os.getenv("RETRY_DELAY_SEC", "2"))
    
    sqs = boto3.client('sqs')
    processed = 0
    failed = 0
    
    for ticker in tickers:
        ticker = ticker.strip()
        if not ticker:
            continue
            
        data = fetch_ticker_data(ticker, max_retries, retry_delay)
        if data is None:
            failed += 1
            continue
            
        response = sqs.send_message(
            QueueUrl=sqs_queue_url,
            MessageBody=json.dumps(data),
            MessageAttributes={
                "source": {
                    "DataType": "String",
                    "StringValue": "producer_yfinance"
                }
            }
        )
        processed += 1
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed": processed,
            "failed": failed
        })
    }
```

### 6.2. Dependências (requirements.txt)
```
yfinance==0.2.28
pandas==2.1.4
boto3==1.34.0
```

## 7. Plano de Testes
### 7.1. Testes Unitários
- Mock de `yfinance.download` para simular respostas válidas, vazias e exceções.
- Verificação do formato JSON de saída.
- Teste do mecanismo de retry com falhas simuladas.

### 7.2. Testes de Integração
- Execução em ambiente sandbox com EventBridge desabilitado (trigger manual).
- Validação de entrega na fila SQS real (sandbox).
- Verificação dos atributos de mensagem.

### 7.3. Testes de Carga
- Processamento de 100 tickers em uma execução.
- Monitoramento de tempo de execução e consumo de memória.

## 8. Rollout e Monitoramento
### 8.1. Implantação
1. Criar função Lambda com runtime Python 3.12.
2. Configurar variáveis de ambiente e permissões IAM.
3. Criar regra do EventBridge com expressão cron `cron(0 18 * * ? *)`.
4. Configurar DLQ (SQS) para a Lambda.
5. Implantar código via CI/CD (ex: AWS SAM, Terraform).

### 8.2. Métricas de Monitoramento (CloudWatch)
- `Invocations`: Número de execuções.
- `Errors`: Falhas na execução.
- `Duration`: Tempo de execução.
- `SuccessRate`: (processed / total tickers) * 100.
- Mensagens na DLQ (alerta se > 0).

### 8.3. Alertas
- Erros consecutivos (> 3 em 1 hora).
- DLQ com mensagens (> 0).
- Duração acima de 2,5 minutos.

---
*Documento gerado por: Arquitetura de Dados*
*Versão: 1.0*
*Última atualização: 2025‑04‑05*
