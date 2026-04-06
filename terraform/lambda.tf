resource "aws_lambda_function" "producer_yfinance" {
  function_name = "financial-producer-yfinance"
  role          = aws_iam_role.producer_role.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 60 
  memory_size   = 128
  filename      = "dummy.zip"

  environment {
    variables = {
      SQS_QUEUE_URL = aws_sqs_queue.financial_data_queue.id
      SQS_DLQ_URL = aws_sqs_queue.financial_dlq.id
    }
  }

  lifecycle {
    ignore_changes = [source_code_hash, filename]
  }
}

# Rodar de segunda a sexta, às 18h00 (UTC)
resource "aws_cloudwatch_event_rule" "daily_market_close" {
  name                = "trigger-daily-market-close"
  description         = "Dispara a lambda de ingestao no fechamento do mercado"
  schedule_expression = "cron(0 18 ? * MON-FRI *)"
}

# Dizemos para a regra engatilha a lambda producer_yfinance
resource "aws_cloudwatch_event_target" "target_yfinance" {
  rule      = aws_cloudwatch_event_rule.daily_market_close.name
  target_id = "TriggerYfinance"
  arn       = aws_lambda_function.producer_yfinance.arn
}

# Permite que o EventBridge invoque a lambda producer_yfinance
resource "aws_lambda_permission" "allow_eventbridge_yfinance" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.producer_yfinance.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_market_close.arn
}

resource "aws_lambda_function" "producer_coingecko" {
  function_name = "financial-producer-coingecko"
  role          = aws_iam_role.producer_role.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 300  # 5 minutos, conforme especificação
  memory_size   = 256
  filename      = "dummy.zip"

  environment {
    variables = {
      SQS_QUEUE_URL = aws_sqs_queue.financial_data_queue.id
      SQS_DLQ_URL   = aws_sqs_queue.financial_dlq.id
      LOG_LEVEL     = "INFO"
    }
  }

  lifecycle {
    ignore_changes = [source_code_hash, filename]
  }
}

resource "aws_cloudwatch_event_rule" "daily_midnight" {
  name                = "trigger-daily-midnight-coingecko"
  description         = "Dispara a lambda producer_coingecko diariamente à meia-noite UTC"
  schedule_expression = "cron(0 0 * * ? *)"
}

resource "aws_cloudwatch_event_target" "target_coingecko" {
  rule      = aws_cloudwatch_event_rule.daily_midnight.name
  target_id = "TriggerCoingecko"
  arn       = aws_lambda_function.producer_coingecko.arn
}

resource "aws_lambda_permission" "allow_eventbridge_coingecko" {
  statement_id  = "AllowExecutionFromEventBridgeCoingecko"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.producer_coingecko.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_midnight.arn
}

resource "aws_lambda_function" "consumer_s3" {
  function_name = "financial-consumer-s3"
  role          = aws_iam_role.consumer_role.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 120
  memory_size   = 256
  filename      = "dummy.zip"

  environment {
    variables = {
      RAW_BUCKET = aws_s3_bucket.layers["raw"].bucket
    }
  }

  lifecycle {
    ignore_changes = [source_code_hash, filename]
  }
}

resource "aws_lambda_event_source_mapping" "sqs_to_lambda" {
  event_source_arn = aws_sqs_queue.financial_data_queue.arn
  function_name    = aws_lambda_function.consumer_s3.arn
  batch_size       = 100
  maximum_batching_window_in_seconds = 60
  
  # Retorna apenas os itens que falharam, não o lote todo
  function_response_types = ["ReportBatchItemFailures"]
}

# Lambda Transformer Trusted
resource "aws_lambda_function" "transformer_trusted" {
  function_name = "financial-transformer-trusted"
  role          = aws_iam_role.transformer_role.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 300  # 5 minutos conforme especificação
  memory_size   = 512  # 512 MB conforme especificação
  filename      = "dummy.zip"

  # AWS Managed Layer para awswrangler (AWS SDK for pandas)
  layers = [
    "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:11"
  ]

  environment {
    variables = {
      TRUSTED_BUCKET       = aws_s3_bucket.layers["trusted"].bucket
      GLUE_DATABASE_NAME   = var.glue_database_name
      ICEBERG_LOCATION_PREFIX = var.iceberg_table_location_prefix
      LOG_LEVEL            = "INFO"
    }
  }

  lifecycle {
    ignore_changes = [source_code_hash, filename]
  }
}
