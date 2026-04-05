resource "aws_lambda_function" "producer_yfinance" {
  function_name = "financial-producer-yfinance"
  role          = aws_iam_role.producer_role.arn
  handler       = "producer.lambda_handler"
  runtime       = "python3.10"
  timeout       = 60 
  memory_size   = 128
  filename      = "dummy.zip"

  environment {
    variables = {
      SQS_QUEUE_URL = aws_sqs_queue.financial_data_queue.id
    }
  }

  lifecycle {
    ignore_changes = [source_code_hash, filename]
  }
}

resource "aws_lambda_function" "producer_crypto" {
  function_name = "financial-producer-crypto"
  role          = aws_iam_role.producer_role.arn # Reutilizamos a mesma role!
  handler       = "producer.lambda_handler"
  runtime       = "python3.10"
  timeout       = 60 
  memory_size   = 128
  filename      = "dummy.zip"

  environment {
    variables = {
      SQS_QUEUE_URL = aws_sqs_queue.financial_data_queue.id
    }
  }

  lifecycle {
    ignore_changes = [source_code_hash, filename]
  }
}

# Rodar de segunda a sexta, às 18h00 (UTC)
resource "aws_cloudwatch_event_rule" "daily_market_close" {
  name                = "trigger-daily-market-close"
  description         = "Dispara as lambdas de ingestao no fechamento do mercado"
  schedule_expression = "cron(0 18 ? * MON-FRI *)"
}

# Dizemos para a regra engatilhar as duas lambdas
resource "aws_cloudwatch_event_target" "target_yfinance" {
  rule      = aws_cloudwatch_event_rule.daily_market_close.name
  target_id = "TriggerYfinance"
  arn       = aws_lambda_function.producer_yfinance.arn
}

resource "aws_cloudwatch_event_target" "target_crypto" {
  rule      = aws_cloudwatch_event_rule.daily_market_close.name
  target_id = "TriggerCrypto"
  arn       = aws_lambda_function.producer_crypto.arn
}

# Permite que o EventBridge invoque as Lambdas
resource "aws_lambda_permission" "allow_eventbridge_yfinance" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.producer_yfinance.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_market_close.arn
}

resource "aws_lambda_permission" "allow_eventbridge_crypto" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.producer_crypto.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_market_close.arn
}

resource "aws_lambda_function" "consumer_s3" {
  function_name = "financial-consumer-s3"
  role          = aws_iam_role.consumer_role.arn
  handler       = "consumer.lambda_handler"
  runtime       = "python3.10"
  timeout       = 120
  memory_size   = 256
  filename      = "dummy.zip"

  environment {
    variables = {
      BRONZE_BUCKET = aws_s3_bucket.layers["bronze"].bucket
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