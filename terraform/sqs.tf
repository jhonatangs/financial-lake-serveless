# A Fila SQS Principal (Onde as Lambdas Produtoras vão jogar os dados)
resource "aws_sqs_queue" "financial_data_queue" {
  name                       = "financial-api-ingestion-queue"
  visibility_timeout_seconds = 180 # Tempo que a Lambda Consumidora tem para processar antes da msg voltar pra fila
  message_retention_seconds  = 86400 # Retém a mensagem por 1 dia se der erro severo
  
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.financial_dlq.arn
    maxReceiveCount     = 3 # Tenta processar 3 vezes. Falhou? Vai pra DLQ.
  })
}

# A Fila DLQ (Dead Letter Queue - Fila de Mensagens Mortas)
resource "aws_sqs_queue" "financial_dlq" {
  name = "financial-api-ingestion-dlq"
}