data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "producer_role" {
  name               = "financial-producer-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  description        = "Role usada pelas lambdas producer (yfinance, crypto, coingecko) para enviar mensagens para SQS e escrever logs"
}

resource "aws_iam_role_policy" "producer_policy" {
  name = "producer-sqs-write-policy"
  role = aws_iam_role.producer_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "sqs:SendMessage"
        ]
        Effect = "Allow"
        Resource = [
          aws_sqs_queue.financial_data_queue.arn,
          aws_sqs_queue.financial_dlq.arn
        ]
      },
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

resource "aws_iam_role" "consumer_role" {
  name               = "financial-consumer-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy" "consumer_policy" {
  name = "consumer-sqs-read-s3-write-policy"
  role = aws_iam_role.consumer_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes"
        ]
        Effect   = "Allow"
        Resource = aws_sqs_queue.financial_data_queue.arn
      },
      {
        Action = [
          "s3:PutObject"
        ]
        Effect   = "Allow"
        Resource = "${aws_s3_bucket.layers["raw"].arn}/*"
      },
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# Role para a Lambda Transformer Trusted
resource "aws_iam_role" "transformer_role" {
  name               = "financial-transformer-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  description        = "Role usada pela lambda transformer_trusted para ler S3 raw, escrever S3 trusted e acessar Glue Data Catalog"
}

resource "aws_iam_role_policy" "transformer_policy" {
  name = "transformer-s3-glue-policy"
  role = aws_iam_role.transformer_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Leitura do bucket raw
      {
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Effect = "Allow"
        Resource = [
          aws_s3_bucket.layers["raw"].arn,
          "${aws_s3_bucket.layers["raw"].arn}/*"
        ]
      },
      # Escrita no bucket trusted (para tabelas Iceberg)
      {
        Action = [
          "s3:GetBucketLocation",
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Effect = "Allow"
        Resource = [
          aws_s3_bucket.layers["trusted"].arn,
          "${aws_s3_bucket.layers["trusted"].arn}/*"
        ]
      },
      # Permissões Glue Data Catalog (Iceberg)
      {
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable"
        ]
        Effect   = "Allow"
        Resource = "*"
      },
      # Permissões Lake Formation (necessárias para operações Iceberg)
      {
        Action = [
          "lakeformation:GetDataAccess"
        ]
        Effect   = "Allow"
        Resource = "*"
      },
      # Permissões Athena (necessárias para wr.athena.to_iceberg)
      {
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:GetWorkGroup"
        ]
        Effect   = "Allow"
        Resource = "*"
      },
      # Logs
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}
