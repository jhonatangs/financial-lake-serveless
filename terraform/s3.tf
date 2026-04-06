resource "aws_s3_bucket" "layers" {
    for_each = toset(var.buckets)

    bucket = "lakehouse-${each.key}-${random_id.bucket_suffix.hex}"
    force_destroy = true

    tags = {
        Name = "${each.key}-zone"
        Environment = "dev"
        ManagedBy = "terraform"
    }
}

resource "aws_s3_bucket" "lambda_deployment" {
    bucket = "lambda-deployment-${random_id.bucket_suffix.hex}"
    force_destroy = true

    tags = {
        Name = "lambda-deployment"
        Environment = "dev"
        ManagedBy = "terraform"
    }
}

resource "aws_s3_bucket_policy" "lambda_deployment_policy" {
    bucket = aws_s3_bucket.lambda_deployment.id
    policy = jsonencode({
        Version = "2012-10-17"
        Statement = [
            {
                Sid       = "AllowLambdaUpdate"
                Effect    = "Allow"
                Principal = {
                    AWS = "*"
                }
                Action   = [
                    "s3:GetObject"
                ]
                Resource = "${aws_s3_bucket.lambda_deployment.arn}/*"
                Condition = {
                    StringEquals = {
                        "aws:SourceAccount" = data.aws_caller_identity.current.account_id
                    }
                }
            },
            {
                Sid       = "AllowGitHubActionsUpload"
                Effect    = "Allow"
                Principal = {
                    AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
                }
                Action   = [
                    "s3:PutObject",
                    "s3:ListBucket"
                ]
                Resource = [
                    aws_s3_bucket.lambda_deployment.arn,
                    "${aws_s3_bucket.lambda_deployment.arn}/*"
                ]
            }
        ]
    })
}

# Notificação S3 para disparar a Lambda Transformer Trusted
resource "aws_s3_bucket_notification" "raw_bucket_notification" {
  bucket = aws_s3_bucket.layers["raw"].id

  lambda_function {
    lambda_function_arn = aws_lambda_function.transformer_trusted.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
    filter_suffix       = ".json"
  }

  depends_on = [
    aws_lambda_permission.allow_s3_raw_to_invoke_transformer
  ]
}

# Permissão para o bucket raw invocar a Lambda
resource "aws_lambda_permission" "allow_s3_raw_to_invoke_transformer" {
  statement_id  = "AllowExecutionFromS3RawBucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transformer_trusted.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.layers["raw"].arn
}

data "aws_caller_identity" "current" {}
