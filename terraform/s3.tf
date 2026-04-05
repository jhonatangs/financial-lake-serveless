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

data "aws_caller_identity" "current" {}
