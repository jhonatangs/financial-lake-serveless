output "s3_bucket_names" {
  description = "Lista dos nomes reais dos buckets criados no S3"
  value       = { for k, v in aws_s3_bucket.layers : k => v.bucket }
}

output "lambda_deployment_bucket_name" {
  description = "Nome do bucket S3 para deployment de pacotes Lambda"
  value       = aws_s3_bucket.lambda_deployment.bucket
}
