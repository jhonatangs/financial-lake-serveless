output "s3_bucket_names" {
  description = "Lista dos nomes reais dos buckets criados no S3"
  value       = { for k, v in aws_s3_bucket.layers : k => v.bucket }
}