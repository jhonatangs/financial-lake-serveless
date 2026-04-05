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