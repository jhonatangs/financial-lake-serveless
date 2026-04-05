resource "random_id" "bucket_suffix" {
  byte_length = 4
}

variable "buckets" {
    description = "List of buckets"
    type        = list(string)
    default     = ["bronze", "silver", "gold"]
}