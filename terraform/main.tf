# main.tf - S3 Static Website Hosting ONLY

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

# --- S3 BUCKET FOR PUBLIC WEBSITE ---
resource "aws_s3_bucket" "dashboard" {
  # Bucket names must be globally unique.
  bucket = "my-unique-ecommerce-dashboard-app-20251103" 
}

# 1. Turn OFF the "Block all public access" settings for this bucket
resource "aws_s3_bucket_public_access_block" "dashboard" {
  bucket = aws_s3_bucket.dashboard.id

  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

# 2. Attach a bucket policy that allows public READ access to all objects
resource "aws_s3_bucket_policy" "dashboard" {
  bucket = aws_s3_bucket.dashboard.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Sid    = "PublicReadGetObject",
      Effect = "Allow",
      Principal = "*",
      Action = "s3:GetObject",
      Resource = "${aws_s3_bucket.dashboard.arn}/*"
    }]
  })
}

# 3. Configure the bucket to act as a website
resource "aws_s3_bucket_website_configuration" "dashboard" {
  bucket = aws_s3_bucket.dashboard.id

  index_document {
    suffix = "index.html"
  }
  error_document {
    key = "error.html"
  }
}

# --- OUTPUTS ---
output "s3_bucket_name" {
  description = "S3 Bucket Name for uploading files"
  value       = aws_s3_bucket.dashboard.id
}

output "s3_website_url" {
  description = "The URL of the S3 static website"
  value       = aws_s3_bucket_website_configuration.dashboard.website_endpoint
}