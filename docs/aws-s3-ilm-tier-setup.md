# Configure An AWS S3 ILM Tier

This guide configures a MinIO tenant with an AWS S3 low-storage tier that is
compatible with CWM MinIO Tierer.

The tierer only adds the configured marker tag. MinIO's lifecycle scanner
performs the transition asynchronously. A temporary restore creates a local
copy while the AWS copy remains authoritative.

The commands were validated using:

- MinIO `RELEASE.2025-07-23T15-54-02Z`
- `mc` `RELEASE.2025-07-21T05-28-08Z`

## Architecture

```text
MinIO tenant
  lifecycle rule: cwm-tier=low
        |
        v
remote tier: AWSLOW
        |
        v
dedicated AWS S3 bucket/prefix using STANDARD_IA
```

## 1. Prepare AWS S3

Use a dedicated bucket and prefix for each MinIO deployment:

```text
Bucket: company-minio-low-tier
Prefix: production-minio-a/
Region: eu-west-1
Class:  STANDARD_IA
```

Recommended AWS settings:

- Block all public access.
- Disable S3 bucket versioning for the destination tier.
- Do not configure expiration or Glacier lifecycle rules on the bucket.
- Do not manually modify objects under the MinIO-managed prefix.
- Use `STANDARD_IA` for multi-AZ durability.
- Do not use Glacier or Deep Archive. MinIO requires immediately retrievable
  remote objects.

`STANDARD_IA` has retrieval charges, a 30-day minimum storage charge, and
minimum billable object-size considerations.

## 2. Create AWS IAM Permissions

Create a dedicated IAM user or role for this MinIO tier. Do not reuse the
tierer's MinIO credentials.

Example policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "BucketMetadata",
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation",
        "s3:ListBucketMultipartUploads"
      ],
      "Resource": "arn:aws:s3:::company-minio-low-tier"
    },
    {
      "Sid": "ListTierPrefix",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::company-minio-low-tier",
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "production-minio-a",
            "production-minio-a/*"
          ]
        }
      }
    },
    {
      "Sid": "TierObjects",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts"
      ],
      "Resource": "arn:aws:s3:::company-minio-low-tier/production-minio-a/*"
    }
  ]
}
```

If the bucket uses SSE-KMS, also grant access to the KMS key, including
`kms:Encrypt`, `kms:Decrypt`, `kms:GenerateDataKey`, and `kms:DescribeKey`.

## 3. Configure The MinIO Alias

Run `mc` from an administrative workstation or pod that can reach the MinIO
tenant endpoint:

```bash
export MINIO_ALIAS=tenant

mc alias set \
  "${MINIO_ALIAS}" \
  "https://minio.example.org" \
  "${MINIO_ADMIN_ACCESS_KEY}" \
  "${MINIO_ADMIN_SECRET_KEY}"

mc admin info "${MINIO_ALIAS}"
```

Use a trusted CA rather than `--insecure`.

## 4. Add The AWS S3 Tier

```bash
export AWS_TIER_NAME=AWSLOW
export AWS_TIER_BUCKET=company-minio-low-tier
export AWS_TIER_PREFIX=production-minio-a/
export AWS_REGION=eu-west-1

mc ilm tier add s3 \
  "${MINIO_ALIAS}" \
  "${AWS_TIER_NAME}" \
  --endpoint "https://s3.${AWS_REGION}.amazonaws.com" \
  --region "${AWS_REGION}" \
  --bucket "${AWS_TIER_BUCKET}" \
  --prefix "${AWS_TIER_PREFIX}" \
  --storage-class "STANDARD_IA" \
  --access-key "${AWS_TIER_ACCESS_KEY}" \
  --secret-key "${AWS_TIER_SECRET_KEY}"
```

The credentials become part of MinIO's tier configuration. Protect shell
history and process visibility when using static credentials.

If the MinIO tenant runs on AWS/EKS, an AWS role or web-identity configuration
is preferable. The relevant options are `--use-aws-role`, `--aws-role-arn`, and
`--aws-web-identity-file`. The role and token must be available to the MinIO
tenant pods.

## 5. Verify The Tier

```bash
mc ilm tier info "${MINIO_ALIAS}" "${AWS_TIER_NAME}"
mc ilm tier check "${MINIO_ALIAS}" "${AWS_TIER_NAME}"
```

`tier check` performs real connectivity and object operations. A failure
usually indicates one of the following:

- Incorrect region or endpoint.
- Missing IAM or KMS permissions.
- DNS, firewall, proxy, or TLS failure.
- Incorrect bucket or prefix.

Do not add lifecycle rules until `tier check` succeeds.

## 6. Add Rules To Source Buckets

Configure CWM MinIO Tierer with the same marker:

```text
TIERER_MARKER_KEY=cwm-tier
TIERER_MARKER_VALUE=low
```

Add a matching lifecycle rule to each MinIO source bucket:

```bash
export SOURCE_BUCKET=mybucket

mc ilm rule add \
  --tags "cwm-tier=low" \
  --transition-days "1" \
  --transition-tier "${AWS_TIER_NAME}" \
  "${MINIO_ALIAS}/${SOURCE_BUCKET}"
```

Verify the rule:

```bash
mc ilm rule ls \
  "${MINIO_ALIAS}/${SOURCE_BUCKET}" \
  --transition
```

Use one transition day for AWS S3 rather than relying on zero-day behavior
documented specifically for remote MinIO tiers. Eligibility is based on object
age, not when the tierer added the tag. With a 24-hour low-usage window, tagged
objects are normally already old enough to become immediately eligible.

Do not use `mc ilm rule remove --all` in production. It would destroy unrelated
lifecycle policies.

For versioned source buckets, this rule applies to current objects. The tierer
intentionally scans current logical objects only. Do not add noncurrent-version
transition rules unless old versions are managed separately.

## 7. Canary Test

Start with a dedicated bucket and an object older than one day:

```bash
mc tag set \
  "${MINIO_ALIAS}/${SOURCE_BUCKET}/canary/object.txt" \
  "cwm-tier=low"
```

Inspect the rule and object:

```bash
mc ilm rule ls "${MINIO_ALIAS}/${SOURCE_BUCKET}" --transition
mc stat "${MINIO_ALIAS}/${SOURCE_BUCKET}/canary/object.txt"
```

Wait for MinIO's asynchronous lifecycle scanner. Once transitioned, `mc stat`
should expose the configured remote tier, such as `AWSLOW`.

Test temporary restore:

```bash
mc ilm restore \
  --days 1 \
  "${MINIO_ALIAS}/${SOURCE_BUCKET}/canary/object.txt"

mc stat "${MINIO_ALIAS}/${SOURCE_BUCKET}/canary/object.txt"
```

The restored local copy expires according to MinIO's UTC calendar-day
semantics. The AWS copy remains in `STANDARD_IA`.

## 8. Tierer Rollout

1. Configure and verify the AWS tier.
2. Add the tag-filtered rule to one canary bucket.
3. Run `minio-tierer` in its default `audit` mode.
4. Verify coverage records, counts, proposed tags, and daily budgets.
5. Switch to `apply` with very small transition and restore budgets.
6. Confirm objects become marked, transitioned to `AWSLOW`, and restorable.
7. Expand to additional buckets and gradually increase budgets.

## Rollback

Return the tierer to audit mode and disable the lifecycle rule to stop new
transitions. Do not delete the AWS tier configuration or S3 objects while
transitioned MinIO objects still reference them.
