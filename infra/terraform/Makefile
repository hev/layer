.PHONY: bootstrap init plan apply destroy fmt validate output \
       kubeconfig status logs ecr-login

TF_BACKEND_CONFIG ?= backend.hcl
TF_STATE_REGION ?= us-east-1
TF_STATE_BUCKET ?= hevlayer-$(shell aws sts get-caller-identity --query Account --output text)-terraform-state
TF_LOCK_TABLE ?= terraform-lock

################################################################################
# Terraform
################################################################################

bootstrap:
	@echo "Creating S3 bucket for Terraform state: $(TF_STATE_BUCKET)"
	aws s3api create-bucket --bucket $(TF_STATE_BUCKET) --region $(TF_STATE_REGION)
	aws s3api put-bucket-versioning --bucket $(TF_STATE_BUCKET) --versioning-configuration Status=Enabled
	aws s3api put-bucket-encryption --bucket $(TF_STATE_BUCKET) --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
	aws s3api put-public-access-block --bucket $(TF_STATE_BUCKET) --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
	@echo "Creating DynamoDB table for state locking..."
	aws dynamodb create-table --table-name $(TF_LOCK_TABLE) --attribute-definitions AttributeName=LockID,AttributeType=S --key-schema AttributeName=LockID,KeyType=HASH --billing-mode PAY_PER_REQUEST --region $(TF_STATE_REGION)
	@echo "Bootstrap complete. Write $(TF_BACKEND_CONFIG), then run 'make init'."

init:
	terraform init -migrate-state -backend-config=$(TF_BACKEND_CONFIG)

plan:
	terraform plan

apply:
	terraform apply

destroy:
	terraform destroy

fmt:
	terraform fmt -recursive

validate:
	terraform validate

output:
	terraform output

################################################################################
# Ops
################################################################################

kubeconfig:
	aws eks update-kubeconfig --region us-east-1 --name $$(terraform output -raw cluster_name)

status:
	kubectl get pods,svc,deployments -n layer

logs:
	kubectl logs -f -l app.kubernetes.io/name=layer -n layer

ecr-login:
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $(shell terraform output -raw ecr_repository_url | cut -d/ -f1)
