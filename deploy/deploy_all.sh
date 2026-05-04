#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# deploy/deploy_all.sh — Build Docker images, push to Artifact Registry,
#                        and redeploy all Cloud Run Jobs + eval GCF.
#
# Run this after every code change to deploy new versions to GCP.
# Run deploy/setup_gcp.sh once first to create infrastructure.
#
# Prerequisites:
#   - gcloud CLI authenticated: gcloud auth login
#   - Docker running locally (for Cloud Run Job image builds)
#   - Artifact Registry repo already created (setup_gcp.sh)
#   Note: the eval Cloud Function is deployed from source — Docker is NOT
#         required for that component.
#
# Usage:
#   export GCP_PROJECT_ID=my-gcp-project
#   export GCP_REGION=us-central1          # optional, defaults to us-central1
#   bash deploy/deploy_all.sh
#
# To deploy only the eval Cloud Function (skip all Docker builds):
#   SKIP_FINANCIAL_UPDATE=1 SKIP_EDGAR=1 SKIP_SCREENER=1 bash deploy/deploy_all.sh
# ---------------------------------------------------------------------------

set -euo pipefail

# Source .env if present so EMAIL_FROM_ADDRESS / EMAIL_TO_ADDRESS and any
# other local overrides are available throughout the script.
if [[ -f ".env" ]]; then
  set -a; source .env; set +a
fi

# ---------------------------------------------------------------------------
# Configuration — override via environment variables
# ---------------------------------------------------------------------------
PROJECT_ID="${GCP_PROJECT_ID:?ERROR: GCP_PROJECT_ID is required}"
REGION="${GCP_REGION:-us-central1}"
ARTIFACT_REGISTRY_REPO="stock-screener"
REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REGISTRY_REPO}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
CLOUDRUN_SA="cloudrun-jobs"
GCF_SA="gcf-eval"
GCS_CONFIG_BUCKET="${GCS_CONFIG_BUCKET:-${PROJECT_ID}-config}"

# Cloud Run Job resource limits — adjust for your workload
CLOUDRUN_CPU="${CLOUDRUN_CPU:-2}"
CLOUDRUN_MEMORY="${CLOUDRUN_MEMORY:-4Gi}"
CLOUDRUN_TIMEOUT="${CLOUDRUN_TIMEOUT:-3600}"  # 1 hour in seconds
CLOUDRUN_MAX_RETRIES="${CLOUDRUN_MAX_RETRIES:-1}"

# Skip flags (set to 1 to skip building/deploying that component)
SKIP_FINANCIAL_UPDATE="${SKIP_FINANCIAL_UPDATE:-0}"
SKIP_EDGAR="${SKIP_EDGAR:-0}"
SKIP_SCREENER="${SKIP_SCREENER:-0}"
SKIP_EVAL_GCF="${SKIP_EVAL_GCF:-0}"

# Colours
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_step()  { echo -e "${BLUE}[STEP]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

log_info "=== Deploy all — project: ${PROJECT_ID}, region: ${REGION}, tag: ${IMAGE_TAG} ==="

# ---------------------------------------------------------------------------
# Helper: build + push a Docker image
# ---------------------------------------------------------------------------
build_and_push() {
    local image_name="$1"
    local dockerfile="$2"
    local full_image="${REGISTRY}/${image_name}:${IMAGE_TAG}"

    log_step "Building ${image_name}..."
    docker build \
        --file="${dockerfile}" \
        --tag="${full_image}" \
        --platform=linux/amd64 \
        .
    log_step "Pushing ${image_name} to Artifact Registry..."
    docker push "${full_image}"
    log_info "${image_name} image pushed: ${full_image}"
}

# ---------------------------------------------------------------------------
# Helper: upsert a Cloud Run Job (create or update)
# ---------------------------------------------------------------------------
deploy_cloud_run_job() {
    local job_name="$1"
    local image_name="$2"
    local full_image="${REGISTRY}/${image_name}:${IMAGE_TAG}"

    # Build --set-secrets args — all jobs receive the same secret bindings.
    # Each binding maps an env var to a Secret Manager secret (latest version).
    local secret_args=(
        "ANTHROPIC_API_KEY=ANTHROPIC_API_KEY:latest"
        "OPENAI_API_KEY=OPENAI_API_KEY:latest"
        "GOOGLE_API_KEY=GOOGLE_API_KEY:latest"
        "GROQ_API_KEY=GROQ_API_KEY:latest"
        "RESEND_API_KEY=RESEND_API_KEY:latest"
        "AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID:latest"
        "AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY:latest"
    )
    local secrets_flag
    secrets_flag=$(IFS=,; echo "${secret_args[*]}")

    # Pass non-secret config as plain env vars
    local env_vars="GCP_PROJECT_ID=${PROJECT_ID},GCS_CONFIG_BUCKET=${GCS_CONFIG_BUCKET},EMAIL_FROM_ADDRESS=${EMAIL_FROM_ADDRESS:-},EMAIL_TO_ADDRESS=${EMAIL_TO_ADDRESS:-}"

    if gcloud run jobs describe "${job_name}" \
        --region="${REGION}" \
        --project="${PROJECT_ID}" &>/dev/null; then
        log_step "Updating Cloud Run Job '${job_name}'..."
        gcloud run jobs update "${job_name}" \
            --image="${full_image}" \
            --region="${REGION}" \
            --service-account="${CLOUDRUN_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
            --cpu="${CLOUDRUN_CPU}" \
            --memory="${CLOUDRUN_MEMORY}" \
            --task-timeout="${CLOUDRUN_TIMEOUT}s" \
            --max-retries="${CLOUDRUN_MAX_RETRIES}" \
            --set-env-vars="${env_vars}" \
            --set-secrets="${secrets_flag}" \
            --project="${PROJECT_ID}"
    else
        log_step "Creating Cloud Run Job '${job_name}'..."
        gcloud run jobs create "${job_name}" \
            --image="${full_image}" \
            --region="${REGION}" \
            --service-account="${CLOUDRUN_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
            --cpu="${CLOUDRUN_CPU}" \
            --memory="${CLOUDRUN_MEMORY}" \
            --task-timeout="${CLOUDRUN_TIMEOUT}s" \
            --max-retries="${CLOUDRUN_MAX_RETRIES}" \
            --set-env-vars="${env_vars}" \
            --set-secrets="${secrets_flag}" \
            --project="${PROJECT_ID}"
    fi
    log_info "Cloud Run Job '${job_name}' deployed."
}

# ---------------------------------------------------------------------------
# Upload config files to GCS
# Config files contain no secrets — all sensitive values are ${VAR_NAME}
# placeholders resolved at runtime from Secret Manager env vars.
# ---------------------------------------------------------------------------
log_step "Uploading config files to gs://${GCS_CONFIG_BUCKET}/ ..."
gsutil cp config/config.yaml config/tickers.yaml "gs://${GCS_CONFIG_BUCKET}/"
log_info "Config files uploaded to gs://${GCS_CONFIG_BUCKET}/"

# ---------------------------------------------------------------------------
# Configure Docker auth for Artifact Registry
# ---------------------------------------------------------------------------
log_step "Configuring Docker auth for Artifact Registry..."
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

# ---------------------------------------------------------------------------
# financial_update_job
# ---------------------------------------------------------------------------
if [[ "${SKIP_FINANCIAL_UPDATE}" == "0" ]]; then
    build_and_push "financial-update-job" "docker/Dockerfile.financial_update"
    deploy_cloud_run_job "financial-update-job" "financial-update-job"
else
    log_warn "Skipping financial_update_job (SKIP_FINANCIAL_UPDATE=1)"
fi

# ---------------------------------------------------------------------------
# edgar_disclosure_job
# ---------------------------------------------------------------------------
if [[ "${SKIP_EDGAR}" == "0" ]]; then
    build_and_push "edgar-disclosure-job" "docker/Dockerfile.edgar_disclosure"
    deploy_cloud_run_job "edgar-disclosure-job" "edgar-disclosure-job"
else
    log_warn "Skipping edgar_disclosure_job (SKIP_EDGAR=1)"
fi

# ---------------------------------------------------------------------------
# screener_job
# ---------------------------------------------------------------------------
if [[ "${SKIP_SCREENER}" == "0" ]]; then
    build_and_push "screener-job" "docker/Dockerfile.screener"
    deploy_cloud_run_job "screener-job" "screener-job"
else
    log_warn "Skipping screener_job (SKIP_SCREENER=1)"
fi

# ---------------------------------------------------------------------------
# eval GCF — Cloud Function (source deploy, no Docker image needed)
# Cloud Functions gen2 is deployed directly from source via gcloud — no
# Dockerfile or Artifact Registry image required for this component.
# ---------------------------------------------------------------------------
if [[ "${SKIP_EVAL_GCF}" == "0" ]]; then
    log_step "Deploying eval Cloud Function from repo root..."
    gcloud functions deploy "eval-handler" \
        --gen2 \
        --region="${REGION}" \
        --runtime=python311 \
        --source=. \
        --entry-point=eval_handler \
        --trigger-http \
        --no-allow-unauthenticated \
        --service-account="${GCF_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
        --memory=1Gi \
        --timeout=540 \
        --set-env-vars="GCP_PROJECT_ID=${PROJECT_ID},GCS_CONFIG_BUCKET=${GCS_CONFIG_BUCKET}" \
        --set-secrets="ANTHROPIC_API_KEY=ANTHROPIC_API_KEY:latest,OPENAI_API_KEY=OPENAI_API_KEY:latest,GOOGLE_API_KEY=GOOGLE_API_KEY:latest,GROQ_API_KEY=GROQ_API_KEY:latest,RESEND_API_KEY=RESEND_API_KEY:latest" \
        --project="${PROJECT_ID}"

    log_info "eval Cloud Function deployed."
else
    log_warn "Skipping eval Cloud Function (SKIP_EVAL_GCF=1)"
fi

# ---------------------------------------------------------------------------
# Re-deploy Cloud Workflow (picks up any YAML changes)
# ---------------------------------------------------------------------------
log_step "Re-deploying Cloud Workflow 'stock-screener-monthly-pipeline'..."
WORKFLOW_SA="workflow-runner"
gcloud workflows deploy "stock-screener-monthly-pipeline" \
    --source="deploy/workflows/stock-screener-monthly-pipeline.yaml" \
    --location="${REGION}" \
    --service-account="${WORKFLOW_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --project="${PROJECT_ID}"
log_info "Cloud Workflow re-deployed."

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
log_info ""
log_info "=== Deploy complete ==="
log_info ""
log_info "To trigger a manual pipeline run:"
log_info "  gcloud workflows run stock-screener-monthly-pipeline \\"
log_info "    --location=${REGION} \\"
log_info "    --data='{\"project_id\": \"${PROJECT_ID}\", \"region\": \"${REGION}\"}' \\"
log_info "    --project=${PROJECT_ID}"
log_info ""
log_info "To run a single Cloud Run Job manually:"
log_info "  gcloud run jobs execute financial-update-job --region=${REGION} --project=${PROJECT_ID}"
log_info "  gcloud run jobs execute edgar-disclosure-job  --region=${REGION} --project=${PROJECT_ID}"
log_info "  gcloud run jobs execute screener-job          --region=${REGION} --project=${PROJECT_ID}"
