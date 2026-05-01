#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# deploy/setup_gcp.sh — One-time GCP resource creation
#
# Run this ONCE to provision all infrastructure before the first deployment.
# Subsequent deploys use deploy_all.sh.
#
# Prerequisites:
#   - gcloud CLI authenticated: gcloud auth login
#   - Sufficient IAM permissions (Project Owner or equivalent)
#   - Billing enabled on the project
#
# Usage:
#   export GCP_PROJECT_ID=my-gcp-project
#   export GCP_REGION=us-central1          # optional, defaults to us-central1
#   export NOTIFICATION_EMAIL=you@example.com
#   bash deploy/setup_gcp.sh
# ---------------------------------------------------------------------------

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration — override via environment variables
# ---------------------------------------------------------------------------
PROJECT_ID="${GCP_PROJECT_ID:?ERROR: GCP_PROJECT_ID is required}"
REGION="${GCP_REGION:-us-west1}"
ARTIFACT_REGISTRY_REPO="stock-screener"
WORKFLOW_NAME="stock-screener-monthly-pipeline"
SCHEDULER_JOB_NAME="stock-screener-monthly-trigger"
# 1st Friday of month at 3 PM Eastern
SCHEDULER_CRON="0 15 1-7 * 5"
SCHEDULER_TZ="America/New_York"
WORKFLOW_SA="workflow-runner"
CLOUDRUN_SA="cloudrun-jobs"
GCF_SA="gcf-eval"

# Colours for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

log_info "=== GCP one-time setup for project: ${PROJECT_ID} ==="

# ---------------------------------------------------------------------------
# 1. Set active project
# ---------------------------------------------------------------------------
log_info "Setting active GCP project..."
gcloud config set project "${PROJECT_ID}"

# ---------------------------------------------------------------------------
# 2. Enable required APIs
# ---------------------------------------------------------------------------
log_info "Enabling required GCP APIs (this may take ~2 min)..."
gcloud services enable \
    run.googleapis.com \
    cloudfunctions.googleapis.com \
    workflows.googleapis.com \
    cloudscheduler.googleapis.com \
    secretmanager.googleapis.com \
    artifactregistry.googleapis.com \
    firestore.googleapis.com \
    cloudbuild.googleapis.com \
    iam.googleapis.com \
    --project="${PROJECT_ID}"

log_info "APIs enabled."

# ---------------------------------------------------------------------------
# 3. Artifact Registry — Docker repository
# ---------------------------------------------------------------------------
log_info "Creating Artifact Registry repository '${ARTIFACT_REGISTRY_REPO}'..."
if gcloud artifacts repositories describe "${ARTIFACT_REGISTRY_REPO}" \
    --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
    log_warn "Artifact Registry repo '${ARTIFACT_REGISTRY_REPO}' already exists — skipping."
else
    gcloud artifacts repositories create "${ARTIFACT_REGISTRY_REPO}" \
        --repository-format=docker \
        --location="${REGION}" \
        --description="Multi-Agent Stock Screener Docker images" \
        --project="${PROJECT_ID}"
    log_info "Artifact Registry repo created."
fi

# ---------------------------------------------------------------------------
# 4. Service Accounts
# ---------------------------------------------------------------------------
log_info "Creating service accounts..."

create_sa() {
    local sa_name="$1"
    local display="$2"
    if gcloud iam service-accounts describe "${sa_name}@${PROJECT_ID}.iam.gserviceaccount.com" \
        --project="${PROJECT_ID}" &>/dev/null; then
        log_warn "Service account '${sa_name}' already exists — skipping."
    else
        gcloud iam service-accounts create "${sa_name}" \
            --display-name="${display}" \
            --project="${PROJECT_ID}"
        log_info "Created service account: ${sa_name}"
    fi
}

create_sa "${CLOUDRUN_SA}"  "Cloud Run Jobs SA — stock screener"
create_sa "${WORKFLOW_SA}"  "Cloud Workflows runner — stock screener"
create_sa "${GCF_SA}"       "Cloud Function eval SA — stock screener"

# ---------------------------------------------------------------------------
# 5. IAM bindings
# ---------------------------------------------------------------------------
log_info "Binding IAM roles..."

bind_role() {
    local sa="$1"
    local role="$2"
    gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
        --member="serviceAccount:${sa}@${PROJECT_ID}.iam.gserviceaccount.com" \
        --role="${role}" \
        --condition=None \
        --quiet
}

# Cloud Run Jobs SA needs Firestore, Secret Manager, and Artifact Registry read
bind_role "${CLOUDRUN_SA}" "roles/datastore.user"
bind_role "${CLOUDRUN_SA}" "roles/secretmanager.secretAccessor"
bind_role "${CLOUDRUN_SA}" "roles/artifactregistry.reader"
bind_role "${CLOUDRUN_SA}" "roles/logging.logWriter"

# Eval GCF SA — same Firestore + Secret Manager access
bind_role "${GCF_SA}" "roles/datastore.user"
bind_role "${GCF_SA}" "roles/secretmanager.secretAccessor"
bind_role "${GCF_SA}" "roles/logging.logWriter"

# Cloud Workflows SA needs to invoke Cloud Run Jobs and call the eval GCF
bind_role "${WORKFLOW_SA}" "roles/run.invoker"
bind_role "${WORKFLOW_SA}" "roles/run.jobs.jobsRunner"
bind_role "${WORKFLOW_SA}" "roles/cloudfunctions.invoker"
bind_role "${WORKFLOW_SA}" "roles/workflows.invoker"
bind_role "${WORKFLOW_SA}" "roles/logging.logWriter"

# Cloud Scheduler calls Cloud Workflows — needs workflows.invoker
SCHEDULER_SA="$(gcloud iam service-accounts list \
    --filter="displayName:Cloud Scheduler" \
    --format="value(email)" \
    --project="${PROJECT_ID}" 2>/dev/null | head -1 || true)"

if [[ -z "${SCHEDULER_SA}" ]]; then
    log_warn "No dedicated Cloud Scheduler SA found; using Workflows SA for scheduler invocation."
    SCHEDULER_SA="${WORKFLOW_SA}@${PROJECT_ID}.iam.gserviceaccount.com"
fi

log_info "IAM bindings applied."

# ---------------------------------------------------------------------------
# 6. Firestore database
# ---------------------------------------------------------------------------
log_info "Ensuring Firestore database 'multi-agent-stock-screener' exists..."
if gcloud firestore databases describe \
    --database="multi-agent-stock-screener" \
    --project="${PROJECT_ID}" &>/dev/null; then
    log_warn "Firestore database already exists — skipping."
else
    gcloud firestore databases create \
        --database="multi-agent-stock-screener" \
        --location="${REGION}" \
        --type=firestore-native \
        --project="${PROJECT_ID}"
    log_info "Firestore database created."
fi

# ---------------------------------------------------------------------------
# 7. Secret Manager — create secret stubs
# ---------------------------------------------------------------------------
log_info "Creating Secret Manager secret stubs (values must be set separately)..."

create_secret() {
    local secret_name="$1"
    if gcloud secrets describe "${secret_name}" --project="${PROJECT_ID}" &>/dev/null; then
        log_warn "Secret '${secret_name}' already exists — skipping."
    else
        gcloud secrets create "${secret_name}" \
            --replication-policy=automatic \
            --project="${PROJECT_ID}"
        # Add a placeholder version so the secret is not empty
        echo "PLACEHOLDER" | gcloud secrets versions add "${secret_name}" \
            --data-file=- \
            --project="${PROJECT_ID}"
        log_info "Created secret stub: ${secret_name}"
    fi
}

# LLM provider keys
create_secret "ANTHROPIC_API_KEY"
create_secret "OPENAI_API_KEY"
create_secret "GOOGLE_API_KEY"
create_secret "GROQ_API_KEY"

# Email
create_secret "RESEND_API_KEY"

# AWS (only if using S3 backend)
create_secret "AWS_ACCESS_KEY_ID"
create_secret "AWS_SECRET_ACCESS_KEY"

log_warn "Secret stubs created with PLACEHOLDER values."
log_warn "Update each secret via:"
log_warn "  echo -n 'YOUR_KEY' | gcloud secrets versions add SECRET_NAME --data-file=- --project=${PROJECT_ID}"

# ---------------------------------------------------------------------------
# 8. Cloud Workflows — deploy workflow definition
# ---------------------------------------------------------------------------
log_info "Deploying Cloud Workflow '${WORKFLOW_NAME}'..."
gcloud workflows deploy "${WORKFLOW_NAME}" \
    --source="deploy/workflows/stock-screener-monthly-pipeline.yaml" \
    --location="${REGION}" \
    --service-account="${WORKFLOW_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --project="${PROJECT_ID}"
log_info "Cloud Workflow deployed."

# ---------------------------------------------------------------------------
# 9. Cloud Scheduler — monthly trigger
# ---------------------------------------------------------------------------
log_info "Creating Cloud Scheduler job '${SCHEDULER_JOB_NAME}'..."

WORKFLOW_RESOURCE="projects/${PROJECT_ID}/locations/${REGION}/workflows/${WORKFLOW_NAME}"

if gcloud scheduler jobs describe "${SCHEDULER_JOB_NAME}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" &>/dev/null; then
    log_warn "Scheduler job '${SCHEDULER_JOB_NAME}' already exists — updating..."
    gcloud scheduler jobs update http "${SCHEDULER_JOB_NAME}" \
        --location="${REGION}" \
        --schedule="${SCHEDULER_CRON}" \
        --time-zone="${SCHEDULER_TZ}" \
        --uri="https://workflowexecutions.googleapis.com/v1/${WORKFLOW_RESOURCE}/executions" \
        --message-body="{\"argument\": \"{\\\"project_id\\\": \\\"${PROJECT_ID}\\\", \\\"region\\\": \\\"${REGION}\\\"}\"}" \
        --oauth-service-account-email="${WORKFLOW_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
        --project="${PROJECT_ID}"
else
    gcloud scheduler jobs create http "${SCHEDULER_JOB_NAME}" \
        --location="${REGION}" \
        --schedule="${SCHEDULER_CRON}" \
        --time-zone="${SCHEDULER_TZ}" \
        --uri="https://workflowexecutions.googleapis.com/v1/${WORKFLOW_RESOURCE}/executions" \
        --message-body="{\"argument\": \"{\\\"project_id\\\": \\\"${PROJECT_ID}\\\", \\\"region\\\": \\\"${REGION}\\\"}\"}" \
        --oauth-service-account-email="${WORKFLOW_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
        --project="${PROJECT_ID}"
fi
log_info "Cloud Scheduler job created/updated."

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
log_info "=== One-time setup complete ==="
log_info ""
log_info "Next steps:"
log_info "  1. Fill in secret values:"
log_info "       echo -n 'sk-ant-...' | gcloud secrets versions add ANTHROPIC_API_KEY --data-file=- --project=${PROJECT_ID}"
log_info "       echo -n 'AIza...'   | gcloud secrets versions add GOOGLE_API_KEY     --data-file=- --project=${PROJECT_ID}"
log_info "       echo -n 're_...'    | gcloud secrets versions add RESEND_API_KEY      --data-file=- --project=${PROJECT_ID}"
log_info "  2. Run deploy_all.sh to build images and create Cloud Run Jobs:"
log_info "       bash deploy/deploy_all.sh"
log_info "  3. Trigger a manual test run:"
log_info "       gcloud workflows run ${WORKFLOW_NAME} --location=${REGION} --project=${PROJECT_ID}"
