from typing import Any, Dict, List, Optional, Literal, Union
from pydantic import BaseModel, Field, validator
from app.core.config import settings

DocumentMode = Literal["auto", "table", "invoice", "receipt", "bank_statement", "notes", "invoice_receipt"]
RoutableDocumentMode = Literal["table", "invoice", "receipt", "bank_statement", "notes"]

# P3 — vendor rule auto-application modes.
# - suggest:    surface the rule as a suggestion; user fills the draft (legacy)
# - auto_fill:  pre-fill the draft from the rule, user must still confirm
# - auto_ready: pre-fill and mark the AP item Ready to publish automatically
VendorRuleAutoMode = Literal["suggest", "auto_fill", "auto_ready"]


class ImageData(BaseModel):
    """Model for individual image data in requests."""
    
    image: str = Field(..., description="Base64 encoded image data")
    filename: Optional[str] = Field(None, description="Original filename")
    
    @validator("image")
    def validate_base64(cls, v):
        """Validate that image is base64 encoded (supports both data URLs and raw base64)."""
        if not v:
            raise ValueError("Image data cannot be empty")

        import base64

        # Handle data URL format (data:image/png;base64,{base64_data})
        if v.startswith('data:'):
            try:
                # Extract base64 part after comma
                base64_part = v.split(',', 1)[1]
                decoded = base64.b64decode(base64_part, validate=True)
                if len(decoded) < 100:  # Minimum reasonable image size
                    raise ValueError("Image data appears to be too small (minimum 100 bytes)")
            except ValueError as e:
                # Re-raise our custom ValueError
                raise e
            except Exception:
                raise ValueError("Invalid data URL format or base64 encoding")
        else:
            # Handle raw base64 data (no data URL prefix)
            try:
                decoded = base64.b64decode(v, validate=True)
                if len(decoded) < 100:  # Minimum reasonable image size
                    raise ValueError("Image data appears to be too small (minimum 100 bytes)")
            except ValueError as e:
                # Re-raise our custom ValueError
                raise e
            except Exception:
                raise ValueError("Invalid base64 encoding")

        return v


class BatchOptions(BaseModel):
    """Options for batch processing."""
    
    output_type: Literal["consolidated", "separate", "concatenated"] = Field(
        "consolidated", 
        description="Output type: consolidated (multiple sheets), separate (individual files), concatenated (single sheet)"
    )
    sheet_naming: Literal["filename", "auto", "custom"] = Field(
        "auto", 
        description="Sheet naming strategy"
    )
    include_source: bool = Field(
        True, 
        description="Include source image reference in output"
    )


class BatchConvertRequest(BaseModel):
    """Request model for batch image conversion."""
    
    images: List[ImageData] = Field(..., description="List of images to process")
    output_format: Literal["xlsx", "csv", "txt"] = Field("xlsx", description="Output format. Table supports XLSX/CSV; Notes supports readable TXT and table XLSX/CSV when a table is detected.")
    consolidation_strategy: Literal["consolidated", "separate", "concatenated"] = Field(
        "consolidated", 
        description="How to consolidate results: consolidated (multiple sheets), separate (individual files), concatenated (single sheet)"
    )
    document_mode: DocumentMode = Field(
        "table",
        description="Document processing mode. invoice_receipt remains supported as a legacy combined mode."
    )
    workspace_id: Optional[str] = Field(None, description="Authenticated user's active workspace for durable document memory")
    batch_options: Optional[BatchOptions] = Field(
        default_factory=BatchOptions, 
        description="Batch processing options"
    )
    
    @validator("images")
    def validate_images_list(cls, v):
        """Validate images list."""
        if not v:
            raise ValueError("Images list cannot be empty")
        
        if len(v) > settings.max_batch_size:
            raise ValueError(f"Too many images in batch (maximum {settings.max_batch_size})")
        
        return v


class DocumentModeOverrideRequest(BaseModel):
    """Route a previously classified document through a chosen extractor."""

    document_mode: RoutableDocumentMode = Field(..., description="Manual extraction mode selected for this document")
    output_format: Literal["xlsx", "csv", "txt"] = Field("xlsx", description="Format for the regenerated output")
    reason: Optional[str] = Field(None, max_length=240, description="Optional user-provided reason for changing the detected type")


ReviewStatus = Literal["needs_review", "ready", "edited", "failed", "published", "deleted"]


class DocumentReviewChangeRequest(BaseModel):
    """Persist a human correction against one durable extraction unit."""

    processing_unit_id: str = Field(..., min_length=1, description="Extraction unit being corrected")
    field_path: List[Union[str, int]] = Field(..., min_length=1, description="Path within the reviewed JSON payload")
    value: Any = Field(..., description="Corrected value")
    base_review_grid: Optional[List[List[Any]]] = Field(
        None,
        description="Visible spreadsheet grid used to initialize durable table correction state",
    )


class DocumentReviewStatusRequest(BaseModel):
    """Set human-review lifecycle status for one document."""

    review_status: ReviewStatus = Field(..., description="Human-review status")
    reason: Optional[str] = Field(None, max_length=240, description="Optional review status note")


class DocumentDuplicateOverrideRequest(BaseModel):
    """Acknowledge a duplicate warning while retaining the document."""

    warning_id: str = Field(..., min_length=1, description="Duplicate warning being acknowledged")
    reason: Optional[str] = Field(None, max_length=240, description="Optional note explaining why this is a separate document")


class VendorRuleFields(BaseModel):
    """User-approved recurring vendor suggestions."""

    category_account: Optional[str] = Field(None, max_length=120)
    vendor_ref_id: Optional[str] = Field(None, max_length=120)
    account_ref_id: Optional[str] = Field(None, max_length=120)
    tax_code: Optional[str] = Field(None, max_length=80)
    tax_code_ref_id: Optional[str] = Field(None, max_length=120)
    currency: Optional[str] = Field(None, max_length=12)
    payment_terms: Optional[str] = Field(None, max_length=80)
    destination_treatment: Optional[str] = Field(None, max_length=120)


class VendorRuleFromDocumentRequest(BaseModel):
    """Save a remembered vendor only from a reviewed accounting document."""

    suggested_fields: VendorRuleFields


class VendorRuleUpdateRequest(BaseModel):
    """Edit or disable an existing vendor memory rule."""

    display_name: Optional[str] = Field(None, min_length=1, max_length=160)
    suggested_fields: Optional[VendorRuleFields] = None
    enabled: Optional[bool] = None
    auto_mode: Optional[VendorRuleAutoMode] = Field(
        None,
        description="How the rule is applied to new documents from this vendor (suggest / auto_fill / auto_ready)",
    )


AccountsPayableStatus = Literal["needs_coding", "needs_review", "ready_to_publish", "published", "failed", "discarded"]


class AccountsPayableFromDocumentRequest(BaseModel):
    """Place one reviewed invoice in the draft-bill preparation queue."""

    job_id: str = Field(..., min_length=1)
    document_id: str = Field(..., min_length=1)


class AccountsPayableDraftFields(BaseModel):
    """Editable reviewed bill values and selected QuickBooks coding references."""

    vendor: Optional[str] = Field(None, max_length=200)
    vendor_ref_id: Optional[str] = Field(None, max_length=120)
    invoice_date: Optional[str] = Field(None, max_length=40)
    due_date: Optional[str] = Field(None, max_length=40)
    account_category: Optional[str] = Field(None, max_length=160)
    account_ref_id: Optional[str] = Field(None, max_length=120)
    tax_code: Optional[str] = Field(None, max_length=80)
    tax_code_ref_id: Optional[str] = Field(None, max_length=120)
    reference: Optional[str] = Field(None, max_length=160)
    currency: Optional[str] = Field(None, max_length=12)
    line_items: Optional[List[Dict[str, Any]]] = None


class AccountsPayableUpdateRequest(BaseModel):
    """Update coding values or an AP queue status."""

    draft_data: Optional[AccountsPayableDraftFields] = None
    attachment_visible: Optional[bool] = None
    status: Optional[AccountsPayableStatus] = None
    reason: Optional[str] = Field(None, max_length=240)
    acknowledge_auto_applied: Optional[bool] = Field(
        None,
        description="Set true when the reviewer overrides a vendor-rule pre-fill; the auto_applied_rule metadata is cleared on the item.",
    )


class PurchaseOrderImportRequest(BaseModel):
    """Import open purchase orders from pasted/uploaded CSV text."""

    workspace_id: Optional[str] = Field(None)
    csv_text: str = Field(..., min_length=1, max_length=2_000_000)


class PurchaseOrderMatchRequest(BaseModel):
    """Link (or clear) a purchase order on an AP item."""

    po_id: Optional[str] = Field(None, description="Purchase order id to match; null to unmatch")


class AccountsPayableDuplicateDismissRequest(BaseModel):
    """Dismiss a single duplicate warning on an AP item with a reviewer reason."""

    warning_id: str = Field(..., min_length=1, description="ID of the duplicate warning to dismiss")
    reason: Optional[str] = Field(None, max_length=240, description="Short justification (e.g. 'legitimate second invoice')")


class AccountsPayableDiscardRequest(BaseModel):
    """Discard a draft AP item after confirming it is a duplicate."""

    reason: Optional[str] = Field(None, max_length=240, description="Short note explaining why the item was discarded")


ConnectedSourceProvider = Literal["google_drive", "dropbox"]


class ConnectedSourceConnectRequest(BaseModel):
    """Start an OAuth handshake for a Google Drive or Dropbox watch folder."""

    workspace_id: str = Field(..., min_length=1)
    provider: ConnectedSourceProvider
    redirect_after: Optional[str] = Field(
        None,
        max_length=400,
        description="Frontend URL to redirect to after the OAuth callback completes",
    )


class ConnectedSourceCallbackRequest(BaseModel):
    """Exchange an OAuth authorisation code for tokens and record the source."""

    code: str = Field(..., min_length=1)
    state: str = Field(..., min_length=1)


class ConnectedSourceFolderUpdate(BaseModel):
    """Set or change the watched folder for a connected source."""

    watched_folder: Optional[str] = Field(None, max_length=400)
    watched_folder_id: Optional[str] = Field(None, max_length=200)
    display_label: Optional[str] = Field(None, max_length=160)


class AccountsPayableBulkStatusRequest(BaseModel):
    """Record published status for selected prepared draft bills."""

    item_ids: List[str] = Field(..., min_length=1)
    status: Literal["published"] = "published"
    reason: Optional[str] = Field(None, max_length=240)


class AccountsPayableBulkPublishRequest(BaseModel):
    """Publish confirmed AP items as unpaid QuickBooks Bills."""

    item_ids: List[str] = Field(..., min_length=1)


class QuickBooksWorkspaceRequest(BaseModel):
    """Target the user's currently selected workspace for an integration action."""

    workspace_id: Optional[str] = Field(None, description="Owned workspace; defaults to the active workspace")


AccountingDestination = Literal["quickbooks", "xero"]


class AccountingDestinationRequest(BaseModel):
    """Set the per-workspace accounting destination for the AP coding form."""

    workspace_id: Optional[str] = Field(None, description="Owned workspace; defaults to the active workspace")
    destination: AccountingDestination


class WorkspaceCreateRequest(BaseModel):
    """Create one owned workspace."""

    name: str = Field(..., min_length=2, max_length=60)


class WorkspaceReviewerRequest(BaseModel):
    """Invite a minimal review-only workspace member."""

    email: str = Field(..., min_length=3, max_length=320)


class ClientUploadLinkRequest(BaseModel):
    """Issue an expiring client submission link for an owned workspace."""

    workspace_id: str = Field(..., min_length=1)
    label: str = Field("Client upload", min_length=1, max_length=80)
    expires_in_hours: int = Field(168, ge=1, le=720)
    max_submissions: int = Field(25, ge=1, le=250)


ReceiptPublishingDestination = Literal["expense", "bill"]


class ReceiptQuickBooksPublishRequest(BaseModel):
    """Explicitly publish a reviewed receipt as either a paid Purchase or unpaid Bill."""

    destination: ReceiptPublishingDestination = Field(
        ...,
        description="expense creates a paid QuickBooks Purchase; bill creates an unpaid QuickBooks Bill",
    )
    vendor_ref_id: Optional[str] = Field(None, max_length=120)
    account_ref_id: str = Field(..., min_length=1, max_length=120, description="Expense/category account")
    tax_code_ref_id: Optional[str] = Field(None, max_length=120)
    payment_account_ref_id: Optional[str] = Field(None, max_length=120, description="Paid-from account for expenses")
    payment_type: Optional[Literal["Cash", "Check", "CreditCard"]] = Field(
        None,
        description="QuickBooks Purchase payment type; required when destination is expense",
    )
