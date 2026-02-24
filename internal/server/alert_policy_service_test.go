package server

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	monitoringpb "github.com/ata-marzban/cloud-monitoring-emulator/gen/google/monitoring/v3"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/store"
)

func newAlertSvc() *AlertPolicyServiceServer {
	return NewAlertPolicyServiceServer(store.NewMemoryStore())
}

func TestCreateAlertPolicy(t *testing.T) {
	svc := newAlertSvc()
	ctx := context.Background()

	resp, err := svc.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{
		Name: "projects/test-proj",
		AlertPolicy: &monitoringpb.AlertPolicy{
			DisplayName: "High CPU",
			Combiner:    monitoringpb.AlertPolicy_AND,
			Conditions: []*monitoringpb.AlertPolicy_Condition{
				{DisplayName: "CPU > 90%"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if resp.GetName() == "" {
		t.Error("expected auto-generated name")
	}
	if resp.GetDisplayName() != "High CPU" {
		t.Errorf("display_name = %q", resp.GetDisplayName())
	}
	if resp.GetCreationRecord() == nil {
		t.Error("expected creation_record")
	}
	if resp.GetMutationRecord() == nil {
		t.Error("expected mutation_record")
	}
	// Conditions should have auto-generated names.
	if len(resp.GetConditions()) != 1 {
		t.Fatalf("got %d conditions", len(resp.GetConditions()))
	}
	if resp.GetConditions()[0].GetName() == "" {
		t.Error("expected auto-generated condition name")
	}
}

func TestCreateAlertPolicyMissingDisplayName(t *testing.T) {
	svc := newAlertSvc()
	ctx := context.Background()

	_, err := svc.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{
		Name:        "projects/test-proj",
		AlertPolicy: &monitoringpb.AlertPolicy{},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestCreateAlertPolicyMissingPolicy(t *testing.T) {
	svc := newAlertSvc()
	ctx := context.Background()

	_, err := svc.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{
		Name: "projects/test-proj",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestGetAlertPolicy(t *testing.T) {
	svc := newAlertSvc()
	ctx := context.Background()

	created, err := svc.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{
		Name: "projects/test-proj",
		AlertPolicy: &monitoringpb.AlertPolicy{
			DisplayName: "Test Policy",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := svc.GetAlertPolicy(ctx, &monitoringpb.GetAlertPolicyRequest{
		Name: created.GetName(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetDisplayName() != "Test Policy" {
		t.Errorf("display_name = %q", got.GetDisplayName())
	}
}

func TestGetAlertPolicyNotFound(t *testing.T) {
	svc := newAlertSvc()
	ctx := context.Background()

	_, err := svc.GetAlertPolicy(ctx, &monitoringpb.GetAlertPolicyRequest{
		Name: "projects/test-proj/alertPolicies/nonexistent",
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound, got %v", err)
	}
}

func TestListAlertPolicies(t *testing.T) {
	svc := newAlertSvc()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_, err := svc.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{
			Name: "projects/test-proj",
			AlertPolicy: &monitoringpb.AlertPolicy{
				DisplayName: "Policy",
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Also create in another project.
	_, err := svc.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{
		Name: "projects/other-proj",
		AlertPolicy: &monitoringpb.AlertPolicy{
			DisplayName: "Other",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := svc.ListAlertPolicies(ctx, &monitoringpb.ListAlertPoliciesRequest{
		Name: "projects/test-proj",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetAlertPolicies()) != 5 {
		t.Errorf("got %d policies, want 5", len(resp.GetAlertPolicies()))
	}
	if resp.GetTotalSize() != 5 {
		t.Errorf("total_size = %d, want 5", resp.GetTotalSize())
	}
}

func TestListAlertPoliciesPagination(t *testing.T) {
	svc := newAlertSvc()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_, err := svc.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{
			Name: "projects/test-proj",
			AlertPolicy: &monitoringpb.AlertPolicy{
				DisplayName: "Policy",
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// First page.
	resp, err := svc.ListAlertPolicies(ctx, &monitoringpb.ListAlertPoliciesRequest{
		Name:     "projects/test-proj",
		PageSize: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetAlertPolicies()) != 2 {
		t.Errorf("page 1: got %d policies, want 2", len(resp.GetAlertPolicies()))
	}
	if resp.GetNextPageToken() == "" {
		t.Error("expected next_page_token on first page")
	}

	// Second page.
	resp2, err := svc.ListAlertPolicies(ctx, &monitoringpb.ListAlertPoliciesRequest{
		Name:      "projects/test-proj",
		PageSize:  2,
		PageToken: resp.GetNextPageToken(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp2.GetAlertPolicies()) != 2 {
		t.Errorf("page 2: got %d policies, want 2", len(resp2.GetAlertPolicies()))
	}

	// Last page.
	resp3, err := svc.ListAlertPolicies(ctx, &monitoringpb.ListAlertPoliciesRequest{
		Name:      "projects/test-proj",
		PageSize:  2,
		PageToken: resp2.GetNextPageToken(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp3.GetAlertPolicies()) != 1 {
		t.Errorf("page 3: got %d policies, want 1", len(resp3.GetAlertPolicies()))
	}
	if resp3.GetNextPageToken() != "" {
		t.Error("expected empty next_page_token on last page")
	}
}

func TestUpdateAlertPolicyFull(t *testing.T) {
	svc := newAlertSvc()
	ctx := context.Background()

	created, err := svc.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{
		Name: "projects/test-proj",
		AlertPolicy: &monitoringpb.AlertPolicy{
			DisplayName: "Original",
			Enabled:     wrapperspb.Bool(true),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Full update (no field mask).
	updated, err := svc.UpdateAlertPolicy(ctx, &monitoringpb.UpdateAlertPolicyRequest{
		AlertPolicy: &monitoringpb.AlertPolicy{
			Name:        created.GetName(),
			DisplayName: "Updated",
			Enabled:     wrapperspb.Bool(false),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.GetDisplayName() != "Updated" {
		t.Errorf("display_name = %q, want Updated", updated.GetDisplayName())
	}
	if updated.GetEnabled().GetValue() != false {
		t.Error("expected enabled=false")
	}
	// Creation record should be preserved.
	if updated.GetCreationRecord() == nil {
		t.Error("creation_record should be preserved")
	}
	// Mutation record should be updated.
	if updated.GetMutationRecord().GetMutateTime().AsTime().Before(created.GetMutationRecord().GetMutateTime().AsTime()) {
		t.Error("mutation_record should be newer")
	}
}

func TestUpdateAlertPolicyWithMask(t *testing.T) {
	svc := newAlertSvc()
	ctx := context.Background()

	created, err := svc.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{
		Name: "projects/test-proj",
		AlertPolicy: &monitoringpb.AlertPolicy{
			DisplayName: "Original",
			Enabled:     wrapperspb.Bool(true),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Partial update — only update display_name.
	updated, err := svc.UpdateAlertPolicy(ctx, &monitoringpb.UpdateAlertPolicyRequest{
		AlertPolicy: &monitoringpb.AlertPolicy{
			Name:        created.GetName(),
			DisplayName: "Partial Update",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.GetDisplayName() != "Partial Update" {
		t.Errorf("display_name = %q", updated.GetDisplayName())
	}
	// Enabled should be preserved from original.
	if updated.GetEnabled().GetValue() != true {
		t.Error("enabled should be preserved from original")
	}
}

func TestUpdateAlertPolicyNotFound(t *testing.T) {
	svc := newAlertSvc()
	ctx := context.Background()

	_, err := svc.UpdateAlertPolicy(ctx, &monitoringpb.UpdateAlertPolicyRequest{
		AlertPolicy: &monitoringpb.AlertPolicy{
			Name:        "projects/test-proj/alertPolicies/nonexistent",
			DisplayName: "X",
		},
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound, got %v", err)
	}
}

func TestDeleteAlertPolicy(t *testing.T) {
	svc := newAlertSvc()
	ctx := context.Background()

	created, err := svc.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{
		Name: "projects/test-proj",
		AlertPolicy: &monitoringpb.AlertPolicy{
			DisplayName: "To Delete",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = svc.DeleteAlertPolicy(ctx, &monitoringpb.DeleteAlertPolicyRequest{
		Name: created.GetName(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Get should return NotFound.
	_, err = svc.GetAlertPolicy(ctx, &monitoringpb.GetAlertPolicyRequest{
		Name: created.GetName(),
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound after delete, got %v", err)
	}
}

func TestDeleteAlertPolicyNotFound(t *testing.T) {
	svc := newAlertSvc()
	ctx := context.Background()

	_, err := svc.DeleteAlertPolicy(ctx, &monitoringpb.DeleteAlertPolicyRequest{
		Name: "projects/test-proj/alertPolicies/nonexistent",
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound, got %v", err)
	}
}
