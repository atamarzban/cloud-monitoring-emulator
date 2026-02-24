package server

import (
	"context"
	"encoding/base64"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	monitoringpb "github.com/ata-marzban/cloud-monitoring-emulator/gen/google/monitoring/v3"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/store"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/validation"
)

// AlertPolicyServiceServer implements the gRPC AlertPolicyService.
type AlertPolicyServiceServer struct {
	monitoringpb.UnimplementedAlertPolicyServiceServer
	store store.Store
}

func NewAlertPolicyServiceServer(s store.Store) *AlertPolicyServiceServer {
	return &AlertPolicyServiceServer{store: s}
}

func (s *AlertPolicyServiceServer) CreateAlertPolicy(ctx context.Context, req *monitoringpb.CreateAlertPolicyRequest) (*monitoringpb.AlertPolicy, error) {
	project, err := validation.ParseProjectFromName(req.GetName())
	if err != nil {
		return nil, err
	}

	policy := req.GetAlertPolicy()
	if policy == nil {
		return nil, status.Error(codes.InvalidArgument, "alert_policy is required")
	}
	if policy.GetDisplayName() == "" {
		return nil, status.Error(codes.InvalidArgument, "display_name is required")
	}

	return s.store.CreateAlertPolicy(ctx, project, policy)
}

func (s *AlertPolicyServiceServer) GetAlertPolicy(ctx context.Context, req *monitoringpb.GetAlertPolicyRequest) (*monitoringpb.AlertPolicy, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	return s.store.GetAlertPolicy(ctx, req.GetName())
}

func (s *AlertPolicyServiceServer) ListAlertPolicies(ctx context.Context, req *monitoringpb.ListAlertPoliciesRequest) (*monitoringpb.ListAlertPoliciesResponse, error) {
	project, err := validation.ParseProjectFromName(req.GetName())
	if err != nil {
		return nil, err
	}

	all, err := s.store.ListAlertPolicies(ctx, project, req.GetFilter())
	if err != nil {
		return nil, err
	}

	pageSize := int(req.GetPageSize())
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}

	offset := 0
	if req.GetPageToken() != "" {
		decoded, err := base64.StdEncoding.DecodeString(req.GetPageToken())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token")
		}
		offset, err = strconv.Atoi(string(decoded))
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token")
		}
	}

	end := offset + pageSize
	if end > len(all) {
		end = len(all)
	}

	var nextPageToken string
	if end < len(all) {
		nextPageToken = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	}

	page := all[offset:end]

	return &monitoringpb.ListAlertPoliciesResponse{
		AlertPolicies: page,
		NextPageToken: nextPageToken,
		TotalSize:     int32(len(all)),
	}, nil
}

func (s *AlertPolicyServiceServer) UpdateAlertPolicy(ctx context.Context, req *monitoringpb.UpdateAlertPolicyRequest) (*monitoringpb.AlertPolicy, error) {
	policy := req.GetAlertPolicy()
	if policy == nil {
		return nil, status.Error(codes.InvalidArgument, "alert_policy is required")
	}
	if policy.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "alert_policy.name is required")
	}

	return s.store.UpdateAlertPolicy(ctx, policy, req.GetUpdateMask())
}

func (s *AlertPolicyServiceServer) DeleteAlertPolicy(ctx context.Context, req *monitoringpb.DeleteAlertPolicyRequest) (*emptypb.Empty, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if err := s.store.DeleteAlertPolicy(ctx, req.GetName()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}
