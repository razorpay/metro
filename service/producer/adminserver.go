package producer

import (
	"context"

	"github.com/razorpay/metro/internal/project"
	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

type adminServer struct {
	projectCore *project.Core
}

func newAdminServer(projectCore *project.Core) *adminServer {
	return &adminServer{projectCore}
}

func (s adminServer) CreateProject(ctx context.Context, req *metrov1.Project) (*metrov1.Project, error) {
	logger.Ctx(ctx).Infow("create project request received", "req", req)
	p := project.FromProto(req)
	err := s.projectCore.CreateProject(p)
	if err != nil {
		return nil, err
	}
	return req, nil
}
