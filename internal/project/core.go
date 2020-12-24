package project

import "context"

// ICore is an interface over project core
//go:generate go run -mod=mod github.com/golang/mock/mockgen -build_flags=-mod=mod -destination=mocks/core/mock_core.go -package=mocks . ICore
type ICore interface {
	CreateProject(ctx context.Context, m *Model) error
}

// Core implements all business logic for project
type Core struct {
	repo IRepo
}

// NewCore returns an instance of Core
func NewCore(repo IRepo) *Core {
	return &Core{repo}
}

// CreateProject creates a new project
func (c *Core) CreateProject(ctx context.Context, m *Model) error {
	return c.repo.Create(ctx, m)
}
