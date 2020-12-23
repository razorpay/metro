package project

// Core implements all business logic for project
type Core struct {
	repo IRepo
}

// NewCore returns an instance of Core
func NewCore(repo IRepo) *Core {
	return &Core{repo}
}

// CreateProject creates a new project
func (c *Core) CreateProject(m *Model) error {
	return c.repo.Create(m)
}
