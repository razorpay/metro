package service

// IService interface is implemented by various services
type IService interface {
	Start() error
	Stop() error
}
