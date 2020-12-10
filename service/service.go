package service

// IService interface is implemented by various services
type IService interface {
	Start(chan<- error)
	Stop() error
}
