package service

type IService interface {
	Start(chan<- error)
	Stop() error
}
