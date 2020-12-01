package common

type IService interface {
	Start()
	Stop() error
}
