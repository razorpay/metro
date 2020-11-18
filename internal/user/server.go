package user

//
//import (
//	"context"
//
//	ot "github.com/opentracing/opentracing-go"
//
//	"github.com/razorpay/metro/internal/boot"
//	userv1 "github.com/razorpay/metro/rpc/example/user/v1"
//)
//
//// Server has methods implementing of server rpc.
//type Server struct {
//	core ICore
//}
//
//// NewServer returns a server.
//func NewServer(core ICore) *Server {
//	return &Server{
//		core: core,
//	}
//}
//
//// Create creates a new user
//func (s *Server) Create(ctx context.Context, req *userv1.UserCreateRequest) (*userv1.UserResponse, error) {
//	span, ctx := ot.StartSpanFromContext(ctx, "user.server.Create")
//	defer span.Finish()
//
//	boot.Logger(ctx).Infow("user create request", map[string]interface{}{
//		"first_name": req.GetFirstName(),
//		"last_name":  req.GetLastName(),
//	})
//
//	createParams := CreateParams{
//		FirstName: req.GetFirstName(),
//		LastName:  req.GetLastName(),
//	}
//
//	user, err := s.core.Create(ctx, &createParams)
//	if err != nil {
//		return nil, err
//	}
//
//	return toUserResponseProto(user), nil
//}
//
//// Get retrieves a single user record
//func (s *Server) Get(ctx context.Context, request *userv1.UserGetRequest) (*userv1.UserResponse, error) {
//	user, err := s.core.Find(ctx, request.GetId())
//	if err != nil {
//		return nil, err
//	}
//
//	return toUserResponseProto(user), nil
//}
//
//// List fetches a list of filtered user records
//func (s *Server) List(ctx context.Context, request *userv1.UserListRequest) (*userv1.UserListResponse, error) {
//	users, err := s.core.FindMany(ctx, request)
//	if err != nil {
//		return nil, err
//	}
//
//	usersProto := make([]*userv1.UserResponse, 0, len(*users))
//	for _, userModel := range *users {
//		user := toUserResponseProto(&userModel)
//		usersProto = append(usersProto, user)
//	}
//
//	response := userv1.UserListResponse{
//		Count:  int32(len(usersProto)),
//		Entity: "users",
//		Items:  usersProto,
//	}
//
//	return &response, nil
//}
//
//// Approve marks a users status to approved
//func (s *Server) Approve(ctx context.Context, request *userv1.UserApproveRequest) (*userv1.UserResponse, error) {
//	user, err := s.core.Approve(ctx, request.GetId())
//	if err != nil {
//		return nil, err
//	}
//
//	return toUserResponseProto(user), nil
//}
//
//// Delete deletes a user, soft-delete
//func (s *Server) Delete(ctx context.Context, request *userv1.UserDeleteRequest) (*userv1.UserDeleteResponse, error) {
//	err := s.core.Delete(ctx, request.GetId())
//	if err != nil {
//		return nil, err
//	}
//
//	return &userv1.UserDeleteResponse{}, nil
//}
//
//func toUserResponseProto(user *User) *userv1.UserResponse {
//	response := userv1.UserResponse{
//		Id:         user.ID,
//		FirstName:  user.FirstName,
//		LastName:   user.LastName,
//		Status:     user.Status,
//		ApprovedAt: user.ApprovedAt,
//		CreatedAt:  int32(user.CreatedAt),
//	}
//
//	return &response
//}
