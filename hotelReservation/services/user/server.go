package user

import (
	"crypto/sha256"
	"fmt"
	"net"
	"time"

	"hotelReservation/dagor"
	"hotelReservation/registry"
	pb "hotelReservation/services/user/proto"
	"hotelReservation/tls"

	"github.com/google/uuid"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const name = "srv-user"

// Server implements the user service
type Server struct {
	pb.UnimplementedUserServer

	users map[string]string
	uuid  string

	Tracer      opentracing.Tracer
	Registry    *registry.Client
	Port        int
	IpAddr      string
	MongoClient *mongo.Client
}

// Run starts the server
func (s *Server) Run() error {
	if s.Port == 0 {
		return fmt.Errorf("server port must be set")
	}

	if s.users == nil {
		s.users = loadUsers(s.MongoClient)
	}

	s.uuid = uuid.New().String()
	param := dagor.DagorParam{
		NodeName:                     "search",
		BusinessMap:                  map[string]int{"hotel": 1},
		QueuingThresh:                5 * time.Millisecond,
		EntryService:                 false,
		IsEnduser:                    false,
		AdmissionLevelUpdateInterval: 1 * time.Second,
		Alpha:                        0.7,
		Beta:                         0.3,
		Umax:                         1000,
		Bmax:                         500,
		Debug:                        true,
		UseSyncMap:                   false,
	}

	d := dagor.NewDagorNode(param)
	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Timeout: 120 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			PermitWithoutStream: true,
		}),
		grpc.UnaryInterceptor(d.UnaryInterceptorServer),
	}

	if tlsopt := tls.GetServerOpt(); tlsopt != nil {
		opts = append(opts, tlsopt)
	}

	srv := grpc.NewServer(opts...)

	pb.RegisterUserServer(srv, s)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}
	log.Info().Msgf("In reservation s.IpAddr = %s, port = %d", s.IpAddr, s.Port)
	// err = s.Registry.Register(name, s.uuid, s.IpAddr, s.Port)
	// if err != nil {
	// 	return fmt.Errorf("failed register: %v", err)
	// }
	// log.Info().Msg("Successfully registered in consul")

	return srv.Serve(lis)
}

// Shutdown cleans up any processes
func (s *Server) Shutdown() {
	s.Registry.Deregister(s.uuid)
}

// CheckUser returns whether the username and password are correct.
func (s *Server) CheckUser(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	res := new(pb.Result)

	log.Trace().Msg("CheckUser")

	sum := sha256.Sum256([]byte(req.Password))
	pass := fmt.Sprintf("%x", sum)

	res.Correct = false
	if true_pass, found := s.users[req.Username]; found {
		res.Correct = pass == true_pass
	}

	log.Trace().Msgf("CheckUser %d", res.Correct)

	return res, nil
}

// loadUsers loads hotel users from mongodb.
func loadUsers(client *mongo.Client) map[string]string {
	collection := client.Database("user-db").Collection("user")
	curr, err := collection.Find(context.TODO(), bson.D{})
	if err != nil {
		log.Error().Msgf("Failed get users data: ", err)
	}

	var users []User
	curr.All(context.TODO(), &users)
	if err != nil {
		log.Error().Msgf("Failed get users data: ", err)
	}

	res := make(map[string]string)
	for _, user := range users {
		res[user.Username] = user.Password
	}

	log.Trace().Msg("Done load users")

	return res
}

type User struct {
	Username string `bson:"username"`
	Password string `bson:"password"`
}
