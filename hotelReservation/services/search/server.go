package search

import (
	"context"
	"fmt"
	"net"
	"time"

	"hotelReservation/dagor"
	"hotelReservation/registry"
	geo "hotelReservation/services/geo/proto"
	rate "hotelReservation/services/rate/proto"
	pb "hotelReservation/services/search/proto"
	"hotelReservation/tls"

	"github.com/google/uuid"
	_ "github.com/mbobakov/grpc-consul-resolver"

	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const name = "srv-search"

// Server implments the search service
type Server struct {
	pb.UnimplementedSearchServer

	geoClient  geo.GeoClient
	rateClient rate.RateClient
	uuid       string

	Tracer     opentracing.Tracer
	Port       int
	IpAddr     string
	ConsulAddr string
	KnativeDns string
	Registry   *registry.Client
}

// Run starts the server
func (s *Server) Run() error {
	if s.Port == 0 {
		return fmt.Errorf("server port must be set")
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
	pb.RegisterSearchServer(srv, s)

	client_param := dagor.DagorParam{
		NodeName:                     "search",
		BusinessMap:                  map[string]int{"/search.Search/Nearby": 1, "/search.Search/Nearb": 2},
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

	clientDagor := dagor.NewDagorNode(client_param)
	// init grpc clients
	if err := s.initGeoClient("srv-geo", clientDagor); err != nil {
		return err
	}
	if err := s.initRateClient("srv-rate", clientDagor); err != nil {
		return err
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}

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

func (s *Server) initGeoClient(name string, d *dagor.Dagor) error {
	conn, err := s.getGprcConn(name, d)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.geoClient = geo.NewGeoClient(conn)
	return nil
}

func (s *Server) initRateClient(name string, d *dagor.Dagor) error {
	conn, err := s.getGprcConn(name, d)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.rateClient = rate.NewRateClient(conn)
	return nil
}

func (s *Server) getGprcConn(name string, d *dagor.Dagor) (*grpc.ClientConn, error) {
	clientOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(d.UnaryInterceptorClient),
	}
	if name == "srv-reservation" {
		return grpc.Dial("192.168.1.5:8087", clientOptions...)
	} else if name == "srv-profile" {
		return grpc.Dial("192.168.1.4:8081", clientOptions...)
	} else if name == "srv-search" {
		return grpc.Dial("192.168.1.3:8082", clientOptions...)
	} else if name == "srv-user" {
		return grpc.Dial("192.168.1.6:8086", clientOptions...)
	} else if name == "srv-geo" {
		return grpc.Dial("192.168.1.3:8083", clientOptions...)
	} else if name == "srv-rate" {
		return grpc.Dial("192.168.1.6:8084", clientOptions...)
	}

	return grpc.Dial("192.168.1.6:8084", clientOptions...)
}

// Nearby returns ids of nearby hotels ordered by ranking algo
func (s *Server) Nearby(ctx context.Context, req *pb.NearbyRequest) (*pb.SearchResult, error) {
	// find nearby hotels
	log.Trace().Msgf("func Nearby")
	log.Trace().Msg("in Search Nearby")

	log.Trace().Msgf("nearby lat = %f", req.Lat)
	log.Trace().Msgf("nearby lon = %f", req.Lon)

	nearby, err := s.geoClient.Nearby(ctx, &geo.Request{
		Lat: req.Lat,
		Lon: req.Lon,
	})
	if err != nil {
		return nil, err
	}

	for _, hid := range nearby.HotelIds {
		log.Trace().Msgf("get Nearby hotelId = %s", hid)
	}

	// find rates for hotels
	rates, err := s.rateClient.GetRates(ctx, &rate.Request{
		HotelIds: nearby.HotelIds,
		InDate:   req.InDate,
		OutDate:  req.OutDate,
	})
	if err != nil {
		return nil, err
	}

	// TODO(hw): add simple ranking algo to order hotel ids:
	// * geo distance
	// * price (best discount?)
	// * reviews

	// build the response
	res := new(pb.SearchResult)
	for _, ratePlan := range rates.RatePlans {
		log.Trace().Msgf("get RatePlan HotelId = %s, Code = %s", ratePlan.HotelId, ratePlan.Code)
		res.HotelIds = append(res.HotelIds, ratePlan.HotelId)
	}
	return res, nil
}
