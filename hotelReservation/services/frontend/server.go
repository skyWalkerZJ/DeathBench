package frontend

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"

	pb "hotelReservation/services/frontend/proto"

	"hotelReservation/dagor"
	"hotelReservation/registry"
	attractions "hotelReservation/services/attractions/proto"
	profile "hotelReservation/services/profile/proto"
	recommendation "hotelReservation/services/recommendation/proto"
	reservation "hotelReservation/services/reservation/proto"
	review "hotelReservation/services/review/proto"
	search "hotelReservation/services/search/proto"
	user "hotelReservation/services/user/proto"
	"hotelReservation/tls"

	_ "github.com/mbobakov/grpc-consul-resolver"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

var (
	//go:embed static/*
	content embed.FS
)

// Server implements frontend service
type Server struct {
	pb.UnimplementedFrontendServer
	searchClient         search.SearchClient
	profileClient        profile.ProfileClient
	recommendationClient recommendation.RecommendationClient
	userClient           user.UserClient
	reviewClient         review.ReviewClient
	attractionsClient    attractions.AttractionsClient
	reservationClient    reservation.ReservationClient

	KnativeDns string
	IpAddr     string
	ConsulAddr string
	Port       int
	Tracer     opentracing.Tracer
	Registry   *registry.Client
}

// Run the server
func (s *Server) Run() error {
	if s.Port == 0 {
		return fmt.Errorf("Server port must be set")
	}

	client_param := dagor.DagorParam{
		NodeName:                     "frontend",
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
	log.Info().Msg("Initializing gRPC clients...")
	if err := s.initSearchClient("srv-search", clientDagor); err != nil {
		return err
	}

	if err := s.initProfileClient("srv-profile", clientDagor); err != nil {
		return err
	}

	if err := s.initUserClient("srv-user", clientDagor); err != nil {
		return err
	}

	if err := s.initReservation("srv-reservation", clientDagor); err != nil {
		return err
	}

	log.Info().Msg("Successful")

	opentracing.SetGlobalTracer(s.Tracer)

	if s.Port == 0 {
		return fmt.Errorf("server port must be set")
	}

	server_param := dagor.DagorParam{
		NodeName:                     "frontend",
		BusinessMap:                  map[string]int{"/frontend.Frontend/search": 2, "/frontend.Frontend/CheckAvailability": 1},
		QueuingThresh:                5 * time.Millisecond,
		EntryService:                 true,
		IsEnduser:                    false,
		AdmissionLevelUpdateInterval: 1 * time.Second,
		Alpha:                        0.7,
		Beta:                         0.3,
		Umax:                         1000,
		Bmax:                         500,
		Debug:                        true,
		UseSyncMap:                   false,
	}

	d := dagor.NewDagorNode(server_param)

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

	pb.RegisterFrontendServer(srv, s)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}

	log.Info().Msgf("In reservation s.IpAddr = %s, port = %d", s.IpAddr, s.Port)
	return srv.Serve(lis)
}

func (s *Server) initSearchClient(name string, d *dagor.Dagor) error {
	conn, err := s.getGprcConn(name, d)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.searchClient = search.NewSearchClient(conn)
	return nil
}
func (s *Server) initProfileClient(name string, d *dagor.Dagor) error {
	conn, err := s.getGprcConn(name, d)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.profileClient = profile.NewProfileClient(conn)
	return nil
}
func (s *Server) initUserClient(name string, d *dagor.Dagor) error {
	conn, err := s.getGprcConn(name, d)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.userClient = user.NewUserClient(conn)
	return nil
}

func (s *Server) initReservation(name string, d *dagor.Dagor) error {
	conn, err := s.getGprcConn(name, d)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.reservationClient = reservation.NewReservationClient(conn)
	return nil
}

func (s *Server) getGprcConn(name string, d *dagor.Dagor) (*grpc.ClientConn, error) {
	log.Info().Msg("get Grpc conn is :")
	log.Info().Msg(s.KnativeDns)
	log.Info().Msg(fmt.Sprintf("%s.%s", name, s.KnativeDns))

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
	}

	return grpc.Dial("192.168.1.5:8087", clientOptions...)
}
func (s *Server) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error) {

	log.Info().Msg("starts searchHandler")
	inDate := req.GetInDate()
	outDate := req.GetOutDate()

	// in/out dates from query params
	if inDate == "" || outDate == "" {
		log.Error().Msg("Please specify inDate/outDate params")
		return nil, status.Errorf(codes.InvalidArgument, "Please specify inDate/outDate params")
	}

	sLat := req.GetLat()
	sLon := req.GetLon()
	// lan/lon from query params

	if sLat == "" || sLon == "" {
		log.Error().Msg("Please specify location params")
		return nil, status.Errorf(codes.InvalidArgument, "Please specify location params")
	}

	Lat, _ := strconv.ParseFloat(sLat, 32)
	lat := float32(Lat)
	Lon, _ := strconv.ParseFloat(sLon, 32)
	lon := float32(Lon)

	log.Trace().Msg("starts searchHandler querying downstream")

	log.Trace().Msgf("SEARCH [lat: %v, lon: %v, inDate: %v, outDate: %v", lat, lon, inDate, outDate)
	// search for best hotels
	searchResp, err := s.searchClient.Nearby(ctx, &search.NearbyRequest{
		Lat:     lat,
		Lon:     lon,
		InDate:  inDate,
		OutDate: outDate,
	})
	if err != nil {
		log.Error().Msg(err.Error())
		return nil, status.Errorf(codes.Unavailable, "failed to call Nearby service: %v", err)
	}

	log.Trace().Msg("SearchHandler gets searchResp")
	//for _, hid := range searchResp.HotelIds {
	//	log.Trace().Msgf("Search Handler hotelId = %s", hid)
	//}

	locale := "en"

	reservationResp, err := s.reservationClient.CheckAvailability(ctx, &reservation.Request{
		CustomerName: "",
		HotelId:      searchResp.HotelIds,
		InDate:       inDate,
		OutDate:      outDate,
		RoomNumber:   1,
	})
	if err != nil {
		log.Error().Msg("SearchHandler CheckAvailability failed")
		return nil, status.Errorf(codes.Unavailable, "failed to call CheckAvailability service: %v", err)
	}

	log.Trace().Msgf("searchHandler gets reserveResp")
	log.Trace().Msgf("searchHandler gets reserveResp.HotelId = %s", reservationResp.HotelId)

	// hotel profiles
	profileResp, err := s.profileClient.GetProfiles(ctx, &profile.Request{
		HotelIds: reservationResp.HotelId,
		Locale:   locale,
	})
	if err != nil {
		log.Error().Msg("SearchHandler GetProfiles failed")
		return nil, status.Errorf(codes.Unavailable, "failed to call GetProfiles service: %v", err)
	}
	log.Trace().Msg("searchHandler gets profileResp")

	jsonbytes, err := json.Marshal(geoJSONResponse(profileResp.Hotels))
	if err != nil {
		log.Error().Msgf("Failed to marshal geoJSON response: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to call json.Marshal service: %v", err)
	}
	return &pb.SearchResponse{
		Searchresult: string(jsonbytes),
	}, nil
}

// 实现 Reservation RPC
func (s *Server) Reservation(ctx context.Context, req *pb.ReservationRequest) (*pb.ReservationResponse, error) {

	log.Info().Msg("starts searchHandler")
	inDate := req.GetInDate()
	outDate := req.GetOutDate()

	// in/out dates from query params
	if inDate == "" || outDate == "" {
		log.Error().Msg("Please specify inDate/outDate params")
		return nil, status.Errorf(codes.InvalidArgument, "Please specify inDate/outDate params")
	}

	if !checkDataFormat(inDate) || !checkDataFormat(outDate) {
		return nil, status.Errorf(codes.InvalidArgument, "Please specify inDate/outDate params")
	}

	hotelId := req.GetHotelId()
	if hotelId == "" {
		log.Error().Msg("Please specify hotelId params")
		return nil, status.Errorf(codes.InvalidArgument, "Please specify hotelId params")
	}

	customerName := req.GetCustomerName()
	if customerName == "" {
		log.Error().Msg("Please specify customerName params")
		return nil, status.Errorf(codes.InvalidArgument, "Please specify customerName params")
	}

	// username, password := r.URL.Query().Get("username"), r.URL.Query().Get("password")
	// if username == "" || password == "" {
	// 	http.Error(w, "Please specify username and password", http.StatusBadRequest)
	// 	return
	// }

	numberOfRoom := 0
	num := req.GetNumber()
	if num != "" {
		numberOfRoom, _ = strconv.Atoi(num)
	}

	// Check username and password
	// recResp, err := s.userClient.CheckUser(ctx, &user.Request{
	// 	Username: username,
	// 	Password: password,
	// })
	// if err != nil {
	// 	http.Error(w, err.Error(), http.StatusInternalServerError)
	// 	return
	// }

	str := "Reserve successfully!"
	// if recResp.Correct == false {
	// 	str = "Failed. Please check your username and password. "
	// }

	// Make reservation
	resResp, err := s.reservationClient.MakeReservation(ctx, &reservation.Request{
		CustomerName: customerName,
		HotelId:      []string{hotelId},
		InDate:       inDate,
		OutDate:      outDate,
		RoomNumber:   int32(numberOfRoom),
	})
	if err != nil {
		log.Error().Msg(err.Error())
		return nil, status.Errorf(codes.Unavailable, "failed to call MakeReservation service: %v", err)
	}
	if len(resResp.HotelId) == 0 {
		str = "Failed. Already reserved. "
	}

	res := map[string]interface{}{
		"message": str,
	}

	jsonbytes, err := json.Marshal(res)
	if err != nil {
		log.Error().Msg(err.Error())
		return nil, status.Errorf(codes.Unavailable, "failed to call json.Marshal service: %v", err)
	}
	return &pb.ReservationResponse{Reservationresult: string(jsonbytes)}, nil
}

// return a geoJSON response that allows google map to plot points directly on map
// https://developers.google.com/maps/documentation/javascript/datalayer#sample_geojson
func geoJSONResponse(hs []*profile.Hotel) map[string]interface{} {
	fs := []interface{}{}

	for _, h := range hs {
		// 默认经纬度为 0，如果 Address 为 nil
		lon := float32(0)
		lat := float32(0)
		if h.Address != nil {
			lon = h.Address.Lon
			lat = h.Address.Lat
		}

		fs = append(fs, map[string]interface{}{
			"type": "Feature",
			"id":   h.Id,
			"properties": map[string]string{
				"name":         h.Name,
				"phone_number": h.PhoneNumber,
			},
			"geometry": map[string]interface{}{
				"type":        "Point",
				"coordinates": []float32{lon, lat},
			},
		})
	}

	return map[string]interface{}{
		"type":     "FeatureCollection",
		"features": fs,
	}
}

func checkDataFormat(date string) bool {
	if len(date) != 10 {
		return false
	}
	for i := 0; i < 10; i++ {
		if i == 4 || i == 7 {
			if date[i] != '-' {
				return false
			}
		} else {
			if date[i] < '0' || date[i] > '9' {
				return false
			}
		}
	}
	return true
}
