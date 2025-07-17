package frontend

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	pb "hotelReservation/services/frontend/proto"

	"hotelReservation/dialer"
	"hotelReservation/registry"
	attractions "hotelReservation/services/attractions/proto"
	profile "hotelReservation/services/profile/proto"
	recommendation "hotelReservation/services/recommendation/proto"
	reservation "hotelReservation/services/reservation/proto"
	review "hotelReservation/services/review/proto"
	search "hotelReservation/services/search/proto"
	user "hotelReservation/services/user/proto"
	"hotelReservation/tls"

	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	_ "github.com/mbobakov/grpc-consul-resolver"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

	log.Info().Msg("Initializing gRPC clients...")
	if err := s.initSearchClient("srv-search"); err != nil {
		return err
	}

	if err := s.initProfileClient("srv-profile"); err != nil {
		return err
	}

	if err := s.initUserClient("srv-user"); err != nil {
		return err
	}

	if err := s.initReservation("srv-reservation"); err != nil {
		return err
	}

	log.Info().Msg("Successful")

	opentracing.SetGlobalTracer(s.Tracer)

	if s.Port == 0 {
		return fmt.Errorf("server port must be set")
	}

	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Timeout: 120 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			PermitWithoutStream: true,
		}),
		grpc.UnaryInterceptor(
			otgrpc.OpenTracingServerInterceptor(s.Tracer),
		),
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

func (s *Server) initSearchClient(name string) error {
	conn, err := s.getGprcConn(name)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.searchClient = search.NewSearchClient(conn)
	return nil
}

func (s *Server) initReviewClient(name string) error {
	conn, err := dialer.Dial(
		name,
		dialer.WithTracer(s.Tracer),
		dialer.WithBalancer(s.Registry.Client),
	)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.reviewClient = review.NewReviewClient(conn)
	return nil
}

func (s *Server) initAttractionsClient(name string) error {
	conn, err := dialer.Dial(
		name,
		dialer.WithTracer(s.Tracer),
		dialer.WithBalancer(s.Registry.Client),
	)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.attractionsClient = attractions.NewAttractionsClient(conn)
	return nil
}

func (s *Server) initProfileClient(name string) error {
	conn, err := s.getGprcConn(name)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.profileClient = profile.NewProfileClient(conn)
	return nil
}

func (s *Server) initRecommendationClient(name string) error {
	conn, err := s.getGprcConn(name)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.recommendationClient = recommendation.NewRecommendationClient(conn)
	return nil
}

func (s *Server) initUserClient(name string) error {
	conn, err := s.getGprcConn(name)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.userClient = user.NewUserClient(conn)
	return nil
}

func (s *Server) initReservation(name string) error {
	conn, err := s.getGprcConn(name)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.reservationClient = reservation.NewReservationClient(conn)
	return nil
}

func (s *Server) getGprcConn(name string) (*grpc.ClientConn, error) {
	log.Info().Msg("get Grpc conn is :")
	log.Info().Msg(s.KnativeDns)
	log.Info().Msg(fmt.Sprintf("%s.%s", name, s.KnativeDns))

	if name == "srv-reservation" {
		return dialer.Dial(
			"192.168.1.5:8087",
			dialer.WithTracer(s.Tracer),
		)
	} else if name == "srv-profile" {
		return dialer.Dial(
			"192.168.1.4:8081",
			dialer.WithTracer(s.Tracer),
		)
	} else if name == "srv-search" {
		return dialer.Dial(
			"192.168.1.3:8082",
			dialer.WithTracer(s.Tracer),
		)
	} else if name == "srv-user" {
		return dialer.Dial(
			"192.168.1.6:8086",
			dialer.WithTracer(s.Tracer),
		)
	}

	return dialer.Dial(
		"192.168.1.5:8087",
		// fmt.Sprintf("consul://%s/%s", s.ConsulAddr, name),
		dialer.WithTracer(s.Tracer),
		// dialer.WithBalancer(s.Registry.Client),
	)
	// if s.KnativeDns != "" {
	// 	return dialer.Dial(
	// 		fmt.Sprintf("consul://%s/%s.%s", s.ConsulAddr, name, s.KnativeDns),
	// 		dialer.WithTracer(s.Tracer))
	// } else {
	// 	return dialer.Dial(
	// 		fmt.Sprintf("consul://%s/%s", s.ConsulAddr, name),
	// 		dialer.WithTracer(s.Tracer),
	// 		dialer.WithBalancer(s.Registry.Client),
	// 	)
	// }
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

func (s *Server) searchHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	ctx := r.Context()

	log.Trace().Msg("starts searchHandler")

	// in/out dates from query params
	inDate, outDate := r.URL.Query().Get("inDate"), r.URL.Query().Get("outDate")
	if inDate == "" || outDate == "" {
		http.Error(w, "Please specify inDate/outDate params", http.StatusBadRequest)
		return
	}

	// lan/lon from query params
	sLat, sLon := r.URL.Query().Get("lat"), r.URL.Query().Get("lon")
	if sLat == "" || sLon == "" {
		http.Error(w, "Please specify location params", http.StatusBadRequest)
		return
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Trace().Msg("SearchHandler gets searchResp")
	//for _, hid := range searchResp.HotelIds {
	//	log.Trace().Msgf("Search Handler hotelId = %s", hid)
	//}

	// grab locale from query params or default to en
	locale := r.URL.Query().Get("locale")
	if locale == "" {
		locale = "en"
	}

	reservationResp, err := s.reservationClient.CheckAvailability(ctx, &reservation.Request{
		CustomerName: "",
		HotelId:      searchResp.HotelIds,
		InDate:       inDate,
		OutDate:      outDate,
		RoomNumber:   1,
	})
	if err != nil {
		st, _ := status.FromError(err)
		log.Error().Msg("SearchHandler CheckAvailability failed")
		fmt.Println("err code:", st.Code()) // 如 codes.Unavailable, codes.DeadlineExceeded
		fmt.Println("err message:", st.Message())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Trace().Msg("searchHandler gets profileResp")

	json.NewEncoder(w).Encode(geoJSONResponse(profileResp.Hotels))
}

func (s *Server) recommendHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	ctx := r.Context()

	sLat, sLon := r.URL.Query().Get("lat"), r.URL.Query().Get("lon")
	if sLat == "" || sLon == "" {
		http.Error(w, "Please specify location params", http.StatusBadRequest)
		return
	}
	Lat, _ := strconv.ParseFloat(sLat, 64)
	lat := float64(Lat)
	Lon, _ := strconv.ParseFloat(sLon, 64)
	lon := float64(Lon)

	require := r.URL.Query().Get("require")
	if require != "dis" && require != "rate" && require != "price" {
		http.Error(w, "Please specify require params", http.StatusBadRequest)
		return
	}

	// recommend hotels
	recResp, err := s.recommendationClient.GetRecommendations(ctx, &recommendation.Request{
		Require: require,
		Lat:     float64(lat),
		Lon:     float64(lon),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// grab locale from query params or default to en
	locale := r.URL.Query().Get("locale")
	if locale == "" {
		locale = "en"
	}

	// hotel profiles
	profileResp, err := s.profileClient.GetProfiles(ctx, &profile.Request{
		HotelIds: recResp.HotelIds,
		Locale:   locale,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(geoJSONResponse(profileResp.Hotels))
}

func (s *Server) reviewHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	ctx := r.Context()

	username, password := r.URL.Query().Get("username"), r.URL.Query().Get("password")
	if username == "" || password == "" {
		http.Error(w, "Please specify username and password", http.StatusBadRequest)
		return
	}

	// Check username and password
	recResp, err := s.userClient.CheckUser(ctx, &user.Request{
		Username: username,
		Password: password,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	str := "Logged-in successfully!"
	if recResp.Correct == false {
		str = "Failed. Please check your username and password. "
	}

	hotelId := r.URL.Query().Get("hotelId")
	if hotelId == "" {
		http.Error(w, "Please specify hotelId params", http.StatusBadRequest)
		return
	}

	revInput := review.Request{HotelId: hotelId}

	revResp, err := s.reviewClient.GetReviews(ctx, &revInput)

	str = "Have reviews = " + strconv.Itoa(len(revResp.Reviews))
	if len(revResp.Reviews) == 0 {
		str = "Failed. No Reviews. "
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := map[string]interface{}{
		"message": str,
	}

	json.NewEncoder(w).Encode(res)
}

func (s *Server) restaurantHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	ctx := r.Context()

	username, password := r.URL.Query().Get("username"), r.URL.Query().Get("password")
	if username == "" || password == "" {
		http.Error(w, "Please specify username and password", http.StatusBadRequest)
		return
	}

	// Check username and password
	recResp, err := s.userClient.CheckUser(ctx, &user.Request{
		Username: username,
		Password: password,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	str := "Logged-in successfully!"
	if recResp.Correct == false {
		str = "Failed. Please check your username and password. "
	}

	hotelId := r.URL.Query().Get("hotelId")
	if hotelId == "" {
		http.Error(w, "Please specify hotelId params", http.StatusBadRequest)
		return
	}

	revInput := attractions.Request{HotelId: hotelId}

	revResp, err := s.attractionsClient.NearbyRest(ctx, &revInput)

	str = "Have restaurants = " + strconv.Itoa(len(revResp.AttractionIds))
	if len(revResp.AttractionIds) == 0 {
		str = "Failed. No Restaurants. "
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := map[string]interface{}{
		"message": str,
	}

	json.NewEncoder(w).Encode(res)
}

func (s *Server) museumHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	ctx := r.Context()

	username, password := r.URL.Query().Get("username"), r.URL.Query().Get("password")
	if username == "" || password == "" {
		http.Error(w, "Please specify username and password", http.StatusBadRequest)
		return
	}

	// Check username and password
	recResp, err := s.userClient.CheckUser(ctx, &user.Request{
		Username: username,
		Password: password,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	str := "Logged-in successfully!"
	if recResp.Correct == false {
		str = "Failed. Please check your username and password. "
	}

	hotelId := r.URL.Query().Get("hotelId")
	if hotelId == "" {
		http.Error(w, "Please specify hotelId params", http.StatusBadRequest)
		return
	}

	revInput := attractions.Request{HotelId: hotelId}

	revResp, err := s.attractionsClient.NearbyMus(ctx, &revInput)

	str = "Have museums = " + strconv.Itoa(len(revResp.AttractionIds))
	if len(revResp.AttractionIds) == 0 {
		str = "Failed. No Museums. "
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := map[string]interface{}{
		"message": str,
	}

	json.NewEncoder(w).Encode(res)
}

func (s *Server) cinemaHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	ctx := r.Context()

	username, password := r.URL.Query().Get("username"), r.URL.Query().Get("password")
	if username == "" || password == "" {
		http.Error(w, "Please specify username and password", http.StatusBadRequest)
		return
	}

	// Check username and password
	recResp, err := s.userClient.CheckUser(ctx, &user.Request{
		Username: username,
		Password: password,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	str := "Logged-in successfully!"
	if recResp.Correct == false {
		str = "Failed. Please check your username and password. "
	}

	hotelId := r.URL.Query().Get("hotelId")
	if hotelId == "" {
		http.Error(w, "Please specify hotelId params", http.StatusBadRequest)
		return
	}

	revInput := attractions.Request{HotelId: hotelId}

	revResp, err := s.attractionsClient.NearbyCinema(ctx, &revInput)

	str = "Have cinemas = " + strconv.Itoa(len(revResp.AttractionIds))
	if len(revResp.AttractionIds) == 0 {
		str = "Failed. No Cinemas. "
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := map[string]interface{}{
		"message": str,
	}

	json.NewEncoder(w).Encode(res)
}

func (s *Server) userHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	ctx := r.Context()

	username, password := r.URL.Query().Get("username"), r.URL.Query().Get("password")
	if username == "" || password == "" {
		http.Error(w, "Please specify username and password", http.StatusBadRequest)
		return
	}

	// Check username and password
	recResp, err := s.userClient.CheckUser(ctx, &user.Request{
		Username: username,
		Password: password,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	str := "Login successfully!"
	if recResp.Correct == false {
		str = "Failed. Please check your username and password. "
	}

	res := map[string]interface{}{
		"message": str,
	}

	json.NewEncoder(w).Encode(res)
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
func (s *Server) reservationHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	ctx := r.Context()

	inDate, outDate := r.URL.Query().Get("inDate"), r.URL.Query().Get("outDate")
	if inDate == "" || outDate == "" {
		http.Error(w, "Please specify inDate/outDate params", http.StatusBadRequest)
		return
	}

	if !checkDataFormat(inDate) || !checkDataFormat(outDate) {
		http.Error(w, "Please check inDate/outDate format (YYYY-MM-DD)", http.StatusBadRequest)
		return
	}

	hotelId := r.URL.Query().Get("hotelId")
	if hotelId == "" {
		http.Error(w, "Please specify hotelId params", http.StatusBadRequest)
		return
	}

	customerName := r.URL.Query().Get("customerName")
	if customerName == "" {
		http.Error(w, "Please specify customerName params", http.StatusBadRequest)
		return
	}

	// username, password := r.URL.Query().Get("username"), r.URL.Query().Get("password")
	// if username == "" || password == "" {
	// 	http.Error(w, "Please specify username and password", http.StatusBadRequest)
	// 	return
	// }

	numberOfRoom := 0
	num := r.URL.Query().Get("number")
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(resResp.HotelId) == 0 {
		str = "Failed. Already reserved. "
	}

	res := map[string]interface{}{
		"message": str,
	}

	json.NewEncoder(w).Encode(res)
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
