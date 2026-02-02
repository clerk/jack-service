package web

import (
	"crypto/rand"
	"embed"
	"encoding/hex"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"strconv"
	"strings"

	"github.com/clerk/jack-service/internal/storage"
	"github.com/clerk/jack-service/proto/jackpb"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static/*
var staticFS embed.FS

// Console serves the web console.
type Console struct {
	store storage.Store
	tmpl  *template.Template
}

// New creates a new Console.
func New(store storage.Store) *Console {
	tmpl := template.Must(
		template.New("").Funcs(template.FuncMap{
			"map": func(pairs ...interface{}) map[string]interface{} {
				m := make(map[string]interface{})
				for i := 0; i < len(pairs); i += 2 {
					m[pairs[i].(string)] = pairs[i+1]
				}
				return m
			},
		}).ParseFS(templateFS, "templates/*.html"),
	)
	return &Console{store: store, tmpl: tmpl}
}

func generateProducerID() string {
	b := make([]byte, 4)
	rand.Read(b)
	return fmt.Sprintf("prod_%s", hex.EncodeToString(b))
}

// RegisterRoutes adds console routes to the mux.
func (c *Console) RegisterRoutes(mux *http.ServeMux) {
	// Serve static files
	staticContent, _ := fs.Sub(staticFS, "static")
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticContent))))

	// Page routes
	mux.HandleFunc("/", c.handleIndex)
	mux.HandleFunc("/producers", c.handleProducers)
	mux.HandleFunc("/producers/new", c.handleNewProducer)
	mux.HandleFunc("/producers/create", c.handleCreateProducer)
	mux.HandleFunc("/producers/view", c.handleViewProducer)
	mux.HandleFunc("/jobtypes/new", c.handleNewJobType)
	mux.HandleFunc("/jobtypes/create", c.handleCreateJobType)
}

func (c *Console) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	http.Redirect(w, r, "/producers", http.StatusFound)
}

func (c *Console) handleProducers(w http.ResponseWriter, r *http.Request) {
	producers, err := c.store.ListProducers(r.Context())
	if err != nil {
		http.Error(w, "Failed to list producers", http.StatusInternalServerError)
		return
	}
	c.tmpl.ExecuteTemplate(w, "producers", map[string]interface{}{
		"Producers": producers,
	})
}

func (c *Console) handleNewProducer(w http.ResponseWriter, r *http.Request) {
	c.tmpl.ExecuteTemplate(w, "new_producer", nil)
}

func (c *Console) handleCreateProducer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		c.tmpl.ExecuteTemplate(w, "new_producer", map[string]string{"Error": "Name is required"})
		return
	}

	producer := &jackpb.Producer{
		ProducerId:   generateProducerID(),
		Name:         name,
		Description:  strings.TrimSpace(r.FormValue("description")),
		RateLimitRps: 1000,
	}

	if err := c.store.CreateProducer(r.Context(), producer); err != nil {
		c.tmpl.ExecuteTemplate(w, "new_producer", map[string]string{"Error": err.Error()})
		return
	}

	c.tmpl.ExecuteTemplate(w, "producer_created", map[string]interface{}{
		"Producer": producer,
	})
}

func (c *Console) handleViewProducer(w http.ResponseWriter, r *http.Request) {
	producerID := r.URL.Query().Get("id")
	if producerID == "" {
		http.Error(w, "Producer ID required", http.StatusBadRequest)
		return
	}

	producer, err := c.store.GetProducer(r.Context(), producerID)
	if err != nil {
		http.Error(w, "Producer not found", http.StatusNotFound)
		return
	}

	jobTypes, err := c.store.ListJobTypes(r.Context(), producerID)
	if err != nil {
		http.Error(w, "Failed to list job types", http.StatusInternalServerError)
		return
	}

	c.tmpl.ExecuteTemplate(w, "view_producer", map[string]interface{}{
		"Producer": producer,
		"JobTypes": jobTypes,
	})
}

func (c *Console) handleNewJobType(w http.ResponseWriter, r *http.Request) {
	producerID := r.URL.Query().Get("producer_id")
	if producerID == "" {
		http.Error(w, "Producer ID required", http.StatusBadRequest)
		return
	}

	producer, err := c.store.GetProducer(r.Context(), producerID)
	if err != nil {
		http.Error(w, "Producer not found", http.StatusNotFound)
		return
	}

	c.tmpl.ExecuteTemplate(w, "new_jobtype", map[string]interface{}{
		"Producer": producer,
	})
}

func (c *Console) handleCreateJobType(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	producerID := r.FormValue("producer_id")
	name := strings.TrimSpace(r.FormValue("name"))

	if producerID == "" || name == "" {
		http.Error(w, "Producer ID and name required", http.StatusBadRequest)
		return
	}

	queueVal, _ := strconv.Atoi(r.FormValue("queue"))
	maxRetries, _ := strconv.Atoi(r.FormValue("max_retries"))
	if maxRetries <= 0 {
		maxRetries = 3
	}

	jobType := &jackpb.JobType{
		ProducerId:  producerID,
		Name:        name,
		Queue:       jackpb.Queue(queueVal),
		MaxRetries:  int32(maxRetries),
		Description: strings.TrimSpace(r.FormValue("description")),
	}

	if err := c.store.CreateJobType(r.Context(), jobType); err != nil {
		producer, _ := c.store.GetProducer(r.Context(), producerID)
		c.tmpl.ExecuteTemplate(w, "new_jobtype", map[string]interface{}{
			"Producer": producer,
			"Error":    err.Error(),
		})
		return
	}

	http.Redirect(w, r, "/producers/view?id="+producerID, http.StatusFound)
}
