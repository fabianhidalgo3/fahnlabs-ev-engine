package api

import (
	_ "embed"
	"net/http"
	"strings"
)

//go:embed openapi.yaml
var openAPISpec []byte

// swaggerPage renders Swagger UI (pinned version from a CDN) for the spec served next to it.
const swaggerPage = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{TITLE}}</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.17.14/swagger-ui.css">
</head><body><div id="ui"></div>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"></script>
<script>SwaggerUIBundle({ url: "openapi.yaml", dom_id: "#ui", deepLinking: true });</script>
</body></html>`

// registerDocs serves the OpenAPI spec at /api/v1/openapi.yaml and Swagger UI at /api/v1/docs.
func registerDocs(mux *http.ServeMux, title string) {
	mux.HandleFunc("GET /api/v1/openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
		_, _ = w.Write(openAPISpec)
	})
	page := strings.ReplaceAll(swaggerPage, "{{TITLE}}", title)
	mux.HandleFunc("GET /api/v1/docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(page))
	})
}
