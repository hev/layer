package tui

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	hevlayer "github.com/hev/layer/clients/go"
)

func TestCEIndexDetailHistory(t *testing.T) {
	for _, status := range []int{200, 404, 401, 403, 500, 0} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v2/namespaces/local/history" {
					t.Errorf("path=%s", r.URL.Path)
				}
				if status == 0 {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					conn.Close()
					return
				}
				w.WriteHeader(status)
				if status == 200 {
					fmt.Fprint(w, `[{"watermark_ms":1700000000000,"sha":"abc"}]`)
				}
			}))
			defer server.Close()
			m := Model{client: hevlayer.NewClient(hevlayer.WithBaseURL(server.URL)), indexDetail: indexDetailModel{name: "local", entry: hevlayer.NamespaceListEntry{Name: "local", RowCount: 2}, loading: true}}
			msg := m.loadIndexDetail()().(indexDetailLoadedMsg)
			wantOK := status == 200 || status == 404
			if (msg.err == nil) != wantOK {
				t.Fatalf("status=%d err=%v", status, msg.err)
			}
			updated, _ := m.Update(msg)
			m = updated.(Model)
			rendered := m.viewIndexDetail()
			if status == 404 && (len(m.indexDetail.history) != 0 || m.indexDetail.moreThan || m.indexDetail.loading || !strings.Contains(rendered, "Last snapshot") || !strings.Contains(rendered, "—")) {
				t.Fatalf("detail=%+v view=%s", m.indexDetail, rendered)
			}
			if status == 200 && len(m.indexDetail.history) != 1 {
				t.Fatal("history lost")
			}
			if !wantOK && !strings.Contains(rendered, msg.err.Error()) {
				t.Fatalf("error hidden: %s", rendered)
			}
		})
	}
}

func TestCEIndexMetadataErrorsRemainVisible(t *testing.T) {
	for _, status := range []int{404, 401, 403, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
			defer server.Close()
			m := Model{client: hevlayer.NewClient(hevlayer.WithBaseURL(server.URL))}
			msg := m.loadIndexes()().(indexesLoadedMsg)
			if msg.err == nil {
				t.Fatalf("metadata %d was hidden", status)
			}
			updated, _ := m.Update(msg)
			if updated.(Model).indexes.err == nil {
				t.Fatal("metadata error lost by update")
			}
		})
	}
}
