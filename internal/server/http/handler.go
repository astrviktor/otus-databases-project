package internalhttp

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"log"
	"net/http"
	"strconv"
	"strings"
)

//type Description struct {
//	Description string `json:"description"`
//}

type ResponseError struct {
	Error string `json:"error"`
}

//
//type ResponseID struct {
//	ID string `json:"id"`
//}

type ResponseUUID struct {
	UUID uuid.UUID `json:"uuid"`
}

type ResponseSegment struct {
	UUID uuid.UUID `json:"uuid"`
	Size int       `json:"size"`
}

//type ResponseStat struct {
//	ShowCount  int `json:"showCount"`
//	ClickCount int `json:"clickCount"`
//}

func WriteResponse(w http.ResponseWriter, resp interface{}) {
	resBuf, err := json.Marshal(resp)
	if err != nil {
		log.Println(fmt.Sprintf("response marshal error: %s", err))
	}

	resBuf = append(resBuf, []byte("\n")...)
	_, err = w.Write(resBuf)

	if err != nil {
		log.Println(fmt.Sprintf("response marshal error: %s", err))
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
}

// handlers

//func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
//	w.WriteHeader(http.StatusOK)
//	_, _ = io.WriteString(w, "OK")
//}
//
//func (s *Server) handleCreateBanner(w http.ResponseWriter, r *http.Request) {
//	if r.Method == http.MethodPost {
//		s.CreateItem(Banner, w, r)
//		return
//	}
//}
//
//func (s *Server) handleCreateSlot(w http.ResponseWriter, r *http.Request) {
//	if r.Method == http.MethodPost {
//		s.CreateItem(Slot, w, r)
//		return
//	}
//}

// curl --request POST 'http://127.0.0.1:8888/segment/10000'

func (s *Server) handleCreateClients(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusBadRequest)
		WriteResponse(w, &ResponseError{fmt.Sprint("method must be POST")})
		return
	}

	path := r.URL.Path
	params := strings.Split(path, "/")
	if len(params) != 3 {
		w.WriteHeader(http.StatusBadRequest)
		WriteResponse(w, &ResponseError{fmt.Sprintf("request format error %s", path)})
		return
	}

	sizeString := params[2]
	size, err := strconv.Atoi(sizeString)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		WriteResponse(w, &ResponseError{fmt.Sprintf("size must be int, get %s", sizeString)})
		return
	}

	s.storage.CreateClients(size)

	w.WriteHeader(http.StatusOK)
	//WriteResponse(w, &ResponseUUID{UUID: uuid})

	return
}

func (s *Server) handleCreateSegment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusBadRequest)
		WriteResponse(w, &ResponseError{fmt.Sprint("method must be POST")})
		return
	}

	path := r.URL.Path
	params := strings.Split(path, "/")
	if len(params) != 3 {
		w.WriteHeader(http.StatusBadRequest)
		WriteResponse(w, &ResponseError{fmt.Sprintf("request format error %s", path)})
		return
	}

	sizeString := params[2]
	size, err := strconv.Atoi(sizeString)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		WriteResponse(w, &ResponseError{fmt.Sprintf("size must be int, get %s", sizeString)})
		return
	}

	uuid, err := s.storage.CreateSegment(size)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println("ERROR: ", err)
		WriteResponse(w, &ResponseError{fmt.Sprintf("error during segment creation, uuid %s", uuid.String())})
		return
	}

	w.WriteHeader(http.StatusOK)
	WriteResponse(w, &ResponseUUID{UUID: uuid})

	return
}

func (s *Server) handleChangeDatabase(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusBadRequest)
		WriteResponse(w, &ResponseError{fmt.Sprint("method must be POST")})
		return
	}

	path := r.URL.Path
	params := strings.Split(path, "/")
	if len(params) != 3 {
		w.WriteHeader(http.StatusBadRequest)
		WriteResponse(w, &ResponseError{fmt.Sprintf("request format error %s", path)})
		return
	}

	database := params[2]
	err := s.ChangeDatabase(database)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		WriteResponse(w, &ResponseError{fmt.Sprintf("error with change with %s", database)})
		return
	}

	w.WriteHeader(http.StatusOK)

	return
}

func (s *Server) handleDeleteClients(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		w.WriteHeader(http.StatusBadRequest)
		WriteResponse(w, &ResponseError{fmt.Sprint("method must be DELETE")})
		return
	}

	err := s.storage.DeleteClients()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		WriteResponse(w, &ResponseError{fmt.Sprintf("error with delete: %s", err)})
		return
	}

	w.WriteHeader(http.StatusOK)

	return
}

func (s *Server) handleGetSegment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusBadRequest)
		WriteResponse(w, &ResponseError{fmt.Sprint("method must be GET")})
		return
	}

	id, size, err := s.storage.GetSegment()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		WriteResponse(w, &ResponseError{fmt.Sprintf("error with get segment: %s", err)})
		return
	}

	w.WriteHeader(http.StatusOK)
	WriteResponse(w, &ResponseSegment{UUID: id, Size: size})

	return
}

/*
curl --request POST 'http://127.0.0.1:8888/segment' \
--header 'Content-Type: application/json' \
--data-raw '{"description": "123"}'
*/
