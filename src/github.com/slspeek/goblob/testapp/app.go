package main

import (
  "github.com/slspeek/goblob"
	"fmt"
	"html/template"
	"io"
	"labix.org/v2/mgo"
	"log"
	"net/http"
)

var rootTemplate = template.Must(template.New("root").Parse(rootTemplateHTML))

var sess mgo.Session

const rootTemplateHTML = `
<html><body>
<form action="{{.}}" method="POST" enctype="multipart/form-data">
Upload File: <input type="file" name="file"><br>
<input type="submit" name="submit" value="Submit">
</form></body></html>
`

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	uploadURL := "/upload"
	w.Header().Set("Content-Type", "text/html")
	err := rootTemplate.Execute(w, uploadURL)
	if err != nil {
		log.Printf("%v", err)
	}
}

func handleServe(w http.ResponseWriter, r *http.Request) {
	filename := r.FormValue("filename")
	bs, err := goblob.NewBlobService("localhost", "test", "fs")
	check(err)
	gf, err := bs.OpenName(filename)
	io.Copy(w, gf)
	gf.Close()
	bs.Close()
}

func handleUpload(w http.ResponseWriter, r *http.Request) {
	log.Print("Upload")
	reader, err := r.MultipartReader()
	if err != nil {
		fmt.Println(err)
		http.Error(w, "not a form", http.StatusBadRequest)
	}
	defer r.Body.Close()

	filename := ""
	// read part by part
	for {
		part, err := reader.NextPart()
		if err != nil {
			if err == io.EOF {
				// end of parts
				break
			}
			fmt.Println(err)
			http.Error(w, "bad form part", http.StatusBadRequest)
		}
		// use part.Read to read content
		// then
		if part.FormName() == "file" {
			bs, err := goblob.NewBlobService("localhost", "test", "fs")
			if err != nil {
				panic(err)
			}
			filename = part.FileName()
			fmt.Println("filename: ", part.FileName())
			fmt.Println("formname: ", part.FormName())
			gf, err := bs.Create(filename)
			if err != nil {
				panic(err)
			}
			_, err = io.Copy(gf, part)
			if err != nil {
				panic(err)
			}
			gf.Close()
			bs.Close()
		}
		part.Close()
	}
	http.Redirect(w, r, "/serve/?filename="+filename, http.StatusFound)
}

func main() {
	http.HandleFunc("/", handleRoot)
	http.HandleFunc("/serve/", handleServe)
	http.HandleFunc("/upload", handleUpload)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
