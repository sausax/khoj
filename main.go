package main

import (
	"encoding/json"
	"fmt"
	"github.com/blevesearch/bleve/v2"
	index "github.com/blevesearch/bleve_index_api"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

var SearchIndex string = "logs.index"

func indexHandler(c *gin.Context) {
	// open a new index
	mapping := bleve.NewIndexMapping()
	index, err := bleve.New(SearchIndex, mapping)
	if err != nil {
		fmt.Println(err, err.Error())
		index, err = bleve.Open(SearchIndex)
		if err != nil {
			fmt.Println(err, err.Error())
			return
		}
	}
	defer index.Close()
	//data := struct {
	//	Name string
	//}{
	//	Name: "text",
	//}

	var jsonData map[string]interface{} // map[string]interface{}
	data, _ := ioutil.ReadAll(c.Request.Body)
	if e := json.Unmarshal(data, &jsonData); e != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": e.Error()})
		return
	}

	fmt.Println("data: ", data, ", jsonData: ", jsonData)
	// index some data
	id, err := uuid.NewRandom()
	if err != nil {
		fmt.Println("Error while generating uuid")
		return
	}
	index.Index(id.String(), jsonData)

	newFsConfigBytes, _ := json.Marshal(data)

	err = index.SetInternal([]byte("id"), newFsConfigBytes)
	if err != nil {
		log.Fatal("Trouble doing SetInternal!")
	}
}

func searchHandler(c *gin.Context) {
	index, err := bleve.Open(SearchIndex)
	if err != nil {
		fmt.Println("Error while reading index: ", err)
		return
	}

	defer index.Close()

	queryText := c.Query("query")
	// search for some text
	query := bleve.NewMatchQuery(queryText)
	search := bleve.NewSearchRequest(query)
	search.Fields = []string{"*"}
	searchResults, err := index.Search(search)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(searchResults)
	c.JSON(200, gin.H{
		"results": searchResults,
	})

}

func docHandler(c *gin.Context) {
	docId := c.Param("docId")
	fmt.Println("Got docId: ", docId)

	idx, err := bleve.Open(SearchIndex)
	if err != nil {
		fmt.Println("Error while reading index: ", err)
		return
	}

	fmt.Println("Index: ", idx)
	defer idx.Close()
	fmt.Println("after defer Index: ", idx)

	doc, err := idx.Document(docId)
	fmt.Println("Got doc: ", doc)
	if err != nil {
		fmt.Sprintf("error deleting document '%s': %v", docId, err)
		return
	}
	if doc == nil {
		fmt.Sprintf("no such document '%s'", docId)
		return
	}

	fmt.Println("Got doc: ", doc)

	rv := struct {
		ID     string                 `json:"id"`
		Fields map[string]interface{} `json:"fields"`
	}{
		ID:     docId,
		Fields: map[string]interface{}{},
	}

	doc.VisitFields(func(field index.Field) {
		var newval interface{}
		switch field := field.(type) {
		case index.TextField:
			newval = field.Text()
		case index.NumericField:
			n, err := field.Number()
			if err == nil {
				newval = n
			}
		case index.DateTimeField:
			d, err := field.DateTime()
			if err == nil {
				newval = d.Format(time.RFC3339Nano)
			}
		}
		existing, existed := rv.Fields[field.Name()]
		if existed {
			switch existing := existing.(type) {
			case []interface{}:
				rv.Fields[field.Name()] = append(existing, newval)
			case interface{}:
				arr := make([]interface{}, 2)
				arr[0] = existing
				arr[1] = newval
				rv.Fields[field.Name()] = arr
			}
		} else {
			rv.Fields[field.Name()] = newval
		}
	})

	fmt.Println("rv: ", rv)
	docJson, err := json.Marshal(rv)
	if err != nil {
		fmt.Println("Error while marshaling json")
		return
	}

	fmt.Println("docJson: ", docJson)

	c.Writer.Write(docJson)
}

func main() {
	r := gin.Default()
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})
	r.POST("/index", indexHandler)
	r.GET("/search", searchHandler)
	r.GET("/doc/:docId", docHandler)
	r.Run(":9000")
}
