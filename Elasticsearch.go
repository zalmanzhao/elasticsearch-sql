package elasticsearch

import (
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"
)

type esQueryRequest struct {
	Query string `json:"query"`
}

type esCursorRequest struct {
	Cursor string `json:"cursor"`
}

type esColumnInfo struct {
	Name string `json:"name"`
	Type esType `json:"type"`
}

type esResult struct {
	Columns []esColumnInfo  `json:"columns"`
	Rows    [][]interface{} `json:"rows"`
	Cursor  string          `json:"Cursor"`
	Hits    struct{
		Total struct{
			Value int64 `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		MaxScore float64 `json:"max_score"`
		Hits []struct{
			Index string `json:"_index"`
			Type string `json:"_type"`
			ID string `json:"_id"`
			Score float64 `json:"_score"`
			Source map[string]interface{} `json:"_source"`
		} `json:"hits"`
	}     `json:"hits"`
	Took    int64           `json:"took"`
	Shards interface{}       `json:"_shards"`
}


func newRows(dsn string, query string) (*Rows, error) {
	return esRequest(dsn, query)
}

func nextRows(dsn string, cursor string) ([][]driver.Value, string, error) {
	byteReqBody, err := json.Marshal(esCursorRequest{cursor})
	if err != nil {
		return nil, "", err
	}
	result, err := esRequest(dsn, string(byteReqBody))

	return (*result).rows, (*result).cursor, err
}

func parsingDSN(dsn string) (url, username, password string, err error) {
	var protocal, address, port, certBase64 string

	dnsParts := strings.Split(dsn, "://")
	if len(dnsParts) <= 1 {
		protocal = "http"
		dsn = dnsParts[0]
	} else {
		protocal = dnsParts[0]
		dsn = dnsParts[1]
	}

	dnsParts = strings.Split(dsn, "@")
	if len(dnsParts) <= 1 {
		certBase64 = ""
		dsn = dnsParts[0]
	} else {
		certBase64 = dnsParts[0]
		dsn = dnsParts[1]
	}

	if certBase64 != "" {
		certByte, err := base64.URLEncoding.DecodeString(certBase64)
		if err != nil {
			return "", "", "", ErrInvalidArgs
		}
		certPart := strings.Split(string(certByte), ":")
		username, password = certPart[0], certPart[1]
	}

	dnsParts = strings.Split(dsn, ":")
	if len(dnsParts) <= 1 {
		address = "localhost"
		port = "9200"
	} else {
		address = dnsParts[0]
		if len(dnsParts[1]) == 0 {
			port = "9200"
		} else {
			port = dnsParts[1]
		}
	}

	return protocal + "://" + address + ":" + port + "/_opendistro/_sql?sql=", username, password, nil
}

func getEs(dsn string, body string) (string, error) {
	urld, username, password, err := parsingDSN(dsn)
	if err != nil {
		return "", err
	}

	client := http.Client{}
	req, err := http.NewRequest("GET", urld + url.PathEscape(body), nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("Content-type", "application/json")
	if username != "" && password != "" {
		req.SetBasicAuth(username, password)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()


	var esResp string
	buff := make([]byte, 10)
	n, err := io.ReadFull(resp.Body, buff)
	for err != io.EOF {
		esResp = esResp + string(buff[:n])
		n, err = io.ReadFull(resp.Body, buff)
	}

	return esResp, nil
}

func esRequest(dsn string, body string) (*Rows, error) {
	if body == "show tables" {
		body = "show tables like %"
	}
	esResp, err := getEs(dsn, body)
	if err != nil {
		return nil, err
	}
	var columns []string
	var types []esType
	var rows [][]driver.Value

	if body == "show tables like %" {
		esResult := make(map[string]interface{})
		err = json.Unmarshal([]byte(esResp), &esResult)
		if err != nil {
			return nil, err
		}

		columns = append(columns, "name")
		for table := range esResult {
			var row []driver.Value
			row = append(row, table)
			rows = append(rows, row)
		}

		return &Rows{
				dsn:     dsn,
				columns: columns,
				types:   types,
				rows:    rows,
				cursor:  "1",
			},
			nil
	} else {
		esResult := esResult{}
		err = json.Unmarshal([]byte(esResp), &esResult)
		if err != nil {
			return nil, err
		}

		var i = 0
		for _, h := range esResult.Hits.Hits {
			var row []driver.Value
			for k, v := range h.Source {
				if reflect.TypeOf(v).Kind().String() == "slice" {
					continue
				}
				if reflect.TypeOf(v).Kind().String() == "map" {
					for ek, ev := range v.(map[string]interface{}) {
						if i == 0 {
							columns = append(columns, ek)
							types = append(types, "string")
						}
						row = append(row, ev.(string))
					}
				} else {
					if i == 0 {
						columns = append(columns, k)
						types = append(types, "string")
					}
					row = append(row, v.(string))
				}
			}
			rows = append(rows, row)
			i++
		}

		return &Rows{
				dsn:     dsn,
				columns: columns,
				types:   types,
				rows:    rows,
				cursor:  esResult.Cursor,
			},
			nil
	}
}

func typeConvert(t esType, value interface{}) driver.Value {
	//Unsupported
	//esBinary, esByte, esObject, esNested, esUnsupported
	switch t {
	case esKeyword, esText, esIP:
		return value.(string)
	case esShort, esLong, esFloat, esHalfFloat, esScaledFloat, esDouble:
		return value.(float64)
	case esInteger:
		return int(value.(float64))
	case esBoolean:
		return value.(bool)
	case esDatetime:
		t, err := time.Parse(time.RFC3339, value.(string))
		if err != nil {
			return nil
		}
		return t
	case esNull:
		return nil
	default:
		return value
	}

}
