package elasticsearch

import (
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
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
	Columns []esColumnInfo  `json:"schema"`
	Rows    [][]interface{} `json:"datarows"`
	Cursor  string          `json:"Cursor"`
	Total   int64           `json:"total"`
	Status  int64           `json:"status"`
	Size    int64           `json:"size"`
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

func parsingDSN(dsn string) (url, network, username, password string, err error) {
	var protocal, address, port, certBase64 string

	dnsParts := strings.Split(dsn, "://")
	if len(dnsParts) <= 1 {
		protocal = "http"
		dsn = dnsParts[0]
	} else {
		protocal = dnsParts[0]
		dsn = dnsParts[1]
	}

	protocals := strings.Split(protocal, "+")
	if len(dnsParts) == 2 {
		protocal = protocals[0]
		network = protocals[1]
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
			return "", "", "", "", ErrInvalidArgs
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

	return protocal + "://" + address + ":" + port + "/_opendistro/_sql?format=jdbc", network, username, password, nil
}

func getEs(dsn string, body string) (string, error) {
	urld, username, password, err := parsingDSN(dsn)
	if err != nil {
		return "", err
	}

	client := http.Client{}

	// Connect to Server
	dialsLock.RLock()
	dial, ok := dials[network]
	dialsLock.RUnlock()
	if ok {
		client.Transport = &http.Transport{
			DialContext: dial,
		}
	}

	payload := strings.NewReader(fmt.Sprintf("{'query': '%s'}", strings.ReplaceAll(body, ";", "")))
	req, err := http.NewRequest("POST", urld, payload)
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

	esResult := esResult{}
	err = json.Unmarshal([]byte(esResp), &esResult)
	if err != nil {
		return nil, err
	}
	if esResult.Status != 200 {
		return nil, errors.New("Invalid SQL query")
	}

	var columns []string
	var types []esType
	for _, columnInfo := range esResult.Columns {
		columns = append(columns, columnInfo.Name)
		types = append(types, columnInfo.Type)
	}

	var rows [][]driver.Value
	for _, values := range esResult.Rows {
		var row []driver.Value
		for i, value := range values {
			row = append(row, typeConvert(types[i], value))
		}
		rows = append(rows, row)
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

func typeConvert(t esType, value interface{}) driver.Value {
	//Unsupported
	//esBinary, esByte, esObject, esNested, esUnsupported
	if value == nil {
		return ""
	}
	switch t {
	case esKeyword, esText, esIP:
		return fmt.Sprintf("%v", value)
	case esShort, esLong, esFloat, esHalfFloat, esScaledFloat, esDouble:
		oldNum := value.(float64)
		newNum := big.NewRat(1, 1)
		newNum.SetFloat64(oldNum)
		return newNum.FloatString(0)
	case esInteger:
		return int(value.(float64))
	case esBoolean:
		return value.(bool)
	case esDatetime, esDate:
		switch value.(type) {
		case string:
			return fmt.Sprintf("%v", value)
		default:
			oldNum := value.(float64)
			newNum := big.NewRat(1, 1)
			newNum.SetFloat64(oldNum)
			format := "2006-01-02 15:04:05"
			if (len(newNum.FloatString(0)) == 13) {
				return time.Unix(int64(value.(float64))/1000, 0).Format(format)
			} else {
				return time.Unix(int64(value.(float64)), 0).Format(format)
			}
		}
	case esNull:
		return nil
	default:
		return value
	}

}
