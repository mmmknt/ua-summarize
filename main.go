package main

import (
	"encoding/csv"
	"fmt"
	"github.com/ua-parser/uap-go/uaparser"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)

func main() {
	fmt.Println(time.Now())
	// TODO change
	f, err := os.Open("Athenaで集計した結果ファイルのパス")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	var line []string
	var client *uaparser.Client
	uaCount := make(map[string]map[string]int)
	cnt := 0

	// TODO change
	parser, err := uaparser.New("uap-coreのregexes.yamlへのパス")
	if err != nil {
		panic(err)
	}
	for {
		line, err = reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		// skip header
		if line[2] == "count" {
			fmt.Println("skip header")
			continue
		}
		service := line[0]
		ua := line[1]
		reqCnt, err := strconv.Atoi(line[2])
		if err != nil {
			fmt.Errorf("failed to convert to int: %s\n", err.Error())
			continue
		}
		client = parser.Parse(ua)
		sMap := uaCount[service]
		if sMap == nil {
			sMap = make(map[string]int)
		}
		sMap[client.UserAgent.Family] += reqCnt
		//sMap[ua] += reqCnt
		uaCount[service] = sMap
		cnt++
		if cnt % 100000 == 0 {
			fmt.Println("100K done")
			cnt = 0
		}
	}
	fmt.Println("finish parse")
	fmt.Println(time.Now())

	serviceKeys := make([]string, 0, len(uaCount))
	for k := range uaCount {
		serviceKeys = append(serviceKeys, k)
	}
	sort.Strings(serviceKeys)

	// TODO change
	w, err := os.OpenFile("グルーピングした結果の出力ファイルのパス", os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer w.Close()

	writer := csv.NewWriter(w)
	for _, sk := range serviceKeys {

		uaKeys := make([]string, 0, len(sk))
		for uak := range uaCount[sk] {
			uaKeys = append(uaKeys, uak)
		}
		sort.Strings(uaKeys)

		for _, uak := range uaKeys {
			err := writer.Write([]string{sk, uak, strconv.Itoa(uaCount[sk][uak])})
			if err != nil{
				fmt.Errorf(err.Error())
			}
		}
	}
	writer.Flush()
	err = writer.Error()
	if err != nil {
		panic(err)
	}
	fmt.Println("finish write result")
	fmt.Println(time.Now())
}