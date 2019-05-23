package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/smira/go-statsd"
	"github.com/streadway/amqp"
	"log"
	"net/http"
	"runtime"
	"time"
)

// DB settings
const (
	DB_HOST    = "localhost"
	DB_USER    = "postgres"
	DB_NAME    = "gologger"
	DB_SSLMODE = "disable"
	DB_CONNSTR = "host=%s user=%s dbname=%s sslmode=%s"
)

// rabbit settings
const (
	AMQP_CONNSTR = "amqp://%s:%s@%s:%s/"
	AMQP_USER    = "guest"
	AMQP_PASS    = "guest"
	AMQP_HOST    = "localhost"
	AMQP_PORT    = "5672"
	AMQP_Q_NAME  = "gologger"
)

// statsD settings
const (
	STATSD_HOST  = "localhost"
	STATSD_PORT  = "8125"
	STATSD_PRFX  = "gologger."
	STATSD_PSIZE = 1400
)

var db *sql.DB
var sd *statsd.Client

type Record struct {
	StampBegin string                 `json:"stamp_begin,omitempty"` // TODO: Timestamp
	StampEnd   string                 `json:"stamp_end,omitempty"`   // TODO: Timestamp
	FuncName   string                 `json:"func_name,omitempty"`
	Response   map[string]interface{} `json:"response,omitempty"`
	//Response   struct {
	//	Code    int    `json:"code"`
	//	Message string `json:"message"`
	//	map[string]interface{}
	//} `json:"response,omitempty"`
	ServiceName     string                 `json:"service_name,omitempty"`
	ExternalKey     string                 `json:"external_key,omitempty"`
	Request         map[string]interface{} `json:"request,omitempty"`
	ResponseHeaders map[string]interface{} `json:"response_headers,omitempty"`
	RequestHeaders  map[string]interface{} `json:"request_headers,omitempty"`
	Url             string                 `json:"url,omitempty"`
	HttpCode        int                    `json:"http_code,omitempty"`
}

func (record *Record) validate() error {
	var err error = nil
	if record.StampBegin == "" {
		err = errors.New("wrong stamp_begin")
	}
	if record.StampEnd == "" {
		err = errors.New("wrong stamp_end")
	}
	if record.FuncName == "" {
		err = errors.New("empty func_name")
		// TODO: "unknown"
	}

	// TODO: more checks

	return err
}

func (record *Record) dumps() []byte {
	body, err := json.Marshal(record)
	if err != nil {
		failOnError(err, "Failed to marshal a record.") // TODO: continue
	}
	return body
}

func failOnError(err error, msg string) {
	if err != nil {
		_, fn, line, _ := runtime.Caller(1)
		log.Fatalf("[error] %s %s:%d %v", msg, fn, line, err)
	}
}

func continueOnError(err error, msg string) {
	if err != nil {
		_, fn, line, _ := runtime.Caller(1)
		log.Printf("[error] %s %s:%d %v", msg, fn, line, err)
	}
}

func randomRecord() Record {
	// TODO: make it really random
	response := map[string]interface{}{
		"code":           0,
		"message":        "wrong operation",
		"oper":           "operator 100500",
		"oper_id":        4,
		"oper_region_id": 100,
	}
	request := map[string]interface{}{
		"method": "Check",
		"account": map[string]interface{}{
			"msisdn":  79004621520,
			"oper_id": 4,
		},
	}
	responseHeaders := map[string]interface{}{
		"Content-Type": "application/json; charset=utf-8",
	}
	requestHeaders := map[string]interface{}{
		"Host":                     "mocker.demo.ipl",
		"Connection":               "keep-alive",
		"X-Request-ID":             "fac85be0b24366331a7c8356eacf8753",
		"X-Real-IP":                "192.24.49.11",
		"X-Forwarded-For":          "192.24.49.11",
		"X-Forwarded-Host":         "mocker.demo.ipl",
		"X-Forwarded-Port":         "80",
		"X-Forwarded-Proto":        "http",
		"X-Original-URI":           "/mocker/refill?api_key=M97A6LLG1Q&sign=319f9cbc6655a33a54fa3",
		"X-Scheme":                 "http",
		"X-Original-Forwarded-For": "192.127.78.162",
		"Content-Length":           "69",
		"TEST_VERIFY":              "NONE",
		"TEST_PROTOCOL":            "TLSv1.2",
		"Content-Type":             "application/json",
		"X-B3-Traceid":             "bc8a8591c44f875e2af07b2ce538b5",
		"X-B3-Spanid":              "ddc566464220e4",
		"X-B3-Flags":               "0",
		"X-B3-Sampled":             "1",
		"X-B3-Parentspanid":        "40d9ee7b39b436",
		"Accept-Encoding":          "gzip",
	}
	record := Record{
		StampBegin:      "2019-05-16 12:24:37.839537+0000",
		StampEnd:        "2019-05-16 12:24:37.843896+0000",
		FuncName:        "refill_handler",
		Response:        response,
		ServiceName:     "mocker",
		ExternalKey:     "235235235",
		Request:         request,
		ResponseHeaders: responseHeaders,
		RequestHeaders:  requestHeaders,
		Url:             "http://ya.ru",
		HttpCode:        200,
	}
	return record
}

func examplePublish(rch *amqp.Channel, q *amqp.Queue) {
	record := randomRecord()
	body := record.dumps()
	err := rch.Publish(
		"",
		q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	failOnError(err, "Failed to publish a message.")
}

func writeMessage(body []byte, db *sql.DB) {
	startTime := time.Now()

	var logRecord Record

	errUnmarshal := json.Unmarshal(body, &logRecord)
	continueOnError(errUnmarshal, "Failed to parse message.")

	errValidate := logRecord.validate()
	continueOnError(errValidate, "Failed to validate message.")

	responseJson, err := json.Marshal(logRecord.Response)
	continueOnError(err, "Failed to marshall response.")

	requestJson, err := json.Marshal(logRecord.Request)
	continueOnError(err, "Failed to marshall request.")

	requestHeaders, err := json.Marshal(logRecord.RequestHeaders)
	continueOnError(err, "Failed to marshall request_headers")

	responseHeaders, err := json.Marshal(logRecord.ResponseHeaders)
	continueOnError(err, "Failed to marshall response_headers")

	if errUnmarshal == nil && errValidate == nil {
		query := "INSERT INTO gologger.log(stamp_begin, stamp_end, func_name, response, service_name, external_key, request, response_headers, request_headers, url, http_code) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
		result, err := db.Exec(query, logRecord.StampBegin, logRecord.StampEnd, logRecord.FuncName, responseJson, logRecord.ServiceName, logRecord.ExternalKey, requestJson, requestHeaders, responseHeaders, logRecord.Url, logRecord.HttpCode)
		continueOnError(err, "Failed to write record to DB.")
		rowsAffected, err := result.RowsAffected()
		fmt.Println("Rows: ", rowsAffected)
	}

	sd.PrecisionTiming("timers.work", time.Since(startTime))
}

func healthCheck() {

	type details struct {
		DB   bool `json:"db,omitempty"`
		AMQP bool `json:"amqp,omitempty"`
	}

	type healthMessage struct {
		Status  bool    `json:"status,omitempty"`
		Details details `json:"details,omitempty"`
	}

	http.HandleFunc("/healthcheck", func(w http.ResponseWriter, r *http.Request) {
		var dbState, amqpState = true, true
		// check db
		if err := db.Ping(); err != nil {
			dbState = false
		}
		// TODO: check rabbit
		amqpState = true

		status := dbState && amqpState

		details := details{
			DB:   dbState,
			AMQP: amqpState,
		}
		response := healthMessage{
			Status:  status,
			Details: details,
		}
		responseBody, _ := json.Marshal(response) // TODO: handle error

		if dbState == true && amqpState == true {
			http.Error(w, string(responseBody), 500)
		} else {
			fmt.Fprintf(w, string(responseBody))
		}
	})
	http.ListenAndServe(":8082", nil)
}

func main() {

	var err error

	sd = statsd.NewClient(
		STATSD_HOST+":"+STATSD_PORT,
		statsd.MaxPacketSize(STATSD_PSIZE),
		statsd.MetricPrefix(STATSD_PRFX),
	)
	defer sd.Close()

	// db connect
	db, err = sql.Open("postgres", fmt.Sprintf(DB_CONNSTR, DB_HOST, DB_USER, DB_NAME, DB_SSLMODE))
	failOnError(err, "Failed to connect to Postgres.")
	defer db.Close()

	// rabbit connect
	conn, err := amqp.Dial(fmt.Sprintf(AMQP_CONNSTR, AMQP_USER, AMQP_PASS, AMQP_HOST, AMQP_PORT))
	failOnError(err, "Failed to connect to RabbitMQ.")
	defer conn.Close()

	// rabbit chanel
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel.")
	defer ch.Close()

	// queue declaration
	q, err := ch.QueueDeclare(
		AMQP_Q_NAME,
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue.")

	// consuming
	msqs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer.")

	//
	//
	//

	go healthCheck()

	//
	//
	//

	examplePublish(ch, &q) // TODO: rm this

	//

	forever := make(chan bool)

	go func() {
		for d := range msqs {
			sd.Incr("record", 1)
			log.Printf("[new msg] Received a message: %s", d.Body)
			go writeMessage(d.Body, db)
		}
	}()

	log.Printf(" [listen] Waiting for messages. To exit press CTRL+C.")

	<-forever

	//
	//
	//

}
