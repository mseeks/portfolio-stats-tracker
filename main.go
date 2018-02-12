package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/antonholmquist/jason"
	resty "gopkg.in/resty.v1"
)

var (
	apiEndpoint   string
	broker        string
	producerTopic string
)

// The formatter for passing messages into Kafka
type message struct {
	BuyingPower string `json:"buying_power"`
	Equity      string `json:"equity"`
	At          string `json:"at"`
}

// Macro function to run the tracking process
func trackStats() error {
	resp, err := resty.R().
		SetHeader("Accept", "application/json").
		SetHeader("Authorization", fmt.Sprint("Token ", os.Getenv("ROBINHOOD_TOKEN"))).
		Get(fmt.Sprint(apiEndpoint, "accounts/"))
	if err != nil {
		return err
	}

	if resp.StatusCode() != 200 {
		return fmt.Errorf("Incorrect status code: %v", resp.Status())
	}

	value, err := jason.NewObjectFromBytes(resp.Body())
	if err != nil {
		return err
	}

	results, err := value.GetObjectArray("results")
	if err != nil {
		return err
	}

	account := results[0]

	portfolioUrl, err := account.GetString("portfolio")
	if err != nil {
		return err
	}

	resp2, err := resty.R().
		SetHeader("Accept", "application/json").
		SetHeader("Authorization", fmt.Sprint("Token ", os.Getenv("ROBINHOOD_TOKEN"))).
		Get(portfolioUrl)
	if err != nil {
		return err
	}

	if resp.StatusCode() != 200 {
		return fmt.Errorf("Incorrect status code: %v", resp2.Status())
	}

	portfolio, err := jason.NewObjectFromBytes(resp2.Body())
	if err != nil {
		return err
	}
	
	equity, err := portfolio.GetString("equity")
	if err != nil {
		return err
	}

	extendedHoursEquity, err := portfolio.GetString("extended_hours_equity")
	if err != nil {
		if err.Error() != "not a string" {
			return err
		}
	}
	
	buyingPower, err := account.GetString("margin_balances", "unallocated_margin_cash")
	if err != nil {
		return err
	}

	currentEquity := equity
	if len(extendedHoursEquity) > 0 {
		currentEquity = extendedHoursEquity
	}

	producer, err := sarama.NewSyncProducer([]string{broker}, nil)
	if err != nil {
		return err
	}
	defer producer.Close()

	statsMessage := message{
		BuyingPower: buyingPower,
		Equity:      currentEquity,
		At:          time.Now().UTC().Format("2006-01-02 15:04:05 -0700"),
	}

	jsonMessage, err := json.Marshal(statsMessage)
	if err != nil {
		return err
	}

	jsonMessageString := string(jsonMessage)
	fmt.Println("Sending:", jsonMessageString)

	msg := &sarama.ProducerMessage{Topic: producerTopic, Value: sarama.StringEncoder(jsonMessage)}
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		return err
	}

	return nil
}

// Entrypoint for the program
func main() {
	apiEndpoint = "https://api.robinhood.com/"
	broker = os.Getenv("KAFKA_ENDPOINT")
	producerTopic = os.Getenv("KAFKA_PRODUCER_TOPIC")

	for {
		time.Sleep(10 * time.Second)
		err := trackStats()
		if err != nil {
			fmt.Println(err)
		}
	}
}
