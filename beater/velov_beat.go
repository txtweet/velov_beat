package beater

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"

	"github.com/txtweet/velov_beat/config"
)

const (
	apiUrl  = "https://api.jcdecaux.com/"
	apiPath = "vls/v3/stations"
	api_key = "76d6d73e00da651b6e90532a9ba2cfd1d2fabe72"

	selector = "velotest"
)

// velov_beat configuration.
type velov_beat struct {
	done   chan struct{}
	config config.Config
	client beat.Client
}

type apiResponsaData struct {
	Number     int       `json:"number"`
	Contract   string    `json:"contractName"`
	Name       string    `json:"name"`
	Address    string    `json:"address"`
	Position   Coordo    `json:"position"`
	Banking    bool      `json:"banking"`
	Bonus      bool      `json:"bonus"`
	Status     string    `json:"status"`
	LastUpdate time.Time `json:"lastUpdate"`
	Connected  bool      `json:"connected"`
	Overflow   bool      `json:"overflow"`
	Stands     Stand     `json:"totalStands"`
}

type Coordo struct {
	Lat float32 `json:"latitude"`
	Lon float32 `json:"longitude"`
}

type Stand struct {
	Available Availabilities `json:"availabilities"`
	Capacity  int            `json:"capacity"`
}

type Availabilities struct {
	Bikes           int `json:"bikes"`
	Stands          int `json:"stands"`
	MechanicalBikes int `json:"mechanicalBikes"`
	ElectricalBikes int `json:"electricalBikes"`
}

// New creates an instance of velov_beat.
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	c := config.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &velov_beat{
		done:   make(chan struct{}),
		config: c,
	}
	return bt, nil
}

// Run starts velov_beat.
func (bt *velov_beat) Run(b *beat.Beat) error {
	logp.Info("velov_beat is running! Hit CTRL-C to stop it.")

	var err error
	bt.client, err = b.Publisher.Connect()
	if err != nil {
		return err
	}

	ticker := time.NewTicker(bt.config.Period)
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}

		var ParsedUrl *url.URL
		client := &http.Client{}

		ParsedUrl, err := url.Parse(apiUrl)
		if err != nil {
			logp.NewLogger(selector).Error("Unable to parse URL string")
			panic(err)
		}

		ParsedUrl.Path += apiPath

		parameters := url.Values{}
		parameters.Add("apiKey", api_key)
		//parameters.Add("contract", "lyon")

		ParsedUrl.RawQuery = parameters.Encode()

		logp.NewLogger(selector).Debug("Requesting Velov data: ", ParsedUrl.String())
		//fmt.Println(ParsedUrl.String())
		req, err := http.NewRequest("GET", ParsedUrl.String(), nil)
		res, err := client.Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()

		if res.StatusCode != 200 {
			logp.NewLogger(selector).Debug("Status code: ", res.StatusCode)
			logp.NewLogger(selector).Debug("Status code: ", res.Body)
			return fmt.Errorf("HTTP %v", res)
		}

		body, err := ioutil.ReadAll(res.Body)

		// check if the response is not an empty array
		if len(body) <= 2 {
			logp.NewLogger(selector).Debug("API call '", ParsedUrl.String(), "' returns 0 results. Response body: ", string(body))
			return nil
		}

		logp.NewLogger(selector).Debug(string(body))
		if err != nil {
			log.Fatal(err)
			return err
		}
		//fmt.Println(string(body))

		var velosDatas []apiResponsaData
		err = json.Unmarshal(body, &velosDatas)
		if err != nil {
			return err
		}

		transDatas := bt.TransformAPIData(velosDatas)
		for _, d := range transDatas {
			event := beat.Event{
				Timestamp: time.Now(),
				Fields: common.MapStr{
					"station": d,
				},
			}
			bt.client.Publish(event)
			logp.NewLogger(selector).Debug("Event: ", event)
		}

	}
}

// Stop stops velov_beat.
func (bt *velov_beat) Stop() {
	bt.client.Close()
	close(bt.done)
}

func (bt *velov_beat) TransformAPIData(data []apiResponsaData) []common.MapStr {
	var ret []common.MapStr
	for _, stData := range data {
		station := common.MapStr{
			"number":       stData.Number,
			"contractName": stData.Contract,
			"name":         stData.Name,
			"address":      stData.Address,
			"location": common.MapStr{
				"lat": stData.Position.Lat,
				"lon": stData.Position.Lon,
			},
			"banking":    stData.Banking,
			"bonus":      stData.Bonus,
			"status":     stData.Status,
			"lastUpdate": stData.LastUpdate,
			"connected":  stData.Connected,
			"overflow":   stData.Overflow,
			"stands": common.MapStr{
				"capacity": stData.Stands.Capacity,
				"availabilities": common.MapStr{
					"bikes":           stData.Stands.Available.Bikes,
					"stands":          stData.Stands.Available.Stands,
					"mechanicalBikes": stData.Stands.Available.MechanicalBikes,
					"electricalBikes": stData.Stands.Available.ElectricalBikes,
				},
			},
		}
		ret = append(ret, station)
	}
	return ret
}
