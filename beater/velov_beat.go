package beater

import (
	"fmt"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"

	"github.com/txtweet/velov_beat/config"
)

// velov_beat configuration.
type velov_beat struct {
	done   chan struct{}
	config config.Config
	client beat.Client
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
	counter := 1
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}

		event := beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    b.Info.Name,
				"counter": counter,
			},
		}
		bt.client.Publish(event)
		logp.Info("Event sent")
		counter++
	}
}

// Stop stops velov_beat.
func (bt *velov_beat) Stop() {
	bt.client.Close()
	close(bt.done)
}
