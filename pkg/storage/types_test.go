package storage

import (
	"github.com/automuteus/utils/pkg/premium"
	"strings"
	"testing"
)

func TestPostgresGuild_ToCSV(t *testing.T) {
	g := PostgresGuild{
		GuildID:       123,
		GuildName:     "test_name",
		Premium:       int16(premium.SelfHostTier),
		TxTimeUnix:    nil,
		TransferredTo: nil,
		InheritsFrom:  nil,
	}
	if strings.Split(g.ToCSV(), "\n")[1] != "123,test_name,5,,," {
		t.Error("Postgres guild didn't serialize to csv as expected")
	}

	time := int32(456)
	g.TxTimeUnix = &time
	if strings.Split(g.ToCSV(), "\n")[1] != "123,test_name,5,456,," {
		t.Error("Postgres guild didn't serialize txtime to csv as expected")
	}
}

func TestGamesToCSV(t *testing.T) {
	games := []*PostgresGame{
		nil,
		nil,
		nil,
	}
	if len(strings.Split(GamesToCSV(games), "\n")) > 2 {
		t.Error("Expected only 1 line of CSV when provided with nil game ptrs")
	}

	games[0] = &PostgresGame{
		GameID:      0,
		GuildID:     1,
		ConnectCode: "a",
		StartTime:   2,
		WinType:     3,
		EndTime:     4,
	}
	if strings.Split(GamesToCSV(games), "\n")[1] != "0,1,a,2,3,4," {
		t.Error("Games to CSV didn't match expected value")
	}
}

func TestEventsToCSV(t *testing.T) {
	events := []*PostgresGameEvent{
		nil,
		nil,
		nil,
	}
	if len(strings.Split(EventsToCSV(events), "\n")) > 2 {
		t.Error("Expected only 1 line of CSV when provided with nil event ptrs")
	}

	events[0] = &PostgresGameEvent{
		EventID:   0,
		UserID:    nil,
		GameID:    1,
		EventTime: 2,
		EventType: 3,
		Payload:   "some_payload",
	}
	if strings.Split(EventsToCSV(events), "\n")[1] != "0,,1,2,3,some_payload," {
		t.Error("Events to CSV didn't match expected value")
	}
}
