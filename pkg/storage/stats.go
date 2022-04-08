package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/automuteus/utils/pkg/capture"
	"github.com/automuteus/utils/pkg/game"
	"github.com/automuteus/utils/pkg/settings"
	"github.com/bwmarrin/discordgo"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/nicksnyder/go-i18n/v2/i18n"
)

var DiscussCode = fmt.Sprintf("%d", game.DISCUSS)
var TasksCode = fmt.Sprintf("%d", game.TASKS)

type SimpleEventType int

const (
	Tasks SimpleEventType = iota
	Discuss
	PlayerDeath
	PlayerDisconnect
	PlayerExiled
)

type SimpleEvent struct {
	EventType       SimpleEventType
	EventTimeOffset time.Duration
	Data            string
}

type GameStatistics struct {
	GameStartTime   time.Time
	GameEndTime     time.Time
	GameDuration    time.Duration
	WinType         game.GameResult
	WinRole         game.GameRole
	WinPlayerNames  []string
	LosePlayerNames []string

	NumMeetings    int
	NumDeaths      int
	NumVotedOff    int
	NumDisconnects int
	Events         []SimpleEvent
}

func (stats *GameStatistics) FormatGameStatsDescription(sett *settings.GuildSettings) string {
	buf := bytes.NewBuffer([]byte{})
	switch stats.WinType {
	case game.HumansByTask:
		buf.WriteString(sett.LocalizeMessage(&i18n.Message{
			ID:    "responses.matchStatsEmbed.HumansByTask",
			Other: "**Crewmates** won by **completing tasks** !",
		}))
	case game.HumansByVote:
		buf.WriteString(sett.LocalizeMessage(&i18n.Message{
			ID:    "responses.matchStatsEmbed.HumansByVote",
			Other: "**Crewmates** won by **voting off the last Imposter** !",
		}))
	case game.HumansDisconnect:
		buf.WriteString(sett.LocalizeMessage(&i18n.Message{
			ID:    "responses.matchStatsEmbed.HumansDisconnect",
			Other: "**Crewmates** won because **the last Imposter disconnected** !",
		}))
	case game.ImpostorDisconnect:
		buf.WriteString(sett.LocalizeMessage(&i18n.Message{
			ID:    "responses.matchStatsEmbed.ImpostorDisconnect",
			Other: "**Imposters** won because **the last Crewmate disconnected** !",
		}))
	case game.ImpostorBySabotage:
		buf.WriteString(sett.LocalizeMessage(&i18n.Message{
			ID:    "responses.matchStatsEmbed.ImpostorBySabotage",
			Other: "**Imposters** won by **sabotage** !",
		}))
	case game.ImpostorByVote:
		buf.WriteString(sett.LocalizeMessage(&i18n.Message{
			ID:    "responses.matchStatsEmbed.ImpostorByVote",
			Other: "**Imposters** won by **voting off the last Crewmate** !",
		}))
	case game.ImpostorByKill:
		buf.WriteString(sett.LocalizeMessage(&i18n.Message{
			ID:    "responses.matchStatsEmbed.ImpostorByKill",
			Other: "**Imposters** won by **killing the last Crewmate** !",
		}))
	}
	return buf.String()
}

func (stats *GameStatistics) ToDiscordEmbed(combinedID string, sett *settings.GuildSettings) *discordgo.MessageEmbed {
	title := sett.LocalizeMessage(&i18n.Message{
		ID:    "responses.matchStatsEmbed.Title",
		Other: "Game `{{.MatchID}}`",
	}, map[string]interface{}{
		"MatchID": combinedID,
	})

	fields := make([]*discordgo.MessageEmbedField, 0)

	winRoleStr, loseRoleStr := "", ""
	switch stats.WinRole {
	case game.CrewmateRole:
		winRoleStr = sett.LocalizeMessage(&i18n.Message{
			ID:    "responses.matchStatsEmbed.Crewmates",
			Other: "Crewmates",
		})
		loseRoleStr = sett.LocalizeMessage(&i18n.Message{
			ID:    "responses.matchStatsEmbed.Imposters",
			Other: "Imposters",
		})
	case game.ImposterRole:
		winRoleStr = sett.LocalizeMessage(&i18n.Message{
			ID:    "responses.matchStatsEmbed.Imposters",
			Other: "Imposters",
		})
		loseRoleStr = sett.LocalizeMessage(&i18n.Message{
			ID:    "responses.matchStatsEmbed.Crewmates",
			Other: "Crewmates",
		})
	}

	if len(stats.WinPlayerNames) > 0 {
		fields = append(fields, &discordgo.MessageEmbedField{
			Name:   fmt.Sprintf("🏆 %s (%d)", winRoleStr, len(stats.WinPlayerNames)),
			Value:  fmt.Sprintf("%s", strings.Join(stats.WinPlayerNames, ", ")),
			Inline: false,
		})
	}
	if len(stats.LosePlayerNames) > 0 {
		fields = append(fields, &discordgo.MessageEmbedField{
			Name:   fmt.Sprintf("🤢 %s (%d)", loseRoleStr, len(stats.LosePlayerNames)),
			Value:  fmt.Sprintf("%s", strings.Join(stats.LosePlayerNames, ", ")),
			Inline: false,
		})
	}

	fields = append(fields, &discordgo.MessageEmbedField{
		Name: sett.LocalizeMessage(&i18n.Message{
			ID:    "responses.matchStatsEmbed.Start",
			Other: "🕑 Start",
		}),
		Value:  stats.GameStartTime.Add(time.Duration(sett.GetTimeOffset()*60) * time.Minute).Format("Jan 2, 3:04 PM"),
		Inline: true,
	})
	fields = append(fields, &discordgo.MessageEmbedField{
		Name: sett.LocalizeMessage(&i18n.Message{
			ID:    "responses.matchStatsEmbed.End",
			Other: "🕑 End",
		}),
		Value:  stats.GameEndTime.Add(time.Duration(sett.GetTimeOffset()*60) * time.Minute).Format("3:04 PM"),
		Inline: true,
	})
	fields = append(fields, &discordgo.MessageEmbedField{
		Name: sett.LocalizeMessage(&i18n.Message{
			ID:    "responses.matchStatsEmbed.Duration",
			Other: "⏲️ Duration",
		}),
		Value:  stats.GameDuration.String(),
		Inline: true,
	})
	fields = append(fields, &discordgo.MessageEmbedField{
		Name: sett.LocalizeMessage(&i18n.Message{
			ID:    "responses.matchStatsEmbed.Players",
			Other: "🎮 Players",
		}),
		Value:  fmt.Sprintf("%d", len(stats.WinPlayerNames)+len(stats.LosePlayerNames)),
		Inline: true,
	})
	fields = append(fields, &discordgo.MessageEmbedField{
		Name: sett.LocalizeMessage(&i18n.Message{
			ID:    "responses.matchStatsEmbed.Meetings",
			Other: "💬 Meetings",
		}),
		Value:  fmt.Sprintf("%d", stats.NumMeetings),
		Inline: true,
	})
	fields = append(fields, &discordgo.MessageEmbedField{
		Name: sett.LocalizeMessage(&i18n.Message{
			ID:    "responses.matchStatsEmbed.Death",
			Other: "☠️ Death",
		}),
		Value: sett.LocalizeMessage(&i18n.Message{
			ID:    "responses.matchStatsEmbed.DeathDesc",
			Other: "{{.Death}} ({{.Killed}} killed, {{.VotedOff}} exiled)",
		}, map[string]interface{}{
			"Death":    stats.NumDeaths,
			"Killed":   stats.NumDeaths - stats.NumVotedOff,
			"VotedOff": stats.NumVotedOff,
		}),
		Inline: true,
	})

	buf := bytes.NewBuffer([]byte{})
	for _, v := range stats.Events {
		switch {
		case v.EventType == Tasks:
			buf.WriteString(fmt.Sprintf("`%s` %s\n", formatTimeDuration(v.EventTimeOffset), sett.LocalizeMessage(&i18n.Message{
				ID:    "responses.matchStatsEmbed.TaskEvent",
				Other: "🔨 Task",
			})))
		case v.EventType == Discuss:
			buf.WriteString(fmt.Sprintf("`%s` %s\n", formatTimeDuration(v.EventTimeOffset), sett.LocalizeMessage(&i18n.Message{
				ID:    "responses.matchStatsEmbed.DiscussionEvent",
				Other: "💬 Discussion",
			})))
		case v.EventType == PlayerDeath:
			player := game.Player{}
			err := json.Unmarshal([]byte(v.Data), &player)
			if err != nil {
				log.Println(err)
			} else {
				buf.WriteString(fmt.Sprintf("`%s` %s\n", formatTimeDuration(v.EventTimeOffset), sett.LocalizeMessage(&i18n.Message{
					ID:    "responses.matchStatsEmbed.KilledEvent",
					Other: "🔪 **{{.PlayerName}}** Killed",
				}, map[string]interface{}{
					"PlayerName": player.Name,
				})))
			}
		case v.EventType == PlayerDisconnect:
			player := game.Player{}
			err := json.Unmarshal([]byte(v.Data), &player)
			if err != nil {
				log.Println(err)
			} else {
				buf.WriteString(fmt.Sprintf("`%s` %s\n", formatTimeDuration(v.EventTimeOffset), sett.LocalizeMessage(&i18n.Message{
					ID:    "responses.matchStatsEmbed.DisconnectedEvent",
					Other: "🔌 **{{.PlayerName}}** Disconnected",
				}, map[string]interface{}{
					"PlayerName": player.Name,
				})))
			}
		case v.EventType == PlayerExiled:
			player := game.Player{}
			err := json.Unmarshal([]byte(v.Data), &player)
			if err != nil {
				log.Println(err)
			} else {
				buf.WriteString(fmt.Sprintf("`%s` %s\n", formatTimeDuration(v.EventTimeOffset), sett.LocalizeMessage(&i18n.Message{
					ID:    "responses.matchStatsEmbed.ExiledEvent",
					Other: "⛔ **{{.PlayerName}}** Exiled",
				}, map[string]interface{}{
					"PlayerName": player.Name,
				})))
			}
		}
	}
	if len(stats.Events) > 0 {
		fields = append(fields, &discordgo.MessageEmbedField{
			Name: sett.LocalizeMessage(&i18n.Message{
				ID:    "responses.matchStatsEmbed.GameEvents",
				Other: "📋 Game Events",
			}),
			Value:  buf.String(),
			Inline: false,
		})
	}

	msg := discordgo.MessageEmbed{
		URL:         "",
		Type:        "",
		Title:       title,
		Description: stats.FormatGameStatsDescription(sett),
		Timestamp:   "",
		Color:       10181046, // PURPLE
		Footer:      nil,
		Image:       nil,
		Thumbnail:   nil,
		Video:       nil,
		Provider:    nil,
		Author:      nil,
		Fields:      fields,
	}
	return &msg
}

func StatsFromGameAndEvents(pgame *PostgresGame, events []*PostgresGameEvent, users []*PostgresUserGame) GameStatistics {
	stats := GameStatistics{
		GameStartTime:   time.Time{},
		GameEndTime:     time.Time{},
		GameDuration:    0,
		WinType:         game.Unknown,
		WinRole:         game.CrewmateRole,
		WinPlayerNames:  []string{},
		LosePlayerNames: []string{},
		NumMeetings:     0,
		NumDeaths:       0,
		Events:          []SimpleEvent{},
	}

	if pgame != nil {
		stats.GameStartTime = time.Unix(int64(pgame.StartTime), 0)
		stats.GameEndTime = time.Unix(int64(pgame.EndTime), 0)
		stats.GameDuration = time.Second * time.Duration(pgame.EndTime-pgame.StartTime)
		stats.WinType = game.GameResult(pgame.WinType)
	}

	if stats.WinType == game.ImpostorDisconnect || stats.WinType == game.ImpostorBySabotage || stats.WinType == game.ImpostorByVote || stats.WinType == game.ImpostorByKill {
		stats.WinRole = game.ImposterRole
	}

	for _, v := range users {
		if v.PlayerWon {
			stats.WinPlayerNames = append(stats.WinPlayerNames, v.PlayerName)
		} else {
			stats.LosePlayerNames = append(stats.LosePlayerNames, v.PlayerName)
		}
	}

	if len(events) < 2 {
		return stats
	}

	exiledPlayerNames := []string{}
	for _, v := range events {
		if v.EventType == int16(capture.State) {
			if v.Payload == DiscussCode {
				stats.NumMeetings++
				stats.Events = append(stats.Events, SimpleEvent{
					EventType:       Discuss,
					EventTimeOffset: time.Second * time.Duration(v.EventTime-pgame.StartTime),
					Data:            "",
				})
			} else if v.Payload == TasksCode {
				stats.Events = append(stats.Events, SimpleEvent{
					EventType:       Tasks,
					EventTimeOffset: time.Second * time.Duration(v.EventTime-pgame.StartTime),
					Data:            "",
				})
			}
		} else if v.EventType == int16(capture.Player) {
			player := game.Player{}
			err := json.Unmarshal([]byte(v.Payload), &player)
			if err != nil {
				log.Println(err)
			} else {
				switch {
				case player.Action == game.DIED:
					stats.NumDeaths++
					isExiled := false
					for _, exiledPlayerName := range exiledPlayerNames {
						if exiledPlayerName == player.Name {
							isExiled = true
						}
					}
					if !isExiled {
						stats.Events = append(stats.Events, SimpleEvent{
							EventType:       PlayerDeath,
							EventTimeOffset: time.Second * time.Duration(v.EventTime-pgame.StartTime),
							Data:            v.Payload,
						})
					}
				case player.Action == game.EXILED:
					stats.NumVotedOff++
					exiledPlayerNames = append(exiledPlayerNames, player.Name)
					stats.Events = append(stats.Events, SimpleEvent{
						EventType:       PlayerExiled,
						EventTimeOffset: time.Second * time.Duration(v.EventTime-pgame.StartTime),
						Data:            v.Payload,
					})
				case player.Action == game.DISCONNECTED:
					stats.NumDisconnects++
					stats.Events = append(stats.Events, SimpleEvent{
						EventType:       PlayerDisconnect,
						EventTimeOffset: time.Second * time.Duration(v.EventTime-pgame.StartTime),
						Data:            v.Payload,
					})
				}
			}
		}
	}

	return stats
}

func (psqlInterface *PsqlInterface) NumGamesPlayedOnGuild(guildID string) int64 {
	gid, _ := strconv.ParseInt(guildID, 10, 64)
	var r int64
	err := pgxscan.Get(context.Background(), psqlInterface.Pool, &r, "SELECT COUNT(*) FROM games WHERE guild_id=$1 AND end_time != -1;", gid)
	if err != nil {
		return -1
	}
	return r
}

func (psqlInterface *PsqlInterface) NumGamesWonAsRoleOnServer(guildID string, role game.GameRole) int64 {
	gid, _ := strconv.ParseInt(guildID, 10, 64)
	var r int64
	var err error
	if role == game.CrewmateRole {
		err = pgxscan.Get(context.Background(), psqlInterface.Pool, &r, "SELECT COUNT(*) FROM games WHERE guild_id=$1 AND (win_type=0 OR win_type=1 OR win_type=6)", gid)
	} else {
		err = pgxscan.Get(context.Background(), psqlInterface.Pool, &r, "SELECT COUNT(*) FROM games WHERE guild_id=$1 AND (win_type=2 OR win_type=3 OR win_type=4 OR win_type=5)", gid)
	}
	if err != nil {
		log.Println(err)
		return -1
	}
	return r
}

func (psqlInterface *PsqlInterface) NumGamesPlayedByUser(userID string) int64 {
	var r int64
	err := pgxscan.Get(context.Background(), psqlInterface.Pool, &r, "SELECT COUNT(*) FROM users_games WHERE user_id=$1;", userID)
	if err != nil {
		return -1
	}
	return r
}

func (psqlInterface *PsqlInterface) NumGuildsPlayedInByUser(userID string) int64 {
	var r int64
	err := pgxscan.Get(context.Background(), psqlInterface.Pool, &r, "SELECT COUNT(DISTINCT guild_id) FROM users_games WHERE user_id=$1;", userID)
	if err != nil {
		return -1
	}
	return r
}

func (psqlInterface *PsqlInterface) NumGamesPlayedByUserOnServer(userID, guildID string) int64 {
	var r int64
	gid, _ := strconv.ParseInt(guildID, 10, 64)
	err := pgxscan.Get(context.Background(), psqlInterface.Pool, &r, "SELECT COUNT(*) FROM users_games WHERE user_id=$1 AND guild_id=$2", userID, gid)
	if err != nil {
		return -1
	}
	return r
}

func (psqlInterface *PsqlInterface) NumWinsAsRoleOnServer(userID, guildID string, role int16) int64 {
	var r int64
	err := pgxscan.Get(context.Background(), psqlInterface.Pool, &r, "SELECT COUNT(*) FROM users_games WHERE user_id=$1 AND guild_id=$2 AND player_role=$3 AND player_won=true;", userID, guildID, role)
	if err != nil {
		return -1
	}
	return r
}

func (psqlInterface *PsqlInterface) NumWinsAsRole(userID string, role int16) int64 {
	var r int64
	err := pgxscan.Get(context.Background(), psqlInterface.Pool, &r, "SELECT COUNT(*) FROM users_games WHERE user_id=$1 AND player_role=$2 AND player_won=true;", userID, role)
	if err != nil {
		return -1
	}
	return r
}

func (psqlInterface *PsqlInterface) NumGamesAsRoleOnServer(userID, guildID string, role int16) int64 {
	var r int64
	err := pgxscan.Get(context.Background(), psqlInterface.Pool, &r, "SELECT COUNT(*) FROM users_games WHERE user_id=$1 AND guild_id=$2 AND player_role=$3;", userID, guildID, role)
	if err != nil {
		return -1
	}
	return r
}

func (psqlInterface *PsqlInterface) NumGamesAsRole(userID string, role int16) int64 {
	var r int64
	err := pgxscan.Get(context.Background(), psqlInterface.Pool, &r, "SELECT COUNT(*) FROM users_games WHERE user_id=$1 AND player_role=$2;", userID, role)
	if err != nil {
		return -1
	}
	return r
}

func (psqlInterface *PsqlInterface) NumWinsOnServer(userID, guildID string) int64 {
	var r int64
	err := pgxscan.Get(context.Background(), psqlInterface.Pool, &r, "SELECT COUNT(*) FROM users_games WHERE user_id=$1 AND guild_id=$2 AND player_won=true;", userID, guildID)
	if err != nil {
		return -1
	}
	return r
}

func (psqlInterface *PsqlInterface) NumWins(userID string) int64 {
	var r int64
	err := pgxscan.Get(context.Background(), psqlInterface.Pool, &r, "SELECT COUNT(*) FROM users_games WHERE user_id=$1 AND player_won=true;", userID)
	if err != nil {
		return -1
	}
	return r
}

func formatTimeDuration(d time.Duration) string {
	minute := d / time.Minute
	second := (d - minute*time.Minute) / time.Second
	return fmt.Sprintf("%02d:%02d", minute, second)
}

type Int16ModeCount struct {
	Count int64 `db:"count"`
	Mode  int16 `db:"mode"`
}
type Uint64ModeCount struct {
	Count int64  `db:"count"`
	Mode  uint64 `db:"mode"`
}

type StringModeCount struct {
	Count int64  `db:"count"`
	Mode  string `db:"mode"`
}

//func (psqlInterface *PsqlInterface) ColorRankingForPlayer(userID string) []*Int16ModeCount {
//	r := []*Int16ModeCount{}
//	err := pgxscan.Select(context.Background(), psqlInterface.Pool, &r, "SELECT count(*),mode() within GROUP (ORDER BY player_color) AS mode FROM users_games WHERE user_id=$1 GROUP BY player_color ORDER BY count desc;", userID)
//
//	if err != nil {
//		log.Println(err)
//	}
//	return r
//}
func (psqlInterface *PsqlInterface) ColorRankingForPlayerOnServer(userID, guildID string) []*Int16ModeCount {
	r := []*Int16ModeCount{}
	err := pgxscan.Select(context.Background(), psqlInterface.Pool, &r, "SELECT count(*),mode() within GROUP (ORDER BY player_color) AS mode FROM users_games WHERE user_id=$1 AND guild_id=$2 GROUP BY player_color ORDER BY count desc;", userID, guildID)

	if err != nil {
		log.Println(err)
	}
	return r
}

//func (psqlInterface *PsqlInterface) NamesRankingForPlayer(userID string) []*StringModeCount {
//	r := []*StringModeCount{}
//	err := pgxscan.Select(context.Background(), psqlInterface.Pool, &r, "SELECT count(*),mode() within GROUP (ORDER BY player_name) AS mode FROM users_games WHERE user_id=$1 GROUP BY player_name ORDER BY count desc;", userID)
//
//	if err != nil {
//		log.Println(err)
//	}
//	return r
//}

func (psqlInterface *PsqlInterface) NamesRankingForPlayerOnServer(userID, guildID string) []*StringModeCount {
	var r []*StringModeCount
	err := pgxscan.Select(context.Background(), psqlInterface.Pool, &r, "SELECT count(*),mode() within GROUP (ORDER BY player_name) AS mode FROM users_games WHERE user_id=$1 AND guild_id=$2 GROUP BY player_name ORDER BY count desc;", userID, guildID)

	if err != nil {
		log.Println(err)
	}
	return r
}

func (psqlInterface *PsqlInterface) TotalGamesRankingForServer(guildID uint64) []*Uint64ModeCount {
	var r []*Uint64ModeCount
	err := pgxscan.Select(context.Background(), psqlInterface.Pool, &r, "SELECT count(*),mode() within GROUP (ORDER BY user_id) AS mode FROM users_games WHERE guild_id=$1 GROUP BY user_id ORDER BY count desc;", guildID)

	if err != nil {
		log.Println(err)
	}
	return r
}

func (psqlInterface *PsqlInterface) OtherPlayersRankingForPlayerOnServer(userID, guildID string) []*PostgresOtherPlayerRanking {
	var r []*PostgresOtherPlayerRanking
	err := pgxscan.Select(context.Background(), psqlInterface.Pool, &r, "SELECT distinct B.user_id,"+
		"count(*) over (partition by B.user_id),"+
		"(count(*) over (partition by B.user_id)::decimal / (SELECT count(*) from users_games where user_id=$1 AND guild_id=$2))*100 as percent "+
		"FROM users_games A INNER JOIN users_games B ON A.game_id = B.game_id AND A.user_id != B.user_id "+
		"WHERE A.user_id=$1 AND A.guild_id=$2 "+
		"ORDER BY percent desc", userID, guildID)

	if err != nil {
		log.Println(err)
	}
	return r
}

func (psqlInterface *PsqlInterface) TotalWinRankingForServerByRole(guildID uint64, role int16) []*PostgresPlayerRanking {
	var r []*PostgresPlayerRanking
	err := pgxscan.Select(context.Background(), psqlInterface.Pool, &r, "SELECT DISTINCT user_id,"+
		"COUNT(user_id) FILTER ( WHERE player_won = TRUE ) AS win, "+
		// "COUNT(user_id) FILTER ( WHERE player_won = FALSE ) AS loss," +
		"COUNT(*) AS total, "+
		"(COUNT(user_id) FILTER ( WHERE player_won = TRUE )::decimal / COUNT(*)) * 100 AS win_rate "+
		// "(COUNT(user_id) FILTER ( WHERE player_won = FALSE )::decimal / COUNT(*)) * 100 AS loss_rate" +
		"FROM users_games "+
		"WHERE guild_id = $1 AND player_role = $2 "+
		"GROUP BY user_id "+
		"ORDER BY win_rate DESC", guildID, role)

	if err != nil {
		log.Println(err)
	}
	return r
}

func (psqlInterface *PsqlInterface) TotalWinRankingForServer(guildID uint64) []*PostgresPlayerRanking {
	var r []*PostgresPlayerRanking
	err := pgxscan.Select(context.Background(), psqlInterface.Pool, &r, "SELECT DISTINCT user_id,"+
		"COUNT(user_id) FILTER ( WHERE player_won = TRUE ) AS win, "+
		// "COUNT(user_id) FILTER ( WHERE player_won = FALSE ) AS loss," +
		"COUNT(*) AS total, "+
		"(COUNT(user_id) FILTER ( WHERE player_won = TRUE )::decimal / COUNT(*)) * 100 AS win_rate "+
		// "(COUNT(user_id) FILTER ( WHERE player_won = FALSE )::decimal / COUNT(*)) * 100 AS loss_rate" +
		"FROM users_games "+
		"WHERE guild_id = $1 "+
		"GROUP BY user_id "+
		"ORDER BY win_rate DESC", guildID)

	if err != nil {
		log.Println(err)
	}
	return r
}

func (psqlInterface *PsqlInterface) DeleteAllGamesForServer(guildID string) error {
	_, err := psqlInterface.Pool.Exec(context.Background(), "DELETE FROM games WHERE guild_id=$1", guildID)
	return err
}

func (psqlInterface *PsqlInterface) DeleteAllGamesForUser(userID string) error {
	_, err := psqlInterface.Pool.Exec(context.Background(), "DELETE FROM users_games WHERE user_id=$1", userID)
	return err
}

func (psqlInterface *PsqlInterface) BestTeammateByRole(userID, guildID string, role int16, leaderboardMin int) []*PostgresBestTeammatePlayerRanking {
	var r []*PostgresBestTeammatePlayerRanking
	err := pgxscan.Select(context.Background(), psqlInterface.Pool, &r, "SELECT DISTINCT users_games.user_id, "+
		"uG.user_id as teammate_id,"+
		"COUNT(users_games.player_won) as total, "+
		"COUNT(users_games.player_won) FILTER ( WHERE users_games.player_won = TRUE ) as win, "+
		"(COUNT(users_games.user_id) FILTER ( WHERE users_games.player_won = TRUE )::decimal / COUNT(*)) * 100 AS win_rate "+
		"FROM users_games "+
		"INNER JOIN users_games uG ON users_games.game_id = uG.game_id AND users_games.user_id <> uG.user_id "+
		"WHERE users_games.guild_id = $1 AND users_games.player_role = $2 AND uG.player_role = $2 AND users_games.user_id = $3 "+
		"GROUP BY users_games.user_id, uG.user_id "+
		"HAVING COUNT(users_games.player_won) >= $4 "+
		"ORDER BY win_rate DESC, win DESC, total DESC", guildID, role, userID, leaderboardMin)

	if err != nil {
		log.Println(err)
	}
	return r
}

func (psqlInterface *PsqlInterface) WorstTeammateByRole(userID, guildID string, role int16, leaderboardMin int) []*PostgresWorstTeammatePlayerRanking {
	var r []*PostgresWorstTeammatePlayerRanking
	err := pgxscan.Select(context.Background(), psqlInterface.Pool, &r, "SELECT DISTINCT users_games.user_id, "+
		"uG.user_id as teammate_id,"+
		"COUNT(users_games.player_won) as total, "+
		"COUNT(users_games.player_won) FILTER ( WHERE users_games.player_won = FALSE ) as loose, "+
		"(COUNT(users_games.user_id) FILTER ( WHERE users_games.player_won = FALSE )::decimal / COUNT(*)) * 100 AS loose_rate "+
		"FROM users_games "+
		"INNER JOIN users_games uG ON users_games.game_id = uG.game_id AND users_games.user_id <> uG.user_id "+
		"WHERE users_games.guild_id = $1 AND users_games.player_role = $2 AND uG.player_role = $2 AND users_games.user_id = $3 "+
		"GROUP BY users_games.user_id, uG.user_id "+
		"HAVING COUNT(users_games.player_won) >= $4 "+
		"ORDER BY loose_rate DESC, loose DESC, total DESC", guildID, role, userID, leaderboardMin)

	if err != nil {
		log.Println(err)
	}
	return r
}

func (psqlInterface *PsqlInterface) BestTeammateForServerByRole(guildID string, role int16, leaderboardMin int) []*PostgresBestTeammatePlayerRanking {
	var r []*PostgresBestTeammatePlayerRanking
	err := pgxscan.Select(context.Background(), psqlInterface.Pool, &r, "SELECT DISTINCT "+
		"CASE WHEN users_games.user_id > uG.user_id THEN users_games.user_id ELSE uG.user_id END, "+
		"CASE WHEN users_games.user_id > uG.user_id THEN uG.user_id ELSE users_games.user_id END as teammate_id, "+
		"COUNT(users_games.player_won) as total, "+
		"COUNT(users_games.player_won) FILTER ( WHERE users_games.player_won = TRUE ) as win, "+
		"(COUNT(users_games.user_id) FILTER ( WHERE users_games.player_won = TRUE )::decimal / COUNT(*)) * 100 AS win_rate "+
		"FROM users_games "+
		"INNER JOIN users_games uG ON users_games.game_id = uG.game_id AND users_games.user_id <> uG.user_id "+
		"WHERE users_games.guild_id = $1 AND users_games.player_role = $2 and uG.player_role = $2"+
		"GROUP BY users_games.user_id, uG.user_id "+
		"HAVING COUNT(users_games.player_won) >= $3 "+
		"ORDER BY win_rate DESC, win DESC, total DESC", guildID, role, leaderboardMin)

	if err != nil {
		log.Println(err)
	}
	return r
}

func (psqlInterface *PsqlInterface) WorstTeammateForServerByRole(guildID string, role int16, leaderboardMin int) []*PostgresWorstTeammatePlayerRanking {
	var r []*PostgresWorstTeammatePlayerRanking
	err := pgxscan.Select(context.Background(), psqlInterface.Pool, &r, "SELECT DISTINCT "+
		"CASE WHEN users_games.user_id > uG.user_id THEN users_games.user_id ELSE uG.user_id END, "+
		"CASE WHEN users_games.user_id > uG.user_id THEN uG.user_id ELSE users_games.user_id END as teammate_id,"+
		"COUNT(users_games.player_won) as total, "+
		"COUNT(users_games.player_won) FILTER ( WHERE users_games.player_won = FALSE ) as loose, "+
		"(COUNT(users_games.user_id) FILTER ( WHERE users_games.player_won = FALSE )::decimal / COUNT(*)) * 100 AS loose_rate "+
		"FROM users_games "+
		"INNER JOIN users_games uG ON users_games.game_id = uG.game_id AND users_games.user_id <> uG.user_id "+
		"WHERE users_games.guild_id = $1 AND users_games.player_role = $2 AND uG.player_role = $2"+
		"GROUP BY users_games.user_id, uG.user_id "+
		"HAVING COUNT(users_games.player_won) >= $3 "+
		"ORDER BY loose_rate DESC, loose DESC, total DESC", guildID, role, leaderboardMin)

	if err != nil {
		log.Println(err)
	}
	return r
}

func (psqlInterface *PsqlInterface) UserWinByActionAndRole(userdID, guildID string, action string, role int16) []*PostgresUserActionRanking {
	var r []*PostgresUserActionRanking
	err := pgxscan.Select(context.Background(), psqlInterface.Pool, &r, "SELECT users_games.user_id, "+
		"COUNT(ge.user_id) FILTER ( WHERE payload ->> 'Action' = $1 ) as total_action, "+
		"total_user.total as total, "+
		"total_user.win_rate as win_rate "+
		"FROM users_games "+
		"LEFT JOIN (SELECT user_id, guild_id, player_role, "+
		"COUNT(users_games.player_won) as total, "+
		"(COUNT(users_games.user_id) FILTER ( WHERE users_games.player_won = TRUE )::decimal / COUNT(*)) * 100 AS win_rate "+
		"FROM users_games "+
		"GROUP BY user_id, player_role, guild_id "+
		") total_user on total_user.user_id = users_games.user_id and users_games.player_role = total_user.player_role and users_games.guild_id = total_user.guild_id "+
		"LEFT JOIN game_events ge ON users_games.game_id = ge.game_id AND ge.user_id = users_games.user_id "+
		"WHERE users_games.user_id = $2 AND users_games.guild_id = $3 "+
		"AND users_games.player_role = $4 "+
		"GROUP BY users_games.user_id, total, win_rate "+
		"ORDER BY win_rate DESC, total DESC;", action, userdID, guildID, role)

	if err != nil {
		log.Println(err)
	}
	return r
}

func (psqlInterface *PsqlInterface) UserFrequentFirstTarget(userID, guildID string, action string, leaderboardSize int) []*PostgresUserMostFrequentFirstTargetRanking {
	var r []*PostgresUserMostFrequentFirstTargetRanking
	err := pgxscan.Select(context.Background(), psqlInterface.Pool, &r, "SELECT COUNT(*) AS total_death, "+
		"users_games.user_id, total, "+
		"COUNT(*)::decimal / total * 100 AS death_rate "+
		"FROM users_games "+
		"LEFT JOIN LATERAL (SELECT game_events.user_id "+
		"FROM game_events WHERE game_events.game_id = users_games.game_id AND payload ->> 'Action' = $1 "+
		"ORDER BY event_time FETCH FIRST 1 ROW ONLY ) AS ge ON TRUE "+
		"LEFT JOIN LATERAL (SELECT count(*) AS total "+
		"FROM users_games WHERE users_games.user_id = ge.user_id AND users_games.guild_id = $2 AND player_role = 0) AS TOTAL_GAME ON TRUE "+
		"WHERE users_games.guild_id = $2 AND users_games.user_id = ge.user_id AND users_games.user_id = $3"+
		"GROUP BY users_games.user_id, total  "+
		"ORDER BY total_death DESC "+
		"LIMIT $4;", action, guildID, userID, leaderboardSize)

	if err != nil {
		log.Println(err)
	}
	return r
}

func (psqlInterface *PsqlInterface) UserMostFrequentFirstTargetForServer(guildID string, action string, leaderboardSize int) []*PostgresUserMostFrequentFirstTargetRanking {
	var r []*PostgresUserMostFrequentFirstTargetRanking
	err := pgxscan.Select(context.Background(), psqlInterface.Pool, &r, "SELECT COUNT(*) AS total_death, "+
		"users_games.user_id, total, "+
		"COUNT(*)::decimal / total * 100 AS death_rate "+
		"FROM users_games "+
		"LEFT JOIN LATERAL (SELECT game_events.user_id "+
		"FROM game_events WHERE game_events.game_id = users_games.game_id AND payload ->> 'Action' = $1 "+
		"ORDER BY event_time FETCH FIRST 1 ROW ONLY ) AS ge ON TRUE "+
		"LEFT JOIN LATERAL (SELECT COUNT(*) AS total "+
		"FROM users_games WHERE users_games.user_id = ge.user_id AND users_games.guild_id = $2 AND player_role = 0) AS TOTAL_GAME ON TRUE "+
		"WHERE users_games.guild_id = $2 AND users_games.user_id = ge.user_id AND total > 3"+
		"GROUP BY users_games.user_id, total  "+
		"ORDER BY death_rate DESC, total_death DESC "+
		"LIMIT $3;", action, guildID, leaderboardSize)

	if err != nil {
		log.Println(err)
	}
	return r
}

func (psqlInterface *PsqlInterface) UserMostFrequentKilledBy(userID, guildID string) []*PostgresUserMostFrequentKilledByanking {
	var r []*PostgresUserMostFrequentKilledByanking
	err := pgxscan.Select(context.Background(), psqlInterface.Pool, &r, "SELECT users_games.user_id, "+
		"usG.user_id as teammate_id, "+
		"COUNT(ge.user_id) FILTER ( WHERE payload ->> 'Action' = $1 ) as total_death, "+
		"COUNT(usG.user_id) as encounter, (COUNT(ge.user_id) FILTER ( WHERE payload ->> 'Action' = $1 ))::decimal/count(usG.player_name) * 100 as death_rate "+
		"FROM users_games "+
		"LEFT JOIN users_games usG on users_games.game_id = usG.game_id and usG.player_role = $2 "+
		"LEFT JOIN (SELECT user_id, guild_id, player_role, COUNT(users_games.player_won) as total "+
		"FROM users_games "+
		"GROUP BY user_id, player_role, guild_id) total_user on total_user.user_id = users_games.user_id and users_games.player_role = total_user.player_role and users_games.guild_id = total_user.guild_id "+
		"LEFT JOIN game_events ge ON users_games.game_id = ge.game_id AND ge.user_id = $3 "+
		"WHERE users_games.guild_id = $4 AND users_games.user_id = $3 AND users_games.player_role = $5 "+
		"GROUP BY users_games.user_id, usG.user_id, users_games.user_id, total "+
		"ORDER BY death_rate DESC, total_death DESC, encounter DESC;", strconv.Itoa(int(game.DIED)), strconv.Itoa(int(game.ImposterRole)), userID, guildID, strconv.Itoa(int(game.CrewmateRole)))
	if err != nil {
		log.Println(err)
	}
	return r
}

func (psqlInterface *PsqlInterface) UserMostFrequentKilledByServer(guildID string) []*PostgresUserMostFrequentKilledByanking {
	var r []*PostgresUserMostFrequentKilledByanking
	err := pgxscan.Select(context.Background(), psqlInterface.Pool, &r, "SELECT users_games.user_id, "+
		"usG.user_id as teammate_id, "+
		"COUNT(ge.user_id) FILTER ( WHERE payload ->> 'Action' = $1 ) as total_death, "+
		"COUNT(usG.user_id) as encounter, (COUNT(ge.user_id) FILTER ( WHERE payload ->> 'Action' = $1 ))::decimal/count(usG.player_name) * 100 as death_rate "+
		"FROM users_games "+
		"INNER JOIN users_games usG on users_games.game_id = usG.game_id and usG.player_role = $2 "+
		"INNER JOIN (SELECT user_id, guild_id, player_role, COUNT(users_games.player_won) as total "+
		"FROM users_games "+
		"GROUP BY user_id, player_role, guild_id) total_user on total_user.user_id = users_games.user_id and users_games.player_role = total_user.player_role and users_games.guild_id = total_user.guild_id "+
		"INNER JOIN game_events ge ON users_games.game_id = ge.game_id AND ge.user_id = users_games.user_id "+
		"WHERE users_games.guild_id = $3 AND users_games.player_role = $4 "+
		"GROUP BY users_games.user_id, usG.user_id, users_games.user_id, total "+
		"ORDER BY death_rate DESC, total_death DESC, encounter DESC;", strconv.Itoa(int(game.DIED)), strconv.Itoa(int(game.ImposterRole)), guildID, strconv.Itoa(int(game.CrewmateRole)))
	if err != nil {
		log.Println(err)
	}
	return r
}
