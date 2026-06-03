package tdesktop

import (
	"crypto/sha256"
	"encoding/binary"
	"time"

	"github.com/gotd/td/tg"
)

const (
	appConfigHash     = 4
	countriesListHash = 1
	timezonesListHash = 1
)

// AppConfig returns the fallback TDesktop startup app config used when HelpService is absent.
func AppConfig(hash int) tg.HelpAppConfigClass {
	if hash == appConfigHash {
		return &tg.HelpAppConfigNotModified{}
	}
	return &tg.HelpAppConfig{
		Hash:   appConfigHash,
		Config: readMarkAppConfig(),
	}
}

func readMarkAppConfig() *tg.JSONObject {
	return &tg.JSONObject{Value: []tg.JSONObjectValue{
		{Key: "chat_read_mark_size_threshold", Value: &tg.JSONNumber{Value: 50}},
		{Key: "chat_read_mark_expire_period", Value: &tg.JSONNumber{Value: 604800}},
		{Key: "pm_read_date_expire_period", Value: &tg.JSONNumber{Value: 604800}},
		{Key: "quote_length_max", Value: &tg.JSONNumber{Value: 1024}},
		{Key: "telegram_antispam_group_size_min", Value: &tg.JSONNumber{Value: 200}},
		{Key: "telegram_antispam_user_id", Value: &tg.JSONString{Value: "5434988373"}},
	}}
}

// TimezonesList returns a small non-empty timezone set for TDesktop business settings preloading.
func TimezonesList(hash int) tg.HelpTimezonesListClass {
	if hash == timezonesListHash {
		return &tg.HelpTimezonesListNotModified{}
	}
	return &tg.HelpTimezonesList{
		Hash: timezonesListHash,
		Timezones: []tg.Timezone{
			{ID: "Etc/UTC", Name: "UTC", UtcOffset: 0},
			{ID: "America/New_York", Name: "Eastern Time", UtcOffset: -5 * 60 * 60},
			{ID: "America/Chicago", Name: "Central Time", UtcOffset: -6 * 60 * 60},
			{ID: "America/Denver", Name: "Mountain Time", UtcOffset: -7 * 60 * 60},
			{ID: "America/Los_Angeles", Name: "Pacific Time", UtcOffset: -8 * 60 * 60},
			{ID: "Asia/Shanghai", Name: "China Standard Time", UtcOffset: 8 * 60 * 60},
		},
	}
}

// CountriesList returns the fallback login country list used when HelpService is absent.
func CountriesList(hash int) tg.HelpCountriesListClass {
	if hash == countriesListHash {
		return &tg.HelpCountriesListNotModified{}
	}
	return &tg.HelpCountriesList{
		Hash: countriesListHash,
		Countries: []tg.HelpCountry{
			{
				ISO2:        "US",
				DefaultName: "United States",
				CountryCodes: []tg.HelpCountryCode{
					{CountryCode: "1", Prefixes: []string{"1"}},
				},
			},
			{
				ISO2:        "CN",
				DefaultName: "China",
				CountryCodes: []tg.HelpCountryCode{
					{CountryCode: "86", Prefixes: []string{"86"}},
				},
			},
		},
	}
}

// LoginToken returns a short-lived placeholder token for TDesktop's QR-login
// screen. First phase only supports phone login, but TDesktop expects this RPC
// to produce an auth.loginToken while the QR screen is visible.
func LoginToken(now time.Time, authKeyID [8]byte, sessionID int64) *tg.AuthLoginToken {
	var seed [len(authKeyID) + 8 + 8]byte
	copy(seed[:len(authKeyID)], authKeyID[:])
	binary.LittleEndian.PutUint64(seed[len(authKeyID):], uint64(sessionID))
	binary.LittleEndian.PutUint64(seed[len(authKeyID)+8:], uint64(now.UnixNano()))
	token := sha256.Sum256(seed[:])
	return &tg.AuthLoginToken{
		Expires: int(now.Add(30 * time.Second).Unix()),
		Token:   token[:],
	}
}
