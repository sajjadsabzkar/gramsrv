package domain

const (
	// OfficialSystemUserID 是 Telegram 兼容客户端识别的官方系统账号。
	OfficialSystemUserID int64 = 777000
)

// OfficialSystemUser 返回第一阶段内置的官方系统账号。
func OfficialSystemUser() User {
	return User{
		ID:         OfficialSystemUserID,
		AccessHash: 6599886787491911851,
		Phone:      "42777",
		FirstName:  "Telegram",
		Username:   "telegram",
		Verified:   true,
		Support:    true,
	}
}
