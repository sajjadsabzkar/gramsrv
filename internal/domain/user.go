package domain

// UserIDSequenceBase 是普通用户 ID 的起始值。
//
// 取 2026-06-01 00:00:00 Asia/Shanghai 的 Unix 秒级时间戳。
// 777000 等兼容系统账号低于该区间，业务注册用户从这里开始递增。
const UserIDSequenceBase int64 = 1780243200

// User 是一个账号。第一阶段仅保留登录链路必须字段；
// access_hash 为任何 InputUser 校验所必须，不可省。
type User struct {
	ID          int64
	AccessHash  int64
	Phone       string
	FirstName   string
	LastName    string
	About       string
	Username    string
	CountryCode string
	Verified    bool
	Support     bool
	Contact     bool
	Mutual      bool
	// Profile photo：反范式存于 users 表，便于无 join 渲染头像。PhotoID==0 表示无头像。
	PhotoID       int64
	PhotoDCID     int
	PhotoStripped []byte
	LastSeenAt    int
	Status        UserStatus
}

// UserStatusKind is a protocol-neutral account presence state.
type UserStatusKind int

const (
	UserStatusUnknown UserStatusKind = iota
	UserStatusOnline
	UserStatusOffline
	UserStatusRecently
	UserStatusLastWeek
	UserStatusLastMonth
	UserStatusEmpty
)

// UserStatus describes the currently visible presence state for a user.
//
// Expires and WasOnline are absolute Unix timestamps in seconds, matching
// Telegram's UserStatus semantics without leaking tg.* into domain.
type UserStatus struct {
	Kind      UserStatusKind
	Expires   int
	WasOnline int
}

// UserProfileUpdate 描述 account.updateProfile 的可选字段更新。
type UserProfileUpdate struct {
	FirstName    string
	HasFirstName bool
	LastName     string
	HasLastName  bool
	About        string
	HasAbout     bool
}
