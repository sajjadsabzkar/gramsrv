package domain

// Authorization 是一条设备授权：auth_key 与 user 的绑定 + initConnection 设备信息。
// auth_key 是协议产物、授权是业务产物，故独立于 store.AuthKeyData。
type Authorization struct {
	AuthKeyID     [8]byte // 协议原生 auth_key_id；store 边界按小端转 int64
	UserID        int64
	Layer         int
	DeviceModel   string
	Platform      string
	SystemVersion string
	APIID         int
	AppVersion    string
	IP            string
}
