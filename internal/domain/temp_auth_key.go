package domain

// TempAuthKeyBinding 是 auth.bindTempAuthKey 的持久化记录。
type TempAuthKeyBinding struct {
	TempAuthKeyID    [8]byte
	PermAuthKeyID    int64
	Nonce            int64
	TempSessionID    int64
	ExpiresAt        int
	EncryptedMessage []byte
}
