package auth

import (
	"context"
	"crypto/aes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gotd/ige"
	"github.com/gotd/td/bin"
	mtcrypto "github.com/gotd/td/crypto"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// 登录错误。
var (
	ErrCodeExpired             = errors.New("phone code expired or not found")
	ErrCodeInvalid             = errors.New("phone code invalid")
	ErrEncryptedMessageInvalid = errors.New("encrypted message invalid")
)

// Service 实现登录/注册业务。第一阶段为开发固定验证码（不真实下发短信）。
type Service struct {
	users     store.UserStore
	auths     store.AuthorizationStore
	codes     store.CodeStore
	authKeys  store.AuthKeyStore
	tempKeys  store.TempAuthKeyBindingStore
	messages  store.MessageStore
	dialogs   store.DialogStore
	fixedCode string
	codeTTL   time.Duration
}

// Option 调整登录服务的可选依赖。
type Option func(*Service)

// WithLoginMessages 在登录成功后写入官方系统账号的登录消息与会话摘要。
func WithLoginMessages(messages store.MessageStore, dialogs store.DialogStore) Option {
	return func(s *Service) {
		s.messages = messages
		s.dialogs = dialogs
	}
}

// NewService 创建登录服务。fixedCode 为开发固定验证码。
func NewService(users store.UserStore, auths store.AuthorizationStore, codes store.CodeStore, authKeys store.AuthKeyStore, tempKeys store.TempAuthKeyBindingStore, fixedCode string, opts ...Option) *Service {
	s := &Service{users: users, auths: auths, codes: codes, authKeys: authKeys, tempKeys: tempKeys, fixedCode: fixedCode, codeTTL: 5 * time.Minute}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BindTempAuthKey 校验并记录 TDesktop PFS temp→perm auth key 绑定。
func (s *Service) BindTempAuthKey(ctx context.Context, sessionID int64, binding domain.TempAuthKeyBinding) error {
	if s.authKeys != nil {
		inner, err := s.validateBindTempAuthKey(ctx, sessionID, binding)
		if err != nil {
			return err
		}
		binding.TempSessionID = inner.TempSessionID
	}
	if s.tempKeys == nil {
		return nil
	}
	return s.tempKeys.Save(ctx, binding)
}

// ResolveAuthKey 将已绑定的 temp auth_key 解析为对应 perm auth_key。
func (s *Service) ResolveAuthKey(ctx context.Context, authKeyID [8]byte) ([8]byte, bool, error) {
	if s == nil || s.tempKeys == nil {
		return [8]byte{}, false, nil
	}
	binding, found, err := s.tempKeys.GetByTemp(ctx, authKeyID)
	if err != nil || !found {
		return [8]byte{}, found, err
	}
	if binding.ExpiresAt <= int(time.Now().Unix()) {
		return [8]byte{}, false, nil
	}
	return authKeyIDFromInt64(binding.PermAuthKeyID), true, nil
}

// UserID 返回 auth_key 当前绑定的用户。未登录时 found=false。
func (s *Service) UserID(ctx context.Context, authKeyID [8]byte) (int64, bool, error) {
	if s == nil || s.auths == nil {
		return 0, false, nil
	}
	a, found, err := s.auths.ByAuthKey(ctx, authKeyID)
	if err != nil || !found {
		return 0, found, err
	}
	return a.UserID, true, nil
}

// SendCode 为 phone 生成 phone_code_hash，暂存（开发）固定验证码，返回 hash。
func (s *Service) SendCode(ctx context.Context, phone string) (string, error) {
	hash, err := randomHex(8)
	if err != nil {
		return "", err
	}
	if err := s.codes.Set(ctx, hash, store.PhoneCode{Phone: normalizePhone(phone), Code: s.fixedCode}, s.codeTTL); err != nil {
		return "", fmt.Errorf("store code: %w", err)
	}
	return hash, nil
}

// SignIn 校验验证码并尝试登录。
// needSignUp=true 表示验证码正确但用户不存在，调用方应引导注册（此时不删验证码，留给 SignUp）。
func (s *Service) SignIn(ctx context.Context, auth domain.Authorization, phone, phoneCodeHash, code string) (u domain.User, loginMessage domain.Message, needSignUp bool, err error) {
	phone = normalizePhone(phone)
	rec, found, err := s.codes.Get(ctx, phoneCodeHash)
	if err != nil {
		return domain.User{}, domain.Message{}, false, err
	}
	if !found {
		return domain.User{}, domain.Message{}, false, ErrCodeExpired
	}
	if rec.Phone != phone || rec.Code != code {
		return domain.User{}, domain.Message{}, false, ErrCodeInvalid
	}

	existing, found, err := s.users.ByPhone(ctx, phone)
	if err != nil {
		return domain.User{}, domain.Message{}, false, err
	}
	if !found {
		return domain.User{}, domain.Message{}, true, nil // 验证码对、但需注册
	}
	if err := s.bind(ctx, auth, existing.ID); err != nil {
		return domain.User{}, domain.Message{}, false, err
	}
	loginMessage, err = s.recordLoginMessage(ctx, existing.ID, rec.Code)
	if err != nil {
		return domain.User{}, domain.Message{}, false, err
	}
	_ = s.codes.Del(ctx, phoneCodeHash)
	return existing, loginMessage, false, nil
}

// SignUp 在 SignIn 判定需注册后创建用户并绑定授权。
// signUp 的 TL 请求不带验证码，这里校验 phone_code_hash 仍有效且手机号匹配。
func (s *Service) SignUp(ctx context.Context, auth domain.Authorization, phone, phoneCodeHash, firstName, lastName string) (domain.User, domain.Message, error) {
	phone = normalizePhone(phone)
	firstName = strings.TrimSpace(firstName)
	lastName = strings.TrimSpace(lastName)
	if firstName == "" || utf8.RuneCountInString(firstName) > 64 || utf8.RuneCountInString(lastName) > 64 {
		return domain.User{}, domain.Message{}, domain.ErrFirstNameInvalid
	}
	rec, found, err := s.codes.Get(ctx, phoneCodeHash)
	if err != nil {
		return domain.User{}, domain.Message{}, err
	}
	if !found {
		return domain.User{}, domain.Message{}, ErrCodeExpired
	}
	if rec.Phone != phone {
		return domain.User{}, domain.Message{}, ErrCodeInvalid
	}

	accessHash, err := randomInt64()
	if err != nil {
		return domain.User{}, domain.Message{}, err
	}
	u, err := s.users.Create(ctx, domain.User{
		AccessHash: accessHash,
		Phone:      phone,
		FirstName:  firstName,
		LastName:   lastName,
	})
	if err != nil {
		return domain.User{}, domain.Message{}, err
	}
	if err := s.bind(ctx, auth, u.ID); err != nil {
		return domain.User{}, domain.Message{}, err
	}
	loginMessage, err := s.recordLoginMessage(ctx, u.ID, rec.Code)
	if err != nil {
		return domain.User{}, domain.Message{}, err
	}
	_ = s.codes.Del(ctx, phoneCodeHash)
	return u, loginMessage, nil
}

// LogOut 解绑当前 auth_key 的授权。
func (s *Service) LogOut(ctx context.Context, authKeyID [8]byte) error {
	return s.auths.Delete(ctx, authKeyID)
}

func (s *Service) bind(ctx context.Context, auth domain.Authorization, userID int64) error {
	auth.UserID = userID
	return s.auths.Bind(ctx, auth)
}

const loginMessageTpl = `Login code: %s. Do not give this code to anyone, even if they say they are from Telegram!

This code can be used to log in to your Telegram account. We never ask it for anything else.

If you didn't request this code by trying to log in on another device, simply ignore this message.`

func (s *Service) recordLoginMessage(ctx context.Context, userID int64, code string) (domain.Message, error) {
	if s.messages == nil || s.dialogs == nil {
		return domain.Message{}, nil
	}
	body := fmt.Sprintf(loginMessageTpl, code)
	codeOffset := len("Login code: ")
	msg, err := s.messages.Create(ctx, domain.Message{
		OwnerUserID: userID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: domain.OfficialSystemUserID},
		From:        domain.Peer{Type: domain.PeerTypeUser, ID: domain.OfficialSystemUserID},
		Date:        int(time.Now().Unix()),
		Body:        body,
		Entities: []domain.MessageEntity{
			{Type: domain.MessageEntityBold, Offset: 0, Length: len("Login code:")},
			{Type: domain.MessageEntityBold, Offset: codeOffset, Length: len(code)},
		},
	})
	if err != nil {
		return domain.Message{}, err
	}
	if err := s.dialogs.Upsert(ctx, userID, domain.Dialog{
		Peer:           msg.Peer,
		TopMessage:     msg.ID,
		TopMessageDate: msg.Date,
		UnreadCount:    1,
	}); err != nil {
		return domain.Message{}, err
	}
	return msg, nil
}

func (s *Service) validateBindTempAuthKey(ctx context.Context, sessionID int64, binding domain.TempAuthKeyBinding) (mtcrypto.BindAuthKeyInner, error) {
	if binding.ExpiresAt <= int(time.Now().Unix()) {
		return mtcrypto.BindAuthKeyInner{}, ErrEncryptedMessageInvalid
	}

	permID := authKeyIDFromInt64(binding.PermAuthKeyID)
	perm, found, err := s.authKeys.Get(ctx, permID)
	if err != nil {
		return mtcrypto.BindAuthKeyInner{}, err
	}
	if !found {
		return mtcrypto.BindAuthKeyInner{}, ErrEncryptedMessageInvalid
	}

	inner, err := decryptBindAuthKeyInner(perm, binding.EncryptedMessage)
	if err != nil {
		return mtcrypto.BindAuthKeyInner{}, ErrEncryptedMessageInvalid
	}
	if inner.Nonce != binding.Nonce ||
		inner.TempAuthKeyID != authKeyIDInt64(binding.TempAuthKeyID) ||
		inner.PermAuthKeyID != binding.PermAuthKeyID ||
		inner.TempSessionID != sessionID ||
		inner.ExpiresAt != binding.ExpiresAt {
		return mtcrypto.BindAuthKeyInner{}, ErrEncryptedMessageInvalid
	}
	return inner, nil
}

func decryptBindAuthKeyInner(perm store.AuthKeyData, encrypted []byte) (mtcrypto.BindAuthKeyInner, error) {
	var msg mtcrypto.EncryptedMessage
	if err := msg.Decode(&bin.Buffer{Buf: encrypted}); err != nil {
		return mtcrypto.BindAuthKeyInner{}, err
	}
	if msg.AuthKeyID != perm.ID || len(msg.EncryptedData) == 0 || len(msg.EncryptedData)%aes.BlockSize != 0 {
		return mtcrypto.BindAuthKeyInner{}, ErrEncryptedMessageInvalid
	}

	key, iv := mtcrypto.KeysV1(mtcrypto.Key(perm.Value), msg.MsgKey)
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return mtcrypto.BindAuthKeyInner{}, err
	}
	plaintext := make([]byte, len(msg.EncryptedData))
	ige.DecryptBlocks(block, iv[:], plaintext, msg.EncryptedData)

	const headerLen = 16 + 8 + 4 + 4
	if len(plaintext) < headerLen {
		return mtcrypto.BindAuthKeyInner{}, ErrEncryptedMessageInvalid
	}
	b := &bin.Buffer{Buf: plaintext}
	randomPrefix := make([]byte, 16)
	if err := b.ConsumeN(randomPrefix, len(randomPrefix)); err != nil {
		return mtcrypto.BindAuthKeyInner{}, err
	}
	if _, err := b.Long(); err != nil {
		return mtcrypto.BindAuthKeyInner{}, err
	}
	if _, err := b.Int32(); err != nil {
		return mtcrypto.BindAuthKeyInner{}, err
	}
	msgLen, err := b.Int32()
	if err != nil {
		return mtcrypto.BindAuthKeyInner{}, err
	}
	if msgLen <= 0 {
		return mtcrypto.BindAuthKeyInner{}, ErrEncryptedMessageInvalid
	}
	bodyEnd := headerLen + int(msgLen)
	if bodyEnd > len(plaintext) {
		return mtcrypto.BindAuthKeyInner{}, ErrEncryptedMessageInvalid
	}
	if msg.MsgKey != mtcrypto.MessageKeyV1(plaintext[:bodyEnd]) {
		return mtcrypto.BindAuthKeyInner{}, ErrEncryptedMessageInvalid
	}

	body := plaintext[headerLen:bodyEnd]
	var inner mtcrypto.BindAuthKeyInner
	if err := inner.Decode(&bin.Buffer{Buf: body}); err != nil {
		return mtcrypto.BindAuthKeyInner{}, err
	}
	return inner, nil
}

func authKeyIDFromInt64(v int64) [8]byte {
	var id [8]byte
	binary.LittleEndian.PutUint64(id[:], uint64(v))
	return id
}

func authKeyIDInt64(id [8]byte) int64 {
	return int64(binary.LittleEndian.Uint64(id[:]))
}

func normalizePhone(phone string) string {
	var b strings.Builder
	b.Grow(len(phone))
	for _, r := range phone {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return phone
	}
	return b.String()
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("rand: %w", err)
	}
	return hex.EncodeToString(b), nil
}

func randomInt64() (int64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, fmt.Errorf("rand: %w", err)
	}
	return int64(binary.LittleEndian.Uint64(b[:])), nil
}
