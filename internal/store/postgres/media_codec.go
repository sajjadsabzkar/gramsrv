package postgres

import (
	"encoding/json"

	"telesrv/internal/domain"
)

// 媒体相关 JSON 编解码：domain 值对象 ↔ JSONB 列。domain.* 带 json tag，可直接 marshal。

// jsonArrayOrEmpty 把切片序列化为 JSONB；nil 序列化为 "[]"（列为 NOT NULL DEFAULT '[]'）。
func jsonArrayOrEmpty(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if string(b) == "null" {
		return []byte("[]"), nil
	}
	return b, nil
}

// encodeMessageMedia 把消息媒体快照序列化为 JSONB；无媒体序列化为 "{}"。
func encodeMessageMedia(m *domain.MessageMedia) ([]byte, error) {
	if m.IsZero() {
		return []byte("{}"), nil
	}
	return json.Marshal(m)
}

// decodeMessageMedia 把消息行的 media JSONB 文本还原为 *MessageMedia；空载荷返回 nil。
func decodeMessageMedia(s string) (*domain.MessageMedia, error) {
	if s == "" || s == "{}" || s == "null" {
		return nil, nil
	}
	var m domain.MessageMedia
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, err
	}
	if m.IsZero() {
		return nil, nil
	}
	return &m, nil
}

func decodePhotoSizes(s string) ([]domain.PhotoSize, error) {
	if s == "" || s == "[]" || s == "null" {
		return nil, nil
	}
	var out []domain.PhotoSize
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeDocumentAttributes(s string) ([]domain.DocumentAttribute, error) {
	if s == "" || s == "[]" || s == "null" {
		return nil, nil
	}
	var out []domain.DocumentAttribute
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeInt64Slice(s string) ([]int64, error) {
	if s == "" || s == "[]" || s == "null" {
		return nil, nil
	}
	var out []int64
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeStickerPacks(s string) ([]domain.StickerPack, error) {
	if s == "" || s == "[]" || s == "null" {
		return nil, nil
	}
	var out []domain.StickerPack
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, err
	}
	return out, nil
}
