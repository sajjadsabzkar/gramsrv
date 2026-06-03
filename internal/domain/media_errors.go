package domain

import "errors"

// 媒体 / 文件相关业务错误。rpc 层据此映射为对应 rpc_error（见 internal/rpc/errors.go）。
var (
	ErrFilePartInvalid  = errors.New("file part invalid")
	ErrFilePartsInvalid = errors.New("file parts invalid")
	ErrFilePartTooBig   = errors.New("file part too big")
	ErrFileReference    = errors.New("file reference invalid")
	ErrMediaInvalid     = errors.New("media invalid")
	ErrMediaEmpty       = errors.New("media empty")
	ErrPhotoInvalid     = errors.New("photo invalid")
	ErrStickersetInvalid = errors.New("stickerset invalid")
	ErrDocumentInvalid  = errors.New("document invalid")
)
