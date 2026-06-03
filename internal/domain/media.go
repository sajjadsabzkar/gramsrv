package domain

// 本文件定义媒体相关的业务值对象（文档、照片、贴纸集、可用 reaction、消息媒体）。
// 这些类型完全不依赖 tg.*；rpc 层负责 domain↔tg 转换。
//
// 字段带 json tag 是为了 store 层可直接 json.Marshal 落 JSONB（消息 media 快照、
// 文档/照片元数据）。它们是协议无关的纯数据，不是 tg 生成类型。

// MediaBackend 标识 blob 字节实际存放后端。第一阶段只有本地磁盘。
type MediaBackend string

const (
	// MediaBackendLocalFS 表示 blob 字节存在本地磁盘（object_key 为相对路径）。
	MediaBackendLocalFS MediaBackend = "localfs"
)

// FileBlob 是一个可下载的二进制对象的索引项：location_key → 后端/对象键/大小/mime。
// 真正的字节由 blob backend 按 ObjectKey 读写；本结构只描述定位与元数据。
type FileBlob struct {
	// LocationKey 是稳定的逻辑定位键，由 getFile 的 InputFileLocation 推导：
	//   doc:<id>            文档主体
	//   doc:<id>:<type>     文档缩略图（PhotoSize type）
	//   photo:<id>:<type>   照片某尺寸
	LocationKey string       `json:"location_key"`
	Backend     MediaBackend `json:"backend"`
	ObjectKey   string       `json:"object_key"`
	Size        int64        `json:"size"`
	SHA256      []byte       `json:"sha256,omitempty"`
	MimeType    string       `json:"mime_type,omitempty"`
}

// UploadPart 是 upload.saveFilePart/saveBigFilePart 累积的一个分片（落 PG，组装后清理）。
type UploadPart struct {
	OwnerUserID int64
	FileID      int64
	Part        int
	TotalParts  int // big file 已知总数；small file 为 0
	Big         bool
	Bytes       []byte
}

// UploadedFileRef 引用一个客户端已通过 upload.saveFilePart(Big) 上传完毕的文件。
// rpc 层从 tg.InputFile/InputFileBig 转换得到；files 服务据此组装 blob。
type UploadedFileRef struct {
	OwnerUserID int64
	FileID      int64
	Parts       int
	Name        string
	Big         bool
	MD5         string // small file 客户端 md5_checksum（hex），可校验；big file 为空
}

// DocumentSpec 描述从上传文件创建 Document 的元数据（来自 InputMediaUploadedDocument）。
type DocumentSpec struct {
	MimeType   string
	Attributes []DocumentAttribute
	Thumb      *UploadedFileRef // 可选缩略图上传，生成 doc:<id>:m
	ForceFile  bool
}

// FileDownloadRequest 是 upload.getFile 解析后的下载请求；
// LocationKey 由 rpc 层从 tg.InputFileLocation 推导（doc:<id> / photo:<id>:<type> 等）。
type FileDownloadRequest struct {
	LocationKey string
	Offset      int64
	Limit       int
}

// FileChunk 是 upload.getFile 返回的一段内容。
type FileChunk struct {
	Bytes    []byte
	MimeType string
	Total    int64
}

// PhotoSizeKind 标识 PhotoSize 的 TL 变体。
type PhotoSizeKind string

const (
	// PhotoSizeKindDefault → photoSize（可下载，type/w/h/size）。
	PhotoSizeKindDefault PhotoSizeKind = "size"
	// PhotoSizeKindStripped → photoStrippedSize（内联字节，秒开模糊图）。
	PhotoSizeKindStripped PhotoSizeKind = "stripped"
	// PhotoSizeKindCached → photoCachedSize（内联字节 + w/h）。
	PhotoSizeKindCached PhotoSizeKind = "cached"
	// PhotoSizeKindPath → photoPathSize（内联 svg path 字节，矢量占位）。
	PhotoSizeKindPath PhotoSizeKind = "path"
	// PhotoSizeKindProgressive → photoSizeProgressive（渐进式 jpeg 多段大小）。
	PhotoSizeKindProgressive PhotoSizeKind = "progressive"
)

// PhotoSize 描述照片/缩略图的一种渲染尺寸。
type PhotoSize struct {
	Kind  PhotoSizeKind `json:"kind"`
	Type  string        `json:"type"`
	W     int           `json:"w,omitempty"`
	H     int           `json:"h,omitempty"`
	Size  int           `json:"size,omitempty"`
	Bytes []byte        `json:"bytes,omitempty"` // stripped/cached/path 内联内容
	Sizes []int         `json:"sizes,omitempty"` // progressive
}

// Downloadable 表示该尺寸需要客户端通过 upload.getFile 拉取（而非内联字节）。
func (s PhotoSize) Downloadable() bool {
	return s.Kind == PhotoSizeKindDefault || s.Kind == PhotoSizeKindProgressive
}

// DocumentAttributeKind 标识 TL DocumentAttribute 变体。
type DocumentAttributeKind string

const (
	DocAttrImageSize   DocumentAttributeKind = "image_size"
	DocAttrAnimated    DocumentAttributeKind = "animated"
	DocAttrSticker     DocumentAttributeKind = "sticker"
	DocAttrVideo       DocumentAttributeKind = "video"
	DocAttrAudio       DocumentAttributeKind = "audio"
	DocAttrFilename    DocumentAttributeKind = "filename"
	DocAttrCustomEmoji DocumentAttributeKind = "custom_emoji"
)

// DocumentAttribute 是主路径用到的 TL DocumentAttribute 变体的并集。
type DocumentAttribute struct {
	Kind DocumentAttributeKind `json:"kind"`

	// image_size / video / sticker box
	W int `json:"w,omitempty"`
	H int `json:"h,omitempty"`

	// sticker / custom_emoji
	Alt                  string `json:"alt,omitempty"`
	Mask                 bool   `json:"mask,omitempty"`
	StickerSetID         int64  `json:"sticker_set_id,omitempty"`
	StickerSetAccessHash int64  `json:"sticker_set_access_hash,omitempty"`
	Free                 bool   `json:"free,omitempty"`       // custom_emoji
	TextColor            bool   `json:"text_color,omitempty"` // custom_emoji

	// video
	Duration          float64 `json:"duration,omitempty"`
	RoundMessage      bool    `json:"round_message,omitempty"`
	SupportsStreaming bool    `json:"supports_streaming,omitempty"`

	// audio
	AudioDuration int    `json:"audio_duration,omitempty"`
	Voice         bool   `json:"voice,omitempty"`
	Title         string `json:"title,omitempty"`
	Performer     string `json:"performer,omitempty"`
	Waveform      []byte `json:"waveform,omitempty"`

	// filename
	FileName string `json:"file_name,omitempty"`
}

// Document 是已存储的 Telegram 文档（贴纸、gif、文件、视频、音频、自定义 emoji……）。
type Document struct {
	ID            int64               `json:"id"`
	AccessHash    int64               `json:"access_hash"`
	FileReference []byte              `json:"file_reference,omitempty"`
	Date          int                 `json:"date,omitempty"`
	MimeType      string              `json:"mime_type,omitempty"`
	Size          int64               `json:"size,omitempty"`
	DCID          int                 `json:"dc_id,omitempty"`
	Attributes    []DocumentAttribute `json:"attributes,omitempty"`
	Thumbs        []PhotoSize         `json:"thumbs,omitempty"`
}

// StickerSetRef 返回该文档归属的贴纸集引用（若有 sticker/custom_emoji 属性）。
func (d Document) StickerSetRef() (id, accessHash int64, ok bool) {
	for _, attr := range d.Attributes {
		if attr.Kind == DocAttrSticker || attr.Kind == DocAttrCustomEmoji {
			if attr.StickerSetID != 0 {
				return attr.StickerSetID, attr.StickerSetAccessHash, true
			}
		}
	}
	return 0, 0, false
}

// Photo 是已存储的 Telegram 照片（头像或图片消息）。
type Photo struct {
	ID            int64       `json:"id"`
	AccessHash    int64       `json:"access_hash"`
	FileReference []byte      `json:"file_reference,omitempty"`
	Date          int         `json:"date,omitempty"`
	DCID          int         `json:"dc_id,omitempty"`
	HasStickers   bool        `json:"has_stickers,omitempty"`
	Sizes         []PhotoSize `json:"sizes,omitempty"`
}

// MessageMediaKind 枚举消息可挂载的媒体载荷。
type MessageMediaKind string

const (
	MessageMediaKindNone     MessageMediaKind = ""
	MessageMediaKindPhoto    MessageMediaKind = "photo"
	MessageMediaKindDocument MessageMediaKind = "document"
)

// MessageMedia 是一条消息媒体载荷的业务表示（落库为消息行上的 JSONB 快照）。
type MessageMedia struct {
	Kind       MessageMediaKind `json:"kind"`
	Photo      *Photo           `json:"photo,omitempty"`
	Document   *Document        `json:"document,omitempty"`
	Spoiler    bool             `json:"spoiler,omitempty"`
	TTLSeconds int              `json:"ttl_seconds,omitempty"`
	Nopremium  bool             `json:"nopremium,omitempty"`
	Voice      bool             `json:"voice,omitempty"`
	Round      bool             `json:"round,omitempty"`
	Video      bool             `json:"video,omitempty"`
}

// IsZero 表示无媒体（用于落库时跳过空快照、转换时回退 MessageMediaEmpty）。
func (m *MessageMedia) IsZero() bool {
	return m == nil || m.Kind == MessageMediaKindNone
}

// StickerPack 是 emoji→文档 id 的映射条目（messages.stickerSet.packs）。
type StickerPack struct {
	Emoticon    string  `json:"emoticon"`
	DocumentIDs []int64 `json:"document_ids"`
}

// StickerSetKind 区分贴纸集用途（影响 getAllStickers / getEmojiStickers 归类）。
type StickerSetKind string

const (
	StickerSetKindStickers StickerSetKind = "stickers"
	StickerSetKindEmoji    StickerSetKind = "emoji"
	StickerSetKindMasks    StickerSetKind = "masks"
	// StickerSetKindSystem 是 TDesktop 通过 InputStickerSetDice/AnimatedEmoji 等系统集请求的内置集。
	StickerSetKindSystem StickerSetKind = "system"
)

// StickerSet 是贴纸/自定义 emoji 集的元数据 + 有序文档 id。
type StickerSet struct {
	ID              int64          `json:"id"`
	AccessHash      int64          `json:"access_hash"`
	ShortName       string         `json:"short_name"`
	Title           string         `json:"title"`
	Count           int            `json:"count"`
	Hash            int            `json:"hash"`
	Kind            StickerSetKind `json:"set_kind"`
	Official        bool           `json:"official,omitempty"`
	Animated        bool           `json:"animated,omitempty"`
	Videos          bool           `json:"videos,omitempty"`
	Emojis          bool           `json:"emojis,omitempty"`
	Masks           bool           `json:"masks,omitempty"`
	Installed       bool           `json:"installed,omitempty"`
	Archived        bool           `json:"archived,omitempty"`
	InstalledDate   int            `json:"installed_date,omitempty"`
	ThumbDocumentID int64          `json:"thumb_document_id,omitempty"`
	Thumbs          []PhotoSize    `json:"thumbs,omitempty"`
	ThumbDCID       int            `json:"thumb_dc_id,omitempty"`
	ThumbVersion    int            `json:"thumb_version,omitempty"`
	DocumentIDs     []int64        `json:"document_ids,omitempty"`
	Packs           []StickerPack  `json:"packs,omitempty"`
	SortOrder       int            `json:"sort_order,omitempty"`
	// SystemKey 是 TDesktop 系统集的稳定标识（如 "animated_emoji"、"dice:🎲"），用于 InputStickerSet* 路由。
	SystemKey string `json:"system_key,omitempty"`
}

// ProfilePhotoRef 是渲染头像所需的最小信息（当前 profile photo）。
type ProfilePhotoRef struct {
	PhotoID  int64
	DCID     int
	Stripped []byte // photoStrippedSize 内联缩略图，可空
}

// StrippedFromSizes 从照片尺寸列表里取出 stripped 缩略图字节（用于 UserProfilePhoto/ChatPhoto 占位）。
func StrippedFromSizes(sizes []PhotoSize) []byte {
	for _, s := range sizes {
		if s.Kind == PhotoSizeKindStripped {
			return s.Bytes
		}
	}
	return nil
}

// StickerSetRefKind 标识 InputStickerSet 的解析方式。
type StickerSetRefKind string

const (
	StickerSetRefByID        StickerSetRefKind = "id"
	StickerSetRefByShortName StickerSetRefKind = "short_name"
	StickerSetRefBySystem    StickerSetRefKind = "system"
)

// StickerSetRef 是 rpc 层从 tg.InputStickerSet 转换得到的贴纸集引用。
type StickerSetRef struct {
	Kind       StickerSetRefKind
	ID         int64
	AccessHash int64
	ShortName  string
	SystemKey  string
}

// AvailableReaction 描述 messages.getAvailableReactions 的一项（真实资源由文档 id 引用）。
type AvailableReaction struct {
	Reaction            string `json:"reaction"`
	Title               string `json:"title"`
	Inactive            bool   `json:"inactive,omitempty"`
	Premium             bool   `json:"premium,omitempty"`
	StaticIconID        int64  `json:"static_icon_id,omitempty"`
	AppearAnimationID   int64  `json:"appear_animation_id,omitempty"`
	SelectAnimationID   int64  `json:"select_animation_id,omitempty"`
	ActivateAnimationID int64  `json:"activate_animation_id,omitempty"`
	EffectAnimationID   int64  `json:"effect_animation_id,omitempty"`
	AroundAnimationID   int64  `json:"around_animation_id,omitempty"`
	CenterIconID        int64  `json:"center_icon_id,omitempty"`
	Order               int    `json:"order,omitempty"`
}

// DocumentIDs 收集该 reaction 引用的全部文档 id（去零去重，便于批量加载）。
func (r AvailableReaction) DocumentIDs() []int64 {
	raw := []int64{
		r.StaticIconID, r.AppearAnimationID, r.SelectAnimationID,
		r.ActivateAnimationID, r.EffectAnimationID, r.AroundAnimationID, r.CenterIconID,
	}
	out := make([]int64, 0, len(raw))
	seen := make(map[int64]struct{}, len(raw))
	for _, id := range raw {
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
