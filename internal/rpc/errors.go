package rpc

import (
	"errors"
	"fmt"

	"github.com/gotd/td/tgerr"

	"telesrv/internal/app/auth"
	"telesrv/internal/domain"
)

// 本文件集中构造所有 rpc_error：error_code 遵循 Telegram 约定（4xx 客户端 / 5xx 服务端），
// error_message 为 SCREAMING_SNAKE_CASE。统一用 tgerr.New，由它从 message 解析 Type/Argument
// （正确处理 FLOOD_WAIT_X 等带参数错误），避免手写 &tgerr.Error{} 时重复且易错。

// internalErr 是兜底的服务端错误。
func internalErr() error { return tgerr.New(500, "INTERNAL_SERVER_ERROR") }

// notImplementedErr 表示 RPC 未实现（落兼容矩阵），让客户端继续运行而非断连。
func notImplementedErr() error { return tgerr.New(500, "NOT_IMPLEMENTED") }

// wrapperTooDeepErr 表示 invokeWithLayer/initConnection 等 wrapper 嵌套过深。
func wrapperTooDeepErr() error { return tgerr.New(400, "WRAPPER_TOO_DEEP") }

// inputConstructorInvalidErr 表示客户端传入的 TL 构造器不在当前 RPC 接受范围内。
func inputConstructorInvalidErr() error { return tgerr.New(400, "INPUT_CONSTRUCTOR_INVALID") }

// folderIDInvalidErr 表示客户端传入多个 folder peer 或非法 folder。
func folderIDInvalidErr() error { return tgerr.New(400, "FOLDER_ID_INVALID") }

func filterIDInvalidErr() error { return tgerr.New(400, "FILTER_ID_INVALID") }

func filterTitleEmptyErr() error { return tgerr.New(400, "FILTER_TITLE_EMPTY") }

// peerIDInvalidErr 表示目标 peer 不存在或当前阶段不支持。
func peerIDInvalidErr() error { return tgerr.New(400, "PEER_ID_INVALID") }

func parentPeerInvalidErr() error { return tgerr.New(400, "PARENT_PEER_INVALID") }

func sendAsPeerInvalidErr() error { return tgerr.New(400, "SEND_AS_PEER_INVALID") }

func limitInvalidErr() error { return tgerr.New(400, "LIMIT_INVALID") }

func secondsInvalidErr() error { return tgerr.New(400, "SECONDS_INVALID") }

func ttlPeriodInvalidErr() error { return tgerr.New(400, "TTL_PERIOD_INVALID") }

func chatInvalidErr() error { return tgerr.New(500, "CHAT_INVALID") }

func addressInvalidErr() error { return tgerr.New(400, "ADDRESS_INVALID") }

func mediaInvalidErr() error { return tgerr.New(400, "MEDIA_INVALID") }

// 文件上传 / 下载相关错误。
func filePartInvalidErr() error      { return tgerr.New(400, "FILE_PART_INVALID") }
func filePartsInvalidErr() error     { return tgerr.New(400, "FILE_PARTS_INVALID") }
func filePartTooBigErr() error       { return tgerr.New(400, "FILE_PART_TOO_BIG") }
func fileReferenceInvalidErr() error { return tgerr.New(400, "FILE_REFERENCE_INVALID") }
func locationInvalidErr() error      { return tgerr.New(400, "LOCATION_INVALID") }
func fileIDInvalidErr() error        { return tgerr.New(400, "FILE_ID_INVALID") }

func mediaEmptyErr() error { return tgerr.New(400, "MEDIA_EMPTY") }

func photoInvalidErr() error { return tgerr.New(400, "PHOTO_INVALID") }

func stickersetInvalidErr() error { return tgerr.New(406, "STICKERSET_INVALID") }

func mediaCaptionTooLongErr() error { return tgerr.New(400, "MEDIA_CAPTION_TOO_LONG") }

func replyMarkupInvalidErr() error { return tgerr.New(400, "REPLY_MARKUP_INVALID") }

func shortcutInvalidErr() error { return tgerr.New(400, "SHORTCUT_INVALID") }

func effectIDInvalidErr() error { return tgerr.New(400, "EFFECT_ID_INVALID") }

func paymentUnsupportedErr() error { return tgerr.New(406, "PAYMENT_UNSUPPORTED") }

func balanceTooLowErr() error { return tgerr.New(400, "BALANCE_TOO_LOW") }

func starsAmountInvalidErr() error { return tgerr.New(400, "STARS_AMOUNT_INVALID") }

func suggestedPostPeerInvalidErr() error { return tgerr.New(400, "SUGGESTED_POST_PEER_INVALID") }

func storyIDInvalidErr() error { return tgerr.New(400, "STORY_ID_INVALID") }

func replyToMonoforumPeerInvalidErr() error { return tgerr.New(400, "REPLY_TO_MONOFORUM_PEER_INVALID") }

func optionsTooMuchErr() error { return tgerr.New(400, "OPTIONS_TOO_MUCH") }

func optionInvalidErr() error { return tgerr.New(400, "OPTION_INVALID") }

func pollOptionInvalidErr() error { return tgerr.New(400, "POLL_OPTION_INVALID") }

func pollAnswerInvalidErr() error { return tgerr.New(400, "POLL_ANSWER_INVALID") }

func reactionInvalidErr() error { return tgerr.New(400, "REACTION_INVALID") }

func todoItemsEmptyErr() error { return tgerr.New(400, "TODO_ITEMS_EMPTY") }

func todoNotModifiedErr() error { return tgerr.New(400, "TODO_NOT_MODIFIED") }

func searchQueryEmptyErr() error { return tgerr.New(400, "SEARCH_QUERY_EMPTY") }

func queryTooShortErr() error { return tgerr.New(400, "QUERY_TOO_SHORT") }

func usernameInvalidErr() error { return tgerr.New(400, "USERNAME_INVALID") }

func usernameOccupiedErr() error { return tgerr.New(400, "USERNAME_OCCUPIED") }

func usernameNotOccupiedErr() error { return tgerr.New(400, "USERNAME_NOT_OCCUPIED") }

func usernameNotModifiedErr() error { return tgerr.New(400, "USERNAME_NOT_MODIFIED") }

func phoneNotOccupiedErr() error { return tgerr.New(400, "PHONE_NOT_OCCUPIED") }

func userIDInvalidErr() error { return tgerr.New(400, "USER_ID_INVALID") }

func usersTooFewErr() error { return tgerr.New(400, "USERS_TOO_FEW") }

func firstNameInvalidErr() error { return tgerr.New(400, "FIRSTNAME_INVALID") }

func aboutTooLongErr() error { return tgerr.New(400, "ABOUT_TOO_LONG") }

func contactIDInvalidErr() error { return tgerr.New(400, "CONTACT_ID_INVALID") }

func contactNameEmptyErr() error { return tgerr.New(400, "CONTACT_NAME_EMPTY") }

// messageEmptyErr 表示发送空文本。
func messageEmptyErr() error { return tgerr.New(400, "MESSAGE_EMPTY") }

// messageTooLongErr 表示文本超出当前阶段限制。
func messageTooLongErr() error { return tgerr.New(400, "MESSAGE_TOO_LONG") }

func messageIDInvalidErr() error { return tgerr.New(400, "MESSAGE_ID_INVALID") }

func msgIDInvalidErr() error { return tgerr.New(400, "MSG_ID_INVALID") }

func messageAuthorRequiredErr() error { return tgerr.New(403, "MESSAGE_AUTHOR_REQUIRED") }

func messageNotModifiedErr() error { return tgerr.New(400, "MESSAGE_NOT_MODIFIED") }

func messageEditForbiddenErr() error { return tgerr.New(403, "EDIT_MESSAGES_FORBIDDEN") }

func messageDeleteForbiddenErr() error { return tgerr.New(403, "DELETE_MESSAGES_FORBIDDEN") }

func messageNotReadYetErr() error { return tgerr.New(400, "MESSAGE_NOT_READ_YET") }

func replyMessageIDInvalidErr() error { return tgerr.New(400, "REPLY_MESSAGE_ID_INVALID") }

func chatForwardsRestrictedErr() error { return tgerr.New(400, "CHAT_FORWARDS_RESTRICTED") }

func inputRequestInvalidErr() error { return tgerr.New(400, "INPUT_REQUEST_INVALID") }

func persistentTimestampInvalidErr() error { return tgerr.New(400, "PERSISTENT_TIMESTAMP_INVALID") }

func channelForumMissingErr() error { return tgerr.New(400, "CHANNEL_FORUM_MISSING") }

func topicTitleEmptyErr() error { return tgerr.New(400, "TOPIC_TITLE_EMPTY") }
func topicIDInvalidErr() error  { return tgerr.New(400, "TOPIC_ID_INVALID") }

// randomIDEmptyErr 表示发送消息缺少 random_id。
func randomIDEmptyErr() error { return tgerr.New(400, "RANDOM_ID_EMPTY") }

// scheduleDateInvalidErr 表示当前阶段不支持定时消息。
func scheduleDateInvalidErr() error { return tgerr.New(400, "SCHEDULE_DATE_INVALID") }

// floodWaitErr 表示触发写操作限流。
func floodWaitErr(seconds int) error {
	if seconds <= 0 {
		seconds = 1
	}
	return tgerr.New(420, fmt.Sprintf("FLOOD_WAIT_%d", seconds))
}

// signInErr 把登录业务错误映射为客户端可识别的 rpc_error。
func signInErr(err error) error {
	switch {
	case errors.Is(err, auth.ErrCodeInvalid):
		return tgerr.New(400, "PHONE_CODE_INVALID")
	case errors.Is(err, auth.ErrCodeExpired):
		return tgerr.New(400, "PHONE_CODE_EXPIRED")
	case errors.Is(err, domain.ErrFirstNameInvalid):
		return firstNameInvalidErr()
	default:
		return internalErr()
	}
}

// bindTempAuthKeyErr 映射 PFS temp auth key 绑定错误。
func bindTempAuthKeyErr(err error) error {
	switch {
	case errors.Is(err, auth.ErrEncryptedMessageInvalid):
		return tgerr.New(400, "ENCRYPTED_MESSAGE_INVALID")
	default:
		return internalErr()
	}
}
