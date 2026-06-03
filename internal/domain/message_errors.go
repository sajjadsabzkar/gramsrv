package domain

import "errors"

var (
	ErrMessageIDInvalid       = errors.New("message id invalid")
	ErrMessageAuthorRequired  = errors.New("message author required")
	ErrMessageNotModified     = errors.New("message not modified")
	ErrMessageNotReadYet      = errors.New("message not read yet")
	ErrReplyMessageIDInvalid  = errors.New("reply message id invalid")
	ErrChatForwardsRestricted = errors.New("chat forwards restricted")
)
