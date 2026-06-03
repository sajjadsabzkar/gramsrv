package domain

import "errors"

var (
	ErrUsernameInvalid     = errors.New("username invalid")
	ErrUsernameOccupied    = errors.New("username occupied")
	ErrUsernameNotOccupied = errors.New("username not occupied")
	ErrPhoneNotOccupied    = errors.New("phone not occupied")
	ErrFirstNameInvalid    = errors.New("first name invalid")
	ErrAboutTooLong        = errors.New("about too long")
)
