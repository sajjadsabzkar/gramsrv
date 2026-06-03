package domain

import (
	"errors"
	"testing"
)

func TestValidateMessageReplyBoundsRejectsQuoteOffsetAsTextOffset(t *testing.T) {
	reply := &MessageReply{
		MessageID:   1,
		QuoteText:   "hello",
		QuoteOffset: MaxMessageReplyQuoteOffset + 1,
	}
	if err := ValidateMessageReplyBounds(reply); !errors.Is(err, ErrReplyMessageIDInvalid) {
		t.Fatalf("ValidateMessageReplyBounds err = %v, want ErrReplyMessageIDInvalid", err)
	}
}

func TestValidateMessageReplyBoundsAllowsForumTopicOnlyHeader(t *testing.T) {
	reply := &MessageReply{
		TopMessageID: 10,
		ForumTopic:   true,
	}
	if err := ValidateMessageReplyBounds(reply); err != nil {
		t.Fatalf("ValidateMessageReplyBounds err = %v, want nil", err)
	}
}
