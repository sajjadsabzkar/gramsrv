package tdesktop

import (
	"time"

	"github.com/gotd/td/tg"
)

// NotifySettings returns default per-peer notification settings for empty first-phase accounts.
func NotifySettings() *tg.PeerNotifySettings {
	settings := &tg.PeerNotifySettings{}
	settings.SetShowPreviews(true)
	settings.SetSilent(false)
	settings.SetMuteUntil(0)
	settings.SetIosSound(&tg.NotificationSoundDefault{})
	settings.SetAndroidSound(&tg.NotificationSoundDefault{})
	settings.SetOtherSound(&tg.NotificationSoundDefault{})
	settings.SetStoriesMuted(false)
	settings.SetStoriesHideSender(false)
	settings.SetStoriesIosSound(&tg.NotificationSoundDefault{})
	settings.SetStoriesAndroidSound(&tg.NotificationSoundDefault{})
	settings.SetStoriesOtherSound(&tg.NotificationSoundDefault{})
	return settings
}

// ReactionsNotifySettings returns conservative defaults for reaction notifications.
func ReactionsNotifySettings() *tg.ReactionsNotifySettings {
	return &tg.ReactionsNotifySettings{
		MessagesNotifyFrom:  &tg.ReactionNotificationsFromContacts{},
		StoriesNotifyFrom:   &tg.ReactionNotificationsFromContacts{},
		PollVotesNotifyFrom: &tg.ReactionNotificationsFromContacts{},
		Sound:               &tg.NotificationSoundDefault{},
		ShowPreviews:        true,
	}
}

func PrivacyRules(key tg.InputPrivacyKeyClass) *tg.AccountPrivacyRules {
	var rule tg.PrivacyRuleClass = &tg.PrivacyValueAllowAll{}
	switch key.(type) {
	case *tg.InputPrivacyKeyPhoneNumber:
		rule = &tg.PrivacyValueDisallowAll{}
	case *tg.InputPrivacyKeyBirthday:
		rule = &tg.PrivacyValueAllowContacts{}
	}
	return &tg.AccountPrivacyRules{
		Rules: []tg.PrivacyRuleClass{rule},
		Chats: []tg.ChatClass{},
		Users: []tg.UserClass{},
	}
}

func Authorizations() *tg.AccountAuthorizations {
	return &tg.AccountAuthorizations{Authorizations: []tg.Authorization{}}
}

func Passkeys() *tg.AccountPasskeys {
	return &tg.AccountPasskeys{Passkeys: []tg.Passkey{}}
}

func ContentSettings() *tg.AccountContentSettings {
	return &tg.AccountContentSettings{}
}

func GlobalPrivacySettings() *tg.GlobalPrivacySettings {
	return &tg.GlobalPrivacySettings{}
}

func AccountThemes() tg.AccountThemesClass {
	return &tg.AccountThemesNotModified{}
}

func DefaultEmojiStatuses() tg.AccountEmojiStatusesClass {
	return &tg.AccountEmojiStatusesNotModified{}
}

func CollectibleEmojiStatuses() tg.AccountEmojiStatusesClass {
	return &tg.AccountEmojiStatuses{Hash: 0, Statuses: []tg.EmojiStatusClass{}}
}

func DefaultGroupPhotoEmojis() tg.EmojiListClass {
	return &tg.EmojiList{Hash: 0, DocumentID: []int64{}}
}

func ConnectedBots() *tg.AccountConnectedBots {
	return &tg.AccountConnectedBots{ConnectedBots: []tg.ConnectedBot{}, Users: []tg.UserClass{}}
}

const availableReactionsHash = 20260602
const emptyStickerSetHash = 20260602

type defaultReaction struct {
	emoticon string
	title    string
}

var defaultAvailableReactions = []defaultReaction{
	{emoticon: "\U0001f44d", title: "Thumbs Up"},
	{emoticon: "\u2764\ufe0f", title: "Red Heart"},
	{emoticon: "\U0001f602", title: "Face With Tears of Joy"},
	{emoticon: "\U0001f62e", title: "Face With Open Mouth"},
	{emoticon: "\U0001f622", title: "Crying Face"},
	{emoticon: "\U0001f64f", title: "Folded Hands"},
}

// DefaultReactionEmoticons returns the TDesktop-compatible emoji reaction catalog order.
func DefaultReactionEmoticons() []string {
	out := make([]string, 0, len(defaultAvailableReactions))
	for _, reaction := range defaultAvailableReactions {
		out = append(out, reaction.emoticon)
	}
	return out
}

func AvailableReactions(hash int) tg.MessagesAvailableReactionsClass {
	if hash == availableReactionsHash {
		return &tg.MessagesAvailableReactionsNotModified{}
	}
	reactions := make([]tg.AvailableReaction, 0, len(defaultAvailableReactions))
	for i, reaction := range defaultAvailableReactions {
		reactions = append(reactions, availableReaction(reaction, i))
	}
	return &tg.MessagesAvailableReactions{
		Hash:      availableReactionsHash,
		Reactions: reactions,
	}
}

func availableReaction(reaction defaultReaction, index int) tg.AvailableReaction {
	const documentBaseID int64 = 900000000000000000
	doc := func(slot int64) tg.DocumentClass {
		return &tg.DocumentEmpty{ID: documentBaseID + int64(index)*10 + slot}
	}
	return tg.AvailableReaction{
		Reaction:          reaction.emoticon,
		Title:             reaction.title,
		StaticIcon:        doc(1),
		AppearAnimation:   doc(2),
		SelectAnimation:   doc(3),
		ActivateAnimation: doc(4),
		EffectAnimation:   doc(5),
	}
}

func Stickers() tg.MessagesStickersClass {
	return &tg.MessagesStickersNotModified{}
}

func StickerSet(req *tg.MessagesGetStickerSetRequest) tg.MessagesStickerSetClass {
	if req != nil && req.Hash == emptyStickerSetHash {
		return &tg.MessagesStickerSetNotModified{}
	}
	title, shortName := "Telesrv Empty Sticker Set", "telesrv_empty"
	if req != nil {
		switch set := req.Stickerset.(type) {
		case *tg.InputStickerSetAnimatedEmoji:
			title, shortName = "Animated Emoji", "AnimatedEmojies"
		case *tg.InputStickerSetAnimatedEmojiAnimations:
			title, shortName = "Emoji Animations", "EmojiAnimations"
		case *tg.InputStickerSetEmojiGenericAnimations:
			title, shortName = "Emoji Generic Animations", "EmojiGenericAnimations"
		case *tg.InputStickerSetDice:
			title, shortName = "Dice Animations", "AnimatedDices"
			if set.Emoticon != "" {
				shortName = "AnimatedDice"
			}
		case *tg.InputStickerSetPremiumGifts:
			title, shortName = "Premium Gifts", "GiftsPremium"
		case *tg.InputStickerSetShortName:
			if set.ShortName != "" {
				title, shortName = set.ShortName, set.ShortName
			}
		}
	}
	return &tg.MessagesStickerSet{
		Set: tg.StickerSet{
			ID:         910000000000000000,
			AccessHash: 910000000000000001,
			Title:      title,
			ShortName:  shortName,
			Count:      0,
			Hash:       emptyStickerSetHash,
		},
		Packs:     []tg.StickerPack{},
		Keywords:  []tg.StickerKeyword{},
		Documents: []tg.DocumentClass{},
	}
}

func EmojiGroups() tg.MessagesEmojiGroupsClass {
	return &tg.MessagesEmojiGroupsNotModified{}
}

func EmojiProfilePhotoGroups() tg.MessagesEmojiGroupsClass {
	return &tg.MessagesEmojiGroups{Hash: 0, Groups: []tg.EmojiGroupClass{}}
}

func AttachMenuBots() tg.AttachMenuBotsClass {
	return &tg.AttachMenuBotsNotModified{}
}

func QuickReplies() tg.MessagesQuickRepliesClass {
	return &tg.MessagesQuickRepliesNotModified{}
}

func TopPeers() tg.ContactsTopPeersClass {
	return &tg.ContactsTopPeersDisabled{}
}

func BlockedContacts() tg.ContactsBlockedClass {
	return &tg.ContactsBlocked{
		Blocked: []tg.PeerBlocked{},
		Chats:   []tg.ChatClass{},
		Users:   []tg.UserClass{},
	}
}

func PeerColors() tg.HelpPeerColorsClass {
	return &tg.HelpPeerColorsNotModified{}
}

func PromoData(now time.Time) tg.HelpPromoDataClass {
	return &tg.HelpPromoDataEmpty{Expires: int(now.Add(time.Hour).Unix())}
}

func TermsOfServiceUpdate(now time.Time) tg.HelpTermsOfServiceUpdateClass {
	return &tg.HelpTermsOfServiceUpdateEmpty{Expires: int(now.Add(24 * time.Hour).Unix())}
}

func PremiumPromo() *tg.HelpPremiumPromo {
	return &tg.HelpPremiumPromo{}
}

func AllStories() tg.StoriesAllStoriesClass {
	return &tg.StoriesAllStories{
		State:       "",
		StealthMode: tg.StoriesStealthMode{},
	}
}

func StoriesArchive() *tg.StoriesStories {
	return &tg.StoriesStories{}
}

func PinnedStories() *tg.StoriesStories {
	return &tg.StoriesStories{}
}

func StoryAlbums() tg.StoriesAlbumsClass {
	return &tg.StoriesAlbums{Hash: 0, Albums: []tg.StoryAlbum{}}
}

func StarGiftActiveAuctions() tg.PaymentsStarGiftActiveAuctionsClass {
	return &tg.PaymentsStarGiftActiveAuctionsNotModified{}
}

func SavedStarGifts() *tg.PaymentsSavedStarGifts {
	return &tg.PaymentsSavedStarGifts{
		Gifts: []tg.SavedStarGift{},
		Chats: []tg.ChatClass{},
		Users: []tg.UserClass{},
	}
}

func AiComposeTones() tg.AicomposeTonesClass {
	return &tg.AicomposeTonesNotModified{}
}

func WebPage(url string) *tg.MessagesWebPage {
	page := &tg.WebPageEmpty{ID: 0}
	if url != "" {
		page.SetURL(url)
	}
	return &tg.MessagesWebPage{
		Webpage: page,
		Chats:   []tg.ChatClass{},
		Users:   []tg.UserClass{},
	}
}
