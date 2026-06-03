package tdesktop

import (
	"testing"

	"github.com/gotd/td/tg"
)

func TestNotifySettingsDefaultIsAudible(t *testing.T) {
	settings := NotifySettings()
	if value, ok := settings.GetShowPreviews(); !ok || !value {
		t.Fatalf("show_previews = %v ok=%v, want true", value, ok)
	}
	if value, ok := settings.GetSilent(); !ok || value {
		t.Fatalf("silent = %v ok=%v, want explicit false", value, ok)
	}
	if value, ok := settings.GetMuteUntil(); !ok || value != 0 {
		t.Fatalf("mute_until = %d ok=%v, want explicit 0", value, ok)
	}
	if value, ok := settings.GetOtherSound(); !ok || value == nil {
		t.Fatalf("other_sound = %#v ok=%v, want default sound", value, ok)
	}
}

func TestTimezonesListIsNonEmptyAndHashable(t *testing.T) {
	got, ok := TimezonesList(0).(*tg.HelpTimezonesList)
	if !ok || got.Hash == 0 || len(got.Timezones) == 0 {
		t.Fatalf("TimezonesList(0) = %#v, want non-empty modified list", got)
	}
	if _, ok := TimezonesList(got.Hash).(*tg.HelpTimezonesListNotModified); !ok {
		t.Fatalf("TimezonesList(hash) = %#v, want notModified", TimezonesList(got.Hash))
	}
}

func TestAvailableReactionsCatalogIsNonEmptyAndHashable(t *testing.T) {
	got, ok := AvailableReactions(0).(*tg.MessagesAvailableReactions)
	if !ok {
		t.Fatalf("AvailableReactions(0) = %T, want modified list", got)
	}
	if got.Hash == 0 {
		t.Fatal("AvailableReactions(0).Hash = 0, want stable cache hash")
	}
	if len(got.Reactions) == 0 {
		t.Fatal("AvailableReactions(0).Reactions is empty")
	}
	for i, reaction := range got.Reactions {
		if reaction.Reaction == "" {
			t.Fatalf("reaction[%d].Reaction is empty", i)
		}
		if reaction.Title == "" {
			t.Fatalf("reaction[%d].Title is empty", i)
		}
		if reaction.StaticIcon == nil ||
			reaction.AppearAnimation == nil ||
			reaction.SelectAnimation == nil ||
			reaction.ActivateAnimation == nil ||
			reaction.EffectAnimation == nil {
			t.Fatalf("reaction[%d] has nil required document: %#v", i, reaction)
		}
		if reaction.Inactive || reaction.Premium {
			t.Fatalf("reaction[%d] flags = inactive %v premium %v, want active non-premium", i, reaction.Inactive, reaction.Premium)
		}
	}
	if _, ok := AvailableReactions(got.Hash).(*tg.MessagesAvailableReactionsNotModified); !ok {
		t.Fatalf("AvailableReactions(hash) = %#v, want notModified", AvailableReactions(got.Hash))
	}
}

func TestCollectibleEmojiStatusesIsEmptyModifiedList(t *testing.T) {
	got, ok := CollectibleEmojiStatuses().(*tg.AccountEmojiStatuses)
	if !ok {
		t.Fatalf("CollectibleEmojiStatuses() = %T, want empty modified list", got)
	}
	if got.Hash != 0 {
		t.Fatalf("CollectibleEmojiStatuses().Hash = %d, want 0", got.Hash)
	}
	if len(got.Statuses) != 0 {
		t.Fatalf("CollectibleEmojiStatuses().Statuses length = %d, want 0", len(got.Statuses))
	}
}

func TestDefaultGroupPhotoEmojisIsEmptyModifiedList(t *testing.T) {
	got, ok := DefaultGroupPhotoEmojis().(*tg.EmojiList)
	if !ok {
		t.Fatalf("DefaultGroupPhotoEmojis() = %T, want empty modified list", got)
	}
	if got.Hash != 0 {
		t.Fatalf("DefaultGroupPhotoEmojis().Hash = %d, want 0", got.Hash)
	}
	if len(got.DocumentID) != 0 {
		t.Fatalf("DefaultGroupPhotoEmojis().DocumentID length = %d, want 0", len(got.DocumentID))
	}
}

func TestEmojiProfilePhotoGroupsIsEmptyModifiedList(t *testing.T) {
	got, ok := EmojiProfilePhotoGroups().(*tg.MessagesEmojiGroups)
	if !ok {
		t.Fatalf("EmojiProfilePhotoGroups() = %T, want empty modified list", got)
	}
	if got.Hash != 0 {
		t.Fatalf("EmojiProfilePhotoGroups().Hash = %d, want 0", got.Hash)
	}
	if len(got.Groups) != 0 {
		t.Fatalf("EmojiProfilePhotoGroups().Groups length = %d, want 0", len(got.Groups))
	}
}

func TestConnectedBotsIsEmptyList(t *testing.T) {
	got := ConnectedBots()
	if got == nil {
		t.Fatal("ConnectedBots() = nil")
	}
	if len(got.ConnectedBots) != 0 || len(got.Users) != 0 {
		t.Fatalf("ConnectedBots() = bots %d users %d, want empty vectors", len(got.ConnectedBots), len(got.Users))
	}
}

func TestStoryAlbumsIsEmptyModifiedList(t *testing.T) {
	got, ok := StoryAlbums().(*tg.StoriesAlbums)
	if !ok {
		t.Fatalf("StoryAlbums() = %T, want empty modified list", got)
	}
	if got.Hash != 0 {
		t.Fatalf("StoryAlbums().Hash = %d, want 0", got.Hash)
	}
	if len(got.Albums) != 0 {
		t.Fatalf("StoryAlbums().Albums length = %d, want 0", len(got.Albums))
	}
}

func TestStickerSetReturnsEmptyModifiedSetForColdRequest(t *testing.T) {
	got, ok := StickerSet(&tg.MessagesGetStickerSetRequest{
		Stickerset: &tg.InputStickerSetEmojiGenericAnimations{},
		Hash:       0,
	}).(*tg.MessagesStickerSet)
	if !ok {
		t.Fatalf("StickerSet(hash=0) = %T, want modified empty set", got)
	}
	if got.Set.Hash == 0 {
		t.Fatal("StickerSet(hash=0).Set.Hash = 0, want stable cache hash")
	}
	if got.Set.ShortName != "EmojiGenericAnimations" {
		t.Fatalf("StickerSet(hash=0).Set.ShortName = %q, want EmojiGenericAnimations", got.Set.ShortName)
	}
	if len(got.Packs) != 0 || len(got.Keywords) != 0 || len(got.Documents) != 0 {
		t.Fatalf("StickerSet(hash=0) = packs %d keywords %d documents %d, want empty vectors", len(got.Packs), len(got.Keywords), len(got.Documents))
	}
	if _, ok := StickerSet(&tg.MessagesGetStickerSetRequest{
		Stickerset: &tg.InputStickerSetEmojiGenericAnimations{},
		Hash:       got.Set.Hash,
	}).(*tg.MessagesStickerSetNotModified); !ok {
		t.Fatalf("StickerSet(hash) = %#v, want notModified", StickerSet(&tg.MessagesGetStickerSetRequest{
			Stickerset: &tg.InputStickerSetEmojiGenericAnimations{},
			Hash:       got.Set.Hash,
		}))
	}
}
