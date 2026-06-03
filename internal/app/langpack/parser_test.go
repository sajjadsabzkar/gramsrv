package langpack

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseTDesktopFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tdesktop_en_v42.strings")
	if err := os.WriteFile(path, []byte(`
"lng_plain" = "Plain value";
"lng_escape" = "Line\nTwo";
"lng_items#one" = "{count} item";
"lng_items#other" = "{count} items";
`), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	pack, err := ParseTDesktopFile(path)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if pack.LangPack != "tdesktop" || pack.LangCode != "en" || pack.Version != 42 {
		t.Fatalf("pack meta = %+v", pack)
	}
	if len(pack.Strings) != 3 {
		t.Fatalf("strings count = %d, want 3", len(pack.Strings))
	}
	if got := pack.Strings[1].Value; got != "Line\nTwo" {
		t.Fatalf("escape value = %q", got)
	}
	plural := pack.Strings[2]
	if !plural.Pluralized || plural.Key != "lng_items" || plural.OneValue == "" || plural.OtherValue == "" {
		t.Fatalf("plural string = %+v", plural)
	}
}
