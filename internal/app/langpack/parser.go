package langpack

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"telesrv/internal/domain"
)

var tdesktopStringRE = regexp.MustCompile(`(?s)"((?:\\.|[^"\\])*)"\s*=\s*"((?:\\.|[^"\\])*)";`)

// ParseTDesktopFile 解析 TDesktop .strings 文件为 domain 语言包。
func ParseTDesktopFile(path string) (domain.LangPack, error) {
	pack, err := packFromFilename(path)
	if err != nil {
		return domain.LangPack{}, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return domain.LangPack{}, fmt.Errorf("read tdesktop langpack %q: %w", path, err)
	}

	plain := make([]domain.LangPackString, 0)
	plurals := make(map[string]*domain.LangPackString)
	pluralOrder := make([]string, 0)
	for _, match := range tdesktopStringRE.FindAllStringSubmatch(string(data), -1) {
		key := unquoteTDesktop(match[1])
		value := unquoteTDesktop(match[2])
		base, plural := splitPluralKey(key)
		if plural == "" {
			plain = append(plain, domain.LangPackString{Key: key, Value: value})
			continue
		}
		item, ok := plurals[base]
		if !ok {
			plurals[base] = &domain.LangPackString{Key: base, Pluralized: true}
			item = plurals[base]
			pluralOrder = append(pluralOrder, base)
		}
		setPluralValue(item, plural, value)
	}

	pack.Strings = make([]domain.LangPackString, 0, len(plain)+len(pluralOrder))
	pack.Strings = append(pack.Strings, plain...)
	for _, key := range pluralOrder {
		pack.Strings = append(pack.Strings, *plurals[key])
	}
	return pack, nil
}

func packFromFilename(path string) (domain.LangPack, error) {
	name := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	const prefix = "tdesktop_"
	if !strings.HasPrefix(name, prefix) {
		return domain.LangPack{}, fmt.Errorf("invalid tdesktop langpack filename %q", filepath.Base(path))
	}
	rest := strings.TrimPrefix(name, prefix)
	idx := strings.LastIndex(rest, "_v")
	if idx <= 0 || idx+2 >= len(rest) {
		return domain.LangPack{}, fmt.Errorf("invalid tdesktop langpack filename %q", filepath.Base(path))
	}
	version, err := strconv.Atoi(rest[idx+2:])
	if err != nil {
		return domain.LangPack{}, fmt.Errorf("parse langpack version %q: %w", rest[idx+2:], err)
	}
	return domain.LangPack{
		LangPack: "tdesktop",
		LangCode: rest[:idx],
		Version:  version,
	}, nil
}

func splitPluralKey(key string) (base, plural string) {
	for _, suffix := range []string{"#zero", "#one", "#two", "#few", "#many", "#other"} {
		if strings.HasSuffix(key, suffix) {
			return strings.TrimSuffix(key, suffix), strings.TrimPrefix(suffix, "#")
		}
	}
	return key, ""
}

func setPluralValue(item *domain.LangPackString, plural, value string) {
	switch plural {
	case "zero":
		item.ZeroValue = value
	case "one":
		item.OneValue = value
	case "two":
		item.TwoValue = value
	case "few":
		item.FewValue = value
	case "many":
		item.ManyValue = value
	case "other":
		item.OtherValue = value
	}
}

func unquoteTDesktop(s string) string {
	v, err := strconv.Unquote(`"` + s + `"`)
	if err != nil {
		return s
	}
	return v
}
