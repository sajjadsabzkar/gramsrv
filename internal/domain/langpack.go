package domain

// LangPack 是一份客户端语言包的查询结果。
type LangPack struct {
	LangPack    string
	LangCode    string
	FromVersion int
	Version     int
	Strings     []LangPackString
}

// LangPackString 是语言包中的一个普通或复数形式字符串。
type LangPackString struct {
	Key        string
	Value      string
	Pluralized bool
	ZeroValue  string
	OneValue   string
	TwoValue   string
	FewValue   string
	ManyValue  string
	OtherValue string
	Deleted    bool
}
