package domain

// UpdateState 是账号的 update 状态（pts/qts/seq/date）。
// 第一阶段空账号为零值；真实状态机属第二阶段。
type UpdateState struct {
	Pts  int
	Qts  int
	Date int
	Seq  int
}
