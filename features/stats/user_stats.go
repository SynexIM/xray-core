package stats

// UserCounters 的句柄在身份装载时建立，连接只读句柄；Release 按装载引用回收同名身份。
type UserCounters interface {
	Uplink() Counter
	Downlink() Counter
	Active() Counter
	Online() OnlineMap
	SiteCounter(name string) (Counter, error)
	Enable(uplink, downlink, online bool)
	Release()
}
type UserRegistry interface {
	AcquireUser(email string, uplink, downlink, online bool) (UserCounters, error)
	VisitChangedUsers(emails []string, visitor func(string, UserCounters))
}

func UserTrafficCounterName(email, direction string) string {
	return "user>>>" + email + ">>>traffic>>>" + direction
}
func UserOnlineMapName(email string) string { return "user>>>" + email + ">>>online" }
