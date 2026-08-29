package stats

// ActiveConnectionCounterName is the stable process-local inbound gauge used
// to determine when an inbound has drained established connections.
func ActiveConnectionCounterName(inboundTag string) string {
	return "inbound>>>" + inboundTag + ">>>connections>>>active"
}

// ActiveUserConnectionCounterName identifies an authenticated user's active
// connections. The caller supplies the stable user email/UID.
func ActiveUserConnectionCounterName(userUID string) string {
	return "user>>>" + userUID + ">>>connections>>>active"
}
