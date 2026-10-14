package command

import (
	featurestats "github.com/xtls/xray-core/features/stats"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *statsServer) changedUsersStats(request *GetUsersStatsRequest) (*GetUsersStatsResponse, error) {
	if len(request.Emails) > 2000 {
		return nil, status.Error(codes.InvalidArgument, "at most 2000 user identities may be requested")
	}
	registry, ok := server.stats.(featurestats.UserRegistry)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "statistics manager does not support incremental export")
	}
	response := &GetUsersStatsResponse{}
	registry.VisitChangedUsers(request.Emails, func(email string, counters featurestats.UserCounters) {
		row := &UserStat{Email: email}
		if online := counters.Online(); online != nil {
			online.ForEach(func(ip string, lastSeen int64) bool {
				row.Ips = append(row.Ips, &OnlineIPEntry{Ip: ip, LastSeen: lastSeen})
				return true
			})
		}
		if request.IncludeTraffic {
			read := func(counter featurestats.Counter) int64 {
				if counter == nil {
					return 0
				}
				if request.Reset_ {
					return counter.Set(0)
				}
				return counter.Value()
			}
			row.Traffic = &TrafficUserStat{Uplink: read(counters.Uplink()), Downlink: read(counters.Downlink())}
		}
		response.Users = append(response.Users, row)
	})
	return response, nil
}
