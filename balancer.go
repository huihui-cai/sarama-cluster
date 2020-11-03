package cluster

import (
	"math"
	"sort"

	"github.com/Shopify/sarama"
)

// NotificationType defines the type of notification
type NotificationType uint8

// String describes the notification type
func (t NotificationType) String() string {
	switch t {
	case RebalanceStart:
		return "rebalance start"
	case RebalanceOK:
		return "rebalance OK"
	case RebalanceError:
		return "rebalance error"
	}
	return "unknown"
}

const (
	UnknownNotification NotificationType = iota
	RebalanceStart
	RebalanceOK
	RebalanceError
)

// Notification are state events emitted by the consumers on rebalance
type Notification struct {
	// Type exposes the notification type
	Type NotificationType

	// Claimed contains topic/partitions that were claimed by this rebalance cycle
	Claimed map[string][]int32

	// Released contains topic/partitions that were released as part of this rebalance cycle
	Released map[string][]int32

	// Current are topic/partitions that are currently claimed to the consumer
	Current map[string][]int32
}

func newNotification(current map[string][]int32) *Notification {
	return &Notification{
		Type:    RebalanceStart,
		Current: current,
	}
}

func (n *Notification) success(current map[string][]int32) *Notification {
	o := &Notification{
		Type:     RebalanceOK,
		Claimed:  make(map[string][]int32),
		Released: make(map[string][]int32),
		Current:  current,
	}
	for topic, partitions := range current {
		o.Claimed[topic] = int32Slice(partitions).Diff(int32Slice(n.Current[topic]))
	}
	for topic, partitions := range n.Current {
		o.Released[topic] = int32Slice(partitions).Diff(int32Slice(current[topic]))
	}
	return o
}

func (n *Notification) error() *Notification {
	o := &Notification{
		Type:     RebalanceError,
		Claimed:  make(map[string][]int32),
		Released: make(map[string][]int32),
		Current:  make(map[string][]int32),
	}
	for topic, partitions := range n.Claimed {
		o.Claimed[topic] = append(make([]int32, 0, len(partitions)), partitions...)
	}
	for topic, partitions := range n.Released {
		o.Released[topic] = append(make([]int32, 0, len(partitions)), partitions...)
	}
	for topic, partitions := range n.Current {
		o.Current[topic] = append(make([]int32, 0, len(partitions)), partitions...)
	}
	return o
}

// --------------------------------------------------------------------

type topicInfo struct {
	Partitions []int32
	MemberIDs  []string
}

func reverseStringSlice(s []string) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func (info topicInfo) RotateMemberIDs(k int) {
	s := info.MemberIDs
	sort.Strings(s)
	mLen := len(s)
	// No need to rotate
	if mLen <= 1 {
		return
	}

	// Array will be same after rotating if pos is 0
	pos := k % mLen
	if pos == 0 {
		return
	}
	reverseStringSlice(s[0:pos])
	reverseStringSlice(s[pos:])
	reverseStringSlice(s)
}

func (info topicInfo) Perform(offset int, s Strategy) map[string][]int32 {
	if s == StrategyRoundRobin {
		return info.RoundRobin(offset)
	}
	return info.Ranges(offset)
}

func (info topicInfo) Ranges(k int) map[string][]int32 {
	info.RotateMemberIDs(k)

	mlen := len(info.MemberIDs)
	plen := len(info.Partitions)
	res := make(map[string][]int32, mlen)

	for pos, memberID := range info.MemberIDs {
		n, i := float64(plen)/float64(mlen), float64(pos)
		min := int(math.Floor(i*n + 0.5))
		max := int(math.Floor((i+1)*n + 0.5))
		sub := info.Partitions[min:max]
		if len(sub) > 0 {
			res[memberID] = sub
		}
	}
	return res
}

func (info topicInfo) RoundRobin(k int) map[string][]int32 {
	info.RotateMemberIDs(k)

	mlen := len(info.MemberIDs)
	res := make(map[string][]int32, mlen)
	for i, pnum := range info.Partitions {
		memberID := info.MemberIDs[i%mlen]
		res[memberID] = append(res[memberID], pnum)
	}
	return res
}

// --------------------------------------------------------------------

type balancer struct {
	client   sarama.Client
	topics   map[string]topicInfo
	strategy Strategy
}

func newBalancerFromMeta(client sarama.Client, strategy Strategy, members map[string]sarama.ConsumerGroupMemberMetadata) (*balancer, error) {
	balancer := newBalancer(client, strategy)
	for memberID, meta := range members {
		for _, topic := range meta.Topics {
			if err := balancer.Topic(topic, memberID); err != nil {
				return nil, err
			}
		}
	}
	return balancer, nil
}

func newBalancer(client sarama.Client, strategy Strategy) *balancer {
	return &balancer{
		client:   client,
		topics:   make(map[string]topicInfo),
		strategy: strategy,
	}
}

func (r *balancer) Topic(name string, memberID string) error {
	topic, ok := r.topics[name]
	if !ok {
		nums, err := r.client.Partitions(name)
		if err != nil {
			return err
		}
		topic = topicInfo{
			Partitions: nums,
			MemberIDs:  make([]string, 0, 1),
		}
	}
	topic.MemberIDs = append(topic.MemberIDs, memberID)
	r.topics[name] = topic
	return nil
}

func (r *balancer) Perform() map[string]map[string][]int32 {
	res := make(map[string]map[string][]int32, 1)
	tLen := len(r.topics)
	topics := make([]string, 0, tLen)
	for topic, _ := range r.topics {
		topics = append(topics, topic)
	}
	sort.Strings(topics)

	for i, topic := range topics {
		info := r.topics[topic]
		for memberID, partitions := range info.Perform(i, r.strategy) {
			if _, ok := res[memberID]; !ok {
				res[memberID] = make(map[string][]int32, 1)
			}
			res[memberID][topic] = partitions
		}
	}
	return res
}
