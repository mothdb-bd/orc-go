package block

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/mothdb-bd/orc-go/pkg/basic"
	"github.com/mothdb-bd/orc-go/pkg/maths"
	"github.com/mothdb-bd/orc-go/pkg/properties"
	"github.com/mothdb-bd/orc-go/pkg/treemap"
	"github.com/mothdb-bd/orc-go/pkg/util"
)

var (
	UTC_KEY               *TimeZoneKey = NewTimeZoneKey("UTC", 0)
	MAX_TIME_ZONE_KEY     int16
	ZONE_ID_TO_KEY        map[string]*TimeZoneKey
	ZONE_KEYS             util.SetInterface[string]
	TIME_ZONE_KEYS        []*TimeZoneKey
	OFFSET_TIME_ZONE_MIN  int16                     = -14 * 60
	OFFSET_TIME_ZONE_MAX  int16                     = 14 * 60
	OFFSET_TIME_ZONE_KEYS []*TimeZoneKey            = make([]*TimeZoneKey, OFFSET_TIME_ZONE_MAX-OFFSET_TIME_ZONE_MIN+1)
	UTC_EQUIVALENTS       util.SetInterface[string] = util.NewSetWithItems(util.SET_NonThreadSafe, "GMT", "GMT0", "GMT+0", "GMT-0", "Etc/GMT", "Etc/GMT0", "Etc/GMT+0", "Etc/GMT-0", "UT", "UT+0", "UT-0", "Etc/UT", "Etc/UT+0", "Etc/UT-0", "UTC", "UTC+0", "UTC-0", "Etc/UTC", "Etc/UTC+0", "Etc/UTC-0", "+0000", "+00:00", "-0000", "-00:00", "Z", "Zulu", "UCT", "Greenwich", "Universal", "Etc/Universal", "Etc/UCT")
)

type TimeZoneKey struct {
	id  string
	key int16
}

func init() {
	data := properties.NewProperties()
	in, _ := os.Open("zone-index.properties")
	data.Load(in)
	if data.Get("0") != nil {
		panic("Zone file should not contain a mapping for key 0")
	}

	// Map<String, TimeZoneKey> zoneIdToKey = new TreeMap<>();
	zoneIdToKey := treemap.New[string, *TimeZoneKey]()
	zoneIdToKey.Set(UTC_KEY.GetId(), UTC_KEY)
	maxZoneKey := util.INT16_ZERO
	for _, dKey := range data.Keys() {
		tmp, _ := strconv.ParseInt(strings.Trim(dKey.(string), " "), 10, 16)
		zoneKey := int16(tmp)
		zoneId := strings.Trim(data.Get(dKey).(string), " ")
		maxZoneKey = maths.MaxInt16(maxZoneKey, zoneKey)
		zoneIdToKey.Set(zoneId, NewTimeZoneKey(zoneId, zoneKey))
	}
	MAX_TIME_ZONE_KEY = maxZoneKey

	ZONE_KEYS = util.NewSet[string](util.SET_NonThreadSafe)

	TIME_ZONE_KEYS = make([]*TimeZoneKey, maxZoneKey+1)

	iter := zoneIdToKey.Iterator()
	for ; iter.Valid(); iter.Next() {
		ZONE_KEYS.Add(iter.Key())

		timeZoneKey := iter.Value()
		TIME_ZONE_KEYS[timeZoneKey.GetKey()] = timeZoneKey
	}

	for offset := OFFSET_TIME_ZONE_MIN; offset <= OFFSET_TIME_ZONE_MAX; offset++ {
		if offset == 0 {
			continue
		}
		zoneId := zoneIdForOffset(int64(offset))
		zoneKey := ZONE_ID_TO_KEY[zoneId]
		OFFSET_TIME_ZONE_KEYS[offset-OFFSET_TIME_ZONE_MIN] = zoneKey
	}
}

func GetTimeZoneKeys() util.SetInterface[string] {
	return ZONE_KEYS
}

// @JsonCreator
func GetTimeZoneKey(timeZoneKey int16) *TimeZoneKey {
	checkArgument(int(timeZoneKey) < len(TIME_ZONE_KEYS) && TIME_ZONE_KEYS[timeZoneKey] != nil, "Invalid time zone key %s", timeZoneKey)
	return TIME_ZONE_KEYS[timeZoneKey]
}

func GetTimeZoneKeyForOffset(offsetMinutes int64) *TimeZoneKey {
	if offsetMinutes == 0 {
		return UTC_KEY
	}
	if !(offsetMinutes >= int64(OFFSET_TIME_ZONE_MIN) && offsetMinutes <= int64(OFFSET_TIME_ZONE_MAX)) {
		panic(fmt.Sprintf("Invalid offset minutes %d", offsetMinutes))
	}
	timeZoneKey := OFFSET_TIME_ZONE_KEYS[offsetMinutes-int64(OFFSET_TIME_ZONE_MIN)]
	if timeZoneKey == nil {
		panic(zoneIdForOffset(offsetMinutes))
	}
	return timeZoneKey
}
func NewTimeZoneKey(id string, key int16) *TimeZoneKey {
	ty := new(TimeZoneKey)
	ty.id = id
	if key < 0 {
		panic("key is negative")
	}
	ty.key = key
	return ty
}

func (ty *TimeZoneKey) GetId() string {
	return ty.id
}

// @JsonValue
func (ty *TimeZoneKey) GetKey() int16 {
	return ty.key
}

// @Override
func (ty *TimeZoneKey) ToString() string {
	return ty.id
}

func zoneIdForOffset(offset int64) string {
	return fmt.Sprintf("%s%02f:%02f", util.If(offset < 0, "-", "+").(string), math.Abs(float64(offset)/60), math.Abs(float64(offset%60)))
}

func checkArgument(check bool, message string, args ...basic.Object) {
	if !check {
		panic(fmt.Sprintf(message, args))
	}
}
