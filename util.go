package userutil

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/subiz/executor/v2"
	"github.com/subiz/goutils/business_hours"
	"github.com/subiz/header"
	apb "github.com/subiz/header/account"
	"github.com/subiz/log"
	"github.com/thanhpk/ascii"
)

const Tolerance = 0.000001
const NPartition = 50

func ps(s string) *string { return &s }

var httpclient = http.Client{Timeout: 30 * time.Second}

func applyTextTransform(str string, transforms []*header.TextTransform) string {
	if len(transforms) == 0 {
		return str
	}

	transform := transforms[0]
	if transform.GetName() == "trim" {
		str = strings.TrimSpace(str)
	}

	if transform.GetName() == "lower_case" {
		str = strings.ToLower(str)
	}

	if transform.GetName() == "upper_case" {
		str = strings.ToUpper(str)
	}

	return applyTextTransform(str, transforms[1:])
}

func applyFloatTransform(fl float64, transforms []*header.FloatTransform) float64 {
	return fl
}

func EvaluateText(has bool, str string, cond *header.TextCondition) bool {
	str = applyTextTransform(str, cond.GetTransforms())
	if !cond.GetCaseSensitive() {
		str = strings.ToLower(str)
	}
	if !cond.GetAccentSensitive() {
		str = ascii.Convert(str)
	}

	switch cond.GetOp() {
	case "any":
		return true
	case "has_value":
		return has
	case "is_empty":
		return strings.TrimSpace(str) == ""
	case "eq":
		if len(cond.GetEq()) == 0 {
			return true
		}
		if !has {
			return false
		}
		for _, cs := range cond.GetEq() {
			if !cond.GetCaseSensitive() {
				cs = strings.ToLower(cs)
			}
			if !cond.GetAccentSensitive() {
				cs = ascii.Convert(cs)
			}

			if strings.TrimSpace(str) == strings.TrimSpace(cs) {
				return true
			}
		}
		return false
	case "neq":
		if len(cond.GetNeq()) == 0 {
			return true
		}

		for _, cs := range cond.GetNeq() {
			if !cond.GetCaseSensitive() {
				cs = strings.ToLower(cs)
			}
			if !cond.GetAccentSensitive() {
				cs = ascii.Convert(cs)
			}

			if strings.TrimSpace(str) == strings.TrimSpace(cs) {
				return false
			}
		}
		return true
	case "regex":
		if !has {
			return false
		}

		regexp.MatchString(cond.GetRegex(), str)
	case "start_with":
		if !has {
			return false
		}
		for _, cs := range cond.GetStartWith() {
			if !cond.GetCaseSensitive() {
				cs = strings.ToLower(cs)
			}
			if !cond.GetAccentSensitive() {
				cs = ascii.Convert(cs)
			}

			if strings.HasPrefix(strings.TrimSpace(str), strings.TrimSpace(cs)) {
				return true
			}
		}
		return false

	case "end_with":
		if !has {
			return false
		}
		for _, cs := range cond.GetEndWith() {
			if !cond.GetCaseSensitive() {
				cs = strings.ToLower(cs)
			}
			if !cond.GetAccentSensitive() {
				cs = ascii.Convert(cs)
			}

			if strings.HasSuffix(strings.TrimSpace(str), strings.TrimSpace(cs)) {
				return true
			}
		}
		return false
	case "contain":
		if !has {
			return false
		}
		for _, cs := range cond.GetContain() {
			if !cond.GetCaseSensitive() {
				cs = strings.ToLower(cs)
			}
			if !cond.GetAccentSensitive() {
				cs = ascii.Convert(cs)
			}

			if strings.Contains(strings.TrimSpace(str), strings.TrimSpace(cs)) {
				return true
			}
		}
		return false
	case "not_contain":
		if !has {
			return false
		}
		for _, cs := range cond.GetNotContain() {
			if !cond.GetCaseSensitive() {
				cs = strings.ToLower(cs)
			}
			if !cond.GetAccentSensitive() {
				cs = ascii.Convert(cs)
			}

			if strings.Contains(strings.TrimSpace(str), strings.TrimSpace(cs)) {
				return false
			}
		}
		return true
	case "not_start_with":
		if !has {
			return false
		}
		for _, cs := range cond.GetNotStartWith() {
			if !cond.GetCaseSensitive() {
				cs = strings.ToLower(cs)
			}
			if !cond.GetAccentSensitive() {
				cs = ascii.Convert(cs)
			}

			if strings.HasPrefix(strings.TrimSpace(str), strings.TrimSpace(cs)) {
				return false
			}
		}
		return true
	case "not_end_with":
		if !has {
			return false
		}
		for _, cs := range cond.GetEndWith() {
			if !cond.GetCaseSensitive() {
				cs = strings.ToLower(cs)
			}
			if !cond.GetAccentSensitive() {
				cs = ascii.Convert(cs)
			}

			if strings.HasSuffix(strings.TrimSpace(str), strings.TrimSpace(cs)) {
				return false
			}
		}
		return true
	default:
		return true
	}

	return true
}

func EvaluateTexts(strs []string, cond *header.TextCondition) bool {
	if len(cond.GetTransforms()) > 0 {
		for i, str := range strs {
			strs[i] = applyTextTransform(str, cond.GetTransforms())
		}
	}

	if !cond.GetCaseSensitive() {
		for i, str := range strs {
			strs[i] = strings.ToLower(str)
		}
	}
	if !cond.GetAccentSensitive() {
		for i, str := range strs {
			strs[i] = ascii.Convert(str)
		}
	}

	switch cond.GetOp() {
	case "any":
		return true
	case "has_value":
		return len(strs) == 0
	case "is_empty":
		return len(strs) == 0
	case "eq":
		if len(cond.GetEq()) == 0 {
			return true
		}
		for _, cs := range cond.GetEq() {
			if !cond.GetCaseSensitive() {
				cs = strings.ToLower(cs)
			}
			if !cond.GetAccentSensitive() {
				cs = ascii.Convert(cs)
			}
			for _, str := range strs {

				if strings.TrimSpace(str) == strings.TrimSpace(cs) {
					return true
				}
			}
		}
		return false
	case "neq":
		if len(cond.GetNeq()) == 0 {
			return true
		}

		for _, cs := range cond.GetNeq() {
			if !cond.GetCaseSensitive() {
				cs = strings.ToLower(cs)
			}
			if !cond.GetAccentSensitive() {
				cs = ascii.Convert(cs)
			}
			for _, str := range strs {
				if strings.TrimSpace(str) == strings.TrimSpace(cs) {
					return false
				}
			}
		}
		return true
	case "regex":
		for _, str := range strs {
			if b, _ := regexp.MatchString(cond.GetRegex(), str); b {
				return true
			}
			return false
		}
	case "start_with":
		for _, cs := range cond.GetStartWith() {
			if !cond.GetCaseSensitive() {
				cs = strings.ToLower(cs)
			}
			if !cond.GetAccentSensitive() {
				cs = ascii.Convert(cs)
			}
			for _, str := range strs {
				if strings.HasPrefix(strings.TrimSpace(str), strings.TrimSpace(cs)) {
					return true
				}
			}
		}
		return false

	case "end_with":
		for _, cs := range cond.GetEndWith() {
			if !cond.GetCaseSensitive() {
				cs = strings.ToLower(cs)
			}
			if !cond.GetAccentSensitive() {
				cs = ascii.Convert(cs)
			}

			for _, str := range strs {
				if strings.HasSuffix(strings.TrimSpace(str), strings.TrimSpace(cs)) {
					return true
				}
			}
		}
		return false
	case "contain":
		for _, cs := range cond.GetContain() {
			if !cond.GetCaseSensitive() {
				cs = strings.ToLower(cs)
			}
			if !cond.GetAccentSensitive() {
				cs = ascii.Convert(cs)
			}
			for _, str := range strs {
				if strings.Contains(strings.TrimSpace(str), strings.TrimSpace(cs)) {
					return true
				}
			}
		}
		return false
	case "not_contain":
		for _, cs := range cond.GetNotContain() {
			if !cond.GetCaseSensitive() {
				cs = strings.ToLower(cs)
			}
			if !cond.GetAccentSensitive() {
				cs = ascii.Convert(cs)
			}
			for _, str := range strs {
				if strings.Contains(strings.TrimSpace(str), strings.TrimSpace(cs)) {
					return false
				}
			}
		}
		return true
	case "not_start_with":
		for _, cs := range cond.GetNotStartWith() {
			if !cond.GetCaseSensitive() {
				cs = strings.ToLower(cs)
			}
			if !cond.GetAccentSensitive() {
				cs = ascii.Convert(cs)
			}
			for _, str := range strs {
				if strings.HasPrefix(strings.TrimSpace(str), strings.TrimSpace(cs)) {
					return false
				}
			}
		}
		return true
	case "not_end_with":

		for _, cs := range cond.GetEndWith() {
			if !cond.GetCaseSensitive() {
				cs = strings.ToLower(cs)
			}
			if !cond.GetAccentSensitive() {
				cs = ascii.Convert(cs)
			}
			for _, str := range strs {
				if strings.HasSuffix(strings.TrimSpace(str), strings.TrimSpace(cs)) {
					return false
				}
			}
		}
		return true
	default:
		return true
	}

	return true
}

func EvaluateFloat(found bool, fl float64, cond *header.FloatCondition) bool {
	fl = applyFloatTransform(fl, cond.GetTransforms())

	switch cond.GetOp() {
	case "has_value":
		if !cond.GetHasValue() {
			return !found
		}
		return found

	case "is_empty":
		return !found
	case "eq":
		if len(cond.GetEq()) == 0 {
			return true
		}
		for _, cf := range cond.GetEq() {
			if math.Abs(cf-fl) < Tolerance {
				return true
			}
		}
		return false

	case "neq":
		if len(cond.GetNeq()) == 0 {
			return true
		}
		for _, cf := range cond.GetNeq() {
			if math.Abs(cf-fl) < Tolerance {
				return false
			}
		}
		return true
	case "gt":
		return fl > cond.GetGt()
	case "lt":
		return fl <= cond.GetLt()
	case "gte":
		return fl >= cond.GetLte() || math.Abs(fl-cond.GetGte()) < Tolerance
	case "lte":
		return fl <= cond.GetLte() || math.Abs(fl-cond.GetLte()) < Tolerance
	case "in_range":
		if len(cond.GetInRange()) < 2 {
			return false
		}
		return cond.GetInRange()[0] <= fl && fl <= cond.GetInRange()[1]
	case "not_in_range":
		if len(cond.GetNotInRange()) < 2 {
			return false
		}
		return fl <= cond.GetNotInRange()[0] || cond.GetNotInRange()[1] <= fl
	}

	return true
}

func EvaluateBool(found, boo bool, cond *header.BoolCondition) bool {
	switch cond.GetOp() {
	// apply transform first
	case "has_value":
		return found
	case "true":
		return boo
	case "false":
		return !boo
	}
	return true
}

func EvaluateDatetime(acc *apb.Account, found bool, accid string, unixms int64, cond *header.DatetimeCondition) bool {
	t := time.Unix(unixms/1000, 0)

	switch cond.GetOp() {
	case "any":
		return true
	case "unset":
		return !found
	case "has_value":
		return found
	// apply transform first
	case "in_business_hour":
		inbusinesshours, _ := business_hours.DuringBusinessHour(acc.GetBusinessHours(), t, acc.GetTimezone())
		return inbusinesshours
	case "non_business_hour":
		inbusinesshours, _ := business_hours.DuringBusinessHour(acc.GetBusinessHours(), t, acc.GetTimezone())
		return !inbusinesshours
	case "today":
		utc := time.Now().UTC()
		startoftheday := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, utc.Location())
		endoftheday := time.Date(utc.Year(), utc.Month(), utc.Day(), 23, 59, 59, 0, utc.Location())
		h, m, _ := business_hours.SplitTzOffset(acc.GetTimezone())
		tzsec := int64(h*3600 + m*60)
		if startoftheday.Unix()+tzsec <= t.Unix() && t.Unix() <= endoftheday.Unix()+tzsec {
			return true
		}
		return false
	case "date_last_30mins":
		now := time.Now().Unix()
		last30mins := now - 1800
		return last30mins <= t.Unix() && t.Unix() <= now
	case "date_last_2hours":
		now := time.Now().Unix()
		last2hours := now - 7200
		return last2hours <= t.Unix() && t.Unix() <= now
	case "date_last_24h":
		now := time.Now().Unix()
		last1days := now - 86400
		return last1days <= t.Unix() && t.Unix() <= now
	case "date_last_7days":
		now := time.Now().Unix()
		last7days := now - 7*86400
		return last7days <= t.Unix() && t.Unix() <= now
	case "date_last_30days":
		now := time.Now().Unix()
		last30days := now - 30*86400
		return last30days <= t.Unix() && t.Unix() <= now
	case "yesterday":
		utc := time.Now().UTC()
		startoftheday := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, utc.Location()).Unix()
		startofyesterday := startoftheday - 86400

		h, m, _ := business_hours.SplitTzOffset(acc.GetTimezone())
		tzsec := int64(h*3600 + m*60)
		return startofyesterday+tzsec <= t.Unix() && t.Unix() <= startoftheday+tzsec
	case "last_week":
		utc := time.Now().UTC()
		startoftheday := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, utc.Location())
		weekday := int64(startoftheday.Weekday())
		weekday--
		if weekday == -1 {
			weekday = 7
		}
		startoftheweek := time.Unix(startoftheday.Unix()-weekday*86400, 0)
		endoftheweek := time.Unix(startoftheday.Unix()*(7-weekday)*86400+86400, 0)
		// endoftheday := time.Date(utc.Year(), utc.Month(), utc.Day(), 23, 59, 59, 0, utc.Location())

		h, m, _ := business_hours.SplitTzOffset(acc.GetTimezone())
		tzsec := int64(h*3600 + m*60)
		if startoftheweek.Unix()-604800+tzsec <= t.Unix() && t.Unix() <= endoftheweek.Unix()-604800+tzsec { // 604800 is to move back 7 day
			return true
		}
		return false
	case "this_week":
		utc := time.Now().UTC()
		startoftheday := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, utc.Location())
		weekday := int64(startoftheday.Weekday())
		weekday--
		if weekday == -1 {
			weekday = 7
		}
		startoftheweek := time.Unix(startoftheday.Unix()-weekday*86400, 0)
		endoftheweek := time.Unix(startoftheday.Unix()*(7-weekday)*86400+86400, 0)
		// endoftheday := time.Date(utc.Year(), utc.Month(), utc.Day(), 23, 59, 59, 0, utc.Location())

		h, m, _ := business_hours.SplitTzOffset(acc.GetTimezone())
		tzsec := int64(h*3600 + m*60)
		if startoftheweek.Unix()+tzsec <= t.Unix() && t.Unix() <= endoftheweek.Unix()+tzsec { // 604800 is to move back 7 day
			return true
		}
		return false
	case "last_month":
		utc := time.Now().UTC()
		endOfMonth := time.Date(utc.Year(), utc.Month(), 1, 0, 0, 0, 0, utc.Location())
		endOfMonth = endOfMonth.AddDate(0, 0, -1)

		startOfMonth := time.Date(endOfMonth.Year(), endOfMonth.Month(), 1, 0, 0, 0, 0, utc.Location())
		h, m, _ := business_hours.SplitTzOffset(acc.GetTimezone())
		tzsec := int64(h*3600 + m*60)
		if startOfMonth.Unix()+tzsec <= t.Unix() && t.Unix() <= endOfMonth.Unix()+tzsec {
			return true
		}
		return false
	case "this_month":
		utc := time.Now().UTC()
		firstOfMonth := time.Date(utc.Year(), utc.Month(), 1, 0, 0, 0, 0, utc.Location())
		lastOfMonth := firstOfMonth.AddDate(0, 1, -1)

		h, m, _ := business_hours.SplitTzOffset(acc.GetTimezone())
		tzsec := int64(h*3600 + m*60)
		if firstOfMonth.Unix()+tzsec <= t.Unix() && t.Unix() <= lastOfMonth.Unix()+tzsec {
			return true
		}
		return false
	case "last":
		a := time.Now().Unix() - cond.GetLast()
		b := time.Now().Unix()
		return a <= t.Unix() && t.Unix() <= b
	case "before_ago":
		return t.Unix() < time.Now().Unix()-cond.GetBeforeAgo()
	case "days_of_week":
		for _, weekday := range cond.GetDaysOfWeek() {
			if strings.EqualFold(weekday, t.Weekday().String()) {
				return true
			}
		}
		return false
	case "after":
		return time.Unix(cond.GetAfter()/1000, 0).Unix() <= t.Unix()
	case "before":
		return t.Unix() <= time.Unix(cond.GetBefore()/1000, 0).Unix()
	case "between":
		if len(cond.GetBetween()) != 2 {
			return true
		}
		a := cond.GetBetween()[0] / 1000
		b := cond.GetBetween()[1] / 1000
		return a <= t.Unix() && t.Unix() <= b
	case "outside":
		if len(cond.GetOutside()) != 2 {
			return true
		}
		a := cond.GetOutside()[0] / 1000
		b := cond.GetOutside()[1] / 1000
		return t.Unix() <= a || b <= t.Unix()
	}
	return true
}

func RsCheck(acc *apb.Account, defM map[string]*header.AttributeDefinition, u *header.User, cond *header.UserViewCondition, deleted bool) bool {
	if len(cond.GetOne()) > 0 {
		for _, c := range cond.GetOne() {
			if RsCheck(acc, defM, u, c, deleted) {
				return true
			}
		}
		return false
	}

	if len(cond.GetAll()) > 0 {
		for _, c := range cond.GetAll() {
			if !RsCheck(acc, defM, u, c, deleted) {
				return false
			}
		}
		return true
	}
	return evaluateSingleCond(acc, defM, u, cond, deleted)
}

func evaluateSingleCond(acc *apb.Account, defM map[string]*header.AttributeDefinition, u *header.User, cond *header.UserViewCondition, deleted bool) bool {
	if deleted && u.Deleted == 0 {
		return false
	}

	if !deleted && u.Deleted > 0 {
		return false
	}

	accid := u.GetAccountId()
	if cond.GetKey() == "id" {
		id := u.GetId()
		return EvaluateText(true, id, cond.GetText())
	}

	if cond.GetKey() == "channel" {
		return EvaluateText(true, u.Channel, cond.GetText())
	}

	if cond.GetKey() == "channel_source" {
		return EvaluateText(true, u.ChannelSource, cond.GetText())
	}

	if cond.GetKey() == "keyword" && len(cond.GetText().GetContain()) > 0 { // email phone or name
		// remove space
		keyword := ascii.Convert(SpaceStringsBuilder(strings.ToLower(cond.GetText().GetContain()[0])))

		for _, attr := range u.Attributes {
			if attr.Text != "" {
				if strings.Contains(ascii.Convert(strings.ToLower(SpaceStringsBuilder(attr.Text))), keyword) {
					return true
				}
			}
		}

		return strings.Contains(strings.TrimSpace(strings.ToLower(u.Id)), keyword)
	}

	if cond.GetKey() == "lead_owners" {
		for _, owner := range u.GetLeadOwners() {
			if EvaluateText(true, owner, cond.GetText()) {
				return true
			}
		}

		if len(u.GetLeadOwners()) == 0 {
			if EvaluateText(false, "", cond.Text) {
				return true
			}
		}
		return false
	}

	if cond.GetKey() == "lead_conversion_bys" {
		for _, by := range u.GetLeadConversionBys() {
			if EvaluateText(true, by, cond.GetText()) {
				return true
			}
		}

		if len(u.GetLeadConversionBys()) == 0 {
			if EvaluateText(false, "", cond.Text) {
				return true
			}
		}
		return false
	}

	if cond.GetKey() == "start_content_view:by:device:ip" {
		return EvaluateText(u.StartContentView != nil, u.GetStartContentView().GetBy().GetDevice().GetIp(), cond.Text)
	}
	if cond.GetKey() == "start_content_view:by:device:language" {
		return EvaluateText(u.StartContentView != nil, u.GetStartContentView().GetBy().GetDevice().GetLanguage(), cond.Text)
	}
	if cond.GetKey() == "start_content_view:by:device:page_title" {
		return EvaluateText(u.StartContentView != nil, u.GetStartContentView().GetBy().GetDevice().GetPageTitle(), cond.Text)
	}
	if cond.GetKey() == "start_content_view:by:device:page_url" {
		return EvaluateText(u.StartContentView != nil, u.GetStartContentView().GetBy().GetDevice().GetPageUrl(), cond.Text)
	}
	if cond.GetKey() == "start_content_view:by:device:platform" {
		return EvaluateText(u.StartContentView != nil, u.GetStartContentView().GetBy().GetDevice().GetPlatform(), cond.Text)
	}
	if cond.GetKey() == "start_content_view:by:device:referrer" {
		return EvaluateText(u.StartContentView != nil, u.GetStartContentView().GetBy().GetDevice().GetReferrer(), cond.Text)
	}
	if cond.GetKey() == "start_content_view:by:device:screen_resolution" {
		return EvaluateText(u.StartContentView != nil, u.GetStartContentView().GetBy().GetDevice().GetScreenResolution(), cond.Text)
	}
	if cond.GetKey() == "start_content_view:by:device:source" {
		return EvaluateText(u.StartContentView != nil, u.GetStartContentView().GetBy().GetDevice().GetSource(), cond.Text)
	}
	if cond.GetKey() == "start_content_view:by:device:type" {
		return EvaluateText(u.StartContentView != nil, u.GetStartContentView().GetBy().GetDevice().GetType(), cond.Text)
	}
	if cond.GetKey() == "start_content_view:by:device:user_agent" {
		return EvaluateText(u.StartContentView != nil, u.GetStartContentView().GetBy().GetDevice().GetUserAgent(), cond.Text)
	}

	if cond.GetKey() == "start_content_view:by:device:utm:name" {
		return EvaluateText(u.StartContentView != nil, u.GetStartContentView().GetBy().GetDevice().GetUtm().GetName(), cond.Text)
	}

	if cond.GetKey() == "start_content_view:by:device:utm:source" {
		return EvaluateText(u.StartContentView != nil, u.GetStartContentView().GetBy().GetDevice().GetUtm().GetSource(), cond.Text)
	}

	if cond.GetKey() == "start_content_view:by:device:utm:medium" {
		return EvaluateText(u.StartContentView != nil, u.GetStartContentView().GetBy().GetDevice().GetUtm().GetMedium(), cond.Text)
	}

	if cond.GetKey() == "start_content_view:by:device:utm:term" {
		return EvaluateText(u.StartContentView != nil, u.GetStartContentView().GetBy().GetDevice().GetUtm().GetTerm(), cond.Text)
	}

	if cond.GetKey() == "start_content_view:by:device:utm:content" {
		return EvaluateText(u.StartContentView != nil, u.GetStartContentView().GetBy().GetDevice().GetUtm().GetContent(), cond.Text)
	}

	if cond.GetKey() == "first_content_view:by:device:ip" {
		return EvaluateText(u.FirstContentView != nil, u.GetFirstContentView().GetBy().GetDevice().GetIp(), cond.Text)
	}
	if cond.GetKey() == "first_content_view:by:device:language" {
		return EvaluateText(u.FirstContentView != nil, u.GetFirstContentView().GetBy().GetDevice().GetLanguage(), cond.Text)
	}
	if cond.GetKey() == "first_content_view:by:device:page_title" {
		return EvaluateText(u.FirstContentView != nil, u.GetFirstContentView().GetBy().GetDevice().GetPageTitle(), cond.Text)
	}
	if cond.GetKey() == "first_content_view:by:device:page_url" {
		return EvaluateText(u.FirstContentView != nil, u.GetFirstContentView().GetBy().GetDevice().GetPageUrl(), cond.Text)
	}
	if cond.GetKey() == "first_content_view:by:device:platform" {
		return EvaluateText(u.FirstContentView != nil, u.GetFirstContentView().GetBy().GetDevice().GetPlatform(), cond.Text)
	}
	if cond.GetKey() == "first_content_view:by:device:referrer" {
		return EvaluateText(u.FirstContentView != nil, u.GetFirstContentView().GetBy().GetDevice().GetReferrer(), cond.Text)
	}
	if cond.GetKey() == "first_content_view:by:device:screen_resolution" {
		return EvaluateText(u.FirstContentView != nil, u.GetFirstContentView().GetBy().GetDevice().GetScreenResolution(), cond.Text)
	}
	if cond.GetKey() == "first_content_view:by:device:source" {
		return EvaluateText(u.FirstContentView != nil, u.GetFirstContentView().GetBy().GetDevice().GetSource(), cond.Text)
	}
	if cond.GetKey() == "first_content_view:by:device:type" {
		return EvaluateText(u.FirstContentView != nil, u.GetFirstContentView().GetBy().GetDevice().GetType(), cond.Text)
	}
	if cond.GetKey() == "first_content_view:by:device:user_agent" {
		return EvaluateText(u.FirstContentView != nil, u.GetFirstContentView().GetBy().GetDevice().GetUserAgent(), cond.Text)
	}

	if cond.GetKey() == "first_content_view:by:device:utm:name" {
		return EvaluateText(u.FirstContentView != nil, u.GetFirstContentView().GetBy().GetDevice().GetUtm().GetName(), cond.Text)
	}

	if cond.GetKey() == "first_content_view:by:device:utm:source" {
		return EvaluateText(u.FirstContentView != nil, u.GetFirstContentView().GetBy().GetDevice().GetUtm().GetSource(), cond.Text)
	}

	if cond.GetKey() == "first_content_view:by:device:utm:medium" {
		return EvaluateText(u.FirstContentView != nil, u.GetFirstContentView().GetBy().GetDevice().GetUtm().GetMedium(), cond.Text)
	}

	if cond.GetKey() == "first_content_view:by:device:utm:term" {
		return EvaluateText(u.FirstContentView != nil, u.GetFirstContentView().GetBy().GetDevice().GetUtm().GetTerm(), cond.Text)
	}

	if cond.GetKey() == "first_content_view:by:device:utm:content" {
		return EvaluateText(u.FirstContentView != nil, u.GetFirstContentView().GetBy().GetDevice().GetUtm().GetContent(), cond.Text)
	}

	if cond.GetKey() == "labels" {
		labels := []string{}
		for _, label := range u.Labels {
			labels = append(labels, label.Label)
		}

		return EvaluateTexts(labels, cond.Text)
	}

	if cond.GetKey() == "segment" {
		segs := []string{}
		for _, seg := range u.Segments {
			segs = append(segs, seg.GetSegmentId())
		}
		return EvaluateTexts(segs, cond.Text)
	}

	if strings.HasPrefix(cond.GetKey(), "attr:") || strings.HasPrefix(cond.GetKey(), "attr.") {
		key := cond.GetKey()[5:]

		def := defM[key]
		if def == nil {
			// def not found
			return false
		}

		defType := def.GetType()

		if defType == "list" || defType == "" {
			defType = "text"
		}
		text, num, date, boo, found := FindAttr(u, key, def.Type)
		if defType == "text" {
			return EvaluateText(found, text, cond.GetText())
		}

		if defType == "number" {
			return EvaluateFloat(found, num, cond.GetNumber())
		}
		if defType == "boolean" {
			return EvaluateBool(found, boo, cond.GetBoolean())
		}
		if defType == "datetime" { // consider number in ms
			return EvaluateDatetime(acc, found, accid, date, cond.Datetime)
		}
	}
	return true
}

func FindAttr(u *header.User, key string, typ string) (string, float64, int64, bool, bool) {
	for _, a := range u.Attributes {
		if a.Key != key {
			continue
		}
		t, err := time.Parse(time.RFC3339, a.GetDatetime())
		if err != nil {
			t = time.Unix(0, 0)
		}

		text := a.Text
		return text, a.Number, t.UnixMilli(), a.Boolean, true
	}
	return "", 0, 0, false, false
}

func SpaceStringsBuilder(str string) string {
	var b strings.Builder
	b.Grow(len(str))
	for _, ch := range str {
		if !unicode.IsSpace(ch) {
			b.WriteRune(ch)
		}
	}
	return b.String()
}

func PureFilterUsers(acc *apb.Account, cond *header.UserViewCondition, leads []*header.User, anchor string, limit int, orderby string, defM map[string]*header.AttributeDefinition, ignoreIds map[string]bool) *header.Users {
	if cond == nil {
		cond = &header.UserViewCondition{}
	}
	if orderby == "" {
		orderby = "-id"
	}

	// "-segment_joined"

	desc := false
	if orderby[0] != '-' && orderby[0] != '+' {
		desc = true
	} else {
		if orderby[0] == '-' {
			desc = true
		}
		orderby = orderby[1:]
	}

	out := make([]*header.User, 0)
	var valM = map[string]string{}
	anchorUserId := ""
	anchorsplit := strings.Split(anchor, ".")
	if len(anchorsplit) > 1 {
		anchorUserId = anchorsplit[len(anchorsplit)-1]
		valM[anchorUserId] = strings.Join(anchorsplit[:len(anchorsplit)-1], ".")
	}

	// segment
	segmentid := ""
	for _, cond := range cond.GetAll() {
		if cond.GetKey() == "segment" && cond.GetText().GetOp() == "eq" && len(cond.GetText().GetEq()) > 0 {
			segmentid = cond.GetText().GetEq()[0]
			break
		}
	}

	total := 0
	executor.Async(len(leads), func(i int, lock *sync.Mutex) {
		u := leads[i]
		if u.Id == "" || u.PrimaryId != "" || ignoreIds[u.Id] {
			return
		}

		if !RsCheck(acc, defM, u, cond, cond.Deleted) {
			return
		}

		val := ""
		if orderby == "+segment_joined" || orderby == "-segment_joined" {
			val = GetSortValSegmentId(segmentid, u)
		} else {
			val = GetSortVal(orderby, u, defM)
		}

		lock.Lock()
		total++
		defer lock.Unlock()

		valM[u.Id] = val
		if anchorUserId == "" {
			out = append(out, u)
			return
		}

		// ignore the item that already in the anchor
		if u.Id == anchorUserId {
			return
		}

		// skip less than anchor value
		if LessVal(u.Id, anchorUserId, valM, desc) {
			return
		}
		out = append(out, u)
	}, 20)

	sort.Slice(out, func(i int, j int) bool {
		return LessVal(out[i].Id, out[j].Id, valM, desc)
	})

	res := []*header.User{}
	for _, user := range out {
		if len(res) >= limit {
			break
		}
		res = append(res, user)
	}

	if len(res) > 0 {
		lastUserId := res[len(res)-1].Id
		anchor = valM[lastUserId] + "." + lastUserId
	}
	return &header.Users{Users: res, Hit: int64(len(res)), Total: int64(total), Anchor: anchor}
}

func MergeUserResult(dst, src *header.Users, limit int, segmentid, orderby string, defM map[string]*header.AttributeDefinition) *header.Users {
	if orderby == "" {
		orderby = "-id"
	}
	desc := false
	if orderby[0] != '-' && orderby[0] != '+' {
		desc = true
	} else {
		if orderby[0] == '-' {
			desc = true
		}
	}

	userm := map[string]*header.User{}
	for _, user := range dst.GetUsers() {
		if user.PrimaryId != "" {
			continue
		}
		userm[user.Id] = user
	}

	// override dst
	for _, user := range src.GetUsers() {
		if user.PrimaryId != "" {
			continue
		}
		userm[user.Id] = user
	}

	var valM = map[string]string{}
	out := []*header.User{}
	for _, user := range userm {
		val := ""
		if orderby == "+segment_joined" || orderby == "-segment_joined" {
			val = GetSortValSegmentId(segmentid, user)
		} else {
			val = GetSortVal(orderby, user, defM)
		}
		valM[user.Id] = val
		out = append(out, user)
	}

	sort.Slice(out, func(i int, j int) bool {
		return LessVal(out[i].Id, out[j].Id, valM, desc)
	})

	res := []*header.User{}
	for _, user := range out {
		if len(res) >= limit {
			break
		}
		res = append(res, user)
	}

	anchor := ""
	if len(res) > 0 {
		lastUserId := res[len(res)-1].Id
		anchor = valM[lastUserId] + "." + lastUserId
	}

	return &header.Users{Users: res, Hit: int64(len(res)), Total: dst.GetTotal() + src.GetTotal(), Anchor: anchor}
}

func LessVal(iid, jid string, valM map[string]string, desc bool) bool {
	less := false
	if valM[iid][0] == 's' {
		if valM[iid] == valM[jid] {
			less = iid < jid
		} else {
			less = valM[iid] < valM[jid]
		}
	}

	if valM[iid][0] == 'f' {
		fi, _ := strconv.ParseFloat(valM[iid][1:], 64)
		fj, _ := strconv.ParseFloat(valM[jid][1:], 64)
		if math.Abs(fi-fj) < Tolerance {
			less = iid < jid
		} else {
			less = fi < fj
		}
	}

	if valM[iid][0] == 'l' {
		valsi := strings.Split(valM[iid][1:], ".")
		leni, _ := strconv.Atoi(valsi[0])
		vali := strings.Join(valsi[1:], ".")

		valsj := strings.Split(valM[jid][1:], ".")
		lenj, _ := strconv.Atoi(valsj[0])
		valj := strings.Join(valsj[1:], ".")

		if leni < lenj {
			less = true
		} else if leni == lenj {
			less = vali < valj
			if vali == valj {
				less = iid < jid
			}
		} else {
			less = false
		}
	}

	if desc {
		return !less
	}
	return less
}

func GetSortValSegmentId(segmentid string, user *header.User) string {
	if segmentid == "" {
		return "f" + user.Id
	}

	for _, segment := range user.Segments {
		if segment.SegmentId == segmentid {
			return "f" + strconv.Itoa(int(segment.Created))
		}
	}

	return "f0"
}

func GetSortVal(orderby string, user *header.User, defM map[string]*header.AttributeDefinition) string {
	if orderby == "" {
		orderby = "id"
	}
	if orderby[0] == '-' || orderby[0] == '+' {
		orderby = orderby[1:]
	}

	var val = "s"
	if orderby == "id" {
		val = "s" + user.Id
	}
	if orderby == "lead_owners" {
		val = "l" + strconv.Itoa(len(user.LeadOwners)) + "." + strings.Join(user.LeadOwners, ",")
	}
	if orderby == "labels" {
		val = ""
		for _, l := range user.Labels {
			val += l.Label
		}
		val = "l" + strconv.Itoa(len(user.Labels)) + "." + val
	}

	if strings.HasPrefix(orderby, "attr:") || strings.HasPrefix(orderby, "attr.") {
		key := orderby[5:]
		def := defM[key]
		if def != nil {
			text, num, date, boo, _ := FindAttr(user, key, def.Type)
			if def.Type == "text" || def.Type == "" || def.Type == "list" {
				val = "s" + text
			}
			if def.Type == "number" {
				val = "f" + strconv.FormatFloat(num, 'E', -1, 64)
			}
			if def.Type == "boolean" {
				if !boo {
					val = "s0."
				} else {
					val = "s1."
				}
			}
			if def.Type == "datetime" { // consider number in ms
				val = "s" + time.Unix(date/1000, 0).Format(time.RFC3339)
			}
		}
	}
	return val
}

const UserQueryURL = "https://user-query-66xno24cra-as.a.run.app"

func DoFilter(version int, acc *apb.Account, cond *header.UserViewCondition, defM map[string]*header.AttributeDefinition, anchor, orderby string, limit int, ignoreIds []string) (*header.Users, error) {
	accid := acc.GetId()
	userQuery := &header.UserQueryBody{
		Condition:  cond,
		Account:    &apb.Account{Id: &accid, Timezone: ps(acc.GetTimezone()), BusinessHours: acc.GetBusinessHours()},
		Def:        defM,
		IgnoreUids: ignoreIds,
	}
	body, _ := json.Marshal(userQuery)
	wg := sync.WaitGroup{}
	lock := &sync.Mutex{}
	res := &header.Users{}
	wg.Add(NPartition)
	var outerr = make([]error, NPartition)

	segmentid := ""
	for _, cond := range cond.GetAll() {
		if cond.GetKey() == "segment" && cond.GetText().GetOp() == "eq" && len(cond.GetText().GetEq()) > 0 {
			segmentid = cond.GetText().GetEq()[0]
			break
		}
	}

	for i := 0; i < NPartition; i++ {
		go func(i int) {
			defer wg.Done()
			query := url.Values{}
			query.Add("path", accid+"_"+strconv.Itoa(i)+"_v"+strconv.Itoa(version)+".dat")
			query.Add("limit", strconv.Itoa(limit))
			query.Add("order_by", orderby)
			query.Add("anchor", anchor)

			resp, err := httpclient.Post(UserQueryURL+"/query?"+query.Encode(), "application/json", bytes.NewBuffer(body))
			if err != nil {
				outerr[i] = err
				return
			}

			out, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				outerr[i] = err
				return
			}

			users := &header.Users{}
			if err := json.Unmarshal(out, users); err != nil {
				outerr[i] = log.EData(err, out, log.M{"accid": accid, "i": i})
				return
			}

			lock.Lock()
			res = MergeUserResult(res, users, limit, segmentid, orderby, defM)
			lock.Unlock()
		}(i)
	}
	wg.Wait()
	for _, err := range outerr {
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func DoFilterBatch(version int, acc *apb.Account, conds []*header.UserViewCondition, defM map[string]*header.AttributeDefinition, orderbys []string, limit int, ignoreIds []string) ([]*header.Users, error) {
	if len(conds) == 0 {
		return nil, nil
	}
	accid := acc.GetId()
	userQuery := &header.UserQueryBody{
		Conditions: conds,
		Account:    &apb.Account{Id: &accid, Timezone: ps(acc.GetTimezone()), BusinessHours: acc.GetBusinessHours()},
		Def:        defM,
		IgnoreUids: ignoreIds,
	}
	body, _ := json.Marshal(userQuery)
	wg := sync.WaitGroup{}
	lock := &sync.Mutex{}
	res := make([]*header.Users, len(conds))
	wg.Add(NPartition)
	var outerr = make([]error, NPartition)

	for i := 0; i < NPartition; i++ {
		go func(i int) {
			defer wg.Done()
			query := url.Values{}
			query.Add("path", accid+"_"+strconv.Itoa(i)+"_v"+strconv.Itoa(version)+".dat")
			query.Add("limit", strconv.Itoa(limit))
			query.Add("order_bys", strings.Join(orderbys, ";"))

			resp, err := httpclient.Post(UserQueryURL+"/batch-query?"+query.Encode(), "application/json", bytes.NewBuffer(body))
			if err != nil {
				outerr[i] = err
				return
			}

			out, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				outerr[i] = err
				return
			}

			userss := []*header.Users{}
			if err := json.Unmarshal(out, &userss); err != nil {
				outerr[i] = log.EData(err, out, log.M{"accid": accid, "i": i})
				return
			}
			lock.Lock()
			for i, users := range userss {
				segmentid := ""
				if len(conds) > i {
					for _, cond := range conds[i].GetAll() {
						if cond.GetKey() == "segment" && cond.GetText().GetOp() == "eq" && len(cond.GetText().GetEq()) > 0 {
							segmentid = cond.GetText().GetEq()[0]
							break
						}
					}
				}
				res[i] = MergeUserResult(res[i], users, limit, segmentid, orderbys[i], defM)
			}
			lock.Unlock()
		}(i)
	}
	wg.Wait()
	for _, err := range outerr {
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func DoListUserInSegment(version int, accid string, segmentids []string) (map[string][]string, error) {
	wg := sync.WaitGroup{}
	lock := &sync.Mutex{}
	wg.Add(NPartition)
	var outerr = make([]error, NPartition)
	outsegments := map[string][]string{}
	for i := 0; i < NPartition; i++ {
		go func(i int) {
			defer wg.Done()
			query := url.Values{}
			query.Add("path", accid+"_"+strconv.Itoa(i)+"_v"+strconv.Itoa(version)+".dat")
			query.Add("segments", strings.Join(segmentids, ","))
			resp, err := httpclient.Get(UserQueryURL + "/list-segment-user?" + query.Encode())
			if err != nil {
				outerr[i] = err
				return
			}

			out, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				outerr[i] = err
				return
			}

			segments := &header.Segments{}
			if err := json.Unmarshal(out, segments); err != nil {
				outerr[i] = log.EData(err, out, log.M{"accid": accid, "i": i})
				return
			}

			lock.Lock()
			for _, seg := range segments.GetSegments() {
				segmentMembers := outsegments[seg.GetId()]
				segmentMembers = append(segmentMembers, seg.Members...)
				outsegments[seg.GetId()] = segmentMembers
			}
			lock.Unlock()
		}(i)
	}
	wg.Wait()
	for _, err := range outerr {
		if err != nil {
			return nil, err
		}
	}

	return outsegments, nil
}

func DoCount(version int, acc *apb.Account, conds []*header.UserViewCondition, defM map[string]*header.AttributeDefinition, ignoreIds []string) ([]int64, error) {
	if len(conds) == 0 {
		return []int64{}, nil
	}
	accid := acc.GetId()
	userQuery := &header.UserCountBody{
		Conditions: conds,
		Account:    &apb.Account{Id: &accid, Timezone: ps(acc.GetTimezone()), BusinessHours: acc.GetBusinessHours()},
		Def:        defM,
		IgnoreUids: ignoreIds,
	}
	body, _ := json.Marshal(userQuery)
	wg := sync.WaitGroup{}
	lock := &sync.Mutex{}
	wg.Add(NPartition)
	var outerr = make([]error, NPartition)
	totals := make([]int64, len(conds))

	for i := 0; i < NPartition; i++ {
		go func(i int) {
			defer wg.Done()
			query := url.Values{}
			query.Add("path", accid+"_"+strconv.Itoa(i)+"_v"+strconv.Itoa(version)+".dat")

			resp, err := httpclient.Post(UserQueryURL+"/count?"+query.Encode(), "application/json", bytes.NewBuffer(body))
			if err != nil {
				outerr[i] = err
				return
			}

			out, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				outerr[i] = err
				return
			}

			segments := &header.Segments{}
			if err := json.Unmarshal(out, segments); err != nil {
				outerr[i] = log.EData(err, out, log.M{"accid": accid, "i": i})
				return
			}

			lock.Lock()
			for j, seg := range segments.GetSegments() {
				totals[j] = totals[j] + seg.GetTotal()
			}
			lock.Unlock()
		}(i)
	}
	wg.Wait()
	for _, err := range outerr {
		if err != nil {
			return nil, err
		}
	}
	return totals, nil
}
