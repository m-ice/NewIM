package authsession

import (
	"slices"
	"testing"

	app "github.com/m-ice/NewIM/server/auth/session"
)

func TestChooseVictimsAppliesPlatformBeforeAccountLimit(t *testing.T) {
	devices := []activeDevice{
		{id: "older_desktop", platform: "desktop"},
		{id: "older_mobile", platform: "mobile"},
		{id: "newer_mobile", platform: "mobile"},
	}
	request := app.AdmissionRequest{
		DeviceID: "incoming",
		Platform: "mobile",
		Limits:   app.DeviceLimits{Account: 3, Platform: 2},
	}
	victims, err := chooseVictims(devices, request)
	if err != nil || !slices.Equal(victims, []string{"older_mobile"}) {
		t.Fatalf("victims=%v error=%v", victims, err)
	}
}

func TestChooseVictimsPreservesRequestedDeviceWhenLimitsDecrease(t *testing.T) {
	devices := []activeDevice{
		{id: "incoming", platform: "mobile"},
		{id: "older_desktop", platform: "desktop"},
		{id: "older_mobile", platform: "mobile"},
		{id: "newer_mobile", platform: "mobile"},
	}
	request := app.AdmissionRequest{
		DeviceID: "incoming",
		Platform: "mobile",
		Limits:   app.DeviceLimits{Account: 1, Platform: 2},
	}
	victims, err := chooseVictims(devices, request)
	if err != nil || !slices.Equal(victims, []string{"older_mobile", "older_desktop", "newer_mobile"}) {
		t.Fatalf("victims=%v error=%v", victims, err)
	}
}

func TestChooseVictimsReconcilesOtherPlatformsAfterLimitDecrease(t *testing.T) {
	devices := []activeDevice{
		{id: "older_desktop", platform: "desktop"},
		{id: "android_a", platform: "android"},
		{id: "android_b", platform: "android"},
		{id: "android_c", platform: "android"},
		{id: "android_d", platform: "android"},
	}
	request := app.AdmissionRequest{
		DeviceID: "incoming",
		Platform: "ios",
		Limits:   app.DeviceLimits{Account: 6, Platform: 3},
	}
	victims, err := chooseVictims(devices, request)
	if err != nil || !slices.Equal(victims, []string{"android_a"}) {
		t.Fatalf("victims=%v error=%v", victims, err)
	}
}
