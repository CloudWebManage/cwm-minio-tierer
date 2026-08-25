package tierer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/tags"
)

func TestMinIOAdapterListsSortedBucketsAndCurrentObjectsWithMetadata(t *testing.T) {
	t.Parallel()
	sdk := &fakeMinIOSDK{
		buckets: []minio.BucketInfo{{Name: "z"}, {Name: "a"}},
		objects: []minio.ObjectInfo{{Key: "one", ETag: "etag", Size: 4, LastModified: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), VersionID: "v1", StorageClass: "WARM"}},
	}
	adapter := NewMinIOAdapter(sdk)
	buckets, err := adapter.Buckets(context.Background())
	if err != nil || len(buckets) != 2 || buckets[0] != "a" || buckets[1] != "z" {
		t.Fatalf("Buckets() = %v, %v", buckets, err)
	}
	listed := collectObjects(t, adapter.Objects(context.Background(), "a", "before"))
	if len(listed) != 1 || listed[0].Name != "one" || !listed[0].State.Transitioned || !listed[0].StateKnown {
		t.Fatalf("Objects() = %+v", listed)
	}
	if sdk.listBucket != "a" || sdk.listOptions.StartAfter != "before" || !sdk.listOptions.Recursive || !sdk.listOptions.WithMetadata || sdk.listOptions.WithVersions {
		t.Fatalf("ListObjects options = %+v, bucket = %q", sdk.listOptions, sdk.listBucket)
	}
}

func TestMinIOAdapterUsesListStateFastPathAndStatFallback(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	sdk := &fakeMinIOSDK{stat: minio.ObjectInfo{Key: "o", ETag: "e", Size: 2, LastModified: now, VersionID: "v", StorageClass: "WARM", Restore: &minio.RestoreInfo{ExpiryTime: now.Add(time.Hour)}}}
	adapter := NewMinIOAdapter(sdk)
	known := Object{Bucket: "b", Name: "o", StateKnown: true, State: ObjectState{Transitioned: true}}
	got, err := adapter.ResolveState(context.Background(), known)
	if err != nil || !got.State.Transitioned || sdk.statCalls != 0 {
		t.Fatalf("ResolveState(fast) = %+v, %v; stat calls = %d", got, err, sdk.statCalls)
	}
	unknown := Object{Bucket: "b", Name: "o", ETag: "e", Size: 2, LastModified: now, VersionID: "v"}
	got, err = adapter.ResolveState(context.Background(), unknown)
	if err != nil || got.State.Restore == nil || !got.State.Restore.Expires.Equal(now.Add(time.Hour)) || sdk.statVersion != "" {
		t.Fatalf("ResolveState(fallback) = %+v, %v; version = %q", got, err, sdk.statVersion)
	}
}

func TestMinIOAdapterStatsCurrentLogicalKeyAndCapturesOverwriteVersion(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	listed := Object{Bucket: "b", Name: "o", ETag: "old", Size: 2, LastModified: now, VersionID: "v1"}
	sdk := &fakeMinIOSDK{stat: minio.ObjectInfo{Key: "o", ETag: "new", Size: 3, LastModified: now.Add(time.Second), VersionID: "v2"}}
	current, err := NewMinIOAdapter(sdk).Stat(context.Background(), listed)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if sdk.statVersion != "" || current.VersionID != "v2" {
		t.Fatalf("Stat() requested version %q and returned version %q", sdk.statVersion, current.VersionID)
	}
	if SameIdentity(listed, current) {
		t.Fatal("SameIdentity() = true after current-key overwrite")
	}
}

func TestSameIdentityRequiresAvailableVersionAndAllStableFields(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	listed := Object{ETag: "e", LastModified: now, Size: 3, VersionID: "v1"}
	if !SameIdentity(listed, listed) {
		t.Fatal("SameIdentity() = false for identical object")
	}
	for _, changed := range []Object{
		{ETag: "other", LastModified: now, Size: 3, VersionID: "v1"},
		{ETag: "e", LastModified: now.Add(time.Nanosecond), Size: 3, VersionID: "v1"},
		{ETag: "e", LastModified: now, Size: 4, VersionID: "v1"},
		{ETag: "e", LastModified: now, Size: 3, VersionID: "v2"},
	} {
		if SameIdentity(listed, changed) {
			t.Fatalf("SameIdentity() = true for changed %+v", changed)
		}
	}
	listed.VersionID = ""
	changed := listed
	changed.VersionID = "newly-available"
	if !SameIdentity(listed, changed) {
		t.Fatal("SameIdentity() compared a version unavailable in listed identity")
	}
}

func TestSameIdentityAcceptsStatSecondPrecisionForSameListedInstant(t *testing.T) {
	t.Parallel()
	listed := Object{
		ETag:         "etag",
		LastModified: time.Date(2026, 8, 25, 11, 13, 28, 985_000_000, time.UTC),
		Size:         6,
	}
	current := listed
	current.LastModified = time.Date(2026, 8, 25, 11, 13, 28, 0, time.UTC)
	current.VersionID = "current-version"

	if !SameIdentity(listed, current) {
		t.Fatal("SameIdentity() rejected Stat's HTTP-second representation of the listed timestamp")
	}
}

func TestMinIOAdapterPlansMergesAndVerifiesExactVersionTagChanges(t *testing.T) {
	t.Parallel()
	sdk := &fakeMinIOSDK{tagMaps: []map[string]string{{"keep": "yes", "reserved": "old"}, {"keep": "yes", "reserved": "new"}}}
	adapter := NewMinIOAdapter(sdk)
	object := Object{Bucket: "b", Name: "o", VersionID: "v2"}
	plan, err := adapter.PlanMarker(context.Background(), object, "reserved", "new")
	if err != nil || !plan.Required || plan.Outcome != MarkerReplaced || plan.Tags["keep"] != "yes" || plan.Tags["reserved"] != "new" {
		t.Fatalf("PlanMarker() = %+v, %v", plan, err)
	}
	if err := adapter.ApplyMarker(context.Background(), object, plan); err != nil {
		t.Fatalf("ApplyMarker() error = %v", err)
	}
	if sdk.getTagVersions[0] != "v2" || sdk.putTagVersion != "v2" || sdk.putTags["keep"] != "yes" || sdk.putTags["reserved"] != "new" {
		t.Fatalf("tag versions/get=%v put=%q tags=%v", sdk.getTagVersions, sdk.putTagVersion, sdk.putTags)
	}
}

func TestMinIOAdapterMarkerNoopAndTenUnrelatedTags(t *testing.T) {
	t.Parallel()
	object := Object{Bucket: "b", Name: "o", VersionID: "v"}
	matching := &fakeMinIOSDK{tagMaps: []map[string]string{{"reserved": "new"}}}
	plan, err := NewMinIOAdapter(matching).PlanMarker(context.Background(), object, "reserved", "new")
	if err != nil || plan.Required || plan.Outcome != MarkerMatched {
		t.Fatalf("matching PlanMarker() = %+v, %v", plan, err)
	}
	full := make(map[string]string, 10)
	for _, key := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"} {
		full[key] = "v"
	}
	plan, err = NewMinIOAdapter(&fakeMinIOSDK{tagMaps: []map[string]string{full}}).PlanMarker(context.Background(), object, "reserved", "new")
	if err != nil || plan.Required || plan.Outcome != MarkerTagLimit {
		t.Fatalf("full PlanMarker() = %+v, %v", plan, err)
	}
}

func TestMinIOAdapterRestoreTargetsVersionAndAcceptsAlreadyInProgress(t *testing.T) {
	t.Parallel()
	sdk := &fakeMinIOSDK{}
	adapter := NewMinIOAdapter(sdk)
	object := Object{Bucket: "b", Name: "o", VersionID: "v3"}
	if pending, err := adapter.Restore(context.Background(), object, 7); err != nil || pending {
		t.Fatalf("Restore() = pending %v, error %v", pending, err)
	}
	if sdk.restoreVersion != "v3" || sdk.restoreDays != 7 {
		t.Fatalf("restore version = %q, days = %d", sdk.restoreVersion, sdk.restoreDays)
	}
	sdk.restoreErr = minio.ErrorResponse{Code: "RestoreAlreadyInProgress", Message: "pending"}
	if pending, err := adapter.Restore(context.Background(), object, 7); err != nil || !pending {
		t.Fatalf("Restore(already pending) = pending %v, error %v", pending, err)
	}
	sdk.restoreErr = errors.New("network")
	if _, err := adapter.Restore(context.Background(), object, 7); err == nil {
		t.Fatal("Restore(network) error = nil")
	}
}

func collectObjects(t *testing.T, stream <-chan ObjectResult) []Object {
	t.Helper()
	var result []Object
	for item := range stream {
		if item.Err != nil {
			t.Fatalf("Objects() error = %v", item.Err)
		}
		result = append(result, item.Object)
	}
	return result
}

type fakeMinIOSDK struct {
	buckets    []minio.BucketInfo
	objects    []minio.ObjectInfo
	listErr    error
	stat       minio.ObjectInfo
	statErr    error
	tagMaps    []map[string]string
	getTagErr  error
	putTagErr  error
	restoreErr error

	listBucket     string
	listOptions    minio.ListObjectsOptions
	statCalls      int
	statVersion    string
	getTagVersions []string
	putTagVersion  string
	putTags        map[string]string
	restoreVersion string
	restoreDays    int
}

func (f *fakeMinIOSDK) ListBuckets(context.Context) ([]minio.BucketInfo, error) {
	return f.buckets, nil
}

func (f *fakeMinIOSDK) ListObjects(_ context.Context, bucket string, options minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	f.listBucket, f.listOptions = bucket, options
	stream := make(chan minio.ObjectInfo, len(f.objects)+1)
	for _, object := range f.objects {
		stream <- object
	}
	if f.listErr != nil {
		stream <- minio.ObjectInfo{Err: f.listErr}
	}
	close(stream)
	return stream
}

func (f *fakeMinIOSDK) StatObject(_ context.Context, _, _ string, options minio.StatObjectOptions) (minio.ObjectInfo, error) {
	f.statCalls++
	f.statVersion = options.VersionID
	return f.stat, f.statErr
}

func (f *fakeMinIOSDK) GetObjectTagging(_ context.Context, _, _ string, options minio.GetObjectTaggingOptions) (*tags.Tags, error) {
	f.getTagVersions = append(f.getTagVersions, options.VersionID)
	if f.getTagErr != nil {
		return nil, f.getTagErr
	}
	var values map[string]string
	if len(f.tagMaps) > 0 {
		values = f.tagMaps[0]
		f.tagMaps = f.tagMaps[1:]
	} else {
		values = map[string]string{}
	}
	return tags.NewTags(values, true)
}

func (f *fakeMinIOSDK) PutObjectTagging(_ context.Context, _, _ string, value *tags.Tags, options minio.PutObjectTaggingOptions) error {
	f.putTagVersion = options.VersionID
	f.putTags = value.ToMap()
	return f.putTagErr
}

func (f *fakeMinIOSDK) RestoreObject(_ context.Context, _, _, version string, request minio.RestoreRequest) error {
	f.restoreVersion = version
	if request.Days != nil {
		f.restoreDays = *request.Days
	}
	return f.restoreErr
}
