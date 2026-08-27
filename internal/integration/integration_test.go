//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/orihoch/cwm-minio-tierer/internal/contracts"
	"github.com/orihoch/cwm-minio-tierer/internal/tierer"
	"github.com/redis/go-redis/v9"
)

const integrationInstance = "compose-integration"

func TestTransitionStorageClassUsesMetadataFallback(t *testing.T) {
	t.Parallel()
	info := minio.ObjectInfo{Metadata: http.Header{"X-Amz-Storage-Class": []string{"CWMLOW"}}}
	if got := transitionStorageClass(info); got != "CWMLOW" {
		t.Fatalf("transitionStorageClass() = %q, want CWMLOW", got)
	}
}

func TestRedisConfigurationAndUpdaterHTTPAtomicLua(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	redisClient := integrationRedis(t)

	for key, want := range map[string]string{"appendonly": "yes", "appendfsync": "always", "maxmemory-policy": "noeviction"} {
		config, err := redisClient.ConfigGet(ctx, key).Result()
		if err != nil {
			t.Fatalf("CONFIG GET %s: %v", key, err)
		}
		if got := config[key]; got != want {
			t.Fatalf("Redis %s = %q, want %q", key, got, want)
		}
	}

	token := uniqueToken(t)
	object := "http/duplicate-" + token
	acceptedBefore := time.Now().UTC()
	status := postRecords(t, ctx, updaterURL(t), "application/x-ndjson",
		fmt.Sprintf("{\"bucket\":\"integration\",\"object\":%q}\n{\"bucket\":\"integration\",\"object\":%q}\n", object, object))
	acceptedAfter := time.Now().UTC()
	if status != http.StatusOK {
		t.Fatalf("duplicate updater POST status = %d, want 200", status)
	}
	key, hour := populatedAccessKey(t, ctx, redisClient, "integration", object, acceptedBefore, acceptedAfter)
	if got, err := redisClient.Get(ctx, key).Result(); err != nil || got != "2" {
		t.Fatalf("aggregated access counter = %q, %v; want 2", got, err)
	}
	expiresUnix, err := redisClient.ExpireTime(ctx, key).Result()
	if err != nil {
		t.Fatalf("EXPIRETIME %q: %v", key, err)
	}
	expires := time.Unix(int64(expiresUnix/time.Second), 0).UTC()
	wantExpiry := hour.Add(time.Hour + 72*time.Hour)
	if !expires.Equal(wantExpiry) {
		t.Fatalf("counter expiry = %s, want %s", expires, wantExpiry)
	}

	rejectedObject := "http/rejected-" + token
	status = postRecords(t, ctx, updaterURL(t), "application/json",
		fmt.Sprintf("{\"bucket\":\"integration\",\"object\":%q,\"unknown\":true}", rejectedObject))
	if status != http.StatusInternalServerError {
		t.Fatalf("strict updater POST status = %d, want 500", status)
	}
	assertNoAccessCounter(t, ctx, redisClient, "integration", rejectedObject, time.Now().UTC())

	badObject := "http/wrong-type-" + token
	goodObject := "http/atomic-good-" + token
	boundary := time.Now().UTC().Truncate(time.Hour)
	for _, candidateHour := range []time.Time{boundary, boundary.Add(time.Hour)} {
		badKey, err := contracts.AccessKey(integrationInstance, "integration", badObject, candidateHour)
		if err != nil {
			t.Fatal(err)
		}
		if err := redisClient.RPush(ctx, badKey, "wrong-type").Err(); err != nil {
			t.Fatalf("seed wrong-type counter: %v", err)
		}
	}
	status = postRecords(t, ctx, updaterURL(t), "application/x-ndjson",
		fmt.Sprintf("{\"bucket\":\"integration\",\"object\":%q}\n{\"bucket\":\"integration\",\"object\":%q}\n", goodObject, badObject))
	if status != http.StatusInternalServerError {
		t.Fatalf("wrong-type atomic updater POST status = %d, want 500", status)
	}
	assertNoAccessCounter(t, ctx, redisClient, "integration", goodObject, boundary)

	if err := redisClient.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	noscriptObject := "http/noscript-" + token
	status = postRecords(t, ctx, updaterURL(t), "application/json",
		fmt.Sprintf("{\"bucket\":\"integration\",\"object\":%q}", noscriptObject))
	if status != http.StatusOK {
		t.Fatalf("NOSCRIPT recovery POST status = %d, want 200", status)
	}
	_, _ = populatedAccessKey(t, ctx, redisClient, "integration", noscriptObject, time.Now().Add(-time.Second), time.Now().Add(time.Second))
}

func TestRealRedisCursorReadsAndAtomicBudgets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := integrationRedis(t)
	instance := "integration-" + uniqueToken(t)
	store := tierer.NewRedisStore(tierer.NewRedisClient(client), instance, "coverage:g1:2006:01:02:15", "complete:g1", true)

	scope := tierer.ScopeHash([]string{"excluded"}, []string{"tmp-"}, "cwm-tier", "low")
	wantCursor := tierer.Cursor{Bucket: "integration", Object: "cursor/object", UpdatedAt: time.Now().UTC().Truncate(time.Second)}
	if err := store.SaveCursor(ctx, scope, wantCursor); err != nil {
		t.Fatalf("SaveCursor: %v", err)
	}
	reopened := tierer.NewRedisStore(tierer.NewRedisClient(client), instance, "coverage:g1:2006:01:02:15", "complete:g1", true)
	gotCursor, err := reopened.LoadCursor(ctx, scope)
	if err != nil || gotCursor == nil || gotCursor.Bucket != wantCursor.Bucket || gotCursor.Object != wantCursor.Object || !gotCursor.UpdatedAt.Equal(wantCursor.UpdatedAt) {
		t.Fatalf("LoadCursor = %+v, %v; want %+v", gotCursor, err, wantCursor)
	}

	now := time.Now().UTC()
	results := make(chan tierer.BudgetReservation, 2)
	errors := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			reservation, err := store.Reserve(ctx, now, tierer.BudgetTransition, tierer.BudgetLimit{Attempts: 1, Bytes: 10}, 6)
			results <- reservation
			errors <- err
		}()
	}
	wait.Wait()
	close(results)
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatalf("Reserve: %v", err)
		}
	}
	allowed := 0
	for result := range results {
		if result.Allowed {
			allowed++
		}
	}
	if allowed != 1 {
		t.Fatalf("concurrent allowed reservations = %d, want 1", allowed)
	}

	hour := now.Truncate(time.Hour).Add(-time.Hour)
	accessKey, err := contracts.AccessKey(instance, "integration", "wrong-type", hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.RPush(ctx, accessKey, "bad").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadCounts(ctx, "integration", "wrong-type", []time.Time{hour}); err == nil {
		t.Fatal("ReadCounts accepted a real Redis wrong-type counter")
	}
}

func TestPinnedMinIOCurrentVersionAndExactVersionTags(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := integrationMinIO(t)
	bucket := "metadata-" + uniqueToken(t)
	makeVersionedBucket(t, ctx, client, bucket)
	t.Cleanup(func() { cleanupBucket(t, client, bucket) })

	key := "logical/current.txt"
	first, err := client.PutObject(ctx, bucket, key, strings.NewReader("first"), 5, minio.PutObjectOptions{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("put first version: %v", err)
	}
	second, err := client.PutObject(ctx, bucket, key, strings.NewReader("second"), 6, minio.PutObjectOptions{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("put second version: %v", err)
	}
	if first.VersionID == "" || second.VersionID == "" || first.VersionID == second.VersionID {
		t.Fatalf("MinIO version IDs first=%q second=%q", first.VersionID, second.VersionID)
	}
	keepTags, err := tags.NewTags(map[string]string{"keep": "yes"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.PutObjectTagging(ctx, bucket, key, keepTags, minio.PutObjectTaggingOptions{VersionID: second.VersionID}); err != nil {
		t.Fatalf("put exact-current tags: %v", err)
	}

	adapter := tierer.NewMinIOAdapter(client)
	var listed []tierer.Object
	for result := range adapter.Objects(ctx, bucket, "") {
		if result.Err != nil {
			t.Fatalf("list current objects: %v", result.Err)
		}
		listed = append(listed, result.Object)
	}
	if len(listed) != 1 || listed[0].Name != key {
		t.Fatalf("current logical listing = %+v, want one current key", listed)
	}
	if listed[0].VersionID != "" {
		t.Fatalf("exact-release current listing unexpectedly supplied VersionID %q", listed[0].VersionID)
	}
	t.Log("exact-release current listing omitted VersionID; validating current-key Stat fallback")
	current, err := adapter.Stat(ctx, listed[0])
	if err != nil {
		t.Fatalf("stat current logical key: %v", err)
	}
	if current.VersionID != second.VersionID || !tierer.SameIdentity(listed[0], current) {
		t.Fatalf("current stat = %+v, listed = %+v", current, listed[0])
	}
	plan, err := adapter.PlanMarker(ctx, current, "cwm-tier", "low")
	if err != nil || !plan.Required || plan.Outcome != tierer.MarkerAdded {
		t.Fatalf("PlanMarker = %+v, %v", plan, err)
	}
	if err := adapter.ApplyMarker(ctx, current, plan); err != nil {
		t.Fatalf("ApplyMarker: %v", err)
	}
	actualTags, err := client.GetObjectTagging(ctx, bucket, key, minio.GetObjectTaggingOptions{VersionID: second.VersionID})
	if err != nil {
		t.Fatalf("get exact-current tags: %v", err)
	}
	if got := actualTags.ToMap(); got["keep"] != "yes" || got["cwm-tier"] != "low" || len(got) != 2 {
		t.Fatalf("merged tags = %v", got)
	}
	firstTags, err := client.GetObjectTagging(ctx, bucket, key, minio.GetObjectTaggingOptions{VersionID: first.VersionID})
	if err != nil {
		t.Fatalf("get historical tags: %v", err)
	}
	if len(firstTags.ToMap()) != 0 {
		t.Fatalf("historical version tags changed: %v", firstTags.ToMap())
	}
}

func TestPinnedMinIOTransitionAndRestoreMetadata(t *testing.T) {
	if os.Getenv("INTEGRATION_MINIO_ILM") != "true" {
		t.Skip("set INTEGRATION_MINIO_ILM=true only when the external tier bootstrap fixture is active")
	}
	deadline := envDuration(t, "INTEGRATION_MINIO_POLL_TIMEOUT", 3*time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), deadline+30*time.Second)
	defer cancel()
	client := integrationMinIO(t)
	bucket := envOr("INTEGRATION_MINIO_ILM_BUCKET", "tierer-integration")
	key := "transition/" + uniqueToken(t) + ".txt"
	body := "transition-and-restore"
	upload, err := client.PutObject(ctx, bucket, key, strings.NewReader(body), int64(len(body)), minio.PutObjectOptions{ContentType: "text/plain", UserTags: map[string]string{"cwm-tier": "low"}})
	if err != nil {
		t.Fatalf("put transition fixture: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = client.RemoveObject(cleanupCtx, bucket, key, minio.RemoveObjectOptions{VersionID: upload.VersionID})
	})

	transitioned := pollStat(t, ctx, client, bucket, key, deadline, func(info minio.ObjectInfo) bool {
		class := transitionStorageClass(info)
		return class != "" && !strings.EqualFold(class, "STANDARD")
	})
	adapter := tierer.NewMinIOAdapter(client)
	object, err := adapter.Stat(ctx, tierer.Object{Bucket: bucket, Name: key, ETag: transitioned.ETag, LastModified: transitioned.LastModified, Size: transitioned.Size, VersionID: transitioned.VersionID})
	if err != nil {
		t.Fatalf("adapter stat transitioned object: %v", err)
	}
	if !object.StateKnown || !object.State.Transitioned {
		t.Fatalf("adapter did not recognize transition metadata: %+v", object)
	}
	pending, err := adapter.Restore(ctx, object, 1)
	if err != nil {
		t.Fatalf("initial restore: %v", err)
	}
	t.Logf("initial restore accepted (already pending=%v), storage class=%q", pending, transitionStorageClass(transitioned))
	restored := pollStat(t, ctx, client, bucket, key, deadline, func(info minio.ObjectInfo) bool { return info.Restore != nil })
	if restored.Restore == nil {
		t.Fatal("restore metadata missing after bounded poll")
	}
	sawOngoing := restored.Restore.OngoingRestore
	if restored.Restore.OngoingRestore {
		restored = pollStat(t, ctx, client, bucket, key, deadline, func(info minio.ObjectInfo) bool {
			return info.Restore != nil && !info.Restore.OngoingRestore && info.Restore.ExpiryTime.After(time.Now().UTC())
		})
	}
	t.Logf("initial restore completed (observed ongoing=%v), expiry=%s", sawOngoing, restored.Restore.ExpiryTime)
	active, err := adapter.Stat(ctx, tierer.Object{Bucket: bucket, Name: key, ETag: restored.ETag, LastModified: restored.LastModified, Size: restored.Size, VersionID: restored.VersionID})
	if err != nil {
		t.Fatalf("adapter stat restored object: %v", err)
	}
	if active.State.Restore == nil || active.State.Restore.Ongoing || !active.State.Restore.Expires.After(time.Now().UTC()) {
		t.Fatalf("active restore metadata = %+v", active.State.Restore)
	}
	initialExpiry := active.State.Restore.Expires
	const renewalDays = 3
	if pending, err := adapter.Restore(ctx, active, renewalDays); err != nil {
		t.Fatalf("restore renewal: %v", err)
	} else {
		t.Logf("restore renewal accepted (already pending=%v), previous expiry=%s, requested days=%d", pending, initialExpiry, renewalDays)
	}
	renewed := pollStat(t, ctx, client, bucket, key, deadline, func(info minio.ObjectInfo) bool {
		return info.Restore != nil && !info.Restore.OngoingRestore && info.Restore.ExpiryTime.After(initialExpiry)
	})
	if renewed.Restore == nil || !renewed.Restore.ExpiryTime.After(initialExpiry) {
		t.Fatalf("renewed expiry = %+v, want after %s", renewed.Restore, initialExpiry)
	}
	t.Logf("restore expiry extended from %s to %s", initialExpiry, renewed.Restore.ExpiryTime)
}

func TestRunningServicesHealthAndAuditMetrics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for name, endpoint := range map[string]string{
		"updater": strings.TrimRight(updaterURL(t), "/") + "/readyz",
		"tierer":  strings.TrimRight(requireEnv(t, "INTEGRATION_TIERER_URL"), "/") + "/readyz",
	} {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatalf("%s readiness: %v", name, err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s readiness status = %d", name, response.StatusCode)
		}
	}

	metricsURL := strings.TrimRight(requireEnv(t, "INTEGRATION_TIERER_URL"), "/") + "/metrics"
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("tierer metrics: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		t.Fatal(err)
	}
	for _, metric := range []string{"cwm_minio_tierer_scan", "cwm_minio_tierer_coverage"} {
		if !bytes.Contains(body, []byte(metric)) {
			t.Fatalf("tierer metrics missing %q", metric)
		}
	}
}

func integrationRedis(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: requireEnv(t, "INTEGRATION_REDIS_ADDR"), DialTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second})
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis ping: %v", err)
	}
	return client
}

func integrationMinIO(t *testing.T) *minio.Client {
	t.Helper()
	client, err := minio.New(requireEnv(t, "INTEGRATION_MINIO_ENDPOINT"), &minio.Options{
		Creds:  credentials.NewStaticV4(requireEnv(t, "INTEGRATION_MINIO_ACCESS_KEY"), requireEnv(t, "INTEGRATION_MINIO_SECRET_KEY"), ""),
		Secure: false,
	})
	if err != nil {
		t.Fatalf("MinIO client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.ListBuckets(ctx); err != nil {
		t.Fatalf("MinIO list buckets: %v", err)
	}
	return client
}

func updaterURL(t *testing.T) string {
	t.Helper()
	return strings.TrimRight(requireEnv(t, "INTEGRATION_UPDATER_URL"), "/") + "/"
}

func postRecords(t *testing.T, ctx context.Context, endpoint, mediaType, body string) int {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", mediaType)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("POST updater: %v", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	return response.StatusCode
}

func populatedAccessKey(t *testing.T, ctx context.Context, client *redis.Client, bucket, object string, before, after time.Time) (string, time.Time) {
	t.Helper()
	first := before.UTC().Truncate(time.Hour)
	last := after.UTC().Truncate(time.Hour)
	for hour := first; !hour.After(last); hour = hour.Add(time.Hour) {
		key, err := contracts.AccessKey(integrationInstance, bucket, object, hour)
		if err != nil {
			t.Fatal(err)
		}
		if client.Exists(ctx, key).Val() == 1 {
			return key, hour
		}
	}
	t.Fatalf("no access key populated for %s/%s between %s and %s", bucket, object, before, after)
	return "", time.Time{}
}

func assertNoAccessCounter(t *testing.T, ctx context.Context, client *redis.Client, bucket, object string, around time.Time) {
	t.Helper()
	hour := around.UTC().Truncate(time.Hour)
	for _, candidate := range []time.Time{hour.Add(-time.Hour), hour, hour.Add(time.Hour)} {
		key, err := contracts.AccessKey(integrationInstance, bucket, object, candidate)
		if err != nil {
			t.Fatal(err)
		}
		if kind, err := client.Type(ctx, key).Result(); err != nil || kind != "none" {
			t.Fatalf("unexpected access counter %q type=%q error=%v", key, kind, err)
		}
	}
}

func makeVersionedBucket(t *testing.T, ctx context.Context, client *minio.Client, bucket string) {
	t.Helper()
	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		t.Fatalf("make bucket %q: %v", bucket, err)
	}
	if err := client.EnableVersioning(ctx, bucket); err != nil {
		t.Fatalf("enable versioning %q: %v", bucket, err)
	}
}

func cleanupBucket(t *testing.T, client *minio.Client, bucket string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for object := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
		if object.Err != nil {
			t.Logf("cleanup list %q: %v", bucket, object.Err)
			return
		}
		if err := client.RemoveObject(ctx, bucket, object.Key, minio.RemoveObjectOptions{VersionID: object.VersionID}); err != nil {
			t.Logf("cleanup object %q/%q version %q: %v", bucket, object.Key, object.VersionID, err)
		}
	}
	if err := client.RemoveBucket(ctx, bucket); err != nil {
		t.Logf("cleanup bucket %q: %v", bucket, err)
	}
}

func pollStat(t *testing.T, ctx context.Context, client *minio.Client, bucket, key string, timeout time.Duration, done func(minio.ObjectInfo) bool) minio.ObjectInfo {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var last minio.ObjectInfo
	var lastErr error
	for {
		last, lastErr = client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
		if lastErr == nil && done(last) {
			return last
		}
		select {
		case <-ctx.Done():
			t.Fatalf("poll stat %s/%s: %v (last error %v, last metadata %+v)", bucket, key, ctx.Err(), lastErr, last)
		case <-deadline.C:
			t.Fatalf("poll stat %s/%s exceeded %s (last error %v, storage class %q, restore %+v)", bucket, key, timeout, lastErr, transitionStorageClass(last), last.Restore)
		case <-ticker.C:
		}
	}
}

func transitionStorageClass(info minio.ObjectInfo) string {
	class := info.StorageClass
	if class == "" && info.Metadata != nil {
		class = info.Metadata.Get("X-Amz-Storage-Class")
	}
	return class
}

func uniqueToken(t *testing.T) string {
	t.Helper()
	buffer := make([]byte, 6)
	if _, err := rand.Read(buffer); err != nil {
		t.Fatalf("random token: %v", err)
	}
	return hex.EncodeToString(buffer)
}

func requireEnv(t *testing.T, key string) string {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		t.Skipf("%s is required for integration tests", key)
	}
	return value
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envDuration(t *testing.T, key string, fallback time.Duration) time.Duration {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		t.Fatalf("%s must be a positive Go duration", key)
	}
	return parsed
}
