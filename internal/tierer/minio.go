package tierer

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/tags"
)

type minioSDK interface {
	ListBuckets(context.Context) ([]minio.BucketInfo, error)
	ListObjects(context.Context, string, minio.ListObjectsOptions) <-chan minio.ObjectInfo
	StatObject(context.Context, string, string, minio.StatObjectOptions) (minio.ObjectInfo, error)
	GetObjectTagging(context.Context, string, string, minio.GetObjectTaggingOptions) (*tags.Tags, error)
	PutObjectTagging(context.Context, string, string, *tags.Tags, minio.PutObjectTaggingOptions) error
	RestoreObject(context.Context, string, string, string, minio.RestoreRequest) error
}

type MinIOAdapter struct {
	client minioSDK
}

func NewMinIOAdapter(client minioSDK) *MinIOAdapter { return &MinIOAdapter{client: client} }

type Object struct {
	Bucket       string
	Name         string
	ETag         string
	LastModified time.Time
	Size         int64
	VersionID    string
	State        ObjectState
	StateKnown   bool
}

type ObjectResult struct {
	Object Object
	Err    error
}

func (a *MinIOAdapter) Buckets(ctx context.Context) ([]string, error) {
	buckets, err := a.client.ListBuckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("list MinIO buckets: %w", err)
	}
	names := make([]string, len(buckets))
	for i, bucket := range buckets {
		names[i] = bucket.Name
	}
	sort.Strings(names)
	return names, nil
}

func (a *MinIOAdapter) Objects(ctx context.Context, bucket, startAfter string) <-chan ObjectResult {
	result := make(chan ObjectResult)
	options := minio.ListObjectsOptions{
		Recursive:    true,
		StartAfter:   startAfter,
		WithMetadata: true,
		WithVersions: false,
	}
	source := a.client.ListObjects(ctx, bucket, options)
	go func() {
		defer close(result)
		previous := startAfter
		for info := range source {
			if info.Err != nil {
				select {
				case result <- ObjectResult{Err: fmt.Errorf("list objects in bucket %q: %w", bucket, info.Err)}:
				case <-ctx.Done():
				}
				return
			}
			if info.Key <= previous {
				select {
				case result <- ObjectResult{Err: fmt.Errorf("MinIO object listing is not strictly sorted after %q", previous)}:
				case <-ctx.Done():
				}
				return
			}
			previous = info.Key
			select {
			case result <- ObjectResult{Object: objectFromInfo(bucket, info, false)}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return result
}

func objectFromInfo(bucket string, info minio.ObjectInfo, stat bool) Object {
	state, known := stateFromInfo(info)
	if stat && !known {
		known = true
	}
	return Object{
		Bucket:       bucket,
		Name:         info.Key,
		ETag:         strings.Trim(info.ETag, `"`),
		LastModified: info.LastModified,
		Size:         info.Size,
		VersionID:    info.VersionID,
		State:        state,
		StateKnown:   known,
	}
}

func stateFromInfo(info minio.ObjectInfo) (ObjectState, bool) {
	class := info.StorageClass
	if class == "" && info.Metadata != nil {
		class = info.Metadata.Get("X-Amz-Storage-Class")
	}
	state := ObjectState{}
	known := class != ""
	if class != "" && !strings.EqualFold(class, "STANDARD") {
		state.Transitioned = true
	}
	if info.Restore != nil {
		state.Transitioned = true
		known = true
		state.Restore = &RestoreState{Ongoing: info.Restore.OngoingRestore, Expires: info.Restore.ExpiryTime}
	}
	return state, known
}

func (a *MinIOAdapter) Stat(ctx context.Context, object Object) (Object, error) {
	info, err := a.client.StatObject(ctx, object.Bucket, object.Name, minio.StatObjectOptions{})
	if err != nil {
		return Object{}, fmt.Errorf("stat MinIO object: %w", err)
	}
	if info.Key == "" {
		info.Key = object.Name
	}
	return objectFromInfo(object.Bucket, info, true), nil
}

func (a *MinIOAdapter) ResolveState(ctx context.Context, object Object) (Object, error) {
	if object.StateKnown {
		return object, nil
	}
	return a.Stat(ctx, object)
}

func SameIdentity(listed, current Object) bool {
	if strings.Trim(listed.ETag, `"`) != strings.Trim(current.ETag, `"`) ||
		!sameLastModified(listed.LastModified, current.LastModified) || listed.Size != current.Size {
		return false
	}
	return listed.VersionID == "" || listed.VersionID == current.VersionID
}

// S3 listings carry subsecond LastModified values, while HEAD represents the
// same value through an HTTP-date with second precision. Only normalize that
// specific loss of precision from the current-key Stat response.
func sameLastModified(listed, current time.Time) bool {
	return listed.Equal(current) || (current.Nanosecond() == 0 && listed.Truncate(time.Second).Equal(current))
}

type MarkerOutcome string

const (
	MarkerMatched  MarkerOutcome = "matched"
	MarkerAdded    MarkerOutcome = "added"
	MarkerReplaced MarkerOutcome = "replaced"
	MarkerTagLimit MarkerOutcome = "tag_limit"
)

type MarkerPlan struct {
	Required bool
	Outcome  MarkerOutcome
	Tags     map[string]string
}

func (a *MinIOAdapter) PlanMarker(ctx context.Context, object Object, key, value string) (MarkerPlan, error) {
	current, err := a.client.GetObjectTagging(ctx, object.Bucket, object.Name, minio.GetObjectTaggingOptions{VersionID: object.VersionID})
	if err != nil {
		return MarkerPlan{}, fmt.Errorf("get exact-version object tags: %w", err)
	}
	values := current.ToMap()
	if existing, present := values[key]; present && existing == value {
		return MarkerPlan{Outcome: MarkerMatched, Tags: values}, nil
	} else if present {
		values[key] = value
		return MarkerPlan{Required: true, Outcome: MarkerReplaced, Tags: values}, nil
	}
	if len(values) >= 10 {
		return MarkerPlan{Outcome: MarkerTagLimit, Tags: values}, nil
	}
	values[key] = value
	return MarkerPlan{Required: true, Outcome: MarkerAdded, Tags: values}, nil
}

func (a *MinIOAdapter) ApplyMarker(ctx context.Context, object Object, plan MarkerPlan) error {
	if !plan.Required {
		return nil
	}
	objectTags, err := tags.NewTags(plan.Tags, true)
	if err != nil {
		return fmt.Errorf("construct object tags: %w", err)
	}
	options := minio.PutObjectTaggingOptions{VersionID: object.VersionID}
	if err := a.client.PutObjectTagging(ctx, object.Bucket, object.Name, objectTags, options); err != nil {
		return fmt.Errorf("put exact-version object tags: %w", err)
	}
	verified, err := a.client.GetObjectTagging(ctx, object.Bucket, object.Name, minio.GetObjectTaggingOptions{VersionID: object.VersionID})
	if err != nil {
		return fmt.Errorf("verify exact-version object tags: %w", err)
	}
	actual := verified.ToMap()
	if len(actual) != len(plan.Tags) {
		return errors.New("object tag verification failed: tag count changed")
	}
	for key, value := range plan.Tags {
		if actual[key] != value {
			return fmt.Errorf("object tag verification failed for key %q", key)
		}
	}
	return nil
}

func (a *MinIOAdapter) Restore(ctx context.Context, object Object, days int) (bool, error) {
	if days <= 0 {
		return false, errors.New("restore days must be positive")
	}
	request := minio.RestoreRequest{}
	request.SetDays(days)
	request.SetGlacierJobParameters(minio.GlacierJobParameters{Tier: minio.TierStandard})
	err := a.client.RestoreObject(ctx, object.Bucket, object.Name, object.VersionID, request)
	if err == nil {
		return false, nil
	}
	if minio.ToErrorResponse(err).Code == "RestoreAlreadyInProgress" {
		return true, nil
	}
	return false, fmt.Errorf("restore exact-version object: %w", err)
}
